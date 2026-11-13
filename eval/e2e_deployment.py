"""Temporarily instrument exact deployed packages; preserve originals and guard rollback."""
import base64
import hashlib
import io
import json
import time
import zipfile
from pathlib import Path

PATCH_FILES = {
    "LostLink-MatchWorker": ["pipeline/worker.py", "pipeline/lambda_worker.py", "pipeline/impl/description.py"],
    "LostLink-ReportsFn": ["api/reports_handler.py"],
    "LostLink-ClaimsFn": ["api/claims_handler.py"],
    "LostLink-DescriptionsFn": ["api/descriptions_handler.py", "pipeline/impl/description.py"],
}


class ExperimentDeployment:
    def __init__(self, snapshot, client):
        self.directory = Path(snapshot)
        self.path = self.directory / "snapshot.json"
        self.state = json.loads(self.path.read_text())
        self.client = client
        self.root = Path(__file__).resolve().parents[1]

    def save(self):
        self.path.write_text(json.dumps(self.state, indent=2, default=str))

    def install(self):
        if self.state.get("source_mismatches") or not self.state.get("sender_is_disabled"):
            raise RuntimeError("Snapshot did not verify matching source and disabled email delivery.")
        for name, files in PATCH_FILES.items():
            saved = self.state["functions"][name]
            current = self.client.get_function_configuration(FunctionName=name)
            if current["CodeSha256"] != saved["CodeSha256"]:
                raise RuntimeError(f"{name} changed since snapshot; refusing to overwrite it.")
            replacements = {file: (self.root / "backend" / file).read_bytes() for file in files}
            replacements["pipeline/telemetry.py"] = (self.root / "backend/pipeline/telemetry.py").read_bytes()
            data = io.BytesIO()
            with zipfile.ZipFile(self.directory / (name + ".zip")) as source, zipfile.ZipFile(data, "w", zipfile.ZIP_DEFLATED) as target:
                for member in source.infolist():
                    if member.filename not in replacements:
                        target.writestr(member, source.read(member.filename))
                for path, value in replacements.items():
                    target.writestr(path, value)
            package = data.getvalue()
            (self.directory / (name + ".instrumented.zip")).write_bytes(package)
            saved["instrumented_hash"] = base64.b64encode(hashlib.sha256(package).digest()).decode()
            self.save()  # Recovery information exists before the mutation.
            self.client.update_function_code(FunctionName=name, ZipFile=package, RevisionId=current["RevisionId"])
            self.client.get_waiter("function_updated_v2").wait(FunctionName=name)
            print(f"Instrumented {name}", flush=True)

    def worker_limit(self, maximum):
        mapping = self.state["worker_mappings"][0]
        current = self.client.get_event_source_mapping(UUID=mapping["UUID"])
        allowed = [mapping.get("ScalingConfig", {}), self.state.get("experiment_scaling", {})]
        if current.get("ScalingConfig", {}) not in allowed:
            raise RuntimeError("Worker scaling was changed externally; refusing to overwrite it.")
        scaling = {} if maximum is None else {"MaximumConcurrency": maximum}
        self.state["experiment_scaling"] = scaling
        self.save()
        self.client.update_event_source_mapping(UUID=mapping["UUID"], ScalingConfig=scaling)
        for _ in range(60):
            current = self.client.get_event_source_mapping(UUID=mapping["UUID"])
            if current["State"] == "Enabled" and current.get("ScalingConfig", {}) == scaling:
                print(f"SQS maximum worker concurrency: {maximum or 'original uncapped'}", flush=True)
                return
            time.sleep(1)
        raise RuntimeError("SQS event-source update did not settle.")

    def restore(self):
        errors = []
        mapping = self.state["worker_mappings"][0]
        if "experiment_scaling" in self.state:
            try:
                current = self.client.get_event_source_mapping(UUID=mapping["UUID"])
                original = mapping.get("ScalingConfig", {})
                if current.get("ScalingConfig", {}) not in [self.state["experiment_scaling"], original]:
                    raise RuntimeError("Scaling changed externally; manual review required.")
                self.client.update_event_source_mapping(UUID=mapping["UUID"], ScalingConfig=original)
                for _ in range(60):
                    verified = self.client.get_event_source_mapping(UUID=mapping["UUID"])
                    if verified["State"] == "Enabled" and verified.get("ScalingConfig", {}) == original:
                        self.state["scaling_restored"] = True
                        break
                    time.sleep(1)
                else:
                    raise RuntimeError("Original scaling configuration did not settle.")
            except Exception as error:
                errors.append(str(error))
        for name, saved in self.state["functions"].items():
            if not saved.get("instrumented_hash"):
                continue
            try:
                current = self.client.get_function_configuration(FunctionName=name)
                if current["CodeSha256"] == saved["CodeSha256"]:
                    saved["restored"] = True
                    continue
                if current["CodeSha256"] != saved["instrumented_hash"]:
                    raise RuntimeError(f"{name} changed externally; manual rollback review required.")
                self.client.update_function_code(FunctionName=name, ZipFile=(self.directory / (name + ".zip")).read_bytes(),
                                                 RevisionId=current["RevisionId"])
                self.client.get_waiter("function_updated_v2").wait(FunctionName=name)
                saved["restored"] = True
                print(f"Restored {name}", flush=True)
            except Exception as error:
                errors.append(str(error))
            self.save()
        self.state["restore_errors"] = errors
        self.save()
        if errors:
            raise RuntimeError("Rollback needs review: " + "; ".join(errors))
