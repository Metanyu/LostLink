#!/usr/bin/env python3
"""Join customer observations, internal stage timings, and CloudWatch into one report."""
import argparse
import csv
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import re
import statistics

import boto3
from cloudwatch_loadtest import collect, utc

FUNCTIONS = ["LostLink-MatchWorker", "LostLink-ReportsFn", "LostLink-ClaimsFn", "LostLink-DescriptionsFn"]
STAGES = ["load_item", "text_embedding", "persist_enrichment", "retrieve_candidates", "invalidate_old_matches",
          "score_candidates", "persist_matches", "notification_path", "update_matched_report", "final_status"]


def stats(values):
    if not values:
        return None
    values = sorted(values)
    return {"n": len(values), "mean": statistics.mean(values), "p50": values[math.ceil(len(values) * .5) - 1],
            "p95": values[math.ceil(len(values) * .95) - 1], "max": max(values)}


def logs_for(logs, function, start, finish, pattern):
    return [event for page in logs.get_paginator("filter_log_events").paginate(
        logGroupName="/aws/lambda/" + function, startTime=int(start.timestamp() * 1000),
        endTime=int(finish.timestamp() * 1000), filterPattern=pattern) for event in page["events"]]


def get_evidence(run, session, directory):
    start, finish = utc(run["measured_started_at_utc"]), utc(run["finished_at_utc"]) + timedelta(seconds=2)
    logs = session.client("logs")
    records, runtime, errors = [], [], []
    for function in FUNCTIONS:
        try:
            for event in logs_for(logs, function, start, finish, '"lostlink_timing_v1"'):
                message = event["message"]
                record = json.loads(message[message.index("{"):])
                record["function"] = function
                records.append(record)
            for event in logs_for(logs, function, start, finish, '"REPORT RequestId"'):
                runtime.append({"function": function, "message": event["message"]})
        except Exception as error:
            errors.append({"function": function, "error": str(error)})
    evidence = {"collected_at_utc": datetime.now(timezone.utc).isoformat(), "records": records, "runtime": runtime, "errors": errors}
    (directory / (run["name"] + "-traces.json")).write_text(json.dumps(evidence, indent=2))
    cloud_run = {"run_id": run["run_id"], "region": session.region_name,
                 "measured_phase_started_at_utc": run["measured_started_at_utc"],
                 "measured_phase_finished_at_utc": run["measured_finished_at_utc"],
                 "finished_at_utc": run["finished_at_utc"],
                 "submission_observations": [{**row, "start_offset_seconds": row.get("report_start_offset_seconds", row["start_offset_seconds"])}
                                             for row in run["observations"]]}
    cloud = collect(cloud_run, session)
    (directory / (run["name"] + "-cloudwatch.json")).write_text(json.dumps(cloud, indent=2, default=str))
    return evidence, cloud


def summarize(run, evidence, cloud):
    accepted = {row["item_id"]: row for row in run["observations"] if row.get("http_status") == 201}
    jobs = {}
    for record in sorted(evidence["records"], key=lambda row: row["started_epoch_ms"]):
        if record["kind"] == "match" and record.get("item_id") in accepted and record["outcome"] == "complete":
            jobs.setdefault(record["item_id"], record)
    posts = [row for row in evidence["records"] if row["kind"] == "api" and row.get("item_id") in accepted]
    read_ids = {row.get("request_id") for row in run["read_observations"] if row.get("request_id")}
    reads = [row for row in evidence["records"] if row["kind"] == "api" and row.get("request_id") in read_ids]
    description_ids = {row.get("description_request_id") for row in run["observations"] if row.get("description_request_id")}
    descriptions = [row for row in evidence["records"] if row.get("description_request_id") in description_ids]
    durations = [row["duration_ms"] / 1000 for row in jobs.values()]
    stages = {name: stats([job.get("stages", {}).get(name, {}).get("duration_ms", 0) / 1000 for job in jobs.values()]) for name in STAGES}
    queue = [row["started_epoch_ms"] / 1000 - float(row["sent_epoch_ms"]) / 1000 for row in jobs.values() if row.get("sent_epoch_ms")]
    actual_queue = [(float(row["first_receive_epoch_ms"]) - float(row["sent_epoch_ms"])) / 1000
                    for row in jobs.values() if row.get("sent_epoch_ms") and row.get("first_receive_epoch_ms")]
    result = {"name": run["name"], "inventory": run["inventory"], "scheduled": run["scheduled"],
              "attempted": len(run["observations"]), "accepted": len(accepted), "customer_visible": run["customer_visible"],
              "worker_complete": len(jobs), "missing_trace_ids": sorted(set(accepted) - set(jobs)),
              "submit_http_statuses": {str(code): sum(row.get("http_status") == code for row in run["observations"])
                                       for code in {row.get("http_status") for row in run["observations"]}},
              "failed_steps": {name: sum(row.get("failed_step") == name for row in run["observations"])
                               for name in {row.get("failed_step") for row in run["observations"] if row.get("failed_step")}},
              "read_attempts": len(run["read_observations"]),
              "read_failures": sum(row.get("http_status") != 200 for row in run["read_observations"]),
              "read_trace_count": len(reads), "post_trace_count": len(posts), "description_trace_count": len(descriptions),
              "customer_visible_seconds": stats([row["visible_seconds"] for row in run["observations"] if row.get("visible_seconds")]),
              "post_seconds": stats([row["steps"]["report_post"] for row in accepted.values()]),
              "worker_seconds": stats(durations), "enqueue_to_job_start_seconds": stats(queue),
              "enqueue_to_first_receive_seconds": stats(actual_queue),
              "stages_seconds": stages, "candidate_count": stats([row.get("candidate_count", 0) for row in jobs.values()]),
              "match_count": stats([row.get("match_count", 0) for row in jobs.values()]),
              "results_api_seconds": stats([row["latency_seconds"] for row in run["read_observations"] if row.get("http_status") == 200]),
              "results_server_stages_seconds": {name: stats([row.get("stages", {}).get(name, {}).get("duration_ms", 0) / 1000 for row in reads if row.get("http_status") == 200])
                                                for name in ["results_list_reports", "results_read_matches", "results_read_found"]},
              "api_post_stages_seconds": {name: stats([row.get("stages", {}).get(name, {}).get("duration_ms", 0) / 1000 for row in posts])
                                          for name in ["write_report", "enqueue_sqs"]},
              "photo_client_steps_seconds": {name: stats([row["steps"][name] for row in run["observations"] if name in row["steps"]])
                                             for name in ["upload_url", "s3_upload", "description_api"]},
              "photo_server_stages_seconds": {name: stats([row.get("stages", {}).get(name, {}).get("duration_ms", 0) / 1000 for row in descriptions if row.get("http_status") == 200])
                                             for name in ["read_photo_s3", "description_model"]},
              "arrival_schedule_valid": run["arrival_schedule_valid"], "cleanup_complete": run["cleanup_complete"],
              "telemetry_errors": evidence["errors"] + cloud["errors"]}
    # Keep each time bucket; never sum concurrency peaks or average percentile buckets.
    result["lambda_activity"] = [row for row in cloud["metrics"] if row["namespace"] == "AWS/Lambda"
                                 and row["name"] in ["ConcurrentExecutions", "Throttles", "Errors", "Invocations"]]
    runtime_ids = {row.get("invocation_id") for row in jobs.values()}
    selected_runtime = [row["message"] for row in evidence["runtime"] if any(value and value in row["message"] for value in runtime_ids)]
    result["worker_max_memory_mb"] = stats([float(match[1]) for text in selected_runtime
                                             if (match := re.search(r"Max Memory Used:\s*([\d.]+)", text))])
    result["worker_invocation_seconds"] = stats([float(match[1]) / 1000 for text in selected_runtime
                                                 if (match := re.search(r"\bDuration:\s*([\d.]+)", text))])
    result["worker_timeout_invocations"] = sum("Status: timeout" in text for text in selected_runtime)
    attempts = [row for row in evidence["records"] if row["kind"] == "match" and row.get("item_id") in accepted]
    result["worker_attempt_records"] = len(attempts)
    result["max_receive_count"] = max((int(row.get("receive_count") or 1) for row in attempts), default=None)
    result["worker_cold_starts"] = sum("Init Duration:" in text for text in selected_runtime)
    return result


def number(summary, key="mean"):
    return f"{summary[key]:.3f}" if summary else "missing"


def write_report(summaries, directory):
    lines = ["# LostLink consolidated end-to-end bottleneck experiment", "",
             "This experiment measures authenticated text submission through customer-visible expected matches, with a separate photo-upload and AI-description workload. Internal stage timings come from structured logs added to the exact deployed Lambda packages. Original packages and SQS settings are restored after testing.", "",
             "Ten temporary Individual accounts are used. Text runs offer 0.5 reports/s for 20 seconds, 2/s for 20 seconds, then 5/s for 10 seconds: 100 attempted arrivals per run. Photo testing uses 12 journeys with at most two simultaneous upload/description/submission flows. The inventory is synthetic and each report has a known expected candidate. Customer result polling is independent per account with two seconds between cycles; this is an API journey, not browser rendering or email receipt.", "",
             "| Case | Inventory | Offered / POST accepted / visible by 180 s | Worker completed | Successful POST p95 (s) | Customer-visible p50 / p95 (s) | Worker p95 (s) | Enqueue-to-job-start p95 (s) |",
             "| --- | ---: | --- | ---: | ---: | --- | ---: | ---: |"]
    for row in summaries:
        lines.append(f"| {row['name']} | {row['inventory']} | {row['scheduled']} / {row['accepted']} / {row['customer_visible']} | {row['worker_complete']} | {number(row['post_seconds'],'p95')} | {number(row['customer_visible_seconds'],'p50')} / {number(row['customer_visible_seconds'],'p95')} | {number(row['worker_seconds'],'p95')} | {number(row['enqueue_to_job_start_seconds'],'p95')} |")
    lines += ["", "## Where a matching job spends time", "", "Values are mean seconds per completed, traced job. Repeated calls within a job are summed. Conditional stages with no work are zero; missing jobs are excluded and trace coverage is reported separately. Stage means can be compared with worker mean; stage p95 values must not be added.", "",
              "| Stage | " + " | ".join(row["name"] for row in summaries) + " |",
              "| --- | " + " | ".join("---:" for _ in summaries) + " |"]
    for name in STAGES:
        lines.append("| " + name + " | " + " | ".join(number(row["stages_seconds"][name]) for row in summaries) + " |")
    lines.append("| Full worker job | " + " | ".join(number(row["worker_seconds"]) for row in summaries) + " |")
    lines += ["", "## Result delivery and photo stages", ""]
    for row in summaries:
        lines += [f"### {row['name']}", "",
                  f"Customer results API: {row['read_failures']} failed reads / {row['read_attempts']} attempts; successful read p95 {number(row['results_api_seconds'],'p95')} s. Mean candidates retrieved {number(row['candidate_count'])}; mean qualifying matches written {number(row['match_count'])}.", "",
                  "| Stage | Mean (s) | p95 (s) |", "| --- | ---: | ---: |"]
        sections = [row["api_post_stages_seconds"], row["results_server_stages_seconds"], row["photo_client_steps_seconds"], row["photo_server_stages_seconds"]]
        for section in sections:
            for name, values in section.items():
                if values:
                    lines.append(f"| {name} | {number(values)} | {number(values,'p95')} |")
        lines += ["", f"Trace coverage: {row['worker_complete']}/{row['accepted']} acknowledged jobs; API post traces {row['post_trace_count']}; result API traces {row['read_trace_count']}; description API traces {row['description_trace_count']}. Generator schedule valid: {row['arrival_schedule_valid']}. Test-record cleanup: {row['cleanup_complete']}. Telemetry errors: {len(row['telemetry_errors'])}.", ""]
    lines += ["## Interpretation boundaries", "",
              "These are short controlled stress episodes, not a sustained-capacity certification. Worker-cap comparisons alter one setting; inventory comparisons also change the number of potential matches, so compare candidate and match counts alongside timings. Synthetic image size/content and model cold starts limit generalization to real photographs. Login is measured separately during setup, with no human think time.", "",
              "Customer-visible latency includes preparation for photo journeys, HTTP submission, queue/dispatch delay, matching, result retrieval, and polling delay. Backend stage durations use monotonic clocks within Lambda. Enqueue-to-job-start compares AWS timestamps and includes queue polling, dispatch/cold-start, and earlier records in an SQS batch; it is not pure queue residence. Missing/failed requests are retained, and successful-only latencies do not describe all customers.", "",
              "Inspect summary.json, per-run JSON, traces, and CloudWatch exports for the actual throttles, memory use, queue history, and errors. Empty CloudWatch series are not zeros. Minute metric buckets can include adjacent preparation/cleanup; internal job traces are joined by exact acknowledged report ID. Temporary accounts, photos, description markers, and report/inventory rows are removed after queue drain."]
    (directory / "REPORT.md").write_text("\n".join(lines) + "\n")
    (directory / "summary.json").write_text(json.dumps(summaries, indent=2, default=str))
    with (directory / "stage-summary.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["case", "stage", "n", "mean_seconds", "p50_seconds", "p95_seconds", "max_seconds"])
        for row in summaries:
            for stage, values in row["stages_seconds"].items():
                if values:
                    writer.writerow([row["name"], stage, values["n"], values["mean"], values["p50"], values["p95"], values["max"]])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--reuse-evidence", action="store_true")
    args = parser.parse_args()
    session = boto3.Session(region_name="ap-southeast-1")
    summaries = []
    for name in ["text-current-n100", "text-worker2-n100", "text-worker2-n500", "photo-worker2-n100"]:
        path = args.directory / (name + ".json")
        if not path.exists():
            continue
        run = json.loads(path.read_text())
        if "measured_finished_at_utc" not in run or "finished_at_utc" not in run:
            continue
        if args.reuse_evidence:
            evidence = json.loads((args.directory / (name + "-traces.json")).read_text())
            cloud = json.loads((args.directory / (name + "-cloudwatch.json")).read_text())
        else:
            evidence, cloud = get_evidence(run, session, args.directory)
        row = summarize(run, evidence, cloud)
        summaries.append(row)
        print(f"{name}: {row['accepted']}/{row['scheduled']} accepted; {row['customer_visible']} visible; {row['worker_complete']} traced complete", flush=True)
    write_report(summaries, args.directory)
    print(args.directory / "REPORT.md")


if __name__ == "__main__":
    main()
