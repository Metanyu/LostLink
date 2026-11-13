#!/usr/bin/env python3
"""Bounded customer-API bottleneck experiment, with reversible deployment instrumentation."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import secrets
from threading import Event, Lock, Thread
import time
import urllib.error
import urllib.request
import uuid

import boto3
import loadtest as base
from e2e_deployment import ExperimentDeployment

PHASES = [{"seconds": 20, "rps": 0.5}, {"seconds": 20, "rps": 2}, {"seconds": 10, "rps": 5}]
WAIT_SECONDS = 180


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def subject(token):
    encoded = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))["sub"]


def http(method, path, token, body=None, expected=200):
    request = urllib.request.Request(base.API + path, method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Authorization": token, "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != expected:
            raise RuntimeError(f"Unexpected HTTP {response.status}; expected {expected}")
        return json.loads(response.read() or b"{}"), response.headers.get("apigw-requestid")


def photo_fixture():
    """Fixed synthetic image exercises image upload/model path, not photo recognition accuracy."""
    from PIL import Image, ImageDraw
    image = Image.new("RGB", (1024, 768), (224, 222, 216))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((300, 140, 720, 700), radius=75, fill=(15, 135, 140), outline=(15, 45, 45), width=10)
    draw.arc((385, 55, 635, 240), 180, 360, fill=(20, 30, 30), width=25)
    draw.rounded_rectangle((365, 360, 655, 605), radius=30, fill=(10, 110, 120), outline=(15, 45, 45), width=8)
    draw.rectangle((480, 255, 545, 310), fill=(190, 30, 25))
    draw.line((380, 160, 340, 640), fill=(20, 30, 30), width=23)
    draw.line((640, 160, 680, 640), fill=(20, 30, 30), width=23)
    stream = io.BytesIO()
    image.save(stream, format="JPEG", quality=92)
    return stream.getvalue()


def item_description(index, photo=False):
    if photo and index == 0:
        return "teal fabric backpack with black straps, front pocket and a red rectangular patch"
    objects = ["leather wallet with a brass clasp", "canvas backpack with a broken zipper", "metal water bottle with stickers",
               "folding umbrella with a wooden handle", "wireless earbuds in a charging case", "plastic sunglasses in a soft pouch",
               "laptop sleeve with a side pocket", "notebook with a spiral binding", "key ring with a rubber animal charm",
               "sports watch with a silicone strap"]
    colors = ["black", "red", "blue", "green", "yellow", "purple", "orange", "pink", "white", "grey"]
    return f"{colors[(index // 10) % 10]} {objects[index % 10]}, small label {index:04d}"


def create_customers(client, pool, client_id, directory):
    customers = []
    manifest = directory / "test-users.json"
    prefix = "e2e-" + uuid.uuid4().hex[:10]
    for index in range(10):
        name = f"{prefix}-{index}@lostlink.example"
        password = secrets.token_urlsafe(24) + "Aa1!"
        client.admin_create_user(UserPoolId=pool, Username=name, MessageAction="SUPPRESS",
                                 UserAttributes=[{"Name": "email", "Value": name}, {"Name": "email_verified", "Value": "true"}])
        customers.append({"username": name, "alias": f"customer-{index}"})
        manifest.write_text(json.dumps([{k: v for k, v in row.items() if k != "token"} for row in customers], indent=2))
        client.admin_set_user_password(UserPoolId=pool, Username=name, Password=password, Permanent=True)
        client.admin_add_user_to_group(UserPoolId=pool, Username=name, GroupName="Individual")
        started = time.monotonic()
        auth = client.initiate_auth(ClientId=client_id, AuthFlow="USER_PASSWORD_AUTH",
                                    AuthParameters={"USERNAME": name, "PASSWORD": password})
        customers[-1].update(token=auth["AuthenticationResult"]["IdToken"], authentication_seconds=time.monotonic() - started)
    return customers


def run_case(name, inventory, customers, directory, photo=False):
    base.RUN_ID = uuid.uuid4().hex[:10]
    base.RUN_TAG = "loadtest-" + base.RUN_ID
    base.require_empty_stack()
    result = {"name": name, "run_id": base.RUN_ID, "inventory": inventory, "workload": "photo" if photo else "text",
              "started_at_utc": utc_now(), "customers": len(customers), "observations": [], "read_observations": [],
              "queue_observations": [], "errors": [], "cleanup_complete": False}
    path = directory / (name + ".json")
    lock, stop = Lock(), Event()
    found_ids, lost_ids, photos, markers = [], set(), [], []
    pending = {}
    staff = base.token(base.STAFF_EMAIL, base.STAFF_PASSWORD)
    image = photo_fixture() if photo else None
    if image:
        (directory / "photo-fixture.jpg").write_bytes(image)
        result["photo_fixture_bytes"] = len(image)
    pollers = []

    def save():
        path.write_text(json.dumps(result, indent=2))

    def observe_customer(customer_index):
        # Independent user polling, staggered to avoid an artificial synchronized read burst.
        stop.wait(customer_index * 0.2)
        while not stop.is_set():
            with lock:
                owned = [row for row in pending.values()
                         if row["customer_index"] == customer_index
                         and not row.get("visible_seconds") and not row.get("visibility_deadline_exceeded")]
            if not owned:
                stop.wait(0.2)
                continue
            started = time.monotonic()
            read = {"customer_index": customer_index, "started_offset_seconds": started - wall_start}
            try:
                body, request_id = http("GET", "/matches", customers[customer_index]["token"])
                pairs = {(match["queryItemId"], match["candidateItemId"]) for match in body.get("matches", [])}
                read.update(http_status=200, request_id=request_id, suggestions=len(body.get("matches", [])))
                now = time.monotonic()
                with lock:
                    for row in owned:
                        elapsed = now - wall_start - row["start_offset_seconds"]
                        if (row["item_id"], row["expected_found_id"]) in pairs and elapsed <= WAIT_SECONDS:
                            row["visible_seconds"] = elapsed
                        elif elapsed >= WAIT_SECONDS:
                            row["visibility_deadline_exceeded"] = True
            except Exception as error:
                read.update(http_status=getattr(error, "code", None), error=str(error))
                with lock:
                    for row in owned:
                        if time.monotonic() - wall_start - row["start_offset_seconds"] >= WAIT_SECONDS:
                            row["visibility_deadline_exceeded"] = True
            read["latency_seconds"] = time.monotonic() - started
            with lock:
                result["read_observations"].append(read)
            stop.wait(2)

    def observe_queue():
        while not stop.is_set():
            try:
                sample = {"sampled_at_utc": utc_now(), "offset_seconds": time.monotonic() - wall_start, **base.queue_depth()}
                with lock:
                    result["queue_observations"].append(sample)
            except Exception as error:
                result["errors"].append("queue observer: " + str(error))
            stop.wait(2)

    def submit(index, scheduled_offset=None):
        started = time.monotonic()
        customer_index = index % len(customers)
        customer = customers[customer_index]
        target = 0 if photo else index % inventory
        row = {"index": index, "customer_index": customer_index, "start_offset_seconds": started - wall_start,
               "expected_found_id": found_ids[target], "steps": {}}
        if scheduled_offset is not None:
            row["scheduled_offset_seconds"] = scheduled_offset
            row["arrival_lag_seconds"] = max(0, started - wall_start - scheduled_offset)
        body = {"description": base.RUN_TAG + " lost " + item_description(target, photo),
                "locationZone": f"zone-{target % 5}", "eventTime": "2026-09-20T09:00:00+00:00"}
        current_step = "report_post"
        try:
            if photo:
                current_step = "upload_url"
                mark = time.monotonic()
                upload, _ = http("POST", "/uploads", customer["token"], {"contentType": "image/jpeg"})
                row["steps"][current_step] = time.monotonic() - mark
                photo_row = {"key": upload["photoKey"]}
                with lock:
                    photos.append(photo_row)
                current_step = "s3_upload"
                mark = time.monotonic()
                request = urllib.request.Request(upload["uploadUrl"], data=image, method="PUT", headers={"Content-Type": "image/jpeg"})
                with urllib.request.urlopen(request, timeout=30) as response:
                    photo_row["version_id"] = response.headers.get("x-amz-version-id")
                row["steps"][current_step] = time.monotonic() - mark
                current_step = "description_api"
                request_id = uuid.uuid4().hex
                row["description_request_id"] = request_id
                with lock:
                    markers.append("description-request#" + subject(customer["token"]) + "#" + request_id)
                mark = time.monotonic()
                description, _ = http("POST", "/descriptions/generate", customer["token"],
                                      {"photoKey": upload["photoKey"], "requestId": request_id})
                row["steps"][current_step] = time.monotonic() - mark
                body.update({key: description[key] for key in ["description", "descriptionSource", "descriptionPhotoKey", "descriptionGeneratedAt"]})
                body["description"] = base.RUN_TAG + " " + body["description"]
                body["photoKey"] = upload["photoKey"]
            current_step = "report_post"
            mark = time.monotonic()
            row["report_start_offset_seconds"] = mark - wall_start
            response, request_id = http("POST", "/reports", customer["token"], body, expected=201)
            row["steps"][current_step] = time.monotonic() - mark
            row.update(http_status=201, item_id=response["itemId"], request_id=request_id)
            with lock:
                lost_ids.add(row["item_id"])
                pending[row["item_id"]] = row
        except Exception as error:
            row.update(failed_step=current_step, http_status=getattr(error, "code", None), error=str(error))
            if hasattr(error, "headers"):
                row["request_id"] = error.headers.get("apigw-requestid")
        row["submit_journey_seconds"] = time.monotonic() - started
        return row

    try:
        print(f"{name}: seeding {inventory} found items", flush=True)
        for index in range(inventory):
            body = {"description": base.RUN_TAG + " found " + item_description(index, photo),
                    "locationZone": f"zone-{index % 5}", "eventTime": "2026-09-20T10:00:00+00:00"}
            found_ids.append(base.post("/items", staff, body)["itemId"])
            time.sleep(0.3)
        if not base.wait_for_queue_drain(timeout_seconds=600):
            raise RuntimeError("Inventory jobs did not drain.")
        base.verify_inventory(set(found_ids))
        result["measured_started_at_utc"] = utc_now()
        wall_start = time.monotonic()
        pollers = [Thread(target=observe_customer, args=(index,), daemon=True) for index in range(len(customers))]
        pollers.append(Thread(target=observe_queue, daemon=True))
        for poller in pollers:
            poller.start()
        with ThreadPoolExecutor(max_workers=2 if photo else 20) as executor:
            if photo:
                futures = [executor.submit(submit, index) for index in range(12)]
                result["scheduled"] = 12
                result["client_drops"] = []
                result["schedule"] = "12 photo journeys, at most 2 simultaneous submission flows"
            else:
                plan = base.arrival_plan(json.dumps(PHASES))
                result["scheduled"] = len(plan["offsets"])
                result["schedule"] = PHASES
                futures, drops = base.paced_submissions(executor, submit, plan["offsets"], plan["seconds"], wall_start, 20)
                result["client_drops"] = drops
            for future in as_completed(futures):
                result["observations"].append(future.result())
        result["submission_finished_seconds"] = time.monotonic() - wall_start
        print(f"{name}: submitted {len(lost_ids)}/{result['scheduled']}; watching customer-visible matches", flush=True)
        while True:
            with lock:
                done = all(row.get("visible_seconds") or row.get("visibility_deadline_exceeded") for row in pending.values())
            if done or time.monotonic() - wall_start > result["submission_finished_seconds"] + WAIT_SECONDS + 35:
                break
            if not all(poller.is_alive() for poller in pollers):
                raise RuntimeError("Customer API observer stopped.")
            time.sleep(0.25)
        stop.set()
        for poller in pollers:
            poller.join(timeout=35)
        if any(poller.is_alive() for poller in pollers):
            raise RuntimeError("Customer API observer did not stop.")
        result["measured_finished_at_utc"] = utc_now()
        result["measured_monotonic_seconds"] = time.monotonic() - wall_start
        result["observations"].sort(key=lambda row: row["index"])
        result["accepted"] = len(lost_ids)
        result["customer_visible"] = sum(bool(row.get("visible_seconds")) for row in result["observations"])
        result["arrival_schedule_valid"] = not result["client_drops"] and max(
            (row.get("arrival_lag_seconds", 0) for row in result["observations"]), default=0) <= 0.25
    except Exception as error:
        result["errors"].append(str(error))
    finally:
        stop.set()
        for poller in pollers:
            poller.join(timeout=35)
        result["cleanup_manifest"] = {"found_ids": found_ids, "lost_ids": sorted(lost_ids), "photos": photos, "description_markers": markers}
        save()
        print(f"{name}: draining and cleaning exact test records", flush=True)
        if base.wait_for_queue_drain(timeout_seconds=900):
            result["cleanup_complete"] = base.cleanup(found_ids, lost_ids)
            if result["cleanup_complete"]:
                for marker in markers:
                    base.items_t.delete_item(Key={"itemId": marker})
                s3 = boto3.client("s3", region_name=base.REGION)
                for image_row in photos:
                    arguments = {"Bucket": os.environ["PHOTOS_BUCKET"], "Key": image_row["key"]}
                    if image_row.get("version_id"):
                        arguments["VersionId"] = image_row["version_id"]
                    s3.delete_object(**arguments)
        result["finished_at_utc"] = utc_now()
        save()
    print(f"{name}: accepted={result.get('accepted')} visible={result.get('customer_visible')} cleanup={result['cleanup_complete']}", flush=True)
    if not result["cleanup_complete"] or result["errors"]:
        raise RuntimeError(f"{name} needs inspection; evidence saved to {path}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--restore-only", action="store_true")
    args = parser.parse_args()
    session = boto3.Session(region_name=base.REGION)
    if session.client("sts").get_caller_identity()["Account"] != os.environ.get("LT_EXPECTED_AWS_ACCOUNT"):
        raise SystemExit("AWS account mismatch.")
    deployment = ExperimentDeployment(args.snapshot, session.client("lambda"))
    if args.restore_only:
        deployment.restore()
        return
    if os.environ.get("LT_CONFIRM_NONPROD") != "1":
        raise SystemExit("Set LT_CONFIRM_NONPROD=1 for a dedicated test deployment.")
    directory = args.snapshot / "experiment"
    directory.mkdir(exist_ok=False)
    base.require_empty_stack()
    cognito = session.client("cognito-idp")
    pool = os.environ["USER_POOL_ID"]
    try:
        deployment.install()
        customers = create_customers(cognito, pool, base.CLIENT, directory)
        (directory / "authentication.json").write_text(json.dumps(
            [{"alias": row["alias"], "authentication_seconds": row["authentication_seconds"]} for row in customers], indent=2))
        run_case("text-current-n100", 100, customers, directory)
        deployment.worker_limit(2)
        run_case("text-worker2-n100", 100, customers, directory)
        run_case("text-worker2-n500", 500, customers, directory)
        run_case("photo-worker2-n100", 100, customers, directory, photo=True)
    finally:
        try:
            manifest = directory / "test-users.json"
            if manifest.exists():
                for row in json.loads(manifest.read_text()):
                    cognito.admin_delete_user(UserPoolId=pool, Username=row["username"])
                (directory / "test-users-deleted.txt").write_text(utc_now())
        finally:
            deployment.restore()
    print(f"Experiment complete; original deployment restored. Evidence: {directory}", flush=True)


if __name__ == "__main__":
    main()
