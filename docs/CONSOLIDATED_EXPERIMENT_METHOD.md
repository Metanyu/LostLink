# Consolidated end-to-end bottleneck experiment

This replaces an unconnected set of concurrency bursts with a controlled API journey and one evidence report. The objective is to distinguish admission failure, queue/dispatch delay, worker service time, and customer result delivery; then test how restricting workers and increasing inventory changes those costs.

## Workloads and controls

| Case | Found inventory | SQS maximum worker concurrency | Workload |
| --- | ---: | --- | --- |
| text-current-n100 | 100 | Existing setting, uncapped in the snapshot | 100 scheduled text reports |
| text-worker2-n100 | 100 | 2 | Identical arrival schedule |
| text-worker2-n500 | 500 | 2 | Identical arrival schedule; five times the inventory |
| photo-worker2-n100 | 100 | 2 | 12 upload/AI-description/report journeys; at most two simultaneous submission flows |

Each text schedule offers 0.5 reports/s for 20 seconds, 2/s for 20 seconds, then 5/s for 10 seconds. Ten temporary Individual accounts receive requests round-robin. Each account independently polls its customer-facing `GET /matches` endpoint, with a two-second sleep between read cycles and staggered starts. A journey succeeds when its expected found-item ID is returned for its new report within 180 seconds of journey start. A qualifying row for some other item is not sufficient.

Text descriptions use deterministic common objects, colors, and numbered labels. Every seed/report carries a run tag for exact cleanup. Locations use five legacy zones and timestamps are fixed one hour apart. This is controlled synthetic traffic, not a realistic matching-quality benchmark. Both the total candidates and number of qualifying matches must accompany timing comparisons. An increased number of matches can increase write and notification-path work independently of vector comparison cost.

The separate photo workload obtains a presigned URL through `POST /uploads`, uploads a fixed 1024-by-768 synthetic backpack JPEG to S3, invokes `POST /descriptions/generate` with a fresh request ID, and submits that description/photo metadata through `POST /reports`. It verifies the seeded backpack's ID through the customer API. Fixture bytes are saved with evidence. Synthetic image size/content does not establish latency for all real photographs.

## What is measured

Client records include offered arrival, actual start/lag, failure step and HTTP status, report POST latency, each photo-preparation step, customer-result read latency/errors, and first expected-match visibility. Unsent arrivals are recorded as generator drops. There are no automatic report POST retries because create is not idempotent. Identity tokens are acquired in setup; sign-in timings are stored separately and not included in the measured arrival schedule. There is no browser rendering, human think time, claim workflow, or email-delivery measurement.

Structured Lambda timing logs record:

- API report persistence and SQS enqueue, correlated by item ID and API request ID.
- SQS sent/first-receive timestamps, worker job start, receive count, and invocation ID.
- Item loading, text embedding, embedding persistence, candidate retrieval, invalidation, scoring, match persistence, notification-path work, and final status update.
- Result-page report enumeration, match queries, and found-record reads.
- Photo S3 read and Claude invocation inside description generation.

One log record per API invocation or job contains summed stage durations and call counts, without descriptions, image bytes, credentials, email addresses, or tokens. Worker stage times use a monotonic clock. Enqueue-to-job-start uses AWS timestamps and includes SQS polling/dispatch, Lambda initialization, and earlier records within a batch; it must not be labelled pure queue residence. API and worker totals include residual orchestration overhead. Model duration is a component of description API latency, not an additional independent delay to add again.

`eval/e2e_report.py` joins exact accepted IDs/request IDs to logs, exports CloudWatch evidence, and produces `REPORT.md`, `summary.json`, and `stage-summary.csv`. Missing logs and failed requests stay visible. Successful-only p95s do not describe rejected or timed-out customers. Per-stage p95s cannot be added into an end-to-end p95. Minute CloudWatch buckets can overlap preparation/cleanup; job traces use exact IDs to isolate measured work.

## Deployment and cleanup

The experiment uses `eval/e2e_deployment.py` to patch only specified source members in copies of the deployed ZIP packages. Original ZIPs, hashes, revision IDs, and event-source settings are saved first. The deployed source was checked against pre-instrumentation local source. Code updates use revision guards; rollback refuses to overwrite an unrelated concurrent code/configuration change. The experiment does not change matching weights, API behavior, memory, timeouts, account quotas, or data schema. The SQS worker cap changes only in the controlled comparison and is restored afterwards.

Preconditions are the verified dedicated account, empty Items/Matches tables, a quiet matching queue, and a disabled email sender. New temporary accounts suppress invitation messages and are deleted afterwards. Per-run found/lost records are deleted only after queue drain; uploaded images and description-generation markers are deleted by exact recorded IDs/keys. Cleanup manifests are saved before cleanup. Original functions/settings are restored in a `finally` block, with an explicit recovery command if the process is externally interrupted.

## Commands for this prepared experiment

Run in WSL Bash from the repository root. The snapshot directory below contains the verified original packages for this deployment; it is not interchangeable with a snapshot from another deployment. The initial experiment output directory must not already exist.

```bash
cd /home/metanyu/LostLink
source backend/.venv/bin/activate
python -m pip install -r eval/requirements.txt
export AWS_REGION=ap-southeast-1
export LT_EXPECTED_AWS_ACCOUNT=037918102087
export LT_CONFIRM_NONPROD=1

bash scripts/loadtest.sh --e2e \
  --snapshot eval/output/e2e-preparation-20261113T013940Z
```

After CloudWatch ingestion, produce the consolidated evidence report:

```bash
python eval/e2e_report.py \
  eval/output/e2e-preparation-20261113T013940Z/experiment
```

To regenerate tables from downloaded evidence without AWS calls:

```bash
python eval/e2e_report.py \
  eval/output/e2e-preparation-20261113T013940Z/experiment \
  --reuse-evidence
```

If the process was interrupted, restore the original function packages/settings using the snapshot, then inspect saved per-run cleanup manifests and `test-users.json`. Restoration alone does not delete unfinished experiment records:

```bash
bash scripts/loadtest.sh --e2e \
  --snapshot eval/output/e2e-preparation-20261113T013940Z --restore-only
```

## How to use the result as a SaaS operator

Prioritize based on observed stage costs and failures. API throttles at the shared account limit call for workload isolation and sufficient Lambda quota. A worker cap protects admission but lowers drain capacity: compare queue age and complete-job throughput before declaring it a solution. Predominantly database/network time calls for fewer reads/writes and better candidate selection before simply increasing memory. Predominantly scoring CPU warrants testing memory/CPU allocation. Increasing match volume can amplify per-match writes, owner lookups, and later customer-result reads. Bedrock delay/throttles require model/region/quota investigation; SQS buffering cannot accelerate model calls. Photo description generation should have a separate latency and cost budget.

For rough planning, worker concurrency divided by mean job service time estimates an idealized drain rate. It excludes polling, batching, retries, quota contention, and other workloads, so use it to explain queue growth, not certify sustainable capacity. More jobs increase total work; more candidates or qualifying matches increase work per job. Repeat a longer fixed-rate test only after the measured bottlenecks and desired customer latency targets are addressed.
