# Run Project End-to-End

Day-to-day operation: triggering runs, backfilling data, checking pipeline state, and reading a failed execution. For one-time deployment, see [`setup.md`](setup.md).

## A note on resource names

Every command below uses the default `project_name=pulsegrid`, `environment=dev` naming (`pulsegrid-dev-watermarks`, `pulsegrid-dev-raw`, etc.) — see [`setup.md`](setup.md#2-clone-and-configure) if you renamed it during deployment.

To find your state machine's ARN without relying on a specific Terraform output:

```bash
aws stepfunctions list-state-machines --query "stateMachines[?contains(name, 'sensor-etl-orchestration')].stateMachineArn" --output text
```

## A note on PowerShell and JSON

Every `--input`/`--key`/`--item` argument below is JSON. PowerShell mangles inline JSON with special characters (`{`, `}`, `$`) in ways `bash` doesn't — the reliable fix used throughout this project is writing the JSON to a small local file and passing `file://path.json` instead of inlining it. Every example below uses this pattern for exactly that reason.

## Triggering a run manually

**Via the console:** Step Functions → the state machine → **Start execution** → paste JSON directly into the Input box → Start. The graph view updates live.

**Via CLI:**

```bash
aws stepfunctions start-execution \
  --state-machine-arn <arn-from-the-lookup-above> \
  --input file://execution-input.json
```

The response includes an `executionArn` — save it to check status without opening the console:

```bash
aws stepfunctions describe-execution --execution-arn <executionArn> --query "status" --output text
```

### Common input shapes

Full pipeline, both sections:
```json
{}
```
or explicitly:
```json
{"mode": "full"}
```
Either works identically — both `ModeCheck1` and `ModeCheck2` treat an absent `mode` the same as `mode="full"`. This isn't quite what the real scheduled trigger literally sends, though: the actual deployed schedule sends `{"mode":"full","triggered_by":"schedule"}` — confirmable directly:
```bash
aws scheduler get-schedule --name pulsegrid-dev-sensor-etl-daily --query "Target.Input" --output text
```
`triggered_by` only affects whether a *failure* gets emailed — it has no bearing on which sections run.

Just `sensor_etl`:
```json
{"mode": "glue_only"}
```

Just `redshift_refresh`:
```json
{"mode": "redshift_only"}
```

Backfilling a specific historical range (works combined with any `mode` above — add `backfill` as a sibling field, not nested inside `mode`):
```json
{"mode": "full", "backfill": {"start_date": "2026-06-01", "end_date": "2026-06-05"}}
```

The regression guard means this is always safe to run against dates that are already correctly loaded — the underlying data reprocesses cleanly (idempotent), but neither watermark will move backward.

Note: none of these manual examples include `triggered_by` — that's intentional. Only the real EventBridge trigger sends it, which is precisely what keeps a manual test run's failure silent instead of emailing you. See [Interpreting a failed execution](#interpreting-a-failed-execution) below.

If you specifically want to verify the failure-notification path itself works — worth doing once, right after setup — you can deliberately include it in a manual test:
```json
{"mode": "glue_only", "triggered_by": "schedule", "backfill": {"start_date": "2026-06-05", "end_date": "2026-06-01"}}
```
This combines a deliberately-reversed `backfill` range (a safe, guaranteed failure) with the schedule marker, so you can confirm a real email arrives without needing to wait for an actual failure.

## Checking watermark state

`sensor-etl-key.json`:
```json
{"pipeline_id": {"S": "sensor_etl"}}
```
`redshift-key.json`:
```json
{"pipeline_id": {"S": "redshift_refresh"}}
```

```bash
aws dynamodb get-item --table-name pulsegrid-dev-watermarks --key file://sensor-etl-key.json
aws dynamodb get-item --table-name pulsegrid-dev-watermarks --key file://redshift-key.json
```

`status: "RUNNING"` means a lock is currently held by an in-progress execution — `status: "IDLE"` means it's free.

## Backfilling raw data manually

Before `generate_hourly` existed, or to fill a specific historical gap on demand:

```bash
python3 -c "
from datetime import datetime, timezone
from pathlib import Path
from sensor_etl.generate import run

run(
    start=datetime(2026, 6, 1, 0, tzinfo=timezone.utc),
    end=datetime(2026, 6, 1, 23, tzinfo=timezone.utc),
    output_dir=Path('data/sample'),
    seed=42,
)
"
aws s3 cp data/sample/dt=2026-06-01 s3://pulsegrid-dev-raw/dt=2026-06-01/ --recursive
```

No manual crawl needed afterward — `StartRawCrawler` re-catalogs `raw` automatically at the start of every `sensor_etl` execution.

## Running the test suite

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

21 tests, using `moto` (AWS mocking) and `freezegun` (deterministic time). `etl_job.py`'s own core logic isn't unit-tested — it's verified through real execution history instead; see [`design-choices.md`](design-choices.md) for why.

## Interpreting a failed execution

1. **Step Functions console → the failed execution → graph view.** The specific state that failed is highlighted; click it to see the actual error under "Exception."
2. **CloudWatch Logs**, for anything Glue- or Lambda-related — the Glue job in particular self-logs meaningful lines, including the watermark guard's own decision:
```
   Watermark NOT advanced: 2026-08-27T23 is not newer than the current watermark. Left unchanged (likely a manual reprocess).
```
   Seeing this specific line is normal, expected behavior for an out-of-order backfill — not an error to chase.
3. **A `FAILED` or `ABORTED` Redshift statement** is visible directly via `DescribeRedshiftStatement`'s output in that state's own execution detail, without needing to leave the console.
4. **If the failure was on the real scheduled trigger**, an email should already be sitting in the alert inbox — check spam if it's not in the primary inbox. If it's missing entirely, confirm the subscription is actually active, not just requested:
```bash
   aws sns list-subscriptions-by-topic --topic-arn <topic-arn-from-terraform-output>
```
   `"SubscriptionArn": "PendingConfirmation"` means the one-time confirmation email was never clicked — see [`setup.md`](setup.md). A manually-triggered execution's failure never sends an email at all, by design — this step only applies to a real scheduled run.

## If a lock ever appears stuck

A `status: "RUNNING"` that never returns to `IDLE` blocks every future execution (`AlreadyRunning`/`RedshiftAlreadyRunning` will fire silently instead of doing real work). Both release states carry their own retry, so this should be rare — but if it happens:

`reset-attrs.json`:
```json
{"#s": "status"}
```
`reset-values.json`:
```json
{":idle": {"S": "IDLE"}}
```

```bash
aws dynamodb update-item --table-name pulsegrid-dev-watermarks \
  --key file://sensor-etl-key.json \
  --update-expression "SET #s = :idle" \
  --expression-attribute-names file://reset-attrs.json \
  --expression-attribute-values file://reset-values.json
```

(swap `sensor-etl-key.json` for `redshift-key.json` to reset the other pipeline's lock instead)