# Design Choices

The reasoning behind PulseGrid's core decisions, and its known, deliberately-accepted limitations.

## Design principles

**Self-verified, forward-only watermarks.** Neither pipeline trusts what it was *asked* to process — each queries its own real output (`MAX(dt, hour)` for Glue, a verification `SELECT MAX(dt)` for Redshift) before writing anything to DynamoDB. The write itself is guarded by an atomic `ConditionExpression`, not a read-then-compare in application code, so there's no race window between two concurrent executions. This wasn't the original design — an early version trusted the *request*, and a request for data that didn't exist could silently advance the watermark anyway. Proven live against a real out-of-order backfill: the underlying data reprocessed correctly while the watermark itself refused to move backward.

**Locks claimed only once there's real work.** Both orchestration sections check for missing data *before* claiming their DynamoDB lock or starting a crawler — an empty check touches no shared state at all.

**Idempotent by construction.** Every write — Glue's dynamic partition overwrite, Redshift's `DELETE`+`INSERT` — is safe to run twice. This is what makes `backfill` safe as a mechanism at all: reprocessing an already-correct date produces the same correct result, never a duplicate.

**Existence checked explicitly, never assumed.** Both pipelines guard against absent or unexpected data with a dedicated check before acting on it — an empty missing-groups array, a genuinely `NULL` aggregation result, a `mode` field that might not exist at all in the real trigger's input — rather than letting a downstream step fail on a shape it didn't expect.

**Two independent pipelines, one execution.** `sensor_etl` and `redshift_refresh` keep fully separate watermarks and locks, even though they usually run back-to-back in one chained execution. `mode` (`full`/`glue_only`/`redshift_only`) and `backfill` are orthogonal parameters — every combination is meaningful and tested, including backfilling just one pipeline.

## Known limitations

**Mid-day gaps can be silently skipped.** `sensor_etl`'s watermark advances to the *maximum* hour it finds, not a check that every hour was present — a gap anywhere but the final hour of a day gets skipped forever. Only a missing *final* hour (`23`) self-heals, since `redshift_refresh` specifically requires it before considering a day safe. Given the simulator's real per-device dropout rate, a mid-day all-devices gap is extremely unlikely; each run's audit record remains the honest fallback signal.

**The DQ gate mirrors the simulator's own valid ranges.** It catches the corruption the simulator deliberately injects, but can't distinguish a systematic bias in the simulator from genuinely healthy data, since anything the simulator produces normally already falls inside what's considered "valid." Deliberately deferred — the fix is real effort, and nothing currently depends on catching this specific failure mode.

## Two engineering stories worth telling

**An error type no `Catch` block can ever intercept.** A JSONPath casing typo once caused a live execution to fail — and its `Catch: States.ALL` block, seemingly built for exactly this, never fired, leaving a lock stuck. The cause: AWS explicitly documents `States.Runtime` errors (malformed JSONPath references) as one of a small set of errors `States.ALL` can never catch — a broken reference is treated as a state-machine *definition* bug, not a catchable runtime failure. The real fix wasn't a broader catch — it was restructuring every reference to be provably safe by construction, guaranteed present because a prior `Choice` state already checked, rather than relying on error handling to save something that shouldn't have been able to fail.

**A `NULL` that isn't `null`.** `redshift_refresh` needed to tell "a real date was found" apart from "the aggregation matched zero rows" (SQL `NULL`). The natural assumption — that a missing value shows up as an absent or `null` field — was wrong. AWS's Redshift Data API represents every value as a tagged union: exactly one of `{"StringValue": ...}`, `{"LongValue": ...}`, or `{"IsNull": true}` is ever present, never a `StringValue` holding `null`. A NULL result doesn't produce an empty field to check — it produces a *different key entirely*. Checking for that key's presence, not its value, turned out to be the only correct approach. A related trap sat right next to it: the AWS CLI normalizes this same API's response casing to `camelCase` for display, while Step Functions' native integration passes through the raw `PascalCase` — a value copied from a CLI test silently didn't match what the live state machine actually received.