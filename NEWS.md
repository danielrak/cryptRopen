# cryptRopen 0.1.0

First public release milestone — closes the `refactor-v1` branch. No
public API change vs. the historical `0.0.0.9000`; under the hood,
`crypt_r()` is now non-blocking with mirai orchestration, a streaming
engine handles large parquet / CSV inputs, correspondence tables live
in a package-private environment, and the codebase has been
restructured for readability.

## Readability audit (refactor-v1)

The refactor branch ran a six-step readability audit after the hardening
phases. No API change, no semantic change; tests and `devtools::check()`
stay at 0/0/0 throughout.

* **Audit 0** — retire `{fusen}`: `R/*.R` becomes the single source of
  truth; `dev/flat_*.Rmd` removed.
* **Audit A** — `R/cryptRopen-package.R` stub with consolidated
  `@importFrom` and `utils::globalVariables()`.
* **Audit B** — redundant `requireNamespace()` calls removed on hard
  `Imports`; `function (` → `function(` on three sites.
* **Audit C** — `R/crypt_r.R` (1094 lines) split into seven
  engine-specific files: `crypt_r.R`, `crypt_r_result.R`,
  `crypt_r_dispatcher.R`, `crypt_r_engine_in_memory.R`,
  `crypt_r_engine_stream_shared.R`, `crypt_r_engine_stream_parquet.R`,
  `crypt_r_engine_stream_csv.R`.
* **Audit D.1** — cryptic local names expanded: `sm` → `mask_row`,
  `tf` → `transformed`, `ds` → `arrow_dataset`, `x0` → `col`,
  redundant `g` alias dropped.
* **Audit D.2** — `styler::style_pkg()`: de-aligned `=` in multi-line
  calls, normalised whitespace.
* **Audit E** — roxygen bug fixes, `@family async_job`, missing
  `@return`, typos, inline phase references migrated to this file.

# cryptRopen 0.0.0.9000

## Phase 2 — Hardening

### Phase 2.C — retire `assign_to_global()` and clear `R CMD check`

* `R/assign_to_global.R` removed along with its fusen flat file and
  `dev/config_fusen.yaml` entry.
* `^tests/baseline$` added to `.Rbuildignore`; baseline fixtures stay
  accessible to `devtools::test()` via `skip_if_no_baseline()` but are
  excluded from the tarball.
* `nanoparquet` moved to `Imports` (was an implicit dependency of
  `rio::export()` for parquet output; `R CMD check --as-cran` exposed
  the gap).
* `test-baseline.R`: `source("cases.R")` guarded so the file no longer
  errors when the baseline directory is stripped from the tarball.
* `mirai::mirai()` body resolves `.process_mask_row()` via
  `utils::getFromNamespace()` instead of the `:::` operator — clears
  the `::: calls to the package's namespace in its code` NOTE.
* `devtools::check()` reaches **0 ERROR / 0 WARNING / 0 NOTE** for the
  first time in the refactor.

### Phase 2.B — `crypt_r()` fast-fail + `cryptR_results()`

* `crypt_r()` validates `output_path` and `intermediate_path` before
  dispatching mirai tasks. Per-row input paths stay unchecked — a
  per-row failure must not interrupt the rest.
* `get_correspondence_tables(names = NULL)`: optional character vector
  to select and order the returned tables. Missing keys become `NULL`
  entries.
* New exported `cryptR_results(job)`: disk-oriented companion to
  `cryptR_status()`. One row per filtered mask row with
  `output_file_path`, live `exists`, worker-captured `size_bytes` /
  `sha256`, `success`, `error_message`. Joins cleanly with
  `cryptR_status()` on `encrypted_file`.

### Phase 2.A — enriched job state and `summary()` method

* `cryptR_status()` grows from 3 to 7 columns: `start_time`,
  `end_time`, `duration_sec`, `n_rows_processed` are extracted from
  the per-task payload. No breaking change.
* New S3 method `summary.cryptR_job()` (exported) returns a compact
  dashboard object; its `print()` method renders it.
* `print.cryptR_job()` gains an active-workers line.
* Shared helper `.n_workers_active()` de-duplicates the three
  open-coded `mirai::status()$daemons` call sites.

## Phase 1.D — `crypt_r()` refactor and mirai orchestration

### Phase 1.D.6.c — log xlsx, TC round-trip, auto-watcher

* Engines return a typed `list(success, error_message, tc_name, tc_df,
  metrics)` via new helper `.make_row_result()`.
* New file `R/cryptR_log.R` with the log-writing and correspondence-
  table re-injection helpers.
* New file `R/cryptR_watcher.R` with a `later::later()` self-
  rescheduling watcher so the recap log is written automatically when
  the last mirai task resolves. Graceful fallback when `{later}` is
  unavailable.
* `cryptR_collect()` now runs the shared finalize pipeline (TC re-
  injection + log writer), idempotent against a watcher that has
  already fired.
* The TC limitation from 1.D.6.b is lifted:
  `get_correspondence_tables()` in the parent process sees the TCs
  produced by mirai daemons.

### Phase 1.D.6.b — mirai orchestration inside `crypt_r()`

* `crypt_r()` becomes non-blocking: one `mirai::mirai()` per filtered
  mask row, results aggregated into a `cryptR_job` returned
  immediately.
* New parameter `n_workers = NULL` with heuristic
  `min(detectCores() - 1, n_rows, 8L)` (floored at 1). Existing
  daemons on the default profile are reused; only spawned daemons are
  torn down by `cryptR_collect()`.
* `.process_mask_row_in_memory()` no longer writes to `globalenv()`:
  no `assign_to_global()`, no `eval(parse(text = ...))`. The
  correspondence table lives in `.cryptRopen_env` + on disk.
* `DESCRIPTION`: `{job}` removed from `Imports`, `{parallel}` added.
* `assign_to_global()` demoted to `@noRd` (kept as a no-op to avoid
  breaking downstream code that may still reference it).

### Phase 1.D.6.a — `cryptR_job` scaffolding

* New S3 class `cryptR_job` and companions `cryptR_status()`,
  `cryptR_wait()`, `cryptR_collect()`, `print.cryptR_job()` — still
  wired in isolation; `crypt_r()` not touched yet.
* New files `R/cryptR_job.R` and `R/cryptR_job_helpers.R`.
* `DESCRIPTION`: `{mirai}` added to `Imports`, `{withr}` to
  `Suggests` (test-only, for daemon teardown).

### Phase 1.D.5 — baseline covers large parquet + multi-chunk streaming

* New case `large_parquet_multichunk` (50 000 rows, `chunk_size =
  15000`) in `tests/baseline/cases.R`; exercises multi-iteration
  scanner reads.
* `tests/baseline/generate_baseline.R`: `capture_one_crypt_data()`
  now reads correspondence tables via
  `cryptRopen::get_correspondence_tables()` — fixes a latent
  regression from Phase 1.C where TC names were captured empty.

### Phase 1.D.4 — streaming engines

* **1.D.4.a** — `crypt_r(engine, chunk_size)` parameters added and
  plumbed through the dispatcher (no routing change yet); `arrow`
  promoted from `Suggests` to `Imports`.
* **1.D.4.b** — parquet streaming engine via `arrow::open_dataset()`
  + Scanner + `ParquetFileWriter`. Correspondence table built
  incrementally with `distinct()`.
* **1.D.4.c** — CSV streaming engine via
  `arrow::open_csv_dataset()` for reads; `utils::write.table(append
  = TRUE)` for writes (`arrow::CsvWriter` not universally exported).
* **1.D.4.d** — shared helpers factored out
  (`.transform_stream_chunk()`, `.finalize_stream_tc()`,
  `.write_stream_inspect()`); dispatcher routes parquet-in/parquet-out
  and csv-in/csv-out to streaming, mixed or non-streamable endpoints
  to in-memory. `rio::import()` kept on the inspect relecture to
  preserve baseline `class()` tuples on CSV date columns.

### Phase 1.D.1 → 1.D.3 — extract and freeze the in-memory engine

* **1.D.1** — `.process_mask_row()` extracted from `crypt_r()`; thin
  orchestrator + helper.
* **1.D.2** — `.process_mask_row()` becomes a dispatcher; historical
  body moves into `.process_mask_row_in_memory()` unchanged.
* **1.D.3** — three invariant test blocks lock the in-memory engine:
  inspect xlsx layout, TC parquet uniqueness, output column order.
  `.process_mask_row_in_memory()` marked `@section FROZEN`.

## Phase 1.A → 1.C — API surface refactor

* **Phase 1.A** — `crypt_vector()` cleaned up: helper
  `.normalize_crypt_input()` extracted, mask-before-hash,
  `vapply(USE.NAMES = FALSE)`.
* **Phase 1.B** — `inspect()` cleaned up: helper `.inspect_column()`
  extracted, `mutate_if` → `vapply`/`lapply` on POSIXct columns,
  `map_df` → `list_rbind(names_to = "variables")`.
* **Phase 1.C** — `crypt_data()` decoupled from `globalenv()`.
  Correspondence tables now routed through `.cryptRopen_env`
  (`R/private_env.R`) and retrievable via the new exported
  `get_correspondence_tables()`.

## Phase 0 — baseline regression infrastructure

* `tests/baseline/` with 29 cases (12 `crypt_vector`, 6 `crypt_data`,
  11 `crypt_r`) generated by `generate_baseline.R` and compared with
  `test-baseline.R`. All subsequent refactors preserve byte-level
  equality of disk outputs and semantic equality of in-memory
  results.
