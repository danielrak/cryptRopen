# Changelog

## cryptRopen 0.2.0

This release closes the 0.1.x cycle and consolidates the package into an
API-stable, CRAN-ready state. Submission to CRAN is planned once the
package has been validated on production workloads.

### API stability

The public surface
([`crypt_vector()`](https://danielrak.github.io/cryptRopen/reference/crypt_vector.md),
[`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md),
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
and its async companions,
[`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md),
[`inspect()`](https://danielrak.github.io/cryptRopen/reference/inspect.md))
is committed across the 0.x series: existing arguments will not be
removed or renamed; new arguments may be added with defaults.

### Documentation overhaul (for CRAN clarity)

- Title and Description rewritten to describe the package honestly:
  pseudonymization via salted hash, not reversible encryption.
- [`?cryptRopen`](https://danielrak.github.io/cryptRopen/reference/cryptRopen-package.md)
  is now a real index — entry points, async companions, and pointers to
  the two vignettes.
- The Getting Started vignette gained a “When *not* to use cryptRopen”
  section and an “Algorithm choice” section.
- All `@param` descriptions are standardised; two factually wrong notes
  were corrected
  ([`inspect()`](https://danielrak.github.io/cryptRopen/reference/inspect.md)
  does not require the data frame to live in
  [`globalenv()`](https://rdrr.io/r/base/environment.html);
  [`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
  accepts a data frame value, not an expression).
- Examples on the async companions migrated from `\dontrun{}` to
  runnable `\donttest{}` blocks anchored on the shipped `persons.csv`
  fixture.
- US English spelling propagated throughout (pseudonymize, normalize,
  finalize, behavior, …).

### Code modernization

- `dplyr::filter_all(any_vars(...))` replaced by
  `dplyr::filter(if_any(...))` in the mask import step (the superseded
  family was generating soft deprecation warnings on `dplyr` \>= 1.1).
- [`inspect()`](https://danielrak.github.io/cryptRopen/reference/inspect.md)’s
  optional row-count side effect now flows through
  [`message()`](https://rdrr.io/r/base/message.html) instead of
  [`print()`](https://rdrr.io/r/base/print.html).
- [`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
  argument-validation error messages reformulated for clarity, with
  `call. = FALSE`.

### Packaging

- `DESCRIPTION` gained `URL`, `BugReports`, `Depends: R (>= 4.1.0)`,
  `Language: en-US`, `Config/testthat/edition: 3`.
- `devtools` removed from `Suggests` (was unused at runtime).
- The async test suite (`tests/testthat/test-crypt_r_async.R`) now skips
  on CRAN via `skip_on_cran()`. Full coverage is exercised in CI on
  every push.

## cryptRopen 0.1.1

### Bug fixes

- [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  no longer crashes on mask rows with an **empty** `vars_to_encrypt`
  cell. Such rows are now treated as legitimate “copy / convert only”
  instructions: the input file is written to `output_path` as-is
  (re-encoded to the requested output format), `vars_to_remove` is
  applied if non-empty, no `_crypt` columns are emitted, and no
  `tc_*.parquet` is produced. The recap log records `success = TRUE` and
  `tc_name = NA` for such rows. Previously the three engines tried to
  build a zero-length `_crypt` data frame and failed at the assembly
  step with “Can’t recycle `..1` (size 0) to match `..2`” (more visible
  at CSV streaming scale).

### Improvements

- New private helper `.parse_mask_vars()` centralizes the
  `str_split(",") %>% unlist() %>% str_trim()` idiom that the three
  engines duplicated. It also normalizes `NA` / `""` / whitespace-only /
  list-internal blanks (`"a,,b"`) to `character(0)` / clean items,
  fixing a latent edge case in `vars_to_remove` as well.
- `.transform_stream_chunk()` and `.process_mask_row_in_memory()` now
  short-circuit encryption + correspondence-table construction when
  `vars_to_encrypt` is empty, while still applying `vars_to_remove`.

### Breaking-ish change

- [`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
  is now **fail-fast** on an empty `vars_to_encrypt`: it raises an
  explicit error pointing the user to `dplyr` / `rio` for plain column
  dropping or format conversion. Previously it errored too, but later
  and with a misleading message (“All indicated `vars_to_encrypt` must
  be effectively a variable name”). The asymmetry with
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  is intentional:
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  is driven by a hand-filled spreadsheet describing a heterogeneous
  batch, where a “copy / purge only” row is a reasonable use case;
  [`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
  is a direct call on a loaded object, where calling it with nothing to
  encrypt is a misuse and silently succeeding would hide a mistake.
- Side effect:
  [`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
  now trims `vars_to_encrypt` **before** the membership check, so a typo
  like `" mpg "` (with surrounding whitespace) is now silently accepted
  instead of producing the “All indicated `vars_to_encrypt`…” error. The
  historical trim happening later in the function already absorbed those
  whitespace cases on successful runs, so this aligns the membership
  check with the rest of the pipeline.

### Tests

- Two new baseline cases lock the new
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  empty-vars behavior: `empty_vars_remove_csv` (purge-only CSV) and
  `empty_vars_copy_rds` (verbatim copy of an RDS). Their `intermediate/`
  directories are empty by contract, asserted by `expect_setequal()` in
  `test-baseline.R`.
- New `test-parse_mask_vars.R` covers the helper’s edge cases (12
  assertions).
- `test-crypt_data.R` adds three `expect_error()` for the fail-fast on
  `character(0)`, `""`, `c(NA_character_, " ")`.

## cryptRopen 0.1.0

First public release milestone — closes the `refactor-v1` branch. No
public API change vs. the historical `0.0.0.9000`; under the hood,
[`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
is now non-blocking with mirai orchestration, a streaming engine handles
large parquet / CSV inputs, correspondence tables live in a
package-private environment, and the codebase has been restructured for
readability.

### Readability audit (refactor-v1)

The refactor branch ran a six-step readability audit after the hardening
phases. No API change, no semantic change; tests and
[`devtools::check()`](https://devtools.r-lib.org/reference/check.html)
stay at 0/0/0 throughout.

- **Audit 0** — retire [fusen](https://thinkr-open.github.io/fusen/):
  `R/*.R` becomes the single source of truth; `dev/flat_*.Rmd` removed.
- **Audit A** — `R/cryptRopen-package.R` stub with consolidated
  `@importFrom` and
  [`utils::globalVariables()`](https://rdrr.io/r/utils/globalVariables.html).
- **Audit B** — redundant
  [`requireNamespace()`](https://rdrr.io/r/base/ns-load.html) calls
  removed on hard `Imports`; `function (` → `function(` on three sites.
- **Audit C** — `R/crypt_r.R` (1094 lines) split into seven
  engine-specific files: `crypt_r.R`, `crypt_r_result.R`,
  `crypt_r_dispatcher.R`, `crypt_r_engine_in_memory.R`,
  `crypt_r_engine_stream_shared.R`, `crypt_r_engine_stream_parquet.R`,
  `crypt_r_engine_stream_csv.R`.
- **Audit D.1** — cryptic local names expanded: `sm` → `mask_row`, `tf`
  → `transformed`, `ds` → `arrow_dataset`, `x0` → `col`, redundant `g`
  alias dropped.
- **Audit D.2** — `styler::style_pkg()`: de-aligned `=` in multi-line
  calls, normalized whitespace.
- **Audit E** — roxygen bug fixes, `@family async_job`, missing
  `@return`, typos, inline phase references migrated to this file.

## cryptRopen 0.0.0.9000

### Phase 2 — Hardening

#### Phase 2.C — retire `assign_to_global()` and clear `R CMD check`

- `R/assign_to_global.R` removed along with its fusen flat file and
  `dev/config_fusen.yaml` entry.
- `^tests/baseline$` added to `.Rbuildignore`; baseline fixtures stay
  accessible to
  [`devtools::test()`](https://devtools.r-lib.org/reference/test.html)
  via `skip_if_no_baseline()` but are excluded from the tarball.
- `nanoparquet` moved to `Imports` (was an implicit dependency of
  [`rio::export()`](http://gesistsa.github.io/rio/reference/export.md)
  for parquet output; `R CMD check --as-cran` exposed the gap).
- `test-baseline.R`: `source("cases.R")` guarded so the file no longer
  errors when the baseline directory is stripped from the tarball.
- [`mirai::mirai()`](https://mirai.r-lib.org/reference/mirai.html) body
  resolves `.process_mask_row()` via
  [`utils::getFromNamespace()`](https://rdrr.io/r/utils/getFromNamespace.html)
  instead of the `:::` operator — clears the
  `::: calls to the package's namespace in its code` NOTE.
- [`devtools::check()`](https://devtools.r-lib.org/reference/check.html)
  reaches **0 ERROR / 0 WARNING / 0 NOTE** for the first time in the
  refactor.

#### Phase 2.B — `crypt_r()` fast-fail + `cryptR_results()`

- [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  validates `output_path` and `intermediate_path` before dispatching
  mirai tasks. Per-row input paths stay unchecked — a per-row failure
  must not interrupt the rest.
- `get_correspondence_tables(names = NULL)`: optional character vector
  to select and order the returned tables. Missing keys become `NULL`
  entries.
- New exported `cryptR_results(job)`: disk-oriented companion to
  [`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md).
  One row per filtered mask row with `output_file_path`, live `exists`,
  worker-captured `size_bytes` / `sha256`, `success`, `error_message`.
  Joins cleanly with
  [`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md)
  on `encrypted_file`.

#### Phase 2.A — enriched job state and `summary()` method

- [`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md)
  grows from 3 to 7 columns: `start_time`, `end_time`, `duration_sec`,
  `n_rows_processed` are extracted from the per-task payload. No
  breaking change.
- New S3 method
  [`summary.cryptR_job()`](https://danielrak.github.io/cryptRopen/reference/summary.cryptR_job.md)
  (exported) returns a compact dashboard object; its
  [`print()`](https://rdrr.io/r/base/print.html) method renders it.
- [`print.cryptR_job()`](https://danielrak.github.io/cryptRopen/reference/print.cryptR_job.md)
  gains an active-workers line.
- Shared helper `.n_workers_active()` de-duplicates the three open-coded
  `mirai::status()$daemons` call sites.

### Phase 1.D — `crypt_r()` refactor and mirai orchestration

#### Phase 1.D.6.c — log xlsx, TC round-trip, auto-watcher

- Engines return a typed
  `list(success, error_message, tc_name, tc_df, metrics)` via new helper
  `.make_row_result()`.
- New file `R/cryptR_log.R` with the log-writing and correspondence-
  table re-injection helpers.
- New file `R/cryptR_watcher.R` with a
  [`later::later()`](https://later.r-lib.org/reference/later.html) self-
  rescheduling watcher so the recap log is written automatically when
  the last mirai task resolves. Graceful fallback when
  [later](https://later.r-lib.org) is unavailable.
- [`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md)
  now runs the shared finalize pipeline (TC re- injection + log writer),
  idempotent against a watcher that has already fired.
- The TC limitation from 1.D.6.b is lifted:
  [`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md)
  in the parent process sees the TCs produced by mirai daemons.

#### Phase 1.D.6.b — mirai orchestration inside `crypt_r()`

- [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  becomes non-blocking: one
  [`mirai::mirai()`](https://mirai.r-lib.org/reference/mirai.html) per
  filtered mask row, results aggregated into a `cryptR_job` returned
  immediately.
- New parameter `n_workers = NULL` with heuristic
  `min(detectCores() - 1, n_rows, 8L)` (floored at 1). Existing daemons
  on the default profile are reused; only spawned daemons are torn down
  by
  [`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md).
- `.process_mask_row_in_memory()` no longer writes to
  [`globalenv()`](https://rdrr.io/r/base/environment.html): no
  `assign_to_global()`, no `eval(parse(text = ...))`. The correspondence
  table lives in `.cryptRopen_env` + on disk.
- `DESCRIPTION`: [job](https://lindeloev.github.io/job/) removed from
  `Imports`, `{parallel}` added.
- `assign_to_global()` demoted to `@noRd` (kept as a no-op to avoid
  breaking downstream code that may still reference it).

#### Phase 1.D.6.a — `cryptR_job` scaffolding

- New S3 class `cryptR_job` and companions
  [`cryptR_status()`](https://danielrak.github.io/cryptRopen/reference/cryptR_status.md),
  [`cryptR_wait()`](https://danielrak.github.io/cryptRopen/reference/cryptR_wait.md),
  [`cryptR_collect()`](https://danielrak.github.io/cryptRopen/reference/cryptR_collect.md),
  [`print.cryptR_job()`](https://danielrak.github.io/cryptRopen/reference/print.cryptR_job.md)
  — still wired in isolation;
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md)
  not touched yet.
- New files `R/cryptR_job.R` and `R/cryptR_job_helpers.R`.
- `DESCRIPTION`: [mirai](https://mirai.r-lib.org) added to `Imports`,
  [withr](https://withr.r-lib.org) to `Suggests` (test-only, for daemon
  teardown).

#### Phase 1.D.5 — baseline covers large parquet + multi-chunk streaming

- New case `large_parquet_multichunk` (50 000 rows,
  `chunk_size = 15000`) in `tests/baseline/cases.R`; exercises
  multi-iteration scanner reads.
- `tests/baseline/generate_baseline.R`: `capture_one_crypt_data()` now
  reads correspondence tables via
  [`cryptRopen::get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md)
  — fixes a latent regression from Phase 1.C where TC names were
  captured empty.

#### Phase 1.D.4 — streaming engines

- **1.D.4.a** — `crypt_r(engine, chunk_size)` parameters added and
  plumbed through the dispatcher (no routing change yet); `arrow`
  promoted from `Suggests` to `Imports`.
- **1.D.4.b** — parquet streaming engine via
  [`arrow::open_dataset()`](https://arrow.apache.org/docs/r/reference/open_dataset.html)
  - Scanner + `ParquetFileWriter`. Correspondence table built
    incrementally with `distinct()`.
- **1.D.4.c** — CSV streaming engine via
  [`arrow::open_csv_dataset()`](https://arrow.apache.org/docs/r/reference/open_delim_dataset.html)
  for reads; `utils::write.table(append = TRUE)` for writes
  (`arrow::CsvWriter` not universally exported).
- **1.D.4.d** — shared helpers factored out
  (`.transform_stream_chunk()`, `.finalize_stream_tc()`,
  `.write_stream_inspect()`); dispatcher routes parquet-in/parquet-out
  and csv-in/csv-out to streaming, mixed or non-streamable endpoints to
  in-memory.
  [`rio::import()`](http://gesistsa.github.io/rio/reference/import.md)
  kept on the inspect relecture to preserve baseline
  [`class()`](https://rdrr.io/r/base/class.html) tuples on CSV date
  columns.

#### Phase 1.D.1 → 1.D.3 — extract and freeze the in-memory engine

- **1.D.1** — `.process_mask_row()` extracted from
  [`crypt_r()`](https://danielrak.github.io/cryptRopen/reference/crypt_r.md);
  thin orchestrator + helper.
- **1.D.2** — `.process_mask_row()` becomes a dispatcher; historical
  body moves into `.process_mask_row_in_memory()` unchanged.
- **1.D.3** — three invariant test blocks lock the in-memory engine:
  inspect xlsx layout, TC parquet uniqueness, output column order.
  `.process_mask_row_in_memory()` marked `@section FROZEN`.

### Phase 1.A → 1.C — API surface refactor

- **Phase 1.A** —
  [`crypt_vector()`](https://danielrak.github.io/cryptRopen/reference/crypt_vector.md)
  cleaned up: helper `.normalize_crypt_input()` extracted,
  mask-before-hash, `vapply(USE.NAMES = FALSE)`.
- **Phase 1.B** —
  [`inspect()`](https://danielrak.github.io/cryptRopen/reference/inspect.md)
  cleaned up: helper `.inspect_column()` extracted, `mutate_if` →
  `vapply`/`lapply` on POSIXct columns, `map_df` →
  `list_rbind(names_to = "variables")`.
- **Phase 1.C** —
  [`crypt_data()`](https://danielrak.github.io/cryptRopen/reference/crypt_data.md)
  decoupled from
  [`globalenv()`](https://rdrr.io/r/base/environment.html).
  Correspondence tables now routed through `.cryptRopen_env`
  (`R/private_env.R`) and retrievable via the new exported
  [`get_correspondence_tables()`](https://danielrak.github.io/cryptRopen/reference/get_correspondence_tables.md).

### Phase 0 — baseline regression infrastructure

- `tests/baseline/` with 29 cases (12 `crypt_vector`, 6 `crypt_data`, 11
  `crypt_r`) generated by `generate_baseline.R` and compared with
  `test-baseline.R`. All subsequent refactors preserve byte-level
  equality of disk outputs and semantic equality of in-memory results.
