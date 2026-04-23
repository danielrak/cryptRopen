# tests/testthat/test-baseline.R
#
# Non-regression tests: for every case defined in tests/baseline/cases.R,
# re-run the relevant function with the (possibly refactored) package and
# compare to the baseline outputs captured by tests/baseline/generate_baseline.R.
#
# Skipped automatically when baseline fixtures are absent.


# ---------------------------------------------------------------------------
# Locating the baseline
# ---------------------------------------------------------------------------

baseline_root <- function() {
  # test_path() points at tests/testthat/
  p <- testthat::test_path("..", "baseline")
  normalizePath(p, winslash = "/", mustWork = FALSE)
}

skip_if_no_baseline <- function() {
  root <- baseline_root()
  if (!file.exists(file.path(root, "manifest.json"))) {
    testthat::skip("baseline fixtures not generated yet")
  }
  if (!dir.exists(file.path(root, "outputs"))) {
    testthat::skip("baseline outputs directory missing")
  }
  root
}

# Load case definitions once (shared with generate_baseline.R)
source(testthat::test_path("..", "baseline", "cases.R"))


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------

read_any <- function(path) {
  ext <- tolower(tools::file_ext(path))
  switch(
    ext,
    "parquet" = as.data.frame(arrow::read_parquet(path)),
    "rds"     = readRDS(path),
    "xlsx"    = as.data.frame(readxl::read_xlsx(path)),
    "csv"     = utils::read.csv(path, stringsAsFactors = FALSE,
                                na.strings = c("NA", ""),
                                check.names = FALSE),
    # Fallback via rio for other formats
    as.data.frame(rio::import(path))
  )
}

# Inspect files are xlsx with a specific shape — compare as-read.
read_inspect <- function(path) {
  as.data.frame(readxl::read_xlsx(path, col_names = FALSE))
}

# Normalize TC dataframe: sort rows deterministically across all cols.
sort_tc <- function(df) {
  df[do.call(order, as.list(df)), , drop = FALSE]
}

compare_dataset <- function(ref_path, new_path, info = NULL) {
  testthat::expect_true(file.exists(new_path),
                        info = paste(info, "- file exists:", new_path))
  ref <- read_any(ref_path)
  new <- read_any(new_path)
  # Row names are meaningless once written to disk and read back;
  # ensure both sides are numeric 1..n for comparison.
  rownames(ref) <- NULL
  rownames(new) <- NULL
  testthat::expect_equal(new, ref, info = info)
}

compare_tc <- function(ref_path, new_path, info = NULL) {
  testthat::expect_true(file.exists(new_path),
                        info = paste(info, "- file exists:", new_path))
  ref <- sort_tc(as.data.frame(arrow::read_parquet(ref_path)))
  new <- sort_tc(as.data.frame(arrow::read_parquet(new_path)))
  rownames(ref) <- NULL
  rownames(new) <- NULL
  testthat::expect_equal(new, ref, info = info)
}

compare_inspect <- function(ref_path, new_path, info = NULL) {
  testthat::expect_true(file.exists(new_path),
                        info = paste(info, "- file exists:", new_path))
  ref <- read_inspect(ref_path)
  new <- read_inspect(new_path)
  rownames(ref) <- NULL
  rownames(new) <- NULL
  testthat::expect_equal(new, ref, info = info)
}


# ---------------------------------------------------------------------------
# crypt_vector baseline
# ---------------------------------------------------------------------------

test_that("crypt_vector matches baseline", {
  root <- skip_if_no_baseline()

  for (case in crypt_vector_cases) {
    ref_path <- file.path(root, "outputs", "crypt_vector",
                          paste0(case$name, ".rds"))
    if (!file.exists(ref_path)) {
      fail(paste0("missing baseline for crypt_vector case: ", case$name))
      next
    }
    ref <- readRDS(ref_path)
    new <- do.call(crypt_vector, case$args)
    expect_equal(new, ref, info = case$name)
  }
})


# ---------------------------------------------------------------------------
# crypt_data baseline
# ---------------------------------------------------------------------------

test_that("crypt_data matches baseline (result + tc_* via get_correspondence_tables())", {
  root <- skip_if_no_baseline()

  for (case in crypt_data_cases) {
    ref_path <- file.path(root, "outputs", "crypt_data",
                          paste0(case$name, ".rds"))
    if (!file.exists(ref_path)) {
      fail(paste0("missing baseline for crypt_data case: ", case$name))
      next
    }
    ref <- readRDS(ref_path)

    # Reset the package-private env so we only observe this case's output.
    cryptRopen:::.clear_correspondence_tables()
    # Also assert crypt_data no longer pollutes globalenv().
    pre_globals <- ls(envir = globalenv())

    args <- case$args_factory()
    new_result <- do.call(crypt_data, args)

    expect_equal(new_result, ref$result, info = paste(case$name, "- result"))

    expect_equal(setdiff(ls(envir = globalenv()), pre_globals),
                 character(0),
                 info = paste(case$name, "- no globalenv pollution"))

    new_tcs <- get_correspondence_tables()

    expect_equal(sort(names(new_tcs)), sort(names(ref$tcs)),
                 info = paste(case$name, "- tc names"))
    for (nm in names(ref$tcs)) {
      if (nm %in% names(new_tcs)) {
        expect_equal(new_tcs[[nm]], ref$tcs[[nm]],
                     info = paste(case$name, "- tc:", nm))
      }
    }
  }

  cryptRopen:::.clear_correspondence_tables()
})


# ---------------------------------------------------------------------------
# crypt_r baseline
#
# Strategy:
#   - Regenerate inputs (datasets + masks) into a tempdir so we don't
#     depend on committed absolute paths.
#   - Rewrite body(crypt_r) to strip job::job() calls while the current
#     implementation still uses job. See install_sync_crypt_r_patch() in
#     tests/baseline/cases.R. After the Phase 1.D.6 mirai refactor, the
#     rewriter is a no-op (no job::job in the AST) and the test waits via
#     cryptR_wait() on the returned cryptR_job object.
# ---------------------------------------------------------------------------

run_crypt_r_case <- function(case, masks_dir, output_dir, intermediate_dir) {
  pre <- ls(envir = globalenv())
  on.exit({
    post <- ls(envir = globalenv())
    new_names <- setdiff(post, pre)
    if (length(new_names) > 0) rm(list = new_names, envir = globalenv())
  }, add = TRUE)

  # Explicit cryptRopen:: prefix forces namespace lookup, so the body rewrite
  # installed by install_sync_crypt_r_patch() (which only updates the namespace
  # binding reliably) is always picked up.
  #
  # chunk_size is case-optional (introduced Phase 1.D.5). When NULL, rely on
  # crypt_r()'s default (1e6); when set (e.g. large_parquet_multichunk), it is
  # forwarded explicitly so the baseline exercises real multi-chunk streaming.
  args <- list(
    mask_folder_path     = masks_dir,
    mask_file            = case$mask,
    output_path          = output_dir,
    intermediate_path    = intermediate_dir,
    encryption_key       = CRYPT_R_KEY,
    algorithm            = CRYPT_R_ALGO,
    correspondence_table = case$correspondence_table
  )
  if (!is.null(case$chunk_size)) {
    args$chunk_size <- case$chunk_size
  }
  result <- do.call(cryptRopen::crypt_r, args)

  # Post-refactor: result will be a cryptR_job — wait for completion.
  if (inherits(result, "cryptR_job") &&
      exists("cryptR_wait", envir = asNamespace("cryptRopen"))) {
    cryptRopen::cryptR_wait(result)
  }

  invisible(result)
}

compare_crypt_r_file <- function(case_name, rel_file, ref_path, new_path) {
  ext <- tolower(tools::file_ext(rel_file))
  is_tc <- grepl("^tc_", basename(rel_file)) && ext == "parquet"
  is_inspect <- grepl("^inspect_", basename(rel_file)) && ext == "xlsx"

  info <- paste0(case_name, " :: ", rel_file)
  if (is_tc) {
    compare_tc(ref_path, new_path, info = info)
  } else if (is_inspect) {
    compare_inspect(ref_path, new_path, info = info)
  } else {
    compare_dataset(ref_path, new_path, info = info)
  }
}

test_that("crypt_r matches baseline", {
  root <- skip_if_no_baseline()
  manifest <- jsonlite::read_json(file.path(root, "manifest.json"),
                                  simplifyVector = FALSE)

  # Set up a fresh tempdir with regenerated inputs
  tmp_root <- tempfile("cryptR_baseline_")
  dir.create(tmp_root)
  on.exit(unlink(tmp_root, recursive = TRUE, force = TRUE), add = TRUE)

  tmp_datasets <- file.path(tmp_root, "datasets")
  tmp_masks    <- file.path(tmp_root, "masks")
  write_all_datasets(tmp_datasets)
  write_all_masks(tmp_masks, normalizePath(tmp_datasets, winslash = "/",
                                           mustWork = TRUE))

  # Deterministic env during comparison
  old_tz <- Sys.getenv("TZ")
  old_locale <- Sys.getlocale("LC_COLLATE")
  Sys.setenv(TZ = "UTC")
  Sys.setlocale("LC_COLLATE", "C")
  on.exit({
    Sys.setenv(TZ = old_tz)
    Sys.setlocale("LC_COLLATE", old_locale)
  }, add = TRUE)

  # Rewrite body(crypt_r) to strip job::job() for synchronous execution while
  # the pre-refactor implementation is still in place. No-op once crypt_r is
  # refactored away from the `job` package.
  restore_crypt_r <- install_sync_crypt_r_patch()
  on.exit(restore_crypt_r(), add = TRUE)

  abs_masks <- normalizePath(tmp_masks, winslash = "/", mustWork = TRUE)

  for (case_entry in manifest$crypt_r) {
    case <- Filter(function(x) x$name == case_entry$name, crypt_r_cases)[[1]]

    case_dir      <- file.path(tmp_root, "cases", case$name)
    new_output    <- file.path(case_dir, "output")
    new_intermed  <- file.path(case_dir, "intermediate")
    dir.create(new_output,   recursive = TRUE, showWarnings = FALSE)
    dir.create(new_intermed, recursive = TRUE, showWarnings = FALSE)

    run_crypt_r_case(case, abs_masks, new_output, new_intermed)

    # Compare every file listed in the manifest
    ref_case_dir <- file.path(root, "outputs", "crypt_r", case$name)
    for (f in case_entry$files) {
      ref_path <- file.path(ref_case_dir, f$dir, f$relpath)
      new_path <- file.path(case_dir,     f$dir, f$relpath)
      compare_crypt_r_file(case$name, f$relpath, ref_path, new_path)
    }

    # Also verify no unexpected extra files appeared
    new_out_files <- list.files(new_output,   recursive = TRUE)
    new_int_files <- list.files(new_intermed, recursive = TRUE)
    expected_out <- vapply(Filter(function(x) x$dir == "output",
                                  case_entry$files),
                           function(x) x$relpath, character(1))
    expected_int <- vapply(Filter(function(x) x$dir == "intermediate",
                                  case_entry$files),
                           function(x) x$relpath, character(1))
    expect_setequal(new_out_files, expected_out)
    expect_setequal(new_int_files, expected_int)
  }
})
