# tests/baseline/generate_baseline.R
#
# One-shot script to capture reference outputs of the CURRENT (pre-refactor)
# package. Outputs are committed under tests/baseline/ and compared by
# tests/testthat/test-baseline.R against the refactored code.
#
# -----------------------------------------------------------------------------
# USAGE (from the package root, C:/cryptRopen/cryptRopen/):
#
#   devtools::load_all(".")
#   source("tests/baseline/generate_baseline.R")
#
# Required once: install.packages(c("arrow", "readxl", "jsonlite", "nanoparquet"))
#
# Idempotent: re-running overwrites inputs and outputs. Run it ONCE against
# the baseline version of the package, then commit the outputs. Do NOT run it
# against a refactored version — that would erase the regression reference.
# -----------------------------------------------------------------------------


# All script-level work is wrapped in a function so its local variables
# (loop counters, per-case scratch) never pollute globalenv(). This matters
# because the capture itself inspects globalenv() to detect crypt_data's
# and crypt_r's intentional side effects.
.generate_baseline <- function() {

  if (!exists("crypt_vector",  envir = asNamespace("cryptRopen")) ||
      !exists("crypt_data",    envir = asNamespace("cryptRopen")) ||
      !exists("crypt_r",       envir = asNamespace("cryptRopen"))) {
    stop("cryptRopen functions not found. Run devtools::load_all('.') first.")
  }

  # Deterministic environment
  Sys.setenv(TZ = "UTC")
  old_locale <- Sys.getlocale("LC_COLLATE")
  Sys.setlocale("LC_COLLATE", "C")
  on.exit(Sys.setlocale("LC_COLLATE", old_locale), add = TRUE)

  # Paths
  baseline_dir <- "tests/baseline"
  inputs_dir   <- file.path(baseline_dir, "inputs")
  datasets_dir <- file.path(inputs_dir, "datasets")
  masks_dir    <- file.path(inputs_dir, "masks")
  outputs_dir  <- file.path(baseline_dir, "outputs")
  cv_out_dir   <- file.path(outputs_dir, "crypt_vector")
  cd_out_dir   <- file.path(outputs_dir, "crypt_data")
  cr_out_dir   <- file.path(outputs_dir, "crypt_r")

  for (d in c(datasets_dir, masks_dir, cv_out_dir, cd_out_dir, cr_out_dir)) {
    dir.create(d, recursive = TRUE, showWarnings = FALSE)
  }

  # Wipe any pre-existing outputs so we start from a clean slate.
  unlink(list.files(cv_out_dir, full.names = TRUE), recursive = TRUE, force = TRUE)
  unlink(list.files(cd_out_dir, full.names = TRUE), recursive = TRUE, force = TRUE)
  unlink(list.files(cr_out_dir, full.names = TRUE), recursive = TRUE, force = TRUE)

  # Load case definitions + helpers (mask/dataset builders, sync patch)
  # locally so none of these symbols end up in globalenv().
  local_env <- environment()
  sys.source(file.path(baseline_dir, "cases.R"), envir = local_env)

  sha256_file <- function(path) {
    digest::digest(file = path, algo = "sha256")
  }

  # -----------------------------------------------------------------------
  # Inputs
  # -----------------------------------------------------------------------
  message("Generating input datasets ...")
  write_all_datasets(datasets_dir)

  message("Generating input masks ...")
  abs_datasets_dir <- normalizePath(datasets_dir, winslash = "/",
                                    mustWork = TRUE)
  write_all_masks(masks_dir, abs_datasets_dir)

  # -----------------------------------------------------------------------
  # crypt_vector
  # -----------------------------------------------------------------------
  message("Capturing crypt_vector outputs ...")
  cv_manifest <- lapply(crypt_vector_cases, function(case) {
    res <- do.call(cryptRopen::crypt_vector, case$args)
    out_path <- file.path(cv_out_dir, paste0(case$name, ".rds"))
    saveRDS(res, out_path)
    list(
      name   = case$name,
      output = file.path("crypt_vector", paste0(case$name, ".rds")),
      sha256 = sha256_file(out_path)
    )
  })

  # -----------------------------------------------------------------------
  # crypt_data
  # -----------------------------------------------------------------------
  message("Capturing crypt_data outputs ...")
  capture_one_crypt_data <- function(case) {
    # Phase 1.C aligned: crypt_data() no longer pollutes globalenv() — TCs
    # live in the package-private env `.cryptRopen_env`, retrieved via
    # `get_correspondence_tables()`. Clear the env case-to-case so each
    # capture only records this case's TCs. A belt-and-braces globalenv
    # cleanup remains in case an older (pre-1.C) code path ever slips
    # back in; it should be a no-op post-1.C.
    cryptRopen:::.clear_correspondence_tables()
    pre <- ls(envir = globalenv())

    args <- case$args_factory()
    res  <- do.call(cryptRopen::crypt_data, args)

    tcs <- cryptRopen::get_correspondence_tables()

    new_names <- setdiff(ls(envir = globalenv()), pre)
    if (length(new_names) > 0L) {
      rm(list = new_names, envir = globalenv())
    }

    payload <- list(result = res, tcs = tcs)
    out_path <- file.path(cd_out_dir, paste0(case$name, ".rds"))
    saveRDS(payload, out_path)

    cryptRopen:::.clear_correspondence_tables()

    list(
      name     = case$name,
      output   = file.path("crypt_data", paste0(case$name, ".rds")),
      sha256   = sha256_file(out_path),
      tc_names = as.list(names(tcs))
    )
  }
  cd_manifest <- lapply(crypt_data_cases, capture_one_crypt_data)

  # -----------------------------------------------------------------------
  # crypt_r
  # -----------------------------------------------------------------------
  message("Capturing crypt_r outputs (with synchronous crypt_r patch) ...")
  restore_crypt_r <- install_sync_crypt_r_patch()
  on.exit(restore_crypt_r(), add = TRUE)

  abs_masks_dir <- normalizePath(masks_dir, winslash = "/", mustWork = TRUE)

  capture_one_crypt_r <- function(case) {
    message("  case: ", case$name)
    case_dir          <- file.path(cr_out_dir, case$name)
    case_output       <- file.path(case_dir, "output")
    case_intermediate <- file.path(case_dir, "intermediate")

    unlink(case_dir, recursive = TRUE, force = TRUE)
    dir.create(case_output,       recursive = TRUE, showWarnings = FALSE)
    dir.create(case_intermediate, recursive = TRUE, showWarnings = FALSE)

    pre <- ls(envir = globalenv())

    cryptRopen::crypt_r(
      mask_folder_path     = abs_masks_dir,
      mask_file            = case$mask,
      output_path          = case_output,
      intermediate_path    = case_intermediate,
      encryption_key       = CRYPT_R_KEY,
      algorithm            = CRYPT_R_ALGO,
      correspondence_table = case$correspondence_table
    )

    new_names <- setdiff(ls(envir = globalenv()), pre)
    if (length(new_names) > 0L) {
      rm(list = new_names, envir = globalenv())
    }

    out_files <- list.files(case_output,       recursive = TRUE, full.names = FALSE)
    int_files <- list.files(case_intermediate, recursive = TRUE, full.names = FALSE)

    files_record <- c(
      lapply(out_files, function(f) list(
        dir     = "output",
        relpath = f,
        sha256  = sha256_file(file.path(case_output, f))
      )),
      lapply(int_files, function(f) list(
        dir     = "intermediate",
        relpath = f,
        sha256  = sha256_file(file.path(case_intermediate, f))
      ))
    )

    list(
      name                 = case$name,
      mask                 = case$mask,
      correspondence_table = case$correspondence_table,
      encryption_key       = CRYPT_R_KEY,
      algorithm            = CRYPT_R_ALGO,
      files                = files_record
    )
  }
  cr_manifest <- lapply(crypt_r_cases, capture_one_crypt_r)

  # -----------------------------------------------------------------------
  # Manifest
  # -----------------------------------------------------------------------
  manifest <- list(
    package_version = as.character(utils::packageVersion("cryptRopen")),
    generated_at    = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    crypt_vector    = cv_manifest,
    crypt_data      = cd_manifest,
    crypt_r         = cr_manifest
  )

  manifest_path <- file.path(baseline_dir, "manifest.json")
  jsonlite::write_json(manifest, manifest_path,
                       auto_unbox = TRUE, pretty = TRUE, null = "null",
                       na = "null")

  message("Baseline written:")
  message("  ", length(cv_manifest), " crypt_vector cases")
  message("  ", length(cd_manifest), " crypt_data   cases")
  message("  ", length(cr_manifest), " crypt_r      cases")
  message("Manifest: ", manifest_path)

  invisible(manifest)
}

.generate_baseline()
