#' Industrialized dataset variables encryption.
#'
#' This function consolidates a set of procedures made to encrypt
#' variables from datasets. Variables to encrypt and input datasets can
#' be in any number, following user's needs.
#'
#' Since Phase 1.D.6.b, `crypt_r()` is **non-blocking**: it builds a
#' `cryptR_job` (one `mirai` task per filtered mask row) and returns
#' immediately. The caller inspects / waits / finalises the job through
#' the companion API (see *Value* and *See Also*). A per-row failure
#' does not interrupt the other rows — the failure is captured on the
#' corresponding task and surfaces via [cryptR_status()].
#'
#' @param mask_folder_path Folder path of the excel mask.
#' @param mask_file File name (with extension) of the excel mask.
#' @param output_path Folder path where the encrypted datasets
#' will be created.
#' @param intermediate_path Folder path where intermediate files
#' (correspondence tables for e.g) will be created.
#' @param encryption_key Character vector.
#' @param algorithm Algorithm to use.
#' From digest::digest()'s algo argument.
#' @param correspondence_table Logical vector. If TRUE, the correspondence tables will computed
#' and stored in the indicated intermediate_path.
#' @param engine One of `"auto"`, `"in_memory"`, `"streaming"`.
#'   Selects the per-row processing engine. `"auto"` (default) and
#'   `"streaming"` both route parquet-in/parquet-out and csv-in/csv-out
#'   to the streaming engines (arrow Scanner by chunks, progressive
#'   write); mixed or non-streamable endpoints (rds, xlsx, parquet→csv,
#'   csv→parquet) silently fall back to `"in_memory"` to preserve
#'   non-regression. `"in_memory"` always reads the full input into
#'   RAM (historical behaviour). Introduced in Phase 1.D.4.a
#'   (plumbing), streaming engines in 1.D.4.b/c, auto-routing rule in
#'   1.D.4.d.
#' @param chunk_size Integer. Number of rows per chunk when the
#'   effective engine is streaming. Ignored by `"in_memory"` (and by
#'   `"auto"` / `"streaming"` when falling back to in_memory). Default
#'   `1e6`.
#' @param n_workers Integer or `NULL`. Number of `mirai` daemons to
#'   spawn for the dispatch. When `NULL` (default), `crypt_r()` picks
#'   `min(parallel::detectCores() - 1, n_rows, 8L)` (floored at 1).
#'   If daemons are already running on the default profile when
#'   `crypt_r()` is called, `n_workers` is ignored and the existing
#'   daemons are reused — the user retains control and
#'   [cryptR_collect()] will not tear them down. Added in Phase 1.D.6.b.
#' @return A [`cryptR_job`][cryptR_status] object carrying one `mirai`
#'   task per filtered mask row. Inspect with [cryptR_status()]; block
#'   with [cryptR_wait()]; finalise (write the recap log and, when
#'   applicable, tear down the daemons) with [cryptR_collect()].
#' @seealso [cryptR_status()], [cryptR_wait()], [cryptR_collect()],
#'   [get_correspondence_tables()].
#' @export
crypt_r <- function(mask_folder_path, mask_file,
                    output_path, intermediate_path,
                    encryption_key, algorithm = "md5",
                    correspondence_table = TRUE,
                    engine = c("auto", "in_memory", "streaming"),
                    chunk_size = 1e6L,
                    n_workers = NULL) {
  engine <- match.arg(engine)
  stopifnot(
    is.numeric(chunk_size),
    length(chunk_size) == 1L,
    !is.na(chunk_size),
    chunk_size > 0
  )
  chunk_size <- as.integer(chunk_size)

  if (!is.null(n_workers)) {
    stopifnot(
      is.numeric(n_workers), length(n_workers) == 1L,
      !is.na(n_workers), n_workers >= 1
    )
    n_workers <- as.integer(n_workers)
  }

  # --- Fast-fail on invalid output directories (Phase 2.B) -------------
  # Both paths are *shared* by every worker. If either is missing, all
  # dispatched tasks would fail silently (bubbling up as per-task errors
  # only via cryptR_status()) and the user would only learn about it
  # after the whole job has resolved. Validating up-front turns this
  # into an actionable stop() at call time.
  #
  # Per-input paths (resolved from mask$folder_path + mask$file) are NOT
  # validated here — per CLAUDE.md, a failing row must not interrupt the
  # others, and the per-row tryCatch inside the engines already captures
  # "file not found" errors.
  .assert_writable_dir <- function(path, label) {
    if (!is.character(path) || length(path) != 1L || is.na(path) ||
      !nzchar(path)) {
      stop(
        sprintf(
          "`%s` must be a non-empty character(1) path. Got: %s",
          label,
          if (is.character(path)) {
            paste(utils::capture.output(dput(path)),
              collapse = " "
            )
          } else {
            class(path)[1]
          }
        ),
        call. = FALSE
      )
    }
    if (!dir.exists(path)) {
      stop(
        sprintf(
          "`%s` directory does not exist: %s\nCreate it before calling crypt_r().",
          label, path
        ),
        call. = FALSE
      )
    }
    invisible(NULL)
  }
  .assert_writable_dir(output_path, "output_path")
  .assert_writable_dir(intermediate_path, "intermediate_path")

  # The Excel mask:
  mask <-
    rio::import(file.path(mask_folder_path, mask_file)) %>%
    # Without eventual blank lines in the mask (for visualization ease):
    dplyr::filter_all(dplyr::any_vars(!is.na(.))) %>%
    # Inserting a row_number (useful for technical reasons):
    dplyr::mutate(row_number = dplyr::row_number()) %>%
    # Keep only lines on datasets the user want to encrypt:
    dplyr::filter(to_encrypt == "X")

  # Check eventual duplicated file names, if there is, add indication
  # to keep file names unique:
  mask <-
    dplyr::mutate(
      mask,
      dupl_encrypted_file = duplicated(encrypted_file),
      ndupl_encrypted_file = cumsum(dupl_encrypted_file) %>%
        (\(n) paste0("DUPL", n)),
      encrypted_file =
        ifelse(dupl_encrypted_file,
          paste0(
            ndupl_encrypted_file, "_",
            encrypted_file
          ),
          encrypted_file
        )
    )

  n_rows <- nrow(mask)

  # Empty mask: nothing to do. Return a trivial cryptR_job so the caller
  # can still introspect / collect uniformly. The watcher short-circuits
  # to an immediate finalize on empty-mask jobs (an empty xlsx log is
  # still produced for uniformity).
  if (n_rows == 0L) {
    job <- .new_cryptR_job(
      tasks                = list(),
      mask_rows            = mask,
      output_path          = output_path,
      intermediate_path    = intermediate_path,
      daemons_owned_by_job = FALSE
    )
    return(.start_watcher(job))
  }

  # --- n_workers resolution --------------------------------------------
  # Heuristic: leave one core to the OS, cap at 8 to avoid hammering
  # shared machines, never exceed the number of tasks.
  if (is.null(n_workers)) {
    cores <- tryCatch(parallel::detectCores(logical = TRUE),
      error = function(e) 2L
    )
    if (is.null(cores) || is.na(cores) || cores < 2L) cores <- 2L
    n_workers <- max(1L, min(as.integer(cores) - 1L, n_rows, 8L))
  } else {
    n_workers <- max(1L, min(n_workers, n_rows))
  }

  # --- Daemons ownership ------------------------------------------------
  # If daemons are already running on the default profile, we reuse them
  # and leave teardown to the user. If none are running, we set up our
  # own and flag the job so cryptR_collect() tears them down at the end.
  # `.n_workers_active()` returns NA if mirai::status() errors outright;
  # in that case we fall back to "no daemons active" (0L) and spawn our
  # own, matching the previous open-coded tryCatch behaviour.
  n_existing <- .n_workers_active()
  if (is.na(n_existing)) n_existing <- 0L

  daemons_owned_by_job <- FALSE
  if (n_existing == 0L) {
    mirai::daemons(n_workers)
    daemons_owned_by_job <- TRUE
  }

  # Load the package in each daemon so `cryptRopen:::.process_mask_row`
  # resolves. Silently tolerate failures: if cryptRopen is not installed
  # in the daemon's library paths (dev session), task dispatch below will
  # still surface the lookup error as a failed task via cryptR_status().
  try(mirai::everywhere({
    suppressPackageStartupMessages(requireNamespace("cryptRopen",
      quietly = TRUE
    ))
  }), silent = TRUE)

  # --- Dispatch one mirai task per filtered mask row -------------------
  input_paths <- paste0(mask$folder_path, "/", mask$file)

  tasks <- purrr::map(seq_len(n_rows), \(i) {
    mask_row_i <- mask[i, , drop = FALSE]
    input_path_i <- input_paths[[i]]

    # Resolve the internal dispatcher inside the daemon via
    # `getFromNamespace()` rather than `cryptRopen:::.process_mask_row`
    # at the call site. Same runtime behaviour, but `:::` in a published
    # function body triggers an R CMD check NOTE
    # ("::: calls to the package's namespace in its code"); the
    # `getFromNamespace()` form is the documented escape hatch. The
    # daemon has already been asked to load cryptRopen via the
    # `mirai::everywhere()` block above, so the namespace lookup
    # succeeds in the worker.
    mirai::mirai(
      {
        .pmr <- utils::getFromNamespace(".process_mask_row", "cryptRopen")
        .pmr(
          mask_row             = mask_row_i,
          input_path           = input_path_i,
          output_path          = output_path,
          intermediate_path    = intermediate_path,
          encryption_key       = encryption_key,
          algorithm            = algorithm,
          correspondence_table = correspondence_table,
          engine               = engine,
          chunk_size           = chunk_size
        )
      },
      mask_row_i = mask_row_i,
      input_path_i = input_path_i,
      output_path = output_path,
      intermediate_path = intermediate_path,
      encryption_key = encryption_key,
      algorithm = algorithm,
      correspondence_table = correspondence_table,
      engine = engine,
      chunk_size = chunk_size
    )
  })

  names(tasks) <- as.character(mask$encrypted_file)

  job <- .new_cryptR_job(
    tasks                = tasks,
    mask_rows            = mask,
    output_path          = output_path,
    intermediate_path    = intermediate_path,
    daemons_owned_by_job = daemons_owned_by_job
  )

  # Phase 1.D.6.c: register the auto-watcher so the recap xlsx log is
  # written as soon as the last mirai task resolves, without the user
  # having to call cryptR_collect() manually. Idempotent wrt a manual
  # collect — see R/cryptR_watcher.R + `.log_already_written()`.
  .start_watcher(job)
}
