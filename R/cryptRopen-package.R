#' cryptRopen: Industrialized Pseudonymization of Variables Across Datasets
#'
#' Deterministic, salted-hash pseudonymization of one or many datasets,
#' driven by a single Excel mask that lists, per input file, which
#' columns to hash and which to drop.
#'
#' @section Where to start:
#' Three entry points cover the typical needs:
#'
#' \itemize{
#'   \item [crypt_vector()] — hash one vector.
#'   \item [crypt_data()] — pseudonymize one data frame already in
#'     memory; the in-session workflow.
#'   \item [crypt_r()] — pseudonymize N files described by an Excel
#'     mask, in parallel, non-blocking; the industrialized workflow.
#'     Inspect / wait / finalize the returned job with
#'     [cryptR_status()], [cryptR_wait()], [cryptR_collect()],
#'     [cryptR_results()], and `summary()`.
#' }
#'
#' Use [get_correspondence_tables()] to retrieve the
#' `(original, hashed)` mapping stored after [crypt_data()] /
#' [crypt_r()] runs.
#'
#' @section Learn more:
#' \itemize{
#'   \item `vignette("cryptRopen")` — getting started.
#'   \item `vignette("crypt_r-workflow")` — industrialized walkthrough.
#'   \item `NEWS.md` — change history.
#' }
#'
#' @keywords internal
#' @importFrom magrittr %>%
#' @importFrom nanoparquet read_parquet
"_PACKAGE"

# `nanoparquet` is a transitive runtime dependency: `rio::export()` and
# `rio::import()` dispatch parquet I/O to it (the TC parquet written by
# the in-memory engine, the final dataset when the output format is
# parquet, and any parquet input). Declaring it in DESCRIPTION Imports
# pulls it at install time; the `@importFrom` above satisfies R CMD
# check's "Namespaces in Imports field not imported from" rule. We do
# not call `nanoparquet::read_parquet()` directly — keeping
# `rio::export()` is deliberate (baseline byte-identity, see the
# in-memory engine).

# Package-level globalVariables declarations. These are symbols that
# R CMD check would otherwise flag as "no visible binding" because they
# are referenced inside NSE contexts (dplyr pronouns, tibble()'s lazy
# self-referencing columns) rather than being locals.
utils::globalVariables(unique(c(
  ".", "to_encrypt", "encrypted_file",
  "dupl_encrypted_file", "ndupl_encrypted_file",
  # .inspect_column() uses tibble()'s lazy self-referencing columns
  # (e.g. prop_distinct = nb_distinct / rows). R CMD check does not
  # see these as locals; declare them here to silence the NOTE.
  "nb_distinct", "nb_na", "nb_void"
)))
