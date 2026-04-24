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
