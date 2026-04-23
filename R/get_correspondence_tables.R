#' Retrieve correspondence tables produced by the last calls of
#' `crypt_data()` or `crypt_r()`.
#'
#' Correspondence tables are pairs of (original, encrypted) values built
#' during encryption. They are stored in a package-private environment
#' across successive calls within the same R session (they accumulate
#' until the end of the session).
#'
#' @param names Optional character vector — names of the tables to
#'   return. When `NULL` (default), every table currently stored is
#'   returned. When a vector is passed, the result is a subset
#'   preserving the requested order; names absent from the store are
#'   returned as missing entries (the output is still named so the
#'   caller can detect the miss via `is.null(result[[name]])`). Added
#'   in Phase 2.B.
#'
#' @return A named list of data frames / tibbles. Names follow the
#'   convention `tc_crypt_<label>` for `crypt_data()` and
#'   `tc_<encrypted_file_sans_ext>` for `crypt_r()`. Empty list if no
#'   correspondence table has been produced yet in the session.
#'
#' @export
#'
#' @examples
#' crypt_data(
#'   loaded_dataset = mtcars[1:5, ],
#'   vars_to_encrypt = "mpg",
#'   encryption_key = "1234567",
#'   correspondence_table = TRUE,
#'   correspondence_table_label = "demo"
#' )
#' get_correspondence_tables()
#' get_correspondence_tables(names = "tc_crypt_demo")
get_correspondence_tables <- function(names = NULL) {
  if (is.null(names)) {
    return(as.list(.cryptRopen_env))
  }

  stopifnot(is.character(names), !any(is.na(names)))
  # `mget` with `ifnotfound` returns NULL for missing keys, preserving
  # the requested order. The result is always a named list of the same
  # length as `names`; callers introspect `is.null(x[[n]])` to detect
  # the miss.
  mget(names, envir = .cryptRopen_env,
       ifnotfound = list(NULL))
}
