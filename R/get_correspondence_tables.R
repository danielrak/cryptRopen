#' Retrieve Correspondence Tables from the Current Session
#'
#' Correspondence tables are the `(original, hashed)` mappings built
#' during a call to [crypt_data()] or [crypt_r()]. They are stored in a
#' package-private environment that accumulates across successive calls
#' within the same R session and is cleared when the session ends.
#'
#' @param names Optional character vector of table names to return.
#'   When `NULL` (default), every table currently stored is returned.
#'   When a vector is passed, the result preserves the requested order;
#'   names absent from the store appear as `NULL` entries so the caller
#'   can detect misses via `is.null(result[[name]])`.
#' @return A named list of data frames / tibbles. Names follow the
#'   convention `tc_crypt_<label>` for [crypt_data()] and
#'   `tc_<encrypted_file_without_extension>` for [crypt_r()]. An empty
#'   list when no correspondence table has been produced yet in the
#'   session.
#' @seealso [crypt_data()], [crypt_r()].
#' @export
#'
#' @examples
#' # Run crypt_data() once to populate the store, then retrieve.
#' crypt_data(
#'   loaded_dataset             = mtcars[1:5, ],
#'   vars_to_encrypt            = "mpg",
#'   encryption_key             = "1234567",
#'   correspondence_table       = TRUE,
#'   correspondence_table_label = "demo"
#' )
#'
#' # Full store as a named list.
#' get_correspondence_tables()
#'
#' # Subset by name, preserving the requested order.
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
  mget(names,
    envir = .cryptRopen_env,
    ifnotfound = list(NULL)
  )
}
