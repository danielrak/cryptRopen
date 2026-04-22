#' Retrieve correspondence tables produced by the last calls of
#' `crypt_data()` or `crypt_r()`.
#'
#' Correspondence tables are pairs of (original, encrypted) values built
#' during encryption. They are stored in a package-private environment
#' across successive calls within the same R session (they accumulate
#' until the end of the session).
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
get_correspondence_tables <- function() {
  as.list(.cryptRopen_env)
}
