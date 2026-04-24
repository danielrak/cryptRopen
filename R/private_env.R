# Private package environment — holds correspondence tables produced by
# crypt_data() and crypt_r(). Replaces the historical assignments into
# globalenv(). Accessed by users via get_correspondence_tables().

.cryptRopen_env <- new.env(parent = emptyenv())

#' Store a correspondence table in the package-private environment.
#'
#' @param name Character(1). Name under which the table is stored
#'   (e.g. `"tc_crypt_<label>"`).
#' @param df A data.frame / tibble.
#' @return Invisibly, `df`.
#' @noRd
.store_correspondence_table <- function(name, df) {
  assign(name, df, envir = .cryptRopen_env)
  invisible(df)
}

#' Remove all correspondence tables from the package-private environment.
#'
#' Useful between test cases and available to users who want a clean
#' slate before a new run.
#'
#' @return Invisible `NULL`.
#' @noRd
.clear_correspondence_tables <- function() {
  rm(
    list = ls(envir = .cryptRopen_env, all.names = TRUE),
    envir = .cryptRopen_env
  )
  invisible(NULL)
}
