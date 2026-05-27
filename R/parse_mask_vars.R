#' Parse a comma-separated mask cell into a clean character vector.
#'
#' Centralises the historical
#'   `cell %>% str_split(",") %>% unlist() %>% str_trim()`
#' idiom duplicated across the three engines, and fixes its edge case:
#' an empty / NA / whitespace-only cell must yield `character(0)`, NOT
#' `""` or `NA` (both length 1). The length-1 form made
#' `crypt_vector(chunk[[""]])` return a length-0 vector and
#' `bind_cols()` abort with "Can't recycle `..1` (size 0)".
#'
#' Rules:
#'   - `NULL`, `NA`, `""`, `"   "`          -> `character(0)`
#'   - `"a,b , c"`                          -> `c("a", "b", "c")`
#'   - blank items inside a list (`"a,,b"`) -> dropped (`c("a", "b")`)
#'
#' @param cell A length-1 character (or NA) from a mask column.
#' @return A character vector, possibly length 0. Never `NA`, never `""`.
#' @noRd
.parse_mask_vars <- function(cell) {
  if (is.null(cell) || length(cell) == 0L || all(is.na(cell))) {
    return(character(0))
  }
  parts <- cell %>%
    stringr::str_split(",") %>%
    unlist() %>%
    stringr::str_trim()
  parts[!is.na(parts) & nchar(parts) > 0L]
}
