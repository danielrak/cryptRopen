#' Inspect a data frame
#'
#' Given a data frame as an input, this function outputs an broad view
#' of this data frame contents:
#' dimensions, list of variables and
#' systematic corresponding information (type,
#' frequence and proportion of unique values, missing and 0-length values;
#' and up to the 10 first unique values).
#' @param data_frame The data.frame to explore.
#' Need to exist in the Global Environment.
#' @param nrow Logical. If TRUE, the number of observations of
#' the dataset is printed on stdout in addition to the returned tibble.
#' @return A tibble with one row per column of `data_frame` (or several
#'   rows when `class(column)` has length > 1, e.g. ordered factors),
#'   holding systematic inspection metrics.
#' @export
#'
#' @examples
#' inspect(CO2)
inspect <- function(data_frame, nrow = FALSE) {
  rows <- base::nrow(data_frame)

  # Date-time class correction to keep class(.x) single-valued
  # (base R equivalent of dplyr::mutate_if(is.POSIXct, ...),
  # avoids tidyselect deprecation warnings):
  posixct_cols <- names(data_frame)[
    vapply(data_frame, lubridate::is.POSIXct, logical(1))
  ]
  if (length(posixct_cols) > 0) {
    data_frame[posixct_cols] <- lapply(
      data_frame[posixct_cols],
      \(x) structure(as.character(x), class = "Date-time")
    )
  }

  df <- purrr::list_rbind(
    purrr::map(data_frame, \(x) .inspect_column(x, rows = rows)),
    names_to = "variables"
  )

  if (nrow) {
    print(base::nrow(data_frame))
  }
  df
}

#' Per-column inspection metrics
#'
#' Returns a tibble with the inspection metrics for a single column.
#' Multiple rows are produced when `class(x)` has length > 1
#' (e.g. ordered factors have `class = c("ordered", "factor")`).
#' This reproduces the historical behavior of building the result via
#' `dplyr::tibble(class = class(x), ...)` which vectorises over `class`.
#'
#' @param x A vector (data frame column).
#' @param rows The total number of rows in the parent data frame,
#'   used as the denominator for proportion metrics.
#' @return A tibble with one row per class of `x`.
#' @noRd
.inspect_column <- function(x, rows) {
  n_val_unique <- dplyr::n_distinct(x)
  nchar_values <- nchar(as.character(x))
  n_char_unique <- dplyr::n_distinct(nchar_values)

  dplyr::tibble(
    class = class(x),
    nb_distinct = n_val_unique,
    prop_distinct = nb_distinct / rows,
    nb_na = sum(is.na(x)),
    prop_na = nb_na / rows,
    nb_void = sum(x == "", na.rm = TRUE),
    prop_void = nb_void / rows,
    nchars = paste(
      unique(sort(nchar_values))[1:min(n_char_unique, 10)],
      collapse = " / "
    ),
    modalities = paste(
      sort(unique(x))[1:min(n_val_unique, 10)],
      collapse = " / "
    )
  )
}
