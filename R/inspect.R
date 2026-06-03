#' Profile a Data Frame
#'
#' Returns a one-row-per-column profile of a data frame: class, number
#' and proportion of distinct values, number and proportion of missing
#' values, number and proportion of empty strings, length range of the
#' character representation, and the first ten unique modalities.
#'
#' Columns whose `class()` has length > 1 (e.g. ordered factors with
#' `class = c("ordered", "factor")`) produce one row per class entry —
#' this mirrors the historical behavior built on
#' `tibble(class = class(x), ...)`.
#'
#' @param data_frame A data frame.
#' @param nrow Logical scalar. If `TRUE`, the number of rows of
#'   `data_frame` is emitted on stderr via [message()] in addition to
#'   the returned tibble. Defaults to `FALSE`. Note that this parameter
#'   shadows the base function [base::nrow()] inside the function body;
#'   callers passing a positional argument should keep `data_frame`
#'   first.
#' @return A tibble with one row per column of `data_frame` (or several
#'   when `class(column)` has length > 1), holding the per-column
#'   inspection metrics described above.
#' @export
#'
#' @examples
#' # One row per column of CO2, with the distinct / NA / empty
#' # counts and the first ten unique modalities.
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
    message("Number of observations: ", base::nrow(data_frame))
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
