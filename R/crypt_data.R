#' Encrypt variables within a data frame
#'
#' This function corresponds to the
#' in-session version of the encryption process.
#' For the considered loaded dataset, variables to encrypt can be
#' in any number, following users's needs.
#'
#' Side-effect note: when `correspondence_table = TRUE`, the resulting
#' table is stored in a package-private environment (no more pollution
#' of `globalenv()`). Retrieve it with [get_correspondence_tables()].
#'
#' `crypt_data()` is **fail-fast** on an empty `vars_to_encrypt`: it
#' raises an explicit error rather than silently returning the input
#' unchanged. For a "drop columns / convert format" workflow on a
#' batch of files described by an Excel mask, use [crypt_r()] with a
#' blank `vars_to_encrypt` cell — that path is legitimate. On a single
#' in-memory object, use [dplyr::select()] / [rio::export()] directly.
#'
#' @param loaded_dataset The dataset for which user wants
#' to encrypt at least one variable.
#' It must be an expression and not a character vector.
#' @param vars_to_encrypt Character vector of variables to encrypt.
#' Must resolve to at least one non-empty entry after trimming and
#' dropping `NA` / blank values; an empty vector raises an error.
#' @param vars_to_remove Character vector of variables to remove.
#' @param encryption_key Character vector.
#' @param algorithm Algorithm to use.
#' From digest::digest()'s algo argument.
#' @param correspondence_table Logical 1L. TRUE if user wants a correspondence table
#' between initial and encrypted values.
#' @param correspondence_table_label Character 1L. Label of the correspondence table.
#' @return A data frame identical to `loaded_dataset` with the
#'   `vars_to_encrypt` columns replaced by their hashed counterparts
#'   (suffix `_crypt`) and `vars_to_remove` columns dropped. Side
#'   effect: when `correspondence_table = TRUE`, a table
#'   `tc_crypt_<correspondence_table_label>` is stored in the package-
#'   private environment — see [get_correspondence_tables()].
#' @seealso [crypt_r()], [digest::digest()], [get_correspondence_tables()].
#' @export
#'
#' @examples
#' crypt_data(
#'   loaded_dataset = mtcars[1:5, ],
#'   vars_to_encrypt = "mpg",
#'   vars_to_remove = "cyl",
#'   encryption_key = "1234567",
#'   algorithm = "md5",
#'   correspondence_table = FALSE
#' )
crypt_data <- function(loaded_dataset,
                       vars_to_encrypt,
                       vars_to_remove = NULL,
                       encryption_key,
                       algorithm = "md5",
                       correspondence_table = TRUE,
                       correspondence_table_label = NULL) {
  # First checks:
  vars <- names(loaded_dataset)

  # Fail-fast: an empty / NA / whitespace-only `vars_to_encrypt` is a
  # misuse — crypt_data() exists to encrypt. To merely drop columns or
  # convert a file, the user should use dplyr / rio directly. Note that
  # `crypt_r()` accepts the empty case (see mask-driven "copy / convert
  # only" rows handled by the engines).
  # Normalisation (trim + drop NA/empty) is applied here so the
  # membership check below runs on a clean vector and absorbs the
  # historical `str_trim()` previously sitting just above the encrypt
  # loop.
  vars_to_encrypt <- stringr::str_trim(vars_to_encrypt)
  vars_to_encrypt <- vars_to_encrypt[
    !is.na(vars_to_encrypt) & nchar(vars_to_encrypt) > 0L
  ]
  if (length(vars_to_encrypt) == 0L) {
    stop(
      "`vars_to_encrypt` must contain at least one variable name. ",
      "crypt_data() is for encryption; to merely drop columns or ",
      "convert a file, use dplyr / rio directly.",
      call. = FALSE
    )
  }

  if (!all(vars_to_encrypt %in% vars)) {
    stop("All indicated vars_to_encrypt must be effectively a variable name.")
  }

  if (!is.null(vars_to_remove) &&
    !all(vars_to_remove %in% vars)) {
    stop("All indicated vars_to_remove must be effectively a variable name.")
  }

  if (correspondence_table &&
    is.null(correspondence_table_label)) {
    stop("If the correspondence_table arg is TRUE, correspondence_table_label must be indicated.")
  }

  # Clean: trim character columns, turn blank strings into NAs.
  loaded_dataset <- loaded_dataset %>%
    dplyr::mutate(
      dplyr::across(dplyr::where(is.character), \(col) {
        col <- stringr::str_trim(col)
        col[nchar(col) == 0] <- NA
        col
      })
    )

  # Encrypt: (vars_to_encrypt has already been trimmed and validated
  # non-empty at the top of the function).
  encrypted_data <- purrr::map(
    vars_to_encrypt, \(x)
    crypt_vector(loaded_dataset[[x]],
      key = encryption_key,
      algo = algorithm
    )
  )
  encrypted_data <- do.call(what = cbind, encrypted_data)
  encrypted_data <- as.data.frame(encrypted_data)
  colnames(encrypted_data) <- paste0(vars_to_encrypt, "_crypt")

  if (any(duplicated(c(names(encrypted_data), names(loaded_dataset))))) {
    dedupl_char_values <- function(x) {
      u <- unique(x[duplicated(x)])
      l <- lapply(u, function(d) which(x == d))
      l <- stats::setNames(l, u)
      l <- lapply(l, \(v) {
        v1 <- v[2:length(v)]
        v2 <- 1:length(v1)
      })
      l <- lapply(names(l), \(n) {
        w <- which(x == n)
        w <- w[2:length(w)]
        x[w] <- paste0(x[w], "_dupl", l[[n]])
        x
      })
      l <- do.call(what = rbind, l)
      l <- apply(l, 2, \(r) {
        if (length(unique(r)) == 1) {
          unique(r)
        } else {
          r[grep("dupl", r)]
        }
      })
      l
    }

    namestot <- c(names(encrypted_data), names(loaded_dataset))
    namestot_dedupl <- dedupl_char_values(namestot)
    names(loaded_dataset) <- namestot_dedupl[
      (length(names(encrypted_data)) + 1):length(namestot_dedupl)
    ]
  }

  encrypted_data <- cbind(encrypted_data, loaded_dataset)

  # Correspondence table: stored in the package-private environment
  # (no more globalenv() pollution). Retrieve via get_correspondence_tables().
  if (correspondence_table) {
    .store_correspondence_table(
      name = paste0("tc_crypt_", correspondence_table_label),
      df = dplyr::select(
        encrypted_data,
        dplyr::all_of(c(
          vars_to_encrypt,
          paste0(vars_to_encrypt, "_crypt")
        ))
      )
    )
  }

  # Output without original variables and additional variables to remove:
  if (!is.null(vars_to_remove)) {
    encrypted_data <- dplyr::select(
      encrypted_data,
      -dplyr::all_of(c(vars_to_encrypt, vars_to_remove))
    )
  } else {
    encrypted_data <- dplyr::select(
      encrypted_data,
      -dplyr::all_of(vars_to_encrypt)
    )
  }

  encrypted_data
}
