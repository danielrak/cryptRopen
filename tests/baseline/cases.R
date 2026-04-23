# tests/baseline/cases.R
#
# Source of truth for all non-regression test cases. Sourced by:
#   - tests/baseline/generate_baseline.R  (to capture reference outputs)
#   - tests/testthat/test-baseline.R      (to re-run against refactored code)
#
# DO NOT modify case args without regenerating the baseline.


# ---------------------------------------------------------------------------
# Dataset builders (deterministic from set.seed)
# ---------------------------------------------------------------------------

build_persons_simple <- function() {
  data.frame(
    id          = sprintf("ID%03d", 1:10),
    name        = c("Alice", "Bob", "Charlie", "David", "Eve",
                    "Frank", "Grace", "Heidi", "Ivan", "Judy"),
    email       = paste0(letters[1:10], "@example.com"),
    city        = rep(c("Paris", "Lyon"), 5),
    joined_date = as.character(as.Date("2020-01-01") + 0:9),
    stringsAsFactors = FALSE
  )
}

build_persons_with_na <- function() {
  data.frame(
    id = c(sprintf("ID%03d", 1:14), NA),
    name_with_spaces = c(" Alice ", "Bob", "  Charlie", "", NA,
                         "Dave ", "Eve", "Frank", "  ", "Grace",
                         "Heidi", "Ivan", "Judy", "Karl", "Liam"),
    email = c("a@ex.com", "", NA, "d@ex.com", "e@ex.com",
              "f@ex.com", "g@ex.com", "h@ex.com", "i@ex.com", "j@ex.com",
              "k@ex.com", "l@ex.com", "m@ex.com", "n@ex.com", "o@ex.com"),
    city = c("Paris", "Lyon", "Paris", "Marseille", "Lyon",
             "Paris", "Lyon", "Marseille", "Paris", "Lyon",
             "Marseille", "Paris", "Lyon", "Marseille", "Paris"),
    stringsAsFactors = FALSE
  )
}

build_orders <- function() {
  set.seed(2026)
  n <- 5000
  data.frame(
    order_id    = sprintf("ORD%06d", seq_len(n)),
    customer_id = sprintf("CUST%04d", sample(1:500, n, replace = TRUE)),
    amount      = round(runif(n, 10, 1000), 2),
    order_date  = as.character(as.Date("2024-01-01") +
                                 sample(0:365, n, replace = TRUE)),
    stringsAsFactors = FALSE
  )
}

build_products <- function() {
  set.seed(2027)
  data.frame(
    product_id   = sprintf("P%03d", 1:50),
    product_name = paste("Product", LETTERS[sample(1:26, 50, replace = TRUE)]),
    category     = sample(c("A", "B", "C"), 50, replace = TRUE),
    stringsAsFactors = FALSE
  )
}

build_employees <- function() {
  set.seed(2028)
  data.frame(
    emp_id     = sprintf("E%04d", 1:100),
    full_name  = paste0("Emp", 1:100),
    department = sample(c("Sales", "Eng", "HR"), 100, replace = TRUE),
    manager_id = sprintf("E%04d", sample(1:10, 100, replace = TRUE)),
    stringsAsFactors = FALSE
  )
}

build_orders_large <- function() {
  # 50k-row parquet fixture introduced in Phase 1.D.5 to exercise the
  # streaming engine beyond a single scanner chunk.
  #
  # Case definition pairs this dataset with `chunk_size = 15000L`, giving
  # 4 iterations of the arrow Scanner (~3 full chunks + 1 partial),
  # so the multi-chunk paths are genuinely covered:
  #   - .transform_stream_chunk() called N times, tc_accum merged via
  #     bind_rows + distinct across iterations;
  #   - arrow::ParquetFileWriter initialised on chunk 1, WriteTable()
  #     called N times (schema conformance across chunks);
  #   - .finalize_stream_tc() + .write_stream_inspect() invoked exactly
  #     once on the accumulated state.
  #
  # Columns span the type variety that `inspect()` reports differently
  # under arrow vs rio — confirming the rio::import() relecture fix
  # from Phase 1.D.4.d holds at scale.
  set.seed(2029)
  n <- 50000L
  data.frame(
    order_id    = sprintf("ORD%08d", seq_len(n)),
    customer_id = sprintf("CUST%05d", sample(1:5000, n, replace = TRUE)),
    amount      = round(stats::runif(n, 1, 10000), 2),
    order_date  = as.character(as.Date("2020-01-01") +
                                 sample(0:1825, n, replace = TRUE)),
    city        = sample(c("Paris", "Lyon", "Marseille", "Nice",
                           "Toulouse", "Bordeaux"),
                         n, replace = TRUE),
    stringsAsFactors = FALSE
  )
}

build_special_chars <- function() {
  df <- data.frame(
    a = sprintf("X%02d", 1:20),
    b = c("Jose", "Muller", "Oyvind", "Zoe", "Chloe",
          rep("Foo", 15)),
    c = 1:20,
    stringsAsFactors = FALSE
  )
  # Column names with spaces / accents
  colnames(df) <- c("id number", "nom accentue", "valeur")
  df
}


write_all_datasets <- function(datasets_dir) {
  dir.create(datasets_dir, recursive = TRUE, showWarnings = FALSE)

  utils::write.csv(build_persons_simple(),
                   file.path(datasets_dir, "persons_simple.csv"),
                   row.names = FALSE)
  utils::write.csv(build_persons_with_na(),
                   file.path(datasets_dir, "persons_with_na.csv"),
                   row.names = FALSE, na = "")
  arrow::write_parquet(build_orders(),
                       file.path(datasets_dir, "orders.parquet"))
  arrow::write_parquet(build_orders_large(),
                       file.path(datasets_dir, "orders_large.parquet"))
  saveRDS(build_products(),
          file.path(datasets_dir, "products.rds"))
  writexl::write_xlsx(build_employees(),
                      file.path(datasets_dir, "employees.xlsx"))
  writexl::write_xlsx(build_special_chars(),
                      file.path(datasets_dir, "special_chars.xlsx"))

  invisible(NULL)
}


# ---------------------------------------------------------------------------
# Mask builders (absolute path to datasets_dir injected at call-time)
# ---------------------------------------------------------------------------

mask_row <- function(folder_path, file, to_encrypt, encrypted_file,
                     vars_to_encrypt, vars_to_remove = NA) {
  data.frame(
    folder_path     = folder_path,
    file            = file,
    to_encrypt      = to_encrypt,
    encrypted_file  = encrypted_file,
    vars_to_encrypt = vars_to_encrypt,
    vars_to_remove  = vars_to_remove,
    stringsAsFactors = FALSE
  )
}

make_mask_simple <- function(datasets_dir) {
  mask_row(datasets_dir, "persons_simple.csv", "X",
           "persons_simple_crypt.csv", "id")
}

make_mask_multi <- function(datasets_dir) {
  rbind(
    mask_row(datasets_dir, "persons_simple.csv", "X",
             "persons_simple_crypt.csv", "id"),
    mask_row(datasets_dir, "orders.parquet", "X",
             "orders_crypt.parquet", "customer_id"),
    mask_row(datasets_dir, "products.rds", "X",
             "products_crypt.rds", "product_id")
  )
}

make_mask_with_skipped <- function(datasets_dir) {
  rbind(
    mask_row(datasets_dir, "persons_simple.csv", "X",
             "persons_simple_crypt.csv", "id"),
    # Blank line (all NA) — should be filtered by filter_all(any_vars(!is.na(.)))
    mask_row(NA, NA, NA, NA, NA, NA),
    # to_encrypt != "X" — should be skipped
    mask_row(datasets_dir, "products.rds", "N",
             "products_crypt.rds", "product_id"),
    mask_row(datasets_dir, "persons_with_na.csv", "X",
             "persons_with_na_crypt.csv", "id")
  )
}

make_mask_dupl_files <- function(datasets_dir) {
  rbind(
    mask_row(datasets_dir, "persons_simple.csv", "X",
             "out_crypt.csv", "id"),
    mask_row(datasets_dir, "persons_with_na.csv", "X",
             "out_crypt.csv", "id")
  )
}

make_mask_multi_vars <- function(datasets_dir) {
  mask_row(datasets_dir, "persons_simple.csv", "X",
           "persons_simple_crypt.csv", "id, email")
}

make_mask_with_remove <- function(datasets_dir) {
  rbind(
    mask_row(datasets_dir, "persons_simple.csv", "X",
             "persons_simple_crypt.csv", "id", vars_to_remove = "city"),
    mask_row(datasets_dir, "products.rds", "X",
             "products_crypt.rds", "product_id", vars_to_remove = NA)
  )
}

make_mask_xlsx_input <- function(datasets_dir) {
  mask_row(datasets_dir, "employees.xlsx", "X",
           "employees_crypt.xlsx", "emp_id")
}

make_mask_parquet_input <- function(datasets_dir) {
  mask_row(datasets_dir, "orders.parquet", "X",
           "orders_crypt.parquet", "customer_id")
}

make_mask_rds_input <- function(datasets_dir) {
  mask_row(datasets_dir, "products.rds", "X",
           "products_crypt.rds", "product_id")
}

make_mask_special_chars <- function(datasets_dir) {
  mask_row(datasets_dir, "special_chars.xlsx", "X",
           "special_chars_crypt.xlsx", "id number")
}

make_mask_large_parquet <- function(datasets_dir) {
  mask_row(datasets_dir, "orders_large.parquet", "X",
           "orders_large_crypt.parquet", "customer_id")
}


write_all_masks <- function(masks_dir, datasets_dir) {
  dir.create(masks_dir, recursive = TRUE, showWarnings = FALSE)
  builders <- list(
    mask_simple         = make_mask_simple,
    mask_multi          = make_mask_multi,
    mask_with_skipped   = make_mask_with_skipped,
    mask_dupl_files     = make_mask_dupl_files,
    mask_multi_vars     = make_mask_multi_vars,
    mask_with_remove    = make_mask_with_remove,
    mask_xlsx_input     = make_mask_xlsx_input,
    mask_parquet_input  = make_mask_parquet_input,
    mask_rds_input      = make_mask_rds_input,
    mask_special_chars  = make_mask_special_chars,
    mask_large_parquet  = make_mask_large_parquet
  )
  for (nm in names(builders)) {
    writexl::write_xlsx(builders[[nm]](datasets_dir),
                        file.path(masks_dir, paste0(nm, ".xlsx")))
  }
  invisible(names(builders))
}


# ---------------------------------------------------------------------------
# Case definitions
# ---------------------------------------------------------------------------

crypt_vector_cases <- list(
  list(
    name = "ascii_simple",
    args = list(vector = c("1234", "5678", "9101112"),
                key = "key_abc", algo = "md5")
  ),
  list(
    name = "with_na_and_empty",
    args = list(vector = c("1234", NA_character_, "", "5678"),
                key = "key_abc", algo = "md5")
  ),
  list(
    name = "leading_trailing_spaces",
    args = list(vector = c("  foo  ", "bar  ", "  baz"),
                key = "key_abc", algo = "md5")
  ),
  list(
    name = "internal_spaces",
    args = list(vector = c("hello world", "a b c d", "no_spaces"),
                key = "key_abc", algo = "md5")
  ),
  list(
    name = "mixed_case",
    args = list(vector = c("Hello", "WORLD", "miXeD"),
                key = "key_abc", algo = "md5")
  ),
  list(
    name = "unicode",
    args = list(vector = c("cafe", "naive", "Zoe", "Muller"),
                key = "key_abc", algo = "md5")
  ),
  list(
    name = "numeric_vector",
    args = list(vector = c(1234, 5678, 9101112),
                key = "key_abc", algo = "md5")
  ),
  list(
    name = "date_vector",
    args = list(vector = as.Date(c("2024-01-01", "2024-06-15", "2025-12-31")),
                key = "key_abc", algo = "md5")
  ),
  list(
    name = "length_one",
    args = list(vector = "singleton", key = "key_abc", algo = "md5")
  ),
  list(
    name = "algo_sha1",
    args = list(vector = c("abc", "def", "ghi"),
                key = "key_abc", algo = "sha1")
  ),
  list(
    name = "algo_sha256",
    args = list(vector = c("abc", "def", "ghi"),
                key = "key_abc", algo = "sha256")
  ),
  list(
    name = "special_key",
    args = list(vector = c("abc", "def"),
                key = "k3y!@#$%&*()", algo = "md5")
  )
)


# For crypt_data, we use args_factory (function returning list) because some
# args (e.g. mtcars subsets) are better built inside the script than embedded.
crypt_data_cases <- list(
  list(
    name = "readme_example_no_tc",
    args_factory = function() list(
      loaded_dataset = mtcars[1:5, ],
      vars_to_encrypt = "mpg",
      vars_to_remove = "cyl",
      encryption_key = "1234567",
      algorithm = "md5",
      correspondence_table = FALSE
    )
  ),
  list(
    name = "with_tc",
    args_factory = function() list(
      loaded_dataset = mtcars[1:5, ],
      vars_to_encrypt = "mpg",
      vars_to_remove = NULL,
      encryption_key = "1234567",
      algorithm = "md5",
      correspondence_table = TRUE,
      correspondence_table_label = "readme_mtcars"
    )
  ),
  list(
    name = "multi_vars",
    args_factory = function() list(
      loaded_dataset = data.frame(
        id     = sprintf("ID%03d", 1:10),
        email  = paste0(letters[1:10], "@ex.com"),
        name   = letters[1:10],
        amount = 1:10,
        stringsAsFactors = FALSE
      ),
      vars_to_encrypt = c("id", "email"),
      vars_to_remove = NULL,
      encryption_key = "testkey",
      algorithm = "md5",
      correspondence_table = FALSE
    )
  ),
  list(
    name = "vars_to_remove_null",
    args_factory = function() list(
      loaded_dataset = mtcars[1:5, ],
      vars_to_encrypt = "mpg",
      vars_to_remove = NULL,
      encryption_key = "k",
      algorithm = "md5",
      correspondence_table = FALSE
    )
  ),
  list(
    name = "dedup_name_collision",
    args_factory = function() {
      # A column literally named "mpg_crypt" already exists in loaded_dataset:
      # crypt_data will create a second "mpg_crypt" → dedupl path triggered.
      df <- mtcars[1:5, ]
      df$mpg_crypt <- paste0("PREEXISTING_", seq_len(nrow(df)))
      list(
        loaded_dataset = df,
        vars_to_encrypt = "mpg",
        vars_to_remove = NULL,
        encryption_key = "k",
        algorithm = "md5",
        correspondence_table = FALSE
      )
    }
  ),
  list(
    name = "trim_and_na_chars",
    args_factory = function() list(
      loaded_dataset = data.frame(
        id = c(" A1 ", "  ", "", NA, "A2", "  A3"),
        v  = 1:6,
        stringsAsFactors = FALSE
      ),
      vars_to_encrypt = "id",
      vars_to_remove = NULL,
      encryption_key = "k",
      algorithm = "md5",
      correspondence_table = FALSE
    )
  )
)


# For crypt_r, cases reference a mask file name (built by write_all_masks)
# and carry the param values passed to crypt_r() at call time.
crypt_r_cases <- list(
  list(name = "simple_csv",
       mask = "mask_simple.xlsx",            correspondence_table = TRUE),
  list(name = "simple_csv_no_tc",
       mask = "mask_simple.xlsx",            correspondence_table = FALSE),
  list(name = "multi_files",
       mask = "mask_multi.xlsx",             correspondence_table = TRUE),
  list(name = "with_skipped_lines",
       mask = "mask_with_skipped.xlsx",      correspondence_table = TRUE),
  list(name = "dupl_files",
       mask = "mask_dupl_files.xlsx",        correspondence_table = TRUE),
  list(name = "multi_vars_per_file",
       mask = "mask_multi_vars.xlsx",        correspondence_table = TRUE),
  list(name = "with_remove",
       mask = "mask_with_remove.xlsx",       correspondence_table = TRUE),
  list(name = "xlsx_input",
       mask = "mask_xlsx_input.xlsx",        correspondence_table = TRUE),
  list(name = "parquet_input",
       mask = "mask_parquet_input.xlsx",     correspondence_table = TRUE),
  list(name = "rds_input",
       mask = "mask_rds_input.xlsx",         correspondence_table = TRUE),
  list(name = "special_chars_cols",
       mask = "mask_special_chars.xlsx",     correspondence_table = TRUE),

  # Phase 1.D.5 — large parquet case exercising multi-chunk streaming.
  # chunk_size = 15000 on a 50k-row input triggers ~4 scanner iterations,
  # forcing real accumulation in .transform_stream_chunk() / tc_accum and
  # repeated WriteTable() calls on the same ParquetFileWriter.
  list(name = "large_parquet_multichunk",
       mask = "mask_large_parquet.xlsx",     correspondence_table = TRUE,
       chunk_size = 15000L)
)

CRYPT_R_KEY <- "baseline_key_2026"
CRYPT_R_ALGO <- "md5"


# ---------------------------------------------------------------------------
# Synchronous patch for crypt_r()
#
# `crypt_r()` dispatches per-row work asynchronously — pre-1.D.6.b via
# `job::job()`, post-1.D.6.b via `mirai::mirai()` + owned `mirai::daemons()`.
# Running dispatched work in a subprocess is great in production but
# incompatible with the baseline harness: (a) we want to assert on files
# immediately after `crypt_r()` returns, and (b) spinning up mirai daemons
# per case would make the baseline suite slow and fragile in dev sessions
# where `cryptRopen` is pkgload'd and not installed in the daemon's library
# paths.
#
# We therefore rewrite `body(cryptRopen::crypt_r)` itself so that every
# async wrapper is neutralised:
#
#   - `job::job(BODY, title = ...)`  -> BODY         (recursively patched)
#   - `job::export(...)`             -> invisible(NULL)
#   - `mirai::mirai(BODY, ...)`      -> BODY         (recursively patched;
#                                        named mirai args are already bound
#                                        in the enclosing `purrr::map()`
#                                        lambda, so BODY resolves them
#                                        locally once inlined)
#   - any other `mirai::*` call      -> invisible(NULL)  (status/daemons/
#                                        everywhere/... become no-ops; the
#                                        surrounding daemons-ownership book-
#                                        keeping still runs but has nothing
#                                        to teardown)
#
# `.new_cryptR_job(...)` is intentionally NOT stripped — after the inline
# synchronous execution, it builds a `cryptR_job` carrying a list of NULLs
# (whatever `.process_mask_row()` returned). Baseline callers discard the
# return value so this is harmless and keeps the post-refactor contract
# (crypt_r always returns a cryptR_job) visible to the tests that do want
# to see it.
#
# Returns a restorer function; call it to put the original body back.
#
# Safe to call no matter which async wrapper is in use — unknown nodes
# pass through unchanged.
# ---------------------------------------------------------------------------

install_sync_crypt_r_patch <- function() {
  ns <- asNamespace("cryptRopen")
  original <- get("crypt_r", envir = ns)

  rewrite <- function(expr) {
    if (is.call(expr)) {
      fn <- expr[[1L]]
      if (is.call(fn) && identical(fn[[1L]], as.name("::"))) {
        pkg_name <- as.character(fn[[2L]])
        fun_name <- as.character(fn[[3L]])
        if (identical(pkg_name, "job")) {
          if (identical(fun_name, "job")) {
            return(rewrite(expr[[2L]]))
          }
          if (identical(fun_name, "export")) {
            return(quote(invisible(NULL)))
          }
        }
        if (identical(pkg_name, "mirai")) {
          if (identical(fun_name, "mirai")) {
            # First positional arg is the task expression; strip the wrapper
            # and keep only that expression, recursively patched so a nested
            # mirai:: call (shouldn't happen, but defensive) is also reduced.
            return(rewrite(expr[[2L]]))
          }
          # daemons / everywhere / status / call_mirai / unresolved /
          # is_error_value / ... — all neutralised to invisible(NULL).
          return(quote(invisible(NULL)))
        }
      }
      # Bare `.new_cryptR_job(...)` calls — replace with invisible(NULL) so
      # the patched crypt_r() returns NULL instead of trying to wrap a list
      # of non-mirai values (what remains of `tasks` after `mirai::mirai()`
      # has been inlined to synchronous evaluation) into a `cryptR_job`.
      # `run_crypt_r_case()` in test-baseline.R detects NULL via the
      # `inherits(result, "cryptR_job")` guard and skips `cryptR_wait()`.
      if (is.symbol(fn) &&
          identical(as.character(fn), ".new_cryptR_job")) {
        return(quote(invisible(NULL)))
      }
      for (i in seq_along(expr)) {
        expr[[i]] <- rewrite(expr[[i]])
      }
    }
    expr
  }

  new_fn <- original
  body(new_fn) <- rewrite(body(original))

  rebind <- function(env, name, value) {
    if (!exists(name, envir = env, inherits = FALSE)) return(invisible())
    if (bindingIsLocked(name, env)) unlockBinding(name, env)
    assign(name, value, envir = env)
    lockBinding(name, env)
  }

  # Assigning to only one env is fragile: pkgload/devtools/testthat create
  # additional "shim" envs that hold their own reference to `crypt_r`, and
  # `body(f) <- ...` always copies the function (other references keep the
  # original body). We exhaustively rebind in every env we can reach:
  #   - the cryptRopen namespace itself
  #   - every attached env on search()
  #   - every loaded namespace (and its imports parent) that re-exports crypt_r
  collect_targets <- function() {
    targets <- list()
    add <- function(env) {
      if (is.environment(env) &&
          exists("crypt_r", envir = env, inherits = FALSE)) {
        targets[[length(targets) + 1L]] <<- env
      }
    }
    add(ns)
    for (nm in search()) add(as.environment(nm))
    for (ns_name in loadedNamespaces()) {
      nse <- asNamespace(ns_name)
      add(nse)
      add(parent.env(nse))
    }
    targets
  }

  targets <- collect_targets()
  for (env in targets) rebind(env, "crypt_r", new_fn)

  function() {
    # Re-collect at restore time in case new envs were created meanwhile.
    for (env in collect_targets()) rebind(env, "crypt_r", original)
  }
}
