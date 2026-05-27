# Unit tests for the private helper .parse_mask_vars(): the centralised
# parser used by the three engines to read mask cells (vars_to_encrypt,
# vars_to_remove). The behaviour matters because the historical idiom
# (`str_split %>% unlist %>% str_trim`) turned empty cells into a
# length-1 vector containing "" or NA, which broke the downstream
# `bind_cols()` step on legitimate "copy/convert only" mask rows.


test_that(".parse_mask_vars normalises empty cells to character(0)", {
  f <- cryptRopen:::.parse_mask_vars
  expect_identical(f(NA), character(0))
  expect_identical(f(NA_character_), character(0))
  expect_identical(f(""), character(0))
  expect_identical(f("   "), character(0))
  expect_identical(f(NULL), character(0))
  expect_identical(f(character(0)), character(0))
})

test_that(".parse_mask_vars splits and trims", {
  f <- cryptRopen:::.parse_mask_vars
  expect_identical(f("a"), "a")
  expect_identical(f("a,b"), c("a", "b"))
  expect_identical(f("a, b ,  c"), c("a", "b", "c"))
})

test_that(".parse_mask_vars drops blank items inside a list", {
  f <- cryptRopen:::.parse_mask_vars
  expect_identical(f("a,,b"), c("a", "b"))
  expect_identical(f("a, ,b"), c("a", "b"))
  expect_identical(f(",a,"), "a")
})
