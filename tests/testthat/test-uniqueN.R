context("uniqueN")

test_uniqueN <- function(x, na.rm) {
  stopifnot(isTRUE(all.equal(lfdcast::uniqueN(x, na.rm), data.table::uniqueN(x, na.rm = na.rm))))
}

value.var_char <- lapply(sample.int(5, 1000, replace = TRUE),
                         function(size) sample(c(letters[1:4], NA_character_, ""), size = size, replace = TRUE))
value.var_int <- lapply(sample.int(5, 1000, replace = TRUE),
                        function(size) sample(c(1:4, NA_integer_, -.Machine$integer.max, .Machine$integer.max), size = size, replace = TRUE))
value.var_real <- lapply(sample.int(5, 1000, replace = TRUE),
                         function(size) sample(c(-99, 1, -Inf, Inf, NA_real_, NaN, 0, -0, 0/0, -NaN, -NA_real_), size = size, replace = TRUE))
value.var_lgl <- lapply(sample.int(5, 1000, replace = TRUE),
                        function(size) sample(c(TRUE, FALSE, NA), size = size, replace = TRUE))

test_that("guniqueN", {
  # uniqueN ----
  value.var <- c(value.var_char, value.var_int, value.var_real, value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    x = value.var,
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_uniqueN, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})
