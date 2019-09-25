test_fun.aggregate <- function(fun, value.var, fill, na.rm,
                               input_rows_in_output_col,
                               map_input_rows_to_output_rows) {

  if (missing(input_rows_in_output_col)) {
    input_rows_in_output_col <- seq_along(value.var)[sample.int(length(value.var), size = sample.int(length(value.var)))]
  }

  if (missing(map_input_rows_to_output_rows)) {
    map_input_rows_to_output_rows <- sample.int(length(value.var), replace = TRUE)
  }

  agg <- lfdcast:::fun.aggregates[fun]
  res <- data.frame(rep(fill, length(value.var)))

  res <- .Call("test_fun_aggregate", agg, res, value.var, na.rm,
        as.integer(input_rows_in_output_col) - 1L,
        as.integer(map_input_rows_to_output_rows) - 1L, PACKAGE = "lfdcast")

  res2 <- data.frame(rep(fill, length(value.var)))

  if (na.rm) {
    input_rows_in_output_col <- input_rows_in_output_col[!is.na(value.var[input_rows_in_output_col])]
  }
  map_input_rows_to_output_rows <- map_input_rows_to_output_rows[input_rows_in_output_col]
  value.var <- value.var[input_rows_in_output_col]

  for (i_output in unique(map_input_rows_to_output_rows)) {
    switch(fun,
      count = res2[i_output, 1] <- sum(map_input_rows_to_output_rows == i_output),
      existence = res2[i_output, 1] <- TRUE,
      sum = res2[i_output, 1] <- sum(value.var[map_input_rows_to_output_rows == i_output]),
      max = res2[i_output, 1] <- min(value.var[map_input_rows_to_output_rows == i_output]),
      uniqueN = res2[i_output, 1] <- data.table::uniqueN(value.var[map_input_rows_to_output_rows == i_output]),
      last = res2[i_output, 1] <- data.table::last(value.var[map_input_rows_to_output_rows == i_output]),
      any = res2[i_output, 1] <- any(value.var[map_input_rows_to_output_rows == i_output]),
      all = res2[i_output, 1] <- all(value.var[map_input_rows_to_output_rows == i_output]),
      mean = res2[i_output, 1] <- mean(value.var[map_input_rows_to_output_rows == i_output]),
      median = res2[i_output, 1] <- median(value.var[map_input_rows_to_output_rows == i_output])
    )
  }

  if (!(isTRUE(all.equal(res, res2)))) browser()
  stopifnot(isTRUE(all.equal(res, res2)))
}

value.var_char <- lapply(sample.int(5, 1000, replace = TRUE),
                         function(size) sample(c(letters[1:4], NA_character_), size = size, replace = TRUE))
value.var_int <- lapply(sample.int(5, 1000, replace = TRUE),
                        function(size) sample(c(1:4, NA_integer_), size = size, replace = TRUE))
value.var_real <- lapply(sample.int(5, 1000, replace = TRUE),
                        function(size) sample(c(1, -Inf, Inf, NA_real_, NaN, 0, -0, 0/0), size = size, replace = TRUE))
value.var_lgl <- lapply(sample.int(5, 1000, replace = TRUE),
                         function(size) sample(c(TRUE, FALSE, NA), size = size, replace = TRUE))

# count ----
value.var <- c(value.var_char, value.var_int, value.var_real, value.var_lgl)
names(value.var) <- seq_along(value.var)

test_args_list <- list(
  fun = list(count = "count"),
  value.var = value.var,
  fill = list(int = -6L, NA_int = NA_integer_),
  na.rm = list(T = TRUE, F = FALSE)
)

set.seed(123)
fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
#stopifnot(length(unlist(lapply(lapply(fr, `[[`, i = 1), `[[`, "error"))) == 0L)

fr <- as.data.frame(fr)
stopifnot(all(is.na(fr$error)))


# all ----
value.var <- c(value.var_int, value.var_lgl)
names(value.var) <- seq_along(value.var)

test_args_list <- list(
  fun = list(all = "all"),
  value.var = value.var,
  fill = list(T = TRUE, F = FALSE, "NA" = NA),
  na.rm = list(T = TRUE, F = FALSE)
)

set.seed(123)
fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
# stopifnot(length(unlist(lapply(lapply(fr, `[[`, i = 1), `[[`, "error"))) == 0L)

fr <- as.data.frame(fr)
stopifnot(all(is.na(fr$error)))


# existence ----
value.var <- c(value.var_char, value.var_int, value.var_real, value.var_lgl)
names(value.var) <- seq_along(value.var)

test_args_list <- list(
  fun = list(count = "existence"),
  value.var = value.var,
  fill = list(T = TRUE, F = FALSE, "NA" = NA),
  na.rm = list(T = TRUE, F = FALSE)
)

set.seed(123)
fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
# stopifnot(length(unlist(lapply(lapply(fr, `[[`, i = 1), `[[`, "error"))) == 0L)

fr <- as.data.frame(fr)
stopifnot(all(is.na(fr$error)))
F__(1)

# uniqueN ----
value.var <- c(value.var_char, value.var_int, value.var_real, value.var_lgl)
names(value.var) <- seq_along(value.var)

test_args_list <- list(
  fun = list(count = "uniqueN"),
  value.var = value.var,
  fill = list(int = 5L, NA_int = NA_integer_),
  na.rm = list(T = TRUE, F = FALSE)
)

set.seed(123)
fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
# stopifnot(length(unlist(lapply(lapply(fr, `[[`, i = 1), `[[`, "error"))) == 0L)

fr <- as.data.frame(fr)
stopifnot(all(is.na(fr$error)))
