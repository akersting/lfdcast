tests2skip <- c("glength", "glength_gt0", "gall", "gany", "guniqueN",
                "gfirst", "glast", "gmedian", "gmean", "gmin", "gmax", "gsum",
                "gsample", "glist")
tests2skip <- character()

n <- 100
value.var_char <- lapply(sample.int(5, n, replace = TRUE),
                         function(size) sample(c(letters[1:4], NA_character_, ""), size = size, replace = TRUE))
value.var_int <- lapply(sample.int(5, n, replace = TRUE),
                        function(size) sample(c(1:4, NA_integer_, -.Machine$integer.max, .Machine$integer.max), size = size, replace = TRUE))
value.var_real <- lapply(sample.int(5, n, replace = TRUE),
                         function(size) sample(c(-99, 1, -Inf, Inf, NA_real_, NaN, 0, -0, 0/0, -NaN, -NA_real_), size = size, replace = TRUE))
value.var_lgl <- lapply(sample.int(5, n, replace = TRUE),
                        function(size) sample(c(TRUE, FALSE, NA), size = size, replace = TRUE))


test_fun.aggregate <- function(fun, value.var, fill, na.rm,
                               input_rows_in_output_col,
                               map_input_rows_to_output_rows) {

  if (missing(input_rows_in_output_col)) {
    input_rows_in_output_col <- sort(seq_along(value.var)[sample.int(length(value.var), size = sample.int(length(value.var), 1))])
  }

  if (missing(map_input_rows_to_output_rows)) {
    map_input_rows_to_output_rows <- sample.int(length(value.var), replace = TRUE)
  }

  agg <- lfdcast:::fun.aggregates[fun]

  res <- data.table::data.table(rep(fill, length(value.var)))
  res <- .Call("test_fun_aggregate", agg, res, value.var, na.rm,
               as.integer(input_rows_in_output_col) - 1L,
               as.integer(map_input_rows_to_output_rows) - 1L, PACKAGE = "lfdcast")


  res2 <- data.table::data.table(rep(fill, length(value.var)))

  if (na.rm) {
    input_rows_in_output_col <- input_rows_in_output_col[!is.na(value.var[input_rows_in_output_col])]
  }
  map_input_rows_to_output_rows <- map_input_rows_to_output_rows[input_rows_in_output_col]
  value.var <- value.var[input_rows_in_output_col]

  for (i_output in unique(map_input_rows_to_output_rows)) {
    switch(fun,
           glength = res2[i_output, 1] <- sum(map_input_rows_to_output_rows == i_output),
           glength_gt0 = res2[i_output, 1] <- TRUE,
           gsum = res2[i_output, 1] <- sum(value.var[map_input_rows_to_output_rows == i_output]),
           gmax = res2[i_output, 1] <- max(value.var[map_input_rows_to_output_rows == i_output]),
           gmin = res2[i_output, 1] <- min(value.var[map_input_rows_to_output_rows == i_output]),
           guniqueN = res2[i_output, 1] <- data.table::uniqueN(value.var[map_input_rows_to_output_rows == i_output]),
           gfirst = res2[i_output, 1] <- data.table::first(value.var[map_input_rows_to_output_rows == i_output]),
           glast = res2[i_output, 1] <- data.table::last(value.var[map_input_rows_to_output_rows == i_output]),
           gany = res2[i_output, 1] <- any(value.var[map_input_rows_to_output_rows == i_output]),
           gall = res2[i_output, 1] <- all(value.var[map_input_rows_to_output_rows == i_output]),
           gmean = res2[i_output, 1] <- mean(value.var[map_input_rows_to_output_rows == i_output]),
           gmedian = res2[i_output, 1] <- median(value.var[map_input_rows_to_output_rows == i_output]),
           glist = {
             r <- res2[[1]]
             r[i_output] <- list(value.var[map_input_rows_to_output_rows == i_output])
             res2[[1]] <- r
           }
    )
  }

  if (fun != "gsample") {
    if (!isTRUE(all.equal(res, res2))) stop(all.equal(res, res2))
  } else {
    sample_OK <- all(unlist(lapply(unique(map_input_rows_to_output_rows),
                                   function(o_row) res[[1]][o_row] %in% value.var[map_input_rows_to_output_rows == o_row]))) &&
      all(res[[1]] %in% c(value.var, fill))
    stopifnot(sample_OK)

  }
}

test_that("glength", {
  if ("glength" %in% tests2skip) skip("glength")
  skip_if_not_installed("fuzzr")
  # length ----
  value.var <- c(value.var_char, value.var_int, value.var_real, value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "glength"),
    value.var = value.var,
    fill = list(int = -6L, NA_int = NA_integer_),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})


test_that("glength_gt0", {
  if ("glength_gt0" %in% tests2skip)  skip("glength_gt0")
  skip_if_not_installed("fuzzr")
  # length_gt0 ----
  value.var <- c(value.var_char, value.var_int, value.var_real, value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "glength_gt0"),
    value.var = value.var,
    fill = list(F = FALSE, NA_lgs = NA),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})

test_that("gall", {
  if ("gall" %in% tests2skip) skip("gall")
  skip_if_not_installed("fuzzr")
  # all ----
  value.var <- c(value.var_int, value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(all = "gall"),
    value.var = value.var,
    fill = list(T = TRUE, F = FALSE, "NA" = NA),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})

test_that("gany", {
  if ("gany" %in% tests2skip) skip("gany")
  skip_if_not_installed("fuzzr")
  # any ----
  value.var <- c(value.var_int, value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(all = "gany"),
    value.var = value.var,
    fill = list(T = TRUE, F = FALSE, "NA" = NA),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})

test_that("guniqueN", {
  if ("guniqueN" %in% tests2skip) skip("guniqueN")
  skip_if_not_installed("fuzzr")
  # uniqueN ----
  value.var <- c(value.var_char, value.var_int, value.var_real, value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "guniqueN"),
    value.var = value.var,
    fill = list(int = 5L, NA_int = NA_integer_),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})

test_that("gfirst", {
  if ("gfirst" %in% tests2skip) skip("gfirst")
  skip_if_not_installed("fuzzr")
  # first ----
  value.var <- c(value.var_char)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "gfirst"),
    value.var = value.var,
    fill = list(d = "def"),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))



  value.var <- c(value.var_int)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "gfirst"),
    value.var = value.var,
    fill = list(int = 7L, NA_int = NA_integer_),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))


  value.var <- c(value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "gfirst"),
    value.var = value.var,
    fill = list(lgl = TRUE, NA_lglt = NA),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))


  value.var <- c(value.var_real)
  names(value.var) <- seq_along(value.var)
  test_args_list <- list(
    fun = list(count = "gfirst"),
    value.var = value.var,
    fill = list(real = 7, NA_real = NA_real_),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})

test_that("glast", {
  if ("glast" %in% tests2skip) skip("glast")
  skip_if_not_installed("fuzzr")
  # last ----
  value.var <- c(value.var_char)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "glast"),
    value.var = value.var,
    fill = list(d = "def"),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))



  value.var <- c(value.var_int)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "glast"),
    value.var = value.var,
    fill = list(int = 7L, NA_int = NA_integer_),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))


  value.var <- c(value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "glast"),
    value.var = value.var,
    fill = list(lgl = TRUE, NA_lglt = NA),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))


  value.var <- c(value.var_real)
  names(value.var) <- seq_along(value.var)
  test_args_list <- list(
    fun = list(count = "glast"),
    value.var = value.var,
    fill = list(real = 7, NA_real = NA_real_),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})


test_that("gmedian", {
  if ("gmedian" %in% tests2skip) skip("gmedian")
  skip_if_not_installed("fuzzr")
  # median ----
  value.var <- c(value.var_int, value.var_lgl, value.var_real)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(all = "gmedian"),
    value.var = value.var,
    fill = list(NA_real = NA_real_, real = 5, nreal = -10),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})


test_that("gmean", {
  if ("gmean" %in% tests2skip) skip("gmean")
  skip_if_not_installed("fuzzr")
  # mean ----
  value.var <- c(value.var_int, value.var_lgl, value.var_real)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(all = "gmean"),
    value.var = value.var,
    fill = list(NA_real = NA_real_, real = 5, nreal = -10, zero = 0),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})


test_that("gmin", {
  if ("gmin" %in% tests2skip) skip("gmin")
  skip_if_not_installed("fuzzr")
  # min ----
  value.var <- c(value.var_int, value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(all = "gmin"),
    value.var = value.var,
    fill = list(NA_int = NA_integer_, int = 5L, nint = -10L),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))



  value.var <- c(value.var_int, value.var_lgl, value.var_real)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(all = "gmin"),
    value.var = value.var,
    fill = list(NA_real = NA_real_, real = 5, nreal = -10, ninf = -Inf, inf = Inf),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})



test_that("gmax", {
  if ("gmax" %in% tests2skip) skip("gmax")
  skip_if_not_installed("fuzzr")
  # max ----
  value.var <- c(value.var_int, value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(all = "gmax"),
    value.var = value.var,
    fill = list(NA_int = NA_integer_, int = 5L, nint = -10L),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))



  value.var <- c(value.var_int, value.var_lgl, value.var_real)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(all = "gmax"),
    value.var = value.var,
    fill = list(NA_real = NA_real_, real = 5, nreal = -10, ninf = -Inf, inf = Inf),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})



test_that("gsum", {
  if ("gsum" %in% tests2skip) skip("gsum")
  skip_if_not_installed("fuzzr")
  # sum ----
  value.var <- c(value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(all = "gsum"),
    value.var = value.var,
    fill = list(NA_int = NA_integer_, int = 5L, nint = -10L, zero = 0L),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))



  value.var <- c(value.var_int, value.var_real)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(all = "gsum"),
    value.var = value.var,
    fill = list(NA_real = NA_real_, real = 5, nreal = -10, ninf = -Inf, inf = Inf, zero = 0),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})


test_that("gsample", {
  if ("gsample" %in% tests2skip)  skip("gsample")
  skip_if_not_installed("fuzzr")
  # sample ----
  value.var <- c(value.var_char)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "gsample"),
    value.var = value.var,
    fill = list(d = "def"),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))



  value.var <- c(value.var_int)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "gsample"),
    value.var = value.var,
    fill = list(int = 7L, NA_int = NA_integer_),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))


  value.var <- c(value.var_lgl)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "gsample"),
    value.var = value.var,
    fill = list(lgl = TRUE, NA_lglt = NA),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))


  value.var <- c(value.var_real)
  names(value.var) <- seq_along(value.var)
  test_args_list <- list(
    fun = list(count = "gsample"),
    value.var = value.var,
    fill = list(real = 7, NA_real = NA_real_),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})


test_that("glist", {
  if ("glist" %in% tests2skip) skip("glist")
  skip_if_not_installed("fuzzr")
  # list ----
  value.var <- c(value.var_char, value.var_int, value.var_lgl, value.var_real)
  names(value.var) <- seq_along(value.var)

  test_args_list <- list(
    fun = list(count = "glist"),
    value.var = value.var,
    fill = list(char = list(c("d", "e", "f", "ault")), int = list(123L),
                real = list(c(321, 123)), lgl = list(c(TRUE, FALSE, NA)),
                null = list(NULL)),
    na.rm = list(T = TRUE, F = FALSE)
  )

  set.seed(123)
  fr <- fuzzr::p_fuzz_function(test_fun.aggregate, test_args_list)
  fr <- as.data.frame(fr)
  expect_true(all(is.na(fr$error)))
})
