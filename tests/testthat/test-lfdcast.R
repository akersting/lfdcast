# test_that("lfdcast produces same results as data.table::dcast", {
#   n <- 1e4
#
#   X <- data.table::data.table(
#     int = sample(c(1:10, NA_integer_), n, replace = TRUE),
#     num = sample(c(1:10000, NA_real_, NaN, -Inf, Inf), n, replace = TRUE),
#     char = sample(c(1:10, NA_character_), n, replace = TRUE),
#     bool = sample(c(TRUE, FALSE, NA), n, replace = TRUE)
#   )
#
#   A <- dcast(X, "int", num = make_cast_args("char", fun.aggregate = c(uniqueN = "uniqueN", min = "min", max = "max", sum = "sum", length = "count", "function" = "existence"), value.var = c("num", "num", "num", "num", "num", "num")))
#   B <- data.table::dcast(data.table::as.data.table(X), int ~ char, fun.aggregate = list(uniqueN, min, max, sum, length, function(x) length(x) > 0L), value.var = c("num", "num", "num", "num", "num", "num"))
#
#   expect_equivalent(
#     lfdcast(X, c("a", "b"), c("c", "d")),
#     data.table:::dcast.data.table(data.table::as.data.table(X), a + b ~ c + d, fun.aggregate = length)
#   )
# })

library(data.table)
test_that("lfdcast produces same results as data.table by", {
  iris <- as.data.table(iris)
  X <- lfdcast::dcast(iris, "Species", agg(NULL,
                                           S = gsum(Petal.Length),
                                           M = gmean(Sepal.Length)))
  assignInNamespace("cedta.override",
                    "lfdcast",
                    "data.table")
  Y <- iris[, .(S = sum(Petal.Length), M = mean(Sepal.Length)), keyby = "Species"]
  assignInNamespace("cedta.override",
                    NULL,
                    "data.table")
  expect_identical(X, Y)
})

test_that("lfdcast produces same results as data.table::dcast", {
  iris <- as.data.table(iris)
  X <- lfdcast::dcast(iris, "Species", agg("Petal.Width",
                                           Petal.Length_sum = gsum(Petal.Length),
                                           Sepal.Length_mean = gmean(Sepal.Length),
                                           names.fun.args = list(prefix.with.colname = FALSE)))
  Y <- data.table::dcast(iris, Species ~ Petal.Width,
                         fun.aggregate = list(sum, mean),
                         value.var = list("Petal.Length", "Sepal.Length"),
                         drop = TRUE)
  expect_identical(X, Y)


  iris[["Species"]] <- as.character(iris[["Species"]])
  X <- lfdcast::dcast(iris, "Species", agg("Petal.Width",
                                           Petal.Length_sum = gsum(Petal.Length),
                                           Sepal.Length_mean = gmean(Sepal.Length),
                                           Species.1_last = glast(Species),
                                           names.fun.args = list(prefix.with.colname = FALSE)))
  Y <- data.table::dcast(iris, Species ~ Petal.Width,
                         fun.aggregate = list(sum, mean, last),
                         value.var = list("Petal.Length", "Sepal.Length", "Species"),
                         drop = TRUE)
  expect_identical(X, Y)
})

test_that("special features of lfdcast work as expected", {
  iris[["Species"]] <- as.character(iris[["Species"]])
  X <- lfdcast::dcast(iris, "Species", agg("Petal.Width",
                                           Petal.Length_sum = gsum(Petal.Length^2, fill = -Inf) > 30,
                                           Sepal.Length_mean = gmean(Sepal.Length + Sepal.Width, fill = NA),
                                           Species.1_last = toupper(glast(Species, fill = "")),
                                           names.fun.args = list(prefix.with.colname = FALSE)))

  iris1 <- data.table::as.data.table(iris)
  iris1[["Petal.Length"]] <- iris1[["Petal.Length"]]^2
  sum_fun <- function(x, fill2, na.rm = FALSE) {
    if (length(x)) {
      sum(x, na.rm = na.rm)
    } else {
      fill2
    }
  }
  Y1 <- data.table::dcast(iris1, Species ~ Petal.Width,
                          fun.aggregate = sum_fun,
                          value.var = "Petal.Length",
                          drop = TRUE, fill2 = -Inf)
  Y1_post <- lapply(names(Y1)[-1], function(col) Y1[[col]] > 30)
  Y1 <- c(list(Y1[[1]]), Y1_post)


  iris2 <- data.table::as.data.table(iris)
  iris2[["Sepal.Length"]] <- iris2[["Sepal.Length"]] + iris2[["Sepal.Width"]]
  mean_fun <- function(x, fill2, na.rm = FALSE) {
    if (length(x)) {
      mean(x, na.rm = na.rm)
    } else {
      fill2
    }
  }
  Y2 <- data.table::dcast(iris2, Species ~ Petal.Width,
                          fun.aggregate = mean_fun,
                          value.var = "Sepal.Length",
                          drop = TRUE, fill2 = NA_real_)
  Y2 <- as.list(Y2)
  #Y2_post <- lapply(names(Y2)[-1], function(col) Y2[[col]] > 30)
  #Y1 <- data.table::setDT(c(list(Y1[[1]]), Y1_post))

  iris3 <- data.table::as.data.table(iris)
  last_fun <- function(x, fill2, na.rm = FALSE) {
    if (length(x)) {
      data.table::last(x)
    } else {
      fill2
    }
  }
  Y3 <- data.table::dcast(iris3, Species ~ Petal.Width,
                          fun.aggregate = last_fun,
                          value.var = "Species",
                          drop = TRUE, fill2 = "")
  Y3_post <- lapply(names(Y3)[-1], function(col) toupper(Y3[[col]]))
  Y3 <- c(list(Y3[[1]]), Y3_post)

  Y <- data.table::setDT(c(Y1, Y2[-1L], Y3[-1L]))
  expect_equivalent(X, Y)
})


test_that("edgecases work", {
  expect_silent(lfdcast::dcast(iris, character()))
  expect_silent(lfdcast::dcast(iris, character(), agg(NULL)))
})
