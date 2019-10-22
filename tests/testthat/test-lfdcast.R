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
