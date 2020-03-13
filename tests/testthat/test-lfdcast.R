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



  X <- lfdcast::dcast(as.data.table(iris), "Species",
                      agg("Species", gsum(Petal.Width),
                          to.keep = data.frame(Species = "setosa")))
  Y <- data.table::dcast(as.data.table(iris), Species ~ Species, fun.aggregate = sum, value.var = "Petal.Width")
  set(Y, j = "versicolor", value = NULL)
  set(Y, j = "virginica", value = NULL)
  expect_equal(X, Y)



  expr <- quote(gsum(Petal.Width))
  X <- lfdcast::dcast(as.data.table(iris), "Species",
                      agg("Species", expr, subsetq = Sepal.Width == round(Sepal.Width)), nthread = 1L)
  Y <- data.table::dcast(as.data.table(iris[iris$Sepal.Width == round(iris$Sepal.Width), , drop = FALSE]),
                         Species ~ Species, fun.aggregate = sum, value.var = "Petal.Width")
  expect_equal(X, Y)
})


test_that("edgecases work", {
  expect_silent(lfdcast::dcast(iris, character()))
  expect_silent(lfdcast::dcast(iris, character(), agg(NULL)))

  # 0 column result
  X <- data.frame(a = 1:10, b = 1:10)
  expect_silent(dcast(X, "a", agg("b", xxx = gsum(b), to.keep = data.frame(b = 0))))
})

test_that("correct errors are thrown", {
  expect_error(lfdcast::dcast(iris, "Species", agg(NULL,
                                                   col = gsample(Sepal.Length),
                                                   col = glast(Sepal.Width))),
               regexp = "result would have invalid or non-unique column names; base::make.names would make the following changes: col -> col\\.1")

  expect_error(lfdcast::dcast(iris, "Species", agg(NULL,
                                                   gsample(Sepal.Length))))

  expr <- quote(sum(Sepal.Length))
  expect_error(lfdcast::dcast(iris, "Species", agg(NULL,
                                                   col = expr)),
               regexp = "expr neither is nor evaluates to an expression containing a call to an aggregation function")

  expect_error(lfdcast::dcast(iris, "Species", agg(NULL,
                                                   col = gsum(sum(Sepal.Length)))),
               regexp = "sum\\(Sepal\\.Length\\) does not evaluate to an atomic vector of length 150 \\(= nrow\\(X\\)\\)")

  expect_error(lfdcast::dcast(iris, "Species", agg(NULL,
                                                   col = gsum(Sepal.Length), subset = quote(42))),
               regexp = "42 does not evaluate to a logical vector \\(without missings\\) of length 150 \\(= nrow\\(X\\)\\)")

  expect_error(lfdcast::dcast(iris, "Species", agg(NULL,
                                                   col = gsum(Species))),
               regexp = "the aggregation function 'gsum' does not support a value.var of class\\(es\\) 'factor' and storage mode 'integer'")
})
