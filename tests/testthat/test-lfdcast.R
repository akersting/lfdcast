library(data.table)

test_that("lfdcast produces same results as data.table by", {
  iris <- as.data.table(iris)
  subset <- sample(c(TRUE, FALSE), 150, TRUE)

  agg1 <- agg(list(NULL),
              S = gsum(Petal.Length),
              M = gmean(Sepal.Length),
              subset = subset)

  X <- lfdcast::dcast(iris, "Species", agg1)
  assignInNamespace("cedta.override",
                    c("lfdcast", data.table:::cedta.override),
                    "data.table")
  Y <- iris[subset, .(S = sum(Petal.Length), M = mean(Sepal.Length)), keyby = "Species"]
  assignInNamespace("cedta.override",
                    data.table:::cedta.override[-1],
                    "data.table")
  expect_identical(X, Y)
})

test_that("lfdcast produces same results as data.table::dcast", {
  iris <- as.data.table(iris)
  X <- lfdcast::dcast(iris, "Species", agg("Petal.Width",
                                           Petal.Length_sum = gsum(Petal.Length),
                                           Sepal.Length_mean = gmean(Sepal.Length),
                                           Sepal.Width_list = glist(Sepal.Width),
                                           names.fun.args = list(prefix.with.colname = FALSE)))
  Y <- data.table::dcast(iris, Species ~ Petal.Width,
                         fun.aggregate = list(sum, mean, list),
                         value.var = list("Petal.Length", "Sepal.Length", "Sepal.Width"),
                         drop = TRUE)
  expect_equal(X, Y)


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



  X <- lfdcast::dcast(as.data.table(iris), "Species",
                      agg(list("Species", c("Species", "Petal.Width")), gsum(Petal.Width),
                          to.keep = list(data.frame(Species = "setosa"),
                                         data.frame(Petal.Width = iris$Petal.Width[1:10])),
                          names.fun.args = list(prefix.with.colname = TRUE)))
  Y1 <- data.table::dcast(as.data.table(iris), Species ~ Species, fun.aggregate = sum, value.var = "Petal.Width")
  set(Y1, j = "versicolor", value = NULL)
  set(Y1, j = "virginica", value = NULL)
  Y2 <- data.table::dcast(as.data.table(iris), Species ~ Species + Petal.Width, fun.aggregate = sum, value.var = "Petal.Width")
  Y2 <- Y2[, 2:5]

  Y <- cbind(Y1, Y2)
  setnames(Y, names(X))

  expect_equal(X, Y)



  expr <- quote(gsum(Petal.Width))
  X <- lfdcast::dcast(as.data.table(iris), "Species",
                      agg("Species", expr, subsetq = Sepal.Width == round(Sepal.Width)), nthread = 1L)
  Y <- data.table::dcast(as.data.table(iris[iris$Sepal.Width == round(iris$Sepal.Width), , drop = FALSE]),
                         Species ~ Species, fun.aggregate = sum, value.var = "Petal.Width")
  expect_equal(X, Y)



  X <- data.frame(int = 1:100)
  expect_silent(
    X <- lfdcast::dcast(X, "int", agg(NULL,
                                      a = glist(int),
                                      b = glist(as.numeric(int)),
                                      c = glist(as.logical(int)),
                                      d = glist(as.character(int)),
                                      e = gmin(as.Date(Sys.Date() + int))))
  )
})


test_that("edgecases work", {
  expect_silent(lfdcast::dcast(iris, character()))
  expect_silent(lfdcast::dcast(iris, character(), agg(NULL)))

  # 0 column result
  X <- data.frame(a = 1:10, b = 1:10)
  expect_silent(dcast(X, "a", agg("b", xxx = gsum(b), to.keep = data.frame(b = 0))))
})

test_that("regression test", {
  skip_on_cran()
  skip_if_not_installed("digest")

  n <- 1000
  set.seed(123)

  X <- data.frame(
    i = sample(c(-156L, -1L, 0L, 1L, 161L, NA_integer_), n, TRUE),
    n = sample(c(-Inf, -1615, -1, -0, 0, 1, 1561, Inf, NA_real_, NaN), n, TRUE),
    l = sample(c(TRUE, FALSE, NA), n, TRUE),
    c = sample(c(letters, NA_character_, ""), n, TRUE),
    f = factor(sample(c(LETTERS, NA_character_), n, TRUE)),
    d = as.Date("2022-02-06") + sample(c(-351, -156, 156, 618, NA_real_), n, TRUE)
  )

  res <- dcast(X, c("n", "i", "l", "f", "d", "c"),
               agg(c("d", "f", "c", "l", "n", "i"),
                   i = gsum(i),
                   n = gmedian(n),
                   l = gall(l),
                   c = gfirst(c),
                   f = glist(f),
                   d = gmax(d)), assert.valid.names = FALSE)
  expect_identical(digest::digest(res, algo = "md5"),
                   "0991b4b4f4e00c4902a4d3e2e865b8d8")
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
