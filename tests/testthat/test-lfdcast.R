test_that("lfdcast produces same results as dcast", {
  X <- data.frame(
    a = rep(letters, 2),
    b = 1:4,
    c = sample.int(100, 52),
    d = LETTERS[sample.int(26, 52, replace = TRUE)]
  )
  expect_equivalent(
    lfdcast(X, c("a", "b"), c("c", "d")),
    data.table:::dcast.data.table(data.table::as.data.table(X), a + b ~ c + d, fun.aggregate = length)
  )
})
