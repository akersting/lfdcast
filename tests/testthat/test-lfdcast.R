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
