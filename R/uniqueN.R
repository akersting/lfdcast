#' Number of Unique Elements
#'
#' @param x an atomic vector.
#' @param na.rm should \code{NA}s (and \code{NaN}s) be removed?
#'
#' @return the number of unique elements in \code{x}.
#'
#' @export
uniqueN <- function(x, na.rm = FALSE) {
  .Call("uniqueN_vec", x, na.rm, PACKAGE = "lfdcast")
}
