#' Number of Unique Elements
#'
#' @param x an atomic vector.
#' @param na.rm should \code{NA}s (and \code{NaN}s) be removed?
#'
#' @return the number of unique elements in \code{x}.
#'
#' @export
uniqueN <- function(x, na.rm = FALSE) {
  .Call(C_uniqueN_vec, x, na.rm)
}

#' Split a Character Vector into its Unique Elements and a Mapping on These
#'
#' @param x a character vector.
#'
#' @return a list with two elements \code{"chars"} and \code{"idx"} such that
#'   \code{ret$chars[ret$idx]} is identical to \code{x}.
#'
#' @export
char_map <- function(x) {
  stopifnot(is.character(x))
  res <- .Call(C_char_map, x)
  names(res) <- c("chars", "idx")
  res
}
