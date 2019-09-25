#' @export
uniqueN <- function(x, na.rm = FALSE) {
  .Call("uniqueN_vec", x, na.rm, PACKAGE = "lfdcast")
}
