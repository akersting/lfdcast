fun.aggregates <- list()

#' @export
register_fun.aggregate <- function(name, ptr) {
  fun.aggregates[[name]] <<- ptr
}
