# nocov start
fun.aggregates <- list()
fun.aggregates_rng <- list()
supports <- list()

#' Register an Aggregation Function with lfdcast
#'
#' @param name the name of the aggregation function
#' @param ptr the external pointer to the function
#' @param ... on or more objects as returned by \code{support}
#' @param rng does this function make use of the R random number generator?
#' @export
register_fun.aggregate <- function(name, ptr, ..., rng = FALSE) {
  fun.aggregates[[name]] <<- ptr
  fun.aggregates_rng[[name]] <<- rng
  supports[[name]] <<- list(...)
}

#' @param class a character vector with the class(es) of inputs to which this
#'   function can be applied. An \code{NA} elements means that it can be applied
#'   to unclassed objects. A 0-length value for \code{class} means that all
#'   classes are supported.
#' @param storage.mode a character vector with the storage mode(s) of inputs to
#'   which this function can be applied
#' @param fill.default the default value with which to fill empty cells
#' @param fill.storage.modes a character vector with the storage mode(s)
#'   supported for a user-supplied fill value
#' @param convert.fill.from a (potentially empty) character vector with storage
#'   mode(s) (distinct from \code{fill.storage.modes}) which should be converted
#'   to the storage mode \code{fill.storage.modes[1]}
#' @param keep.attr should the resulting column(s) get the same attributes as the
#'   input column? Can also be a character vector with the names of the
#'   attributes to keep.
#'
#' @rdname register_fun.aggregate
#' @export
support <- function(class, storage.mode, fill.default, fill.storage.modes,
                    convert.fill.from, keep.attr) {
  list(class = class,
       storage.mode = storage.mode,
       fill.default = fill.default,
       fill.storage.modes = fill.storage.modes,
       convert.fill.from = convert.fill.from,
       keep.attr = keep.attr)
}
# nocov end

#' Find the Best Fitting Support Declaration
#'
#' @param l the list of support declarations of the aggregation function
#' @param fun.aggregate the name of the aggregation function (just used for
#'   error messages)
#' @param expr the expression containing the call to the aggregation function
#'   (just used for error messages)
#' @param value.var the input vector to which to aggregation function should be
#'   applied
#'
#' @return The best fitting support declaration given the \code{value.var}, i.e.
#'   an element of \code{l}.
#'
#' @importFrom stats na.omit
#' @keywords internal
getSupport <- function(l, fun.aggregate, expr, value.var) {
  # NA -> not supported
  # 1 -> most specific class in support declaration
  # 2 -> second most specific class in support declaration
  # ...
  # Inf -> supported just via storage mode
  res <- unlist(lapply(l, function(e) {
    # storage mode not supported
    if (!storage.mode(value.var) %in% e$storage.mode) return(NA_real_)

    # support declaration without class (anything goes)
    if (length(e$class) == 0L) return(Inf)

    class_idx <- inherits(value.var, na.omit(e$class), which = TRUE)
    class_idx <- class_idx[class_idx != 0L]

    # no match between class of value.var and support declaration
    if (length(class_idx) == 0L) {
      # does declaration support unclassed objects and is the object unclassed?
      if (any(is.na(e$class)) && length(attr(value.var, "class")) == 0L) {
        return(Inf)
      } else {
        return(NA_real_)  # class mismatch -> not supported
      }
    }

    # return index of most specific class supported
    return(as.double(min(class_idx)))
  }))

  if (all(is.na(res))) stop("the aggregation function '", fun.aggregate,
                            "' does not support a value.var of class(es) '",
                            paste0(class(value.var), collapse = ", "),
                            "' and storage mode '", storage.mode(value.var),
                            "' in '", deparse1(expr), "'.")

  idx <- which.min(res)  # best fitting declaration

  # multiple declarations fit equally well
  if (sum(res == res[idx], na.rm = TRUE) != 1L)
    stop("wrong support specification for fun.aggregate '", fun.aggregate, "'") # nocov

  l[[idx]]
}
