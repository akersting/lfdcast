fun.aggregates <- list()
supports <- list()

#' @export
register_fun.aggregate <- function(name, ptr, ...) {
  fun.aggregates[[name]] <<- ptr
  supports[[name]] <<- list(...)
}

support <- function(class, storage.mode, fill.default, fill.storage.modes, convert.fill.from, keep.attr) {
  list(class = class, storage.mode = storage.mode, fill.default = fill.default,
       fill.storage.modes = fill.storage.modes, convert.fill.from = convert.fill.from,
       keep.attr = keep.attr)
}

getSupport <- function(l, fun.aggregate, value.var) {
  res <- unlist(lapply(l, function(e) {
    if (!storage.mode(value.var) %in% e$storage.mode) return(NA_real_)
    #if (length(e$class) == 0L && length(attr(value.var, "class")) != 0L) return(NA_real_)
    if (length(e$class) == 0L) return(Inf)

    class_idx <- inherits(value.var, na.omit(e$class), which = TRUE)
    class_idx <- class_idx[class_idx != 0L]
    if (length(class_idx) == 0L) {
      if (any(is.na(e$class)) && length(attr(value.var, "class")) == 0L) {
        return(Inf)
      } else {
        return(NA_real_)
      }
    }
    return(as.double(min(class_idx)))
  }))

  if (all(is.na(res))) stop("fun.aggregate '", fun.aggregate,
                            "' does not support a value.var of class(es) '",
                            paste0(class(value.var), collapse = ", "),
                            "' and storage mode '", storage.mode(value.var), "'.")
  idx <- which.min(res)
  if (sum(res == res[idx], na.rm = TRUE) != 1L) stop("wrong support specification")

  l[[idx]]
}
