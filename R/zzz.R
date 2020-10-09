# nocov start
.onLoad <- function(libname, pkgname) {

  ptr <- getNativeSymbolInfo("count", "lfdcast")[["address"]]
  register_fun.aggregate("glength", ptr,
                         support(
                           class = NULL,
                           storage.mode = c("integer", "logical", "double", "character"),
                           fill.default = 0L,
                           fill.storage.modes = c("integer"),
                           convert.fill.from = c("logical", "double"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("existence", "lfdcast")[["address"]]
  register_fun.aggregate("glength_gt0", ptr,
                         support(
                           class = NULL,
                           storage.mode = c("integer", "logical", "double", "character"),
                           fill.default = FALSE,
                           fill.storage.modes = c("logical"),
                           convert.fill.from = c("integer"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("sum", "lfdcast")[["address"]]
  register_fun.aggregate("gsum", ptr,
                         support(
                           class = NA_character_,
                           storage.mode = c("logical"),
                           fill.default = 0L,
                           fill.storage.modes = c("integer"),
                           convert.fill.from = c("logical", "double"),
                           keep.attr = FALSE
                         ),
                         support(
                           class = NA_character_,
                           storage.mode = c("integer", "double"),
                           fill.default = 0,
                           fill.storage.modes = c("double"),
                           convert.fill.from = c("logical", "integer"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("uniqueN", "lfdcast")[["address"]]
  register_fun.aggregate("guniqueN", ptr,
                         support(
                           class = NULL,
                           storage.mode = c("integer", "logical", "double", "character"),
                           fill.default = 0L,
                           fill.storage.modes = c("integer"),
                           convert.fill.from = c("logical", "double"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("min", "lfdcast")[["address"]]
  register_fun.aggregate("gmin", ptr,
                         support(
                           class = "ordered",
                           storage.mode = "integer",
                           fill.default = NA_integer_,
                           fill.storage.modes = "integer",
                           convert.fill.from = "logical",
                           keep.attr = TRUE
                         ), support(
                           class = c(NA_character_, "Date"),
                           storage.mode = c("integer", "logical"),
                           fill.default = Inf,
                           fill.storage.modes = c("integer", "double"),
                           convert.fill.from = c("logical"),
                           keep.attr = TRUE
                         ), support(
                           class = c(NA_character_, "Date", "POSIXct"),
                           storage.mode = "double",
                           fill.default = Inf,
                           fill.storage.modes = "double",
                           convert.fill.from = c("logical", "integer"),
                           keep.attr = TRUE
                         ))

  ptr <- getNativeSymbolInfo("max", "lfdcast")[["address"]]
  register_fun.aggregate("gmax", ptr,
                         support(
                           class = "ordered",
                           storage.mode = "integer",
                           fill.default = NA_integer_,
                           fill.storage.modes = "integer",
                           convert.fill.from = "logical",
                           keep.attr = TRUE
                         ), support(
                           class = c(NA_character_, "Date"),
                           storage.mode = c("integer", "logical"),
                           fill.default = -Inf,
                           fill.storage.modes = c("integer", "double"),
                           convert.fill.from = c("logical"),
                           keep.attr = TRUE
                         ), support(
                           class = c(NA_character_, "Date", "POSIXct"),
                           storage.mode = "double",
                           fill.default = -Inf,
                           fill.storage.modes = "double",
                           convert.fill.from = c("logical", "integer"),
                           keep.attr = TRUE
                         ))

  ptr <- getNativeSymbolInfo("last", "lfdcast")[["address"]]
  register_fun.aggregate("glast", ptr,
                         support(
                           class = NULL,
                           storage.mode = c("logical"),
                           fill.default = NA,
                           fill.storage.modes = c("logical"),
                           convert.fill.from = c("integer", "double"),
                           keep.attr = TRUE
                         ),
                         support(
                           class = NULL,
                           storage.mode = c("integer"),
                           fill.default = NA_integer_,
                           fill.storage.modes = c("integer"),
                           convert.fill.from = c("logical", "double"),
                           keep.attr = TRUE
                         ),
                         support(
                           class = NULL,
                           storage.mode = c("double"),
                           fill.default = NA_real_,
                           fill.storage.modes = c("double"),
                           convert.fill.from = c("logical", "integer"),
                           keep.attr = TRUE
                         ),
                         support(
                           class = NULL,
                           storage.mode = c("character"),
                           fill.default = NA_character_,
                           fill.storage.modes = c("character"),
                           convert.fill.from = NULL,
                           keep.attr = TRUE
                         ))

  ptr <- getNativeSymbolInfo("sample", "lfdcast")[["address"]]
  register_fun.aggregate("gsample", ptr,
                         support(
                           class = NULL,
                           storage.mode = c("logical"),
                           fill.default = NA,
                           fill.storage.modes = c("logical"),
                           convert.fill.from = c("integer", "double"),
                           keep.attr = TRUE
                         ),
                         support(
                           class = NULL,
                           storage.mode = c("integer"),
                           fill.default = NA_integer_,
                           fill.storage.modes = c("integer"),
                           convert.fill.from = c("logical", "double"),
                           keep.attr = TRUE
                         ),
                         support(
                           class = NULL,
                           storage.mode = c("double"),
                           fill.default = NA_real_,
                           fill.storage.modes = c("double"),
                           convert.fill.from = c("logical", "integer"),
                           keep.attr = TRUE
                         ),
                         support(
                           class = NULL,
                           storage.mode = c("character"),
                           fill.default = NA_character_,
                           fill.storage.modes = c("character"),
                           convert.fill.from = NULL,
                           keep.attr = TRUE
                         ), rng = TRUE)

  ptr <- getNativeSymbolInfo("all", "lfdcast")[["address"]]
  register_fun.aggregate("gall", ptr,
                         support(
                           class = NA_character_,
                           storage.mode = c("integer", "logical"),
                           fill.default = TRUE,
                           fill.storage.modes = c("logical"),
                           convert.fill.from = c("integer"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("any", "lfdcast")[["address"]]
  register_fun.aggregate("gany", ptr,
                         support(
                           class = NA_character_,
                           storage.mode = c("integer", "logical"),
                           fill.default = FALSE,
                           fill.storage.modes = c("logical"),
                           convert.fill.from = c("integer"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("mean", "lfdcast")[["address"]]
  register_fun.aggregate("gmean", ptr,
                         support(
                           class = NA_character_,
                           storage.mode = c("logical", "integer", "double"),
                           fill.default = NaN,
                           fill.storage.modes = c("double"),
                           convert.fill.from = c("logical", "integer"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("median", "lfdcast")[["address"]]
  register_fun.aggregate("gmedian", ptr,
                         support(
                           class = NA_character_,
                           storage.mode = c("logical", "integer", "double"),
                           fill.default = NA_real_,
                           fill.storage.modes = c("double"),
                           convert.fill.from = c("logical", "integer"),
                           keep.attr = FALSE
                         ))
}

#' Aggregate by Taking the Length
#'
#' @param x an expression to be evaluated in the context of the data frame
#'   \code{\link[lfdcast:dcast]{X}} to cast and resulting in an atomic vector of
#'   length \code{nrow(\link[lfdcast:dcast]{X})}. In the simplest case this is
#'   just an unquoted column name of the data frame
#'   \code{\link[lfdcast:dcast]{X}}.
#' @param na.rm should \code{NA}s be removed from the vector resulting from the
#'   evaluation of \code{x} before applying the aggregation function to it?
#' @param fill value with which to fill empty cells in the result.
#'
#' @export
glength <- function(x, na.rm = FALSE, fill = 0L)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' @rdname glength
#' @export
glength_gt0 <- function(x, na.rm = FALSE, fill = FALSE)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' Aggregate Numeric Vectors by Summing
#' @inheritParams glength
#' @export
gsum <- function(x, na.rm = FALSE, fill = 0)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' Aggregate by Counting Unique Elements
#' @inheritParams  glength
#' @export
guniqueN <- function(x, na.rm = FALSE, fill = 0L)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' Aggregate Numeric Vectors by Taking Maxima/Minima
#'
#' @note These functions can also be applied to \emph{ordered} factors. In this
#'   case the default \code{fill} value is \code{NA_integer_}.
#'
#' @inheritParams glength
#' @export
gmax <- function(x, na.rm = FALSE, fill = -Inf)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' @rdname gmax
#' @export
gmin <- function(x, na.rm = FALSE, fill = Inf)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' Aggregate by Selecting one Element
#' @inheritParams glength
#' @export
glast <- function(x, na.rm = FALSE, fill = NA)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' @rdname glast
#' @export
gsample <- function(x, na.rm = FALSE, fill = NA)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' Aggregate Logical Vectors by Any/All
#' @inheritParams glength
#' @export
gany <- function(x, na.rm = FALSE, fill = FALSE)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' @rdname gany
#' @export
gall <- function(x, na.rm = FALSE, fill = TRUE)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' Aggregate Numeric Vectors by Averaging
#' @inheritParams glength
#' @export
gmean <- function(x, na.rm = FALSE, fill = NaN)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

#' @rdname gmean
#' @export
gmedian <- function(x, na.rm = FALSE, fill = NA_real_)
  stop("This function must not be called directly. Have a look at ?lfdcast::dcast.")

# nocov end
