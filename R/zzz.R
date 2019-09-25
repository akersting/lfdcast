.onLoad <- function(libname, pkgname) {

  ptr <- getNativeSymbolInfo("count", "lfdcast")[["address"]]
  register_fun.aggregate("count", ptr,
                         support(
                           class = NULL,
                           storage.mode = c("integer", "logical", "double", "character"),
                           fill.default = 0L,
                           fill.storage.modes = c("integer"),
                           convert.fill.from = c("logical", "double"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("existence", "lfdcast")[["address"]]
  register_fun.aggregate("existence", ptr,
                         support(
                           class = NULL,
                           storage.mode = c("integer", "logical", "double", "character"),
                           fill.default = FALSE,
                           fill.storage.modes = c("logical"),
                           convert.fill.from = c("integer"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("sum", "lfdcast")[["address"]]
  register_fun.aggregate("sum", ptr,
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
  register_fun.aggregate("uniqueN", ptr,
                         support(
                           class = NULL,
                           storage.mode = c("integer", "logical", "double", "character"),
                           fill.default = 0L,
                           fill.storage.modes = c("integer"),
                           convert.fill.from = c("logical", "double"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("min", "lfdcast")[["address"]]
  register_fun.aggregate("min", ptr,
                         support(
                           class = c(NA_character_, "Date", "ordered"),
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
  register_fun.aggregate("max", ptr,
                         support(
                           class = c(NA_character_, "Date", "ordered"),
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

  ptr <- getNativeSymbolInfo("last", "lfdcast")[["address"]]
  register_fun.aggregate("last", ptr,
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
  register_fun.aggregate("sample", ptr,
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

  ptr <- getNativeSymbolInfo("all", "lfdcast")[["address"]]
  register_fun.aggregate("all", ptr,
                         support(
                           class = NA_character_,
                           storage.mode = c("integer", "logical"),
                           fill.default = TRUE,
                           fill.storage.modes = c("logical"),
                           convert.fill.from = c("integer"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("any", "lfdcast")[["address"]]
  register_fun.aggregate("any", ptr,
                         support(
                           class = NA_character_,
                           storage.mode = c("integer", "logical"),
                           fill.default = TRUE,
                           fill.storage.modes = c("logical"),
                           convert.fill.from = c("integer"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("mean", "lfdcast")[["address"]]
  register_fun.aggregate("mean", ptr,
                         support(
                           class = NA_character_,
                           storage.mode = c("logical", "integer", "double"),
                           fill.default = NaN,
                           fill.storage.modes = c("double"),
                           convert.fill.from = c("logical", "integer"),
                           keep.attr = FALSE
                         ))

  ptr <- getNativeSymbolInfo("median", "lfdcast")[["address"]]
  register_fun.aggregate("median", ptr,
                         support(
                           class = NA_character_,
                           storage.mode = c("logical", "integer", "double"),
                           fill.default = NA_real_,
                           fill.storage.modes = c("double"),
                           convert.fill.from = c("logical", "integer"),
                           keep.attr = FALSE
                         ))
}

#' @export
SUM__ <- function(x, na.rm, fill) stop("eee")
