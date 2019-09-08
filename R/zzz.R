.onLoad <- function(libname, pkgname) {

  ptr <- getNativeSymbolInfo("count", "lfdcast")[["address"]]
  register_fun.aggregate("count", ptr)

  ptr <- getNativeSymbolInfo("existence", "lfdcast")[["address"]]
  register_fun.aggregate("existence", ptr)

  ptr <- getNativeSymbolInfo("sum", "lfdcast")[["address"]]
  register_fun.aggregate("sum", ptr)

  ptr <- getNativeSymbolInfo("uniqueN", "lfdcast")[["address"]]
  register_fun.aggregate("uniqueN", ptr)

  ptr <- getNativeSymbolInfo("min", "lfdcast")[["address"]]
  register_fun.aggregate("min", ptr)

  ptr <- getNativeSymbolInfo("max", "lfdcast")[["address"]]
  register_fun.aggregate("max", ptr)

  ptr <- getNativeSymbolInfo("last", "lfdcast")[["address"]]
  register_fun.aggregate("last", ptr)

  ptr <- getNativeSymbolInfo("sample", "lfdcast")[["address"]]
  register_fun.aggregate("sample", ptr)
}
