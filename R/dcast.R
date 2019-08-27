#' @useDynLib lfdcast
#' @export
dcast <- function(X, by, to, fun.aggregate, value.var, to.keep = NULL,
                  na.rm = FALSE, sep = "_", nthread = 2L) {

  map_input_rows_to_output_rows <-
    data.table::frankv(X, cols = by, na.last = FALSE,
                       ties.method = "dense") - 1L  # 0-based
  n_row_output <- max(map_input_rows_to_output_rows) + 1L

  if (!is.list(to)) {
    to <- list(to)
    fun.aggregate <- list(fun.aggregate)
    value.var <- list(value.var)
    na.rm <- list(na.rm)
    to.keep <- list(to.keep)
  }

  arg_list_for_core <- list()

  for (i in seq_along(to)) {
    to_cols <- to[[i]]

    if (length(to_cols) > 0L) {
      col_order <- data.table:::forderv(X, by = to_cols, retGrp = TRUE,
                                        sort = TRUE)
      col_grp_starts <- attr(col_order, "starts")

      # handle special case where X is already in correct order
      if (length(col_order) == 0L) col_order <- seq_len(nrow(X))

      X_first_row_of_col_grp <- data.table::setDT(
        X[col_order[col_grp_starts], to_cols, drop = FALSE])

      col_grp_starts <- c(col_grp_starts, nrow(X) + 1L)  # might be double (OK)
      col_order <- col_order - 1L  # make it 0-based

      if (!is.null(to.keep[[i]])) {
        this_to.keep <-
          data.table:::merge.data.table(X_first_row_of_col_grp,
                                        cbind(unique(to.keep[[i]]),
                                              "__LFDCAST__KEEP__" = TRUE),
                                        by = names(to.keep[[i]]),
                                        all.x = TRUE,
                                        sort = FALSE)[["__LFDCAST__KEEP__"]]
      } else {
        this_to.keep <- rep(TRUE, nrow(X_first_row_of_col_grp))
      }

      map_output_cols_to_input_rows <-
        lapply(which(this_to.keep), function(s) {
          col_order[seq(col_grp_starts[s], col_grp_starts[s + 1L] - 1L)]
        })
      n_col_output <- length(map_output_cols_to_input_rows)

    } else {
      map_output_cols_to_input_rows <- list(seq(0L, length.out = nrow(X)))
      n_col_output <- 1L
      this_to.keep <- TRUE
    }

    for (j in seq_along(fun.aggregate[[i]])) {
      fun <- fun.aggregate[[i]][j]
      vvar <- value.var[[i]][j]
      if (fun %in% c("existence")) {
        default <- FALSE
      } else if (fun %in% c("count", "uniqueN") ||
                 (fun == "sum" && is.logical(X[[vvar]])) ||
                 (fun %in% c("min", "max") && (is.logical(X[[vvar]]) ||
                                               is.integer(X[[vvar]])))) {
        default <- 0L
      } else if (fun %in% c() ||
                 (fun == "sum" && is.numeric(X[[vvar]])) ||
                 (fun %in% c("min", "max") && is.numeric(X[[vvar]]))) {
        default <- 0
      } else {
        stop("invalid 'fun.aggregate - value.var' combination found")
      }
      res <- lapply(seq_len(sum(!is.na(this_to.keep))),
                    function(i, x, times) rep.int(x, times),
                    x = default, times = n_row_output)


      if (length(to_cols) > 0L) {
        res_names <- do.call(paste, c(unname(X_first_row_of_col_grp[which(this_to.keep), , drop = FALSE]), sep = sep))
        if (!is.null(names(fun.aggregate[[i]])))
          res_names <- paste(names(fun.aggregate[[i]])[j], res_names, sep = sep)
        if (!is.null(names(to)))
          res_names <- paste(names(to)[i], res_names, sep = sep)
      } else {
        res_names <- names(fun.aggregate[[i]])[j]
      }

      names(res) <- res_names

      value_var <- X[[vvar]]
      if (is.character(value_var)) value_var <- enc2utf8(value_var)

      agg <- match(fun, c("count", "existence", "sum", "uniqueN", "min", "max"))

      arg_list_for_core <- c(
        agg = list(c(arg_list_for_core[["agg"]], rep(agg, length(res)))),
        value_var = list(c(arg_list_for_core[["value_var"]], lapply(seq_len(length(res)), function(i) value_var))),
        na_rm = list(c(arg_list_for_core[["na_rm"]], rep(na.rm[[i]][j], length(res)))),
        map_output_cols_to_input_rows = list(c(arg_list_for_core[["map_output_cols_to_input_rows"]], map_output_cols_to_input_rows)),
        res = list(c(arg_list_for_core[["res"]], res))
      )
    }
  }

  # map output cols to threads (0-based)
  cols_split <- split(seq(0L, length.out = length(arg_list_for_core[["res"]])),
                      rep_len(seq_len(nthread),
                              length(arg_list_for_core[["res"]])
                      )[sample.int(length(arg_list_for_core[["res"]]))])

  arg_list_for_core <- c(
    "lfdcast",
    arg_list_for_core,
    lengths_map_output_cols_to_input_rows = list(unlist(lapply(arg_list_for_core[["map_output_cols_to_input_rows"]], length))),
    map_input_rows_to_output_rows = list(map_input_rows_to_output_rows),
    cols_split = list(cols_split),
    n_row_output_SEXP = n_row_output,
    nthread_SEXP = min(as.integer(nthread), length(cols_split)),
    PACKAGE = "lfdcast"
  )

  res <- do.call(.Call, arg_list_for_core)

  row_ranks_unique_pos <- which(!duplicated(map_input_rows_to_output_rows, nmax = n_row_output))
  row_ranks_unique <- map_input_rows_to_output_rows[row_ranks_unique_pos]
  row_ranks_unique_pos_ordered <- row_ranks_unique_pos[order(row_ranks_unique)]
  id_cols <- X[row_ranks_unique_pos_ordered, by, drop = FALSE]

  if (data.table::is.data.table(X)) {
    res <- data.table::setDT(c(id_cols, res))
    data.table::setattr(res, "sorted", by)
  } else {
    res <- data.table::setDF(c(id_cols, res))
  }

  res
}
