#' @export
make_cast_args <- function(to, fun.aggregate, value.var, fill = NULL,
                           to.keep = NULL, subset = NULL, na.rm = FALSE,
                           sep = "_") {
  list(
    to = to,
    fun.aggregate = fun.aggregate,
    value.var = value.var,
    fill = if (is.null(fill)) {
      vector("list", length(value.var))
    } else if (length(fill) == 1L) {
      as.list(rep(fill, length(value.var)))
    } else {
      fill
    },
    to.keep = to.keep,
    subset = subset,
    na.rm = if (length(na.rm) == 1L) rep(na.rm, length(value.var)),
    sep = if (length(sep) == 1L) rep(sep, length(value.var))
  )
}

#' @useDynLib lfdcast
#' @export
dcast <- function(X, by, ..., nthread = 2L) {

  map_input_rows_to_output_rows <-
    data.table::frankv(X, cols = by, na.last = FALSE,
                       ties.method = "dense") - 1L  # 0-based
  n_row_output <- max(map_input_rows_to_output_rows) + 1L

  args <- list(...)

  arg_list_for_core <- list()

  for (i in seq_along(args)) {
    to <- args[[i]][["to"]]
    fun.aggregate <- args[[i]][["fun.aggregate"]]
    value.var <- args[[i]][["value.var"]]
    fill <- args[[i]][["fill"]]
    to.keep <- args[[i]][["to.keep"]]
    subset <- args[[i]][["subset"]]
    na.rm <- args[[i]][["na.rm"]]
    sep <- args[[i]][["sep"]]

    if (length(subset) > 0L) {
      rows2keep <- eval(subset, X, enclos = parent.frame())
    } else {
      rows2keep <- rep(TRUE, nrow(X))
    }

    if (length(to) > 0L) {
      col_order <- data.table:::forderv(X, by = to, retGrp = TRUE,
                                        sort = TRUE)
      col_grp_starts <- attr(col_order, "starts")

      # handle special case where X is already in correct order
      if (length(col_order) == 0L) col_order <- seq_len(nrow(X))

      X_first_row_of_col_grp <- data.table::setDT(
        X[col_order[col_grp_starts], to, drop = FALSE])

      col_grp_starts <- c(col_grp_starts, nrow(X) + 1L)  # might be double (OK)

      if (!is.null(to.keep)) {
        this_to.keep <-
          data.table:::merge.data.table(X_first_row_of_col_grp,
                                        cbind(unique(to.keep),
                                              "__LFDCAST__KEEP__" = TRUE),
                                        by = names(to.keep),
                                        all.x = TRUE,
                                        sort = FALSE)[["__LFDCAST__KEEP__"]]
      } else {
        this_to.keep <- rep(TRUE, nrow(X_first_row_of_col_grp))
      }

      map_output_cols_to_input_rows <-
        lapply(which(this_to.keep), function(s) {
          res <- col_order[seq(col_grp_starts[s], col_grp_starts[s + 1L] - 1L)]
          res[rows2keep[res]] - 1L
        })
      n_col_output <- length(map_output_cols_to_input_rows)

    } else {
      map_output_cols_to_input_rows <- list(which(rows2keep) - 1L)
      n_col_output <- 1L
      this_to.keep <- TRUE
    }

    for (j in seq_along(fun.aggregate)) {
      fun <- fun.aggregate[j]
      vvar <- value.var[j]

      if (!is.null(fill[[j]])) {
        default <- fill[[j]]
      } else {
        value_var_bare <- unclass(X[[vvar]])
        if (fun %in% c("existence")) {
          default <- FALSE
        } else if (fun %in% c("count", "uniqueN") ||
                   (fun == "sum" && is.logical(value_var_bare))) {
          default <- 0L
        } else if (fun %in% c() ||
                   (fun == "sum" && is.numeric(value_var_bare))) {
          default <- 0
        } else if (fun %in% c("min")) {
          default <- Inf
        } else if (fun %in% c("max")) {
          default <- -Inf
        } else {
          stop("invalid 'fun.aggregate - value.var' combination found")
        }
      }

      if (fun %in% c("min", "max")) {
        attribs <- attributes(X[[vvar]])
      } else {
        attribs <- NULL
      }

      res <- lapply(seq_len(sum(!is.na(this_to.keep))),
                    function(i, x, times, attribs) {
                      col <- rep.int(x, times)
                      attributes(col) <- attribs
                      col
                    },
                    x = default, times = n_row_output, attribs = attribs)


      if (length(to) > 0L) {
        res_names <- do.call(paste, c(unname(X_first_row_of_col_grp[which(this_to.keep), , drop = FALSE]), sep = sep[j]))
        if (!is.null(names(fun.aggregate)))
          res_names <- paste(names(fun.aggregate)[j], res_names, sep = sep[j])
        if (!is.null(names(args)))
          res_names <- paste(names(args)[i], res_names, sep = sep[j])
      } else {
        res_names <- names(fun.aggregate)[j]
      }

      names(res) <- res_names

      value_var <- X[[vvar]]
      if (is.character(value_var)) value_var <- enc2utf8(value_var)

      agg <- match(fun, c("count", "existence", "sum", "uniqueN", "min", "max"))

      arg_list_for_core <- c(
        agg = list(c(arg_list_for_core[["agg"]], rep(agg, length(res)))),
        value_var = list(c(arg_list_for_core[["value_var"]], lapply(seq_len(length(res)), function(i) value_var))),
        na_rm = list(c(arg_list_for_core[["na_rm"]], rep(na.rm[j], length(res)))),
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

  # row_ranks_unique_pos <- which(!duplicated(map_input_rows_to_output_rows, nmax = n_row_output))
  row_ranks_unique_pos <- .Call("get_row_ranks_unique_pos",
                                x_SEXP = map_input_rows_to_output_rows,
                                res_SEXP = rep(NA_integer_, n_row_output),
                                PACKAGE = "lfdcast")
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
