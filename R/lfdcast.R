#' @useDynLib lfdcast
#' @export
lfdcast <- function(x, lhs, rhs,
                    fun.aggregate = c("count", "existence", "sum"),
                    sep = "_",
                    value.var = NA_character_, na.rm = FALSE,
                    rhs_keep = NULL,
                    nthread = 2L) {
  if (missing(fun.aggregate))
    fun.aggregate <- match.arg(fun.aggregate)

  row_ranks <- data.table::frankv(x, cols = lhs, ties.method = "dense") - 1L
  n_row <- max(row_ranks) + 1L

  res_list <- list()

  if (!is.list(rhs)) {
    rhs <- list(rhs)
    fun.aggregate <- list(fun.aggregate)
    value.var <- list(value.var)
    na.rm <- list(na.rm)
    rhs_keep <- list(rhs_keep)
  }
  for (i in seq_along(rhs)) {
    r <- rhs[[i]]

    col_order <- data.table:::forderv(x, by = r, retGrp = TRUE, sort = TRUE)
    col_grp_starts <- attr(col_order, "starts")
    n_col <- length(col_grp_starts)
    if (length(col_order) == 0L) col_order <- seq_len(nrow(x))

    x_rhs <- x[col_order[col_grp_starts], r, drop = FALSE]
    col_grp_starts <- c(col_grp_starts - 1L, nrow(x))
    col_order <- col_order - 1L

    if (!is.null(rhs_keep[[i]])) {
      this_rhs_keep <- data.table:::merge.data.table(x_rhs,
                                                     cbind(unique(rhs_keep[[i]]), "__LFDCAST__KEEP__" = TRUE),
                                                     all.x = TRUE, sort = FALSE)[["__LFDCAST__KEEP__"]]
    } else {
      this_rhs_keep <- rep(TRUE, nrow(x_rhs))
    }

    cols_res <- rep(NA_integer_, n_col)
    cols_res[which(this_rhs_keep)] <- seq(0L, length.out = sum(!is.na(this_rhs_keep)))

    cols_split <- split(seq(0L, length.out = n_col), sample.int(nthread, n_col, replace = nthread < n_col))

    for (j in seq_along(fun.aggregate[[i]])) {
      fun <- fun.aggregate[[i]][j]
      vvar <- value.var[[i]][j]
      if (fun %in% c("existence")) {
        default <- FALSE
      } else if (fun %in% c("count") ||
                 (fun == "sum" && is.logical(x[[vvar]]))) {
        default <- 0L
      } else if (fun %in% c() ||
                 (fun == "sum" && is.numeric(x[[vvar]]))) {
        default <- 0
      } else {
        stop("invalid 'fun.aggregate - value.var' combination found")
      }
      res <- data.table::setDF(lapply(seq_len(sum(!is.na(this_rhs_keep))),
                                      function(i, x, times) rep.int(x, times),
                                      x = default, times = n_row))
      names(res) <- do.call(paste, c(unname(x_rhs[which(this_rhs_keep), , drop = FALSE]), sep = sep))
      if (!is.null(names(fun.aggregate[[i]])))
        names(res) <- paste(names(fun.aggregate[[i]])[j], names(res), sep = sep)
      if (!is.null(names(rhs)))
        names(res) <- paste(names(rhs)[i], names(res), sep = sep)

      res_list <- c(res_list,
                    list(.Call("lfdcast", match(fun, eval(formals()[["fun.aggregate"]])),
                               x[[vvar]], na.rm[[i]][j], cols_split, res, col_order, col_grp_starts, cols_res, row_ranks,
                               min(as.integer(nthread), length(cols_split)),
                               PACKAGE = "lfdcast")))
    }
  }

  row_ranks_unique_pos <- which(!duplicated(row_ranks, nmax = n_row))
  row_ranks_unique <- row_ranks[row_ranks_unique_pos]
  row_ranks_unique_pos_ordered <- row_ranks_unique_pos[order(row_ranks_unique)]
  id_cols <- x[row_ranks_unique_pos_ordered, lhs, drop = FALSE]

  if (data.table::is.data.table(x)) {
    res <- data.table::setDT(c(id_cols, unlist(res_list, recursive = FALSE)))
    data.table::setattr(res, "sorted", lhs)
  } else {
    res <- data.table::setDF(c(id_cols, unlist(res_list, recursive = FALSE)))
  }

  res
}
