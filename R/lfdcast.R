#' @useDynLib lfdcast
#' @export
lfdcast <- function(x, lhs, rhs,
                    fun.aggregate = c("count", "existence", "sum"),
                    sep = "_",
                    value.var = NULL, na.rm = FALSE,
                    rhs_keep = NULL) {
  fun.aggregate <- match.arg(fun.aggregate)

  row_ranks <- data.table::frankv(x, cols = lhs, ties.method = "dense")
  n_row <- max(row_ranks)

  col_order <- data.table:::forderv(x, by = rhs, retGrp = TRUE, sort = TRUE)
  col_grp_starts <- attr(col_order, "starts")
  n_col <- length(col_grp_starts)
  if (length(col_order) == 0L) col_order <- seq_len(nrow(x))

  x_rhs <- x[col_order[col_grp_starts], rhs, drop = FALSE]

  if (!is.null(rhs_keep)) {
    rhs_keep <- data.table:::merge.data.table(x_rhs,
                                              cbind(rhs_keep, "__LFDCAST__KEEP__" = TRUE),
                                              all.x = TRUE, sort = FALSE)[["__LFDCAST__KEEP__"]]
  } else {
    rhs_keep <- rep(TRUE, nrow(x_rhs))
  }

  if (fun.aggregate %in% c("existence")) {
    default <- FALSE
  } else if (fun.aggregate %in% c("count") ||
             (fun.aggregate == "sum" && is.logical(x[[value.var]]))) {
    default <- 0L
  } else if (fun.aggregate %in% c() ||
             (fun.aggregate == "sum" && is.numeric(x[[value.var]]))) {
    default <- 0
  }
  res <- data.table::setDF(lapply(seq_len(sum(!is.na(rhs_keep))),
                                  function(i, x, times) rep.int(x, times),
                                  x = default, times = n_row))
  names(res) <- do.call(paste, c(unname(x_rhs[which(rhs_keep), , drop = FALSE]), sep = sep))

  cols_res <- rep(NA_integer_, length(col_grp_starts))
  cols_res[which(rhs_keep)] <- seq_len(sum(!is.na(rhs_keep)))

  col_grp_starts <- c(col_grp_starts, nrow(x) + 1L)
  res <- .Call("lfdcast", match(fun.aggregate, eval(formals()[["fun.aggregate"]])),
               x[[value.var]], na.rm, res, col_order, col_grp_starts, cols_res, row_ranks,
               PACKAGE = "lfdcast")

  # col_cntr <- 1L
  # for (j in seq_len(n_col)) {
  #   if (is.na(rhs_keep[j])) next
  #
  #   for (i in col_order[seq(col_grp_starts[j], col_grp_starts[j + 1L] - 1L)]) {
  #     res[row_ranks[i], col_cntr] <- res[row_ranks[i], col_cntr] + 1L
  #   }
  #   col_cntr <- col_cntr + 1L
  # }


  row_ranks_unique_pos <- which(!duplicated(row_ranks, nmax = n_row))
  row_ranks_unique <- row_ranks[row_ranks_unique_pos]
  row_ranks_unique_pos_ordered <- row_ranks_unique_pos[order(row_ranks_unique)]
  id_cols <- x[row_ranks_unique_pos_ordered, lhs, drop = FALSE]
  # attr(id_cols, "row.names") <- .set_row_names(nrow(id_cols))

  res <- data.table::setDF(c(id_cols, res))
  res
}
