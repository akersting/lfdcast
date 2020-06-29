#' Data Frame Casting
#'
#' @param X the data frame to cast.
#' @param by a character vector with zero, one or more column names of \code{X}
#'   to group by (the left hand side of the formula in
#'   \code{reshape2::\link[reshape2]{dcast}} /
#'   \code{data.table::\link[data.table]{dcast}}).
#' @param ... for \code{dcast}, one or more objects as returned by \code{agg}.
#'
#'   For \code{agg}, one or more unquoted expressions, each containing exactly
#'   one call to an aggregation function like \code{gsum}. These expressions are
#'   evaluated in three stages:
#'
#'   \bold{First}, the expression \code{\link[lfdcast:gsum]{x}} passed as the
#'   first argument to the aggregation function is evaluated in the context of
#'   the data frame \code{X}. \bold{Second}, the aggregation function is called
#'   on all subsets (as defined by \code{by}, \code{to}, \code{to.keep} and
#'   \code{subset}) of the vector resulting from the first stage. \bold{Third},
#'   the complete expression is evaluated once for each corresponding
#'   \emph{result} column with the call to the aggregation function being
#'   replaced by the results of the aggregation function for this column.
#'
#'   In the simplest case such a \code{...}-expression is just the call to the
#'   aggregation function, in which case there is no third stage.
#'
#'   Instead of specifying an expression containing the call to an aggregation
#'   function directly, it is also possible to specify an expression which
#'   evaluates to such an expression.
#' @param to a character vector with zero, one or more column names of \code{X}
#'   (or a list hereof) by which to spread \code{X} (the right hand side of the
#'   formula in \code{reshape2::\link[reshape2]{dcast}} /
#'   \code{data.table::\link[data.table]{dcast}}).
#' @param to.keep a data.frame with one or more of the columns given as
#'   \code{to}: keep only result columns corresponding to combinations of values
#'   on the \code{to} columns that are contained in \code{to.keep}.
#' @param subset,subsetq an expression which is evaluated in the context of
#'   \code{X} and must return a logical vector of length \code{nrow(X)}
#'   indicating for each row of \code{X} whether it should be passed to the
#'   aggregation function or not. For \code{subset} the expression must already
#'   be quoted, while for \code{subsetq} \code{agg} takes care of the quoting.
#'   Only one of the two arguments may be given.
#' @param names.fun function which is called to generate the column names of the
#'   result. It must accept at least the two arguments \code{e.name} and
#'   \code{to.cols}. The former is the name of the current \code{...}-argument
#'   of \code{agg}, which is missing if it is unnamed. The latter is a
#'   data.frame with the columns given as the argument \code{to} (and an
#'   undefined number of rows), which is missing if \code{length(to) == 0}. It
#'   must return a character vector of length \code{nrow(to.cols)} or - if that
#'   argument is missing - of length \code{1}. If both arguments are missing,
#'   the default function throws an error.
#' @param names.fun.args a list with additional arguments to pass to
#'   \code{names.fun}.
#' @param assert.valid.names should an error be thrown if the column names of
#'   the result are invalid or not unique?
#' @param nthread \emph{a hint} to the function on how many threads to use.
#'
#' @useDynLib lfdcast, .registration = TRUE, .fixes = "C_"
#' @export
dcast <- function(X, by, ..., assert.valid.names = TRUE, nthread = 2L) {
  if (any(dim(X) == 0L)) {
    stop("'X' must have a strictly positive number of both rows and columns.")
  }

  # frankv_job <- parallel::mcparallel(
  #   data.table::frankv(X, cols = by, na.last = FALSE,
  #                      ties.method = "dense") - 1L  # 0-based
  # )
  if (length(by) > 0L) {
    map_input_rows_to_output_rows <- data.table::frankv(X, cols = by, na.last = FALSE,
                                                        ties.method = "dense") - 1L  # 0-based
  } else {
    map_input_rows_to_output_rows <- seq(0L, nrow(X) - 1L)
  }

  args <- list()
  args_nested <- list(...)
  for (i in seq_along(args_nested)) {
    if (inherits(args_nested[[i]], "lfdcast_nested_agg_arg")) {
      for (j in seq_along(args_nested[[i]])) {
        args[[length(args) + 1L]] <- args_nested[[i]][[j]]
      }
    } else {
      args[[length(args) + 1L]] <- args_nested[[i]]
    }
  }

  arg_list_for_core <- list()
  post_expr_list <- list()

  for (i in seq_along(args)) {
    to <- args[[i]][["to"]]
    fun.aggregate <- args[[i]][["fun.aggregate"]]
    value.var <- args[[i]][["value.var"]]
    fill <- args[[i]][["fill"]]
    post.expr <- args[[i]][["post.expr"]]
    post.expr_pos <- args[[i]][["post.expr_pos"]]
    names.fun <- args[[i]][["names.fun"]]
    names.fun.args <- args[[i]][["names.fun.args"]]
    to.keep <- args[[i]][["to.keep"]]
    subset <- args[[i]][["subset"]]
    na.rm <- args[[i]][["na.rm"]]

    if (length(subset) > 0L) {
      rows2keep <- eval(subset, X, enclos = parent.frame())

      if (!is.logical(rows2keep) || length(rows2keep) != nrow(X) ||
          anyNA(rows2keep)) {
        stop(deparse(subset), " does not evaluate to a logical vector ",
             "(without missings) of length ", nrow(X), " (= nrow(X)).")
      }
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

      if (!is.null(to.keep)) {
        this_to.keep <-
          merge(X_first_row_of_col_grp,
                cbind(unique(to.keep),
                      "LFDCAST__KEEP__" = TRUE),
                by = names(to.keep),
                all.x = TRUE,
                sort = FALSE)[["LFDCAST__KEEP__"]]
      } else {
        this_to.keep <- rep(TRUE, nrow(X_first_row_of_col_grp))
      }

      col_grp_starts <- c(col_grp_starts, nrow(X) + 1L)  # might be double (OK)
      map_output_cols_to_input_rows <-
        lapply(which(this_to.keep), function(s) {
          res <- col_order[seq(col_grp_starts[s], col_grp_starts[s + 1L] - 1L)]
          res[rows2keep[res]] - 1L  # 0-based
        })
      n_col_output <- length(map_output_cols_to_input_rows)

    } else {  # by only
      map_output_cols_to_input_rows <- list(which(rows2keep) - 1L)  # 0-based
      n_col_output <- 1L
      this_to.keep <- TRUE
    }

    for (j in seq_along(fun.aggregate)) {
      fun <- fun.aggregate[[j]]

      if (is.symbol(value.var[[j]]) && as.character(value.var[[j]]) %in% names(X)) {
        value_var <- X[[as.character(value.var[[j]])]]
      } else {
        value_var <- eval(value.var[[j]], X, enclos = parent.frame())
        if (!is.atomic(value_var) || length(value_var) != nrow(X)) {
          stop(deparse(value.var[[j]]), " does not evaluate to an atomic ",
               "vector of length ", nrow(X), " (= nrow(X)).")
        }
      }

      support <- getSupport(supports[[fun]], fun, value_var)

      if (!is.null(fill[[j]])) {
        if (!storage.mode(fill[[j]]) %in% support[["fill.storage.modes"]]) {
          if (storage.mode(fill[[j]]) %in% support[["convert.fill.from"]]) {
            default <- switch(support[["fill.storage.modes"]][1L],
              logical = as.logical(fill[[j]]),
              integer = as.integer(fill[[j]]),
              double = as.double(fill[[j]]),
              charcter = as.character(fill[[j]])
            )
          } else {
            stop("unsupported storage mode of 'fill'")
          }
        } else {
          default <- fill[[j]]
        }
      } else {
        default <- support[["fill.default"]]
      }

      if (isTRUE(support[["keep.attr"]])) {
        attribs <- attributes(value_var)
      } else if (!isFALSE(support[["keep.attr"]])) {
        attribs <- attributes(value_var)[support[["keep.attr"]]]
      } else {
        attribs <- NULL
      }

      res <- lapply(seq_len(sum(!is.na(this_to.keep))),
                    function(i, x, attribs) {
                      col <- x
                      attributes(col) <- attribs
                      col
                    },
                    x = default, attribs = attribs)

      if (length(res) > 0) {
        res_names <-
          do.call(names.fun,
                  c(e.name = if (!is.null(names(post.expr)[j]) &&
                                 nchar(names(post.expr)[j]) > 0) {
                    list(names(post.expr)[j])
                  } else {
                    NULL
                  },
                  to.cols = if (length(to) > 0) {
                    list(X_first_row_of_col_grp[which(this_to.keep), , drop = FALSE])
                  } else {
                    NULL
                  },
                  names.fun.args))

        names(res) <- res_names
      }

      if (is.character(value_var)) value_var <- enc2utf8(value_var)

      agg <- fun.aggregates[fun]
      rng <- fun.aggregates_rng[[fun]]

      arg_list_for_core <- c(
        agg = list(c(arg_list_for_core[["agg"]], rep(agg, length(res)))),
        rng = list(c(arg_list_for_core[["rng"]], rep(rng, length(res)))),
        value_var = list(c(arg_list_for_core[["value_var"]],
                           lapply(seq_len(length(res)), function(i) value_var))),
        na_rm = list(c(arg_list_for_core[["na_rm"]], rep(na.rm[[j]], length(res)))),
        map_output_cols_to_input_rows = list(c(arg_list_for_core[["map_output_cols_to_input_rows"]],
                                               map_output_cols_to_input_rows)),
        res = list(c(arg_list_for_core[["res"]], res))
      )

      post_expr_list <- c(post_expr_list,
                          rep(list(list(expr = post.expr[[j]],
                                        expr_pos = post.expr_pos[[j]])),
                              length(res)))
    }
  }

  # map output cols to threads (0-based)
  seq_along_res <- seq(0L, length.out = length(arg_list_for_core[["res"]]))
  if (nthread > 1L) {
    #rng_cols_idx <- names(arg_list_for_core$agg) %in% c("sample")
    # funs which make use of rng must all be called from the same thread (1)
    rng_cols_idx <- arg_list_for_core$rng
    rng_cols <- seq_along_res[rng_cols_idx]
    if (length(rng_cols_idx) > 0L) {
      non_rng_cols <- seq_along_res[!rng_cols_idx]
    } else {
      non_rng_cols <- integer()
    }
    cols_split <- split(non_rng_cols,
                        rep_len(seq_len(nthread), length(non_rng_cols)
                        )[sample.int(length(non_rng_cols))])
    if (length(rng_cols) > 0L) {
      cols_split <- c(list(rng_cols), cols_split)
    }
  } else {
    cols_split <- list(seq_along_res)
  }

  # map_input_rows_to_output_rows <- parallel::mccollect(frankv_job)[[1L]]
  n_row_output <- max(map_input_rows_to_output_rows) + 1L

  arg_list_for_core <- c(
    list(C_lfdcast),
    arg_list_for_core[c("agg", "value_var", "na_rm", "map_output_cols_to_input_rows")],
    res = list(lapply(arg_list_for_core[["res"]], function(col) rep(col, n_row_output))),
    lengths_map_output_cols_to_input_rows =
      list(unlist(lapply(arg_list_for_core[["map_output_cols_to_input_rows"]], length))),
    map_input_rows_to_output_rows = list(map_input_rows_to_output_rows),
    cols_split = list(cols_split),
    n_row_output_SEXP = n_row_output,
    nthread_SEXP = length(cols_split)
  )

  if (assert.valid.names) {
    actual_names <- c(by, names(arg_list_for_core[["res"]]))
    valid_names <- make.names(actual_names, unique = TRUE)
    if (!identical(actual_names, valid_names)) {
      stop("result would have invalid or non-unique column names;",
           " base::make.names would make the following changes: ",
           paste0(actual_names[actual_names != valid_names], " -> ",
                  valid_names[actual_names != valid_names], collapse = ", "))

    }
  }

  if (length(arg_list_for_core$res) > 0L) {
    res <- do.call(.Call, arg_list_for_core)
  } else {
    res <- list()
  }

  for (j in seq_along(res)) {
    if (length(post_expr_list[[j]][["expr_pos"]]) > 0L) {
      e <- new.env(parent = parent.frame())
      post_expr_list[[j]][["expr"]][[post_expr_list[[j]][["expr_pos"]]]] <- res[[j]]
      res[[j]] <- eval(post_expr_list[[j]][["expr"]], e)
    }
  }

  # row_ranks_unique_pos <-
  #   which(!duplicated(map_input_rows_to_output_rows, nmax = n_row_output))
  row_ranks_unique_pos <- .Call(C_get_row_ranks_unique_pos,
                                x_SEXP = map_input_rows_to_output_rows,
                                res_SEXP = rep(NA_integer_, n_row_output))
  row_ranks_unique <- map_input_rows_to_output_rows[row_ranks_unique_pos]
  row_ranks_unique_pos_ordered <- row_ranks_unique_pos[order(row_ranks_unique)]

  id_cols <- lapply(X[by], function(col) col[row_ranks_unique_pos_ordered])

  if (data.table::is.data.table(X)) {
    res <- data.table::setDT(c(id_cols, res))
    data.table::setattr(res, "sorted", by)
  } else {
    # setDF fails for empty list
    res <- c(id_cols, res)
    if (length(res) > 0L) {
      res <- data.table::setDF(res)
    } else {
      res <- as.data.frame(res)
    }
  }

  res
}


#' @rdname dcast
#' @export
agg <- function(to, ..., to.keep = NULL, subset = NULL, subsetq = NULL,
                names.fun = make.cnames,
                names.fun.args = list(sep1 = "_", prefix.with.colname = FALSE)) {
  aggs <- substitute(...())

  specs <- lapply(aggs, extract_agg_fun)
  specs <- lapply(seq_along(aggs), function(i, e) {
    if (is.null(specs[[i]])) {
      spec <- extract_agg_fun(eval(aggs[[i]], e))
      if (is.null(spec)) {
        stop(deparse(aggs[[i]]), " neither is nor evaluates to an expression ",
             "containing a call to an aggregation function.")
      }
      spec
    } else {
      specs[[i]]
    }
  }, e = parent.frame())

  na.rm <- lapply(specs,
                  function(spec) if (is.null(s <- spec[["call"]][["na.rm"]])) FALSE else eval(s, envir = parent.frame()))
  fill <- lapply(specs, function(spec) eval(spec[["call"]][["fill"]], envir = parent.frame()))
  value.var <- lapply(specs, function(spec) spec[["call"]][["x"]])
  fun.aggregate <- lapply(specs, function(spec) spec[["agg_fun"]])
  post.expr_pos <- lapply(specs, function(spec) spec[["pos"]])

  subsetq <- substitute(subsetq)
  if (!is.null(subset) && !is.null(subsetq)) {
    stop("only one of 'subset' and 'subsetq' can be given.")
  }
  if (!is.null(subsetq)) subset <- subsetq

  if (is.list(to)) {
    structure(
      lapply(to, function(this_to) {
        list(
          to = this_to,
          fun.aggregate = fun.aggregate,
          value.var = value.var,
          fill = fill,
          to.keep = to.keep,
          subset = subset,
          na.rm = na.rm,
          post.expr = as.list(aggs),
          post.expr_pos = post.expr_pos,
          names.fun = match.fun(names.fun),
          names.fun.args = names.fun.args
        )
      }),
      class = "lfdcast_nested_agg_arg"
    )
  } else {
    list(
      to = to,
      fun.aggregate = fun.aggregate,
      value.var = value.var,
      fill = fill,
      to.keep = to.keep,
      subset = subset,
      na.rm = na.rm,
      post.expr = as.list(aggs),
      post.expr_pos = post.expr_pos,
      names.fun = match.fun(names.fun),
      names.fun.args = names.fun.args
    )
  }
}

#' Extract Details on the Call to an Aggregation Function from an Unevaluated
#' Expression
#'
#' @param x an unevaluated expression
#' @param agg_funs a character vector with the names of all aggregations
#'   functions to look for
#' @param pos internal parameter, which must not be altered.
#'
#' @return If a call to an aggregation function was found inside the expression
#'   \code{x}, a list with the following components is returned: \code{agg_fun}
#'   - the name of the aggregation function found; \code{call} - the call to the
#'   aggregation function with all of the specified arguments being specified by
#'   their full names; \code{pos} - an integer giving the position of the call
#'   inside the expression \code{x}, i.e. \code{x[[pos]]} is the call to the
#'   aggregation function. If no call to an aggregation function was found,
#'   an error is signalled.
#'
#' @keywords internal
extract_agg_fun <- function(x, agg_funs = names(fun.aggregates), pos = integer()) {
  if (is.call(x) &&
      any(idx <- unlist(lapply(agg_funs,
                               function(agg_fun) identical(x[[1L]],
                                                           as.symbol(agg_fun)))))) {
    return(list(
      agg_fun = agg_funs[idx],
      call = match.call(function(x, na.rm = FALSE, fill = NULL) {}, x),
      pos = pos
    ))
  }

  for (i in seq_along(x)[-1L]) {
    res <- extract_agg_fun(x[[i]], agg_funs, pos = c(pos, i))
    if (!is.null(res)) return(res)
  }

  return(NULL)
}

make.cnames <- function(e.name, to.cols, sep1 = "_", sep2 = sep1, sep3= sep1,
                        prefix.with.colname = FALSE) {
  if (!missing(to.cols)) {
    if (prefix.with.colname) {
      to.cols <- lapply(seq_along(to.cols),
                        function(i) paste(names(to.cols)[i], to.cols[[i]], sep = sep3))
    }
    res_names <- do.call(paste, c(unname(to.cols), sep = sep2))

    if (!missing(e.name)) {
      res_names <- do.call(paste, c(e.name, list(res_names), sep = sep1))
    }
  } else if (!missing(e.name)) {
    res_names <- e.name
  } else {
    stop("cannot name result column")
  }

  res_names
}
