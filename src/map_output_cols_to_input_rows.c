#include "lfdcast.h"

SEXP get_map_output_cols_to_input_rows(SEXP ranks_SEXP,
                                       SEXP ncols_SEXP,
                                       SEXP rows2keep_SEXP) {
  int ncols = asInteger(ncols_SEXP);
  int *rows_per_col = (int *) R_alloc((size_t) ncols, sizeof(int));
  memset(rows_per_col, 0, ncols * sizeof(int));

  int n = LENGTH(ranks_SEXP);
  int *ranks = INTEGER(ranks_SEXP);

  SEXP idx_SEXP = PROTECT(allocVector(INTSXP, ncols));
  int *idx = INTEGER(idx_SEXP);
  if (rows2keep_SEXP == R_NilValue) {
    for (int i = 0; i < n; i++) {
      idx[ranks[i] - 1] = i + 1;
      rows_per_col[ranks[i] - 1]++;
    }
  } else {
    int *rows2keep = LOGICAL(rows2keep_SEXP);
    for (int i = 0; i < n; i++) {
      idx[ranks[i] - 1] = i + 1;
      if (rows2keep[i])
        rows_per_col[ranks[i] - 1]++;
    }
  }


  SEXP map = PROTECT(allocVector(VECSXP, ncols));
  for (int i = 0; i < ncols; i++) {
    SET_VECTOR_ELT(map, i, allocVector(INTSXP, rows_per_col[i]));
  }

  memset(rows_per_col, 0, ncols * sizeof(int));
  if (rows2keep_SEXP == R_NilValue) {
    for (int i = 0; i < n; i++) {
      INTEGER(VECTOR_ELT(map, ranks[i] - 1))[rows_per_col[ranks[i] - 1]++] = i;
    }
  } else{
    int *rows2keep = LOGICAL(rows2keep_SEXP);
    for (int i = 0; i < n; i++) {
      if (rows2keep[i])
        INTEGER(VECTOR_ELT(map, ranks[i] - 1))[rows_per_col[ranks[i] - 1]++] = i;
    }
  }

  SEXP ret = allocVector(VECSXP, 2);
  SET_VECTOR_ELT(ret, 0, map);
  SET_VECTOR_ELT(ret, 1, idx_SEXP);
  UNPROTECT(2);
  return ret;
}
