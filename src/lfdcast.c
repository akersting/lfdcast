#include "Rinternals.h"

#define CAST_START                                             \
  for (int j = 0; j < LENGTH(col_grp_starts) - 1; j++) {       \
    if (INTEGER(cols_res)[j] == NA_INTEGER) continue;          \
                                                               \
    SEXP col = VECTOR_ELT(res, INTEGER(cols_res)[j] - 1);      \
                                                               \
    for (int ii = INTEGER(col_grp_starts)[j] - 1;              \
         ii < INTEGER(col_grp_starts)[j + 1] - 1;              \
         ii++) {                                               \
      int i = INTEGER(col_order)[ii] - 1;                      \

#define CAST_END                                               \
    }                                                          \
  }

SEXP lfdcast(SEXP agg, SEXP value_var, SEXP na_rm, SEXP res, SEXP col_order,
             SEXP col_grp_starts, SEXP cols_res, SEXP row_ranks) {
  /*int col_cntr = 0;

  for (int j = 0; j < LENGTH(col_grp_starts) - 1; j++) {
    if (INTEGER(cols_res)[j] == NA_INTEGER) continue;

    SEXP col = VECTOR_ELT(res, col_cntr);

    for (int ii = INTEGER(col_grp_starts)[j] - 1;
         ii < INTEGER(col_grp_starts)[j + 1] - 1;
         ii++) {
      int i = INTEGER(col_order)[ii] - 1;*/
  switch(INTEGER(agg)[0]) {
    case 1: // count
      CAST_START
      (INTEGER(col)[INTEGER(row_ranks)[i] - 1])++;
      CAST_END
      break;
    case 2: // existence
      CAST_START
      LOGICAL(col)[INTEGER(row_ranks)[i] - 1] = TRUE;
      CAST_END
      break;
    case 3: // sum
      if (TYPEOF(value_var) == LGLSXP) {
        CAST_START
        if (INTEGER(col)[INTEGER(row_ranks)[i] - 1] == NA_INTEGER) {
          continue;
        } else if (LOGICAL(value_var)[i] == NA_LOGICAL) {
          if (LOGICAL(na_rm)[0]) {
            continue;
          } else {
            INTEGER(col)[INTEGER(row_ranks)[i] - 1] = NA_INTEGER;
          }
        } else {
          INTEGER(col)[INTEGER(row_ranks)[i] - 1] += LOGICAL(value_var)[i];
        }
        CAST_END
      } else if (TYPEOF(value_var) == INTSXP) {
        CAST_START
        if (INTEGER(value_var)[i] == NA_INTEGER) {
          if (LOGICAL(na_rm)[0]) {
            continue;
          } else {
            REAL(col)[INTEGER(row_ranks)[i] - 1] = NA_REAL;
          }
        } else {
          REAL(col)[INTEGER(row_ranks)[i] - 1] += INTEGER(value_var)[i];
        }
        CAST_END
      } else if (TYPEOF(value_var) == REALSXP) {
        CAST_START
        if (LOGICAL(na_rm)[0] && REAL(value_var)[i] == NA_REAL) {
          continue;
        } else {
          REAL(col)[INTEGER(row_ranks)[i] - 1] += REAL(value_var)[i];
        }
        CAST_END
      } else {
        error("value.var must be numeric or logical if fun.aggregate == 'sum'");
      }
      break;
  }
  /*  }
    col_cntr++;
  }*/

  return res;
}
