#include "lfdcast.h"

SEXP get_map_output_cols_to_input_rows(SEXP col_order,
                                       SEXP col_grp_starts,
                                       SEXP nrow_X,
                                       SEXP to_keep,
                                       SEXP rows2keep) {

  int *cgs = (int *) R_alloc((LENGTH(col_grp_starts) + 1), sizeof(int));

  for (int i = 0; i < LENGTH(col_grp_starts); i++) {
    cgs[i] = INTEGER(col_grp_starts)[i] - 1;
  }
  cgs[LENGTH(col_grp_starts)] = asInteger(nrow_X);

  SEXP map = PROTECT(allocVector(VECSXP, LENGTH(to_keep)));
  for (int i = 0; i < LENGTH(to_keep); i++) {
    int s = INTEGER(to_keep)[i] - 1;

    SEXP res_SEXP = allocVector(INTSXP, cgs[s + 1] - cgs[s]);
    SET_VECTOR_ELT(map, i, res_SEXP);
    int *res = INTEGER(res_SEXP);
    if (rows2keep == R_NilValue) {
      int *co = INTEGER(col_order);
      for (int j = 0; j < cgs[s + 1] - cgs[s]; j++) {
        res[j] = co[cgs[s] + j] - 1;
      }
    } else {

      int *co = INTEGER(col_order);
      int *r2k = LOGICAL(rows2keep);

      int cntr = 0;
      for (int j = cgs[s]; j < cgs[s + 1]; j++) {
        if (!r2k[co[j] - 1]) continue;
        res[cntr] = co[j] - 1;
        cntr++;
      }
      SETLENGTH(res_SEXP, cntr);
    }
  }

  UNPROTECT(1);
  return map;
}
