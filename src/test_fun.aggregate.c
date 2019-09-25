#include "lfdcast.h"
#include "R.h"

SEXP test_fun_aggregate(SEXP agg, SEXP res, SEXP value_var, SEXP na_rm,
                        SEXP input_rows_in_output_col,
                        SEXP map_input_rows_to_output_rows) {
  int n_row_output = LENGTH(VECTOR_ELT(res, 0));
  int typeof_res = TYPEOF(VECTOR_ELT(res, 0));
  lfdcast_agg_fun_t fun = ((struct lfdcast_agg *) R_ExternalPtrAddr(VECTOR_ELT(agg, 0)))->fun;

  int *hit = (int *) malloc(n_row_output * sizeof(int));
  memset(hit, 0, n_row_output * sizeof(int));

  fun(DATAPTR(VECTOR_ELT(res, 0)), typeof_res, DATAPTR(value_var), TYPEOF(value_var),
      INTEGER(na_rm)[0], INTEGER(input_rows_in_output_col),
      LENGTH(input_rows_in_output_col), INTEGER(map_input_rows_to_output_rows),
      n_row_output, hit);

  free(hit);

  return res;
}
