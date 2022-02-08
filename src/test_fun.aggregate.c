#include "lfdcast.h"
#include "list.h"
#include "R.h"

SEXP test_fun_aggregate(SEXP agg, SEXP res, SEXP value_var, SEXP na_rm,
                        SEXP input_rows_in_output_col,
                        SEXP map_input_rows_to_output_rows) {
  int n_row_output = LENGTH(VECTOR_ELT(res, 0));
  int typeof_res = TYPEOF(VECTOR_ELT(res, 0));
  lfdcast_agg_fun_t fun = ((struct lfdcast_agg *) R_ExternalPtrAddr(VECTOR_ELT(agg, 0)))->fun;

  int *hit = (int *) malloc((size_t) n_row_output * sizeof(int));
  if (hit == NULL) error("'malloc' failed");
  memset(hit, 0, (size_t) n_row_output * sizeof(int));


  void *res_ptr = (void *) R_alloc((size_t) 1, sizeof(void *));
  if (TYPEOF(VECTOR_ELT(res, 0)) == STRSXP) {
    res_ptr = R_alloc((size_t) n_row_output, sizeof(int));
    memset(res_ptr, -1, (size_t) n_row_output * sizeof(int));
  } else if (TYPEOF(VECTOR_ELT(res, 0)) == VECSXP) {
    int n = n_row_output;
    res_ptr = R_alloc((size_t) n, sizeof(int **));
    int **col = res_ptr;
    for (int j = 0; j < n; j++) {
      col[j] = NULL;
    }
  } else {
    res_ptr = DATAPTR(VECTOR_ELT(res, 0));
  }


  fun(res_ptr, typeof_res, DATAPTR(value_var), TYPEOF(value_var),
      INTEGER(na_rm)[0], INTEGER(input_rows_in_output_col),
      LENGTH(input_rows_in_output_col), INTEGER(map_input_rows_to_output_rows),
      n_row_output, hit);

  free(hit);

  if (TYPEOF(VECTOR_ELT(res, 0)) == STRSXP) {
    int_res_to_char_res(res_ptr, VECTOR_ELT(res, 0), value_var, LENGTH(VECTOR_ELT(res, 0)));
  } else if (TYPEOF(VECTOR_ELT(res, 0)) == VECSXP) {
    allocate_list_res(res_ptr, VECTOR_ELT(res, 0), value_var);
  }

  return res;
}
