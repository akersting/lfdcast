#include "R.h"
#include "Rinternals.h"

#include "../inst/include/lfdcast.h"

SEXP lfdcast(SEXP agg, SEXP value_var, SEXP na_rm,
             SEXP map_output_cols_to_input_rows, SEXP res,
             SEXP map_output_cols_to_input_rows_lengths,
             SEXP map_input_rows_to_output_rows,
             SEXP cols_split, SEXP n_row_output_SEXP,
             SEXP nthread_SEXP);

SEXP get_row_ranks_unique_pos(SEXP x_SEXP, SEXP res_SEXP);
SEXP get_map_output_cols_to_input_rows(SEXP ranks_SEXP,
                                       SEXP ncols_SEXP,
                                       SEXP rows2keep_SEXP);

void int_res_to_char_res(const void *restrict res_ptr_j, SEXP res_j, SEXP value_var_j, int n);
