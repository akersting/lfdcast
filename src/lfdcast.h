#include "../inst/include/lfdcast.h"

// required for DATAPTR
#include <Rversion.h>
#if defined(R_VERSION) && R_VERSION < R_Version(3, 5, 0)
#define USE_RINTERNALS
#endif

#include "R.h"
#include "Rinternals.h"

SEXP lfdcast(SEXP agg, SEXP value_var, SEXP na_rm,
             SEXP map_output_cols_to_input_rows, SEXP res,
             SEXP map_output_cols_to_input_rows_lengths,
             SEXP map_input_rows_to_output_rows,
             SEXP cols_split, SEXP n_row_output_SEXP,
             SEXP nthread_SEXP);

SEXP uniqueN_vec(SEXP x, SEXP na_rm_);
SEXP char_map(SEXP x);

SEXP get_row_ranks_unique_pos(SEXP x_SEXP, SEXP res_SEXP);
