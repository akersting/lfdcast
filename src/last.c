#include "lfdcast.h"

int last_(void *res, int typeof_res, void *value_var, int typeof_value_var,
          int na_rm, int *input_rows_in_output_col, int n_input_rows_in_output_col,
          int *map_input_rows_to_output_rows, int n_row_output, int *hit) {

  if (typeof_value_var == LGLSXP || typeof_value_var == INTSXP) {
    int *output = (int *) res;
    int *input = (int *) value_var;

    LOOP_OVER_ROWS {
      if (na_rm && input[i] == NA_INTEGER) continue;
      OUTPUT_I = input[i];
    }
  } else if (typeof_value_var == REALSXP) {
    double *output = (double *) res;
    double *input = (double *) value_var;

    LOOP_OVER_ROWS {
      if (na_rm && ISNAN(input[i])) continue;
      OUTPUT_I = input[i];
    }
  } else if (typeof_value_var == STRSXP) {
    int *output = (int *) res;
    SEXP *input = (SEXP *) value_var;

    LOOP_OVER_ROWS {
      if (na_rm && input[i] == NA_STRING) continue;
      OUTPUT_I = i;
    }
  }

  return 0;
}

struct lfdcast_agg last = {
  .fun = last_
};
