#include "lfdcast.h"

int first_(void *restrict res, const int typeof_res, const void *restrict value_var,
          const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
          const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
          const int n_row_output, int *restrict hit) {

  if (typeof_value_var == LGLSXP || typeof_value_var == INTSXP) {
    int *restrict output = (int *) res;
    const int *restrict input = (int *) value_var;

    LOOP_OVER_ROWS {
      if (na_rm && input[i] == NA_INTEGER) continue;
      if (HIT_I == 0) {
        OUTPUT_I = input[i];
        HIT_I = 1;
      }
    }
  } else if (typeof_value_var == REALSXP) {
    double *restrict output = (double *) res;
    const double *restrict input = (double *) value_var;

    LOOP_OVER_ROWS {
      if (na_rm && ISNAN(input[i])) continue;
      if (HIT_I == 0) {
        OUTPUT_I = input[i];
        HIT_I = 1;
      }
    }
  } else if (typeof_value_var == STRSXP) {
    int *output = (int *) res;
    SEXP *input = (SEXP *) value_var;

    LOOP_OVER_ROWS {
      if (na_rm && input[i] == NA_STRING) continue;
      if (HIT_I == 0) {
        OUTPUT_I = i;
        HIT_I = 1;
      }
    }
  }

  return 0;
}

struct lfdcast_agg first = {
  .fun = first_
};
