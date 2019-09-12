#include "lfdcast.h"

int sum_(void *restrict res, const int typeof_res, const void *restrict value_var,
         const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
         const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
         const int n_row_output, int *restrict hit) {

  if (typeof_res == INTSXP) {
    int *restrict output = (int *) res;
    const int *restrict input = (int *) value_var;

    int default_res = output[0];
    if (default_res != 0) {
      memset(output, 0, n_row_output * sizeof(int));
    }

    LOOP_OVER_ROWS {
      if (input[i] == NA_LOGICAL) {
        if (na_rm) continue;
        OUTPUT_I = NA_INTEGER;
      } else {
        if (OUTPUT_I == NA_INTEGER) continue;
        OUTPUT_I += input[i];
      }
      HIT_I = 1;
    }

    if (default_res != 0) {
      FILL_OUTPUT
    }

  } else {
    const void *restrict input = value_var;
    double *restrict output = (double *) res;

    double default_res = output[0];
    if (default_res != 0) {
      for (int i = 0; i < n_row_output; i++) {
        output[i] = 0;
      }
    }

    LOOP_OVER_ROWS {
      if (ISNA_INPUT_I) {
        if (na_rm) continue;
        OUTPUT_I = NA_REAL;
      } else {
        if (ISNAN(OUTPUT_I)) continue;
        OUTPUT_I += INPUT_I;
      }
      HIT_I = 1;
    }

    if (default_res != 0) {
      FILL_OUTPUT
    }
  }

  return 0;
}

struct lfdcast_agg sum = {
  .fun = sum_
};
