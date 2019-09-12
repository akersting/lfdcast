#include "lfdcast.h"

int min_(void *restrict res, const int typeof_res, const void *restrict value_var,
         const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
         const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
         const int n_row_output, int *restrict hit) {

  if (typeof_res == INTSXP) {
    const int *restrict input = (int *) value_var;
    int *restrict output = (int *) res;

    int default_res = output[0];
    if (default_res != INT_MAX) {
      for (int i = 0; i < n_row_output; i++) {
        output[i] = INT_MAX;
      }
    }

    LOOP_OVER_ROWS {
      if (input[i] == NA_INTEGER) {
        if (na_rm) continue;
        OUTPUT_I = NA_INTEGER;
      } else {
        if (OUTPUT_I == NA_INTEGER) continue;
        if (input[i] < OUTPUT_I) OUTPUT_I = input[i];
      }
      HIT_I = 1;
    }

    if (default_res != INT_MAX) {
      FILL_OUTPUT
    }

  } else {
    const void *restrict input = value_var;
    double *restrict output = (double *) res;

    double default_res = output[0];
    if (default_res != R_PosInf) {
      for (int i = 0; i < n_row_output; i++) {
        output[i] = R_PosInf;
      }
    }

    LOOP_OVER_ROWS {
      if (ISNA_INPUT_I) {
        if (na_rm) continue;
        OUTPUT_I = NA_REAL;
      } else {
        if (ISNAN(OUTPUT_I)) continue;
        if (INPUT_I < OUTPUT_I) OUTPUT_I = INPUT_I;
      }
      HIT_I = 1;
    }

    if (default_res != R_PosInf) {
      FILL_OUTPUT
    }
  }

  return 0;
}

struct lfdcast_agg min = {
  .fun = min_
};
