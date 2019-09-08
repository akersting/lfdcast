#include "lfdcast.h"

int min_(void *res, int typeof_res, void *value_var, int typeof_value_var,
         int na_rm, int *input_rows_in_output_col, int n_input_rows_in_output_col,
         int *map_input_rows_to_output_rows, int n_row_output, int *hit) {

  if (typeof_res == INTSXP) {
    int *input = (int *) value_var;
    int *output = (int *) res;

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
    void *input = value_var;
    double *output = (double *) res;

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
