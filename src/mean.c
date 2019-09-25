#include "lfdcast.h"

int mean_(void *restrict res, const int typeof_res, const void *restrict value_var,
         const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
         const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
         const int n_row_output, int *restrict hit) {

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
    HIT_I++;
  }

  if (default_res != 0) {
    for (int i = 0; i < n_row_output; i++) {
      if (hit[i] > 0) {
        output[i] /= hit[i];
      } else {
        output[i] = default_res;
      }
    }
  } else {
    for (int i = 0; i < n_row_output; i++) {
      if (hit[i] > 0) output[i] /= hit[i];
    }
  }

  return 0;
}

struct lfdcast_agg mean = {
  .fun = mean_
};
