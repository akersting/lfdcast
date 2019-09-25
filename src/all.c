#include "lfdcast.h"

int all_(void *restrict res, const int typeof_res, const void *restrict value_var,
         const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
         const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
         const int n_row_output, int *restrict hit) {

  int *restrict output = (int *) res;
  const int *restrict input = (int *) value_var;

  int default_res = output[0];
  if (default_res != TRUE) {
    for (int i = 0; i < n_row_output; i++) {
      output[i] = TRUE;
    }
  }

  LOOP_OVER_ROWS {
    if (input[i] == NA_INTEGER) {
      if (na_rm) continue;
      if (OUTPUT_I == TRUE) OUTPUT_I = NA_INTEGER;
    } else if (!input[i]) {
      OUTPUT_I = FALSE;
    }
    HIT_I = 1;
  }

  if (default_res != TRUE) {
    FILL_OUTPUT
  }

  return 0;
}

struct lfdcast_agg all = {
  .fun = all_
};
