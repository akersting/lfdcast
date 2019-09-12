#include "lfdcast.h"

int count_(void *restrict res, const int typeof_res, const void *restrict value_var,
           const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
           const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
           const int n_row_output, int *restrict hit) {

  int *restrict output = (int *) res;
  const void *restrict input = value_var;

  int default_res = output[0];
  if (default_res != 0) {
    memset(output, 0, n_row_output * sizeof(int));
  }

  LOOP_OVER_ROWS {
    if (na_rm && ISNA_INPUT_I) continue;
    OUTPUT_I++;
    HIT_I = 1;
  }

  if (default_res != 0) {
    FILL_OUTPUT
  }

  return 0;
}

struct lfdcast_agg count = {
  .fun = count_
};
