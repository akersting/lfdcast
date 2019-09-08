#include "lfdcast.h"

int count_(void *res, int typeof_res, void *value_var, int typeof_value_var,
           int na_rm, int *input_rows_in_output_col, int n_input_rows_in_output_col,
           int *map_input_rows_to_output_rows, int n_row_output, int *hit) {

  int *output = (int *) res;
  void *input = value_var;

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
