#include "lfdcast.h"

int existence_(void *res, int typeof_res, void *value_var, int typeof_value_var,
               int na_rm, int *input_rows_in_output_col, int n_input_rows_in_output_col,
               int *map_input_rows_to_output_rows, int n_row_output, int *hit) {

  int *output = (int *) res;
  void *input = value_var;

  LOOP_OVER_ROWS {
    if (na_rm && ISNA_INPUT_I) continue;
    OUTPUT_I = TRUE;
  }

  return 0;
}

struct lfdcast_agg existence = {
  .fun = existence_
};
