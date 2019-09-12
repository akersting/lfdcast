#include "lfdcast.h"

int existence_(void *restrict res, const int typeof_res, const void *restrict value_var,
               const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
               const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
               const int n_row_output, int *restrict hit) {

  int *restrict output = (int *) res;
  const void *restrict input = value_var;

  LOOP_OVER_ROWS {
    if (na_rm && ISNA_INPUT_I) continue;
    OUTPUT_I = TRUE;
  }

  return 0;
}

struct lfdcast_agg existence = {
  .fun = existence_
};
