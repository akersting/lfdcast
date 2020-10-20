#include "lfdcast.h"
#include <R_ext/Rdynload.h>

void R_init_lfdcast(DllInfo *info) {
  static const R_CallMethodDef callMethods[]  = {
    {"lfdcast", (DL_FUNC) &lfdcast, 10},
    {"get_row_ranks_unique_pos", (DL_FUNC) &get_row_ranks_unique_pos, 2},
    {"get_map_output_cols_to_input_rows", (DL_FUNC) &get_map_output_cols_to_input_rows, 5},
    {NULL, NULL, 0}
  };

  R_registerRoutines(info, NULL, callMethods, NULL, NULL);
  R_useDynamicSymbols(info, TRUE);
  R_forceSymbols(info, FALSE);
}
