#ifndef LFDCAST_H
#define LFDCAST_H

struct lfdcast_agg {
   int (*fun)(void *res, int typeof_res, void *value_var, int typeof_value_var,
        int na_rm, int *input_rows_in_output_col, int n_input_rows_in_output_col,
        int *map_input_rows_to_output_rows, int n_row_output, int *hit);
};

#define INPUT_I                                                \
(typeof_value_var == LGLSXP ||                              \
  typeof_value_var == INTSXP ?                              \
  (double) ((int *) input)[i] :                                \
   ((double *) input)[i])

#define ISNA_INPUT_I (((typeof_value_var == INTSXP ||       \
 typeof_value_var == LGLSXP)                                \
   && ((int *)input)[i] == NA_INTEGER) ||                      \
     (typeof_value_var == REALSXP &&                        \
     ISNAN(((double *)input)[i])) ||                           \
     (typeof_value_var == STRSXP &&                         \
     ((SEXP *)input)[i] == NA_STRING))

#define LOOP_OVER_ROWS                                                                                       \
   for (int ii = 0, i = input_rows_in_output_col[0];                                                 \
        ii < n_input_rows_in_output_col && ((i = input_rows_in_output_col[ii]) || TRUE);                                                       \
        ii++)                                                      \

#define OUTPUT_I output[map_input_rows_to_output_rows[i]]
#define HIT_I hit[map_input_rows_to_output_rows[i]]

#define FILL_OUTPUT                                            \
   for (int i = 0; i < n_row_output; i++) {                    \
     if (hit[i] > 0) continue;                                 \
     output[i] = default_res;                                  \
   }


#endif
