#ifndef LFDCAST_H
#define LFDCAST_H

typedef char *(*lfdcast_agg_fun_t)(void *restrict res, const int typeof_res, const void *restrict value_var,
               const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
               const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
               const int n_row_output, int *restrict hit);

struct lfdcast_agg {
   lfdcast_agg_fun_t fun;
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
