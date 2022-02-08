#ifndef LFDCAST_H
#define LFDCAST_H

#include <Rinternals.h>

#if __has_include(<immintrin.h>)
#include <immintrin.h>
#endif

typedef char *(*lfdcast_agg_fun_t)(void *restrict res, const int typeof_res, const void *restrict value_var,
               const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
               const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
               const int n_row_output, int *restrict hit);

struct lfdcast_agg {
   lfdcast_agg_fun_t fun;
};

#define INPUT_I                  \
(typeof_value_var == LGLSXP ||   \
   typeof_value_var == INTSXP ?  \
   (double) ((int *) input)[i] : \
    ((double *) input)[i])

#define ISNA_INPUT_I (((typeof_value_var == INTSXP || \
 typeof_value_var == LGLSXP)                          \
    && ((int *)input)[i] == NA_INTEGER) ||            \
       (typeof_value_var == REALSXP &&                \
       ISNAN(((double *)input)[i])) ||                \
       (typeof_value_var == STRSXP &&                 \
       ((SEXP *)input)[i] == NA_STRING))

#define LOOP_OVER_ROWS                                                                    \
    for (int ii = 0, i = input_rows_in_output_col[0];                                     \
         ii < n_input_rows_in_output_col && ((i = input_rows_in_output_col[ii]) || TRUE); \
         ii++)                                                                            \

static inline int sizeof_type(int type) {
   switch(type) {
   case LGLSXP: return sizeof(int);
   case INTSXP: return sizeof(int);
   case REALSXP: return sizeof(double);
   case STRSXP: return sizeof(void *);
   default: return 0; // #nocov
   }
}

#define IS_VOID_PTR(x) _Generic((x), const void *: 1, void *: 1, default: 0)

#define PREFETCH_NOTHING 0
#define PREFETCH_INPUT   1
#define PREFETCH_OUTPUT  2
#define PREFETCH_HIT     4

#define PREFETCH_EXPR(...) __VA_ARGS__

#if __has_include(<immintrin.h>)
#define LOOP_OVER_ROWS_W_PREFETCH(N, WHAT, EXPR)                                                                                 \
   if (N > 0) {                                                                                                                  \
      for (int ii = 0; ii < N && ii < (n_input_rows_in_output_col - N); ii++) {                                                  \
         if ((WHAT) & PREFETCH_OUTPUT || (WHAT) & PREFETCH_HIT) {                                                                \
            _mm_prefetch(map_input_rows_to_output_rows + input_rows_in_output_col[ii], _MM_HINT_T0);                             \
         }                                                                                                                       \
         if ((WHAT) & PREFETCH_INPUT) {                                                                                          \
            if (IS_VOID_PTR(input)) {                                                                                            \
               _mm_prefetch((char *) input + sizeof_type(typeof_value_var) * input_rows_in_output_col[ii], _MM_HINT_T0);         \
            } else {                                                                                                             \
               _Pragma("GCC diagnostic push")                                                                                    \
               _Pragma("GCC diagnostic ignored \"-Wpointer-arith\"")                                                             \
               _mm_prefetch(input + input_rows_in_output_col[ii], _MM_HINT_T0);                                                  \
               _Pragma("GCC diagnostic pop")                                                                                     \
            }                                                                                                                    \
         }                                                                                                                       \
         if ((WHAT) & PREFETCH_OUTPUT) {                                                                                         \
            _mm_prefetch(output + map_input_rows_to_output_rows[input_rows_in_output_col[ii]], _MM_HINT_T0);                     \
         }                                                                                                                       \
         if ((WHAT) & PREFETCH_HIT) {                                                                                            \
            _mm_prefetch(hit + map_input_rows_to_output_rows[input_rows_in_output_col[ii]], _MM_HINT_T0);                        \
         }                                                                                                                       \
      }                                                                                                                          \
      for (int ii = 0, i; ii < (n_input_rows_in_output_col - N); ii++) {                                                         \
         if ((WHAT) & PREFETCH_OUTPUT || (WHAT) & PREFETCH_HIT) {                                                                \
            _mm_prefetch(map_input_rows_to_output_rows + input_rows_in_output_col[ii + N], _MM_HINT_T0);                         \
         }                                                                                                                       \
         if ((WHAT) & PREFETCH_INPUT) {                                                                                          \
            if (IS_VOID_PTR(input)) {                                                                                            \
               _mm_prefetch((char *) input + sizeof_type(typeof_value_var) * input_rows_in_output_col[ii + N / 2], _MM_HINT_T0); \
            } else {                                                                                                             \
               _Pragma("GCC diagnostic push")                                                                                    \
               _Pragma("GCC diagnostic ignored \"-Wpointer-arith\"")                                                             \
               _mm_prefetch(input + input_rows_in_output_col[ii + N / 2], _MM_HINT_T0);                                          \
               _Pragma("GCC diagnostic pop")                                                                                     \
            }                                                                                                                    \
         }                                                                                                                       \
         if ((WHAT) & PREFETCH_OUTPUT) {                                                                                         \
            _mm_prefetch(output + map_input_rows_to_output_rows[input_rows_in_output_col[ii + N / 2]], _MM_HINT_T0);             \
         }                                                                                                                       \
         if ((WHAT) & PREFETCH_HIT) {                                                                                            \
            _mm_prefetch(hit + map_input_rows_to_output_rows[input_rows_in_output_col[ii + N / 2]], _MM_HINT_T0);                \
         }                                                                                                                       \
         i = input_rows_in_output_col[ii];                                                                                       \
         EXPR                                                                                                                    \
      }                                                                                                                          \
      for (int ii = (n_input_rows_in_output_col - N > 0 ? n_input_rows_in_output_col - N : 0), i;                                \
           ii < n_input_rows_in_output_col; ii++) {                                                                              \
         i = input_rows_in_output_col[ii];                                                                                       \
         EXPR                                                                                                                    \
      }                                                                                                                          \
   } else {                                                                                                                      \
      for (int ii = 0, i; ii < n_input_rows_in_output_col; ii++) {                                                               \
         i = input_rows_in_output_col[ii];                                                                                       \
         EXPR                                                                                                                    \
      }                                                                                                                          \
   }
#else
#define LOOP_OVER_ROWS_W_PREFETCH(N, WHAT, EXPR)                \
   for (int ii = 0, i; ii < n_input_rows_in_output_col; ii++) { \
      i = input_rows_in_output_col[ii];                         \
      EXPR                                                      \
   }
#endif


#define OUTPUT_I output[map_input_rows_to_output_rows[i]]
#define HIT_I hit[map_input_rows_to_output_rows[i]]

#define FILL_OUTPUT                         \
   for (int i = 0; i < n_row_output; i++) { \
     if (hit[i] > 0) continue;              \
     output[i] = default_res;               \
   }


#endif
