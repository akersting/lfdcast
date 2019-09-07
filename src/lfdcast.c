// required for DATAPTR
#include <Rversion.h>
#if defined(R_VERSION) && R_VERSION < R_Version(3, 5, 0)
#define USE_RINTERNALS
#endif

#include "R.h"
#include "Rinternals.h"
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>

#define n_pass_rank 4
#define n_pass_value 8
#define shift 8
#define n_bucket 256
#define mask 0xFF

struct uniqueN_data {
  uint32_t rank;
  uint64_t value;
};

void rsort(struct uniqueN_data *x, int n, int hist_rank[][n_bucket],
           int hist_value[][n_bucket]) {

  struct uniqueN_data *s = (struct uniqueN_data *) malloc(n * sizeof(struct uniqueN_data));

  int not_skip_rank[n_pass_rank];
  for (int j = 0; j < n_pass_rank; j++) {
    not_skip_rank[j] = -1;
    int cumsum = 0;
    for (int i = 0; i < n_bucket; i++) {
      if (hist_rank[j][i] != 0) not_skip_rank[j]++;
      cumsum += hist_rank[j][i];
      hist_rank[j][i] = cumsum - hist_rank[j][i];
    }
  }

  int not_skip_value[n_pass_value];
  for (int j = 0; j < n_pass_value; j++) {
    not_skip_value[j] = -1;
    int cumsum = 0;
    for (int i = 0; i < n_bucket; i++) {
      if (hist_value[j][i] != 0) not_skip_value[j]++;
      cumsum += hist_value[j][i];
      hist_value[j][i] = cumsum - hist_value[j][i];
    }
  }

  int pass = 0;
  for (int j = 0; j < n_pass_value; j++) {
    if (!not_skip_value[j]) continue;

    for (int i = 0; i < n; i++) {
      unsigned int pos = (x + i)->value >> j * shift & mask;
      s[hist_value[j][pos]++] = x[i];
    }

    struct uniqueN_data *tmp = s;
    s = x;
    x = tmp;

    pass++;
  }

  for (int j = 0; j < n_pass_rank; j++) {
    if (!not_skip_rank[j]) continue;

    for (int i = 0; i < n; i++) {
      unsigned int pos = (x + i)->rank >> j * shift & mask;
      s[hist_rank[j][pos]++] = x[i];
    }

    struct uniqueN_data *tmp = s;
    s = x;
    x = tmp;

    pass++;
  }

  if (pass % 2 == 0) {
    free(s);
  } else {
    memcpy(s, x, n * sizeof(struct uniqueN_data));
    free(x);
  }
}

void isort(struct uniqueN_data *x, int n) {
  for (int i = 1; i < n; i++) {
    struct uniqueN_data tmp = x[i];
    int j = i;
    while(j > 0 &&
          ((tmp.rank < (x + j - 1)->rank) || (tmp.rank == (x + j - 1)->rank && tmp.value < (x + j - 1)->value))) {
      x[j] = x[j - 1];
      j--;
    }
    x[j] = tmp;
  }
}

struct thread_data {
  int *agg;
  void **value_var;
  int *typeof_value_var;
  int *typeof_res;
  int *na_rm;
  int *cols_split;
  int length_cols_split;
  void **res;
  int **map_output_cols_to_input_rows;
  int *map_output_cols_to_input_rows_lengths;
  int *map_input_rows_to_output_rows;
  int n_row_output;
};


struct uniqueN_int_data {
  int rank;
  int value;
};


int uniqueN_int_cmp(const void *x, const void *y) {
  struct uniqueN_data *xx = (struct uniqueN_data *) x;
  struct uniqueN_data *yy = (struct uniqueN_data *) y;

  if (xx->rank < yy->rank) {
    return -1;
  } else if (xx->rank > yy->rank) {
    return 1;
  } else if (xx->value < yy->value) {
    return -1;
  } else if (xx->value > yy->value) {
    return 1;
  } else {
    return 0;
  }
}


struct uniqueN_double_data {
  int rank;
  double value;
};

int uniqueN_double_cmp_no_NaN(const void *x, const void *y) {
  struct uniqueN_double_data *xx = (struct uniqueN_double_data *) x;
  struct uniqueN_double_data *yy = (struct uniqueN_double_data *) y;

  if (xx->rank < yy->rank) {
    return -1;
  } else if (xx->rank > yy->rank) {
    return 1;
  } else if (xx->value < yy->value) {
    return -1;
  } else if (xx->value > yy->value) {
    return 1;
  } else {
    return 0;
  }
}

int uniqueN_double_cmp(const void *x, const void *y) {
  struct uniqueN_double_data *xx = (struct uniqueN_double_data *) x;
  struct uniqueN_double_data *yy = (struct uniqueN_double_data *) y;

  if (xx->rank < yy->rank) {
    return -1;
  } else if (xx->rank > yy->rank) {
    return 1;
  } else {
    // NA < NaN < -Inf < ...
    if (ISNA(xx->value)) {
      if (ISNA(yy->value)) {
        return 0;
      } else {
        return -1;
      }
    }

    if (R_IsNaN(xx->value)) {
      if (ISNA(yy->value)) {
        return 1;
      } else if (R_IsNaN(yy->value)) {
        return 0;
      } else
        return -1;
    }

    if (ISNAN(yy->value)) {
      return 1;
    }

    if (xx->value < yy->value) {
      return -1;
    } else if (xx->value > yy->value) {
      return 1;
    } else {
      return 0;
    }
  }
}

struct uniqueN_char_data {
  int rank;
  intptr_t value;
};


int uniqueN_char_cmp(const void *x, const void *y) {
  struct uniqueN_char_data *xx = (struct uniqueN_char_data *) x;
  struct uniqueN_char_data *yy = (struct uniqueN_char_data *) y;

  if (xx->rank < yy->rank) {
    return -1;
  } else if (xx->rank > yy->rank) {
    return 1;
  } else if (xx->value < yy->value) {
    return -1;
  } else if (xx->value > yy->value) {
    return 1;
  } else {
    return 0;
  }
}

#define INPUT_I                                                \
  (typeof_value_var[j] == LGLSXP ||                            \
   typeof_value_var[j] == INTSXP ?                             \
    (double) ((int *) input)[i] :                              \
    ((double *) input)[i])

#define ISNA_INPUT_I (((typeof_value_var[j] == INTSXP ||       \
                        typeof_value_var[j] == LGLSXP)         \
                       && ((int *)input)[i] == NA_INTEGER) ||  \
                      (typeof_value_var[j] == REALSXP &&       \
                       ISNAN(((double *)input)[i])) ||         \
                      (typeof_value_var[j] == STRSXP &&        \
                       ((SEXP *)input)[i] == NA_STRING))

#define LOOP_OVER_ROWS                                         \
  for (int ii = 0, i = map_output_cols_to_input_rows[j][0];    \
    ii < map_output_cols_to_input_rows_lengths[j];             \
    ii++, i = map_output_cols_to_input_rows[j][ii])                                                      \

#define OUTPUT_I output[map_input_rows_to_output_rows[i]]
#define HIT_I hit[map_input_rows_to_output_rows[i]]

#define FILL_OUTPUT                                            \
    for (int i = 0; i < n_row_output; i++) {                   \
      if (hit[i] > 0) continue;                                \
      output[i] = default_res;                                 \
    }

pthread_mutex_t string_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t rng_mutex = PTHREAD_MUTEX_INITIALIZER;

void *lfdcast_core(void *td_void) {
  struct thread_data *td = (struct thread_data *) td_void;
  int *agg = td->agg;
  void **value_var = td->value_var;
  int *typeof_value_var = td->typeof_value_var;
  int *typeof_res = td->typeof_res;
  int *na_rm = td->na_rm;
  int *cols_split = td->cols_split;
  int length_cols_split = td->length_cols_split;
  void **res = td->res;
  int **map_output_cols_to_input_rows = td->map_output_cols_to_input_rows;
  int *map_output_cols_to_input_rows_lengths = td->map_output_cols_to_input_rows_lengths;
  int *map_input_rows_to_output_rows = td->map_input_rows_to_output_rows;
  int n_row_output = td->n_row_output;

  int *hit = (int *) malloc(n_row_output * sizeof(int));

  for (int jj = 0; jj < length_cols_split; jj++) {
    int j = cols_split[jj];

    memset(hit, 0, n_row_output * sizeof(int));

    switch(agg[j]) {
    case 1:  // count
    {
      int *output = ((int **) res)[j];
      void *input = value_var[j];

      int default_res = output[0];
      if (default_res != 0) {
        memset(output, 0, n_row_output * sizeof(int));
      }

      LOOP_OVER_ROWS {
        if (na_rm[j] && ISNA_INPUT_I) continue;
        OUTPUT_I++;
        HIT_I = 1;
      }

      if (default_res != 0) {
        FILL_OUTPUT
      }

      break;
    }

    case 2:  // existence
    {
      int *output = ((int **) res)[j];
      void *input = value_var[j];

      LOOP_OVER_ROWS {
        if (na_rm[j] && ISNA_INPUT_I) continue;
        OUTPUT_I = TRUE;
      }

      break;
    }

    case 3:  // sum
    {
      if (typeof_res[j] == INTSXP) {
        int *output = ((int **) res)[j];
        int *input = ((int **) value_var)[j];

        int default_res = output[0];
        if (default_res != 0) {
          memset(output, 0, n_row_output * sizeof(int));
        }

        LOOP_OVER_ROWS {
          if (input[i] == NA_LOGICAL) {
            if (na_rm[j]) continue;
            OUTPUT_I = NA_INTEGER;
          } else {
            if (OUTPUT_I == NA_INTEGER) continue;
            OUTPUT_I += input[i];
          }
          HIT_I = 1;
        }

        if (default_res != 0) {
          FILL_OUTPUT
        }

      } else {
        void *input = value_var[j];
        double *output = ((double **) res)[j];

        double default_res = output[0];
        if (default_res != 0) {
          for (int i = 0; i < n_row_output; i++) {
            output[i] = 0;
          }
        }

        LOOP_OVER_ROWS {
          if (ISNA_INPUT_I) {
            if (na_rm[j]) continue;
            OUTPUT_I = NA_REAL;
          } else {
            if (ISNAN(OUTPUT_I)) continue;
            OUTPUT_I += INPUT_I;
          }
          HIT_I = 1;
        }

        if (default_res != 0) {
          FILL_OUTPUT
        }
      }

      break;
    }
    case 4:  // uniqueN
    {
      int *output = ((int **) res)[j];
      void *input = value_var[j];

      struct uniqueN_data *uniqueN_data =
        (struct uniqueN_data *) malloc(map_output_cols_to_input_rows_lengths[j] * sizeof(struct uniqueN_data));

      int (*hist_rank)[n_bucket] = malloc(sizeof(int[n_pass_rank][n_bucket]));
      int (*hist_value)[n_bucket] = malloc(sizeof(int[n_pass_value][n_bucket]));
      memset(hist_rank, 0, n_pass_rank * n_bucket * sizeof(int));
      memset(hist_value, 0, n_pass_value * n_bucket * sizeof(int));

      int uniqueN_data_length = 0;
      double zero = 0;
      LOOP_OVER_ROWS {
        if (na_rm[j] && ISNA_INPUT_I) continue;

        (uniqueN_data + uniqueN_data_length)->rank = map_input_rows_to_output_rows[i];
        if (typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) {
          (uniqueN_data + uniqueN_data_length)->value = ((int *) input)[i];
        } else if (typeof_value_var[j] == REALSXP) {
          if (((double *) input)[i] == 0) {
            (uniqueN_data + uniqueN_data_length)->value = *(uint64_t *) &(zero);
          } else {
            (uniqueN_data + uniqueN_data_length)->value = *(uint64_t *) &(((double *) input)[i]);
          }
        } else if (typeof_value_var[j] == STRSXP) {
          (uniqueN_data + uniqueN_data_length)->value = ((intptr_t *) input)[i];
        }

        for (int jj = 0; jj < n_pass_rank; jj++) {
          hist_rank[jj][(uniqueN_data + uniqueN_data_length)->rank >> jj * shift & mask]++;
        }
        for (int jj = 0; jj < n_pass_value; jj++) {
          hist_value[jj][(uniqueN_data + uniqueN_data_length)->value >> jj * shift & mask]++;
        }

        uniqueN_data_length++;
      }

      if (uniqueN_data_length > 0) {
        //isort(uniqueN_data, uniqueN_data_length);
        rsort(uniqueN_data, uniqueN_data_length, hist_rank, hist_value);
        //qsort(uniqueN_data, uniqueN_data_length, sizeof(struct uniqueN_data), uniqueN_int_cmp);

        output[(uniqueN_data)->rank] = 1;

        if (typeof_value_var[j] == REALSXP) {
          for (int i = 1; i < uniqueN_data_length; i ++) {
            if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
              double v = *(double *) &(uniqueN_data + i)->value;
              double vp = *(double *) &(uniqueN_data + i - 1)->value;
              if ((ISNA(v) != ISNA(vp)) ||
                  (R_IsNaN(v) != R_IsNaN(vp)) ||
                  (!ISNAN(v) && !ISNAN(vp) && (v != vp))) {
                output[(uniqueN_data + i)->rank]++;
              }
            } else {
              output[(uniqueN_data + i)->rank] = 1;
            }
          }
        } else {
          for (int i = 1; i < uniqueN_data_length; i ++) {
            if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
              if ((uniqueN_data + i)->value != (uniqueN_data + i - 1)->value) {
                output[(uniqueN_data + i)->rank]++;
              }
            } else {
              output[(uniqueN_data + i)->rank] = 1;
            }
          }
        }
      }

      free(uniqueN_data);
      free(hist_rank);
      free(hist_value);

      break;
    }

    case 5:  // min
    {
      if (typeof_res[j] == INTSXP) {
        int *input = ((int **) value_var)[j];
        int *output = ((int **) res)[j];

        int default_res = output[0];
        if (default_res != INT_MAX) {
          for (int i = 0; i < n_row_output; i++) {
            output[i] = INT_MAX;
          }
        }

        LOOP_OVER_ROWS {
          if (input[i] == NA_INTEGER) {
            if (na_rm[j]) continue;
            OUTPUT_I = NA_INTEGER;
          } else {
            if (OUTPUT_I == NA_INTEGER) continue;
            if (input[i] < OUTPUT_I) OUTPUT_I = input[i];
          }
          HIT_I = 1;
        }

        if (default_res != INT_MAX) {
          FILL_OUTPUT
        }

      } else {
        void *input = value_var[j];
        double *output = ((double **) res)[j];

        double default_res = output[0];
        if (default_res != R_PosInf) {
          for (int i = 0; i < n_row_output; i++) {
            output[i] = R_PosInf;
          }
        }

        LOOP_OVER_ROWS {
          if (ISNA_INPUT_I) {
            if (na_rm[j]) continue;
            OUTPUT_I = NA_REAL;
          } else {
            if (ISNAN(OUTPUT_I)) continue;
            if (INPUT_I < OUTPUT_I) OUTPUT_I = INPUT_I;
          }
          HIT_I = 1;
        }

        if (default_res != R_PosInf) {
          FILL_OUTPUT
        }
      }

      break;
    }

    case 6:  // max
    {
      if (typeof_res[j] == INTSXP) {
        int *input = ((int **) value_var)[j];
        int *output = ((int **) res)[j];

        int default_res = output[0];
        if (default_res != INT_MIN + 1) {
          for (int i = 0; i < n_row_output; i++) {
            output[i] = INT_MIN + 1;
          }
        }

        LOOP_OVER_ROWS {
          if (input[i] == NA_INTEGER) {
            if (na_rm[j]) continue;
            OUTPUT_I = NA_INTEGER;
          } else {
            if (OUTPUT_I == NA_INTEGER) continue;
            if (input[i] > OUTPUT_I) OUTPUT_I = input[i];
          }
          HIT_I = 1;
        }

        if (default_res != INT_MIN + 1) {
          FILL_OUTPUT
        }

      } else {
        void *input = value_var[j];
        double *output = ((double **) res)[j];

        double default_res = output[0];
        if (default_res != R_NegInf) {
          for (int i = 0; i < n_row_output; i++) {
            output[i] = R_NegInf;
          }
        }

        LOOP_OVER_ROWS {
          if (ISNA_INPUT_I) {
            if (na_rm[j]) continue;
            OUTPUT_I = NA_REAL;
          } else {
            if (ISNAN(OUTPUT_I)) continue;
            if (INPUT_I > OUTPUT_I) OUTPUT_I = INPUT_I;
          }
          HIT_I = 1;
        }

        if (default_res != R_NegInf) {
          FILL_OUTPUT
        }
      }

      break;
    }
    case 7:  // last
    {
      if (typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) {
        int *output = ((int **) res)[j];
        int *input = ((int **) value_var)[j];

        LOOP_OVER_ROWS {
          if (na_rm[j] && input[i] == NA_INTEGER) continue;
          OUTPUT_I = input[i];
        }
      } else if (typeof_value_var[j] == REALSXP) {
        double *output = ((double **) res)[j];
        double *input = ((double **) value_var)[j];

        LOOP_OVER_ROWS {
          if (na_rm[j] && ISNAN(input[i])) continue;
          OUTPUT_I = input[i];
        }
      } else if (typeof_value_var[j] == STRSXP) {
        int *output = ((int **) res)[j];
        SEXP *input = ((SEXP **) value_var)[j];

        LOOP_OVER_ROWS {
          if (na_rm[j] && input[i] == NA_STRING) continue;
          OUTPUT_I = i;
        }
      }

      break;
    }
    case 8: // sample
    {
      void *input = value_var[j];

      struct uniqueN_data *uniqueN_data =
        (struct uniqueN_data *) malloc(map_output_cols_to_input_rows_lengths[j] * sizeof(struct uniqueN_data));

      int (*hist_rank)[n_bucket] = malloc(sizeof(int[n_pass_rank][n_bucket]));
      int (*hist_value)[n_bucket] = malloc(sizeof(int[n_pass_value][n_bucket]));
      memset(hist_rank, 0, n_pass_rank * n_bucket * sizeof(int));
      memset(hist_value, 0, n_pass_value * n_bucket * sizeof(int));

      int uniqueN_data_length = 0;
      double zero = 0;
      LOOP_OVER_ROWS {
        if (na_rm[j] && ISNA_INPUT_I) continue;

        (uniqueN_data + uniqueN_data_length)->rank = map_input_rows_to_output_rows[i];
        if (typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) {
          (uniqueN_data + uniqueN_data_length)->value = ((int *) input)[i];
        } else if (typeof_value_var[j] == REALSXP) {
          if (((double *) input)[i] == 0) {
            (uniqueN_data + uniqueN_data_length)->value = *(uint64_t *) &(zero);
          } else {
            (uniqueN_data + uniqueN_data_length)->value = *(uint64_t *) &(((double *) input)[i]);
          }
        } else if (typeof_value_var[j] == STRSXP) {
          (uniqueN_data + uniqueN_data_length)->value = i;
        }

        for (int jj = 0; jj < n_pass_rank; jj++) {
          hist_rank[jj][(uniqueN_data + uniqueN_data_length)->rank >> jj * shift & mask]++;
        }
        for (int jj = 0; jj < n_pass_value; jj++) {
          hist_value[jj][(uniqueN_data + uniqueN_data_length)->value >> jj * shift & mask]++;
        }

        uniqueN_data_length++;
      }

      if (uniqueN_data_length > 0) {
        //isort(uniqueN_data, uniqueN_data_length);
        rsort(uniqueN_data, uniqueN_data_length, hist_rank, hist_value);
        //qsort(uniqueN_data, uniqueN_data_length, sizeof(struct uniqueN_data), uniqueN_int_cmp);

        pthread_mutex_lock(&rng_mutex);

        int cntr = 1;
        if (typeof_value_var[j] == REALSXP) {
          double *output = ((double **) res)[j];
          for (int i = 1; i < uniqueN_data_length; i ++) {
            if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
              cntr++;
            } else {
              output[(uniqueN_data + i - 1)->rank] = *(double *) &(uniqueN_data + i - 1 - (int) R_unif_index(cntr))->value;
              cntr = 1;
            }
          }
          output[(uniqueN_data + uniqueN_data_length - 1)->rank] = *(double *) &(uniqueN_data + uniqueN_data_length - 1 - (int) R_unif_index(cntr))->value;
        } else if (typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) {
          int *output = ((int **) res)[j];
          for (int i = 1; i < uniqueN_data_length; i ++) {
            if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
              cntr++;
            } else {
              output[(uniqueN_data + i - 1)->rank] = *(long *) &(uniqueN_data + i - 1 - (int) R_unif_index(cntr))->value;
              cntr = 1;
            }
          }
          output[(uniqueN_data + uniqueN_data_length - 1)->rank] = *(long *) &(uniqueN_data + uniqueN_data_length - 1 - (int) R_unif_index(cntr))->value;
        } else if (typeof_value_var[j] == STRSXP) {
          int *output = ((int **) res)[j];
          for (int i = 1; i < uniqueN_data_length; i ++) {
            if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
              cntr++;
            } else {
              output[(uniqueN_data + i - 1)->rank] = *(long *) &(uniqueN_data + i - 1 - (int) R_unif_index(cntr))->value;
              cntr = 1;
            }
          }
          output[(uniqueN_data + uniqueN_data_length - 1)->rank] = *(long *) &(uniqueN_data + uniqueN_data_length - 1 - (int) R_unif_index(cntr))->value;
        }

        pthread_mutex_unlock(&rng_mutex);
      }

      free(uniqueN_data);
      free(hist_rank);
      free(hist_value);


      break;
    }
    }
  }

  free(hit);
  return NULL;
}


SEXP lfdcast(SEXP agg, SEXP value_var, SEXP na_rm,
             SEXP map_output_cols_to_input_rows, SEXP res,
             SEXP map_output_cols_to_input_rows_lengths,
             SEXP map_input_rows_to_output_rows,
             SEXP cols_split, SEXP n_row_output_SEXP,
             SEXP nthread_SEXP) {

  void **res_ptr = (void **) R_alloc(LENGTH(res), sizeof(void *));
  for (int i = 0; i < LENGTH(res); i++) {
    if (TYPEOF(VECTOR_ELT(res, i)) == STRSXP) {
      res_ptr[i] = R_alloc(INTEGER(n_row_output_SEXP)[0], sizeof(int));
      memset(res_ptr[i], -1, INTEGER(n_row_output_SEXP)[0] * sizeof(int));
    } else {
      res_ptr[i] = DATAPTR(VECTOR_ELT(res, i));
    }
  }

  void **value_var_ptr = (void **) R_alloc(LENGTH(value_var), sizeof(void *));
  for (int i = 0; i < LENGTH(value_var); i++) {
    value_var_ptr[i] = DATAPTR(VECTOR_ELT(value_var, i));
  }

  int *typeof_value_var = (int *) R_alloc(LENGTH(value_var), sizeof(int));
  for (int i = 0; i < LENGTH(value_var); i++) {
    typeof_value_var[i] = TYPEOF(VECTOR_ELT(value_var, i));
  }

  int *typeof_res = (int *) R_alloc(LENGTH(res), sizeof(int));
  for (int i = 0; i < LENGTH(res); i++) {
    typeof_res[i] = TYPEOF(VECTOR_ELT(res, i));
  }

  int **map_output_cols_to_input_rows_ptr = (int **) R_alloc(LENGTH(map_output_cols_to_input_rows), sizeof(int *));
  for (int i = 0; i < LENGTH(map_output_cols_to_input_rows); i++) {
    map_output_cols_to_input_rows_ptr[i] = INTEGER(VECTOR_ELT(map_output_cols_to_input_rows, i));
  }

  struct thread_data td_template = {
    .agg = INTEGER(agg),
    .value_var = value_var_ptr,
    .typeof_value_var = typeof_value_var,
    .typeof_res = typeof_res,
    .na_rm = LOGICAL(na_rm),
    .cols_split = NULL,
    .length_cols_split = 0,
    .res = res_ptr,
    .map_output_cols_to_input_rows = map_output_cols_to_input_rows_ptr,
    .map_output_cols_to_input_rows_lengths = INTEGER(map_output_cols_to_input_rows_lengths),
    .map_input_rows_to_output_rows = INTEGER(map_input_rows_to_output_rows),
    .n_row_output = INTEGER(n_row_output_SEXP)[0]
  };

  int nthread = INTEGER(nthread_SEXP)[0];

  struct thread_data td[nthread];

  GetRNGstate();

  int failure = 0;
  pthread_t thread_ids[nthread];
  for (int i = 0; i < nthread; i++) {
    td[i] = td_template;
    td[i].cols_split = INTEGER(VECTOR_ELT(cols_split, i));
    td[i].length_cols_split = LENGTH(VECTOR_ELT(cols_split, i));
    if (pthread_create(thread_ids + i, NULL, lfdcast_core, (void *) (td + i)) != 0) {
      failure = 1;
      nthread = i;
      break;
    }
  }


  for (int i = 0; i < nthread; i++) {
    if (pthread_join(*(thread_ids + i), NULL) != 0) {
      failure = 1;
    }
  }

  PutRNGstate();

  if (failure) error("something went wrong");

  for (int j = 0; j < LENGTH(res); j++) {
    if (TYPEOF(VECTOR_ELT(res, j)) != STRSXP) continue;
    int *output = ((int **) res_ptr)[j];
    for (int i = 0; i < INTEGER(n_row_output_SEXP)[0]; i++) {
      if (output[i] < 0) continue;
      SET_STRING_ELT(VECTOR_ELT(res, j), i, STRING_ELT(VECTOR_ELT(value_var, i), output[i]));
    }
  }

  return res;
}


SEXP get_row_ranks_unique_pos(SEXP x_SEXP, SEXP res_SEXP) {
  int *x = INTEGER(x_SEXP);
  int *res = INTEGER(res_SEXP);

  for (int i = 0; i < LENGTH(x_SEXP); i++) {
    res[x[i]] = i + 1;
  }

  return res_SEXP;
}
