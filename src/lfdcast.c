// required for DATAPTR
#include <Rversion.h>
#if defined(R_VERSION) && R_VERSION < R_Version(3, 5, 0)
#define USE_RINTERNALS
#endif

#include "Rinternals.h"
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>

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
  struct uniqueN_int_data *xx = (struct uniqueN_int_data *) x;
  struct uniqueN_int_data *yy = (struct uniqueN_int_data *) y;

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


#define LOOP_OVER_ROWS                                         \
  for (int ii = 0, i = map_output_cols_to_input_rows[j][0];    \
    ii < map_output_cols_to_input_rows_lengths[j];             \
    ii++, i = map_output_cols_to_input_rows[j][ii])                                                      \


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

  for (int jj = 0; jj < length_cols_split; jj++) {
    int j = cols_split[jj];

    switch(agg[j]) {
    case 1:  // count
    {
      int *output = ((int **) res)[j];

      if (output[0] == 0) {
        if (na_rm[j]) {
          if (typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) {
            int *input = ((int **) value_var)[j];
            LOOP_OVER_ROWS {
              if (input[i] != NA_INTEGER) {
                output[map_input_rows_to_output_rows[i]]++;
              }
            }
          } else if (typeof_value_var[j] == REALSXP) {
            double *input = ((double **) value_var)[j];
            LOOP_OVER_ROWS {
              if (!ISNAN(input[i])) {
                output[map_input_rows_to_output_rows[i]]++;
              }
            }
          } else if (typeof_value_var[j] == STRSXP) {
            SEXP *input = ((SEXP **) value_var)[j];
            LOOP_OVER_ROWS {
              if (input[i] != NA_STRING) {
                output[map_input_rows_to_output_rows[i]]++;
              }
            }
          }
        } else {
          LOOP_OVER_ROWS {
            output[map_input_rows_to_output_rows[i]]++;
          }
        }
      } else {
        char *hit = (char *) calloc(n_row_output, sizeof(char));

        if (na_rm[j]) {
          if (typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) {
            int *input = ((int **) value_var)[j];
            LOOP_OVER_ROWS {
              if (input[i] != NA_INTEGER) {
                if (hit[map_input_rows_to_output_rows[i]]) {
                  output[map_input_rows_to_output_rows[i]]++;
                } else {
                  output[map_input_rows_to_output_rows[i]] = 1;
                  hit[map_input_rows_to_output_rows[i]] = 1;
                }
              }
            }
          } else if (typeof_value_var[j] == REALSXP) {
            double *input = ((double **) value_var)[j];
            LOOP_OVER_ROWS {
              if (!ISNAN(input[i])) {
                if (hit[map_input_rows_to_output_rows[i]]) {
                  output[map_input_rows_to_output_rows[i]]++;
                } else {
                  output[map_input_rows_to_output_rows[i]] = 1;
                  hit[map_input_rows_to_output_rows[i]] = 1;
                }
              }
            }
          } else if (typeof_value_var[j] == STRSXP) {
            SEXP *input = ((SEXP **) value_var)[j];
            LOOP_OVER_ROWS {
              if (input[i] != NA_STRING) {
                if (hit[map_input_rows_to_output_rows[i]]) {
                  output[map_input_rows_to_output_rows[i]]++;
                } else {
                  output[map_input_rows_to_output_rows[i]] = 1;
                  hit[map_input_rows_to_output_rows[i]] = 1;
                }
              }
            }
          }
        } else {
          LOOP_OVER_ROWS {
            if (hit[map_input_rows_to_output_rows[i]]) {
              output[map_input_rows_to_output_rows[i]]++;
            } else {
              output[map_input_rows_to_output_rows[i]] = 1;
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          }
        }
        free(hit);
      }


      break;
    }

    case 2:  // existence
    {
      int *output = ((int **) res)[j];

      if (na_rm[j]) {
        if (typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) {
          int *input = ((int **) value_var)[j];
          LOOP_OVER_ROWS {
            if (input[i] != NA_INTEGER) {
              output[map_input_rows_to_output_rows[i]] = TRUE;
            }
          }
        } else if (typeof_value_var[j] == REALSXP) {
          double *input = ((double **) value_var)[j];
          LOOP_OVER_ROWS {
            if (!ISNAN(input[i])) {
              output[map_input_rows_to_output_rows[i]] = TRUE;
            }
          }
        } else if (typeof_value_var[j] == STRSXP) {
          SEXP *input = ((SEXP **) value_var)[j];
          LOOP_OVER_ROWS {
            if (input[i] != NA_STRING) {
              output[map_input_rows_to_output_rows[i]] = TRUE;
            }
          }
        }
      } else {
        LOOP_OVER_ROWS {
          output[map_input_rows_to_output_rows[i]] = TRUE;
        }
      }

      break;
    }

    case 3:  // sum
    {
      if (typeof_value_var[j] == LGLSXP) {
        int *output = ((int **) res)[j];
        int *input = ((int **) value_var)[j];

        if (output[0] == 0) {
          if (na_rm[j]) {
            LOOP_OVER_ROWS {
              if (input[i] != NA_LOGICAL) {
                output[map_input_rows_to_output_rows[i]] += input[i];
              }
            }
          } else {
            LOOP_OVER_ROWS {
              if (output[map_input_rows_to_output_rows[i]] != NA_INTEGER) {
                if (input[i] != NA_LOGICAL) {
                  output[map_input_rows_to_output_rows[i]] += input[i];
                } else {
                  output[map_input_rows_to_output_rows[i]] = NA_INTEGER;
                }
              }
            }
          }
        } else {
          char *hit = (char *) calloc(n_row_output, sizeof(char));

          if (na_rm[j]) {
            LOOP_OVER_ROWS {
              if (input[i] != NA_LOGICAL) {
                if (hit[map_input_rows_to_output_rows[i]]) {
                  output[map_input_rows_to_output_rows[i]] += input[i];
                } else {
                  output[map_input_rows_to_output_rows[i]] = input[i];
                  hit[map_input_rows_to_output_rows[i]] = 1;
                }
              }
            }
          } else {
            LOOP_OVER_ROWS {
              if (hit[map_input_rows_to_output_rows[i]]) {
                if (output[map_input_rows_to_output_rows[i]] != NA_INTEGER) {
                  if (input[i] != NA_LOGICAL) {
                    output[map_input_rows_to_output_rows[i]] += input[i];
                  } else {
                    output[map_input_rows_to_output_rows[i]] = NA_INTEGER;
                  }
                }
              } else {
                output[map_input_rows_to_output_rows[i]] = input[i];
                hit[map_input_rows_to_output_rows[i]] = 1;
              }
            }
          }

          free(hit);
        }

      } else if (typeof_value_var[j] == INTSXP) {
        double *output = ((double **) res)[j];
        int *input = ((int **) value_var)[j];

        if (output[0] == 0) {
          if (na_rm[j]) {
            LOOP_OVER_ROWS {
              if (input[i] != NA_INTEGER) {
                output[map_input_rows_to_output_rows[i]] += input[i];
              }
            }
          } else {
            LOOP_OVER_ROWS {
              if (!ISNAN(output[map_input_rows_to_output_rows[i]])) {
                if (input[i] != NA_INTEGER) {
                  output[map_input_rows_to_output_rows[i]] += input[i];
                } else {
                  output[map_input_rows_to_output_rows[i]] = NA_REAL;
                }
              }
            }
          }
        } else {
          char *hit = (char *) calloc(n_row_output, sizeof(char));

          if (na_rm[j]) {
            LOOP_OVER_ROWS {
              if (input[i] != NA_INTEGER) {
                if (hit[map_input_rows_to_output_rows[i]]) {
                  output[map_input_rows_to_output_rows[i]] += input[i];
                } else {
                  output[map_input_rows_to_output_rows[i]] = input[i];
                  hit[map_input_rows_to_output_rows[i]] = 1;
                }
              }
            }
          } else {
            LOOP_OVER_ROWS {
              if (hit[map_input_rows_to_output_rows[i]]) {
                if (!ISNAN(output[map_input_rows_to_output_rows[i]])) {
                  if (input[i] != NA_INTEGER) {
                    output[map_input_rows_to_output_rows[i]] += input[i];
                  } else {
                    output[map_input_rows_to_output_rows[i]] = NA_REAL;
                  }
                }
              } else {
                if (input[i] != NA_INTEGER) {
                  output[map_input_rows_to_output_rows[i]] = input[i];
                } else {
                  output[map_input_rows_to_output_rows[i]] = NA_REAL;
                }
                hit[map_input_rows_to_output_rows[i]] = 1;
              }
            }
          }

          free(hit);
        }

      } else if (typeof_value_var[j] == REALSXP) {
        double *output = ((double **) res)[j];
        double *input = ((double **) value_var)[j];

        if (output[0] == 0) {
          if (na_rm[j]) {
            LOOP_OVER_ROWS {
              if (!ISNAN(input[i])) {
                output[map_input_rows_to_output_rows[i]] += input[i];
              }
            }
          } else {
            LOOP_OVER_ROWS {
              output[map_input_rows_to_output_rows[i]] += input[i];
            }
          }
        } else {
          char *hit = (char *) calloc(n_row_output, sizeof(char));

          if (na_rm[j]) {
            LOOP_OVER_ROWS {
              if (!ISNAN(input[i])) {
                if (hit[map_input_rows_to_output_rows[i]]) {
                  output[map_input_rows_to_output_rows[i]] += input[i];
                } else {
                  output[map_input_rows_to_output_rows[i]] = input[i];
                  hit[map_input_rows_to_output_rows[i]] = 1;
                }
              }
            }
          } else {
            LOOP_OVER_ROWS {
              if (hit[map_input_rows_to_output_rows[i]]) {
                output[map_input_rows_to_output_rows[i]] += input[i];
              } else {
                output[map_input_rows_to_output_rows[i]] = input[i];
                hit[map_input_rows_to_output_rows[i]] = 1;
              }
            }
          }

          free(hit);
        }
      }

      break;
    }

    case 4:  // uniqueN
    {
      if (typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) {
        int *output = ((int **) res)[j];
        int *input = ((int **) value_var)[j];

        struct uniqueN_int_data *uniqueN_data =
          (struct uniqueN_int_data *) malloc(map_output_cols_to_input_rows_lengths[j] * sizeof(struct uniqueN_int_data));

        int uniqueN_data_length = 0;
        if (na_rm[j]) {
          LOOP_OVER_ROWS {
            if (input[i] != NA_INTEGER) {
              (uniqueN_data + uniqueN_data_length)->rank = map_input_rows_to_output_rows[i];
              (uniqueN_data + uniqueN_data_length)->value = input[i];
              uniqueN_data_length++;
            }
          }
        } else {
          LOOP_OVER_ROWS {
            (uniqueN_data + ii)->rank = map_input_rows_to_output_rows[i];
            (uniqueN_data + ii)->value = input[i];
          }
          uniqueN_data_length = map_output_cols_to_input_rows_lengths[j];
        }

        if (uniqueN_data_length > 0) {
          qsort(uniqueN_data, uniqueN_data_length, sizeof(struct uniqueN_int_data), uniqueN_int_cmp);

          output[(uniqueN_data)->rank] = 1;
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

        free(uniqueN_data);

      } else if (typeof_value_var[j] == REALSXP) {
        int *output = ((int **) res)[j];
        double *input = ((double **) value_var)[j];

        struct uniqueN_double_data *uniqueN_data =
          (struct uniqueN_double_data *) malloc(map_output_cols_to_input_rows_lengths[j] * sizeof(struct uniqueN_double_data));

        if (na_rm[j]) {
          int uniqueN_data_length = 0;

          LOOP_OVER_ROWS {
            if (!ISNAN(input[i])) {
              (uniqueN_data + uniqueN_data_length)->rank = map_input_rows_to_output_rows[i];
              (uniqueN_data + uniqueN_data_length)->value = input[i];
              uniqueN_data_length++;
            }
          }

          if (uniqueN_data_length > 0) {
            qsort(uniqueN_data, uniqueN_data_length, sizeof(struct uniqueN_double_data), uniqueN_double_cmp_no_NaN);

            output[(uniqueN_data)->rank] = 1;
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

        } else {
          LOOP_OVER_ROWS {
            (uniqueN_data + ii)->rank = map_input_rows_to_output_rows[i];
            (uniqueN_data + ii)->value = input[i];
          }

          qsort(uniqueN_data, map_output_cols_to_input_rows_lengths[j], sizeof(struct uniqueN_double_data), uniqueN_double_cmp);

          if (map_output_cols_to_input_rows_lengths[j] > 0) {
            output[(uniqueN_data)->rank] = 1;
          }

          for (int i = 1; i < map_output_cols_to_input_rows_lengths[j]; i ++) {
            if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
              if ((ISNA((uniqueN_data + i)->value) != ISNA((uniqueN_data + i - 1)->value)) ||
                  (R_IsNaN((uniqueN_data + i)->value) != R_IsNaN((uniqueN_data + i - 1)->value)) ||
                  (!ISNAN((uniqueN_data + i)->value) && !ISNAN((uniqueN_data + i - 1)->value) && (uniqueN_data + i)->value != (uniqueN_data + i - 1)->value)) {
                output[(uniqueN_data + i)->rank]++;
              }
            } else {
              output[(uniqueN_data + i)->rank] = 1;
            }
          }
        }

        free(uniqueN_data);


      } else if (typeof_value_var[j] == STRSXP) {
        int *output = ((int **) res)[j];
        intptr_t *input = ((intptr_t **) value_var)[j];

        struct uniqueN_char_data *uniqueN_data =
          (struct uniqueN_char_data *) malloc(map_output_cols_to_input_rows_lengths[j] * sizeof(struct uniqueN_char_data));

        int uniqueN_data_length = 0;
        if (na_rm[j]) {
          LOOP_OVER_ROWS {
            if (input[i] != (intptr_t) NA_STRING) {
              (uniqueN_data + uniqueN_data_length)->rank = map_input_rows_to_output_rows[i];
              (uniqueN_data + uniqueN_data_length)->value = input[i];
              uniqueN_data_length++;
            }
          }
        } else {
          LOOP_OVER_ROWS {
            (uniqueN_data + ii)->rank = map_input_rows_to_output_rows[i];
            (uniqueN_data + ii)->value = input[i];
          }
          uniqueN_data_length = map_output_cols_to_input_rows_lengths[j];
        }

        if (uniqueN_data_length > 0) {
          qsort(uniqueN_data, uniqueN_data_length, sizeof(struct uniqueN_char_data), uniqueN_char_cmp);

          output[(uniqueN_data)->rank] = 1;
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

        free(uniqueN_data);
      }

      break;
    }

    case 5:  // min
    {
      if ((typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) &&
          typeof_res[j] == INTSXP) {
        int *output = ((int **) res)[j];
        int *input = ((int **) value_var)[j];

        char *hit = (char *) calloc(n_row_output, sizeof(char));

        if (na_rm[j]) {
          LOOP_OVER_ROWS {
            // this exploits that NA_LOGICAL := NA_INTEGER
            if (input[i] == NA_INTEGER) continue;
            if (hit[map_input_rows_to_output_rows[i]]) {
              if (input[i] < output[map_input_rows_to_output_rows[i]]) {
                output[map_input_rows_to_output_rows[i]] = input[i];
              }
            } else {
              output[map_input_rows_to_output_rows[i]] = input[i];
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          }
        } else {
          LOOP_OVER_ROWS {
            if (hit[map_input_rows_to_output_rows[i]]) {
              // this exploits that NA_INTEGER := INT_MIN
              if (input[i] < output[map_input_rows_to_output_rows[i]]) {
                output[map_input_rows_to_output_rows[i]] = input[i];
              }
            } else {
              output[map_input_rows_to_output_rows[i]] = input[i];
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          }
        }

        free(hit);
      } else if ((typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) &&
                 typeof_res[j] == REALSXP) {
        double *output = ((double **) res)[j];
        int *input = ((int **) value_var)[j];

        char *hit = (char *) calloc(n_row_output, sizeof(char));

        if (na_rm[j]) {
          LOOP_OVER_ROWS {
            // this exploits that NA_LOGICAL := NA_INTEGER
            if (input[i] == NA_INTEGER) continue;
            if (hit[map_input_rows_to_output_rows[i]]) {
              if (input[i] < output[map_input_rows_to_output_rows[i]]) {
                output[map_input_rows_to_output_rows[i]] = input[i];
              }
            } else {
              output[map_input_rows_to_output_rows[i]] = input[i];
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          }
        } else {
          LOOP_OVER_ROWS {
            if (hit[map_input_rows_to_output_rows[i]]) {
              if (input[i] != NA_INTEGER) {
                if(!ISNAN(output[map_input_rows_to_output_rows[i]]) &&
                   input[i] < output[map_input_rows_to_output_rows[i]]) {
                  output[map_input_rows_to_output_rows[i]] = input[i];
                }
              } else {
                output[map_input_rows_to_output_rows[i]] = NA_REAL;
              }
            } else {
              if (input[i] != NA_INTEGER) {
                output[map_input_rows_to_output_rows[i]] = input[i];
              } else {
                output[map_input_rows_to_output_rows[i]] = NA_REAL;
              }
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          }
        }

        free(hit);
      } else if (typeof_value_var[j] == REALSXP) {
        double *output = ((double **) res)[j];
        double *input = ((double **) value_var)[j];

        char *hit = (char *) calloc(n_row_output, sizeof(char));

        LOOP_OVER_ROWS {
          if (ISNAN(input[i])) {
            if (na_rm[j]) {
              continue;
            } else {
              output[map_input_rows_to_output_rows[i]] = NA_REAL;
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          } else if (hit[map_input_rows_to_output_rows[i]]) {
            if (!ISNAN(output[map_input_rows_to_output_rows[i]]) &&
                input[i] < output[map_input_rows_to_output_rows[i]]) {
              output[map_input_rows_to_output_rows[i]] = input[i];
            }
          } else {
            output[map_input_rows_to_output_rows[i]] = input[i];
            hit[map_input_rows_to_output_rows[i]] = 1;
          }
        }

        free(hit);
      }

      break;
    }

    case 6:  // max
    {
      if ((typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) &&
          typeof_res[j] == INTSXP) {
        int *output = ((int **) res)[j];
        int *input = ((int **) value_var)[j];

        char *hit = (char *) calloc(n_row_output, sizeof(char));

        if (na_rm[j]) {
          LOOP_OVER_ROWS {
            // this exploits that NA_LOGICAL := NA_INTEGER
            if (input[i] == NA_INTEGER) continue;
            if (hit[map_input_rows_to_output_rows[i]]) {
              if (input[i] > output[map_input_rows_to_output_rows[i]]) {
                output[map_input_rows_to_output_rows[i]] = input[i];
              }
            } else {
              output[map_input_rows_to_output_rows[i]] = input[i];
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          }
        } else {
          LOOP_OVER_ROWS {
            if (hit[map_input_rows_to_output_rows[i]]) {
              if (input[i] == NA_INTEGER ||
                  (output[map_input_rows_to_output_rows[i]] != NA_INTEGER &&
                  input[i] > output[map_input_rows_to_output_rows[i]])) {
                output[map_input_rows_to_output_rows[i]] = input[i];
              }
            } else {
              output[map_input_rows_to_output_rows[i]] = input[i];
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          }
        }

        free(hit);
      } else if ((typeof_value_var[j] == LGLSXP || typeof_value_var[j] == INTSXP) &&
        typeof_res[j] == REALSXP) {
        double *output = ((double **) res)[j];
        int *input = ((int **) value_var)[j];

        char *hit = (char *) calloc(n_row_output, sizeof(char));

        if (na_rm[j]) {
          LOOP_OVER_ROWS {
            // this exploits that NA_LOGICAL := NA_INTEGER
            if (input[i] == NA_INTEGER) continue;
            if (hit[map_input_rows_to_output_rows[i]]) {
              if (input[i] > output[map_input_rows_to_output_rows[i]]) {
                output[map_input_rows_to_output_rows[i]] = input[i];
              }
            } else {
              output[map_input_rows_to_output_rows[i]] = input[i];
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          }
        } else {
          LOOP_OVER_ROWS {
            if (hit[map_input_rows_to_output_rows[i]]) {
              if (input[i] != NA_INTEGER) {
                if(!ISNAN(output[map_input_rows_to_output_rows[i]]) &&
                   input[i] > output[map_input_rows_to_output_rows[i]]) {
                  output[map_input_rows_to_output_rows[i]] = input[i];
                }
              } else {
                output[map_input_rows_to_output_rows[i]] = NA_REAL;
              }
            } else {
              if (input[i] != NA_INTEGER) {
                output[map_input_rows_to_output_rows[i]] = input[i];
              } else {
                output[map_input_rows_to_output_rows[i]] = NA_REAL;
              }
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          }
        }

        free(hit);
      } else if (typeof_value_var[j] == REALSXP) {
        double *output = ((double **) res)[j];
        double *input = ((double **) value_var)[j];

        char *hit = (char *) calloc(n_row_output, sizeof(char));

        LOOP_OVER_ROWS {
          if (ISNAN(input[i])) {
            if (na_rm[j]) {
              continue;
            } else {
              output[map_input_rows_to_output_rows[i]] = NA_REAL;
              hit[map_input_rows_to_output_rows[i]] = 1;
            }
          } else if (hit[map_input_rows_to_output_rows[i]]) {
            if (!ISNAN(output[map_input_rows_to_output_rows[i]]) &&
                input[i] > output[map_input_rows_to_output_rows[i]]) {
              output[map_input_rows_to_output_rows[i]] = input[i];
            }
          } else {
            output[map_input_rows_to_output_rows[i]] = input[i];
            hit[map_input_rows_to_output_rows[i]] = 1;
          }
        }

        free(hit);
      }

      break;
    }
    }
  }

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
    res_ptr[i] = DATAPTR(VECTOR_ELT(res, i));
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

  if (failure) error("something went wrong");

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
