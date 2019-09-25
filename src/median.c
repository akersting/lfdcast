#include "lfdcast.h"
#include "rsort.h"

int median_(void *restrict res, const int typeof_res, const void *restrict value_var,
             const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
             const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
             const int n_row_output, int *restrict hit) {

  double *restrict output = (double *) res;
  const void *restrict input = value_var;

  struct uniqueN_data * restrict uniqueN_data =
    (struct uniqueN_data *) malloc(n_input_rows_in_output_col * sizeof(struct uniqueN_data));

  int (*restrict hist_rank)[n_bucket] = malloc(sizeof(int[n_pass_rank][n_bucket]));
  int (*restrict hist_value)[n_bucket] = malloc(sizeof(int[n_pass_value][n_bucket]));
  memset(hist_rank, 0, n_pass_rank * n_bucket * sizeof(int));
  memset(hist_value, 0, n_pass_value * n_bucket * sizeof(int));

  int uniqueN_data_length = 0;
  double zero = 0;
  LOOP_OVER_ROWS {
    if (na_rm && ISNA_INPUT_I) continue;

    (uniqueN_data + uniqueN_data_length)->rank = map_input_rows_to_output_rows[i];
    if (typeof_value_var == LGLSXP || typeof_value_var == INTSXP) {
      (uniqueN_data + uniqueN_data_length)->value = ((int *) input)[i];
    } else if (typeof_value_var == REALSXP) {
      if (((double *) input)[i] == 0) {
        memcpy(&(uniqueN_data + uniqueN_data_length)->value, &zero, sizeof(double));
        //(uniqueN_data + uniqueN_data_length)->value = *(uint64_t *) &(zero);
      } else {
        memcpy(&(uniqueN_data + uniqueN_data_length)->value, ((double *) input) + i, sizeof(double));
        //(uniqueN_data + uniqueN_data_length)->value = *(uint64_t *) &(((double *) input)[i]);
      }
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

    int cntr = 1;
    double tmp1;
    double tmp2;
    if (typeof_value_var == REALSXP) {
      for (int i = 1; i < uniqueN_data_length; i ++) {
        if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
          cntr++;
        } else {
          if (cntr % 2 == 0) {
            memcpy(&tmp1, &(uniqueN_data + i - cntr / 2)->value, sizeof(double));
            memcpy(&tmp2, &(uniqueN_data + i - cntr / 2 - 1)->value, sizeof(double));
            output[(uniqueN_data + i - 1)->rank] = (tmp1 + tmp2) / 2;
          } else {
            memcpy(&output[(uniqueN_data + i - 1)->rank], &(uniqueN_data + i - cntr / 2 - 1)->value, sizeof(double));
          }
          cntr = 1;
        }
      }

      if (cntr % 2 == 0) {
        memcpy(&tmp1, &(uniqueN_data + uniqueN_data_length - cntr / 2)->value, sizeof(double));
        memcpy(&tmp2, &(uniqueN_data + uniqueN_data_length - cntr / 2 - 1)->value, sizeof(double));
        output[(uniqueN_data + uniqueN_data_length - 1)->rank] = (tmp1 + tmp2) / 2;
      } else {
        memcpy(&output[(uniqueN_data + uniqueN_data_length - 1)->rank], &(uniqueN_data + uniqueN_data_length - cntr / 2 - 1)->value, sizeof(double));
      }

    } else {
      for (int i = 1; i < uniqueN_data_length; i ++) {
        if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
          if ((uniqueN_data + i)->value != (uniqueN_data + i - 1)->value) {
            cntr++;
          }
        } else {
          if (cntr % 2 == 0) {
            tmp1 = *(long *) &(uniqueN_data + i - cntr / 2)->value;
            tmp2 = *(long *) &(uniqueN_data + i - cntr / 2 - 1)->value;
            output[(uniqueN_data + i - 1)->rank] = (tmp1 + tmp2) / 2;
          } else {
            output[(uniqueN_data + i - 1)->rank] = *(long *) &(uniqueN_data + i - cntr / 2 - 1)->value;
          }
          cntr = 1;
        }
      }

      if (cntr % 2 == 0) {
        tmp1 = *(long *) &(uniqueN_data + uniqueN_data_length - cntr / 2)->value;
        tmp2 = *(long *) &(uniqueN_data + uniqueN_data_length - cntr / 2 - 1)->value;
        output[(uniqueN_data + uniqueN_data_length - 1)->rank] = (tmp1 + tmp2) / 2;
      } else {
        output[(uniqueN_data + uniqueN_data_length - 1)->rank] = *(long *) &(uniqueN_data + uniqueN_data_length - cntr / 2 - 1)->value;
      }

    }
  }

  free(uniqueN_data);
  free(hist_rank);
  free(hist_value);

  return 0;
}

struct lfdcast_agg median = {
  .fun = median_
};
