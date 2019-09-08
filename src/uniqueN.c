#include "lfdcast.h"
#include "rsort.h"

int uniqueN_(void *res, int typeof_res, void *value_var, int typeof_value_var,
             int na_rm, int *input_rows_in_output_col, int n_input_rows_in_output_col,
             int *map_input_rows_to_output_rows, int n_row_output, int *hit) {

  int *output = (int *) res;
  void *input = value_var;

  struct uniqueN_data *uniqueN_data =
    (struct uniqueN_data *) malloc(n_input_rows_in_output_col * sizeof(struct uniqueN_data));

  int (*hist_rank)[n_bucket] = malloc(sizeof(int[n_pass_rank][n_bucket]));
  int (*hist_value)[n_bucket] = malloc(sizeof(int[n_pass_value][n_bucket]));
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
        (uniqueN_data + uniqueN_data_length)->value = *(uint64_t *) &(zero);
      } else {
        (uniqueN_data + uniqueN_data_length)->value = *(uint64_t *) &(((double *) input)[i]);
      }
    } else if (typeof_value_var == STRSXP) {
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

    if (typeof_value_var == REALSXP) {
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

  return 0;
}

struct lfdcast_agg uniqueN = {
  .fun = uniqueN_
};
