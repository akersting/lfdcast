#include "lfdcast.h"
#include "rsort.h"

int sample_(void *restrict res, const int typeof_res, const void *restrict value_var,
            const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
            const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
            const int n_row_output, int *restrict hit) {

  const void *restrict input = value_var;

  struct uniqueN_data *restrict uniqueN_data =
    (struct uniqueN_data *) malloc(n_input_rows_in_output_col * sizeof(struct uniqueN_data));

  int (*restrict hist_rank)[n_bucket] = malloc(sizeof(int[n_pass_rank][n_bucket]));
  memset(hist_rank, 0, n_pass_rank * n_bucket * sizeof(int));

  int uniqueN_data_length = 0;
  LOOP_OVER_ROWS {
    if (na_rm && ISNA_INPUT_I) continue;

    (uniqueN_data + uniqueN_data_length)->rank = map_input_rows_to_output_rows[i];
    if (typeof_value_var == LGLSXP || typeof_value_var == INTSXP) {
      (uniqueN_data + uniqueN_data_length)->value = ((int *) input)[i];
    } else if (typeof_value_var == REALSXP) {
      memcpy(&(uniqueN_data + uniqueN_data_length)->value, ((double *) input) + i, sizeof(double));
    } else if (typeof_value_var == STRSXP) {
      (uniqueN_data + uniqueN_data_length)->value = i;
    }

    for (int jj = 0; jj < n_pass_rank; jj++) {
      hist_rank[jj][(uniqueN_data + uniqueN_data_length)->rank >> jj * shift & mask]++;
    }

    uniqueN_data_length++;
  }

  if (uniqueN_data_length > 0) {
    //isort(uniqueN_data, uniqueN_data_length);
    rsort(uniqueN_data, uniqueN_data_length, hist_rank, NULL, RANK_THEN_VALUE);
    //qsort(uniqueN_data, uniqueN_data_length, sizeof(struct uniqueN_data), uniqueN_int_cmp);

    int cntr = 1;
    if (typeof_value_var == REALSXP) {
      double *restrict output = (double *) res;
      for (int i = 1; i < uniqueN_data_length; i ++) {
        if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
          cntr++;
        } else {
          memcpy(&output[(uniqueN_data + i - 1)->rank], &(uniqueN_data + i - 1 - (int) R_unif_index(cntr))->value, sizeof(double));
          //output[(uniqueN_data + i - 1)->rank] = *(double *) &(uniqueN_data + i - 1 - (int) R_unif_index(cntr))->value;
          cntr = 1;
        }
      }
      memcpy(&output[(uniqueN_data + uniqueN_data_length - 1)->rank], &(uniqueN_data + uniqueN_data_length - 1 - (int) R_unif_index(cntr))->value, sizeof(double));
      //output[(uniqueN_data + uniqueN_data_length - 1)->rank] = *(double *) &(uniqueN_data + uniqueN_data_length - 1 - (int) R_unif_index(cntr))->value;
    } else if (typeof_value_var == LGLSXP || typeof_value_var == INTSXP) {
      int *restrict output = (int *) res;
      for (int i = 1; i < uniqueN_data_length; i ++) {
        if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
          cntr++;
        } else {
          output[(uniqueN_data + i - 1)->rank] = *(long *) &(uniqueN_data + i - 1 - (int) R_unif_index(cntr))->value;
          cntr = 1;
        }
      }
      output[(uniqueN_data + uniqueN_data_length - 1)->rank] = *(long *) &(uniqueN_data + uniqueN_data_length - 1 - (int) R_unif_index(cntr))->value;
    } else if (typeof_value_var == STRSXP) {
      int *restrict output = (int *) res;
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

  }

  free(uniqueN_data);
  free(hist_rank);

  return 0;
}


struct lfdcast_agg sample = {
  .fun = sample_
};
