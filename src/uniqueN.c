#include "lfdcast.h"
#include "rsort.h"

char *uniqueN_(void *restrict res, const int typeof_res, const void *restrict value_var,
               const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
               const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
               const int n_row_output, int *restrict hit) {

  char *ret = NULL;

  int *restrict output = (int *) res;
  const void *restrict input = value_var;

  struct uniqueN_data * restrict uniqueN_data = NULL;
  int (*restrict hist_rank)[n_bucket] = NULL;
  int (*restrict hist_value)[n_bucket] = NULL;

  uniqueN_data = malloc(n_input_rows_in_output_col * sizeof(struct uniqueN_data));
  if (uniqueN_data == NULL) {
    ret = "'malloc' failed";
    goto cleanup;
  }

  hist_rank = malloc(sizeof(int[n_pass_rank][n_bucket]));
  if (hist_rank == NULL) {
    ret = "'malloc' failed";
    goto cleanup;
  }

  hist_value = malloc(sizeof(int[n_pass_value][n_bucket]));
  if (hist_value == NULL) {
    ret = "'malloc' failed";
    goto cleanup;
  }

  memset(hist_rank, 0, n_pass_rank * n_bucket * sizeof(int));
  memset(hist_value, 0, n_pass_value * n_bucket * sizeof(int));

  int uniqueN_data_length = 0;
  double zero = 0;
  double na = NA_REAL;
  double nan = R_NaN;
  LOOP_OVER_ROWS {
    if (na_rm && ISNA_INPUT_I) continue;

    (uniqueN_data + uniqueN_data_length)->rank = map_input_rows_to_output_rows[i];
    if (typeof_value_var == LGLSXP || typeof_value_var == INTSXP) {
      (uniqueN_data + uniqueN_data_length)->value = ((int *) input)[i];
    } else if (typeof_value_var == REALSXP) {
      if (((double *) input)[i] == 0) {
        memcpy(&(uniqueN_data + uniqueN_data_length)->value, &zero, sizeof(double));
        //(uniqueN_data + uniqueN_data_length)->value = *(uint64_t *) &(zero);
      } else if (R_IsNA(((double *) input)[i])) {
        memcpy(&(uniqueN_data + uniqueN_data_length)->value, &na, sizeof(double));
      } else if (R_IsNaN(((double *) input)[i])) {
        memcpy(&(uniqueN_data + uniqueN_data_length)->value, &nan, sizeof(double));
      } else {
        memcpy(&(uniqueN_data + uniqueN_data_length)->value, ((double *) input) + i, sizeof(double));
        //(uniqueN_data + uniqueN_data_length)->value = *(uint64_t *) &(((double *) input)[i]);
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
    if (rsort(uniqueN_data, uniqueN_data_length, hist_rank, hist_value, RANK_THEN_VALUE) != 0) {
      ret = "'rsort' failed (out of memory)";
      goto cleanup;
    }
    //qsort(uniqueN_data, uniqueN_data_length, sizeof(struct uniqueN_data), uniqueN_int_cmp);

    output[(uniqueN_data)->rank] = 1;

    if (typeof_value_var == REALSXP) {
      for (int i = 1; i < uniqueN_data_length; i ++) {
        if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
          double v, vp;
          memcpy(&v, &(uniqueN_data + i)->value, sizeof(double));
          memcpy(&vp, &(uniqueN_data + i - 1)->value, sizeof(double));
          //double v = *(double *) &(uniqueN_data + i)->value;
          //double vp = *(double *) &(uniqueN_data + i - 1)->value;
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

  cleanup:
  free(uniqueN_data);
  free(hist_rank);
  free(hist_value);

  return ret;
}

struct lfdcast_agg uniqueN = {
  .fun = uniqueN_
};
