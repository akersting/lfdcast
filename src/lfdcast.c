#include "lfdcast.h"
#include "list.h"

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>

struct thread_data {
  lfdcast_agg_fun_t *agg;
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

void *lfdcast_core(void *td_void) {
  struct thread_data *td = (struct thread_data *) td_void;
  lfdcast_agg_fun_t *agg = td->agg;
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

  int *hit = (int *) malloc((size_t) n_row_output * sizeof(int));
  if (hit == NULL) {
    return "'malloc' failed";
  }

  char *ret = NULL;
  for (int jj = 0; jj < length_cols_split; jj++) {
    int j = cols_split[jj];

    memset(hit, 0, (size_t) n_row_output * sizeof(int));

    ret = agg[j](res[j], typeof_res[j], value_var[j], typeof_value_var[j],
                 na_rm[j], map_output_cols_to_input_rows[j], map_output_cols_to_input_rows_lengths[j],
                 map_input_rows_to_output_rows, n_row_output, hit);

    if (ret != NULL) break;
  }

  free(hit);
  return ret;
}

SEXP lfdcast(SEXP agg, SEXP value_var, SEXP na_rm,
             SEXP map_output_cols_to_input_rows, SEXP res,
             SEXP map_output_cols_to_input_rows_lengths,
             SEXP map_input_rows_to_output_rows,
             SEXP cols_split, SEXP n_row_output_SEXP,
             SEXP nthread_SEXP) {

  void **res_ptr = (void **) R_alloc((size_t) LENGTH(res), sizeof(void *));
  for (int i = 0; i < LENGTH(res); i++) {
    if (TYPEOF(VECTOR_ELT(res, i)) == STRSXP) {
      res_ptr[i] = R_alloc((size_t) INTEGER(n_row_output_SEXP)[0], sizeof(int));
      memset(res_ptr[i], -1, (size_t) INTEGER(n_row_output_SEXP)[0] * sizeof(int));
    } else if (TYPEOF(VECTOR_ELT(res, i)) == VECSXP) {
      int n = INTEGER(n_row_output_SEXP)[0];
      res_ptr[i] = R_alloc((size_t) n, sizeof(int **));
      int **col = res_ptr[i];
      for (int j = 0; j < n; j++) {
        col[j] = NULL;
      }
    } else {
      res_ptr[i] = DATAPTR(VECTOR_ELT(res, i));
    }
  }

  void **value_var_ptr = (void **) R_alloc((size_t) LENGTH(value_var), sizeof(void *));
  for (int i = 0; i < LENGTH(value_var); i++) {
    value_var_ptr[i] = DATAPTR(VECTOR_ELT(value_var, i));
  }

  int *typeof_value_var = (int *) R_alloc((size_t) LENGTH(value_var), sizeof(int));
  for (int i = 0; i < LENGTH(value_var); i++) {
    typeof_value_var[i] = TYPEOF(VECTOR_ELT(value_var, i));
  }

  int *typeof_res = (int *) R_alloc((size_t) LENGTH(res), sizeof(int));
  for (int i = 0; i < LENGTH(res); i++) {
    typeof_res[i] = TYPEOF(VECTOR_ELT(res, i));
  }

  int **map_output_cols_to_input_rows_ptr = (int **) R_alloc((size_t) LENGTH(map_output_cols_to_input_rows), sizeof(int *));
  for (int i = 0; i < LENGTH(map_output_cols_to_input_rows); i++) {
    map_output_cols_to_input_rows_ptr[i] = INTEGER(VECTOR_ELT(map_output_cols_to_input_rows, i));
  }

  lfdcast_agg_fun_t *agg_ptr = (lfdcast_agg_fun_t *) R_alloc((size_t) LENGTH(agg), sizeof(lfdcast_agg_fun_t *));
  for (int i = 0; i < LENGTH(agg); i++) {
    agg_ptr[i] = ((struct lfdcast_agg *) R_ExternalPtrAddr(VECTOR_ELT(agg, i)))->fun;
  }

  struct thread_data td_template = {
    .agg = agg_ptr,
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
      REprintf("not all required pthreads could be created\n");
      failure = 1;
      nthread = i;
      break;
    }
  }


  void *ret;
  for (int i = 0; i < nthread; i++) {
    if (pthread_join(*(thread_ids + i), &ret) != 0) {
      REprintf("failed to join pthread %d\n", i);
      failure = 1;
    } else if (ret != NULL) {
      REprintf("an aggregation function failed with '%s'\n", ret);
      failure = 1;
    }
  }

  PutRNGstate();

  if (failure) error("there were errors in the pthreaded code; see above");

  for (int j = 0; j < LENGTH(res); j++) {
    if (TYPEOF(VECTOR_ELT(res, j)) == STRSXP) {
      int_res_to_char_res(res_ptr[j], VECTOR_ELT(res, j), VECTOR_ELT(value_var, j), LENGTH(VECTOR_ELT(res, j)));
    } else if (TYPEOF(VECTOR_ELT(res, j)) == VECSXP) {
      allocate_list_res(res_ptr[j], VECTOR_ELT(res, j), VECTOR_ELT(value_var, j));
    }
  }

  return res;
}

void int_res_to_char_res(const void *restrict res_ptr_j, SEXP res_j, SEXP value_var_j, int n) {
  int *restrict output = (int *) res_ptr_j;
  for (int i = 0; i < n; i++) {
    if (output[i] < 0) continue;
    SET_STRING_ELT(res_j, i, STRING_ELT(value_var_j, output[i]));
  }
}

SEXP get_row_ranks_unique_pos(SEXP x_SEXP, SEXP res_SEXP) {
  int *restrict x = INTEGER(x_SEXP);
  int *restrict res = INTEGER(res_SEXP);

  int n = LENGTH(x_SEXP);
  for (int i = 0; i < n; i++) {
    res[x[i]] = i + 1;
  }

  return res_SEXP;
}
