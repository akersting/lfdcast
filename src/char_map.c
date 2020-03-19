#include "lfdcast.h"
#include "rsort.h"

SEXP char_map(SEXP x) {
  const void *restrict input = DATAPTR(x);
  int n = LENGTH(x);

  struct uniqueN_data * restrict uniqueN_data =
    (struct uniqueN_data *) malloc(n * sizeof(struct uniqueN_data));

  int (*restrict hist_rank)[n_bucket] = malloc(sizeof(int[n_pass_rank][n_bucket]));
  int (*restrict hist_value)[n_bucket] = malloc(sizeof(int[n_pass_value][n_bucket]));
  memset(hist_rank, 0, n_pass_rank * n_bucket * sizeof(int));
  memset(hist_value, 0, n_pass_value * n_bucket * sizeof(int));

  int uniqueN_data_length = 0;
  for (int i = 0; i < n; i++) {
    (uniqueN_data + uniqueN_data_length)->rank = i;
    (uniqueN_data + uniqueN_data_length)->value = ((intptr_t *) input)[i];


    for (int jj = 0; jj < n_pass_rank; jj++) {
      hist_rank[jj][(uniqueN_data + uniqueN_data_length)->rank >> jj * shift & mask]++;
    }
    for (int jj = 0; jj < n_pass_value; jj++) {
      hist_value[jj][(uniqueN_data + uniqueN_data_length)->value >> jj * shift & mask]++;
    }

    uniqueN_data_length++;
  }

  SEXP map = PROTECT(allocVector(INTSXP, n));
  SEXP unique;
  int unique_cntr = 0;

  if (uniqueN_data_length > 0) {
    rsort(uniqueN_data, uniqueN_data_length, hist_rank, hist_value, VALUE_THEN_RANK);

    unique_cntr = 1;
    INTEGER(map)[uniqueN_data->rank] = unique_cntr;

    for (int i = 1; i < uniqueN_data_length; i ++) {
      if ((uniqueN_data + i)->value != (uniqueN_data + i - 1)->value) {
        unique_cntr++;
      }
      INTEGER(map)[(uniqueN_data + i)->rank] = unique_cntr;
    }

    unique = PROTECT(allocVector(STRSXP, unique_cntr));
    SET_STRING_ELT(unique, 0, (SEXP) uniqueN_data->value);

    unique_cntr = 0;
    for (int i = 1; i < uniqueN_data_length; i ++) {
      if ((uniqueN_data + i)->value != (uniqueN_data + i - 1)->value) {
        SET_STRING_ELT(unique, ++unique_cntr, (SEXP) (uniqueN_data + i)->value);
      }
    }
  } else {
    unique = PROTECT(allocVector(STRSXP, 0));
  }

  free(uniqueN_data);
  free(hist_rank);
  free(hist_value);

  SEXP ret = PROTECT(allocVector(VECSXP, 2));
  SET_VECTOR_ELT(ret, 0, unique);
  SET_VECTOR_ELT(ret, 1, map);

  UNPROTECT(3);
  return(ret);
}
