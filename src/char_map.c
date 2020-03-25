#include "lfdcast.h"
#include "rsort.h"

SEXP char_map(SEXP x) {
  SEXP unique;
  SEXP map;

  const int n = LENGTH(x);

  if (n == 0) {
    unique = PROTECT(allocVector(STRSXP, 0));
    map = PROTECT(allocVector(INTSXP, 0));
    goto end;
  }

  const intptr_t *restrict input = (intptr_t *) DATAPTR(x);

  struct uniqueN_data * restrict uniqueN_data =
    (struct uniqueN_data *) malloc(n * sizeof(struct uniqueN_data));

  int (*restrict hist_value)[n_bucket] = malloc(sizeof(int[n_pass_value][n_bucket]));
  memset(hist_value, 0, n_pass_value * n_bucket * sizeof(int));

  for (uint32_t i = 0; i < n; i++) {
    (uniqueN_data + i)->rank = i;
    (uniqueN_data + i)->value = input[i];

    //for (int jj = 0; jj < n_pass_value; jj++) {
    //  hist_value[jj][(uniqueN_data + i)->value >> jj * shift & mask]++;
    //}

    _Static_assert(n_pass_value == 8, "n_pass_value must be 8");
    hist_value[0][input[i] >> 0 * shift & mask]++;
    hist_value[1][input[i] >> 1 * shift & mask]++;
    hist_value[2][input[i] >> 2 * shift & mask]++;
    hist_value[3][input[i] >> 3 * shift & mask]++;
    hist_value[4][input[i] >> 4 * shift & mask]++;
    hist_value[5][input[i] >> 5 * shift & mask]++;
    hist_value[6][input[i] >> 6 * shift & mask]++;
    hist_value[7][input[i] >> 7 * shift & mask]++;
  }

  rsort(uniqueN_data, n, NULL, hist_value, VALUE_THEN_RANK);
  free(hist_value);

  map = PROTECT(allocVector(INTSXP, n));
  int *restrict map_ptr = INTEGER(map);
  int unique_cntr = 1;
  map_ptr[uniqueN_data->rank] = unique_cntr;

  for (int i = 1, ii = 0; i < n; i++) {
    /*if ((uniqueN_data + i)->value != (uniqueN_data + i - 1)->value) {
      unique_cntr++;
      map_ptr[(uniqueN_data + i)->rank] = unique_cntr;
      (uniqueN_data + ii)->rank = i;
      ii = i;
    } else {
      map_ptr[(uniqueN_data + i)->rank] = unique_cntr;
    }*/

    if ((uniqueN_data + i)->value != (uniqueN_data + i - 1)->value) {
      unique_cntr++;
      (uniqueN_data + ii)->rank = i;
      ii = i;
    }
    map_ptr[(uniqueN_data + i)->rank] = unique_cntr;
  }

  unique = PROTECT(allocVector(STRSXP, unique_cntr));
  SET_STRING_ELT(unique, 0, (SEXP) uniqueN_data->value);

  for (int i = 1, ii = 0; i < unique_cntr; i++) {
    ii = (uniqueN_data + ii)->rank;
    SET_STRING_ELT(unique, i, (SEXP) (uniqueN_data + ii)->value);
  }

  free(uniqueN_data);

  end: ;
  SEXP ret = PROTECT(allocVector(VECSXP, 2));
  SET_VECTOR_ELT(ret, 0, unique);
  SET_VECTOR_ELT(ret, 1, map);

  UNPROTECT(3);
  return(ret);
}
