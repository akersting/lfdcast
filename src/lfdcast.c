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
  int agg;
  void *value_var;
  int typeof_value_var;
  int na_rm;
  int *cols_split;
  int length_cols_split;
  void **res;
  int *col_order;
  int *col_grp_starts;
  int *cols_res;
  int *row_ranks;
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

#define CAST_START                                             \
  for (int jj = 0; jj < length_cols_split; jj++) {             \
    int j = cols_split[jj];                                    \
    if (cols_res[j] == NA_INTEGER) continue;                   \
                                                               \
    void *col = res[cols_res[j]];                              \
                                                               \
    for (int ii = col_grp_starts[j];                           \
         ii < col_grp_starts[j + 1];                           \
         ii++) {                                               \
      int i = col_order[ii];                                   \

#define CAST_END                                               \
    }                                                          \
  }

void *lfdcast_core(void *td_void) {
  struct thread_data *td = (struct thread_data *) td_void;
  int agg = td->agg;
  void *value_var = td->value_var;
  int typeof_value_var = td->typeof_value_var;
  int na_rm = td->na_rm;
  int *cols_split = td->cols_split;
  int length_cols_split = td->length_cols_split;
  void **res = td->res;
  int *col_order = td->col_order;
  int *col_grp_starts = td->col_grp_starts;
  int *cols_res = td->cols_res;
  int *row_ranks = td->row_ranks;

  switch(agg) {
  case 1: // count
    CAST_START
    ((int *) col)[row_ranks[i]]++;
    CAST_END
      break;
  case 2: // existence
    CAST_START
    ((int *) col)[row_ranks[i]] = TRUE;
    CAST_END
      break;
  case 3: // sum
    if (typeof_value_var == LGLSXP) {
      CAST_START
      if (((int *) col)[row_ranks[i]] == NA_INTEGER) {
        continue;
      } else if (((int *)value_var)[i] == NA_LOGICAL) {
        if (na_rm) {
          continue;
        } else {
          ((int *) col)[row_ranks[i]] = NA_INTEGER;
        }
      } else {
        ((int *) col)[row_ranks[i]] += ((int *)value_var)[i];
      }
      CAST_END
    } else if (typeof_value_var == INTSXP) {
      CAST_START
      if (((int *)value_var)[i] == NA_INTEGER) {
        if (na_rm) {
          continue;
        } else {
          ((double *) col)[row_ranks[i]] = NA_REAL;
        }
      } else {
        ((double *) col)[row_ranks[i]] += ((int *)value_var)[i];
      }
      CAST_END
    } else if (typeof_value_var == REALSXP) {
      CAST_START
      if (na_rm && ((double *)value_var)[i] == NA_REAL) {
        continue;
      } else {
        ((double *) col)[row_ranks[i]] += ((double *)value_var)[i];
      }
      CAST_END
    } /*else {
      error("value.var must be numeric or logical if fun.aggregate == 'sum'");
    }*/
    break;
  case 4: // uniqueN
    if (typeof_value_var == LGLSXP || typeof_value_var == INTSXP) {
      for (int jj = 0; jj < length_cols_split; jj++) {
        int j = cols_split[jj];
        if (cols_res[j] == NA_INTEGER) continue;

        void *col = res[cols_res[j]];

        struct uniqueN_int_data *uniqueN_data =
          (struct uniqueN_int_data *) malloc((col_grp_starts[j + 1] - col_grp_starts[j]) * sizeof(struct uniqueN_int_data));

        for (int ii = col_grp_starts[j], cntr = 0;
             ii < col_grp_starts[j + 1];
             ii++, cntr++) {
          int i = col_order[ii];
          (uniqueN_data + cntr)->rank = row_ranks[i];
          (uniqueN_data + cntr)->value = ((int *)value_var)[i];
        }

        qsort(uniqueN_data, col_grp_starts[j + 1] - col_grp_starts[j], sizeof(struct uniqueN_int_data), uniqueN_int_cmp);

        ((int *) col)[(uniqueN_data)->rank] = 1;
        for (int i = 1; i < col_grp_starts[j + 1] - col_grp_starts[j]; i ++) {
          if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
            if ((uniqueN_data + i)->value != (uniqueN_data + i - 1)->value) {
              ((int *) col)[(uniqueN_data + i)->rank]++;
            }
          } else {
            ((int *) col)[(uniqueN_data + i)->rank] = 1;
          }
        }

        free(uniqueN_data);
      }
    } else if (typeof_value_var == REALSXP) {
      for (int jj = 0; jj < length_cols_split; jj++) {
        int j = cols_split[jj];
        if (cols_res[j] == NA_INTEGER) continue;

        void *col = res[cols_res[j]];

        struct uniqueN_double_data *uniqueN_data =
          (struct uniqueN_double_data *) malloc((col_grp_starts[j + 1] - col_grp_starts[j]) * sizeof(struct uniqueN_double_data));

        for (int ii = col_grp_starts[j], cntr = 0;
             ii < col_grp_starts[j + 1];
             ii++, cntr++) {
          int i = col_order[ii];
          (uniqueN_data + cntr)->rank = row_ranks[i];
          (uniqueN_data + cntr)->value = ((double *)value_var)[i];
        }

        qsort(uniqueN_data, col_grp_starts[j + 1] - col_grp_starts[j], sizeof(struct uniqueN_double_data), uniqueN_double_cmp);

        ((int *) col)[(uniqueN_data)->rank] = 1;
        for (int i = 1; i < col_grp_starts[j + 1] - col_grp_starts[j]; i ++) {
          if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
            if ((ISNA((uniqueN_data + i)->value) != ISNA((uniqueN_data + i - 1)->value)) ||
                (R_IsNaN((uniqueN_data + i)->value) != R_IsNaN((uniqueN_data + i - 1)->value)) ||
                (!ISNAN((uniqueN_data + i)->value) && !ISNAN((uniqueN_data + i - 1)->value) && (uniqueN_data + i)->value != (uniqueN_data + i - 1)->value)) {
              ((int *) col)[(uniqueN_data + i)->rank]++;
            }
          } else {
            ((int *) col)[(uniqueN_data + i)->rank] = 1;
          }
        }

        free(uniqueN_data);
      }
    } else if (typeof_value_var == STRSXP) {
      for (int jj = 0; jj < length_cols_split; jj++) {
        int j = cols_split[jj];
        if (cols_res[j] == NA_INTEGER) continue;

        void *col = res[cols_res[j]];

        struct uniqueN_char_data *uniqueN_data =
          (struct uniqueN_char_data *) malloc((col_grp_starts[j + 1] - col_grp_starts[j]) * sizeof(struct uniqueN_char_data));

        for (int ii = col_grp_starts[j], cntr = 0;
             ii < col_grp_starts[j + 1];
             ii++, cntr++) {
          int i = col_order[ii];
          (uniqueN_data + cntr)->rank = row_ranks[i];
          (uniqueN_data + cntr)->value = (intptr_t) ((SEXP *)value_var)[i];
        }

        qsort(uniqueN_data, col_grp_starts[j + 1] - col_grp_starts[j], sizeof(struct uniqueN_char_data), uniqueN_int_cmp);

        ((int *) col)[(uniqueN_data)->rank] = 1;
        for (int i = 1; i < col_grp_starts[j + 1] - col_grp_starts[j]; i ++) {
          if ((uniqueN_data + i)->rank == (uniqueN_data + i - 1)->rank) {
            if ((uniqueN_data + i)->value != (uniqueN_data + i - 1)->value) {
              ((int *) col)[(uniqueN_data + i)->rank]++;
            }
          } else {
            ((int *) col)[(uniqueN_data + i)->rank] = 1;
          }
        }

        free(uniqueN_data);
      }
    }

    break;
  }

  return NULL;
}


SEXP lfdcast(SEXP agg, SEXP value_var, SEXP na_rm, SEXP cols_split,
             SEXP res, SEXP col_order,
             SEXP col_grp_starts, SEXP cols_res, SEXP row_ranks,
             SEXP nthread_SEXP) {

  void **res_ptr = (void **) R_alloc(LENGTH(res), sizeof(void *));
  if (TYPEOF(VECTOR_ELT(res, 0)) == LGLSXP ||
      TYPEOF(VECTOR_ELT(res, 0)) == INTSXP) {
    for (int i = 0; i < LENGTH(res); i++) {
      ((int **) res_ptr)[i] = INTEGER(VECTOR_ELT(res, i));
    }
  } else if (TYPEOF(VECTOR_ELT(res, 0)) == REALSXP) {
    for (int i = 0; i < LENGTH(res); i++) {
      ((double **) res_ptr)[i] = REAL(VECTOR_ELT(res, i));
    }
  } else {
    error("something went wrong");
  }

  struct thread_data td_template = {
    .agg = INTEGER(agg)[0],
    .value_var = DATAPTR(value_var),
    .typeof_value_var = TYPEOF(value_var),
    .na_rm = LOGICAL(na_rm)[0],
    .cols_split = NULL,
    .length_cols_split = 0,
    .res = res_ptr,
    .col_order = INTEGER(col_order),
    .col_grp_starts = INTEGER(col_grp_starts),
    .cols_res = INTEGER(cols_res),
    .row_ranks = INTEGER(row_ranks)
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
