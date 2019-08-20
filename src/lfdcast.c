#include "Rinternals.h"
#include <pthread.h>

struct thread_data {
  SEXP agg;
  SEXP value_var;
  SEXP na_rm;
  SEXP cols_split;
  SEXP res;
  SEXP col_order;
  SEXP col_grp_starts;
  SEXP cols_res;
  SEXP row_ranks;
};

#define CAST_START                                             \
  for (int jj = 0; jj < LENGTH(cols_split); jj++) {            \
    int j = INTEGER(cols_split)[jj] - 1;                       \
    if (INTEGER(cols_res)[j] == NA_INTEGER) continue;          \
                                                               \
    SEXP col = VECTOR_ELT(res, INTEGER(cols_res)[j] - 1);      \
                                                               \
    for (int ii = INTEGER(col_grp_starts)[j] - 1;              \
         ii < INTEGER(col_grp_starts)[j + 1] - 1;              \
         ii++) {                                               \
      int i = INTEGER(col_order)[ii] - 1;                      \

#define CAST_END                                               \
    }                                                          \
  }

void *lfdcast_core(void *td_void) {
  struct thread_data *td = (struct thread_data *) td_void;
  SEXP agg = td->agg;
  SEXP value_var = td->value_var;
  SEXP na_rm = td->na_rm;
  SEXP cols_split = td->cols_split;
  SEXP res = td->res;
  SEXP col_order = td->col_order;
  SEXP col_grp_starts = td->col_grp_starts;
  SEXP cols_res = td->cols_res;
  SEXP row_ranks = td->row_ranks;

  switch(INTEGER(agg)[0]) {
  case 1: // count
    CAST_START
    (INTEGER(col)[INTEGER(row_ranks)[i] - 1])++;
    CAST_END
      break;
  case 2: // existence
    CAST_START
    LOGICAL(col)[INTEGER(row_ranks)[i] - 1] = TRUE;
    CAST_END
      break;
  case 3: // sum
    if (TYPEOF(value_var) == LGLSXP) {
      CAST_START
      if (INTEGER(col)[INTEGER(row_ranks)[i] - 1] == NA_INTEGER) {
        continue;
      } else if (LOGICAL(value_var)[i] == NA_LOGICAL) {
        if (LOGICAL(na_rm)[0]) {
          continue;
        } else {
          INTEGER(col)[INTEGER(row_ranks)[i] - 1] = NA_INTEGER;
        }
      } else {
        INTEGER(col)[INTEGER(row_ranks)[i] - 1] += LOGICAL(value_var)[i];
      }
      CAST_END
    } else if (TYPEOF(value_var) == INTSXP) {
      CAST_START
      if (INTEGER(value_var)[i] == NA_INTEGER) {
        if (LOGICAL(na_rm)[0]) {
          continue;
        } else {
          REAL(col)[INTEGER(row_ranks)[i] - 1] = NA_REAL;
        }
      } else {
        REAL(col)[INTEGER(row_ranks)[i] - 1] += INTEGER(value_var)[i];
      }
      CAST_END
    } else if (TYPEOF(value_var) == REALSXP) {
      CAST_START
      if (LOGICAL(na_rm)[0] && REAL(value_var)[i] == NA_REAL) {
        continue;
      } else {
        REAL(col)[INTEGER(row_ranks)[i] - 1] += REAL(value_var)[i];
      }
      CAST_END
    } /*else {
      error("value.var must be numeric or logical if fun.aggregate == 'sum'");
    }*/
    break;
  }

  return NULL;
}


SEXP lfdcast(SEXP agg, SEXP value_var, SEXP na_rm, SEXP cols_split,
             SEXP res, SEXP col_order,
             SEXP col_grp_starts, SEXP cols_res, SEXP row_ranks,
             SEXP nthread_SEXP) {

  struct thread_data td_template = {
    .agg = agg,
    .value_var = value_var,
    .na_rm = na_rm,
    .cols_split = NULL,
    .res = res,
    .col_order = col_order,
    .col_grp_starts = col_grp_starts,
    .cols_res = cols_res,
    .row_ranks = row_ranks
  };

  int nthread = INTEGER(nthread_SEXP)[0];

  struct thread_data td[nthread];

  int failure = 0;
  pthread_t thread_ids[nthread];
  for (int i = 0; i < nthread; i++) {
    td[i] = td_template;
    td[i].cols_split = VECTOR_ELT(cols_split, i);
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
