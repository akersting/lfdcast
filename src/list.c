#include "lfdcast.h"
#include "list.h"

#include "vg/memcheck.h"

char *list_(void *restrict res, const int typeof_res, const void *restrict value_var,
            const int typeof_value_var, const int na_rm, const int *restrict input_rows_in_output_col,
            const int n_input_rows_in_output_col, const int *restrict map_input_rows_to_output_rows,
            const int n_row_output, int *restrict hit) {

  char **restrict output = (char **) res;

  {
    const void *restrict input = value_var;
    LOOP_OVER_ROWS_W_PREFETCH(
      10, PREFETCH_INPUT | PREFETCH_HIT,
      if (na_rm && ISNA_INPUT_I) continue;
      HIT_I++;
    )
  }

  SEXP off = allocVector(RAWSXP, 1);
  size_t offset = sizeof(R_allocator_t) + ((char *) DATAPTR(off) - (char *) off);
  size_t data_size;

  if (typeof_value_var == LGLSXP || typeof_value_var == INTSXP) {
    for (int i = 0; i < n_row_output; i++) {
      if (hit[i] == 0) continue;

      data_size = INT2VEC((size_t) hit[i]) * sizeof(VECREC);
      output[i] = malloc(offset + data_size);
      if (output[i] == NULL) {
        for (int ii = 0; ii < i; ii++) { // # nocov
          free(output[ii]); // # nocov
        }
        return "'malloc' failed"; // # nocov
      }
      memcpy(output[i], hit + i, sizeof(int));
    }
    memset(hit, 0, (size_t) n_row_output * sizeof(int));

    const int *restrict input = (int *) value_var;

    LOOP_OVER_ROWS_W_PREFETCH(
      10, PREFETCH_INPUT | PREFETCH_OUTPUT | PREFETCH_HIT,
      if (na_rm && input[i] == NA_INTEGER) continue;
      ((int *) (OUTPUT_I + offset))[HIT_I] = input[i];
      HIT_I++;
    )
  } else if (typeof_value_var == REALSXP) {
    for (int i = 0; i < n_row_output; i++) {
      if (hit[i] == 0) continue;

      data_size = FLOAT2VEC((size_t) hit[i]) * sizeof(VECREC);
      output[i] = malloc(offset + data_size);
      if (output[i] == NULL) {
        for (int ii = 0; ii < i; ii++) { // # nocov
          free(output[ii]); // # nocov
        }
        return "'malloc' failed"; // # nocov
      }
      memcpy(output[i], hit + i, sizeof(int));
    }
    memset(hit, 0, (size_t) n_row_output * sizeof(int));

    const double *restrict input = (double *) value_var;

    LOOP_OVER_ROWS_W_PREFETCH(
      10, PREFETCH_INPUT | PREFETCH_OUTPUT | PREFETCH_HIT,
      if (na_rm && ISNAN(input[i])) continue;
      ((double *) (OUTPUT_I + offset))[HIT_I] = input[i];
      HIT_I++;
    )
  } else if (typeof_value_var == STRSXP) {
    for (int i = 0; i < n_row_output; i++) {
      if (hit[i] == 0) continue;

      data_size = INT2VEC((size_t) hit[i]) * sizeof(VECREC);
      output[i] = malloc(offset + data_size);
      if (output[i] == NULL) {
        for (int ii = 0; ii < i; ii++) { // # nocov
          free(output[ii]); // # nocov
        }
        return "'malloc' failed"; // # nocov
      }
      memcpy(output[i], hit + i, sizeof(int));
    }
    memset(hit, 0, (size_t) n_row_output * sizeof(int));

    const SEXP *restrict input = (SEXP *) value_var;

    LOOP_OVER_ROWS_W_PREFETCH(
      10, PREFETCH_INPUT | PREFETCH_OUTPUT | PREFETCH_HIT,
      if (na_rm && input[i] == NA_STRING) continue;
      ((int *) (OUTPUT_I + offset))[HIT_I] = i;
      HIT_I++;
    )

  }

  return NULL;
}

struct lfdcast_agg list = {
  .fun = list_
};

void* list_alloc(R_allocator_t *allocator, size_t size) {
  allocator_data *data = allocator->data;
  return data->ptr;
}

void list_free(R_allocator_t *allocator, void *addr) {
  allocator_data *data = allocator->data;
  if (addr != data->ptr) {
    error("'addr' not equal to 'data->ptr' in 'list_free'"); // # nocov
  }

  free(data->ptr);
  free(data);
}

static inline void allocate_scalar_in_list_res(const int i, char **restrict output, SEXP res_j, SEXP value_var_j) {
  SEXP off = allocVector(RAWSXP, 1);
  size_t offset = sizeof(R_allocator_t) + ((char *) DATAPTR(off) - (char *) off);

  SET_VECTOR_ELT(res_j, i, allocVector(TYPEOF(value_var_j), 1));

  switch(TYPEOF(value_var_j)) {
  case LGLSXP:
  case INTSXP:
    INTEGER(VECTOR_ELT(res_j, i))[0] = ((int *) (output[i] + offset))[0];
    break;
  case REALSXP:
    REAL(VECTOR_ELT(res_j, i))[0] = ((double *) (output[i] + offset))[0];
    break;
  case STRSXP:
    SET_STRING_ELT(VECTOR_ELT(res_j, i), 0, STRING_ELT(value_var_j, ((int *) (output[i] + offset))[0]));
    break;
  default: // # nocov
    error("unsupported SEXP type for 'x': %s", type2char(TYPEOF(value_var_j))); // # nocov
  }
  free(output[i]);
}

static inline void allocate_vector_in_list_res(const int i, const int n, char **restrict output, SEXP res_j, SEXP value_var_j) {
  SEXP off = allocVector(RAWSXP, 1);
  size_t offset = sizeof(R_allocator_t) + ((char *) DATAPTR(off) - (char *) off);

  if (TYPEOF(value_var_j) == STRSXP) {
    SET_VECTOR_ELT(res_j, i, allocVector(STRSXP, n));
    int_res_to_char_res(output[i] + offset, VECTOR_ELT(res_j, i), value_var_j, n);
    free(output[i]);
    return;
  }

  allocator_data* data = malloc(sizeof(allocator_data));
  if (data == NULL) {
    error("'malloc' failed to allocate %zu bytes", sizeof(allocator_data)); // # nocov
  }

  data->ptr = output[i];

  R_allocator_t allocator;
  allocator.mem_alloc = &list_alloc;
  allocator.mem_free = &list_free;
  allocator.res = NULL;
  allocator.data = data;

  SET_VECTOR_ELT(res_j, i, allocVector3(TYPEOF(value_var_j), n, &allocator));

  size_t dataptr_size;
  switch(TYPEOF(value_var_j)) {
  case LGLSXP:
  case INTSXP:
    dataptr_size = n * sizeof(int);
    break;
  case REALSXP:
    dataptr_size = n * sizeof(double);
    break;
  default: // # nocov
    list_free(&allocator, output[i]); // # nocov
    error("unsupported SEXP type: %s", type2char(TYPEOF(value_var_j))); // # nocov
  }
  VALGRIND_MAKE_MEM_DEFINED(DATAPTR(VECTOR_ELT(res_j, i)), dataptr_size);
}

void allocate_list_res(void **restrict res_ptr_j, SEXP res_j, SEXP value_var_j) {
  char **restrict output = (char **) res_ptr_j;
  int n_row_output = LENGTH(res_j);

  for (int i = 0; i < n_row_output; i++) {
    if (output[i] != NULL) {
      int n = ((int *) (output[i]))[0];

      if (n == 1) {
        allocate_scalar_in_list_res(i, output, res_j, value_var_j);
      } else {
        allocate_vector_in_list_res(i, n, output, res_j, value_var_j);
      }
      copyMostAttrib(value_var_j, VECTOR_ELT(res_j, i));
    }
  }
}
