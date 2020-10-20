#include <Rinternals.h>
#include <R_ext/Rallocators.h>

// https://github.com/wch/r-source/blob/tags/R-3-6-3/src/include/Defn.h#L409-L422
typedef struct {
  union {
  SEXP		backpointer;
  double		align;
} u;
} VECREC;

/* Vector Heap Macros */
#define BYTE2VEC(n)	(((n)>0)?(((n)-1)/sizeof(VECREC)+1):0)
#define INT2VEC(n)	(((n)>0)?(((n)*sizeof(int)-1)/sizeof(VECREC)+1):0)
#define FLOAT2VEC(n)	(((n)>0)?(((n)*sizeof(double)-1)/sizeof(VECREC)+1):0)
#define COMPLEX2VEC(n)	(((n)>0)?(((n)*sizeof(Rcomplex)-1)/sizeof(VECREC)+1):0)

typedef struct allocator_data {
  void *ptr;
} allocator_data;

void* list_alloc(R_allocator_t *allocator, size_t size);
void list_free(R_allocator_t *allocator, void *addr);

void allocate_list_res(void **restrict res_ptr_j, SEXP res_j, SEXP value_var_j);
