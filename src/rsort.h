#ifndef RSORT_H
#define RSORT_H

#include <stdint.h>
#include <stdlib.h>

#define n_pass_rank 4
#define n_pass_value 8
#define shift 8
#define n_bucket 256
#define mask 0xFF

#define RANK_THEN_VALUE 0
#define VALUE_THEN_RANK 1

struct uniqueN_data {
  uint32_t rank;
  uint64_t value;
};

int rsort(struct uniqueN_data *restrict x,
          int n,
          int hist_rank[restrict][n_bucket],
          int hist_value[restrict][n_bucket],
          int order) __attribute__((warn_unused_result));

#endif
