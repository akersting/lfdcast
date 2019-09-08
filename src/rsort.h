#ifndef RSORT_H
#define RSORT_H

#include <stdint.h>
#include <stdlib.h>

#define n_pass_rank 4
#define n_pass_value 8
#define shift 8
#define n_bucket 256
#define mask 0xFF

struct uniqueN_data {
  uint32_t rank;
  uint64_t value;
};

void rsort(struct uniqueN_data *x,
           int n,
           int hist_rank[][n_bucket],
           int hist_value[][n_bucket]);

#endif
