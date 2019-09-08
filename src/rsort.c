#include "rsort.h"
#include <stdlib.h>
#include <string.h>

void rsort(struct uniqueN_data *x, int n, int hist_rank[][n_bucket],
           int hist_value[][n_bucket]) {

  struct uniqueN_data *s = (struct uniqueN_data *) malloc(n * sizeof(struct uniqueN_data));

  int not_skip_rank[n_pass_rank];
  for (int j = 0; j < n_pass_rank; j++) {
    not_skip_rank[j] = -1;
    int cumsum = 0;
    for (int i = 0; i < n_bucket; i++) {
      if (hist_rank[j][i] != 0) not_skip_rank[j]++;
      cumsum += hist_rank[j][i];
      hist_rank[j][i] = cumsum - hist_rank[j][i];
    }
  }

  int not_skip_value[n_pass_value];
  for (int j = 0; j < n_pass_value; j++) {
    not_skip_value[j] = -1;
    int cumsum = 0;
    for (int i = 0; i < n_bucket; i++) {
      if (hist_value[j][i] != 0) not_skip_value[j]++;
      cumsum += hist_value[j][i];
      hist_value[j][i] = cumsum - hist_value[j][i];
    }
  }

  int pass = 0;
  for (int j = 0; j < n_pass_value; j++) {
    if (!not_skip_value[j]) continue;

    for (int i = 0; i < n; i++) {
      unsigned int pos = (x + i)->value >> j * shift & mask;
      s[hist_value[j][pos]++] = x[i];
    }

    struct uniqueN_data *tmp = s;
    s = x;
    x = tmp;

    pass++;
  }

  for (int j = 0; j < n_pass_rank; j++) {
    if (!not_skip_rank[j]) continue;

    for (int i = 0; i < n; i++) {
      unsigned int pos = (x + i)->rank >> j * shift & mask;
      s[hist_rank[j][pos]++] = x[i];
    }

    struct uniqueN_data *tmp = s;
    s = x;
    x = tmp;

    pass++;
  }

  if (pass % 2 == 0) {
    free(s);
  } else {
    memcpy(s, x, n * sizeof(struct uniqueN_data));
    free(x);
  }
}
