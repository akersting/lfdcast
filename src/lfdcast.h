#include "../inst/include/lfdcast.h"

// required for DATAPTR
#include <Rversion.h>
#if defined(R_VERSION) && R_VERSION < R_Version(3, 5, 0)
#define USE_RINTERNALS
#endif

#include "R.h"
#include "Rinternals.h"
