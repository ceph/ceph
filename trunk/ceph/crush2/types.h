#ifndef _CRUSH_TYPES_H
#define _CRUSH_TYPES_H

#include <linux/types.h>  /* just for int types */

#ifndef BUG_ON
# include <assert.h>
# define BUG_ON(x) assert(!(x))
#endif

#endif
