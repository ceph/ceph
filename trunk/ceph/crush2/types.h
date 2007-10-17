#ifndef _CRUSH_TYPES_H
#define _CRUSH_TYPES_H

#ifdef KERNEL
# define free(x) kfree(x)
#else
# include <stdlib.h>
#endif


#include <linux/types.h>  /* just for int types */

#ifndef BUG_ON
# include <assert.h>
# define BUG_ON(x) assert(!(x))
#endif

#endif
