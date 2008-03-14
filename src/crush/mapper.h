#ifndef _CRUSH_MAPPER_H
#define _CRUSH_MAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "crush.h"

extern int crush_find_rule(struct crush_map *map, int pool, int type, int size);
extern int crush_do_rule(struct crush_map *map,
			 int ruleno,
			 int x, int *result, int result_max,
			 int forcefeed); /* -1 for none */

#ifdef __cplusplus
}
#endif

#endif
