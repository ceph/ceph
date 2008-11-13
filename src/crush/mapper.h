#ifndef _CRUSH_MAPPER_H
#define _CRUSH_MAPPER_H

#include "crush.h"

/*
 * CRUSH functions for find rules and then mapping an input to an
 * output set.
 *
 * LGPL2
 */
extern int crush_find_rule(struct crush_map *map, int pool, int type, int size);
extern int crush_do_rule(struct crush_map *map,
			 int ruleno,
			 int x, int *result, int result_max,
			 int forcefeed,
			 __u32 *weights); /* -1 for none */

#endif
