#ifndef _CRUSH_BUILDER_H
#define _CRUSH_BUILDER_H

#include "crush.h"

#ifdef __cplusplus
extern "C" {
#endif

extern struct crush_map *crush_create();
extern void crush_finalize(struct crush_map *map);
extern void crush_destroy(struct crush_map *map);

extern int crush_add_rule(struct crush_map *map,
			  int ruleno,
			  struct crush_rule *rule);
extern struct crush_rule *crush_make_rule();
extern void crush_rule_add_step(struct crush_rule *rule, int op, int arg1, int arg2);

extern int crush_add_bucket(struct crush_map *map,
			    struct crush_bucket *bucket);

struct crush_bucket_uniform *
crush_make_uniform_bucket(int type, int size,
			  int *items,
			  int item_weight);
struct crush_bucket_list*
crush_make_list_bucket(int type, int size,
		       int *items,
		       int *weights);
struct crush_bucket_tree*
crush_make_tree_bucket(int type, int size,
		       int *items,    /* in leaf order */
		       int *weights);
struct crush_bucket_straw *
crush_make_straw_bucket(int type, int size,
			int *items,
			int *weights);

#ifdef __cplusplus
}
#endif

#endif
