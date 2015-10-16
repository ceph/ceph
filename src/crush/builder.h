#ifndef CEPH_CRUSH_BUILDER_H
#define CEPH_CRUSH_BUILDER_H

#include "crush.h"

extern struct crush_map *crush_create();
extern void crush_finalize(struct crush_map *map);

/* rules */
extern struct crush_rule *crush_make_rule(int len, int ruleset, int type, int minsize, int maxsize);
extern void crush_rule_set_step(struct crush_rule *rule, int pos, int op, int arg1, int arg2);
extern int crush_add_rule(struct crush_map *map, struct crush_rule *rule, int ruleno);

/* buckets */
extern int crush_get_next_bucket_id(struct crush_map *map);
extern int crush_add_bucket(struct crush_map *map,
			    int bucketno,
			    struct crush_bucket *bucket, int *idout);
struct crush_bucket *crush_make_bucket(struct crush_map *map, int alg, int hash, int type, int size, int *items, int *weights);
extern int crush_bucket_add_item(struct crush_map *map, struct crush_bucket *bucket, int item, int weight);
extern int crush_bucket_adjust_item_weight(struct crush_map *map, struct crush_bucket *bucket, int item, int weight);
extern int crush_reweight_bucket(struct crush_map *crush, struct crush_bucket *bucket);
extern int crush_remove_bucket(struct crush_map *map, struct crush_bucket *bucket);
extern int crush_bucket_remove_item(struct crush_map *map, struct crush_bucket *bucket, int item);

struct crush_bucket_uniform *
crush_make_uniform_bucket(int hash, int type, int size,
			  int *items,
			  int item_weight);
struct crush_bucket_list*
crush_make_list_bucket(int hash, int type, int size,
		       int *items,
		       int *weights);
struct crush_bucket_tree*
crush_make_tree_bucket(int hash, int type, int size,
		       int *items,    /* in leaf order */
		       int *weights);
struct crush_bucket_straw *
crush_make_straw_bucket(struct crush_map *map,
			int hash, int type, int size,
			int *items,
			int *weights);

extern int crush_addition_is_unsafe(__u32 a, __u32 b);
extern int crush_multiplication_is_unsafe(__u32  a, __u32 b);

#endif
