
#ifdef __KERNEL__
# include <linux/slab.h>
#include "../ceph_tools.h"
#else
# include <stdlib.h>
# include <assert.h>
# define kfree(x) free(x)
# define BUG_ON(x) assert(!(x))
#endif

#include "crush.h"

int crush_get_bucket_item_weight(struct crush_bucket *b, int pos)
{
	if (pos >= b->size) 
		return 0;
	switch (b->alg) {
	case CRUSH_BUCKET_UNIFORM:
		return ((struct crush_bucket_uniform*)b)->item_weight;
	case CRUSH_BUCKET_LIST:
		return ((struct crush_bucket_list*)b)->item_weights[pos];
	case CRUSH_BUCKET_TREE: 
		if (pos & 1)
			return ((struct crush_bucket_tree*)b)->node_weights[pos];
		return 0;
	case CRUSH_BUCKET_STRAW:
		return ((struct crush_bucket_straw*)b)->item_weights[pos];
	}
	return 0;
}


void crush_calc_parents(struct crush_map *map)
{
	int i, b, c;
	for (b=0; b<map->max_buckets; b++) {
		if (map->buckets[b] == NULL) continue;
		for (i=0; i<map->buckets[b]->size; i++) {
			c = map->buckets[b]->items[i];
			BUG_ON(c >= map->max_devices);
			if (c >= 0)
				map->device_parents[c] = map->buckets[b]->id;
			else
				map->bucket_parents[-1-c] = map->buckets[b]->id;
		}
	}
}

void crush_destroy_bucket_uniform(struct crush_bucket_uniform *b)
{
	kfree(b->primes);
	kfree(b->h.items);
	kfree(b);
}

void crush_destroy_bucket_list(struct crush_bucket_list *b)
{
	kfree(b->item_weights);
	kfree(b->sum_weights);
	kfree(b->h.items);
	kfree(b);
}

void crush_destroy_bucket_tree(struct crush_bucket_tree *b)
{
	kfree(b->node_weights);
	kfree(b);
}

void crush_destroy_bucket_straw(struct crush_bucket_straw *b)
{
	kfree(b->straws);
	kfree(b->item_weights);
	kfree(b->h.items);
	kfree(b);
}


/* 
 * deallocate
 */
void crush_destroy(struct crush_map *map)
{
	int b;
	
	/* buckets */
	if (map->buckets) {
		for (b=0; b<map->max_buckets; b++) {
			if (map->buckets[b] == NULL) continue;
			switch (map->buckets[b]->alg) {
			case CRUSH_BUCKET_UNIFORM:
				crush_destroy_bucket_uniform((struct crush_bucket_uniform*)map->buckets[b]);
				break;
			case CRUSH_BUCKET_LIST:
				crush_destroy_bucket_list((struct crush_bucket_list*)map->buckets[b]);
				break;
			case CRUSH_BUCKET_TREE:
				crush_destroy_bucket_tree((struct crush_bucket_tree*)map->buckets[b]);
				break;
			case CRUSH_BUCKET_STRAW:
				crush_destroy_bucket_straw((struct crush_bucket_straw*)map->buckets[b]);
				break;
			}
		}
		kfree(map->buckets);
	}
	
	/* rules */
	if (map->rules) {
		for (b=0; b<map->max_rules; b++) {
			if (map->rules[b] == NULL) continue;
			kfree(map->rules[b]);
		}
		kfree(map->rules);
	}
	
	if (map->bucket_parents)
		kfree(map->bucket_parents);
	if (map->device_parents)
		kfree(map->device_parents);
	if (map->device_offload)
		kfree(map->device_offload);
	kfree(map);
}


