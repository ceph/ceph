
#ifdef __KERNEL__
# include <linux/slab.h>
# define free(x) kfree(x)
#else
# include <stdlib.h>
#endif

#include "crush.h"

void crush_calc_parents(struct crush_map *map)
{
	int i, b, c;
	for (b=0; b<map->max_buckets; b++) {
		if (map->buckets[b] == 0) continue;
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
	free(b->primes);
	free(b->h.items);
	free(b);
}

void crush_destroy_bucket_list(struct crush_bucket_list *b)
{
	free(b->item_weights);
	free(b->sum_weights);
	free(b->h.items);
	free(b);
}

void crush_destroy_bucket_tree(struct crush_bucket_tree *b)
{
	free(b->node_weights);
	free(b);
}

void crush_destroy_bucket_straw(struct crush_bucket_straw *b)
{
	free(b->straws);
	free(b->h.items);
	free(b);
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
			if (map->buckets[b] == 0) continue;
			switch (map->buckets[b]->type) {
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
		free(map->buckets);
	}
	
	/* rules */
	if (map->rules) {
		for (b=0; b<map->max_rules; b++) {
			if (map->rules[b] == 0) continue;
			free(map->rules[b]);
		}
		free(map->rules);
	}
	
	if (map->bucket_parents)
		free(map->bucket_parents);
	if (map->device_parents)
		free(map->device_parents);
	if (map->device_offload)
		free(map->device_offload);
	free(map);
}


