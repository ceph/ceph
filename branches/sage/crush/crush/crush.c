
#ifdef __KERNEL__
# define free(x) kfree(x)
#else
# include <stdlib.h>
#endif

#include "crush.h"

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
	
	/* rules */
	for (b=0; b<map->max_rules; b++) {
		if (map->rules[b] == 0) continue;
		if (map->rules[b]->steps)
			free(map->rules[b]->steps);
		free(map->rules[b]);
	}
	free(map->rules);
	
	free(map->bucket_parents);
	free(map->device_parents);
	free(map->device_offload);
	free(map);
}


