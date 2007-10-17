
#include <string.h>
#include <math.h>
#include <stdlib.h>

#include "builder.h"
#include "hash.h"


struct crush_map *crush_new()
{
	struct crush_map *m;
	m = malloc(sizeof(*m));
	memset(&m, 0, sizeof(m));
	return m;
}

/*
 * finalize should be called _after_ all buckets are added to the map.
 */
void crush_finalize(struct crush_map *map)
{
	int b, i, c;
	
	/* calc max_devices */
	for (b=0; b<map->max_buckets; b++) {
		if (map->buckets[b] == 0) continue;
		for (i=0; i<map->buckets[b]->size; i++) 
			if (map->buckets[b]->items[i] > map->max_devices)
				map->max_devices = map->buckets[b]->items[i];
	}
	
	/* allocate arrays */
	map->device_parents = malloc(sizeof(map->device_parents[0]) * map->max_devices);
	map->device_offload = malloc(sizeof(map->device_offload[0]) * map->max_devices);
	memset(map->device_parents, 0, sizeof(map->device_parents[0]) * map->max_devices);
	memset(map->device_offload, 0, sizeof(map->device_offload[0]) * map->max_devices);
	map->bucket_parents = malloc(sizeof(map->bucket_parents[0]) * map->max_buckets);
	memset(map->bucket_parents, 0, sizeof(map->bucket_parents[0]) * map->max_buckets);
	
	/* build reverse map */
	for (b=0; b<map->max_buckets; b++) {
		if (map->buckets[b] == 0) continue;
		for (i=0; i<map->buckets[b]->size; i++) {
			c = map->buckets[b]->items[i];
			if (c >= 0)
				map->device_parents[c] = map->buckets[b]->id;
			else
				map->bucket_parents[-c] = map->buckets[b]->id;
		}
	}
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
	
	memset(map, 0, sizeof(*map));
}




/** rules **/

int crush_add_rule(struct crush_map *map,
		   struct crush_rule *rule)
{
	int id;
	
	/* find a rule id */
	for (id=0; id < map->max_rules; id++)
		if (map->rules[id] == 0) break;
	if (id == map->max_rules) {
		/* expand array */
		if (map->max_rules)
			map->max_rules *= 2;
		else
			map->max_rules = 8;
		map->rules = realloc(map->rules, map->max_rules * sizeof(map->rules[0]));
	}
	
	/* add it */
	map->rules[id] = rule;
	return id;
}

struct crush_rule *crush_make_rule()
{
	struct crush_rule *rule;
	
	rule = malloc(sizeof(struct crush_rule));
	memset(&rule, 0, sizeof(rule));
	return rule;
}

void crush_rule_add_step(struct crush_rule *rule, int op, int arg1, int arg2)
{
	rule->len++;
	if (rule->steps)
		rule->steps = malloc(sizeof(rule->steps[0])*rule->len);
	else
		rule->steps = realloc(rule->steps, sizeof(rule->steps[0])*rule->len);
	rule->steps[rule->len-1].op = op;
	rule->steps[rule->len-1].arg1 = arg1;
	rule->steps[rule->len-1].arg2 = arg2;
}


/** buckets **/

int crush_add_bucket(struct crush_map *map,
		     struct crush_bucket *bucket)
{
	int id;
	
	/* find a bucket id */
	for (id=0; id < map->max_buckets; id++)
		if (map->buckets[id] == 0) break;
	if (id == map->max_buckets) {
		/* expand array */
		if (map->max_buckets)
			map->max_buckets *= 2;
		else
			map->max_buckets = 8;
		map->buckets = realloc(map->buckets, map->max_buckets * sizeof(map->buckets[0]));
	}
	
	/* add it */
	bucket->id = id;
	map->buckets[id] = bucket;
	return id;
}


void crush_make_uniform_bucket(struct crush_map *map,
			       struct crush_bucket_uniform *bucket,
			       int size,
			       int *items,
			       int item_weight)
{
	int i, j, x;
	
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.size = size;
	bucket->h.weight = size * item_weight;
	
	bucket->item_weight = item_weight;
	
	bucket->h.items = malloc(sizeof(__u32)*size);
	for (i=0; i<size; i++)
		bucket->h.items[i] = items[i];
	
	/* generate some primes */
	bucket->primes = malloc(sizeof(__u32)*size);

	x = size + 1;
	x += crush_hash32(size) % (3*size);  /* make it big */
	x |= 1;                              /* and odd */
	
	i=0;
	while (i < size) {
		for (j=2; j*j <= x; j++) 
			if (x % j == 0) break;
		if (j*j > x) 
			bucket->primes[i++] = x;
		x += 2;
	}
}

void crush_make_list_bucket(struct crush_map *map,
			    struct crush_bucket_list *bucket,
			    int size,
			    int *items,
			    int *weights)
{
	int i;
	int w;
	
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.size = size;
	
	bucket->h.items = malloc(sizeof(__u32)*size);
	bucket->item_weights = malloc(sizeof(__u32)*size);
	bucket->sum_weights = malloc(sizeof(__u32)*size);
	w = 0;
	for (i=0; i<size; i++) {
		bucket->h.items[i] = items[i];
		bucket->item_weights[i] = weights[i];
		w += weights[i];
		bucket->sum_weights[i] = w;
	}
	
	bucket->h.weight = w;
}


void crush_make_straw_bucket(struct crush_map *map,
			     struct crush_bucket_straw *bucket,
			     int size,
			     int *items,
			     int *weights)
{
	int *reverse;
	int i, j, k;
	
	double straw, wbelow, lastw, wnext, pbelow;
	int numleft;
	
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.size = size;
	
	bucket->h.items = malloc(sizeof(__u32)*size);
	bucket->straws = malloc(sizeof(__u32)*size);
	
	bucket->h.weight = 0;
	for (i=0; i<size; i++) {
		bucket->h.items[i] = items[i];
		bucket->h.weight += weights[i];
	}
	
	/* reverse sort by weight */
	reverse = malloc(sizeof(int) * size);
	reverse[0] = items[0];
	for (i=1; i<size; i++) {
		for (j=0; j<i; j++) {
			if (weights[i] < weights[reverse[j]]) {
				/* insert here */
				for (k=i; k>j; k--)
					reverse[k] = reverse[k-1];
				reverse[j] = items[i];
			}
		}
		if (j == i)
			reverse[i] = items[i];
	}
	
	numleft = size;
	straw = 1.0;
	wbelow = 0;
	lastw = 0;
	
	i=0;
	while (i < size) {
		/* set this item's straw */
		bucket->straws[reverse[i]] = straw * 0x10000;
		i++;
		if (i == size) break;
		
		/* same weight as previous? */
		if (weights[reverse[i]] == weights[reverse[i-1]]) 
			continue;
		
		/* adjust straw for next guy */
		wbelow += (((double)weights[reverse[i-1]] / (double)0x10000) - lastw) * numleft;
		numleft--;
		wnext = numleft * ((double)(weights[reverse[i]] - weights[reverse[i-1]]) / (double)0x10000);
		pbelow = wbelow / (wbelow + wnext);
		
		straw *= pow((double)1.0 / pbelow, (double)1.0 / numleft);
		
		lastw = weights[reverse[i-1]];
	}
	
	free(reverse);
	
}

