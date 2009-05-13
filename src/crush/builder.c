
#include <string.h>
#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "builder.h"
#include "hash.h"

#define BUG_ON(x) assert(!(x))

struct crush_map *crush_create()
{
	struct crush_map *m;
	m = malloc(sizeof(*m));
	memset(m, 0, sizeof(*m));
	return m;
}

/*
 * finalize should be called _after_ all buckets are added to the map.
 */
void crush_finalize(struct crush_map *map)
{
	int b, i;

	/* calc max_devices */
	for (b=0; b<map->max_buckets; b++) {
		if (map->buckets[b] == 0) continue;
		for (i=0; i<map->buckets[b]->size; i++)
			if (map->buckets[b]->items[i] >= map->max_devices)
				map->max_devices = map->buckets[b]->items[i] + 1;
	}

	/* allocate arrays */
	map->device_parents = malloc(sizeof(map->device_parents[0]) * map->max_devices);
	memset(map->device_parents, 0, sizeof(map->device_parents[0]) * map->max_devices);
	map->bucket_parents = malloc(sizeof(map->bucket_parents[0]) * map->max_buckets);
	memset(map->bucket_parents, 0, sizeof(map->bucket_parents[0]) * map->max_buckets);

	/* build parent maps */
	crush_calc_parents(map);
}





/** rules **/

int crush_add_rule(struct crush_map *map, struct crush_rule *rule, int ruleno)
{
	int oldsize;

	if (ruleno < 0)
		for (ruleno=0; ruleno < map->max_rules; ruleno++)
			if (map->rules[ruleno] == 0) break;

	if (ruleno >= map->max_rules) {
		/* expand array */
		oldsize = map->max_rules;
		map->max_rules = ruleno+1;
		map->rules = realloc(map->rules, map->max_rules * sizeof(map->rules[0]));
		memset(map->rules + oldsize, 0, (map->max_rules-oldsize) * sizeof(map->rules[0]));
	}

	/* add it */
	map->rules[ruleno] = rule;
	return ruleno;
}

struct crush_rule *crush_make_rule(int len, int ruleset, int type, int minsize, int maxsize)
{
	struct crush_rule *rule;
	rule = malloc(crush_rule_size(len));
	rule->len = len;
	rule->mask.ruleset = ruleset;
	rule->mask.type = type;
	rule->mask.min_size = minsize;
	rule->mask.max_size = maxsize;
	return rule;
}

/*
 * be careful; this doesn't verify that the buffer you allocated is big enough!
 */
void crush_rule_set_step(struct crush_rule *rule, int n, int op, int arg1, int arg2)
{
	assert(n < rule->len);
	rule->steps[n].op = op;
	rule->steps[n].arg1 = arg1;
	rule->steps[n].arg2 = arg2;
}


/** buckets **/
int crush_get_next_bucket_id(struct crush_map *map)
{
	int pos;
	for (pos=0; pos < map->max_buckets; pos++)
		if (map->buckets[pos] == 0)
			break;
	return -1 - pos;
}


int crush_add_bucket(struct crush_map *map,
		     int id,
		     struct crush_bucket *bucket)
{
	int oldsize;
	int pos;

	/* find a bucket id */
	if (id == 0)
		id = crush_get_next_bucket_id(map);
	pos = -1 - id;

	while (pos >= map->max_buckets) {
		/* expand array */
		oldsize = map->max_buckets;
		if (map->max_buckets)
			map->max_buckets *= 2;
		else
			map->max_buckets = 8;
		map->buckets = realloc(map->buckets, map->max_buckets * sizeof(map->buckets[0]));
		memset(map->buckets + oldsize, 0, (map->max_buckets-oldsize) * sizeof(map->buckets[0]));
	}

	assert(map->buckets[pos] == 0);

   	/* add it */
	bucket->id = id;
	map->buckets[pos] = bucket;

	return id;
}


/* uniform bucket */

struct crush_bucket_uniform *
crush_make_uniform_bucket(int type, int size,
			  int *items,
			  int item_weight)
{
	int i, j, x;
	struct crush_bucket_uniform *bucket;

	bucket = malloc(sizeof(*bucket));
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.alg = CRUSH_BUCKET_UNIFORM;
	bucket->h.type = type;
	bucket->h.size = size;
	bucket->h.weight = size * item_weight;

	bucket->item_weight = item_weight;

	bucket->h.items = malloc(sizeof(__u32)*size);
	for (i=0; i<size; i++)
		bucket->h.items[i] = items[i];

	/* generate some primes */
	bucket->perm = malloc(sizeof(__u32)*size);

	if (size < 1)
		return bucket;

	return bucket;
}


/* list bucket */

struct crush_bucket_list*
crush_make_list_bucket(int type, int size,
		       int *items,
		       int *weights)
{
	int i;
	int w;
	struct crush_bucket_list *bucket;

	bucket = malloc(sizeof(*bucket));
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.alg = CRUSH_BUCKET_LIST;
	bucket->h.type = type;
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
		/*printf("pos %d item %d weight %d sum %d\n",
		  i, items[i], weights[i], bucket->sum_weights[i]);*/
	}

	bucket->h.weight = w;

	return bucket;
}


/* tree bucket */

static int height(int n) {
	int h = 0;
	while ((n & 1) == 0) {
		h++;
		n = n >> 1;
	}
	return h;
}
static int on_right(int n, int h) {
	return n & (1 << (h+1));
}
static int parent(int n)
{
	int h = height(n);
	if (on_right(n, h))
		return n - (1<<h);
	else
		return n + (1<<h);
}

struct crush_bucket_tree*
crush_make_tree_bucket(int type, int size,
		       int *items,    /* in leaf order */
		       int *weights)
{
	struct crush_bucket_tree *bucket;
	int depth;
	int node;
	int t, i, j;

	bucket = malloc(sizeof(*bucket));
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.alg = CRUSH_BUCKET_TREE;
	bucket->h.type = type;

	/* calc tree depth */
	depth = 1;
	t = size - 1;
	while (t) {
		t = t >> 1;
		depth++;
	}
	bucket->h.size = 1 << depth;

	bucket->h.items = malloc(sizeof(__u32)*bucket->h.size);
	bucket->node_weights = malloc(sizeof(__u32)*bucket->h.size);

	memset(bucket->h.items, 0, sizeof(__u32)*bucket->h.size);
	memset(bucket->node_weights, 0, sizeof(__u32)*bucket->h.size);

	for (i=0; i<size; i++) {
		node = ((i+1) << 1)-1;
		bucket->h.items[node] = items[i];
		bucket->node_weights[node] = weights[i];
		bucket->h.weight += weights[i];
		for (j=1; j<depth; j++) {
			node = parent(node);
			bucket->node_weights[node] += weights[i];
		}
	}
	BUG_ON(bucket->node_weights[bucket->h.size/2] != bucket->h.weight);

	return bucket;
}



/* straw bucket */

struct crush_bucket_straw *
crush_make_straw_bucket(int type,
			int size,
			int *items,
			int *weights)
{
	struct crush_bucket_straw *bucket;
	int *reverse;
	int i, j, k;

	double straw, wbelow, lastw, wnext, pbelow;
	int numleft;

	bucket = malloc(sizeof(*bucket));
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.alg = CRUSH_BUCKET_STRAW;
	bucket->h.type = type;
	bucket->h.size = size;

	bucket->h.items = malloc(sizeof(__u32)*size);
	bucket->item_weights = malloc(sizeof(__u32)*size);
	bucket->straws = malloc(sizeof(__u32)*size);

	bucket->h.weight = 0;
	for (i=0; i<size; i++) {
		bucket->h.items[i] = items[i];
		bucket->h.weight += weights[i];
		bucket->item_weights[i] = weights[i];
	}

	/* reverse sort by weight (simple insertion sort) */
	reverse = malloc(sizeof(int) * size);
	reverse[0] = 0;
	for (i=1; i<size; i++) {
		for (j=0; j<i; j++) {
			if (weights[i] < weights[reverse[j]]) {
				/* insert here */
				for (k=i; k>j; k--)
					reverse[k] = reverse[k-1];
				reverse[j] = i;
				break;
			}
		}
		if (j == i)
			reverse[i] = i;
	}

	numleft = size;
	straw = 1.0;
	wbelow = 0;
	lastw = 0;

	i=0;
	while (i < size) {
		/* set this item's straw */
		bucket->straws[reverse[i]] = straw * 0x10000;
		/*printf("item %d at %d weight %d straw %d (%lf)\n",
		       items[reverse[i]],
		       reverse[i], weights[reverse[i]], bucket->straws[reverse[i]], straw);*/
		i++;
		if (i == size) break;

		/* same weight as previous? */
		if (weights[reverse[i]] == weights[reverse[i-1]]) {
			/*printf("same as previous\n");*/
			continue;
		}

		/* adjust straw for next guy */
		wbelow += ((double)weights[reverse[i-1]] - lastw) * numleft;
		for (j=i; j<size; j++)
			if (weights[reverse[j]] == weights[reverse[i]])
				numleft--;
			else
				break;
		wnext = numleft * (weights[reverse[i]] - weights[reverse[i-1]]);
		pbelow = wbelow / (wbelow + wnext);
		/*printf("wbelow %lf  wnext %lf  pbelow %lf\n", wbelow, wnext, pbelow);*/

		straw *= pow((double)1.0 / pbelow, (double)1.0 / (double)numleft);

		lastw = weights[reverse[i-1]];
	}

	free(reverse);

	return bucket;
}



struct crush_bucket*
crush_make_bucket(int alg, int type, int size,
		  int *items,
		  int *weights)
{
	int item_weight;

	switch (alg) {
	case CRUSH_BUCKET_UNIFORM:
		if (size && weights)
			item_weight = weights[0];
		else
			item_weight = 0;
		return (struct crush_bucket *)crush_make_uniform_bucket(type, size, items, item_weight);

	case CRUSH_BUCKET_LIST:
		return (struct crush_bucket *)crush_make_list_bucket(type, size, items, weights);

	case CRUSH_BUCKET_TREE:
		return (struct crush_bucket *)crush_make_tree_bucket(type, size, items, weights);

	case CRUSH_BUCKET_STRAW:
		return (struct crush_bucket *)crush_make_straw_bucket(type, size, items, weights);
	}
	return 0;
}
