#include <string.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>

#include "include/int_types.h"

#include "builder.h"
#include "hash.h"

#define BUG_ON(x) assert(!(x))

struct crush_map *crush_create()
{
	struct crush_map *m;
	m = malloc(sizeof(*m));
        if (!m)
                return NULL;
	memset(m, 0, sizeof(*m));

	/* initialize legacy tunable values */
	m->choose_local_tries = 2;
	m->choose_local_fallback_tries = 5;
	m->choose_total_tries = 19;
	m->chooseleaf_descend_once = 0;
	return m;
}

/*
 * finalize should be called _after_ all buckets are added to the map.
 */
void crush_finalize(struct crush_map *map)
{
	int b;
	__u32 i;

	/* calc max_devices */
	map->max_devices = 0;
	for (b=0; b<map->max_buckets; b++) {
		if (map->buckets[b] == 0)
			continue;
		for (i=0; i<map->buckets[b]->size; i++)
			if (map->buckets[b]->items[i] >= map->max_devices)
				map->max_devices = map->buckets[b]->items[i] + 1;
	}
}





/** rules **/

int crush_add_rule(struct crush_map *map, struct crush_rule *rule, int ruleno)
{
	__u32 r;

	if (ruleno < 0) {
		for (r=0; r < map->max_rules; r++)
			if (map->rules[r] == 0)
				break;
		assert(r <= INT_MAX);
	}
	else
		r = ruleno;

	if (r >= map->max_rules) {
		/* expand array */
		int oldsize;
		void *_realloc = NULL;
		oldsize = map->max_rules;
		map->max_rules = r+1;
		if ((_realloc = realloc(map->rules, map->max_rules * sizeof(map->rules[0]))) == NULL) {
			return -ENOMEM; 
		} else {
			map->rules = _realloc;
		} 
		memset(map->rules + oldsize, 0, (map->max_rules-oldsize) * sizeof(map->rules[0]));
	}

	/* add it */
	map->rules[r] = rule;
	return r;
}

struct crush_rule *crush_make_rule(int len, int ruleset, int type, int minsize, int maxsize)
{
	struct crush_rule *rule;
	rule = malloc(crush_rule_size(len));
        if (!rule)
                return NULL;
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
	assert((__u32)n < rule->len);
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
		     struct crush_bucket *bucket,
		     int *idout)
{
	int pos;

	/* find a bucket id */
	if (id == 0)
		id = crush_get_next_bucket_id(map);
	pos = -1 - id;

	while (pos >= map->max_buckets) {
		/* expand array */
		int oldsize = map->max_buckets;
		if (map->max_buckets)
			map->max_buckets *= 2;
		else
			map->max_buckets = 8;
		void *_realloc = NULL;
		if ((_realloc = realloc(map->buckets, map->max_buckets * sizeof(map->buckets[0]))) == NULL) {
			return -ENOMEM; 
		} else {
			map->buckets = _realloc;
		}
		memset(map->buckets + oldsize, 0, (map->max_buckets-oldsize) * sizeof(map->buckets[0]));
	}

	if (map->buckets[pos] != 0) {
		return -EEXIST;
	}

        /* add it */
	bucket->id = id;
	map->buckets[pos] = bucket;

	if (idout) *idout = id;
	return 0;
}

int crush_remove_bucket(struct crush_map *map, struct crush_bucket *bucket)
{
	int pos = -1 - bucket->id;

	map->buckets[pos] = NULL;
	crush_destroy_bucket(bucket);
	return 0;
}


/* uniform bucket */

struct crush_bucket_uniform *
crush_make_uniform_bucket(int hash, int type, int size,
			  int *items,
			  int item_weight)
{
	int i;
	struct crush_bucket_uniform *bucket;

	bucket = malloc(sizeof(*bucket));
        if (!bucket)
                return NULL;
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.alg = CRUSH_BUCKET_UNIFORM;
	bucket->h.hash = hash;
	bucket->h.type = type;
	bucket->h.size = size;

	if (crush_multiplication_is_unsafe(size, item_weight))
                goto err;

	bucket->h.weight = size * item_weight;
	bucket->item_weight = item_weight;
	bucket->h.items = malloc(sizeof(__s32)*size);

        if (!bucket->h.items)
                goto err;

        bucket->h.perm = malloc(sizeof(__u32)*size);

        if (!bucket->h.perm)
                goto err;
	for (i=0; i<size; i++)
		bucket->h.items[i] = items[i];

	return bucket;
err:
        free(bucket->h.perm);
        free(bucket->h.items);
        free(bucket);
        return NULL;
}


/* list bucket */

struct crush_bucket_list*
crush_make_list_bucket(int hash, int type, int size,
		       int *items,
		       int *weights)
{
	int i;
	int w;
	struct crush_bucket_list *bucket;

	bucket = malloc(sizeof(*bucket));
        if (!bucket)
                return NULL;
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.alg = CRUSH_BUCKET_LIST;
	bucket->h.hash = hash;
	bucket->h.type = type;
	bucket->h.size = size;

	bucket->h.items = malloc(sizeof(__s32)*size);
        if (!bucket->h.items)
                goto err;
	bucket->h.perm = malloc(sizeof(__u32)*size);
        if (!bucket->h.perm)
                goto err;


        bucket->item_weights = malloc(sizeof(__u32)*size);
        if (!bucket->item_weights)
                goto err;
	bucket->sum_weights = malloc(sizeof(__u32)*size);
        if (!bucket->sum_weights)
                goto err;
	w = 0;
	for (i=0; i<size; i++) {
		bucket->h.items[i] = items[i];
		bucket->item_weights[i] = weights[i];

		if (crush_addition_is_unsafe(w, weights[i]))
                        goto err;

		w += weights[i];
		bucket->sum_weights[i] = w;
		/*printf("pos %d item %d weight %d sum %d\n",
		  i, items[i], weights[i], bucket->sum_weights[i]);*/
	}

	bucket->h.weight = w;

	return bucket;
err:
        free(bucket->sum_weights);
        free(bucket->item_weights);
        free(bucket->h.perm);
        free(bucket->h.items);
        free(bucket);
        return NULL;
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

static int calc_depth(int size)
{
	int depth = 1;
	int t = size - 1;
	while (t) {
		t = t >> 1;
		depth++;
	}
	return depth;
}

struct crush_bucket_tree*
crush_make_tree_bucket(int hash, int type, int size,
		       int *items,    /* in leaf order */
		       int *weights)
{
	struct crush_bucket_tree *bucket;
	int depth;
	int node;
	int i, j;

	bucket = malloc(sizeof(*bucket));
        if (!bucket)
                return NULL;
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.alg = CRUSH_BUCKET_TREE;
	bucket->h.hash = hash;
	bucket->h.type = type;
	bucket->h.size = size;

	bucket->h.items = malloc(sizeof(__s32)*size);
        if (!bucket->h.items)
                goto err;
	bucket->h.perm = malloc(sizeof(__u32)*size);
        if (!bucket->h.perm)
                goto err;

	/* calc tree depth */
	depth = calc_depth(size);
	bucket->num_nodes = 1 << depth;
	printf("size %d depth %d nodes %d\n", size, depth, bucket->num_nodes);

        bucket->node_weights = malloc(sizeof(__u32)*bucket->num_nodes);
        if (!bucket->node_weights)
                goto err;

	memset(bucket->h.items, 0, sizeof(__s32)*bucket->h.size);
	memset(bucket->node_weights, 0, sizeof(__u32)*bucket->num_nodes);

	for (i=0; i<size; i++) {
		bucket->h.items[i] = items[i];
		node = crush_calc_tree_node(i);
		printf("item %d node %d weight %d\n", i, node, weights[i]);
		bucket->node_weights[node] = weights[i];

		if (crush_addition_is_unsafe(bucket->h.weight, weights[i]))
                        goto err;

		bucket->h.weight += weights[i];
		for (j=1; j<depth; j++) {
			node = parent(node);

                        if (crush_addition_is_unsafe(bucket->node_weights[node], weights[i]))
                                goto err;

			bucket->node_weights[node] += weights[i];
			printf(" node %d weight %d\n", node, bucket->node_weights[node]);
		}
	}
	BUG_ON(bucket->node_weights[bucket->num_nodes/2] != bucket->h.weight);

	return bucket;
err:
        free(bucket->node_weights);
        free(bucket->h.perm);
        free(bucket->h.items);
        free(bucket);
        return NULL;
}



/* straw bucket */

int crush_calc_straw(struct crush_bucket_straw *bucket)
{
	int *reverse;
	int i, j, k;
	double straw, wbelow, lastw, wnext, pbelow;
	int numleft;
	int size = bucket->h.size;
	__u32 *weights = bucket->item_weights;

	/* reverse sort by weight (simple insertion sort) */
	reverse = malloc(sizeof(int) * size);
        if (!reverse)
                return -ENOMEM;
	if (size)
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
		/* zero weight items get 0 length straws! */
		if (weights[reverse[i]] == 0) {
			bucket->straws[reverse[i]] = 0;
			i++;
			continue;
		}

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
	return 0;
}

struct crush_bucket_straw *
crush_make_straw_bucket(int hash, 
			int type,
			int size,
			int *items,
			int *weights)
{
	struct crush_bucket_straw *bucket;
	int i;

	bucket = malloc(sizeof(*bucket));
        if (!bucket)
                return NULL;
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.alg = CRUSH_BUCKET_STRAW;
	bucket->h.hash = hash;
	bucket->h.type = type;
	bucket->h.size = size;

        bucket->h.items = malloc(sizeof(__s32)*size);
        if (!bucket->h.items)
                goto err;
	bucket->h.perm = malloc(sizeof(__u32)*size);
        if (!bucket->h.perm)
                goto err;
	bucket->item_weights = malloc(sizeof(__u32)*size);
        if (!bucket->item_weights)
                goto err;
        bucket->straws = malloc(sizeof(__u32)*size);
        if (!bucket->straws)
                goto err;

        bucket->h.weight = 0;
	for (i=0; i<size; i++) {
		bucket->h.items[i] = items[i];
		bucket->h.weight += weights[i];
		bucket->item_weights[i] = weights[i];
	}

        if (crush_calc_straw(bucket) < 0)
                goto err;

	return bucket;
err:
        free(bucket->straws);
        free(bucket->item_weights);
        free(bucket->h.perm);
        free(bucket->h.items);
        free(bucket);
        return NULL;
}



struct crush_bucket*
crush_make_bucket(int alg, int hash, int type, int size,
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
		return (struct crush_bucket *)crush_make_uniform_bucket(hash, type, size, items, item_weight);

	case CRUSH_BUCKET_LIST:
		return (struct crush_bucket *)crush_make_list_bucket(hash, type, size, items, weights);

	case CRUSH_BUCKET_TREE:
		return (struct crush_bucket *)crush_make_tree_bucket(hash, type, size, items, weights);

	case CRUSH_BUCKET_STRAW:
		return (struct crush_bucket *)crush_make_straw_bucket(hash, type, size, items, weights);
	}
	return 0;
}


/************************************************/

int crush_add_uniform_bucket_item(struct crush_bucket_uniform *bucket, int item, int weight)
{
        int newsize = bucket->h.size + 1;
	void *_realloc = NULL;
	
	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
	}
	if ((_realloc = realloc(bucket->h.perm, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.perm = _realloc;
	}

	bucket->h.items[newsize-1] = item;

        if (crush_addition_is_unsafe(bucket->h.weight, weight))
                return -ERANGE;

        bucket->h.weight += weight;
        bucket->h.size++;

        return 0;
}

int crush_add_list_bucket_item(struct crush_bucket_list *bucket, int item, int weight)
{
        int newsize = bucket->h.size + 1;
	void *_realloc = NULL;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
	}
	if ((_realloc = realloc(bucket->h.perm, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.perm = _realloc;
	}
	if ((_realloc = realloc(bucket->item_weights, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->item_weights = _realloc;
	}
	if ((_realloc = realloc(bucket->sum_weights, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->sum_weights = _realloc;
	}
	
	bucket->h.items[newsize-1] = item;
	bucket->item_weights[newsize-1] = weight;
	if (newsize > 1) {

                if (crush_addition_is_unsafe(bucket->sum_weights[newsize-2], weight))
                        return -ERANGE;

                bucket->sum_weights[newsize-1] = bucket->sum_weights[newsize-2] + weight;
	}

        else {
                bucket->sum_weights[newsize-1] = weight;
        }

	bucket->h.weight += weight;
	bucket->h.size++;
	return 0;
}

int crush_add_tree_bucket_item(struct crush_bucket_tree *bucket, int item, int weight)
{
	int newsize = bucket->h.size + 1;
	int depth = calc_depth(newsize);;
	int node;
	int j;
	void *_realloc = NULL;

	bucket->num_nodes = 1 << depth;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
	}
	if ((_realloc = realloc(bucket->h.perm, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.perm = _realloc;
	}
	if ((_realloc = realloc(bucket->node_weights, sizeof(__u32)*bucket->num_nodes)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->node_weights = _realloc;
	}
	
	node = crush_calc_tree_node(newsize-1);
	bucket->node_weights[node] = weight;

	for (j=1; j<depth; j++) {
		node = parent(node);

                if (!crush_addition_is_unsafe(bucket->node_weights[node], weight))
                        return -ERANGE;

		bucket->node_weights[node] += weight;
                printf(" node %d weight %d\n", node, bucket->node_weights[node]);
	}


	if (crush_addition_is_unsafe(bucket->h.weight, weight))
                return -ERANGE;
	
        bucket->h.weight += weight;
        bucket->h.size++;

	return 0;
}

int crush_add_straw_bucket_item(struct crush_bucket_straw *bucket, int item, int weight)
{
	int newsize = bucket->h.size + 1;
	
	void *_realloc = NULL;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
	}
	if ((_realloc = realloc(bucket->h.perm, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.perm = _realloc;
	}
	if ((_realloc = realloc(bucket->item_weights, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->item_weights = _realloc;
	}
	if ((_realloc = realloc(bucket->straws, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->straws = _realloc;
	}

	bucket->h.items[newsize-1] = item;
	bucket->item_weights[newsize-1] = weight;

	if (crush_addition_is_unsafe(bucket->h.weight, weight))
                return -ERANGE;

	 bucket->h.weight += weight;
	 bucket->h.size++;
	
	return crush_calc_straw(bucket);
}

int crush_bucket_add_item(struct crush_bucket *b, int item, int weight)
{
	/* invalidate perm cache */
	b->perm_n = 0;

	switch (b->alg) {
	case CRUSH_BUCKET_UNIFORM:
		return crush_add_uniform_bucket_item((struct crush_bucket_uniform *)b, item, weight);
	case CRUSH_BUCKET_LIST:
		return crush_add_list_bucket_item((struct crush_bucket_list *)b, item, weight);
	case CRUSH_BUCKET_TREE:
		return crush_add_tree_bucket_item((struct crush_bucket_tree *)b, item, weight);
	case CRUSH_BUCKET_STRAW:
		return crush_add_straw_bucket_item((struct crush_bucket_straw *)b, item, weight);
	default:
		return -1;
	}
	return 0;
}

/************************************************/

int crush_remove_uniform_bucket_item(struct crush_bucket_uniform *bucket, int item)
{
	unsigned i, j;
	int newsize;
	void *_realloc = NULL;
	
	for (i = 0; i < bucket->h.size; i++)
		if (bucket->h.items[i] == item)
			break;
	if (i == bucket->h.size)
		return -ENOENT;

	for (j = i; j < bucket->h.size; j++)
		bucket->h.items[j] = bucket->h.items[j+1];
	newsize = --bucket->h.size;
	bucket->h.weight -= bucket->item_weight;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
	}
	if ((_realloc = realloc(bucket->h.perm, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.perm = _realloc;
	}
	return 0;
}

int crush_remove_list_bucket_item(struct crush_bucket_list *bucket, int item)
{
	unsigned i, j;
	int newsize;
	int weight;

	for (i = 0; i < bucket->h.size; i++)
		if (bucket->h.items[i] == item)
			break;
	if (i == bucket->h.size)
		return -ENOENT;

	weight = bucket->item_weights[i];
	for (j = i; j < bucket->h.size; j++) {
		bucket->h.items[j] = bucket->h.items[j+1];
		bucket->item_weights[j] = bucket->item_weights[j+1];
		bucket->sum_weights[j] = bucket->sum_weights[j+1] - weight;
	}
	bucket->h.weight -= weight;
	newsize = --bucket->h.size;
	
	void *_realloc = NULL;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
	}
	if ((_realloc = realloc(bucket->h.perm, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.perm = _realloc;
	}
	if ((_realloc = realloc(bucket->item_weights, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->item_weights = _realloc;
	}
	if ((_realloc = realloc(bucket->sum_weights, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->sum_weights = _realloc;
	}
	return 0;
}

int crush_remove_tree_bucket_item(struct crush_bucket_tree *bucket, int item)
{
	unsigned i;
	unsigned newsize;

	for (i = 0; i < bucket->h.size; i++) {
		int node;
		int weight;
		int j;
		int depth = calc_depth(bucket->h.size);

		if (bucket->h.items[i] != item)
			continue;
		
		node = crush_calc_tree_node(i);
		weight = bucket->node_weights[node];
		bucket->node_weights[node] = 0;

		for (j = 1; j < depth; j++) {
			node = parent(node);
			bucket->node_weights[node] -= weight;
			printf(" node %d weight %d\n", node, bucket->node_weights[node]);
		}
		bucket->h.weight -= weight;
		break;
	}
	if (i == bucket->h.size)
		return -ENOENT;

	newsize = bucket->h.size;
	while (newsize > 0) {
		int node = crush_calc_tree_node(newsize - 1);
		if (bucket->node_weights[node])
			break;
		--newsize;
	}

	if (newsize != bucket->h.size) {
		int olddepth, newdepth;

		void *_realloc = NULL;

		if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
			return -ENOMEM;
		} else {
			bucket->h.items = _realloc;
		}
		if ((_realloc = realloc(bucket->h.perm, sizeof(__u32)*newsize)) == NULL) {
			return -ENOMEM;
		} else {
			bucket->h.perm = _realloc;
		}

		olddepth = calc_depth(bucket->h.size);
		newdepth = calc_depth(newsize);
		if (olddepth != newdepth) {
			bucket->num_nodes = 1 << newdepth;
			if ((_realloc = realloc(bucket->node_weights, 
						sizeof(__u32)*bucket->num_nodes)) == NULL) {
				return -ENOMEM;
			} else {
				bucket->node_weights = _realloc;
			}
		}

		bucket->h.size = newsize;
	}
	return 0;
}

int crush_remove_straw_bucket_item(struct crush_bucket_straw *bucket, int item)
{
	int newsize = bucket->h.size - 1;
	unsigned i, j;

	for (i = 0; i < bucket->h.size; i++) {
		if (bucket->h.items[i] == item) {
			bucket->h.size--;
			bucket->h.weight -= bucket->item_weights[i];
			for (j = i; j < bucket->h.size; j++) {
				bucket->h.items[j] = bucket->h.items[j+1];
				bucket->item_weights[j] = bucket->item_weights[j+1];
			}
			break;
		}
	}
	if (i == bucket->h.size)
		return -ENOENT;
	
	void *_realloc = NULL;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
	}
	if ((_realloc = realloc(bucket->h.perm, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.perm = _realloc;
	}
	if ((_realloc = realloc(bucket->item_weights, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->item_weights = _realloc;
	}
	if ((_realloc = realloc(bucket->straws, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->straws = _realloc;
	}

	return crush_calc_straw(bucket);
}

int crush_bucket_remove_item(struct crush_bucket *b, int item)
{
	/* invalidate perm cache */
	b->perm_n = 0;

	switch (b->alg) {
	case CRUSH_BUCKET_UNIFORM:
		return crush_remove_uniform_bucket_item((struct crush_bucket_uniform *)b, item);
	case CRUSH_BUCKET_LIST:
		return crush_remove_list_bucket_item((struct crush_bucket_list *)b, item);
	case CRUSH_BUCKET_TREE:
		return crush_remove_tree_bucket_item((struct crush_bucket_tree *)b, item);
	case CRUSH_BUCKET_STRAW:
		return crush_remove_straw_bucket_item((struct crush_bucket_straw *)b, item);
	default:
		return -1;
	}
	return 0;
}


/************************************************/

int crush_adjust_uniform_bucket_item_weight(struct crush_bucket_uniform *bucket, int item, int weight)
{
	int diff = (weight - bucket->item_weight) * bucket->h.size;

	bucket->item_weight = weight;
	bucket->h.weight = bucket->item_weight * bucket->h.size;

	return diff;
}

int crush_adjust_list_bucket_item_weight(struct crush_bucket_list *bucket, int item, int weight)
{
	int diff;
	unsigned i, j;

	for (i = 0; i < bucket->h.size; i++) {
		if (bucket->h.items[i] == item)
			break;
	}
	if (i == bucket->h.size)
		return 0;

	diff = weight - bucket->item_weights[i];
	bucket->item_weights[i] = weight;
	bucket->h.weight += diff;

	for (j = i; j < bucket->h.size; j++)
		bucket->sum_weights[j] += diff;

	return diff;
}

int crush_adjust_tree_bucket_item_weight(struct crush_bucket_tree *bucket, int item, int weight)
{
	int diff;
	int node;
	unsigned i, j;
	unsigned depth = calc_depth(bucket->h.size);

	for (i = 0; i < bucket->h.size; i++) {
		if (bucket->h.items[i] == item)
			break;
	}
	if (i == bucket->h.size)
		return 0;
	
	node = crush_calc_tree_node(i);
	diff = weight - bucket->node_weights[node];
	bucket->node_weights[node] = weight;
	bucket->h.weight += diff;

	for (j=1; j<depth; j++) {
		node = parent(node);
		bucket->node_weights[node] += diff;
	}

	return diff;
}

int crush_adjust_straw_bucket_item_weight(struct crush_bucket_straw *bucket, int item, int weight)
{
	unsigned idx;
	int diff;
        int r;

	for (idx = 0; idx < bucket->h.size; idx++)
		if (bucket->h.items[idx] == item)
			break;
	if (idx == bucket->h.size)
		return 0;

	diff = weight - bucket->item_weights[idx];
	bucket->item_weights[idx] = weight;
	bucket->h.weight += diff;

	r = crush_calc_straw(bucket);
        if (r < 0)
                return r;

	return diff;
}

int crush_bucket_adjust_item_weight(struct crush_bucket *b, int item, int weight)
{
	switch (b->alg) {
	case CRUSH_BUCKET_UNIFORM:
		return crush_adjust_uniform_bucket_item_weight((struct crush_bucket_uniform *)b,
							     item, weight);
	case CRUSH_BUCKET_LIST:
		return crush_adjust_list_bucket_item_weight((struct crush_bucket_list *)b,
							    item, weight);
	case CRUSH_BUCKET_TREE:
		return crush_adjust_tree_bucket_item_weight((struct crush_bucket_tree *)b,
							    item, weight);
	case CRUSH_BUCKET_STRAW:
		return crush_adjust_straw_bucket_item_weight((struct crush_bucket_straw *)b,
							     item, weight);
	default:
		return -1;
	}
	return 0;
}

/************************************************/

static int crush_reweight_uniform_bucket(struct crush_map *crush, struct crush_bucket_uniform *bucket)
{
	unsigned i;
	unsigned sum = 0, n = 0, leaves = 0;

	for (i = 0; i < bucket->h.size; i++) {
		int id = bucket->h.items[i];
		if (id < 0) {
			struct crush_bucket *c = crush->buckets[-1-id];
			crush_reweight_bucket(crush, c);

			if (crush_addition_is_unsafe(sum, c->weight))
                                return -ERANGE;

			sum += c->weight;
			n++;
		} else {
			leaves++;
		}
	}

	if (n > leaves)
		bucket->item_weight = sum / n;  // more bucket children than leaves, average!
	bucket->h.weight = bucket->item_weight * bucket->h.size;

	return 0;
}

static int crush_reweight_list_bucket(struct crush_map *crush, struct crush_bucket_list *bucket)
{
	unsigned i;

	bucket->h.weight = 0;
	for (i = 0; i < bucket->h.size; i++) {
		int id = bucket->h.items[i];
		if (id < 0) {
			struct crush_bucket *c = crush->buckets[-1-id];
			crush_reweight_bucket(crush, c);
			bucket->item_weights[i] = c->weight;
		}

		if (crush_addition_is_unsafe(bucket->h.weight, bucket->item_weights[i]))
                        return -ERANGE;

		bucket->h.weight += bucket->item_weights[i];
	}

	return 0;
}

static int crush_reweight_tree_bucket(struct crush_map *crush, struct crush_bucket_tree *bucket)
{
	unsigned i;

	bucket->h.weight = 0;
	for (i = 0; i < bucket->h.size; i++) {
		int node = crush_calc_tree_node(i);
		int id = bucket->h.items[i];
		if (id < 0) {
			struct crush_bucket *c = crush->buckets[-1-id];
			crush_reweight_bucket(crush, c);
			bucket->node_weights[node] = c->weight;
		}

		if (crush_addition_is_unsafe(bucket->h.weight, bucket->node_weights[node]))
                        return -ERANGE;

		bucket->h.weight += bucket->node_weights[node];


	}

	return 0;
}

static int crush_reweight_straw_bucket(struct crush_map *crush, struct crush_bucket_straw *bucket)
{
	unsigned i;

	bucket->h.weight = 0;
	for (i = 0; i < bucket->h.size; i++) {
		int id = bucket->h.items[i];
		if (id < 0) {
			struct crush_bucket *c = crush->buckets[-1-id];
			crush_reweight_bucket(crush, c);
			bucket->item_weights[i] = c->weight;
		}

                if (crush_addition_is_unsafe(bucket->h.weight, bucket->item_weights[i]))
                        return -ERANGE;

                bucket->h.weight += bucket->item_weights[i];
	}

	return 0;
}

int crush_reweight_bucket(struct crush_map *crush, struct crush_bucket *b)
{
	switch (b->alg) {
	case CRUSH_BUCKET_UNIFORM:
		return crush_reweight_uniform_bucket(crush, (struct crush_bucket_uniform *)b);
	case CRUSH_BUCKET_LIST:
		return crush_reweight_list_bucket(crush, (struct crush_bucket_list *)b);
	case CRUSH_BUCKET_TREE:
		return crush_reweight_tree_bucket(crush, (struct crush_bucket_tree *)b);
	case CRUSH_BUCKET_STRAW:
		return crush_reweight_straw_bucket(crush, (struct crush_bucket_straw *)b);
	default:
		return -1;
	}
	return 0;
}

/***************************/

