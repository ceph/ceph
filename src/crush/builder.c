#include <string.h>
#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>

#include "crush/crush.h"
#include "builder.h"

#define dprintk(args...) /* printf(args) */

#define BUG_ON(x) assert(!(x))

struct crush_map *crush_create()
{
	struct crush_map *m;
	m = malloc(sizeof(*m));
        if (!m)
                return NULL;
	memset(m, 0, sizeof(*m));

	set_optimal_crush_map(m);
	return m;
}

/*
 * finalize should be called _after_ all buckets are added to the map.
 */
void crush_finalize(struct crush_map *map)
{
	int b;
	__u32 i;

	/* Calculate the needed working space while we do other
	   finalization tasks. */
	map->working_size = sizeof(struct crush_work);
	/* Space for the array of pointers to per-bucket workspace */
	map->working_size += map->max_buckets *
		sizeof(struct crush_work_bucket *);

	/* calc max_devices */
	map->max_devices = 0;
	for (b=0; b<map->max_buckets; b++) {
		if (map->buckets[b] == 0)
			continue;
		for (i=0; i<map->buckets[b]->size; i++)
			if (map->buckets[b]->items[i] >= map->max_devices)
				map->max_devices = map->buckets[b]->items[i] + 1;

		switch (map->buckets[b]->alg) {
		default:
			/* The base case, permutation variables and
			   the pointer to the permutation array. */
			map->working_size += sizeof(struct crush_work_bucket);
			break;
		}
		/* Every bucket has a permutation array. */
		map->working_size += map->buckets[b]->size * sizeof(__u32);
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
		assert(r < CRUSH_MAX_RULES);
	}
	else
		r = ruleno;

	if (r >= map->max_rules) {
		/* expand array */
		int oldsize;
		void *_realloc = NULL;
		if (map->max_rules +1 > CRUSH_MAX_RULES)
			return -ENOSPC;
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

struct crush_rule *crush_make_rule(int len, int type)
{
	struct crush_rule *rule;
	rule = malloc(crush_rule_size(len));
        if (!rule)
                return NULL;
	rule->len = len;
	rule->type = type;
	rule->deprecated_min_size = 1;
	rule->deprecated_max_size = 100;
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
       assert(pos < map->max_buckets);
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

	if (size == 0) {
		return bucket;
	}
	bucket->h.items = malloc(sizeof(__s32)*size);

        if (!bucket->h.items)
                goto err;

	for (i=0; i<size; i++)
		bucket->h.items[i] = items[i];

	return bucket;
err:
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

	if (size == 0) {
		return bucket;
	}

	bucket->h.items = malloc(sizeof(__s32)*size);
        if (!bucket->h.items)
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
		/*dprintk("pos %d item %d weight %d sum %d\n",
		  i, items[i], weights[i], bucket->sum_weights[i]);*/
	}

	bucket->h.weight = w;

	return bucket;
err:
        free(bucket->sum_weights);
        free(bucket->item_weights);
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
	if (size == 0) {
		return 0;
	}

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

	if (size == 0) {
		/* printf("size 0 depth 0 nodes 0\n"); */
		return bucket;
	}

	bucket->h.items = malloc(sizeof(__s32)*size);
        if (!bucket->h.items)
                goto err;

	/* calc tree depth */
	depth = calc_depth(size);
	bucket->num_nodes = 1 << depth;
	dprintk("size %d depth %d nodes %d\n", size, depth, bucket->num_nodes);

        bucket->node_weights = malloc(sizeof(__u32)*bucket->num_nodes);
        if (!bucket->node_weights)
                goto err;

	memset(bucket->h.items, 0, sizeof(__s32)*bucket->h.size);
	memset(bucket->node_weights, 0, sizeof(__u32)*bucket->num_nodes);

	for (i=0; i<size; i++) {
		bucket->h.items[i] = items[i];
		node = crush_calc_tree_node(i);
		dprintk("item %d node %d weight %d\n", i, node, weights[i]);
		bucket->node_weights[node] = weights[i];

		if (crush_addition_is_unsafe(bucket->h.weight, weights[i]))
                        goto err;

		bucket->h.weight += weights[i];
		for (j=1; j<depth; j++) {
			node = parent(node);

                        if (crush_addition_is_unsafe(bucket->node_weights[node], weights[i]))
                                goto err;

			bucket->node_weights[node] += weights[i];
			dprintk(" node %d weight %d\n", node, bucket->node_weights[node]);
		}
	}
	BUG_ON(bucket->node_weights[bucket->num_nodes/2] != bucket->h.weight);

	return bucket;
err:
        free(bucket->node_weights);
        free(bucket->h.items);
        free(bucket);
        return NULL;
}



/* straw bucket */

/*
 * this code was written 8 years ago.  i have a vague recollection of
 * drawing boxes underneath bars of different lengths, where the bar
 * length represented the probability/weight, and that there was some
 * trial and error involved in arriving at this implementation.
 * however, reading the code now after all this time, the intuition
 * that motivated is lost on me.  lame.  my only excuse is that I now
 * know that the approach is fundamentally flawed and am not
 * particularly motivated to reconstruct the flawed reasoning.
 *
 * as best as i can remember, the idea is: sort the weights, and start
 * with the smallest.  arbitrarily scale it at 1.0 (16-bit fixed
 * point).  look at the next larger weight, and calculate the scaling
 * factor for that straw based on the relative difference in weight so
 * far.  what's not clear to me now is why we are looking at wnext
 * (the delta to the next bigger weight) for all remaining weights,
 * and slicing things horizontally instead of considering just the
 * next item or set of items.  or why pow() is used the way it is.
 *
 * note that the original version 1 of this function made special
 * accommodation for the case where straw lengths were identical.  this
 * is also flawed in a non-obvious way; version 2 drops the special
 * handling and appears to work just as well.
 *
 * moral of the story: if you do something clever, write down why it
 * works.
 */
int crush_calc_straw(struct crush_map *map, struct crush_bucket_straw *bucket)
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
		if (map->straw_calc_version == 0) {
			/* zero weight items get 0 length straws! */
			if (weights[reverse[i]] == 0) {
				bucket->straws[reverse[i]] = 0;
				i++;
				continue;
			}

			/* set this item's straw */
			bucket->straws[reverse[i]] = straw * 0x10000;
			dprintk("item %d at %d weight %d straw %d (%lf)\n",
				bucket->h.items[reverse[i]],
				reverse[i], weights[reverse[i]],
				bucket->straws[reverse[i]], straw);
			i++;
			if (i == size)
				break;

			/* same weight as previous? */
			if (weights[reverse[i]] == weights[reverse[i-1]]) {
				dprintk("same as previous\n");
				continue;
			}

			/* adjust straw for next guy */
			wbelow += ((double)weights[reverse[i-1]] - lastw) *
				numleft;
			for (j=i; j<size; j++)
				if (weights[reverse[j]] == weights[reverse[i]])
					numleft--;
				else
					break;
			wnext = numleft * (weights[reverse[i]] -
					   weights[reverse[i-1]]);
			pbelow = wbelow / (wbelow + wnext);
			dprintk("wbelow %lf  wnext %lf  pbelow %lf  numleft %d\n",
				wbelow, wnext, pbelow, numleft);

			straw *= pow((double)1.0 / pbelow, (double)1.0 /
				     (double)numleft);

			lastw = weights[reverse[i-1]];
		} else if (map->straw_calc_version >= 1) {
			/* zero weight items get 0 length straws! */
			if (weights[reverse[i]] == 0) {
				bucket->straws[reverse[i]] = 0;
				i++;
				numleft--;
				continue;
			}

			/* set this item's straw */
			bucket->straws[reverse[i]] = straw * 0x10000;
			dprintk("item %d at %d weight %d straw %d (%lf)\n",
				bucket->h.items[reverse[i]],
				reverse[i], weights[reverse[i]],
				bucket->straws[reverse[i]], straw);
			i++;
			if (i == size)
				break;

			/* adjust straw for next guy */
			wbelow += ((double)weights[reverse[i-1]] - lastw) *
				numleft;
			numleft--;
			wnext = numleft * (weights[reverse[i]] -
					   weights[reverse[i-1]]);
			pbelow = wbelow / (wbelow + wnext);
			dprintk("wbelow %lf  wnext %lf  pbelow %lf  numleft %d\n",
				wbelow, wnext, pbelow, numleft);

			straw *= pow((double)1.0 / pbelow, (double)1.0 /
				     (double)numleft);

			lastw = weights[reverse[i-1]];
		}
	}

	free(reverse);
	return 0;
}

struct crush_bucket_straw *
crush_make_straw_bucket(struct crush_map *map,
			int hash,
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
	bucket->item_weights = malloc(sizeof(__u32)*size);
        if (!bucket->item_weights)
                goto err;
        bucket->straws = malloc(sizeof(__u32)*size);
        if (!bucket->straws)
                goto err;

	for (i=0; i<size; i++) {
		bucket->h.items[i] = items[i];
		bucket->h.weight += weights[i];
		bucket->item_weights[i] = weights[i];
	}

        if (crush_calc_straw(map, bucket) < 0)
                goto err;

	return bucket;
err:
        free(bucket->straws);
        free(bucket->item_weights);
        free(bucket->h.items);
        free(bucket);
        return NULL;
}

struct crush_bucket_straw2 *
crush_make_straw2_bucket(struct crush_map *map,
			 int hash,
			 int type,
			 int size,
			 int *items,
			 int *weights)
{
	struct crush_bucket_straw2 *bucket;
	int i;

	bucket = malloc(sizeof(*bucket));
        if (!bucket)
                return NULL;
	memset(bucket, 0, sizeof(*bucket));
	bucket->h.alg = CRUSH_BUCKET_STRAW2;
	bucket->h.hash = hash;
	bucket->h.type = type;
	bucket->h.size = size;

	if (size == 0) {
		return bucket;
	}

        bucket->h.items = malloc(sizeof(__s32)*size);
        if (!bucket->h.items)
                goto err;
	bucket->item_weights = malloc(sizeof(__u32)*size);
        if (!bucket->item_weights)
                goto err;

	for (i=0; i<size; i++) {
		bucket->h.items[i] = items[i];
		bucket->h.weight += weights[i];
		bucket->item_weights[i] = weights[i];
	}

	return bucket;
err:
        free(bucket->item_weights);
        free(bucket->h.items);
        free(bucket);
        return NULL;
}



struct crush_bucket*
crush_make_bucket(struct crush_map *map,
		  int alg, int hash, int type, int size,
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
		return (struct crush_bucket *)crush_make_straw_bucket(map, hash, type, size, items, weights);
	case CRUSH_BUCKET_STRAW2:
		return (struct crush_bucket *)crush_make_straw2_bucket(map, hash, type, size, items, weights);
	}
	return 0;
}


/************************************************/

int crush_add_uniform_bucket_item(struct crush_bucket_uniform *bucket, int item, int weight)
{
        int newsize = bucket->h.size + 1;
	void *_realloc = NULL;

	/* In such situation 'CRUSH_BUCKET_UNIFORM', the weight
	   provided for the item should be the same as
	   bucket->item_weight defined with 'crush_make_bucket'. This
	   assumption is enforced by the return value which is always
	   0. */
	if (bucket->item_weight != weight) {
	  return -EINVAL;
	}

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
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
	if ((_realloc = realloc(bucket->node_weights, sizeof(__u32)*bucket->num_nodes)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->node_weights = _realloc;
	}

	node = crush_calc_tree_node(newsize-1);
	bucket->node_weights[node] = weight;

	/* if the depth increase, we need to initialize the new root node's weight before add bucket item */
	int root = bucket->num_nodes/2;
	if (depth >= 2 && (node - 1) == root) {
		/* if the new item is the first node in right sub tree, so
		* the root node initial weight is left sub tree's weight
		*/
		bucket->node_weights[root] = bucket->node_weights[root/2];
	}

	for (j=1; j<depth; j++) {
		node = parent(node);

                if (crush_addition_is_unsafe(bucket->node_weights[node], weight))
                        return -ERANGE;

		bucket->node_weights[node] += weight;
                dprintk(" node %d weight %d\n", node, bucket->node_weights[node]);
	}


	if (crush_addition_is_unsafe(bucket->h.weight, weight))
                return -ERANGE;
	
	bucket->h.items[newsize-1] = item;
        bucket->h.weight += weight;
        bucket->h.size++;

	return 0;
}

int crush_add_straw_bucket_item(struct crush_map *map,
				struct crush_bucket_straw *bucket,
				int item, int weight)
{
	int newsize = bucket->h.size + 1;

	void *_realloc = NULL;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
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
	
	return crush_calc_straw(map, bucket);
}

int crush_add_straw2_bucket_item(struct crush_map *map,
				 struct crush_bucket_straw2 *bucket,
				 int item, int weight)
{
	int newsize = bucket->h.size + 1;

	void *_realloc = NULL;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
	}
	if ((_realloc = realloc(bucket->item_weights, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->item_weights = _realloc;
	}

	bucket->h.items[newsize-1] = item;
	bucket->item_weights[newsize-1] = weight;

	if (crush_addition_is_unsafe(bucket->h.weight, weight))
                return -ERANGE;

	bucket->h.weight += weight;
	bucket->h.size++;

	return 0;
}

int crush_bucket_add_item(struct crush_map *map,
			  struct crush_bucket *b, int item, int weight)
{
	switch (b->alg) {
	case CRUSH_BUCKET_UNIFORM:
		return crush_add_uniform_bucket_item((struct crush_bucket_uniform *)b, item, weight);
	case CRUSH_BUCKET_LIST:
		return crush_add_list_bucket_item((struct crush_bucket_list *)b, item, weight);
	case CRUSH_BUCKET_TREE:
		return crush_add_tree_bucket_item((struct crush_bucket_tree *)b, item, weight);
	case CRUSH_BUCKET_STRAW:
		return crush_add_straw_bucket_item(map, (struct crush_bucket_straw *)b, item, weight);
	case CRUSH_BUCKET_STRAW2:
		return crush_add_straw2_bucket_item(map, (struct crush_bucket_straw2 *)b, item, weight);
	default:
		return -1;
	}
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
	if (bucket->item_weight < bucket->h.weight)
		bucket->h.weight -= bucket->item_weight;
	else
		bucket->h.weight = 0;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
	}
	return 0;
}

int crush_remove_list_bucket_item(struct crush_bucket_list *bucket, int item)
{
	unsigned i, j;
	int newsize;
	unsigned weight;

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
	if (weight < bucket->h.weight)
		bucket->h.weight -= weight;
	else
		bucket->h.weight = 0;
	newsize = --bucket->h.size;
	
	void *_realloc = NULL;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
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
		unsigned weight;
		int j;
		int depth = calc_depth(bucket->h.size);

		if (bucket->h.items[i] != item)
			continue;

		bucket->h.items[i] = 0;
		node = crush_calc_tree_node(i);
		weight = bucket->node_weights[node];
		bucket->node_weights[node] = 0;

		for (j = 1; j < depth; j++) {
			node = parent(node);
			bucket->node_weights[node] -= weight;
			dprintk(" node %d weight %d\n", node, bucket->node_weights[node]);
		}
		if (weight < bucket->h.weight)
			bucket->h.weight -= weight;
		else
			bucket->h.weight = 0;
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

int crush_remove_straw_bucket_item(struct crush_map *map,
				   struct crush_bucket_straw *bucket, int item)
{
	int newsize = bucket->h.size - 1;
	unsigned i, j;

	for (i = 0; i < bucket->h.size; i++) {
		if (bucket->h.items[i] == item) {
			if (bucket->item_weights[i] < bucket->h.weight)
				bucket->h.weight -= bucket->item_weights[i];
			else
				bucket->h.weight = 0;
			for (j = i; j < bucket->h.size - 1; j++) {
				bucket->h.items[j] = bucket->h.items[j+1];
				bucket->item_weights[j] = bucket->item_weights[j+1];
			}
			break;
		}
	}
	if (i == bucket->h.size)
		return -ENOENT;
	bucket->h.size--;
	if (bucket->h.size == 0) {
		/* don't bother reallocating */
		return 0;
	}
	void *_realloc = NULL;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
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

	return crush_calc_straw(map, bucket);
}

int crush_remove_straw2_bucket_item(struct crush_map *map,
				    struct crush_bucket_straw2 *bucket, int item)
{
	int newsize = bucket->h.size - 1;
	unsigned i, j;

	for (i = 0; i < bucket->h.size; i++) {
		if (bucket->h.items[i] == item) {
			if (bucket->item_weights[i] < bucket->h.weight)
				bucket->h.weight -= bucket->item_weights[i];
			else
				bucket->h.weight = 0;
			for (j = i; j < bucket->h.size - 1; j++) {
				bucket->h.items[j] = bucket->h.items[j+1];
				bucket->item_weights[j] = bucket->item_weights[j+1];
			}
			break;
		}
	}
	if (i == bucket->h.size)
		return -ENOENT;

	bucket->h.size--;
	if (!newsize) {
		/* don't bother reallocating a 0-length array. */
		return 0;
	}

	void *_realloc = NULL;

	if ((_realloc = realloc(bucket->h.items, sizeof(__s32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->h.items = _realloc;
	}
	if ((_realloc = realloc(bucket->item_weights, sizeof(__u32)*newsize)) == NULL) {
		return -ENOMEM;
	} else {
		bucket->item_weights = _realloc;
	}

	return 0;
}

int crush_bucket_remove_item(struct crush_map *map, struct crush_bucket *b, int item)
{
	switch (b->alg) {
	case CRUSH_BUCKET_UNIFORM:
		return crush_remove_uniform_bucket_item((struct crush_bucket_uniform *)b, item);
	case CRUSH_BUCKET_LIST:
		return crush_remove_list_bucket_item((struct crush_bucket_list *)b, item);
	case CRUSH_BUCKET_TREE:
		return crush_remove_tree_bucket_item((struct crush_bucket_tree *)b, item);
	case CRUSH_BUCKET_STRAW:
		return crush_remove_straw_bucket_item(map, (struct crush_bucket_straw *)b, item);
	case CRUSH_BUCKET_STRAW2:
		return crush_remove_straw2_bucket_item(map, (struct crush_bucket_straw2 *)b, item);
	default:
		return -1;
	}
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

int crush_adjust_straw_bucket_item_weight(struct crush_map *map,
					  struct crush_bucket_straw *bucket,
					  int item, int weight)
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

	r = crush_calc_straw(map, bucket);
        if (r < 0)
                return r;

	return diff;
}

int crush_adjust_straw2_bucket_item_weight(struct crush_map *map,
					   struct crush_bucket_straw2 *bucket,
					   int item, int weight)
{
	unsigned idx;
	int diff;

	for (idx = 0; idx < bucket->h.size; idx++)
		if (bucket->h.items[idx] == item)
			break;
	if (idx == bucket->h.size)
		return 0;

	diff = weight - bucket->item_weights[idx];
	bucket->item_weights[idx] = weight;
	bucket->h.weight += diff;

	return diff;
}

int crush_bucket_adjust_item_weight(struct crush_map *map,
				    struct crush_bucket *b,
				    int item, int weight)
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
		return crush_adjust_straw_bucket_item_weight(map,
							     (struct crush_bucket_straw *)b,
							     item, weight);
	case CRUSH_BUCKET_STRAW2:
		return crush_adjust_straw2_bucket_item_weight(map,
							      (struct crush_bucket_straw2 *)b,
							     item, weight);
	default:
		return -1;
	}
}

/************************************************/

static int crush_reweight_uniform_bucket(struct crush_map *map, struct crush_bucket_uniform *bucket)
{
	unsigned i;
	unsigned sum = 0, n = 0, leaves = 0;

	for (i = 0; i < bucket->h.size; i++) {
		int id = bucket->h.items[i];
		if (id < 0) {
			struct crush_bucket *c = map->buckets[-1-id];
			crush_reweight_bucket(map, c);

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

static int crush_reweight_list_bucket(struct crush_map *map, struct crush_bucket_list *bucket)
{
	unsigned i;

	bucket->h.weight = 0;
	for (i = 0; i < bucket->h.size; i++) {
		int id = bucket->h.items[i];
		if (id < 0) {
			struct crush_bucket *c = map->buckets[-1-id];
			crush_reweight_bucket(map, c);
			bucket->item_weights[i] = c->weight;
		}

		if (crush_addition_is_unsafe(bucket->h.weight, bucket->item_weights[i]))
                        return -ERANGE;

		bucket->h.weight += bucket->item_weights[i];
	}

	return 0;
}

static int crush_reweight_tree_bucket(struct crush_map *map, struct crush_bucket_tree *bucket)
{
	unsigned i;

	bucket->h.weight = 0;
	for (i = 0; i < bucket->h.size; i++) {
		int node = crush_calc_tree_node(i);
		int id = bucket->h.items[i];
		if (id < 0) {
			struct crush_bucket *c = map->buckets[-1-id];
			crush_reweight_bucket(map, c);
			bucket->node_weights[node] = c->weight;
		}

		if (crush_addition_is_unsafe(bucket->h.weight, bucket->node_weights[node]))
                        return -ERANGE;

		bucket->h.weight += bucket->node_weights[node];


	}

	return 0;
}

static int crush_reweight_straw_bucket(struct crush_map *map, struct crush_bucket_straw *bucket)
{
	unsigned i;

	bucket->h.weight = 0;
	for (i = 0; i < bucket->h.size; i++) {
		int id = bucket->h.items[i];
		if (id < 0) {
			struct crush_bucket *c = map->buckets[-1-id];
			crush_reweight_bucket(map, c);
			bucket->item_weights[i] = c->weight;
		}

                if (crush_addition_is_unsafe(bucket->h.weight, bucket->item_weights[i]))
                        return -ERANGE;

                bucket->h.weight += bucket->item_weights[i];
	}
	crush_calc_straw(map, bucket);

	return 0;
}

static int crush_reweight_straw2_bucket(struct crush_map *map, struct crush_bucket_straw2 *bucket)
{
	unsigned i;

	bucket->h.weight = 0;
	for (i = 0; i < bucket->h.size; i++) {
		int id = bucket->h.items[i];
		if (id < 0) {
			struct crush_bucket *c = map->buckets[-1-id];
			crush_reweight_bucket(map, c);
			bucket->item_weights[i] = c->weight;
		}

                if (crush_addition_is_unsafe(bucket->h.weight, bucket->item_weights[i]))
                        return -ERANGE;

                bucket->h.weight += bucket->item_weights[i];
	}

	return 0;
}

int crush_reweight_bucket(struct crush_map *map, struct crush_bucket *b)
{
	switch (b->alg) {
	case CRUSH_BUCKET_UNIFORM:
		return crush_reweight_uniform_bucket(map, (struct crush_bucket_uniform *)b);
	case CRUSH_BUCKET_LIST:
		return crush_reweight_list_bucket(map, (struct crush_bucket_list *)b);
	case CRUSH_BUCKET_TREE:
		return crush_reweight_tree_bucket(map, (struct crush_bucket_tree *)b);
	case CRUSH_BUCKET_STRAW:
		return crush_reweight_straw_bucket(map, (struct crush_bucket_straw *)b);
	case CRUSH_BUCKET_STRAW2:
		return crush_reweight_straw2_bucket(map, (struct crush_bucket_straw2 *)b);
	default:
		return -1;
	}
}

struct crush_choose_arg *crush_make_choose_args(struct crush_map *map, int num_positions)
{
  int b;
  int sum_bucket_size = 0;
  int bucket_count = 0;
  for (b = 0; b < map->max_buckets; b++) {
    if (map->buckets[b] == 0)
      continue;
    sum_bucket_size += map->buckets[b]->size;
    bucket_count++;
  }
  dprintk("sum_bucket_size %d max_buckets %d bucket_count %d\n",
          sum_bucket_size, map->max_buckets, bucket_count);
  int size = (sizeof(struct crush_choose_arg) * map->max_buckets +
              sizeof(struct crush_weight_set) * bucket_count * num_positions +
              sizeof(__u32) * sum_bucket_size * num_positions + // weights
              sizeof(__s32) * sum_bucket_size); // ids
  char *space = malloc(size);
  struct crush_choose_arg *arg = (struct crush_choose_arg *)space;
  struct crush_weight_set *weight_set = (struct crush_weight_set *)(arg + map->max_buckets);
  __u32 *weights = (__u32 *)(weight_set + bucket_count * num_positions);
  char *weight_set_ends __attribute__((unused)) = (char*)weights;
  __s32 *ids = (__s32 *)(weights + sum_bucket_size * num_positions);
  char *weights_end __attribute__((unused)) = (char *)ids;
  char *ids_end __attribute__((unused)) = (char *)(ids + sum_bucket_size);
  BUG_ON(space + size != ids_end);
  for (b = 0; b < map->max_buckets; b++) {
    if (map->buckets[b] == 0) {
      memset(&arg[b], '\0', sizeof(struct crush_choose_arg));
      continue;
    }
    struct crush_bucket_straw2 *bucket = (struct crush_bucket_straw2 *)map->buckets[b];

    int position;
    for (position = 0; position < num_positions; position++) {
      memcpy(weights, bucket->item_weights, sizeof(__u32) * bucket->h.size);
      weight_set[position].weights = weights;
      weight_set[position].size = bucket->h.size;
      dprintk("moving weight %d bytes forward\n", (int)((weights + bucket->h.size) - weights));
      weights += bucket->h.size;
    }
    arg[b].weight_set = weight_set;
    arg[b].weight_set_positions = num_positions;
    weight_set += position;

    memcpy(ids, bucket->h.items, sizeof(__s32) * bucket->h.size);
    arg[b].ids = ids;
    arg[b].ids_size = bucket->h.size;
    ids += bucket->h.size;
  }
  BUG_ON((char*)weight_set_ends != (char*)weight_set);
  BUG_ON((char*)weights_end != (char*)weights);
  BUG_ON((char*)ids != (char*)ids_end);
  return arg;
}

void crush_destroy_choose_args(struct crush_choose_arg *args)
{
  free(args);
}

/***************************/

/* methods to check for safe arithmetic operations */

int crush_addition_is_unsafe(__u32 a, __u32 b)
{
	if ((((__u32)(-1)) - b) < a)
		return 1;
	else
		return 0;
}

int crush_multiplication_is_unsafe(__u32  a, __u32 b)
{
	/* prevent division by zero */
        if (!a)
                return 0;
	if (!b)
		return 1;
	if ((((__u32)(-1)) / b) < a)
		return 1;
	else
		return 0;
}

/***************************/

/* methods to configure crush_map */

void set_legacy_crush_map(struct crush_map *map) {
  /* initialize legacy tunable values */
  map->choose_local_tries = 2;
  map->choose_local_fallback_tries = 5;
  map->choose_total_tries = 19;
  map->chooseleaf_descend_once = 0;
  map->chooseleaf_vary_r = 0;
  map->chooseleaf_stable = 0;
  map->straw_calc_version = 0;

  // by default, use legacy types, and also exclude tree,
  // since it was buggy.
  map->allowed_bucket_algs = CRUSH_LEGACY_ALLOWED_BUCKET_ALGS;
}

void set_optimal_crush_map(struct crush_map *map) {
  map->choose_local_tries = 0;
  map->choose_local_fallback_tries = 0;
  map->choose_total_tries = 50;
  map->chooseleaf_descend_once = 1;
  map->chooseleaf_vary_r = 1;
  map->chooseleaf_stable = 1;
  map->allowed_bucket_algs = (
    (1 << CRUSH_BUCKET_UNIFORM) |
    (1 << CRUSH_BUCKET_LIST) |
    (1 << CRUSH_BUCKET_STRAW) |
    (1 << CRUSH_BUCKET_STRAW2));
}
