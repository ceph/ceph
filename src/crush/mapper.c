/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Intel Corporation All Rights Reserved
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifdef __KERNEL__
# include <linux/string.h>
# include <linux/slab.h>
# include <linux/bug.h>
# include <linux/kernel.h>
# include <linux/crush/crush.h>
# include <linux/crush/hash.h>
#else
# include "crush_compat.h"
# include "crush.h"
# include "hash.h"
#endif
#include "crush_ln_table.h"
#include "mapper.h"

#define dprintk(args...) /* printf(args) */

#define MIN(x, y) ((x) > (y) ? (y) : (x))
#define MAX(y, x) ((x) < (y) ? (y) : (x))

/*
 * Implement the core CRUSH mapping algorithm.
 */

/*
 * bucket choose methods
 *
 * For each bucket algorithm, we have a "choose" method that, given a
 * crush input @x and replica position (usually, position in output set) @r,
 * will produce an item in the bucket.
 */

/*
 * Choose based on a random permutation of the bucket.
 *
 * We used to use some prime number arithmetic to do this, but it
 * wasn't very random, and had some other bad behaviors.  Instead, we
 * calculate an actual random permutation of the bucket members.
 * Since this is expensive, we optimize for the r=0 case, which
 * captures the vast majority of calls.
 */
static int bucket_perm_choose(const struct crush_bucket *bucket,
			      struct crush_work_bucket *work,
			      int x, int r)
{
	unsigned int pr = r % bucket->size;
	unsigned int i, s;

	/* start a new permutation if @x has changed */
	if (work->perm_x != (__u32)x || work->perm_n == 0) {
		dprintk("bucket %d new x=%d\n", bucket->id, x);
		work->perm_x = x;

		/* optimize common r=0 case */
		if (pr == 0) {
			s = crush_hash32_3(bucket->hash, x, bucket->id, 0) %
				bucket->size;
			work->perm[0] = s;
			work->perm_n = 0xffff;   /* magic value, see below */
			goto out;
		}

		for (i = 0; i < bucket->size; i++)
			work->perm[i] = i;
		work->perm_n = 0;
	} else if (work->perm_n == 0xffff) {
		/* clean up after the r=0 case above */
		for (i = 1; i < bucket->size; i++)
			work->perm[i] = i;
		work->perm[work->perm[0]] = 0;
		work->perm_n = 1;
	}

	/* calculate permutation up to pr */
	for (i = 0; i < work->perm_n; i++)
		dprintk(" perm_choose have %d: %d\n", i, work->perm[i]);
	while (work->perm_n <= pr) {
		unsigned int p = work->perm_n;
		/* no point in swapping the final entry */
		if (p < bucket->size - 1) {
			i = crush_hash32_3(bucket->hash, x, bucket->id, p) %
				(bucket->size - p);
			if (i) {
				unsigned int t = work->perm[p + i];
				work->perm[p + i] = work->perm[p];
				work->perm[p] = t;
			}
			dprintk(" perm_choose swap %d with %d\n", p, p+i);
		}
		work->perm_n++;
	}
	for (i = 0; i < bucket->size; i++)
		dprintk(" perm_choose  %d: %d\n", i, work->perm[i]);

	s = work->perm[pr];
out:
	dprintk(" perm_choose %d sz=%d x=%d r=%d (%d) s=%d\n", bucket->id,
		bucket->size, x, r, pr, s);
	return bucket->items[s];
}

/* uniform */
static int bucket_uniform_choose(const struct crush_bucket_uniform *bucket,
				 struct crush_work_bucket *work, int x, int r)
{
	return bucket_perm_choose(&bucket->h, work, x, r);
}

/* list */
static int bucket_list_choose(const struct crush_bucket_list *bucket,
			      int x, int r)
{
	int i;

	for (i = bucket->h.size-1; i >= 0; i--) {
		__u64 w = crush_hash32_4(bucket->h.hash, x, bucket->h.items[i],
					 r, bucket->h.id);
		w &= 0xffff;
		dprintk("list_choose i=%d x=%d r=%d item %d weight %x "
			"sw %x rand %llx",
			i, x, r, bucket->h.items[i], bucket->item_weights[i],
			bucket->sum_weights[i], w);
		w *= bucket->sum_weights[i];
		w = w >> 16;
		/*dprintk(" scaled %llx\n", w);*/
		if (w < bucket->item_weights[i]) {
			return bucket->h.items[i];
		}
	}

	dprintk("bad list sums for bucket %d\n", bucket->h.id);
	return bucket->h.items[0];
}


/* (binary) tree */
static int height(int n)
{
	int h = 0;
	while ((n & 1) == 0) {
		h++;
		n = n >> 1;
	}
	return h;
}

static int left(int x)
{
	int h = height(x);
	return x - (1 << (h-1));
}

static int right(int x)
{
	int h = height(x);
	return x + (1 << (h-1));
}

static int terminal(int x)
{
	return x & 1;
}

static int bucket_tree_choose(const struct crush_bucket_tree *bucket,
			      int x, int r)
{
	int n;
	__u32 w;
	__u64 t;

	/* start at root */
	n = bucket->num_nodes >> 1;

	while (!terminal(n)) {
		int l;
		/* pick point in [0, w) */
		w = bucket->node_weights[n];
		t = (__u64)crush_hash32_4(bucket->h.hash, x, n, r,
					  bucket->h.id) * (__u64)w;
		t = t >> 32;

		/* descend to the left or right? */
		l = left(n);
		if (t < bucket->node_weights[l])
			n = l;
		else
			n = right(n);
	}

	return bucket->h.items[n >> 1];
}


/* straw */

static int bucket_straw_choose(const struct crush_bucket_straw *bucket,
			       int x, int r)
{
	__u32 i;
	int high = 0;
	__u64 high_draw = 0;
	__u64 draw;

	for (i = 0; i < bucket->h.size; i++) {
		draw = crush_hash32_3(bucket->h.hash, x, bucket->h.items[i], r);
		draw &= 0xffff;
		draw *= bucket->straws[i];
		if (i == 0 || draw > high_draw) {
			high = i;
			high_draw = draw;
		}
	}
	return bucket->h.items[high];
}

/* compute 2^44*log2(input+1) */
static __u64 crush_ln(unsigned int xin)
{
	unsigned int x = xin;
	int iexpon, index1, index2;
	__u64 RH, LH, LL, xl64, result;

	x++;

	/* normalize input */
	iexpon = 15;

	// figure out number of bits we need to shift and
	// do it in one step instead of iteratively
	if (!(x & 0x18000)) {
	  int bits = __builtin_clz(x & 0x1FFFF) - 16;
	  x <<= bits;
	  iexpon = 15 - bits;
	}

	index1 = (x >> 8) << 1;
	/* RH ~ 2^56/index1 */
	RH = __RH_LH_tbl[index1 - 256];
	/* LH ~ 2^48 * log2(index1/256) */
	LH = __RH_LH_tbl[index1 + 1 - 256];

	/* RH*x ~ 2^48 * (2^15 + xf), xf<2^8 */
	xl64 = (__s64)x * RH;
	xl64 >>= 48;

	result = iexpon;
	result <<= (12 + 32);

	index2 = xl64 & 0xff;
	/* LL ~ 2^48*log2(1.0+index2/2^15) */
	LL = __LL_tbl[index2];

	LH = LH + LL;

	LH >>= (48 - 12 - 32);
	result += LH;

	return result;
}


/*
 * straw2
 *
 * Suppose we have two osds: osd.0 and osd.1, with weight 8 and 4 respectively, It means:
 *   a). For osd.0, the time interval between each io request apply to exponential distribution 
 *       with lamba equals 8
 *   b). For osd.1, the time interval between each io request apply to exponential distribution 
 *       with lamba equals 4
 *   c). If we apply to each osd's exponential random variable, then the total pgs on each osd
 *       is proportional to its weight.
 *
 * for reference, see:
 *
 * http://en.wikipedia.org/wiki/Exponential_distribution#Distribution_of_the_minimum_of_exponential_random_variables
 */

static inline __u32 *get_choose_arg_weights(const struct crush_bucket_straw2 *bucket,
                                            const struct crush_choose_arg *arg,
                                            int position)
{
	if ((arg == NULL) || (arg->weight_set == NULL))
		return bucket->item_weights;
	if (position >= arg->weight_set_positions)
		position = arg->weight_set_positions - 1;
	return arg->weight_set[position].weights;
}

static inline __s32 *get_choose_arg_ids(const struct crush_bucket_straw2 *bucket,
					const struct crush_choose_arg *arg)
{
	if ((arg == NULL) || (arg->ids == NULL))
		return bucket->h.items;
	return arg->ids;
}

/*
 * Compute exponential random variable using inversion method.
 *
 * for reference, see the exponential distribution example at:  
 * https://en.wikipedia.org/wiki/Inverse_transform_sampling#Examples
 */
static inline __s64 generate_exponential_distribution(int type, int x, int y, int z, 
                                                      int weight)
{
	unsigned int u = crush_hash32_3(type, x, y, z);
	u &= 0xffff;

	/*
	 * for some reason slightly less than 0x10000 produces
	 * a slightly more accurate distribution... probably a
	 * rounding effect.
	 *
	 * the natural log lookup table maps [0,0xffff]
	 * (corresponding to real numbers [1/0x10000, 1] to
	 * [0, 0xffffffffffff] (corresponding to real numbers
	 * [-11.090355,0]).
	 */
	__s64 ln = crush_ln(u) - 0x1000000000000ll;

	/*
	 * divide by 16.16 fixed-point weight.  note
	 * that the ln value is negative, so a larger
	 * weight means a larger (less negative) value
	 * for draw.
	 */
	return div64_s64(ln, weight);
}

static int bucket_straw2_choose(const struct crush_bucket_straw2 *bucket,
				int x, int r, const struct crush_choose_arg *arg,
                                int position)
{
	unsigned int i, high = 0;
	__s64 draw, high_draw = 0;
        __u32 *weights = get_choose_arg_weights(bucket, arg, position);
        __s32 *ids = get_choose_arg_ids(bucket, arg);
	for (i = 0; i < bucket->h.size; i++) {
                dprintk("weight 0x%x item %d\n", weights[i], ids[i]);
		if (weights[i]) {
			draw = generate_exponential_distribution(bucket->h.hash, x, ids[i], r, weights[i]);
		} else {
			draw = S64_MIN;
		}

		if (i == 0 || draw > high_draw) {
			high = i;
			high_draw = draw;
		}
	}

	return bucket->h.items[high];
}


static int crush_bucket_choose(const struct crush_bucket *in,
			       struct crush_work_bucket *work,
			       int x, int r,
                               const struct crush_choose_arg *arg,
                               int position)
{
	dprintk(" crush_bucket_choose %d x=%d r=%d\n", in->id, x, r);
	BUG_ON(in->size == 0);
	switch (in->alg) {
	case CRUSH_BUCKET_UNIFORM:
		return bucket_uniform_choose(
			(const struct crush_bucket_uniform *)in,
			work, x, r);
	case CRUSH_BUCKET_LIST:
		return bucket_list_choose((const struct crush_bucket_list *)in,
					  x, r);
	case CRUSH_BUCKET_TREE:
		return bucket_tree_choose((const struct crush_bucket_tree *)in,
					  x, r);
	case CRUSH_BUCKET_STRAW:
		return bucket_straw_choose(
			(const struct crush_bucket_straw *)in,
			x, r);
	case CRUSH_BUCKET_STRAW2:
		return bucket_straw2_choose(
			(const struct crush_bucket_straw2 *)in,
			x, r, arg, position);
	default:
		dprintk("unknown bucket %d alg %d\n", in->id, in->alg);
		return in->items[0];
	}
}

/*
 * true if device is marked "out" (failed, fully offloaded)
 * of the cluster
 */
static int is_out(const struct crush_map *map,
		  const __u32 *weight, int weight_max,
		  int item, int x)
{
	if (item >= weight_max)
		return 1;
	if (weight[item] >= 0x10000)
		return 0;
	if (weight[item] == 0)
		return 1;
	if ((crush_hash32_2(CRUSH_HASH_RJENKINS1, x, item) & 0xffff)
	    < weight[item])
		return 0;
	return 1;
}

/**
 * crush_choose_firstn - choose numrep distinct items of given type
 * @map: the crush_map
 * @bucket: the bucket we are choose an item from
 * @x: crush input value
 * @numrep: the number of items to choose
 * @type: the type of item to choose
 * @out: pointer to output vector
 * @outpos: our position in that vector
 * @out_size: size of the out vector
 * @tries: number of attempts to make
 * @recurse_tries: number of attempts to have recursive chooseleaf make
 * @local_retries: localized retries
 * @local_fallback_retries: localized fallback retries
 * @recurse_to_leaf: true if we want one device under each item of given type (chooseleaf instead of choose)
 * @stable: stable mode starts rep=0 in the recursive call for all replicas
 * @vary_r: pass r to recursive calls
 * @out2: second output vector for leaf items (if @recurse_to_leaf)
 * @parent_r: r value passed from the parent
 */
static int crush_choose_firstn(const struct crush_map *map,
			       struct crush_work *work,
			       const struct crush_bucket *bucket,
			       const __u32 *weight, int weight_max,
			       int x, int numrep, int type,
			       int *out, int outpos,
			       int out_size,
			       unsigned int tries,
			       unsigned int recurse_tries,
			       unsigned int local_retries,
			       unsigned int local_fallback_retries,
			       int recurse_to_leaf,
			       unsigned int vary_r,
			       unsigned int stable,
			       int *out2,
			       int parent_r,
                               const struct crush_choose_arg *choose_args)
{
	int rep;
	unsigned int ftotal, flocal;
	int retry_descent, retry_bucket, skip_rep;
	const struct crush_bucket *in = bucket;
	int r;
	int i;
	int item = 0;
	int itemtype;
	int collide, reject;
	int count = out_size;

	dprintk("CHOOSE%s bucket %d x %d outpos %d numrep %d tries %d \
recurse_tries %d local_retries %d local_fallback_retries %d \
parent_r %d stable %d\n",
		recurse_to_leaf ? "_LEAF" : "",
		bucket->id, x, outpos, numrep,
		tries, recurse_tries, local_retries, local_fallback_retries,
		parent_r, stable);

	for (rep = stable ? 0 : outpos; rep < numrep && count > 0 ; rep++) {
		/* keep trying until we get a non-out, non-colliding item */
		ftotal = 0;
		skip_rep = 0;
		do {
			retry_descent = 0;
			in = bucket;              /* initial bucket */

			/* choose through intervening buckets */
			flocal = 0;
			do {
				collide = 0;
				retry_bucket = 0;
				r = rep + parent_r;
				/* r' = r + f_total */
				r += ftotal;

				/* bucket choose */
				if (in->size == 0) {
					reject = 1;
					goto reject;
				}
				if (local_fallback_retries > 0 &&
				    flocal >= (in->size>>1) &&
				    flocal > local_fallback_retries)
					item = bucket_perm_choose(
						in, work->work[-1-in->id],
						x, r);
				else
					item = crush_bucket_choose(
						in, work->work[-1-in->id],
						x, r,
                                                (choose_args ? &choose_args[-1-in->id] : 0),
                                                outpos);
				if (item >= map->max_devices) {
					dprintk("   bad item %d\n", item);
					skip_rep = 1;
					break;
				}

				/* desired type? */
				if (item < 0)
					itemtype = map->buckets[-1-item]->type;
				else
					itemtype = 0;
				dprintk("  item %d type %d\n", item, itemtype);

				/* keep going? */
				if (itemtype != type) {
					if (item >= 0 ||
					    (-1-item) >= map->max_buckets) {
						dprintk("   bad item type %d\n", type);
						skip_rep = 1;
						break;
					}
					in = map->buckets[-1-item];
					retry_bucket = 1;
					continue;
				}

				/* collision? */
				for (i = 0; i < outpos; i++) {
					if (out[i] == item) {
						collide = 1;
						break;
					}
				}

				reject = 0;
				if (!collide && recurse_to_leaf) {
					if (item < 0) {
						int sub_r;
						if (vary_r)
							sub_r = r >> (vary_r-1);
						else
							sub_r = 0;
						if (crush_choose_firstn(
							    map,
							    work,
							    map->buckets[-1-item],
							    weight, weight_max,
							    x, stable ? 1 : outpos+1, 0,
							    out2, outpos, count,
							    recurse_tries, 0,
							    local_retries,
							    local_fallback_retries,
							    0,
							    vary_r,
							    stable,
							    NULL,
							    sub_r,
                                                            choose_args) <= outpos)
							/* didn't get leaf */
							reject = 1;
					} else {
						/* we already have a leaf! */
						out2[outpos] = item;
		                        }
				}

				if (!reject && !collide) {
					/* out? */
					if (itemtype == 0)
						reject = is_out(map, weight,
								weight_max,
								item, x);
				}

reject:
				if (reject || collide) {
					ftotal++;
					flocal++;

					if (collide && flocal <= local_retries)
						/* retry locally a few times */
						retry_bucket = 1;
					else if (local_fallback_retries > 0 &&
						 flocal <= in->size + local_fallback_retries)
						/* exhaustive bucket search */
						retry_bucket = 1;
					else if (ftotal < tries)
						/* then retry descent */
						retry_descent = 1;
					else
						/* else give up */
						skip_rep = 1;
					dprintk("  reject %d  collide %d  "
						"ftotal %u  flocal %u\n",
						reject, collide, ftotal,
						flocal);
				}
			} while (retry_bucket);
		} while (retry_descent);

		if (skip_rep) {
			dprintk("skip rep\n");
			continue;
		}

		dprintk("CHOOSE got %d\n", item);
		out[outpos] = item;
		outpos++;
		count--;
#ifndef __KERNEL__
		if (map->choose_tries && ftotal <= map->choose_total_tries)
			map->choose_tries[ftotal]++;
#endif
	}

	dprintk("CHOOSE returns %d\n", outpos);
	return outpos;
}


/**
 * crush_choose_indep: alternative breadth-first positionally stable mapping
 *
 */
static void crush_choose_indep(const struct crush_map *map,
			       struct crush_work *work,
			       const struct crush_bucket *bucket,
			       const __u32 *weight, int weight_max,
			       int x, int left, int numrep, int type,
			       int *out, int outpos,
			       unsigned int tries,
			       unsigned int recurse_tries,
			       int recurse_to_leaf,
			       int *out2,
			       int parent_r,
                               const struct crush_choose_arg *choose_args)
{
	const struct crush_bucket *in = bucket;
	int endpos = outpos + left;
	int rep;
	unsigned int ftotal;
	int r;
	int i;
	int item = 0;
	int itemtype;
	int collide;

	dprintk("CHOOSE%s INDEP bucket %d x %d outpos %d numrep %d\n", recurse_to_leaf ? "_LEAF" : "",
		bucket->id, x, outpos, numrep);

	/* initially my result is undefined */
	for (rep = outpos; rep < endpos; rep++) {
		out[rep] = CRUSH_ITEM_UNDEF;
		if (out2)
			out2[rep] = CRUSH_ITEM_UNDEF;
	}

	for (ftotal = 0; left > 0 && ftotal < tries; ftotal++) {
#ifdef DEBUG_INDEP
		if (out2 && ftotal) {
			dprintk("%u %d a: ", ftotal, left);
			for (rep = outpos; rep < endpos; rep++) {
				dprintk(" %d", out[rep]);
			}
			dprintk("\n");
			dprintk("%u %d b: ", ftotal, left);
			for (rep = outpos; rep < endpos; rep++) {
				dprintk(" %d", out2[rep]);
			}
			dprintk("\n");
		}
#endif
		for (rep = outpos; rep < endpos; rep++) {
			if (out[rep] != CRUSH_ITEM_UNDEF)
				continue;

			in = bucket;  /* initial bucket */

			/* choose through intervening buckets */
			for (;;) {
				/* note: we base the choice on the position
				 * even in the nested call.  that means that
				 * if the first layer chooses the same bucket
				 * in a different position, we will tend to
				 * choose a different item in that bucket.
				 * this will involve more devices in data
				 * movement and tend to distribute the load.
				 */
				r = rep + parent_r;

				/* be careful */
				if (in->alg == CRUSH_BUCKET_UNIFORM &&
				    in->size % numrep == 0)
					/* r'=r+(n+1)*f_total */
					r += (numrep+1) * ftotal;
				else
					/* r' = r + n*f_total */
					r += numrep * ftotal;

				/* bucket choose */
				if (in->size == 0) {
					dprintk("   empty bucket\n");
					break;
				}

				item = crush_bucket_choose(
					in, work->work[-1-in->id],
					x, r,
                                        (choose_args ? &choose_args[-1-in->id] : 0),
                                        outpos);
				if (item >= map->max_devices) {
					dprintk("   bad item %d\n", item);
					out[rep] = CRUSH_ITEM_NONE;
					if (out2)
						out2[rep] = CRUSH_ITEM_NONE;
					left--;
					break;
				}

				/* desired type? */
				if (item < 0)
					itemtype = map->buckets[-1-item]->type;
				else
					itemtype = 0;
				dprintk("  item %d type %d\n", item, itemtype);

				/* keep going? */
				if (itemtype != type) {
					if (item >= 0 ||
					    (-1-item) >= map->max_buckets) {
						dprintk("   bad item type %d\n", type);
						out[rep] = CRUSH_ITEM_NONE;
						if (out2)
							out2[rep] =
								CRUSH_ITEM_NONE;
						left--;
						break;
					}
					in = map->buckets[-1-item];
					continue;
				}

				/* collision? */
				collide = 0;
				for (i = outpos; i < endpos; i++) {
					if (out[i] == item) {
						collide = 1;
						break;
					}
				}
				if (collide)
					break;

				if (recurse_to_leaf) {
					if (item < 0) {
						crush_choose_indep(
							map,
							work,
							map->buckets[-1-item],
							weight, weight_max,
							x, 1, numrep, 0,
							out2, rep,
							recurse_tries, 0,
							0, NULL, r, choose_args);
						if (out2 && out2[rep] == CRUSH_ITEM_NONE) {
							/* placed nothing; no leaf */
							break;
						}
					} else if (out2) {
						/* we already have a leaf! */
						out2[rep] = item;
					}
				}

				/* out? */
				if (itemtype == 0 &&
				    is_out(map, weight, weight_max, item, x))
					break;

				/* yay! */
				out[rep] = item;
				left--;
				break;
			}
		}
	}
	for (rep = outpos; rep < endpos; rep++) {
		if (out[rep] == CRUSH_ITEM_UNDEF) {
			out[rep] = CRUSH_ITEM_NONE;
		}
		if (out2 && out2[rep] == CRUSH_ITEM_UNDEF) {
			out2[rep] = CRUSH_ITEM_NONE;
		}
	}
#ifndef __KERNEL__
	if (map->choose_tries && ftotal <= map->choose_total_tries)
		map->choose_tries[ftotal]++;
#endif
#ifdef DEBUG_INDEP
	if (out2) {
		dprintk("%u %d a: ", ftotal, left);
		for (rep = outpos; rep < endpos; rep++) {
			dprintk(" %d", out[rep]);
		}
		dprintk("\n");
		dprintk("%u %d b: ", ftotal, left);
		for (rep = outpos; rep < endpos; rep++) {
			dprintk(" %d", out2[rep]);
		}
		dprintk("\n");
	}
#endif
}

static int crush_do_rule_no_retry(
	const struct crush_map *map,
	int ruleno, int x, int *result, int result_max,
	const __u32 *weight, int weight_max,
	void *cwin, const struct crush_choose_arg *choose_args)
{
	int result_len;
	struct crush_work *cw = cwin;
	int *a = (int *)((char *)cw + map->working_size);
	int *b = a + result_max;
	int *c = b + result_max;
	int *w = a;
	int *o = b;
	int recurse_to_leaf;
	int wsize = 0;
	int osize;
	int *tmp;
	const struct crush_rule *rule;
	__u32 step;
	int i, j;
	int numrep;
	int out_size;
	/*
	 * the original choose_total_tries value was off by one (it
	 * counted "retries" and not "tries").  add one.
	 */
	int choose_tries = map->choose_total_tries + 1;
	int choose_leaf_tries = 0;
	/*
	 * the local tries values were counted as "retries", though,
	 * and need no adjustment
	 */
	int choose_local_retries = map->choose_local_tries;
	int choose_local_fallback_retries = map->choose_local_fallback_tries;

	int vary_r = map->chooseleaf_vary_r;
	int stable = map->chooseleaf_stable;

	if ((__u32)ruleno >= map->max_rules) {
		dprintk(" bad ruleno %d\n", ruleno);
		return 0;
	}

	rule = map->rules[ruleno];
	result_len = 0;

	for (step = 0; step < rule->len; step++) {
		int firstn = 0;
		const struct crush_rule_step *curstep = &rule->steps[step];

		switch (curstep->op) {
		case CRUSH_RULE_TAKE:
			if ((curstep->arg1 >= 0 &&
			     curstep->arg1 < map->max_devices) ||
			    (-1-curstep->arg1 >= 0 &&
			     -1-curstep->arg1 < map->max_buckets &&
			     map->buckets[-1-curstep->arg1])) {
				w[0] = curstep->arg1;
				wsize = 1;
			} else {
				dprintk(" bad take value %d\n", curstep->arg1);
			}
			break;

		case CRUSH_RULE_SET_CHOOSE_TRIES:
			if (curstep->arg1 > 0)
				choose_tries = curstep->arg1;
			break;

		case CRUSH_RULE_SET_CHOOSELEAF_TRIES:
			if (curstep->arg1 > 0)
				choose_leaf_tries = curstep->arg1;
			break;

		case CRUSH_RULE_SET_CHOOSE_LOCAL_TRIES:
			if (curstep->arg1 >= 0)
				choose_local_retries = curstep->arg1;
			break;

		case CRUSH_RULE_SET_CHOOSE_LOCAL_FALLBACK_TRIES:
			if (curstep->arg1 >= 0)
				choose_local_fallback_retries = curstep->arg1;
			break;

		case CRUSH_RULE_SET_CHOOSELEAF_VARY_R:
			if (curstep->arg1 >= 0)
				vary_r = curstep->arg1;
			break;

		case CRUSH_RULE_SET_CHOOSELEAF_STABLE:
			if (curstep->arg1 >= 0)
				stable = curstep->arg1;
			break;

		case CRUSH_RULE_CHOOSELEAF_FIRSTN:
		case CRUSH_RULE_CHOOSE_FIRSTN:
			firstn = 1;
			/* fall through */
		case CRUSH_RULE_CHOOSELEAF_INDEP:
		case CRUSH_RULE_CHOOSE_INDEP:
			if (wsize == 0)
				break;

			recurse_to_leaf =
				curstep->op ==
				 CRUSH_RULE_CHOOSELEAF_FIRSTN ||
				curstep->op ==
				CRUSH_RULE_CHOOSELEAF_INDEP;

			/* reset output */
			osize = 0;

			for (i = 0; i < wsize; i++) {
				int bno;
				numrep = curstep->arg1;
				if (numrep <= 0) {
					numrep += result_max;
					if (numrep <= 0)
						continue;
				}
				j = 0;
				/* make sure bucket id is valid */
				bno = -1 - w[i];
				if (bno < 0 || bno >= map->max_buckets) {
					// w[i] is probably CRUSH_ITEM_NONE
					dprintk("  bad w[i] %d\n", w[i]);
					continue;
				}
				if (firstn) {
					int recurse_tries;
					if (choose_leaf_tries)
						recurse_tries =
							choose_leaf_tries;
					else if (map->chooseleaf_descend_once)
						recurse_tries = 1;
					else
						recurse_tries = choose_tries;
					osize += crush_choose_firstn(
						map,
						cw,
						map->buckets[bno],
						weight, weight_max,
						x, numrep,
						curstep->arg2,
						o+osize, j,
						result_max-osize,
						choose_tries,
						recurse_tries,
						choose_local_retries,
						choose_local_fallback_retries,
						recurse_to_leaf,
						vary_r,
						stable,
						c+osize,
						0,
						choose_args);
				} else {
					out_size = ((numrep < (result_max-osize)) ?
						    numrep : (result_max-osize));
					crush_choose_indep(
						map,
						cw,
						map->buckets[bno],
						weight, weight_max,
						x, out_size, numrep,
						curstep->arg2,
						o+osize, j,
						choose_tries,
						choose_leaf_tries ?
						   choose_leaf_tries : 1,
						recurse_to_leaf,
						c+osize,
						0,
						choose_args);
					osize += out_size;
				}
			}

			if (recurse_to_leaf)
				/* copy final _leaf_ values to output set */
				memcpy(o, c, osize*sizeof(*o));

			/* swap o and w arrays */
			tmp = o;
			o = w;
			w = tmp;
			wsize = osize;
			break;


		case CRUSH_RULE_EMIT:
			for (i = 0; i < wsize && result_len < result_max; i++) {
				result[result_len] = w[i];
				result_len++;
			}
			wsize = 0;
			break;

		default:
			dprintk(" unknown op %d at step %d\n",
				curstep->op, step);
			break;
		}
	}

	return result_len;
}

/// invariant through crush_msr_do_rule invocation
struct crush_msr_input {
	const struct crush_map *map;
	const struct crush_rule *rule;

	const unsigned result_max;

	const unsigned weight_len;
	const __u32 *weights;

	const int map_input;
	const struct crush_choose_arg *choose_args;

	const unsigned msr_descents;
	const unsigned msr_collision_tries;
};

/// encapsulates work space, invariant within an EMIT block
struct crush_msr_workspace {
	const unsigned start_stepno;
	const unsigned end_stepno;

	const unsigned result_len;

	const struct crush_work *crush_work;

	// step_vecs has shape int[end_stepno - start_stepno][result_len]
	int **step_vecs;
};

/**
 * crush_msr_output
 *
 * Encapsulates output space.  Successive results through a crush_msr_do_rule
 * invocation are placed into *out.
 */
struct crush_msr_output {
	const unsigned result_len;
	unsigned returned_so_far;
	int *out;
};

/**
 * crush_msr_scan_config_steps
 *
 * Scans possibly empty sequence of CRUSH_RULE_SET_MSR_*_TRIES
 * steps at the start of the rule.  Returns index of next step.
 * Populates *msr_descents and *msr_collision_tries (if non-null) with
 * last matching rule.
 * @steps: steps to scan
 * @step_len: length of steps
 * @msr_descents: out param for CRUSH_RULE_SET_MSR_DESCENTS
 * @msr_collision_tries: out param for CRUSH_RULE_SET_MSR_COLLISION_TRIES
 */
static unsigned crush_msr_scan_config_steps(
	const struct crush_rule_step *steps,
	unsigned step_len,
	unsigned *msr_descents,
	unsigned *msr_collision_tries)
{
	unsigned stepno = 0;
	for (; stepno < step_len; ++stepno) {
		const struct crush_rule_step *step = &steps[stepno];
		switch (step->op) {
		case CRUSH_RULE_SET_MSR_DESCENTS:
			if (msr_descents) *msr_descents = step->arg1;
			break;
		case CRUSH_RULE_SET_MSR_COLLISION_TRIES:
			if (msr_collision_tries) *msr_collision_tries = step->arg1;
			break;
		default:
			return stepno;
		}
	}
	return stepno;
}

/// clear workspace represented by *ws
static void crush_msr_clear_workspace(
	struct crush_msr_workspace *ws)
{
	for (unsigned stepno = ws->start_stepno; stepno < ws->end_stepno;
	     ++stepno) {
		for (unsigned i = 0; i < ws->result_len; ++i) {
			ws->step_vecs[stepno - ws->start_stepno][i] =
				CRUSH_ITEM_UNDEF;
		}
	}
}

/**
 * crush_msr_scan_next
 *
 * Validates an EMIT block of the form (TAKE CHOOSE_MSR* EMIT)
 * If sequence is valid, populates total_children with the width
 * of the mapping from the choose steps and next_emit with the
 * index of the next EMIT step.
 *
 * @rule: rule to scan
 * @result_max: max number of results to return
 * @stepno: points at step at which to begin scanning, must be CRUSH_RULE_TAKE
 * @total_children: output param for total fanout of EMIT block
 * @next_emit: output param for ending EMIT step
 * @return 0 if valid, -1 if there were validation errors
 */
static int crush_msr_scan_next(
	const struct crush_rule *rule,
	unsigned result_max,
	unsigned stepno,
	unsigned *total_children,
	unsigned *next_emit)
{
	if (stepno + 1 >= rule->len) {
		dprintk("stepno too large\n");
		return -1;
	}
	if (rule->steps[stepno].op != CRUSH_RULE_TAKE) {
		dprintk("first rule not CRUSH_RULE_TAKE\n");
		return -1;
	}
	++stepno;

	if (total_children) *total_children = 1;
	for (; stepno < rule->len; ++stepno) {
		const struct crush_rule_step *curstep =
			&(rule->steps[stepno]);
		if (curstep->op == CRUSH_RULE_EMIT) {
			break;
		}
		if (rule->steps[stepno].op != CRUSH_RULE_CHOOSE_MSR) {
			dprintk("found non-choose non-emit step %d\n", stepno);
			return -1;
		}
		if (total_children) {
			*total_children *= curstep->arg1 ? curstep->arg1
				: result_max;
		}
	}
	if (stepno >= rule->len) {
		dprintk("did not find emit\n");
		return -1;
	}
	if (next_emit) {
		*next_emit = stepno;
	}
	return 0;
}

/**
 * crush_msr_scan_rule
 *
 * MSR rules must have the form:
 * 1) Possibly empty sequence of CRUSH_RULE_SET_MSR_.*_TRIES steps
 * 2) A sequence of EMIT blocks of the form
 *   (TAKE CHOOSE_MSR* EMIT)*
 *
 * crush_msr_scan_rule validates that the form obeys the above form and
 * populates max_steps with the length of the longest sequence of CHOOSE_MSR
 * steps.
 *
 * crush_msr_scan_rule replicates the scan behavior of crush_msr_do_rule.
 *
 * @rule: rule to scan
 * @result_max: max number of results to return
 * @max_steps: length of longest string of choosemsr steps
 * @return 0 if valid, -1 otherwise
 */
static int crush_msr_scan_rule(
	const struct crush_rule *rule,
	unsigned result_max,
	unsigned *max_steps)
{
	if (max_steps) *max_steps = 0;
	unsigned next_stepno = crush_msr_scan_config_steps(
		rule->steps,
		rule->len,
		NULL, NULL);
	while (next_stepno < rule->len) {
		unsigned next_emit_stepno;
		int r = crush_msr_scan_next(
			rule, result_max, next_stepno,
			NULL, &next_emit_stepno);
		if (r < 0) return r;

		if (max_steps) {
			*max_steps = MAX(
				*max_steps,
				next_emit_stepno - (next_stepno + 1));
		}
		next_stepno = next_emit_stepno + 1;
	}
	return 0;
}

/// Returns true if all leaf slots in [start, end) are mapped
static int crush_msr_leaf_vec_populated(
	const struct crush_msr_workspace *workspace,
	const unsigned start, const unsigned end)
{
	BUG_ON(start >= end);
	BUG_ON(end > workspace->result_len);
	BUG_ON(workspace->end_stepno <= workspace->start_stepno);
	// we check the last step vector here because output
	// won't be ordered by index for FIRSTN rules
	int *leaf_vec = workspace->step_vecs[
	  workspace->end_stepno - workspace->start_stepno - 1];
	for (unsigned i = start; i < end; ++i) {
		if (leaf_vec[i] == CRUSH_ITEM_UNDEF) {
			return 0;
		}
	}
	return 1;
}

/// Returns try value to pass to crush based on index, tries, and local_tries
static unsigned crush_msr_get_retry_value(
	const unsigned result_max,
	const unsigned index,
	const unsigned msr_descents,
	const unsigned msr_collision_tries)
{
	const unsigned total_index = (msr_descents * result_max) + index;
	return (total_index << 16) + msr_collision_tries;
}

/**
 * crush_msr_descend
 *
 * Descend recursively from bucket until we either hit a leaf or an
 * interior node of type type.
 * @input: crush input information
 * @workspace: struct with working space
 * @bucket: bucket from which to descend
 * @type: target node type
 * @tryno: top level try number, incremented with each call into crush_msr_choose
 *         from crush_msr_do_rule
 * @local_tryno: local collision try number, incremented with each call into
 *               crush_msr_descend from crush_msr_choose after collision
 * @index: mapping index
 */
static int crush_msr_descend(
	const struct crush_msr_input *input,
	const struct crush_msr_workspace *workspace,
	const struct crush_bucket *bucket,
	const int type,
	const unsigned tryno,
	const unsigned local_tryno,
	const unsigned index)
{
	dprintk(" crush_msr_descend type %d tryno %d local_tryno %d index %d\n",
		type, tryno, local_tryno, index);
	while (1) {
		const int child_bucket_candidate = crush_bucket_choose(
			bucket,
			workspace->crush_work->work[-1 - bucket->id],
			input->map_input,
			crush_msr_get_retry_value(
				input->result_max,
				index, tryno, local_tryno),
			(input->choose_args ?
			 &(input->choose_args[-1 - bucket->id]) : 0),
			index);

		if (child_bucket_candidate >= 0) {
			return child_bucket_candidate;
		}

		bucket = input->map->buckets[-1 - child_bucket_candidate];
		if (bucket->type == type) {
			return child_bucket_candidate;
		}
	}
}

/**
 * crush_msr_valid_candidate
 *
 * Checks whether candidate is a valid choice given buckets already
 * mapped for step stepno.
 *
 * If candidate has already been mapped for a position in
 * [include_start, include_end), candidate is valid.
 *
 * Else, if candidate has already been mapped for a position in
 * [exclude_start, exclude_end), candidate is invalid.
 *
 * Otherwise, candidate is valid.
 *
 * @stepno: step to check
 * @exclude_start: start of exclusion range
 * @exclude_end: end of exlusion range
 * @include_start: start of inclusion range
 * @include_end: end of inclusion range
 * @candidate: bucket to check
 *
 * Note, [exclude_start, exclude_end) must contain [include_start, include_end).
 */
static int crush_msr_valid_candidate(
	const struct crush_msr_workspace *workspace,
	unsigned stepno,
	unsigned exclude_start,
	unsigned exclude_end,
	unsigned include_start,
	unsigned include_end,
	int candidate)
{
	BUG_ON(stepno >= workspace->end_stepno);
	BUG_ON(stepno < workspace->start_stepno);

	BUG_ON(exclude_end <= exclude_start);
	BUG_ON(include_end <= include_start);

	BUG_ON(exclude_start > include_start);
	BUG_ON(exclude_end < include_end);

	BUG_ON(exclude_end > workspace->result_len);

	int *vec = workspace->step_vecs[stepno - workspace->start_stepno];
	for (unsigned i = exclude_start; i < exclude_end; ++i) {
		if (vec[i] == candidate) {
			if (i >= include_start && i < include_end) {
				dprintk(" crush_msr_valid_candidate: "
					"candidate %d already chosen for "
					"stride\n",
					candidate);
				return 1;
			} else {
				dprintk(" crush_msr_valid_candidate: "
					"candidate %d collision\n",
					candidate);
				return 0;
			}
		}
	}
	dprintk(" crush_msr_valid_candidate: candidate %d no collision\n",
		candidate);
	return 1;
}

/**
 * crush_msr_push_used
 *
 * See crush_msr_choose for details, used to push bucket indicies onto collision
 * set for specified stride.  User is responsible for ensuring that
 * [stride_start, stride_end) never holds more than stride_end - stride_start
 * entries.
 * @workspace: holds working space information
 * @stepno: index of step
 * @stride_start: start of stride
 * @stride_end: one past end of stride
 * @candidate: element to add to set
 * @return 1 if added (not already present), 0 if not added due to already
 *           being present
 */
static int crush_msr_push_used(
	const struct crush_msr_workspace *workspace,
	unsigned stepno,
	unsigned stride_start,
	unsigned stride_end,
	int candidate)
{
	BUG_ON(stepno >= workspace->end_stepno);
	BUG_ON(stepno < workspace->start_stepno);

	BUG_ON(stride_end <= stride_start);
	BUG_ON(stride_end > workspace->result_len);
	int *vec = workspace->step_vecs[stepno - workspace->start_stepno];
	for (unsigned i = stride_start; i < stride_end; ++i) {
		if (vec[i] == candidate) {
			return 0;
		} else if (vec[i] == CRUSH_ITEM_UNDEF) {
			vec[i] = candidate;
			return 1;
		}
	}
	BUG_ON("impossible");
	return 0;
}

/**
 * crush_msr_pop_used
 *
 * See crush_msr_choose for details, used to pop bucket indicies from collision
 * set for specified stride.  If an element is to be popped, crush_msr_pop_used
 * must be called prior to pushing another element.
 * @workspace: holds working space information
 * @stepno: index of step
 * @stride_start: start of stride
 * @stride_end: one past end of stride
 * @candidate: element to pop from set
 */
static void crush_msr_pop_used(
	const struct crush_msr_workspace *workspace,
	unsigned stepno,
	unsigned stride_start,
	unsigned stride_end,
	int candidate)
{
	BUG_ON(stepno >= workspace->end_stepno);
	BUG_ON(stepno < workspace->start_stepno);

	BUG_ON(stride_end <= stride_start);
	BUG_ON(stride_end > workspace->result_len);
	int *vec = workspace->step_vecs[stepno - workspace->start_stepno];
	for (unsigned i = stride_end; i > stride_start;) {
		--i;
		if (vec[i] != CRUSH_ITEM_UNDEF) {
			BUG_ON(vec[i] != candidate);
			vec[i] = CRUSH_ITEM_UNDEF;
			return;
		}
	}
	BUG_ON(0 == "impossible");
}

/**
 * crush_msr_emit_result
 *
 * Outputs mapping result from specified position.  Position in output
 * buffer depends on rule type -- FIRSTN outputs in output order, INDEP
 * outputs into specified position.
 * @output: output buffer
 * @rule_type: CRUSH_RULE_TYPE_MSR_FIRSTN or CRUSH_RULE_TYPE_MSR_INDEP
 * @position: mapping position
 * @result: mapping value to output
 */
static void crush_msr_emit_result(
	struct crush_msr_output *output,
	int rule_type,
	unsigned position,
	int result)
{
	BUG_ON(position >= output->result_len);
	BUG_ON(output->returned_so_far >= output->result_len);
	if (rule_type == CRUSH_RULE_TYPE_MSR_FIRSTN) {
		BUG_ON(output->out[output->returned_so_far] != CRUSH_ITEM_NONE);
		output->out[(output->returned_so_far)++] = result;
	} else {
		BUG_ON(output->out[position] != CRUSH_ITEM_NONE);
		output->out[position] = result;
		++output->returned_so_far;
	}
	dprintk(" emit: %d, returned_so_far: %d\n",
		result, output->returned_so_far);
}

/**
 * crush_msr_choose
 *
 * Performs mapping for a single EMIT block containing CHOOSE steps
 * [current_stepno, end_stepno) into mapping indices [start_index, end_index).
 *
 * Like chooseleaf, crush_msr_choose is essentially depth-first -- it chooses
 * an item and all of the descendents under that item before moving to the
 * next item.  Each choose step in the block gets its own workspace for
 * collision detection.
 *
 * crush_msr_choose (and its recursive calls) will locally retry any bucket
 * selections that produce a collision (up to msr_collision_tries times), but
 * won't retry if it hits an out osd -- that's handled by calling back into
 * crush_msr_choose up to msr_descents times.
 *
 * @input: crush input information
 * @workspace: working space for this EMIT block
 * @output: crush mapping output buffer specification
 * @total_children: total number of children implied by the step sequence, may
 *                  be larger than end_index - start_index.
 * @start_index: start mapping index
 * @end_index: end mapping index
 * @current_stepno: first choose step
 * @end_stepno: one past last choose step, must be an EMIT
 * @tryno: try number, see crush_msr_do_rule
 */
static unsigned crush_msr_choose(
	const struct crush_msr_input *input,
	const struct crush_msr_workspace *workspace,
	struct crush_msr_output *output,
	const struct crush_bucket *bucket,
	const unsigned total_descendants,
	const unsigned start_index, const unsigned end_index,
	const unsigned current_stepno, const unsigned end_stepno,
	const unsigned tryno)
{
	dprintk("crush_msr_choose: bucket %d, start_index %d, end_index %d\n",
		bucket->id, start_index, end_index);

	BUG_ON(current_stepno >= input->rule->len);
	const struct crush_rule_step *curstep =
		&(input->rule->steps[current_stepno]);
	BUG_ON(curstep->op != CRUSH_RULE_CHOOSE_MSR);

	/* This call into crush_msr_choose is responsible, ultimately, for
	 * populating indices [start_index, end_index).  We do this by
	 * dividing that range into a set of strides specified in the
	 * step -- choosemsr 4 host would dictate that the range be divided
	 * into 4 strides.
	 *
	 * If the full rule is
	 *
	 * ...
	 * step take root
	 * step choosemsr 4 host
	 * step choosemsr 2 osd
	 * step emit
	 *
	 * total_descendants for the initial call would be 8 (4*2) with
	 * num_stride=4 (4 hosts) and stride_length = 2 (2 osds per host).
	 * For the recursive calls, total_descendants would be 2 (8 / 4),
	 * stride_length would be 1 and num_strides would be 2.
	 */

	// choosemsr 0 host should select result_max hosts
	const unsigned num_strides = curstep->arg1 ? curstep->arg1
		: input->result_max;

	// total_descendants is the product of the steps in the block
	BUG_ON(total_descendants % num_strides != 0);
	const unsigned stride_length = total_descendants / num_strides;

	/* MSR steps like
	 *
	 * step choosemsr 4 host
	 *
	 * guarantee that the output mapping will be divided into at least
	 * 4 hosts, not exactly 4 hosts.  We achieve this by ensuring that
	 * the sets of hosts for each stride are disjoint -- a host selected
	 * for stride 0 will not be used for any other stride.
	 *
	 * However, a single stride might end up using more than one host.
	 * If an OSD on a host is marked out, crush_msr_choose will simply
	 * skip that index when it hits it.  crush_msr_do_rule will then
	 * call back into crush_msr_choose and eventually find another OSD
	 * either on the same host or on another one not already used in
	 * another stride. For this reason, a single stride may need to
	 * remember up to stride_length entries for collision detection
	 * purposes.
	 *
	 * Unfortunately, we only have stride_length entries to work with
	 * in workspace.  Thus, prior to returning from crush_msr_choose,
	 * we remove entries that didn't actually result in a mapping.  We
	 * use the following undo vector to achieve this -- any strides that
	 * didn't result in a successful mapping are set in undo to be undone
	 * immediately prior to returning.
	 *
	 * Why prior to returning and not immediately?  Selecting a bucket in
	 * a stride impacts subsequent choices as they may have collided.  In
	 * order to limit the impact of marking an OSD out, we treat it as
	 * collidable until the next pass.
	 */
	int undo[num_strides];
	for (unsigned stride = 0; stride < num_strides; ++stride) {
		undo[stride] = CRUSH_ITEM_UNDEF;
	}

	dprintk("crush_msr_choose: bucket %d, start_index %d, "
		"end_index %d, stride_length %d\n",
		bucket->id, start_index, end_index, stride_length);

	unsigned mapped = 0;
	unsigned stride_index = 0;
	for (unsigned stride_start = start_index;
	     stride_start < end_index;
	     stride_start += stride_length, ++stride_index) {
		const unsigned stride_end =
		  MIN(stride_start + stride_length, end_index);

		// all descendants for this stride have been mapped already
		if (crush_msr_leaf_vec_populated(
		      workspace, stride_start, stride_end)) {
		  continue;
		}

		int found = 0;
		int child_bucket_candidate;
		for (unsigned local_tryno = 0;
		     local_tryno < input->msr_collision_tries;
		     ++local_tryno) {
			child_bucket_candidate = crush_msr_descend(
				input, workspace, bucket,
				curstep->arg2, tryno, local_tryno,
				stride_index);

			/* candidate is valid if:
			 * - we already chose it for this stride
			 * - it hasn't been chosen for any stride */
			if (crush_msr_valid_candidate(
				    workspace,
				    current_stepno,
				    // Collision on elements in [start_index, end_index)
				    start_index, end_index,
				    // ...unless in [stride_start, stride_end)
				    stride_start, stride_end,
				    child_bucket_candidate)) {
				found = 1;
				break;
			}
		}

		/* failed to find non-colliding choice after msr_collision_tries
		 * attempts */
		if (!found) continue;

		if (curstep->arg2 == 0 /* leaf */) {
			if (stride_length != 1 ||
			    (current_stepno + 1 != end_stepno)) {
				/* Either condition above implies that there's
				 * another step after a choosemsr step for the
				 * leaf type, rule is malformed, bail */
				continue;
			}
			if (is_out(input->map, input->weights,
				   input->weight_len,
				   child_bucket_candidate, input->map_input)) {
				dprintk(" crush_msr_choose: item %d out\n",
					child_bucket_candidate);
				/* crush_msr_do_rule will try again,
				 * msr_descents permitting */
				continue;
			}
			// for collision detection
			int pushed = crush_msr_push_used(
				workspace, current_stepno, stride_start, stride_end,
				child_bucket_candidate);
			/* stride_length == 1, can't already be there */
			BUG_ON(!pushed);
			// final output, ordering depending on input->rule->type
			crush_msr_emit_result(
				output, input->rule->type,
				stride_start, child_bucket_candidate);
			mapped++;
		} else /* not leaf */ {
			if (current_stepno + 1 >= end_stepno) {
				/* Type isn't leaf, rule is malformed since there
				 * isn't another step */
				continue;
			}
			struct crush_bucket *child_bucket = input->map->buckets[
				-1 - child_bucket_candidate];
			unsigned child_mapped = crush_msr_choose(
				input, workspace, output,
				child_bucket,
				// total_descendants for recursive call
				stride_length,
				// recursive call populates
				// [stride_start, stride_end)
				stride_start, stride_end,
				// next step
				current_stepno + 1, end_stepno,
				tryno);
			int pushed = crush_msr_push_used(
				workspace,
				current_stepno,
				stride_start,
				stride_end,
				child_bucket_candidate);
			/* pushed may be false if we already chose this bucket
			 * for this stride.  If so, child_mapped must have been
			 * != 0 at the time, so we still retain it */
			if (pushed && (child_mapped == 0)) {
				// no child mapped, and we didn't choose it
				// before
				undo[stride_index] = child_bucket_candidate;
			} else {
				mapped += child_mapped;
			}
		}
	}

	// pop unused buckets
	stride_index = 0;
	for (unsigned stride_start = start_index;
	     stride_start < end_index;
	     stride_start += stride_length, ++stride_index) {
		if (undo[stride_index] != CRUSH_ITEM_UNDEF) {
			unsigned stride_end =
			  MIN(stride_start + stride_length, end_index);
			crush_msr_pop_used(
				workspace,
				current_stepno,
				stride_start,
				stride_end,
				undo[stride_index]);
		}
	}

	return mapped;
}

/**
 * crush_msr_do_rule - calculate a mapping with the given input and msr rule
 *
 * msr_firstn and msr_indep rules are intended to address a limitation of
 * conventional crush rules in that they do not retry steps outside of
 * a CHOOSELEAF step.  In the case of a crush rule like
 *
 * rule replicated_rule_1 {
 *   ...
 *   step take default class hdd
 *   step chooseleaf firstn 3 type host
 *   step emit
 * }
 *
 * the chooseleaf step will ensure that if all of the osds on a
 * particular host are marked out, mappings including those OSDs would
 * end up on another host (provided that there are enough hosts).
 *
 * However, if the rule used two choose steps instead
 *
 * rule replicated_rule_1 {
 *   ...
 *   step take default class hdd
 *   step choose firstn 3 type host
 *   step choose firstn 1 type osd
 *   step emit
 * }
 *
 * marking an OSD down could cause it to be remapped to another on the same
 * host, but not to another host.  If all of the OSDs on a host are marked
 * down, the PGs will simply be degraded and unable to remap until the host
 * is removed from the CRUSH heirarchy or reweighted to 0.
 *
 * Normally, we can comfortably work around this by using a chooseleaf
 * step as in the first example, but there are cases where we want to map
 * multiple OSDs to each host (wide EC codes on small clusters, for
 * example) which can't be handled with chooseleaf as it currently
 * exists.
 *
 * rule ecpool-86 {
 *   type msr_indep
 *   ...
 *   step choosemsr 4 type host
 *   step choosemsr 4 type osd
 *   step emit
 * }
 *
 * With an 8+6 code, this rule can tolerate a host and a single OSD down without
 * becoming unavailable on 4 hosts.  It relies on ensuring that no more than 4
 * OSDs are mapped to any single host, however, which can't be done with a
 * conventional CRUSH rule without the drawback described above.  By using
 * msr_indep, this rule can deal with an OSD failure by remapping to another
 * host.
 *
 * MSR rules have some structural differences from conventional rules:
 * - The rule type determines whether the mapping is FIRSTN or INDEP.  Because
 *   the descent can retry steps, it doesn't really make sense for steps to
 *   individually specify output order and I'm not really aware of any use cases
 *   that would benefit from it.
 * - MSR rules *must* be structured as a (possibly empty) prefix of config
 *   steps (CRUSH_RULE_SET_MSR*) followed by a sequence of EMIT blocks
 *   each comprised of a TAKE step, a sequence of CHOOSE_MSR steps, and
 *   ended by an EMIT step.
 * - MSR choose steps must be choosemsr.  choose and chooseleaf are not permitted.
 *
 * MSR rules also have different requirements for working space.  Conventional
 * CRUSH requires 3 vectors of size result_max to use for working space -- two
 * to alternate as it processes each rule and one, additionally, for chooseleaf.
 * MSR rules need N vectors where N is the number of choosemsr in the longest
 * EMIT block since it needs to retain all of the choices made as part of each
 * descent.
 *
 * See crush_msr_choose for details.
 *
 * doc/dev/crush-msr.rst has an overview of the motivation behind CRUSH MSR
 * rules and should be kept up to date with any changes to implementation or
 * documentation in this file.
 *
 * @map: the crush_map
 * @ruleno: the rule id
 * @x: hash input
 * @result: pointer to result vector
 * @result_max: maximum result size
 * @weight: weight vector (for map leaves)
 * @weight_max: size of weight vector
 * @cwin: Pointer to at least map->working_size bytes of memory or NULL.
 */
static int crush_msr_do_rule(
	const struct crush_map *map,
	int ruleno, int map_input, int *result, int result_max,
	const __u32 *weight, int weight_max,
	void *cwin, const struct crush_choose_arg *choose_args)
{
	unsigned msr_descents = map->msr_descents;
	unsigned msr_collision_tries = map->msr_collision_tries;
	struct crush_rule *rule = map->rules[ruleno];
	unsigned start_stepno = crush_msr_scan_config_steps(
		rule->steps, rule->len,
		&msr_descents, &msr_collision_tries);

	struct crush_msr_input input = {
		.map = map,
		.rule = map->rules[ruleno],
		.result_max = result_max,
		.weight_len = weight_max,
		.weights = weight,
		.map_input = map_input,
		.choose_args = choose_args,
		.msr_descents = msr_descents,
		.msr_collision_tries = msr_collision_tries
	};

	struct crush_msr_output output = {
		.result_len = result_max,
		.returned_so_far = 0,
		.out = result
	};
	for (unsigned i = 0; i < output.result_len; ++i) {
		output.out[i] = CRUSH_ITEM_NONE;
	}

	unsigned start_index = 0;
	while (start_stepno < input.rule->len) {
		unsigned emit_stepno, total_children;
		if (crush_msr_scan_next(
			    input.rule, input.result_max,
			    start_stepno, &total_children,
			    &emit_stepno) != 0) {
			// invalid rule, return whatever we have
			dprintk("crush_msr_scan_returned -1\n");
			return 0;
		}

		const struct crush_rule_step *take_step =
			&(input.rule->steps[start_stepno]);
		BUG_ON(take_step->op != CRUSH_RULE_TAKE);

		if (take_step->arg1 >= 0) {
			if (start_stepno + 1 != emit_stepno) {
				// invalid rule
				dprintk("take step specifies osd, but "
					"there are subsequent choose steps\n");
				return 0;
			} else {
				crush_msr_emit_result(
					&output, input.rule->type,
					start_index, take_step->arg1);
			}
		} else {
			dprintk("start_stepno %d\n", start_stepno);
			dprintk("root bucket: %d\n",
				input.rule->steps[start_stepno].arg1);
			struct crush_bucket *root_bucket = input.map->buckets[
				-1 - input.rule->steps[start_stepno].arg1];
			dprintk(
				"root bucket: %d %p\n",
				input.rule->steps[start_stepno].arg1,
				root_bucket);

			++start_stepno;
			BUG_ON(emit_stepno >= input.rule->len);
			BUG_ON(emit_stepno < start_stepno);
			BUG_ON(start_stepno >= input.rule->len);

			struct crush_work *cw = cwin;
			int *out_vecs[input.rule->len];
			for (unsigned stepno = 0;
			     stepno < input.rule->len; ++stepno) {
				out_vecs[stepno] =
					(int*)((char*)cw + map->working_size) +
					(stepno * result_max);
			}
			struct crush_msr_workspace workspace = {
				.start_stepno = start_stepno,
				.end_stepno = emit_stepno,
				.result_len = result_max,
				.crush_work = cw,
				.step_vecs = out_vecs
			};
			crush_msr_clear_workspace(&workspace);


			unsigned tries_so_far = 0;
			unsigned end_index = MIN(start_index + total_children,
						 input.result_max);
			unsigned return_limit_for_block =
				output.returned_so_far + (end_index - start_index);
			while (tries_so_far < input.msr_descents &&
			       output.returned_so_far < return_limit_for_block) {
				crush_msr_choose(
					&input, &workspace, &output,
					root_bucket,
					total_children,
					start_index,
					end_index,
					start_stepno, emit_stepno,
					tries_so_far);
				dprintk("returned_so_far: %d\n",
					output.returned_so_far);
				++tries_so_far;
			}
			start_index = end_index;
			start_stepno = emit_stepno + 1;
		}
	}

	if (rule->type == CRUSH_RULE_TYPE_MSR_FIRSTN) {
		return output.returned_so_far;
	} else {
		return input.result_max;
	}
}

/// Return 1 if msr, 0 otherwise
static int rule_type_is_msr(int type)
{
	return type == CRUSH_RULE_TYPE_MSR_FIRSTN ||
		type == CRUSH_RULE_TYPE_MSR_INDEP;
}

size_t crush_work_size(const struct crush_map *map,
		       int result_max)
{
	unsigned ruleno;
	unsigned out_vecs = 3; /* normal do_rule needs 3 outvecs */
	for (ruleno = 0; ruleno < map->max_rules; ++ruleno) {
		const struct crush_rule *rule = map->rules[ruleno];
		if (!rule) continue;
		if (!rule_type_is_msr(rule->type))
			continue;
		unsigned rule_max_msr_steps;
		// we ignore the return value because rule_max_msr_steps will be
		// populated with the longest step sequence before hitting
		// the error
		(void)crush_msr_scan_rule(rule, result_max, &rule_max_msr_steps);
		out_vecs = MAX(rule_max_msr_steps, out_vecs);
	}
	return map->working_size + result_max * out_vecs * sizeof(__u32);
}

/* This takes a chunk of memory and sets it up to be a shiny new
   working area for a CRUSH placement computation. It must be called
   on any newly allocated memory before passing it in to
   crush_do_rule. It may be used repeatedly after that, so long as the
   map has not changed. If the map /has/ changed, you must make sure
   the working size is no smaller than what was allocated and re-run
   crush_init_workspace.

   If you do retain the working space between calls to crush, make it
   thread-local. If you reinstitute the locking I've spent so much
   time getting rid of, I will be very unhappy with you. */

void crush_init_workspace(const struct crush_map *m, void *v) {
	/* We work by moving through the available space and setting
	   values and pointers as we go.

	   It's a bit like Forth's use of the 'allot' word since we
	   set the pointer first and then reserve the space for it to
	   point to by incrementing the point. */
	struct crush_work *w = (struct crush_work *)v;
	char *point = (char *)v;
	__s32 b;
	point += sizeof(struct crush_work);
	w->work = (struct crush_work_bucket **)point;
	point += m->max_buckets * sizeof(struct crush_work_bucket *);
	for (b = 0; b < m->max_buckets; ++b) {
		if (m->buckets[b] == 0)
			continue;

		w->work[b] = (struct crush_work_bucket *) point;
		switch (m->buckets[b]->alg) {
		default:
			point += sizeof(struct crush_work_bucket);
			break;
		}
		w->work[b]->perm_x = 0;
		w->work[b]->perm_n = 0;
		w->work[b]->perm = (__u32 *)point;
		point += m->buckets[b]->size * sizeof(__u32);
	}
	BUG_ON((char *)point - (char *)w != m->working_size);
}

/**
 * crush_do_rule - calculate a mapping with the given input and rule
 * @map: the crush_map
 * @ruleno: the rule id
 * @x: hash input
 * @result: pointer to result vector
 * @result_max: maximum result size
 * @weight: weight vector (for map leaves)
 * @weight_max: size of weight vector
 * @cwin: Pointer to at least map->working_size bytes of memory or NULL.
 */
int crush_do_rule(const struct crush_map *map,
		  int ruleno, int x, int *result, int result_max,
		  const __u32 *weight, int weight_max,
		  void *cwin, const struct crush_choose_arg *choose_args)
{
	const struct crush_rule *rule;

	if ((__u32)ruleno >= map->max_rules) {
		dprintk(" bad ruleno %d\n", ruleno);
		return 0;
	}

	rule = map->rules[ruleno];
	if (rule_type_is_msr(rule->type)) {
		return crush_msr_do_rule(
			map,
			ruleno,
			x,
			result,
			result_max,
			weight,
			weight_max,
			cwin,
			choose_args);
	} else {
		return crush_do_rule_no_retry(
			map,
			ruleno,
			x,
			result,
			result_max,
			weight,
			weight_max,
			cwin,
			choose_args);
	}
}
