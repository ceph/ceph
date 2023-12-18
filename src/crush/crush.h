#ifndef CEPH_CRUSH_CRUSH_H
#define CEPH_CRUSH_CRUSH_H

#ifdef __KERNEL__
# include <linux/types.h>
#else
# include "crush_compat.h"
#endif

/*
 * CRUSH is a pseudo-random data distribution algorithm that
 * efficiently distributes input values (typically, data objects)
 * across a heterogeneous, structured storage cluster.
 *
 * The algorithm was originally described in detail in this paper
 * (although the algorithm has evolved somewhat since then):
 *
 *     http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf
 *
 * LGPL-2.1 or LGPL-3.0
 */


#define CRUSH_MAGIC 0x00010000ul   /* for detecting algorithm revisions */

#define CRUSH_MAX_DEPTH 10  /* max crush hierarchy depth */
#define CRUSH_MAX_RULES (1<<8)  /* max crush rule id */

#define CRUSH_MAX_DEVICE_WEIGHT (100u * 0x10000u)
#define CRUSH_MAX_BUCKET_WEIGHT (65535u * 0x10000u)

#define CRUSH_ITEM_UNDEF  0x7ffffffe  /* undefined result (internal use only) */
/** @ingroup API
 * The equivalent of NULL for an item, i.e. the absence of an item.
 */
#define CRUSH_ITEM_NONE   0x7fffffff

/*
 * CRUSH uses user-defined "rules" to describe how inputs should be
 * mapped to devices.  A rule consists of sequence of steps to perform
 * to generate the set of output devices.
 */
struct crush_rule_step {
	__u32 op;
	__s32 arg1;
	__s32 arg2;
};

/** @ingroup API
 */
enum crush_opcodes {
        /*! do nothing
         */
	CRUSH_RULE_NOOP = 0,
	CRUSH_RULE_TAKE = 1,          /* arg1 = value to start with */
	CRUSH_RULE_CHOOSE_FIRSTN = 2, /* arg1 = num items to pick */
				      /* arg2 = type */
	CRUSH_RULE_CHOOSE_INDEP = 3,  /* same */
	CRUSH_RULE_EMIT = 4,          /* no args */
	CRUSH_RULE_CHOOSELEAF_FIRSTN = 6,
	CRUSH_RULE_CHOOSELEAF_INDEP = 7,

	CRUSH_RULE_SET_CHOOSE_TRIES = 8, /* override choose_total_tries */
	CRUSH_RULE_SET_CHOOSELEAF_TRIES = 9, /* override chooseleaf_descend_once */
	CRUSH_RULE_SET_CHOOSE_LOCAL_TRIES = 10,
	CRUSH_RULE_SET_CHOOSE_LOCAL_FALLBACK_TRIES = 11,
	CRUSH_RULE_SET_CHOOSELEAF_VARY_R = 12,
	CRUSH_RULE_SET_CHOOSELEAF_STABLE = 13,

	CRUSH_RULE_SET_MSR_DESCENTS = 14,
	CRUSH_RULE_SET_MSR_COLLISION_TRIES = 15,

	/* choose variant without FIRSTN|INDEP */
	CRUSH_RULE_CHOOSE_MSR = 16
};

/*
 * for specifying choose num (arg1) relative to the max parameter
 * passed to do_rule
 */
#define CRUSH_CHOOSE_N            0
#define CRUSH_CHOOSE_N_MINUS(x)   (-(x))

struct crush_rule {
	__u32 len;
	__u8 __unused_was_rule_mask_ruleset;
	__u8 type;
	__u8 deprecated_min_size;
	__u8 deprecated_max_size;
	struct crush_rule_step steps[0];
};

#define crush_rule_size(len) (sizeof(struct crush_rule) + \
			      (len)*sizeof(struct crush_rule_step))

enum crush_rule_type {
	CRUSH_RULE_TYPE_REPLICATED = 1,
	CRUSH_RULE_TYPE_ERASURE = 3,
	CRUSH_RULE_TYPE_MSR_FIRSTN = 4,
	CRUSH_RULE_TYPE_MSR_INDEP = 5
};

/*
 * A bucket is a named container of other items (either devices or
 * other buckets).
 */

/** @ingroup API
 *
 * Items within a bucket are chosen with crush_do_rule() using one of
 * three algorithms representing a tradeoff between performance and
 * reorganization efficiency. If you are unsure of which bucket type
 * to use, we recommend using ::CRUSH_BUCKET_STRAW2.
 *
 * The table summarizes how the speed of each option measures up
 * against mapping stability when items are added or removed.
 *
 * 	Bucket Alg     Speed       Additions    Removals
 * 	------------------------------------------------
 * 	uniform         O(1)       poor         poor
 * 	list            O(n)       optimal      poor
 * 	straw2          O(n)       optimal      optimal
 */
enum crush_algorithm {
       /*!
        * Devices are rarely added individually in a large system.
        * Instead, new storage is typically deployed in blocks of identical
        * devices, often as an additional shelf in a server rack or perhaps
        * an entire cabinet. Devices reaching their end of life are often
        * similarly decommissioned as a set (individual failures aside),
        * making it natural to treat them as a unit.  CRUSH uniform buckets
        * are used to represent an identical set of devices in such
        * circumstances. The key advantage in doing so is performance
        * related: CRUSH can map replicas into uniform buckets in constant
        * time. In cases where the uniformity restrictions are not
        * appropriate, other bucket types can be used.  If the size of a
        * uniform bucket changes, there is a complete reshuffling of data
        * between devices, much like conventional hash-based distribution
        * strategies.
        */
	CRUSH_BUCKET_UNIFORM = 1,
        /*!
         * List buckets structure their contents as a linked list, and
         * can contain items with arbitrary weights.  To place a
         * replica, CRUSH begins at the head of the list with the most
         * recently added item and compares its weight to the sum of
         * all remaining items' weights.  Depending on the value of
         * hash( x , r , item), either the current item is chosen with
         * the appropriate probability, or the process continues
         * recursively down the list.  This is a natural and intuitive
         * choice for an expanding cluster: either an object is
         * relocated to the newest device with some appropriate
         * probability, or it remains on the older devices as before.
         * The result is optimal data migration when items are added
         * to the bucket. Items removed from the middle or tail of the
         * list, however, can result in a significant amount of
         * unnecessary movement, making list buckets most suitable for
         * circumstances in which they never (or very rarely) shrink.
         */
	CRUSH_BUCKET_LIST = 2,
        /*! @cond INTERNAL */
	CRUSH_BUCKET_TREE = 3,
	CRUSH_BUCKET_STRAW = 4,
	/*! @endcond */
        /*!
         * List and tree buckets are structured such that a limited
         * number of hash values need to be calculated and compared to
         * weights in order to select a bucket item.  In doing so,
         * they divide and conquer in a way that either gives certain
         * items precedence (e. g., those at the beginning of a list)
         * or obviates the need to consider entire subtrees of items
         * at all. That improves the performance of the replica
         * placement process, but can also introduce suboptimal
         * reorganization behavior when the contents of a bucket
         * change due an addition, removal, or re-weighting of an
         * item.
         *
         * The straw2 bucket type allows all items to fairly "compete"
         * against each other for replica placement through a process
         * analogous to a draw of straws.  To place a replica, a straw
         * of random length is drawn for each item in the bucket.  The
         * item with the longest straw wins.  The length of each straw
         * is initially a value in a fixed range.  Each straw length
         * is scaled by a factor based on the item's weight so that
         * heavily weighted items are more likely to win the draw.
         * Although this process is almost twice as slow (on average)
         * than a list bucket and even slower than a tree bucket
         * (which scales logarithmically), straw2 buckets result in
         * optimal data movement between nested items when modified.
         */
	CRUSH_BUCKET_STRAW2 = 5,
};
extern const char *crush_bucket_alg_name(int alg);

/*
 * although tree was a legacy algorithm, it has been buggy, so
 * exclude it.
 */
#define CRUSH_LEGACY_ALLOWED_BUCKET_ALGS (	\
		(1 << CRUSH_BUCKET_UNIFORM) |	\
		(1 << CRUSH_BUCKET_LIST) |	\
		(1 << CRUSH_BUCKET_STRAW))

/** @ingroup API
 *
 * A bucket contains __size__ __items__ which are either positive
 * numbers or negative numbers that reference other buckets and is
 * uniquely identified with __id__ which is a negative number.  The
 * __weight__ of a bucket is the cumulative weight of all its
 * children.  A bucket is assigned a ::crush_algorithm that is used by
 * crush_do_rule() to draw an item depending on its weight.  A bucket
 * can be assigned a strictly positive (> 0) __type__ defined by the
 * caller. The __type__ can be used by crush_do_rule(), when it is
 * given as an argument of a rule step.
 *
 * A pointer to crush_bucket can safely be cast into the following
 * structure, depending on the value of __alg__:
 *
 * - __alg__ == ::CRUSH_BUCKET_UNIFORM cast to crush_bucket_uniform
 * - __alg__ == ::CRUSH_BUCKET_LIST cast to crush_bucket_list
 * - __alg__ == ::CRUSH_BUCKET_STRAW2 cast to crush_bucket_straw2
 *
 * The weight of each item depends on the algorithm and the
 * information about it is available in the corresponding structure
 * (crush_bucket_uniform, crush_bucket_list or crush_bucket_straw2).
 *
 * See crush_map for more information on how __id__ is used
 * to reference the bucket.
 */
struct crush_bucket {
	__s32 id;        /*!< bucket identifier, < 0 and unique within a crush_map */
	__u16 type;      /*!< > 0 bucket type, defined by the caller */
	__u8 alg;        /*!< the item selection ::crush_algorithm */
        /*! @cond INTERNAL */
	__u8 hash;       /* which hash function to use, CRUSH_HASH_* */
	/*! @endcond */
	__u32 weight;    /*!< 16.16 fixed point cumulated children weight */
	__u32 size;      /*!< size of the __items__ array */
        __s32 *items;    /*!< array of children: < 0 are buckets, >= 0 items */
};

/** @ingroup API
 *
 * Replacement weights for each item in a bucket. The size of the
 * array must be exactly the size of the straw2 bucket, just as the
 * item_weights array.
 *
 */
struct crush_weight_set {
  __u32 *weights; /*!< 16.16 fixed point weights in the same order as items */
  __u32 size;     /*!< size of the __weights__ array */
};

/** @ingroup API
 *
 * Replacement weights and ids for a given straw2 bucket, for
 * placement purposes.
 *
 * When crush_do_rule() chooses the Nth item from a straw2 bucket, the
 * replacement weights found at __weight_set[N]__ are used instead of
 * the weights from __item_weights__. If __N__ is greater than
 * __weight_set_positions__, the weights found at __weight_set_positions-1__ are
 * used instead. For instance if __weight_set__ is:
 *
 *    [ [ 0x10000, 0x20000 ],   // position 0
 *      [ 0x20000, 0x40000 ] ]  // position 1
 *
 * choosing the 0th item will use position 0 weights [ 0x10000, 0x20000 ]
 * choosing the 1th item will use position 1 weights [ 0x20000, 0x40000 ]
 * choosing the 2th item will use position 1 weights [ 0x20000, 0x40000 ]
 * etc.
 *
 */
struct crush_choose_arg {
  __s32 *ids;                           /*!< values to use instead of items */
  __u32 ids_size;                       /*!< size of the __ids__ array */
  struct crush_weight_set *weight_set;  /*!< weight replacements for a given position */
  __u32 weight_set_positions;           /*!< size of the __weight_set__ array */
};

/** @ingroup API
 *
 * Replacement weights and ids for each bucket in the crushmap. The
 * __size__ of the __args__ array must be exactly the same as the
 * __map->max_buckets__.
 *
 * The __crush_choose_arg__ at index N will be used when choosing
 * an item from the bucket __map->buckets[N]__ bucket, provided it
 * is a straw2 bucket.
 *
 */
struct crush_choose_arg_map {
  struct crush_choose_arg *args; /*!< replacement for each bucket in the crushmap */
  __u32 size;                    /*!< size of the __args__ array */
};

/** @ingroup API
 * The weight of each item in the bucket when
 * __h.alg__ == ::CRUSH_BUCKET_UNIFORM.
 */
struct crush_bucket_uniform {
       struct crush_bucket h; /*!< generic bucket information */
	__u32 item_weight;  /*!< 16.16 fixed point weight for each item */
};

/** @ingroup API
 * The weight of each item in the bucket when
 * __h.alg__ == ::CRUSH_BUCKET_LIST.
 *
 * The weight of __h.items[i]__ is __item_weights[i]__ for i in
 * [0,__h.size__[. The __sum_weight__[i] is the sum of the __item_weights[j]__
 * for j in [0,i[.
 *
 */
struct crush_bucket_list {
        struct crush_bucket h; /*!< generic bucket information */
	__u32 *item_weights;  /*!< 16.16 fixed point weight for each item */
	__u32 *sum_weights;   /*!< 16.16 fixed point sum of the weights */
};

struct crush_bucket_tree {
	struct crush_bucket h;  /* note: h.size is _tree_ size, not number of
				   actual items */
	__u8 num_nodes;
	__u32 *node_weights;
};

struct crush_bucket_straw {
	struct crush_bucket h;
	__u32 *item_weights;   /* 16-bit fixed point */
	__u32 *straws;         /* 16-bit fixed point */
};

/** @ingroup API
 * The weight of each item in the bucket when
 * __h.alg__ == ::CRUSH_BUCKET_STRAW2.
 *
 * The weight of __h.items[i]__ is __item_weights[i]__ for i in
 * [0,__h.size__].
 */
struct crush_bucket_straw2 {
        struct crush_bucket h; /*!< generic bucket information */
	__u32 *item_weights;   /*!< 16.16 fixed point weight for each item */
};



/** @ingroup API
 *
 * A crush map define a hierarchy of crush_bucket that end with leaves
 * (buckets and leaves are called items) and a set of crush_rule to
 * map an integer to items with the crush_do_rule() function.
 *
 */
struct crush_map {
        /*! An array of crush_bucket pointers of size __max_buckets__.
         * An element of the array may be NULL if the bucket was removed with
         * crush_remove_bucket(). The buckets must be added with crush_add_bucket().
         * The bucket found at __buckets[i]__ must have a crush_bucket.id == -1-i.
         */
	struct crush_bucket **buckets;
        /*! An array of crush_rule pointers of size __max_rules__.
         * An element of the array may be NULL if the rule was removed (there is
         * no API to do so but there may be one in the future). The rules must be added
         * with crush_add_rule().
         */
	struct crush_rule **rules;
        __s32 max_buckets; /*!< the size of __buckets__ */
	__u32 max_rules; /*!< the size of __rules__ */
        /*! The value of the highest item stored in the crush_map + 1
         */
	__s32 max_devices;

	/*! Backward compatibility tunable. It implements a bad solution
         * and must always be set to 0 except for backward compatibility
         * purposes
         */
	__u32 choose_local_tries;
	/*! Backward compatibility tunable. It implements a bad solution
         * and must always be set to 0 except for backward compatibility
         * purposes
         */
	__u32 choose_local_fallback_tries;
	/*! Tunable. The default value when the CHOOSE_TRIES or
         * CHOOSELEAF_TRIES steps are omitted in a rule. See the
         * documentation for crush_rule_set_step() for more
         * information
         */
	__u32 choose_total_tries;
	/*! Backward compatibility tunable. It should always be set
         *  to 1 except for backward compatibility. Implemented in 2012
         *  it was generalized late 2013 and is mostly unused except
         *  in one border case, reason why it must be set to 1.
         *
         *  Attempt chooseleaf inner descent once for firstn mode; on
         *  reject retry outer descent.  Note that this does *not*
         *  apply to a collision: in that case we will retry as we
         *  used to.
         */
	__u32 chooseleaf_descend_once;
	/*! Backward compatibility tunable. It is a fix for bad
         *  mappings implemented in 2014 at
         *  https://github.com/ceph/ceph/pull/1185. It should always
         *  be set to 1 except for backward compatibility.
         *
         *  If non-zero, feed r into chooseleaf, bit-shifted right by
	 *  (r-1) bits.  a value of 1 is best for new clusters.  for
	 *  legacy clusters that want to limit reshuffling, a value of
	 *  3 or 4 will make the mappings line up a bit better with
	 *  previous mappings.
         */
	__u8 chooseleaf_vary_r;

	/*! Backward compatibility tunable. It is an improvement that
         *  avoids unnecessary mapping changes, implemented at
         *  https://github.com/ceph/ceph/pull/6572 and explained in
         *  this post: "chooseleaf may cause some unnecessary pg
         *  migrations" in October 2015
         *  https://www.mail-archive.com/ceph-devel@vger.kernel.org/msg26075.html
         *  It should always be set to 1 except for backward compatibility.
         */
	__u8 chooseleaf_stable;

	/*! Sets total descents for MSR rules */
	__u32 msr_descents;

	/*! Sets local collision retries for MSR rules */
	__u32 msr_collision_tries;

        /*! @cond INTERNAL */
	/* This value is calculated after decode or construction by
	   the builder. It is exposed here (rather than having a
	   'build CRUSH working space' function) so that callers can
	   reserve a static buffer, allocate space on the stack, or
	   otherwise avoid calling into the heap allocator if they
	   want to. The size of the working space depends on the map,
	   while the size of the scratch vector passed to the mapper
	   depends on the size of the desired result set.

	   Nothing stops the caller from allocating both in one swell
	   foop and passing in two points, though. */
	size_t working_size;

#ifndef __KERNEL__
	/*! @endcond */
	/*! Backward compatibility tunable. It is a fix for the straw
         *  scaler values for the straw algorithm which is deprecated
         *  (straw2 replaces it) implemented at
         *  https://github.com/ceph/ceph/pull/3057. It should always
         *  be set to 1 except for backward compatibility.
         *
	 */
	__u8 straw_calc_version;

        /*! @cond INTERNAL */
	/*
	 * allowed bucket algs is a bitmask, here the bit positions
	 * are CRUSH_BUCKET_*.  note that these are *bits* and
	 * CRUSH_BUCKET_* values are not, so we need to or together (1
	 * << CRUSH_BUCKET_WHATEVER).  The 0th bit is not used to
	 * minimize confusion (bucket type values start at 1).
	 */
	__u32 allowed_bucket_algs;

	__u32 *choose_tries;
#endif
	/*! @endcond */
};


/* crush.c */
/** @ingroup API
 *
 * Return the 16.16 fixed point weight of the item at __pos__ (zero
 * based index) within the bucket __b__. If __pos__ is negative or
 * greater or equal to the number of items in the bucket, return 0.
 *
 * @param b the bucket containing items
 * @param pos the zero based index of the item
 *
 * @returns the 16.16 fixed point item weight
 */
extern int crush_get_bucket_item_weight(const struct crush_bucket *b, int pos);
extern void crush_destroy_bucket_uniform(struct crush_bucket_uniform *b);
extern void crush_destroy_bucket_list(struct crush_bucket_list *b);
extern void crush_destroy_bucket_tree(struct crush_bucket_tree *b);
extern void crush_destroy_bucket_straw(struct crush_bucket_straw *b);
extern void crush_destroy_bucket_straw2(struct crush_bucket_straw2 *b);
/** @ingroup API
 *
 * Deallocate a bucket created via crush_add_bucket().
 *
 * @param b the bucket to deallocate
 */
extern void crush_destroy_bucket(struct crush_bucket *b);
/** @ingroup API
 *
 * Deallocate a rule created via crush_add_rule().
 *
 * @param r the rule to deallocate
 */
extern void crush_destroy_rule(struct crush_rule *r);
/** @ingroup API
 *
 * Deallocate the __map__, previously allocated with crush_create.
 *
 * @param map the crush map
 */
extern void crush_destroy(struct crush_map *map);

static inline int crush_calc_tree_node(int i)
{
	return ((i+1) << 1)-1;
}

static inline const char *crush_alg_name(int alg)
{
	switch (alg) {
	case CRUSH_BUCKET_UNIFORM:
		return "uniform";
	case CRUSH_BUCKET_LIST:
		return "list";
	case CRUSH_BUCKET_TREE:
		return "tree";
	case CRUSH_BUCKET_STRAW:
		return "straw";
	case CRUSH_BUCKET_STRAW2:
		return "straw2";
	default:
		return "unknown";
	}
}

/* ---------------------------------------------------------------------
			       Private
   --------------------------------------------------------------------- */

/* These data structures are private to the CRUSH implementation. They
   are exposed in this header file because builder needs their
   definitions to calculate the total working size.

   Moving this out of the crush map allow us to treat the CRUSH map as
   immutable within the mapper and removes the requirement for a CRUSH
   map lock. */

struct crush_work_bucket {
	__u32 perm_x; /* @x for which *perm is defined */
	__u32 perm_n; /* num elements of *perm that are permuted/defined */
	__u32 *perm;  /* Permutation of the bucket's items */
} __attribute__ ((packed));

struct crush_work {
	struct crush_work_bucket **work; /* Per-bucket working store */
};

#endif
