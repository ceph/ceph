#ifndef CEPH_CRUSH_BUILDER_H
#define CEPH_CRUSH_BUILDER_H

#include "include/int_types.h"

struct crush_bucket;
struct crush_choose_arg;
struct crush_map;
struct crush_rule;

/** @ingroup API
 *
 * Allocate a crush_map with __malloc(3)__ and initialize it. The
 * caller is responsible for deallocating the crush_map with
 * crush_destroy().
 *
 * The content of the allocated crush_map is set with
 * set_optimal_crush_map(). The caller is responsible for setting each
 * tunable in the __crush_map__ for backward compatibility or mapping
 * stability.
 *
 * @returns a pointer to the newly created crush_map or NULL
 */
extern struct crush_map *crush_create();
/** @ingroup API
 *
 * Analyze the content of __map__ and set the internal values required
 * before it can be used to map values with crush_do_rule(). The caller
 * must make sure it is run before crush_do_rule() and after any
 * function that modifies the __map__ (crush_add_bucket(), etc.).
 *
 * @param map the crush_map
 */
extern void crush_finalize(struct crush_map *map);

/* rules */
/** @ingroup API
 *
 * Allocate an empty crush_rule structure large enough to store __len__ steps.
 * Steps can be added to a rule via crush_rule_set_step(). The __ruleset__
 * is a user defined integer, not used by __libcrush__ and stored in
 * the allocated rule at __rule->mask.ruleset__.
 *
 * The rule is designed to allow crush_do_rule() to get at least __minsize__ items
 * and at most __maxsize__ items.
 *
 * The __type__ is defined by the caller and will be used by
 * crush_find_rule() when looking for a rule and by
 * __CRUSH_RULE_CHOOSE*__ steps when looking for items.
 *
 * The caller is responsible for deallocating the returned pointer via
 * crush_destroy_rule().
 *
 * If __malloc(3)__ fails, return NULL.
 *
 * @param len number of steps in the rule
 * @param ruleset user defined value
 * @param type user defined value
 * @param minsize minimum number of items the rule can map
 * @param maxsize maximum number of items the rule can map
 *
 * @returns a pointer to the newly created rule or NULL
 */
extern struct crush_rule *crush_make_rule(int len, int ruleset, int type, int minsize, int maxsize);
/** @ingroup API
 *
 * Set the __pos__ step of the __rule__ to an operand and up to two arguments.
 * The value of the operand __op__ determines if the arguments are used and how:
 *
 * - __CRUSH_RULE_NOOP__ do nothing.
 * - __CRUSH_RULE_TAKE__ select the __arg1__ item
 * - __CRUSH_RULE_EMIT__ append the selection to the results and clear
 *     the selection
 *
 * - __CRUSH_RULE_CHOOSE_FIRSTN__ and __CRUSH_RULE_CHOOSE_INDEP__
 *     recursively explore each bucket currently selected, looking for
 *     __arg1__ items of type __arg2__ and select them.
 * - __CRUSH_RULE_CHOOSELEAF_FIRSTN__ and __CRUSH_RULE_CHOOSELEAF_INDEP__
 *     recursively explore each bucket currently selected, looking for
 *     __arg1__ leaves within all the buckets of type __arg2__ and
 *     select them.
 *
 * In all __CHOOSE__ steps, if __arg1__ is less than or equal to zero,
 * the number of items to select is equal to the __max_result__ argument
 * of crush_do_rule() minus __arg1__. It is common to set __arg1__ to zero
 * to select as many items as requested by __max_result__.
 *
 * - __CRUSH_RULE_SET_CHOOSE_TRIES__ and __CRUSH_RULE_SET_CHOOSELEAF_TRIES__
 *
 *   The CHOOSE_FIRSTN and CHOOSE_INDEP rule step look for buckets of
 *   a given type, randomly selecting them. If they are unlucky and
 *   find the same bucket twice, they will try N+1 times (N being the
 *   value of the choose_total_tries tunable). If there is a previous
 *   SET_CHOOSE_TRIES step in the same rule, it will try C times
 *   instead (C being the value of the argument of the
 *   SET_CHOOSE_TRIES step).
 *
 *   Note: the __choose_total_tries__ tunable defined in crush_map is
 *   the number of retry, not the number of tries. The number of tries
 *   is the number of retry+1. The SET_CHOOSE_TRIES rule step sets the
 *   number of tries and does not need the + 1. This confusing
 *   difference is inherited from an off-by-one bug from years ago.
 *
 *   The CHOOSELEAF_FIRSTN and CHOOSELEAF_INDEP rule step do the same
 *   as CHOOSE_FIRSTN and CHOOSE_INDEP but also recursively explore
 *   each bucket found, looking for a single device. The same device
 *   may be found in two different buckets because the crush map is
 *   not a strict hierarchy, it is a DAG. When such a collision
 *   happens, they will try again. The number of times they try to
 *   find a non colliding device is:
 *
 *   - If FIRSTN and there is no previous SET_CHOOSELEAF_TRIES rule
 *     step: try N + 1 times (N being the value of the
 *     __choose_total_tries__ tunable defined in crush_map)
 *
 *   - If FIRSTN and there is a previous SET_CHOOSELEAF_TRIES rule
 *     step: try P times (P being the value of the argument of the
 *     SET_CHOOSELEAF_TRIES rule step)
 *
 *   - If INDEP and there is no previous SET_CHOOSELEAF_TRIES rule
 *     step: try 1 time.
 *
 *   - If INDEP and there is a previous SET_CHOOSELEAF_TRIES rule step: try
 *     P times (P being the value of the argument of the SET_CHOOSELEAF_TRIES
 *     rule step)
 *
 * @param rule the rule in which the step is inserted
 * @param pos the zero based step index
 * @param op one of __CRUSH_RULE_NOOP__, __CRUSH_RULE_TAKE__, __CRUSH_RULE_CHOOSE_FIRSTN__, __CRUSH_RULE_CHOOSE_INDEP__, __CRUSH_RULE_CHOOSELEAF_FIRSTN__, __CRUSH_RULE_CHOOSELEAF_INDEP__, __CRUSH_RULE_SET_CHOOSE_TRIES__, __CRUSH_RULE_SET_CHOOSELEAF_TRIES__ or __CRUSH_RULE_EMIT__
 * @param arg1 first argument for __op__
 * @param arg2 second argument for __op__
 */
extern void crush_rule_set_step(struct crush_rule *rule, int pos, int op, int arg1, int arg2);
/** @ingroup API
 *
 * Add the __rule__ into the crush __map__ and assign it the
 * __ruleno__ unique identifier. If __ruleno__ is -1, the function will
 * assign the lowest available identifier. The __ruleno__ value must be
 * a positive integer lower than __CRUSH_MAX_RULES__.
 *
 * - return -ENOSPC if the rule identifier is >= __CRUSH_MAX_RULES__
 * - return -ENOMEM if __realloc(3)__ fails to expand the array of
 *   rules in the __map__
 *
 * @param map the crush_map
 * @param rule the rule to add to the __map__
 * @param ruleno a positive integer < __CRUSH_MAX_RULES__ or -1
 *
 * @returns the rule unique identifier on success, < 0 on error
 */
extern int crush_add_rule(struct crush_map *map, struct crush_rule *rule, int ruleno);

/* buckets */
extern int crush_get_next_bucket_id(struct crush_map *map);
/** @ingroup API
 *
 * Add __bucket__ into the crush __map__ and assign it the
 * __bucketno__ unique identifier. If __bucketno__ is 0, the function
 * will assign the lowest available identifier.  The bucket identifier
 * must be a negative integer. The bucket identifier is returned via
 * __idout__.
 *
 * - return -ENOMEM if __realloc(3)__ fails to expand the array of
 *   buckets in the __map__
 * - return -EEXIST if the __bucketno__ identifier is already assigned
 *   to another bucket.
 *
 * @param[in] map the crush_map
 * @param[in] bucketno the bucket unique identifer or 0
 * @param[in] bucket the bucket to add to the __map__
 * @param[out] idout a pointer to the bucket identifier
 *
 * @returns 0 on success, < 0 on error
 */
extern int crush_add_bucket(struct crush_map *map,
			    int bucketno,
			    struct crush_bucket *bucket, int *idout);
/** @ingroup API
 *
 * Allocate a crush_bucket with __malloc(3)__ and initialize it. The
 * content of the bucket is filled with __size__ items from
 * __items__. The item selection is set to use __alg__ which is one of
 * ::CRUSH_BUCKET_UNIFORM , ::CRUSH_BUCKET_LIST or
 * ::CRUSH_BUCKET_STRAW2. The initial __items__ are assigned a
 * weight from the __weights__ array, depending on the value of
 * __alg__. If __alg__ is ::CRUSH_BUCKET_UNIFORM, all items are set
 * to have a weight equal to __weights[0]__, otherwise the weight of
 * __items[x]__ is set to be the value of __weights[x]__.
 *
 * The caller is responsible for deallocating the returned pointer via
 * crush_destroy_bucket().
 *
 * @param map __unused__
 * @param alg algorithm for item selection
 * @param hash always set to CRUSH_HASH_RJENKINS1
 * @param type user defined bucket type
 * @param size of the __items__ array
 * @param items array of __size__ items
 * @param weights the weight of each item in __items__, depending on __alg__
 *
 * @returns a pointer to the newly created bucket or NULL
 */
struct crush_bucket *crush_make_bucket(struct crush_map *map, int alg, int hash, int type, int size, int *items, int *weights);
extern struct crush_choose_arg *crush_make_choose_args(struct crush_map *map, int num_positions);
extern void crush_destroy_choose_args(struct crush_choose_arg *args);
/** @ingroup API
 *
 * Add __item__ to __bucket__ with __weight__. The weight of the new
 * item is added to the weight of the bucket so that it reflects
 * the total weight of all items.
 *
 * If __bucket->alg__ is ::CRUSH_BUCKET_UNIFORM, the value of __weight__ must be equal to
 * __(struct crush_bucket_uniform *)bucket->item_weight__.
 *
 * - return -ENOMEM if the __bucket__ cannot be resized with __realloc(3)__.
 * - return -ERANGE if adding __weight__ to the weight of the bucket overflows.
 * - return -EINVAL if __bucket->alg__ is ::CRUSH_BUCKET_UNIFORM and
 *   the __weight__ is not equal to __(struct crush_bucket_uniform *)bucket->item_weight__.
 * - return -1 if the value of __bucket->alg__ is unknown.
 *
 * @returns 0 on success, < 0 on error
 */
extern int crush_bucket_add_item(struct crush_map *map, struct crush_bucket *bucket, int item, int weight);
/** @ingroup API
 *
 * If __bucket->alg__ is ::CRUSH_BUCKET_UNIFORM,
 * __(struct crush_bucket_uniform *)bucket->item_weight__ is set to __weight__ and the
 * weight of the bucket is set to be the number of items in the bucket times the weight.
 * The return value is the difference between the new bucket weight and the former
 * bucket weight. The __item__ argument is ignored.
 *
 * If __bucket->alg__ is different from ::CRUSH_BUCKET_UNIFORM,
 * set the  __weight__ of  __item__ in __bucket__. The former weight of the
 * item is subtracted from the weight of the bucket and the new weight is added.
 * The return value is the difference between the new item weight and the former
 * item weight.
 *
 * @returns the difference between the new weight and the former weight
 */
extern int crush_bucket_adjust_item_weight(struct crush_map *map, struct crush_bucket *bucket, int item, int weight);
/** @ingroup API
 *
 * Recursively update the weight of __bucket__ and its children, deep
 * first. The __bucket__ weight is set to the sum of the weight of the
 * items it contains.
 *
 * - return -ERANGE if the sum of the weight of the items in __bucket__ overflows.
 * - return -1 if the value of __bucket->alg__ is unknown.
 *
 * @param map a crush_map containing __bucket__
 * @param bucket the root of the tree to reweight
 * @returns 0 on success, < 0 on error
 */
extern int crush_reweight_bucket(struct crush_map *map, struct crush_bucket *bucket);
/** @ingroup API
 *
 * Remove __bucket__ from __map__ and deallocate it via crush_destroy_bucket().
 * __assert(3)__ that __bucket__ is in __map__. The caller is responsible for
 * making sure the bucket is not the child of any other bucket in the __map__.
 *
 * @param map a crush_map containing __bucket__
 * @param bucket the bucket to remove from __map__
 * @returns 0
 */
extern int crush_remove_bucket(struct crush_map *map, struct crush_bucket *bucket);
/** @ingroup API
 *
 * Remove __item__ from __bucket__ and subtract the item weight from
 * the bucket weight. If the weight of the item is greater than the
 * weight of the bucket, silentely set the bucket weight to zero.
 *
 * - return -ENOMEM if the __bucket__ cannot be sized down with __realloc(3)__.
 * - return -1 if the value of __bucket->alg__ is unknown.
 *
 * @param map __unused__
 * @param bucket the bucket from which __item__ is removed
 * @param item the item to remove from __bucket__
 * @returns 0 on success, < 0 on error
 */
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

/** @ingroup API
 *
 * Set the __map__ tunables to implement the most ancient behavior,
 * for backward compatibility purposes only.
 *
 * - choose_local_tries == 2
 * - choose_local_fallback_tries == 5
 * - choose_total_tries == 19
 * - chooseleaf_descend_once == 0
 * - chooseleaf_vary_r == 0
 * - straw_calc_version == 0
 * - chooseleaf_stable = 0
 *
 * See the __crush_map__ documentation for more information about
 * each tunable.
 *
 * @param map a crush_map
 */
extern void set_legacy_crush_map(struct crush_map *map);
/** @ingroup API
 *
 * Set the __map__ tunables to implement the optimal behavior. These
 * are the values set by crush_create(). It does not guarantee a
 * stable mapping after an upgrade.
 *
 * For instance when a bug is fixed it may significantly change the
 * mapping. In that case a new tunable (say tunable_new) is added so
 * the caller can control when the bug fix is activated. The
 * set_optimal_crush_map() function will always set all tunables,
 * including tunable_new, to fix all bugs even if it means changing
 * the mapping. If the caller needs fine grained control on the
 * tunables to upgrade to a new version without changing the mapping,
 * it needs to set the __crush_map__ tunables individually.
 *
 * See the __crush_map__ documentation for more information about
 * each tunable.
 *
 * @param map a crush_map
 */
extern void set_optimal_crush_map(struct crush_map *map);

#endif
