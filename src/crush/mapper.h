#ifndef CEPH_CRUSH_MAPPER_H
#define CEPH_CRUSH_MAPPER_H

/*
 * CRUSH functions for find rules and then mapping an input to an
 * output set.
 *
 * LGPL-2.1 or LGPL-3.0
 */

#include "crush.h"

/** @ingroup API
 *
 * Map __x__ to __result_max__ items and store them in the __result__
 * array. The mapping is done by following each step of the rule
 * __ruleno__. See crush_make_rule(), crush_rule_set_step() and
 * crush_add_rule() for more information on how the rules are created,
 * populated and added to the crush __map__.
 *
 * The return value is the the number of items in the __result__
 * array. If the caller asked for __result_max__ items and the return
 * value is X where X < __result_max__, the content of __result[0,X[__
 * is defined but the content of __result[X,result_max[__ is
 * undefined. For example:
 *
 *     crush_do_rule(map, ruleno=1, x=1, result, result_max=3,...) == 1
 *     result[0] is set
 *     result[1] is undefined
 *     result[2] is undefined
 *
 * An entry in the __result__ array is either an item in the crush
 * __map__ or ::CRUSH_ITEM_NONE if no item was found. For example:
 *
 *     crush_do_rule(map, ruleno=1, x=1, result, result_max=4,...) == 2
 *     result[0] is CRUSH_ITEM_NONE
 *     result[1] is item number 5
 *     result[2] is undefined
 *     result[3] is undefined
 *
 * The __weight__ array contains the probabilities that a leaf is
 * ignored even if it is selected. It is a 16.16 fixed point
 * number in the range [0x00000,0x10000]. The lower the value, the
 * more often the leaf is ignored. For instance:
 *
 * - weight[leaf] == 0x00000 == 0.0 always ignore
 * - weight[leaf] == 0x10000 == 1.0 never ignore
 * - weight[leaf] == 0x08000 == 0.5 ignore 50% of the time
 * - weight[leaf] == 0x04000 == 0.25 ignore 75% of the time
 * - etc.
 *
 * During mapping, each leaf is checked against the __weight__ array,
 * using the leaf as an index. If there is no entry in __weight__ for
 * the leaf, it is ignored. If there is an entry, the leaf will be
 * ignored some of the time, depending on the probability.
 *
 * The __cwin__ argument must be set as follows:
 *
 *         char __cwin__[crush_work_size(__map__, __result_max__)];
 *         crush_init_workspace(__map__, __cwin__);
 *
 * There are two CRUSH variants implemented.  Rules of type
 * - CRUSH_RULE_TYPE_REPLICATED
 * - CRUSH_RULE_TYPE_ERASURE
 * use crush_do_rule_no_retry.  The crush descent algorithm implemented
 * there cannot retry prior steps upon hitting an out osd, so such rules
 * rely on the chooseleaf variant to implement failure domains and have
 * important limitations when mapping multiple OSDs per failure domain.
 * See crush_msr_do_rule in mapper.c for a more detailed explanation.
 *
 * Rules of type
 * - CRUSH_RULE_TYPE_MSR_FIRSTN
 * - CRUSH_RULE_TYPE_MSR_INDEP
 * use crush_msr_do_rule, which retries the full descent when it hits an
 * out OSD.  This extra flexibility allows it to more effectively map multiple
 * OSDs per failure domain.  See the comment on crush_msr_do_rule in mapper.c
 * for more details.
 *
 * @param map the crush_map
 * @param ruleno a positive integer < __CRUSH_MAX_RULES__
 * @param x the value to map to __result_max__ items
 * @param result an array of items of size __result_max__
 * @param result_max the size of the __result__ array
 * @param weights an array of weights of size __weight_max__
 * @param weight_max the size of the __weights__ array
 * @param cwin must be an char array initialized by crush_init_workspace
 * @param choose_args weights and ids for each known bucket
 *
 * @return 0 on error or the size of __result__ on success
 */
extern int crush_do_rule(const struct crush_map *map,
			 int ruleno,
			 int x, int *result, int result_max,
			 const __u32 *weights, int weight_max,
			 void *cwin, const struct crush_choose_arg *choose_args);

/* Returns enough workspace for any crush rule within map to generate
   result_max outputs. The caller can then allocate this much on its own,
   either on the stack, in a per-thread long-lived buffer, or however it likes.*/
extern size_t crush_work_size(const struct crush_map *map,
			      int result_max);

extern void crush_init_workspace(const struct crush_map *m, void *v);

#endif
