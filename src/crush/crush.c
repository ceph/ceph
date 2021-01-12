#ifdef __KERNEL__
# include <linux/slab.h>
# include <linux/crush/crush.h>
#else
# include "crush_compat.h"
# include "crush.h"
#endif

const char *crush_bucket_alg_name(int alg)
{
	switch (alg) {
	case CRUSH_BUCKET_UNIFORM: return "uniform";
	case CRUSH_BUCKET_LIST: return "list";
	case CRUSH_BUCKET_TREE: return "tree";
	case CRUSH_BUCKET_STRAW: return "straw";
	case CRUSH_BUCKET_STRAW2: return "straw2";
	default: return "unknown";
	}
}

/**
 * crush_get_bucket_item_weight - Get weight of an item in given bucket
 * @b: bucket pointer
 * @p: item index in bucket
 */
int crush_get_bucket_item_weight(const struct crush_bucket *b, int p)
{
	if ((__u32)p >= b->size)
		return 0;

	switch (b->alg) {
	case CRUSH_BUCKET_UNIFORM:
		return ((struct crush_bucket_uniform *)b)->item_weight;
	case CRUSH_BUCKET_LIST:
		return ((struct crush_bucket_list *)b)->item_weights[p];
	case CRUSH_BUCKET_TREE:
		return ((struct crush_bucket_tree *)b)->node_weights[crush_calc_tree_node(p)];
	case CRUSH_BUCKET_STRAW:
		return ((struct crush_bucket_straw *)b)->item_weights[p];
	case CRUSH_BUCKET_STRAW2:
		return ((struct crush_bucket_straw2 *)b)->item_weights[p];
	}
	return 0;
}

int crush_get_bucket_item_performance_range_set_num(const struct crush_bucket *b, int p)
{
	if ((__u32)p >= b->size)
		return 0;

	switch (b->alg) {
	case CRUSH_BUCKET_STRAW2:
		return ((struct crush_bucket_straw2 *)b)->item_performance_range_sets_num[p];
	}
	return 0;
}

__u32* crush_get_bucket_item_performance_range_set(const struct crush_bucket *b, int p)
{
	__u32 *ret;
	if ((__u32)p >= b->size)
		return 0;

	switch (b->alg) {
	case CRUSH_BUCKET_STRAW2:
		/* TODO: we did not free this */
		ret = malloc(sizeof(__u32)*((struct crush_bucket_straw2 *)b)->item_performance_range_sets_num[p]);
		memcpy(ret, ((struct crush_bucket_straw2 *)b)->item_performance_range_sets[p], \
				sizeof(__u32)*((struct crush_bucket_straw2 *)b)->item_performance_range_sets_num[p]);
		return ret;
	}
	return 0;
}

void crush_destroy_bucket_uniform(struct crush_bucket_uniform *b)
{
	kfree(b->h.items);
	kfree(b);
}

void crush_destroy_bucket_list(struct crush_bucket_list *b)
{
	kfree(b->item_weights);
	kfree(b->sum_weights);
	kfree(b->h.items);
	kfree(b);
}

void crush_destroy_bucket_tree(struct crush_bucket_tree *b)
{
	kfree(b->h.items);
	kfree(b->node_weights);
	kfree(b);
}

void crush_destroy_bucket_straw(struct crush_bucket_straw *b)
{
	kfree(b->straws);
	kfree(b->item_weights);
	kfree(b->h.items);
	kfree(b);
}

void crush_destroy_bucket_straw2(struct crush_bucket_straw2 *b)
{
	int i;

	for (i=0; i<b->h.size; i++) {
		if (b->item_performance_range_sets[i] != NULL)
			kfree(b->item_performance_range_sets[i]);
	}
	if (b->h.size != 0) {
		kfree(b->item_performance_range_sets);
		kfree(b->item_performance_range_sets_num);
	}
	kfree(b->item_weights);
	kfree(b->h.items);
	if (b->h.performance_range_set_num != 0)
		kfree(b->h.performance_range_set);
	kfree(b);
}

void crush_destroy_bucket(struct crush_bucket *b)
{
	switch (b->alg) {
	case CRUSH_BUCKET_UNIFORM:
		crush_destroy_bucket_uniform((struct crush_bucket_uniform *)b);
		break;
	case CRUSH_BUCKET_LIST:
		crush_destroy_bucket_list((struct crush_bucket_list *)b);
		break;
	case CRUSH_BUCKET_TREE:
		crush_destroy_bucket_tree((struct crush_bucket_tree *)b);
		break;
	case CRUSH_BUCKET_STRAW:
		crush_destroy_bucket_straw((struct crush_bucket_straw *)b);
		break;
	case CRUSH_BUCKET_STRAW2:
		crush_destroy_bucket_straw2((struct crush_bucket_straw2 *)b);
		break;
	}
}

/**
 * crush_destroy - Destroy a crush_map
 * @map: crush_map pointer
 */
void crush_destroy(struct crush_map *map)
{
	/* buckets */
	if (map->buckets) {
		__s32 b;
		for (b = 0; b < map->max_buckets; b++) {
			if (map->buckets[b] == NULL)
				continue;
			crush_destroy_bucket(map->buckets[b]);
		}
		kfree(map->buckets);
	}

	/* rules */
	if (map->rules) {
		__u32 b;
		for (b = 0; b < map->max_rules; b++)
			crush_destroy_rule(map->rules[b]);
		kfree(map->rules);
	}

#ifndef __KERNEL__
	kfree(map->choose_tries);
#endif
	kfree(map);
}

void crush_destroy_rule(struct crush_rule *rule)
{
	kfree(rule);
}

void union_performance_range_set(__u32 **target_performance_range_set, __u32 *target_performance_range_set_num, \
				   const __u32 *performance_range_set, int performance_range_set_num) {
	__u32 *old_target = *target_performance_range_set;
	__u32 old_target_num = *target_performance_range_set_num;
	__u32 tmp[old_target_num + performance_range_set_num];
	int i, j, k;
	int new_target_num;

	for (i=0; i<old_target_num; i++)
		tmp[i] = old_target[i];
	for (j=0; j<performance_range_set_num; j++) {
		/* check whether this performance range is repeated */
		for (k=0; k<i; k++) {
			if ((performance_range_set[j]/performance_threshold) * performance_threshold == tmp[k])
				break;
		}
		/* this performance range is not repeated */
		if (k == i || i == 0) {
			tmp[i++] = (performance_range_set[j]/performance_threshold) * performance_threshold;
		}
	}
	/* sort tmp from small to big */
	new_target_num = i;
	for (i=0; i<new_target_num; i++) {
		for (j=0; j<new_target_num-1; j++) {
			if (tmp[j] > tmp[j+1]) {
				int tmp_a = tmp[j];
				tmp[j] = tmp[j+1];
				tmp[j+1] = tmp_a;
			}
		}
	}
	/* write back */
	*target_performance_range_set_num = new_target_num;
	if (new_target_num == 0)
		*target_performance_range_set = NULL;
	else
		*target_performance_range_set = malloc(sizeof(__u32)*new_target_num);
	for (i=0; i<new_target_num; i++)
		(*target_performance_range_set)[i] = tmp[i];

	if (old_target_num != 0)
		free(old_target);
}

void subtract_performance_range_set(__u32 **target_performance_range_set, __u32 *target_performance_range_set_num, \
				   const __u32 *performance_range_set, int performance_range_set_num) {
	__u32 *old_target = *target_performance_range_set;
	__u32 old_target_num = *target_performance_range_set_num;
	__u32 is_repeated[old_target_num];
	__u32 repeated_cnt = 0;
	int i, j;
	int new_target_num;

	for (i=0; i<old_target_num; i++)
		is_repeated[i] = 0;
	for (i=0; i<performance_range_set_num; i++) {
		/* check whether this performance range is repeated */
		for (j=0; j<old_target_num; j++) {
			if ((performance_range_set[i]/performance_threshold) * performance_threshold == old_target[j]) {
				repeated_cnt++;
				is_repeated[j] = 1;
				break;
			}
		}
	}
	new_target_num = old_target_num - repeated_cnt;
	/* write back */
	*target_performance_range_set_num = new_target_num;
	if (new_target_num == 0)
		*target_performance_range_set = NULL;
	else
		*target_performance_range_set = malloc(sizeof(__u32)*new_target_num);
	j = 0;
	for (i=0; i<old_target_num; i++)
		if (is_repeated[i] == 0)
			(*target_performance_range_set)[j++] = old_target[i];

	if (old_target_num != 0)
		free(old_target);
}

/* >0 means same, or not same */
int is_same_performance_range_set(__u32 *performance_range_set1, int performance_range_set1_num, \
				   const __u32 *performance_range_set2, int performance_range_set2_num) {
	int i;

	if (performance_range_set1_num != performance_range_set2_num)
		return 0;
	for (i=0; i<performance_range_set1_num; i++)
		if (performance_range_set1[i] != performance_range_set2[i])
			return 0;
	return 1;	
}