#ifndef _CRUSH_CRUSH_H
#define _CRUSH_CRUSH_H

#ifdef __cplusplus
extern "C" {
#endif

#include "types.h"
#include "buckets.h"

enum {
	CRUSH_RULE_TAKE,
	CRUSH_RULE_CHOOSE_FIRSTN,
	CRUSH_RULE_CHOOSE_INDEP,
	CRUSH_RULE_EMIT
};

#define CRUSH_MAX_DEPTH 10
#define CRUSH_MAX_SET   10

struct crush_rule_step {
	__u32 op;
	__s32 arg1;
	__s32 arg2;
};

struct crush_rule {
	__u32 len;
	struct crush_rule_step *steps;
};

struct crush_map {
	struct crush_bucket **buckets;
	struct crush_rule **rules;
	
	/* parent pointers */
	__u32 *bucket_parents;
	__u32 *device_parents;
	
	/* offload
	 * size max_devices, values 0...0xffff
	 *        0 == normal
	 *  0x10000 == 100% offload (i.e. failed)
	 */
	__u32 *device_offload;   
	
	__u32 max_buckets;
	__u32 max_rules;
	__u32 max_devices;
};


extern void crush_index(struct crush_map *map); 

extern int crush_do_rule(struct crush_map *map,
			 int ruleno,
			 int x, int *result, int result_max,
			 int forcefeed); /* -1 for none */


/*extern int crush_decode(struct crush_map *map, struct ceph_bufferlist *bl);*/

#ifdef __cplusplus
}
#endif

#endif
