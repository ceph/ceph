#ifndef _FS_CEPH_MONMAP_H
#define _FS_CEPH_MONMAP_H

#include <linux/ceph_fs.h>

/*
 * monitor map
 */
struct ceph_monmap {
  __u64 epoch;
  __u32 num_mon;
  struct ceph_entity_inst *mon_inst;
};

extern int ceph_monmap_decode(struct ceph_monmap *m, void **p, void *end);

#endif
