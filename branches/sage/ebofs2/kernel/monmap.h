#ifndef _FS_CEPH_MONMAP_H
#define _FS_CEPH_MONMAP_H

#include <linux/uio.h>

/*
 * monitor map
 */
struct ceph_monmap {
  __u64 m_epoch;
  __u32 m_num_mon;
  __u32 m_last_mon;
  struct ceph_entity_inst m_mon_inst;
};

extern int ceph_monmap_pick_mon(struct ceph_monmap *m);
extern int ceph_monmap_decode(struct ceph_monmap *m, struct kvec *v);

#endif
