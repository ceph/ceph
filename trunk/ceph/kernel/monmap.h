#ifndef _FS_CEPH_MONMAP_H
#define _FS_CEPH_MONMAP_H

/*
 * monitor map
 */
struct ceph_monmap {
  __u64 m_epoch;
  __u32 m_num_mon;
  __u32 m_last_mon;
  struct ceph_entity_inst m_mon_inst;
};

extern int ceph_monmap_pick_mon(ceph_monmap *m);
extern int ceph_monmap_decode(ceph_monmap *m, iovec *v);

#endif
