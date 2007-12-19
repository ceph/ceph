#ifndef _FS_CEPH_MON_CLIENT_H
#define _FS_CEPH_MON_CLIENT_H

#include "messenger.h"

struct ceph_mount_args;

struct ceph_monmap {
	ceph_epoch_t epoch;
	struct ceph_fsid fsid;
	__u32 num_mon;
	struct ceph_entity_inst *mon_inst;
};


struct ceph_mon_client {
	int last_mon;  /* last monitor i contacted */
	struct ceph_monmap monmap;
};

extern int ceph_monmap_decode(struct ceph_monmap *m, void *p, void *end);

extern void ceph_monc_init(struct ceph_mon_client *monc);

extern void ceph_monc_request_mdsmap(struct ceph_mon_client *monc, __u64 have);
extern void ceph_monc_request_osdmap(struct ceph_mon_client *monc, __u64 have);
extern void ceph_monc_request_umount(struct ceph_mon_client *monc);
extern void ceph_monc_report_failure(struct ceph_mon_client *monc, struct ceph_entity_inst *who);

#endif
