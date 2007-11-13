#ifndef _FS_CEPH_MON_CLIENT_H
#define _FS_CEPH_MON_CLIENT_H

#include "monmap.h"

struct ceph_mount_args;

struct ceph_mon_client {
	int last_mon;  /* last monitor i contacted */
	struct ceph_monmap monmap;
};

extern void ceph_monc_init(struct ceph_mon_client *monc);
extern void ceph_monc_handle_monmap(struct ceph_mon_client *monc, struct ceph_message *m);

extern void ceph_monc_request_mdsmap(struct ceph_mon_client *monc, __u64 have);
extern void ceph_monc_request_osdmap(struct ceph_mon_client *monc, __u64 have);
extern void ceph_monc_request_umount(struct ceph_mon_client *monc);
extern void ceph_monc_report_failure(struct ceph_mon_client *monc, struct ceph_entity_inst *who);

#endif
