#ifndef _FS_CEPH_MON_CLIENT_H
#define _FS_CEPH_MON_CLIENT_H


struct ceph_mon_client {
	int last_mon;  /* last monitor i contacted */
	
};


extern void ceph_monc_request_mdsmap(struct ceph_mon_client *monc, __u64 have);
extern void ceph_monc_request_osdmap(struct ceph_mon_client *monc, __u64 have);
extern void ceph_monc_request_mount(struct ceph_mon_client *monc);
extern void ceph_monc_request_umount(struct ceph_mon_client *monc);
extern void ceph_monc_report_failure(struct ceph_mon_client *monc, struct ceph_entity_inst *who);

#endif
