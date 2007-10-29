#ifndef _FS_CEPH_MON_CLIENT_H
#define _FS_CEPH_MON_CLIENT_H


struct ceph_mon_client {


};


extern ceph_monc_request_mdsmap(struct ceph_mon_client *monc, epoch_t have);
extern ceph_monc_request_osdmap(struct ceph_mon_client *monc, epoch_t have);
extern ceph_monc_request_mount(struct ceph_mon_client *monc);
extern ceph_monc_request_umount(struct ceph_mon_client *monc);
extern ceph_monc_report_failure(struct ceph_mon_client *monc, struct entity_inst_t who);

#endif
