/* ceph_fs_msgs.h
 *
 * message types
 */

#ifndef _FS_CEPH_CEPH_FS_MSGS_H
#define _FS_CEPH_CEPH_FS_MSGS_H

/* misc */
#define CEPH_MSG_SHUTDOWN               1
#define CEPH_MSG_PING                   2
#define CEPH_MSG_PING_ACK               3

/* client <-> monitor */
#define CEPH_MSG_MON_MAP                4
#define CEPH_MSG_CLIENT_MOUNT           10
#define CEPH_MSG_CLIENT_UNMOUNT         11
#define CEPH_MSG_STATFS                 12
#define CEPH_MSG_STATFS_REPLY           13

/* client <-> mds */
#define CEPH_MSG_MDS_GETMAP                  20
#define CEPH_MSG_MDS_MAP                     21

#define CEPH_MSG_CLIENT_SESSION         22   // start or stop
#define CEPH_MSG_CLIENT_RECONNECT       23

#define CEPH_MSG_CLIENT_REQUEST         24
#define CEPH_MSG_CLIENT_REQUEST_FORWARD 25
#define CEPH_MSG_CLIENT_REPLY           26
#define CEPH_MSG_CLIENT_FILECAPS        0x310  // 

/* osd */
#define CEPH_MSG_OSD_GETMAP       40
#define CEPH_MSG_OSD_MAP          41
#define CEPH_MSG_OSD_OP           42    // delete, etc.
#define CEPH_MSG_OSD_OPREPLY      43    // delete, etc.

#endif
