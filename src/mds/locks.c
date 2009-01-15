
// there must be a better way?
typedef char bool;
#define false 0
#define true  1

#include <netinet/in.h>
#include <linux/types.h>
#include <string.h>
#include <fcntl.h>

#include "include/ceph_fs.h"
#include "locks.h"

struct sm_state_t sm_simplelock[13] = {
                      // stable     loner  rep state  r     rp   rd   wr   l    x    caps,other
    [LOCK_SYNC]      = { 0,         false, LOCK_SYNC, ANY,  0,   ANY, 0,   ANY, 0,   CEPH_CAP_GRDCACHE,0,CEPH_CAP_GRDCACHE },
    [LOCK_LOCK]      = { 0,         false, LOCK_LOCK, AUTH, 0,   0,   0,   0,   ANY, 0,0,0 },
    [LOCK_XLOCK]     = { 0,         false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },
    [LOCK_XLOCKDONE] = { LOCK_SYNC, false, LOCK_LOCK, XCL,  XCL, XCL, 0,   XCL, XCL, 0,0,0 },
    [LOCK_SYNC_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, ANY,  0,   0,   0,   0,   0,   0,0,0 }, 
    [LOCK_LOCK_SYNC] = { LOCK_SYNC, false, LOCK_SYNC, ANY,  XCL, XCL, 0,   XCL, 0,   0,0,0 },
    [LOCK_EXCL]      = { 0,         true,  LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,CEPH_CAP_GEXCL|CEPH_CAP_GRDCACHE,0 },
    [LOCK_EXCL_SYNC] = { LOCK_SYNC, true,  LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,CEPH_CAP_GRDCACHE,0 },
    [LOCK_EXCL_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },
    [LOCK_SYNC_EXCL] = { LOCK_EXCL, true,  LOCK_LOCK, ANY,  0,   0,   0,   0,   0,   0,CEPH_CAP_GRDCACHE,0 },
    [LOCK_LOCK_EXCL] = { LOCK_EXCL, false, LOCK_LOCK, ANY,  0,   0,   0,   0,   0,   CEPH_CAP_GRDCACHE,0,0 },
    [LOCK_REMOTEXLOCK]={ LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },
};
