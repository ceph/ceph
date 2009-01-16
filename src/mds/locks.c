
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

struct sm_state_t simplelock[20] = {
                      // stable     loner  rep state  r     rp   rd   wr   l    x    caps,other
    [LOCK_SYNC]      = { 0,         false, LOCK_SYNC, ANY,  0,   ANY, 0,   ANY, 0,   CEPH_CAP_GRDCACHE,0,CEPH_CAP_GRDCACHE },
    [LOCK_LOCK_SYNC] = { LOCK_SYNC, false, LOCK_SYNC, ANY,  XCL, XCL, 0,   XCL, 0,   0,0,0 },
    [LOCK_EXCL_SYNC] = { LOCK_SYNC, true,  LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,CEPH_CAP_GRDCACHE,0 },

    [LOCK_LOCK]      = { 0,         false, LOCK_LOCK, AUTH, 0,   0,   0,   0,   ANY, 0,0,0 },
    [LOCK_SYNC_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, ANY,  0,   0,   0,   0,   0,   0,0,0 }, 
    [LOCK_EXCL_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },

    [LOCK_XLOCK]     = { LOCK_SYNC, false, LOCK_LOCK, 0,    XCL, 0,   0,   0,   0,   0,0,0 },
    [LOCK_XLOCKDONE] = { LOCK_SYNC, false, LOCK_LOCK, XCL,  XCL, XCL, 0,   XCL, XCL, 0,0,0 },
    [LOCK_LOCK_XLOCK]= { LOCK_XLOCK,false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },

    [LOCK_EXCL]      = { 0,         true,  LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,CEPH_CAP_GEXCL|CEPH_CAP_GRDCACHE,0 },
    [LOCK_SYNC_EXCL] = { LOCK_EXCL, true,  LOCK_LOCK, ANY,  0,   0,   0,   0,   0,   0,CEPH_CAP_GRDCACHE,0 },
    [LOCK_LOCK_EXCL] = { LOCK_EXCL, false, LOCK_LOCK, ANY,  0,   0,   0,   0,   0,   CEPH_CAP_GRDCACHE,0,0 },

    [LOCK_REMOTEXLOCK]={ LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },
};

struct sm_t sm_simplelock = {
	.states = simplelock,
	.allowed_ever_auth = CEPH_CAP_GRDCACHE | CEPH_CAP_GEXCL,
	.allowed_ever_replica = CEPH_CAP_GRDCACHE,
	.careful = CEPH_CAP_GRDCACHE | CEPH_CAP_GEXCL,
	.can_remote_xlock = 1,
};


// lock state machine states:
//  Sync  --  Lock  --  sCatter
//  Tempsync _/
// (out of date)

struct sm_state_t scatterlock[30] = {
                      // stable     loner  rep state  r     rp   rd   wr   l    x    caps,other
    [LOCK_SYNC]      = { 0,         false, LOCK_SYNC, ANY,  0,   ANY, 0,   ANY, 0,   CEPH_CAP_GRDCACHE,0,CEPH_CAP_GRDCACHE },
    [LOCK_LOCK_SYNC] = { LOCK_SYNC, false, LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   0,0,0 },
    [LOCK_MIX_SYNC]  = { LOCK_SYNC, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },
    
    [LOCK_LOCK]      = { 0,         false, LOCK_LOCK, AUTH, 0,   FW,  AUTH,0,   ANY, 0,0,0 },
    [LOCK_SYNC_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   0,0,0 },
    [LOCK_MIX_LOCK]  = { LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },
    [LOCK_TSYN_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },
    
    [LOCK_TSYN]      = { 0,         false, LOCK_LOCK, AUTH, 0,   AUTH,0,   0,   0,   0,0,0 },
    [LOCK_LOCK_TSYN] = { LOCK_TSYN, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },
    [LOCK_MIX_TSYN]  = { LOCK_TSYN, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },

    [LOCK_MIX]       = { 0,         false, LOCK_MIX,  0,    0,   FW,  ANY, 0,   0,   0,0,0 },
    [LOCK_TSYN_MIX]  = { LOCK_MIX,  false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },
    [LOCK_SYNC_MIX]  = { LOCK_MIX,  false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,0,0 },
};

struct sm_t sm_scatterlock = {
	.states = scatterlock,
	.allowed_ever_auth = CEPH_CAP_GRDCACHE | CEPH_CAP_GEXCL,
	.allowed_ever_replica = CEPH_CAP_GRDCACHE,
	.careful = CEPH_CAP_GRDCACHE | CEPH_CAP_GEXCL,
	.can_remote_xlock = 0,
};

struct sm_state_t filelock[30] = {
                      // stable     loner  rep state  r     rp   rd   wr   l    x    caps,other
    [LOCK_SYNC]      = { 0,         false, LOCK_SYNC, ANY,  0,   ANY, 0,   ANY, 0,   CEPH_CAP_GRDCACHE|CEPH_CAP_GRD,0,CEPH_CAP_GRDCACHE|CEPH_CAP_GRD },
    [LOCK_LOCK_SYNC] = { LOCK_SYNC, false, LOCK_SYNC, AUTH, 0,   AUTH,0,   0,   0,   CEPH_CAP_GRDCACHE|CEPH_CAP_GWRBUFFER,0,0 },
    [LOCK_EXCL_SYNC] = { LOCK_SYNC, true,  LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,CEPH_CAP_GRDCACHE|CEPH_CAP_GRD,0 },
    [LOCK_MIX_SYNC]  = { LOCK_SYNC, false, LOCK_MIX,  0,    0,   0,   0,   0,   0,   CEPH_CAP_GRD,0,CEPH_CAP_GRD },
    [LOCK_MIX_SYNC2] = { LOCK_SYNC, false, 0,         0,    0,   0,   0,   0,   0,   CEPH_CAP_GRD,0,CEPH_CAP_GRD },
    
    [LOCK_LOCK]      = { 0,         false, LOCK_LOCK, AUTH, 0,   AUTH,AUTH,0,   ANY, CEPH_CAP_GRDCACHE|CEPH_CAP_GWRBUFFER,0,0 },
    [LOCK_SYNC_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   CEPH_CAP_GRDCACHE,0,CEPH_CAP_GRDCACHE },
    [LOCK_EXCL_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   CEPH_CAP_GRDCACHE|CEPH_CAP_GWRBUFFER,0,CEPH_CAP_GRDCACHE },
    [LOCK_MIX_LOCK]  = { LOCK_LOCK, false, LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   0,0,0 },

    [LOCK_XLOCK]     = { LOCK_SYNC, false, LOCK_LOCK, 0,    XCL, 0,   0,   0,   0,   0,0,0 },
    [LOCK_XLOCKDONE] = { LOCK_SYNC, false, LOCK_LOCK, XCL,  XCL, XCL, 0,   XCL, XCL, 0,0,0 },

    [LOCK_MIX]       = { 0,         false, LOCK_MIX,  0,    0,   FW,  ANY, 0,   0,   CEPH_CAP_GRD|CEPH_CAP_GWR,0,CEPH_CAP_GRD },
    [LOCK_SYNC_MIX]  = { LOCK_MIX,  false, LOCK_MIX,  ANY,  0,   0,   0,   0,   0,   CEPH_CAP_GRD,0,CEPH_CAP_GRD },
    [LOCK_EXCL_MIX]  = { LOCK_MIX,  true,  LOCK_LOCK, 0,    0,   0,   XCL, 0,   0,   0,CEPH_CAP_GRD|CEPH_CAP_GWR,0 },
    
    [LOCK_EXCL]      = { 0,         true,  LOCK_LOCK, 0,    0,   FW,  0,   0,   0,   0,CEPH_CAP_GRDCACHE|CEPH_CAP_GEXCL|CEPH_CAP_GRD|CEPH_CAP_GWR|CEPH_CAP_GWRBUFFER,0 },
    [LOCK_SYNC_EXCL] = { LOCK_EXCL, true,  LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   0,CEPH_CAP_GRDCACHE|CEPH_CAP_GRD,0 },
    [LOCK_MIX_EXCL]  = { LOCK_EXCL, true,  LOCK_LOCK, 0,    0,   0,   XCL, 0,   0,   0,CEPH_CAP_GRD|CEPH_CAP_GWR,0 },
    [LOCK_LOCK_EXCL] = { LOCK_EXCL, true,  LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   0,CEPH_CAP_GRDCACHE|CEPH_CAP_GWRBUFFER,0 },
};

struct sm_t sm_filelock = {
	.states = filelock,
	.allowed_ever_auth = (CEPH_CAP_GRDCACHE |
			      CEPH_CAP_GEXCL |
			      CEPH_CAP_GRD |
			      CEPH_CAP_GWR |
			      CEPH_CAP_GWREXTEND |
			      CEPH_CAP_GWRBUFFER | 
			      CEPH_CAP_GLAZYIO),
	.allowed_ever_replica = (CEPH_CAP_GRDCACHE |
				 CEPH_CAP_GRD | 
				 CEPH_CAP_GLAZYIO),
	.careful = (CEPH_CAP_GRDCACHE | 
		    CEPH_CAP_GEXCL | 
		    CEPH_CAP_GWRBUFFER),
	.can_remote_xlock = 0,
};

