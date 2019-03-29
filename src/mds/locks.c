#include "include/int_types.h"

#include <string.h>
#include <fcntl.h>

#include "include/ceph_fs.h"
#include "locks.h"

static const struct sm_state_t simplelock[LOCK_MAX] = {
                      // stable     loner  rep state  r     rp   rd   wr   fwr  l    x    caps,other
    [LOCK_SYNC]      = { 0,         false, LOCK_SYNC, ANY,  0,   ANY, 0,   0,   ANY, 0,   CEPH_CAP_GSHARED,0,0,CEPH_CAP_GSHARED },
    [LOCK_LOCK_SYNC] = { LOCK_SYNC, false, LOCK_LOCK, AUTH, XCL, XCL, 0,   0,   XCL, 0,   0,0,0,0 },
    [LOCK_EXCL_SYNC] = { LOCK_SYNC, true,  LOCK_LOCK, 0,    0,   0,   0,   XCL, 0,   0,   0,CEPH_CAP_GSHARED,0,0 },
    [LOCK_SNAP_SYNC] = { LOCK_SYNC, false, LOCK_LOCK, 0,    0,   0,   0,   AUTH,0,   0,   0,0,0,0 },

    [LOCK_LOCK]      = { 0,         false, LOCK_LOCK, AUTH, 0,   REQ, 0,   0,   0,   0,   0,0,0,0 },
    [LOCK_SYNC_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, ANY,  0,   0,   0,   0,   0,   0,   0,0,0,0 }, 
    [LOCK_EXCL_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   XCL, 0,   0,   0,0,0,0 },

    [LOCK_PREXLOCK]  = { LOCK_LOCK, false, LOCK_LOCK, 0,    XCL, 0,   0,   0,   0,   ANY, 0,0,0,0 },
    [LOCK_XLOCK]     = { LOCK_SYNC, false, LOCK_LOCK, 0,    XCL, 0,   0,   0,   0,   0,   0,0,0,0 },
    [LOCK_XLOCKDONE] = { LOCK_SYNC, false, LOCK_LOCK, XCL,  XCL, XCL, 0,   0,   XCL, 0,   0,0,CEPH_CAP_GSHARED,0 },
    [LOCK_LOCK_XLOCK]= { LOCK_PREXLOCK,false,LOCK_LOCK,0,   XCL, 0,   0,   0,   0,   XCL, 0,0,0,0 },

    [LOCK_EXCL]      = { 0,         true,  LOCK_LOCK, 0,    0,   REQ, XCL, 0,   0,   0,   0,CEPH_CAP_GEXCL|CEPH_CAP_GSHARED,0,0 },
    [LOCK_SYNC_EXCL] = { LOCK_EXCL, true,  LOCK_LOCK, ANY,  0,   0,   0,   0,   0,   0,   0,CEPH_CAP_GSHARED,0,0 },
    [LOCK_LOCK_EXCL] = { LOCK_EXCL, false, LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   0,   CEPH_CAP_GSHARED,0,0,0 },

    [LOCK_REMOTEXLOCK]={ LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,   0,0,0,0 },

};

const struct sm_t sm_simplelock = {
	.states = simplelock,
	.allowed_ever_auth = CEPH_CAP_GSHARED | CEPH_CAP_GEXCL,
	.allowed_ever_replica = CEPH_CAP_GSHARED,
	.careful = CEPH_CAP_GSHARED | CEPH_CAP_GEXCL,
	.can_remote_xlock = 1,
};


// lock state machine states:
//  Sync  --  Lock  --  sCatter
//  Tempsync _/
// (out of date)

static const struct sm_state_t scatterlock[LOCK_MAX] = {
                      // stable     loner  rep state  r     rp   rd   wr   fwr  l    x    caps,other
    [LOCK_SYNC]      = { 0,         false, LOCK_SYNC, ANY,  0,   ANY, 0,   0,   ANY, 0,   CEPH_CAP_GSHARED,0,0,CEPH_CAP_GSHARED },
    [LOCK_LOCK_SYNC] = { LOCK_SYNC, false, LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   0,   0,0,0,0 },
    [LOCK_MIX_SYNC]  = { LOCK_SYNC, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,   0,0,0,0 },
    [LOCK_SNAP_SYNC] = { LOCK_SYNC, false, LOCK_LOCK, 0,    0,   0,   0,   AUTH,0,   0,   0,0,0,0 },
   
    [LOCK_LOCK]      = { 0,         false, LOCK_LOCK, AUTH, 0,   REQ, AUTH,0,   0,   ANY, 0,0,0,0 },
    [LOCK_SYNC_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, ANY,  0,   0,   0,   0,   0,   0,   0,0,0,0 },
    [LOCK_MIX_LOCK]  = { LOCK_LOCK, false, LOCK_MIX,  0,    0,   0,   0,   0,   0,   0,   0,0,0,0 },
    [LOCK_MIX_LOCK2] = { LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,   0,0,0,0 },
    [LOCK_TSYN_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   0,   0,0,0,0 },
    
    [LOCK_TSYN]      = { 0,         false, LOCK_LOCK, AUTH, 0,   AUTH,0,   0,   0,   0,   0,0,0,0 },
    [LOCK_LOCK_TSYN] = { LOCK_TSYN, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,   0,0,0,0 },
    [LOCK_MIX_TSYN]  = { LOCK_TSYN, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,   0,0,0,0 },

    [LOCK_MIX]       = { 0,         false, LOCK_MIX,  0,    0,   REQ, ANY, 0,   0,   0,   0,0,0,0 },
    [LOCK_TSYN_MIX]  = { LOCK_MIX,  false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,   0,0,0,0 },
    [LOCK_SYNC_MIX]  = { LOCK_MIX,  false, LOCK_SYNC_MIX2,ANY,0, 0,   0,   0,   0,   0,   0,0,0,0 },
    [LOCK_SYNC_MIX2] = { LOCK_MIX,  false, 0,         0,    0,   0,   0,   0,   0,   0,   0,0,0,0 },
};

const struct sm_t sm_scatterlock = {
	.states = scatterlock,
	.allowed_ever_auth = CEPH_CAP_GSHARED | CEPH_CAP_GEXCL,
	.allowed_ever_replica = CEPH_CAP_GSHARED,
	.careful = CEPH_CAP_GSHARED | CEPH_CAP_GEXCL,
	.can_remote_xlock = 0,
};

const struct sm_state_t filelock[LOCK_MAX] = {
                      // stable     loner  rep state  r     rp   rd   wr   fwr  l    x    caps(any,loner,xlocker,replica)
    [LOCK_SYNC]      = { 0,         false, LOCK_SYNC, ANY,  0,   ANY, 0,   0,   ANY, 0,   CEPH_CAP_GSHARED|CEPH_CAP_GCACHE|CEPH_CAP_GRD|CEPH_CAP_GLAZYIO,0,0,CEPH_CAP_GSHARED|CEPH_CAP_GCACHE|CEPH_CAP_GRD },
    [LOCK_LOCK_SYNC] = { LOCK_SYNC, false, LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   0,   CEPH_CAP_GCACHE,0,0,0 },
    [LOCK_EXCL_SYNC] = { LOCK_SYNC, true,  LOCK_LOCK, 0,    0,   0,   0,   XCL, 0,   0,   0,CEPH_CAP_GSHARED|CEPH_CAP_GCACHE|CEPH_CAP_GRD,0,0 },
    [LOCK_MIX_SYNC]  = { LOCK_SYNC, false, LOCK_MIX_SYNC2,0,0,   0,   0,   0,   0,   0,   CEPH_CAP_GRD|CEPH_CAP_GLAZYIO,0,0,CEPH_CAP_GRD },
    [LOCK_MIX_SYNC2] = { LOCK_SYNC, false, 0,         0,    0,   0,   0,   0,   0,   0,   CEPH_CAP_GRD|CEPH_CAP_GLAZYIO,0,0,CEPH_CAP_GRD },
    [LOCK_SNAP_SYNC] = { LOCK_SYNC, false, LOCK_LOCK, 0,    0,   0,   0,   AUTH,0,   0,   0,0,0,0 },
    [LOCK_XSYN_SYNC] = { LOCK_SYNC, true,  LOCK_LOCK, AUTH, 0,   AUTH,0,   0,   0,   0,   0,CEPH_CAP_GCACHE,0,0 },
  
    [LOCK_LOCK]      = { 0,         false, LOCK_LOCK, AUTH, 0,   REQ, AUTH,0,   0,   0,   CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,0,0 },
    [LOCK_SYNC_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, ANY,  0,   REQ, 0,   0,   0,   0,   CEPH_CAP_GCACHE,0,0,0 },
    [LOCK_EXCL_LOCK] = { LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   XCL, 0,   0,   CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,0,0 },
    [LOCK_MIX_LOCK]  = { LOCK_LOCK, false, LOCK_MIX,  0,    0,   REQ, 0,   0,   0,   0,   0,0,0,0 },
    [LOCK_MIX_LOCK2] = { LOCK_LOCK, false, LOCK_LOCK, 0,    0,   REQ, 0,   0,   0,   0,   0,0,0,0 },
    [LOCK_XSYN_LOCK] = { LOCK_LOCK, true,  LOCK_LOCK, AUTH, 0,   0,   XCL, 0,   0,   0,   0,CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,0 },

    [LOCK_PREXLOCK]  = { LOCK_LOCK, false, LOCK_LOCK, 0,    XCL, 0,   0,   0,   0,   ANY, CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,0,0 },
    [LOCK_XLOCK]     = { LOCK_LOCK, false, LOCK_LOCK, 0,    XCL, 0,   0,   0,   0,   0,   CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,0,0 },
    [LOCK_XLOCKDONE] = { LOCK_LOCK, false, LOCK_LOCK, XCL,  XCL, XCL, 0,   0,   XCL, 0,   CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,CEPH_CAP_GSHARED,0 },
    [LOCK_XLOCKSNAP] = { LOCK_LOCK, false, LOCK_LOCK, 0,    XCL, 0,   0,   0,   0,   0,   CEPH_CAP_GCACHE,0,0,0 },
    [LOCK_LOCK_XLOCK]= { LOCK_PREXLOCK,false,LOCK_LOCK,0,   XCL, 0,   0,   0,   0,   XCL, CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,0,0 },

    [LOCK_MIX]       = { 0,         false, LOCK_MIX,  0,    0,   REQ, ANY, 0,   0,   0,   CEPH_CAP_GRD|CEPH_CAP_GWR|CEPH_CAP_GLAZYIO,0,0,CEPH_CAP_GRD },
    [LOCK_SYNC_MIX]  = { LOCK_MIX,  false, LOCK_SYNC_MIX2,ANY,0, 0,   0,   0,   0,   0,   CEPH_CAP_GRD|CEPH_CAP_GLAZYIO,0,0,CEPH_CAP_GRD },
    [LOCK_SYNC_MIX2] = { LOCK_MIX,  false, 0,         0,    0,   0,   0,   0,   0,   0,   CEPH_CAP_GRD|CEPH_CAP_GLAZYIO,0,0,CEPH_CAP_GRD },
    [LOCK_EXCL_MIX]  = { LOCK_MIX,  true,  LOCK_LOCK, 0,    0,   0,   XCL, 0,   0,   0,   0,CEPH_CAP_GRD|CEPH_CAP_GWR,0,0 },
    [LOCK_XSYN_MIX]  = { LOCK_MIX,  true,  LOCK_LOCK, 0,    0,   0,   XCL, 0,   0,   0,   0,0,0,0 },
    
    [LOCK_EXCL]      = { 0,         true,  LOCK_LOCK, 0,    0,   XCL, XCL, 0,   0,   0,   0,CEPH_CAP_GSHARED|CEPH_CAP_GEXCL|CEPH_CAP_GCACHE|CEPH_CAP_GRD|CEPH_CAP_GWR|CEPH_CAP_GBUFFER,0,0 },
    [LOCK_SYNC_EXCL] = { LOCK_EXCL, true,  LOCK_LOCK, ANY,  0,   0,   0,   0,   0,   0,   0,CEPH_CAP_GSHARED|CEPH_CAP_GCACHE|CEPH_CAP_GRD,0,0 },
    [LOCK_MIX_EXCL]  = { LOCK_EXCL, true,  LOCK_LOCK, 0,    0,   0,   XCL, 0,   0,   0,   0,CEPH_CAP_GRD|CEPH_CAP_GWR,0,0 },
    [LOCK_LOCK_EXCL] = { LOCK_EXCL, true,  LOCK_LOCK, AUTH, 0,   0,   0,   0,   0,   0,   0,CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,0 },
    [LOCK_XSYN_EXCL] = { LOCK_EXCL, true,  LOCK_LOCK, AUTH, 0,   XCL, 0,   0,   0,   0,   0,CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,0 },

    [LOCK_XSYN]      = { 0,         true,  LOCK_LOCK, AUTH, AUTH,AUTH,XCL, 0,   0,   0,   0,CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,0 },
    [LOCK_EXCL_XSYN] = { LOCK_XSYN, false, LOCK_LOCK, 0,    0,   XCL, 0,   0,   0,   0,   0,CEPH_CAP_GCACHE|CEPH_CAP_GBUFFER,0,0 },

    [LOCK_PRE_SCAN]  = { LOCK_SCAN, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,   0,0,0,0 },
    [LOCK_SCAN]      = { LOCK_LOCK, false, LOCK_LOCK, 0,    0,   0,   0,   0,   0,   0,   0,0,0,0 },
};

const struct sm_t sm_filelock = {
	.states = filelock,
	.allowed_ever_auth = (CEPH_CAP_GSHARED |
			      CEPH_CAP_GEXCL |
			      CEPH_CAP_GCACHE |
			      CEPH_CAP_GRD |
			      CEPH_CAP_GWR |
			      CEPH_CAP_GWREXTEND |
			      CEPH_CAP_GBUFFER | 
			      CEPH_CAP_GLAZYIO),
	.allowed_ever_replica = (CEPH_CAP_GSHARED |
				 CEPH_CAP_GCACHE |
				 CEPH_CAP_GRD | 
				 CEPH_CAP_GLAZYIO),
	.careful = (CEPH_CAP_GSHARED | 
		    CEPH_CAP_GEXCL | 
		    CEPH_CAP_GCACHE |
		    CEPH_CAP_GBUFFER),
	.can_remote_xlock = 0,
};


const struct sm_state_t locallock[LOCK_MAX] = {
                      // stable     loner  rep state  r     rp   rd   wr   fwr  l    x    caps(any,loner,xlocker,replica)
    [LOCK_LOCK]      = { 0,         false, LOCK_LOCK, ANY,  0,   ANY, 0,   0,   ANY, AUTH,0,0,0,0 },
};

const struct sm_t sm_locallock = {
  .states = locallock,
  .allowed_ever_auth = 0,
  .allowed_ever_replica = 0,
  .careful = 0,
  .can_remote_xlock = 0,
};
