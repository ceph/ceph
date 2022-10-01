#ifndef CEPH_MDS_LOCKS_H
#define CEPH_MDS_LOCKS_H
#include <stdbool.h>

struct sm_state_t {
  int next;         // 0 if stable
  bool loner;
  int replica_state;
  char can_read;
  char can_read_projected;
  char can_rdlock;
  char can_wrlock;
  char can_force_wrlock;
  char can_lease;
  char can_xlock;
  int caps;
  int loner_caps;
  int xlocker_caps;
  int replica_caps;
};

struct sm_t {
  const struct sm_state_t *states;
  int allowed_ever_auth;
  int allowed_ever_replica;
  int careful;
  int can_remote_xlock;
};

#define ANY  1 // auth or replica
#define AUTH 2 // auth only
#define XCL  3 // auth or exclusive client
//#define FW   4 // fw to auth, if replica
#define REQ  5 // req state change from auth, if replica

extern const struct sm_t sm_simplelock;
extern const struct sm_t sm_filelock;
extern const struct sm_t sm_scatterlock;
extern const struct sm_t sm_locallock;



// -- lock states --
// sync <-> lock
enum {
  LOCK_UNDEF = 0,

  //                                    auth               rep
  LOCK_SYNC,    // AR   R . RD L . / C .   R RD L . / C . 
  LOCK_LOCK,    // AR   R . .. . X / . .   . .. . . / . .

  LOCK_PREXLOCK,    // A    . . .. . . / . .   (lock)
  LOCK_XLOCK,       // A    . . .. . . / . .   (lock)
  LOCK_XLOCKDONE,   // A    r p rd l x / . .   (lock)  <-- by same client only!!
  LOCK_XLOCKSNAP,   // also revoke Fb
  LOCK_LOCK_XLOCK,

  LOCK_SYNC_LOCK,    // AR   R . .. . . / . .   R .. . . / . .
  LOCK_LOCK_SYNC,    // A    R p rd l . / . .   (lock)  <-- lc by same client only

  LOCK_EXCL,         // A    . . .. . . / c x * (lock)
  LOCK_EXCL_SYNC,    // A    . . .. . . / c . * (lock)
  LOCK_EXCL_LOCK,    // A    . . .. . . / . .   (lock)
  LOCK_SYNC_EXCL,    // Ar   R . .. . . / c . * (sync->lock)
  LOCK_LOCK_EXCL,    // A    R . .. . . / . .   (lock)

  LOCK_REMOTEXLOCK,  // on NON-auth

  // * = loner mode

  LOCK_MIX,
  LOCK_SYNC_MIX,
  LOCK_SYNC_MIX2,
  LOCK_LOCK_MIX,
  LOCK_EXCL_MIX,
  LOCK_MIX_SYNC,
  LOCK_MIX_SYNC2,
  LOCK_MIX_LOCK,
  LOCK_MIX_LOCK2,
  LOCK_MIX_EXCL,

  LOCK_TSYN,
  LOCK_TSYN_LOCK,
  LOCK_TSYN_MIX,
  LOCK_LOCK_TSYN,
  LOCK_MIX_TSYN,

  LOCK_PRE_SCAN,
  LOCK_SCAN,

  LOCK_SNAP_SYNC,

  LOCK_XSYN,
  LOCK_XSYN_EXCL,
  LOCK_EXCL_XSYN,
  LOCK_XSYN_SYNC,
  LOCK_XSYN_LOCK,
  LOCK_XSYN_MIX,

  LOCK_MAX,
};

// -------------------------
// lock actions

// for replicas
#define LOCK_AC_SYNC        -1
#define LOCK_AC_MIX         -2
#define LOCK_AC_LOCK        -3
#define LOCK_AC_LOCKFLUSHED -4

// for auth
#define LOCK_AC_SYNCACK      1
#define LOCK_AC_MIXACK     2
#define LOCK_AC_LOCKACK      3

#define LOCK_AC_REQSCATTER   7
#define LOCK_AC_REQUNSCATTER 8
#define LOCK_AC_NUDGE        9
#define LOCK_AC_REQRDLOCK   10

#define LOCK_AC_FOR_REPLICA(a)  ((a) < 0)
#define LOCK_AC_FOR_AUTH(a)     ((a) > 0)


#endif
