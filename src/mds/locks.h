
#ifndef __CEPH_MDS_LOCKS_H
#define __CEPH_MDS_LOCKS_H

struct sm_state_t {
  int next;         // 0 if stable
  char loner;
  int replica_state;
  char can_read;
  char can_read_projected;
  char can_rdlock;
  char can_wrlock;
  char can_lease;
  char can_xlock;
  int caps;
  int loner_caps;
  int replica_caps;
};

struct sm_t {
  struct sm_state_t *states;
  int allowed_ever_auth;
  int allowed_ever_replica;
  int careful;
  int can_remote_xlock;
};

#define ANY  1 // auth or replica
#define AUTH 2 // auth only
#define XCL  3 // auth or exclusive client
#define FW   4 // fw to auth, if replica

extern struct sm_t sm_simplelock;
extern struct sm_t sm_filelock;
extern struct sm_t sm_scatterlock;




// -- lock states --
// sync <-> lock
#define LOCK_UNDEF    0

//                                    auth               rep
#define LOCK_SYNC        1    // AR   R . RD L . / C .   R RD L . / C . 
#define LOCK_LOCK        2    // AR   R . .. . X / . .   . .. . . / . .

#define LOCK_XLOCK       3    // A    . . .. . . / . .   (lock)
#define LOCK_XLOCKDONE   4    // A    r p rd l x / . .   (lock)  <-- by same client only!!
#define LOCK_LOCK_XLOCK  5

#define LOCK_SYNC_LOCK   6    // AR   R . .. . . / . .   R .. . . / . .
#define LOCK_LOCK_SYNC   7    // A    R p rd l . / . .   (lock)  <-- lc by same client only

#define LOCK_EXCL        8    // A    . . .. . . / c x * (lock)
#define LOCK_EXCL_SYNC   9    // A    . . .. . . / c . * (lock)
#define LOCK_EXCL_LOCK  10    // A    . . .. . . / . .   (lock)
#define LOCK_SYNC_EXCL  11    // Ar   R . .. . . / c . * (sync->lock)
#define LOCK_LOCK_EXCL  12    // A    R . .. . . / . .   (lock)

#define LOCK_REMOTEXLOCK  13  // on NON-auth

// * = loner mode

#define LOCK_MIX      14
#define LOCK_SYNC_MIX 15
#define LOCK_SYNC_MIX2 16
#define LOCK_LOCK_MIX 17
#define LOCK_EXCL_MIX 18
#define LOCK_MIX_SYNC 19
#define LOCK_MIX_SYNC2 20
#define LOCK_MIX_LOCK 21
#define LOCK_MIX_EXCL 22

#define LOCK_TSYN      23
#define LOCK_TSYN_LOCK 24
#define LOCK_TSYN_MIX  25
#define LOCK_LOCK_TSYN 26
#define LOCK_MIX_TSYN  27


// -------------------------
// lock actions

// for replicas
#define LOCK_AC_SYNC        -1
#define LOCK_AC_MIX         -2
#define LOCK_AC_LOCK        -3

// for auth
#define LOCK_AC_SYNCACK      1
#define LOCK_AC_MIXACK     2
#define LOCK_AC_LOCKACK      3

#define LOCK_AC_REQSCATTER   7
#define LOCK_AC_REQUNSCATTER 8
#define LOCK_AC_NUDGE        9

#define LOCK_AC_FOR_REPLICA(a)  ((a) < 0)
#define LOCK_AC_FOR_AUTH(a)     ((a) > 0)


static inline const char *get_lock_action_name(int a) {
  switch (a) {
  case LOCK_AC_SYNC: return "sync";
  case LOCK_AC_MIX: return "mix";
  case LOCK_AC_LOCK: return "lock";

  case LOCK_AC_SYNCACK: return "syncack";
  case LOCK_AC_MIXACK: return "mixack";
  case LOCK_AC_LOCKACK: return "lockack";

  case LOCK_AC_REQSCATTER: return "reqscatter";
  case LOCK_AC_REQUNSCATTER: return "requnscatter";
  case LOCK_AC_NUDGE: return "nudge";
  default: return "???";
  }
}



#endif
