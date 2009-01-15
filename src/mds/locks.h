
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

#define ANY 1 //static const char ANY  = 1;  // auth + replica
#define AUTH 2 //static const char AUTH = 2;  // auth
#define XCL 3 //static const char XCL  = 3;  // auth + exclusive cilent

extern struct sm_state_t sm_simplelock[];
extern struct sm_state_t sm_filelock[];
extern struct sm_state_t sm_scatterlock[];


// -- lock states --
// sync <-> lock
#define LOCK_UNDEF    0

//                                    auth               rep
#define LOCK_SYNC        1    // AR   R . RD L . / C .   R RD L . / C . 
#define LOCK_LOCK        2    // AR   R . .. . X / . .   . .. . . / . .
#define LOCK_XLOCK       3    // A    . . .. . . / . .   (lock)
#define LOCK_XLOCKDONE   4    // A    r p rd l x / . .   (lock)  <-- by same client only!!

#define LOCK_SYNC_LOCK   5    // AR   R . .. . . / . .   R .. . . / . .
#define LOCK_LOCK_SYNC   6    // A    R p rd l . / . .   (lock)  <-- lc by same client only

#define LOCK_EXCL        7    // A    . . .. . . / c x * (lock)
#define LOCK_EXCL_SYNC   8    // A    . . .. . . / c . * (lock)
#define LOCK_EXCL_LOCK   9    // A    . . .. . . / . .   (lock)
#define LOCK_SYNC_EXCL  10    // Ar   R . .. . . / c . * (sync->lock)
#define LOCK_LOCK_EXCL  11    // A    R . .. . . / . .   (lock)

#define LOCK_REMOTEXLOCK  12  // on NON-auth

// * = loner mode



#endif
