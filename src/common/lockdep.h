#ifndef _CEPH_LOCKDEP_H
#define _CEPH_LOCKDEP_H

extern int g_lockdep;

extern int lockdep_register(const char *n);
extern int lockdep_will_lock(const char *n, int id);
extern int lockdep_locked(const char *n, int id, bool force_backtrace=false);
extern int lockdep_will_unlock(const char *n, int id);
extern int lockdep_dump_locks();

#endif
