#ifndef CEPH_DENTRYLEASE_H
#define CEPH_DENTRYLEASE_H

#include "include/xlist.h"
#include "include/fs_types.h"
#include "common/config.h"

class CDentry;
class Session;

class DentryLease {
  CDentry *dn;
  Session *session;
public:

  ceph_seq_t seq;
  utime_t ttl;
  xlist<DentryLease*>::item item_session_lease; // per-session list

  DentryLease(CDentry *d, Session *s) :
    dn(d), session(s), seq(0), item_session_lease(this) {}
  ~DentryLease() {
    assert(!item_session_lease.is_on_list());
  }

  CDentry *get_dentry() const { return dn; }
  Session *get_session() const { return session; }
}; 
#endif
