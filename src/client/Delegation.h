// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef _CEPH_CLIENT_DELEGATION_H
#define _CEPH_CLIENT_DELEGATION_H

#include "common/Clock.h"
#include "common/Timer.h"

class Fh;

/* Commands for manipulating delegation state */
#ifndef CEPH_DELEGATION_NONE
# define CEPH_DELEGATION_NONE	0
# define CEPH_DELEGATION_RD	1
# define CEPH_DELEGATION_WR	2
#endif

/* Callback for delegation recalls */
typedef void (*ceph_deleg_cb_t)(Fh *fh, void *priv);

/* Converts CEPH_DELEGATION_* to cap mask */
int ceph_deleg_caps_for_type(unsigned type);

/*
 * A delegation is a container for holding caps on behalf of a client that
 * wants to be able to rely on them until recalled.
 */
class Delegation {
public:
  Delegation(Fh *_fh, unsigned _type, ceph_deleg_cb_t _cb, void *_priv);
  ~Delegation();
  Fh *get_fh() { return fh; }
  unsigned get_type() { return type; }
  bool is_recalled() { return !recall_time.is_zero(); }

  void reinit(unsigned _type, ceph_deleg_cb_t _recall_cb, void *_priv);
  void recall(bool skip_read);
private:
  // Filehandle against which it was acquired
  Fh				*fh;

  // opaque token that will be passed to the callback
  void				*priv;

  // CEPH_DELEGATION_* type
  unsigned			type;

  // callback into application to recall delegation
  ceph_deleg_cb_t		recall_cb;

  // time of first recall
  utime_t			recall_time;

  // timer for unreturned delegations
  Context			*timeout_event;

  void arm_timeout();
  void disarm_timeout();
};

#endif /* _CEPH_CLIENT_DELEGATION_H */
