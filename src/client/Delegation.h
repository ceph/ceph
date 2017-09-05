// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef _CEPH_CLIENT_DELEGATION_H
#define _CEPH_CLIENT_DELEGATION_H

#include "include/xlist.h"
#include "common/Clock.h"
#include "common/Timer.h"

#include "Fh.h"

/* Commands for manipulating delegation state */
#ifndef CEPH_DELEGATION_NONE
# define CEPH_DELEGATION_NONE	0
# define CEPH_DELEGATION_RD	1
# define CEPH_DELEGATION_WR	2
#endif

typedef void (*ceph_deleg_cb_t)(Fh *fh, void *priv);

/*
 * A delegation is a container for holding caps on behalf of a client that
 * wants to be able to rely on them until recalled.
 */
class Delegation {
protected:
  // per-inode linkage
  xlist<Delegation*>::item	inode_item;

  friend class Inode;

private:
  // Filehandle against which it was acquired
  Fh				*fh;

  // opaque token that will be passed to the callback
  void				*priv;

  // Delegation mode (CEPH_FILE_MODE_RD, CEPH_FILE_MODE_RDWR) (other flags later?)
  unsigned			mode;

  // callback into application to recall delegation
  ceph_deleg_cb_t		recall_cb;

  // time of first recall
  utime_t			recall_time;

  // timer for unreturned delegations
  Context			*timeout_event;
public:
  Delegation(Fh *_fh, unsigned _mode, ceph_deleg_cb_t _cb, void *_priv)
	: inode_item(this), fh(_fh), priv(_priv), mode(_mode),
	  recall_cb(_cb), recall_time(utime_t()), timeout_event(nullptr) {};

  Fh *get_fh() { return fh; }
  unsigned get_mode() { return mode; }

  /* Update existing deleg with new mode, cb, and priv value */
  void reinit(unsigned _mode, ceph_deleg_cb_t _recall_cb, void *_priv)
  {
    mode = _mode;
    recall_cb = _recall_cb;
    priv = _priv;
  }

  void arm_timeout(SafeTimer *timer, Context *event, double timeout)
  {
    if (timeout_event) {
      delete event;
      return;
    }

    timeout_event = event;
    // FIXME: make timeout tunable
    timer->add_event_after(timeout, event);
  }

  void disarm_timeout(SafeTimer *timer)
  {
    if (!timeout_event)
      return;

    timer->cancel_event(timeout_event);
    timeout_event = nullptr;
  }

  bool is_recalled() { return !recall_time.is_zero(); }

  void recall(bool skip_read)
  {
    /* If ro is true, don't break read delegations */
    if (skip_read && mode == CEPH_FILE_MODE_RD)
      return;

    if (!is_recalled()) {
      recall_cb(fh, priv);
      recall_time = ceph_clock_now();
    }
  }
};

#endif /* _CEPH_CLIENT_DELEGATION_H */
