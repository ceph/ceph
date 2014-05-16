// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef XIO_MESSENGER_H
#define XIO_MESSENGER_H

#include "SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}
#include "XioConnection.h"
#include "XioPortal.h"
#include "DispatchStrategy.h"
#include "include/atomic.h"
#include "common/Thread.h"
#include "common/Mutex.h"

extern struct xio_mempool *xio_msgr_mpool;

class XioMessenger : public SimplePolicyMessenger
{
private:
  static atomic_t nInstances;
  Mutex conns_lock;
  XioConnection::EntitySet conns_entity_map;
  XioPortals portals;
  DispatchStrategy* dispatch_strategy;
  int port_shift;
  uint32_t magic;
  uint32_t special_handling;

public:
  XioMessenger(CephContext *cct, entity_name_t name,
	       string mname, uint64_t nonce, int nportals,
	       DispatchStrategy* ds);

  virtual ~XioMessenger();

  void set_port_shift(int shift) { port_shift = shift; }

  XioPortal* default_portal() { return portals.get_portal0(); }

  uint32_t get_magic() { return magic; }
  void set_magic(int _magic) { magic = _magic; }
  uint32_t get_special_handling() { return special_handling; }
  void set_special_handling(int n) { special_handling = n; }

  /* xio hooks */
  int new_session(struct xio_session *session,
		  struct xio_new_session_req *req,
		  void *cb_user_context);

  int session_event(struct xio_session *session,
		    struct xio_session_event_data *event_data,
		    void *cb_user_context);

  /* Messenger interface */
  virtual void set_addr_unknowns(entity_addr_t &addr)
    { } /* XXX applicable? */

  virtual int get_dispatch_queue_len()
    { return 0; } /* XXX bogus? */

  virtual double get_dispatch_queue_max_age(utime_t now)
    { return 0; } /* XXX bogus? */

  virtual void set_cluster_protocol(int p)
    { }

  virtual int bind(const entity_addr_t& bind_addr);

  virtual int start();

  virtual void wait();

  virtual int shutdown();

  virtual int send_message(Message *m, const entity_inst_t& dest);

  virtual int send_message(Message *m, Connection *con);

  virtual int lazy_send_message(Message *m, const entity_inst_t& dest)
    { return EINVAL; }

  virtual int lazy_send_message(Message *m, Connection *con)
    { return EINVAL; }

  virtual ConnectionRef get_connection(const entity_inst_t& dest);

  virtual ConnectionRef get_loopback_connection();

  virtual int send_keepalive(const entity_inst_t& dest)
    { return EINVAL; }

  virtual int send_keepalive(Connection *con)
    { return EINVAL; }

  virtual void mark_down(const entity_addr_t& a)
    { }

  virtual void mark_down(Connection *con)
    { }

  virtual void mark_down_on_empty(Connection *con)
    { }

  virtual void mark_disposable(Connection *con)
    { }

  virtual void mark_down_all()
    { }

  void ds_dispatch(Message *m)
    { dispatch_strategy->ds_dispatch(m); }

protected:
  virtual void ready()
    { }

public:
};

#endif /* XIO_MESSENGER_H */
