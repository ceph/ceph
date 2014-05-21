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

#ifndef XIO_CONNECTION_H
#define XIO_CONNECTION_H

#include <boost/intrusive/avl_set.hpp>
extern "C" {
#include "libxio.h"
}
#include "Connection.h"
#include "Messenger.h"
#include "include/atomic.h"

#define XXX_XIO_ALL_FEATURES ULONG_MAX

namespace bi = boost::intrusive;

class XioPortal;
class XioMessenger;

class XioConnection : public Connection
{
public:
  enum type { ACTIVE, PASSIVE };
private:
  XioConnection::type xio_conn_type;
  XioPortal *portal;
  entity_inst_t peer;
  struct xio_session *session;
  struct xio_connection	*conn;
  pthread_spinlock_t sp;
  atomic_t send;
  atomic_t recv;
  uint32_t magic;
  uint32_t special_handling;

  /* batching */
  struct msg_seq {
    bool p;
    int cnt;
    pthread_spinlock_t sp;
    list<struct xio_msg *> seq;
    msg_seq() : p(false) {
      pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
    }
    void append(struct xio_msg* req) { seq.push_back(req); --cnt; }
  } in_seq;

  // conns_entity_map comparison functor
  struct EntityComp
  {
    // for internal ordering
    bool operator()(const XioConnection &lhs,  const XioConnection &rhs) const
      {  return lhs.get_peer() < rhs.get_peer(); }

    // for external search by entity_inst_t(peer)
    bool operator()(const entity_inst_t &peer, const XioConnection &c) const
      {  return peer < c.get_peer(); }

    bool operator()(const XioConnection &c, const entity_inst_t &peer) const
      {  return c.get_peer() < peer;  }
  };

  bi::avl_set_member_hook<> conns_entity_map_hook;

  typedef bi::member_hook<XioConnection, bi::avl_set_member_hook<>,
			  &XioConnection::conns_entity_map_hook> EntityHook;
  typedef bi::avl_set< XioConnection, EntityHook,
		       bi::compare<EntityComp> > EntitySet;

  friend class XioPortal;
  friend class XioMessenger;
  friend class XioCompletionHook;
  friend class boost::intrusive_ptr<XioConnection>;

public:
  XioConnection(XioMessenger *m, XioConnection::type _type,
		const entity_inst_t& peer);

  bool is_connected() { return !!conn; }

  const entity_inst_t& get_peer() const { return peer; }

  XioConnection* get() {
    return static_cast<XioConnection*>(RefCountedObject::get());
  }

  uint32_t get_magic() { return magic; }
  void set_magic(int _magic) { magic = _magic; }
  uint32_t get_special_handling() { return special_handling; }
  void set_special_handling(int n) { special_handling = n; }

  int passive_setup(); /* XXX */

  int on_msg_req(struct xio_session *session, struct xio_msg *req,
		 int more_in_batch, void *cb_user_context);

  int on_msg_delivered(struct xio_session *session, struct xio_msg *msg,
		       int more_in_batch, void *conn_user_context);

  int on_msg_error(struct xio_session *session, enum xio_status error,
		   struct xio_msg  *msg, void *conn_user_context);

  int on_msg_send_complete(struct xio_session *session,
			   struct xio_msg *rsp, void *conn_user_context);
};

typedef boost::intrusive_ptr<XioConnection> XioConnectionRef;

class XioLoopbackConnection : public Connection
{
private:
public:
  XioLoopbackConnection(Messenger *m) : Connection(m)
    {
      const entity_inst_t& m_inst = m->get_myinst();
      peer_addr = m_inst.addr;
      peer_type = m_inst.name.type();
      set_features(XXX_XIO_ALL_FEATURES); /* XXXX set to ours */
    }
  XioLoopbackConnection* get() {
    return static_cast<XioLoopbackConnection*>(RefCountedObject::get());
  }
  virtual bool is_connected() { return true; }
};

typedef boost::intrusive_ptr<XioLoopbackConnection> LoopbackConnectionRef;

#endif /* XIO_CONNECTION_H */
