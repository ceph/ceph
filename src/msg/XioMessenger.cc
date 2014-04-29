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

#include <arpa/inet.h>
#include <boost/lexical_cast.hpp>
#include <set>
#include <stdlib.h>
#include <memory>

#include "XioMsg.h"
#include "XioMessenger.h"
#define dout_subsys ceph_subsys_xio

Mutex mtx("XioMessenger Package Lock");
atomic_t initialized;

atomic_t XioMessenger::nInstances;

struct xio_mempool *xio_msgr_noreg_mpool;

static struct xio_session_ops xio_msgr_ops;

/* Accelio API callouts */
static int on_session_event(struct xio_session *session,
			    struct xio_session_event_data *event_data,
			    void *cb_user_context)
{
  XioMessenger *msgr = static_cast<XioMessenger*>(cb_user_context);

  dout(4) << dout_format( "session event: %s. reason: %s",
	   xio_session_event_str(event_data->event),
	   xio_strerror(event_data->reason)) << dendl;

  return msgr->session_event(session, event_data, cb_user_context);
}

static int on_new_session(struct xio_session *session,
			  struct xio_new_session_req *req,
			  void *cb_user_context)
{
  XioMessenger *msgr = static_cast<XioMessenger*>(cb_user_context);

  dout(4) << dout_format("new session %p user_context %p", session, cb_user_context) << dendl;

  return (msgr->new_session(session, req, cb_user_context));
}

static int on_msg_send_complete(struct xio_session *session,
				struct xio_msg *rsp,
				void *conn_user_context)
{
  XioConnection *xcon =
    static_cast<XioConnection*>(conn_user_context);

  dout(4) << dout_format("msg send complete: session: %p rsp: %p user_context %p",
	   session, rsp, conn_user_context) << dendl;

  return xcon->on_msg_send_complete(session, rsp, conn_user_context);
}

static int on_msg(struct xio_session *session,
		  struct xio_msg *req,
		  int more_in_batch,
		  void *cb_user_context)
{
  XioConnection *xcon =
    static_cast<XioConnection*>(cb_user_context);

  dout(4) << dout_format("on_msg session %p xcon %p", session, xcon) << dendl;

  return xcon->on_msg_req(session, req, more_in_batch,
			  cb_user_context);
}

static int on_msg_delivered(struct xio_session *session,
			    struct xio_msg *msg,
			    int more_in_batch,
			    void *conn_user_context)
{
  XioConnection *xcon =
    static_cast<XioConnection*>(conn_user_context);

  dout(4) <<
    dout_format(
      "msg delivered session: %p msg: %p more: %d conn_user_context %p",
      session, msg, more_in_batch, conn_user_context) << dendl;

  return xcon->on_msg_delivered(session, msg, more_in_batch,
				conn_user_context);
}

static int on_msg_error(struct xio_session *session,
			enum xio_status error,
			struct xio_msg  *msg,
			void *conn_user_context)
{
  /* XIO promises to flush back undelivered messages */
  XioConnection *xcon =
    static_cast<XioConnection*>(conn_user_context);

  dout(4) << dout_format("msg error session: %p error: %s msg: %p conn_user_context %p",
	 session, xio_strerror(error), msg, conn_user_context) << dendl;

  return xcon->on_msg_error(session, error, msg, conn_user_context);
}

static int on_cancel(struct xio_session *session,
		     struct xio_msg  *msg,
		     enum xio_status result,
		     void *conn_user_context)
{
#if 0
  XioConnection *xcon =
    static_cast<XioConnection*>(conn_user_context);

  dout(4) << dout_format("on cancel: session: %p msg: %p conn_user_context %p",
	 session, msg, conn_user_context) << dendl;
#endif

  return 0;
}

static int on_cancel_request(struct xio_session *session,
			     struct xio_msg  *msg,
			     void *conn_user_context)
{
#if 0
  XioConnection *xcon =
    static_cast<XioConnection*>(conn_user_context);

  dout(4) << dout_format(
    "on cancel request: session: %p msg: %p conn_user_context %p",
    session, msg, conn_user_context) << dendl;
#endif

  return 0;
}

/* free functions */
static string xio_uri_from_entity(const entity_addr_t& addr, bool want_port)
{
  const char *host = NULL;
  char addr_buf[129];

  switch(addr.addr.ss_family) {
  case AF_INET:
    host = inet_ntop(AF_INET, &addr.addr4.sin_addr, addr_buf,
		     INET_ADDRSTRLEN);
    break;
  case AF_INET6:
    host = inet_ntop(AF_INET6, &addr.addr6.sin6_addr, addr_buf,
		     INET6_ADDRSTRLEN);
    break;
  default:
    abort();
    break;
  };

  /* The following can only succeed if the host is rdma-capable */
  string xio_uri = "rdma://";
  xio_uri += host;
  if (want_port) {
    xio_uri += ":";
    xio_uri += boost::lexical_cast<std::string>(addr.get_port());
  }

  return xio_uri;
} /* xio_uri_from_entity */

/* XioMessenger */
XioMessenger::XioMessenger(CephContext *cct, entity_name_t name,
			   string mname, uint64_t nonce, int nportals,
			   DispatchStrategy *ds)
  : SimplePolicyMessenger(cct, name, mname, nonce),
    conns_lock("XioMessenger::conns_lock"),
    portals(this, nportals),
    dispatch_strategy(ds),
    port_shift(0),
    magic(0),
    special_handling(0)
{
  /* package init */
  if (! initialized.read()) {

    mtx.Lock();
    if (! initialized.read()) {

      xio_init();

      unsigned xopt;

      if (magic & (MSG_MAGIC_TRACE_XIO)) {
	xopt = XIO_LOG_LEVEL_TRACE;
	xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_LOG_LEVEL,
		    &xopt, sizeof(unsigned));
      }

      xopt = 1;
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_DISABLE_HUGETBL,
		  &xopt, sizeof(unsigned));

      /* and unregisterd one */
#define XMSG_MEMPOOL_MIN 4096
#define XMSG_MEMPOOL_MAX 4096

      xio_msgr_noreg_mpool =
	xio_mempool_create_ex(-1 /* nodeid */,
			      XIO_MEMPOOL_FLAG_REGULAR_PAGES_ALLOC);
      for (int i = 64; i < 131072; i <<= 2) {
	(void) xio_mempool_add_allocator(xio_msgr_noreg_mpool, i, 0,
					 XMSG_MEMPOOL_MAX, XMSG_MEMPOOL_MIN);
      }

      /* initialize ops singleton */
      xio_msgr_ops.on_session_event = on_session_event;
      xio_msgr_ops.on_new_session = on_new_session;
      xio_msgr_ops.on_session_established = NULL;
      xio_msgr_ops.on_msg_send_complete	= on_msg_send_complete;
      xio_msgr_ops.on_msg = on_msg;
      xio_msgr_ops.on_msg_delivered = on_msg_delivered;
      xio_msgr_ops.on_msg_error = on_msg_error;
      xio_msgr_ops.on_cancel = on_cancel;
      xio_msgr_ops.on_cancel_request = on_cancel_request;

      /* mark initialized */
      initialized.set(1);
    }
    mtx.Unlock();
  }

  dispatch_strategy->set_messenger(this);

  /* update class instance count */
  nInstances.inc();

} /* ctor */

int XioMessenger::new_session(struct xio_session *session,
			      struct xio_new_session_req *req,
			      void *cb_user_context)
{
  return portals.accept(session, req, cb_user_context);
} /* new_session */

int XioMessenger::session_event(struct xio_session *session,
				struct xio_session_event_data *event_data,
				void *cb_user_context)
{
  XioConnection *xcon;

  switch (event_data->event) {
  case XIO_SESSION_NEW_CONNECTION_EVENT:
  {
    xcon = new XioConnection(this, XioConnection::PASSIVE, entity_inst_t());
    xcon->session = session;

    struct xio_connection *conn = event_data->conn;
    struct xio_connection_attr xcona;

    (void) xio_query_connection(conn, &xcona, XIO_CONNECTION_ATTR_CTX);

    struct xio_context_attr xctxa;
    (void) xio_query_context(xcona.ctx, &xctxa, XIO_CONTEXT_ATTR_USER_CTX);

    xcon->conn = conn;
    xcon->portal = static_cast<XioPortal*>(xctxa.user_context);
    assert(xcon->portal);

    xcona.user_context = xcon;
    (void) xio_modify_connection(conn, &xcona, XIO_CONNECTION_ATTR_USER_CTX);

    dout(4) << dout_format("new connection session %p xcon %p", session, xcon) << dendl;
  }
  break;
  case XIO_SESSION_CONNECTION_CLOSED_EVENT: /* orderly discon */
  case XIO_SESSION_CONNECTION_DISCONNECTED_EVENT: /* unexpected discon */
    dout(4) << dout_format("xio client disconnection %p", event_data->conn_user_context) << dendl;
    /* clean up mapped connections */
    xcon = static_cast<XioConnection*>(event_data->conn_user_context);
    {
      XioConnection::EntitySet::iterator conn_iter =
	conns_entity_map.find(xcon->peer, XioConnection::EntityComp());
      if (conn_iter != conns_entity_map.end()) {
	XioConnection *xcon2 = &(*conn_iter);
	if (xcon == xcon2) {
	  conns_entity_map.erase(conn_iter);
	}
      }
      /* XXX it could make sense to track ephemeral connections, but
       * we don't currently, so if conn_iter points to nothing or to
       * an address other than xcon, there's nothing to clean up */
    }
#if 0 /* XXX remove from an ephemeral_conns list? */
    xcon->put(); /* XXX currently, there is no sentinel ref */
#endif
    break;
  case XIO_SESSION_TEARDOWN_EVENT:
    dout(4) << dout_format("xio_session_teardown %p", session) << dendl;
    xio_session_destroy(session);
    break;
  default:
    break;
  };

  return 0;
}

enum bl_type
{
  BUFFER_PAYLOAD,
  BUFFER_MIDDLE,
  BUFFER_DATA
};

static inline void
xio_place_buffers(buffer::list& bl, XioMsg *xmsg, struct xio_msg* req,
		  struct xio_iovec_ex*& msg_iov, struct xio_iovec_ex*& iov,
		  int ex_cnt, int& msg_off, int& req_off, bl_type type)
{

  const std::list<buffer::ptr>& buffers = bl.buffers();
  list<bufferptr>::const_iterator pb;
  for (pb = buffers.begin(); pb != buffers.end(); ++pb) {

    /* assign buffer */
    iov = &msg_iov[msg_off];
    iov->iov_base = (void *) pb->c_str(); // is this efficient?
    iov->iov_len = pb->length();

    /* track iovlen */
    req->out.data_iovlen = msg_off+1;

    /* XXXX this SHOULD work fine (Eyal) */
    switch (type) {
    case BUFFER_DATA:
      //break;
    default:
    {
      struct xio_mempool_obj *mp = get_xio_mp(*pb);
      if (mp) {
#if 0
// XXX disable for delivery receipt experiment
	iov->user_context = mp;
#endif
	iov->mr = mp->mr;
      }
    }
      break;
    }

    /* advance iov(s) */
    if (++msg_off >= XIO_MAX_IOV) {
      if (++req_off < ex_cnt) {
	/* next record */
	req->out.data_iovlen = XIO_MAX_IOV;
	req->more_in_batch++;
	/* XXX chain it */
	req = &xmsg->req_arr[req_off];
	req->user_context = xmsg->get();
	msg_iov = req->out.data_iov;
	msg_off = 0;
      }
    }
  }
}

int XioMessenger::bind(const entity_addr_t& addr)
{
  const entity_addr_t *a = &addr;
  if (a->is_blank_ip()) {
    struct entity_addr_t _addr = *a;
    a = &_addr;
    std::vector <std::string> my_sections;
    g_conf->get_my_sections(my_sections);
    std::string rdma_local_str;
    if (g_conf->get_val_from_conf_file(my_sections, "rdma local",
				      rdma_local_str, true) == 0) {
      struct entity_addr_t local_rdma_addr;
      local_rdma_addr = *a;
      const char *ep;
      if (!local_rdma_addr.parse(rdma_local_str.c_str(), &ep)) {
	derr << "ERROR:  Cannot parse rdma local: " << rdma_local_str << dendl;
	return -1;
      }
      if (*ep) {
	derr << "WARNING: 'rdma local trailing garbage ignored: '" << ep << dendl;
      }
      int p = _addr.get_port();
      _addr.set_sockaddr(reinterpret_cast<struct sockaddr *>(
			  &local_rdma_addr.ss_addr()));
      _addr.set_port(p);
    } else {
      derr << "WARNING: need 'rdma local' config for remote use!" <<dendl;
    }
  }
  string base_uri = xio_uri_from_entity(*a, false /* want_port */);
  dout(4) << dout_format("XioMessenger %p bind: xio_uri %s:%d",
	 this, base_uri.c_str(), a->get_port()) << dendl;

  return portals.bind(&xio_msgr_ops, base_uri, a->get_port());
} /* bind */

int XioMessenger::start()
{
  portals.start();
  dispatch_strategy->start();
  started = true;
  return 0;
}

void XioMessenger::wait()
{
  portals.join();
} /* wait */

int XioMessenger::send_message(Message *m, const entity_inst_t& dest)
{
  ConnectionRef conn = get_connection(dest);
  if (conn)
    return send_message(m, conn.get() /* intrusive_pointer */);
  else
    return EINVAL;
} /* send_message(Message *, const entity_inst_t&) */

static inline XioMsg* pool_alloc_xio_msg(Message *m, XioConnection *xcon)
{
  struct xio_mempool_obj mp_mem;
  int e = xio_mempool_alloc(xio_msgr_noreg_mpool, sizeof(XioMsg), &mp_mem);
  assert(e == 0);
  XioMsg *xmsg = (XioMsg*) mp_mem.addr;
  assert(!!xmsg);
  new (xmsg) XioMsg(m, xcon, mp_mem);
  return xmsg;
}

int XioMessenger::send_message(Message *m, Connection *con)
{
  XioConnection *xcon = static_cast<XioConnection*>(con);
  int code = 0;
  bool trace_hdr = false;

  m->set_seq(0); /* XIO handles seq */
  m->encode(xcon->get_features(), !this->cct->_conf->ms_nocrc);

  /* trace flag */
  m->set_magic(magic);
  m->set_special_handling(special_handling);

  /* get an XioMsg frame */
  XioMsg *xmsg = pool_alloc_xio_msg(m, xcon);

  dout(4) << "\nsend_message " << m << " new XioMsg " << xmsg
       << " req_0 " << &xmsg->req_0 << " msg type " << m->get_type()
       << " features: " << xcon->get_features() << dendl;
  if (magic & (MSG_MAGIC_XIO)) {

    /* XXXX verify */
    switch (m->get_type()) {
    case 43:
    // case 15:
      cout << "stop 43 " << m->get_type() << " " << *m << std::endl;
      buffer::list &payload = m->get_payload();
      cout << "payload dump:" << std::endl;
      payload.hexdump(cout);
      trace_hdr = true;
    }
  }

  struct xio_msg *req = &xmsg->req_0;
  struct xio_iovec_ex *msg_iov = req->out.data_iov, *iov = NULL;
  int ex_cnt;

  buffer::list &payload = m->get_payload();
  buffer::list &middle = m->get_middle();
  buffer::list &data = m->get_data();

  if (magic & (MSG_MAGIC_XIO)) {
    cout << "payload: " << payload.buffers().size() <<
      " middle: " << middle.buffers().size() <<
      " data: " << data.buffers().size() <<
      std::endl;
  }

  xmsg->nbuffers = payload.buffers().size() + middle.buffers().size() +
    data.buffers().size();
  ex_cnt = ((3 + xmsg->nbuffers) / XIO_MAX_IOV);
  xmsg->hdr.msg_cnt = 1 + ex_cnt;

  if (ex_cnt > 0) {
    xmsg->req_arr =
      (struct xio_msg *) calloc(ex_cnt, sizeof(struct xio_msg));
  }

  /* do the invariant part */
  int msg_off = 0;
  int req_off = -1; /* most often, not used */

  xio_place_buffers(payload, xmsg, req, msg_iov, iov, ex_cnt, msg_off,
		    req_off, BUFFER_PAYLOAD);

  xio_place_buffers(middle, xmsg, req, msg_iov, iov, ex_cnt, msg_off,
		    req_off, BUFFER_MIDDLE);

  xio_place_buffers(data, xmsg, req, msg_iov, iov, ex_cnt, msg_off,
		    req_off, BUFFER_DATA);

  /* fixup first msg */
  req = &xmsg->req_0;

  if (trace_hdr) {
    void print_xio_msg_hdr(XioMsgHdr &hdr);
    print_xio_msg_hdr(xmsg->hdr);

    void print_ceph_msg(Message *m);
    print_ceph_msg(m);
  }

  const std::list<buffer::ptr>& header = xmsg->hdr.get_bl().buffers();
  assert(header.size() == 1); /* XXX */
  list<bufferptr>::const_iterator pb = header.begin();
  req->out.header.iov_base = (char*) pb->c_str();
  req->out.header.iov_len = pb->length();

  /* deliver via xio, preserve ordering */
  struct xio_msg *head = &xmsg->req_0;
  if (xmsg->hdr.msg_cnt > 1) {
    struct xio_msg *tail = head;
    for (req_off = 1; req_off < ex_cnt; ++req_off) {
      req = &xmsg->req_arr[req_off];
      tail->next = req;
      tail = req;
     }
  }
  xcon->portal->enqueue_for_send(xcon, xmsg);

  return code;
} /* send_message(Message *, Connection *) */

int XioMessenger::shutdown()
{

  portals.shutdown();
  started = false;
  return 0;
} /* shutdown */

ConnectionRef XioMessenger::get_connection(const entity_inst_t& dest)
{
  entity_inst_t _dest = dest;
  if (port_shift) {
    _dest.addr.set_port(
      _dest.addr.get_port() + port_shift);
  }
  XioConnection::EntitySet::iterator conn_iter =
    conns_entity_map.find(_dest, XioConnection::EntityComp());
  if (conn_iter != conns_entity_map.end())
    return static_cast<Connection*>(&(*conn_iter));
  else {
    string xio_uri = xio_uri_from_entity(_dest.addr, true /* want_port */);

    dout(4) << dout_format("XioMessenger %p get_connection: xio_uri %s",
	   this, xio_uri.c_str()) << dendl;

    /* XXX client session attributes */
    struct xio_session_attr attr = {
      &xio_msgr_ops,
      NULL, /* XXX server private data? */
      0     /* XXX? */
    };

    XioConnection *conn = new XioConnection(this, XioConnection::ACTIVE,
					    _dest);

    conn->session = xio_session_create(XIO_SESSION_REQ, &attr, xio_uri.c_str(),
				       0, 0, this);
    if (! conn->session) {
      delete conn;
      return NULL;
    }

    /* this should cause callbacks with user context of conn, but
     * we can always set it explicitly */
    conn->conn = xio_connect(conn->session, this->portals.get_portal0()->ctx,
			     0, NULL, conn);

    /* conn has nref == 1 */
    conns_entity_map.insert(*conn);

    return conn;
  }
} /* get_connection */

ConnectionRef XioMessenger::get_loopback_connection()
{
  abort();
  return NULL;
} /* get_loopback_connection */

XioMessenger::~XioMessenger()
{
  delete dispatch_strategy;
  nInstances.dec();
} /* dtor */
