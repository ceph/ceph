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

#ifndef XIO_PORTAL_H
#define XIO_PORTAL_H

extern "C" {
#include "libxio.h"
}
#include <boost/lexical_cast.hpp>
#include "SimplePolicyMessenger.h"
#include "XioConnection.h"
#include "XioMsg.h"

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64 /* XXX arch-specific define */
#endif
#define CACHE_PAD(_n) char __pad ## _n [CACHE_LINE_SIZE]

class XioMessenger;

class XioPortal : public Thread
{
private:

  struct SubmitQueue
  {
    const static int nlanes = 7;

    struct Lane
    {
      uint32_t size;
      XioMsg::Queue q;
      pthread_spinlock_t sp;
      pthread_mutex_t mtx;
      CACHE_PAD(0);
    };

    Lane qlane[nlanes];

    SubmitQueue()
      {
	int ix;
	Lane* lane;

	for (ix = 0; ix < nlanes; ++ix) {
	  lane = &qlane[ix];
	  pthread_spin_init(&lane->sp, PTHREAD_PROCESS_PRIVATE);
	  pthread_mutex_init(&lane->mtx, NULL);
	  lane->size = 0;
	}
      }

    inline Lane* get_lane(XioConnection *xcon)
      {
	return &qlane[((uint64_t) xcon) % nlanes];
      }

    void enq(XioConnection *xcon, XioSubmit* xs)
      {
	Lane* lane = get_lane(xcon);
#if 0
	pthread_spin_lock(&lane->sp);
#else
	pthread_mutex_lock(&lane->mtx);
#endif
	lane->q.push_back(*xs);
	++(lane->size);
#if 0
	pthread_spin_unlock(&lane->sp);
#else
	pthread_mutex_unlock(&lane->mtx);
#endif
      }

    void deq(XioSubmit::Queue &send_q)
      {
	int ix;
	Lane* lane;

	for (ix = 0; ix < nlanes; ++ix) {
	  lane = &qlane[ix];
#if 0
	pthread_spin_lock(&lane->sp);
#else
	pthread_mutex_lock(&lane->mtx);
#endif
	  if (lane->size > 0) {
	    XioSubmit::Queue::const_iterator i1 = send_q.end();
	    send_q.splice(i1, lane->q);
	    lane->size = 0;
	  }
#if 0
	pthread_spin_unlock(&lane->sp);
#else
	pthread_mutex_unlock(&lane->mtx);
#endif
	}
      }

  };

  XioMessenger *msgr;
  struct xio_context *ctx;
  struct xio_server *server;
  SubmitQueue submit_q;
  pthread_spinlock_t sp;
  pthread_mutex_t mtx;
  void *ev_loop;
  string xio_uri;
  char *portal_id;
  bool _shutdown;
  bool drained;
  uint32_t magic;
  uint32_t special_handling;

  friend class XioPortals;
  friend class XioMessenger;

public:
  XioPortal(XioMessenger *_msgr) :
  msgr(_msgr), ctx(NULL), server(NULL), submit_q(), xio_uri(""),
  portal_id(NULL), _shutdown(false), drained(false),
  magic(0),
  special_handling(0)
    {
      pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
      pthread_mutex_init(&mtx, NULL);

      /* a portal is an xio_context and event loop */
      ctx = xio_context_create(NULL, 0 /* poll timeout */, -1 /* cpu hint */);

      /* associate this XioPortal object with the xio_context handle */
      struct xio_context_attr xca;
      xca.user_context = this;
      xio_modify_context(ctx, &xca, XIO_CONTEXT_ATTR_USER_CTX);

      if (magic & (MSG_MAGIC_XIO)) {
	printf("XioPortal %p created ev_loop %p ctx %p\n",
	       this, ev_loop, ctx);
      }
    }

  int bind(struct xio_session_ops *ops, const string &_uri);

  void enqueue_for_send(XioConnection *xcon, XioSubmit *xs)
    {
      if (! _shutdown) {
	submit_q.enq(xcon, xs);
	xio_context_stop_loop(ctx, false);
      }
    }

  void *entry()
    {
      int ix, size, code;
      XioSubmit::Queue send_q;
      XioSubmit::Queue::iterator q_iter;
      struct xio_msg *msg = NULL;
      list <struct xio_msg *>::iterator iter;
      XioSubmit *xs;
      XioMsg *xmsg;

      do {
	submit_q.deq(send_q);
	size = send_q.size();

	/* shutdown() barrier */
#if 0
	pthread_spin_lock(&sp);
#else
	pthread_mutex_lock(&mtx);
#endif

	if (_shutdown) {
	  drained = true;
	}

	if (size > 0) {
	  /* XXX look out, no flow control */
	  for (ix = 0; ix < size; ++ix) {
	    q_iter = send_q.begin();
	    xs = &(*q_iter);
	    send_q.erase(q_iter);

	    switch(xs->type) {
	    case XIO_MSG_TYPE_REQ: /* it was an outgoing 1-way */
	      xmsg = static_cast<XioMsg*>(xs);
	      msg = &xmsg->req_0;
	      code = xio_send_msg(xs->xcon->conn, msg);
	      xs->xcon->send.set(msg->timestamp); /* XXX atomic? */
	      break;
	    default:
	      /* XIO_MSG_TYPE_RSP */
	    {
	      XioRsp* xrsp = static_cast<XioRsp*>(xs);
	      list <struct xio_msg *>& msg_seq = xrsp->get_xhook()->get_seq();
	      for (iter = msg_seq.begin(); iter != msg_seq.end();
		   ++iter) {
		msg = *iter;
		code = xio_release_msg(msg);
	      }
	      xrsp->finalize();
	    }
	    break;
	    };

	    if (code) { // XXX cleanup or discard
	      cerr << "IGNORING THIS FAILURE: xio_send_" << ((long)(msg))<< " "
		   << (!msg->request ? "request" : "response") <<
		" failed, code=" << code << std::endl;
	      continue; /* XXX messages will be queued for cleanup */
	    }
	  }
	}

#if 0
	pthread_spin_unlock(&sp);
#else
	pthread_mutex_unlock(&mtx);
#endif

	xio_context_run_loop(ctx, 80);

      } while ((!_shutdown) || (!drained));

      /* shutting down */
      if (server) {
	xio_unbind(server);
      }
      xio_context_destroy(ctx);
      return NULL;
    }

  void shutdown()
    {
#if 0
	pthread_spin_lock(&sp);
#else
	pthread_mutex_lock(&mtx);
#endif
      xio_context_stop_loop(ctx, false);
      _shutdown = true;
#if 0
	pthread_spin_unlock(&sp);
#else
	pthread_mutex_unlock(&mtx);
#endif
    }

  ~XioPortal()
    {
      free(portal_id);
    }
};

class XioPortals
{
private:
  vector<XioPortal*> portals;
  char **p_vec;
  int n;

public:
  XioPortals(XioMessenger *msgr, int _n) : p_vec(NULL), n(_n)
    {
      /* n session portals, plus 1 to accept new sessions */
      int ix, np = n+1;
      for (ix = 0; ix < np; ++ix) {
	portals.push_back(new XioPortal(msgr));
      }
    }

  vector<XioPortal*>& get() { return portals; }

  const char **get_vec()
    {
      return (const char **) p_vec;
    }

  int get_portals_len()
    {
      return n;
    }

  XioPortal* get_portal0()
    {
      return portals[0];
    }

  int bind(struct xio_session_ops *ops, const string& base_uri,
	   const int base_port);

    int accept(struct xio_session *session,
		 struct xio_new_session_req *req,
		 void *cb_user_context)
    {
      const char **portals_vec = get_vec()+1;
      int portals_len = get_portals_len()-1;

      return xio_accept(session,
			portals_vec,
			portals_len,
			NULL, 0);
    }

  void start()
    {
      XioPortal *portal;
      int p_ix, nportals = portals.size();

      /* portal_0 is the new-session handler, portal_1+ terminate
       * active sessions */

      p_vec = new char*[(nportals-1)];
      for (p_ix = 1; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	/* shift left */
	p_vec[(p_ix-1)] = (char*) /* portal->xio_uri.c_str() */
	  portal->portal_id;
      }

      for (p_ix = 0; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	portal->create();
      }
    }

  void shutdown()
    {
      XioPortal *portal;
      int nportals = portals.size();
      for (int p_ix = 0; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	portal->shutdown();
      }
    }

  void join()
    {
      XioPortal *portal;
      int nportals = portals.size();
      for (int p_ix = 0; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	portal->join();
      }
    }

  ~XioPortals()
    {
      int nportals = portals.size();
      for (int ix = 0; ix < nportals; ++ix) {
	delete(*(portals.begin()));
      }
      if (p_vec) {
	delete[] p_vec;
      }
    }
};

#endif /* XIO_PORTAL_H */
