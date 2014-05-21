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

#include "XioMsg.h"
#include "XioConnection.h"
#include "XioMessenger.h"
#include "messages/MDataPing.h"

extern struct xio_mempool *xio_msgr_mpool;
extern struct xio_mempool *xio_msgr_noreg_mpool;

void print_xio_msg_hdr(XioMsgHdr &hdr)
{

  cout << "ceph header: " <<
    " front_len: " << hdr.hdr->front_len <<
    " seq: " << hdr.hdr->seq <<
    " tid: " << hdr.hdr->tid <<
    " type: " << hdr.hdr->type <<
    " prio: " << hdr.hdr->priority <<
    " name type: " << (int) hdr.hdr->src.type <<
    " name num: " << (int) hdr.hdr->src.num <<
    " version: " << hdr.hdr->version <<
    " compat_version: " << hdr.hdr->compat_version <<
    " front_len: " << hdr.hdr->front_len <<
    " middle_len: " << hdr.hdr->middle_len <<
    " data_len: " << hdr.hdr->data_len <<
    " xio header: " <<
    " msg_cnt: " << hdr.msg_cnt <<
    std::endl;

  cout << "ceph footer: " <<
    " front_crc: " << hdr.ftr->front_crc <<
    " middle_crc: " << hdr.ftr->middle_crc <<
    " data_crc: " << hdr.ftr->data_crc <<
    " sig: " << hdr.ftr->sig <<
    " flags: " << (uint32_t) hdr.ftr->flags <<
    std::endl;
}

void print_ceph_msg(Message *m)
{
  if (m->get_magic() & (MSG_MAGIC_XIO & MSG_MAGIC_TRACE_DTOR)) {
    ceph_msg_header& header = m->get_header();
    cout << "header version " << header.version <<
      " compat version " << header.compat_version <<
      std::endl;
  }
}

XioConnection::XioConnection(XioMessenger *m, XioConnection::type _type,
			     const entity_inst_t& _peer) :
  Connection(m),
  xio_conn_type(_type),
  portal(m->default_portal()),
  peer(_peer),
  session(NULL),
  conn(NULL),
  magic(m->get_magic()),
  in_seq()
{
  pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
  peer_addr = peer.addr;
  peer_type = peer.name.type();
  set_peer_addr(peer.addr);

  /* XXXX fake features, aieee! */
  set_features(XXX_XIO_ALL_FEATURES);

}

int XioConnection::passive_setup()
{
  /* XXX passive setup is a placeholder for (potentially active-side
     initiated) feature and auth* negotiation */
  static bufferlist authorizer, authorizer_reply; /* static because fake */
  static CryptoKey session_key; /* ditto */
  bool authorizer_valid;

  XioMessenger *msgr = static_cast<XioMessenger*>(get_messenger());

  /* XXX fake authorizer! */
  msgr->ms_deliver_verify_authorizer(
    this, peer_type, CEPH_AUTH_NONE,
    authorizer,
    authorizer_reply,
    authorizer_valid,
    session_key);

  return (0);
}

#define uint_to_timeval(tv, s) ((tv).tv_sec = (s), (tv).tv_usec = 0)

static inline XioCompletionHook* pool_alloc_xio_completion_hook(
  XioConnection *xcon, Message *m, list <struct xio_msg *>& msg_seq)
{
  struct xio_mempool_obj mp_mem;
  int e = xio_mempool_alloc(xio_msgr_noreg_mpool,
			    sizeof(XioCompletionHook), &mp_mem);
  assert(e == 0);
  XioCompletionHook *xhook = (XioCompletionHook*) mp_mem.addr;
  new (xhook) XioCompletionHook(xcon, m, msg_seq, mp_mem);
  return xhook;
}

int XioConnection::on_msg_req(struct xio_session *session,
			      struct xio_msg *req,
			      int more_in_batch,
			      void *cb_user_context)
{
  struct xio_msg *treq = req;

  /* XXX Accelio guarantees message ordering at
   * xio_session */

  if (! in_seq.p) {
#if 0 /* XXX */
    printf("receive req %p treq %p iov_base %p iov_len %d data_iovlen %d\n",
	   req, treq, treq->in.header.iov_base,
	   (int) treq->in.header.iov_len,
	   (int) treq->in.data_iovlen);
#endif
    XioMsgCnt msg_cnt(
      buffer::create_static(treq->in.header.iov_len,
			    (char*) treq->in.header.iov_base));
    in_seq.cnt = msg_cnt.msg_cnt;
    in_seq.p = true;
  }
  in_seq.append(req);
  if (in_seq.cnt > 0) {
    return 0;
  }
  else
    in_seq.p = false;

  XioMessenger *msgr = static_cast<XioMessenger*>(get_messenger());
  XioCompletionHook *m_hook =
    pool_alloc_xio_completion_hook(this, NULL /* msg */, in_seq.seq);
  list<struct xio_msg *>& msg_seq = m_hook->msg_seq;
  in_seq.seq.clear();

  ceph_msg_header header;
  ceph_msg_footer footer;
  buffer::list payload, middle, data;

  struct timeval t1, t2;
  uint64_t seq;

  list<struct xio_msg *>::iterator msg_iter = msg_seq.begin();
  treq = *msg_iter;
  XioMsgHdr hdr(header, footer,
		buffer::create_static(treq->in.header.iov_len,
				      (char*) treq->in.header.iov_base));

  uint_to_timeval(t1, treq->timestamp);

  if (magic & (MSG_MAGIC_TRACE_XCON)) {
    if (hdr.hdr->type == 43) {
      print_xio_msg_hdr(hdr);
    }
  }

  unsigned int ix, blen, iov_len;
  struct xio_iovec_ex *msg_iov;
  uint32_t take_len, left_len = 0;
  char *left_base = NULL;

  ix = 0;
  blen = header.front_len;

  while (blen && (msg_iter != msg_seq.end())) {
    treq = *msg_iter;
    iov_len = treq->in.data_iovlen;
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &treq->in.data_iov[ix];

      /* XXX need to detect any buffer which needs to be
       * split due to coalescing of a segment (front, middle,
       * data) boundary */

      take_len = MIN(blen, msg_iov->iov_len);
      payload.append(
	buffer::create_msg(
	  take_len, (char*) msg_iov->iov_base, m_hook));
      blen -= take_len;
      if (! blen) {
	left_len = msg_iov->iov_len - take_len;
	if (left_len) {
	  left_base = ((char*) msg_iov->iov_base) + take_len;
	}
      }
    }
    /* XXX as above, if a buffer is split, then we needed to track
     * the new start (carry) and not advance */
    if (ix == iov_len) {
      msg_iter++;
      ix = 0;
    }
  }

  if (magic & (MSG_MAGIC_TRACE_XCON)) {
    if (hdr.hdr->type == 43) {
      cout << "front (payload) dump:" << std::endl;
      payload.hexdump(cout);
    }
  }

  blen = header.middle_len;

  if (blen && left_len) {
    middle.append(
      buffer::create_msg(left_len, left_base, m_hook));
    left_len = 0;
  }

  while (blen && (msg_iter != msg_seq.end())) {
    treq = *msg_iter;
    iov_len = treq->in.data_iovlen;
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &treq->in.data_iov[ix];

      take_len = MIN(blen, msg_iov->iov_len);
      middle.append(
	buffer::create_msg(
	  take_len, (char*) msg_iov->iov_base, m_hook));
      blen -= take_len;
      if (! blen) {
	left_len = msg_iov->iov_len - take_len;
	if (left_len) {
	  left_base = ((char*) msg_iov->iov_base) + take_len;
	}
      }
    }
    if (ix == iov_len) {
      msg_iter++;
      ix = 0;
    }
  }

  blen = header.data_len;

  if (blen && left_len) {
    data.append(
      buffer::create_msg(left_len, left_base, m_hook));
    left_len = 0;
  }

  while (blen && (msg_iter != msg_seq.end())) {
    treq = *msg_iter;
    iov_len = treq->in.data_iovlen;
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &treq->in.data_iov[ix];
      data.append(
	buffer::create_msg(
	  msg_iov->iov_len, (char*) msg_iov->iov_base, m_hook));
      blen -= msg_iov->iov_len;
    }
    if (ix == iov_len) {
      msg_iter++;
      ix = 0;
    }
  }

  seq = treq->sn;
  uint_to_timeval(t2, treq->timestamp);

  /* update connection timestamp */
  recv.set(treq->timestamp);

  Message *m =
    decode_message(msgr->cct, header, footer, payload, middle, data);

  if (m) {
    /* completion */
    this->get(); /* XXX getting underrun */
    m->set_connection(this);

    /* reply hook */
    m_hook->set_message(m);
    m->set_completion_hook(m_hook);

    /* trace flag */
    m->set_magic(magic);

    /* update timestamps */
    m->set_recv_stamp(t1);
    m->set_recv_complete_stamp(t2);
    m->set_seq(seq);

    /* XXXX validate peer type */
    if (peer_type != (int) hdr.peer_type) { /* XXX isn't peer_type -1? */
      peer_type = hdr.peer_type;
      if (xio_conn_type == XioConnection::PASSIVE) {
	/* XXX kick off feature/authn/authz negotiation
	 * nb:  very possibly the active side should initiate this, but
	 * for now, call a passive hook so OSD and friends can create
	 * sessions without actually negotiating
	 */
	passive_setup();
      }
    }

    if (magic & (MSG_MAGIC_TRACE_XCON)) {
      cout << "decode m is " << m->get_type() << std::endl;

      if (m->get_type() == 4) {
	cout << "stop 4 " << std::endl;
      }

      if (m->get_type() == 15) {
	cout << "stop 15 " << std::endl;
      }

      if (m->get_type() == 18) {
	cout << "stop 18 " << std::endl;
      }

      if (m->get_type() == 48) {
	cout << "stop 48 " << std::endl;
      }
    }

    /* dispatch it */
    msgr->ds_dispatch(m);
  } else {
    /* responds for undecoded messages and frees hook */
    cout << "decode m failed" << std::endl;
    m_hook->on_err_finalize(this);
  }

  return 0;
}

int XioConnection::on_msg_send_complete(struct xio_session *session,
					struct xio_msg *rsp,
					void *conn_user_context)
{
  abort(); /* XXX */
} /* on_msg_send_complete */

static uint64_t rcount;

int XioConnection::on_msg_delivered(struct xio_session *session,
				    struct xio_msg *req,
				    int more_in_batch,
				    void *conn_user_context)
{
  /* requester send complete (one-way) */
  uint64_t rc = ++rcount;

  XioMsg* xmsg = static_cast<XioMsg*>(req->user_context);
  if ((rc % 1000000) == 0)
    cout << "xio finished " << rc << " " << time(0) << std::endl;
  if (xmsg)
    xmsg->put();

  return 0;
}  /* on_msg_delivered */

int XioConnection::on_msg_error(struct xio_session *session,
				enum xio_status error,
				struct xio_msg  *msg,
				void *conn_user_context)
{
  XioMsg *xmsg = static_cast<XioMsg*>(msg->user_context);
  if (xmsg)
    xmsg->put();

  return 0;
} /* on_msg_error */
