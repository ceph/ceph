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
#include "msg/msg_types.h"
#include "auth/none/AuthNoneProtocol.h" // XXX

#include "include/assert.h"
#include "common/dout.h"

extern struct xio_mempool *xio_msgr_mpool;
extern struct xio_mempool *xio_msgr_noreg_mpool;

#define dout_subsys ceph_subsys_xio

void print_xio_msg_hdr(CephContext *cct, const char *tag,
		       const XioMsgHdr &hdr, const struct xio_msg *msg)
{
  if (msg) {
    ldout(cct,4) << tag <<
      " xio msg:" <<
      " sn: " << msg->sn <<
      " timestamp: " << msg->timestamp <<
      dendl;
  }

  ldout(cct,4) << tag <<
    " ceph header: " <<
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
    dendl;

  ldout(cct,4) << tag <<
    " ceph footer: " <<
    " front_crc: " << hdr.ftr->front_crc <<
    " middle_crc: " << hdr.ftr->middle_crc <<
    " data_crc: " << hdr.ftr->data_crc <<
    " sig: " << hdr.ftr->sig <<
    " flags: " << (uint32_t) hdr.ftr->flags <<
    dendl;
}

void print_ceph_msg(CephContext *cct, const char *tag, Message *m)
{
  if (m->get_magic() & (MSG_MAGIC_XIO & MSG_MAGIC_TRACE_DTOR)) {
    ceph_msg_header& header = m->get_header();
    ldout(cct,4) << tag << " header version " << header.version <<
      " compat version " << header.compat_version <<
      dendl;
  }
}

XioConnection::XioConnection(XioMessenger *m, XioConnection::type _type,
			     const entity_inst_t& _peer) :
  Connection(m->cct, m),
  xio_conn_type(_type),
  portal(m->get_portal()),
  connected(false),
  peer(_peer),
  session(NULL),
  conn(NULL),
  magic(m->get_magic()),
  scount(0),
  send_ctr(0),
  in_seq(),
  cstate(this)
{
  pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
  set_peer_type(peer.name.type());
  set_peer_addr(peer.addr);

  Messenger::Policy policy;
  int64_t max_msgs = 0, max_bytes = 0, bytes_opt = 0;
  int xopt;

  policy = m->get_policy(peer_type);

  if (policy.throttler_messages) {
    max_msgs = policy.throttler_messages->get_max();
    ldout(m->cct,4) << "XioMessenger throttle_msgs: " << max_msgs << dendl;
  }

  xopt = m->cct->_conf->xio_queue_depth;
  if (max_msgs > xopt)
    xopt = max_msgs;

  /* set high mark for send, reserved 20% for credits */
  q_high_mark = xopt * 4 / 5;
  q_low_mark = q_high_mark/2;

  /* set send & receive msgs queue depth */
  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_SND_QUEUE_DEPTH_MSGS,
             &xopt, sizeof(xopt));
  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_RCV_QUEUE_DEPTH_MSGS,
             &xopt, sizeof(xopt));

  if (policy.throttler_bytes) {
    max_bytes = policy.throttler_bytes->get_max();
    ldout(m->cct,4) << "XioMessenger throttle_bytes: " << max_bytes << dendl;
  }

  bytes_opt = (2 << 28); /* default: 512 MB */
  if (max_bytes > bytes_opt)
    bytes_opt = max_bytes;

  /* set send & receive total bytes throttle */
  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_SND_QUEUE_DEPTH_BYTES,
             &bytes_opt, sizeof(bytes_opt));
  xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_RCV_QUEUE_DEPTH_BYTES,
             &bytes_opt, sizeof(bytes_opt));

  ldout(m->cct,4) << "Peer type: " << peer.name.type_str() <<
        " throttle_msgs: " << xopt << " throttle_bytes: " << bytes_opt << dendl;

  /* XXXX fake features, aieee! */
  set_features(XIO_ALL_FEATURES);
}

int XioConnection::send_message(Message *m)
{
  XioMessenger *ms = static_cast<XioMessenger*>(get_messenger());
  return ms->_send_message(m, this);
}

void XioConnection::send_keepalive_or_ack(bool ack, const utime_t *tp)
{
  /* If con is not in READY state, we need to queue the request */
  if (cstate.session_state.read() != XioConnection::UP) {
    pthread_spin_lock(&sp);
    if (cstate.session_state.read() != XioConnection::UP) {
      if (ack) {
	outgoing.ack = true;
	outgoing.ack_time = *tp;
      }
      else {
	outgoing.keepalive = true;
      }
      pthread_spin_unlock(&sp);
      return;
    }
    pthread_spin_unlock(&sp);
  }

  send_keepalive_or_ack_internal(ack, tp);
}

void XioConnection::send_keepalive_or_ack_internal(bool ack, const utime_t *tp)
{
  XioCommand *xcmd = pool_alloc_xio_command(this);
  if (! xcmd) {
    /* could happen if Accelio has been shutdown */
    return;
  }

  struct ceph_timespec ts;
  if (ack) {
    assert(tp);
    tp->encode_timeval(&ts);
    xcmd->get_bl_ref().append(CEPH_MSGR_TAG_KEEPALIVE2_ACK);
    xcmd->get_bl_ref().append((char*)&ts, sizeof(ts));
  } else if (has_feature(CEPH_FEATURE_MSGR_KEEPALIVE2)) {
    utime_t t = ceph_clock_now(msgr->cct);
    t.encode_timeval(&ts);
    xcmd->get_bl_ref().append(CEPH_MSGR_TAG_KEEPALIVE2);
    xcmd->get_bl_ref().append((char*)&ts, sizeof(ts));
  } else {
    xcmd->get_bl_ref().append(CEPH_MSGR_TAG_KEEPALIVE);
  }

  const std::list<buffer::ptr>& header = xcmd->get_bl_ref().buffers();
  assert(header.size() == 1);  /* accelio header must be without scatter gather */
  list<bufferptr>::const_iterator pb = header.begin();
  assert(pb->length() < XioMsgHdr::get_max_encoded_length());
  struct xio_msg * msg = xcmd->get_xio_msg();
  msg->out.header.iov_base = (char*) pb->c_str();
  msg->out.header.iov_len = pb->length();

  ldout(msgr->cct,8) << __func__ << " sending command with tag " << (int)(*(char*)msg->out.header.iov_base)
       << " len " << msg->out.header.iov_len << dendl;

  portal->enqueue(this, xcmd);
}


int XioConnection::passive_setup()
{
  /* XXX passive setup is a placeholder for (potentially active-side
     initiated) feature and auth* negotiation */
  static bufferlist authorizer_reply; /* static because fake */
  static CryptoKey session_key; /* ditto */
  bool authorizer_valid;

  XioMessenger *msgr = static_cast<XioMessenger*>(get_messenger());

  // fake an auth buffer
  EntityName name;
  name.set_type(peer.name.type());

  AuthNoneAuthorizer auth;
  auth.build_authorizer(name, peer.name.num());

  /* XXX fake authorizer! */
  msgr->ms_deliver_verify_authorizer(
    this, peer_type, CEPH_AUTH_NONE,
    auth.bl,
    authorizer_reply,
    authorizer_valid,
    session_key);

  /* notify hook */
  msgr->ms_deliver_handle_accept(this);
  msgr->ms_deliver_handle_fast_accept(this);

  /* try to insert in conns_entity_map */
  msgr->try_insert(this);
  return (0);
}

static inline XioDispatchHook* pool_alloc_xio_dispatch_hook(
  XioConnection *xcon, Message *m, XioInSeq& msg_seq)
{
  struct xio_reg_mem mp_mem;
  int e = xpool_alloc(xio_msgr_noreg_mpool,
		      sizeof(XioDispatchHook), &mp_mem);
  if (!!e)
    return NULL;
  XioDispatchHook *xhook = static_cast<XioDispatchHook*>(mp_mem.addr);
  new (xhook) XioDispatchHook(xcon, m, msg_seq, mp_mem);
  return xhook;
}

int XioConnection::handle_data_msg(struct xio_session *session,
			      struct xio_msg *msg,
			      int more_in_batch,
			      void *cb_user_context)
{
  struct xio_msg *tmsg = msg;

  /* XXX Accelio guarantees message ordering at
   * xio_session */

  if (! in_seq.p()) {
    if (!tmsg->in.header.iov_len) {
	ldout(msgr->cct,0) << __func__ << " empty header: packet out of sequence?" << dendl;
	xio_release_msg(msg);
	return 0;
    }
    const size_t sizeof_tag = 1;
    XioMsgCnt msg_cnt(
      buffer::create_static(tmsg->in.header.iov_len-sizeof_tag,
			    ((char*) tmsg->in.header.iov_base)+sizeof_tag));
    ldout(msgr->cct,10) << __func__ << " receive msg " << "tmsg " << tmsg
      << " msg_cnt " << msg_cnt.msg_cnt
      << " iov_base " << tmsg->in.header.iov_base
      << " iov_len " << (int) tmsg->in.header.iov_len
      << " nents " << tmsg->in.pdata_iov.nents
      << " conn " << conn << " sess " << session
      << " sn " << tmsg->sn << dendl;
    assert(session == this->session);
    in_seq.set_count(msg_cnt.msg_cnt);
  } else {
    /* XXX major sequence error */
    assert(! tmsg->in.header.iov_len);
  }

  in_seq.append(msg);
  if (in_seq.count() > 0) {
    return 0;
  }

  XioMessenger *msgr = static_cast<XioMessenger*>(get_messenger());
  XioDispatchHook *m_hook =
    pool_alloc_xio_dispatch_hook(this, NULL /* msg */, in_seq);
  XioInSeq& msg_seq = m_hook->msg_seq;
  in_seq.clear();

  ceph_msg_header header;
  ceph_msg_footer footer;
  buffer::list payload, middle, data;

  const utime_t recv_stamp = ceph_clock_now(msgr->cct);

  ldout(msgr->cct,4) << __func__ << " " << "msg_seq.size()="  << msg_seq.size() <<
    dendl;

  struct xio_msg* msg_iter = msg_seq.begin();
  tmsg = msg_iter;
  XioMsgHdr hdr(header, footer,
		buffer::create_static(tmsg->in.header.iov_len,
				      (char*) tmsg->in.header.iov_base));

  if (magic & (MSG_MAGIC_TRACE_XCON)) {
    if (hdr.hdr->type == 43) {
      print_xio_msg_hdr(msgr->cct, "on_msg", hdr, NULL);
    }
  }

  unsigned int ix, blen, iov_len;
  struct xio_iovec_ex *msg_iov, *iovs;
  uint32_t take_len, left_len = 0;
  char *left_base = NULL;

  ix = 0;
  blen = header.front_len;

  while (blen && (msg_iter != msg_seq.end())) {
    tmsg = msg_iter;
    iov_len = vmsg_sglist_nents(&tmsg->in);
    iovs = vmsg_sglist(&tmsg->in);
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &iovs[ix];

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
      msg_seq.next(&msg_iter);
      ix = 0;
    }
  }

  if (magic & (MSG_MAGIC_TRACE_XCON)) {
    if (hdr.hdr->type == 43) {
      ldout(msgr->cct,4) << "front (payload) dump:";
      payload.hexdump( *_dout );
      *_dout << dendl;
    }
  }

  blen = header.middle_len;

  if (blen && left_len) {
    middle.append(
      buffer::create_msg(left_len, left_base, m_hook));
    left_len = 0;
  }

  while (blen && (msg_iter != msg_seq.end())) {
    tmsg = msg_iter;
    iov_len = vmsg_sglist_nents(&tmsg->in);
    iovs = vmsg_sglist(&tmsg->in);
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &iovs[ix];
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
      msg_seq.next(&msg_iter);
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
    tmsg = msg_iter;
    iov_len = vmsg_sglist_nents(&tmsg->in);
    iovs = vmsg_sglist(&tmsg->in);
    for (; blen && (ix < iov_len); ++ix) {
      msg_iov = &iovs[ix];
      data.append(
	buffer::create_msg(
	  msg_iov->iov_len, (char*) msg_iov->iov_base, m_hook));
      blen -= msg_iov->iov_len;
    }
    if (ix == iov_len) {
      msg_seq.next(&msg_iter);
      ix = 0;
    }
  }

  /* update connection timestamp */
  recv.set(tmsg->timestamp);

  Message *m =
    decode_message(msgr->cct, msgr->crcflags, header, footer, payload, middle,
		   data);

  if (m) {
    /* completion */
    m->set_connection(this);

    /* reply hook */
    m_hook->set_message(m);
    m->set_completion_hook(m_hook);

    /* trace flag */
    m->set_magic(magic);

    /* update timestamps */
    m->set_recv_stamp(recv_stamp);
    m->set_recv_complete_stamp(ceph_clock_now(msgr->cct));
    m->set_seq(header.seq);

    /* MP-SAFE */
    state.set_in_seq(header.seq);

    /* XXXX validate peer type */
    if (peer_type != (int) hdr.peer_type) { /* XXX isn't peer_type -1? */
      peer_type = hdr.peer_type;
      peer_addr = hdr.addr;
      peer.addr = peer_addr;
      peer.name = entity_name_t(hdr.hdr->src);
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
      ldout(msgr->cct,4) << "decode m is " << m->get_type() << dendl;
    }

    /* dispatch it */
    msgr->ds_dispatch(m);
  } else {
    /* responds for undecoded messages and frees hook */
    ldout(msgr->cct,4) << "decode m failed" << dendl;
    m_hook->on_err_finalize(this);
  }

  return 0;
}

int XioConnection::on_msg(struct xio_session *session,
			      struct xio_msg *msg,
			      int more_in_batch,
			      void *cb_user_context)
{
  char tag = CEPH_MSGR_TAG_MSG;
  if (msg->in.header.iov_len)
    tag = *(char*)msg->in.header.iov_base;

  ldout(msgr->cct,8) << __func__ << " receive msg with iov_len "
    << (int) msg->in.header.iov_len << " tag " << (int)tag << dendl;

  //header_len_without_tag is only meaningful in case we have tag
  size_t header_len_without_tag = msg->in.header.iov_len - sizeof(tag);

  switch(tag) {
  case CEPH_MSGR_TAG_MSG:
    ldout(msgr->cct, 20) << __func__ << " got data message" << dendl;
    return handle_data_msg(session, msg, more_in_batch, cb_user_context);

  case CEPH_MSGR_TAG_KEEPALIVE:
    ldout(msgr->cct, 20) << __func__ << " got KEEPALIVE" << dendl;
    set_last_keepalive(ceph_clock_now(nullptr));
    break;

  case CEPH_MSGR_TAG_KEEPALIVE2:
    if (header_len_without_tag < sizeof(ceph_timespec)) {
      lderr(msgr->cct) << __func__ << " too few data for KEEPALIVE2: got " << header_len_without_tag <<
         " bytes instead of " << sizeof(ceph_timespec) << " bytes" << dendl;
    }
    else {
      ceph_timespec *t = (ceph_timespec *) ((char*)msg->in.header.iov_base + sizeof(tag));
      utime_t kp_t = utime_t(*t);
      ldout(msgr->cct, 20) << __func__ << " got KEEPALIVE2 with timestamp" << kp_t << dendl;
      send_keepalive_or_ack(true, &kp_t);
      set_last_keepalive(ceph_clock_now(nullptr));
    }

    break;

  case CEPH_MSGR_TAG_KEEPALIVE2_ACK:
    if (header_len_without_tag < sizeof(ceph_timespec)) {
      lderr(msgr->cct) << __func__ << " too few data for KEEPALIVE2_ACK: got " << header_len_without_tag <<
         " bytes instead of " << sizeof(ceph_timespec) << " bytes" << dendl;
    }
    else {
      ceph_timespec *t = (ceph_timespec *) ((char*)msg->in.header.iov_base + sizeof(tag));
      utime_t kp_t(*t);
      ldout(msgr->cct, 20) << __func__ << " got KEEPALIVE2_ACK with timestamp" << kp_t << dendl;
      set_last_keepalive_ack(kp_t);
    }
    break;

  default:
    lderr(msgr->cct) << __func__ << " unsupported message tag " << (int) tag << dendl;
    assert(! "unsupported message tag");
  }

  xio_release_msg(msg);
  return 0;
}


int XioConnection::on_ow_msg_send_complete(struct xio_session *session,
					   struct xio_msg *req,
					   void *conn_user_context)
{
  /* requester send complete (one-way) */
  uint64_t rc = ++scount;

  XioSend* xsend = static_cast<XioSend*>(req->user_context);
  if (unlikely(magic & MSG_MAGIC_TRACE_CTR)) {
    if (unlikely((rc % 1000000) == 0)) {
      std::cout << "xio finished " << rc << " " << time(0) << std::endl;
    }
  } /* trace ctr */

  ldout(msgr->cct,11) << "on_msg_delivered xcon: " << xsend->xcon <<
    " session: " << session << " msg: " << req << " sn: " << req->sn << dendl;

  XioMsg *xmsg = dynamic_cast<XioMsg*>(xsend);
  if (xmsg) {
    ldout(msgr->cct,11) << "on_msg_delivered xcon: " <<
      " type: " << xmsg->m->get_type() << " tid: " << xmsg->m->get_tid() <<
      " seq: " << xmsg->m->get_seq() << dendl;
  }

  --send_ctr; /* atomic, because portal thread */

  /* unblock flow-controlled connections, avoid oscillation */
  if (unlikely(cstate.session_state.read() ==
	       XioConnection::FLOW_CONTROLLED)) {
    if ((send_ctr <= uint32_t(xio_qdepth_low_mark())) &&
	(1 /* XXX memory <= memory low-water mark */))  {
      cstate.state_up_ready(XioConnection::CState::OP_FLAG_NONE);
      ldout(msgr->cct,2) << "on_msg_delivered xcon: " << xsend->xcon <<
        " session: " << session << " up_ready from flow_controlled" << dendl;
    }
  }

  xsend->put();

  return 0;
}  /* on_msg_delivered */

void XioConnection::msg_send_fail(XioSend *xsend, int code)
{
  ldout(msgr->cct,2) << "xio_send_msg FAILED xcon: " << this <<
    " msg: " << xsend->get_xio_msg() << " code=" << code <<
    " (" << xio_strerror(code) << ")" << dendl;
  /* return refs taken for each xio_msg */
  xsend->put_msg_refs();
} /* msg_send_fail */

void XioConnection::msg_release_fail(struct xio_msg *msg, int code)
{
  ldout(msgr->cct,2) << "xio_release_msg FAILED xcon: " << this <<
    " msg: " << msg <<  "code=" << code <<
    " (" << xio_strerror(code) << ")" << dendl;
} /* msg_release_fail */

int XioConnection::flush_out_queues(uint32_t flags) {
  XioMessenger* msgr = static_cast<XioMessenger*>(get_messenger());
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_lock(&sp);

  if (outgoing.keepalive) {
    outgoing.keepalive = false;
    send_keepalive_or_ack_internal();
  }

  if (outgoing.ack) {
    outgoing.ack = false;
    send_keepalive_or_ack_internal(true, &outgoing.ack_time);
  }

  // send deferred 1 (direct backpresssure)
  if (outgoing.requeue.size() > 0)
    portal->requeue(this, outgoing.requeue);

  // send deferred 2 (sent while deferred)
  int ix, q_size = outgoing.mqueue.size();
  for (ix = 0; ix < q_size; ++ix) {
    Message::Queue::iterator q_iter = outgoing.mqueue.begin();
    Message* m = &(*q_iter);
    outgoing.mqueue.erase(q_iter);
    msgr->_send_message_impl(m, this);
  }
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&sp);
  return 0;
}

int XioConnection::discard_out_queues(uint32_t flags)
{
  Message::Queue disc_q;
  XioSubmit::Queue deferred_q;

  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_lock(&sp);

  /* the two send queues contain different objects:
   * - anything on the mqueue is a Message
   * - anything on the requeue is an XioSend
   */
  Message::Queue::const_iterator i1 = disc_q.end();
  disc_q.splice(i1, outgoing.mqueue);

  XioSubmit::Queue::const_iterator i2 = deferred_q.end();
  deferred_q.splice(i2, outgoing.requeue);

  outgoing.keepalive = outgoing.ack = false;

  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&sp);

  // mqueue
  while (!disc_q.empty()) {
    Message::Queue::iterator q_iter = disc_q.begin();
    Message* m = &(*q_iter);
    disc_q.erase(q_iter);
    m->put();
  }

  // requeue
  while (!deferred_q.empty()) {
    XioSubmit::Queue::iterator q_iter = deferred_q.begin();
    XioSubmit* xs = &(*q_iter);
    XioSend* xsend;
    switch (xs->type) {
      case XioSubmit::OUTGOING_MSG:
	xsend = static_cast<XioSend*>(xs);
	deferred_q.erase(q_iter);
	// release once for each chained xio_msg
	xsend->put(xsend->get_msg_count());
	break;
      case XioSubmit::INCOMING_MSG_RELEASE:
	deferred_q.erase(q_iter);
	portal->release_xio_msg(static_cast<XioRsp*>(xs));
	break;
      default:
	ldout(msgr->cct,0) << __func__ << ": Unknown Msg type " << xs->type << dendl;
	break;
    }
  }

  return 0;
}

int XioConnection::adjust_clru(uint32_t flags)
{
  if (flags & CState::OP_FLAG_LOCKED)
    pthread_spin_unlock(&sp);

  XioMessenger* msgr = static_cast<XioMessenger*>(get_messenger());
  msgr->conns_sp.lock();
  pthread_spin_lock(&sp);

  if (cstate.flags & CState::FLAG_MAPPED) {
    XioConnection::ConnList::iterator citer =
      XioConnection::ConnList::s_iterator_to(*this);
    msgr->conns_list.erase(citer);
    msgr->conns_list.push_front(*this); // LRU
  }

  msgr->conns_sp.unlock();

  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&sp);

  return 0;
}

int XioConnection::on_msg_error(struct xio_session *session,
				enum xio_status error,
				struct xio_msg  *msg,
				void *conn_user_context)
{
  XioSend *xsend = static_cast<XioSend*>(msg->user_context);
  if (xsend)
    xsend->put();

  --send_ctr; /* atomic, because portal thread */
  return 0;
} /* on_msg_error */

void XioConnection::mark_down()
{
  _mark_down(XioConnection::CState::OP_FLAG_NONE);
}

int XioConnection::_mark_down(uint32_t flags)
{
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_lock(&sp);

  // per interface comment, we only stage a remote reset if the
  // current policy required it
  if (cstate.policy.resetcheck)
    cstate.flags |= CState::FLAG_RESET;

  disconnect();

  /* XXX this will almost certainly be called again from
   * on_disconnect_event() */
  discard_out_queues(flags|CState::OP_FLAG_LOCKED);

  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&sp);

  return 0;
}

void XioConnection::mark_disposable()
{
  _mark_disposable(XioConnection::CState::OP_FLAG_NONE);
}

int XioConnection::_mark_disposable(uint32_t flags)
{
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_lock(&sp);

  cstate.policy.lossy = true;

  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&sp);

  return 0;
}

int XioConnection::CState::state_up_ready(uint32_t flags)
{
  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_lock(&xcon->sp);

  xcon->flush_out_queues(flags|CState::OP_FLAG_LOCKED);

  session_state.set(UP);
  startup_state.set(READY);

  if (! (flags & CState::OP_FLAG_LOCKED))
    pthread_spin_unlock(&xcon->sp);

  return (0);
}

int XioConnection::CState::state_discon()
{
  session_state.set(DISCONNECTED);
  startup_state.set(IDLE);

  return 0;
}

int XioConnection::CState::state_flow_controlled(uint32_t flags)
{
  if (! (flags & OP_FLAG_LOCKED))
    pthread_spin_lock(&xcon->sp);

  session_state.set(FLOW_CONTROLLED);

  if (! (flags & OP_FLAG_LOCKED))
    pthread_spin_unlock(&xcon->sp);

  return (0);
}

int XioConnection::CState::state_fail(Message* m, uint32_t flags)
{
  if (! (flags & OP_FLAG_LOCKED))
    pthread_spin_lock(&xcon->sp);

  // advance to state FAIL, drop queued, msgs, adjust LRU
  session_state.set(DISCONNECTED);
  startup_state.set(FAIL);

  xcon->discard_out_queues(flags|OP_FLAG_LOCKED);
  xcon->adjust_clru(flags|OP_FLAG_LOCKED|OP_FLAG_LRU);

  xcon->disconnect();

  if (! (flags & OP_FLAG_LOCKED))
    pthread_spin_unlock(&xcon->sp);

  // notify ULP
  XioMessenger* msgr = static_cast<XioMessenger*>(xcon->get_messenger());
  msgr->ms_deliver_handle_reset(xcon);
  m->put();

  return 0;
}


int XioLoopbackConnection::send_message(Message *m)
{
  XioMessenger *ms = static_cast<XioMessenger*>(get_messenger());
  m->set_connection(this);
  m->set_seq(next_seq());
  m->set_src(ms->get_myinst().name);
  ms->ds_dispatch(m);
  return 0;
}

void XioLoopbackConnection::send_keepalive()
{
  utime_t t = ceph_clock_now(nullptr);
  set_last_keepalive(t);
  set_last_keepalive_ack(t);
}
