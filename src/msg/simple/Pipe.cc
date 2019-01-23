// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <limits.h>
#include <poll.h>

#include "msg/Message.h"
#include "Pipe.h"
#include "SimpleMessenger.h"

#include "common/debug.h"
#include "common/errno.h"
#include "common/valgrind.h"

// Below included to get encode_encrypt(); That probably should be in Crypto.h, instead

#include "auth/cephx/CephxProtocol.h"
#include "auth/AuthSessionHandler.h"

#include "include/compat.h"
#include "include/sock_compat.h"
#include "include/random.h"

// Constant to limit starting sequence number to 2^31.  Nothing special about it, just a big number.  PLR
#define SEQ_MASK  0x7fffffff 
#define dout_subsys ceph_subsys_ms

#undef dout_prefix
#define dout_prefix *_dout << *this
ostream& Pipe::_pipe_prefix(std::ostream &out) const {
  return out << "-- " << msgr->get_myaddr_legacy() << " >> " << peer_addr
	     << " pipe(" << this
	     << " sd=" << sd << " :" << port
             << " s=" << state
             << " pgs=" << peer_global_seq
             << " cs=" << connect_seq
             << " l=" << policy.lossy
             << " c=" << connection_state
             << ").";
}

ostream& operator<<(ostream &out, const Pipe &pipe) {
  return pipe._pipe_prefix(out);
}

/**
 * The DelayedDelivery is for injecting delays into Message delivery off
 * the socket. It is only enabled if delays are requested, and if they
 * are then it pulls Messages off the DelayQueue and puts them into the
 * in_q (SimpleMessenger::dispatch_queue).
 * Please note that this probably has issues with Pipe shutdown and
 * replacement semantics. I've tried, but no guarantees.
 */
class Pipe::DelayedDelivery: public Thread {
  Pipe *pipe;
  std::deque< pair<utime_t,Message*> > delay_queue;
  Mutex delay_lock;
  Cond delay_cond;
  int flush_count;
  bool active_flush;
  bool stop_delayed_delivery;
  bool delay_dispatching; // we are in fast dispatch now
  bool stop_fast_dispatching_flag; // we need to stop fast dispatching

public:
  explicit DelayedDelivery(Pipe *p)
    : pipe(p),
      delay_lock("Pipe::DelayedDelivery::delay_lock"), flush_count(0),
      active_flush(false),
      stop_delayed_delivery(false),
      delay_dispatching(false),
      stop_fast_dispatching_flag(false) { }
  ~DelayedDelivery() override {
    discard();
  }
  void *entry() override;
  void queue(utime_t release, Message *m) {
    Mutex::Locker l(delay_lock);
    delay_queue.push_back(make_pair(release, m));
    delay_cond.Signal();
  }
  void discard();
  void flush();
  bool is_flushing() {
    Mutex::Locker l(delay_lock);
    return flush_count > 0 || active_flush;
  }
  void wait_for_flush() {
    Mutex::Locker l(delay_lock);
    while (flush_count > 0 || active_flush)
      delay_cond.Wait(delay_lock);
  }
  void stop() {
    delay_lock.Lock();
    stop_delayed_delivery = true;
    delay_cond.Signal();
    delay_lock.Unlock();
  }
  void steal_for_pipe(Pipe *new_owner) {
    Mutex::Locker l(delay_lock);
    pipe = new_owner;
  }
  /**
   * We need to stop fast dispatching before we need to stop putting
   * normal messages into the DispatchQueue.
   */
  void stop_fast_dispatching();
};

/**************************************
 * Pipe
 */

Pipe::Pipe(SimpleMessenger *r, int st, PipeConnection *con)
  : RefCountedObject(r->cct),
    reader_thread(this),
    writer_thread(this),
    delay_thread(NULL),
    msgr(r),
    conn_id(r->dispatch_queue.get_id()),
    recv_ofs(0),
    recv_len(0),
    sd(-1), port(0),
    peer_type(-1),
    pipe_lock("SimpleMessenger::Pipe::pipe_lock"),
    state(st),
    connection_state(NULL),
    reader_running(false), reader_needs_join(false),
    reader_dispatching(false), notify_on_dispatch_done(false),
    writer_running(false),
    in_q(&(r->dispatch_queue)),
    send_keepalive(false),
    send_keepalive_ack(false),
    connect_seq(0), peer_global_seq(0),
    out_seq(0), in_seq(0), in_seq_acked(0) {
  ANNOTATE_BENIGN_RACE_SIZED(&sd, sizeof(sd), "Pipe socket");
  ANNOTATE_BENIGN_RACE_SIZED(&state, sizeof(state), "Pipe state");
  ANNOTATE_BENIGN_RACE_SIZED(&recv_len, sizeof(recv_len), "Pipe recv_len");
  ANNOTATE_BENIGN_RACE_SIZED(&recv_ofs, sizeof(recv_ofs), "Pipe recv_ofs");
  if (con) {
    connection_state = con;
    connection_state->reset_pipe(this);
  } else {
    connection_state = new PipeConnection(msgr->cct, msgr);
    connection_state->pipe = get();
  }

  randomize_out_seq();

  msgr->timeout = msgr->cct->_conf->ms_tcp_read_timeout * 1000; //convert to ms
  if (msgr->timeout == 0)
    msgr->timeout = -1;

  recv_max_prefetch = msgr->cct->_conf->ms_tcp_prefetch_max_size;
  recv_buf = new char[recv_max_prefetch];
}

Pipe::~Pipe()
{
  ceph_assert(out_q.empty());
  ceph_assert(sent.empty());
  delete delay_thread;
  delete[] recv_buf;
}

void Pipe::handle_ack(uint64_t seq)
{
  lsubdout(msgr->cct, ms, 15) << "reader got ack seq " << seq << dendl;
  // trim sent list
  while (!sent.empty() &&
	 sent.front()->get_seq() <= seq) {
    Message *m = sent.front();
    sent.pop_front();
    lsubdout(msgr->cct, ms, 10) << "reader got ack seq "
				<< seq << " >= " << m->get_seq() << " on " << m << " " << *m << dendl;
    m->put();
  }
}

void Pipe::start_reader()
{
  ceph_assert(pipe_lock.is_locked());
  ceph_assert(!reader_running);
  if (reader_needs_join) {
    reader_thread.join();
    reader_needs_join = false;
  }
  reader_running = true;
  reader_thread.create("ms_pipe_read", msgr->cct->_conf->ms_rwthread_stack_bytes);
}

void Pipe::maybe_start_delay_thread()
{
  if (!delay_thread) {
    auto pos = msgr->cct->_conf.get_val<std::string>("ms_inject_delay_type").find(ceph_entity_type_name(connection_state->peer_type));
    if (pos != string::npos) {
      lsubdout(msgr->cct, ms, 1) << "setting up a delay queue on Pipe " << this << dendl;
      delay_thread = new DelayedDelivery(this);
      delay_thread->create("ms_pipe_delay");
    }
  }
}

void Pipe::start_writer()
{
  ceph_assert(pipe_lock.is_locked());
  ceph_assert(!writer_running);
  writer_running = true;
  writer_thread.create("ms_pipe_write", msgr->cct->_conf->ms_rwthread_stack_bytes);
}

void Pipe::join_reader()
{
  if (!reader_running)
    return;
  cond.Signal();
  pipe_lock.Unlock();
  reader_thread.join();
  pipe_lock.Lock();
  reader_needs_join = false;
}

void Pipe::DelayedDelivery::discard()
{
  lgeneric_subdout(pipe->msgr->cct, ms, 20) << *pipe << "DelayedDelivery::discard" << dendl;
  Mutex::Locker l(delay_lock);
  while (!delay_queue.empty()) {
    Message *m = delay_queue.front().second;
    pipe->in_q->dispatch_throttle_release(m->get_dispatch_throttle_size());
    m->put();
    delay_queue.pop_front();
  }
}

void Pipe::DelayedDelivery::flush()
{
  lgeneric_subdout(pipe->msgr->cct, ms, 20) << *pipe << "DelayedDelivery::flush" << dendl;
  Mutex::Locker l(delay_lock);
  flush_count = delay_queue.size();
  delay_cond.Signal();
}

void *Pipe::DelayedDelivery::entry()
{
  Mutex::Locker locker(delay_lock);
  lgeneric_subdout(pipe->msgr->cct, ms, 20) << *pipe << "DelayedDelivery::entry start" << dendl;

  while (!stop_delayed_delivery) {
    if (delay_queue.empty()) {
      lgeneric_subdout(pipe->msgr->cct, ms, 30) << *pipe << "DelayedDelivery::entry sleeping on delay_cond because delay queue is empty" << dendl;
      delay_cond.Wait(delay_lock);
      continue;
    }
    utime_t release = delay_queue.front().first;
    Message *m = delay_queue.front().second;
    string delay_msg_type = pipe->msgr->cct->_conf->ms_inject_delay_msg_type;
    if (!flush_count &&
        (release > ceph_clock_now() &&
         (delay_msg_type.empty() || m->get_type_name() == delay_msg_type))) {
      lgeneric_subdout(pipe->msgr->cct, ms, 10) << *pipe << "DelayedDelivery::entry sleeping on delay_cond until " << release << dendl;
      delay_cond.WaitUntil(delay_lock, release);
      continue;
    }
    lgeneric_subdout(pipe->msgr->cct, ms, 10) << *pipe << "DelayedDelivery::entry dequeuing message " << m << " for delivery, past " << release << dendl;
    delay_queue.pop_front();
    if (flush_count > 0) {
      --flush_count;
      active_flush = true;
    }
    if (pipe->in_q->can_fast_dispatch(m)) {
      if (!stop_fast_dispatching_flag) {
        delay_dispatching = true;
        delay_lock.Unlock();
        pipe->in_q->fast_dispatch(m);
        delay_lock.Lock();
        delay_dispatching = false;
        if (stop_fast_dispatching_flag) {
          // we need to let the stopping thread proceed
          delay_cond.Signal();
          delay_lock.Unlock();
          delay_lock.Lock();
        }
      }
    } else {
      pipe->in_q->enqueue(m, m->get_priority(), pipe->conn_id);
    }
    active_flush = false;
  }
  lgeneric_subdout(pipe->msgr->cct, ms, 20) << *pipe << "DelayedDelivery::entry stop" << dendl;
  return NULL;
}

void Pipe::DelayedDelivery::stop_fast_dispatching() {
  Mutex::Locker l(delay_lock);
  stop_fast_dispatching_flag = true;
  while (delay_dispatching)
    delay_cond.Wait(delay_lock);
}


int Pipe::accept()
{
  ldout(msgr->cct,10) << "accept" << dendl;
  ceph_assert(pipe_lock.is_locked());
  ceph_assert(state == STATE_ACCEPTING);

  pipe_lock.Unlock();

  // vars
  bufferlist addrs;
  entity_addr_t socket_addr;
  socklen_t len;
  int r;
  char banner[strlen(CEPH_BANNER)+1];
  bufferlist addrbl;
  ceph_msg_connect connect;
  ceph_msg_connect_reply reply;
  Pipe *existing = 0;
  bufferptr bp;
  bufferlist authorizer, authorizer_reply;
  bool authorizer_valid;
  uint64_t feat_missing;
  bool replaced = false;
  // this variable denotes if the connection attempt from peer is a hard 
  // reset or not, it is true if there is an existing connection and the
  // connection sequence from peer is equal to zero
  bool is_reset_from_peer = false;
  CryptoKey session_key;
  int removed; // single-use down below

  // this should roughly mirror pseudocode at
  //  http://ceph.com/wiki/Messaging_protocol
  int reply_tag = 0;
  uint64_t existing_seq = -1;

  // used for reading in the remote acked seq on connect
  uint64_t newly_acked_seq = 0;

  bool need_challenge = false;
  bool had_challenge = false;
  std::unique_ptr<AuthAuthorizerChallenge> authorizer_challenge;

  recv_reset();

  set_socket_options();

  // announce myself.
  r = tcp_write(CEPH_BANNER, strlen(CEPH_BANNER));
  if (r < 0) {
    ldout(msgr->cct,10) << "accept couldn't write banner" << dendl;
    goto fail_unlocked;
  }

  // and my addr
  encode(msgr->my_addr, addrs, 0);  // legacy

  port = msgr->my_addr.get_port();

  // and peer's socket addr (they might not know their ip)
  sockaddr_storage ss;
  len = sizeof(ss);
  r = ::getpeername(sd, (sockaddr*)&ss, &len);
  if (r < 0) {
    ldout(msgr->cct,0) << "accept failed to getpeername " << cpp_strerror(errno) << dendl;
    goto fail_unlocked;
  }
  socket_addr.set_sockaddr((sockaddr*)&ss);
  encode(socket_addr, addrs, 0);  // legacy

  r = tcp_write(addrs.c_str(), addrs.length());
  if (r < 0) {
    ldout(msgr->cct,10) << "accept couldn't write my+peer addr" << dendl;
    goto fail_unlocked;
  }

  ldout(msgr->cct,1) << "accept sd=" << sd << " " << socket_addr << dendl;
  
  // identify peer
  if (tcp_read(banner, strlen(CEPH_BANNER)) < 0) {
    ldout(msgr->cct,10) << "accept couldn't read banner" << dendl;
    goto fail_unlocked;
  }
  if (memcmp(banner, CEPH_BANNER, strlen(CEPH_BANNER))) {
    banner[strlen(CEPH_BANNER)] = 0;
    ldout(msgr->cct,1) << "accept peer sent bad banner '" << banner << "' (should be '" << CEPH_BANNER << "')" << dendl;
    goto fail_unlocked;
  }
  {
    bufferptr tp(sizeof(ceph_entity_addr));
    addrbl.push_back(std::move(tp));
  }
  if (tcp_read(addrbl.c_str(), addrbl.length()) < 0) {
    ldout(msgr->cct,10) << "accept couldn't read peer_addr" << dendl;
    goto fail_unlocked;
  }
  try {
    auto ti = addrbl.cbegin();
    decode(peer_addr, ti);
  } catch (const buffer::error& e) {
    ldout(msgr->cct,2) << __func__ <<  " decode peer_addr failed: " << e.what()
			<< dendl;
    goto fail_unlocked;
  }

  ldout(msgr->cct,10) << "accept peer addr is " << peer_addr << dendl;
  if (peer_addr.is_blank_ip()) {
    // peer apparently doesn't know what ip they have; figure it out for them.
    int port = peer_addr.get_port();
    peer_addr.u = socket_addr.u;
    peer_addr.set_port(port);
    ldout(msgr->cct,0) << "accept peer addr is really " << peer_addr
	    << " (socket is " << socket_addr << ")" << dendl;
  }
  set_peer_addr(peer_addr);  // so that connection_state gets set up
  
  while (1) {
    if (tcp_read((char*)&connect, sizeof(connect)) < 0) {
      ldout(msgr->cct,10) << "accept couldn't read connect" << dendl;
      goto fail_unlocked;
    }

    authorizer.clear();
    if (connect.authorizer_len) {
      bp = buffer::create(connect.authorizer_len);
      if (tcp_read(bp.c_str(), connect.authorizer_len) < 0) {
        ldout(msgr->cct,10) << "accept couldn't read connect authorizer" << dendl;
        goto fail_unlocked;
      }
      authorizer.push_back(std::move(bp));
      authorizer_reply.clear();
    }

    ldout(msgr->cct,20) << "accept got peer connect_seq " << connect.connect_seq
	     << " global_seq " << connect.global_seq
	     << dendl;
    
    msgr->lock.Lock();   // FIXME
    pipe_lock.Lock();
    if (msgr->dispatch_queue.stop)
      goto shutting_down;
    if (state != STATE_ACCEPTING) {
      goto shutting_down;
    }

    // note peer's type, flags
    set_peer_type(connect.host_type);
    policy = msgr->get_policy(connect.host_type);
    ldout(msgr->cct,10) << "accept of host_type " << connect.host_type
			<< ", policy.lossy=" << policy.lossy
			<< " policy.server=" << policy.server
			<< " policy.standby=" << policy.standby
			<< " policy.resetcheck=" << policy.resetcheck
			<< dendl;

    memset(&reply, 0, sizeof(reply));
    reply.protocol_version = msgr->get_proto_version(peer_type, false);
    msgr->lock.Unlock();

    // mismatch?
    ldout(msgr->cct,10) << "accept my proto " << reply.protocol_version
	     << ", their proto " << connect.protocol_version << dendl;
    if (connect.protocol_version != reply.protocol_version) {
      reply.tag = CEPH_MSGR_TAG_BADPROTOVER;
      goto reply;
    }

    // require signatures for cephx?
    if (connect.authorizer_protocol == CEPH_AUTH_CEPHX) {
      if (peer_type == CEPH_ENTITY_TYPE_OSD ||
	  peer_type == CEPH_ENTITY_TYPE_MDS ||
	  peer_type == CEPH_ENTITY_TYPE_MGR) {
	if (msgr->cct->_conf->cephx_require_signatures ||
	    msgr->cct->_conf->cephx_cluster_require_signatures) {
	  ldout(msgr->cct,10) << "using cephx, requiring MSG_AUTH feature bit for cluster" << dendl;
	  policy.features_required |= CEPH_FEATURE_MSG_AUTH;
	}
	if (msgr->cct->_conf->cephx_require_version >= 2 ||
	    msgr->cct->_conf->cephx_cluster_require_version >= 2) {
	  ldout(msgr->cct,10) << "using cephx, requiring cephx v2 feature bit for cluster" << dendl;
	  policy.features_required |= CEPH_FEATUREMASK_CEPHX_V2;
	}
      } else {
	if (msgr->cct->_conf->cephx_require_signatures ||
	    msgr->cct->_conf->cephx_service_require_signatures) {
	  ldout(msgr->cct,10) << "using cephx, requiring MSG_AUTH feature bit for service" << dendl;
	  policy.features_required |= CEPH_FEATURE_MSG_AUTH;
	}
	if (msgr->cct->_conf->cephx_require_version >= 2 ||
	    msgr->cct->_conf->cephx_service_require_version >= 2) {
	  ldout(msgr->cct,10) << "using cephx, requiring cephx v2 feature bit for cluster" << dendl;
	  policy.features_required |= CEPH_FEATUREMASK_CEPHX_V2;
	}
      }
    }

    feat_missing = policy.features_required & ~(uint64_t)connect.features;
    if (feat_missing) {
      ldout(msgr->cct,1) << "peer missing required features " << std::hex << feat_missing << std::dec << dendl;
      reply.tag = CEPH_MSGR_TAG_FEATURES;
      goto reply;
    }
    
    // Check the authorizer.  If not good, bail out.

    pipe_lock.Unlock();

    need_challenge = HAVE_FEATURE(connect.features, CEPHX_V2);
    had_challenge = (bool)authorizer_challenge;
    authorizer_reply.clear();
    if (!msgr->ms_deliver_verify_authorizer(
	  connection_state.get(), peer_type, connect.authorizer_protocol,
	  authorizer,
	  authorizer_reply, authorizer_valid, session_key,
	  nullptr /* connection_secret */,
	  need_challenge ? &authorizer_challenge : nullptr) ||
	!authorizer_valid) {
      pipe_lock.Lock();
      if (state != STATE_ACCEPTING)
	goto shutting_down_msgr_unlocked;
      if (!had_challenge && need_challenge && authorizer_challenge) {
	ldout(msgr->cct,10) << "accept: challenging authorizer "
			    << authorizer_reply.length()
			    << " bytes" << dendl;
	ceph_assert(authorizer_reply.length());
	reply.tag = CEPH_MSGR_TAG_CHALLENGE_AUTHORIZER;
      } else {
	ldout(msgr->cct,0) << "accept: got bad authorizer" << dendl;
	reply.tag = CEPH_MSGR_TAG_BADAUTHORIZER;
      }
      session_security.reset();
      goto reply;
    } 

    // We've verified the authorizer for this pipe, so set up the session security structure.  PLR

    ldout(msgr->cct,10) << "accept:  setting up session_security." << dendl;

  retry_existing_lookup:
    msgr->lock.Lock();
    pipe_lock.Lock();
    if (msgr->dispatch_queue.stop)
      goto shutting_down;
    if (state != STATE_ACCEPTING)
      goto shutting_down;
    
    // existing?
    existing = msgr->_lookup_pipe(peer_addr);
    if (existing) {
      existing->pipe_lock.Lock(true);  // skip lockdep check (we are locking a second Pipe here)
      if (existing->reader_dispatching) {
	/** we need to wait, or we can deadlock if downstream
	 *  fast_dispatchers are (naughtily!) waiting on resources
	 *  held by somebody trying to make use of the SimpleMessenger lock.
	 *  So drop locks, wait, and retry. It just looks like a slow network
	 *  to everybody else.
	 *
	 *  We take a ref to existing here since it might get reaped before we
	 *  wake up (see bug #15870).  We can be confident that it lived until
	 *  locked it since we held the msgr lock from _lookup_pipe through to
	 *  locking existing->lock and checking reader_dispatching.
	 */
	existing->get();
	pipe_lock.Unlock();
	msgr->lock.Unlock();
	existing->notify_on_dispatch_done = true;
	while (existing->reader_dispatching)
	  existing->cond.Wait(existing->pipe_lock);
	existing->pipe_lock.Unlock();
	existing->put();
	existing = nullptr;
	goto retry_existing_lookup;
      }

      if (connect.global_seq < existing->peer_global_seq) {
	ldout(msgr->cct,10) << "accept existing " << existing << ".gseq " << existing->peer_global_seq
		 << " > " << connect.global_seq << ", RETRY_GLOBAL" << dendl;
	reply.tag = CEPH_MSGR_TAG_RETRY_GLOBAL;
	reply.global_seq = existing->peer_global_seq;  // so we can send it below..
	existing->pipe_lock.Unlock();
	msgr->lock.Unlock();
	goto reply;
      } else {
	ldout(msgr->cct,10) << "accept existing " << existing << ".gseq " << existing->peer_global_seq
		 << " <= " << connect.global_seq << ", looks ok" << dendl;
      }
      
      if (existing->policy.lossy) {
	ldout(msgr->cct,0) << "accept replacing existing (lossy) channel (new one lossy="
	        << policy.lossy << ")" << dendl;
	existing->was_session_reset();
	goto replace;
      }

      ldout(msgr->cct,0) << "accept connect_seq " << connect.connect_seq
			 << " vs existing " << existing->connect_seq
			 << " state " << existing->get_state_name() << dendl;

      if (connect.connect_seq == 0 && existing->connect_seq > 0) {
	ldout(msgr->cct,0) << "accept peer reset, then tried to connect to us, replacing" << dendl;
        // this is a hard reset from peer
        is_reset_from_peer = true;
	if (policy.resetcheck)
	  existing->was_session_reset(); // this resets out_queue, msg_ and connect_seq #'s
	goto replace;
      }

      if (connect.connect_seq < existing->connect_seq) {
	// old attempt, or we sent READY but they didn't get it.
	ldout(msgr->cct,10) << "accept existing " << existing << ".cseq " << existing->connect_seq
			    << " > " << connect.connect_seq << ", RETRY_SESSION" << dendl;
	goto retry_session;
      }

      if (connect.connect_seq == existing->connect_seq) {
	// if the existing connection successfully opened, and/or
	// subsequently went to standby, then the peer should bump
	// their connect_seq and retry: this is not a connection race
	// we need to resolve here.
	if (existing->state == STATE_OPEN ||
	    existing->state == STATE_STANDBY) {
	  ldout(msgr->cct,10) << "accept connection race, existing " << existing
			      << ".cseq " << existing->connect_seq
			      << " == " << connect.connect_seq
			      << ", OPEN|STANDBY, RETRY_SESSION" << dendl;
	  goto retry_session;
	}

	// connection race?
	if (peer_addr < msgr->my_addr ||
	    existing->policy.server) {
	  // incoming wins
	  ldout(msgr->cct,10) << "accept connection race, existing " << existing << ".cseq " << existing->connect_seq
		   << " == " << connect.connect_seq << ", or we are server, replacing my attempt" << dendl;
	  if (!(existing->state == STATE_CONNECTING ||
		existing->state == STATE_WAIT))
	    lderr(msgr->cct) << "accept race bad state, would replace, existing="
			     << existing->get_state_name()
			     << " " << existing << ".cseq=" << existing->connect_seq
			     << " == " << connect.connect_seq
			     << dendl;
	  ceph_assert(existing->state == STATE_CONNECTING ||
		 existing->state == STATE_WAIT);
	  goto replace;
	} else {
	  // our existing outgoing wins
	  ldout(msgr->cct,10) << "accept connection race, existing " << existing << ".cseq " << existing->connect_seq
		   << " == " << connect.connect_seq << ", sending WAIT" << dendl;
	  ceph_assert(peer_addr > msgr->my_addr);
	  if (!(existing->state == STATE_CONNECTING))
	    lderr(msgr->cct) << "accept race bad state, would send wait, existing="
			     << existing->get_state_name()
			     << " " << existing << ".cseq=" << existing->connect_seq
			     << " == " << connect.connect_seq
			     << dendl;
	  ceph_assert(existing->state == STATE_CONNECTING);
	  // make sure our outgoing connection will follow through
	  existing->_send_keepalive();
	  reply.tag = CEPH_MSGR_TAG_WAIT;
	  existing->pipe_lock.Unlock();
	  msgr->lock.Unlock();
	  goto reply;
	}
      }

      ceph_assert(connect.connect_seq > existing->connect_seq);
      ceph_assert(connect.global_seq >= existing->peer_global_seq);
      if (policy.resetcheck &&   // RESETSESSION only used by servers; peers do not reset each other
	  existing->connect_seq == 0) {
	ldout(msgr->cct,0) << "accept we reset (peer sent cseq " << connect.connect_seq 
		 << ", " << existing << ".cseq = " << existing->connect_seq
		 << "), sending RESETSESSION" << dendl;
	reply.tag = CEPH_MSGR_TAG_RESETSESSION;
	msgr->lock.Unlock();
	existing->pipe_lock.Unlock();
	goto reply;
      }

      // reconnect
      ldout(msgr->cct,10) << "accept peer sent cseq " << connect.connect_seq
	       << " > " << existing->connect_seq << dendl;
      goto replace;
    } // existing
    else if (connect.connect_seq > 0) {
      // we reset, and they are opening a new session
      ldout(msgr->cct,0) << "accept we reset (peer sent cseq " << connect.connect_seq << "), sending RESETSESSION" << dendl;
      msgr->lock.Unlock();
      reply.tag = CEPH_MSGR_TAG_RESETSESSION;
      goto reply;
    } else {
      // new session
      ldout(msgr->cct,10) << "accept new session" << dendl;
      existing = NULL;
      goto open;
    }
    ceph_abort();

  retry_session:
    ceph_assert(existing->pipe_lock.is_locked());
    ceph_assert(pipe_lock.is_locked());
    reply.tag = CEPH_MSGR_TAG_RETRY_SESSION;
    reply.connect_seq = existing->connect_seq + 1;
    existing->pipe_lock.Unlock();
    msgr->lock.Unlock();
    goto reply;    

  reply:
    ceph_assert(pipe_lock.is_locked());
    reply.features = ((uint64_t)connect.features & policy.features_supported) | policy.features_required;
    reply.authorizer_len = authorizer_reply.length();
    pipe_lock.Unlock();
    r = tcp_write((char*)&reply, sizeof(reply));
    if (r < 0)
      goto fail_unlocked;
    if (reply.authorizer_len) {
      r = tcp_write(authorizer_reply.c_str(), authorizer_reply.length());
      if (r < 0)
	goto fail_unlocked;
    }
  }
  
 replace:
  ceph_assert(existing->pipe_lock.is_locked());
  ceph_assert(pipe_lock.is_locked());
  // if it is a hard reset from peer, we don't need a round-trip to negotiate in/out sequence
  if ((connect.features & CEPH_FEATURE_RECONNECT_SEQ) && !is_reset_from_peer) {
    reply_tag = CEPH_MSGR_TAG_SEQ;
    existing_seq = existing->in_seq;
  }
  ldout(msgr->cct,10) << "accept replacing " << existing << dendl;
  existing->stop();
  existing->unregister_pipe();
  replaced = true;

  if (existing->policy.lossy) {
    // disconnect from the Connection
    ceph_assert(existing->connection_state);
    if (existing->connection_state->clear_pipe(existing))
      msgr->dispatch_queue.queue_reset(existing->connection_state.get());
  } else {
    // queue a reset on the new connection, which we're dumping for the old
    msgr->dispatch_queue.queue_reset(connection_state.get());

    // drop my Connection, and take a ref to the existing one. do not
    // clear existing->connection_state, since read_message and
    // write_message both dereference it without pipe_lock.
    connection_state = existing->connection_state;

    // make existing Connection reference us
    connection_state->reset_pipe(this);

    if (existing->delay_thread) {
      existing->delay_thread->steal_for_pipe(this);
      delay_thread = existing->delay_thread;
      existing->delay_thread = NULL;
      delay_thread->flush();
    }

    // steal incoming queue
    uint64_t replaced_conn_id = conn_id;
    conn_id = existing->conn_id;
    existing->conn_id = replaced_conn_id;

    // reset the in_seq if this is a hard reset from peer,
    // otherwise we respect our original connection's value
    in_seq = is_reset_from_peer ? 0 : existing->in_seq;
    in_seq_acked = in_seq;

    // steal outgoing queue and out_seq
    existing->requeue_sent();
    out_seq = existing->out_seq;
    ldout(msgr->cct,10) << "accept re-queuing on out_seq " << out_seq << " in_seq " << in_seq << dendl;
    for (map<int, list<Message*> >::iterator p = existing->out_q.begin();
         p != existing->out_q.end();
         ++p)
      out_q[p->first].splice(out_q[p->first].begin(), p->second);
  }
  existing->stop_and_wait();
  existing->pipe_lock.Unlock();

 open:
  // open
  ceph_assert(pipe_lock.is_locked());
  connect_seq = connect.connect_seq + 1;
  peer_global_seq = connect.global_seq;
  ceph_assert(state == STATE_ACCEPTING);
  state = STATE_OPEN;
  ldout(msgr->cct,10) << "accept success, connect_seq = " << connect_seq << ", sending READY" << dendl;

  // send READY reply
  reply.tag = (reply_tag ? reply_tag : CEPH_MSGR_TAG_READY);
  reply.features = policy.features_supported;
  reply.global_seq = msgr->get_global_seq();
  reply.connect_seq = connect_seq;
  reply.flags = 0;
  reply.authorizer_len = authorizer_reply.length();
  if (policy.lossy)
    reply.flags = reply.flags | CEPH_MSG_CONNECT_LOSSY;

  connection_state->set_features((uint64_t)reply.features & (uint64_t)connect.features);
  ldout(msgr->cct,10) << "accept features " << connection_state->get_features() << dendl;

  session_security.reset(
      get_auth_session_handler(msgr->cct,
			       connect.authorizer_protocol,
			       session_key,
			       string(), /* connection_secret */
			       connection_state->get_features()));

  // notify
  msgr->dispatch_queue.queue_accept(connection_state.get());
  msgr->ms_deliver_handle_fast_accept(connection_state.get());

  // ok!
  if (msgr->dispatch_queue.stop)
    goto shutting_down;
  removed = msgr->accepting_pipes.erase(this);
  ceph_assert(removed == 1);
  register_pipe();
  msgr->lock.Unlock();
  pipe_lock.Unlock();

  r = tcp_write((char*)&reply, sizeof(reply));
  if (r < 0) {
    goto fail_registered;
  }

  if (reply.authorizer_len) {
    r = tcp_write(authorizer_reply.c_str(), authorizer_reply.length());
    if (r < 0) {
      goto fail_registered;
    }
  }

  if (reply_tag == CEPH_MSGR_TAG_SEQ) {
    if (tcp_write((char*)&existing_seq, sizeof(existing_seq)) < 0) {
      ldout(msgr->cct,2) << "accept write error on in_seq" << dendl;
      goto fail_registered;
    }
    if (tcp_read((char*)&newly_acked_seq, sizeof(newly_acked_seq)) < 0) {
      ldout(msgr->cct,2) << "accept read error on newly_acked_seq" << dendl;
      goto fail_registered;
    }
  }

  pipe_lock.Lock();
  discard_requeued_up_to(newly_acked_seq);
  if (state != STATE_CLOSED) {
    ldout(msgr->cct,10) << "accept starting writer, state " << get_state_name() << dendl;
    start_writer();
  }
  ldout(msgr->cct,20) << "accept done" << dendl;

  maybe_start_delay_thread();

  return 0;   // success.

 fail_registered:
  ldout(msgr->cct, 10) << "accept fault after register" << dendl;

  if (msgr->cct->_conf->ms_inject_internal_delays) {
    ldout(msgr->cct, 10) << " sleep for " << msgr->cct->_conf->ms_inject_internal_delays << dendl;
    utime_t t;
    t.set_from_double(msgr->cct->_conf->ms_inject_internal_delays);
    t.sleep();
  }

 fail_unlocked:
  pipe_lock.Lock();
  if (state != STATE_CLOSED) {
    bool queued = is_queued();
    ldout(msgr->cct, 10) << "  queued = " << (int)queued << dendl;
    if (queued) {
      state = policy.server ? STATE_STANDBY : STATE_CONNECTING;
    } else if (replaced) {
      state = STATE_STANDBY;
    } else {
      state = STATE_CLOSED;
      state_closed = true;
    }
    fault();
    if (queued || replaced)
      start_writer();
  }
  return -1;

 shutting_down:
  msgr->lock.Unlock();
 shutting_down_msgr_unlocked:
  ceph_assert(pipe_lock.is_locked());

  if (msgr->cct->_conf->ms_inject_internal_delays) {
    ldout(msgr->cct, 10) << " sleep for " << msgr->cct->_conf->ms_inject_internal_delays << dendl;
    utime_t t;
    t.set_from_double(msgr->cct->_conf->ms_inject_internal_delays);
    t.sleep();
  }

  state = STATE_CLOSED;
  state_closed = true;
  fault();
  return -1;
}

void Pipe::set_socket_options()
{
  // disable Nagle algorithm?
  if (msgr->cct->_conf->ms_tcp_nodelay) {
    int flag = 1;
    int r = ::setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(flag));
    if (r < 0) {
      r = -errno;
      ldout(msgr->cct,0) << "couldn't set TCP_NODELAY: "
                         << cpp_strerror(r) << dendl;
    }
  }
  if (msgr->cct->_conf->ms_tcp_rcvbuf) {
    int size = msgr->cct->_conf->ms_tcp_rcvbuf;
    int r = ::setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (void*)&size, sizeof(size));
    if (r < 0)  {
      r = -errno;
      ldout(msgr->cct,0) << "couldn't set SO_RCVBUF to " << size
                         << ": " << cpp_strerror(r) << dendl;
    }
  }

  // block ESIGPIPE
#ifdef CEPH_USE_SO_NOSIGPIPE
  int val = 1;
  int r = ::setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, (void*)&val, sizeof(val));
  if (r) {
    r = -errno;
    ldout(msgr->cct,0) << "couldn't set SO_NOSIGPIPE: "
                       << cpp_strerror(r) << dendl;
  }
#endif

#ifdef SO_PRIORITY
  int prio = msgr->get_socket_priority();
  if (prio >= 0) {
    int r = -1;
#ifdef IPTOS_CLASS_CS6
    int iptos = IPTOS_CLASS_CS6;
    int addr_family = 0;
    if (!peer_addr.is_blank_ip()) {
      addr_family = peer_addr.get_family();
    } else {
      addr_family = msgr->get_myaddr_legacy().get_family();
    }
    switch (addr_family) {
    case AF_INET:
      r = ::setsockopt(sd, IPPROTO_IP, IP_TOS, &iptos, sizeof(iptos));
      break;
    case AF_INET6:
      r = ::setsockopt(sd, IPPROTO_IPV6, IPV6_TCLASS, &iptos, sizeof(iptos));
      break;
    default:
      lderr(msgr->cct) << "couldn't set ToS of unknown family ("
		       << addr_family << ")"
		       << " to " << iptos << dendl;
      return;
    }
    if (r < 0) {
      r = -errno;
      ldout(msgr->cct,0) << "couldn't set TOS to " << iptos
			 << ": " << cpp_strerror(r) << dendl;
    }
#endif
    // setsockopt(IPTOS_CLASS_CS6) sets the priority of the socket as 0.
    // See http://goo.gl/QWhvsD and http://goo.gl/laTbjT
    // We need to call setsockopt(SO_PRIORITY) after it.
    r = ::setsockopt(sd, SOL_SOCKET, SO_PRIORITY, &prio, sizeof(prio));
    if (r < 0) {
      r = -errno;
      ldout(msgr->cct,0) << "couldn't set SO_PRIORITY to " << prio
                         << ": " << cpp_strerror(r) << dendl;
    }
  }
#endif
}

int Pipe::connect()
{
  ldout(msgr->cct,10) << "connect " << connect_seq << dendl;
  ceph_assert(pipe_lock.is_locked());

  __u32 cseq = connect_seq;
  __u32 gseq = msgr->get_global_seq();

  // stop reader thread
  join_reader();

  pipe_lock.Unlock();
  
  char tag = -1;
  int rc = -1;
  struct msghdr msg;
  struct iovec msgvec[2];
  int msglen;
  char banner[strlen(CEPH_BANNER) + 1];  // extra byte makes coverity happy
  entity_addr_t paddr;
  entity_addr_t peer_addr_for_me, socket_addr;
  AuthAuthorizer *authorizer = NULL;
  bufferlist addrbl, myaddrbl;
  const auto& conf = msgr->cct->_conf;

  // close old socket.  this is safe because we stopped the reader thread above.
  if (sd >= 0)
    ::close(sd);

  // create socket?
  sd = socket_cloexec(peer_addr.get_family(), SOCK_STREAM, 0);
  if (sd < 0) {
    int e = errno;
    lderr(msgr->cct) << "connect couldn't create socket " << cpp_strerror(e) << dendl;
    rc = -e;
    goto fail;
  }

  recv_reset();

  set_socket_options();

  {
    entity_addr_t addr2bind = msgr->get_myaddr_legacy();
    if (msgr->cct->_conf->ms_bind_before_connect && (!addr2bind.is_blank_ip())) {
      addr2bind.set_port(0);
      int r = ::bind(sd , addr2bind.get_sockaddr(), addr2bind.get_sockaddr_len());
      if (r < 0) {
        ldout(msgr->cct,2) << "client bind error " << ", " << cpp_strerror(errno) << dendl;
        goto fail;
      }
    }
  }

  // connect!
  ldout(msgr->cct,10) << "connecting to " << peer_addr << dendl;
  rc = ::connect(sd, peer_addr.get_sockaddr(), peer_addr.get_sockaddr_len());
  if (rc < 0) {
    int stored_errno = errno;
    ldout(msgr->cct,2) << "connect error " << peer_addr
	     << ", " << cpp_strerror(stored_errno) << dendl;
    if (stored_errno == ECONNREFUSED) {
      ldout(msgr->cct, 2) << "connection refused!" << dendl;
      msgr->dispatch_queue.queue_refused(connection_state.get());
    }
    goto fail;
  }

  // verify banner
  // FIXME: this should be non-blocking, or in some other way verify the banner as we get it.
  rc = tcp_read((char*)&banner, strlen(CEPH_BANNER));
  if (rc < 0) {
    ldout(msgr->cct,2) << "connect couldn't read banner, " << cpp_strerror(rc) << dendl;
    goto fail;
  }
  if (memcmp(banner, CEPH_BANNER, strlen(CEPH_BANNER))) {
    ldout(msgr->cct,0) << "connect protocol error (bad banner) on peer " << peer_addr << dendl;
    goto fail;
  }

  memset(&msg, 0, sizeof(msg));
  msgvec[0].iov_base = banner;
  msgvec[0].iov_len = strlen(CEPH_BANNER);
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 1;
  msglen = msgvec[0].iov_len;
  rc = do_sendmsg(&msg, msglen);
  if (rc < 0) {
    ldout(msgr->cct,2) << "connect couldn't write my banner, " << cpp_strerror(rc) << dendl;
    goto fail;
  }

  // identify peer
  {
#if defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__)
    bufferptr p(sizeof(ceph_entity_addr) * 2);
#else
    int wirelen = sizeof(__u32) * 2 + sizeof(ceph_sockaddr_storage);
    bufferptr p(wirelen * 2);
#endif
    addrbl.push_back(std::move(p));
  }
  rc = tcp_read(addrbl.c_str(), addrbl.length());
  if (rc < 0) {
    ldout(msgr->cct,2) << "connect couldn't read peer addrs, " << cpp_strerror(rc) << dendl;
    goto fail;
  }
  try {
    auto p = addrbl.cbegin();
    decode(paddr, p);
    decode(peer_addr_for_me, p);
  }
  catch (buffer::error& e) {
    ldout(msgr->cct,2) << "connect couldn't decode peer addrs: " << e.what()
		       << dendl;
    goto fail;
  }
  port = peer_addr_for_me.get_port();

  ldout(msgr->cct,20) << "connect read peer addr " << paddr << " on socket " << sd << dendl;
  if (peer_addr != paddr) {
    if (paddr.is_blank_ip() &&
	peer_addr.get_port() == paddr.get_port() &&
	peer_addr.get_nonce() == paddr.get_nonce()) {
      ldout(msgr->cct,0) << "connect claims to be " 
	      << paddr << " not " << peer_addr << " - presumably this is the same node!" << dendl;
    } else {
      ldout(msgr->cct,10) << "connect claims to be "
			  << paddr << " not " << peer_addr << dendl;
      goto fail;
    }
  }

  ldout(msgr->cct,20) << "connect peer addr for me is " << peer_addr_for_me << dendl;

  msgr->learned_addr(peer_addr_for_me);

  encode(msgr->my_addr, myaddrbl, 0);  // legacy

  memset(&msg, 0, sizeof(msg));
  msgvec[0].iov_base = myaddrbl.c_str();
  msgvec[0].iov_len = myaddrbl.length();
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 1;
  msglen = msgvec[0].iov_len;
  rc = do_sendmsg(&msg, msglen);
  if (rc < 0) {
    ldout(msgr->cct,2) << "connect couldn't write my addr, " << cpp_strerror(rc) << dendl;
    goto fail;
  }
  ldout(msgr->cct,10) << "connect sent my addr " << msgr->my_addr << dendl;


  while (1) {
    if (!authorizer) {
      authorizer = msgr->ms_deliver_get_authorizer(peer_type);
    }
    bufferlist authorizer_reply;

    ceph_msg_connect connect;
    connect.features = policy.features_supported;
    connect.host_type = msgr->get_myname().type();
    connect.global_seq = gseq;
    connect.connect_seq = cseq;
    connect.protocol_version = msgr->get_proto_version(peer_type, true);
    connect.authorizer_protocol = authorizer ? authorizer->protocol : 0;
    connect.authorizer_len = authorizer ? authorizer->bl.length() : 0;
    if (authorizer) 
      ldout(msgr->cct,10) << "connect.authorizer_len=" << connect.authorizer_len
	       << " protocol=" << connect.authorizer_protocol << dendl;
    connect.flags = 0;
    if (policy.lossy)
      connect.flags |= CEPH_MSG_CONNECT_LOSSY;  // this is fyi, actually, server decides!
    memset(&msg, 0, sizeof(msg));
    msgvec[0].iov_base = (char*)&connect;
    msgvec[0].iov_len = sizeof(connect);
    msg.msg_iov = msgvec;
    msg.msg_iovlen = 1;
    msglen = msgvec[0].iov_len;
    if (authorizer) {
      msgvec[1].iov_base = authorizer->bl.c_str();
      msgvec[1].iov_len = authorizer->bl.length();
      msg.msg_iovlen++;
      msglen += msgvec[1].iov_len;
    }

    ldout(msgr->cct,10) << "connect sending gseq=" << gseq << " cseq=" << cseq
	     << " proto=" << connect.protocol_version << dendl;
    rc = do_sendmsg(&msg, msglen);
    if (rc < 0) {
      ldout(msgr->cct,2) << "connect couldn't write gseq, cseq, " << cpp_strerror(rc) << dendl;
      goto fail;
    }

    ldout(msgr->cct,20) << "connect wrote (self +) cseq, waiting for reply" << dendl;
    ceph_msg_connect_reply reply;
    rc = tcp_read((char*)&reply, sizeof(reply));
    if (rc < 0) {
      ldout(msgr->cct,2) << "connect read reply " << cpp_strerror(rc) << dendl;
      goto fail;
    }

    ldout(msgr->cct,20) << "connect got reply tag " << (int)reply.tag
			<< " connect_seq " << reply.connect_seq
			<< " global_seq " << reply.global_seq
			<< " proto " << reply.protocol_version
			<< " flags " << (int)reply.flags
			<< " features " << reply.features
			<< dendl;

    authorizer_reply.clear();

    if (reply.authorizer_len) {
      ldout(msgr->cct,10) << "reply.authorizer_len=" << reply.authorizer_len << dendl;
      bufferptr bp = buffer::create(reply.authorizer_len);
      rc = tcp_read(bp.c_str(), reply.authorizer_len);
      if (rc < 0) {
        ldout(msgr->cct,10) << "connect couldn't read connect authorizer_reply" << cpp_strerror(rc) << dendl;
	goto fail;
      }
      authorizer_reply.push_back(bp);
    }

    if (reply.tag == CEPH_MSGR_TAG_CHALLENGE_AUTHORIZER) {
      authorizer->add_challenge(msgr->cct, authorizer_reply);
      ldout(msgr->cct,10) << " got authorizer challenge, " << authorizer_reply.length()
			  << " bytes" << dendl;
      continue;
    }

    if (authorizer) {
      auto iter = authorizer_reply.cbegin();
      if (!authorizer->verify_reply(iter, nullptr /* connection_secret */)) {
        ldout(msgr->cct,0) << "failed verifying authorize reply" << dendl;
	goto fail;
      }
    }

    if (conf->ms_inject_internal_delays) {
      ldout(msgr->cct, 10) << " sleep for " << msgr->cct->_conf->ms_inject_internal_delays << dendl;
      utime_t t;
      t.set_from_double(msgr->cct->_conf->ms_inject_internal_delays);
      t.sleep();
    }

    pipe_lock.Lock();
    if (state != STATE_CONNECTING) {
      ldout(msgr->cct,0) << "connect got RESETSESSION but no longer connecting" << dendl;
      goto stop_locked;
    }

    if (reply.tag == CEPH_MSGR_TAG_FEATURES) {
      ldout(msgr->cct,0) << "connect protocol feature mismatch, my " << std::hex
	      << connect.features << " < peer " << reply.features
	      << " missing " << (reply.features & ~policy.features_supported)
	      << std::dec << dendl;
      goto fail_locked;
    }

    if (reply.tag == CEPH_MSGR_TAG_BADPROTOVER) {
      ldout(msgr->cct,0) << "connect protocol version mismatch, my " << connect.protocol_version
	      << " != " << reply.protocol_version << dendl;
      goto fail_locked;
    }

    if (reply.tag == CEPH_MSGR_TAG_BADAUTHORIZER) {
      ldout(msgr->cct,0) << "connect got BADAUTHORIZER" << dendl;
      goto fail_locked;
    }
    if (reply.tag == CEPH_MSGR_TAG_RESETSESSION) {
      ldout(msgr->cct,0) << "connect got RESETSESSION" << dendl;
      was_session_reset();
      cseq = 0;
      pipe_lock.Unlock();
      continue;
    }
    if (reply.tag == CEPH_MSGR_TAG_RETRY_GLOBAL) {
      gseq = msgr->get_global_seq(reply.global_seq);
      ldout(msgr->cct,10) << "connect got RETRY_GLOBAL " << reply.global_seq
	       << " chose new " << gseq << dendl;
      pipe_lock.Unlock();
      continue;
    }
    if (reply.tag == CEPH_MSGR_TAG_RETRY_SESSION) {
      ceph_assert(reply.connect_seq > connect_seq);
      ldout(msgr->cct,10) << "connect got RETRY_SESSION " << connect_seq
	       << " -> " << reply.connect_seq << dendl;
      cseq = connect_seq = reply.connect_seq;
      pipe_lock.Unlock();
      continue;
    }

    if (reply.tag == CEPH_MSGR_TAG_WAIT) {
      ldout(msgr->cct,3) << "connect got WAIT (connection race)" << dendl;
      state = STATE_WAIT;
      goto stop_locked;
    }

    if (reply.tag == CEPH_MSGR_TAG_READY ||
        reply.tag == CEPH_MSGR_TAG_SEQ) {
      uint64_t feat_missing = policy.features_required & ~(uint64_t)reply.features;
      if (feat_missing) {
	ldout(msgr->cct,1) << "missing required features " << std::hex << feat_missing << std::dec << dendl;
	goto fail_locked;
      }

      if (reply.tag == CEPH_MSGR_TAG_SEQ) {
        ldout(msgr->cct,10) << "got CEPH_MSGR_TAG_SEQ, reading acked_seq and writing in_seq" << dendl;
        uint64_t newly_acked_seq = 0;
        rc = tcp_read((char*)&newly_acked_seq, sizeof(newly_acked_seq));
        if (rc < 0) {
          ldout(msgr->cct,2) << "connect read error on newly_acked_seq" << cpp_strerror(rc) << dendl;
          goto fail_locked;
        }
	ldout(msgr->cct,2) << " got newly_acked_seq " << newly_acked_seq
			   << " vs out_seq " << out_seq << dendl;
	while (newly_acked_seq > out_seq) {
	  Message *m = _get_next_outgoing();
	  ceph_assert(m);
	  ldout(msgr->cct,2) << " discarding previously sent " << m->get_seq()
			     << " " << *m << dendl;
	  ceph_assert(m->get_seq() <= newly_acked_seq);
	  m->put();
	  ++out_seq;
	}
        if (tcp_write((char*)&in_seq, sizeof(in_seq)) < 0) {
          ldout(msgr->cct,2) << "connect write error on in_seq" << dendl;
          goto fail_locked;
        }
      }

      // hooray!
      peer_global_seq = reply.global_seq;
      policy.lossy = reply.flags & CEPH_MSG_CONNECT_LOSSY;
      state = STATE_OPEN;
      connect_seq = cseq + 1;
      ceph_assert(connect_seq == reply.connect_seq);
      backoff = utime_t();
      connection_state->set_features((uint64_t)reply.features & (uint64_t)connect.features);
      ldout(msgr->cct,10) << "connect success " << connect_seq << ", lossy = " << policy.lossy
	       << ", features " << connection_state->get_features() << dendl;
      

      // If we have an authorizer, get a new AuthSessionHandler to deal with ongoing security of the
      // connection.  PLR

      if (authorizer != NULL) {
	session_security.reset(
            get_auth_session_handler(
	      msgr->cct,
	      authorizer->protocol,
	      authorizer->session_key,
	      string() /* connection secret*/,
	      connection_state->get_features()));
      }  else {
        // We have no authorizer, so we shouldn't be applying security to messages in this pipe.  PLR
	session_security.reset();
      }

      msgr->dispatch_queue.queue_connect(connection_state.get());
      msgr->ms_deliver_handle_fast_connect(connection_state.get());
      
      if (!reader_running) {
	ldout(msgr->cct,20) << "connect starting reader" << dendl;
	start_reader();
      }
      maybe_start_delay_thread();
      delete authorizer;
      return 0;
    }
    
    // protocol error
    ldout(msgr->cct,0) << "connect got bad tag " << (int)tag << dendl;
    goto fail_locked;
  }

 fail:
  if (conf->ms_inject_internal_delays) {
    ldout(msgr->cct, 10) << " sleep for " << msgr->cct->_conf->ms_inject_internal_delays << dendl;
    utime_t t;
    t.set_from_double(msgr->cct->_conf->ms_inject_internal_delays);
    t.sleep();
  }

  pipe_lock.Lock();
 fail_locked:
  if (state == STATE_CONNECTING)
    fault();
  else
    ldout(msgr->cct,3) << "connect fault, but state = " << get_state_name()
		       << " != connecting, stopping" << dendl;

 stop_locked:
  delete authorizer;
  return rc;
}

void Pipe::register_pipe()
{
  ldout(msgr->cct,10) << "register_pipe" << dendl;
  ceph_assert(msgr->lock.is_locked());
  Pipe *existing = msgr->_lookup_pipe(peer_addr);
  ceph_assert(existing == NULL);
  msgr->rank_pipe[peer_addr] = this;
}

void Pipe::unregister_pipe()
{
  ceph_assert(msgr->lock.is_locked());
  ceph::unordered_map<entity_addr_t,Pipe*>::iterator p = msgr->rank_pipe.find(peer_addr);
  if (p != msgr->rank_pipe.end() && p->second == this) {
    ldout(msgr->cct,10) << "unregister_pipe" << dendl;
    msgr->rank_pipe.erase(p);
  } else {
    ldout(msgr->cct,10) << "unregister_pipe - not registered" << dendl;
    msgr->accepting_pipes.erase(this);  // somewhat overkill, but safe.
  }
}

void Pipe::join()
{
  ldout(msgr->cct, 20) << "join" << dendl;
  if (writer_thread.is_started())
    writer_thread.join();
  if (reader_thread.is_started())
    reader_thread.join();
  if (delay_thread) {
    ldout(msgr->cct, 20) << "joining delay_thread" << dendl;
    delay_thread->stop();
    delay_thread->join();
  }
}

void Pipe::requeue_sent()
{
  if (sent.empty())
    return;

  list<Message*>& rq = out_q[CEPH_MSG_PRIO_HIGHEST];
  while (!sent.empty()) {
    Message *m = sent.back();
    sent.pop_back();
    ldout(msgr->cct,10) << "requeue_sent " << *m << " for resend seq " << out_seq
			<< " (" << m->get_seq() << ")" << dendl;
    rq.push_front(m);
    out_seq--;
  }
}

void Pipe::discard_requeued_up_to(uint64_t seq)
{
  ldout(msgr->cct, 10) << "discard_requeued_up_to " << seq << dendl;
  if (out_q.count(CEPH_MSG_PRIO_HIGHEST) == 0) {
    out_seq = seq;
    return;
  }
  list<Message*>& rq = out_q[CEPH_MSG_PRIO_HIGHEST];
  while (!rq.empty()) {
    Message *m = rq.front();
    if (m->get_seq() == 0 || m->get_seq() > seq)
      break;
    ldout(msgr->cct,10) << "discard_requeued_up_to " << *m << " for resend seq " << out_seq
			<< " <= " << seq << ", discarding" << dendl;
    m->put();
    rq.pop_front();
    out_seq++;
  }
  if (rq.empty())
    out_q.erase(CEPH_MSG_PRIO_HIGHEST);
}

/*
 * Tears down the Pipe's message queues, and removes them from the DispatchQueue
 * Must hold pipe_lock prior to calling.
 */
void Pipe::discard_out_queue()
{
  ldout(msgr->cct,10) << "discard_queue" << dendl;

  for (list<Message*>::iterator p = sent.begin(); p != sent.end(); ++p) {
    ldout(msgr->cct,20) << "  discard " << *p << dendl;
    (*p)->put();
  }
  sent.clear();
  for (map<int,list<Message*> >::iterator p = out_q.begin(); p != out_q.end(); ++p)
    for (list<Message*>::iterator r = p->second.begin(); r != p->second.end(); ++r) {
      ldout(msgr->cct,20) << "  discard " << *r << dendl;
      (*r)->put();
    }
  out_q.clear();
}

void Pipe::fault(bool onread)
{
  const auto& conf = msgr->cct->_conf;
  ceph_assert(pipe_lock.is_locked());
  cond.Signal();

  if (onread && state == STATE_CONNECTING) {
    ldout(msgr->cct,10) << "fault already connecting, reader shutting down" << dendl;
    return;
  }
  
  ldout(msgr->cct,2) << "fault " << cpp_strerror(errno) << dendl;

  if (state == STATE_CLOSED ||
      state == STATE_CLOSING) {
    ldout(msgr->cct,10) << "fault already closed|closing" << dendl;
    if (connection_state->clear_pipe(this))
      msgr->dispatch_queue.queue_reset(connection_state.get());
    return;
  }

  shutdown_socket();

  // lossy channel?
  if (policy.lossy && state != STATE_CONNECTING) {
    ldout(msgr->cct,10) << "fault on lossy channel, failing" << dendl;

    // disconnect from Connection, and mark it failed.  future messages
    // will be dropped.
    ceph_assert(connection_state);
    stop();
    bool cleared = connection_state->clear_pipe(this);

    // crib locks, blech.  note that Pipe is now STATE_CLOSED and the
    // rank_pipe entry is ignored by others.
    pipe_lock.Unlock();

    if (conf->ms_inject_internal_delays) {
      ldout(msgr->cct, 10) << " sleep for " << msgr->cct->_conf->ms_inject_internal_delays << dendl;
      utime_t t;
      t.set_from_double(msgr->cct->_conf->ms_inject_internal_delays);
      t.sleep();
    }

    msgr->lock.Lock();
    pipe_lock.Lock();
    unregister_pipe();
    msgr->lock.Unlock();

    if (delay_thread)
      delay_thread->discard();
    in_q->discard_queue(conn_id);
    discard_out_queue();
    if (cleared)
      msgr->dispatch_queue.queue_reset(connection_state.get());
    return;
  }

  // queue delayed items immediately
  if (delay_thread)
    delay_thread->flush();

  // requeue sent items
  requeue_sent();

  if (policy.standby && !is_queued()) {
    ldout(msgr->cct,0) << "fault with nothing to send, going to standby" << dendl;
    state = STATE_STANDBY;
    return;
  }

  if (state != STATE_CONNECTING) {
    if (policy.server) {
      ldout(msgr->cct,0) << "fault, server, going to standby" << dendl;
      state = STATE_STANDBY;
    } else {
      ldout(msgr->cct,0) << "fault, initiating reconnect" << dendl;
      connect_seq++;
      state = STATE_CONNECTING;
    }
    backoff = utime_t();
  } else if (backoff == utime_t()) {
    ldout(msgr->cct,0) << "fault" << dendl;
    backoff.set_from_double(conf->ms_initial_backoff);
  } else {
    ldout(msgr->cct,10) << "fault waiting " << backoff << dendl;
    cond.WaitInterval(pipe_lock, backoff);
    backoff += backoff;
    if (backoff > conf->ms_max_backoff)
      backoff.set_from_double(conf->ms_max_backoff);
    ldout(msgr->cct,10) << "fault done waiting or woke up" << dendl;
  }
}

void Pipe::randomize_out_seq()
{
  if (connection_state->get_features() & CEPH_FEATURE_MSG_AUTH) {
    // Set out_seq to a random value, so CRC won't be predictable.
    out_seq = ceph::util::generate_random_number<uint64_t>(0, SEQ_MASK);
    lsubdout(msgr->cct, ms, 10) << "randomize_out_seq " << out_seq << dendl;
  } else {
    // previously, seq #'s always started at 0.
    out_seq = 0;
  }
}

void Pipe::was_session_reset()
{
  ceph_assert(pipe_lock.is_locked());

  ldout(msgr->cct,10) << "was_session_reset" << dendl;
  in_q->discard_queue(conn_id);
  if (delay_thread)
    delay_thread->discard();
  discard_out_queue();

  msgr->dispatch_queue.queue_remote_reset(connection_state.get());

  randomize_out_seq();

  in_seq = 0;
  connect_seq = 0;
}

void Pipe::stop()
{
  ldout(msgr->cct,10) << "stop" << dendl;
  ceph_assert(pipe_lock.is_locked());
  state = STATE_CLOSED;
  state_closed = true;
  cond.Signal();
  shutdown_socket();
}

void Pipe::stop_and_wait()
{
  ceph_assert(pipe_lock.is_locked_by_me());
  if (state != STATE_CLOSED)
    stop();

  if (msgr->cct->_conf->ms_inject_internal_delays) {
    ldout(msgr->cct, 10) << __func__ << " sleep for "
			 << msgr->cct->_conf->ms_inject_internal_delays
			 << dendl;
    utime_t t;
    t.set_from_double(msgr->cct->_conf->ms_inject_internal_delays);
    t.sleep();
  }
  
  if (delay_thread) {
    pipe_lock.Unlock();
    delay_thread->stop_fast_dispatching();
    pipe_lock.Lock();
  }
  while (reader_running &&
	 reader_dispatching)
    cond.Wait(pipe_lock);
}

/* read msgs from socket.
 * also, server.
 */
void Pipe::reader()
{
  pipe_lock.Lock();

  if (state == STATE_ACCEPTING) {
    accept();
    ceph_assert(pipe_lock.is_locked());
  }

  // loop.
  while (state != STATE_CLOSED &&
	 state != STATE_CONNECTING) {
    ceph_assert(pipe_lock.is_locked());

    // sleep if (re)connecting
    if (state == STATE_STANDBY) {
      ldout(msgr->cct,20) << "reader sleeping during reconnect|standby" << dendl;
      cond.Wait(pipe_lock);
      continue;
    }

    // get a reference to the AuthSessionHandler while we have the pipe_lock
    std::shared_ptr<AuthSessionHandler> auth_handler = session_security;

    pipe_lock.Unlock();

    char tag = -1;
    ldout(msgr->cct,20) << "reader reading tag..." << dendl;
    if (tcp_read((char*)&tag, 1) < 0) {
      pipe_lock.Lock();
      ldout(msgr->cct,2) << "reader couldn't read tag, " << cpp_strerror(errno) << dendl;
      fault(true);
      continue;
    }

    if (tag == CEPH_MSGR_TAG_KEEPALIVE) {
      ldout(msgr->cct,2) << "reader got KEEPALIVE" << dendl;
      pipe_lock.Lock();
      connection_state->set_last_keepalive(ceph_clock_now());
      continue;
    }
    if (tag == CEPH_MSGR_TAG_KEEPALIVE2) {
      ldout(msgr->cct,30) << "reader got KEEPALIVE2 tag ..." << dendl;
      ceph_timespec t;
      int rc = tcp_read((char*)&t, sizeof(t));
      pipe_lock.Lock();
      if (rc < 0) {
	ldout(msgr->cct,2) << "reader couldn't read KEEPALIVE2 stamp "
			   << cpp_strerror(errno) << dendl;
	fault(true);
      } else {
	send_keepalive_ack = true;
	keepalive_ack_stamp = utime_t(t);
	ldout(msgr->cct,2) << "reader got KEEPALIVE2 " << keepalive_ack_stamp
			   << dendl;
	connection_state->set_last_keepalive(ceph_clock_now());
	cond.Signal();
      }
      continue;
    }
    if (tag == CEPH_MSGR_TAG_KEEPALIVE2_ACK) {
      ldout(msgr->cct,2) << "reader got KEEPALIVE_ACK" << dendl;
      struct ceph_timespec t;
      int rc = tcp_read((char*)&t, sizeof(t));
      pipe_lock.Lock();
      if (rc < 0) {
	ldout(msgr->cct,2) << "reader couldn't read KEEPALIVE2 stamp " << cpp_strerror(errno) << dendl;
	fault(true);
      } else {
	connection_state->set_last_keepalive_ack(utime_t(t));
      }
      continue;
    }

    // open ...
    if (tag == CEPH_MSGR_TAG_ACK) {
      ldout(msgr->cct,20) << "reader got ACK" << dendl;
      ceph_le64 seq;
      int rc = tcp_read((char*)&seq, sizeof(seq));
      pipe_lock.Lock();
      if (rc < 0) {
	ldout(msgr->cct,2) << "reader couldn't read ack seq, " << cpp_strerror(errno) << dendl;
	fault(true);
      } else if (state != STATE_CLOSED) {
        handle_ack(seq);
      }
      continue;
    }

    else if (tag == CEPH_MSGR_TAG_MSG) {
      ldout(msgr->cct,20) << "reader got MSG" << dendl;
      Message *m = 0;
      int r = read_message(&m, auth_handler.get());

      pipe_lock.Lock();
      
      if (!m) {
	if (r < 0)
	  fault(true);
	continue;
      }

      m->trace.event("pipe read message");

      if (state == STATE_CLOSED ||
	  state == STATE_CONNECTING) {
	in_q->dispatch_throttle_release(m->get_dispatch_throttle_size());
	m->put();
	continue;
      }

      // check received seq#.  if it is old, drop the message.  
      // note that incoming messages may skip ahead.  this is convenient for the client
      // side queueing because messages can't be renumbered, but the (kernel) client will
      // occasionally pull a message out of the sent queue to send elsewhere.  in that case
      // it doesn't matter if we "got" it or not.
      if (m->get_seq() <= in_seq) {
	ldout(msgr->cct,0) << "reader got old message "
		<< m->get_seq() << " <= " << in_seq << " " << m << " " << *m
		<< ", discarding" << dendl;
	in_q->dispatch_throttle_release(m->get_dispatch_throttle_size());
	m->put();
	if (connection_state->has_feature(CEPH_FEATURE_RECONNECT_SEQ) &&
	    msgr->cct->_conf->ms_die_on_old_message)
	  ceph_abort_msg("old msgs despite reconnect_seq feature");
	continue;
      }
      if (m->get_seq() > in_seq + 1) {
	ldout(msgr->cct,0) << "reader missed message?  skipped from seq "
			   << in_seq << " to " << m->get_seq() << dendl;
	if (msgr->cct->_conf->ms_die_on_skipped_message)
	  ceph_abort_msg("skipped incoming seq");
      }

      m->set_connection(connection_state.get());

      // note last received message.
      in_seq = m->get_seq();

      cond.Signal();  // wake up writer, to ack this
      
      ldout(msgr->cct,10) << "reader got message "
	       << m->get_seq() << " " << m << " " << *m
	       << dendl;
      in_q->fast_preprocess(m);

      if (delay_thread) {
        utime_t release;
        if (rand() % 10000 < msgr->cct->_conf->ms_inject_delay_probability * 10000.0) {
          release = m->get_recv_stamp();
          release += msgr->cct->_conf->ms_inject_delay_max * (double)(rand() % 10000) / 10000.0;
          lsubdout(msgr->cct, ms, 1) << "queue_received will delay until " << release << " on " << m << " " << *m << dendl;
        }
        delay_thread->queue(release, m);
      } else {
        if (in_q->can_fast_dispatch(m)) {
	  reader_dispatching = true;
          pipe_lock.Unlock();
          in_q->fast_dispatch(m);
          pipe_lock.Lock();
	  reader_dispatching = false;
	  if (state == STATE_CLOSED ||
	      notify_on_dispatch_done) { // there might be somebody waiting
	    notify_on_dispatch_done = false;
	    cond.Signal();
	  }
        } else {
          in_q->enqueue(m, m->get_priority(), conn_id);
        }
      }
    }
    
    else if (tag == CEPH_MSGR_TAG_CLOSE) {
      ldout(msgr->cct,20) << "reader got CLOSE" << dendl;
      pipe_lock.Lock();
      if (state == STATE_CLOSING) {
	state = STATE_CLOSED;
	state_closed = true;
      } else {
	state = STATE_CLOSING;
      }
      cond.Signal();
      break;
    }
    else {
      ldout(msgr->cct,0) << "reader bad tag " << (int)tag << dendl;
      pipe_lock.Lock();
      fault(true);
    }
  }

 
  // reap?
  reader_running = false;
  reader_needs_join = true;
  unlock_maybe_reap();
  ldout(msgr->cct,10) << "reader done" << dendl;
}

/* write msgs to socket.
 * also, client.
 */
void Pipe::writer()
{
  pipe_lock.Lock();
  while (state != STATE_CLOSED) {// && state != STATE_WAIT) {
    ldout(msgr->cct,10) << "writer: state = " << get_state_name()
			<< " policy.server=" << policy.server << dendl;

    // standby?
    if (is_queued() && state == STATE_STANDBY && !policy.server)
      state = STATE_CONNECTING;

    // connect?
    if (state == STATE_CONNECTING) {
      ceph_assert(!policy.server);
      connect();
      continue;
    }
    
    if (state == STATE_CLOSING) {
      // write close tag
      ldout(msgr->cct,20) << "writer writing CLOSE tag" << dendl;
      char tag = CEPH_MSGR_TAG_CLOSE;
      state = STATE_CLOSED;
      state_closed = true;
      pipe_lock.Unlock();
      if (sd >= 0) {
	// we can ignore return value, actually; we don't care if this succeeds.
	int r = ::write(sd, &tag, 1);
	(void)r;
      }
      pipe_lock.Lock();
      continue;
    }

    if (state != STATE_CONNECTING && state != STATE_WAIT && state != STATE_STANDBY &&
	(is_queued() || in_seq > in_seq_acked)) {

      // keepalive?
      if (send_keepalive) {
	int rc;
	if (connection_state->has_feature(CEPH_FEATURE_MSGR_KEEPALIVE2)) {
	  pipe_lock.Unlock();
	  rc = write_keepalive2(CEPH_MSGR_TAG_KEEPALIVE2,
				ceph_clock_now());
	} else {
	  pipe_lock.Unlock();
	  rc = write_keepalive();
	}
	pipe_lock.Lock();
	if (rc < 0) {
	  ldout(msgr->cct,2) << "writer couldn't write keepalive[2], "
			     << cpp_strerror(errno) << dendl;
	  fault();
 	  continue;
	}
	send_keepalive = false;
      }
      if (send_keepalive_ack) {
	utime_t t = keepalive_ack_stamp;
	pipe_lock.Unlock();
	int rc = write_keepalive2(CEPH_MSGR_TAG_KEEPALIVE2_ACK, t);
	pipe_lock.Lock();
	if (rc < 0) {
	  ldout(msgr->cct,2) << "writer couldn't write keepalive_ack, " << cpp_strerror(errno) << dendl;
	  fault();
	  continue;
	}
	send_keepalive_ack = false;
      }

      // send ack?
      if (in_seq > in_seq_acked) {
	uint64_t send_seq = in_seq;
	pipe_lock.Unlock();
	int rc = write_ack(send_seq);
	pipe_lock.Lock();
	if (rc < 0) {
	  ldout(msgr->cct,2) << "writer couldn't write ack, " << cpp_strerror(errno) << dendl;
	  fault();
 	  continue;
	}
	in_seq_acked = send_seq;
      }

      // grab outgoing message
      Message *m = _get_next_outgoing();
      if (m) {
	m->set_seq(++out_seq);
	if (!policy.lossy) {
	  // put on sent list
	  sent.push_back(m); 
	  m->get();
	}

	// associate message with Connection (for benefit of encode_payload)
	m->set_connection(connection_state.get());

	uint64_t features = connection_state->get_features();

	if (m->empty_payload())
	  ldout(msgr->cct,20) << "writer encoding " << m->get_seq() << " features " << features
			      << " " << m << " " << *m << dendl;
	else
	  ldout(msgr->cct,20) << "writer half-reencoding " << m->get_seq() << " features " << features
			      << " " << m << " " << *m << dendl;

	// encode and copy out of *m
	m->encode(features, msgr->crcflags);

	// prepare everything
	const ceph_msg_header& header = m->get_header();
	const ceph_msg_footer& footer = m->get_footer();

	// Now that we have all the crcs calculated, handle the
	// digital signature for the message, if the pipe has session
	// security set up.  Some session security options do not
	// actually calculate and check the signature, but they should
	// handle the calls to sign_message and check_signature.  PLR
	if (session_security.get() == NULL) {
	  ldout(msgr->cct, 20) << "writer no session security" << dendl;
	} else {
	  if (session_security->sign_message(m)) {
	    ldout(msgr->cct, 20) << "writer failed to sign seq # " << header.seq
				 << "): sig = " << footer.sig << dendl;
	  } else {
	    ldout(msgr->cct, 20) << "writer signed seq # " << header.seq
				 << "): sig = " << footer.sig << dendl;
	  }
	}

	bufferlist blist = m->get_payload();
	blist.append(m->get_middle());
	blist.append(m->get_data());

        pipe_lock.Unlock();

        m->trace.event("pipe writing message");

        ldout(msgr->cct,20) << "writer sending " << m->get_seq() << " " << m << dendl;
	int rc = write_message(header, footer, blist);

	pipe_lock.Lock();
	if (rc < 0) {
          ldout(msgr->cct,1) << "writer error sending " << m << ", "
		  << cpp_strerror(errno) << dendl;
	  fault();
        }
	m->put();
      }
      continue;
    }
    
    // wait
    ldout(msgr->cct,20) << "writer sleeping" << dendl;
    cond.Wait(pipe_lock);
  }
  
  ldout(msgr->cct,20) << "writer finishing" << dendl;

  // reap?
  writer_running = false;
  unlock_maybe_reap();
  ldout(msgr->cct,10) << "writer done" << dendl;
}

void Pipe::unlock_maybe_reap()
{
  if (!reader_running && !writer_running) {
    shutdown_socket();
    pipe_lock.Unlock();
    if (delay_thread && delay_thread->is_flushing()) {
      delay_thread->wait_for_flush();
    }
    msgr->queue_reap(this);
  } else {
    pipe_lock.Unlock();
  }
}

static void alloc_aligned_buffer(bufferlist& data, unsigned len, unsigned off)
{
  // create a buffer to read into that matches the data alignment
  unsigned left = len;
  if (off & ~CEPH_PAGE_MASK) {
    // head
    unsigned head = 0;
    head = std::min<uint64_t>(CEPH_PAGE_SIZE - (off & ~CEPH_PAGE_MASK), left);
    data.push_back(buffer::create(head));
    left -= head;
  }
  unsigned middle = left & CEPH_PAGE_MASK;
  if (middle > 0) {
    data.push_back(buffer::create_small_page_aligned(middle));
    left -= middle;
  }
  if (left) {
    data.push_back(buffer::create(left));
  }
}

int Pipe::read_message(Message **pm, AuthSessionHandler* auth_handler)
{
  int ret = -1;
  // envelope
  //ldout(msgr->cct,10) << "receiver.read_message from sd " << sd  << dendl;
  
  ceph_msg_header header; 
  ceph_msg_footer footer;
  __u32 header_crc = 0;

  if (tcp_read((char*)&header, sizeof(header)) < 0)
    return -1;
  if (msgr->crcflags & MSG_CRC_HEADER) {
    header_crc = ceph_crc32c(0, (unsigned char *)&header, sizeof(header) - sizeof(header.crc));
  }

  ldout(msgr->cct,20) << "reader got envelope type=" << header.type
           << " src " << entity_name_t(header.src)
           << " front=" << header.front_len
	   << " data=" << header.data_len
	   << " off " << header.data_off
           << dendl;

  // verify header crc
  if ((msgr->crcflags & MSG_CRC_HEADER) && header_crc != header.crc) {
    ldout(msgr->cct,0) << "reader got bad header crc " << header_crc << " != " << header.crc << dendl;
    return -1;
  }

  bufferlist front, middle, data;
  int front_len, middle_len;
  unsigned data_len, data_off;
  int aborted;
  Message *message;
  utime_t recv_stamp = ceph_clock_now();

  if (policy.throttler_messages) {
    ldout(msgr->cct,10) << "reader wants " << 1 << " message from policy throttler "
			<< policy.throttler_messages->get_current() << "/"
			<< policy.throttler_messages->get_max() << dendl;
    policy.throttler_messages->get();
  }

  uint64_t message_size = header.front_len + header.middle_len + header.data_len;
  if (message_size) {
    if (policy.throttler_bytes) {
      ldout(msgr->cct,10) << "reader wants " << message_size << " bytes from policy throttler "
	       << policy.throttler_bytes->get_current() << "/"
	       << policy.throttler_bytes->get_max() << dendl;
      policy.throttler_bytes->get(message_size);
    }

    // throttle total bytes waiting for dispatch.  do this _after_ the
    // policy throttle, as this one does not deadlock (unless dispatch
    // blocks indefinitely, which it shouldn't).  in contrast, the
    // policy throttle carries for the lifetime of the message.
    ldout(msgr->cct,10) << "reader wants " << message_size << " from dispatch throttler "
	     << in_q->dispatch_throttler.get_current() << "/"
	     << in_q->dispatch_throttler.get_max() << dendl;
    in_q->dispatch_throttler.get(message_size);
  }

  utime_t throttle_stamp = ceph_clock_now();

  // read front
  front_len = header.front_len;
  if (front_len) {
    bufferptr bp = buffer::create(front_len);
    if (tcp_read(bp.c_str(), front_len) < 0)
      goto out_dethrottle;
    front.push_back(std::move(bp));
    ldout(msgr->cct,20) << "reader got front " << front.length() << dendl;
  }

  // read middle
  middle_len = header.middle_len;
  if (middle_len) {
    bufferptr bp = buffer::create(middle_len);
    if (tcp_read(bp.c_str(), middle_len) < 0)
      goto out_dethrottle;
    middle.push_back(std::move(bp));
    ldout(msgr->cct,20) << "reader got middle " << middle.length() << dendl;
  }


  // read data
  data_len = le32_to_cpu(header.data_len);
  data_off = le32_to_cpu(header.data_off);
  if (data_len) {
    unsigned offset = 0;
    unsigned left = data_len;

    bufferlist newbuf, rxbuf;
    bufferlist::iterator blp;
    int rxbuf_version = 0;
	
    while (left > 0) {
      // wait for data
      if (tcp_read_wait() < 0)
	goto out_dethrottle;

      // get a buffer
      connection_state->lock.Lock();
      map<ceph_tid_t,pair<bufferlist,int> >::iterator p = connection_state->rx_buffers.find(header.tid);
      if (p != connection_state->rx_buffers.end()) {
	if (rxbuf.length() == 0 || p->second.second != rxbuf_version) {
	  ldout(msgr->cct,10) << "reader seleting rx buffer v " << p->second.second
		   << " at offset " << offset
		   << " len " << p->second.first.length() << dendl;
	  rxbuf = p->second.first;
	  rxbuf_version = p->second.second;
	  // make sure it's big enough
	  if (rxbuf.length() < data_len)
	    rxbuf.push_back(buffer::create(data_len - rxbuf.length()));
	  blp = p->second.first.begin();
	  blp.advance(offset);
	}
      } else {
	if (!newbuf.length()) {
	  ldout(msgr->cct,20) << "reader allocating new rx buffer at offset " << offset << dendl;
	  alloc_aligned_buffer(newbuf, data_len, data_off);
	  blp = newbuf.begin();
	  blp.advance(offset);
	}
      }
      bufferptr bp = blp.get_current_ptr();
      int read = std::min(bp.length(), left);
      ldout(msgr->cct,20) << "reader reading nonblocking into " << (void*)bp.c_str() << " len " << bp.length() << dendl;
      ssize_t got = tcp_read_nonblocking(bp.c_str(), read);
      ldout(msgr->cct,30) << "reader read " << got << " of " << read << dendl;
      connection_state->lock.Unlock();
      if (got < 0)
	goto out_dethrottle;
      if (got > 0) {
	blp.advance(static_cast<size_t>(got));
	data.append(bp, 0, got);
	offset += got;
	left -= got;
      } // else we got a signal or something; just loop.
    }
  }

  // footer
  if (connection_state->has_feature(CEPH_FEATURE_MSG_AUTH)) {
    if (tcp_read((char*)&footer, sizeof(footer)) < 0)
      goto out_dethrottle;
  } else {
    ceph_msg_footer_old old_footer;
    if (tcp_read((char*)&old_footer, sizeof(old_footer)) < 0)
      goto out_dethrottle;
    footer.front_crc = old_footer.front_crc;
    footer.middle_crc = old_footer.middle_crc;
    footer.data_crc = old_footer.data_crc;
    footer.sig = 0;
    footer.flags = old_footer.flags;
  }
  
  aborted = (footer.flags & CEPH_MSG_FOOTER_COMPLETE) == 0;
  ldout(msgr->cct,10) << "aborted = " << aborted << dendl;
  if (aborted) {
    ldout(msgr->cct,0) << "reader got " << front.length() << " + " << middle.length() << " + " << data.length()
	    << " byte message.. ABORTED" << dendl;
    ret = 0;
    goto out_dethrottle;
  }

  ldout(msgr->cct,20) << "reader got " << front.length() << " + " << middle.length() << " + " << data.length()
	   << " byte message" << dendl;
  message = decode_message(msgr->cct, msgr->crcflags, header, footer,
                           front, middle, data, connection_state.get());
  if (!message) {
    ret = -EINVAL;
    goto out_dethrottle;
  }

  //
  //  Check the signature if one should be present.  A zero return indicates success. PLR
  //

  if (auth_handler == NULL) {
    ldout(msgr->cct, 10) << "No session security set" << dendl;
  } else {
    if (auth_handler->check_message_signature(message)) {
      ldout(msgr->cct, 0) << "Signature check failed" << dendl;
      message->put();
      ret = -EINVAL;
      goto out_dethrottle;
    } 
  }

  message->set_byte_throttler(policy.throttler_bytes);
  message->set_message_throttler(policy.throttler_messages);

  // store reservation size in message, so we don't get confused
  // by messages entering the dispatch queue through other paths.
  message->set_dispatch_throttle_size(message_size);

  message->set_recv_stamp(recv_stamp);
  message->set_throttle_stamp(throttle_stamp);
  message->set_recv_complete_stamp(ceph_clock_now());

  *pm = message;
  return 0;

 out_dethrottle:
  // release bytes reserved from the throttlers on failure
  if (policy.throttler_messages) {
    ldout(msgr->cct,10) << "reader releasing " << 1 << " message to policy throttler "
			<< policy.throttler_messages->get_current() << "/"
			<< policy.throttler_messages->get_max() << dendl;
    policy.throttler_messages->put();
  }
  if (message_size) {
    if (policy.throttler_bytes) {
      ldout(msgr->cct,10) << "reader releasing " << message_size << " bytes to policy throttler "
			  << policy.throttler_bytes->get_current() << "/"
			  << policy.throttler_bytes->get_max() << dendl;
      policy.throttler_bytes->put(message_size);
    }

    in_q->dispatch_throttle_release(message_size);
  }
  return ret;
}

int Pipe::do_sendmsg(struct msghdr *msg, unsigned len, bool more)
{
  MSGR_SIGPIPE_STOPPER;
  while (len > 0) {
    int r;
    r = ::sendmsg(sd, msg, MSG_NOSIGNAL | (more ? MSG_MORE : 0));
    if (r == 0) 
      ldout(msgr->cct,10) << "do_sendmsg hmm do_sendmsg got r==0!" << dendl;
    if (r < 0) {
      r = -errno; 
      ldout(msgr->cct,1) << "do_sendmsg error " << cpp_strerror(r) << dendl;
      return r;
    }
    if (state == STATE_CLOSED) {
      ldout(msgr->cct,10) << "do_sendmsg oh look, state == CLOSED, giving up" << dendl;
      return -EINTR; // close enough
    }

    len -= r;
    if (len == 0) break;
    
    // hrmph.  trim r bytes off the front of our message.
    ldout(msgr->cct,20) << "do_sendmsg short write did " << r << ", still have " << len << dendl;
    while (r > 0) {
      if (msg->msg_iov[0].iov_len <= (size_t)r) {
	// lose this whole item
	//ldout(msgr->cct,30) << "skipping " << msg->msg_iov[0].iov_len << ", " << (msg->msg_iovlen-1) << " v, " << r << " left" << dendl;
	r -= msg->msg_iov[0].iov_len;
	msg->msg_iov++;
	msg->msg_iovlen--;
      } else {
	// partial!
	//ldout(msgr->cct,30) << "adjusting " << msg->msg_iov[0].iov_len << ", " << msg->msg_iovlen << " v, " << r << " left" << dendl;
	msg->msg_iov[0].iov_base = (char *)msg->msg_iov[0].iov_base + r;
	msg->msg_iov[0].iov_len -= r;
	break;
      }
    }
  }
  return 0;
}


int Pipe::write_ack(uint64_t seq)
{
  ldout(msgr->cct,10) << "write_ack " << seq << dendl;

  char c = CEPH_MSGR_TAG_ACK;
  ceph_le64 s;
  s = seq;

  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  struct iovec msgvec[2];
  msgvec[0].iov_base = &c;
  msgvec[0].iov_len = 1;
  msgvec[1].iov_base = &s;
  msgvec[1].iov_len = sizeof(s);
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 2;
  
  if (do_sendmsg(&msg, 1 + sizeof(s), true) < 0)
    return -1;	
  return 0;
}

int Pipe::write_keepalive()
{
  ldout(msgr->cct,10) << "write_keepalive" << dendl;

  char c = CEPH_MSGR_TAG_KEEPALIVE;

  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  struct iovec msgvec[2];
  msgvec[0].iov_base = &c;
  msgvec[0].iov_len = 1;
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 1;
  
  if (do_sendmsg(&msg, 1) < 0)
    return -1;	
  return 0;
}

int Pipe::write_keepalive2(char tag, const utime_t& t)
{
  ldout(msgr->cct,10) << "write_keepalive2 " << (int)tag << " " << t << dendl;
  struct ceph_timespec ts;
  t.encode_timeval(&ts);
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  struct iovec msgvec[2];
  msgvec[0].iov_base = &tag;
  msgvec[0].iov_len = 1;
  msgvec[1].iov_base = &ts;
  msgvec[1].iov_len = sizeof(ts);
  msg.msg_iov = msgvec;
  msg.msg_iovlen = 2;

  if (do_sendmsg(&msg, 1 + sizeof(ts)) < 0)
    return -1;
  return 0;
}


int Pipe::write_message(const ceph_msg_header& header, const ceph_msg_footer& footer, bufferlist& blist)
{
  int ret;

  // set up msghdr and iovecs
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = msgvec;
  int msglen = 0;
  
  // send tag
  char tag = CEPH_MSGR_TAG_MSG;
  msgvec[msg.msg_iovlen].iov_base = &tag;
  msgvec[msg.msg_iovlen].iov_len = 1;
  msglen++;
  msg.msg_iovlen++;

  // send envelope
  msgvec[msg.msg_iovlen].iov_base = (char*)&header;
  msgvec[msg.msg_iovlen].iov_len = sizeof(header);
  msglen += sizeof(header);
  msg.msg_iovlen++;

  // payload (front+data)
  auto pb = std::cbegin(blist.buffers());
  unsigned b_off = 0;  // carry-over buffer offset, if any
  unsigned bl_pos = 0; // blist pos
  unsigned left = blist.length();

  while (left > 0) {
    unsigned donow = std::min(left, pb->length()-b_off);
    if (donow == 0) {
      ldout(msgr->cct,0) << "donow = " << donow << " left " << left << " pb->length " << pb->length()
                         << " b_off " << b_off << dendl;
    }
    ceph_assert(donow > 0);
    ldout(msgr->cct,30) << " bl_pos " << bl_pos << " b_off " << b_off
	     << " leftinchunk " << left
	     << " buffer len " << pb->length()
	     << " writing " << donow 
	     << dendl;
    
    if (msg.msg_iovlen >= SM_IOV_MAX-2) {
      if (do_sendmsg(&msg, msglen, true))
	goto fail;
      
      // and restart the iov
      msg.msg_iov = msgvec;
      msg.msg_iovlen = 0;
      msglen = 0;
    }
    
    msgvec[msg.msg_iovlen].iov_base = (void*)(pb->c_str()+b_off);
    msgvec[msg.msg_iovlen].iov_len = donow;
    msglen += donow;
    msg.msg_iovlen++;
    
    ceph_assert(left >= donow);
    left -= donow;
    b_off += donow;
    bl_pos += donow;
    if (left == 0)
      break;
    while (b_off == pb->length()) {
      ++pb;
      b_off = 0;
    }
  }
  ceph_assert(left == 0);

  // send footer; if receiver doesn't support signatures, use the old footer format

  ceph_msg_footer_old old_footer;
  if (connection_state->has_feature(CEPH_FEATURE_MSG_AUTH)) {
    msgvec[msg.msg_iovlen].iov_base = (void*)&footer;
    msgvec[msg.msg_iovlen].iov_len = sizeof(footer);
    msglen += sizeof(footer);
    msg.msg_iovlen++;
  } else {
    if (msgr->crcflags & MSG_CRC_HEADER) {
      old_footer.front_crc = footer.front_crc;
      old_footer.middle_crc = footer.middle_crc;
    } else {
	old_footer.front_crc = old_footer.middle_crc = 0;
    }
    old_footer.data_crc = msgr->crcflags & MSG_CRC_DATA ? footer.data_crc : 0;
    old_footer.flags = footer.flags;
    msgvec[msg.msg_iovlen].iov_base = (char*)&old_footer;
    msgvec[msg.msg_iovlen].iov_len = sizeof(old_footer);
    msglen += sizeof(old_footer);
    msg.msg_iovlen++;
  }

  // send
  if (do_sendmsg(&msg, msglen))
    goto fail;

  ret = 0;

 out:
  return ret;

 fail:
  ret = -1;
  goto out;
}


int Pipe::tcp_read(char *buf, unsigned len)
{
  if (sd < 0)
    return -EINVAL;

  while (len > 0) {

    if (msgr->cct->_conf->ms_inject_socket_failures && sd >= 0) {
      if (rand() % msgr->cct->_conf->ms_inject_socket_failures == 0) {
	ldout(msgr->cct, 0) << "injecting socket failure" << dendl;
	::shutdown(sd, SHUT_RDWR);
      }
    }

    if (tcp_read_wait() < 0)
      return -1;

    ssize_t got = tcp_read_nonblocking(buf, len);

    if (got < 0)
      return -1;

    len -= got;
    buf += got;
    //lgeneric_dout(cct, DBL) << "tcp_read got " << got << ", " << len << " left" << dendl;
  }
  return 0;
}

int Pipe::tcp_read_wait()
{
  if (sd < 0)
    return -EINVAL;
  struct pollfd pfd;
  short evmask;
  pfd.fd = sd;
  pfd.events = POLLIN;
#if defined(__linux__)
  pfd.events |= POLLRDHUP;
#endif

  if (has_pending_data())
    return 0;

  int r = poll(&pfd, 1, msgr->timeout);
  if (r < 0)
    return -errno;
  if (r == 0)
    return -EAGAIN;

  evmask = POLLERR | POLLHUP | POLLNVAL;
#if defined(__linux__)
  evmask |= POLLRDHUP;
#endif
  if (pfd.revents & evmask)
    return -1;

  if (!(pfd.revents & POLLIN))
    return -1;

  return 0;
}

ssize_t Pipe::do_recv(char *buf, size_t len, int flags)
{
again:
  ssize_t got = ::recv( sd, buf, len, flags );
  if (got < 0) {
    if (errno == EINTR) {
      goto again;
    }
    ldout(msgr->cct, 10) << __func__ << " socket " << sd << " returned "
		     << got << " " << cpp_strerror(errno) << dendl;
    return -1;
  }
  if (got == 0) {
    return -1;
  }
  return got;
}

ssize_t Pipe::buffered_recv(char *buf, size_t len, int flags)
{
  size_t left = len;
  ssize_t total_recv = 0;
  if (recv_len > recv_ofs) {
    int to_read = std::min(recv_len - recv_ofs, left);
    memcpy(buf, &recv_buf[recv_ofs], to_read);
    recv_ofs += to_read;
    left -= to_read;
    if (left == 0) {
      return to_read;
    }
    buf += to_read;
    total_recv += to_read;
  }

  /* nothing left in the prefetch buffer */

  if (left > recv_max_prefetch) {
    /* this was a large read, we don't prefetch for these */
    ssize_t ret = do_recv(buf, left, flags );
    if (ret < 0) {
      if (total_recv > 0)
        return total_recv;
      return ret;
    }
    total_recv += ret;
    return total_recv;
  }


  ssize_t got = do_recv(recv_buf, recv_max_prefetch, flags);
  if (got < 0) {
    if (total_recv > 0)
      return total_recv;

    return got;
  }

  recv_len = (size_t)got;
  got = std::min(left, (size_t)got);
  memcpy(buf, recv_buf, got);
  recv_ofs = got;
  total_recv += got;
  return total_recv;
}

ssize_t Pipe::tcp_read_nonblocking(char *buf, unsigned len)
{
  ssize_t got = buffered_recv(buf, len, MSG_DONTWAIT );
  if (got < 0) {
    ldout(msgr->cct, 10) << __func__ << " socket " << sd << " returned "
		         << got << " " << cpp_strerror(errno) << dendl;
    return -1;
  }
  if (got == 0) {
    /* poll() said there was data, but we didn't read any - peer
     * sent a FIN.  Maybe POLLRDHUP signals this, but this is
     * standard socket behavior as documented by Stevens.
     */
    return -1;
  }
  return got;
}

int Pipe::tcp_write(const char *buf, unsigned len)
{
  if (sd < 0)
    return -1;
  struct pollfd pfd;
  pfd.fd = sd;
  pfd.events = POLLOUT | POLLHUP | POLLNVAL | POLLERR;
#if defined(__linux__)
  pfd.events |= POLLRDHUP;
#endif

  if (msgr->cct->_conf->ms_inject_socket_failures && sd >= 0) {
    if (rand() % msgr->cct->_conf->ms_inject_socket_failures == 0) {
      ldout(msgr->cct, 0) << "injecting socket failure" << dendl;
      ::shutdown(sd, SHUT_RDWR);
    }
  }

  if (poll(&pfd, 1, -1) < 0)
    return -1;

  if (!(pfd.revents & POLLOUT))
    return -1;

  //lgeneric_dout(cct, DBL) << "tcp_write writing " << len << dendl;
  ceph_assert(len > 0);
  while (len > 0) {
    MSGR_SIGPIPE_STOPPER;
    int did = ::send( sd, buf, len, MSG_NOSIGNAL );
    if (did < 0) {
      //lgeneric_dout(cct, 1) << "tcp_write error did = " << did << " " << cpp_strerror(errno) << dendl;
      //lgeneric_derr(cct, 1) << "tcp_write error did = " << did << " " << cpp_strerror(errno) << dendl;
      return did;
    }
    len -= did;
    buf += did;
    //lgeneric_dout(cct, DBL) << "tcp_write did " << did << ", " << len << " left" << dendl;
  }
  return 0;
}
