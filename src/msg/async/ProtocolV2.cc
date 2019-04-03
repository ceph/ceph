// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <type_traits>

#include "ProtocolV2.h"
#include "AsyncMessenger.h"

#include "common/EventTrace.h"
#include "common/ceph_crypto.h"
#include "common/errno.h"
#include "include/random.h"
#include "auth/AuthClient.h"
#include "auth/AuthServer.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _conn_prefix(_dout)
ostream &ProtocolV2::_conn_prefix(std::ostream *_dout) {
  return *_dout << "--2- " << messenger->get_myaddrs() << " >> "
                << *connection->peer_addrs << " conn(" << connection << " "
                << this
		<< " " << ceph_con_mode_name(auth_meta->con_mode)
		<< " :" << connection->port
                << " s=" << get_state_name(state) << " pgs=" << peer_global_seq
                << " cs=" << connect_seq << " l=" << connection->policy.lossy
                << " rx=" << session_stream_handlers.rx.get()
                << " tx=" << session_stream_handlers.tx.get()
                << ").";
}

using namespace ceph::msgr::v2;

using CtPtr = Ct<ProtocolV2> *;
using CtRef = Ct<ProtocolV2> &;

void ProtocolV2::run_continuation(CtPtr pcontinuation) {
  if (pcontinuation) {
    run_continuation(*pcontinuation);
  }
}

void ProtocolV2::run_continuation(CtRef continuation) {
  try {
    CONTINUATION_RUN(continuation)
  } catch (const buffer::error &e) {
    lderr(cct) << __func__ << " failed decoding of frame header: " << e
               << dendl;
    _fault();
  } catch (const ceph::crypto::onwire::MsgAuthError &e) {
    lderr(cct) << __func__ << " " << e.what() << dendl;
    _fault();
  } catch (const DecryptionError &) {
    lderr(cct) << __func__ << " failed to decrypt frame payload" << dendl;
  }
}

#define WRITE(B, D, C) write(D, CONTINUATION(C), B)

#define READ(L, C) read(CONTINUATION(C), buffer::ptr_node::create(buffer::create(L)))

#define READ_RXBUF(B, C) read(CONTINUATION(C), B)

#ifdef UNIT_TESTS_BUILT

#define INTERCEPT(S) { \
if(connection->interceptor) { \
  auto a = connection->interceptor->intercept(connection, (S)); \
  if (a == Interceptor::ACTION::FAIL) { \
    return _fault(); \
  } else if (a == Interceptor::ACTION::STOP) { \
    stop(); \
    connection->dispatch_queue->queue_reset(connection); \
    return nullptr; \
  }}}
  
#else
#define INTERCEPT(S)
#endif

ProtocolV2::ProtocolV2(AsyncConnection *connection)
    : Protocol(2, connection),
      state(NONE),
      peer_required_features(0),
      client_cookie(0),
      server_cookie(0),
      global_seq(0),
      connect_seq(0),
      peer_global_seq(0),
      message_seq(0),
      reconnecting(false),
      replacing(false),
      can_write(false),
      bannerExchangeCallback(nullptr),
      next_tag(static_cast<Tag>(0)),
      keepalive(false) {
}

ProtocolV2::~ProtocolV2() {
}

void ProtocolV2::connect() {
  ldout(cct, 1) << __func__ << dendl;
  state = START_CONNECT;
  pre_auth.enabled = true;
}

void ProtocolV2::accept() {
  ldout(cct, 1) << __func__ << dendl;
  state = START_ACCEPT;
}

bool ProtocolV2::is_connected() { return can_write; }

/*
 * Tears down the message queues, and removes them from the
 * DispatchQueue Must hold write_lock prior to calling.
 */
void ProtocolV2::discard_out_queue() {
  ldout(cct, 10) << __func__ << " started" << dendl;

  for (list<Message *>::iterator p = sent.begin(); p != sent.end(); ++p) {
    ldout(cct, 20) << __func__ << " discard " << *p << dendl;
    (*p)->put();
  }
  sent.clear();
  for (auto& [ prio, entries ] : out_queue) {
    static_cast<void>(prio);
    for (auto& entry : entries) {
      ldout(cct, 20) << __func__ << " discard " << *entry.m << dendl;
      entry.m->put();
    }
  }
  out_queue.clear();
}

void ProtocolV2::reset_session() {
  ldout(cct, 1) << __func__ << dendl;

  std::lock_guard<std::mutex> l(connection->write_lock);
  if (connection->delay_state) {
    connection->delay_state->discard();
  }

  connection->dispatch_queue->discard_queue(connection->conn_id);
  discard_out_queue();
  connection->outcoming_bl.clear();

  connection->dispatch_queue->queue_remote_reset(connection);

  out_seq = 0;
  in_seq = 0;
  client_cookie = 0;
  server_cookie = 0;
  connect_seq = 0;
  peer_global_seq = 0;
  message_seq = 0;
  ack_left = 0;
  can_write = false;
}

void ProtocolV2::stop() {
  ldout(cct, 1) << __func__ << dendl;
  if (state == CLOSED) {
    return;
  }

  if (connection->delay_state) connection->delay_state->flush();

  std::lock_guard<std::mutex> l(connection->write_lock);

  reset_recv_state();
  discard_out_queue();

  connection->_stop();

  can_write = false;
  state = CLOSED;
}

void ProtocolV2::fault() { _fault(); }

void ProtocolV2::requeue_sent() {
  if (sent.empty()) {
    return;
  }

  auto& rq = out_queue[CEPH_MSG_PRIO_HIGHEST];
  out_seq -= sent.size();
  while (!sent.empty()) {
    Message *m = sent.back();
    sent.pop_back();
    ldout(cct, 5) << __func__ << " requeueing message m=" << m
                  << " seq=" << m->get_seq() << " type=" << m->get_type() << " "
                  << *m << dendl;
    rq.emplace_front(out_queue_entry_t{false, m});
  }
}

uint64_t ProtocolV2::discard_requeued_up_to(uint64_t out_seq, uint64_t seq) {
  ldout(cct, 10) << __func__ << " " << seq << dendl;
  std::lock_guard<std::mutex> l(connection->write_lock);
  if (out_queue.count(CEPH_MSG_PRIO_HIGHEST) == 0) {
    return seq;
  }
  auto& rq = out_queue[CEPH_MSG_PRIO_HIGHEST];
  uint64_t count = out_seq;
  while (!rq.empty()) {
    Message* const m = rq.front().m;
    if (m->get_seq() == 0 || m->get_seq() > seq) break;
    ldout(cct, 5) << __func__ << " discarding message m=" << m
                  << " seq=" << m->get_seq() << " ack_seq=" << seq << " "
                  << *m << dendl;
    m->put();
    rq.pop_front();
    count++;
  }
  if (rq.empty()) out_queue.erase(CEPH_MSG_PRIO_HIGHEST);
  return count;
}

void ProtocolV2::reset_recv_state() {
  if ((state >= AUTH_CONNECTING && state <= SESSION_RECONNECTING) ||
      state == READY) {
    auth_meta.reset(new AuthConnectionMeta);
    session_stream_handlers.tx.reset(nullptr);
    session_stream_handlers.rx.reset(nullptr);
    pre_auth.txbuf.clear();
    pre_auth.rxbuf.clear();
  }

  // clean read and write callbacks
  connection->pendingReadLen.reset();
  connection->writeCallback.reset();

  next_tag = static_cast<Tag>(0);

  reset_throttle();
}

size_t ProtocolV2::get_current_msg_size() const {
  ceph_assert(!rx_segments_desc.empty());
  size_t sum = 0;
  // we don't include SegmentIndex::Msg::HEADER.
  for (__u8 idx = 1; idx < rx_segments_desc.size(); idx++) {
    sum += rx_segments_desc[idx].length;
  }
  return sum;
}

void ProtocolV2::reset_throttle() {
  if (state > THROTTLE_MESSAGE && state <= THROTTLE_DONE &&
      connection->policy.throttler_messages) {
    ldout(cct, 10) << __func__ << " releasing " << 1
                   << " message to policy throttler "
                   << connection->policy.throttler_messages->get_current()
                   << "/" << connection->policy.throttler_messages->get_max()
                   << dendl;
    connection->policy.throttler_messages->put();
  }
  if (state > THROTTLE_BYTES && state <= THROTTLE_DONE) {
    if (connection->policy.throttler_bytes) {
      const size_t cur_msg_size = get_current_msg_size();
      ldout(cct, 10) << __func__ << " releasing " << cur_msg_size
                     << " bytes to policy throttler "
                     << connection->policy.throttler_bytes->get_current() << "/"
                     << connection->policy.throttler_bytes->get_max() << dendl;
      connection->policy.throttler_bytes->put(cur_msg_size);
    }
  }
  if (state > THROTTLE_DISPATCH_QUEUE && state <= THROTTLE_DONE) {
    const size_t cur_msg_size = get_current_msg_size();
    ldout(cct, 10)
        << __func__ << " releasing " << cur_msg_size
        << " bytes to dispatch_queue throttler "
        << connection->dispatch_queue->dispatch_throttler.get_current() << "/"
        << connection->dispatch_queue->dispatch_throttler.get_max() << dendl;
    connection->dispatch_queue->dispatch_throttle_release(cur_msg_size);
  }
}

CtPtr ProtocolV2::_fault() {
  ldout(cct, 10) << __func__ << dendl;

  if (state == CLOSED || state == NONE) {
    ldout(cct, 10) << __func__ << " connection is already closed" << dendl;
    return nullptr;
  }

  if (connection->policy.lossy &&
      !(state >= START_CONNECT && state <= SESSION_RECONNECTING)) {
    ldout(cct, 2) << __func__ << " on lossy channel, failing" << dendl;
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return nullptr;
  }

  connection->write_lock.lock();

  can_write = false;
  // requeue sent items
  requeue_sent();

  if (out_queue.empty() && state >= START_ACCEPT &&
      state <= SESSION_ACCEPTING && !replacing) {
    ldout(cct, 2) << __func__ << " with nothing to send and in the half "
                   << " accept state just closed" << dendl;
    connection->write_lock.unlock();
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return nullptr;
  }

  replacing = false;
  connection->fault();
  reset_recv_state();

  reconnecting = false;

  if (connection->policy.standby && out_queue.empty() && !keepalive &&
      state != WAIT) {
    ldout(cct, 1) << __func__ << " with nothing to send, going to standby"
                  << dendl;
    state = STANDBY;
    connection->write_lock.unlock();
    return nullptr;
  }
  if (connection->policy.server) {
    ldout(cct, 1) << __func__ << " server, going to standby, even though i have stuff queued" << dendl;
    state = STANDBY;
    connection->write_lock.unlock();
    return nullptr;
  }

  connection->write_lock.unlock();

  if (!(state >= START_CONNECT && state <= SESSION_RECONNECTING) &&
      state != WAIT &&
      state != SESSION_ACCEPTING /* due to connection race */) {
    // policy maybe empty when state is in accept
    if (connection->policy.server) {
      ldout(cct, 1) << __func__ << " server, going to standby" << dendl;
      state = STANDBY;
    } else {
      ldout(cct, 1) << __func__ << " initiating reconnect" << dendl;
      connect_seq++;
      global_seq = messenger->get_global_seq();
      state = START_CONNECT;
      pre_auth.enabled = true;
      connection->state = AsyncConnection::STATE_CONNECTING;
    }
    backoff = utime_t();
    connection->center->dispatch_event_external(connection->read_handler);
  } else {
    if (state == WAIT) {
      backoff.set_from_double(cct->_conf->ms_max_backoff);
    } else if (backoff == utime_t()) {
      backoff.set_from_double(cct->_conf->ms_initial_backoff);
    } else {
      backoff += backoff;
      if (backoff > cct->_conf->ms_max_backoff)
        backoff.set_from_double(cct->_conf->ms_max_backoff);
    }

    if (server_cookie) {
      connect_seq++;
    }

    global_seq = messenger->get_global_seq();
    state = START_CONNECT;
    pre_auth.enabled = true;
    connection->state = AsyncConnection::STATE_CONNECTING;
    ldout(cct, 1) << __func__ << " waiting " << backoff << dendl;
    // woke up again;
    connection->register_time_events.insert(
        connection->center->create_time_event(backoff.to_nsec() / 1000,
                                              connection->wakeup_handler));
  }
  return nullptr;
}

void ProtocolV2::prepare_send_message(uint64_t features,
				      Message *m) {
  ldout(cct, 20) << __func__ << " m=" << *m << dendl;

  // associate message with Connection (for benefit of encode_payload)
  ldout(cct, 20) << __func__ << (m->empty_payload() ? " encoding features " : " half-reencoding features ")
		 << features << " " << m  << " " << *m << dendl;

  // encode and copy out of *m
  m->encode(features, 0);
}

void ProtocolV2::send_message(Message *m) {
  uint64_t f = connection->get_features();

  // TODO: Currently not all messages supports reencode like MOSDMap, so here
  // only let fast dispatch support messages prepare message
  const bool can_fast_prepare = messenger->ms_can_fast_dispatch(m);
  if (can_fast_prepare) {
    prepare_send_message(f, m);
  }

  std::lock_guard<std::mutex> l(connection->write_lock);
  bool is_prepared = can_fast_prepare;
  // "features" changes will change the payload encoding
  if (can_fast_prepare && (!can_write || connection->get_features() != f)) {
    // ensure the correctness of message encoding
    m->clear_payload();
    is_prepared = false;
    ldout(cct, 10) << __func__ << " clear encoded buffer previous " << f
                   << " != " << connection->get_features() << dendl;
  }
  if (state == CLOSED) {
    ldout(cct, 10) << __func__ << " connection closed."
                   << " Drop message " << m << dendl;
    m->put();
  } else {
    ldout(cct, 5) << __func__ << " enqueueing message m=" << m
                  << " type=" << m->get_type() << " " << *m << dendl;
    m->trace.event("async enqueueing message");
    out_queue[m->get_priority()].emplace_back(
      out_queue_entry_t{is_prepared, m});
    ldout(cct, 15) << __func__ << " inline write is denied, reschedule m=" << m
                   << dendl;
    if ((!replacing && can_write) || state == STANDBY) {
      connection->center->dispatch_event_external(connection->write_handler);
    }
  }
}

void ProtocolV2::send_keepalive() {
  ldout(cct, 10) << __func__ << dendl;
  std::lock_guard<std::mutex> l(connection->write_lock);
  if (state != CLOSED) {
    keepalive = true;
    connection->center->dispatch_event_external(connection->write_handler);
  }
}

void ProtocolV2::read_event() {
  ldout(cct, 20) << __func__ << dendl;

  switch (state) {
    case START_CONNECT:
      run_continuation(CONTINUATION(start_client_banner_exchange));
      break;
    case START_ACCEPT:
      run_continuation(CONTINUATION(start_server_banner_exchange));
      break;
    case READY:
      run_continuation(CONTINUATION(read_frame));
      break;
    case THROTTLE_MESSAGE:
      run_continuation(CONTINUATION(throttle_message));
      break;
    case THROTTLE_BYTES:
      run_continuation(CONTINUATION(throttle_bytes));
      break;
    case THROTTLE_DISPATCH_QUEUE:
      run_continuation(CONTINUATION(throttle_dispatch_queue));
      break;
    default:
      break;
  }
}

ProtocolV2::out_queue_entry_t ProtocolV2::_get_next_outgoing() {
  out_queue_entry_t out_entry;

  if (!out_queue.empty()) {
    auto it = out_queue.rbegin();
    auto& entries = it->second;
    ceph_assert(!entries.empty());
    out_entry = entries.front();
    entries.pop_front();
    if (entries.empty()) {
      out_queue.erase(it->first);
    }
  }
  return out_entry;
}

ssize_t ProtocolV2::write_message(Message *m, bool more) {
  FUNCTRACE(cct);
  ceph_assert(connection->center->in_thread());
  m->set_seq(++out_seq);

  connection->lock.lock();
  uint64_t ack_seq = in_seq;
  ack_left = 0;
  connection->lock.unlock();

  ceph_msg_header &header = m->get_header();
  ceph_msg_footer &footer = m->get_footer();

  ceph_msg_header2 header2{header.seq,        header.tid,
                           header.type,       header.priority,
                           header.version,
                           0,                 header.data_off,
                           ack_seq,
                           footer.flags,      header.compat_version,
                           header.reserved};

  auto message = MessageFrame::Encode(
			     header2,
			     m->get_payload(),
			     m->get_middle(),
			     m->get_data());
  connection->outcoming_bl.append(message.get_buffer(session_stream_handlers));

  ldout(cct, 5) << __func__ << " sending message m=" << m
                << " seq=" << m->get_seq() << " " << *m << dendl;

  m->trace.event("async writing message");
  ldout(cct, 20) << __func__ << " sending m=" << m << " seq=" << m->get_seq()
                 << " src=" << entity_name_t(messenger->get_myname())
                 << " off=" << header2.data_off
                 << dendl;
  ssize_t total_send_size = connection->outcoming_bl.length();
  ssize_t rc = connection->_try_send(more);
  if (rc < 0) {
    ldout(cct, 1) << __func__ << " error sending " << m << ", "
                  << cpp_strerror(rc) << dendl;
  } else {
    connection->logger->inc(
        l_msgr_send_bytes, total_send_size - connection->outcoming_bl.length());
    ldout(cct, 10) << __func__ << " sending " << m
                   << (rc ? " continuely." : " done.") << dendl;
  }

#if defined(WITH_LTTNG) && defined(WITH_EVENTTRACE)
  if (m->get_type() == CEPH_MSG_OSD_OP)
    OID_EVENT_TRACE_WITH_MSG(m, "SEND_MSG_OSD_OP_END", false);
  else if (m->get_type() == CEPH_MSG_OSD_OPREPLY)
    OID_EVENT_TRACE_WITH_MSG(m, "SEND_MSG_OSD_OPREPLY_END", false);
#endif
  m->put();

  return rc;
}

void ProtocolV2::append_keepalive() {
  ldout(cct, 10) << __func__ << dendl;
  auto keepalive_frame = KeepAliveFrame::Encode();
  connection->outcoming_bl.append(keepalive_frame.get_buffer(session_stream_handlers));
}

void ProtocolV2::append_keepalive_ack(utime_t &timestamp) {
  auto keepalive_ack_frame = KeepAliveFrameAck::Encode(timestamp);
  connection->outcoming_bl.append(keepalive_ack_frame.get_buffer(session_stream_handlers));
}

void ProtocolV2::handle_message_ack(uint64_t seq) {
  if (connection->policy.lossy) {  // lossy connections don't keep sent messages
    return;
  }

  ldout(cct, 15) << __func__ << " seq=" << seq << dendl;

  // trim sent list
  static const int max_pending = 128;
  int i = 0;
  Message *pending[max_pending];
  connection->write_lock.lock();
  while (!sent.empty() && sent.front()->get_seq() <= seq && i < max_pending) {
    Message *m = sent.front();
    sent.pop_front();
    pending[i++] = m;
    ldout(cct, 10) << __func__ << " got ack seq " << seq
                   << " >= " << m->get_seq() << " on " << m << " " << *m
                   << dendl;
  }
  connection->write_lock.unlock();
  for (int k = 0; k < i; k++) {
    pending[k]->put();
  }
}

void ProtocolV2::write_event() {
  ldout(cct, 10) << __func__ << dendl;
  ssize_t r = 0;

  connection->write_lock.lock();
  if (can_write) {
    if (keepalive) {
      append_keepalive();
      keepalive = false;
    }

    auto start = ceph::mono_clock::now();
    bool more;
    do {
      const auto out_entry = _get_next_outgoing();
      if (!out_entry.m) {
        break;
      }

      if (!connection->policy.lossy) {
        // put on sent list
        sent.push_back(out_entry.m);
        out_entry.m->get();
      }
      more = !out_queue.empty();
      connection->write_lock.unlock();

      // send_message or requeue messages may not encode message
      if (!out_entry.is_prepared) {
        prepare_send_message(connection->get_features(), out_entry.m);
      }

      r = write_message(out_entry.m, more);

      connection->write_lock.lock();
      if (r == 0) {
        ;
      } else if (r < 0) {
        ldout(cct, 1) << __func__ << " send msg failed" << dendl;
        break;
      } else if (r > 0)
        break;
    } while (can_write);

    // if r > 0 mean data still lefted, so no need _try_send.
    if (r == 0) {
      uint64_t left = ack_left;
      if (left) {
        auto ack = AckFrame::Encode(in_seq);
        connection->outcoming_bl.append(ack.get_buffer(session_stream_handlers));
        ldout(cct, 10) << __func__ << " try send msg ack, acked " << left
                       << " messages" << dendl;
        ack_left -= left;
        left = ack_left;
        r = connection->_try_send(left);
      } else if (is_queued()) {
        r = connection->_try_send();
      }
    }
    connection->write_lock.unlock();

    connection->logger->tinc(l_msgr_running_send_time,
                             ceph::mono_clock::now() - start);
    if (r < 0) {
      ldout(cct, 1) << __func__ << " send msg failed" << dendl;
      connection->lock.lock();
      fault();
      connection->lock.unlock();
      return;
    }
  } else {
    connection->write_lock.unlock();
    connection->lock.lock();
    connection->write_lock.lock();
    if (state == STANDBY && !connection->policy.server && is_queued()) {
      ldout(cct, 10) << __func__ << " policy.server is false" << dendl;
      if (server_cookie) {  // only increment connect_seq if there is a session
        connect_seq++;
      }
      connection->_connect();
    } else if (connection->cs && state != NONE && state != CLOSED &&
               state != START_CONNECT) {
      r = connection->_try_send();
      if (r < 0) {
        ldout(cct, 1) << __func__ << " send outcoming bl failed" << dendl;
        connection->write_lock.unlock();
        fault();
        connection->lock.unlock();
        return;
      }
    }
    connection->write_lock.unlock();
    connection->lock.unlock();
  }
}

bool ProtocolV2::is_queued() {
  return !out_queue.empty() || connection->is_queued();
}

uint32_t ProtocolV2::get_onwire_size(const uint32_t logical_size) const {
  if (session_stream_handlers.rx) {
    return segment_onwire_size(logical_size);
  } else {
    return logical_size;
  }
}

uint32_t ProtocolV2::get_epilogue_size() const {
  // In secure mode size of epilogue is flexible and depends on particular
  // cipher implementation. See the comment for epilogue_secure_block_t or
  // epilogue_plain_block_t.
  if (session_stream_handlers.rx) {
    return FRAME_SECURE_EPILOGUE_SIZE + \
        session_stream_handlers.rx->get_extra_size_at_final();
  } else {
    return FRAME_PLAIN_EPILOGUE_SIZE;
  }
}

CtPtr ProtocolV2::read(CONTINUATION_RXBPTR_TYPE<ProtocolV2> &next,
                       rx_buffer_t &&buffer) {
  const auto len = buffer->length();
  const auto buf = buffer->c_str();
  next.node = std::move(buffer);
  ssize_t r = connection->read(len, buf,
    [&next, this](char *buffer, int r) {
      if (unlikely(pre_auth.enabled) && r >= 0) {
        pre_auth.rxbuf.append(*next.node);
	ceph_assert(!cct->_conf->ms_die_on_bug ||
		    pre_auth.rxbuf.length() < 1000000);
      }
      next.r = r;
      run_continuation(next);
    });
  if (r <= 0) {
    // error or done synchronously
    if (unlikely(pre_auth.enabled) && r >= 0) {
      pre_auth.rxbuf.append(*next.node);
      ceph_assert(!cct->_conf->ms_die_on_bug ||
		  pre_auth.rxbuf.length() < 1000000);
    }
    next.r = r;
    return &next;
  }

  return nullptr;
}

template <class F>
CtPtr ProtocolV2::write(const std::string &desc,
                        CONTINUATION_TYPE<ProtocolV2> &next,
                        F &frame) {
  ceph::bufferlist bl = frame.get_buffer(session_stream_handlers);
  return write(desc, next, bl);
}

CtPtr ProtocolV2::write(const std::string &desc,
                        CONTINUATION_TYPE<ProtocolV2> &next,
                        bufferlist &buffer) {
  if (unlikely(pre_auth.enabled)) {
    pre_auth.txbuf.append(buffer);
    ceph_assert(!cct->_conf->ms_die_on_bug ||
		pre_auth.txbuf.length() < 1000000);
  }

  ssize_t r =
      connection->write(buffer, [&next, desc, this](int r) {
        if (r < 0) {
          ldout(cct, 1) << __func__ << " " << desc << " write failed r=" << r
                        << " (" << cpp_strerror(r) << ")" << dendl;
          connection->inject_delay();
          _fault();
        }
        run_continuation(next);
      });

  if (r < 0) {
    ldout(cct, 1) << __func__ << " " << desc << " write failed r=" << r
                  << " (" << cpp_strerror(r) << ")" << dendl;
    return _fault();
  } else if (r == 0) {
    next.setParams();
    return &next;
  }

  return nullptr;
}

CtPtr ProtocolV2::_banner_exchange(CtRef callback) {
  ldout(cct, 20) << __func__ << dendl;
  bannerExchangeCallback = &callback;

  bufferlist banner_payload;
  encode((uint64_t)CEPH_MSGR2_SUPPORTED_FEATURES, banner_payload, 0);
  encode((uint64_t)CEPH_MSGR2_REQUIRED_FEATURES, banner_payload, 0);

  bufferlist bl;
  bl.append(CEPH_BANNER_V2_PREFIX, strlen(CEPH_BANNER_V2_PREFIX));
  encode((uint16_t)banner_payload.length(), bl, 0);
  bl.claim_append(banner_payload);

  INTERCEPT(state == BANNER_CONNECTING ? 3 : 4);

  return WRITE(bl, "banner", _wait_for_peer_banner);
}

CtPtr ProtocolV2::_wait_for_peer_banner() {
  unsigned banner_len = strlen(CEPH_BANNER_V2_PREFIX) + sizeof(__le16);
  return READ(banner_len, _handle_peer_banner);
}

CtPtr ProtocolV2::_handle_peer_banner(rx_buffer_t &&buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read peer banner failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  unsigned banner_prefix_len = strlen(CEPH_BANNER_V2_PREFIX);

  if (memcmp(buffer->c_str(), CEPH_BANNER_V2_PREFIX, banner_prefix_len)) {
    if (memcmp(buffer->c_str(), CEPH_BANNER, strlen(CEPH_BANNER)) == 0) {
      lderr(cct) << __func__ << " peer " << *connection->peer_addrs
                 << " is using msgr V1 protocol" << dendl;
      return _fault();
    }
    ldout(cct, 1) << __func__ << " accept peer sent bad banner" << dendl;
    return _fault();
  }

  uint16_t payload_len;
  bufferlist bl;
  buffer->set_offset(banner_prefix_len);
  buffer->set_length(sizeof(__le16));
  bl.push_back(std::move(buffer));
  auto ti = bl.cbegin();
  try {
    decode(payload_len, ti);
  } catch (const buffer::error &e) {
    lderr(cct) << __func__ << " decode banner payload len failed " << dendl;
    return _fault();
  }

  INTERCEPT(state == BANNER_CONNECTING ? 5 : 6);

  return READ(payload_len, _handle_peer_banner_payload);
}

CtPtr ProtocolV2::_handle_peer_banner_payload(rx_buffer_t &&buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read peer banner payload failed r=" << r
                  << " (" << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  uint64_t peer_supported_features;
  uint64_t peer_required_features;

  bufferlist bl;
  bl.push_back(std::move(buffer));
  auto ti = bl.cbegin();
  try {
    decode(peer_supported_features, ti);
    decode(peer_required_features, ti);
  } catch (const buffer::error &e) {
    lderr(cct) << __func__ << " decode banner payload failed " << dendl;
    return _fault();
  }

  ldout(cct, 1) << __func__ << " supported=" << std::hex
                << peer_supported_features << " required=" << std::hex
                << peer_required_features << std::dec << dendl;

  // Check feature bit compatibility

  uint64_t supported_features = CEPH_MSGR2_SUPPORTED_FEATURES;
  uint64_t required_features = CEPH_MSGR2_REQUIRED_FEATURES;

  if ((required_features & peer_supported_features) != required_features) {
    ldout(cct, 1) << __func__ << " peer does not support all required features"
                  << " required=" << std::hex << required_features
                  << " supported=" << std::hex << peer_supported_features
                  << std::dec << dendl;
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return nullptr;
  }
  if ((supported_features & peer_required_features) != peer_required_features) {
    ldout(cct, 1) << __func__ << " we do not support all peer required features"
                  << " required=" << std::hex << peer_required_features
                  << " supported=" << supported_features << std::dec << dendl;
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return nullptr;
  }

  this->peer_required_features = peer_required_features;
  if (this->peer_required_features == 0) {
    this->connection_features = msgr2_required;
  }

  // at this point we can change how the client protocol behaves based on
  // this->peer_required_features

  if (state == BANNER_CONNECTING) {
    state = HELLO_CONNECTING;
  }
  else {
    ceph_assert(state == BANNER_ACCEPTING);
    state = HELLO_ACCEPTING;
  }

  auto hello = HelloFrame::Encode(messenger->get_mytype(),
                                  connection->target_addr);

  INTERCEPT(state == HELLO_CONNECTING ? 7 : 8);

  return WRITE(hello, "hello frame", read_frame);
}

CtPtr ProtocolV2::handle_hello(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != HELLO_CONNECTING && state != HELLO_ACCEPTING) {
    lderr(cct) << __func__ << " not in hello exchange state!" << dendl;
    return _fault();
  }

  auto hello = HelloFrame::Decode(payload);

  ldout(cct, 5) << __func__ << " received hello:"
                << " peer_type=" << (int)hello.entity_type()
                << " peer_addr_for_me=" << hello.peer_addr() << dendl;

  if (connection->get_peer_type() == -1) {
    connection->set_peer_type(hello.entity_type());

    ceph_assert(state == HELLO_ACCEPTING);
    connection->policy = messenger->get_policy(hello.entity_type());
    ldout(cct, 10) << __func__ << " accept of host_type "
                   << (int)hello.entity_type()
                   << ", policy.lossy=" << connection->policy.lossy
                   << " policy.server=" << connection->policy.server
                   << " policy.standby=" << connection->policy.standby
                   << " policy.resetcheck=" << connection->policy.resetcheck
                   << dendl;
  } else {
    ceph_assert(state == HELLO_CONNECTING);
    if (connection->get_peer_type() != hello.entity_type()) {
      ldout(cct, 1) << __func__ << " connection peer type does not match what"
                    << " peer advertises " << connection->get_peer_type()
                    << " != " << (int)hello.entity_type() << dendl;
      stop();
      connection->dispatch_queue->queue_reset(connection);
      return nullptr;
    }
  }

  CtPtr callback;
  callback = bannerExchangeCallback;
  bannerExchangeCallback = nullptr;
  ceph_assert(callback);
  return callback;
}

CtPtr ProtocolV2::read_frame() {
  if (state == CLOSED) {
    return nullptr;
  }

  ldout(cct, 20) << __func__ << dendl;
  return READ(FRAME_PREAMBLE_SIZE, handle_read_frame_preamble_main);
}

CtPtr ProtocolV2::handle_read_frame_preamble_main(rx_buffer_t &&buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read frame length and tag failed r=" << r
                  << " (" << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  ceph::bufferlist preamble;
  preamble.push_back(std::move(buffer));

  ldout(cct, 30) << __func__ << " preamble\n";
  preamble.hexdump(*_dout);
  *_dout << dendl;

  if (session_stream_handlers.rx) {
    ceph_assert(session_stream_handlers.rx);

    session_stream_handlers.rx->reset_rx_handler();
    preamble = session_stream_handlers.rx->authenticated_decrypt_update(
      std::move(preamble), segment_t::DEFAULT_ALIGNMENT);

    ldout(cct, 10) << __func__ << " got encrypted preamble."
                   << " after decrypt premable.length()=" << preamble.length()
                   << dendl;

    ldout(cct, 30) << __func__ << " preamble after decrypt\n";
    preamble.hexdump(*_dout);
    *_dout << dendl;
  }

  {
    // I expect ceph_le32 will make the endian conversion for me. Passing
    // everything through ::Decode is unnecessary.
    const auto& main_preamble = \
      reinterpret_cast<preamble_block_t&>(*preamble.c_str());

    // verify preamble's CRC before any further processing
    const auto rx_crc = ceph_crc32c(0,
      reinterpret_cast<const unsigned char*>(&main_preamble),
      sizeof(main_preamble) - sizeof(main_preamble.crc));
    if (rx_crc != main_preamble.crc) {
      ldout(cct, 10) << __func__ << " crc mismatch for main preamble"
		     << " rx_crc=" << rx_crc
		     << " tx_crc=" << main_preamble.crc << dendl;
      return _fault();
    }

    // currently we do support between 1 and MAX_NUM_SEGMENTS segments
    if (main_preamble.num_segments < 1 ||
        main_preamble.num_segments > MAX_NUM_SEGMENTS) {
      ldout(cct, 10) << __func__ << " unsupported num_segments="
		     << " tx_crc=" << main_preamble.num_segments << dendl;
      return _fault();
    }

    next_tag = static_cast<Tag>(main_preamble.tag);

    rx_segments_desc.clear();
    rx_segments_data.clear();

    if (main_preamble.num_segments > MAX_NUM_SEGMENTS) {
      ldout(cct, 30) << __func__
		     << " num_segments=" << main_preamble.num_segments
		     << " is too much" << dendl;
      return _fault();
    }
    for (std::uint8_t idx = 0; idx < main_preamble.num_segments; idx++) {
      ldout(cct, 10) << __func__ << " got new segment:"
		     << " len=" << main_preamble.segments[idx].length
		     << " align=" << main_preamble.segments[idx].alignment
		     << dendl;
      rx_segments_desc.emplace_back(main_preamble.segments[idx]);
    }
  }

  // does it need throttle?
  if (next_tag == Tag::MESSAGE) {
    if (state != READY) {
      lderr(cct) << __func__ << " not in ready state!" << dendl;
      return _fault();
    }
    state = THROTTLE_MESSAGE;
    return CONTINUE(throttle_message);
  } else {
    return read_frame_segment();
  }
}

CtPtr ProtocolV2::handle_read_frame_dispatch() {
  ldout(cct, 10) << __func__
                 << " tag=" << static_cast<uint32_t>(next_tag) << dendl;

  switch (next_tag) {
    case Tag::HELLO:
    case Tag::AUTH_REQUEST:
    case Tag::AUTH_BAD_METHOD:
    case Tag::AUTH_REPLY_MORE:
    case Tag::AUTH_REQUEST_MORE:
    case Tag::AUTH_DONE:
    case Tag::AUTH_SIGNATURE:
    case Tag::CLIENT_IDENT:
    case Tag::SERVER_IDENT:
    case Tag::IDENT_MISSING_FEATURES:
    case Tag::SESSION_RECONNECT:
    case Tag::SESSION_RESET:
    case Tag::SESSION_RETRY:
    case Tag::SESSION_RETRY_GLOBAL:
    case Tag::SESSION_RECONNECT_OK:
    case Tag::KEEPALIVE2:
    case Tag::KEEPALIVE2_ACK:
    case Tag::ACK:
    case Tag::WAIT:
      return handle_frame_payload();
    case Tag::MESSAGE:
      return handle_message();
    default: {
      lderr(cct) << __func__
                 << " received unknown tag=" << static_cast<uint32_t>(next_tag)
                 << dendl;
      return _fault();
    }
  }

  return nullptr;
}

CtPtr ProtocolV2::read_frame_segment() {
  ldout(cct, 20) << __func__ << dendl;
  ceph_assert(!rx_segments_desc.empty());

  // description of current segment to read
  const auto& cur_rx_desc = rx_segments_desc.at(rx_segments_data.size());
  rx_buffer_t rx_buffer;
  try {
    rx_buffer = buffer::ptr_node::create(buffer::create_aligned(
      get_onwire_size(cur_rx_desc.length), cur_rx_desc.alignment));
  } catch (std::bad_alloc&) {
    // Catching because of potential issues with satisfying alignment.
    ldout(cct, 20) << __func__ << " can't allocate aligned rx_buffer "
		   << " len=" << get_onwire_size(cur_rx_desc.length)
		   << " align=" << cur_rx_desc.alignment
		   << dendl;
    return _fault();
  }

  return READ_RXBUF(std::move(rx_buffer), handle_read_frame_segment);
}

CtPtr ProtocolV2::handle_read_frame_segment(rx_buffer_t &&rx_buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read frame segment failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  rx_segments_data.emplace_back();
  rx_segments_data.back().push_back(std::move(rx_buffer));

  // decrypt incoming data
  // FIXME: if (auth_meta->is_mode_secure()) {
  if (session_stream_handlers.rx) {
    ceph_assert(session_stream_handlers.rx);

    auto& new_seg = rx_segments_data.back();
    if (new_seg.length()) {
      auto padded = session_stream_handlers.rx->authenticated_decrypt_update(
          std::move(new_seg), segment_t::DEFAULT_ALIGNMENT);
      const auto idx = rx_segments_data.size() - 1;
      new_seg.clear();
      padded.splice(0, rx_segments_desc[idx].length, &new_seg);

      ldout(cct, 20) << __func__
                     << " unpadded new_seg.length()=" << new_seg.length()
                     << dendl;
    }
  }

  if (rx_segments_desc.size() == rx_segments_data.size()) {
    // OK, all segments planned to read are read. Can go with epilogue.
    return READ(get_epilogue_size(), handle_read_frame_epilogue_main);
  } else {
    // TODO: for makeshift only. This will be more generic and throttled
    return read_frame_segment();
  }
}

CtPtr ProtocolV2::handle_frame_payload() {
  ceph_assert(!rx_segments_data.empty());
  auto& payload = rx_segments_data.back();

  ldout(cct, 30) << __func__ << "\n";
  payload.hexdump(*_dout);
  *_dout << dendl;

  switch (next_tag) {
    case Tag::HELLO:
      return handle_hello(payload);
    case Tag::AUTH_REQUEST:
      return handle_auth_request(payload);
    case Tag::AUTH_BAD_METHOD:
      return handle_auth_bad_method(payload);
    case Tag::AUTH_REPLY_MORE:
      return handle_auth_reply_more(payload);
    case Tag::AUTH_REQUEST_MORE:
      return handle_auth_request_more(payload);
    case Tag::AUTH_DONE:
      return handle_auth_done(payload);
    case Tag::AUTH_SIGNATURE:
      return handle_auth_signature(payload);
    case Tag::CLIENT_IDENT:
      return handle_client_ident(payload);
    case Tag::SERVER_IDENT:
      return handle_server_ident(payload);
    case Tag::IDENT_MISSING_FEATURES:
      return handle_ident_missing_features(payload);
    case Tag::SESSION_RECONNECT:
      return handle_reconnect(payload);
    case Tag::SESSION_RESET:
      return handle_session_reset(payload);
    case Tag::SESSION_RETRY:
      return handle_session_retry(payload);
    case Tag::SESSION_RETRY_GLOBAL:
      return handle_session_retry_global(payload);
    case Tag::SESSION_RECONNECT_OK:
      return handle_reconnect_ok(payload);
    case Tag::KEEPALIVE2:
      return handle_keepalive2(payload);
    case Tag::KEEPALIVE2_ACK:
      return handle_keepalive2_ack(payload);
    case Tag::ACK:
      return handle_message_ack(payload);
    case Tag::WAIT:
      return handle_wait(payload);
    default:
      ceph_abort();
  }
  return nullptr;
}

CtPtr ProtocolV2::ready() {
  ldout(cct, 25) << __func__ << dendl;

  reconnecting = false;
  replacing = false;

  // make sure no pending tick timer
  if (connection->last_tick_id) {
    connection->center->delete_time_event(connection->last_tick_id);
  }
  connection->last_tick_id = connection->center->create_time_event(
      connection->inactive_timeout_us, connection->tick_handler);

  {
    std::lock_guard<std::mutex> l(connection->write_lock);
    can_write = true;
    if (!out_queue.empty()) {
      connection->center->dispatch_event_external(connection->write_handler);
    }
  }

  connection->maybe_start_delay_thread();

  state = READY;
  ldout(cct, 1) << __func__ << " entity=" << peer_name << " client_cookie="
                << std::hex << client_cookie << " server_cookie="
                << server_cookie << std::dec << " in_seq=" << in_seq
                << " out_seq=" << out_seq << dendl;

  INTERCEPT(15);

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_read_frame_epilogue_main(rx_buffer_t &&buffer, int r)
{
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read data error " << dendl;
    return _fault();
  }

  __u8 late_flags;

  // FIXME: if (auth_meta->is_mode_secure()) {
  if (session_stream_handlers.rx) {
    ldout(cct, 1) << __func__ << " read frame epilogue bytes="
                  << get_epilogue_size() << dendl;

    // decrypt epilogue and authenticate entire frame.
    ceph::bufferlist epilogue_bl;
    {
      epilogue_bl.push_back(std::move(buffer));
      try {
        epilogue_bl =
            session_stream_handlers.rx->authenticated_decrypt_update_final(
	        std::move(epilogue_bl), segment_t::DEFAULT_ALIGNMENT);
      } catch (ceph::crypto::onwire::MsgAuthError &e) {
        ldout(cct, 5) << __func__ << " message authentication failed: "
                      << e.what() << dendl;
        return _fault();
      }
    }
    auto& epilogue =
        reinterpret_cast<epilogue_plain_block_t&>(*epilogue_bl.c_str());
    late_flags = epilogue.late_flags;
  } else {
    auto& epilogue = reinterpret_cast<epilogue_plain_block_t&>(*buffer->c_str());

    for (std::uint8_t idx = 0; idx < rx_segments_data.size(); idx++) {
      const __u32 expected_crc = epilogue.crc_values[idx];
      const __u32 calculated_crc = rx_segments_data[idx].crc32c(-1);
      if (expected_crc != calculated_crc) {
	ldout(cct, 5) << __func__ << " message integrity check failed: "
		      << " expected_crc=" << expected_crc
		      << " calculated_crc=" << calculated_crc
		      << dendl;
	return _fault();
      } else {
	ldout(cct, 20) << __func__ << " message integrity check success: "
		       << " expected_crc=" << expected_crc
		       << " calculated_crc=" << calculated_crc
		       << dendl;
      }
    }
    late_flags = epilogue.late_flags;
  }

  // we do have a mechanism that allows transmitter to start sending message
  // and abort after putting entire data field on wire. This will be used by
  // the kernel client to avoid unnecessary buffering.
  if (late_flags & FRAME_FLAGS_LATEABRT) {
    reset_throttle();
    state = READY;
    return CONTINUE(read_frame);
  } else {
    return handle_read_frame_dispatch();
  }
}

CtPtr ProtocolV2::handle_message() {
  ldout(cct, 20) << __func__ << dendl;
  ceph_assert(state == THROTTLE_DONE);

#if defined(WITH_LTTNG) && defined(WITH_EVENTTRACE)
  ltt_recv_stamp = ceph_clock_now();
#endif
  recv_stamp = ceph_clock_now();

  // we need to get the size before std::moving segments data
  const size_t cur_msg_size = get_current_msg_size();
  auto msg_frame = MessageFrame::Decode(std::move(rx_segments_data));

  // XXX: paranoid copy just to avoid oops
  ceph_msg_header2 current_header = msg_frame.header();

  ldout(cct, 5) << __func__
		<< " got " << msg_frame.front_len()
		<< " + " << msg_frame.middle_len()
		<< " + " << msg_frame.data_len()
		<< " byte message."
		<< " envelope type=" << current_header.type
		<< " src " << peer_name
		<< " off " << current_header.data_off
                << dendl;

  INTERCEPT(16);
  ceph_msg_header header{current_header.seq,
                         current_header.tid,
                         current_header.type,
                         current_header.priority,
                         current_header.version,
                         msg_frame.front_len(),
                         msg_frame.middle_len(),
                         msg_frame.data_len(),
                         current_header.data_off,
                         peer_name,
                         current_header.compat_version,
                         current_header.reserved,
                         0};
  ceph_msg_footer footer{0, 0, 0, 0, current_header.flags};

  Message *message = decode_message(cct, 0, header, footer,
      msg_frame.front(),
      msg_frame.middle(),
      msg_frame.data(),
      connection);
  if (!message) {
    ldout(cct, 1) << __func__ << " decode message failed " << dendl;
    return _fault();
  } else {
    state = READ_MESSAGE_COMPLETE;
  }

  INTERCEPT(17);

  message->set_byte_throttler(connection->policy.throttler_bytes);
  message->set_message_throttler(connection->policy.throttler_messages);

  // store reservation size in message, so we don't get confused
  // by messages entering the dispatch queue through other paths.
  message->set_dispatch_throttle_size(cur_msg_size);

  message->set_recv_stamp(recv_stamp);
  message->set_throttle_stamp(throttle_stamp);
  message->set_recv_complete_stamp(ceph_clock_now());

  // check received seq#.  if it is old, drop the message.
  // note that incoming messages may skip ahead.  this is convenient for the
  // client side queueing because messages can't be renumbered, but the (kernel)
  // client will occasionally pull a message out of the sent queue to send
  // elsewhere.  in that case it doesn't matter if we "got" it or not.
  uint64_t cur_seq = in_seq;
  if (message->get_seq() <= cur_seq) {
    ldout(cct, 0) << __func__ << " got old message " << message->get_seq()
                  << " <= " << cur_seq << " " << message << " " << *message
                  << ", discarding" << dendl;
    message->put();
    if (connection->has_feature(CEPH_FEATURE_RECONNECT_SEQ) &&
        cct->_conf->ms_die_on_old_message) {
      ceph_assert(0 == "old msgs despite reconnect_seq feature");
    }
    return nullptr;
  }
  if (message->get_seq() > cur_seq + 1) {
    ldout(cct, 0) << __func__ << " missed message?  skipped from seq "
                  << cur_seq << " to " << message->get_seq() << dendl;
    if (cct->_conf->ms_die_on_skipped_message) {
      ceph_assert(0 == "skipped incoming seq");
    }
  }

  message->set_connection(connection);

#if defined(WITH_LTTNG) && defined(WITH_EVENTTRACE)
  if (message->get_type() == CEPH_MSG_OSD_OP ||
      message->get_type() == CEPH_MSG_OSD_OPREPLY) {
    utime_t ltt_processed_stamp = ceph_clock_now();
    double usecs_elapsed =
        (ltt_processed_stamp.to_nsec() - ltt_recv_stamp.to_nsec()) / 1000;
    ostringstream buf;
    if (message->get_type() == CEPH_MSG_OSD_OP)
      OID_ELAPSED_WITH_MSG(message, usecs_elapsed, "TIME_TO_DECODE_OSD_OP",
                           false);
    else
      OID_ELAPSED_WITH_MSG(message, usecs_elapsed, "TIME_TO_DECODE_OSD_OPREPLY",
                           false);
  }
#endif

  // note last received message.
  in_seq = message->get_seq();
  ldout(cct, 5) << __func__ << " received message m=" << message
                << " seq=" << message->get_seq()
                << " from=" << message->get_source() << " type=" << header.type
                << " " << *message << dendl;

  bool need_dispatch_writer = false;
  if (!connection->policy.lossy) {
    ack_left++;
    need_dispatch_writer = true;
  }

  state = READY;

  connection->logger->inc(l_msgr_recv_messages);
  connection->logger->inc(
      l_msgr_recv_bytes,
      cur_msg_size + sizeof(ceph_msg_header) + sizeof(ceph_msg_footer));

  messenger->ms_fast_preprocess(message);
  auto fast_dispatch_time = ceph::mono_clock::now();
  connection->logger->tinc(l_msgr_running_recv_time,
                           fast_dispatch_time - connection->recv_start_time);
  if (connection->delay_state) {
    double delay_period = 0;
    if (rand() % 10000 < cct->_conf->ms_inject_delay_probability * 10000.0) {
      delay_period =
          cct->_conf->ms_inject_delay_max * (double)(rand() % 10000) / 10000.0;
      ldout(cct, 1) << "queue_received will delay after "
                    << (ceph_clock_now() + delay_period) << " on " << message
                    << " " << *message << dendl;
    }
    connection->delay_state->queue(delay_period, message);
  } else if (messenger->ms_can_fast_dispatch(message)) {
    connection->lock.unlock();
    connection->dispatch_queue->fast_dispatch(message);
    connection->recv_start_time = ceph::mono_clock::now();
    connection->logger->tinc(l_msgr_running_fast_dispatch_time,
                             connection->recv_start_time - fast_dispatch_time);
    connection->lock.lock();
    // we might have been reused by another connection
    // let's check if that is the case
    if (state != READY) {
      // yes, that was the case, let's do nothing
      return nullptr;
    }
  } else {
    connection->dispatch_queue->enqueue(message, message->get_priority(),
                                        connection->conn_id);
  }

  handle_message_ack(current_header.ack_seq);


  if (need_dispatch_writer && connection->is_connected()) {
    connection->center->dispatch_event_external(connection->write_handler);
  }

  return CONTINUE(read_frame);
}


CtPtr ProtocolV2::throttle_message() {
  ldout(cct, 20) << __func__ << dendl;

  if (connection->policy.throttler_messages) {
    ldout(cct, 10) << __func__ << " wants " << 1
                   << " message from policy throttler "
                   << connection->policy.throttler_messages->get_current()
                   << "/" << connection->policy.throttler_messages->get_max()
                   << dendl;
    if (!connection->policy.throttler_messages->get_or_fail()) {
      ldout(cct, 10) << __func__ << " wants 1 message from policy throttle "
                     << connection->policy.throttler_messages->get_current()
                     << "/" << connection->policy.throttler_messages->get_max()
                     << " failed, just wait." << dendl;
      // following thread pool deal with th full message queue isn't a
      // short time, so we can wait a ms.
      if (connection->register_time_events.empty()) {
        connection->register_time_events.insert(
            connection->center->create_time_event(1000,
                                                  connection->wakeup_handler));
      }
      return nullptr;
    }
  }

  state = THROTTLE_BYTES;
  return CONTINUE(throttle_bytes);
}

CtPtr ProtocolV2::throttle_bytes() {
  ldout(cct, 20) << __func__ << dendl;

  const size_t cur_msg_size = get_current_msg_size();
  if (cur_msg_size) {
    if (connection->policy.throttler_bytes) {
      ldout(cct, 10) << __func__ << " wants " << cur_msg_size
                     << " bytes from policy throttler "
                     << connection->policy.throttler_bytes->get_current() << "/"
                     << connection->policy.throttler_bytes->get_max() << dendl;
      if (!connection->policy.throttler_bytes->get_or_fail(cur_msg_size)) {
        ldout(cct, 10) << __func__ << " wants " << cur_msg_size
                       << " bytes from policy throttler "
                       << connection->policy.throttler_bytes->get_current()
                       << "/" << connection->policy.throttler_bytes->get_max()
                       << " failed, just wait." << dendl;
        // following thread pool deal with th full message queue isn't a
        // short time, so we can wait a ms.
        if (connection->register_time_events.empty()) {
          connection->register_time_events.insert(
              connection->center->create_time_event(
                  1000, connection->wakeup_handler));
        }
        return nullptr;
      }
    }
  }

  state = THROTTLE_DISPATCH_QUEUE;
  return CONTINUE(throttle_dispatch_queue);
}

CtPtr ProtocolV2::throttle_dispatch_queue() {
  ldout(cct, 20) << __func__ << dendl;

  const size_t cur_msg_size = get_current_msg_size();
  if (cur_msg_size) {
    if (!connection->dispatch_queue->dispatch_throttler.get_or_fail(
            cur_msg_size)) {
      ldout(cct, 10)
          << __func__ << " wants " << cur_msg_size
          << " bytes from dispatch throttle "
          << connection->dispatch_queue->dispatch_throttler.get_current() << "/"
          << connection->dispatch_queue->dispatch_throttler.get_max()
          << " failed, just wait." << dendl;
      // following thread pool deal with th full message queue isn't a
      // short time, so we can wait a ms.
      if (connection->register_time_events.empty()) {
        connection->register_time_events.insert(
            connection->center->create_time_event(1000,
                                                  connection->wakeup_handler));
      }
      return nullptr;
    }
  }

  throttle_stamp = ceph_clock_now();
  state = THROTTLE_DONE;

  return read_frame_segment();
}

CtPtr ProtocolV2::handle_keepalive2(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != READY) {
    lderr(cct) << __func__ << " not in ready state!" << dendl;
    return _fault();
  }

  auto keepalive_frame = KeepAliveFrame::Decode(payload);

  ldout(cct, 30) << __func__ << " got KEEPALIVE2 tag ..." << dendl;

  connection->write_lock.lock();
  append_keepalive_ack(keepalive_frame.timestamp());
  connection->write_lock.unlock();

  ldout(cct, 20) << __func__ << " got KEEPALIVE2 "
                 << keepalive_frame.timestamp() << dendl;
  connection->set_last_keepalive(ceph_clock_now());

  if (is_connected()) {
    connection->center->dispatch_event_external(connection->write_handler);
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_keepalive2_ack(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != READY) {
    lderr(cct) << __func__ << " not in ready state!" << dendl;
    return _fault();
  }

  auto keepalive_ack_frame = KeepAliveFrameAck::Decode(payload);
  connection->set_last_keepalive_ack(keepalive_ack_frame.timestamp());
  ldout(cct, 20) << __func__ << " got KEEPALIVE_ACK" << dendl;

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_message_ack(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != READY) {
    lderr(cct) << __func__ << " not in ready state!" << dendl;
    return _fault();
  }

  auto ack = AckFrame::Decode(payload);
  handle_message_ack(ack.seq());
  return CONTINUE(read_frame);
}

/* Client Protocol Methods */

CtPtr ProtocolV2::start_client_banner_exchange() {
  ldout(cct, 20) << __func__ << dendl;

  INTERCEPT(1);

  state = BANNER_CONNECTING;

  global_seq = messenger->get_global_seq();

  return _banner_exchange(CONTINUATION(post_client_banner_exchange));
}

CtPtr ProtocolV2::post_client_banner_exchange() {
  ldout(cct, 20) << __func__ << dendl;

  state = AUTH_CONNECTING;

  return send_auth_request();
}

CtPtr ProtocolV2::send_auth_request(std::vector<uint32_t> &allowed_methods) {
  ceph_assert(messenger->auth_client);
  ldout(cct, 20) << __func__ << " peer_type " << (int)connection->peer_type
		 << " auth_client " << messenger->auth_client << dendl;

  bufferlist bl;
  vector<uint32_t> preferred_modes;
  auto am = auth_meta;
  connection->lock.unlock();
  int r = messenger->auth_client->get_auth_request(
    connection, am.get(),
    &am->auth_method, &preferred_modes, &bl);
  connection->lock.lock();
  if (state != AUTH_CONNECTING) {
    ldout(cct, 1) << __func__ << " state changed!" << dendl;
    return _fault();
  }
  if (r < 0) {
    ldout(cct, 0) << __func__ << " get_initial_auth_request returned " << r
		  << dendl;
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return nullptr;
  }

  INTERCEPT(9);

  auto frame = AuthRequestFrame::Encode(auth_meta->auth_method, preferred_modes,
                                        bl);
  return WRITE(frame, "auth request", read_frame);
}

CtPtr ProtocolV2::handle_auth_bad_method(ceph::bufferlist &payload) {
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != AUTH_CONNECTING) {
    lderr(cct) << __func__ << " not in auth connect state!" << dendl;
    return _fault();
  }

  auto bad_method = AuthBadMethodFrame::Decode(payload);
  ldout(cct, 1) << __func__ << " method=" << bad_method.method()
		<< " result " << cpp_strerror(bad_method.result())
                << ", allowed methods=" << bad_method.allowed_methods()
		<< ", allowed modes=" << bad_method.allowed_modes()
                << dendl;
  ceph_assert(messenger->auth_client);
  auto am = auth_meta;
  connection->lock.unlock();
  int r = messenger->auth_client->handle_auth_bad_method(
    connection,
    am.get(),
    bad_method.method(), bad_method.result(),
    bad_method.allowed_methods(),
    bad_method.allowed_modes());
  connection->lock.lock();
  if (state != AUTH_CONNECTING || r < 0) {
    return _fault();
  }
  return send_auth_request(bad_method.allowed_methods());
}

CtPtr ProtocolV2::handle_auth_reply_more(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != AUTH_CONNECTING) {
    lderr(cct) << __func__ << " not in auth connect state!" << dendl;
    return _fault();
  }

  auto auth_more = AuthReplyMoreFrame::Decode(payload);
  ldout(cct, 5) << __func__
                << " auth reply more len=" << auth_more.auth_payload().length()
                << dendl;
  ceph_assert(messenger->auth_client);
  ceph::bufferlist reply;
  auto am = auth_meta;
  connection->lock.unlock();
  int r = messenger->auth_client->handle_auth_reply_more(
    connection, am.get(), auth_more.auth_payload(), &reply);
  connection->lock.lock();
  if (state != AUTH_CONNECTING) {
    ldout(cct, 1) << __func__ << " state changed!" << dendl;
    return _fault();
  }
  if (r < 0) {
    lderr(cct) << __func__ << " auth_client handle_auth_reply_more returned "
	       << r << dendl;
    return _fault();
  }
  auto more_reply = AuthRequestMoreFrame::Encode(reply);
  return WRITE(more_reply, "auth request more", read_frame);
}

CtPtr ProtocolV2::handle_auth_done(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != AUTH_CONNECTING) {
    lderr(cct) << __func__ << " not in auth connect state!" << dendl;
    return _fault();
  }

  auto auth_done = AuthDoneFrame::Decode(payload);

  ceph_assert(messenger->auth_client);
  auto am = auth_meta;
  connection->lock.unlock();
  int r = messenger->auth_client->handle_auth_done(
    connection,
    am.get(),
    auth_done.global_id(),
    auth_done.con_mode(),
    auth_done.auth_payload(),
    &am->session_key,
    &am->connection_secret);
  connection->lock.lock();
  if (state != AUTH_CONNECTING) {
    ldout(cct, 1) << __func__ << " state changed!" << dendl;
    return _fault();
  }
  if (r < 0) {
    return _fault();
  }
  auth_meta->con_mode = auth_done.con_mode();
  session_stream_handlers = \
    ceph::crypto::onwire::rxtx_t::create_handler_pair(cct, *auth_meta, false);

  state = AUTH_CONNECTING_SIGN;

  const auto sig = auth_meta->session_key.empty() ? sha256_digest_t() :
    auth_meta->session_key.hmac_sha256(cct, pre_auth.rxbuf);
  auto sig_frame = AuthSignatureFrame::Encode(sig);
  pre_auth.enabled = false;
  pre_auth.rxbuf.clear();
  return WRITE(sig_frame, "auth signature", read_frame);
}

CtPtr ProtocolV2::finish_client_auth() {
  if (!server_cookie) {
    ceph_assert(connect_seq == 0);
    state = SESSION_CONNECTING;
    return send_client_ident();
  } else {  // reconnecting to previous session
    state = SESSION_RECONNECTING;
    ceph_assert(connect_seq > 0);
    return send_reconnect();
  }
}

CtPtr ProtocolV2::send_client_ident() {
  ldout(cct, 20) << __func__ << dendl;

  if (!connection->policy.lossy && !client_cookie) {
    client_cookie = ceph::util::generate_random_number<uint64_t>(1, -1ll);
  }

  uint64_t flags = 0;
  if (connection->policy.lossy) {
    flags |= CEPH_MSG_CONNECT_LOSSY;
  }

  if (messenger->get_myaddrs().empty() ||
      messenger->get_myaddrs().front().is_blank_ip()) {
    sockaddr_storage ss;
    socklen_t len = sizeof(ss);
    int r = getsockname(connection->cs.socket_fd(), (sockaddr *)&ss, &len);
    ceph_assert(r == 0);
    ldout(cct, 1) << __func__ << " getsockname reveals I am " << (sockaddr *)&ss
                  << " when talking to " << connection->target_addr << dendl;
    entity_addr_t a;
    a.set_type(entity_addr_t::TYPE_MSGR2); // anything but NONE; learned_addr ignores this
    a.set_sockaddr((sockaddr *)&ss);
    a.set_port(0);
    connection->lock.unlock();
    messenger->learned_addr(a);
    if (cct->_conf->ms_inject_internal_delays &&
        cct->_conf->ms_inject_socket_failures) {
      if (rand() % cct->_conf->ms_inject_socket_failures == 0) {
        ldout(cct, 10) << __func__ << " sleep for "
                       << cct->_conf->ms_inject_internal_delays << dendl;
        utime_t t;
        t.set_from_double(cct->_conf->ms_inject_internal_delays);
        t.sleep();
      }
    }
    connection->lock.lock();
    if (state != SESSION_CONNECTING) {
      ldout(cct, 1) << __func__
                    << " state changed while learned_addr, mark_down or "
                    << " replacing must be happened just now" << dendl;
      return nullptr;
    }
  }

  auto client_ident = ClientIdentFrame::Encode(
      messenger->get_myaddrs(),
      connection->target_addr,
      messenger->get_myname().num(),
      global_seq,
      connection->policy.features_supported,
      connection->policy.features_required | msgr2_required,
      flags,
      client_cookie);

  ldout(cct, 5) << __func__ << " sending identification: "
                << "addrs=" << messenger->get_myaddrs()
                << " target=" << connection->target_addr
                << " gid=" << messenger->get_myname().num()
                << " global_seq=" << global_seq
                << " features_supported=" << std::hex
                << connection->policy.features_supported
                << " features_required="
		            << (connection->policy.features_required | msgr2_required)
                << " flags=" << flags
                << " cookie=" << client_cookie << std::dec << dendl;

  INTERCEPT(11);

  return WRITE(client_ident, "client ident", read_frame);
}

CtPtr ProtocolV2::send_reconnect() {
  ldout(cct, 20) << __func__ << dendl;

  auto reconnect = ReconnectFrame::Encode(messenger->get_myaddrs(),
                                          client_cookie,
                                          server_cookie,
                                          global_seq,
                                          connect_seq,
                                          in_seq);

  ldout(cct, 5) << __func__ << " reconnect to session: client_cookie="
                << std::hex << client_cookie << " server_cookie="
                << server_cookie << std::dec
                << " gs=" << global_seq << " cs=" << connect_seq
                << " ms=" << in_seq << dendl;

  INTERCEPT(13);

  return WRITE(reconnect, "reconnect", read_frame);
}

CtPtr ProtocolV2::handle_ident_missing_features(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != SESSION_CONNECTING) {
    lderr(cct) << __func__ << " not in session connect state!" << dendl;
    return _fault();
  }

  auto ident_missing =
      IdentMissingFeaturesFrame::Decode(payload);
  lderr(cct) << __func__
             << " client does not support all server features: " << std::hex
             << ident_missing.features() << std::dec << dendl;

  return _fault();
}

CtPtr ProtocolV2::handle_session_reset(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != SESSION_RECONNECTING) {
    lderr(cct) << __func__ << " not in session reconnect state!" << dendl;
    return _fault();
  }

  auto reset = ResetFrame::Decode(payload);

  ldout(cct, 1) << __func__ << " received session reset full=" << reset.full()
                << dendl;
  if (reset.full()) {
    reset_session();
  } else {
    server_cookie = 0;
    connect_seq = 0;
    in_seq = 0;
  }

  state = SESSION_CONNECTING;
  return send_client_ident();
}

CtPtr ProtocolV2::handle_session_retry(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != SESSION_RECONNECTING) {
    lderr(cct) << __func__ << " not in session reconnect state!" << dendl;
    return _fault();
  }

  auto retry = RetryFrame::Decode(payload);
  connect_seq = retry.connect_seq() + 1;

  ldout(cct, 1) << __func__
                << " received session retry connect_seq=" << retry.connect_seq()
                << ", inc to cs=" << connect_seq << dendl;

  return send_reconnect();
}

CtPtr ProtocolV2::handle_session_retry_global(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != SESSION_RECONNECTING) {
    lderr(cct) << __func__ << " not in session reconnect state!" << dendl;
    return _fault();
  }

  auto retry = RetryGlobalFrame::Decode(payload);
  global_seq = messenger->get_global_seq(retry.global_seq());

  ldout(cct, 1) << __func__ << " received session retry global global_seq="
                << retry.global_seq() << ", choose new gs=" << global_seq
                << dendl;

  return send_reconnect();
}

CtPtr ProtocolV2::handle_wait(ceph::bufferlist &payload) {
  ldout(cct, 20) << __func__
		 << " received WAIT (connection race)"
		 << " payload.length()=" << payload.length()
		 << dendl;

  if (state != SESSION_CONNECTING && state != SESSION_RECONNECTING) {
    lderr(cct) << __func__ << " not in session (re)connect state!" << dendl;
    return _fault();
  }

  state = WAIT;
  WaitFrame::Decode(payload);
  return _fault();
}

CtPtr ProtocolV2::handle_reconnect_ok(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != SESSION_RECONNECTING) {
    lderr(cct) << __func__ << " not in session reconnect state!" << dendl;
    return _fault();
  }

  auto reconnect_ok = ReconnectOkFrame::Decode(payload);
  ldout(cct, 5) << __func__
                << " reconnect accepted: sms=" << reconnect_ok.msg_seq()
                << dendl;

  out_seq = discard_requeued_up_to(out_seq, reconnect_ok.msg_seq());

  backoff = utime_t();
  ldout(cct, 10) << __func__ << " reconnect success " << connect_seq
                 << ", lossy = " << connection->policy.lossy << ", features "
                 << connection->get_features() << dendl;

  if (connection->delay_state) {
    ceph_assert(connection->delay_state->ready());
  }

  connection->dispatch_queue->queue_connect(connection);
  messenger->ms_deliver_handle_fast_connect(connection);

  return ready();
}

CtPtr ProtocolV2::handle_server_ident(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != SESSION_CONNECTING) {
    lderr(cct) << __func__ << " not in session connect state!" << dendl;
    return _fault();
  }

  auto server_ident = ServerIdentFrame::Decode(payload);
  ldout(cct, 5) << __func__ << " received server identification:"
                << " addrs=" << server_ident.addrs()
                << " gid=" << server_ident.gid()
                << " global_seq=" << server_ident.global_seq()
                << " features_supported=" << std::hex
                << server_ident.supported_features()
                << " features_required=" << server_ident.required_features()
                << " flags=" << server_ident.flags() << " cookie=" << std::dec
                << server_ident.cookie() << dendl;

  // is this who we intended to talk to?
  // be a bit forgiving here, since we may be connecting based on addresses parsed out
  // of mon_host or something.
  if (!server_ident.addrs().contains(connection->target_addr)) {
    ldout(cct,1) << __func__ << " peer identifies as " << server_ident.addrs()
		 << ", does not include " << connection->target_addr << dendl;
    return _fault();
  }

  server_cookie = server_ident.cookie();

  connection->set_peer_addrs(server_ident.addrs());
  peer_name = entity_name_t(connection->get_peer_type(), server_ident.gid());
  connection->set_features(server_ident.supported_features() &
                           connection->policy.features_supported);
  peer_global_seq = server_ident.global_seq();

  connection->policy.lossy = server_ident.flags() & CEPH_MSG_CONNECT_LOSSY;

  backoff = utime_t();
  ldout(cct, 10) << __func__ << " connect success " << connect_seq
                 << ", lossy = " << connection->policy.lossy << ", features "
                 << connection->get_features() << dendl;

  if (connection->delay_state) {
    ceph_assert(connection->delay_state->ready());
  }

  connection->dispatch_queue->queue_connect(connection);
  messenger->ms_deliver_handle_fast_connect(connection);

  return ready();
}

/* Server Protocol Methods */

CtPtr ProtocolV2::start_server_banner_exchange() {
  ldout(cct, 20) << __func__ << dendl;

  INTERCEPT(2);

  state = BANNER_ACCEPTING;

  return _banner_exchange(CONTINUATION(post_server_banner_exchange));
}

CtPtr ProtocolV2::post_server_banner_exchange() {
  ldout(cct, 20) << __func__ << dendl;

  state = AUTH_ACCEPTING;

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_auth_request(ceph::bufferlist &payload) {
  ldout(cct, 20) << __func__ << " payload.length()=" << payload.length()
                 << dendl;

  if (state != AUTH_ACCEPTING) {
    lderr(cct) << __func__ << " not in auth accept state!" << dendl;
    return _fault();
  }

  auto request = AuthRequestFrame::Decode(payload);
  ldout(cct, 10) << __func__ << " AuthRequest(method=" << request.method()
		 << ", preferred_modes=" << request.preferred_modes()
                 << ", payload_len=" << request.auth_payload().length() << ")"
                 << dendl;
  auth_meta->auth_method = request.method();
  auth_meta->con_mode = messenger->auth_server->pick_con_mode(
    connection->get_peer_type(), auth_meta->auth_method,
    request.preferred_modes());
  if (auth_meta->con_mode == CEPH_CON_MODE_UNKNOWN) {
    return _auth_bad_method(-EOPNOTSUPP);
  }
  return _handle_auth_request(request.auth_payload(), false);
}

CtPtr ProtocolV2::_auth_bad_method(int r)
{
  ceph_assert(r < 0);
  std::vector<uint32_t> allowed_methods;
  std::vector<uint32_t> allowed_modes;
  messenger->auth_server->get_supported_auth_methods(
    connection->get_peer_type(), &allowed_methods, &allowed_modes);
  ldout(cct, 1) << __func__ << " auth_method " << auth_meta->auth_method
		<< " r " << cpp_strerror(r)
		<< ", allowed_methods " << allowed_methods
		<< ", allowed_modes " << allowed_modes
		<< dendl;
  auto bad_method = AuthBadMethodFrame::Encode(auth_meta->auth_method, r,
                                               allowed_methods, allowed_modes);
  return WRITE(bad_method, "bad auth method", read_frame);
}

CtPtr ProtocolV2::_handle_auth_request(bufferlist& auth_payload, bool more)
{
  if (!messenger->auth_server) {
    return _fault();
  }
  bufferlist reply;
  auto am = auth_meta;
  connection->lock.unlock();
  int r = messenger->auth_server->handle_auth_request(
    connection, am.get(),
    more, am->auth_method, auth_payload,
    &reply);
  connection->lock.lock();
  if (state != AUTH_ACCEPTING && state != AUTH_ACCEPTING_MORE) {
    ldout(cct, 1) << __func__
                  << " state changed while accept, it must be mark_down"
                  << dendl;
    ceph_assert(state == CLOSED);
    return _fault();
  }
  if (r == 1) {
    INTERCEPT(10);
    state = AUTH_ACCEPTING_SIGN;

    auto auth_done = AuthDoneFrame::Encode(connection->peer_global_id,
                                           auth_meta->con_mode,
                                           reply);
    return WRITE(auth_done, "auth done", finish_auth);
  } else if (r == 0) {
    state = AUTH_ACCEPTING_MORE;

    auto more = AuthReplyMoreFrame::Encode(reply);
    return WRITE(more, "auth reply more", read_frame);
  } else if (r == -EBUSY) {
    // kick the client and maybe they'll come back later
    return _fault();
  } else {
    return _auth_bad_method(r);
  }
}

CtPtr ProtocolV2::finish_auth()
{
  ceph_assert(auth_meta);
  // TODO: having a possibility to check whether we're server or client could
  // allow reusing finish_auth().
  session_stream_handlers = \
    ceph::crypto::onwire::rxtx_t::create_handler_pair(cct, *auth_meta, true);

  const auto sig = auth_meta->session_key.empty() ? sha256_digest_t() :
    auth_meta->session_key.hmac_sha256(cct, pre_auth.rxbuf);
  auto sig_frame = AuthSignatureFrame::Encode(sig);
  pre_auth.enabled = false;
  pre_auth.rxbuf.clear();
  return WRITE(sig_frame, "auth signature", read_frame);
}

CtPtr ProtocolV2::handle_auth_request_more(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != AUTH_ACCEPTING_MORE) {
    lderr(cct) << __func__ << " not in auth accept more state!" << dendl;
    return _fault();
  }

  auto auth_more = AuthRequestMoreFrame::Decode(payload);
  return _handle_auth_request(auth_more.auth_payload(), true);
}

CtPtr ProtocolV2::handle_auth_signature(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != AUTH_ACCEPTING_SIGN && state != AUTH_CONNECTING_SIGN) {
    lderr(cct) << __func__
               << " pre-auth verification signature seen in wrong state!"
               << dendl;
    return _fault();
  }

  auto sig_frame = AuthSignatureFrame::Decode(payload);

  const auto actual_tx_sig = auth_meta->session_key.empty() ?
    sha256_digest_t() : auth_meta->session_key.hmac_sha256(cct, pre_auth.txbuf);
  if (sig_frame.signature() != actual_tx_sig) {
    ldout(cct, 2) << __func__ << " pre-auth signature mismatch"
                  << " actual_tx_sig=" << actual_tx_sig
                  << " sig_frame.signature()=" << sig_frame.signature()
                  << dendl;
    return _fault();
  } else {
    ldout(cct, 20) << __func__ << " pre-auth signature success"
                   << " sig_frame.signature()=" << sig_frame.signature()
                   << dendl;
    pre_auth.txbuf.clear();
  }

  if (state == AUTH_ACCEPTING_SIGN) {
    // server had sent AuthDone and client responded with correct pre-auth
    // signature. we can start accepting new sessions/reconnects.
    state = SESSION_ACCEPTING;
    return CONTINUE(read_frame);
  } else if (state == AUTH_CONNECTING_SIGN) {
    // this happened at client side
    return finish_client_auth();
  } else {
    ceph_assert_always("state corruption" == nullptr);
  }
}

CtPtr ProtocolV2::handle_client_ident(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != SESSION_ACCEPTING) {
    lderr(cct) << __func__ << " not in session accept state!" << dendl;
    return _fault();
  }

  auto client_ident = ClientIdentFrame::Decode(payload);

  ldout(cct, 5) << __func__ << " received client identification:"
                << " addrs=" << client_ident.addrs()
		            << " target=" << client_ident.target_addr()
                << " gid=" << client_ident.gid()
                << " global_seq=" << client_ident.global_seq()
                << " features_supported=" << std::hex
                << client_ident.supported_features()
                << " features_required=" << client_ident.required_features()
                << " flags=" << client_ident.flags()
                << " cookie=" << client_ident.cookie() << std::dec << dendl;

  if (client_ident.addrs().empty() ||
      client_ident.addrs().front() == entity_addr_t()) {
    ldout(cct,5) << __func__ << " oops, client_ident.addrs() is empty" << dendl;
    return _fault();  // a v2 peer should never do this
  }
  if (!messenger->get_myaddrs().contains(client_ident.target_addr())) {
    ldout(cct,5) << __func__ << " peer is trying to reach "
		 << client_ident.target_addr()
		 << " which is not us (" << messenger->get_myaddrs() << ")"
		 << dendl;
    return _fault();
  }

  connection->set_peer_addrs(client_ident.addrs());
  connection->target_addr = connection->_infer_target_addr(client_ident.addrs());

  peer_name = entity_name_t(connection->get_peer_type(), client_ident.gid());
  connection->set_peer_id(client_ident.gid());

  client_cookie = client_ident.cookie();

  uint64_t feat_missing =
    (connection->policy.features_required | msgr2_required) &
    ~(uint64_t)client_ident.supported_features();
  if (feat_missing) {
    ldout(cct, 1) << __func__ << " peer missing required features " << std::hex
                  << feat_missing << std::dec << dendl;
    auto ident_missing_features =
        IdentMissingFeaturesFrame::Encode(feat_missing);

    return WRITE(ident_missing_features, "ident missing features", read_frame);
  }

  connection_features =
      client_ident.supported_features() & connection->policy.features_supported;

  peer_global_seq = client_ident.global_seq();

  // Looks good so far, let's check if there is already an existing connection
  // to this peer.

  connection->lock.unlock();
  AsyncConnectionRef existing = messenger->lookup_conn(*connection->peer_addrs);

  if (existing &&
      existing->protocol->proto_type != 2) {
    ldout(cct,1) << __func__ << " existing " << existing << " proto "
		 << existing->protocol.get() << " version is "
		 << existing->protocol->proto_type << ", marking down" << dendl;
    existing->mark_down();
    existing = nullptr;
  }

  connection->inject_delay();

  connection->lock.lock();
  if (state != SESSION_ACCEPTING) {
    ldout(cct, 1) << __func__
                  << " state changed while accept, it must be mark_down"
                  << dendl;
    ceph_assert(state == CLOSED);
    return _fault();
  }

  if (existing) {
    return handle_existing_connection(existing);
  }

  // if everything is OK reply with server identification
  return send_server_ident();
}

CtPtr ProtocolV2::handle_reconnect(ceph::bufferlist &payload)
{
  ldout(cct, 20) << __func__
		 << " payload.length()=" << payload.length() << dendl;

  if (state != SESSION_ACCEPTING) {
    lderr(cct) << __func__ << " not in session accept state!" << dendl;
    return _fault();
  }

  auto reconnect = ReconnectFrame::Decode(payload);

  ldout(cct, 5) << __func__
                << " received reconnect:" 
                << " client_cookie=" << std::hex << reconnect.client_cookie()
                << " server_cookie=" << reconnect.server_cookie() << std::dec
                << " gs=" << reconnect.global_seq()
                << " cs=" << reconnect.connect_seq()
                << " ms=" << reconnect.msg_seq()
		            << dendl;

  // Should we check if one of the ident.addrs match connection->target_addr
  // as we do in ProtocolV1?
  connection->set_peer_addrs(reconnect.addrs());
  connection->target_addr = connection->_infer_target_addr(reconnect.addrs());
  peer_global_seq = reconnect.global_seq();

  connection->lock.unlock();
  AsyncConnectionRef existing = messenger->lookup_conn(*connection->peer_addrs);

  if (existing &&
      existing->protocol->proto_type != 2) {
    ldout(cct,1) << __func__ << " existing " << existing << " proto "
		 << existing->protocol.get() << " version is "
		 << existing->protocol->proto_type << ", marking down" << dendl;
    existing->mark_down();
    existing = nullptr;
  }

  connection->inject_delay();

  connection->lock.lock();
  if (state != SESSION_ACCEPTING) {
    ldout(cct, 1) << __func__
                  << " state changed while accept, it must be mark_down"
                  << dendl;
    ceph_assert(state == CLOSED);
    return _fault();
  }

  if (!existing) {
    // there is no existing connection therefore cannot reconnect to previous
    // session
    ldout(cct, 0) << __func__
                  << " no existing connection exists, reseting client" << dendl;
    auto reset = ResetFrame::Encode(true);
    return WRITE(reset, "session reset", read_frame);
  }

  std::lock_guard<std::mutex> l(existing->lock);

  ProtocolV2 *exproto = dynamic_cast<ProtocolV2 *>(existing->protocol.get());
  if (!exproto) {
    ldout(cct, 1) << __func__ << " existing=" << existing << dendl;
    ceph_assert(false);
  }

  if (exproto->state == CLOSED) {
    ldout(cct, 5) << __func__ << " existing " << existing
                  << " already closed. Reseting client" << dendl;
    auto reset = ResetFrame::Encode(true);
    return WRITE(reset, "session reset", read_frame);
  }

  if (exproto->replacing) {
    ldout(cct, 1) << __func__
                  << " existing racing replace happened while replacing."
                  << " existing=" << existing << dendl;
    auto retry = RetryGlobalFrame::Encode(exproto->peer_global_seq);
    return WRITE(retry, "session retry", read_frame);
  }

  if (exproto->client_cookie != reconnect.client_cookie()) {
    ldout(cct, 1) << __func__ << " existing=" << existing
                  << " client cookie mismatch, I must have reseted:"
                  << " cc=" << std::hex << exproto->client_cookie
                  << " rcc=" << reconnect.client_cookie()
                  << ", reseting client." << std::dec
                  << dendl;
    auto reset = ResetFrame::Encode(connection->policy.resetcheck);
    return WRITE(reset, "session reset", read_frame);
  } else if (exproto->server_cookie == 0) {
    // this happens when:
    //   - a connects to b
    //   - a sends client_ident
    //   - b gets client_ident, sends server_ident and sets cookie X
    //   - connection fault
    //   - b reconnects to a with cookie X, connect_seq=1
    //   - a has cookie==0
    ldout(cct, 1) << __func__ << " I was a client and didn't received the"
                  << " server_ident. Asking peer to resume session"
                  << " establishment" << dendl;
    auto reset = ResetFrame::Encode(false);
    return WRITE(reset, "session reset", read_frame);
  }

  if (exproto->peer_global_seq > reconnect.global_seq()) {
    ldout(cct, 5) << __func__
                  << " stale global_seq: sgs=" << exproto->peer_global_seq
                  << " cgs=" << reconnect.global_seq()
                  << ", ask client to retry global" << dendl;
    auto retry = RetryGlobalFrame::Encode(exproto->peer_global_seq);

    INTERCEPT(18);

    return WRITE(retry, "session retry", read_frame);
  }

  if (exproto->connect_seq > reconnect.connect_seq()) {
    ldout(cct, 5) << __func__
                  << " stale connect_seq scs=" << exproto->connect_seq
                  << " ccs=" << reconnect.connect_seq()
                  << " , ask client to retry" << dendl;
    auto retry = RetryFrame::Encode(exproto->connect_seq);
    return WRITE(retry, "session retry", read_frame);
  }

  if (exproto->connect_seq == reconnect.connect_seq()) {
    // reconnect race: both peers are sending reconnect messages
    if (existing->peer_addrs->msgr2_addr() >
            messenger->get_myaddrs().msgr2_addr() &&
        !existing->policy.server) {
      // the existing connection wins
      ldout(cct, 1)
          << __func__
          << " reconnect race detected, this connection loses to existing="
          << existing << dendl;

      auto wait = WaitFrame::Encode();
      return WRITE(wait, "wait", read_frame);
    } else {
      // this connection wins
      ldout(cct, 1) << __func__
                    << " reconnect race detected, replacing existing="
                    << existing << " socket by this connection's socket"
                    << dendl;
    }
  }

  ldout(cct, 1) << __func__ << " reconnect to existing=" << existing << dendl;

  reconnecting = true;

  // everything looks good
  exproto->connect_seq = reconnect.connect_seq();
  exproto->message_seq = reconnect.msg_seq();

  return reuse_connection(existing, exproto);
}

CtPtr ProtocolV2::handle_existing_connection(AsyncConnectionRef existing) {
  ldout(cct, 20) << __func__ << " existing=" << existing << dendl;

  std::lock_guard<std::mutex> l(existing->lock);

  ProtocolV2 *exproto = dynamic_cast<ProtocolV2 *>(existing->protocol.get());
  if (!exproto) {
    ldout(cct, 1) << __func__ << " existing=" << existing << dendl;
    ceph_assert(false);
  }

  if (exproto->state == CLOSED) {
    ldout(cct, 1) << __func__ << " existing " << existing << " already closed."
                  << dendl;
    return send_server_ident();
  }

  if (exproto->replacing) {
    ldout(cct, 1) << __func__
                  << " existing racing replace happened while replacing."
                  << " existing=" << existing << dendl;
    auto wait = WaitFrame::Encode();
    return WRITE(wait, "wait", read_frame);
  }

  if (exproto->peer_global_seq > peer_global_seq) {
    ldout(cct, 1) << __func__ << " this is a stale connection, peer_global_seq="
                  << peer_global_seq
                  << " existing->peer_global_seq=" << exproto->peer_global_seq
                  << ", stopping this connection." << dendl;
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return nullptr;
  }

  if (existing->policy.lossy) {
    // existing connection can be thrown out in favor of this one
    ldout(cct, 1)
        << __func__ << " existing=" << existing
        << " is a lossy channel. Stopping existing in favor of this connection"
        << dendl;
    existing->protocol->stop();
    existing->dispatch_queue->queue_reset(existing.get());
    return send_server_ident();
  }

  if (exproto->server_cookie && exproto->client_cookie &&
      exproto->client_cookie != client_cookie) {
    // Found previous session
    // peer has reseted and we're going to reuse the existing connection
    // by replacing the communication socket
    ldout(cct, 1) << __func__ << " found previous session existing=" << existing
                  << ", peer must have reseted." << dendl;
    if (connection->policy.resetcheck) {
      exproto->reset_session();
    }
    return reuse_connection(existing, exproto);
  }

  if (exproto->client_cookie == client_cookie) {
    // session establishment interrupted between client_ident and server_ident,
    // continuing...
    ldout(cct, 1) << __func__ << " found previous session existing=" << existing
                  << ", continuing session establishment." << dendl;
    return reuse_connection(existing, exproto);
  }

  if (exproto->state == READY || exproto->state == STANDBY) {
    ldout(cct, 1) << __func__ << " existing=" << existing
                  << " is READY/STANDBY, lets reuse it" << dendl;
    return reuse_connection(existing, exproto);
  }

  // Looks like a connection race: server and client are both connecting to
  // each other at the same time.
  if (connection->peer_addrs->msgr2_addr() <
          messenger->get_myaddrs().msgr2_addr() ||
      existing->policy.server) {
    // this connection wins
    ldout(cct, 1) << __func__
                  << " connection race detected, replacing existing="
                  << existing << " socket by this connection's socket" << dendl;
    return reuse_connection(existing, exproto);
  } else {
    // the existing connection wins
    ldout(cct, 1)
        << __func__
        << " connection race detected, this connection loses to existing="
        << existing << dendl;
    ceph_assert(connection->peer_addrs->msgr2_addr() >
                messenger->get_myaddrs().msgr2_addr());

    // make sure we follow through with opening the existing
    // connection (if it isn't yet open) since we know the peer
    // has something to send to us.
    existing->send_keepalive();
    auto wait = WaitFrame::Encode();
    return WRITE(wait, "wait", read_frame);
  }
}

CtPtr ProtocolV2::reuse_connection(AsyncConnectionRef existing,
                                   ProtocolV2 *exproto) {
  ldout(cct, 20) << __func__ << " existing=" << existing
                 << " reconnect=" << reconnecting << dendl;

  connection->inject_delay();

  std::lock_guard<std::mutex> l(existing->write_lock);

  connection->center->delete_file_event(connection->cs.fd(),
                                        EVENT_READABLE | EVENT_WRITABLE);

  if (existing->delay_state) {
    existing->delay_state->flush();
    ceph_assert(!connection->delay_state);
  }
  exproto->reset_recv_state();
  exproto->pre_auth.enabled = false;

  if (!reconnecting) {
    exproto->client_cookie = client_cookie;
    exproto->peer_name = peer_name;
    exproto->connection_features = connection_features;
    existing->set_features(connection_features);
  }
  exproto->peer_global_seq = peer_global_seq;

  auto temp_cs = std::move(connection->cs);
  EventCenter *new_center = connection->center;
  Worker *new_worker = connection->worker;

  ldout(messenger->cct, 5) << __func__ << " stop myself to swap existing"
                           << dendl;
  // avoid _stop shutdown replacing socket
  // queue a reset on the new connection, which we're dumping for the old
  stop();

  connection->dispatch_queue->queue_reset(connection);

  exproto->can_write = false;
  exproto->reconnecting = reconnecting;
  exproto->replacing = true;
  std::swap(exproto->session_stream_handlers, session_stream_handlers);
  exproto->auth_meta = auth_meta;
  existing->state_offset = 0;
  // avoid previous thread modify event
  exproto->state = NONE;
  existing->state = AsyncConnection::STATE_NONE;
  // Discard existing prefetch buffer in `recv_buf`
  existing->recv_start = existing->recv_end = 0;
  // there shouldn't exist any buffer
  ceph_assert(connection->recv_start == connection->recv_end);

  auto deactivate_existing = std::bind(
      [existing, new_worker, new_center, exproto](ConnectedSocket &cs) mutable {
        // we need to delete time event in original thread
        {
          std::lock_guard<std::mutex> l(existing->lock);
          existing->write_lock.lock();
          exproto->requeue_sent();
          existing->outcoming_bl.clear();
          existing->open_write = false;
          existing->write_lock.unlock();
          if (exproto->state == NONE) {
            existing->shutdown_socket();
            existing->cs = std::move(cs);
            existing->worker->references--;
            new_worker->references++;
            existing->logger = new_worker->get_perf_counter();
            existing->worker = new_worker;
            existing->center = new_center;
            if (existing->delay_state)
              existing->delay_state->set_center(new_center);
          } else if (exproto->state == CLOSED) {
            auto back_to_close = std::bind(
                [](ConnectedSocket &cs) mutable { cs.close(); }, std::move(cs));
            new_center->submit_to(new_center->get_id(),
                                  std::move(back_to_close), true);
            return;
          } else {
            ceph_abort();
          }
        }

        // Before changing existing->center, it may already exists some
        // events in existing->center's queue. Then if we mark down
        // `existing`, it will execute in another thread and clean up
        // connection. Previous event will result in segment fault
        auto transfer_existing = [existing, exproto]() mutable {
          std::lock_guard<std::mutex> l(existing->lock);
          if (exproto->state == CLOSED) return;
          ceph_assert(exproto->state == NONE);

          exproto->state = SESSION_ACCEPTING;
          existing->state = AsyncConnection::STATE_CONNECTION_ESTABLISHED;
          existing->center->create_file_event(existing->cs.fd(), EVENT_READABLE,
                                              existing->read_handler);
          if (!exproto->reconnecting) {
            exproto->run_continuation(exproto->send_server_ident());
          } else {
            exproto->run_continuation(exproto->send_reconnect_ok());
          }
        };
        if (existing->center->in_thread())
          transfer_existing();
        else
          existing->center->submit_to(existing->center->get_id(),
                                      std::move(transfer_existing), true);
      },
      std::move(temp_cs));

  existing->center->submit_to(existing->center->get_id(),
                              std::move(deactivate_existing), true);
  return nullptr;
}

CtPtr ProtocolV2::send_server_ident() {
  ldout(cct, 20) << __func__ << dendl;

  // this is required for the case when this connection is being replaced
  out_seq = discard_requeued_up_to(out_seq, 0);
  in_seq = 0;

  if (!connection->policy.lossy) {
    server_cookie = ceph::util::generate_random_number<uint64_t>(1, -1ll);
  }

  uint64_t flags = 0;
  if (connection->policy.lossy) {
    flags = flags | CEPH_MSG_CONNECT_LOSSY;
  }

  uint64_t gs = messenger->get_global_seq();
  auto server_ident = ServerIdentFrame::Encode(
          messenger->get_myaddrs(),
          messenger->get_myname().num(),
          gs,
          connection->policy.features_supported,
          connection->policy.features_required | msgr2_required,
          flags,
          server_cookie);

  ldout(cct, 5) << __func__ << " sending identification:"
                << " addrs=" << messenger->get_myaddrs()
                << " gid=" << messenger->get_myname().num()
                << " global_seq=" << gs << " features_supported=" << std::hex
                << connection->policy.features_supported
                << " features_required="
		            << (connection->policy.features_required | msgr2_required)
                << " flags=" << flags << " cookie=" << std::dec << server_cookie
                << dendl;

  connection->lock.unlock();
  // Because "replacing" will prevent other connections preempt this addr,
  // it's safe that here we don't acquire Connection's lock
  ssize_t r = messenger->accept_conn(connection);

  connection->inject_delay();

  connection->lock.lock();

  if (r < 0) {
    ldout(cct, 1) << __func__ << " existing race replacing process for addr = "
                  << connection->peer_addrs->msgr2_addr()
                  << " just fail later one(this)" << dendl;
    connection->inject_delay();
    return _fault();
  }
  if (state != SESSION_ACCEPTING) {
    ldout(cct, 1) << __func__
                  << " state changed while accept_conn, it must be mark_down"
                  << dendl;
    ceph_assert(state == CLOSED || state == NONE);
    messenger->unregister_conn(connection);
    connection->inject_delay();
    return _fault();
  }

  connection->set_features(connection_features);

  // notify
  connection->dispatch_queue->queue_accept(connection);
  messenger->ms_deliver_handle_fast_accept(connection);

  INTERCEPT(12);

  return WRITE(server_ident, "server ident", server_ready);
}

CtPtr ProtocolV2::server_ready() {
  ldout(cct, 20) << __func__ << dendl;

  if (connection->delay_state) {
    ceph_assert(connection->delay_state->ready());
  }

  return ready();
}

CtPtr ProtocolV2::send_reconnect_ok() {
  ldout(cct, 20) << __func__ << dendl;

  out_seq = discard_requeued_up_to(out_seq, message_seq);

  uint64_t ms = in_seq;
  auto reconnect_ok = ReconnectOkFrame::Encode(ms);

  ldout(cct, 5) << __func__ << " sending reconnect_ok: msg_seq=" << ms << dendl;

  connection->lock.unlock();
  // Because "replacing" will prevent other connections preempt this addr,
  // it's safe that here we don't acquire Connection's lock
  ssize_t r = messenger->accept_conn(connection);

  connection->inject_delay();

  connection->lock.lock();

  if (r < 0) {
    ldout(cct, 1) << __func__ << " existing race replacing process for addr = "
                  << connection->peer_addrs->msgr2_addr()
                  << " just fail later one(this)" << dendl;
    connection->inject_delay();
    return _fault();
  }
  if (state != SESSION_ACCEPTING) {
    ldout(cct, 1) << __func__
                  << " state changed while accept_conn, it must be mark_down"
                  << dendl;
    ceph_assert(state == CLOSED || state == NONE);
    messenger->unregister_conn(connection);
    connection->inject_delay();
    return _fault();
  }

  // notify
  connection->dispatch_queue->queue_accept(connection);
  messenger->ms_deliver_handle_fast_accept(connection);

  INTERCEPT(14);

  return WRITE(reconnect_ok, "reconnect ok", server_ready);
}
