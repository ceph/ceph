// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ProtocolV2.h"
#include "AsyncMessenger.h"

#include "common/EventTrace.h"
#include "common/errno.h"
#include "include/random.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _conn_prefix(_dout)
ostream &ProtocolV2::_conn_prefix(std::ostream *_dout) {
  return *_dout << "--2- " << messenger->get_myaddrs() << " >> "
                << *connection->peer_addrs
		<< " conn("
                << connection << " " << this
                << " :" << connection->port << " s=" << get_state_name(state)
                << " pgs=" << peer_global_seq << " cs=" << connect_seq
                << " l=" << connection->policy.lossy << ").";
}

const int ASYNC_COALESCE_THRESHOLD = 256;

#define WRITE(B, C) write(CONTINUATION(C), B)

#define READ(L, C) read(CONTINUATION(C), L)

#define READB(L, B, C) read(CONTINUATION(C), L, B)

using CtPtr = Ct<ProtocolV2> *;

static void alloc_aligned_buffer(bufferlist &data, unsigned len, unsigned off) {
  // create a buffer to read into that matches the data alignment
  unsigned alloc_len = 0;
  unsigned left = len;
  unsigned head = 0;
  if (off & ~CEPH_PAGE_MASK) {
    // head
    alloc_len += CEPH_PAGE_SIZE;
    head = std::min<uint64_t>(CEPH_PAGE_SIZE - (off & ~CEPH_PAGE_MASK), left);
    left -= head;
  }
  alloc_len += left;
  bufferptr ptr(buffer::create_small_page_aligned(alloc_len));
  if (head) ptr.set_offset(CEPH_PAGE_SIZE - head);
  data.push_back(std::move(ptr));
}

ProtocolV2::ProtocolV2(AsyncConnection *connection)
    : Protocol(2, connection),
      temp_buffer(nullptr),
      state(NONE),
      peer_required_features(0),
      cookie(0),
      message_seq(0),
      can_write(false),
      connect_seq(0),
      bannerExchangeCallback(nullptr),
      next_frame_len(0),
      keepalive(false) {
  temp_buffer = new char[4096];
}

ProtocolV2::~ProtocolV2() { delete[] temp_buffer; }

void ProtocolV2::connect() { state = START_CONNECT; }

void ProtocolV2::accept() { state = START_ACCEPT; }

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
  for (map<int, list<pair<bufferlist, Message *> > >::iterator p =
           out_queue.begin();
       p != out_queue.end(); ++p) {
    for (list<pair<bufferlist, Message *> >::iterator r = p->second.begin();
         r != p->second.end(); ++r) {
      ldout(cct, 20) << __func__ << " discard " << r->second << dendl;
      r->second->put();
    }
  }
  out_queue.clear();
}

void ProtocolV2::stop() {
  ldout(cct, 2) << __func__ << dendl;
  if (state == CLOSED) {
    return;
  }

  if (connection->delay_state) connection->delay_state->flush();

  std::lock_guard<std::mutex> l(connection->write_lock);

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

  list<pair<bufferlist, Message *> > &rq = out_queue[CEPH_MSG_PRIO_HIGHEST];
  out_seq -= sent.size();
  while (!sent.empty()) {
    Message *m = sent.back();
    sent.pop_back();
    ldout(cct, 10) << __func__ << " " << *m << " for resend "
                   << " (" << m->get_seq() << ")" << dendl;
    rq.push_front(make_pair(bufferlist(), m));
  }
}

void ProtocolV2::reset_recv_state() {
  // clean read and write callbacks
  connection->pendingReadLen.reset();
  connection->writeCallback.reset();

  uint32_t cur_msg_size = current_header.front_len + current_header.middle_len +
                          current_header.data_len;

  if (state > THROTTLE_MESSAGE && state <= READ_MESSAGE_FRONT &&
      connection->policy.throttler_messages) {
    ldout(cct, 10) << __func__ << " releasing " << 1
                   << " message to policy throttler "
                   << connection->policy.throttler_messages->get_current()
                   << "/" << connection->policy.throttler_messages->get_max()
                   << dendl;
    connection->policy.throttler_messages->put();
  }
  if (state > THROTTLE_BYTES && state <= READ_MESSAGE_FRONT) {
    if (connection->policy.throttler_bytes) {
      ldout(cct, 10) << __func__ << " releasing " << cur_msg_size
                     << " bytes to policy throttler "
                     << connection->policy.throttler_bytes->get_current() << "/"
                     << connection->policy.throttler_bytes->get_max() << dendl;
      connection->policy.throttler_bytes->put(cur_msg_size);
    }
  }
  if (state > THROTTLE_DISPATCH_QUEUE && state <= READ_MESSAGE_FRONT) {
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

  if (connection->policy.lossy && state != START_CONNECT &&
      state != CONNECTING) {
    ldout(cct, 1) << __func__ << " on lossy channel, failing" << dendl;
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return nullptr;
  }

  connection->write_lock.lock();

  can_write = false;
  // requeue sent items
  requeue_sent();

  if (out_queue.empty() && state >= START_ACCEPT &&
      state <= ACCEPTED_CLIENT_IDENT) {
    ldout(cct, 10) << __func__ << " with nothing to send and in the half "
                   << " accept state just closed" << dendl;
    stop();
    connection->dispatch_queue->queue_reset(connection);
    connection->write_lock.unlock();
    return nullptr;
  }

  connection->fault();
  reset_recv_state();

  if (connection->policy.standby && out_queue.empty() && state != WAIT) {
    ldout(cct, 10) << __func__ << " with nothing to send, going to standby"
                   << dendl;
    state = STANDBY;
    connection->write_lock.unlock();
    return nullptr;
  }

  connection->write_lock.unlock();

  if (state != START_CONNECT && state != CONNECTING && state != WAIT) {
    // policy maybe empty when state is in accept
    if (connection->policy.server) {
      ldout(cct, 0) << __func__ << " server, going to standby" << dendl;
      state = STANDBY;
    } else {
      ldout(cct, 0) << __func__ << " initiating reconnect" << dendl;
      connect_seq++;
      state = START_CONNECT;
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

    state = START_CONNECT;
    connection->state = AsyncConnection::STATE_CONNECTING;
    ldout(cct, 10) << __func__ << " waiting " << backoff << dendl;
    // woke up again;
    connection->register_time_events.insert(
        connection->center->create_time_event(backoff.to_nsec() / 1000,
                                              connection->wakeup_handler));
  }
  return nullptr;
}

void ProtocolV2::prepare_send_message(uint64_t features, Message *m,
                                      bufferlist &bl) {
  ldout(cct, 20) << __func__ << " m=" << *m << dendl;

  // associate message with Connection (for benefit of encode_payload)
  if (m->empty_payload()) {
    ldout(cct, 20) << __func__ << " encoding features " << features << " " << m
                   << " " << *m << dendl;
  } else {
    ldout(cct, 20) << __func__ << " half-reencoding features " << features
                   << " " << m << " " << *m << dendl;
  }

  // encode and copy out of *m
  m->encode(features, messenger->crcflags);

  bl.append(m->get_payload());
  bl.append(m->get_middle());
  bl.append(m->get_data());
}

void ProtocolV2::send_message(Message *m) {
  bufferlist bl;
  uint64_t f = connection->get_features();

  // TODO: Currently not all messages supports reencode like MOSDMap, so here
  // only let fast dispatch support messages prepare message
  bool can_fast_prepare = messenger->ms_can_fast_dispatch(m);
  if (can_fast_prepare) {
    prepare_send_message(f, m, bl);
  }

  std::lock_guard<std::mutex> l(connection->write_lock);
  // "features" changes will change the payload encoding
  if (can_fast_prepare && (!can_write || connection->get_features() != f)) {
    // ensure the correctness of message encoding
    bl.clear();
    m->clear_payload();
    ldout(cct, 5) << __func__ << " clear encoded buffer previous " << f
                  << " != " << connection->get_features() << dendl;
  }
  if (state == CLOSED) {
    ldout(cct, 10) << __func__ << " connection closed."
                   << " Drop message " << m << dendl;
    m->put();
  } else {
    m->trace.event("async enqueueing message");
    out_queue[m->get_priority()].emplace_back(std::move(bl), m);
    ldout(cct, 15) << __func__ << " inline write is denied, reschedule m=" << m
                   << dendl;
    if (can_write) {
      connection->center->dispatch_event_external(connection->write_handler);
    }
  }
}

void ProtocolV2::send_keepalive() {
  ldout(cct, 10) << __func__ << dendl;
  std::lock_guard<std::mutex> l(connection->write_lock);
  if (can_write) {
    keepalive = true;
    connection->center->dispatch_event_external(connection->write_handler);
  }
}

void ProtocolV2::read_event() {
  ldout(cct, 20) << __func__ << dendl;

  switch (state) {
    case START_CONNECT:
      CONTINUATION_RUN(CONTINUATION(start_client_banner_exchange));
      break;
    case START_ACCEPT:
      CONTINUATION_RUN(CONTINUATION(start_server_banner_exchange));
      break;
    case READY:
      CONTINUATION_RUN(CONTINUATION(read_frame));
      break;
    case THROTTLE_MESSAGE:
      CONTINUATION_RUN(CONTINUATION(throttle_message));
      break;
    case THROTTLE_BYTES:
      CONTINUATION_RUN(CONTINUATION(throttle_bytes));
      break;
    case THROTTLE_DISPATCH_QUEUE:
      CONTINUATION_RUN(CONTINUATION(throttle_dispatch_queue));
      break;
    default:
      break;
  }
}

Message *ProtocolV2::_get_next_outgoing(bufferlist *bl) {
  Message *m = 0;
  if (!out_queue.empty()) {
    map<int, list<pair<bufferlist, Message *> > >::reverse_iterator it =
        out_queue.rbegin();
    ceph_assert(!it->second.empty());
    list<pair<bufferlist, Message *> >::iterator p = it->second.begin();
    m = p->second;
    if (bl) {
      bl->swap(p->first);
    }
    it->second.erase(p);
    if (it->second.empty()) {
      out_queue.erase(it->first);
    }
  }
  return m;
}

ssize_t ProtocolV2::write_message(Message *m, bufferlist &bl, bool more) {
  FUNCTRACE(cct);
  ceph_assert(connection->center->in_thread());
  m->set_seq(++out_seq);

  connection->lock.lock();
  uint64_t ack_seq = in_seq;
  ack_left = 0;
  connection->lock.unlock();

  MessageFrame message(m, bl, ack_seq, messenger->crcflags & MSG_CRC_HEADER);

  ldout(cct, 20) << __func__ << " sending message type=" << message.header2.type
                 << " src " << entity_name_t(messenger->get_myname())
                 << " front=" << message.header2.front_len
                 << " data=" << message.header2.data_len << " off "
                 << message.header2.data_off << dendl;

  bufferlist &msg_bl = message.get_buffer();
  connection->outcoming_bl.claim_append(msg_bl);

  m->trace.event("async writing message");
  ldout(cct, 20) << __func__ << " sending " << m->get_seq() << " " << m
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
  if (m->get_type() == CEPH_MSG_OSD_OP)
    OID_EVENT_TRACE_WITH_MSG(m, "SEND_MSG_OSD_OP_END", false);
  else if (m->get_type() == CEPH_MSG_OSD_OPREPLY)
    OID_EVENT_TRACE_WITH_MSG(m, "SEND_MSG_OSD_OPREPLY_END", false);
  m->put();

  return rc;
}

void ProtocolV2::append_keepalive() {
  ldout(cct, 10) << __func__ << dendl;
  KeepAliveFrame keepalive_frame;
  connection->outcoming_bl.claim_append(keepalive_frame.get_buffer());
}

void ProtocolV2::append_keepalive_ack(utime_t &timestamp) {
  struct ceph_timespec ts;
  timestamp.encode_timeval(&ts);
  KeepAliveFrame keepalive_ack_frame(ts);
  connection->outcoming_bl.claim_append(keepalive_ack_frame.get_buffer());
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
      bufferlist data;
      Message *m = _get_next_outgoing(&data);
      if (!m) {
        break;
      }

      if (!connection->policy.lossy) {
        // put on sent list
        sent.push_back(m);
        m->get();
      }
      more = !out_queue.empty();
      connection->write_lock.unlock();

      // send_message or requeue messages may not encode message
      if (!data.length()) {
        prepare_send_message(connection->get_features(), m, data);
      }

      r = write_message(m, data, more);

      connection->write_lock.lock();
      if (r == 0) {
        ;
      } else if (r < 0) {
        ldout(cct, 1) << __func__ << " send msg failed" << dendl;
        break;
      } else if (r > 0)
        break;
    } while (can_write);
    connection->write_lock.unlock();

    // if r > 0 mean data still lefted, so no need _try_send.
    if (r == 0) {
      uint64_t left = ack_left;
      if (left) {
        ceph_le64 s;
        s = in_seq;
        AckFrame ack(in_seq);
        connection->outcoming_bl.claim_append(ack.get_buffer());
        ldout(cct, 10) << __func__ << " try send msg ack, acked " << left
                       << " messages" << dendl;
        ack_left -= left;
        left = ack_left;
        r = connection->_try_send(left);
      } else if (is_queued()) {
        r = connection->_try_send();
      }
    }

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

bool ProtocolV2::is_queued() { return false; }

CtPtr ProtocolV2::read(CONTINUATION_PARAM(next, ProtocolV2, char *, int),
                       int len, char *buffer) {
  if (!buffer) {
    buffer = temp_buffer;
  }
  ssize_t r = connection->read(len, buffer,
                               [CONTINUATION(next), this](char *buffer, int r) {
                                 CONTINUATION(next)->setParams(buffer, r);
                                 CONTINUATION_RUN(CONTINUATION(next));
                               });
  if (r <= 0) {
    return CONTINUE(next, buffer, r);
  }

  return nullptr;
}

CtPtr ProtocolV2::write(CONTINUATION_PARAM(next, ProtocolV2, int),
                        bufferlist &buffer) {
  ssize_t r = connection->write(buffer, [CONTINUATION(next), this](int r) {
    CONTINUATION(next)->setParams(r);
    CONTINUATION_RUN(CONTINUATION(next));
  });
  if (r <= 0) {
    return CONTINUE(next, r);
  }

  return nullptr;
}

CtPtr ProtocolV2::_banner_exchange(CtPtr callback) {
  ldout(cct, 20) << __func__ << dendl;
  bannerExchangeCallback = callback;

  uint8_t type = messenger->get_mytype();
  __le64 supported_features = CEPH_MSGR2_SUPPORTED_FEATURES;
  __le64 required_features = CEPH_MSGR2_REQUIRED_FEATURES;

  size_t banner_prefix_len = strlen(CEPH_BANNER_V2_PREFIX);
  size_t banner_len = banner_prefix_len + sizeof(uint8_t) + 2 * sizeof(__le64);
  char banner[banner_len];
  uint8_t offset = 0;
  memcpy(banner, CEPH_BANNER_V2_PREFIX, banner_prefix_len);
  offset += banner_prefix_len;
  memcpy(banner + offset, (void *)&type, sizeof(uint8_t));
  offset += sizeof(uint8_t);
  memcpy(banner + offset, (void *)&supported_features, sizeof(__le64));
  offset += sizeof(__le64);
  memcpy(banner + offset, (void *)&required_features, sizeof(__le64));

  bufferlist bl;
  bl.append(banner, banner_len);

  return WRITE(bl, _banner_exchange_handle_write);
}

CtPtr ProtocolV2::_banner_exchange_handle_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;
  if (r < 0) {
    ldout(cct, 1) << __func__ << " write banner failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  unsigned banner_len =
      strlen(CEPH_BANNER_V2_PREFIX) + sizeof(uint8_t) + 2 * sizeof(__le64);
  return READ(banner_len, _banner_exchange_handle_peer_banner);
}

CtPtr ProtocolV2::_banner_exchange_handle_peer_banner(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read peer banner failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

unsigned banner_prefix_len = strlen(CEPH_BANNER_V2_PREFIX);

  if (memcmp(buffer, CEPH_BANNER_V2_PREFIX, banner_prefix_len)) {
    if (memcmp(buffer, CEPH_BANNER, strlen(CEPH_BANNER))) {
      lderr(cct) << __func__ << " peer " << *connection->peer_addrs
                 << " is using msgr V1 protocol" << dendl;
      return _fault();
    }
    ldout(cct, 1) << __func__ << " accept peer sent bad banner" << dendl;
    return _fault();
  }

  uint8_t peer_type = 0;
  __le64 peer_supported_features;
  __le64 peer_required_features;

  uint8_t offset = banner_prefix_len;
  peer_type = *(uint8_t *)(buffer + offset);
  offset += sizeof(uint8_t);
  peer_supported_features = *(__le64 *)(buffer + offset);
  offset += sizeof(__le64);
  peer_required_features = *(__le64 *)(buffer + offset);

  ldout(cct, 1) << __func__ << " banner peer_type=" << (int)peer_type
                << " supported=" << std::hex << peer_supported_features
                << " required=" << std::hex << peer_required_features
                << std::dec << dendl;

  if (connection->get_peer_type() == -1) {
    connection->set_peer_type(peer_type);

    ceph_assert(state == ACCEPTING);
    connection->policy = messenger->get_policy(peer_type);
    ldout(cct, 10) << __func__ << " accept of host_type " << peer_type
                   << ", policy.lossy=" << connection->policy.lossy
                   << " policy.server=" << connection->policy.server
                   << " policy.standby=" << connection->policy.standby
                   << " policy.resetcheck=" << connection->policy.resetcheck
                   << dendl;
  } else {
    if (connection->get_peer_type() != peer_type) {
      ldout(cct, 1) << __func__ << " connection peer type does not match what"
                    << " peer advertises " << connection->get_peer_type()
                    << " != " << peer_type << dendl;
      stop();
      connection->dispatch_queue->queue_reset(connection);
      return nullptr;
    }
  }

  // Check feature bit compatibility

  __le64 supported_features = CEPH_MSGR2_SUPPORTED_FEATURES;
  __le64 required_features = CEPH_MSGR2_REQUIRED_FEATURES;

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
                  << " supported=" << std::hex << supported_features << dendl;
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return nullptr;
  }

  this->peer_required_features = peer_required_features;

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
  return READ(sizeof(__le32) * 2, handle_read_frame_length_and_tag);
}

CtPtr ProtocolV2::handle_read_frame_length_and_tag(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read frame length and tag failed r=" << r
                  << " (" << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  next_frame_len = *(uint32_t *)buffer - sizeof(uint32_t);
  next_tag = static_cast<Tag>(*(uint32_t *)(buffer + sizeof(uint32_t)));

  ldout(cct, 10) << __func__ << " next frame_len=" << next_frame_len
                 << " tag=" << static_cast<uint32_t>(next_tag) << dendl;

  switch (next_tag) {
    case Tag::AUTH_REQUEST:
    case Tag::AUTH_BAD_METHOD:
    case Tag::AUTH_BAD_AUTH:
    case Tag::AUTH_MORE:
    case Tag::AUTH_DONE:
    case Tag::IDENT:
    case Tag::IDENT_MISSING_FEATURES:
    case Tag::KEEPALIVE2:
    case Tag::KEEPALIVE2_ACK:
    case Tag::ACK:
      return READ(next_frame_len, handle_frame_payload);
    case Tag::MESSAGE:
      return handle_message();
  }

  return nullptr;
}

CtPtr ProtocolV2::handle_frame_payload(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read frame payload failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  switch (next_tag) {
    case Tag::AUTH_REQUEST:
      return handle_auth_request(buffer, next_frame_len);
    case Tag::AUTH_BAD_METHOD:
      return handle_auth_bad_method(buffer, next_frame_len);
    case Tag::AUTH_BAD_AUTH:
      return handle_auth_bad_auth(buffer, next_frame_len);
    case Tag::AUTH_MORE:
      return handle_auth_more(buffer, next_frame_len);
    case Tag::AUTH_DONE:
      return handle_auth_done(buffer, next_frame_len);
    case Tag::IDENT:
      return handle_ident(buffer, next_frame_len);
    case Tag::IDENT_MISSING_FEATURES:
      return handle_ident_missing_features(buffer, next_frame_len);
    case Tag::KEEPALIVE2:
      return handle_keepalive2(buffer, next_frame_len);
    case Tag::KEEPALIVE2_ACK:
      return handle_keepalive2_ack(buffer, next_frame_len);
    case Tag::ACK:
      return handle_message_ack(buffer, next_frame_len);
    default:
      ceph_abort();
  }
  return nullptr;
}

CtPtr ProtocolV2::handle_auth_more(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  AuthMoreFrame auth_more(payload, length);
  ldout(cct, 1) << __func__ << " auth more len=" << auth_more.len << dendl;

  /* BEGIN TO REMOVE */
  auto p = auth_more.auth_payload.cbegin();
  int32_t i;
  std::string s;
  try {
    decode(i, p);
    decode(s, p);
  } catch (const buffer::error &e) {
    lderr(cct) << __func__ << " decode auth_payload failed" << dendl;
    return _fault();
  }

  ldout(cct, 10) << __func__ << " (TO REMOVE) auth_more (" << (int32_t)i << ", "
                 << s << ")" << dendl;

  if (i == 45 && s == "hello server more") {
    bufferlist auth_bl;
    encode((int32_t)55, auth_bl, 0);
    std::string hello("hello client more");
    encode(hello, auth_bl, 0);
    /* END TO REMOVE */
    AuthMoreFrame more(auth_bl);
    bufferlist &bl = more.get_buffer();
    return WRITE(bl, handle_auth_more_write);
  }
  /* END TO REMOVE */

  AuthDoneFrame auth_done(0);

  bufferlist &bl = auth_done.get_buffer();
  return WRITE(bl, handle_auth_done_write);
}

CtPtr ProtocolV2::handle_auth_more_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " auth more write failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_ident(char *payload, uint32_t length) {
  if (state == CONNECTING) {
    return handle_server_ident(payload, length);
  }
  if (state == ACCEPTING) {
    return handle_client_ident(payload, length);
  }
  ceph_abort("wrong state at handle_ident");
}

CtPtr ProtocolV2::ready() {
  ldout(cct, 25) << __func__ << dendl;

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

  state = READY;

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_message() {
  ldout(cct, 20) << __func__ << dendl;

  ceph_assert(state == READY);

  return READ(sizeof(ceph_msg_header2), handle_message_header);
}

CtPtr ProtocolV2::handle_message_header(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read message header failed" << dendl;
    return _fault();
  }

  ceph_msg_header2 header;
  header = *((ceph_msg_header2 *)buffer);

  entity_name_t src(connection->peer_type, connection->peer_global_id);

  ldout(cct, 20) << __func__ << " got envelope type=" << header.type << " src "
                 << src << " front=" << header.front_len
                 << " data=" << header.data_len << " off " << header.data_off
                 << dendl;

  if (messenger->crcflags & MSG_CRC_HEADER) {
    __u32 header_crc = 0;
    header_crc = ceph_crc32c(0, (unsigned char *)&header,
                             sizeof(header) - sizeof(header.header_crc));
    // verify header crc
    if (header_crc != header.header_crc) {
      ldout(cct, 0) << __func__ << " got bad header crc " << header_crc
                    << " != " << header.header_crc << dendl;
      return _fault();
    }
  }

  // Reset state
  data_buf.clear();
  front.clear();
  middle.clear();
  data.clear();
  current_header = header;

  state = THROTTLE_MESSAGE;
  return CONTINUE(throttle_message);
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

  uint32_t cur_msg_size = current_header.front_len + current_header.middle_len +
                          current_header.data_len;
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

  uint32_t cur_msg_size = current_header.front_len + current_header.middle_len +
                          current_header.data_len;

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

  state = READ_MESSAGE_FRONT;
  return read_message_front();
}

CtPtr ProtocolV2::read_message_front() {
  ldout(cct, 20) << __func__ << dendl;

  unsigned front_len = current_header.front_len;
  if (front_len) {
    if (!front.length()) {
      front.push_back(buffer::create(front_len));
    }
    return READB(front_len, front.c_str(), handle_message_front);
  }
  return read_message_middle();
}

CtPtr ProtocolV2::handle_message_front(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read message front failed" << dendl;
    return _fault();
  }

  ldout(cct, 20) << __func__ << " got front " << front.length() << dendl;

  return read_message_middle();
}

CtPtr ProtocolV2::read_message_middle() {
  ldout(cct, 20) << __func__ << dendl;

  if (current_header.middle_len) {
    if (!middle.length()) {
      middle.push_back(buffer::create(current_header.middle_len));
    }
    return READB(current_header.middle_len, middle.c_str(),
                 handle_message_middle);
  }

  return read_message_data_prepare();
}

CtPtr ProtocolV2::handle_message_middle(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read message middle failed" << dendl;
    return _fault();
  }

  ldout(cct, 20) << __func__ << " got middle " << middle.length() << dendl;

  return read_message_data_prepare();
}

CtPtr ProtocolV2::read_message_data_prepare() {
  ldout(cct, 20) << __func__ << dendl;

  unsigned data_len = le32_to_cpu(current_header.data_len);
  unsigned data_off = le32_to_cpu(current_header.data_off);

  if (data_len) {
    // get a buffer
    map<ceph_tid_t, pair<bufferlist, int> >::iterator p =
        connection->rx_buffers.find(current_header.tid);
    if (p != connection->rx_buffers.end()) {
      ldout(cct, 10) << __func__ << " seleting rx buffer v " << p->second.second
                     << " at offset " << data_off << " len "
                     << p->second.first.length() << dendl;
      data_buf = p->second.first;
      // make sure it's big enough
      if (data_buf.length() < data_len)
        data_buf.push_back(buffer::create(data_len - data_buf.length()));
      data_blp = data_buf.begin();
    } else {
      ldout(cct, 20) << __func__ << " allocating new rx buffer at offset "
                     << data_off << dendl;
      alloc_aligned_buffer(data_buf, data_len, data_off);
      data_blp = data_buf.begin();
    }
  }

  msg_left = data_len;

  return CONTINUE(read_message_data);
}

CtPtr ProtocolV2::read_message_data() {
  ldout(cct, 20) << __func__ << " msg_left=" << msg_left << dendl;

  if (msg_left > 0) {
    bufferptr bp = data_blp.get_current_ptr();
    unsigned read_len = std::min(bp.length(), msg_left);

    return READB(read_len, bp.c_str(), handle_message_data);
  }

  state = READ_MESSAGE_COMPLETE;
  return handle_message_complete();
}

CtPtr ProtocolV2::handle_message_data(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read data error " << dendl;
    return _fault();
  }

  bufferptr bp = data_blp.get_current_ptr();
  unsigned read_len = std::min(bp.length(), msg_left);
  ceph_assert(read_len < std::numeric_limits<int>::max());
  data_blp.advance(read_len);
  data.append(bp, 0, read_len);
  msg_left -= read_len;

  return CONTINUE(read_message_data);
}

CtPtr ProtocolV2::handle_message_complete() {
  ldout(cct, 20) << __func__ << dendl;

  ldout(cct, 20) << __func__ << " got " << front.length() << " + "
                 << middle.length() << " + " << data.length() << " byte message"
                 << dendl;

  ceph_msg_header header{
      current_header.seq,
      current_header.tid,
      current_header.type,
      current_header.priority,
      current_header.version,
      current_header.front_len,
      current_header.middle_len,
      current_header.data_len,
      current_header.data_off,
      entity_name_t(connection->peer_type, connection->peer_global_id),
      current_header.compat_version,
      current_header.reserved,
      0};
  ceph_msg_footer footer{current_header.front_crc, current_header.middle_crc,
                         current_header.data_crc, 0, current_header.flags};

  Message *message = decode_message(cct, messenger->crcflags, header, footer,
                                    front, middle, data, connection);
  if (!message) {
    ldout(cct, 1) << __func__ << " decode message failed " << dendl;
    return _fault();
  }

  //
  //  Check the signature if one should be present.  A zero return indicates
  //  success. PLR
  //

  // if (session_security.get() == NULL) {
  //   ldout(cct, 10) << __func__ << " no session security set" << dendl;
  // } else {
  //   if (session_security->check_message_signature(message)) {
  //     ldout(cct, 0) << __func__ << " Signature check failed" << dendl;
  //     message->put();
  //     return _fault();
  //   }
  // }
  message->set_byte_throttler(connection->policy.throttler_bytes);
  message->set_message_throttler(connection->policy.throttler_messages);

  uint32_t cur_msg_size = current_header.front_len + current_header.middle_len +
                          current_header.data_len;

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
  ldout(cct, 5) << " rx " << message->get_source() << " seq "
                << message->get_seq() << " " << message << " " << *message
                << dendl;

  bool need_dispatch_writer = true;
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
  } else {
    connection->dispatch_queue->enqueue(message, message->get_priority(),
                                        connection->conn_id);
  }

  handle_message_ack(current_header.ack_seq);

  // clean up local buffer references
  data_buf.clear();
  front.clear();
  middle.clear();
  data.clear();

  if (need_dispatch_writer && connection->is_connected()) {
    connection->center->dispatch_event_external(connection->write_handler);
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_keepalive2(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  KeepAliveFrame keepalive_frame(payload, length);

  ldout(cct, 30) << __func__ << " got KEEPALIVE2 tag ..." << dendl;

  utime_t kp_t = utime_t(keepalive_frame.timestamp);
  connection->write_lock.lock();
  append_keepalive_ack(kp_t);
  connection->write_lock.unlock();

  ldout(cct, 20) << __func__ << " got KEEPALIVE2 " << kp_t << dendl;
  connection->set_last_keepalive(ceph_clock_now());

  if (is_connected()) {
    connection->center->dispatch_event_external(connection->write_handler);
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_keepalive2_ack(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  KeepAliveFrame keepalive_ack_frame(payload, length);
  connection->set_last_keepalive_ack(utime_t(keepalive_ack_frame.timestamp));
  ldout(cct, 20) << __func__ << " got KEEPALIVE_ACK" << dendl;

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_message_ack(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  AckFrame ack(payload, length);
  handle_message_ack(ack.seq);
  return CONTINUE(read_frame);
}

/* Client Protocol Methods */

CtPtr ProtocolV2::start_client_banner_exchange() {
  ldout(cct, 20) << __func__ << dendl;
  state = CONNECTING;

  return _banner_exchange(CONTINUATION(post_client_banner_exchange));
}

CtPtr ProtocolV2::post_client_banner_exchange() {
  ldout(cct, 20) << __func__ << dendl;

  // at this point we can change how the client protocol behaves based on
  // this->peer_required_features

  return send_auth_request();
}

CtPtr ProtocolV2::send_auth_request(std::vector<__u32> allowed_methods) {
  ldout(cct, 20) << __func__ << dendl;

  // We need to get an authorizer at this point.
  // this->messenger->get_authorizer(...)

  bufferlist auth_bl;
  /* BEGIN TO REMOVE */
  encode((int32_t)35, auth_bl, 0);
  std::string hello("hello");
  encode(hello, auth_bl, 0);
  /* END TO REMOVE */
  __le32 method;
  if (allowed_methods.empty()) {
    // choose client's preferred method
    method = 23;  // 23 is just for testing purposes (REMOVE THIS)
  } else {
    // choose one of the allowed methods
    method = allowed_methods[0];
  }
  AuthRequestFrame authFrame(method, auth_bl);

  bufferlist &bl = authFrame.get_buffer();
  return WRITE(bl, handle_auth_request_write);
}

CtPtr ProtocolV2::handle_auth_request_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " auth request write failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_auth_bad_method(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  AuthBadMethodFrame bad_method(payload, length);
  ldout(cct, 1) << __func__ << " auth method=" << bad_method.method
                << " rejected, allowed methods=" << bad_method.allowed_methods
                << dendl;

  return send_auth_request(bad_method.allowed_methods);
}

CtPtr ProtocolV2::handle_auth_bad_auth(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  AuthBadAuthFrame bad_auth(payload, length);
  ldout(cct, 1) << __func__ << " authentication failed"
                << " error code=" << bad_auth.error_code
                << " error message=" << bad_auth.error_msg << dendl;

  return _fault();
}

CtPtr ProtocolV2::handle_auth_done(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  AuthDoneFrame auth_done(payload, length);
  ldout(cct, 1) << __func__ << " authentication done,"
                << " flags=" << auth_done.flags << dendl;

  return send_client_ident();
}

CtPtr ProtocolV2::send_client_ident() {
  ldout(cct, 20) << __func__ << dendl;

  uint64_t flags = 0;
  if (connection->policy.lossy) {
    flags |= CEPH_MSG_CONNECT_LOSSY;
  }

  cookie = ceph::util::generate_random_number<uint64_t>(0, -1ll);

  IdentFrame ident(messenger->get_myaddrs(), messenger->get_myname().num(),
                   connection->policy.features_supported,
                   connection->policy.features_required, flags, cookie);

  ldout(cct, 5) << __func__ << " sending identification: "
                << "addrs: " << ident.addrs << " gid: " << ident.gid
                << " features_supported: " << std::hex
                << ident.supported_features
                << " features_required: " << ident.required_features
                << " flags: " << ident.flags << " cookie: " << std::dec
                << ident.cookie << dendl;

  bufferlist &bl = ident.get_buffer();
  return WRITE(bl, handle_client_ident_write);
}

CtPtr ProtocolV2::handle_client_ident_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " client ident write failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_ident_missing_features(char *payload,
                                                uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  IdentMissingFeaturesFrame ident_missing(payload, length);
  lderr(cct) << __func__
             << " client does not support all server features: " << std::hex
             << ident_missing.features << std::dec << dendl;

  return _fault();
}

CtPtr ProtocolV2::handle_server_ident(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  IdentFrame server_ident(payload, length);
  ldout(cct, 5) << __func__ << " received server identification: "
                << "addrs: " << server_ident.addrs
                << " gid: " << server_ident.gid
                << " features_supported: " << std::hex
                << server_ident.supported_features
                << " features_required: " << server_ident.required_features
                << " flags: " << server_ident.flags << " cookie: " << std::dec
                << server_ident.cookie << dendl;

  connection->set_peer_addrs(server_ident.addrs);
  connection->peer_global_id = server_ident.gid;
  connection->set_features(server_ident.required_features &
                           connection->policy.features_supported);

  return ready();
}

/* Server Protocol Methods */

CtPtr ProtocolV2::start_server_banner_exchange() {
  ldout(cct, 20) << __func__ << dendl;
  state = ACCEPTING;

  return _banner_exchange(CONTINUATION(post_server_banner_exchange));
}

CtPtr ProtocolV2::post_server_banner_exchange() {
  ldout(cct, 20) << __func__ << dendl;

  // at this point we can change how the server protocol behaves based on
  // this->peer_required_features

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_auth_request(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  AuthRequestFrame auth_request(payload, length);

  ldout(cct, 10) << __func__ << " AuthRequest(method=" << auth_request.method
                 << ", auth_len=" << auth_request.len << ")" << dendl;

  /* BEGIN TO REMOVE */
  auto p = auth_request.auth_payload.cbegin();
  int32_t i;
  std::string s;
  try {
    decode(i, p);
    decode(s, p);
  } catch (const buffer::error &e) {
    lderr(cct) << __func__ << " decode auth_payload failed" << dendl;
    return _fault();
  }

  ldout(cct, 10) << __func__ << " (TO REMOVE) auth_payload (" << (int32_t)i
                 << ", " << s << ")" << dendl;

  /* END TO REMOVE */

  /*
   * Get the auth methods from somewhere.
   * In V1 the allowed auth methods depend on the peer_type.
   * In V2, at this stage, we still don't know the peer_type so either
   * we define the set of allowed auth methods for any entity type,
   * or we need to exchange the entity type before reaching this point.
   */

  std::vector<__u32> allowed_methods = {CEPH_AUTH_NONE, CEPH_AUTH_CEPHX};

  bool found = false;
  for (const auto &a_method : allowed_methods) {
    if (a_method == auth_request.method) {
      // auth method allowed by the server
      found = true;
      break;
    }
  }

  if (!found) {
    ldout(cct, 1) << __func__ << " auth method=" << auth_request.method
                  << " not allowed" << dendl;
    AuthBadMethodFrame bad_method(auth_request.method, allowed_methods);
    bufferlist &bl = bad_method.get_buffer();
    return WRITE(bl, handle_auth_bad_method_write);
  }

  ldout(cct, 10) << __func__ << " auth method=" << auth_request.method
                 << " accepted" << dendl;
  // verify authorization blob
  bool valid = i == 35;

  if (!valid) {
    AuthBadAuthFrame bad_auth(12, "Permission denied");
    bufferlist &bl = bad_auth.get_buffer();
    return WRITE(bl, handle_auth_bad_auth_write);
  }

  bufferlist auth_bl;
  /* BEGIN TO REMOVE */
  encode((int32_t)45, auth_bl, 0);
  std::string hello("hello server more");
  encode(hello, auth_bl, 0);
  /* END TO REMOVE */
  AuthMoreFrame more(auth_bl);
  bufferlist &bl = more.get_buffer();
  return WRITE(bl, handle_auth_more_write);
}

CtPtr ProtocolV2::handle_auth_bad_method_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " auth bad method write failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_auth_bad_auth_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " auth bad auth write failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_auth_done_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " auth done write failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_client_ident(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << std::dec << length << dendl;

  IdentFrame client_ident(payload, length);

  ldout(cct, 5) << __func__ << " received client identification: "
                << "addrs: " << client_ident.addrs
                << " gid: " << client_ident.gid
                << " features_supported: " << std::hex
                << client_ident.supported_features
                << " features_required: " << client_ident.required_features
                << " flags: " << client_ident.flags << " cookie: " << std::dec
                << client_ident.cookie << dendl;

  if (client_ident.addrs.empty()) {
    connection->set_peer_addr(connection->target_addr);
  } else {
    // Should we check if one of the ident.addrs match connection->target_addr
    // as we do in ProtocolV1?
    connection->set_peer_addrs(client_ident.addrs);
  }

  uint64_t feat_missing = connection->policy.features_required &
                          ~(uint64_t)client_ident.supported_features;
  if (feat_missing) {
    ldout(cct, 1) << __func__ << " peer missing required features " << std::hex
                  << feat_missing << std::dec << dendl;
    IdentMissingFeaturesFrame ident_missing_features(feat_missing);

    bufferlist &bl = ident_missing_features.get_buffer();
    return WRITE(bl, handle_ident_missing_features_write);
  }

  state = ACCEPTED_CLIENT_IDENT;

  // if everything is OK reply with server identification
  connection->peer_global_id = client_ident.gid;
  cookie = client_ident.cookie;

  uint64_t flags = 0;
  if (connection->policy.lossy) {
    flags = flags | CEPH_MSG_CONNECT_LOSSY;
  }
  IdentFrame ident(messenger->get_myaddrs(), messenger->get_myname().num(),
                   connection->policy.features_supported,
                   connection->policy.features_required, flags, cookie);

  ldout(cct, 5) << __func__ << " sending identification: "
                << "addrs: " << ident.addrs << " gid: " << ident.gid
                << " features_supported: " << std::hex
                << ident.supported_features
                << " features_required: " << ident.required_features
                << " flags: " << ident.flags << " cookie: " << std::dec
                << ident.cookie << dendl;

  connection->lock.unlock();
  // Because "replacing" will prevent other connections preempt this addr,
  // it's safe that here we don't acquire Connection's lock
  ssize_t r = messenger->accept_conn(connection);

  connection->inject_delay();

  connection->lock.lock();

  if (r < 0) {
    ldout(cct, 1) << __func__ << " existing race replacing process for addr = "
                  << connection->peer_addrs.msgr2_addr()
                  << " just fail later one(this)" << dendl;
    ldout(cct, 10) << "accept fault after register" << dendl;
    connection->inject_delay();
    return _fault();
  }
  if (state != ACCEPTED_CLIENT_IDENT) {
    ldout(cct, 1) << __func__
                  << " state changed while accept_conn, it must be mark_down"
                  << dendl;
    ceph_assert(state == CLOSED || state == NONE);
    ldout(cct, 10) << "accept fault after register" << dendl;
    connection->inject_delay();
    return _fault();
  }

  bufferlist &bl = ident.get_buffer();
  return WRITE(bl, handle_send_server_ident_write);
}

CtPtr ProtocolV2::handle_ident_missing_features_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " ident missing features write failed r=" << r
                  << " (" << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  // notify
  connection->dispatch_queue->queue_accept(connection);
  messenger->ms_deliver_handle_fast_accept(connection);

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_send_server_ident_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " server ident write failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  return ready();
}
