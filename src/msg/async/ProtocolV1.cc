// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ProtocolV1.h"

#include "common/errno.h"

#include "AsyncConnection.h"
#include "AsyncMessenger.h"
#include "common/EventTrace.h"
#include "include/random.h"
#include "auth/AuthClient.h"
#include "auth/AuthServer.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _conn_prefix(_dout)
std::ostream &ProtocolV1::_conn_prefix(std::ostream *_dout) {
  return *_dout << "--1- " << messenger->get_myaddrs() << " >> "
                << *connection->peer_addrs
		<< " conn("
                << connection << " " << this
                << " :" << connection->port << " s=" << get_state_name(state)
                << " pgs=" << peer_global_seq << " cs=" << connect_seq
                << " l=" << connection->policy.lossy << ").";
}

#define WRITE(B, C) write(CONTINUATION(C), B)

#define READ(L, C) read(CONTINUATION(C), L)

#define READB(L, B, C) read(CONTINUATION(C), L, B)

// Constant to limit starting sequence number to 2^31.  Nothing special about
// it, just a big number.  PLR
#define SEQ_MASK 0x7fffffff

const int ASYNC_COALESCE_THRESHOLD = 256;

using namespace std;

static void alloc_aligned_buffer(ceph::buffer::list &data, unsigned len, unsigned off) {
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
  ceph::bufferptr ptr(ceph::buffer::create_small_page_aligned(alloc_len));
  if (head) ptr.set_offset(CEPH_PAGE_SIZE - head);
  data.push_back(std::move(ptr));
}

/**
 * Protocol V1
 **/

ProtocolV1::ProtocolV1(AsyncConnection *connection)
    : Protocol(1, connection),
      temp_buffer(nullptr),
      can_write(WriteStatus::NOWRITE),
      keepalive(false),
      connect_seq(0),
      peer_global_seq(0),
      msg_left(0),
      cur_msg_size(0),
      replacing(false),
      is_reset_from_peer(false),
      once_ready(false),
      state(NONE),
      global_seq(0),
      wait_for_seq(false) {
  temp_buffer = new char[4096];
}

ProtocolV1::~ProtocolV1() {
  ceph_assert(out_q.empty());
  ceph_assert(sent.empty());

  delete[] temp_buffer;
}

void ProtocolV1::connect() {
  this->state = START_CONNECT;

  // reset connect state variables
  authorizer_buf.clear();
  // FIPS zeroization audit 20191115: these memsets are not security related.
  memset(&connect_msg, 0, sizeof(connect_msg));
  memset(&connect_reply, 0, sizeof(connect_reply));

  global_seq = messenger->get_global_seq();
}

void ProtocolV1::accept() { this->state = START_ACCEPT; }

bool ProtocolV1::is_connected() {
  return can_write.load() == WriteStatus::CANWRITE;
}

void ProtocolV1::stop() {
  ldout(cct, 20) << __func__ << dendl;
  if (state == CLOSED) {
    return;
  }

  if (connection->delay_state) connection->delay_state->flush();

  ldout(cct, 2) << __func__ << dendl;
  std::lock_guard<std::mutex> l(connection->write_lock);

  reset_recv_state();
  discard_out_queue();

  connection->_stop();

  can_write = WriteStatus::CLOSED;
  state = CLOSED;
}

void ProtocolV1::fault() {
  ldout(cct, 20) << __func__ << dendl;

  if (state == CLOSED || state == NONE) {
    ldout(cct, 10) << __func__ << " connection is already closed" << dendl;
    return;
  }

  if (connection->policy.lossy && state != START_CONNECT &&
      state != CONNECTING) {
    ldout(cct, 1) << __func__ << " on lossy channel, failing" << dendl;
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return;
  }

  connection->write_lock.lock();
  can_write = WriteStatus::NOWRITE;
  is_reset_from_peer = false;

  // requeue sent items
  requeue_sent();

  if (!once_ready && out_q.empty() && state >= START_ACCEPT &&
      state <= ACCEPTING_WAIT_CONNECT_MSG_AUTH && !replacing) {
    ldout(cct, 10) << __func__ << " with nothing to send and in the half "
                   << " accept state just closed" << dendl;
    connection->write_lock.unlock();
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return;
  }
  replacing = false;

  connection->fault();

  reset_recv_state();

  if (connection->policy.standby && out_q.empty() && !keepalive &&
      state != WAIT) {
    ldout(cct, 10) << __func__ << " with nothing to send, going to standby"
                   << dendl;
    state = STANDBY;
    connection->write_lock.unlock();
    return;
  }

  connection->write_lock.unlock();

  if ((state >= START_CONNECT && state <= CONNECTING_SEND_CONNECT_MSG) ||
      state == WAIT) {
    // backoff!
    if (state == WAIT) {
      backoff.set_from_double(cct->_conf->ms_max_backoff);
    } else if (backoff == utime_t()) {
      backoff.set_from_double(cct->_conf->ms_initial_backoff);
    } else {
      backoff += backoff;
      if (backoff > cct->_conf->ms_max_backoff)
        backoff.set_from_double(cct->_conf->ms_max_backoff);
    }

    global_seq = messenger->get_global_seq();
    state = START_CONNECT;
    connection->state = AsyncConnection::STATE_CONNECTING;
    ldout(cct, 10) << __func__ << " waiting " << backoff << dendl;
    // woke up again;
    connection->register_time_events.insert(
        connection->center->create_time_event(backoff.to_nsec() / 1000,
                                              connection->wakeup_handler));
  } else {
    // policy maybe empty when state is in accept
    if (connection->policy.server) {
      ldout(cct, 0) << __func__ << " server, going to standby" << dendl;
      state = STANDBY;
    } else {
      ldout(cct, 0) << __func__ << " initiating reconnect" << dendl;
      connect_seq++;
      global_seq = messenger->get_global_seq();
      state = START_CONNECT;
      connection->state = AsyncConnection::STATE_CONNECTING;
    }
    backoff = utime_t();
    connection->center->dispatch_event_external(connection->read_handler);
  }
}

void ProtocolV1::send_message(Message *m) {
  ceph::buffer::list bl;
  uint64_t f = connection->get_features();

  // TODO: Currently not all messages supports reencode like MOSDMap, so here
  // only let fast dispatch support messages prepare message
  bool can_fast_prepare = messenger->ms_can_fast_dispatch(m);
  bool is_prepared = false;
  if (can_fast_prepare && f) {
    prepare_send_message(f, m, bl);
    is_prepared = true;
  }

  std::lock_guard<std::mutex> l(connection->write_lock);
  // "features" changes will change the payload encoding
  if (can_fast_prepare &&
      (can_write == WriteStatus::NOWRITE || connection->get_features() != f)) {
    // ensure the correctness of message encoding
    bl.clear();
    m->clear_payload();
    ldout(cct, 5) << __func__ << " clear encoded buffer previous " << f
                  << " != " << connection->get_features() << dendl;
  }
  if (can_write == WriteStatus::CLOSED) {
    ldout(cct, 10) << __func__ << " connection closed."
                   << " Drop message " << m << dendl;
    m->put();
  } else {
    m->queue_start = ceph::mono_clock::now();
    m->trace.event("async enqueueing message");
    out_q[m->get_priority()].emplace_back(out_q_entry_t{
      std::move(bl), m, is_prepared});
    ldout(cct, 15) << __func__ << " inline write is denied, reschedule m=" << m
                   << dendl;
    if (can_write != WriteStatus::REPLACING && !write_in_progress) {
      write_in_progress = true;
      connection->center->dispatch_event_external(connection->write_handler);
    }
  }
}

void ProtocolV1::prepare_send_message(uint64_t features, Message *m,
                                      ceph::buffer::list &bl) {
  ldout(cct, 20) << __func__ << " m " << *m << dendl;

  // associate message with Connection (for benefit of encode_payload)
  ldout(cct, 20) << __func__ << (m->empty_payload() ? " encoding features " : " half-reencoding features ")
		 << features << " " << m  << " " << *m << dendl;

  // encode and copy out of *m
  // in write_message we update header.seq and need recalc crc
  // so skip calc header in encode function.
  m->encode(features, messenger->crcflags, true);

  bl.append(m->get_payload());
  bl.append(m->get_middle());
  bl.append(m->get_data());
}

void ProtocolV1::send_keepalive() {
  ldout(cct, 10) << __func__ << dendl;
  std::lock_guard<std::mutex> l(connection->write_lock);
  if (can_write != WriteStatus::CLOSED) {
    keepalive = true;
    connection->center->dispatch_event_external(connection->write_handler);
  }
}

void ProtocolV1::read_event() {
  ldout(cct, 20) << __func__ << dendl;
  switch (state) {
    case START_CONNECT:
      CONTINUATION_RUN(CONTINUATION(send_client_banner));
      break;
    case START_ACCEPT:
      CONTINUATION_RUN(CONTINUATION(send_server_banner));
      break;
    case OPENED:
      CONTINUATION_RUN(CONTINUATION(wait_message));
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

void ProtocolV1::write_event() {
  ldout(cct, 10) << __func__ << dendl;
  ssize_t r = 0;

  connection->write_lock.lock();
  if (can_write == WriteStatus::CANWRITE) {
    if (keepalive) {
      append_keepalive_or_ack();
      keepalive = false;
    }

    auto start = ceph::mono_clock::now();
    bool more;
    do {
      if (connection->is_queued()) {
	if (r = connection->_try_send(); r!= 0) {
	  // either fails to send or not all queued buffer is sent
	  break;
	}
      }

      const out_q_entry_t out_entry = _get_next_outgoing();
      Message *m = out_entry.m;
      ceph::buffer::list data = out_entry.bl;

      if (!m) {
        break;
      }

      if (!connection->policy.lossy) {
        // put on sent list
        sent.push_back(m);
        m->get();
      }
      more = !out_q.empty();
      connection->write_lock.unlock();

      // send_message or requeue messages may not encode message
      if (!data.length() || !out_entry.is_prepared) {
        prepare_send_message(connection->get_features(), m, data);
      }

      if (m->queue_start != ceph::mono_time()) {
        connection->logger->tinc(l_msgr_send_messages_queue_lat,
				 ceph::mono_clock::now() - m->queue_start);
      }

      r = write_message(m, data, more);

      connection->write_lock.lock();
      if (r == 0) {
        ;
      } else if (r < 0) {
        ldout(cct, 1) << __func__ << " send msg failed" << dendl;
        break;
      } else if (r > 0) {
	// Outbound message in-progress, thread will be re-awoken
	// when the outbound socket is writeable again
	break;
      }
    } while (can_write == WriteStatus::CANWRITE);
    write_in_progress = false;
    connection->write_lock.unlock();

    // if r > 0 mean data still lefted, so no need _try_send.
    if (r == 0) {
      uint64_t left = ack_left;
      if (left) {
        ceph_le64 s;
        s = in_seq;
        connection->outgoing_bl.append(CEPH_MSGR_TAG_ACK);
        connection->outgoing_bl.append((char *)&s, sizeof(s));
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
    write_in_progress = false;
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

bool ProtocolV1::is_queued() {
  return !out_q.empty() || connection->is_queued();
}

void ProtocolV1::run_continuation(CtPtr pcontinuation) {
  if (pcontinuation) {
    CONTINUATION_RUN(*pcontinuation);
  }
}

CtPtr ProtocolV1::read(CONTINUATION_RX_TYPE<ProtocolV1> &next,
                       int len, char *buffer) {
  if (!buffer) {
    buffer = temp_buffer;
  }
  ssize_t r = connection->read(len, buffer,
                               [&next, this](char *buffer, int r) {
                                 next.setParams(buffer, r);
                                 CONTINUATION_RUN(next);
                               });
  if (r <= 0) {
    next.setParams(buffer, r);
    return &next;
  }

  return nullptr;
}

CtPtr ProtocolV1::write(CONTINUATION_TX_TYPE<ProtocolV1> &next,
                        ceph::buffer::list &buffer) {
  ssize_t r = connection->write(buffer, [&next, this](int r) {
    next.setParams(r);
    CONTINUATION_RUN(next);
  });
  if (r <= 0) {
    next.setParams(r);
    return &next;
  }

  return nullptr;
}

CtPtr ProtocolV1::ready() {
  ldout(cct, 25) << __func__ << dendl;

  // make sure no pending tick timer
  if (connection->last_tick_id) {
    connection->center->delete_time_event(connection->last_tick_id);
  }
  connection->last_tick_id = connection->center->create_time_event(
      connection->inactive_timeout_us, connection->tick_handler);

  connection->write_lock.lock();
  can_write = WriteStatus::CANWRITE;
  if (is_queued()) {
    connection->center->dispatch_event_external(connection->write_handler);
  }
  connection->write_lock.unlock();
  connection->maybe_start_delay_thread();

  state = OPENED;
  return wait_message();
}

CtPtr ProtocolV1::wait_message() {
  if (state != OPENED) {  // must have changed due to a replace
    return nullptr;
  }

  ldout(cct, 20) << __func__ << dendl;

  return READ(sizeof(char), handle_message);
}

CtPtr ProtocolV1::handle_message(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read tag failed" << dendl;
    return _fault();
  }

  char tag = buffer[0];
  ldout(cct, 20) << __func__ << " process tag " << (int)tag << dendl;

  if (tag == CEPH_MSGR_TAG_KEEPALIVE) {
    ldout(cct, 20) << __func__ << " got KEEPALIVE" << dendl;
    connection->set_last_keepalive(ceph_clock_now());
  } else if (tag == CEPH_MSGR_TAG_KEEPALIVE2) {
    return READ(sizeof(ceph_timespec), handle_keepalive2);
  } else if (tag == CEPH_MSGR_TAG_KEEPALIVE2_ACK) {
    return READ(sizeof(ceph_timespec), handle_keepalive2_ack);
  } else if (tag == CEPH_MSGR_TAG_ACK) {
    return READ(sizeof(ceph_le64), handle_tag_ack);
  } else if (tag == CEPH_MSGR_TAG_MSG) {
    recv_stamp = ceph_clock_now();
    ldout(cct, 20) << __func__ << " begin MSG" << dendl;
    return READ(sizeof(ceph_msg_header), handle_message_header);
  } else if (tag == CEPH_MSGR_TAG_CLOSE) {
    ldout(cct, 20) << __func__ << " got CLOSE" << dendl;
    stop();
  } else {
    ldout(cct, 0) << __func__ << " bad tag " << (int)tag << dendl;
    return _fault();
  }
  return nullptr;
}

CtPtr ProtocolV1::handle_keepalive2(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read keeplive timespec failed" << dendl;
    return _fault();
  }

  ldout(cct, 30) << __func__ << " got KEEPALIVE2 tag ..." << dendl;

  ceph_timespec *t;
  t = (ceph_timespec *)buffer;
  utime_t kp_t = utime_t(*t);
  connection->write_lock.lock();
  append_keepalive_or_ack(true, &kp_t);
  connection->write_lock.unlock();

  ldout(cct, 20) << __func__ << " got KEEPALIVE2 " << kp_t << dendl;
  connection->set_last_keepalive(ceph_clock_now());

  if (is_connected()) {
    connection->center->dispatch_event_external(connection->write_handler);
  }

  return CONTINUE(wait_message);
}

void ProtocolV1::append_keepalive_or_ack(bool ack, utime_t *tp) {
  ldout(cct, 10) << __func__ << dendl;
  if (ack) {
    ceph_assert(tp);
    struct ceph_timespec ts;
    tp->encode_timeval(&ts);
    connection->outgoing_bl.append(CEPH_MSGR_TAG_KEEPALIVE2_ACK);
    connection->outgoing_bl.append((char *)&ts, sizeof(ts));
  } else if (connection->has_feature(CEPH_FEATURE_MSGR_KEEPALIVE2)) {
    struct ceph_timespec ts;
    utime_t t = ceph_clock_now();
    t.encode_timeval(&ts);
    connection->outgoing_bl.append(CEPH_MSGR_TAG_KEEPALIVE2);
    connection->outgoing_bl.append((char *)&ts, sizeof(ts));
  } else {
    connection->outgoing_bl.append(CEPH_MSGR_TAG_KEEPALIVE);
  }
}

CtPtr ProtocolV1::handle_keepalive2_ack(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read keeplive timespec failed" << dendl;
    return _fault();
  }

  ceph_timespec *t;
  t = (ceph_timespec *)buffer;
  connection->set_last_keepalive_ack(utime_t(*t));
  ldout(cct, 20) << __func__ << " got KEEPALIVE_ACK" << dendl;

  return CONTINUE(wait_message);
}

CtPtr ProtocolV1::handle_tag_ack(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read ack seq failed" << dendl;
    return _fault();
  }

  ceph_le64 seq;
  seq = *(ceph_le64 *)buffer;
  ldout(cct, 20) << __func__ << " got ACK" << dendl;

  ldout(cct, 15) << __func__ << " got ack seq " << seq << dendl;
  // trim sent list
  static const int max_pending = 128;
  int i = 0;
  auto now = ceph::mono_clock::now();
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
  connection->logger->tinc(l_msgr_handle_ack_lat, ceph::mono_clock::now() - now);
  for (int k = 0; k < i; k++) {
    pending[k]->put();
  }

  return CONTINUE(wait_message);
}

CtPtr ProtocolV1::handle_message_header(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read message header failed" << dendl;
    return _fault();
  }

  ldout(cct, 20) << __func__ << " got MSG header" << dendl;

  current_header = *((ceph_msg_header *)buffer);

  ldout(cct, 20) << __func__ << " got envelope type=" << current_header.type << " src "
                 << entity_name_t(current_header.src) << " front=" << current_header.front_len
                 << " data=" << current_header.data_len << " off " << current_header.data_off
                 << dendl;

  if (messenger->crcflags & MSG_CRC_HEADER) {
    __u32 header_crc = 0;
    header_crc = ceph_crc32c(0, (unsigned char *)&current_header,
                             sizeof(current_header) - sizeof(current_header.crc));
    // verify header crc
    if (header_crc != current_header.crc) {
      ldout(cct, 0) << __func__ << " got bad header crc " << header_crc
                    << " != " << current_header.crc << dendl;
      return _fault();
    }
  }

  // Reset state
  data_buf.clear();
  front.clear();
  middle.clear();
  data.clear();

  state = THROTTLE_MESSAGE;
  return CONTINUE(throttle_message);
}

CtPtr ProtocolV1::throttle_message() {
  ldout(cct, 20) << __func__ << dendl;

  if (connection->policy.throttler_messages) {
    ldout(cct, 10) << __func__ << " wants " << 1
                   << " message from policy throttler "
                   << connection->policy.throttler_messages->get_current()
                   << "/" << connection->policy.throttler_messages->get_max()
                   << dendl;
    if (!connection->policy.throttler_messages->get_or_fail()) {
      ldout(cct, 1) << __func__ << " wants 1 message from policy throttle "
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

CtPtr ProtocolV1::throttle_bytes() {
  ldout(cct, 20) << __func__ << dendl;

  cur_msg_size = current_header.front_len + current_header.middle_len +
                 current_header.data_len;
  if (cur_msg_size) {
    if (connection->policy.throttler_bytes) {
      ldout(cct, 10) << __func__ << " wants " << cur_msg_size
                     << " bytes from policy throttler "
                     << connection->policy.throttler_bytes->get_current() << "/"
                     << connection->policy.throttler_bytes->get_max() << dendl;
      if (!connection->policy.throttler_bytes->get_or_fail(cur_msg_size)) {
        ldout(cct, 1) << __func__ << " wants " << cur_msg_size
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

CtPtr ProtocolV1::throttle_dispatch_queue() {
  ldout(cct, 20) << __func__ << dendl;

  if (cur_msg_size) {
    if (!connection->dispatch_queue->dispatch_throttler.get_or_fail(
            cur_msg_size)) {
      ldout(cct, 1)
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

CtPtr ProtocolV1::read_message_front() {
  ldout(cct, 20) << __func__ << dendl;

  unsigned front_len = current_header.front_len;
  if (front_len) {
    if (!front.length()) {
      front.push_back(ceph::buffer::create(front_len));
    }
    return READB(front_len, front.c_str(), handle_message_front);
  }
  return read_message_middle();
}

CtPtr ProtocolV1::handle_message_front(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read message front failed" << dendl;
    return _fault();
  }

  ldout(cct, 20) << __func__ << " got front " << front.length() << dendl;

  return read_message_middle();
}

CtPtr ProtocolV1::read_message_middle() {
  ldout(cct, 20) << __func__ << dendl;

  if (current_header.middle_len) {
    if (!middle.length()) {
      middle.push_back(ceph::buffer::create(current_header.middle_len));
    }
    return READB(current_header.middle_len, middle.c_str(),
                 handle_message_middle);
  }

  return read_message_data_prepare();
}

CtPtr ProtocolV1::handle_message_middle(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read message middle failed" << dendl;
    return _fault();
  }

  ldout(cct, 20) << __func__ << " got middle " << middle.length() << dendl;

  return read_message_data_prepare();
}

CtPtr ProtocolV1::read_message_data_prepare() {
  ldout(cct, 20) << __func__ << dendl;

  unsigned data_len = current_header.data_len;
  unsigned data_off = current_header.data_off;

  if (data_len) {
    // get a buffer
#if 0
    // rx_buffers is broken by design... see
    //  http://tracker.ceph.com/issues/22480
    map<ceph_tid_t, pair<ceph::buffer::list, int> >::iterator p =
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
#else
    ldout(cct, 20) << __func__ << " allocating new rx buffer at offset "
		   << data_off << dendl;
    alloc_aligned_buffer(data_buf, data_len, data_off);
    data_blp = data_buf.begin();
#endif
  }

  msg_left = data_len;

  return CONTINUE(read_message_data);
}

CtPtr ProtocolV1::read_message_data() {
  ldout(cct, 20) << __func__ << " msg_left=" << msg_left << dendl;

  if (msg_left > 0) {
    auto bp = data_blp.get_current_ptr();
    unsigned read_len = std::min(bp.length(), msg_left);

    return READB(read_len, bp.c_str(), handle_message_data);
  }

  return read_message_footer();
}

CtPtr ProtocolV1::handle_message_data(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read data error " << dendl;
    return _fault();
  }

  auto bp = data_blp.get_current_ptr();
  unsigned read_len = std::min(bp.length(), msg_left);
  ceph_assert(read_len <
	      static_cast<unsigned>(std::numeric_limits<int>::max()));
  data_blp += read_len;
  data.append(bp, 0, read_len);
  msg_left -= read_len;

  return CONTINUE(read_message_data);
}

CtPtr ProtocolV1::read_message_footer() {
  ldout(cct, 20) << __func__ << dendl;

  state = READ_FOOTER_AND_DISPATCH;

  unsigned len;
  if (connection->has_feature(CEPH_FEATURE_MSG_AUTH)) {
    len = sizeof(ceph_msg_footer);
  } else {
    len = sizeof(ceph_msg_footer_old);
  }

  return READ(len, handle_message_footer);
}

CtPtr ProtocolV1::handle_message_footer(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read footer data error " << dendl;
    return _fault();
  }

  ceph_msg_footer footer;
  ceph_msg_footer_old old_footer;

  if (connection->has_feature(CEPH_FEATURE_MSG_AUTH)) {
    footer = *((ceph_msg_footer *)buffer);
  } else {
    old_footer = *((ceph_msg_footer_old *)buffer);
    footer.front_crc = old_footer.front_crc;
    footer.middle_crc = old_footer.middle_crc;
    footer.data_crc = old_footer.data_crc;
    footer.sig = 0;
    footer.flags = old_footer.flags;
  }

  int aborted = (footer.flags & CEPH_MSG_FOOTER_COMPLETE) == 0;
  ldout(cct, 10) << __func__ << " aborted = " << aborted << dendl;
  if (aborted) {
    ldout(cct, 0) << __func__ << " got " << front.length() << " + "
                  << middle.length() << " + " << data.length()
                  << " byte message.. ABORTED" << dendl;
    return _fault();
  }

  ldout(cct, 20) << __func__ << " got " << front.length() << " + "
                 << middle.length() << " + " << data.length() << " byte message"
                 << dendl;
  Message *message = decode_message(cct, messenger->crcflags, current_header,
                                    footer, front, middle, data, connection);
  if (!message) {
    ldout(cct, 1) << __func__ << " decode message failed " << dendl;
    return _fault();
  }

  //
  //  Check the signature if one should be present.  A zero return indicates
  //  success. PLR
  //

  if (session_security.get() == NULL) {
    ldout(cct, 10) << __func__ << " no session security set" << dendl;
  } else {
    if (session_security->check_message_signature(message)) {
      ldout(cct, 0) << __func__ << " Signature check failed" << dendl;
      message->put();
      return _fault();
    }
  }
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

#if defined(WITH_EVENTTRACE)
  if (message->get_type() == CEPH_MSG_OSD_OP ||
      message->get_type() == CEPH_MSG_OSD_OPREPLY) {
    utime_t ltt_processed_stamp = ceph_clock_now();
    double usecs_elapsed =
      ((double)(ltt_processed_stamp.to_nsec() - recv_stamp.to_nsec())) / 1000;
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

  bool need_dispatch_writer = false;
  if (!connection->policy.lossy) {
    ack_left++;
    need_dispatch_writer = true;
  }

  state = OPENED;

  ceph::mono_time fast_dispatch_time;

  if (connection->is_blackhole()) {
    ldout(cct, 10) << __func__ << " blackhole " << *message << dendl;
    message->put();
    goto out;
  }

  connection->logger->inc(l_msgr_recv_messages);
  connection->logger->inc(
      l_msgr_recv_bytes,
      cur_msg_size + sizeof(ceph_msg_header) + sizeof(ceph_msg_footer));

  messenger->ms_fast_preprocess(message);
  fast_dispatch_time = ceph::mono_clock::now();
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

 out:
  // clean up local buffer references
  data_buf.clear();
  front.clear();
  middle.clear();
  data.clear();

  if (need_dispatch_writer && connection->is_connected()) {
    connection->center->dispatch_event_external(connection->write_handler);
  }

  return CONTINUE(wait_message);
}

void ProtocolV1::session_reset() {
  ldout(cct, 10) << __func__ << " started" << dendl;

  std::lock_guard<std::mutex> l(connection->write_lock);
  if (connection->delay_state) {
    connection->delay_state->discard();
  }

  connection->dispatch_queue->discard_queue(connection->conn_id);
  discard_out_queue();
  // note: we need to clear outgoing_bl here, but session_reset may be
  // called by other thread, so let caller clear this itself!
  // outgoing_bl.clear();

  connection->dispatch_queue->queue_remote_reset(connection);

  randomize_out_seq();

  in_seq = 0;
  connect_seq = 0;
  // it's safe to directly set 0, double locked
  ack_left = 0;
  once_ready = false;
  can_write = WriteStatus::NOWRITE;
}

void ProtocolV1::randomize_out_seq() {
  if (connection->get_features() & CEPH_FEATURE_MSG_AUTH) {
    // Set out_seq to a random value, so CRC won't be predictable.
    auto rand_seq = ceph::util::generate_random_number<uint64_t>(0, SEQ_MASK);
    ldout(cct, 10) << __func__ << " randomize_out_seq " << rand_seq << dendl;
    out_seq = rand_seq;
  } else {
    // previously, seq #'s always started at 0.
    out_seq = 0;
  }
}

ssize_t ProtocolV1::write_message(Message *m, ceph::buffer::list &bl, bool more) {
  FUNCTRACE(cct);
  ceph_assert(connection->center->in_thread());
  m->set_seq(++out_seq);

  if (messenger->crcflags & MSG_CRC_HEADER) {
    m->calc_header_crc();
  }

  ceph_msg_header &header = m->get_header();
  ceph_msg_footer &footer = m->get_footer();

  // TODO: let sign_message could be reentry?
  // Now that we have all the crcs calculated, handle the
  // digital signature for the message, if the AsyncConnection has session
  // security set up.  Some session security options do not
  // actually calculate and check the signature, but they should
  // handle the calls to sign_message and check_signature.  PLR
  if (session_security.get() == NULL) {
    ldout(cct, 20) << __func__ << " no session security" << dendl;
  } else {
    if (session_security->sign_message(m)) {
      ldout(cct, 20) << __func__ << " failed to sign m=" << m
                     << "): sig = " << footer.sig << dendl;
    } else {
      ldout(cct, 20) << __func__ << " signed m=" << m
                     << "): sig = " << footer.sig << dendl;
    }
  }

  connection->outgoing_bl.append(CEPH_MSGR_TAG_MSG);
  connection->outgoing_bl.append((char *)&header, sizeof(header));

  ldout(cct, 20) << __func__ << " sending message type=" << header.type
                 << " src " << entity_name_t(header.src)
                 << " front=" << header.front_len << " data=" << header.data_len
                 << " off " << header.data_off << dendl;

  if ((bl.length() <= ASYNC_COALESCE_THRESHOLD) && (bl.get_num_buffers() > 1)) {
    for (const auto &pb : bl.buffers()) {
      connection->outgoing_bl.append((char *)pb.c_str(), pb.length());
    }
  } else {
    connection->outgoing_bl.claim_append(bl);
  }

  // send footer; if receiver doesn't support signatures, use the old footer
  // format
  ceph_msg_footer_old old_footer;
  if (connection->has_feature(CEPH_FEATURE_MSG_AUTH)) {
    connection->outgoing_bl.append((char *)&footer, sizeof(footer));
  } else {
    if (messenger->crcflags & MSG_CRC_HEADER) {
      old_footer.front_crc = footer.front_crc;
      old_footer.middle_crc = footer.middle_crc;
    } else {
      old_footer.front_crc = old_footer.middle_crc = 0;
    }
    old_footer.data_crc =
        messenger->crcflags & MSG_CRC_DATA ? footer.data_crc : 0;
    old_footer.flags = footer.flags;
    connection->outgoing_bl.append((char *)&old_footer, sizeof(old_footer));
  }

  m->trace.event("async writing message");
  ldout(cct, 20) << __func__ << " sending " << m->get_seq() << " " << m
                 << dendl;
  ssize_t total_send_size = connection->outgoing_bl.length();
  ssize_t rc = connection->_try_send(more);
  if (rc < 0) {
    ldout(cct, 1) << __func__ << " error sending " << m << ", "
                  << cpp_strerror(rc) << dendl;
  } else {
    connection->logger->inc(
        l_msgr_send_bytes, total_send_size - connection->outgoing_bl.length());
    ldout(cct, 10) << __func__ << " sending " << m
                   << (rc ? " continuely." : " done.") << dendl;
  }

#if defined(WITH_EVENTTRACE)
  if (m->get_type() == CEPH_MSG_OSD_OP)
    OID_EVENT_TRACE_WITH_MSG(m, "SEND_MSG_OSD_OP_END", false);
  else if (m->get_type() == CEPH_MSG_OSD_OPREPLY)
    OID_EVENT_TRACE_WITH_MSG(m, "SEND_MSG_OSD_OPREPLY_END", false);
#endif
  m->put();

  return rc;
}

void ProtocolV1::requeue_sent() {
  write_in_progress = false;
  if (sent.empty()) {
    return;
  }

  list<out_q_entry_t> &rq = out_q[CEPH_MSG_PRIO_HIGHEST];
  out_seq -= sent.size();
  while (!sent.empty()) {
    Message *m = sent.back();
    sent.pop_back();
    ldout(cct, 10) << __func__ << " " << *m << " for resend "
                   << " (" << m->get_seq() << ")" << dendl;
    m->clear_payload();
    rq.push_front(out_q_entry_t{ceph::buffer::list(), m, false});
  }
}

uint64_t ProtocolV1::discard_requeued_up_to(uint64_t out_seq, uint64_t seq) {
  ldout(cct, 10) << __func__ << " " << seq << dendl;
  std::lock_guard<std::mutex> l(connection->write_lock);
  if (out_q.count(CEPH_MSG_PRIO_HIGHEST) == 0) {
    return seq;
  }
  list<out_q_entry_t> &rq = out_q[CEPH_MSG_PRIO_HIGHEST];
  uint64_t count = out_seq;
  while (!rq.empty()) {
    Message* const m = rq.front().m;
    if (m->get_seq() == 0 || m->get_seq() > seq) break;
    ldout(cct, 10) << __func__ << " " << *(m) << " for resend seq "
                   << m->get_seq() << " <= " << seq << ", discarding"
                   << dendl;
    m->put();
    rq.pop_front();
    count++;
  }
  if (rq.empty()) out_q.erase(CEPH_MSG_PRIO_HIGHEST);
  return count;
}

/*
 * Tears down the message queues, and removes them from the
 * DispatchQueue Must hold write_lock prior to calling.
 */
void ProtocolV1::discard_out_queue() {
  ldout(cct, 10) << __func__ << " started" << dendl;

  for (list<Message *>::iterator p = sent.begin(); p != sent.end(); ++p) {
    ldout(cct, 20) << __func__ << " discard " << *p << dendl;
    (*p)->put();
  }
  sent.clear();
  for (map<int, list<out_q_entry_t>>::iterator p =
           out_q.begin();
       p != out_q.end(); ++p) {
    for (list<out_q_entry_t>::iterator r = p->second.begin();
         r != p->second.end(); ++r) {
      ldout(cct, 20) << __func__ << " discard " << r->m << dendl;
      r->m->put();
    }
  }
  out_q.clear();
  write_in_progress = false;
}

void ProtocolV1::reset_security()
{
  ldout(cct, 5) << __func__ << dendl;

  auth_meta.reset(new AuthConnectionMeta);
  authorizer_more.clear();
  session_security.reset();
}

void ProtocolV1::reset_recv_state()
{
  ldout(cct, 5) << __func__ << dendl;

  // execute in the same thread that uses the `session_security`.
  // We need to do the warp because holding `write_lock` is not
  // enough as `write_event()` releases it just before calling
  // `write_message()`. `submit_to()` here is NOT blocking.
  if (!connection->center->in_thread()) {
    connection->center->submit_to(connection->center->get_id(), [this] {
      ldout(cct, 5) << "reset_recv_state (warped) reseting security handlers"
                    << dendl;
      // Possibly unnecessary. See the comment in `deactivate_existing`.
      std::lock_guard<std::mutex> l(connection->lock);
      std::lock_guard<std::mutex> wl(connection->write_lock);
      reset_security();
    }, /* always_async = */true);
  } else {
    reset_security();
  }

  // clean read and write callbacks
  connection->pendingReadLen.reset();
  connection->writeCallback.reset();

  if (state > THROTTLE_MESSAGE && state <= READ_FOOTER_AND_DISPATCH &&
      connection->policy.throttler_messages) {
    ldout(cct, 10) << __func__ << " releasing " << 1
                   << " message to policy throttler "
                   << connection->policy.throttler_messages->get_current()
                   << "/" << connection->policy.throttler_messages->get_max()
                   << dendl;
    connection->policy.throttler_messages->put();
  }
  if (state > THROTTLE_BYTES && state <= READ_FOOTER_AND_DISPATCH) {
    if (connection->policy.throttler_bytes) {
      ldout(cct, 10) << __func__ << " releasing " << cur_msg_size
                     << " bytes to policy throttler "
                     << connection->policy.throttler_bytes->get_current() << "/"
                     << connection->policy.throttler_bytes->get_max() << dendl;
      connection->policy.throttler_bytes->put(cur_msg_size);
    }
  }
  if (state > THROTTLE_DISPATCH_QUEUE && state <= READ_FOOTER_AND_DISPATCH) {
    ldout(cct, 10)
        << __func__ << " releasing " << cur_msg_size
        << " bytes to dispatch_queue throttler "
        << connection->dispatch_queue->dispatch_throttler.get_current() << "/"
        << connection->dispatch_queue->dispatch_throttler.get_max() << dendl;
    connection->dispatch_queue->dispatch_throttle_release(cur_msg_size);
  }
}

ProtocolV1::out_q_entry_t ProtocolV1::_get_next_outgoing() {
  out_q_entry_t out_entry;
  if (!out_q.empty()) {
    map<int, list<out_q_entry_t>>::reverse_iterator it =
        out_q.rbegin();
    ceph_assert(!it->second.empty());
    list<out_q_entry_t>::iterator p = it->second.begin();
    out_entry = *p;
    it->second.erase(p);
    if (it->second.empty()) out_q.erase(it->first);
  }
  return out_entry;
}

/**
 * Client Protocol V1
 **/

CtPtr ProtocolV1::send_client_banner() {
  ldout(cct, 20) << __func__ << dendl;
  state = CONNECTING;

  ceph::buffer::list bl;
  bl.append(CEPH_BANNER, strlen(CEPH_BANNER));
  return WRITE(bl, handle_client_banner_write);
}

CtPtr ProtocolV1::handle_client_banner_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " write client banner failed" << dendl;
    return _fault();
  }
  ldout(cct, 10) << __func__ << " connect write banner done: "
                 << connection->get_peer_addr() << dendl;

  return wait_server_banner();
}

CtPtr ProtocolV1::wait_server_banner() {
  state = CONNECTING_WAIT_BANNER_AND_IDENTIFY;

  ldout(cct, 20) << __func__ << dendl;

  ceph::buffer::list myaddrbl;
  unsigned banner_len = strlen(CEPH_BANNER);
  unsigned need_len = banner_len + sizeof(ceph_entity_addr) * 2;
  return READ(need_len, handle_server_banner_and_identify);
}

CtPtr ProtocolV1::handle_server_banner_and_identify(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read banner and identify addresses failed"
                  << dendl;
    return _fault();
  }

  unsigned banner_len = strlen(CEPH_BANNER);
  if (memcmp(buffer, CEPH_BANNER, banner_len)) {
    ldout(cct, 0) << __func__ << " connect protocol error (bad banner) on peer "
                  << connection->get_peer_addr() << dendl;
    return _fault();
  }

  ceph::buffer::list bl;
  entity_addr_t paddr, peer_addr_for_me;

  bl.append(buffer + banner_len, sizeof(ceph_entity_addr) * 2);
  auto p = bl.cbegin();
  try {
    decode(paddr, p);
    decode(peer_addr_for_me, p);
  } catch (const ceph::buffer::error &e) {
    lderr(cct) << __func__ << " decode peer addr failed " << dendl;
    return _fault();
  }
  ldout(cct, 20) << __func__ << " connect read peer addr " << paddr
                 << " on socket " << connection->cs.fd() << dendl;

  entity_addr_t peer_addr = connection->peer_addrs->legacy_addr();
  if (peer_addr != paddr) {
    if (paddr.is_blank_ip() && peer_addr.get_port() == paddr.get_port() &&
        peer_addr.get_nonce() == paddr.get_nonce()) {
      ldout(cct, 0) << __func__ << " connect claims to be " << paddr << " not "
                    << peer_addr << " - presumably this is the same node!"
                    << dendl;
    } else {
      ldout(cct, 10) << __func__ << " connect claims to be " << paddr << " not "
                     << peer_addr << dendl;
      return _fault();
    }
  }

  ldout(cct, 20) << __func__ << " connect peer addr for me is "
                 << peer_addr_for_me << dendl;
  if (messenger->get_myaddrs().empty() ||
      messenger->get_myaddrs().front().is_blank_ip()) {
    sockaddr_storage ss;
    socklen_t len = sizeof(ss);
    getsockname(connection->cs.fd(), (sockaddr *)&ss, &len);
    entity_addr_t a;
    if (cct->_conf->ms_learn_addr_from_peer) {
      ldout(cct, 1) << __func__ << " peer " << connection->target_addr
		    << " says I am " << peer_addr_for_me << " (socket says "
		    << (sockaddr*)&ss << ")" << dendl;
      a = peer_addr_for_me;
    } else {
      ldout(cct, 1) << __func__ << " socket to  " << connection->target_addr
		    << " says I am " << (sockaddr*)&ss
		    << " (peer says " << peer_addr_for_me << ")" << dendl;
      a.set_sockaddr((sockaddr *)&ss);
    }
    a.set_type(entity_addr_t::TYPE_LEGACY); // anything but NONE; learned_addr ignores this
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
    if (state != CONNECTING_WAIT_BANNER_AND_IDENTIFY) {
      ldout(cct, 1) << __func__
                  << " state changed while learned_addr, mark_down or "
		    << " replacing must be happened just now" << dendl;
      return nullptr;
    }
  }

  ceph::buffer::list myaddrbl;
  encode(messenger->get_myaddr_legacy(), myaddrbl, 0);  // legacy
  return WRITE(myaddrbl, handle_my_addr_write);
}

CtPtr ProtocolV1::handle_my_addr_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 2) << __func__ << " connect couldn't write my addr, "
                  << cpp_strerror(r) << dendl;
    return _fault();
  }
  ldout(cct, 10) << __func__ << " connect sent my addr "
                 << messenger->get_myaddr_legacy() << dendl;

  return CONTINUE(send_connect_message);
}

CtPtr ProtocolV1::send_connect_message()
{
  state = CONNECTING_SEND_CONNECT_MSG;

  ldout(cct, 20) << __func__ << dendl;
  ceph_assert(messenger->auth_client);

  ceph::buffer::list auth_bl;
  vector<uint32_t> preferred_modes;

  if (connection->peer_type != CEPH_ENTITY_TYPE_MON ||
      messenger->get_myname().type() == CEPH_ENTITY_TYPE_MON) {
    if (authorizer_more.length()) {
      ldout(cct,10) << __func__ << " using augmented (challenge) auth payload"
		    << dendl;
      auth_bl = authorizer_more;
    } else {
      auto am = auth_meta;
      authorizer_more.clear();
      connection->lock.unlock();
      int r = messenger->auth_client->get_auth_request(
	connection, am.get(),
	&am->auth_method, &preferred_modes, &auth_bl);
      connection->lock.lock();
      if (r < 0) {
	return _fault();
      }
      if (state != CONNECTING_SEND_CONNECT_MSG) {
	ldout(cct, 1) << __func__ << " state changed!" << dendl;
	return _fault();
      }
    }
  }

  ceph_msg_connect connect;
  connect.features = connection->policy.features_supported;
  connect.host_type = messenger->get_myname().type();
  connect.global_seq = global_seq;
  connect.connect_seq = connect_seq;
  connect.protocol_version =
      messenger->get_proto_version(connection->peer_type, true);
  if (auth_bl.length()) {
    ldout(cct, 10) << __func__
                   << " connect_msg.authorizer_len=" << auth_bl.length()
                   << " protocol=" << auth_meta->auth_method << dendl;
    connect.authorizer_protocol = auth_meta->auth_method;
    connect.authorizer_len = auth_bl.length();
  } else {
    connect.authorizer_protocol = 0;
    connect.authorizer_len = 0;
  }

  connect.flags = 0;
  if (connection->policy.lossy) {
    connect.flags |=
        CEPH_MSG_CONNECT_LOSSY;  // this is fyi, actually, server decides!
  }

  ceph::buffer::list bl;
  bl.append((char *)&connect, sizeof(connect));
  if (auth_bl.length()) {
    bl.append(auth_bl.c_str(), auth_bl.length());
  }

  ldout(cct, 10) << __func__ << " connect sending gseq=" << global_seq
                 << " cseq=" << connect_seq
                 << " proto=" << connect.protocol_version << dendl;

  return WRITE(bl, handle_connect_message_write);
}

CtPtr ProtocolV1::handle_connect_message_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 2) << __func__ << " connect couldn't send reply "
                  << cpp_strerror(r) << dendl;
    return _fault();
  }

  ldout(cct, 20) << __func__
                 << " connect wrote (self +) cseq, waiting for reply" << dendl;

  return wait_connect_reply();
}

CtPtr ProtocolV1::wait_connect_reply() {
  ldout(cct, 20) << __func__ << dendl;

  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(&connect_reply, 0, sizeof(connect_reply));
  return READ(sizeof(connect_reply), handle_connect_reply_1);
}

CtPtr ProtocolV1::handle_connect_reply_1(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read connect reply failed" << dendl;
    return _fault();
  }

  connect_reply = *((ceph_msg_connect_reply *)buffer);

  ldout(cct, 20) << __func__ << " connect got reply tag "
                 << (int)connect_reply.tag << " connect_seq "
                 << connect_reply.connect_seq << " global_seq "
                 << connect_reply.global_seq << " proto "
                 << connect_reply.protocol_version << " flags "
                 << (int)connect_reply.flags << " features "
                 << connect_reply.features << dendl;

  if (connect_reply.authorizer_len) {
    return wait_connect_reply_auth();
  }

  return handle_connect_reply_2();
}

CtPtr ProtocolV1::wait_connect_reply_auth() {
  ldout(cct, 20) << __func__ << dendl;

  ldout(cct, 10) << __func__
                 << " reply.authorizer_len=" << connect_reply.authorizer_len
                 << dendl;

  ceph_assert(connect_reply.authorizer_len < 4096);

  return READ(connect_reply.authorizer_len, handle_connect_reply_auth);
}

CtPtr ProtocolV1::handle_connect_reply_auth(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read connect reply authorizer failed"
                  << dendl;
    return _fault();
  }

  ceph::buffer::list authorizer_reply;
  authorizer_reply.append(buffer, connect_reply.authorizer_len);

  if (connection->peer_type != CEPH_ENTITY_TYPE_MON ||
      messenger->get_myname().type() == CEPH_ENTITY_TYPE_MON) {
    auto am = auth_meta;
    bool more = (connect_reply.tag == CEPH_MSGR_TAG_CHALLENGE_AUTHORIZER);
    ceph::buffer::list auth_retry_bl;
    int r;
    connection->lock.unlock();
    if (more) {
      r = messenger->auth_client->handle_auth_reply_more(
	connection, am.get(), authorizer_reply, &auth_retry_bl);
    } else {
      // these aren't used for v1
      CryptoKey skey;
      string con_secret;
      r = messenger->auth_client->handle_auth_done(
	connection, am.get(),
	0 /* global id */, 0 /* con mode */,
	authorizer_reply,
	&skey, &con_secret);
    }
    connection->lock.lock();
    if (state != CONNECTING_SEND_CONNECT_MSG) {
      ldout(cct, 1) << __func__ << " state changed" << dendl;
      return _fault();
    }
    if (r < 0) {
      return _fault();
    }
    if (more && r == 0) {
      authorizer_more = auth_retry_bl;
      return CONTINUE(send_connect_message);
    }
  }

  return handle_connect_reply_2();
}

CtPtr ProtocolV1::handle_connect_reply_2() {
  ldout(cct, 20) << __func__ << dendl;

  if (connect_reply.tag == CEPH_MSGR_TAG_FEATURES) {
    ldout(cct, 0) << __func__ << " connect protocol feature mismatch, my "
                  << std::hex << connection->policy.features_supported
                  << " < peer " << connect_reply.features << " missing "
                  << (connect_reply.features &
                      ~connection->policy.features_supported)
                  << std::dec << dendl;
    return _fault();
  }

  if (connect_reply.tag == CEPH_MSGR_TAG_BADPROTOVER) {
    ldout(cct, 0) << __func__ << " connect protocol version mismatch, my "
                  << messenger->get_proto_version(connection->peer_type, true)
                  << " != " << connect_reply.protocol_version << dendl;
    return _fault();
  }

  if (connect_reply.tag == CEPH_MSGR_TAG_BADAUTHORIZER) {
    ldout(cct, 0) << __func__ << " connect got BADAUTHORIZER" << dendl;
    authorizer_more.clear();
    return _fault();
  }

  if (connect_reply.tag == CEPH_MSGR_TAG_RESETSESSION) {
    ldout(cct, 0) << __func__ << " connect got RESETSESSION" << dendl;
    session_reset();
    connect_seq = 0;

    // see session_reset
    connection->outgoing_bl.clear();

    return CONTINUE(send_connect_message);
  }

  if (connect_reply.tag == CEPH_MSGR_TAG_RETRY_GLOBAL) {
    global_seq = messenger->get_global_seq(connect_reply.global_seq);
    ldout(cct, 5) << __func__ << " connect got RETRY_GLOBAL "
                  << connect_reply.global_seq << " chose new " << global_seq
                  << dendl;
    return CONTINUE(send_connect_message);
  }

  if (connect_reply.tag == CEPH_MSGR_TAG_RETRY_SESSION) {
    ceph_assert(connect_reply.connect_seq > connect_seq);
    ldout(cct, 5) << __func__ << " connect got RETRY_SESSION " << connect_seq
                  << " -> " << connect_reply.connect_seq << dendl;
    connect_seq = connect_reply.connect_seq;
    return CONTINUE(send_connect_message);
  }

  if (connect_reply.tag == CEPH_MSGR_TAG_WAIT) {
    ldout(cct, 1) << __func__ << " connect got WAIT (connection race)" << dendl;
    state = WAIT;
    return _fault();
  }

  uint64_t feat_missing;
  feat_missing =
      connection->policy.features_required & ~(uint64_t)connect_reply.features;
  if (feat_missing) {
    ldout(cct, 1) << __func__ << " missing required features " << std::hex
                  << feat_missing << std::dec << dendl;
    return _fault();
  }

  if (connect_reply.tag == CEPH_MSGR_TAG_SEQ) {
    ldout(cct, 10)
        << __func__
        << " got CEPH_MSGR_TAG_SEQ, reading acked_seq and writing in_seq"
        << dendl;

    return wait_ack_seq();
  }

  if (connect_reply.tag == CEPH_MSGR_TAG_READY) {
    ldout(cct, 10) << __func__ << " got CEPH_MSGR_TAG_READY " << dendl;
  }

  return client_ready();
}

CtPtr ProtocolV1::wait_ack_seq() {
  ldout(cct, 20) << __func__ << dendl;

  return READ(sizeof(uint64_t), handle_ack_seq);
}

CtPtr ProtocolV1::handle_ack_seq(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read connect ack seq failed" << dendl;
    return _fault();
  }

  uint64_t newly_acked_seq = 0;

  newly_acked_seq = *((uint64_t *)buffer);
  ldout(cct, 2) << __func__ << " got newly_acked_seq " << newly_acked_seq
                << " vs out_seq " << out_seq << dendl;
  out_seq = discard_requeued_up_to(out_seq, newly_acked_seq);

  ceph::buffer::list bl;
  uint64_t s = in_seq;
  bl.append((char *)&s, sizeof(s));

  return WRITE(bl, handle_in_seq_write);
}

CtPtr ProtocolV1::handle_in_seq_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 10) << __func__ << " failed to send in_seq " << dendl;
    return _fault();
  }

  ldout(cct, 10) << __func__ << " send in_seq done " << dendl;

  return client_ready();
}

CtPtr ProtocolV1::client_ready() {
  ldout(cct, 20) << __func__ << dendl;

  // hooray!
  peer_global_seq = connect_reply.global_seq;
  connection->policy.lossy = connect_reply.flags & CEPH_MSG_CONNECT_LOSSY;

  once_ready = true;
  connect_seq += 1;
  ceph_assert(connect_seq == connect_reply.connect_seq);
  backoff = utime_t();
  connection->set_features((uint64_t)connect_reply.features &
                           (uint64_t)connection->policy.features_supported);
  ldout(cct, 10) << __func__ << " connect success " << connect_seq
                 << ", lossy = " << connection->policy.lossy << ", features "
                 << connection->get_features() << dendl;

  // If we have an authorizer, get a new AuthSessionHandler to deal with
  // ongoing security of the connection.  PLR
  if (auth_meta->authorizer) {
    ldout(cct, 10) << __func__ << " setting up session_security with auth "
		   << auth_meta->authorizer.get() << dendl;
    session_security.reset(get_auth_session_handler(
        cct, auth_meta->authorizer->protocol,
	auth_meta->session_key,
        connection->get_features()));
  } else {
    // We have no authorizer, so we shouldn't be applying security to messages
    // in this AsyncConnection.  PLR
    ldout(cct, 10) << __func__ << " no authorizer, clearing session_security"
		   << dendl;
    session_security.reset();
  }

  if (connection->delay_state) {
    ceph_assert(connection->delay_state->ready());
  }
  connection->dispatch_queue->queue_connect(connection);
  messenger->ms_deliver_handle_fast_connect(connection);

  return ready();
}

/**
 * Server Protocol V1
 **/

CtPtr ProtocolV1::send_server_banner() {
  ldout(cct, 20) << __func__ << dendl;
  state = ACCEPTING;

  ceph::buffer::list bl;

  bl.append(CEPH_BANNER, strlen(CEPH_BANNER));

  // as a server, we should have a legacy addr if we accepted this connection.
  auto legacy = messenger->get_myaddrs().legacy_addr();
  encode(legacy, bl, 0);  // legacy
  connection->port = legacy.get_port();
  encode(connection->target_addr, bl, 0);  // legacy

  ldout(cct, 1) << __func__ << " sd=" << connection->cs.fd()
		<< " legacy " << legacy
		<< " socket_addr " << connection->socket_addr
		<< " target_addr " << connection->target_addr
		<< dendl;

  return WRITE(bl, handle_server_banner_write);
}

CtPtr ProtocolV1::handle_server_banner_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << " write server banner failed" << dendl;
    return _fault();
  }
  ldout(cct, 10) << __func__ << " write banner and addr done: "
                 << connection->get_peer_addr() << dendl;

  return wait_client_banner();
}

CtPtr ProtocolV1::wait_client_banner() {
  ldout(cct, 20) << __func__ << dendl;

  return READ(strlen(CEPH_BANNER) + sizeof(ceph_entity_addr),
              handle_client_banner);
}

CtPtr ProtocolV1::handle_client_banner(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read peer banner and addr failed" << dendl;
    return _fault();
  }

  if (memcmp(buffer, CEPH_BANNER, strlen(CEPH_BANNER))) {
    ldout(cct, 1) << __func__ << " accept peer sent bad banner '" << buffer
                  << "' (should be '" << CEPH_BANNER << "')" << dendl;
    return _fault();
  }

  ceph::buffer::list addr_bl;
  entity_addr_t peer_addr;

  addr_bl.append(buffer + strlen(CEPH_BANNER), sizeof(ceph_entity_addr));
  try {
    auto ti = addr_bl.cbegin();
    decode(peer_addr, ti);
  } catch (const ceph::buffer::error &e) {
    lderr(cct) << __func__ << " decode peer_addr failed " << dendl;
    return _fault();
  }

  ldout(cct, 10) << __func__ << " accept peer addr is " << peer_addr << dendl;
  if (peer_addr.is_blank_ip()) {
    // peer apparently doesn't know what ip they have; figure it out for them.
    int port = peer_addr.get_port();
    peer_addr.set_sockaddr(connection->target_addr.get_sockaddr());
    peer_addr.set_port(port);

    ldout(cct, 0) << __func__ << " accept peer addr is really " << peer_addr
                  << " (socket is " << connection->target_addr << ")" << dendl;
  }
  connection->set_peer_addr(peer_addr);  // so that connection_state gets set up
  connection->target_addr = peer_addr;

  return CONTINUE(wait_connect_message);
}

CtPtr ProtocolV1::wait_connect_message() {
  ldout(cct, 20) << __func__ << dendl;

  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(&connect_msg, 0, sizeof(connect_msg));
  return READ(sizeof(connect_msg), handle_connect_message_1);
}

CtPtr ProtocolV1::handle_connect_message_1(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read connect msg failed" << dendl;
    return _fault();
  }

  connect_msg = *((ceph_msg_connect *)buffer);

  state = ACCEPTING_WAIT_CONNECT_MSG_AUTH;

  if (connect_msg.authorizer_len) {
    return wait_connect_message_auth();
  }

  return handle_connect_message_2();
}

CtPtr ProtocolV1::wait_connect_message_auth() {
  ldout(cct, 20) << __func__ << dendl;
  authorizer_buf.clear();
  authorizer_buf.push_back(ceph::buffer::create(connect_msg.authorizer_len));
  return READB(connect_msg.authorizer_len, authorizer_buf.c_str(),
               handle_connect_message_auth);
}

CtPtr ProtocolV1::handle_connect_message_auth(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read connect authorizer failed" << dendl;
    return _fault();
  }

  return handle_connect_message_2();
}

CtPtr ProtocolV1::handle_connect_message_2() {
  ldout(cct, 20) << __func__ << dendl;

  ldout(cct, 20) << __func__ << " accept got peer connect_seq "
                 << connect_msg.connect_seq << " global_seq "
                 << connect_msg.global_seq << dendl;

  connection->set_peer_type(connect_msg.host_type);
  connection->policy = messenger->get_policy(connect_msg.host_type);

  ldout(cct, 10) << __func__ << " accept of host_type " << connect_msg.host_type
                 << ", policy.lossy=" << connection->policy.lossy
                 << " policy.server=" << connection->policy.server
                 << " policy.standby=" << connection->policy.standby
                 << " policy.resetcheck=" << connection->policy.resetcheck
		 << " features 0x" << std::hex << (uint64_t)connect_msg.features
		 << std::dec
                 << dendl;

  ceph_msg_connect_reply reply;
  ceph::buffer::list authorizer_reply;

  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(&reply, 0, sizeof(reply));
  reply.protocol_version =
      messenger->get_proto_version(connection->peer_type, false);

  // mismatch?
  ldout(cct, 10) << __func__ << " accept my proto " << reply.protocol_version
                 << ", their proto " << connect_msg.protocol_version << dendl;

  if (connect_msg.protocol_version != reply.protocol_version) {
    return send_connect_message_reply(CEPH_MSGR_TAG_BADPROTOVER, reply,
                                      authorizer_reply);
  }

  // require signatures for cephx?
  if (connect_msg.authorizer_protocol == CEPH_AUTH_CEPHX) {
    if (connection->peer_type == CEPH_ENTITY_TYPE_OSD ||
        connection->peer_type == CEPH_ENTITY_TYPE_MDS ||
        connection->peer_type == CEPH_ENTITY_TYPE_MGR) {
      if (cct->_conf->cephx_require_signatures ||
          cct->_conf->cephx_cluster_require_signatures) {
        ldout(cct, 10)
            << __func__
            << " using cephx, requiring MSG_AUTH feature bit for cluster"
            << dendl;
        connection->policy.features_required |= CEPH_FEATURE_MSG_AUTH;
      }
      if (cct->_conf->cephx_require_version >= 2 ||
          cct->_conf->cephx_cluster_require_version >= 2) {
        ldout(cct, 10)
            << __func__
            << " using cephx, requiring cephx v2 feature bit for cluster"
            << dendl;
        connection->policy.features_required |= CEPH_FEATUREMASK_CEPHX_V2;
      }
    } else {
      if (cct->_conf->cephx_require_signatures ||
          cct->_conf->cephx_service_require_signatures) {
        ldout(cct, 10)
            << __func__
            << " using cephx, requiring MSG_AUTH feature bit for service"
            << dendl;
        connection->policy.features_required |= CEPH_FEATURE_MSG_AUTH;
      }
      if (cct->_conf->cephx_require_version >= 2 ||
          cct->_conf->cephx_service_require_version >= 2) {
        ldout(cct, 10)
            << __func__
            << " using cephx, requiring cephx v2 feature bit for service"
            << dendl;
        connection->policy.features_required |= CEPH_FEATUREMASK_CEPHX_V2;
      }
    }
  }

  uint64_t feat_missing =
      connection->policy.features_required & ~(uint64_t)connect_msg.features;
  if (feat_missing) {
    ldout(cct, 1) << __func__ << " peer missing required features " << std::hex
                  << feat_missing << std::dec << dendl;
    return send_connect_message_reply(CEPH_MSGR_TAG_FEATURES, reply,
                                      authorizer_reply);
  }

  ceph::buffer::list auth_bl_copy = authorizer_buf;
  auto am = auth_meta;
  am->auth_method = connect_msg.authorizer_protocol;
  if (!HAVE_FEATURE((uint64_t)connect_msg.features, CEPHX_V2)) {
    // peer doesn't support it and we won't get here if we require it
    am->skip_authorizer_challenge = true;
  }
  connection->lock.unlock();
  ldout(cct,10) << __func__ << " authorizor_protocol "
		<< connect_msg.authorizer_protocol
		<< " len " << auth_bl_copy.length()
		<< dendl;
  bool more = (bool)auth_meta->authorizer_challenge;
  int r = messenger->auth_server->handle_auth_request(
    connection,
    am.get(),
    more,
    am->auth_method,
    auth_bl_copy,
    &authorizer_reply);
  if (r < 0) {
    connection->lock.lock();
    if (state != ACCEPTING_WAIT_CONNECT_MSG_AUTH) {
      ldout(cct, 1) << __func__ << " state changed" << dendl;
      return _fault();
    }
    ldout(cct, 0) << __func__ << ": got bad authorizer, auth_reply_len="
		  << authorizer_reply.length() << dendl;
    session_security.reset();
    return send_connect_message_reply(CEPH_MSGR_TAG_BADAUTHORIZER, reply,
				      authorizer_reply);
  }
  if (r == 0) {
    connection->lock.lock();
    if (state != ACCEPTING_WAIT_CONNECT_MSG_AUTH) {
      ldout(cct, 1) << __func__ << " state changed" << dendl;
      return _fault();
    }
    ldout(cct, 10) << __func__ << ": challenging authorizer" << dendl;
    ceph_assert(authorizer_reply.length());
    return send_connect_message_reply(CEPH_MSGR_TAG_CHALLENGE_AUTHORIZER,
				      reply, authorizer_reply);
  }

  // We've verified the authorizer for this AsyncConnection, so set up the
  // session security structure.  PLR
  ldout(cct, 10) << __func__ << " accept setting up session_security." << dendl;

  if (connection->policy.server &&
      connection->policy.lossy &&
      !connection->policy.register_lossy_clients) {
    // incoming lossy client, no need to register this connection
    // new session
    ldout(cct, 10) << __func__ << " accept new session" << dendl;
    connection->lock.lock();
    return open(reply, authorizer_reply);
  }

  AsyncConnectionRef existing = messenger->lookup_conn(*connection->peer_addrs);

  connection->inject_delay();

  connection->lock.lock();
  if (state != ACCEPTING_WAIT_CONNECT_MSG_AUTH) {
    ldout(cct, 1) << __func__ << " state changed" << dendl;
    return _fault();
  }

  if (existing == connection) {
    existing = nullptr;
  }
  if (existing && existing->protocol->proto_type != 1) {
    ldout(cct,1) << __func__ << " existing " << existing << " proto "
		 << existing->protocol.get() << " version is "
		 << existing->protocol->proto_type << ", marking down" << dendl;
    existing->mark_down();
    existing = nullptr;
  }

  if (existing) {
    // There is no possible that existing connection will acquire this
    // connection's lock
    existing->lock.lock();  // skip lockdep check (we are locking a second
                            // AsyncConnection here)

    ldout(cct,10) << __func__ << " existing=" << existing << " exproto="
		  << existing->protocol.get() << dendl;
    ProtocolV1 *exproto = dynamic_cast<ProtocolV1 *>(existing->protocol.get());
    ceph_assert(exproto);
    ceph_assert(exproto->proto_type == 1);

    if (exproto->state == CLOSED) {
      ldout(cct, 1) << __func__ << " existing " << existing
		    << " already closed." << dendl;
      existing->lock.unlock();
      existing = nullptr;

      return open(reply, authorizer_reply);
    }

    if (exproto->replacing) {
      ldout(cct, 1) << __func__
                    << " existing racing replace happened while replacing."
                    << " existing_state="
                    << connection->get_state_name(existing->state) << dendl;
      reply.global_seq = exproto->peer_global_seq;
      existing->lock.unlock();
      return send_connect_message_reply(CEPH_MSGR_TAG_RETRY_GLOBAL, reply,
                                        authorizer_reply);
    }

    if (connect_msg.global_seq < exproto->peer_global_seq) {
      ldout(cct, 10) << __func__ << " accept existing " << existing << ".gseq "
                     << exproto->peer_global_seq << " > "
                     << connect_msg.global_seq << ", RETRY_GLOBAL" << dendl;
      reply.global_seq = exproto->peer_global_seq;  // so we can send it below..
      existing->lock.unlock();
      return send_connect_message_reply(CEPH_MSGR_TAG_RETRY_GLOBAL, reply,
                                        authorizer_reply);
    } else {
      ldout(cct, 10) << __func__ << " accept existing " << existing << ".gseq "
                     << exproto->peer_global_seq
                     << " <= " << connect_msg.global_seq << ", looks ok"
                     << dendl;
    }

    if (existing->policy.lossy) {
      ldout(cct, 0)
          << __func__
          << " accept replacing existing (lossy) channel (new one lossy="
          << connection->policy.lossy << ")" << dendl;
      exproto->session_reset();
      return replace(existing, reply, authorizer_reply);
    }

    ldout(cct, 1) << __func__ << " accept connect_seq "
                  << connect_msg.connect_seq
                  << " vs existing csq=" << exproto->connect_seq
                  << " existing_state="
                  << connection->get_state_name(existing->state) << dendl;

    if (connect_msg.connect_seq == 0 && exproto->connect_seq > 0) {
      ldout(cct, 0)
          << __func__
          << " accept peer reset, then tried to connect to us, replacing"
          << dendl;
      // this is a hard reset from peer
      is_reset_from_peer = true;
      if (connection->policy.resetcheck) {
        exproto->session_reset();  // this resets out_queue, msg_ and
                                   // connect_seq #'s
      }
      return replace(existing, reply, authorizer_reply);
    }

    if (connect_msg.connect_seq < exproto->connect_seq) {
      // old attempt, or we sent READY but they didn't get it.
      ldout(cct, 10) << __func__ << " accept existing " << existing << ".cseq "
                     << exproto->connect_seq << " > " << connect_msg.connect_seq
                     << ", RETRY_SESSION" << dendl;
      reply.connect_seq = exproto->connect_seq + 1;
      existing->lock.unlock();
      return send_connect_message_reply(CEPH_MSGR_TAG_RETRY_SESSION, reply,
                                        authorizer_reply);
    }

    if (connect_msg.connect_seq == exproto->connect_seq) {
      // if the existing connection successfully opened, and/or
      // subsequently went to standby, then the peer should bump
      // their connect_seq and retry: this is not a connection race
      // we need to resolve here.
      if (exproto->state == OPENED || exproto->state == STANDBY) {
        ldout(cct, 10) << __func__ << " accept connection race, existing "
                       << existing << ".cseq " << exproto->connect_seq
                       << " == " << connect_msg.connect_seq
                       << ", OPEN|STANDBY, RETRY_SESSION " << dendl;
        // if connect_seq both zero, dont stuck into dead lock. it's ok to
        // replace
        if (connection->policy.resetcheck && exproto->connect_seq == 0) {
          return replace(existing, reply, authorizer_reply);
        }

        reply.connect_seq = exproto->connect_seq + 1;
        existing->lock.unlock();
        return send_connect_message_reply(CEPH_MSGR_TAG_RETRY_SESSION, reply,
                                          authorizer_reply);
      }

      // connection race?
      if (connection->peer_addrs->legacy_addr() < messenger->get_myaddr_legacy() ||
          existing->policy.server) {
        // incoming wins
        ldout(cct, 10) << __func__ << " accept connection race, existing "
                       << existing << ".cseq " << exproto->connect_seq
                       << " == " << connect_msg.connect_seq
                       << ", or we are server, replacing my attempt" << dendl;
        return replace(existing, reply, authorizer_reply);
      } else {
        // our existing outgoing wins
        ldout(messenger->cct, 10)
            << __func__ << " accept connection race, existing " << existing
            << ".cseq " << exproto->connect_seq
            << " == " << connect_msg.connect_seq << ", sending WAIT" << dendl;
        ceph_assert(connection->peer_addrs->legacy_addr() >
                    messenger->get_myaddr_legacy());
        existing->lock.unlock();
	// make sure we follow through with opening the existing
	// connection (if it isn't yet open) since we know the peer
	// has something to send to us.
	existing->send_keepalive();
        return send_connect_message_reply(CEPH_MSGR_TAG_WAIT, reply,
                                          authorizer_reply);
      }
    }

    ceph_assert(connect_msg.connect_seq > exproto->connect_seq);
    ceph_assert(connect_msg.global_seq >= exproto->peer_global_seq);
    if (connection->policy.resetcheck &&  // RESETSESSION only used by servers;
                                          // peers do not reset each other
        exproto->connect_seq == 0) {
      ldout(cct, 0) << __func__ << " accept we reset (peer sent cseq "
                    << connect_msg.connect_seq << ", " << existing
                    << ".cseq = " << exproto->connect_seq
                    << "), sending RESETSESSION " << dendl;
      existing->lock.unlock();
      return send_connect_message_reply(CEPH_MSGR_TAG_RESETSESSION, reply,
                                        authorizer_reply);
    }

    // reconnect
    ldout(cct, 10) << __func__ << " accept peer sent cseq "
                   << connect_msg.connect_seq << " > " << exproto->connect_seq
                   << dendl;
    return replace(existing, reply, authorizer_reply);
  }  // existing
  else if (!replacing && connect_msg.connect_seq > 0) {
    // we reset, and they are opening a new session
    ldout(cct, 0) << __func__ << " accept we reset (peer sent cseq "
                  << connect_msg.connect_seq << "), sending RESETSESSION"
                  << dendl;
    return send_connect_message_reply(CEPH_MSGR_TAG_RESETSESSION, reply,
                                      authorizer_reply);
  } else {
    // new session
    ldout(cct, 10) << __func__ << " accept new session" << dendl;
    existing = nullptr;
    return open(reply, authorizer_reply);
  }
}

CtPtr ProtocolV1::send_connect_message_reply(char tag,
                                             ceph_msg_connect_reply &reply,
                                             ceph::buffer::list &authorizer_reply) {
  ldout(cct, 20) << __func__ << dendl;
  ceph::buffer::list reply_bl;
  reply.tag = tag;
  reply.features =
      ((uint64_t)connect_msg.features & connection->policy.features_supported) |
      connection->policy.features_required;
  reply.authorizer_len = authorizer_reply.length();
  reply_bl.append((char *)&reply, sizeof(reply));

  ldout(cct, 10) << __func__ << " reply features 0x" << std::hex
		 << reply.features << " = (policy sup 0x"
		 << connection->policy.features_supported
		 << " & connect 0x" << (uint64_t)connect_msg.features
		 << ") | policy req 0x"
		 << connection->policy.features_required
		 << dendl;

  if (reply.authorizer_len) {
    reply_bl.append(authorizer_reply.c_str(), authorizer_reply.length());
    authorizer_reply.clear();
  }

  return WRITE(reply_bl, handle_connect_message_reply_write);
}

CtPtr ProtocolV1::handle_connect_message_reply_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << " write connect message reply failed" << dendl;
    connection->inject_delay();
    return _fault();
  }

  return CONTINUE(wait_connect_message);
}

CtPtr ProtocolV1::replace(const AsyncConnectionRef& existing,
                          ceph_msg_connect_reply &reply,
                          ceph::buffer::list &authorizer_reply) {
  ldout(cct, 10) << __func__ << " accept replacing " << existing << dendl;

  connection->inject_delay();
  if (existing->policy.lossy) {
    // disconnect from the Connection
    ldout(cct, 1) << __func__ << " replacing on lossy channel, failing existing"
                  << dendl;
    existing->protocol->stop();
    existing->dispatch_queue->queue_reset(existing.get());
  } else {
    ceph_assert(can_write == WriteStatus::NOWRITE);
    existing->write_lock.lock();

    ProtocolV1 *exproto = dynamic_cast<ProtocolV1 *>(existing->protocol.get());

    // reset the in_seq if this is a hard reset from peer,
    // otherwise we respect our original connection's value
    if (is_reset_from_peer) {
      exproto->is_reset_from_peer = true;
    }

    connection->center->delete_file_event(connection->cs.fd(),
                                          EVENT_READABLE | EVENT_WRITABLE);

    if (existing->delay_state) {
      existing->delay_state->flush();
      ceph_assert(!connection->delay_state);
    }
    exproto->reset_recv_state();

    exproto->connect_msg.features = connect_msg.features;

    auto temp_cs = std::move(connection->cs);
    EventCenter *new_center = connection->center;
    Worker *new_worker = connection->worker;
    // avoid _stop shutdown replacing socket
    // queue a reset on the new connection, which we're dumping for the old
    stop();

    connection->dispatch_queue->queue_reset(connection);
    ldout(messenger->cct, 1)
        << __func__ << " stop myself to swap existing" << dendl;
    exproto->can_write = WriteStatus::REPLACING;
    exproto->replacing = true;
    exproto->write_in_progress = false;
    existing->state_offset = 0;
    // avoid previous thread modify event
    exproto->state = NONE;
    existing->state = AsyncConnection::STATE_NONE;
    // Discard existing prefetch buffer in `recv_buf`
    existing->recv_start = existing->recv_end = 0;
    // there shouldn't exist any buffer
    ceph_assert(connection->recv_start == connection->recv_end);

    auto deactivate_existing = std::bind(
        [existing, new_worker, new_center, exproto, reply,
         authorizer_reply](ConnectedSocket &cs) mutable {
          // we need to delete time event in original thread
          {
            std::lock_guard<std::mutex> l(existing->lock);
            existing->write_lock.lock();
            exproto->requeue_sent();
            existing->outgoing_bl.clear();
            existing->open_write = false;
            existing->write_lock.unlock();
            if (exproto->state == NONE) {
              existing->shutdown_socket();
              existing->cs = std::move(cs);
              existing->worker->references--;
              new_worker->references++;
              existing->logger = new_worker->get_perf_counter();
              existing->labeled_logger = new_worker->get_labeled_perf_counter();
              existing->worker = new_worker;
              existing->center = new_center;
              if (existing->delay_state)
                existing->delay_state->set_center(new_center);
            } else if (exproto->state == CLOSED) {
              auto back_to_close =
                  std::bind([](ConnectedSocket &cs) mutable { cs.close(); },
                            std::move(cs));
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
          auto transfer_existing = [existing, exproto, reply,
                                    authorizer_reply]() mutable {
            std::lock_guard<std::mutex> l(existing->lock);
            if (exproto->state == CLOSED) return;
            ceph_assert(exproto->state == NONE);

            // we have called shutdown_socket above
            ceph_assert(existing->last_tick_id == 0);
            // restart timer since we are going to re-build connection
            existing->last_connect_started = ceph::coarse_mono_clock::now();
            existing->last_tick_id = existing->center->create_time_event(
              existing->connect_timeout_us, existing->tick_handler);
            existing->state = AsyncConnection::STATE_CONNECTION_ESTABLISHED;
            exproto->state = ACCEPTING;

            existing->center->create_file_event(
                existing->cs.fd(), EVENT_READABLE, existing->read_handler);
            reply.global_seq = exproto->peer_global_seq;
            exproto->run_continuation(exproto->send_connect_message_reply(
                CEPH_MSGR_TAG_RETRY_GLOBAL, reply, authorizer_reply));
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
    existing->write_lock.unlock();
    existing->lock.unlock();
    return nullptr;
  }
  existing->lock.unlock();

  return open(reply, authorizer_reply);
}

CtPtr ProtocolV1::open(ceph_msg_connect_reply &reply,
                       ceph::buffer::list &authorizer_reply) {
  ldout(cct, 20) << __func__ << dendl;

  connect_seq = connect_msg.connect_seq + 1;
  peer_global_seq = connect_msg.global_seq;
  ldout(cct, 10) << __func__ << " accept success, connect_seq = " << connect_seq
                 << " in_seq=" << in_seq << ", sending READY" << dendl;

  // if it is a hard reset from peer, we don't need a round-trip to negotiate
  // in/out sequence
  if ((connect_msg.features & CEPH_FEATURE_RECONNECT_SEQ) &&
      !is_reset_from_peer) {
    reply.tag = CEPH_MSGR_TAG_SEQ;
    wait_for_seq = true;
  } else {
    reply.tag = CEPH_MSGR_TAG_READY;
    wait_for_seq = false;
    out_seq = discard_requeued_up_to(out_seq, 0);
    is_reset_from_peer = false;
    in_seq = 0;
  }

  // send READY reply
  reply.features = connection->policy.features_supported;
  reply.global_seq = messenger->get_global_seq();
  reply.connect_seq = connect_seq;
  reply.flags = 0;
  reply.authorizer_len = authorizer_reply.length();
  if (connection->policy.lossy) {
    reply.flags = reply.flags | CEPH_MSG_CONNECT_LOSSY;
  }

  connection->set_features((uint64_t)reply.features &
                           (uint64_t)connect_msg.features);
  ldout(cct, 10) << __func__ << " accept features "
                 << connection->get_features()
		 << " authorizer_protocol "
		 << connect_msg.authorizer_protocol << dendl;

  session_security.reset(
    get_auth_session_handler(cct, auth_meta->auth_method,
			     auth_meta->session_key,
			     connection->get_features()));

  ceph::buffer::list reply_bl;
  reply_bl.append((char *)&reply, sizeof(reply));

  if (reply.authorizer_len) {
    reply_bl.append(authorizer_reply.c_str(), authorizer_reply.length());
  }

  if (reply.tag == CEPH_MSGR_TAG_SEQ) {
    uint64_t s = in_seq;
    reply_bl.append((char *)&s, sizeof(s));
  }

  connection->lock.unlock();
  // Because "replacing" will prevent other connections preempt this addr,
  // it's safe that here we don't acquire Connection's lock
  ssize_t r = messenger->accept_conn(connection);

  connection->inject_delay();

  connection->lock.lock();
  replacing = false;
  if (r < 0) {
    ldout(cct, 1) << __func__ << " existing race replacing process for addr = "
                  << connection->peer_addrs->legacy_addr()
                  << " just fail later one(this)" << dendl;
    ldout(cct, 10) << "accept fault after register" << dendl;
    connection->inject_delay();
    return _fault();
  }
  if (state != ACCEPTING_WAIT_CONNECT_MSG_AUTH) {
    ldout(cct, 1) << __func__
                  << " state changed while accept_conn, it must be mark_down"
                  << dendl;
    ceph_assert(state == CLOSED || state == NONE);
    ldout(cct, 10) << "accept fault after register" << dendl;
    messenger->unregister_conn(connection);
    connection->inject_delay();
    return _fault();
  }

  return WRITE(reply_bl, handle_ready_connect_message_reply_write);
}

CtPtr ProtocolV1::handle_ready_connect_message_reply_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " write ready connect message reply failed"
                  << dendl;
    return _fault();
  }

  // notify
  connection->dispatch_queue->queue_accept(connection);
  messenger->ms_deliver_handle_fast_accept(connection);
  once_ready = true;

  state = ACCEPTING_HANDLED_CONNECT_MSG;

  if (wait_for_seq) {
    return wait_seq();
  }

  return server_ready();
}

CtPtr ProtocolV1::wait_seq() {
  ldout(cct, 20) << __func__ << dendl;

  return READ(sizeof(uint64_t), handle_seq);
}

CtPtr ProtocolV1::handle_seq(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read ack seq failed" << dendl;
    return _fault();
  }

  uint64_t newly_acked_seq = *(uint64_t *)buffer;
  ldout(cct, 2) << __func__ << " accept get newly_acked_seq " << newly_acked_seq
                << dendl;
  out_seq = discard_requeued_up_to(out_seq, newly_acked_seq);

  return server_ready();
}

CtPtr ProtocolV1::server_ready() {
  ldout(cct, 20) << __func__ << " session_security is "
		 << session_security
		 << dendl;

  ldout(cct, 20) << __func__ << " accept done" << dendl;
  // FIPS zeroization audit 20191115: this memset is not security related.
  memset(&connect_msg, 0, sizeof(connect_msg));

  if (connection->delay_state) {
    ceph_assert(connection->delay_state->ready());
  }

  return ready();
}
