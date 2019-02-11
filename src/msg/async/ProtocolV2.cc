// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
                << this << " :" << connection->port
                << " s=" << get_state_name(state) << " pgs=" << peer_global_seq
                << " cs=" << connect_seq << " l=" << connection->policy.lossy
                << ").";
}

using CtPtr = Ct<ProtocolV2> *;

void ProtocolV2::get_auth_allowed_methods(
  int peer_type, std::vector<uint32_t> &allowed_methods)
{
  std::string method;
  if (!cct->_conf->auth_supported.empty()) {
    method = cct->_conf->auth_supported;
  } else if (peer_type == CEPH_ENTITY_TYPE_OSD ||
	     peer_type == CEPH_ENTITY_TYPE_MDS ||
	     peer_type == CEPH_ENTITY_TYPE_MON ||
	     peer_type == CEPH_ENTITY_TYPE_MGR) {
    method = cct->_conf->auth_cluster_required;
  } else {
    method = cct->_conf->auth_client_required;
  }
  AuthMethodList auth_list(cct, method);
  for (auto pt : auth_list.get_supported_set()) {
    allowed_methods.push_back(pt);
  }
}


void ProtocolV2::run_continuation(CtPtr continuation) {
  try {
    CONTINUATION_RUN(continuation)
  } catch (const buffer::error &e) {
    lderr(cct) << __func__ << " failed decoding of frame header: " << e
               << dendl;
    _fault();
  } catch (const SHA256SignatureError &e) {
    lderr(cct) << __func__ << " " << e.what() << dendl;
    _fault();
  } catch (const DecryptionError &) {
    lderr(cct) << __func__ << " failed to decrypt frame payload" << dendl;
  }
}

#define WRITE(B, D, C) write(D, CONTINUATION(C), B)

#define READ(L, C) read(CONTINUATION(C), L)

#define READB(L, B, C) read(CONTINUATION(C), L, B)

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

/**
 * Protocol V2 Frame Structures
 **/

template <class T>
struct Frame {
protected:
  ceph::bufferlist payload;
  ceph::bufferlist::contiguous_filler preamble_filler;

public:
  Frame() : preamble_filler(payload.append_hole(8)) {}

  bufferlist &get_buffer(const uint32_t extra_payload_len = 0) {
    __le32 msg_len = payload.length() + extra_payload_len - sizeof(std::uint32_t);
    preamble_filler.copy_in(sizeof(msg_len),
			    reinterpret_cast<const char*>(&msg_len));

    __le32 tag = static_cast<uint32_t>(static_cast<T *>(this)->tag);
    preamble_filler.copy_in(sizeof(tag),
			    reinterpret_cast<const char*>(&tag));
    ceph_assert(tag != 0);

    return payload;
  }

  void decode_frame(char *payload, uint32_t length) {
    bufferlist bl;
    bl.push_back(buffer::create_static(length, payload));
    auto ti = bl.cbegin();
    static_cast<T *>(this)->decode_payload(ti);
  }

  void decode_payload(bufferlist::const_iterator &ti) {}
};

template <class C, typename... Args>
struct PayloadFrame : public Frame<C> {
protected:
  // this tuple is only used when decoding values from a payload buffer
  std::tuple<Args...> _values;

  template <typename T>
  inline void _encode_payload_each(T &t) {
    if constexpr (std::is_same<T, bufferlist const>()) {
      this->payload.claim_append((bufferlist &)t);
    } else if constexpr (std::is_same<T, std::vector<uint32_t> const>()) {
      encode((uint32_t)t.size(), this->payload, -1ll);
      for (const auto &elem : t) {
        encode(elem, this->payload, 0);
      }
    } else if constexpr (std::is_same<T, ceph_msg_header2 const>()) {
      this->payload.append((char *)&t, sizeof(t));
    } else {
      encode(t, this->payload, -1ll);
    }
  }

  template <typename T>
  inline void _decode_payload_each(T &t, bufferlist::const_iterator &ti) const {
    if constexpr (std::is_same<T, bufferlist>()) {
      if (ti.get_remaining()) {
        t.append(ti.get_current_ptr());
      }
    } else if constexpr (std::is_same<T, std::vector<uint32_t>>()) {
      uint32_t size;
      decode(size, ti);
      t.resize(size);
      for (uint32_t i = 0; i < size; ++i) {
        decode(t[i], ti);
      }
    } else if constexpr (std::is_same<T, ceph_msg_header2>()) {
      auto ptr = ti.get_current_ptr();
      ti.advance(sizeof(T));
      t = *(T *)ptr.raw_c_str();
    } else {
      decode(t, ti);
    }
  }

  template <std::size_t... Is>
  inline void _decode_payload(bufferlist::const_iterator &ti,
                              std::index_sequence<Is...>) const {
    (_decode_payload_each((Args &)std::get<Is>(_values), ti), ...);
  }

  template <std::size_t N>
  inline decltype(auto) get_val() {
    return std::get<N>(_values);
  }

public:
  PayloadFrame(const Args &... args) {
    (_encode_payload_each(args), ...);
  }

  PayloadFrame() = default;
  PayloadFrame(char *payload, uint32_t length) {
    this->decode_frame(payload, length);
  }

  void decode_payload(bufferlist::const_iterator &ti) {
    _decode_payload(ti, std::index_sequence_for<Args...>());
  }
};

struct AuthRequestFrame
  : public PayloadFrame<AuthRequestFrame,
			uint32_t, vector<uint32_t>, bufferlist> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::AUTH_REQUEST;
  using PayloadFrame::PayloadFrame;

  inline uint32_t &method() { return get_val<0>(); }
  inline vector<uint32_t> &preferred_modes() { return get_val<1>(); }
  inline bufferlist &auth_payload() { return get_val<2>(); }
};

struct AuthBadMethodFrame
  : public PayloadFrame<AuthBadMethodFrame,
			uint32_t, // method
			int32_t,  // result
			std::vector<uint32_t>,   // allowed_methods
			std::vector<uint32_t>> { // allowed_modes
  const ProtocolV2::Tag tag = ProtocolV2::Tag::AUTH_BAD_METHOD;
  using PayloadFrame::PayloadFrame;

  inline uint32_t &method() { return get_val<0>(); }
  inline int32_t &result() { return get_val<1>(); }
  inline std::vector<uint32_t> &allowed_methods() { return get_val<2>(); }
  inline std::vector<uint32_t> &allowed_modes() { return get_val<3>(); }
};

struct AuthReplyMoreFrame
    : public PayloadFrame<AuthReplyMoreFrame, bufferlist> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::AUTH_REPLY_MORE;
  using PayloadFrame::PayloadFrame;

  inline bufferlist &auth_payload() { return get_val<0>(); }
};

struct AuthRequestMoreFrame
    : public PayloadFrame<AuthRequestMoreFrame, bufferlist> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::AUTH_REQUEST_MORE;
  using PayloadFrame::PayloadFrame;

  inline bufferlist &auth_payload() { return get_val<0>(); }
};

struct AuthDoneFrame
  : public PayloadFrame<AuthDoneFrame,
			uint64_t, // global_id
			uint32_t, // con_mode
			bufferlist> { // auth method payload
  const ProtocolV2::Tag tag = ProtocolV2::Tag::AUTH_DONE;
  using PayloadFrame::PayloadFrame;

  inline uint64_t &global_id() { return get_val<0>(); }
  inline uint32_t &con_mode() { return get_val<1>(); }
  inline bufferlist &auth_payload() { return get_val<2>(); }
};

template <class T, typename... Args>
struct SignedEncryptedFrame : public PayloadFrame<T, Args...> {
  SignedEncryptedFrame(ProtocolV2 &protocol, const Args &... args)
      : PayloadFrame<T, Args...>(args...) {
    ceph_assert(protocol.session_stream_handlers.tx);

    protocol.session_stream_handlers.tx->reset_tx_handler({
      8, this->payload.length() - 8,
    });

    // NOTE: this is just for the makeshift commits
    ceph::bufferlist trans_bl;
    this->payload.splice(8, this->payload.length() - 8, &trans_bl);
    std::swap(trans_bl, this->payload);

    this->preamble_filler = protocol.session_stream_handlers.tx->reserve(8);

    protocol.session_stream_handlers.tx->authenticated_encrypt_update(
      std::move(this->payload));
    this->payload = \
      protocol.session_stream_handlers.tx->authenticated_encrypt_final();
  }

  SignedEncryptedFrame(ProtocolV2 &protocol, char *payload, uint32_t length)
      : PayloadFrame<T, Args...>() {
    ceph::bufferlist bl;
    bl.push_back(buffer::create_static(length, payload));

    ceph::bufferlist plain_bl = \
      protocol.session_stream_handlers.rx->authenticated_decrypt_update_final(
        std::move(bl), 8);
    this->decode_frame(plain_bl.c_str(), plain_bl.length());
  }
};

struct ClientIdentFrame
    : public SignedEncryptedFrame<ClientIdentFrame, entity_addrvec_t, int64_t,
                                  uint64_t, uint64_t, uint64_t, uint64_t> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::IDENT;
  using SignedEncryptedFrame::SignedEncryptedFrame;

  inline entity_addrvec_t &addrs() { return get_val<0>(); }
  inline int64_t &gid() { return get_val<1>(); }
  inline uint64_t &global_seq() { return get_val<2>(); }
  inline uint64_t &supported_features() { return get_val<3>(); }
  inline uint64_t &required_features() { return get_val<4>(); }
  inline uint64_t &flags() { return get_val<5>(); }
};

struct ServerIdentFrame
    : public SignedEncryptedFrame<ServerIdentFrame, entity_addrvec_t,
                                  entity_addr_t, int64_t, uint64_t, uint64_t,
                                  uint64_t, uint64_t, uint64_t> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::IDENT;
  using SignedEncryptedFrame::SignedEncryptedFrame;

  inline entity_addrvec_t &addrs() { return get_val<0>(); }
  inline entity_addr_t &peer_addr() { return get_val<1>(); }
  inline int64_t &gid() { return get_val<2>(); }
  inline uint64_t &global_seq() { return get_val<3>(); }
  inline uint64_t &supported_features() { return get_val<4>(); }
  inline uint64_t &required_features() { return get_val<5>(); }
  inline uint64_t &flags() { return get_val<6>(); }
  inline uint64_t &cookie() { return get_val<7>(); }
};

struct ReconnectFrame
    : public SignedEncryptedFrame<ReconnectFrame, entity_addrvec_t, uint64_t,
                                  uint64_t, uint64_t, uint64_t> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::SESSION_RECONNECT;
  using SignedEncryptedFrame::SignedEncryptedFrame;

  inline entity_addrvec_t &addrs() { return get_val<0>(); }
  inline uint64_t &cookie() { return get_val<1>(); }
  inline uint64_t &global_seq() { return get_val<2>(); }
  inline uint64_t &connect_seq() { return get_val<3>(); }
  inline uint64_t &msg_seq() { return get_val<4>(); }
};

struct ResetFrame : public Frame<ResetFrame> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::SESSION_RESET;
};

struct RetryFrame : public SignedEncryptedFrame<RetryFrame, uint64_t> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::SESSION_RETRY;
  using SignedEncryptedFrame::SignedEncryptedFrame;

  uint64_t connect_seq() { return get_val<0>(); }
};

struct RetryGlobalFrame
    : public SignedEncryptedFrame<RetryGlobalFrame, uint64_t> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::SESSION_RETRY_GLOBAL;
  using SignedEncryptedFrame::SignedEncryptedFrame;

  inline uint64_t &global_seq() { return get_val<0>(); }
};

struct WaitFrame : public Frame<WaitFrame> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::WAIT;
};

struct ReconnectOkFrame
    : public SignedEncryptedFrame<ReconnectOkFrame, uint64_t> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::SESSION_RECONNECT_OK;
  using SignedEncryptedFrame::SignedEncryptedFrame;

  inline uint64_t &msg_seq() { return get_val<0>(); }
};

struct IdentMissingFeaturesFrame
    : public SignedEncryptedFrame<IdentMissingFeaturesFrame, uint64_t> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::IDENT_MISSING_FEATURES;
  using SignedEncryptedFrame::SignedEncryptedFrame;

  inline uint64_t &features() { return get_val<0>(); }
};

struct KeepAliveFrame : public SignedEncryptedFrame<KeepAliveFrame, utime_t> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::KEEPALIVE2;
  using SignedEncryptedFrame::SignedEncryptedFrame;

  KeepAliveFrame(ProtocolV2 &protocol)
      : KeepAliveFrame(protocol, ceph_clock_now()) {}

  inline utime_t &timestamp() { return get_val<0>(); }
};

struct KeepAliveFrameAck
    : public SignedEncryptedFrame<KeepAliveFrame, utime_t> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::KEEPALIVE2_ACK;
  using SignedEncryptedFrame::SignedEncryptedFrame;

  inline utime_t &timestamp() { return get_val<0>(); }
};

struct AckFrame : public SignedEncryptedFrame<AckFrame, uint64_t> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::ACK;
  using SignedEncryptedFrame::SignedEncryptedFrame;

  inline uint64_t &seq() { return get_val<0>(); }
};

// This class is used for encoding/decoding header of the message frame.
// Body is processed almost independently with the sole junction point
// being the `extra_payload_len` passed to get_buffer().
struct MessageHeaderFrame
    : public PayloadFrame<MessageHeaderFrame, ceph_msg_header2> {
  const ProtocolV2::Tag tag = ProtocolV2::Tag::MESSAGE;

  MessageHeaderFrame(ProtocolV2 &protocol,
		     const ceph_msg_header2 &msghdr,
		     ceph::bufferlist&& front_bl,
		     ceph::bufferlist&& middle_bl,
		     ceph::bufferlist&& data_bl)
      : PayloadFrame<MessageHeaderFrame, ceph_msg_header2>(msghdr) {
    ceph_assert(protocol.session_stream_handlers.tx);

    protocol.session_stream_handlers.tx->reset_tx_handler({
      8,
      this->payload.length(),
      front_bl.length(),
      middle_bl.length(),
      data_bl.length()
    });

    ceph::bufferlist trans_bl;
    this->payload.splice(8, this->payload.length() - 8, &trans_bl);
    std::swap(trans_bl, this->payload);

    this->preamble_filler = protocol.session_stream_handlers.tx->reserve(8);

    protocol.session_stream_handlers.tx->authenticated_encrypt_update(
      std::move(this->payload));
    if (front_bl.length()) {
    protocol.session_stream_handlers.tx->authenticated_encrypt_update(
      std::move(front_bl));
    }
    if (middle_bl.length()) {
    protocol.session_stream_handlers.tx->authenticated_encrypt_update(
      std::move(middle_bl));
    }
    if (data_bl.length()) {
    protocol.session_stream_handlers.tx->authenticated_encrypt_update(
      std::move(data_bl));
    }

    this->payload = \
      protocol.session_stream_handlers.tx->authenticated_encrypt_final();
  }

  MessageHeaderFrame(ProtocolV2 &protocol, char *payload, uint32_t length)
      : PayloadFrame<MessageHeaderFrame, ceph_msg_header2>() {
    ceph::bufferlist bl;
    bl.push_back(buffer::create_static(length, payload));

    ceph::bufferlist plain_bl = \
      protocol.session_stream_handlers.rx->authenticated_decrypt_update(
        std::move(bl), 8);
    this->decode_frame(plain_bl.c_str(), plain_bl.length());
  }

  inline ceph_msg_header2 &header() { return get_val<0>(); }
};

ProtocolV2::ProtocolV2(AsyncConnection *connection)
    : Protocol(2, connection),
      temp_buffer(nullptr),
      state(NONE),
      peer_required_features(0),
      cookie(0),
      global_seq(0),
      connect_seq(0),
      peer_global_seq(0),
      message_seq(0),
      reconnecting(false),
      replacing(false),
      can_write(false),
      bannerExchangeCallback(nullptr),
      next_payload_len(0),
      keepalive(false) {
  temp_buffer = new char[4096];
}

ProtocolV2::~ProtocolV2() {
  delete[] temp_buffer;
}

void ProtocolV2::connect() {
  ldout(cct, 1) << __func__ << dendl;
  state = START_CONNECT;
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
  cookie = 0;
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
  if (state == CONNECTING) {
    auth_meta.authorizer.reset(nullptr);
  }

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
      state <= ACCEPTING_SESSION && !replacing) {
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

  if (state == ACCEPTING_SESSION && !reconnecting) {
    // if the connection is generating a new session and fails during this
    // process, we need to clean the cookie. Otherwise, the connection might
    // later try to reconnect to a non-existing session, and the other side
    // will force a SESSION_RESET, which will cause a drop of the out_queue.
    cookie = 0;
  }
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

  if (state != START_CONNECT &&
      state != CONNECTING &&
      state != WAIT &&
      state != ACCEPTING_SESSION /* due to connection race */) {
    // policy maybe empty when state is in accept
    if (connection->policy.server) {
      ldout(cct, 1) << __func__ << " server, going to standby" << dendl;
      state = STANDBY;
    } else {
      ldout(cct, 1) << __func__ << " initiating reconnect" << dendl;
      connect_seq++;
      global_seq = messenger->get_global_seq();
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

    global_seq = messenger->get_global_seq();
    state = START_CONNECT;
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
  if (m->empty_payload()) {
    ldout(cct, 20) << __func__ << " encoding features " << features << " " << m
                   << " " << *m << dendl;
  } else {
    ldout(cct, 20) << __func__ << " half-reencoding features " << features
                   << " " << m << " " << *m << dendl;
  }

  // encode and copy out of *m
  m->encode(features, messenger->crcflags);
}

void ProtocolV2::send_message(Message *m) {
  uint64_t f = connection->get_features();

  // TODO: Currently not all messages supports reencode like MOSDMap, so here
  // only let fast dispatch support messages prepare message
  bool can_fast_prepare = messenger->ms_can_fast_dispatch(m);
  if (can_fast_prepare) {
    prepare_send_message(f, m);
  }

  std::lock_guard<std::mutex> l(connection->write_lock);
  // "features" changes will change the payload encoding
  if (can_fast_prepare && (!can_write || connection->get_features() != f)) {
    // ensure the correctness of message encoding
    m->clear_payload();
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
      out_queue_entry_t{can_fast_prepare, m});
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
                           header.version,    header.front_len,
                           header.middle_len, 0,
                           header.data_len,   header.data_off,
                           ack_seq,           footer.front_crc,
                           footer.middle_crc, footer.data_crc,
                           footer.flags,      header.compat_version,
                           header.reserved,   0};

  if (messenger->crcflags & MSG_CRC_HEADER) {
    header2.header_crc =
        ceph_crc32c(0, (unsigned char *)&header2,
                    sizeof(header2) - sizeof(header2.header_crc));
  }

  MessageHeaderFrame message(*this, header2,
    ceph::bufferlist(m->get_payload()),
    ceph::bufferlist(m->get_middle()),
    ceph::bufferlist(m->get_data()));

  ldout(cct, 5) << __func__ << " sending message m=" << m
                << " seq=" << m->get_seq() << " " << *m << dendl;

  connection->outcoming_bl.claim_append(message.get_buffer());

  m->trace.event("async writing message");
  ldout(cct, 20) << __func__ << " sending m=" << m << " seq=" << m->get_seq()
                 << " src=" << entity_name_t(messenger->get_myname())
                 << " front=" << header2.front_len
                 << " data=" << header2.data_len << " off=" << header2.data_off
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
  KeepAliveFrame keepalive_frame(*this);
  connection->outcoming_bl.claim_append(keepalive_frame.get_buffer());
}

void ProtocolV2::append_keepalive_ack(utime_t &timestamp) {
  KeepAliveFrameAck keepalive_ack_frame(*this, timestamp);
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
    connection->write_lock.unlock();

    // if r > 0 mean data still lefted, so no need _try_send.
    if (r == 0) {
      uint64_t left = ack_left;
      if (left) {
        ceph_le64 s;
        s = in_seq;
        AckFrame ack(*this, in_seq);
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
      if (cookie) {  // only increment connect_seq if there is a session
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

uint32_t ProtocolV2::calculate_payload_size(
  AuthStreamHandler *stream_handler,
  uint32_t length)
{
  // FIXME: we need add rounding to AES_BLOCK_LEN. AES-GCM is stream cipher
  // and doesn't really need this but other ciphers could do.
  return length;
}

void ProtocolV2::authencrypt_payload(bufferlist &payload) {
  if (auth_meta.is_mode_secure()) {
    // using tx
    ceph_assert(session_security.tx);
    session_security.tx->authenticated_encrypt(payload);
    ceph_assert(payload.length() > 0);
  }
}

void ProtocolV2::authdecrypt_payload(char *payload, uint32_t &length) {
  if (auth_meta.is_mode_secure()) {
    ceph_assert(session_security.rx);
    // using rx
    ceph_assert(length > 0);
    session_security.rx->authenticated_decrypt(payload, length);
  }
}

CtPtr ProtocolV2::read(CONTINUATION_PARAM(next, ProtocolV2, char *, int),
                       int len, char *buffer) {
  if (!buffer) {
    buffer = temp_buffer;
  }
  ssize_t r = connection->read(len, buffer,
                               [CONTINUATION(next), this](char *buffer, int r) {
                                 CONTINUATION(next)->setParams(buffer, r);
                                 run_continuation(CONTINUATION(next));
                               });
  if (r <= 0) {
    return CONTINUE(next, buffer, r);
  }

  return nullptr;
}

CtPtr ProtocolV2::write(const std::string &desc,
                        CONTINUATION_PARAM(next, ProtocolV2),
                        bufferlist &buffer) {
  ssize_t r =
      connection->write(buffer, [CONTINUATION(next), desc, this](int r) {
        if (r < 0) {
          ldout(cct, 1) << __func__ << " " << desc << " write failed r=" << r
                        << " (" << cpp_strerror(r) << ")" << dendl;
          connection->inject_delay();
          _fault();
        }
        run_continuation(CONTINUATION(next));
      });

  if (r <= 0) {
    if (r < 0) {
      ldout(cct, 1) << __func__ << " " << desc << " write failed r=" << r
                    << " (" << cpp_strerror(r) << ")" << dendl;
      return _fault();
    }
    return CONTINUE(next);
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
  __le16 banner_payload_len = sizeof(uint8_t) + 2 * sizeof(__le64);
  size_t banner_len = banner_prefix_len + sizeof(__le16) + banner_payload_len;
  char banner[banner_len];
  uint8_t offset = 0;
  memcpy(banner, CEPH_BANNER_V2_PREFIX, banner_prefix_len);
  offset += banner_prefix_len;
  memcpy(banner + offset, (void *)&banner_payload_len, sizeof(__le16));
  offset += sizeof(__le16);
  memcpy(banner + offset, (void *)&type, sizeof(uint8_t));
  offset += sizeof(uint8_t);
  memcpy(banner + offset, (void *)&supported_features, sizeof(__le64));
  offset += sizeof(__le64);
  memcpy(banner + offset, (void *)&required_features, sizeof(__le64));

  bufferlist bl;
  bl.append(banner, banner_len);

  return WRITE(bl, "banner", _wait_for_peer_banner);
}

CtPtr ProtocolV2::_wait_for_peer_banner() {
  unsigned banner_len = strlen(CEPH_BANNER_V2_PREFIX) + sizeof(__le16) +
                        sizeof(uint8_t) + 2 * sizeof(__le64);
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
  __le16 banner_payload_len = *(__le16 *)(buffer + offset);

  // V2 banner len check
  if (banner_payload_len != (sizeof(uint8_t) + 2 * sizeof(__le64))) {
    lderr(cct) << __func__ << " bad banner length: " << banner_payload_len
               << dendl;
    return _fault();
  }
  offset += sizeof(__le16);

  peer_type = *(uint8_t *)(buffer + offset);
  offset += sizeof(uint8_t);
  peer_supported_features = *(__le64 *)(buffer + offset);
  offset += sizeof(__le64);
  peer_required_features = *(__le64 *)(buffer + offset);

  if (connection->get_peer_type() == -1) {
    connection->set_peer_type(peer_type);

    ceph_assert(state == ACCEPTING);
    connection->policy = messenger->get_policy(peer_type);
    ldout(cct, 10) << __func__ << " accept of host_type " << (int)peer_type
                   << ", policy.lossy=" << connection->policy.lossy
                   << " policy.server=" << connection->policy.server
                   << " policy.standby=" << connection->policy.standby
                   << " policy.resetcheck=" << connection->policy.resetcheck
                   << dendl;
  } else {
    if (connection->get_peer_type() != peer_type) {
      ldout(cct, 1) << __func__ << " connection peer type does not match what"
                    << " peer advertises " << connection->get_peer_type()
                    << " != " << (int)peer_type << dendl;
      stop();
      connection->dispatch_queue->queue_reset(connection);
      return nullptr;
    }
  }

  ldout(cct, 1) << __func__ << " peer_type=" << (int)peer_type
                << " supported=" << std::hex << peer_supported_features
                << " required=" << std::hex << peer_required_features
                << std::dec << dendl;

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

  if (session_stream_handlers.rx) {
    session_stream_handlers.rx->reset_rx_handler();
  }

  bufferlist bl;
  bl.push_back(buffer::create_static(sizeof(uint32_t) * 2, buffer));
  try {
    auto ti = bl.cbegin();
    uint32_t frame_len;
    decode(frame_len, ti);
    next_payload_len = frame_len - sizeof(uint32_t);
    uint32_t tag;
    decode(tag, ti);
    next_tag = static_cast<Tag>(tag);
  } catch (const buffer::error &e) {
    lderr(cct) << __func__ << " failed decoding of frame header: " << e
               << dendl;
    return _fault();
  }

  ldout(cct, 10) << __func__ << " next payload_len=" << next_payload_len
                 << " tag=" << static_cast<uint32_t>(next_tag) << dendl;

  switch (next_tag) {
    case Tag::AUTH_REQUEST:
    case Tag::AUTH_BAD_METHOD:
    case Tag::AUTH_REPLY_MORE:
    case Tag::AUTH_REQUEST_MORE:
    case Tag::AUTH_DONE:
    case Tag::IDENT:
    case Tag::IDENT_MISSING_FEATURES:
    case Tag::SESSION_RECONNECT:
    case Tag::SESSION_RETRY:
    case Tag::SESSION_RETRY_GLOBAL:
    case Tag::SESSION_RECONNECT_OK:
    case Tag::KEEPALIVE2:
    case Tag::KEEPALIVE2_ACK:
    case Tag::ACK:
      return READ(next_payload_len, handle_frame_payload);
    case Tag::SESSION_RESET:
      return handle_session_reset();
    case Tag::WAIT:
      return handle_wait();
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

CtPtr ProtocolV2::handle_frame_payload(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read frame payload failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  ldout(cct, 30) << __func__ << "\n";
  bufferlist bl;
  bl.append(buffer, next_payload_len);
  bl.hexdump(*_dout);
  *_dout << dendl;

  switch (next_tag) {
    case Tag::AUTH_REQUEST:
      return handle_auth_request(buffer, next_payload_len);
    case Tag::AUTH_BAD_METHOD:
      return handle_auth_bad_method(buffer, next_payload_len);
    case Tag::AUTH_REPLY_MORE:
      return handle_auth_reply_more(buffer, next_payload_len);
    case Tag::AUTH_REQUEST_MORE:
      return handle_auth_request_more(buffer, next_payload_len);
    case Tag::AUTH_DONE:
      return handle_auth_done(buffer, next_payload_len);
    case Tag::IDENT:
      return handle_ident(buffer, next_payload_len);
    case Tag::IDENT_MISSING_FEATURES:
      return handle_ident_missing_features(buffer, next_payload_len);
    case Tag::SESSION_RECONNECT:
      return handle_reconnect(buffer, next_payload_len);
    case Tag::SESSION_RETRY:
      return handle_session_retry(buffer, next_payload_len);
    case Tag::SESSION_RETRY_GLOBAL:
      return handle_session_retry_global(buffer, next_payload_len);
    case Tag::SESSION_RECONNECT_OK:
      return handle_reconnect_ok(buffer, next_payload_len);
    case Tag::KEEPALIVE2:
      return handle_keepalive2(buffer, next_payload_len);
    case Tag::KEEPALIVE2_ACK:
      return handle_keepalive2_ack(buffer, next_payload_len);
    case Tag::ACK:
      return handle_message_ack(buffer, next_payload_len);
    default:
      ceph_abort();
  }
  return nullptr;
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
  ldout(cct, 1) << __func__ << " entity=" << peer_name << " cookie=" << std::hex
                << cookie << std::dec << " in_seq=" << in_seq
                << " out_seq=" << out_seq << dendl;

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_message() {
  ldout(cct, 20) << __func__ << dendl;

  ceph_assert(state == READY);

#if defined(WITH_LTTNG) && defined(WITH_EVENTTRACE)
  ltt_recv_stamp = ceph_clock_now();
#endif
  recv_stamp = ceph_clock_now();

  const uint32_t header_len = calculate_payload_size(
    session_security.rx.get(), sizeof(ceph_msg_header2));
  return READ(header_len, handle_message_header);
}

CtPtr ProtocolV2::handle_message_header(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read message header failed" << dendl;
    return _fault();
  }

  const uint32_t header_len = calculate_payload_size(
    session_security.rx.get(), sizeof(ceph_msg_header2));

  MessageHeaderFrame header_frame(*this, buffer, header_len);
  ceph_msg_header2 &header = header_frame.header();

  ldout(cct, 20) << __func__ << " got envelope type=" << header.type << " src "
                 << peer_name << " front=" << header.front_len
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
  extra.clear();
  current_header = header;

  next_payload_len -= header_len;

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

  next_payload_len -= current_header.front_len;

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

  next_payload_len -= current_header.middle_len;

  return read_message_data_prepare();
}

CtPtr ProtocolV2::read_message_data_prepare() {
  ldout(cct, 20) << __func__ << dendl;

  unsigned data_len = le32_to_cpu(current_header.data_len);
  unsigned data_off = le32_to_cpu(current_header.data_off);

  if (data_len) {
    // get a buffer
    map<ceph_tid_t, pair<bufferlist, int>>::iterator p =
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

  next_payload_len -= current_header.data_len;
  if (next_payload_len) {
    // if we still have more bytes to read is because we signed or encrypted
    // the message payload
    ldout(cct, 1) << __func__ << " reading message payload extra bytes left="
                  << next_payload_len << dendl;
    ceph_assert(session_security.rx && session_security.tx &&
		session_stream_handlers.rx && session_stream_handlers.tx &&
		auth_meta.is_mode_secure());
    extra.push_back(buffer::create(next_payload_len));
    return READB(next_payload_len, extra.c_str(), handle_message_extra_bytes);
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

CtPtr ProtocolV2::handle_message_extra_bytes(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read message extra bytes error " << dendl;
    return _fault();
  }

  state = READ_MESSAGE_COMPLETE;
  return handle_message_complete();
}

CtPtr ProtocolV2::handle_message_complete() {
  ldout(cct, 20) << __func__ << dendl;

  ldout(cct, 5) << __func__ << " got " << front.length() << " + "
                << middle.length() << " + " << data.length() << " byte message"
                << dendl;

  ceph_msg_header header{current_header.seq,
                         current_header.tid,
                         current_header.type,
                         current_header.priority,
                         current_header.version,
                         current_header.front_len,
                         current_header.middle_len,
                         current_header.data_len,
                         current_header.data_off,
                         peer_name,
                         current_header.compat_version,
                         current_header.reserved,
                         0};
  ceph_msg_footer footer{current_header.front_crc, current_header.middle_crc,
                         current_header.data_crc, 0, current_header.flags};

  if (auth_meta.is_mode_secure()) {
    //msg_payload.claim_append(extra);

    if (front.length()) {
      front = session_stream_handlers.rx->authenticated_decrypt_update(
        std::move(front), 8);
    }
    if (middle.length()) {
      middle = session_stream_handlers.rx->authenticated_decrypt_update(
        std::move(middle), 8);
    }
    if (data.length()) {
      data = session_stream_handlers.rx->authenticated_decrypt_update(
        std::move(data), 8);
    }
    session_stream_handlers.rx->authenticated_decrypt_update_final(
      std::move(extra), 8);
  }

  Message *message = decode_message(cct, messenger->crcflags, header, footer,
                                    front, middle, data, connection);
  if (!message) {
    ldout(cct, 1) << __func__ << " decode message failed " << dendl;
    return _fault();
  }

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
  extra.clear();

  // we might have been reused by another connection
  // let's check if that is the case
  if (state != READY) {
    // yes, that was the case, let's do nothing
    return nullptr;
  }

  if (need_dispatch_writer && connection->is_connected()) {
    connection->center->dispatch_event_external(connection->write_handler);
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_keepalive2(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  KeepAliveFrame keepalive_frame(*this, payload, length);

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

CtPtr ProtocolV2::handle_keepalive2_ack(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  KeepAliveFrameAck keepalive_ack_frame(*this, payload, length);
  connection->set_last_keepalive_ack(keepalive_ack_frame.timestamp());
  ldout(cct, 20) << __func__ << " got KEEPALIVE_ACK" << dendl;

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_message_ack(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  AckFrame ack(*this, payload, length);
  handle_message_ack(ack.seq());
  return CONTINUE(read_frame);
}

/* Client Protocol Methods */

CtPtr ProtocolV2::start_client_banner_exchange() {
  ldout(cct, 20) << __func__ << dendl;
  state = CONNECTING;

  global_seq = messenger->get_global_seq();

  return _banner_exchange(CONTINUATION(post_client_banner_exchange));
}

CtPtr ProtocolV2::post_client_banner_exchange() {
  ldout(cct, 20) << __func__ << dendl;

  // at this point we can change how the client protocol behaves based on
  // this->peer_required_features

  return send_auth_request();
}

CtPtr ProtocolV2::send_auth_request(std::vector<uint32_t> &allowed_methods) {
  ldout(cct, 20) << __func__ << " peer_type " << (int)connection->peer_type
		 << " auth_client " << messenger->auth_client << dendl;
  ceph_assert(messenger->auth_client);

  bufferlist bl;
  vector<uint32_t> preferred_modes;
  connection->lock.unlock();
  int r = messenger->auth_client->get_auth_request(
    connection, &auth_meta.auth_method, &preferred_modes, &bl);
  connection->lock.lock();
  if (state != State::CONNECTING) {
    return _fault();
  }
  if (r < 0) {
    ldout(cct, 0) << __func__ << " get_initial_auth_request returned " << r
		  << dendl;
    stop();
    connection->dispatch_queue->queue_reset(connection);
    return nullptr;
  }
  AuthRequestFrame frame(auth_meta.auth_method, preferred_modes, bl);
  return WRITE(frame.get_buffer(), "auth request", read_frame);
}

CtPtr ProtocolV2::handle_auth_bad_method(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  AuthBadMethodFrame bad_method(payload, length);
  ldout(cct, 1) << __func__ << " method=" << bad_method.method()
		<< " result " << cpp_strerror(bad_method.result())
                << ", allowed methods=" << bad_method.allowed_methods()
		<< ", allowed modes=" << bad_method.allowed_modes()
                << dendl;
  ceph_assert(messenger->auth_client);
  connection->lock.unlock();
  int r = messenger->auth_client->handle_auth_bad_method(
    connection, bad_method.method(), bad_method.result(),
    bad_method.allowed_methods(),
    bad_method.allowed_modes());
  connection->lock.lock();
  if (state != State::CONNECTING || r < 0) {
    return _fault();
  }
  return send_auth_request(bad_method.allowed_methods());
}

CtPtr ProtocolV2::handle_auth_reply_more(char *payload, uint32_t length)
{
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;
  AuthReplyMoreFrame auth_more(payload, length);
  ldout(cct, 5) << __func__
                << " auth reply more len=" << auth_more.auth_payload().length()
                << dendl;
  ceph_assert(messenger->auth_client);
  bufferlist bl;
  bl.append(payload, length);
  bufferlist reply;
  connection->lock.unlock();
  int r = messenger->auth_client->handle_auth_reply_more(
    connection, auth_more.auth_payload(), &reply);
  connection->lock.lock();
  if (state != State::CONNECTING) {
    return _fault();
  }
  if (r < 0) {
    lderr(cct) << __func__ << " auth_client handle_auth_reply_more returned "
	       << r << dendl;
    return _fault();
  }
  AuthRequestMoreFrame more_reply(reply);
  return WRITE(more_reply.get_buffer(), "auth request more", read_frame);
}

CtPtr ProtocolV2::handle_auth_done(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  AuthDoneFrame auth_done(payload, length);

  ceph_assert(messenger->auth_client);
  connection->lock.unlock();
  int r = messenger->auth_client->handle_auth_done(
    connection,
    auth_done.global_id(),
    auth_done.con_mode(),
    auth_done.auth_payload(),
    &auth_meta.session_key,
    &auth_meta.connection_secret);
  connection->lock.lock();
  if (state != State::CONNECTING) {
    return _fault();
  }
  if (r < 0) {
    return _fault();
  }
  auth_meta.con_mode = auth_done.con_mode();
  session_security =
    AuthStreamHandler::create_stream_handler_pair(cct, auth_meta);
  session_stream_handlers =
    ceph::crypto::onwire::rxtx_t::create_handler_pair(cct, auth_meta, false);

  if (!cookie) {
    ceph_assert(connect_seq == 0);
    return send_client_ident();
  } else {  // reconnecting to previous session
    ceph_assert(connect_seq > 0);
    return send_reconnect();
  }
}

CtPtr ProtocolV2::send_client_ident() {
  ldout(cct, 20) << __func__ << dendl;

  uint64_t flags = 0;
  if (connection->policy.lossy) {
    flags |= CEPH_MSG_CONNECT_LOSSY;
  }

  if (messenger->get_myaddrs().empty() ||
      messenger->get_myaddrs().front().is_blank_ip()) {
    sockaddr_storage ss;
    socklen_t len = sizeof(ss);
    getsockname(connection->cs.fd(), (sockaddr*)&ss, &len);
    ldout(cct,1) << __func__ << " getsockname reveals I am " << (sockaddr*)&ss
		 << " when talking to " << connection->target_addr << dendl;
    entity_addr_t a;
    a.set_type(connection->target_addr.get_type());
    a.set_sockaddr((sockaddr*)&ss);
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
    if (state != CONNECTING) {
      ldout(cct, 1) << __func__
		    << " state changed while learned_addr, mark_down or "
		    << " replacing must be happened just now" << dendl;
      return nullptr;
    }
  }

  ClientIdentFrame client_ident(*this, messenger->get_myaddrs(),
                                messenger->get_myname().num(), global_seq,
                                connection->policy.features_supported,
                                connection->policy.features_required, flags);

  ldout(cct, 5) << __func__ << " sending identification: "
                << "addrs=" << messenger->get_myaddrs()
                << " gid=" << messenger->get_myname().num()
                << " global_seq=" << global_seq
                << " features_supported=" << std::hex
                << connection->policy.features_supported
                << " features_required=" << connection->policy.features_required
                << " flags=" << flags << std::dec << dendl;

  bufferlist &bl = client_ident.get_buffer();
  return WRITE(bl, "client ident", read_frame);
}

CtPtr ProtocolV2::send_reconnect() {
  ldout(cct, 20) << __func__ << dendl;

  ReconnectFrame reconnect(*this, messenger->get_myaddrs(), cookie, global_seq,
                           connect_seq, in_seq);

  ldout(cct, 5) << __func__ << " reconnect to session: cookie=" << cookie
                << " gs=" << global_seq << " cs=" << connect_seq
                << " ms=" << in_seq << dendl;
  bufferlist &bl = reconnect.get_buffer();
  return WRITE(bl, "reconnect", read_frame);
}

CtPtr ProtocolV2::handle_ident_missing_features(char *payload,
                                                uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  IdentMissingFeaturesFrame ident_missing(*this, payload, length);
  lderr(cct) << __func__
             << " client does not support all server features: " << std::hex
             << ident_missing.features() << std::dec << dendl;

  return _fault();
}

CtPtr ProtocolV2::handle_session_reset() {
  ldout(cct, 20) << __func__ << dendl;

  ldout(cct, 1) << __func__ << " received session reset" << dendl;
  reset_session();

  return send_client_ident();
}

CtPtr ProtocolV2::handle_session_retry(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  RetryFrame retry(*this, payload, length);
  connect_seq = retry.connect_seq() + 1;

  ldout(cct, 1) << __func__
                << " received session retry connect_seq=" << retry.connect_seq()
                << ", inc to cs=" << connect_seq << dendl;

  return send_reconnect();
}

CtPtr ProtocolV2::handle_session_retry_global(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  RetryGlobalFrame retry(*this, payload, length);
  global_seq = messenger->get_global_seq(retry.global_seq());

  ldout(cct, 1) << __func__ << " received session retry global global_seq="
                << retry.global_seq() << ", choose new gs=" << global_seq
                << dendl;

  return send_reconnect();
}

CtPtr ProtocolV2::handle_wait() {
  ldout(cct, 20) << __func__ << dendl;
  ldout(cct, 1) << __func__ << " received WAIT (connection race)" << dendl;
  state = WAIT;
  return _fault();
}

CtPtr ProtocolV2::handle_reconnect_ok(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  ReconnectOkFrame reconnect_ok(*this, payload, length);
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

CtPtr ProtocolV2::handle_server_ident(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;

  ServerIdentFrame server_ident(*this, payload, length);
  ldout(cct, 5) << __func__ << " received server identification:"
                << " addrs=" << server_ident.addrs()
                << " my_addr=" << server_ident.peer_addr()
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

  cookie = server_ident.cookie();

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
  AuthRequestFrame request(payload, length);
  ldout(cct, 10) << __func__ << " AuthRequest(method=" << request.method()
		 << ", preferred_modes=" << request.preferred_modes()
                 << ", payload_len=" << request.auth_payload().length() << ")"
                 << dendl;
  auth_meta.auth_method = request.method();

  // select a connection mode
  auto& preferred_modes = request.preferred_modes();
  std::vector<uint32_t> allowed_modes;
  messenger->auth_server->get_supported_con_modes(
    connection->get_peer_type(), auth_meta.auth_method, &allowed_modes);
  for (auto mode : allowed_modes) {
    if (std::find(preferred_modes.begin(), preferred_modes.end(), mode)
	!= preferred_modes.end()) {
      auth_meta.con_mode = mode;
      break;
    }
  }
  if (auth_meta.con_mode == CEPH_CON_MODE_UNKNOWN) {
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
  ldout(cct, 1) << __func__ << " auth_method " << auth_meta.auth_method
		<< " r " << cpp_strerror(r)
		<< ", allowed_methods " << allowed_methods
		<< ", allowed_modes " << allowed_modes
		<< dendl;
  AuthBadMethodFrame bad_method(auth_meta.auth_method, r, allowed_methods,
				allowed_modes);
  return WRITE(bad_method.get_buffer(), "bad auth method", read_frame);
}

CtPtr ProtocolV2::_handle_auth_request(bufferlist& auth_payload, bool more)
{
  if (!messenger->auth_server) {
    return _fault();
  }
  bufferlist reply;
  connection->lock.unlock();
  int r = messenger->auth_server->handle_auth_request(
    connection, more, auth_meta.auth_method, auth_payload,
    &reply);
  connection->lock.lock();
  if (state != ACCEPTING) {
    ldout(cct, 1) << __func__
		  << " state changed while accept, it must be mark_down"
		  << dendl;
    ceph_assert(state == CLOSED);
    return _fault();
  }
  if (r == 1) {
    session_security =
      AuthStreamHandler::create_stream_handler_pair(cct, auth_meta);
    std::swap(session_security.rx, session_security.tx);
    session_stream_handlers = \
      ceph::crypto::onwire::rxtx_t::create_handler_pair(cct, auth_meta, true);
    AuthDoneFrame auth_done(connection->peer_global_id, auth_meta.con_mode,
			    reply);
    return WRITE(auth_done.get_buffer(), "auth done", read_frame);
  } else if (r == 0) {
    AuthReplyMoreFrame more(reply);
    return WRITE(more.get_buffer(), "auth reply more", read_frame);
  } else if (r == -EBUSY) {
    // kick the client and maybe they'll come back later
    return _fault();
  } else {
    return _auth_bad_method(r);
  }
}

CtPtr ProtocolV2::handle_auth_request_more(char *payload, uint32_t length)
{
  ldout(cct, 20) << __func__ << " payload_len=" << length << dendl;
  AuthRequestMoreFrame auth_more(payload, length);
  return _handle_auth_request(auth_more.auth_payload(), true);
}

CtPtr ProtocolV2::handle_client_ident(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << std::dec << length << dendl;

  ClientIdentFrame client_ident(*this, payload, length);

  ldout(cct, 5) << __func__ << " received client identification: "
                << "addrs=" << client_ident.addrs()
                << " gid=" << client_ident.gid()
                << " global_seq=" << client_ident.global_seq()
                << " features_supported=" << std::hex
                << client_ident.supported_features()
                << " features_required=" << client_ident.required_features()
                << " flags=" << client_ident.flags() << std::dec << dendl;
  if (client_ident.addrs().empty()) {
    return _fault();  // a v2 peer should never do this
  }
  entity_addr_t peer_addr = client_ident.addrs().msgr2_addr();
  if (peer_addr.type == entity_addr_t::TYPE_NONE) {
    // no v2 addr!  they must be a client
    if (client_ident.addrs().v.size() > 1) {
      lderr(cct) << __func__ << " rejecting addrvec with >1 addr but no msgr2: " << client_ident.addrs() << dendl;
      return _fault();
    }
    peer_addr = client_ident.addrs().legacy_addr();
    peer_addr.set_type(entity_addr_t::TYPE_MSGR2);
    entity_addrvec_t addrs;
    addrs.v.push_back(peer_addr);
    connection->set_peer_addrs(addrs);
  } else {
    connection->set_peer_addrs(client_ident.addrs());
  }
  connection->target_addr = peer_addr;

  peer_name = entity_name_t(connection->get_peer_type(), client_ident.gid());
  connection->set_peer_id(client_ident.gid());

  uint64_t feat_missing = connection->policy.features_required &
                          ~(uint64_t)client_ident.supported_features();
  if (feat_missing) {
    ldout(cct, 1) << __func__ << " peer missing required features " << std::hex
                  << feat_missing << std::dec << dendl;
    IdentMissingFeaturesFrame ident_missing_features(*this, feat_missing);

    bufferlist &bl = ident_missing_features.get_buffer();
    return WRITE(bl, "ident missing features", read_frame);
  }

  connection_features =
      client_ident.supported_features() & connection->policy.features_supported;

  state = ACCEPTING_SESSION;
  peer_global_seq = client_ident.global_seq();

  // Looks good so far, let's check if there is already an existing connection
  // to this peer.

  connection->lock.unlock();
  AsyncConnectionRef existing = messenger->lookup_conn(*connection->peer_addrs);

  connection->inject_delay();

  connection->lock.lock();
  if (state != ACCEPTING_SESSION) {
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

CtPtr ProtocolV2::handle_reconnect(char *payload, uint32_t length) {
  ldout(cct, 20) << __func__ << " payload_len=" << std::dec << length << dendl;

  ReconnectFrame reconnect(*this, payload, length);

  ldout(cct, 5) << __func__
                << " received reconnect: cookie=" << reconnect.cookie()
                << " gs=" << reconnect.global_seq()
                << " cs=" << reconnect.connect_seq()
                << " ms=" << reconnect.msg_seq() << dendl;

  if (reconnect.addrs().empty()) {
    connection->set_peer_addr(connection->target_addr);
  } else {
    // Should we check if one of the ident.addrs match connection->target_addr
    // as we do in ProtocolV1?
    connection->set_peer_addrs(reconnect.addrs());
    connection->target_addr = reconnect.addrs().msgr2_addr();
  }

  connection->lock.unlock();
  AsyncConnectionRef existing = messenger->lookup_conn(*connection->peer_addrs);

  connection->inject_delay();

  connection->lock.lock();
  if (state != ACCEPTING) {
    ldout(cct, 1) << __func__
                  << " state changed while accept, it must be mark_down"
                  << dendl;
    ceph_assert(state == CLOSED);
    return _fault();
  }

  ResetFrame reset;
  bufferlist &bl = reset.get_buffer();

  if (!existing) {
    // there is no existing connection therefore cannot reconnect to previous
    // session
    ldout(cct, 0) << __func__
                  << " no existing connection exists, reseting client" << dendl;
    return WRITE(bl, "session reset", read_frame);
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
    return WRITE(bl, "session reset", read_frame);
  }

  if (exproto->replacing) {
    ldout(cct, 1) << __func__
                  << " existing racing replace happened while replacing."
                  << " existing=" << existing << dendl;
    RetryGlobalFrame retry(*this, exproto->peer_global_seq);
    bufferlist &bl = retry.get_buffer();
    return WRITE(bl, "session retry", read_frame);
  }

  if (!exproto->cookie) {
    // server connection was reseted, reset client
    ldout(cct, 5) << __func__ << " no cookie set, reseting client" << dendl;
    return WRITE(bl, "session reset", read_frame);
  } else if (exproto->cookie != reconnect.cookie()) {
    ldout(cct, 5) << __func__ << " cookie mismatch sc=" << exproto->cookie
                  << " cc=" << reconnect.cookie() << ", reseting client"
                  << dendl;
    return WRITE(bl, "session reset", read_frame);
  }

  if (exproto->peer_global_seq > reconnect.global_seq()) {
    ldout(cct, 5) << __func__
                  << " stale global_seq: sgs=" << exproto->peer_global_seq
                  << " cgs=" << reconnect.global_seq()
                  << ", ask client to retry global" << dendl;
    RetryGlobalFrame retry(*this, exproto->peer_global_seq);
    bufferlist &bl = retry.get_buffer();
    return WRITE(bl, "session retry", read_frame);
  }

  if (exproto->connect_seq >= reconnect.connect_seq()) {
    ldout(cct, 5) << __func__
                  << " stale connect_seq scs=" << exproto->connect_seq
                  << " ccs=" << reconnect.connect_seq()
                  << " , ask client to retry" << dendl;
    RetryFrame retry(*this, exproto->connect_seq);
    bufferlist &bl = retry.get_buffer();
    return WRITE(bl, "session retry", read_frame);
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
    WaitFrame wait;
    return WRITE(wait.get_buffer(), "wait", read_frame);
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

  if (exproto->cookie) {
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

    existing->lock.unlock();
    // make sure we follow through with opening the existing
		// connection (if it isn't yet open) since we know the peer
		// has something to send to us.
    existing->send_keepalive();
    existing->lock.lock();
    WaitFrame wait;
    bufferlist &bl = wait.get_buffer();
    return WRITE(bl, "wait", read_frame);
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
  if (!reconnecting) {
    exproto->peer_name = peer_name;
    exproto->peer_global_seq = peer_global_seq;
    exproto->connection_features = connection_features;
  }

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
  std::swap(exproto->session_security, session_security);
  std::swap(exproto->session_stream_handlers, session_stream_handlers);
  exproto->auth_meta.con_mode = auth_meta.con_mode;
  exproto->auth_meta.auth_method = auth_meta.auth_method;
  exproto->auth_meta.session_key = auth_meta.session_key;
  exproto->auth_meta.connection_secret = auth_meta.connection_secret;
  exproto->auth_meta.authorizer_challenge = std::move(auth_meta.authorizer_challenge);
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

          exproto->state = ACCEPTING_SESSION;
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
    cookie = ceph::util::generate_random_number<uint64_t>(0, -1ll);
  }

  uint64_t flags = 0;
  if (connection->policy.lossy) {
    flags = flags | CEPH_MSG_CONNECT_LOSSY;
  }

  uint64_t gs = messenger->get_global_seq();
  ServerIdentFrame server_ident(
      *this, messenger->get_myaddrs(), connection->target_addr,
      messenger->get_myname().num(), gs, connection->policy.features_supported,
      connection->policy.features_required, flags, cookie);

  ldout(cct, 5) << __func__ << " sending identification:"
                << " addrs=" << messenger->get_myaddrs()
                << " peer_addr=" << connection->target_addr
                << " gid=" << messenger->get_myname().num()
                << " global_seq=" << gs << " features_supported=" << std::hex
                << connection->policy.features_supported
                << " features_required=" << connection->policy.features_required
                << " flags=" << flags << " cookie=" << std::dec << cookie
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
  if (state != ACCEPTING_SESSION) {
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

  bufferlist &bl = server_ident.get_buffer();
  return WRITE(bl, "server ident", server_ready);
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
  ReconnectOkFrame reconnect_ok(*this, ms);

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
  if (state != ACCEPTING_SESSION) {
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

  bufferlist &bl = reconnect_ok.get_buffer();
  return WRITE(bl, "reconnect ok", server_ready);
}
