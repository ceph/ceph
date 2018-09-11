// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ProtocolV2.h"
#include "AsyncMessenger.h"

#include "common/errno.h"

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

#define WRITE(B, C) write(CONTINUATION(C), B)

#define READ(L, C) read(CONTINUATION(C), L)

#define READB(L, B, C) read(CONTINUATION(C), L, B)

using CtPtr = Ct<ProtocolV2> *;

ProtocolV2::ProtocolV2(AsyncConnection *connection)
    : Protocol(2, connection),
      temp_buffer(nullptr),
      state(NONE),
      bannerExchangeCallback(nullptr) {
  temp_buffer = new char[4096];
}

ProtocolV2::~ProtocolV2() { delete[] temp_buffer; }

void ProtocolV2::connect() { state = START_CONNECT; }

void ProtocolV2::accept() { state = START_ACCEPT; }

bool ProtocolV2::is_connected() { return false; }

void ProtocolV2::stop() {}

void ProtocolV2::fault() { _fault(); }

void ProtocolV2::send_message(Message *m) {}

void ProtocolV2::send_keepalive() {}

void ProtocolV2::read_event() {
  ldout(cct, 20) << __func__ << dendl;

  switch (state) {
    case START_CONNECT:
      CONTINUATION_RUN(CONTINUATION(start_client_banner_exchange));
      break;
    case START_ACCEPT:
      CONTINUATION_RUN(CONTINUATION(start_server_banner_exchange));
      break;
    default:
      break;
  }
}

void ProtocolV2::write_event() {}

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

  __u64 supported_features = CEPH_MSGR2_SUPPORTED_FEATURES;
  __u64 required_features = CEPH_MSGR2_REQUIRED_FEATURES;

  size_t banner_prefix_len = strlen(CEPH_BANNER_V2_PREFIX);
  size_t banner_len = banner_prefix_len + 2 * sizeof(__u64);
  char banner[banner_len];
  memcpy(banner, CEPH_BANNER_V2_PREFIX, banner_prefix_len);
  memcpy(banner + banner_prefix_len, (void *)&supported_features,
         sizeof(__u64));
  memcpy(banner + banner_prefix_len + sizeof(__u64), (void *)&required_features,
         sizeof(__u64));

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

  unsigned banner_len = strlen(CEPH_BANNER_V2_PREFIX) + 2 * sizeof(__u64);
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

  __u64 peer_supported_features;
  __u64 peer_required_features;

  peer_supported_features = *(__u64 *)(buffer + banner_prefix_len);
  peer_required_features =
      *(__u64 *)(buffer + banner_prefix_len + sizeof(__u64));

  ldout(cct, 1) << __func__ << " peer " << *connection->peer_addrs
                << " banner supported=" << std::hex << peer_supported_features
                << " required=" << std::hex << peer_required_features << dendl;

  // Check feature bit compatibility

  __u64 supported_features = CEPH_MSGR2_SUPPORTED_FEATURES;
  __u64 required_features = CEPH_MSGR2_REQUIRED_FEATURES;

  if ((required_features & peer_supported_features) != required_features) {
    ldout(cct, 1) << __func__ << " peer does not support all required features"
                  << " required=" << std::hex << required_features
                  << " supported=" << std::hex << peer_supported_features
                  << dendl;
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

  ceph_abort();
  return nullptr;
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

  ceph_abort();
  return nullptr;
}

