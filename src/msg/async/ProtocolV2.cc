// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ProtocolV2.h"
#include "AsyncMessenger.h"

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

#define WRITE(B, C) write(CONTINUATION(C), B)

#define READ(L, C) read(CONTINUATION(C), L)

#define READB(L, B, C) read(CONTINUATION(C), L, B)

using CtPtr = Ct<ProtocolV2> *;

ProtocolV2::ProtocolV2(AsyncConnection *connection)
    : Protocol(2, connection),
      temp_buffer(nullptr),
      state(NONE),
      peer_required_features(0),
      cookie(0),
      bannerExchangeCallback(nullptr),
      next_frame_len(0) {
  temp_buffer = new char[4096];
}

ProtocolV2::~ProtocolV2() { delete[] temp_buffer; }

void ProtocolV2::connect() { state = START_CONNECT; }

void ProtocolV2::accept() { state = START_ACCEPT; }

bool ProtocolV2::is_connected() { return false; }

void ProtocolV2::stop() {
  ldout(cct, 2) << __func__ << dendl;
  if (state == CLOSED) {
    return;
  }

  if (connection->delay_state) connection->delay_state->flush();

  connection->_stop();

  state = CLOSED;
}

void ProtocolV2::fault() {
  ldout(cct, 10) << __func__ << dendl;

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
}

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
  ldout(cct, 20) << __func__ << dendl;
  return READ(sizeof(__le32), handle_read_frame_length);
}

CtPtr ProtocolV2::handle_read_frame_length(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read frame length failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  next_frame_len = *(__le32 *)buffer;

  return READ(next_frame_len, handle_frame);
}

CtPtr ProtocolV2::handle_frame(char *buffer, int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " read frame payload failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  Tag tag = static_cast<Tag>(*(__le32 *)buffer);
  buffer += sizeof(__le32);
  uint32_t payload_len = next_frame_len - sizeof(__le32);

  ldout(cct, 10) << __func__ << " tag=" << static_cast<uint32_t>(tag) << dendl;

  switch (tag) {
    case Tag::AUTH_REQUEST:
      return handle_auth_request(buffer, payload_len);
    case Tag::AUTH_BAD_METHOD:
      return handle_auth_bad_method(buffer, payload_len);
    case Tag::AUTH_BAD_AUTH:
      return handle_auth_bad_auth(buffer, payload_len);
    case Tag::AUTH_MORE:
      return handle_auth_more(buffer, payload_len);
    case Tag::AUTH_DONE:
      return handle_auth_done(buffer, payload_len);
    case Tag::IDENT:
      return handle_ident(buffer, payload_len);
    case Tag::IDENT_MISSING_FEATURES:
      return handle_ident_missing_features(buffer, payload_len);
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
    bufferlist bl = more.to_bufferlist();
    return WRITE(bl, handle_auth_more_write);
  }
  /* END TO REMOVE */

  AuthDoneFrame auth_done(0);

  auto bl = auth_done.to_bufferlist();
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

  bufferlist bl = authFrame.to_bufferlist();
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

  bufferlist bl = ident.to_bufferlist();
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

  state = READY;

  return CONTINUE(read_frame);
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
    bufferlist bl = bad_method.to_bufferlist();
    return WRITE(bl, handle_auth_bad_method_write);
  }

  ldout(cct, 10) << __func__ << " auth method=" << auth_request.method
                 << " accepted" << dendl;
  // verify authorization blob
  bool valid = i == 35;

  if (!valid) {
    AuthBadAuthFrame bad_auth(12, "Permission denied");
    bufferlist bl = bad_auth.to_bufferlist();
    return WRITE(bl, handle_auth_bad_auth_write);
  }

  bufferlist auth_bl;
  /* BEGIN TO REMOVE */
  encode((int32_t)45, auth_bl, 0);
  std::string hello("hello server more");
  encode(hello, auth_bl, 0);
  /* END TO REMOVE */
  AuthMoreFrame more(auth_bl);
  bufferlist bl = more.to_bufferlist();
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

    bufferlist bl;
    bl = ident_missing_features.to_bufferlist();
    return WRITE(bl, handle_ident_missing_features_write);
  }

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

  bufferlist bl = ident.to_bufferlist();
  return WRITE(bl, handle_send_server_ident_write);
}

CtPtr ProtocolV2::handle_ident_missing_features_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " ident missing features write failed r=" << r
                  << " (" << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  return CONTINUE(read_frame);
}

CtPtr ProtocolV2::handle_send_server_ident_write(int r) {
  ldout(cct, 20) << __func__ << " r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 1) << __func__ << " server ident write failed r=" << r << " ("
                  << cpp_strerror(r) << ")" << dendl;
    return _fault();
  }

  state = READY;

  return CONTINUE(read_frame);
}
