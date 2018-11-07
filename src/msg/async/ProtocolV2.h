// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _MSG_ASYNC_PROTOCOL_V2_
#define _MSG_ASYNC_PROTOCOL_V2_

#include "Protocol.h"

class ProtocolV2 : public Protocol {
private:
  enum State {
    NONE,
    START_CONNECT,
    CONNECTING,
    START_ACCEPT,
    ACCEPTING,
    ACCEPTED_CLIENT_IDENT,
    READY,
    THROTTLE_MESSAGE,
    THROTTLE_BYTES,
    THROTTLE_DISPATCH_QUEUE,
    READ_MESSAGE_FRONT,
    READ_MESSAGE_COMPLETE,
    STANDBY,
    WAIT,
    CLOSED
  };

  static const char *get_state_name(int state) {
    const char *const statenames[] = {"NONE",
                                      "START_CONNECT",
                                      "CONNECTING",
                                      "START_ACCEPT",
                                      "ACCEPTING",
                                      "ACCEPTED_CLIENT_IDENT",
                                      "READY",
                                      "THROTTLE_MESSAGE",
                                      "THROTTLE_BYTES",
                                      "THROTTLE_DISPATCH_QUEUE",
                                      "READ_MESSAGE_FRONT",
                                      "READ_MESSAGE_COMPLETE",
                                      "STANDBY",
                                      "WAIT",
                                      "CLOSED"};
    return statenames[state];
  }

public:
  enum class Tag : uint32_t {
    AUTH_REQUEST,
    AUTH_BAD_METHOD,
    AUTH_BAD_AUTH,
    AUTH_MORE,
    AUTH_DONE,
    IDENT,
    IDENT_MISSING_FEATURES,
    MESSAGE,
    KEEPALIVE2,
    KEEPALIVE2_ACK,
    ACK
  };

  struct Frame {
    uint32_t tag;
    bufferlist payload;
    bufferlist frame_buffer;

    Frame(Tag tag) : tag(static_cast<uint32_t>(tag)) {
      encode(this->tag, payload, 0);
    }

    Frame() {}

    bufferlist &get_buffer() {
      if (frame_buffer.length()) {
        return frame_buffer;
      }
      encode((uint32_t)payload.length(), frame_buffer, 0);
      frame_buffer.claim_append(payload);
      return frame_buffer;
    }
  };

  struct SignedEncryptedFrame : public Frame {
    SignedEncryptedFrame(Tag tag) : Frame(tag) {}
    SignedEncryptedFrame() : Frame() {}
    bufferlist &get_buffer() { return Frame::get_buffer(); }
  };

  struct AuthRequestFrame : public Frame {
    uint32_t method;
    uint32_t len;
    bufferlist auth_payload;

    AuthRequestFrame(uint32_t method, bufferlist &auth_payload)
        : Frame(Tag::AUTH_REQUEST),
          method(method),
          len(auth_payload.length()),
          auth_payload(auth_payload) {
      encode(method, payload, 0);
      encode(len, payload, 0);
      payload.claim_append(auth_payload);
    }

    AuthRequestFrame(char *payload, uint32_t length) : Frame() {
      method = *(uint32_t *)payload;
      len = *(uint32_t *)(payload + sizeof(uint32_t));
      ceph_assert((length - (sizeof(uint32_t) * 2)) == len);
      auth_payload.append((payload + (sizeof(uint32_t) * 2)), len);
    }
  };

  struct AuthBadMethodFrame : public Frame {
    uint32_t method;
    std::vector<__u32> allowed_methods;

    AuthBadMethodFrame(uint32_t method, std::vector<__u32> methods)
        : Frame(Tag::AUTH_BAD_METHOD),
          method(method),
          allowed_methods(methods) {
      encode(method, payload, 0);
      encode((uint32_t)allowed_methods.size(), payload, 0);
      for (const auto &a_meth : allowed_methods) {
        encode(a_meth, payload, 0);
      }
    }

    AuthBadMethodFrame(char *payload, uint32_t length) : Frame() {
      method = *(uint32_t *)payload;
      uint32_t num_methods = *(uint32_t *)(payload + sizeof(uint32_t));
      payload += sizeof(uint32_t) * 2;
      for (unsigned i = 0; i < num_methods; ++i) {
        allowed_methods.push_back(
            *(uint32_t *)(payload + sizeof(uint32_t) * i));
      }
    }
  };

  struct AuthBadAuthFrame : public Frame {
    uint32_t error_code;
    std::string error_msg;

    AuthBadAuthFrame(uint32_t error_code, std::string error_msg)
        : Frame(Tag::AUTH_BAD_AUTH),
          error_code(error_code),
          error_msg(error_msg) {
      encode(error_code, payload, 0);
      encode(error_msg, payload, 0);
    }

    AuthBadAuthFrame(char *payload, uint32_t length) : Frame() {
      error_code = *(uint32_t *)payload;
      uint32_t len = *(uint32_t *)(payload + sizeof(uint32_t));
      error_msg = std::string(payload + sizeof(uint32_t) * 2, len);
    }
  };

  struct AuthMoreFrame : public Frame {
    uint32_t len;
    bufferlist auth_payload;

    AuthMoreFrame(bufferlist &auth_payload)
        : Frame(Tag::AUTH_MORE),
          len(auth_payload.length()),
          auth_payload(auth_payload) {
      encode(len, payload, 0);
      payload.claim_append(auth_payload);
    }

    AuthMoreFrame(char *payload, uint32_t length) : Frame() {
      len = *(uint32_t *)payload;
      ceph_assert((length - sizeof(uint32_t)) == len);
      auth_payload.append(payload + sizeof(uint32_t), len);
    }
  };

  struct AuthDoneFrame : public Frame {
    uint64_t flags;

    AuthDoneFrame(uint64_t flags) : Frame(Tag::AUTH_DONE), flags(flags) {
      encode(flags, payload, 0);
    }

    AuthDoneFrame(char *payload, uint32_t length) : Frame() {
      flags = *(uint64_t *)payload;
    }
  };

  struct IdentFrame : public SignedEncryptedFrame {
    entity_addrvec_t addrs;
    int64_t gid;
    uint64_t supported_features;  // CEPH_FEATURE_*
    uint64_t required_features;   // CEPH_FEATURE_*
    uint64_t flags;               // CEPH_MSG_CONNECT_*
    uint64_t cookie;

    IdentFrame(entity_addrvec_t addrs, int64_t gid, uint64_t supported_features,
               uint64_t required_features, uint64_t flags, uint64_t cookie)
        : SignedEncryptedFrame(Tag::IDENT),
          addrs(addrs),
          gid(gid),
          supported_features(supported_features),
          required_features(required_features),
          flags(flags),
          cookie(cookie) {
      encode(addrs, payload, -1ll);
      encode(gid, payload, -1ll);
      encode(supported_features, payload, -1ll);
      encode(required_features, payload, -1ll);
      encode(flags, payload, -1ll);
      encode(cookie, payload, -1ll);
    }

    IdentFrame(char *payload, uint32_t length) : SignedEncryptedFrame() {
      bufferlist bl;
      bl.push_back(buffer::create_static(length, payload));
      try {
        auto ti = bl.cbegin();
        decode(addrs, ti);
        decode(gid, ti);
        decode(supported_features, ti);
        decode(required_features, ti);
        decode(flags, ti);
        decode(cookie, ti);
      } catch (const buffer::error &e) {
      }
    }
  };

  struct IdentMissingFeaturesFrame : public SignedEncryptedFrame {
    __le64 features;

    IdentMissingFeaturesFrame(uint64_t features)
        : SignedEncryptedFrame(Tag::IDENT_MISSING_FEATURES),
          features(features) {
      encode(features, payload, -1ll);
    }

    IdentMissingFeaturesFrame(char *payload, uint32_t length)
        : SignedEncryptedFrame() {
      features = *(uint64_t *)payload;
    }
  };

  struct MessageFrame : public SignedEncryptedFrame {
    const unsigned int ASYNC_COALESCE_THRESHOLD = 256;

    ceph_msg_header2 header2;

    MessageFrame(Message *msg, bufferlist &data, uint64_t ack_seq,
                 bool calc_crc)
        : SignedEncryptedFrame(Tag::MESSAGE) {
      ceph_msg_header &header = msg->get_header();
      ceph_msg_footer &footer = msg->get_footer();

      header2 = ceph_msg_header2{header.seq,        header.tid,
                                 header.type,       header.priority,
                                 header.version,    header.front_len,
                                 header.middle_len, 0,
                                 header.data_len,   header.data_off,
                                 ack_seq,           footer.front_crc,
                                 footer.middle_crc, footer.data_crc,
                                 footer.flags,      header.compat_version,
                                 header.reserved,   0};

      if (calc_crc) {
        header2.header_crc =
            ceph_crc32c(0, (unsigned char *)&header2,
                        sizeof(header2) - sizeof(header2.header_crc));
      }

      payload.append((char *)&header2, sizeof(header2));
      if ((data.length() <= ASYNC_COALESCE_THRESHOLD) &&
          (data.buffers().size() > 1)) {
        for (const auto &pb : data.buffers()) {
          payload.append((char *)pb.c_str(), pb.length());
        }
      } else {
        payload.claim_append(data);
      }
    }
  };

  struct KeepAliveFrame : public SignedEncryptedFrame {
    struct ceph_timespec timestamp;

    KeepAliveFrame() : SignedEncryptedFrame(Tag::KEEPALIVE2) {
      struct ceph_timespec ts;
      utime_t t = ceph_clock_now();
      t.encode_timeval(&ts);
      payload.append((char *)&ts, sizeof(ts));
    }

    KeepAliveFrame(struct ceph_timespec &timestamp)
        : SignedEncryptedFrame(Tag::KEEPALIVE2_ACK) {
      payload.append((char *)&timestamp, sizeof(timestamp));
    }

    KeepAliveFrame(char *payload, uint32_t length) : SignedEncryptedFrame() {
      ceph_assert(length == sizeof(timestamp));
      timestamp = *(struct ceph_timespec *)payload;
    }
  };

  struct AckFrame : public SignedEncryptedFrame {
    uint64_t seq;

    AckFrame(uint64_t seq) : SignedEncryptedFrame(Tag::ACK) {
      encode(seq, payload, 0);
    }

    AckFrame(char *payload, uint32_t length) : SignedEncryptedFrame() {
      seq = *(uint64_t *)payload;
    }
  };

  char *temp_buffer;
  State state;
  uint64_t peer_required_features;
  uint64_t cookie;
  uint64_t message_seq;
  bool can_write;
  std::map<int, std::list<std::pair<bufferlist, Message *>>> out_queue;
  std::list<Message *> sent;
  __u32 connect_seq;
  std::atomic<uint64_t> out_seq{0};
  std::atomic<uint64_t> in_seq{0};
  std::atomic<uint64_t> ack_left{0};

  using ProtFuncPtr = void (ProtocolV2::*)();
  Ct<ProtocolV2> *bannerExchangeCallback;

  uint32_t next_frame_len;
  Tag next_tag;
  ceph_msg_header2 current_header;
  utime_t backoff;  // backoff time
  utime_t recv_stamp;
  utime_t throttle_stamp;
  unsigned msg_left;
  bufferlist data_buf;
  bufferlist::iterator data_blp;
  bufferlist front, middle, data;

  bool keepalive;

  ostream &_conn_prefix(std::ostream *_dout);

  Ct<ProtocolV2> *read(CONTINUATION_PARAM(next, ProtocolV2, char *, int),
                       int len, char *buffer = nullptr);
  Ct<ProtocolV2> *write(CONTINUATION_PARAM(next, ProtocolV2, int),
                        bufferlist &bl);

  void requeue_sent();
  void reset_recv_state();
  Ct<ProtocolV2> *_fault();
  void discard_out_queue();
  void prepare_send_message(uint64_t features, Message *m, bufferlist &bl);
  Message *_get_next_outgoing(bufferlist *bl);
  ssize_t write_message(Message *m, bufferlist &bl, bool more);
  void append_keepalive();
  void append_keepalive_ack(utime_t &timestamp);
  void handle_message_ack(uint64_t seq);

  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, _banner_exchange_handle_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2,
                                 _banner_exchange_handle_peer_banner);

  Ct<ProtocolV2> *_banner_exchange(Ct<ProtocolV2> *callback);
  Ct<ProtocolV2> *_banner_exchange_handle_write(int r);
  Ct<ProtocolV2> *_banner_exchange_handle_peer_banner(char *buffer, int r);

  CONTINUATION_DECL(ProtocolV2, read_frame);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_read_frame_length_and_tag);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_frame_payload);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_auth_more_write);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_header);
  CONTINUATION_DECL(ProtocolV2, throttle_message);
  CONTINUATION_DECL(ProtocolV2, throttle_bytes);
  CONTINUATION_DECL(ProtocolV2, throttle_dispatch_queue);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_front);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_middle);
  CONTINUATION_DECL(ProtocolV2, read_message_data);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_data);

  Ct<ProtocolV2> *read_frame();
  Ct<ProtocolV2> *handle_read_frame_length_and_tag(char *buffer, int r);
  Ct<ProtocolV2> *handle_frame_payload(char *buffer, int r);

  Ct<ProtocolV2> *handle_auth_more(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_more_write(int r);

  Ct<ProtocolV2> *handle_ident(char *payload, uint32_t length);

  Ct<ProtocolV2> *ready();

  Ct<ProtocolV2> *handle_message();
  Ct<ProtocolV2> *handle_message_header(char *buffer, int r);
  Ct<ProtocolV2> *throttle_message();
  Ct<ProtocolV2> *throttle_bytes();
  Ct<ProtocolV2> *throttle_dispatch_queue();
  Ct<ProtocolV2> *read_message_front();
  Ct<ProtocolV2> *handle_message_front(char *buffer, int r);
  Ct<ProtocolV2> *read_message_middle();
  Ct<ProtocolV2> *handle_message_middle(char *buffer, int r);
  Ct<ProtocolV2> *read_message_data_prepare();
  Ct<ProtocolV2> *read_message_data();
  Ct<ProtocolV2> *handle_message_data(char *buffer, int r);
  Ct<ProtocolV2> *handle_message_complete();

  Ct<ProtocolV2> *handle_keepalive2(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_keepalive2_ack(char *payload, uint32_t length);

  Ct<ProtocolV2> *handle_message_ack(char *payload, uint32_t length);

public:
  ProtocolV2(AsyncConnection *connection);
  virtual ~ProtocolV2();

  virtual void connect() override;
  virtual void accept() override;
  virtual bool is_connected() override;
  virtual void stop() override;
  virtual void fault() override;
  virtual void send_message(Message *m) override;
  virtual void send_keepalive() override;

  virtual void read_event() override;
  virtual void write_event() override;
  virtual bool is_queued() override;

private:
  // Client Protocol
  CONTINUATION_DECL(ProtocolV2, start_client_banner_exchange);
  CONTINUATION_DECL(ProtocolV2, post_client_banner_exchange);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_auth_request_write);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_client_ident_write);

  Ct<ProtocolV2> *start_client_banner_exchange();
  Ct<ProtocolV2> *post_client_banner_exchange();
  Ct<ProtocolV2> *send_auth_request(std::vector<__u32> allowed_methods = {});
  Ct<ProtocolV2> *handle_auth_request_write(int r);
  Ct<ProtocolV2> *handle_auth_bad_method(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_bad_auth(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_done(char *payload, uint32_t length);
  Ct<ProtocolV2> *send_client_ident();
  Ct<ProtocolV2> *handle_client_ident_write(int r);
  Ct<ProtocolV2> *handle_ident_missing_features(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_server_ident(char *payload, uint32_t length);

  // Server Protocol
  CONTINUATION_DECL(ProtocolV2, start_server_banner_exchange);
  CONTINUATION_DECL(ProtocolV2, post_server_banner_exchange);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_auth_bad_method_write);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_auth_bad_auth_write);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_auth_done_write);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2,
                                  handle_ident_missing_features_write);
  WRITE_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_send_server_ident_write);

  Ct<ProtocolV2> *start_server_banner_exchange();
  Ct<ProtocolV2> *post_server_banner_exchange();
  Ct<ProtocolV2> *handle_auth_request(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_bad_method_write(int r);
  Ct<ProtocolV2> *handle_auth_bad_auth_write(int r);
  Ct<ProtocolV2> *handle_auth_done_write(int r);
  Ct<ProtocolV2> *handle_client_ident(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_ident_missing_features_write(int r);
  Ct<ProtocolV2> *handle_send_server_ident_write(int r);
};

#endif /* _MSG_ASYNC_PROTOCOL_V2_ */
