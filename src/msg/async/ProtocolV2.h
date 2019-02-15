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
    CONNECTING,     // banner + authentication + ident
    START_ACCEPT,
    ACCEPTING,      // banner + authentication + ident
    ACCEPTING_SESSION,
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
                                      "ACCEPTING_SESSION",
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
    HELLO = 1,
    AUTH_REQUEST,
    AUTH_BAD_METHOD,
    AUTH_REPLY_MORE,
    AUTH_REQUEST_MORE,
    AUTH_DONE,
    CLIENT_IDENT,
    SERVER_IDENT,
    IDENT_MISSING_FEATURES,
    SESSION_RECONNECT,
    SESSION_RESET,
    SESSION_RETRY,
    SESSION_RETRY_GLOBAL,
    SESSION_RECONNECT_OK,
    WAIT,
    MESSAGE,
    KEEPALIVE2,
    KEEPALIVE2_ACK,
    ACK
  };

private:
  enum class AuthFlag : uint64_t { ENCRYPTED = 1, SIGNED = 2 };

  entity_name_t peer_name;
  char *temp_buffer;
  State state;
  uint64_t peer_required_features;
  std::shared_ptr<AuthSessionHandler> session_security;

  uint64_t cookie;
  uint64_t global_seq;
  uint64_t connect_seq;
  uint64_t peer_global_seq;
  uint64_t message_seq;
  bool reconnecting;
  bool replacing;
  bool can_write;
  std::map<int, std::list<std::pair<bufferlist, Message *>>> out_queue;
  std::list<Message *> sent;
  std::atomic<uint64_t> out_seq{0};
  std::atomic<uint64_t> in_seq{0};
  std::atomic<uint64_t> ack_left{0};

  using ProtFuncPtr = void (ProtocolV2::*)();
  Ct<ProtocolV2> *bannerExchangeCallback;

  uint32_t next_payload_len;
  Tag next_tag;
  ceph_msg_header2 current_header;
  utime_t backoff;  // backoff time
  utime_t recv_stamp;
  utime_t throttle_stamp;
  unsigned msg_left;
  bufferlist data_buf;
  bufferlist::iterator data_blp;
  bufferlist front, middle, data, extra;

  bool keepalive;

  ostream &_conn_prefix(std::ostream *_dout);
  void run_continuation(Ct<ProtocolV2> *continuation);
  void calc_signature(const char *in, uint32_t length, char *out);

  Ct<ProtocolV2> *read(CONTINUATION_PARAM(next, ProtocolV2, char *, int),
                       int len, char *buffer = nullptr);
  Ct<ProtocolV2> *write(const std::string &desc,
                        CONTINUATION_PARAM(next, ProtocolV2),
                        bufferlist &buffer);

  void requeue_sent();
  uint64_t discard_requeued_up_to(uint64_t out_seq, uint64_t seq);
  void reset_recv_state();
  Ct<ProtocolV2> *_fault();
  void discard_out_queue();
  void reset_session();
  void prepare_send_message(uint64_t features, Message *m, bufferlist &bl);
  Message *_get_next_outgoing(bufferlist *bl);
  ssize_t write_message(Message *m, bufferlist &bl, bool more);
  void append_keepalive();
  void append_keepalive_ack(utime_t &timestamp);
  void handle_message_ack(uint64_t seq);

  CONTINUATION_DECL(ProtocolV2, _wait_for_peer_banner);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, _handle_peer_banner);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, _handle_peer_banner_payload);

  Ct<ProtocolV2> *_banner_exchange(Ct<ProtocolV2> *callback);
  Ct<ProtocolV2> *_wait_for_peer_banner();
  Ct<ProtocolV2> *_handle_peer_banner(char *buffer, int r);
  Ct<ProtocolV2> *_handle_peer_banner_payload(char *buffer, int r);
  Ct<ProtocolV2> *handle_hello(char *payload, uint32_t length);

  CONTINUATION_DECL(ProtocolV2, read_frame);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_read_frame_length_and_tag);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_frame_payload);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_header);
  CONTINUATION_DECL(ProtocolV2, throttle_message);
  CONTINUATION_DECL(ProtocolV2, throttle_bytes);
  CONTINUATION_DECL(ProtocolV2, throttle_dispatch_queue);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_front);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_middle);
  CONTINUATION_DECL(ProtocolV2, read_message_data);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_data);
  READ_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_message_extra_bytes);

  Ct<ProtocolV2> *read_frame();
  Ct<ProtocolV2> *handle_read_frame_length_and_tag(char *buffer, int r);
  Ct<ProtocolV2> *handle_frame_payload(char *buffer, int r);

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
  Ct<ProtocolV2> *handle_message_extra_bytes(char *buffer, int r);
  Ct<ProtocolV2> *handle_message_complete();

  Ct<ProtocolV2> *handle_keepalive2(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_keepalive2_ack(char *payload, uint32_t length);

  Ct<ProtocolV2> *handle_message_ack(char *payload, uint32_t length);

public:
  uint64_t connection_features;

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

  void sign_payload(bufferlist &payload);
  void verify_signature(char *payload, uint32_t length);
  void encrypt_payload(bufferlist &payload);
  void decrypt_payload(char *payload, uint32_t &length);
  void calculate_payload_size(uint32_t length, uint32_t *total_len,
                              uint32_t *sig_pad_len = nullptr,
                              uint32_t *enc_pad_len = nullptr);

private:
  // Client Protocol
  CONTINUATION_DECL(ProtocolV2, start_client_banner_exchange);
  CONTINUATION_DECL(ProtocolV2, post_client_banner_exchange);

  Ct<ProtocolV2> *start_client_banner_exchange();
  Ct<ProtocolV2> *post_client_banner_exchange();
  inline Ct<ProtocolV2> *send_auth_request() {
    std::vector<uint32_t> empty;
    return send_auth_request(empty);
  }
  Ct<ProtocolV2> *send_auth_request(std::vector<uint32_t> &allowed_methods);
  Ct<ProtocolV2> *handle_auth_bad_method(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_reply_more(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_done(char *payload, uint32_t length);
  Ct<ProtocolV2> *send_client_ident();
  Ct<ProtocolV2> *send_reconnect();
  Ct<ProtocolV2> *handle_ident_missing_features(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_session_reset();
  Ct<ProtocolV2> *handle_session_retry(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_session_retry_global(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_wait();
  Ct<ProtocolV2> *handle_reconnect_ok(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_server_ident(char *payload, uint32_t length);

  // Server Protocol
  CONTINUATION_DECL(ProtocolV2, start_server_banner_exchange);
  CONTINUATION_DECL(ProtocolV2, post_server_banner_exchange);
  CONTINUATION_DECL(ProtocolV2, server_ready);

  Ct<ProtocolV2> *start_server_banner_exchange();
  Ct<ProtocolV2> *post_server_banner_exchange();
  Ct<ProtocolV2> *handle_auth_request(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_auth_request_more(char *payload, uint32_t length);
  Ct<ProtocolV2> *_handle_auth_request(bufferlist& auth_payload, bool more);
  Ct<ProtocolV2> *_auth_bad_method(int r);
  Ct<ProtocolV2> *handle_client_ident(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_ident_missing_features_write(int r);
  Ct<ProtocolV2> *handle_reconnect(char *payload, uint32_t length);
  Ct<ProtocolV2> *handle_existing_connection(AsyncConnectionRef existing);
  Ct<ProtocolV2> *reuse_connection(AsyncConnectionRef existing,
                                   ProtocolV2 *exproto);
  Ct<ProtocolV2> *send_server_ident();
  Ct<ProtocolV2> *send_reconnect_ok();
  Ct<ProtocolV2> *server_ready();
};

#endif /* _MSG_ASYNC_PROTOCOL_V2_ */
