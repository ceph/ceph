// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef _MSG_ASYNC_PROTOCOL_V2_
#define _MSG_ASYNC_PROTOCOL_V2_

#include "Protocol.h"
#include "crypto_onwire.h"
#include "frames_v2.h"

class ProtocolV2 : public Protocol {
private:
  enum State {
    NONE,
    START_CONNECT,
    BANNER_CONNECTING,
    HELLO_CONNECTING,
    AUTH_CONNECTING,
    AUTH_CONNECTING_SIGN,
    SESSION_CONNECTING,
    SESSION_RECONNECTING,
    START_ACCEPT,
    BANNER_ACCEPTING,
    HELLO_ACCEPTING,
    AUTH_ACCEPTING,
    AUTH_ACCEPTING_MORE,
    AUTH_ACCEPTING_SIGN,
    SESSION_ACCEPTING,
    READY,
    THROTTLE_MESSAGE,
    THROTTLE_BYTES,
    THROTTLE_DISPATCH_QUEUE,
    THROTTLE_DONE,
    READ_MESSAGE_COMPLETE,
    STANDBY,
    WAIT,
    CLOSED
  };

  static const char *get_state_name(int state) {
    const char *const statenames[] = {"NONE",
                                      "START_CONNECT",
                                      "BANNER_CONNECTING",
                                      "HELLO_CONNECTING",
                                      "AUTH_CONNECTING",
                                      "AUTH_CONNECTING_SIGN",
                                      "SESSION_CONNECTING",
                                      "SESSION_RECONNECTING",
                                      "START_ACCEPT",
                                      "BANNER_ACCEPTING",
                                      "HELLO_ACCEPTING",
                                      "AUTH_ACCEPTING",
                                      "AUTH_ACCEPTING_MORE",
                                      "AUTH_ACCEPTING_SIGN",
                                      "SESSION_ACCEPTING",
                                      "READY",
                                      "THROTTLE_MESSAGE",
                                      "THROTTLE_BYTES",
                                      "THROTTLE_DISPATCH_QUEUE",
                                      "THROTTLE_DONE",
                                      "READ_MESSAGE_COMPLETE",
                                      "STANDBY",
                                      "WAIT",
                                      "CLOSED"};
    return statenames[state];
  }

  // TODO: move into auth_meta?
  ceph::crypto::onwire::rxtx_t session_stream_handlers;

  entity_name_t peer_name;
  State state;
  uint64_t peer_required_features;

  uint64_t client_cookie;
  uint64_t server_cookie;
  uint64_t global_seq;
  uint64_t connect_seq;
  uint64_t peer_global_seq;
  uint64_t message_seq;
  bool reconnecting;
  bool replacing;
  bool can_write;
  struct out_queue_entry_t {
    bool is_prepared {false};
    Message* m {nullptr};
  };
  std::map<int, std::list<out_queue_entry_t>> out_queue;
  std::list<Message *> sent;
  std::atomic<uint64_t> out_seq{0};
  std::atomic<uint64_t> in_seq{0};
  std::atomic<uint64_t> ack_left{0};

  using ProtFuncPtr = void (ProtocolV2::*)();
  Ct<ProtocolV2> *bannerExchangeCallback;

  ceph::msgr::v2::FrameAssembler tx_frame_asm;
  ceph::msgr::v2::FrameAssembler rx_frame_asm;

  ceph::bufferlist rx_preamble;
  ceph::bufferlist rx_epilogue;
  ceph::msgr::v2::segment_bls_t rx_segments_data;
  ceph::msgr::v2::Tag next_tag;
  utime_t backoff;  // backoff time
  utime_t recv_stamp;
  utime_t throttle_stamp;

  struct {
    ceph::bufferlist rxbuf;
    ceph::bufferlist txbuf;
    bool enabled {true};
  } pre_auth;

  bool keepalive;
  bool write_in_progress = false;

  std::ostream& _conn_prefix(std::ostream *_dout);
  void run_continuation(Ct<ProtocolV2> *pcontinuation);
  void run_continuation(Ct<ProtocolV2> &continuation);

  Ct<ProtocolV2> *read(CONTINUATION_RXBPTR_TYPE<ProtocolV2> &next,
                       rx_buffer_t&& buffer);
  template <class F>
  Ct<ProtocolV2> *write(const std::string &desc,
                        CONTINUATION_TYPE<ProtocolV2> &next,
			F &frame);
  Ct<ProtocolV2> *write(const std::string &desc,
                        CONTINUATION_TYPE<ProtocolV2> &next,
                        ceph::bufferlist &buffer);

  template <class F>
  bool append_frame(F& frame);

  void requeue_sent();
  uint64_t discard_requeued_up_to(uint64_t out_seq, uint64_t seq);
  void reset_recv_state();
  void reset_security();
  void reset_throttle();
  Ct<ProtocolV2> *_fault();
  void discard_out_queue();
  void reset_session();
  void prepare_send_message(uint64_t features, Message *m);
  out_queue_entry_t _get_next_outgoing();
  ssize_t write_message(Message *m, bool more);
  void handle_message_ack(uint64_t seq);

  CONTINUATION_DECL(ProtocolV2, _wait_for_peer_banner);
  READ_BPTR_HANDLER_CONTINUATION_DECL(ProtocolV2, _handle_peer_banner);
  READ_BPTR_HANDLER_CONTINUATION_DECL(ProtocolV2, _handle_peer_banner_payload);

  Ct<ProtocolV2> *_banner_exchange(Ct<ProtocolV2> &callback);
  Ct<ProtocolV2> *_wait_for_peer_banner();
  Ct<ProtocolV2> *_handle_peer_banner(rx_buffer_t &&buffer, int r);
  Ct<ProtocolV2> *_handle_peer_banner_payload(rx_buffer_t &&buffer, int r);
  Ct<ProtocolV2> *handle_hello(ceph::bufferlist &payload);

  CONTINUATION_DECL(ProtocolV2, read_frame);
  CONTINUATION_DECL(ProtocolV2, finish_auth);
  READ_BPTR_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_read_frame_preamble_main);
  READ_BPTR_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_read_frame_segment);
  READ_BPTR_HANDLER_CONTINUATION_DECL(ProtocolV2, handle_read_frame_epilogue_main);
  CONTINUATION_DECL(ProtocolV2, throttle_message);
  CONTINUATION_DECL(ProtocolV2, throttle_bytes);
  CONTINUATION_DECL(ProtocolV2, throttle_dispatch_queue);

  Ct<ProtocolV2> *read_frame();
  Ct<ProtocolV2> *finish_auth();
  Ct<ProtocolV2> *finish_client_auth();
  Ct<ProtocolV2> *handle_read_frame_preamble_main(rx_buffer_t &&buffer, int r);
  Ct<ProtocolV2> *read_frame_segment();
  Ct<ProtocolV2> *handle_read_frame_segment(rx_buffer_t &&rx_buffer, int r);
  Ct<ProtocolV2> *_handle_read_frame_segment();
  Ct<ProtocolV2> *handle_read_frame_epilogue_main(rx_buffer_t &&buffer, int r);
  Ct<ProtocolV2> *_handle_read_frame_epilogue_main();
  Ct<ProtocolV2> *handle_read_frame_dispatch();
  Ct<ProtocolV2> *handle_frame_payload();

  Ct<ProtocolV2> *ready();

  Ct<ProtocolV2> *handle_message();
  Ct<ProtocolV2> *throttle_message();
  Ct<ProtocolV2> *throttle_bytes();
  Ct<ProtocolV2> *throttle_dispatch_queue();
  Ct<ProtocolV2> *read_message_data_prepare();

  Ct<ProtocolV2> *handle_keepalive2(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_keepalive2_ack(ceph::bufferlist &payload);

  Ct<ProtocolV2> *handle_message_ack(ceph::bufferlist &payload);

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
  Ct<ProtocolV2> *handle_auth_bad_method(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_auth_reply_more(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_auth_done(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_auth_signature(ceph::bufferlist &payload);
  Ct<ProtocolV2> *send_client_ident();
  Ct<ProtocolV2> *send_reconnect();
  Ct<ProtocolV2> *handle_ident_missing_features(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_session_reset(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_session_retry(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_session_retry_global(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_wait(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_reconnect_ok(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_server_ident(ceph::bufferlist &payload);

  // Server Protocol
  CONTINUATION_DECL(ProtocolV2, start_server_banner_exchange);
  CONTINUATION_DECL(ProtocolV2, post_server_banner_exchange);
  CONTINUATION_DECL(ProtocolV2, server_ready);

  Ct<ProtocolV2> *start_server_banner_exchange();
  Ct<ProtocolV2> *post_server_banner_exchange();
  Ct<ProtocolV2> *handle_auth_request(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_auth_request_more(ceph::bufferlist &payload);
  Ct<ProtocolV2> *_handle_auth_request(ceph::bufferlist& auth_payload, bool more);
  Ct<ProtocolV2> *_auth_bad_method(int r);
  Ct<ProtocolV2> *handle_client_ident(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_ident_missing_features_write(int r);
  Ct<ProtocolV2> *handle_reconnect(ceph::bufferlist &payload);
  Ct<ProtocolV2> *handle_existing_connection(const AsyncConnectionRef& existing);
  Ct<ProtocolV2> *reuse_connection(const AsyncConnectionRef& existing,
                                   ProtocolV2 *exproto);
  Ct<ProtocolV2> *send_server_ident();
  Ct<ProtocolV2> *send_reconnect_ok();
  Ct<ProtocolV2> *server_ready();

  size_t get_current_msg_size() const;
};

#endif /* _MSG_ASYNC_PROTOCOL_V2_ */
