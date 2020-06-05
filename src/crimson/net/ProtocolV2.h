// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/sleep.hh>

#include "Protocol.h"
#include "msg/async/frames_v2.h"
#include "msg/async/crypto_onwire.h"

namespace crimson::net {

class ProtocolV2 final : public Protocol {
 public:
  ProtocolV2(ChainedDispatchersRef& dispatcher,
             SocketConnection& conn,
             SocketMessenger& messenger);
  ~ProtocolV2() override;
  void print(std::ostream&) const final;
 private:
  bool is_connected() const override;

  void start_connect(const entity_addr_t& peer_addr,
                     const entity_name_t& peer_name) override;

  void start_accept(SocketRef&& socket,
                    const entity_addr_t& peer_addr) override;

  void trigger_close() override;

  ceph::bufferlist do_sweep_messages(
      const std::deque<MessageRef>& msgs,
      size_t num_msgs,
      bool require_keepalive,
      std::optional<utime_t> keepalive_ack,
      bool require_ack) override;

  void notify_write() override;

 private:
  SocketMessenger &messenger;

  enum class state_t {
    NONE = 0,
    ACCEPTING,
    SERVER_WAIT,
    ESTABLISHING,
    CONNECTING,
    READY,
    STANDBY,
    WAIT,
    REPLACING,
    CLOSING
  };
  state_t state = state_t::NONE;

  static const char *get_state_name(state_t state) {
    const char *const statenames[] = {"NONE",
                                      "ACCEPTING",
                                      "SERVER_WAIT",
                                      "ESTABLISHING",
                                      "CONNECTING",
                                      "READY",
                                      "STANDBY",
                                      "WAIT",
                                      "REPLACING",
                                      "CLOSING"};
    return statenames[static_cast<int>(state)];
  }

  void trigger_state(state_t state, write_state_t write_state, bool reentrant);

  uint64_t connection_features = 0;
  uint64_t peer_required_features = 0;

  uint64_t client_cookie = 0;
  uint64_t server_cookie = 0;
  uint64_t global_seq = 0;
  uint64_t peer_global_seq = 0;
  uint64_t connect_seq = 0;

  seastar::shared_future<> execution_done = seastar::now();

  template <typename Func>
  void gated_execute(const char* what, Func&& func) {
    gate.dispatch_in_background(what, *this, [this, &func] {
      execution_done = seastar::futurize_invoke(std::forward<Func>(func));
      return execution_done.get_future();
    });
  }

  class Timer {
    double last_dur_ = 0.0;
    const SocketConnection& conn;
    std::optional<seastar::abort_source> as;
   public:
    Timer(SocketConnection& conn) : conn(conn) {}
    double last_dur() const { return last_dur_; }
    seastar::future<> backoff(double seconds);
    void cancel() {
      last_dur_ = 0.0;
      if (as) {
        as->request_abort();
        as = std::nullopt;
      }
    }
  };
  Timer protocol_timer;

 // TODO: Frame related implementations, probably to a separate class.
 private:
  bool record_io = false;
  ceph::bufferlist rxbuf;
  ceph::bufferlist txbuf;

  void enable_recording();
  seastar::future<Socket::tmp_buf> read_exactly(size_t bytes);
  seastar::future<bufferlist> read(size_t bytes);
  seastar::future<> write(bufferlist&& buf);
  seastar::future<> write_flush(bufferlist&& buf);

  ceph::crypto::onwire::rxtx_t session_stream_handlers;
  boost::container::static_vector<ceph::msgr::v2::segment_t,
				  ceph::msgr::v2::MAX_NUM_SEGMENTS> rx_segments_desc;
  boost::container::static_vector<ceph::bufferlist,
				  ceph::msgr::v2::MAX_NUM_SEGMENTS> rx_segments_data;

  size_t get_current_msg_size() const;
  seastar::future<ceph::msgr::v2::Tag> read_main_preamble();
  seastar::future<> read_frame_payload();
  template <class F>
  seastar::future<> write_frame(F &frame, bool flush=true);

 private:
  void fault(bool backoff, const char* func_name, std::exception_ptr eptr);
  void reset_session(bool full);
  seastar::future<std::tuple<entity_type_t, entity_addr_t>>
  banner_exchange(bool is_connect);

  enum class next_step_t {
    ready,
    wait,
    none,       // protocol should have been aborted or failed
  };

  // CONNECTING (client)
  seastar::future<> handle_auth_reply();
  inline seastar::future<> client_auth() {
    std::vector<uint32_t> empty;
    return client_auth(empty);
  }
  seastar::future<> client_auth(std::vector<uint32_t> &allowed_methods);

  seastar::future<next_step_t> process_wait();
  seastar::future<next_step_t> client_connect();
  seastar::future<next_step_t> client_reconnect();
  void execute_connecting();

  // ACCEPTING (server)
  seastar::future<> _auth_bad_method(int r);
  seastar::future<> _handle_auth_request(bufferlist& auth_payload, bool more);
  seastar::future<> server_auth();

  bool validate_peer_name(const entity_name_t& peer_name) const;
  seastar::future<next_step_t> send_wait();
  seastar::future<next_step_t> reuse_connection(ProtocolV2* existing_proto,
                                                bool do_reset=false,
                                                bool reconnect=false,
                                                uint64_t conn_seq=0,
                                                uint64_t msg_seq=0);

  seastar::future<next_step_t> handle_existing_connection(SocketConnectionRef existing_conn);
  seastar::future<next_step_t> server_connect();

  seastar::future<next_step_t> read_reconnect();
  seastar::future<next_step_t> send_retry(uint64_t connect_seq);
  seastar::future<next_step_t> send_retry_global(uint64_t global_seq);
  seastar::future<next_step_t> send_reset(bool full);
  seastar::future<next_step_t> server_reconnect();

  void execute_accepting();

  // CONNECTING/ACCEPTING
  seastar::future<> finish_auth();

  // ESTABLISHING
  void execute_establishing(SocketConnectionRef existing_conn, bool dispatch_reset);

  // ESTABLISHING/REPLACING (server)
  seastar::future<> send_server_ident();

  // REPLACING (server)
  void trigger_replacing(bool reconnect,
                         bool do_reset,
                         SocketRef&& new_socket,
                         AuthConnectionMetaRef&& new_auth_meta,
                         ceph::crypto::onwire::rxtx_t new_rxtx,
                         uint64_t new_peer_global_seq,
                         // !reconnect
                         uint64_t new_client_cookie,
                         entity_name_t new_peer_name,
                         uint64_t new_conn_features,
                         // reconnect
                         uint64_t new_connect_seq,
                         uint64_t new_msg_seq);

  // READY
  seastar::future<> read_message(utime_t throttle_stamp);
  void execute_ready(bool dispatch_connect);

  // STANDBY
  void execute_standby();

  // WAIT
  void execute_wait(bool max_backoff);

  // SERVER_WAIT
  void execute_server_wait();
};

} // namespace crimson::net
