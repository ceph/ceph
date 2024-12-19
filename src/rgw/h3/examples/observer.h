#include <iostream>
#include <system_error>
#include <boost/system/error_code.hpp>
#include <h3/observer.h>
#include <h3/ostream.h>

class StderrObserver : public rgw::h3::Observer {
 public:
  using endpoint = boost::asio::ip::udp::endpoint;
  using error_code = boost::system::error_code;

  void on_listener_recvmmsg_error(error_code ec) override
  {
    std::cerr << "recvmmsg() failed: " << ec.message() << '\n';
  }
  void on_listener_sendto_error(const endpoint& peer, error_code ec) override
  {
    std::cerr << "sendto(" << peer << ") failed: " << ec.message() << '\n';
  }
  void on_listener_header_info_error(error_code ec) override
  {
    std::cerr << "quiche_header_info() failed to parse "
        "packet header: " << ec.message() << '\n';
  }
  void on_listener_packet_received(
      uint8_t type, size_t bytes, const endpoint& peer,
      const rgw::h3::connection_id& scid, const rgw::h3::connection_id& dcid,
      const rgw::h3::address_validation_token& token) override
  {
  }
  void on_listener_negotiate_version_error(const endpoint& peer,
                                           error_code ec) override
  {
    std::cerr << "quiche_negotiate_version() for " << peer
        << " failed: " << ec.message() << '\n';
  }
  void on_listener_negotiate_version(const endpoint& peer,
                                     size_t bytes, uint32_t version) override
  {
    std::cerr << "sent version negotitation packet of "
        << bytes << " bytes to " << peer
        << " that requested version=" << version << '\n';
  }
  void on_listener_stateless_retry_error(const endpoint& peer,
                                         error_code ec) override
  {
    std::cerr << "quiche_retry() for " << peer
        << " failed: " << ec.message() << '\n';
  }
  void on_listener_stateless_retry(
      const endpoint& peer, size_t bytes,
      const rgw::h3::address_validation_token& token,
      const rgw::h3::connection_id& cid) override
  {
    std::cerr << "sent retry packet of " << bytes
        << " bytes with token=" << token << " cid=" << cid
        << " to " << peer << '\n';
  }
  void on_listener_token_validation_error(
      const endpoint& peer,
      const rgw::h3::address_validation_token& token) override
  {
    std::cerr << "token validation failed for " << peer
        << "token=" << token << '\n';
  }
  void on_listener_accept_error(const endpoint& peer) override
  {
    std::cerr << "quiche_accept() failed for " << peer << '\n';
  }
  void on_listener_closed(error_code ec) override
  {
    std::cerr << "listener exiting with " << ec.message() << '\n';
  }

  void on_conn_accept(const rgw::h3::connection_id& cid) override
  {
    std::cerr << "conn " << cid << ": accepted\n";
  }
  void on_conn_close_peer(const rgw::h3::connection_id& cid,
                          std::string_view reason,
                          uint64_t code, bool is_app) override
  {
    std::cerr << "conn " << cid
        << ": peer closed the connection with code " << code
        << " reason: " << reason << '\n';
  }
  void on_conn_close_local(const rgw::h3::connection_id& cid,
                           std::string_view reason,
                           uint64_t code, bool is_app) override
  {
    std::cerr << "conn " << cid
        << ": connection closed with code " << code
        << " reason: " << reason << '\n';
  }
  void on_conn_timed_out(const rgw::h3::connection_id& cid) override
  {
    std::cerr << "conn " << cid << ": connection timed out\n";
  }
  void on_conn_destroy(const rgw::h3::connection_id& cid) override
  {
    std::cerr << "conn " << cid << ": destroyed after last use\n";
  }

  void on_conn_schedule_timeout(const rgw::h3::connection_id& cid,
                                std::chrono::nanoseconds ns) override
  {
  }
  void on_conn_pacing_delay(const rgw::h3::connection_id& cid,
                            std::chrono::nanoseconds ns) override
  {
    std::cerr << "conn " << cid
        << ": writes delayed by " << ns
        << " for congestion control\n";
  }
  void on_conn_send_error(const rgw::h3::connection_id& cid,
                          boost::system::error_code ec) override
  {
    std::cerr << "conn " << cid
        << ": quiche_conn_send() failed: " << ec.message() << '\n';
  }
  void on_conn_recv_error(const rgw::h3::connection_id& cid,
                          boost::system::error_code ec) override
  {
    std::cerr << "conn " << cid
        << ": quiche_conn_recv() failed: " << ec.message() << '\n';
  }
  void on_conn_sendmmsg_error(const rgw::h3::connection_id& cid,
                              boost::system::error_code ec) override
  {
    std::cerr << "conn " << cid
        << ": sendmmsg() failed: " << ec.message() << '\n';
  }
  void on_conn_h3_poll_error(const rgw::h3::connection_id& cid, uint64_t stream_id,
                             boost::system::error_code ec) override
  {
    std::cerr << "conn " << cid << " stream #" << stream_id
        << ": poll_events() failed with " << ec.message() << '\n';
  }

  void on_stream_accept(const rgw::h3::connection_id& cid,
                        uint64_t stream_id) override
  {
  }
  void on_stream_recv_body_error(const rgw::h3::connection_id& cid,
                                 uint64_t stream_id,
                                 boost::system::error_code ec) override
  {
    std::cerr << "conn " << cid << " stream #" << stream_id
        << ": quiche_h3_recv_body() failed with " << ec.message() << '\n';
  }
  void on_stream_recv_body(const rgw::h3::connection_id& cid,
                           uint64_t stream_id, size_t bytes) override
  {
  }
  void on_stream_send_body_error(const rgw::h3::connection_id& cid,
                                 uint64_t stream_id,
                                 boost::system::error_code ec) override
  {
    std::cerr << "conn " << cid << " stream #" << stream_id
        << ": quiche_h3_send_body() failed with " << ec.message() << '\n';
  }
  void on_stream_send_body(const rgw::h3::connection_id& cid,
                           uint64_t stream_id, size_t bytes) override
  {
  }
  void on_stream_send_response_error(const rgw::h3::connection_id& cid,
                                     uint64_t stream_id,
                                     boost::system::error_code ec) override
  {
    std::cerr << "conn " << cid << " stream #" << stream_id
        << ": quiche_h3_send_response() failed with " << ec.message() << '\n';
  }
  void on_stream_send_response(const rgw::h3::connection_id& cid,
                               uint64_t stream_id) override
  {
  }
};
