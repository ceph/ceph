// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <dlfcn.h>

#include <list>
#include <memory>
#include <boost/asio/ip/v6_only.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>

#include "common/async/shared_mutex.h"
#include "common/dout.h"
#include "common/strtol.h"

#include <h3/h3.h>
#include <h3/observer.h>
#include <h3/ostream.h>

#include "rgw_dmclock_async_scheduler.h"
#include "rgw_frontend.h"
#include "rgw_process.h"
#include "rgw_request.h"
#include "rgw_tools.h"

#include "rgw_quiche_client_io.h"
#include "rgw_quiche_frontend.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::h3 {

// use mmap/mprotect to allocate 512k coroutine stacks
auto make_stack_allocator() {
  return boost::context::protected_fixedsize_stack{512*1024};
}

/// Synchronization primitive for Frontend::pause_for_new_config()
using SharedMutex = ceph::async::SharedMutex<default_executor>;

class StreamHandlerImpl {
  const DoutPrefixProvider* fdpp; // Frontend's dpp
  const RGWProcessEnv& env;
  const RGWFrontendConfig* conf;
  asio::io_context& context;
  SharedMutex& pause_mutex;
  rgw::dmclock::Scheduler* scheduler;
  std::string uri_prefix;

  struct Prefix : DoutPrefixPipe {
    connection_id cid;
    uint64_t stream_id;
    Prefix(const DoutPrefixProvider& dpp, connection_id cid, uint64_t stream_id)
      : DoutPrefixPipe(dpp), cid(std::move(cid)), stream_id(stream_id)
    {}
    void add_prefix(std::ostream& out) const override {
      out << "conn " << cid << " stream #" << stream_id << ": ";
    }
  };
 public:
  StreamHandlerImpl(const DoutPrefixProvider* fdpp, const RGWProcessEnv& env,
                    const RGWFrontendConfig* conf, asio::io_context& context,
                    SharedMutex& pause_mutex,
                    rgw::dmclock::Scheduler* scheduler)
    : fdpp(fdpp), env(env), context(context),
      pause_mutex(pause_mutex), scheduler(scheduler)
  {
    if (auto prefix = conf->get_val("prefix"); prefix) {
      uri_prefix = *prefix;
    }
  }

  // stackful request coroutine
  void process(boost::intrusive_ptr<Connection> conn, uint64_t stream_id,
               http::fields request, ip::udp::endpoint self,
               ip::udp::endpoint peer, boost::asio::yield_context yield)
  {
    auto dpp = Prefix{*fdpp, conn->get_cid(), stream_id};
    auto cct = dpp.get_cct();

    // wait if paused
    error_code ec;
    auto lock = pause_mutex.async_lock_shared(yield[ec]);
    if (ec == asio::error::operation_aborted) {
      return;
    } else if (ec) {
      ldpp_dout(&dpp, 1) << "failed to lock: " << ec.message() << dendl;
      return;
    }

    // process the request
    RGWRequest req{env.driver->get_new_req_id()};

    auto io = ClientIO{context, yield, conn.get(), stream_id,
        std::move(request), std::move(self), std::move(peer)};
    RGWRestfulIO client(cct, &io);
    optional_yield y = null_yield;
    if (cct->_conf->rgw_beast_enable_async) {
      y = optional_yield{yield};
    }
    int http_ret = 0;
    std::string user = "-";
    //const auto started = ceph::coarse_real_clock::now();
    ceph::coarse_real_clock::duration latency{};
    process_request(env, &req, uri_prefix, &client, y,
                    scheduler, &user, &latency, &http_ret);
#if 0
    if (cct->_conf->subsys.should_gather(ceph_subsys_rgw_access, 1)) {
      // access log line elements begin per Apache Combined Log Format with additions following
      lsubdout(cct, rgw_access, 1) << "quiche: " << std::hex << &req << std::dec << ": "
          << remote_endpoint.address() << " - " << user << " [" << log_apache_time{started} << "] \""
          << message.method_string() << ' ' << message.target() << ' '
          << http_version{message.version()} << "\" " << http_ret << ' '
          << client.get_bytes_sent() + client.get_bytes_received() << ' '
          << log_header{message, http::field::referer, "\""} << ' '
          << log_header{message, http::field::user_agent, "\""} << ' '
          << log_header{message, http::field::range} << " latency="
          << latency << dendl;
    }
#endif
  }

  void operator()(boost::intrusive_ptr<Connection> c, uint64_t s,
                  http::fields r, ip::udp::endpoint self,
                  ip::udp::endpoint peer)
  {
    // spawn the stackful coroutine on the default io_context executor, not the
    // Connection's strand
    boost::asio::spawn(make_strand(context), std::allocator_arg, make_stack_allocator(),
        [this, c=std::move(c), s, r=std::move(r), self=std::move(self),
        peer=std::move(peer)] (boost::asio::yield_context yield) mutable {
          process(std::move(c), s, std::move(r),
                  std::move(self), std::move(peer), yield);
        }, [] (std::exception_ptr eptr) {
          if (eptr) std::rethrow_exception(eptr);
        });
  }
};

class LoggingObserver : public DoutPrefixPipe, public Observer {
  ip::udp::endpoint addr; // local address
 public:
  LoggingObserver(const DoutPrefixProvider& dpp,
                  const ip::udp::endpoint& addr)
    : DoutPrefixPipe(dpp), addr(addr)
  {}

  void add_prefix(std::ostream& out) const override
  {
    out << addr << ' ';
  }

  void on_listener_recvmmsg_error(error_code ec) override
  {
    if (ec == std::errc::bad_file_descriptor) { // socket closed for shutdown
      ldpp_dout(this, 20) << "recvmmsg() failed: " << ec.message() << dendl;
    } else {
      ldpp_dout(this, 1) << "recvmmsg() failed: " << ec.message() << dendl;
    }
  }
  void on_listener_sendto_error(const ip::udp::endpoint& peer,
                                error_code ec) override
  {
    ldpp_dout(this, 1) << "sendto(" << peer
        << ") failed: " << ec.message() << dendl;
  }
  void on_listener_header_info_error(error_code ec) override
  {
    ldpp_dout(this, 20) << "quiche_header_info() failed to parse "
        "packet header: " << ec.message() << dendl;
  }
  void on_listener_packet_received(
      uint8_t type, size_t bytes, const ip::udp::endpoint& peer,
      const connection_id& scid, const connection_id& dcid,
      const address_validation_token& token) override
  {
    ldpp_dout(this, 30) << "received packet type " << PacketType{type}
        << " of " << bytes << " bytes from " << peer
        << " with scid=" << scid << " dcid=" << dcid
        << " token=" << token << dendl;
  }
  void on_listener_negotiate_version_error(const ip::udp::endpoint& peer,
                                           error_code ec) override
  {
    ldpp_dout(this, 20) << "quiche_negotiate_version() for " << peer
        << " failed: " << ec.message() << dendl;
  }
  void on_listener_negotiate_version(const ip::udp::endpoint& peer,
                                     size_t bytes, uint32_t version) override
  {
    ldpp_dout(this, 20) << "sent version negotitation packet of "
        << bytes << " bytes to " << peer
        << " that requested version=" << version << dendl;
  }
  void on_listener_stateless_retry_error(const ip::udp::endpoint& peer,
                                         error_code ec) override
  {
    ldpp_dout(this, 20) << "quiche_retry() for " << peer
        << " failed: " << ec.message() << dendl;
  }
  void on_listener_stateless_retry(
      const ip::udp::endpoint& peer, size_t bytes,
      const address_validation_token& token, const connection_id& cid) override
  {
    ldpp_dout(this, 20) << "sent retry packet of " << bytes
        << " bytes with token=" << token << " cid=" << cid
        << " to " << peer << dendl;
  }
  void on_listener_token_validation_error(
      const ip::udp::endpoint& peer,
      const address_validation_token& token) override
  {
    ldpp_dout(this, 20) << "token validation failed for " << peer
        << " token=" << token << dendl;
  }
  void on_listener_accept_error(const ip::udp::endpoint& peer) override
  {
    ldpp_dout(this, 20) << "quiche_accept() failed for " << peer << dendl;
  }
  void on_listener_closed(error_code ec) override
  {
    ldpp_dout(this, 20) << "listener exiting with " << ec.message() << dendl;
  }

  void on_conn_accept(const connection_id& cid) override
  {
    ldpp_dout(this, 20) << "conn " << cid
        << ": accepted" << dendl;
  }
  void on_conn_close_peer(const connection_id& cid,
                          std::string_view reason,
                          uint64_t code, bool is_app) override
  {
    // TODO: interpret code?
    ldpp_dout(this, 20) << "conn " << cid
        << ": peer closed the connection with code " << code
        << " reason: " << reason << dendl;
  }
  void on_conn_close_local(const connection_id& cid,
                           std::string_view reason,
                           uint64_t code, bool is_app) override
  {
    ldpp_dout(this, 20) << "conn " << cid
        << ": connection closed with code " << code
        << " reason: " << reason << dendl;
  }
  void on_conn_timed_out(const connection_id& cid) override
  {
    ldpp_dout(this, 20) << "conn " << cid
        << ": connection timed out" << dendl;
  }
  void on_conn_destroy(const connection_id& cid) override
  {
    ldpp_dout(this, 20) << "conn " << cid
        << ": destroyed after last use" << dendl;
  }

  void on_conn_schedule_timeout(const connection_id& cid,
                                std::chrono::nanoseconds ns) override
  {
    ldpp_dout(this, 30) << "conn " << cid
        << ": timeout scheduled in " << ns << dendl;
  }
  void on_conn_pacing_delay(const connection_id& cid,
                            std::chrono::nanoseconds ns) override
  {
    ldpp_dout(this, 20) << "conn " << cid
        << ": writes delayed by " << ns
        << " for congestion control" << dendl;
  }
  void on_conn_send_error(const connection_id& cid,
                          boost::system::error_code ec) override
  {
    ldpp_dout(this, 20) << "conn " << cid
        << ": quiche_conn_send() failed: " << ec.message() << dendl;
  }
  void on_conn_recv_error(const connection_id& cid,
                          boost::system::error_code ec) override
  {
    ldpp_dout(this, 20) << "conn " << cid
        << ": quiche_conn_recv() failed: " << ec.message() << dendl;
  }
  void on_conn_sendmmsg_error(const connection_id& cid,
                              boost::system::error_code ec) override
  {
    ldpp_dout(this, 1) << "conn " << cid
        << ": sendmmsg() failed: " << ec.message() << dendl;
  }
  void on_conn_h3_poll_error(const connection_id& cid, uint64_t stream_id,
                             boost::system::error_code ec) override
  {
    ldpp_dout(this, 20) << "conn " << cid << " stream #" << stream_id
        << ": poll_events() failed with " << ec.message() << dendl;
  }

  void on_stream_accept(const connection_id& cid,
                        uint64_t stream_id) override
  {
  }
  void on_stream_recv_body_error(const connection_id& cid,
                                 uint64_t stream_id,
                                 boost::system::error_code ec) override
  {
    ldpp_dout(this, 20) << "conn " << cid << " stream #" << stream_id
        << ": quiche_h3_recv_body() failed with " << ec.message() << dendl;
  }
  void on_stream_recv_body(const connection_id& cid,
                           uint64_t stream_id, size_t bytes) override
  {
    ldpp_dout(this, 30) << "conn " << cid << " stream #" << stream_id
        << ": read_body read " << bytes << " bytes" << dendl;
  }
  void on_stream_send_body_error(const connection_id& cid,
                                 uint64_t stream_id,
                                 boost::system::error_code ec) override
  {
    ldpp_dout(this, 20) << "conn " << cid << " stream #" << stream_id
        << ": quiche_h3_send_body() failed with " << ec.message() << dendl;
  }
  void on_stream_send_body(const connection_id& cid,
                           uint64_t stream_id, size_t bytes) override
  {
    ldpp_dout(this, 30) << "conn " << cid << " stream #" << stream_id
        << ": write_body wrote " << bytes << " bytes" << dendl;
  }
  void on_stream_send_response_error(const connection_id& cid,
                                     uint64_t stream_id,
                                     boost::system::error_code ec) override
  {
    ldpp_dout(this, 20) << "conn " << cid << " stream #" << stream_id
        << ": quiche_h3_send_response() failed with " << ec.message() << dendl;
  }
  void on_stream_send_response(const connection_id& cid,
                               uint64_t stream_id) override
  {
    ldpp_dout(this, 30) << "conn " << cid << " stream #" << stream_id
        << ": write_response done" << dendl;
  }
};

class Frontend : public RGWFrontend, public DoutPrefix {
  const RGWFrontendConfig* conf;
  asio::io_context& context;

  void* handle = nullptr; // dlopen() handle
  std::unique_ptr<Config> config;

  SharedMutex pause_mutex;
  rgw::dmclock::SimpleThrottler scheduler;
  StreamHandler on_new_stream;

  struct Endpoint {
    LoggingObserver observer;
    std::unique_ptr<Listener> listener;

    Endpoint(const DoutPrefixProvider& dpp,
             const ip::udp::endpoint& addr)
      : observer(dpp, addr) {}
  };
  std::vector<Endpoint> endpoints;

 public:
  Frontend(CephContext* cct, const RGWProcessEnv& env,
           const RGWFrontendConfig* conf, asio::io_context& context)
    : DoutPrefix(cct, dout_subsys, "h3: "), conf(conf), context(context),
      pause_mutex(context.get_executor()), scheduler(cct),
      on_new_stream(StreamHandlerImpl{this, env, conf, context,
                                      pause_mutex, &scheduler})
  {}
  ~Frontend() override;

  int init() override;

  int run() override;
  void stop() override;
  void join() override;

  void pause_for_new_config() override;
  void unpause_with_new_config() override;
};

static void log_callback(const char* message, void* argp)
{
  auto cct = reinterpret_cast<CephContext*>(argp);
  ldout(cct, 1) << message << dendl;
}

static uint16_t parse_port(std::string_view input,
                           error_code& ec)
{
  uint16_t port = 0;
  auto [p, errc] = std::from_chars(input.begin(), input.end(), port);
  if (errc != std::errc{}) {
    ec = make_error_code(errc);
  }
  if (port == 0 || p != input.end()) {
    ec = make_error_code(std::errc::invalid_argument);
  }
  return port;
}

Frontend::~Frontend()
{
  if (handle) {
    ::dlclose(handle);
  }
}

int Frontend::init()
{
  // load library and entrypoints
  create_config_fn create_config = nullptr;
  create_listener_fn create_listener = nullptr;

  auto plugin_path = get_cct()->_conf.get_val<std::string>("plugin_dir") +
      "/h3/librgw_h3_quiche.so";
  handle = ::dlopen(plugin_path.c_str(), RTLD_NOW);
  if (handle) {
    create_config = (create_config_fn)::dlsym(handle, "create_h3_config");
    create_listener = (create_listener_fn)::dlsym(handle, "create_h3_listener");
  }

  if (!handle || !create_config || !create_listener) {
    ldpp_dout(this, 1) << "ERROR: failed to load plugin: "
        << ::dlerror() << dendl;
    return -ENOENT;
  }

  Options opts;

  if (auto d = conf->get_val("debug"); d) {
    opts.log_callback = log_callback;
    opts.log_arg = get_cct();
    ldpp_dout(this, 1) << "enabled quiche debug logging" << dendl;
  }
  if (auto o = conf->get_val("cc_alg"); o) {
    opts.cc_algorithm = *o;
  }
  if (auto o = conf->get_val("ack_delay_exponent"); o) {
    auto exp = ceph::parse<uint64_t>(*o);
    if (!exp) {
      ldpp_dout(this, -1) << "frontend config failed to parse "
          "'ack_delay_exponent' as uint64_t" << dendl;
      return -EINVAL;
    }
    opts.ack_delay_exponent = *exp;
  }
  if (auto o = conf->get_val("max_ack_delay_ms"); o) {
    auto ms = ceph::parse<uint64_t>(*o);
    if (!ms) {
      ldpp_dout(this, -1) << "frontend config failed to parse "
          "'max_ack_delay_ms' as uint64_t" << dendl;
      return -EINVAL;
    }
    opts.max_ack_delay = std::chrono::milliseconds(*ms);
  }

  // ssl configuration
  auto cert = conf->get_val("cert");
  if (!cert) {
    ldpp_dout(this, -1) << "frontend config requires a 'cert'" << dendl;
    return -EINVAL;
  }
  auto key = conf->get_val("key");
  if (!key) {
    ldpp_dout(this, -1) << "frontend config requires a 'key'" << dendl;
    return -EINVAL;
  }
  // TODO: quiche has no interfaces for loading certs/keys from memory, so
  // can't support the mon config keys. use asio::ssl wrappers like the
  // beast frontend, and pass its ssl context into quiche_conn_new_with_tls()
  opts.ssl_certificate = cert->c_str();
  opts.ssl_private_key = key->c_str();

  try {
    config = create_config(opts);
  } catch (const std::exception& e) {
    ldpp_dout(this, -1) << e.what() << dendl;
    return -EINVAL;
  }

  // parse endpoints
  auto ports = conf->get_config_map().equal_range("port");
  if (ports.first == ports.second) {
    ldpp_dout(this, -1) << "frontend config requires at least one 'port'" << dendl;
    return -EINVAL;
  }
  for (auto i = ports.first; i != ports.second; ++i) {
    error_code ec;
    uint16_t port = parse_port(i->second.c_str(), ec);
    if (ec) {
      ldpp_dout(this, -1) << "failed to parse port=" << i->second << dendl;
      return -ec.value();
    }

    // bind a nonblocking udp socket for both v4/v6
    const auto endpoint = ip::udp::endpoint{ip::udp::v6(), port};
    auto socket = udp_socket{context};
    socket.open(endpoint.protocol(), ec);
    if (ec) {
      ldpp_dout(this, -1) << "failed to open socket with " << ec.message() << dendl;
      return -ec.value();
    }
    socket.non_blocking(true, ec);
    socket.set_option(ip::v6_only{false}, ec);
    socket.set_option(udp_socket::reuse_address{true}, ec);
    socket.bind(endpoint, ec);
    if (ec) {
      ldpp_dout(this, -1) << "failed to bind address " << endpoint
          << " with " << ec.message() << dendl;
      return -ec.value();
    }
    ldpp_dout(this, 20) << "bound " << endpoint << dendl;

    // construct the Listener and start accepting connections
    endpoints.emplace_back(*this, socket.local_endpoint());
    endpoints.back().listener = create_listener(
        endpoints.back().observer, *config, asio::make_strand(context),
        std::move(socket), on_new_stream);
    endpoints.back().listener->async_listen();
  }
  return 0;
}

int Frontend::run()
{
  // the frontend runs on the global execution context
  return 0;
}

void Frontend::stop()
{
  ldpp_dout(this, 1) << "frontend closing listeners" << dendl;

  // close the listeners and their connections
  for (auto& e : endpoints) {
    e.listener->close();
  }
}

void Frontend::join()
{
  ldpp_dout(this, 1) << "frontend joining..." << dendl;

  // acquire an exclusive lock on the pause_mutex to verify that all readers
  // have finished
  error_code ec;
  pause_mutex.lock(ec);
  if (ec) {
    ldpp_dout(this, 1) << "frontend failed to join: " << ec.message() << dendl;
  } else {
    pause_mutex.unlock();
    ldpp_dout(this, 1) << "frontend finished join" << dendl;
  }
}

void Frontend::pause_for_new_config()
{
  ldpp_dout(this, 4) << "frontend pausing connections..." << dendl;

  // close the listeners and their connections
  for (auto& e : endpoints) {
    e.listener->close();
  }

  // pause and wait for outstanding requests to complete
  error_code ec;
  pause_mutex.lock(ec);

  if (ec) {
    ldpp_dout(this, 1) << "frontend failed to pause: " << ec.message() << dendl;
  } else {
    ldpp_dout(this, 4) << "frontend paused" << dendl;
  }
}

void Frontend::unpause_with_new_config()
{
  // unpause to unblock connections
  pause_mutex.unlock();

  // start accepting connections again
  for (auto& e : endpoints) {
    e.listener->async_listen();
  }

  ldpp_dout(this, 4) << "frontend unpaused" << dendl;
}

auto create_frontend(CephContext* cct,
                     const RGWProcessEnv& env,
                     const RGWFrontendConfig* conf,
                     asio::io_context& context)
  -> std::unique_ptr<RGWFrontend>
{
  return std::make_unique<Frontend>(cct, env, conf, context);
}

} // namespace rgw::h3
