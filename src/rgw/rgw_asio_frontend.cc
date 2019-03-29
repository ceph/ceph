// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <atomic>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING
#include <boost/asio/spawn.hpp>
#include <boost/intrusive/list.hpp>

#include "common/async/shared_mutex.h"
#include "common/errno.h"

#include "rgw_asio_client.h"
#include "rgw_asio_frontend.h"

#ifdef WITH_RADOSGW_BEAST_OPENSSL
#include <boost/asio/ssl.hpp>
#endif

#include "rgw_dmclock_async_scheduler.h"

#define dout_subsys ceph_subsys_rgw

namespace {

using tcp = boost::asio::ip::tcp;
namespace http = boost::beast::http;
#ifdef WITH_RADOSGW_BEAST_OPENSSL
namespace ssl = boost::asio::ssl;
#endif

template <typename Stream>
class StreamIO : public rgw::asio::ClientIO {
  CephContext* const cct;
  Stream& stream;
  boost::beast::flat_buffer& buffer;
 public:
  StreamIO(CephContext *cct, Stream& stream, rgw::asio::parser_type& parser,
           boost::beast::flat_buffer& buffer, bool is_ssl,
           const tcp::endpoint& local_endpoint,
           const tcp::endpoint& remote_endpoint)
      : ClientIO(parser, is_ssl, local_endpoint, remote_endpoint),
        cct(cct), stream(stream), buffer(buffer)
  {}

  size_t write_data(const char* buf, size_t len) override {
    boost::system::error_code ec;
    auto bytes = boost::asio::write(stream, boost::asio::buffer(buf, len), ec);
    if (ec) {
      ldout(cct, 4) << "write_data failed: " << ec.message() << dendl;
      throw rgw::io::Exception(ec.value(), std::system_category());
    }
    return bytes;
  }

  size_t recv_body(char* buf, size_t max) override {
    auto& message = parser.get();
    auto& body_remaining = message.body();
    body_remaining.data = buf;
    body_remaining.size = max;

    while (body_remaining.size && !parser.is_done()) {
      boost::system::error_code ec;
      http::read_some(stream, buffer, parser, ec);
      if (ec == http::error::partial_message ||
          ec == http::error::need_buffer) {
        break;
      }
      if (ec) {
        ldout(cct, 4) << "failed to read body: " << ec.message() << dendl;
        throw rgw::io::Exception(ec.value(), std::system_category());
      }
    }
    return max - body_remaining.size;
  }
};

using SharedMutex = ceph::async::SharedMutex<boost::asio::io_context::executor_type>;

template <typename Stream>
void handle_connection(RGWProcessEnv& env, Stream& stream,
                       boost::beast::flat_buffer& buffer, bool is_ssl,
                       SharedMutex& pause_mutex,
                       rgw::dmclock::Scheduler *scheduler,
                       boost::system::error_code& ec,
                       boost::asio::yield_context yield)
{
  // limit header to 4k, since we read it all into a single flat_buffer
  static constexpr size_t header_limit = 4096;
  // don't impose a limit on the body, since we read it in pieces
  static constexpr size_t body_limit = std::numeric_limits<size_t>::max();

  auto cct = env.store->ctx();

  // read messages from the stream until eof
  for (;;) {
    // configure the parser
    rgw::asio::parser_type parser;
    parser.header_limit(header_limit);
    parser.body_limit(body_limit);

    // parse the header
    http::async_read_header(stream, buffer, parser, yield[ec]);
    if (ec == boost::asio::error::connection_reset ||
        ec == boost::asio::error::bad_descriptor ||
        ec == boost::asio::error::operation_aborted ||
#ifdef WITH_RADOSGW_BEAST_OPENSSL
        ec == ssl::error::stream_truncated ||
#endif
        ec == http::error::end_of_stream) {
      ldout(cct, 20) << "failed to read header: " << ec.message() << dendl;
      return;
    }
    if (ec) {
      ldout(cct, 1) << "failed to read header: " << ec.message() << dendl;
      auto& message = parser.get();
      http::response<http::empty_body> response;
      response.result(http::status::bad_request);
      response.version(message.version() == 10 ? 10 : 11);
      response.prepare_payload();
      http::async_write(stream, response, yield[ec]);
      if (ec) {
        ldout(cct, 5) << "failed to write response: " << ec.message() << dendl;
      }
      ldout(cct, 1) << "====== req done http_status=400 ======" << dendl;
      return;
    }

    {
      auto lock = pause_mutex.async_lock_shared(yield[ec]);
      if (ec == boost::asio::error::operation_aborted) {
        return;
      } else if (ec) {
        ldout(cct, 1) << "failed to lock: " << ec.message() << dendl;
        return;
      }

      // process the request
      RGWRequest req{env.store->get_new_req_id()};

      auto& socket = stream.lowest_layer();
      StreamIO real_client{cct, stream, parser, buffer, is_ssl,
                           socket.local_endpoint(),
                           socket.remote_endpoint()};

      auto real_client_io = rgw::io::add_reordering(
                              rgw::io::add_buffering(cct,
                                rgw::io::add_chunking(
                                  rgw::io::add_conlen_controlling(
                                    &real_client))));
      RGWRestfulIO client(cct, &real_client_io);
      auto y = optional_yield{socket.get_io_context(), yield};
      process_request(env.store, env.rest, &req, env.uri_prefix,
                      *env.auth_registry, &client, env.olog, y, scheduler);
    }

    if (!parser.keep_alive()) {
      return;
    }

    // if we failed before reading the entire message, discard any remaining
    // bytes before reading the next
    while (!parser.is_done()) {
      static std::array<char, 1024> discard_buffer;

      auto& body = parser.get().body();
      body.size = discard_buffer.size();
      body.data = discard_buffer.data();

      http::async_read_some(stream, buffer, parser, yield[ec]);
      if (ec == boost::asio::error::connection_reset) {
        return;
      }
      if (ec) {
        ldout(cct, 5) << "failed to discard unread message: "
            << ec.message() << dendl;
        return;
      }
    }
  }
}

struct Connection : boost::intrusive::list_base_hook<> {
  tcp::socket& socket;
  Connection(tcp::socket& socket) : socket(socket) {}
};

class ConnectionList {
  using List = boost::intrusive::list<Connection>;
  List connections;
  std::mutex mutex;

  void remove(Connection& c) {
    std::lock_guard lock{mutex};
    if (c.is_linked()) {
      connections.erase(List::s_iterator_to(c));
    }
  }
 public:
  class Guard {
    ConnectionList *list;
    Connection *conn;
   public:
    Guard(ConnectionList *list, Connection *conn) : list(list), conn(conn) {}
    ~Guard() { list->remove(*conn); }
  };
  [[nodiscard]] Guard add(Connection& conn) {
    std::lock_guard lock{mutex};
    connections.push_back(conn);
    return Guard{this, &conn};
  }
  void close(boost::system::error_code& ec) {
    std::lock_guard lock{mutex};
    for (auto& conn : connections) {
      conn.socket.close(ec);
    }
    connections.clear();
  }
};

namespace dmc = rgw::dmclock;
class AsioFrontend {
  RGWProcessEnv env;
  RGWFrontendConfig* conf;
  boost::asio::io_context context;
#ifdef WITH_RADOSGW_BEAST_OPENSSL
  boost::optional<ssl::context> ssl_context;
  int init_ssl();
#endif
  SharedMutex pause_mutex;
  std::unique_ptr<rgw::dmclock::Scheduler> scheduler;

  struct Listener {
    tcp::endpoint endpoint;
    tcp::acceptor acceptor;
    tcp::socket socket;
    bool use_ssl = false;
    bool use_nodelay = false;

    explicit Listener(boost::asio::io_context& context)
      : acceptor(context), socket(context) {}
  };
  std::vector<Listener> listeners;

  ConnectionList connections;

  // work guard to keep run() threads busy while listeners are paused
  using Executor = boost::asio::io_context::executor_type;
  std::optional<boost::asio::executor_work_guard<Executor>> work;

  std::vector<std::thread> threads;
  std::atomic<bool> going_down{false};

  CephContext* ctx() const { return env.store->ctx(); }
  std::optional<dmc::ClientCounters> client_counters;
  std::unique_ptr<dmc::ClientConfig> client_config;
  void accept(Listener& listener, boost::system::error_code ec);

 public:
  AsioFrontend(const RGWProcessEnv& env, RGWFrontendConfig* conf,
	       dmc::SchedulerCtx& sched_ctx)
    : env(env), conf(conf), pause_mutex(context.get_executor())
  {
    auto sched_t = dmc::get_scheduler_t(ctx());
    switch(sched_t){
    case dmc::scheduler_t::dmclock:
      scheduler.reset(new dmc::AsyncScheduler(ctx(),
                                              context,
                                              std::ref(sched_ctx.get_dmc_client_counters()),
                                              sched_ctx.get_dmc_client_config(),
                                              *sched_ctx.get_dmc_client_config(),
                                              dmc::AtLimit::Reject));
      break;
    case dmc::scheduler_t::none:
      lderr(ctx()) << "Got invalid scheduler type for beast, defaulting to throttler" << dendl;
      [[fallthrough]];
    case dmc::scheduler_t::throttler:
      scheduler.reset(new dmc::SimpleThrottler(ctx()));

    }
  }

  int init();
  int run();
  void stop();
  void join();
  void pause();
  void unpause(RGWRados* store, rgw_auth_registry_ptr_t);
};

unsigned short parse_port(const char *input, boost::system::error_code& ec)
{
  char *end = nullptr;
  auto port = std::strtoul(input, &end, 10);
  if (port > std::numeric_limits<unsigned short>::max()) {
    ec.assign(ERANGE, boost::system::system_category());
  } else if (port == 0 && end == input) {
    ec.assign(EINVAL, boost::system::system_category());
  }
  return port;
}
	
tcp::endpoint parse_endpoint(boost::asio::string_view input,
                             unsigned short default_port,
                             boost::system::error_code& ec)
{
  tcp::endpoint endpoint;

  if (input.empty()) {
    ec = boost::asio::error::invalid_argument;
    return endpoint;
  }

  if (input[0] == '[') { // ipv6
    const size_t addr_begin = 1;
    const size_t addr_end = input.find(']');
    if (addr_end == input.npos) { // no matching ]
      ec = boost::asio::error::invalid_argument;
      return endpoint;
    }
    if (addr_end + 1 < input.size()) {
      // :port must must follow [ipv6]
      if (input[addr_end + 1] != ':') {
        ec = boost::asio::error::invalid_argument;
        return endpoint;
      } else {
        auto port_str = input.substr(addr_end + 2);
        endpoint.port(parse_port(port_str.data(), ec));
      }
    } else {
      endpoint.port(default_port);
    }
    auto addr = input.substr(addr_begin, addr_end - addr_begin);
    endpoint.address(boost::asio::ip::make_address_v6(addr, ec));
  } else { // ipv4
    auto colon = input.find(':');
    if (colon != input.npos) {
      auto port_str = input.substr(colon + 1);
      endpoint.port(parse_port(port_str.data(), ec));
      if (ec) {
        return endpoint;
      }
    } else {
      endpoint.port(default_port);
    }
    auto addr = input.substr(0, colon);
    endpoint.address(boost::asio::ip::make_address_v4(addr, ec));
  }
  return endpoint;
}

static int drop_privileges(CephContext *ctx)
{
  uid_t uid = ctx->get_set_uid();
  gid_t gid = ctx->get_set_gid();
  std::string uid_string = ctx->get_set_uid_string();
  std::string gid_string = ctx->get_set_gid_string();
  if (gid && setgid(gid) != 0) {
    int err = errno;
    ldout(ctx, -1) << "unable to setgid " << gid << ": " << cpp_strerror(err) << dendl;
    return -err;
  }
  if (uid && setuid(uid) != 0) {
    int err = errno;
    ldout(ctx, -1) << "unable to setuid " << uid << ": " << cpp_strerror(err) << dendl;
    return -err;
  }
  if (uid && gid) {
    ldout(ctx, 0) << "set uid:gid to " << uid << ":" << gid
                  << " (" << uid_string << ":" << gid_string << ")" << dendl;
  }
  return 0;
}

int AsioFrontend::init()
{
  boost::system::error_code ec;
  auto& config = conf->get_config_map();

#ifdef WITH_RADOSGW_BEAST_OPENSSL
  int r = init_ssl();
  if (r < 0) {
    return r;
  }
#endif

  // parse endpoints
  auto ports = config.equal_range("port");
  for (auto i = ports.first; i != ports.second; ++i) {
    auto port = parse_port(i->second.c_str(), ec);
    if (ec) {
      lderr(ctx()) << "failed to parse port=" << i->second << dendl;
      return -ec.value();
    }
    listeners.emplace_back(context);
    listeners.back().endpoint.port(port);
  }

  auto endpoints = config.equal_range("endpoint");
  for (auto i = endpoints.first; i != endpoints.second; ++i) {
    auto endpoint = parse_endpoint(i->second, 80, ec);
    if (ec) {
      lderr(ctx()) << "failed to parse endpoint=" << i->second << dendl;
      return -ec.value();
    }
    listeners.emplace_back(context);
    listeners.back().endpoint = endpoint;
  }
  // parse tcp nodelay
  auto nodelay = config.find("tcp_nodelay");
  if (nodelay != config.end()) {
    for (auto& l : listeners) {
      l.use_nodelay = (nodelay->second == "1");
    }
  }
  
  // start listeners
  for (auto& l : listeners) {
    l.acceptor.open(l.endpoint.protocol(), ec);
    if (ec) {
      lderr(ctx()) << "failed to open socket: " << ec.message() << dendl;
      return -ec.value();
    }
    l.acceptor.set_option(tcp::acceptor::reuse_address(true));
    l.acceptor.bind(l.endpoint, ec);
    if (ec) {
      lderr(ctx()) << "failed to bind address " << l.endpoint
          << ": " << ec.message() << dendl;
      return -ec.value();
    }
    l.acceptor.listen(boost::asio::socket_base::max_connections);
    l.acceptor.async_accept(l.socket,
                            [this, &l] (boost::system::error_code ec) {
                              accept(l, ec);
                            });

    ldout(ctx(), 4) << "frontend listening on " << l.endpoint << dendl;
  }
  return drop_privileges(ctx());
}

#ifdef WITH_RADOSGW_BEAST_OPENSSL
int AsioFrontend::init_ssl()
{
  boost::system::error_code ec;
  auto& config = conf->get_config_map();

  // ssl configuration
  auto cert = config.find("ssl_certificate");
  const bool have_cert = cert != config.end();
  if (have_cert) {
    // only initialize the ssl context if it's going to be used
    ssl_context = boost::in_place(ssl::context::tls);
  }

  auto key = config.find("ssl_private_key");
  const bool have_private_key = key != config.end();
  if (have_private_key) {
    if (!have_cert) {
      lderr(ctx()) << "no ssl_certificate configured for ssl_private_key" << dendl;
      return -EINVAL;
    }
    ssl_context->use_private_key_file(key->second, ssl::context::pem, ec);
    if (ec) {
      lderr(ctx()) << "failed to add ssl_private_key=" << key->second
          << ": " << ec.message() << dendl;
      return -ec.value();
    }
  }
  if (have_cert) {
    ssl_context->use_certificate_chain_file(cert->second, ec);
    if (ec) {
      lderr(ctx()) << "failed to use ssl_certificate=" << cert->second
          << ": " << ec.message() << dendl;
      return -ec.value();
    }
    if (!have_private_key) {
      // attempt to use it as a private key if a separate one wasn't provided
      ssl_context->use_private_key_file(cert->second, ssl::context::pem, ec);
      if (ec) {
        lderr(ctx()) << "failed to use ssl_certificate=" << cert->second
            << " as a private key: " << ec.message() << dendl;
        return -ec.value();
      }
    }
  }

  // parse ssl endpoints
  auto ports = config.equal_range("ssl_port");
  for (auto i = ports.first; i != ports.second; ++i) {
    if (!have_cert) {
      lderr(ctx()) << "no ssl_certificate configured for ssl_port" << dendl;
      return -EINVAL;
    }
    auto port = parse_port(i->second.c_str(), ec);
    if (ec) {
      lderr(ctx()) << "failed to parse ssl_port=" << i->second << dendl;
      return -ec.value();
    }
    listeners.emplace_back(context);
    listeners.back().endpoint.port(port);
    listeners.back().use_ssl = true;
  }

  auto endpoints = config.equal_range("ssl_endpoint");
  for (auto i = endpoints.first; i != endpoints.second; ++i) {
    if (!have_cert) {
      lderr(ctx()) << "no ssl_certificate configured for ssl_endpoint" << dendl;
      return -EINVAL;
    }
    auto endpoint = parse_endpoint(i->second, 443, ec);
    if (ec) {
      lderr(ctx()) << "failed to parse ssl_endpoint=" << i->second << dendl;
      return -ec.value();
    }
    listeners.emplace_back(context);
    listeners.back().endpoint = endpoint;
    listeners.back().use_ssl = true;
  }
  return 0;
}
#endif // WITH_RADOSGW_BEAST_OPENSSL

void AsioFrontend::accept(Listener& l, boost::system::error_code ec)
{
  if (!l.acceptor.is_open()) {
    return;
  } else if (ec == boost::asio::error::operation_aborted) {
    return;
  } else if (ec) {
    throw ec;
  }
  auto socket = std::move(l.socket);
  tcp::no_delay options(l.use_nodelay);
  socket.set_option(options,ec);
  l.acceptor.async_accept(l.socket,
                          [this, &l] (boost::system::error_code ec) {
                            accept(l, ec);
                          });

  // spawn a coroutine to handle the connection
#ifdef WITH_RADOSGW_BEAST_OPENSSL
  if (l.use_ssl) {
    boost::asio::spawn(context,
      [this, s=std::move(socket)] (boost::asio::yield_context yield) mutable {
        Connection conn{s};
        auto c = connections.add(conn);
        // wrap the socket in an ssl stream
        ssl::stream<tcp::socket&> stream{s, *ssl_context};
        boost::beast::flat_buffer buffer;
        // do ssl handshake
        boost::system::error_code ec;
        auto bytes = stream.async_handshake(ssl::stream_base::server,
                                            buffer.data(), yield[ec]);
        if (ec) {
          ldout(ctx(), 1) << "ssl handshake failed: " << ec.message() << dendl;
          return;
        }
        buffer.consume(bytes);
        handle_connection(env, stream, buffer, true, pause_mutex,
                          scheduler.get(), ec, yield);
        if (!ec) {
          // ssl shutdown (ignoring errors)
          stream.async_shutdown(yield[ec]);
        }
        s.shutdown(tcp::socket::shutdown_both, ec);
      });
  } else {
#else
  {
#endif // WITH_RADOSGW_BEAST_OPENSSL
    boost::asio::spawn(context,
      [this, s=std::move(socket)] (boost::asio::yield_context yield) mutable {
        Connection conn{s};
        auto c = connections.add(conn);
        boost::beast::flat_buffer buffer;
        boost::system::error_code ec;
        handle_connection(env, s, buffer, false, pause_mutex,
                          scheduler.get(), ec, yield);
        s.shutdown(tcp::socket::shutdown_both, ec);
      });
  }
}

int AsioFrontend::run()
{
  auto cct = ctx();
  const int thread_count = cct->_conf->rgw_thread_pool_size;
  threads.reserve(thread_count);

  ldout(cct, 4) << "frontend spawning " << thread_count << " threads" << dendl;

  // the worker threads call io_context::run(), which will return when there's
  // no work left. hold a work guard to keep these threads going until join()
  work.emplace(boost::asio::make_work_guard(context));

  for (int i = 0; i < thread_count; i++) {
    threads.emplace_back([=] {
      // request warnings on synchronous librados calls in this thread
      is_asio_thread = true;
      boost::system::error_code ec;
      context.run(ec);
    });
  }
  return 0;
}

void AsioFrontend::stop()
{
  ldout(ctx(), 4) << "frontend initiating shutdown..." << dendl;

  going_down = true;

  boost::system::error_code ec;
  // close all listeners
  for (auto& listener : listeners) {
    listener.acceptor.close(ec);
  }
  // close all connections
  connections.close(ec);
  pause_mutex.cancel();
}

void AsioFrontend::join()
{
  if (!going_down) {
    stop();
  }
  work.reset();

  ldout(ctx(), 4) << "frontend joining threads..." << dendl;
  for (auto& thread : threads) {
    thread.join();
  }
  ldout(ctx(), 4) << "frontend done" << dendl;
}

void AsioFrontend::pause()
{
  ldout(ctx(), 4) << "frontend pausing connections..." << dendl;

  // cancel pending calls to accept(), but don't close the sockets
  boost::system::error_code ec;
  for (auto& l : listeners) {
    l.acceptor.cancel(ec);
  }

  // pause and wait for outstanding requests to complete
  pause_mutex.lock(ec);

  if (ec) {
    ldout(ctx(), 1) << "frontend failed to pause: " << ec.message() << dendl;
  } else {
    ldout(ctx(), 4) << "frontend paused" << dendl;
  }
}

void AsioFrontend::unpause(RGWRados* const store,
                           rgw_auth_registry_ptr_t auth_registry)
{
  env.store = store;
  env.auth_registry = std::move(auth_registry);

  // unpause to unblock connections
  pause_mutex.unlock();

  // start accepting connections again
  for (auto& l : listeners) {
    l.acceptor.async_accept(l.socket,
                            [this, &l] (boost::system::error_code ec) {
                              accept(l, ec);
                            });
  }

  ldout(ctx(), 4) << "frontend unpaused" << dendl;
}

} // anonymous namespace

class RGWAsioFrontend::Impl : public AsioFrontend {
 public:
  Impl(const RGWProcessEnv& env, RGWFrontendConfig* conf,
       rgw::dmclock::SchedulerCtx& sched_ctx)
    : AsioFrontend(env, conf, sched_ctx) {}
};

RGWAsioFrontend::RGWAsioFrontend(const RGWProcessEnv& env,
                                 RGWFrontendConfig* conf,
				 rgw::dmclock::SchedulerCtx& sched_ctx)
  : impl(new Impl(env, conf, sched_ctx))
{
}

RGWAsioFrontend::~RGWAsioFrontend() = default;

int RGWAsioFrontend::init()
{
  return impl->init();
}

int RGWAsioFrontend::run()
{
  return impl->run();
}

void RGWAsioFrontend::stop()
{
  impl->stop();
}

void RGWAsioFrontend::join()
{
  impl->join();
}

void RGWAsioFrontend::pause_for_new_config()
{
  impl->pause();
}

void RGWAsioFrontend::unpause_with_new_config(
  RGWRados* const store,
  rgw_auth_registry_ptr_t auth_registry
) {
  impl->unpause(store, std::move(auth_registry));
}
