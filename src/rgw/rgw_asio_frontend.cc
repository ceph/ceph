// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>

#include "include/scope_guard.h"
#include "common/async/throttle.h"

#include "rgw_asio_client.h"
#include "rgw_asio_frontend.h"

#define dout_subsys ceph_subsys_rgw

namespace {

using tcp = boost::asio::ip::tcp;
namespace beast = boost::beast;

using Throttle = ceph::async::Throttle<boost::asio::io_context::executor_type>;

void handle_connection(RGWProcessEnv& env, tcp::socket& socket,
                       Throttle& request_throttle,
                       boost::asio::yield_context yield)
{
  // limit header to 4k, since we read it all into a single flat_buffer
  static constexpr size_t header_limit = 4096;
  // don't impose a limit on the body, since we read it in pieces
  static constexpr size_t body_limit = std::numeric_limits<size_t>::max();

  auto cct = env.store->ctx();
  boost::system::error_code ec;
  beast::flat_buffer buffer;

  // read messages from the socket until eof
  for (;;) {
    // configure the parser
    rgw::asio::parser_type parser;
    parser.header_limit(header_limit);
    parser.body_limit(body_limit);

    // parse the header
    beast::http::async_read_header(socket, buffer, parser, yield[ec]);
    if (ec == boost::asio::error::connection_reset ||
        ec == beast::http::error::end_of_stream) {
      return;
    }
    if (ec) {
      ldout(cct, 1) << "failed to read header: " << ec.message() << dendl;
      auto& message = parser.get();
      beast::http::response<beast::http::empty_body> response;
      response.result(beast::http::status::bad_request);
      response.version(message.version() == 10 ? 10 : 11);
      response.prepare_payload();
      beast::http::async_write(socket, response, yield[ec]);
      if (ec) {
        ldout(cct, 5) << "failed to write response: " << ec.message() << dendl;
      }
      ldout(cct, 1) << "====== req done http_status=400 ======" << dendl;
      return;
    }

    // wait for request throttle or frontend unpause
    request_throttle.async_get(1, yield[ec]);
    if (ec == boost::asio::error::operation_aborted) {
      return;
    } else if (ec) {
      ldout(cct, 1) << "failed to wait for request throttle: " << ec.message() << dendl;
      return;
    }
    {
      // return throttle when process_request() completes
      auto g = make_scope_guard([&] { request_throttle.put(1); });

      // process the request
      RGWRequest req{env.store->get_new_req_id()};

      rgw::asio::ClientIO real_client{socket, parser, buffer};

      auto real_client_io = rgw::io::add_reordering(
          rgw::io::add_buffering(cct,
                                 rgw::io::add_chunking(
                                     rgw::io::add_conlen_controlling(
                                         &real_client))));
      RGWRestfulIO client(cct, &real_client_io);
      process_request(env.store, env.rest, &req, env.uri_prefix,
                      *env.auth_registry, &client, env.olog);
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

      beast::http::async_read_some(socket, buffer, parser, yield[ec]);
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

class AsioFrontend : public md_config_obs_t {
  RGWProcessEnv env;
  RGWFrontendConfig* conf;
  boost::asio::io_context service;

  struct Listener {
    tcp::endpoint endpoint;
    tcp::acceptor acceptor;
    tcp::socket socket;

    Listener(boost::asio::io_context& service)
      : acceptor(service), socket(service) {}
  };
  std::vector<Listener> listeners;

  // work guard to keep run() threads busy while listeners are paused
  using Executor = boost::asio::io_context::executor_type;
  std::optional<boost::asio::executor_work_guard<Executor>> work;

  Throttle request_throttle;
  std::vector<std::thread> threads;
  std::atomic<bool> going_down{false};

  struct Pauser {
    std::mutex mutex;
    bool paused{false};
    size_t max_requests{0};
  } pauser;

  CephContext* ctx() const { return env.store->ctx(); }

  void accept(Listener& listener, boost::system::error_code ec);

  // config observer
  const char **get_tracked_conf_keys() const override;
  void handle_conf_change(const md_config_t *conf,
                          const std::set<std::string>& changed) override;

 public:
  AsioFrontend(const RGWProcessEnv& env, RGWFrontendConfig* conf);
  ~AsioFrontend();

  int init();
  int run();
  void stop();
  void join();
  void pause();
  void unpause(RGWRados* store, rgw_auth_registry_ptr_t);
};

const auto build_counters = ceph::async::throttle_counters::build;

AsioFrontend::AsioFrontend(const RGWProcessEnv& env, RGWFrontendConfig* conf)
  : env(env), conf(conf),
    request_throttle(service.get_executor(), 0,
                     build_counters(env.store->ctx(),
                                    "throttle-frontend-requests"))
{
  auto _conf = env.store->ctx()->_conf;
  _conf->add_observer(this);
  pauser.max_requests = _conf->get_val<int64_t>("rgw_max_concurrent_requests");
}

AsioFrontend::~AsioFrontend()
{
  env.store->ctx()->_conf->remove_observer(this);
}

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
                             boost::system::error_code& ec)
{
  tcp::endpoint endpoint;

  auto colon = input.find(':');
  if (colon != input.npos) {
    auto port_str = input.substr(colon + 1);
    endpoint.port(parse_port(port_str.data(), ec));
  } else {
    endpoint.port(80);
  }
  if (!ec) {
    auto addr = input.substr(0, colon);
    endpoint.address(boost::asio::ip::make_address(addr, ec));
  }
  return endpoint;
}

int AsioFrontend::init()
{
  boost::system::error_code ec;
  auto& config = conf->get_config_map();

  // parse endpoints
  auto range = config.equal_range("port");
  for (auto i = range.first; i != range.second; ++i) {
    auto port = parse_port(i->second.c_str(), ec);
    if (ec) {
      lderr(ctx()) << "failed to parse port=" << i->second << dendl;
      return -ec.value();
    }
    listeners.emplace_back(service);
    listeners.back().endpoint.port(port);
  }

  range = config.equal_range("endpoint");
  for (auto i = range.first; i != range.second; ++i) {
    auto endpoint = parse_endpoint(i->second, ec);
    if (ec) {
      lderr(ctx()) << "failed to parse endpoint=" << i->second << dendl;
      return -ec.value();
    }
    listeners.emplace_back(service);
    listeners.back().endpoint = endpoint;
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
  return 0;
}

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
  l.acceptor.async_accept(l.socket,
                          [this, &l] (boost::system::error_code ec) {
                            accept(l, ec);
                          });

  // spawn a coroutine to handle the connection
  boost::asio::spawn(service,
    [this, socket=std::move(socket)] (boost::asio::yield_context yield) mutable {
      handle_connection(env, socket, request_throttle, yield);
    });
}

int AsioFrontend::run()
{
  auto cct = ctx();
  const int thread_count = cct->_conf->rgw_thread_pool_size;
  threads.reserve(thread_count);

  ldout(cct, 4) << "frontend spawning " << thread_count << " threads" << dendl;

  // the worker threads call io_context::run(), which will return when there's
  // no work left. hold a work guard to keep these threads going until join()
  work.emplace(boost::asio::make_work_guard(service));

  for (int i = 0; i < thread_count; i++) {
    threads.emplace_back([=] {
      boost::system::error_code ec;
      service.run(ec);
    });
  }

  request_throttle.set_maximum(pauser.max_requests);
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

  // unblock the run() threads
  service.stop();
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

const char** AsioFrontend::get_tracked_conf_keys() const
{
  static const char* keys[] = {
    "rgw_max_concurrent_requests",
    nullptr
  };
  return keys;
}

void AsioFrontend::handle_conf_change(const md_config_t *conf,
                                      const std::set<std::string>& changed)
{
  if (changed.count("rgw_max_concurrent_requests")) {
    std::lock_guard lock{pauser.mutex};
    pauser.max_requests = conf->get_val<int64_t>("rgw_max_concurrent_requests");
    if (pauser.max_requests == 0) { // 0 = unlimited
      pauser.max_requests = std::numeric_limits<size_t>::max();
    }
    if (!pauser.paused) { // don't apply until unpause
      request_throttle.set_maximum(pauser.max_requests);
    }
  }
}

void AsioFrontend::pause()
{
  ldout(ctx(), 4) << "frontend pausing connections..." << dendl;

  // cancel pending calls to accept(), but don't close the sockets
  boost::system::error_code ec;
  for (auto& l : listeners) {
    l.acceptor.cancel(ec);
  }

  auto& mutex = pauser.mutex;
  std::unique_lock lock{mutex};
  pauser.paused = true;

  std::condition_variable cond;
  std::optional<boost::system::error_code> result;

  // set max throttle to 0 and wait for outstanding requests to complete
  request_throttle.async_set_maximum(0, [&] (boost::system::error_code ec) {
      std::lock_guard lock{mutex};
      result = ec;
      cond.notify_one();
    });

  cond.wait(lock, [&] { return result; });

  if (*result) {
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

  size_t max_requests;
  {
    std::lock_guard lock{pauser.mutex};
    pauser.paused = false;
    max_requests = pauser.max_requests;
  }
  // restore max throttle to unblock connections
  request_throttle.set_maximum(max_requests);

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
  Impl(const RGWProcessEnv& env, RGWFrontendConfig* conf) : AsioFrontend(env, conf) {}
};

RGWAsioFrontend::RGWAsioFrontend(const RGWProcessEnv& env,
                                 RGWFrontendConfig* conf)
  : impl(new Impl(env, conf))
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
