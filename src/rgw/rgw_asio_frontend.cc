// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include <boost/asio.hpp>

#include "common/errno.h"

#include "rgw_asio_client.h"
#include "rgw_asio_frontend.h"

#define dout_subsys ceph_subsys_rgw

namespace {

class Pauser {
  std::mutex mutex;
  std::condition_variable cond_ready; // signaled on ready==true
  std::condition_variable cond_paused; // signaled on waiters==thread_count
  bool ready{false};
  int waiters{0};
 public:
  template <typename Func>
  void pause(int thread_count, Func&& func);
  void unpause();
  void wait();
};

template <typename Func>
void Pauser::pause(int thread_count, Func&& func)
{
  std::unique_lock<std::mutex> lock(mutex);
  ready = false;
  lock.unlock();

  func();

  // wait for all threads to pause
  lock.lock();
  cond_paused.wait(lock, [=] { return waiters == thread_count; });
}

void Pauser::unpause()
{
  std::lock_guard<std::mutex> lock(mutex);
  ready = true;
  cond_ready.notify_all();
}

void Pauser::wait()
{
  std::unique_lock<std::mutex> lock(mutex);
  ++waiters;
  cond_paused.notify_one(); // notify pause() that we're waiting
  cond_ready.wait(lock, [this] { return ready; }); // wait for unpause()
  --waiters;
}

using tcp = boost::asio::ip::tcp;
namespace beast = boost::beast;

class Connection {
  RGWProcessEnv& env;
  boost::asio::io_service::strand strand;
  tcp::socket socket;

  // references are bound to callbacks for async operations. if a callback
  // function returns without issuing another operation, the reference is
  // dropped and the Connection is deleted/closed
  std::atomic<int> nref{0};
  using Ref = boost::intrusive_ptr<Connection>;

  // limit header to 4k, since we read it all into a single flat_buffer
  static constexpr size_t header_limit = 4096;
  // don't impose a limit on the body, since we read it in pieces
  static constexpr size_t body_limit = std::numeric_limits<size_t>::max();

  beast::flat_buffer buffer;
  boost::optional<rgw::asio::parser_type> parser;

  using bad_response_type = beast::http::response<beast::http::empty_body>;
  boost::optional<bad_response_type> response;

  CephContext* ctx() const { return env.store->ctx(); }

  void read_header() {
    // configure the parser
    parser.emplace();
    parser->header_limit(header_limit);
    parser->body_limit(body_limit);

    // parse the header
    beast::http::async_read_header(socket, buffer, *parser, strand.wrap(
            std::bind(&Connection::on_header, Ref{this},
                      std::placeholders::_1)));
  }

  void discard_unread_message() {
    if (parser->is_done()) {
      // nothing left to discard, start reading the next message
      read_header();
      return;
    }

    // read the rest of the request into a static buffer. multiple clients could
    // write at the same time, but this is okay because we never read it back
    static std::array<char, 1024> discard_buffer;

    auto& body = parser->get().body();
    body.size = discard_buffer.size();
    body.data = discard_buffer.data();

    beast::http::async_read_some(socket, buffer, *parser, strand.wrap(
            std::bind(&Connection::on_discard_unread, Ref{this},
                      std::placeholders::_1)));
  }

  void on_discard_unread(boost::system::error_code ec) {
    if (ec == boost::asio::error::connection_reset) {
      return;
    }
    if (ec) {
      ldout(ctx(), 5) << "discard_unread_message failed: "
          << ec.message() << dendl;
      return;
    }
    discard_unread_message();
  }

  void on_write_error(boost::system::error_code ec) {
    if (ec) {
      ldout(ctx(), 5) << "failed to write response: " << ec.message() << dendl;
    }
  }

  void on_header(boost::system::error_code ec) {
    if (ec == boost::asio::error::connection_reset ||
        ec == beast::http::error::end_of_stream) {
      return;
    }
    if (ec) {
      auto& message = parser->get();
      ldout(ctx(), 1) << "failed to read header: " << ec.message() << dendl;
      ldout(ctx(), 1) << "====== req done http_status=400 ======" << dendl;
      response.emplace();
      response->result(beast::http::status::bad_request);
      response->version(message.version() == 10 ? 10 : 11);
      response->prepare_payload();
      beast::http::async_write(socket, *response, strand.wrap(
            std::bind(&Connection::on_write_error, Ref{this},
                      std::placeholders::_1)));
      return;
    }

    // process the request
    RGWRequest req{env.store->get_new_req_id()};

    rgw::asio::ClientIO real_client{socket, *parser, buffer};

    auto real_client_io = rgw::io::add_reordering(
                            rgw::io::add_buffering(ctx(),
                              rgw::io::add_chunking(
                                rgw::io::add_conlen_controlling(
                                  &real_client))));
    RGWRestfulIO client(ctx(), &real_client_io);
    process_request(env.store, env.rest, &req, env.uri_prefix,
                    *env.auth_registry, &client, env.olog);

    if (parser->keep_alive()) {
      // parse any unread bytes from the previous message (in case we replied
      // before reading the entire body) before reading the next
      discard_unread_message();
    }
  }

 public:
  Connection(RGWProcessEnv& env, tcp::socket&& socket)
    : env(env), strand(socket.get_io_service()), socket(std::move(socket)) {}

  void on_connect() {
    read_header();
  }

  void get() { ++nref; }
  void put() { if (nref.fetch_sub(1) == 1) { delete this; } }

  friend void intrusive_ptr_add_ref(Connection *c) { c->get(); }
  friend void intrusive_ptr_release(Connection *c) { c->put(); }
};

class AsioFrontend {
  RGWProcessEnv env;
  RGWFrontendConfig* conf;
  boost::asio::io_service service;

  struct Listener {
    tcp::endpoint endpoint;
    tcp::acceptor acceptor;
    tcp::socket socket;

    Listener(boost::asio::io_service& service)
      : acceptor(service), socket(service) {}
  };
  std::vector<Listener> listeners;

  std::vector<std::thread> threads;
  Pauser pauser;
  std::atomic<bool> going_down{false};

  CephContext* ctx() const { return env.store->ctx(); }

  void accept(Listener& listener, boost::system::error_code ec);

 public:
  AsioFrontend(const RGWProcessEnv& env, RGWFrontendConfig* conf)
    : env(env), conf(conf) {}

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

tcp::endpoint parse_endpoint(BOOST_ASIO_STRING_VIEW_PARAM input,
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
  return drop_privileges(ctx());
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

  boost::intrusive_ptr<Connection> conn{new Connection(env, std::move(socket))};
  conn->on_connect();
  // reference drops here, but on_connect() takes another
}

int AsioFrontend::run()
{
  auto cct = ctx();
  const int thread_count = cct->_conf->rgw_thread_pool_size;
  threads.reserve(thread_count);

  ldout(cct, 4) << "frontend spawning " << thread_count << " threads" << dendl;

  for (int i = 0; i < thread_count; i++) {
    threads.emplace_back([=] {
      for (;;) {
        service.run();
        if (going_down) {
          break;
        }
        pauser.wait();
      }
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

  // unblock the run() threads
  service.stop();
}

void AsioFrontend::join()
{
  if (!going_down) {
    stop();
  }
  ldout(ctx(), 4) << "frontend joining threads..." << dendl;
  for (auto& thread : threads) {
    thread.join();
  }
  ldout(ctx(), 4) << "frontend done" << dendl;
}

void AsioFrontend::pause()
{
  ldout(ctx(), 4) << "frontend pausing threads..." << dendl;
  pauser.pause(threads.size(), [=] {
    // unblock the run() threads
    service.stop();
  });
  ldout(ctx(), 4) << "frontend paused" << dendl;
}

void AsioFrontend::unpause(RGWRados* const store,
                           rgw_auth_registry_ptr_t auth_registry)
{
  env.store = store;
  env.auth_registry = std::move(auth_registry);
  ldout(ctx(), 4) << "frontend unpaused" << dendl;
  service.reset();
  pauser.unpause();
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
