// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <boost/optional.hpp>

#include <beast/core/placeholders.hpp>
#include <beast/core/streambuf.hpp>
#include <beast/http/empty_body.hpp>
#include <beast/http/parse_error.hpp>
#include <beast/http/read.hpp>
#include <beast/http/write.hpp>

#include "rgw_asio_frontend.h"
#include "rgw_asio_client.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "asio: ")

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

class AsioConnection : public std::enable_shared_from_this<AsioConnection> {
  RGWProcessEnv& env;
  boost::asio::io_service::strand strand;
  tcp::socket socket;
  tcp::endpoint endpoint;
  beast::streambuf buf;
  beast::http::request_v1<RGWBufferlistBody> request;

 public:
  void on_read(boost::system::error_code ec) {
    auto cct = env.store->ctx();
    if (ec) {
      if (ec.category() == beast::http::get_parse_error_category()) {
        ldout(cct, 1) << "parse failed: " << ec.message() << dendl;
      } else {
        ldout(cct, 1) << "read failed: " << ec.message() << dendl;
      }
      write_bad_request();
      return;
    }
    RGWRequest req{env.store->get_new_req_id()};
    RGWAsioClientIO real_client{std::move(socket), std::move(request)};
    auto real_client_io = rgw::io::add_reordering(
                            rgw::io::add_buffering(
                              rgw::io::add_chunking(
                                rgw::io::add_conlen_controlling(
                                  &real_client))));
    RGWRestfulIO client(&real_client_io);
    process_request(env.store, env.rest, &req, env.uri_prefix, &client,
                    env.olog);
  }

  void write_bad_request() {
    beast::http::response_v1<beast::http::empty_body> response;
    response.status = 400;
    response.reason = "Bad Request";
    /* If the request is so terribly malformed that we can't extract even
     * the protocol version, we will use HTTP/1.1 as a fallback. */
    response.version = request.version ? request.version : 11;
    beast::http::prepare(response);
    beast::http::async_write(socket, std::move(response),
                             std::bind(&AsioConnection::on_write,
                                       shared_from_this(),
                                       beast::asio::placeholders::error));
  }

  void on_write(boost::system::error_code ec) {
    auto cct = env.store->ctx();
    if (ec) {
      ldout(cct, 1) << "write failed: " << ec.message() << dendl;
    }
  }

 public:
  AsioConnection(RGWProcessEnv& env, tcp::socket&& socket)
    : env(env), strand(socket.get_io_service()), socket(std::move(socket))
  {}

  void read() {
    beast::http::async_read(socket, buf, request, strand.wrap(
            std::bind(&AsioConnection::on_read, shared_from_this(),
                      beast::asio::placeholders::error)));
  }
};

class AsioFrontend {
  RGWProcessEnv env;
  boost::asio::io_service service;

  tcp::acceptor acceptor;
  tcp::socket peer_socket;

  std::vector<std::thread> threads;
  Pauser pauser;
  std::atomic<bool> going_down{false};

  CephContext* ctx() const { return env.store->ctx(); }

  void accept(boost::system::error_code ec);

 public:
  AsioFrontend(const RGWProcessEnv& env)
    : env(env), acceptor(service), peer_socket(service) {}

  int init();
  int run();
  void stop();
  void join();
  void pause();
  void unpause(RGWRados *store);
};

int AsioFrontend::init()
{
  auto ep = tcp::endpoint{tcp::v4(), static_cast<unsigned short>(env.port)};
  ldout(ctx(), 4) << "frontend listening on " << ep << dendl;

  acceptor.open(ep.protocol());
  acceptor.set_option(tcp::acceptor::reuse_address(true));
  acceptor.bind(ep);
  acceptor.listen(boost::asio::socket_base::max_connections);
  acceptor.async_accept(peer_socket,
                        [this] (boost::system::error_code ec) {
                          return accept(ec);
                        });
  return 0;
}

void AsioFrontend::accept(boost::system::error_code ec)
{
  if (!acceptor.is_open()) {
    return;
  } else if (ec == boost::asio::error::operation_aborted) {
    return;
  } else if (ec) {
    throw ec;
  }
  auto socket = std::move(peer_socket);

  acceptor.async_accept(peer_socket,
                        [this] (boost::system::error_code ec) {
                          return accept(ec);
                        });

  std::make_shared<AsioConnection>(env, std::move(socket))->read();
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
  acceptor.close(ec); // unblock the run() threads
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
    // stop accepting but leave the port open
    boost::system::error_code ec;
    acceptor.cancel(ec);
  });
  ldout(ctx(), 4) << "frontend paused" << dendl;
}

void AsioFrontend::unpause(RGWRados *store)
{
  env.store = store;
  ldout(ctx(), 4) << "frontend unpaused" << dendl;
  service.reset();
  acceptor.async_accept(peer_socket,
                        [this] (boost::system::error_code ec) {
                          return accept(ec);
                        });
  pauser.unpause();
}

} // anonymous namespace

class RGWAsioFrontend::Impl : public AsioFrontend {
 public:
  Impl(const RGWProcessEnv& env) : AsioFrontend(env) {}
};

RGWAsioFrontend::RGWAsioFrontend(const RGWProcessEnv& env)
  : impl(new Impl(env))
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

void RGWAsioFrontend::unpause_with_new_config(RGWRados *store)
{
  impl->unpause(store);
}
