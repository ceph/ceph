// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
//
// TSAN reproduction test for https://tracker.ceph.com/issues/75031
//
// This test to demonstrate the race condition in rgw_asio_frontend where
// timeout_timer was constructed with context.get_executor() instead of
// yield.get_executor(). The timeout handler's socket operations race with
// the coroutine's HTTP parsing operations.
//
// Build with -DWITH_TSAN=ON

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

using tcp = boost::asio::ip::tcp;
using namespace std::chrono_literals;

// Mirrors rgw_asio_frontend.cc Connection structure.
// Holds socket + parser state (simulated by parse_state)
struct Connection : boost::intrusive_ref_counter<Connection> {
  tcp::socket socket;

  // Here, just simulating the beast parser internal state (it, last pointers into buffer)
  // mimicking boost::beast::http::parser
  struct ParserState {
    char* it = nullptr;    // current parse position
    char* last = nullptr;  // end of buffer
    int bytes_parsed = 0;
  } parser;

  explicit Connection(tcp::socket&& s) noexcept : socket(std::move(s)) {}
  tcp::socket& get_socket() { return socket; }
};

// Mirrors rgw::timeout_handler from rgw_asio_frontend_timer.h
struct timeout_handler {
  boost::intrusive_ptr<Connection> conn;

  explicit timeout_handler(boost::intrusive_ptr<Connection> c) noexcept
      : conn(std::move(c)) {}

  void operator()(boost::system::error_code ec) {
    if (!ec) { // timeout expired (not canceled)
      boost::system::error_code ec_ignored;
      // These operations mirror rgw_asio_frontend_timer.h:21-24
      conn->get_socket().cancel(ec_ignored);
      conn->get_socket().shutdown(tcp::socket::shutdown_both, ec_ignored);

      // The race condition: timeout handler accesses parser state while coroutine
      // may be actively parsing. In actual/real code, cancel() triggers completion
      // of async_read which accesses parser state.
      conn->parser.it = nullptr;      // RACE: corrupts parser state
      conn->parser.bytes_parsed = -1; // RACE: corrupts parser state
    }
  }
};

static void rethrow(std::exception_ptr e) {
  if (e) std::rethrow_exception(e);
}

// connected socket pair to simulate accept()
std::pair<tcp::socket, tcp::socket> make_socket_pair(boost::asio::io_context& ctx) {
  tcp::acceptor acc(ctx, tcp::endpoint(boost::asio::ip::address_v4::loopback(), 0));
  tcp::socket client(ctx);
  client.connect(acc.local_endpoint());
  return {acc.accept(), std::move(client)};
}

// Issue #75031 Reproducer: timeout_timer uses context.get_executor()
// The timeout handler can run on any thread, racing with the coroutine
int test_issue_producer(int iters, int threads) {
  std::cout << "\n============= SHOULD FAIL: context.get_executor() =============" << std::endl;
  std::cout << "TSAN should report races here.\n" << std::endl;

  std::atomic<int> done{0};

  for (int i = 0; i < iters; ++i) {
    boost::asio::io_context ctx;
    auto [server_sock, client_sock] = make_socket_pair(ctx);
    auto conn = boost::intrusive_ptr{new Connection(std::move(server_sock))};

    boost::asio::spawn(boost::asio::make_strand(ctx),
      [&ctx, &done, conn, &client_sock](boost::asio::yield_context yield) {
        // BUG: Timer uses ctx.get_executor() instead of yield.get_executor()
        // This means timeout_handler runs outside of the strand we created.
        boost::asio::steady_timer timer{ctx.get_executor()};
        timer.expires_after(10us);
        timer.async_wait(timeout_handler{conn});

        // Just simulating http::async_read_header parsing loop
        // In real code, e.g., beast parser updates it/last pointers
        char buf[64];
        for (int j = 0; j < 10; ++j) {
          // Simulate parser state updates during read
          conn->parser.it = buf;              // RACE with timeout_handler
          conn->parser.last = buf + sizeof(buf);
          conn->parser.bytes_parsed += 10;    // RACE with timeout_handler

          // Yield to allow timeout handler to fire
          boost::asio::steady_timer pause{yield.get_executor()};
          pause.expires_after(5us);
          boost::system::error_code ec;
          pause.async_wait(yield[ec]);
        }

        timer.cancel();
        done.fetch_add(1);
      }, rethrow);

    std::vector<std::thread> pool;
    for (int j = 0; j < threads; ++j)
      pool.emplace_back([&ctx]{ ctx.run(); });
    for (auto& th : pool)
      th.join();
  }
  std::cout << "============= Done: " << done.load() << "/" << iters << " =============" << std::endl;
  return 0;
}

// FIX: timeout_timer now uses yield.get_executor()
// The timeout handler is serialized through the strand with the coroutine
int test_fixed(int iters, int threads) {
  std::cout << "\n============= FIX: yield.get_executor() =============" << std::endl;
  std::cout << "TSAN should NOT report races here.\n" << std::endl;

  std::atomic<int> done{0};

  for (int i = 0; i < iters; ++i) {
    boost::asio::io_context ctx;
    auto [server_sock, client_sock] = make_socket_pair(ctx);
    auto conn = boost::intrusive_ptr{new Connection(std::move(server_sock))};

    boost::asio::spawn(boost::asio::make_strand(ctx),
      [&done, conn](boost::asio::yield_context yield) {
        // FIX: Timer uses yield.get_executor() (the strand)
        // timeout_handler is now serialized with the coroutine
        boost::asio::steady_timer timer{yield.get_executor()};
        timer.expires_after(10us);
        timer.async_wait(timeout_handler{conn});

        char buf[64];
        for (int j = 0; j < 10; ++j) {
          conn->parser.it = buf;
          conn->parser.last = buf + sizeof(buf);
          conn->parser.bytes_parsed += 10;

          boost::asio::steady_timer pause{yield.get_executor()};
          pause.expires_after(5us);
          boost::system::error_code ec;
          pause.async_wait(yield[ec]);
        }

        timer.cancel();
        done.fetch_add(1);
      }, rethrow);

    std::vector<std::thread> pool;
    for (int j = 0; j < threads; ++j)
      pool.emplace_back([&ctx]{ ctx.run(); });
    for (auto& th : pool)
      th.join();
  }
  std::cout << "============= Done: " << done.load() << "/" << iters << " =============" << std::endl;
  return 0;
}

void usage(const char* prog) {
  std::cout << "Usage: " << prog << " [failure|fixed|both] [iterations] [threads]\n";
  std::cout << "  failure:  Run only the failure test (expect TSAN warnings)\n";
  std::cout << "  fixed:  Run only the fixed test (expect no warnings)\n";
  std::cout << "  both:   Run both tests (default)\n";
}

int main(int argc, char* argv[]) {
  std::string mode = argc > 1 ? argv[1] : "both";
  int iters = argc > 2 ? std::atoi(argv[2]) : 20;
  int threads = argc > 3 ? std::atoi(argv[3]) : 4;

  std::cout << "TSAN Race Test for rgw_asio_frontend timeout_timer" << std::endl;
  std::cout << "Mode: " << mode << ", Iterations: " << iters << ", Threads: " << threads << "\n";

  if (mode == "failure") {
    return test_issue_producer(iters, threads);
  } else if (mode == "fixed") {
    return test_fixed(iters, threads);
  } else if (mode == "both") {
    test_issue_producer(iters, threads);
    test_fixed(iters, threads);
    std::cout << "\n=== DONE ===" << std::endl;
    return 0;
  } else {
    usage(argv[0]);
    return 1;
  }
}
