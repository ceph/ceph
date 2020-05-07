// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/app-template.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>

#include "crimson/common/log.h"
#include "crimson/net/Errors.h"
#include "crimson/net/Fwd.h"
#include "crimson/net/Socket.h"

namespace {

using seastar::engine;
using seastar::future;
using crimson::net::error;
using crimson::net::FixedCPUServerSocket;
using crimson::net::Socket;
using crimson::net::SocketRef;
using crimson::net::stop_t;

using SocketFRef = seastar::foreign_ptr<SocketRef>;

static seastar::logger logger{"crimsontest"};
static entity_addr_t server_addr = [] {
  entity_addr_t saddr;
  saddr.parse("127.0.0.1:9020", nullptr);
  return saddr;
} ();

future<SocketRef> socket_connect() {
  logger.debug("socket_connect()...");
  return Socket::connect(server_addr).then([] (auto socket) {
    logger.debug("socket_connect() connected");
    return socket;
  });
}

future<> test_refused() {
  logger.info("test_refused()...");
  return socket_connect().discard_result().then([] {
    ceph_abort_msg("connection is not refused");
  }).handle_exception_type([] (const std::system_error& e) {
    if (e.code() != std::errc::connection_refused) {
      logger.error("test_refused() got unexpeted error {}", e);
      ceph_abort();
    } else {
      logger.info("test_refused() ok\n");
    }
  }).handle_exception([] (auto eptr) {
    logger.error("test_refused() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_bind_same() {
  logger.info("test_bind_same()...");
  return FixedCPUServerSocket::create().then([] (auto pss1) {
    return pss1->listen(server_addr).then([] {
      // try to bind the same address
      return FixedCPUServerSocket::create().then([] (auto pss2) {
        return pss2->listen(server_addr).then([] {
          ceph_abort("Should raise address_in_use!");
        }).handle_exception_type([] (const std::system_error& e) {
          assert(e.code() == std::errc::address_in_use);
          // successful!
        }).finally([pss2] {
          return pss2->destroy();
        }).handle_exception_type([] (const std::system_error& e) {
          if (e.code() != std::errc::address_in_use) {
            logger.error("test_bind_same() got unexpeted error {}", e);
            ceph_abort();
          } else {
            logger.info("test_bind_same() ok\n");
          }
        });
      });
    }).finally([pss1] {
      return pss1->destroy();
    }).handle_exception([] (auto eptr) {
      logger.error("test_bind_same() got unexpeted exception {}", eptr);
      ceph_abort();
    });
  });
}

future<> test_accept() {
  logger.info("test_accept()");
  return FixedCPUServerSocket::create().then([] (auto pss) {
    return pss->listen(server_addr).then([pss] {
      return pss->accept([] (auto socket, auto paddr) {
        // simple accept
        return seastar::sleep(100ms).then([socket = std::move(socket)] () mutable {
          return socket->close().finally([cleanup = std::move(socket)] {});
        });
      });
    }).then([] {
      return seastar::when_all(
        socket_connect().then([] (auto socket) {
          return socket->close().finally([cleanup = std::move(socket)] {}); }),
        socket_connect().then([] (auto socket) {
          return socket->close().finally([cleanup = std::move(socket)] {}); }),
        socket_connect().then([] (auto socket) {
          return socket->close().finally([cleanup = std::move(socket)] {}); })
      ).discard_result();
    }).then([] {
      // should be enough to be connected locally
      return seastar::sleep(50ms);
    }).then([] {
      logger.info("test_accept() ok\n");
    }).finally([pss] {
      return pss->destroy();
    }).handle_exception([] (auto eptr) {
      logger.error("test_accept() got unexpeted exception {}", eptr);
      ceph_abort();
    });
  });
}

class SocketFactory {
  SocketRef client_socket;
  SocketFRef server_socket;
  FixedCPUServerSocket *pss = nullptr;
  seastar::promise<> server_connected;

 public:
  // cb_client() on CPU#0, cb_server() on CPU#1
  template <typename FuncC, typename FuncS>
  static future<> dispatch_sockets(FuncC&& cb_client, FuncS&& cb_server) {
    assert(seastar::this_shard_id() == 0u);
    auto owner = std::make_unique<SocketFactory>();
    auto psf = owner.get();
    return seastar::smp::submit_to(1u, [psf] {
      return FixedCPUServerSocket::create().then([psf] (auto pss) {
        psf->pss = pss;
        return pss->listen(server_addr);
      });
    }).then([psf] {
      return seastar::when_all_succeed(
        seastar::smp::submit_to(0u, [psf] {
          return socket_connect().then([psf] (auto socket) {
            psf->client_socket = std::move(socket);
          });
        }),
        seastar::smp::submit_to(1u, [psf] {
          return psf->pss->accept([psf] (auto socket, auto paddr) {
            psf->server_socket = seastar::make_foreign(std::move(socket));
            return seastar::smp::submit_to(0u, [psf] {
              psf->server_connected.set_value();
            });
          });
        })
      );
    }).then([psf] {
      return psf->server_connected.get_future();
    }).finally([psf] {
      if (psf->pss) {
        return seastar::smp::submit_to(1u, [psf] {
          return psf->pss->destroy();
        });
      }
      return seastar::now();
    }).then([psf,
             cb_client = std::move(cb_client),
             cb_server = std::move(cb_server)] () mutable {
      logger.debug("dispatch_sockets(): client/server socket are ready");
      return seastar::when_all_succeed(
        seastar::smp::submit_to(0u, [socket = psf->client_socket.get(),
                                     cb_client = std::move(cb_client)] {
          return cb_client(socket).finally([socket] {
            logger.debug("closing client socket...");
            return socket->close();
          }).handle_exception([] (auto eptr) {
            logger.error("dispatch_sockets():"
                " cb_client() got unexpeted exception {}", eptr);
            ceph_abort();
          });
        }),
        seastar::smp::submit_to(1u, [socket = psf->server_socket.get(),
                                     cb_server = std::move(cb_server)] {
          return cb_server(socket).finally([socket] {
            logger.debug("closing server socket...");
            return socket->close();
          }).handle_exception([] (auto eptr) {
            logger.error("dispatch_sockets():"
                " cb_server() got unexpeted exception {}", eptr);
            ceph_abort();
          });
        })
      );
    }).finally([cleanup = std::move(owner)] {});
  }
};

class Connection {
  static const uint64_t DATA_TAIL = 5327;
  static const unsigned DATA_SIZE = 4096;
  std::array<uint64_t, DATA_SIZE> data = {0};

  void verify_data_read(const uint64_t read_data[]) {
    ceph_assert(read_data[0] == read_count);
    ceph_assert(data[DATA_SIZE - 1] = DATA_TAIL);
  }

  Socket* socket = nullptr;
  uint64_t write_count = 0;
  uint64_t read_count = 0;

  Connection(Socket* socket) : socket{socket} {
    assert(socket);
    data[DATA_SIZE - 1] = DATA_TAIL;
  }

  future<> dispatch_write(unsigned round = 0, bool force_shut = false) {
    logger.debug("dispatch_write(round={}, force_shut={})...", round, force_shut);
    return seastar::repeat([this, round, force_shut] {
      if (round != 0 && round <= write_count) {
        return seastar::futurize_apply([this, force_shut] {
          if (force_shut) {
            logger.debug("dispatch_write() done, force shutdown output");
            socket->force_shutdown_out();
          } else {
            logger.debug("dispatch_write() done");
          }
        }).then([] {
          return seastar::make_ready_future<stop_t>(stop_t::yes);
        });
      } else {
        data[0] = write_count;
        return socket->write(seastar::net::packet(
            reinterpret_cast<const char*>(&data), sizeof(data))
        ).then([this] {
          return socket->flush();
        }).then([this] {
          write_count += 1;
          return seastar::make_ready_future<stop_t>(stop_t::no);
        });
      }
    });
  }

  future<> dispatch_write_unbounded() {
    return dispatch_write(
    ).then([] {
      ceph_abort();
    }).handle_exception_type([this] (const std::system_error& e) {
      if (e.code() != std::errc::broken_pipe &&
          e.code() != std::errc::connection_reset) {
        logger.error("dispatch_write_unbounded(): "
                     "unexpected error {}", e);
        throw;
      }
      // successful
      logger.debug("dispatch_write_unbounded(): "
                   "expected error {}", e);
      shutdown();
    });
  }

  future<> dispatch_read(unsigned round = 0, bool force_shut = false) {
    logger.debug("dispatch_read(round={}, force_shut={})...", round, force_shut);
    return seastar::repeat([this, round, force_shut] {
      if (round != 0 && round <= read_count) {
        return seastar::futurize_apply([this, force_shut] {
          if (force_shut) {
            logger.debug("dispatch_read() done, force shutdown input");
            socket->force_shutdown_in();
          } else {
            logger.debug("dispatch_read() done");
          }
        }).then([] {
          return seastar::make_ready_future<stop_t>(stop_t::yes);
        });
      } else {
        return seastar::futurize_apply([this] {
          // we want to test both Socket::read() and Socket::read_exactly()
          if (read_count % 2) {
            return socket->read(DATA_SIZE * sizeof(uint64_t)
            ).then([this] (ceph::bufferlist bl) {
              uint64_t read_data[DATA_SIZE];
              auto p = bl.cbegin();
              ::ceph::decode_raw(read_data, p);
              verify_data_read(read_data);
            });
          } else {
            return socket->read_exactly(DATA_SIZE * sizeof(uint64_t)
            ).then([this] (auto buf) {
              auto read_data = reinterpret_cast<const uint64_t*>(buf.get());
              verify_data_read(read_data);
            });
          }
        }).then([this] {
          ++read_count;
          return seastar::make_ready_future<stop_t>(stop_t::no);
        });
      }
    });
  }

  future<> dispatch_read_unbounded() {
    return dispatch_read(
    ).then([] {
      ceph_abort();
    }).handle_exception_type([this] (const std::system_error& e) {
      if (e.code() != error::read_eof
       && e.code() != std::errc::connection_reset) {
        logger.error("dispatch_read_unbounded(): "
                     "unexpected error {}", e);
        throw;
      }
      // successful
      logger.debug("dispatch_read_unbounded(): "
                   "expected error {}", e);
      shutdown();
    });
  }

  void shutdown() {
    socket->shutdown();
  }

 public:
  static future<> dispatch_rw_bounded(Socket* socket, unsigned round,
                                      bool force_shut = false) {
    logger.debug("dispatch_rw_bounded(round={}, force_shut={})...",
                 round, force_shut);
    return seastar::do_with(Connection{socket},
                            [round, force_shut] (auto& conn) {
      ceph_assert(round != 0);
      return seastar::when_all_succeed(
        conn.dispatch_write(round, force_shut),
        conn.dispatch_read(round, force_shut)
      );
    });
  }

  static future<> dispatch_rw_unbounded(Socket* socket, bool preemptive_shut = false) {
    logger.debug("dispatch_rw_unbounded(preemptive_shut={})...", preemptive_shut);
    return seastar::do_with(Connection{socket}, [preemptive_shut] (auto& conn) {
      return seastar::when_all_succeed(
        conn.dispatch_write_unbounded(),
        conn.dispatch_read_unbounded(),
        seastar::futurize_apply([&conn, preemptive_shut] {
          if (preemptive_shut) {
            return seastar::sleep(100ms).then([&conn] {
              logger.debug("dispatch_rw_unbounded() shutdown socket preemptively(100ms)");
              conn.shutdown();
            });
          } else {
            return seastar::now();
          }
        })
      );
    });
  }
};

future<> test_read_write() {
  logger.info("test_read_write()...");
  return SocketFactory::dispatch_sockets(
    [] (auto cs) { return Connection::dispatch_rw_bounded(cs, 128); },
    [] (auto ss) { return Connection::dispatch_rw_bounded(ss, 128); }
  ).then([] {
    logger.info("test_read_write() ok\n");
  }).handle_exception([] (auto eptr) {
    logger.error("test_read_write() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_unexpected_down() {
  logger.info("test_unexpected_down()...");
  return SocketFactory::dispatch_sockets(
    [] (auto cs) { 
      return Connection::dispatch_rw_bounded(cs, 128, true
        ).handle_exception_type([] (const std::system_error& e) {
        logger.debug("test_unexpected_down(): client get error {}", e);
        ceph_assert(e.code() == error::read_eof);
      });
    },
    [] (auto ss) { return Connection::dispatch_rw_unbounded(ss); }
  ).then([] {
    logger.info("test_unexpected_down() ok\n");
  }).handle_exception([] (auto eptr) {
    logger.error("test_unexpected_down() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_shutdown_propagated() {
  logger.info("test_shutdown_propagated()...");
  return SocketFactory::dispatch_sockets(
    [] (auto cs) {
      logger.debug("test_shutdown_propagated() shutdown client socket");
      cs->shutdown();
      return seastar::now();
    },
    [] (auto ss) { return Connection::dispatch_rw_unbounded(ss); }
  ).then([] {
    logger.info("test_shutdown_propagated() ok\n");
  }).handle_exception([] (auto eptr) {
    logger.error("test_shutdown_propagated() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_preemptive_down() {
  logger.info("test_preemptive_down()...");
  return SocketFactory::dispatch_sockets(
    [] (auto cs) { return Connection::dispatch_rw_unbounded(cs, true); },
    [] (auto ss) { return Connection::dispatch_rw_unbounded(ss); }
  ).then([] {
    logger.info("test_preemptive_down() ok\n");
  }).handle_exception([] (auto eptr) {
    logger.error("test_preemptive_down() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

}

int main(int argc, char** argv)
{
  seastar::app_template app;
  return app.run(argc, argv, [] {
    return seastar::futurize_apply([] {
      return test_refused();
    }).then([] {
      return test_bind_same();
    }).then([] {
      return test_accept();
    }).then([] {
      return test_read_write();
    }).then([] {
      return test_unexpected_down();
    }).then([] {
      return test_shutdown_propagated();
    }).then([] {
      return test_preemptive_down();
    }).then([] {
      logger.info("All tests succeeded");
      // Seastar has bugs to have events undispatched during shutdown,
      // which will result in memory leak and thus fail LeakSanitizer.
      return seastar::sleep(100ms);
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });
}
