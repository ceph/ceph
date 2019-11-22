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

using seastar::future;
using crimson::net::error;
using crimson::net::Socket;
using crimson::net::SocketFRef;
using crimson::net::stop_t;

static seastar::logger logger{"crimsontest"};

template <typename ConcreteService>
class SocketFactoryBase
    : public seastar::peering_sharded_service<ConcreteService> {
  static constexpr const char* server_addr = "127.0.0.1:9020";

  seastar::gate shutdown_gate;
  std::optional<seastar::server_socket> listener;

 public:
  virtual ~SocketFactoryBase() = default;

  virtual future<> bind_accept() {
    return this->container().invoke_on_all([] (auto& factory) {
      entity_addr_t addr;
      addr.parse(server_addr, nullptr);
      seastar::socket_address s_addr(addr.in4_addr());
      seastar::listen_options lo;
      lo.reuse_address = true;
      factory.listener = seastar::listen(s_addr, lo);
    }).then([this] {
      return this->container().invoke_on_all([] (auto& factory) {
        // gate accepting
        // SocketFactoryBase::shutdown() will drain the continuations in the gate
        // so ignore the returned future
        std::ignore = seastar::with_gate(factory.shutdown_gate, [&factory] {
          return seastar::keep_doing([&factory] {
            return Socket::accept(*factory.listener).then(
              [&factory] (SocketFRef socket,
			  entity_addr_t peer_addr) {
              // gate socket dispatching
              std::ignore = seastar::with_gate(factory.shutdown_gate,
                  [&factory, socket = std::move(socket)] () mutable {
                return factory.handle_server_socket(std::move(socket))
                .handle_exception([] (auto eptr) {
                  logger.error("handle_server_socket():"
                               "got unexpected exception {}", eptr);
                  ceph_abort();
                });
              });
            });
          }).handle_exception_type([] (const std::system_error& e) {
            if (e.code() != error::connection_aborted &&
                e.code() != error::invalid_argument) {
              logger.error("accepting: got unexpected error {}", e);
              ceph_abort();
            }
            // successful
          }).handle_exception([] (auto eptr) {
            logger.error("accepting: got unexpected exception {}", eptr);
            ceph_abort();
          });
        });
      });
    });
  }

  future<> shutdown() {
    return this->container().invoke_on_all([] (auto& factory) {
      if (factory.listener) {
        factory.listener.value().abort_accept();
      }
      return factory.shutdown_gate.close();
    });
  }

  future<> stop() { return seastar::now(); }

  static future<SocketFRef> connect() {
    entity_addr_t addr;
    addr.parse(server_addr, nullptr);
    return Socket::connect(addr);
  }

 protected:
  virtual future<> handle_server_socket(SocketFRef&& socket) = 0;
};

class AcceptTest final
    : public SocketFactoryBase<AcceptTest> {
 public:
  future<> handle_server_socket(SocketFRef&& socket) override {
    return seastar::sleep(100ms
    ).then([socket = std::move(socket)] () mutable {
      return socket->close()
      .finally([socket = std::move(socket)] {});
    });
  }
};

future<> test_refused() {
  logger.info("test_refused()...");
  return AcceptTest::connect().discard_result(
  ).then([] {
    ceph_abort_msg("connection is not refused");
  }).handle_exception_type([] (const std::system_error& e) {
    if (e.code() != error::connection_refused) {
      logger.error("test_refused() got unexpeted error {}", e);
      ceph_abort();
    }
    // successful
  }).handle_exception([] (auto eptr) {
    logger.error("test_refused() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_bind_same() {
  logger.info("test_bind_same()...");
  return crimson::net::create_sharded<AcceptTest>().then(
    [] (AcceptTest* factory) {
    return factory->bind_accept().then([] {
      // try to bind the same address
      return crimson::net::create_sharded<AcceptTest>().then(
        [] (AcceptTest* factory2) {
        return factory2->bind_accept().then([] {
          ceph_abort_msg("bind should raise addr-in-use");
	  return seastar::now();
        }).finally([factory2] {
          return factory2->shutdown();
        }).handle_exception_type([] (const std::system_error& e) {
          if (e.code() != error::address_in_use) {
            logger.error("test_bind_same() got unexpeted error {}", e);
            ceph_abort();
          }
          // successful
        });
      });
    }).finally([factory] {
      return factory->shutdown();
    }).handle_exception([] (auto eptr) {
      logger.error("test_bind_same() got unexpeted exception {}", eptr);
      ceph_abort();
    });
  });
}

future<> test_accept() {
  logger.info("test_accept()");
  return crimson::net::create_sharded<AcceptTest>(
  ).then([] (AcceptTest* factory) {
    return factory->bind_accept().then([factory] {
      return seastar::when_all(
        factory->connect().then([] (auto socket) {
          return socket->close().finally([cleanup = std::move(socket)] {}); }),
        factory->connect().then([] (auto socket) {
          return socket->close().finally([cleanup = std::move(socket)] {}); }),
        factory->connect().then([] (auto socket) {
          return socket->close().finally([cleanup = std::move(socket)] {}); })
      ).discard_result();
    }).then([] {
      // should be enough to be connected locally
      return seastar::sleep(50ms);
    }).finally([factory] {
      return factory->shutdown();
    }).handle_exception([] (auto eptr) {
      logger.error("test_accept() got unexpeted exception {}", eptr);
      ceph_abort();
    });
  });
}

class SocketFactory final
    : public SocketFactoryBase<SocketFactory> {
  const seastar::shard_id target_shard;
  seastar::promise<SocketFRef> socket_promise;

  future<> bind_accept() override {
    return SocketFactoryBase<SocketFactory>::bind_accept();
  }

  future<SocketFRef> get_accepted() {
    return socket_promise.get_future();
  }

 public:
  SocketFactory(seastar::shard_id shard) : target_shard{shard} {}

  future<> handle_server_socket(SocketFRef&& socket) override {
    return container().invoke_on(target_shard,
        [socket = std::move(socket)] (auto& factory) mutable {
      factory.socket_promise.set_value(std::move(socket));
    });
  }

  static future<tuple<SocketFRef, SocketFRef>> get_sockets() {
    return crimson::net::create_sharded<SocketFactory>(seastar::engine().cpu_id()
    ).then([] (SocketFactory* factory) {
      return factory->bind_accept().then([factory] {
        return connect();
      }).then([factory] (auto fp_client_socket) {
        return factory->get_accepted().then(
          [fp_client_socket = std::move(fp_client_socket)](auto fp_server_socket) mutable {
	  return seastar::make_ready_future<tuple<SocketFRef, SocketFRef>>(
            std::make_tuple(std::move(fp_client_socket), std::move(fp_server_socket)));
        });
      }).finally([factory] {
        return factory->shutdown();
      });
    });
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

  SocketFRef socket;
  uint64_t write_count = 0;
  uint64_t read_count = 0;

  Connection(SocketFRef&& socket) : socket{std::move(socket)} {
    data[DATA_SIZE - 1] = DATA_TAIL;
  }

  future<> dispatch_write(unsigned round = 0, bool force_shut = false) {
    return seastar::repeat([this, round, force_shut] {
      if (round != 0 && round <= write_count) {
        return seastar::futurize_apply([this, force_shut] {
          if (force_shut) {
            socket->force_shutdown_out();
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
    }).handle_exception_type([] (const std::system_error& e) {
      if (e.code() != error::broken_pipe &&
          e.code() != error::connection_reset) {
        logger.error("dispatch_write_unbounded(): "
                     "unexpected error {}", e);
        throw;
      }
      // successful
      logger.debug("dispatch_write_unbounded(): "
                   "expected error {}", e);
    });
  }

  future<> dispatch_read(unsigned round = 0, bool force_shut = false) {
    return seastar::repeat([this, round, force_shut] {
      if (round != 0 && round <= read_count) {
        return seastar::futurize_apply([this, force_shut] {
          if (force_shut) {
            socket->force_shutdown_in();
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
    }).handle_exception_type([] (const std::system_error& e) {
      if (e.code() != error::read_eof
       && e.code() != error::connection_reset) {
        logger.error("dispatch_read_unbounded(): "
                     "unexpected error {}", e);
        throw;
      }
      // successful
      logger.debug("dispatch_read_unbounded(): "
                   "expected error {}", e);
    });
  }

  void shutdown() {
    socket->shutdown();
  }

  future<> close() {
    return socket->close();
  }

 public:
  static future<> dispatch_rw_bounded(SocketFRef&& socket, bool is_client,
                                      unsigned round, bool force_shut = false) {
    return seastar::smp::submit_to(is_client ? 0 : 1,
        [socket = std::move(socket), round, force_shut] () mutable {
      return seastar::do_with(Connection{std::move(socket)},
                              [round, force_shut] (auto& conn) {
        ceph_assert(round != 0);
        return seastar::when_all_succeed(
          conn.dispatch_write(round, force_shut),
          conn.dispatch_read(round, force_shut)
        ).finally([&conn] {
          return conn.close();
        });
      });
    }).handle_exception([is_client] (auto eptr) {
      logger.error("dispatch_rw_bounded(): {} got unexpected exception {}",
                   is_client ? "client" : "server", eptr);
      ceph_abort();
    });
  }

  static future<> dispatch_rw_unbounded(SocketFRef&& socket, bool is_client,
                                        bool preemptive_shut = false) {
    return seastar::smp::submit_to(is_client ? 0 : 1,
        [socket = std::move(socket), preemptive_shut, is_client] () mutable {
      return seastar::do_with(Connection{std::move(socket)},
                              [preemptive_shut, is_client] (auto& conn) {
        return seastar::when_all_succeed(
          conn.dispatch_write_unbounded(),
          conn.dispatch_read_unbounded(),
          seastar::futurize_apply([&conn, preemptive_shut] {
            if (preemptive_shut) {
              return seastar::sleep(100ms).then([&conn] { conn.shutdown(); });
            } else {
              return seastar::now();
            }
          })
        ).finally([&conn] {
          return conn.close();
        });
      });
    }).handle_exception([is_client] (auto eptr) {
      logger.error("dispatch_rw_unbounded(): {} got unexpected exception {}",
                   is_client ? "client" : "server", eptr);
      ceph_abort();
    });
  }
};

future<> test_read_write() {
  logger.info("test_read_write()...");
  return SocketFactory::get_sockets().then([] (auto sockets) {
    auto [client_socket, server_socket] = std::move(sockets);
    return seastar::when_all_succeed(
      Connection::dispatch_rw_bounded(std::move(client_socket), true, 128),
      Connection::dispatch_rw_bounded(std::move(server_socket), false, 128)
    );
  }).handle_exception([] (auto eptr) {
    logger.error("test_read_write() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_unexpected_down() {
  logger.info("test_unexpected_down()...");
  return SocketFactory::get_sockets().then([] (auto sockets) {
    auto [client_socket, server_socket] = std::move(sockets);
    return seastar::when_all_succeed(
      Connection::dispatch_rw_bounded(std::move(client_socket), true, 128, true),
      Connection::dispatch_rw_unbounded(std::move(server_socket), false)
    );
  }).handle_exception([] (auto eptr) {
    logger.error("test_unexpected_down() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_shutdown_propagated() {
  logger.info("test_shutdown_propagated()...");
  return SocketFactory::get_sockets().then([] (auto sockets) {
    auto [client_socket, server_socket] = std::move(sockets);
    client_socket->shutdown();
    return Connection::dispatch_rw_unbounded(std::move(server_socket), false
    ).finally([client_socket = std::move(client_socket)] () mutable {
      return client_socket->close(
      ).finally([cleanup = std::move(client_socket)] {});
    });
  }).handle_exception([] (auto eptr) {
    logger.error("test_shutdown_propagated() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_preemptive_down() {
  logger.info("test_preemptive_down()...");
  return SocketFactory::get_sockets().then([] (auto sockets) {
    auto [client_socket, server_socket] = std::move(sockets);
    return seastar::when_all_succeed(
      Connection::dispatch_rw_unbounded(std::move(client_socket), true, true),
      Connection::dispatch_rw_unbounded(std::move(server_socket), false)
    );
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
    return test_refused().then([] {
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
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });
}
