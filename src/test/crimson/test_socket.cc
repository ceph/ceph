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
using ceph::net::error;
using ceph::net::Socket;
using ceph::net::SocketFRef;

static seastar::logger logger{"test"};

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
        seastar::with_gate(factory.shutdown_gate, [&factory] {
          return seastar::keep_doing([&factory] {
            return Socket::accept(*factory.listener
            ).then([&factory] (SocketFRef socket,
                               entity_addr_t peer_addr) {
              // gate socket dispatching
              seastar::with_gate(factory.shutdown_gate,
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
  return ceph::net::create_sharded<AcceptTest>(
  ).then([] (AcceptTest* factory) {
    return factory->bind_accept(
    ).then([] {
      // try to bind the same address
      return ceph::net::create_sharded<AcceptTest>(
      ).then([] (AcceptTest* factory2) {
        return factory2->bind_accept(
        ).then([] {
          ceph_abort_msg("bind should raise addr-in-use");
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
  return ceph::net::create_sharded<AcceptTest>(
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
      logger.info("All tests succeeded");
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });
}
