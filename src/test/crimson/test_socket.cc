// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include <fmt/os.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/later.hh>

#include "crimson/common/log.h"
#include "crimson/net/Errors.h"
#include "crimson/net/Fwd.h"
#include "crimson/net/Socket.h"
#include "test/crimson/ctest_utils.h"

using crimson::common::local_conf;

namespace {

using namespace std::chrono_literals;

using seastar::engine;
using seastar::future;
using crimson::net::error;
using crimson::net::listen_ertr;
using crimson::net::ShardedServerSocket;
using crimson::net::Socket;
using crimson::net::SocketRef;
using crimson::net::stop_t;

using SocketFRef = seastar::foreign_ptr<SocketRef>;

seastar::logger &logger() {
  return crimson::get_logger(ceph_subsys_test);
}

entity_addr_t get_server_addr() {
  entity_addr_t saddr;
  saddr.parse("127.0.0.1", nullptr);
  saddr.set_port(9020);
  return saddr;
}

future<SocketRef> socket_connect(const entity_addr_t& saddr) {
  logger().debug("socket_connect() to {} ...", saddr);
  return Socket::connect(saddr).then([](auto socket) {
    logger().debug("socket_connect() connected");
    return socket;
  });
}

future<> test_refused() {
  logger().info("test_refused()...");
  auto saddr = get_server_addr();
  return socket_connect(saddr).discard_result().then([saddr] {
    logger().error("test_refused(): connection to {} is not refused", saddr);
    ceph_abort();
  }).handle_exception_type([](const std::system_error& e) {
    if (e.code() != std::errc::connection_refused) {
      logger().error("test_refused() got unexpeted error {}", e);
      ceph_abort();
    } else {
      logger().info("test_refused() ok\n");
    }
  }).handle_exception([](auto eptr) {
    logger().error("test_refused() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_bind_same(bool is_fixed_cpu) {
  logger().info("test_bind_same()...");
  return ShardedServerSocket::create(is_fixed_cpu
  ).then([is_fixed_cpu](auto pss1) {
    auto saddr = get_server_addr();
    return pss1->listen(saddr).safe_then([saddr, is_fixed_cpu] {
      // try to bind the same address
      return ShardedServerSocket::create(is_fixed_cpu
      ).then([saddr](auto pss2) {
        return pss2->listen(saddr).safe_then([] {
          logger().error("test_bind_same() should raise address_in_use");
          ceph_abort();
        }, listen_ertr::all_same_way(
            [](const std::error_code& e) {
          if (e == std::errc::address_in_use) {
            // successful!
            logger().info("test_bind_same() ok\n");
          } else {
            logger().error("test_bind_same() got unexpected error {}", e);
            ceph_abort();
          }
          // Note: need to return a explicit ready future, or there will be a
          // runtime error: member access within null pointer of type 'struct promise_base'
          return seastar::now();
        })).then([pss2] {
          return pss2->shutdown_destroy();
        });
      });
    }, listen_ertr::all_same_way(
        [saddr](const std::error_code& e) {
      logger().error("test_bind_same(): there is another instance running at {}",
                     saddr);
      ceph_abort();
    })).then([pss1] {
      return pss1->shutdown_destroy();
    }).handle_exception([](auto eptr) {
      logger().error("test_bind_same() got unexpeted exception {}", eptr);
      ceph_abort();
    });
  });
}

future<> test_accept(bool is_fixed_cpu) {
  logger().info("test_accept()");
  return ShardedServerSocket::create(is_fixed_cpu
  ).then([](auto pss) {
    auto saddr = get_server_addr();
    return pss->listen(saddr
    ).safe_then([pss] {
      return pss->accept([](auto socket, auto paddr) {
        logger().info("test_accept(): accepted at shard {}", seastar::this_shard_id());
        // simple accept
        return seastar::sleep(100ms
        ).then([socket = std::move(socket)]() mutable {
          return socket->close(
          ).finally([cleanup = std::move(socket)] {});
        });
      });
    }, listen_ertr::all_same_way(
        [saddr](const std::error_code& e) {
      logger().error("test_accept(): there is another instance running at {}",
                     saddr);
      ceph_abort();
    })).then([saddr] {
      return seastar::when_all(
        socket_connect(saddr).then([](auto socket) {
          return socket->close().finally([cleanup = std::move(socket)] {}); }),
        socket_connect(saddr).then([](auto socket) {
          return socket->close().finally([cleanup = std::move(socket)] {}); }),
        socket_connect(saddr).then([](auto socket) {
          return socket->close().finally([cleanup = std::move(socket)] {}); })
      ).discard_result();
    }).then([] {
      // should be enough to be connected locally
      return seastar::sleep(50ms);
    }).then([] {
      logger().info("test_accept() ok\n");
    }).then([pss] {
      return pss->shutdown_destroy();
    }).handle_exception([](auto eptr) {
      logger().error("test_accept() got unexpeted exception {}", eptr);
      ceph_abort();
    });
  });
}

class SocketFactory {
  static constexpr seastar::shard_id CLIENT_CPU = 0u;
  SocketRef client_socket;
  seastar::promise<> server_connected;

  static constexpr seastar::shard_id SERVER_CPU = 1u;
  ShardedServerSocket *pss = nullptr;

  seastar::shard_id server_socket_CPU;
  SocketFRef server_socket;

 public:
  template <typename FuncC, typename FuncS>
  static future<> dispatch_sockets(
      bool is_fixed_cpu,
      FuncC&& cb_client,
      FuncS&& cb_server) {
    ceph_assert_always(seastar::this_shard_id() == CLIENT_CPU);
    auto owner = std::make_unique<SocketFactory>();
    auto psf = owner.get();
    auto saddr = get_server_addr();
    return seastar::smp::submit_to(SERVER_CPU, [psf, saddr, is_fixed_cpu] {
      return ShardedServerSocket::create(is_fixed_cpu
      ).then([psf, saddr](auto pss) {
        psf->pss = pss;
        return pss->listen(saddr
        ).safe_then([] {
        }, listen_ertr::all_same_way([saddr](const std::error_code& e) {
          logger().error("dispatch_sockets(): there is another instance running at {}",
                         saddr);
          ceph_abort();
        }));
      });
    }).then([psf, saddr] {
      return seastar::when_all_succeed(
        seastar::smp::submit_to(CLIENT_CPU, [psf, saddr] {
          return socket_connect(saddr).then([psf](auto socket) {
            ceph_assert_always(seastar::this_shard_id() == CLIENT_CPU);
            psf->client_socket = std::move(socket);
          });
        }),
        seastar::smp::submit_to(SERVER_CPU, [psf] {
          return psf->pss->accept([psf](auto _socket, auto paddr) {
            logger().info("dispatch_sockets(): accepted at shard {}",
                          seastar::this_shard_id());
            psf->server_socket_CPU = seastar::this_shard_id();
            if (psf->pss->is_fixed_shard_dispatching()) {
              ceph_assert_always(SERVER_CPU == seastar::this_shard_id());
            }
            SocketFRef socket = seastar::make_foreign(std::move(_socket));
            psf->server_socket = std::move(socket);
            return seastar::smp::submit_to(CLIENT_CPU, [psf] {
              psf->server_connected.set_value();
            });
          });
        })
      );
    }).then_unpack([] {
      return seastar::now();
    }).then([psf] {
      return psf->server_connected.get_future();
    }).then([psf] {
      if (psf->pss) {
        return seastar::smp::submit_to(SERVER_CPU, [psf] {
          return psf->pss->shutdown_destroy();
        });
      }
      return seastar::now();
    }).then([psf,
             cb_client = std::move(cb_client),
             cb_server = std::move(cb_server)]() mutable {
      logger().debug("dispatch_sockets(): client/server socket are ready");
      return seastar::when_all_succeed(
        seastar::smp::submit_to(CLIENT_CPU,
            [socket = psf->client_socket.get(), cb_client = std::move(cb_client)] {
          return cb_client(socket).then([socket] {
            logger().debug("closing client socket...");
            return socket->close();
          }).handle_exception([](auto eptr) {
            logger().error("dispatch_sockets():"
                           " cb_client() got unexpeted exception {}", eptr);
            ceph_abort();
          });
        }),
        seastar::smp::submit_to(psf->server_socket_CPU,
            [socket = psf->server_socket.get(), cb_server = std::move(cb_server)] {
          return cb_server(socket).then([socket] {
            logger().debug("closing server socket...");
            return socket->close();
          }).handle_exception([](auto eptr) {
            logger().error("dispatch_sockets():"
                           " cb_server() got unexpeted exception {}", eptr);
            ceph_abort();
          });
        })
      );
    }).then_unpack([] {
      return seastar::now();
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
    logger().debug("dispatch_write(round={}, force_shut={})...", round, force_shut);
    return seastar::repeat([this, round, force_shut] {
      if (round != 0 && round <= write_count) {
        return seastar::futurize_invoke([this, force_shut] {
          if (force_shut) {
            logger().debug("dispatch_write() done, force shutdown output");
            socket->force_shutdown_out();
          } else {
            logger().debug("dispatch_write() done");
          }
        }).then([] {
          return seastar::make_ready_future<stop_t>(stop_t::yes);
        });
      } else {
        data[0] = write_count;
        bufferlist bl;
        bl.append(buffer::copy(
          reinterpret_cast<const char*>(&data), sizeof(data)));
        return socket->write(bl
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
    }).handle_exception_type([this](const std::system_error& e) {
      if (e.code() != std::errc::broken_pipe &&
          e.code() != std::errc::connection_reset) {
        logger().error("dispatch_write_unbounded(): "
                       "unexpected error {}", e);
        throw;
      }
      // successful
      logger().debug("dispatch_write_unbounded(): "
                     "expected error {}", e);
      shutdown();
    });
  }

  future<> dispatch_read(unsigned round = 0, bool force_shut = false) {
    logger().debug("dispatch_read(round={}, force_shut={})...", round, force_shut);
    return seastar::repeat([this, round, force_shut] {
      if (round != 0 && round <= read_count) {
        return seastar::futurize_invoke([this, force_shut] {
          if (force_shut) {
            logger().debug("dispatch_read() done, force shutdown input");
            socket->force_shutdown_in();
          } else {
            logger().debug("dispatch_read() done");
          }
        }).then([] {
          return seastar::make_ready_future<stop_t>(stop_t::yes);
        });
      } else {
        return seastar::futurize_invoke([this] {
          // we want to test both Socket::read() and Socket::read_exactly()
          if (read_count % 2) {
            return socket->read(DATA_SIZE * sizeof(uint64_t)
            ).then([this](ceph::bufferlist bl) {
              uint64_t read_data[DATA_SIZE];
              auto p = bl.cbegin();
              ::ceph::decode_raw(read_data, p);
              verify_data_read(read_data);
            });
          } else {
            return socket->read_exactly(DATA_SIZE * sizeof(uint64_t)
            ).then([this](auto bptr) {
              uint64_t read_data[DATA_SIZE];
              std::memcpy(read_data, bptr.c_str(), DATA_SIZE * sizeof(uint64_t));
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
    }).handle_exception_type([this](const std::system_error& e) {
      if (e.code() != error::read_eof
       && e.code() != std::errc::connection_reset) {
        logger().error("dispatch_read_unbounded(): "
                       "unexpected error {}", e);
        throw;
      }
      // successful
      logger().debug("dispatch_read_unbounded(): "
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
    logger().debug("dispatch_rw_bounded(round={}, force_shut={})...",
                   round, force_shut);
    return seastar::do_with(Connection{socket},
                            [round, force_shut](auto& conn) {
      ceph_assert(round != 0);
      return seastar::when_all_succeed(
        conn.dispatch_write(round, force_shut),
        conn.dispatch_read(round, force_shut)
      ).then_unpack([] {
        return seastar::now();
      });
    });
  }

  static future<> dispatch_rw_unbounded(Socket* socket, bool preemptive_shut = false) {
    logger().debug("dispatch_rw_unbounded(preemptive_shut={})...", preemptive_shut);
    return seastar::do_with(Connection{socket}, [preemptive_shut](auto& conn) {
      return seastar::when_all_succeed(
        conn.dispatch_write_unbounded(),
        conn.dispatch_read_unbounded(),
        seastar::futurize_invoke([&conn, preemptive_shut] {
          if (preemptive_shut) {
            return seastar::sleep(100ms).then([&conn] {
              logger().debug("dispatch_rw_unbounded() shutdown socket preemptively(100ms)");
              conn.shutdown();
            });
          } else {
            return seastar::now();
          }
        })
      ).then_unpack([] {
        return seastar::now();
      });
    });
  }
};

future<> test_read_write(bool is_fixed_cpu) {
  logger().info("test_read_write()...");
  return SocketFactory::dispatch_sockets(
    is_fixed_cpu,
    [](auto cs) { return Connection::dispatch_rw_bounded(cs, 128); },
    [](auto ss) { return Connection::dispatch_rw_bounded(ss, 128); }
  ).then([] {
    logger().info("test_read_write() ok\n");
  }).handle_exception([](auto eptr) {
    logger().error("test_read_write() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_unexpected_down(bool is_fixed_cpu) {
  logger().info("test_unexpected_down()...");
  return SocketFactory::dispatch_sockets(
    is_fixed_cpu,
    [](auto cs) {
      return Connection::dispatch_rw_bounded(cs, 128, true
        ).handle_exception_type([](const std::system_error& e) {
        logger().error("test_unexpected_down(): client get error {}", e);
        // union of errors from both read and write
        // also see dispatch_write_unbounded() and dispatch_read_unbounded()
        ceph_assert(e.code() == error::read_eof ||
		    e.code() == std::errc::connection_reset ||
                    e.code() == std::errc::broken_pipe);
      });
    },
    [](auto ss) { return Connection::dispatch_rw_unbounded(ss); }
  ).then([] {
    logger().info("test_unexpected_down() ok\n");
  }).handle_exception([](auto eptr) {
    logger().error("test_unexpected_down() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_shutdown_propagated(bool is_fixed_cpu) {
  logger().info("test_shutdown_propagated()...");
  return SocketFactory::dispatch_sockets(
    is_fixed_cpu,
    [](auto cs) {
      logger().debug("test_shutdown_propagated() shutdown client socket");
      cs->shutdown();
      return seastar::now();
    },
    [](auto ss) { return Connection::dispatch_rw_unbounded(ss); }
  ).then([] {
    logger().info("test_shutdown_propagated() ok\n");
  }).handle_exception([](auto eptr) {
    logger().error("test_shutdown_propagated() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> test_preemptive_down(bool is_fixed_cpu) {
  logger().info("test_preemptive_down()...");
  return SocketFactory::dispatch_sockets(
    is_fixed_cpu,
    [](auto cs) { return Connection::dispatch_rw_unbounded(cs, true); },
    [](auto ss) { return Connection::dispatch_rw_unbounded(ss); }
  ).then([] {
    logger().info("test_preemptive_down() ok\n");
  }).handle_exception([](auto eptr) {
    logger().error("test_preemptive_down() got unexpeted exception {}", eptr);
    ceph_abort();
  });
}

future<> do_test_with_type(bool is_fixed_cpu) {
  return test_bind_same(is_fixed_cpu
  ).then([is_fixed_cpu] {
    return test_accept(is_fixed_cpu);
  }).then([is_fixed_cpu] {
    return test_read_write(is_fixed_cpu);
  }).then([is_fixed_cpu] {
    return test_unexpected_down(is_fixed_cpu);
  }).then([is_fixed_cpu] {
    return test_shutdown_propagated(is_fixed_cpu);
  }).then([is_fixed_cpu] {
    return test_preemptive_down(is_fixed_cpu);
  });
}

}

seastar::future<int> do_test(seastar::app_template& app)
{
  std::vector<const char*> args;
  std::string cluster;
  std::string conf_file_list;
  auto init_params = ceph_argparse_early_args(args,
                                              CEPH_ENTITY_TYPE_CLIENT,
                                              &cluster,
                                              &conf_file_list);
  return crimson::common::sharded_conf().start(
    init_params.name, cluster
  ).then([] {
    return local_conf().start();
  }).then([conf_file_list] {
    return local_conf().parse_config_files(conf_file_list);
  }).then([] {
    return local_conf().set_val("ms_inject_internal_delays", "0");
  }).then([] {
    return test_refused();
  }).then([] {
    return do_test_with_type(true);
  }).then([] {
    return do_test_with_type(false);
  }).then([] {
    logger().info("All tests succeeded");
    // Seastar has bugs to have events undispatched during shutdown,
    // which will result in memory leak and thus fail LeakSanitizer.
    return seastar::sleep(100ms);
  }).then([] {
    return crimson::common::sharded_conf().stop();
  }).then([] {
    return 0;
  }).handle_exception([](auto eptr) {
    logger().error("Test failed: got exception {}", eptr);
    return 1;
  });
}

int main(int argc, char** argv)
{
  seastar::app_template app{get_smp_opts_from_ctest()};
  return app.run(argc, argv, [&app] {
    return do_test(app);
  });
}
