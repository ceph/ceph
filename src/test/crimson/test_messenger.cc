#include "messages/MPing.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/SocketMessenger.h"
#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>

static seastar::future<> test_echo()
{
  struct test_state {
    entity_addr_t addr;

    struct {
      ceph::net::SocketMessenger messenger{entity_name_t::OSD(1)};
      struct ServerDispatcher : ceph::net::Dispatcher {
        seastar::future<> ms_dispatch(ceph::net::ConnectionRef c,
                                      MessageRef m) override {
          std::cout << "server got " << *m << std::endl;
          // reply with a pong
          return c->send(MessageRef{new MPing(), false});
        }
      } dispatcher;
    } server;

    struct {
      ceph::net::SocketMessenger messenger{entity_name_t::OSD(0)};
      struct ClientDispatcher : ceph::net::Dispatcher {
        seastar::promise<MessageRef> reply;
        seastar::future<> ms_dispatch(ceph::net::ConnectionRef c,
                                      MessageRef m) override {
          reply.set_value(std::move(m));
          return seastar::now();
        }
      } dispatcher;
    } client;
  };
  return seastar::do_with(test_state{},
    [] (test_state& t) {
      // bind the server
      t.addr.set_family(AF_INET);
      t.addr.set_port(9010);
      t.server.messenger.bind(t.addr);

      return t.server.messenger.start(&t.server.dispatcher)
        .then([&] {
          return t.client.messenger.start(&t.client.dispatcher)
            .then([&] {
              return t.client.messenger.connect(t.addr, entity_name_t::TYPE_OSD);
            }).then([] (ceph::net::ConnectionRef conn) {
              std::cout << "client connected" << std::endl;
              return conn->send(MessageRef{new MPing(), false});
            }).then([&] {
              return t.client.dispatcher.reply.get_future();
            }).then([&] (MessageRef msg) {
              std::cout << "client got reply " << *msg << std::endl;
            }).finally([&] {
              std::cout << "client shutting down" << std::endl;
              return t.client.messenger.shutdown();
            });
        }).finally([&] {
          std::cout << "server shutting down" << std::endl;
          return t.server.messenger.shutdown();
        });
    });
}

int main(int argc, char** argv)
{
  seastar::app_template app;
  return app.run(argc, argv, [] {
    return test_echo().then([] {
      std::cout << "All tests succeeded" << std::endl;
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });
}
