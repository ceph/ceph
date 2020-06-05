#include "crimson/net/chained_dispatchers.h"
#include "crimson/net/Connection.h"
#include "msg/Message.h"

seastar::future<>
ChainedDispatchers::ms_dispatch(crimson::net::Connection* conn,
                                MessageRef m) {
  return seastar::do_for_each(dispatchers, [conn, m](Dispatcher& dispatcher) {
    return dispatcher.ms_dispatch(conn, m);
  });
}

void
ChainedDispatchers::ms_handle_accept(crimson::net::ConnectionRef conn) {
  for (auto& dispatcher : dispatchers) {
    dispatcher.ms_handle_accept(conn);
  }
}

void
ChainedDispatchers::ms_handle_connect(crimson::net::ConnectionRef conn) {
  for(auto& dispatcher : dispatchers) {
    dispatcher.ms_handle_connect(conn);
  }
}

void
ChainedDispatchers::ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace) {
  for (auto& dispatcher : dispatchers) {
    dispatcher.ms_handle_reset(conn, is_replace);
  }
}

void
ChainedDispatchers::ms_handle_remote_reset(crimson::net::ConnectionRef conn) {
  for (auto& dispatcher : dispatchers) {
    dispatcher.ms_handle_remote_reset(conn);
  }
}
