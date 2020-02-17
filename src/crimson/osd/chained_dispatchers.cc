#include "crimson/osd/chained_dispatchers.h"
#include "crimson/net/Connection.h"
#include "msg/Message.h"


seastar::future<>
ChainedDispatchers::ms_dispatch(crimson::net::Connection* conn,
                                MessageRef m) {
  return seastar::do_for_each(dispatchers, [conn, m](Dispatcher* dispatcher) {
    return dispatcher->ms_dispatch(conn, m);
  });
}

seastar::future<>
ChainedDispatchers::ms_handle_accept(crimson::net::ConnectionRef conn) {
  return seastar::do_for_each(dispatchers, [conn](Dispatcher* dispatcher) {
    return dispatcher->ms_handle_accept(conn);
  });
}

seastar::future<>
ChainedDispatchers::ms_handle_connect(crimson::net::ConnectionRef conn) {
  return seastar::do_for_each(dispatchers, [conn](Dispatcher* dispatcher) {
    return dispatcher->ms_handle_connect(conn);
  });
}

seastar::future<>
ChainedDispatchers::ms_handle_reset(crimson::net::ConnectionRef conn) {
  return seastar::do_for_each(dispatchers, [conn](Dispatcher* dispatcher) {
    return dispatcher->ms_handle_reset(conn);
  });
}

seastar::future<>
ChainedDispatchers::ms_handle_remote_reset(crimson::net::ConnectionRef conn) {
  return seastar::do_for_each(dispatchers, [conn](Dispatcher* dispatcher) {
    return dispatcher->ms_handle_remote_reset(conn);
  });
}
