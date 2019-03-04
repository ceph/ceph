#include "chained_dispatchers.h"
#include "crimson/net/Connection.h"


seastar::future<>
ChainedDispatchers::ms_dispatch(ceph::net::ConnectionRef conn,
                                MessageRef m) {
  return seastar::do_for_each(dispatchers, [conn, m](Dispatcher* dispatcher) {
    return dispatcher->ms_dispatch(conn, m);
  });
}

seastar::future<>
ChainedDispatchers::ms_handle_accept(ceph::net::ConnectionRef conn) {
  return seastar::do_for_each(dispatchers, [conn](Dispatcher* dispatcher) {
    return dispatcher->ms_handle_accept(conn);
  });
}

seastar::future<>
ChainedDispatchers::ms_handle_connect(ceph::net::ConnectionRef conn) {
  return seastar::do_for_each(dispatchers, [conn](Dispatcher* dispatcher) {
    return dispatcher->ms_handle_connect(conn);
  });
}

seastar::future<>
ChainedDispatchers::ms_handle_reset(ceph::net::ConnectionRef conn) {
  return seastar::do_for_each(dispatchers, [conn](Dispatcher* dispatcher) {
    return dispatcher->ms_handle_reset(conn);
  });
}

seastar::future<>
ChainedDispatchers::ms_handle_remote_reset(ceph::net::ConnectionRef conn) {
  return seastar::do_for_each(dispatchers, [conn](Dispatcher* dispatcher) {
    return dispatcher->ms_handle_remote_reset(conn);
  });
}

AuthAuthorizer*
ChainedDispatchers::ms_get_authorizer(peer_type_t peer_type) const
{
  // since dispatcher returns a nullptr if it does not have the authorizer,
  // let's use the chain-of-responsibility pattern here.
  for (auto dispatcher : dispatchers) {
    if (auto auth = dispatcher->ms_get_authorizer(peer_type); auth) {
      return auth;
    }
  }
  // just give up
  return nullptr;
}	 
