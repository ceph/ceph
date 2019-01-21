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

seastar::future<std::unique_ptr<AuthAuthorizer>>
ChainedDispatchers::ms_get_authorizer(peer_type_t peer_type)
{
  // since dispatcher returns a nullptr if it does not have the authorizer,
  // let's use the chain-of-responsibility pattern here.
  struct Params {
    peer_type_t peer_type;
    std::deque<Dispatcher*>::iterator first, last;
  } params = {peer_type, dispatchers.begin(), dispatchers.end()};
  return seastar::do_with(Params{params}, [this] (Params& params) {
    using result_t = std::unique_ptr<AuthAuthorizer>;
    return seastar::repeat_until_value([&] () {
      auto& first = params.first;
      if (first == params.last) {
        // just give up
        return seastar::make_ready_future<std::optional<result_t>>(result_t{});
      } else {
        return (*first)->ms_get_authorizer(params.peer_type)
          .then([&] (auto&& auth)-> std::optional<result_t> {
          if (auth) {
            // hooray!
            return std::move(auth);
          } else {
            // try next one
            ++first;
            return {};
          }
        });
      }
    });
  });
}	 
