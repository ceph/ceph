#include "crimson/common/log.h"
#include "crimson/net/chained_dispatchers.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "msg/Message.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_ms);
  }
}

namespace crimson::net {

seastar::future<>
ChainedDispatchers::ms_dispatch(ConnectionRef conn,
                                MessageRef m) {
  try {
    for (auto& dispatcher : dispatchers) {
      auto dispatched = dispatcher->ms_dispatch(conn, m);
      if (dispatched.has_value()) {
        return std::move(*dispatched
        ).handle_exception([conn] (std::exception_ptr eptr) {
          logger().error("{} got unexpected exception in ms_dispatch() throttling {}",
                         *conn, eptr);
          ceph_abort();
        });
      }
    }
  } catch (...) {
    logger().error("{} got unexpected exception in ms_dispatch() {}",
                   *conn, std::current_exception());
    ceph_abort();
  }
  if (!dispatchers.empty()) {
    logger().error("ms_dispatch unhandled message {}", *m);
  }
  return seastar::now();
}

void
ChainedDispatchers::ms_handle_shard_change(
    ConnectionRef conn,
    seastar::shard_id new_shard,
    bool ac) {
  try {
    for (auto& dispatcher : dispatchers) {
      dispatcher->ms_handle_shard_change(conn, new_shard, ac);
    }
  } catch (...) {
    logger().error("{} got unexpected exception in ms_handle_shard_change() {}",
                   *conn, std::current_exception());
    ceph_abort();
  }
}

void
ChainedDispatchers::ms_handle_accept(
    ConnectionRef conn,
    seastar::shard_id prv_shard,
    bool is_replace) {
  try {
    for (auto& dispatcher : dispatchers) {
      dispatcher->ms_handle_accept(conn, prv_shard, is_replace);
    }
  } catch (...) {
    logger().error("{} got unexpected exception in ms_handle_accept() {}",
                   *conn, std::current_exception());
    ceph_abort();
  }
}

void
ChainedDispatchers::ms_handle_connect(
    ConnectionRef conn,
    seastar::shard_id prv_shard) {
  try {
    for(auto& dispatcher : dispatchers) {
      dispatcher->ms_handle_connect(conn, prv_shard);
    }
  } catch (...) {
    logger().error("{} got unexpected exception in ms_handle_connect() {}",
                   *conn, std::current_exception());
    ceph_abort();
  }
}

void
ChainedDispatchers::ms_handle_reset(ConnectionRef conn, bool is_replace) {
  try {
    for (auto& dispatcher : dispatchers) {
      dispatcher->ms_handle_reset(conn, is_replace);
    }
  } catch (...) {
    logger().error("{} got unexpected exception in ms_handle_reset() {}",
                   *conn, std::current_exception());
    ceph_abort();
  }
}

void
ChainedDispatchers::ms_handle_remote_reset(ConnectionRef conn) {
  try {
    for (auto& dispatcher : dispatchers) {
      dispatcher->ms_handle_remote_reset(conn);
    }
  } catch (...) {
    logger().error("{} got unexpected exception in ms_handle_remote_reset() {}",
                   *conn, std::current_exception());
    ceph_abort();
  }
}

}
