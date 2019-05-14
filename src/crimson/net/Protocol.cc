// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Protocol.h"

#include "auth/Auth.h"

#include "crimson/common/log.h"
#include "Socket.h"
#include "SocketConnection.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_ms);
  }
}

namespace ceph::net {

Protocol::Protocol(proto_t type,
                   Dispatcher& dispatcher,
                   SocketConnection& conn)
  : proto_type(type),
    dispatcher(dispatcher),
    conn(conn),
    auth_meta{seastar::make_lw_shared<AuthConnectionMeta>()}
{}

Protocol::~Protocol()
{
  ceph_assert(pending_dispatch.is_closed());
}

bool Protocol::is_connected() const
{
  return write_state == write_state_t::open;
}

seastar::future<> Protocol::close()
{
  if (closed) {
    // already closing
    assert(close_ready.valid());
    return close_ready.get_future();
  }

  // unregister_conn() drops a reference, so hold another until completion
  auto cleanup = [conn_ref = conn.shared_from_this(), this] {
      logger().debug("{} closed!", conn);
    };

  trigger_close();

  // close_ready become valid only after state is state_t::closing
  assert(!close_ready.valid());

  if (socket) {
    close_ready = socket->close()
      .then([this] {
        return pending_dispatch.close();
      }).finally(std::move(cleanup));
  } else {
    close_ready = pending_dispatch.close().finally(std::move(cleanup));
  }

  closed = true;
  set_write_state(write_state_t::drop);

  return close_ready.get_future();
}

seastar::future<> Protocol::send(MessageRef msg)
{
  if (write_state != write_state_t::drop) {
    conn.out_q.push_back(std::move(msg));
    write_event();
  }
  return seastar::now();
}

seastar::future<> Protocol::keepalive()
{
  if (!need_keepalive) {
    need_keepalive = true;
    write_event();
  }
  return seastar::now();
}

void Protocol::notify_keepalive_ack(utime_t _keepalive_ack)
{
  logger().debug("{} got keepalive ack {}", conn, _keepalive_ack);
  keepalive_ack = _keepalive_ack;
  write_event();
}

seastar::future<stop_t> Protocol::do_write_dispatch_sweep()
{
  switch (write_state) {
   case write_state_t::open: {
    size_t num_msgs = conn.out_q.size();
    // we must have something to write...
    ceph_assert(num_msgs || need_keepalive || keepalive_ack.has_value());
    Message* msg_ptr = nullptr;
    if (likely(num_msgs)) {
      msg_ptr = conn.out_q.front().get();
    }
    // sweep all pending writes with the concrete Protocol
    return socket->write(do_sweep_messages(
        conn.out_q, num_msgs, need_keepalive, keepalive_ack))
    .then([this, msg_ptr, num_msgs, prv_keepalive_ack=keepalive_ack] {
      need_keepalive = false;
      if (keepalive_ack == prv_keepalive_ack) {
        keepalive_ack = std::nullopt;
      }
      if (likely(num_msgs && msg_ptr == conn.out_q.front().get())) {
        // we have sent some messages successfully
        // and the out_q was not reset during socket write
        conn.out_q.erase(conn.out_q.begin(), conn.out_q.begin()+num_msgs);
      }
      if (conn.out_q.empty() && !keepalive_ack.has_value()) {
        // good, we have nothing pending to send now.
        return socket->flush().then([this] {
          if (conn.out_q.empty() && !need_keepalive && !keepalive_ack.has_value()) {
            // still nothing pending to send after flush,
            // the dispatching can ONLY stop now
            ceph_assert(write_dispatching);
            write_dispatching = false;
            return seastar::make_ready_future<stop_t>(stop_t::yes);
          } else {
            // something is pending to send during flushing
            return seastar::make_ready_future<stop_t>(stop_t::no);
          }
        });
      } else {
        // messages were enqueued during socket write
        return seastar::make_ready_future<stop_t>(stop_t::no);
      }
    }).handle_exception([this] (std::exception_ptr eptr) {
      logger().warn("{} do_write_dispatch_sweep() fault: {}", conn, eptr);
      close();
      return seastar::make_ready_future<stop_t>(stop_t::no);
    });
   }
   case write_state_t::delay: {
    // delay dispatching writes until open
    return state_changed.get_shared_future()
    .then([] { return stop_t::no; });
   }
   case write_state_t::drop:
    ceph_assert(write_dispatching);
    write_dispatching = false;
    return seastar::make_ready_future<stop_t>(stop_t::yes);
   default:
    ceph_assert(false);
  }
}

void Protocol::write_event()
{
  if (write_dispatching) {
    // already dispatching
    return;
  }
  write_dispatching = true;
  switch (write_state) {
   case write_state_t::open:
     [[fallthrough]];
   case write_state_t::delay:
    seastar::with_gate(pending_dispatch, [this] {
      return seastar::repeat([this] {
        return do_write_dispatch_sweep();
      });
    });
    return;
   case write_state_t::drop:
    write_dispatching = false;
   default:
    ceph_assert(false);
  }
}

} // namespace ceph::net
