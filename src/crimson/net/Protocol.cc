// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Protocol.h"

#include "auth/Auth.h"

#include "crimson/common/log.h"
#include "crimson/net/Errors.h"
#include "crimson/net/chained_dispatchers.h"
#include "crimson/net/SocketConnection.h"
#include "crimson/net/SocketMessenger.h"
#include "msg/Message.h"

namespace {

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_ms);
}

} // namespace anonymous

namespace crimson::net {

Protocol::Protocol(ChainedDispatchers& dispatchers,
                   SocketConnection& conn)
  : dispatchers(dispatchers),
    conn(conn),
    frame_assembler(conn)
{}

Protocol::~Protocol()
{
  ceph_assert(gate.is_closed());
  assert(!out_exit_dispatching);
}

ceph::bufferlist Protocol::sweep_out_pending_msgs_to_sent(
  bool require_keepalive,
  std::optional<utime_t> maybe_keepalive_ack,
  bool require_ack)
{
  std::size_t num_msgs = out_pending_msgs.size();
  ceph::bufferlist bl;

  if (unlikely(require_keepalive)) {
    auto keepalive_frame = KeepAliveFrame::Encode();
    bl.append(frame_assembler.get_buffer(keepalive_frame));
  }

  if (unlikely(maybe_keepalive_ack.has_value())) {
    auto keepalive_ack_frame = KeepAliveFrameAck::Encode(*maybe_keepalive_ack);
    bl.append(frame_assembler.get_buffer(keepalive_ack_frame));
  }

  if (require_ack && num_msgs == 0u) {
    auto ack_frame = AckFrame::Encode(get_in_seq());
    bl.append(frame_assembler.get_buffer(ack_frame));
  }

  std::for_each(
      out_pending_msgs.begin(),
      out_pending_msgs.begin()+num_msgs,
      [this, &bl](const MessageURef& msg) {
    // set priority
    msg->get_header().src = conn.messenger.get_myname();

    msg->encode(conn.features, 0);

    ceph_assert(!msg->get_seq() && "message already has seq");
    msg->set_seq(++out_seq);

    ceph_msg_header &header = msg->get_header();
    ceph_msg_footer &footer = msg->get_footer();

    ceph_msg_header2 header2{header.seq,        header.tid,
                             header.type,       header.priority,
                             header.version,
                             ceph_le32(0),      header.data_off,
                             ceph_le64(get_in_seq()),
                             footer.flags,      header.compat_version,
                             header.reserved};

    auto message = MessageFrame::Encode(header2,
        msg->get_payload(), msg->get_middle(), msg->get_data());
    logger().debug("{} --> #{} === {} ({})",
		   conn, msg->get_seq(), *msg, msg->get_type());
    bl.append(frame_assembler.get_buffer(message));
  });

  if (!conn.policy.lossy) {
    out_sent_msgs.insert(
        out_sent_msgs.end(),
        std::make_move_iterator(out_pending_msgs.begin()),
        std::make_move_iterator(out_pending_msgs.end()));
  }
  out_pending_msgs.clear();
  return bl;
}

seastar::future<> Protocol::send(MessageURef msg)
{
  if (out_state != out_state_t::drop) {
    out_pending_msgs.push_back(std::move(msg));
    notify_out_dispatch();
  }
  return seastar::now();
}

seastar::future<> Protocol::send_keepalive()
{
  if (!need_keepalive) {
    need_keepalive = true;
    notify_out_dispatch();
  }
  return seastar::now();
}

void Protocol::set_out_state(
    const Protocol::out_state_t &new_state)
{
  ceph_assert_always(!(
    (new_state == out_state_t::none && out_state != out_state_t::none) ||
    (new_state == out_state_t::open && out_state == out_state_t::open) ||
    (new_state != out_state_t::drop && out_state == out_state_t::drop)
  ));

  if (out_state != out_state_t::open &&
      new_state == out_state_t::open) {
    // to open
    ceph_assert_always(frame_assembler.is_socket_valid());
  } else if (out_state == out_state_t::open &&
             new_state != out_state_t::open) {
    // from open
    if (out_dispatching) {
      ceph_assert_always(!out_exit_dispatching.has_value());
      out_exit_dispatching = seastar::shared_promise<>();
    }
  }

  if (out_state != new_state) {
    out_state = new_state;
    out_state_changed.set_value();
    out_state_changed = seastar::shared_promise<>();
  }
}

void Protocol::notify_keepalive_ack(utime_t keepalive_ack)
{
  logger().trace("{} got keepalive ack {}", conn, keepalive_ack);
  next_keepalive_ack = keepalive_ack;
  notify_out_dispatch();
}

void Protocol::notify_ack()
{
  if (!conn.policy.lossy) {
    ++ack_left;
    notify_out_dispatch();
  }
}

void Protocol::requeue_out_sent()
{
  assert(out_state != out_state_t::open);
  if (out_sent_msgs.empty()) {
    return;
  }

  out_seq -= out_sent_msgs.size();
  logger().debug("{} requeue {} items, revert out_seq to {}",
                 conn, out_sent_msgs.size(), out_seq);
  for (MessageURef& msg : out_sent_msgs) {
    msg->clear_payload();
    msg->set_seq(0);
  }
  out_pending_msgs.insert(
      out_pending_msgs.begin(),
      std::make_move_iterator(out_sent_msgs.begin()),
      std::make_move_iterator(out_sent_msgs.end()));
  out_sent_msgs.clear();
  notify_out_dispatch();
}

void Protocol::requeue_out_sent_up_to(seq_num_t seq)
{
  assert(out_state != out_state_t::open);
  if (out_sent_msgs.empty() && out_pending_msgs.empty()) {
    logger().debug("{} nothing to requeue, reset out_seq from {} to seq {}",
                   conn, out_seq, seq);
    out_seq = seq;
    return;
  }
  logger().debug("{} discarding sent msgs by seq {} (sent_len={}, out_seq={})",
                 conn, seq, out_sent_msgs.size(), out_seq);
  while (!out_sent_msgs.empty()) {
    auto cur_seq = out_sent_msgs.front()->get_seq();
    if (cur_seq == 0 || cur_seq > seq) {
      break;
    } else {
      out_sent_msgs.pop_front();
    }
  }
  requeue_out_sent();
}

void Protocol::reset_out()
{
  assert(out_state != out_state_t::open);
  out_seq = 0;
  out_pending_msgs.clear();
  out_sent_msgs.clear();
  need_keepalive = false;
  next_keepalive_ack = std::nullopt;
  ack_left = 0;
}

void Protocol::ack_out_sent(seq_num_t seq)
{
  if (conn.policy.lossy) {  // lossy connections don't keep sent messages
    return;
  }
  while (!out_sent_msgs.empty() &&
         out_sent_msgs.front()->get_seq() <= seq) {
    logger().trace("{} got ack seq {} >= {}, pop {}",
                   conn, seq, out_sent_msgs.front()->get_seq(),
                   *out_sent_msgs.front());
    out_sent_msgs.pop_front();
  }
}

seastar::future<stop_t> Protocol::try_exit_out_dispatch() {
  assert(!is_out_queued());
  return frame_assembler.flush().then([this] {
    if (!is_out_queued()) {
      // still nothing pending to send after flush,
      // the dispatching can ONLY stop now
      ceph_assert(out_dispatching);
      out_dispatching = false;
      if (unlikely(out_exit_dispatching.has_value())) {
        out_exit_dispatching->set_value();
        out_exit_dispatching = std::nullopt;
        logger().info("{} do_out_dispatch: nothing queued at {},"
                      " set out_exit_dispatching",
                      conn, out_state);
      }
      return seastar::make_ready_future<stop_t>(stop_t::yes);
    } else {
      // something is pending to send during flushing
      return seastar::make_ready_future<stop_t>(stop_t::no);
    }
  });
}

seastar::future<> Protocol::do_out_dispatch()
{
  return seastar::repeat([this] {
    switch (out_state) {
     case out_state_t::open: {
      bool still_queued = is_out_queued();
      if (unlikely(!still_queued)) {
        return try_exit_out_dispatch();
      }
      auto to_ack = ack_left;
      assert(to_ack == 0 || in_seq > 0);
      // sweep all pending out with the concrete Protocol
      return frame_assembler.write(
        sweep_out_pending_msgs_to_sent(
          need_keepalive, next_keepalive_ack, to_ack > 0)
      ).then([this, prv_keepalive_ack=next_keepalive_ack, to_ack] {
        need_keepalive = false;
        if (next_keepalive_ack == prv_keepalive_ack) {
          next_keepalive_ack = std::nullopt;
        }
        assert(ack_left >= to_ack);
        ack_left -= to_ack;
        if (!is_out_queued()) {
          return try_exit_out_dispatch();
        } else {
          // messages were enqueued during socket write
          return seastar::make_ready_future<stop_t>(stop_t::no);
        }
      });
     }
     case out_state_t::delay:
      // delay out dispatching until open
      if (out_exit_dispatching) {
        out_exit_dispatching->set_value();
        out_exit_dispatching = std::nullopt;
        logger().info("{} do_out_dispatch: delay and set out_exit_dispatching ...", conn);
      } else {
        logger().info("{} do_out_dispatch: delay ...", conn);
      }
      return out_state_changed.get_shared_future(
      ).then([] { return stop_t::no; });
     case out_state_t::drop:
      ceph_assert(out_dispatching);
      out_dispatching = false;
      if (out_exit_dispatching) {
        out_exit_dispatching->set_value();
        out_exit_dispatching = std::nullopt;
        logger().info("{} do_out_dispatch: dropped and set out_exit_dispatching", conn);
      } else {
        logger().info("{} do_out_dispatch: dropped", conn);
      }
      return seastar::make_ready_future<stop_t>(stop_t::yes);
     default:
      ceph_assert(false);
    }
  }).handle_exception_type([this] (const std::system_error& e) {
    if (e.code() != std::errc::broken_pipe &&
        e.code() != std::errc::connection_reset &&
        e.code() != error::negotiation_failure) {
      logger().error("{} do_out_dispatch(): unexpected error at {} -- {}",
                     conn, out_state, e);
      ceph_abort();
    }

    if (out_state == out_state_t::open) {
      logger().info("{} do_out_dispatch(): fault at {}, going to delay -- {}",
                    conn, out_state, e);
      std::exception_ptr eptr;
      try {
        throw e;
      } catch(...) {
        eptr = std::current_exception();
      }
      set_out_state(out_state_t::delay);
      notify_out_fault(eptr);
    } else {
      logger().info("{} do_out_dispatch(): fault at {} -- {}",
                    conn, out_state, e);
    }

    return do_out_dispatch();
  });
}

void Protocol::notify_out_dispatch()
{
  notify_out();
  if (out_dispatching) {
    // already dispatching
    return;
  }
  out_dispatching = true;
  switch (out_state) {
   case out_state_t::open:
     [[fallthrough]];
   case out_state_t::delay:
    assert(!gate.is_closed());
    gate.dispatch_in_background("do_out_dispatch", *this, [this] {
      return do_out_dispatch();
    });
    return;
   case out_state_t::drop:
    out_dispatching = false;
    return;
   default:
    ceph_assert(false);
  }
}

} // namespace crimson::net
