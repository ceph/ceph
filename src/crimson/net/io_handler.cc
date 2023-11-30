// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "io_handler.h"

#include "auth/Auth.h"

#include "crimson/common/formatter.h"
#include "crimson/common/log.h"
#include "crimson/net/Errors.h"
#include "crimson/net/chained_dispatchers.h"
#include "crimson/net/SocketMessenger.h"
#include "msg/Message.h"
#include "msg/msg_fmt.h"

using namespace ceph::msgr::v2;
using crimson::common::local_conf;

namespace {

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_ms);
}

[[noreturn]] void abort_in_fault() {
  throw std::system_error(make_error_code(crimson::net::error::negotiation_failure));
}

[[noreturn]] void abort_protocol() {
  throw std::system_error(make_error_code(crimson::net::error::protocol_aborted));
}

std::size_t get_msg_size(const FrameAssembler &rx_frame_asm)
{
  ceph_assert(rx_frame_asm.get_num_segments() > 0);
  size_t sum = 0;
  // we don't include SegmentIndex::Msg::HEADER.
  for (size_t idx = 1; idx < rx_frame_asm.get_num_segments(); idx++) {
    sum += rx_frame_asm.get_segment_logical_len(idx);
  }
  return sum;
}

} // namespace anonymous

namespace crimson::net {

IOHandler::IOHandler(ChainedDispatchers &dispatchers,
                     SocketConnection &conn)
  : shard_states(shard_states_t::create(
        seastar::this_shard_id(), io_state_t::none)),
    dispatchers(dispatchers),
    conn(conn),
    conn_ref(conn.get_local_shared_foreign_from_this())
{}

IOHandler::~IOHandler()
{
  // close_io() must be finished
  ceph_assert_always(maybe_prv_shard_states == nullptr);
  // should be true in the according shard
  // ceph_assert_always(shard_states->assert_closed_and_exit());
  assert(!conn_ref);
}

#ifdef UNIT_TESTS_BUILT
IOHandler::sweep_ret
#else
ceph::bufferlist
#endif
IOHandler::sweep_out_pending_msgs_to_sent(
  bool require_keepalive,
  std::optional<utime_t> maybe_keepalive_ack,
  bool require_ack)
{
  std::size_t num_msgs = out_pending_msgs.size();
  ceph::bufferlist bl;

#ifdef UNIT_TESTS_BUILT
  std::vector<Tag> tags;
#endif

  if (unlikely(require_keepalive)) {
    auto keepalive_frame = KeepAliveFrame::Encode();
    bl.append(frame_assembler->get_buffer(keepalive_frame));
#ifdef UNIT_TESTS_BUILT
    auto tag = KeepAliveFrame::tag;
    tags.push_back(tag);
#endif
  }

  if (unlikely(maybe_keepalive_ack.has_value())) {
    auto keepalive_ack_frame = KeepAliveFrameAck::Encode(*maybe_keepalive_ack);
    bl.append(frame_assembler->get_buffer(keepalive_ack_frame));
#ifdef UNIT_TESTS_BUILT
    auto tag = KeepAliveFrameAck::tag;
    tags.push_back(tag);
#endif
  }

  if (require_ack && num_msgs == 0u) {
    auto ack_frame = AckFrame::Encode(in_seq);
    bl.append(frame_assembler->get_buffer(ack_frame));
#ifdef UNIT_TESTS_BUILT
    auto tag = AckFrame::tag;
    tags.push_back(tag);
#endif
  }

  std::for_each(
      out_pending_msgs.begin(),
      out_pending_msgs.begin()+num_msgs,
      [this, &bl
#ifdef UNIT_TESTS_BUILT
        , &tags
#endif
      ](const MessageFRef& msg) {
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
                             ceph_le64(in_seq),
                             footer.flags,      header.compat_version,
                             header.reserved};

    auto message = MessageFrame::Encode(header2,
        msg->get_payload(), msg->get_middle(), msg->get_data());
    logger().debug("{} --> #{} === {} ({})",
		   conn, msg->get_seq(), *msg, msg->get_type());
    bl.append(frame_assembler->get_buffer(message));
#ifdef UNIT_TESTS_BUILT
    auto tag = MessageFrame::tag;
    tags.push_back(tag);
#endif
  });

  if (!conn.policy.lossy) {
    out_sent_msgs.insert(
        out_sent_msgs.end(),
        std::make_move_iterator(out_pending_msgs.begin()),
        std::make_move_iterator(out_pending_msgs.end()));
  }
  out_pending_msgs.clear();

#ifdef UNIT_TESTS_BUILT
  return sweep_ret{std::move(bl), tags};
#else
  return bl;
#endif
}

seastar::future<> IOHandler::send(MessageURef _msg)
{
  // may be invoked from any core
  MessageFRef msg = seastar::make_foreign(std::move(_msg));
  auto cc_seq = io_crosscore.prepare_submit();
  auto source_core = seastar::this_shard_id();
  // sid may be changed on-the-fly during the submission
  if (source_core == get_shard_id()) {
    return do_send(cc_seq, source_core, std::move(msg));
  } else {
    logger().trace("{} send() {} is directed to core {} -- {}",
                   conn, cc_seq, get_shard_id(), *msg);
    return seastar::smp::submit_to(
        get_shard_id(),
        [this, cc_seq, source_core, msg=std::move(msg)]() mutable {
      return send_recheck_shard(cc_seq, source_core, std::move(msg));
    });
  }
}

seastar::future<> IOHandler::send_recheck_shard(
  cc_seq_t cc_seq,
  core_id_t source_core,
  MessageFRef msg)
{
  // sid may be changed on-the-fly during the submission
  if (seastar::this_shard_id() == get_shard_id()) {
    return do_send(cc_seq, source_core, std::move(msg));
  } else {
    logger().debug("{} send_recheck_shard() {} "
                   "is redirected from core {} to {} -- {}",
                   conn, cc_seq, source_core, get_shard_id(), *msg);
    return seastar::smp::submit_to(
        get_shard_id(),
        [this, cc_seq, source_core, msg=std::move(msg)]() mutable {
      return send_recheck_shard(cc_seq, source_core, std::move(msg));
    });
  }
}

seastar::future<> IOHandler::do_send(
  cc_seq_t cc_seq,
  core_id_t source_core,
  MessageFRef msg)
{
  assert(seastar::this_shard_id() == get_shard_id());
  if (io_crosscore.proceed_or_wait(cc_seq, source_core)) {
    logger().trace("{} do_send() got {} from core {}: send message -- {}",
                   conn, cc_seq, source_core, *msg);
    if (get_io_state() != io_state_t::drop) {
      out_pending_msgs.push_back(std::move(msg));
      notify_out_dispatch();
    }
    return seastar::now();
  } else {
    logger().debug("{} do_send() got {} from core {}, wait at {} -- {}",
                   conn, cc_seq, source_core,
                   io_crosscore.get_in_seq(source_core),
                   *msg);
    return io_crosscore.wait(cc_seq, source_core
    ).then([this, cc_seq, source_core, msg=std::move(msg)]() mutable {
      return send_recheck_shard(cc_seq, source_core, std::move(msg));
    });
  }
}

seastar::future<> IOHandler::send_keepalive()
{
  // may be invoked from any core
  auto cc_seq = io_crosscore.prepare_submit();
  auto source_core = seastar::this_shard_id();
  // sid may be changed on-the-fly during the submission
  if (source_core == get_shard_id()) {
    return do_send_keepalive(cc_seq, source_core);
  } else {
    logger().trace("{} send_keepalive() {} is directed to core {}",
                   conn, cc_seq, get_shard_id());
    return seastar::smp::submit_to(
        get_shard_id(),
        [this, cc_seq, source_core] {
      return send_keepalive_recheck_shard(cc_seq, source_core);
    });
  }
}

seastar::future<> IOHandler::send_keepalive_recheck_shard(
  cc_seq_t cc_seq,
  core_id_t source_core)
{
  // sid may be changed on-the-fly during the submission
  if (seastar::this_shard_id() == get_shard_id()) {
    return do_send_keepalive(cc_seq, source_core);
  } else {
    logger().debug("{} send_keepalive_recheck_shard() {} "
                   "is redirected from core {} to {}",
                   conn, cc_seq, source_core, get_shard_id());
    return seastar::smp::submit_to(
        get_shard_id(),
        [this, cc_seq, source_core] {
      return send_keepalive_recheck_shard(cc_seq, source_core);
    });
  }
}

seastar::future<> IOHandler::do_send_keepalive(
  cc_seq_t cc_seq,
  core_id_t source_core)
{
  assert(seastar::this_shard_id() == get_shard_id());
  if (io_crosscore.proceed_or_wait(cc_seq, source_core)) {
    logger().trace("{} do_send_keeplive() got {} from core {}: need_keepalive={}",
                   conn, cc_seq, source_core, need_keepalive);
    if (!need_keepalive) {
      need_keepalive = true;
      notify_out_dispatch();
    }
    return seastar::now();
  } else {
    logger().debug("{} do_send_keepalive() got {} from core {}, wait at {}",
                   conn, cc_seq, source_core,
                   io_crosscore.get_in_seq(source_core));
    return io_crosscore.wait(cc_seq, source_core
    ).then([this, cc_seq, source_core] {
      return send_keepalive_recheck_shard(cc_seq, source_core);
    });
  }
}

void IOHandler::mark_down()
{
  ceph_assert_always(seastar::this_shard_id() == get_shard_id());
  ceph_assert_always(get_io_state() != io_state_t::none);
  need_dispatch_reset = false;
  if (get_io_state() == io_state_t::drop) {
    return;
  }

  auto cc_seq = proto_crosscore.prepare_submit();
  logger().info("{} mark_down() at {}, send {} notify_mark_down()",
                conn, io_stat_printer{*this}, cc_seq);
  do_set_io_state(io_state_t::drop);
  shard_states->dispatch_in_background(
      "notify_mark_down", conn, [this, cc_seq] {
    return seastar::smp::submit_to(
        conn.get_messenger_shard_id(), [this, cc_seq] {
      return handshake_listener->notify_mark_down(cc_seq);
    });
  });
}

void IOHandler::print_io_stat(std::ostream &out) const
{
  assert(seastar::this_shard_id() == get_shard_id());
  out << "io_stat("
      << "io_state=" << fmt::format("{}", get_io_state())
      << ", in_seq=" << in_seq
      << ", out_seq=" << out_seq
      << ", out_pending_msgs_size=" << out_pending_msgs.size()
      << ", out_sent_msgs_size=" << out_sent_msgs.size()
      << ", need_ack=" << (ack_left > 0)
      << ", need_keepalive=" << need_keepalive
      << ", need_keepalive_ack=" << bool(next_keepalive_ack)
      << ")";
}

void IOHandler::assign_frame_assembler(FrameAssemblerV2Ref fa)
{
  assert(fa != nullptr);
  ceph_assert_always(frame_assembler == nullptr);
  frame_assembler = std::move(fa);
  ceph_assert_always(
      frame_assembler->get_shard_id() == get_shard_id());
  // should have been set through dispatch_accept/connect()
  ceph_assert_always(
      frame_assembler->get_socket_shard_id() == get_shard_id());
  ceph_assert_always(frame_assembler->is_socket_valid());
}

void IOHandler::do_set_io_state(
    io_state_t new_state,
    std::optional<cc_seq_t> cc_seq,
    FrameAssemblerV2Ref fa,
    bool set_notify_out)
{
  ceph_assert_always(seastar::this_shard_id() == get_shard_id());
  auto prv_state = get_io_state();
  logger().debug("{} got {}do_set_io_state(): prv_state={}, new_state={}, "
                 "fa={}, set_notify_out={}, at {}",
                 conn,
                 cc_seq.has_value() ? fmt::format("{} ", *cc_seq) : "",
                 prv_state, new_state,
                 fa ? "present" : "N/A", set_notify_out,
                 io_stat_printer{*this});
  ceph_assert_always(!(
    (new_state == io_state_t::none && prv_state != io_state_t::none) ||
    (new_state == io_state_t::open && prv_state == io_state_t::open)
  ));

  if (prv_state == io_state_t::drop) {
    // only possible due to a racing mark_down() from user
    if (new_state == io_state_t::open) {
      assign_frame_assembler(std::move(fa));
      frame_assembler->shutdown_socket<false>(nullptr);
    } else {
      assert(fa == nullptr);
    }
    return;
  }

  bool dispatch_in = false;
  if (new_state == io_state_t::open) {
    // to open
    ceph_assert_always(protocol_is_connected == true);
    assign_frame_assembler(std::move(fa));
    dispatch_in = true;
  } else if (prv_state == io_state_t::open) {
    // from open
    ceph_assert_always(protocol_is_connected == true);
    protocol_is_connected = false;
    assert(fa == nullptr);
    ceph_assert_always(frame_assembler->is_socket_valid());
    frame_assembler->shutdown_socket<false>(nullptr);
  } else {
    assert(fa == nullptr);
  }

  if (new_state == io_state_t::delay) {
    need_notify_out = set_notify_out;
    if (need_notify_out) {
      maybe_notify_out_dispatch();
    }
  } else {
    assert(set_notify_out == false);
    need_notify_out = false;
  }

  // FIXME: simplify and drop the prv_state == new_state case
  if (prv_state != new_state) {
    shard_states->set_io_state(new_state);
  }

  /*
   * not atomic below
   */

  if (dispatch_in) {
    do_in_dispatch();
  }
}

seastar::future<> IOHandler::set_io_state(
    cc_seq_t cc_seq,
    io_state_t new_state,
    FrameAssemblerV2Ref fa,
    bool set_notify_out)
{
  assert(seastar::this_shard_id() == get_shard_id());
  if (!proto_crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} set_io_state(), wait at {}",
                   conn, cc_seq, proto_crosscore.get_in_seq());
    return proto_crosscore.wait(cc_seq
    ).then([this, cc_seq, new_state,
            fa=std::move(fa), set_notify_out]() mutable {
      return set_io_state(cc_seq, new_state, std::move(fa), set_notify_out);
    });
  }

  do_set_io_state(new_state, cc_seq, std::move(fa), set_notify_out);
  return seastar::now();
}

seastar::future<IOHandler::exit_dispatching_ret>
IOHandler::wait_io_exit_dispatching(
    cc_seq_t cc_seq)
{
  assert(seastar::this_shard_id() == get_shard_id());
  if (!proto_crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} wait_io_exit_dispatching(), wait at {}",
                   conn, cc_seq, proto_crosscore.get_in_seq());
    return proto_crosscore.wait(cc_seq
    ).then([this, cc_seq] {
      return wait_io_exit_dispatching(cc_seq);
    });
  }

  logger().debug("{} got {} wait_io_exit_dispatching()",
                 conn, cc_seq);
  ceph_assert_always(get_io_state() != io_state_t::open);
  ceph_assert_always(frame_assembler != nullptr);
  ceph_assert_always(!frame_assembler->is_socket_valid());
  return seastar::futurize_invoke([this] {
    // cannot be running in parallel with to_new_sid()
    if (maybe_dropped_sid.has_value()) {
      ceph_assert_always(get_io_state() == io_state_t::drop);
      assert(shard_states->assert_closed_and_exit());
      auto prv_sid = *maybe_dropped_sid;
      return seastar::smp::submit_to(prv_sid, [this] {
        logger().debug("{} got wait_io_exit_dispatching from prv_sid", conn);
        assert(maybe_prv_shard_states != nullptr);
        return maybe_prv_shard_states->wait_io_exit_dispatching();
      });
    } else {
      return shard_states->wait_io_exit_dispatching();
    }
  }).then([this] {
    logger().debug("{} finish wait_io_exit_dispatching at {}",
                   conn, io_stat_printer{*this});
    ceph_assert_always(frame_assembler != nullptr);
    ceph_assert_always(!frame_assembler->is_socket_valid());
    frame_assembler->set_shard_id(conn.get_messenger_shard_id());
    return exit_dispatching_ret{
      std::move(frame_assembler),
      get_states()};
  });
}

seastar::future<> IOHandler::reset_session(
    cc_seq_t cc_seq,
    bool full)
{
  assert(seastar::this_shard_id() == get_shard_id());
  if (!proto_crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} reset_session(), wait at {}",
                   conn, cc_seq, proto_crosscore.get_in_seq());
    return proto_crosscore.wait(cc_seq
    ).then([this, cc_seq, full] {
      return reset_session(cc_seq, full);
    });
  }

  logger().debug("{} got {} reset_session({})",
                 conn, cc_seq, full);
  assert(get_io_state() != io_state_t::open);
  reset_in();
  if (full) {
    reset_out();
    dispatch_remote_reset();
  }
  return seastar::now();
}

seastar::future<> IOHandler::reset_peer_state(
    cc_seq_t cc_seq)
{
  assert(seastar::this_shard_id() == get_shard_id());
  if (!proto_crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} reset_peer_state(), wait at {}",
                   conn, cc_seq, proto_crosscore.get_in_seq());
    return proto_crosscore.wait(cc_seq
    ).then([this, cc_seq] {
      return reset_peer_state(cc_seq);
    });
  }

  logger().debug("{} got {} reset_peer_state()",
                 conn, cc_seq);
  assert(get_io_state() != io_state_t::open);
  reset_in();
  do_requeue_out_sent_up_to(0);
  discard_out_sent();
  return seastar::now();
}

seastar::future<> IOHandler::requeue_out_sent(
    cc_seq_t cc_seq)
{
  assert(seastar::this_shard_id() == get_shard_id());
  if (!proto_crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} requeue_out_sent(), wait at {}",
                   conn, cc_seq, proto_crosscore.get_in_seq());
    return proto_crosscore.wait(cc_seq
    ).then([this, cc_seq] {
      return requeue_out_sent(cc_seq);
    });
  }

  logger().debug("{} got {} requeue_out_sent()",
                 conn, cc_seq);
  do_requeue_out_sent();
  return seastar::now();
}

void IOHandler::do_requeue_out_sent()
{
  assert(get_io_state() != io_state_t::open);
  if (out_sent_msgs.empty()) {
    return;
  }

  out_seq -= out_sent_msgs.size();
  logger().debug("{} requeue {} items, revert out_seq to {}",
                 conn, out_sent_msgs.size(), out_seq);
  for (MessageFRef& msg : out_sent_msgs) {
    msg->clear_payload();
    msg->set_seq(0);
  }
  out_pending_msgs.insert(
      out_pending_msgs.begin(),
      std::make_move_iterator(out_sent_msgs.begin()),
      std::make_move_iterator(out_sent_msgs.end()));
  out_sent_msgs.clear();
  maybe_notify_out_dispatch();
}

seastar::future<> IOHandler::requeue_out_sent_up_to(
    cc_seq_t cc_seq,
    seq_num_t msg_seq)
{
  assert(seastar::this_shard_id() == get_shard_id());
  if (!proto_crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} requeue_out_sent_up_to(), wait at {}",
                   conn, cc_seq, proto_crosscore.get_in_seq());
    return proto_crosscore.wait(cc_seq
    ).then([this, cc_seq, msg_seq] {
      return requeue_out_sent_up_to(cc_seq, msg_seq);
    });
  }

  logger().debug("{} got {} requeue_out_sent_up_to({})",
                 conn, cc_seq, msg_seq);
  do_requeue_out_sent_up_to(msg_seq);
  return seastar::now();
}

void IOHandler::do_requeue_out_sent_up_to(seq_num_t seq)
{
  assert(get_io_state() != io_state_t::open);
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
  do_requeue_out_sent();
}

void IOHandler::reset_in()
{
  assert(get_io_state() != io_state_t::open);
  in_seq = 0;
}

void IOHandler::reset_out()
{
  assert(get_io_state() != io_state_t::open);
  discard_out_sent();
  out_pending_msgs.clear();
  need_keepalive = false;
  next_keepalive_ack = std::nullopt;
  ack_left = 0;
}

void IOHandler::discard_out_sent()
{
  assert(get_io_state() != io_state_t::open);
  out_seq = 0;
  out_sent_msgs.clear();
}

seastar::future<>
IOHandler::dispatch_accept(
    cc_seq_t cc_seq,
    seastar::shard_id new_sid,
    ConnectionFRef conn_fref,
    bool is_replace)
{
  return to_new_sid(cc_seq, new_sid, std::move(conn_fref), is_replace);
}

seastar::future<>
IOHandler::dispatch_connect(
    cc_seq_t cc_seq,
    seastar::shard_id new_sid,
    ConnectionFRef conn_fref)
{
  return to_new_sid(cc_seq, new_sid, std::move(conn_fref), std::nullopt);
}

seastar::future<>
IOHandler::cleanup_prv_shard(seastar::shard_id prv_sid)
{
  assert(seastar::this_shard_id() == get_shard_id());
  return seastar::smp::submit_to(prv_sid, [this] {
    logger().debug("{} got cleanup_prv_shard()", conn);
    assert(maybe_prv_shard_states != nullptr);
    auto ref_prv_states = std::move(maybe_prv_shard_states);
    auto &prv_states = *ref_prv_states;
    return prv_states.close(
    ).then([ref_prv_states=std::move(ref_prv_states)] {
      ceph_assert_always(ref_prv_states->assert_closed_and_exit());
    });
  }).then([this] {
    ceph_assert_always(maybe_prv_shard_states == nullptr);
  });
}

seastar::future<>
IOHandler::to_new_sid(
    cc_seq_t cc_seq,
    seastar::shard_id new_sid,
    ConnectionFRef conn_fref,
    std::optional<bool> is_replace)
{
  ceph_assert_always(seastar::this_shard_id() == get_shard_id());
  if (!proto_crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} to_new_sid(), wait at {}",
                   conn, cc_seq, proto_crosscore.get_in_seq());
    return proto_crosscore.wait(cc_seq
    ).then([this, cc_seq, new_sid, is_replace,
            conn_fref=std::move(conn_fref)]() mutable {
      return to_new_sid(cc_seq, new_sid, std::move(conn_fref), is_replace);
    });
  }

  bool is_accept_or_connect = is_replace.has_value();
  logger().debug("{} got {} to_new_sid_1(new_sid={}, {}) at {}",
                 conn, cc_seq, new_sid,
                 fmt::format("{}",
                   is_accept_or_connect ?
                   (*is_replace ? "accept(replace)" : "accept(!replace)") :
                   "connect"),
                 io_stat_printer{*this});
  auto next_cc_seq = ++cc_seq;

  if (get_io_state() != io_state_t::drop) {
    ceph_assert_always(conn_ref);
    if (new_sid != seastar::this_shard_id()) {
      dispatchers.ms_handle_shard_change(conn_ref, new_sid, is_accept_or_connect);
      // user can make changes
    }
  } else {
    // it is possible that both io_handler and protocolv2 are
    // trying to close each other from different cores simultaneously.
    assert(!protocol_is_connected);
  }

  if (get_io_state() != io_state_t::drop) {
    if (is_accept_or_connect) {
      // protocol_is_connected can be from true to true here if the replacing is
      // happening to a connected connection.
    } else {
      ceph_assert_always(protocol_is_connected == false);
    }
    protocol_is_connected = true;
  } else {
    assert(!protocol_is_connected);
  }

  bool is_dropped = false;
  if (get_io_state() == io_state_t::drop) {
    is_dropped = true;
  }
  ceph_assert_always(get_io_state() != io_state_t::open);

  // apply the switching atomically
  ceph_assert_always(conn_ref);
  conn_ref.reset();
  auto prv_sid = get_shard_id();
  ceph_assert_always(maybe_prv_shard_states == nullptr);
  maybe_prv_shard_states = std::move(shard_states);
  shard_states = shard_states_t::create_from_previous(
      *maybe_prv_shard_states, new_sid);
  assert(new_sid == get_shard_id());
  // broadcast shard change to all the io waiters, atomically.
  io_crosscore.reset_wait();

  return seastar::smp::submit_to(new_sid,
      [this, next_cc_seq, is_dropped, prv_sid, is_replace, conn_fref=std::move(conn_fref)]() mutable {
    logger().debug("{} got {} to_new_sid_2(prv_sid={}, is_dropped={}, {}) at {}",
                   conn, next_cc_seq, prv_sid, is_dropped,
                   fmt::format("{}",
                     is_replace.has_value() ?
                     (*is_replace ? "accept(replace)" : "accept(!replace)") :
                     "connect"),
                   io_stat_printer{*this});

    ceph_assert_always(seastar::this_shard_id() == get_shard_id());
    ceph_assert_always(get_io_state() != io_state_t::open);
    ceph_assert_always(!maybe_dropped_sid.has_value());
    ceph_assert_always(proto_crosscore.proceed_or_wait(next_cc_seq));

    if (is_dropped) {
      ceph_assert_always(get_io_state() == io_state_t::drop);
      ceph_assert_always(shard_states->assert_closed_and_exit());
      maybe_dropped_sid = prv_sid;
      // cleanup_prv_shard() will be done in a follow-up close_io()
    } else {
      // possible at io_state_t::drop

      // previous shard is not cleaned,
      // but close_io() is responsible to clean up the current shard,
      // so cleanup the previous shard here.
      shard_states->dispatch_in_background(
          "cleanup_prv_sid", conn, [this, prv_sid] {
        return cleanup_prv_shard(prv_sid);
      });
      maybe_notify_out_dispatch();
    }

    ceph_assert_always(!conn_ref);
    // assign even if already dropping
    conn_ref = make_local_shared_foreign(std::move(conn_fref));

    if (get_io_state() != io_state_t::drop) {
      if (is_replace.has_value()) {
        dispatchers.ms_handle_accept(conn_ref, prv_sid, *is_replace);
      } else {
        dispatchers.ms_handle_connect(conn_ref, prv_sid);
      }
      // user can make changes
    }
  });
}

seastar::future<> IOHandler::set_accepted_sid(
    cc_seq_t cc_seq,
    seastar::shard_id sid,
    ConnectionFRef conn_fref)
{
  assert(seastar::this_shard_id() == get_shard_id());
  assert(get_io_state() == io_state_t::none);
  ceph_assert_always(conn_ref);
  conn_ref.reset();
  assert(maybe_prv_shard_states == nullptr);
  shard_states.reset();
  shard_states = shard_states_t::create(sid, io_state_t::none);
  return seastar::smp::submit_to(sid,
      [this, cc_seq, conn_fref=std::move(conn_fref)]() mutable {
    // must be the first to proceed
    ceph_assert_always(proto_crosscore.proceed_or_wait(cc_seq));

    logger().debug("{} set accepted sid", conn);
    ceph_assert_always(seastar::this_shard_id() == get_shard_id());
    ceph_assert_always(get_io_state() == io_state_t::none);
    assert(maybe_prv_shard_states == nullptr);
    ceph_assert_always(!conn_ref);
    conn_ref = make_local_shared_foreign(std::move(conn_fref));
  });
}

void IOHandler::dispatch_reset(bool is_replace)
{
  ceph_assert_always(get_io_state() == io_state_t::drop);
  if (!need_dispatch_reset) {
    return;
  }
  need_dispatch_reset = false;
  ceph_assert_always(conn_ref);

  dispatchers.ms_handle_reset(conn_ref, is_replace);
  // user can make changes
}

void IOHandler::dispatch_remote_reset()
{
  if (get_io_state() == io_state_t::drop) {
    return;
  }
  ceph_assert_always(conn_ref);

  dispatchers.ms_handle_remote_reset(conn_ref);
  // user can make changes
}

void IOHandler::ack_out_sent(seq_num_t seq)
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

seastar::future<>
IOHandler::do_out_dispatch(shard_states_t &ctx)
{
  return seastar::repeat([this, &ctx] {
    switch (ctx.get_io_state()) {
     case io_state_t::open: {
      if (unlikely(!is_out_queued())) {
        // try exit open dispatching
        return frame_assembler->flush<false>(
        ).then([this, &ctx] {
          if (ctx.get_io_state() != io_state_t::open || is_out_queued()) {
            return seastar::make_ready_future<stop_t>(stop_t::no);
          }
          // still nothing pending to send after flush,
          // open dispatching can ONLY stop now
          ctx.exit_out_dispatching("exit-open", conn);
          return seastar::make_ready_future<stop_t>(stop_t::yes);
        });
      }

      auto require_keepalive = need_keepalive;
      need_keepalive = false;
      auto maybe_keepalive_ack = next_keepalive_ack;
      next_keepalive_ack = std::nullopt;
      auto to_ack = ack_left;
      assert(to_ack == 0 || in_seq > 0);
      ack_left = 0;
#ifdef UNIT_TESTS_BUILT
      auto ret = sweep_out_pending_msgs_to_sent(
          require_keepalive, maybe_keepalive_ack, to_ack > 0);
      return frame_assembler->intercept_frames(ret.tags, true
      ).then([this, bl=std::move(ret.bl)]() mutable {
        return frame_assembler->write<false>(std::move(bl));
      }
#else
      auto bl = sweep_out_pending_msgs_to_sent(
          require_keepalive, maybe_keepalive_ack, to_ack > 0);
      return frame_assembler->write<false>(std::move(bl)
#endif
      ).then([this, &ctx] {
        if (ctx.get_io_state() != io_state_t::open) {
          return frame_assembler->flush<false>(
          ).then([] {
            return seastar::make_ready_future<stop_t>(stop_t::no);
          });
        }

        // FIXME: may leak a flush if state is changed after return and before
        // the next repeat body.
        return seastar::make_ready_future<stop_t>(stop_t::no);
      });
     }
     case io_state_t::delay:
      // delay out dispatching until open
      ctx.notify_out_dispatching_stopped("delay...", conn);
      return ctx.wait_state_change(
      ).then([] { return stop_t::no; });
     case io_state_t::drop:
      ctx.exit_out_dispatching("dropped", conn);
      return seastar::make_ready_future<stop_t>(stop_t::yes);
     case io_state_t::switched:
      ctx.exit_out_dispatching("switched", conn);
      return seastar::make_ready_future<stop_t>(stop_t::yes);
     default:
      ceph_abort("impossible");
    }
  }).handle_exception_type([this, &ctx](const std::system_error& e) {
    auto io_state = ctx.get_io_state();
    if (e.code() != std::errc::broken_pipe &&
        e.code() != std::errc::connection_reset &&
        e.code() != error::negotiation_failure) {
      logger().error("{} do_out_dispatch(): unexpected error at {} -- {}",
                     conn, io_state, e.what());
      ceph_abort();
    }

    if (io_state == io_state_t::open) {
      auto cc_seq = proto_crosscore.prepare_submit();
      logger().info("{} do_out_dispatch(): fault at {}, {}, going to delay -- {}, "
                    "send {} notify_out_fault()",
                    conn, io_state, io_stat_printer{*this}, e.what(), cc_seq);
      std::exception_ptr eptr;
      try {
        throw e;
      } catch(...) {
        eptr = std::current_exception();
      }
      do_set_io_state(io_state_t::delay);
      shard_states->dispatch_in_background(
          "notify_out_fault(out)", conn, [this, cc_seq, eptr] {
        auto states = get_states();
        return seastar::smp::submit_to(
            conn.get_messenger_shard_id(), [this, cc_seq, eptr, states] {
          return handshake_listener->notify_out_fault(
              cc_seq, "do_out_dispatch", eptr, states);
        });
      });
    } else {
      if (io_state != io_state_t::switched) {
        logger().info("{} do_out_dispatch(): fault at {}, {} -- {}",
                      conn, io_state, io_stat_printer{*this}, e.what());
      } else {
        logger().info("{} do_out_dispatch(): fault at {} -- {}",
                      conn, io_state, e.what());
      }
    }

    return do_out_dispatch(ctx);
  });
}

void IOHandler::maybe_notify_out_dispatch()
{
  ceph_assert_always(seastar::this_shard_id() == get_shard_id());
  if (is_out_queued()) {
    notify_out_dispatch();
  }
}

void IOHandler::notify_out_dispatch()
{
  ceph_assert_always(seastar::this_shard_id() == get_shard_id());
  assert(is_out_queued());
  if (need_notify_out) {
    auto cc_seq = proto_crosscore.prepare_submit();
    logger().debug("{} send {} notify_out()",
                   conn, cc_seq);
    shard_states->dispatch_in_background(
        "notify_out", conn, [this, cc_seq] {
      return seastar::smp::submit_to(
          conn.get_messenger_shard_id(), [this, cc_seq] {
        return handshake_listener->notify_out(cc_seq);
      });
    });
  }
  if (shard_states->try_enter_out_dispatching()) {
    shard_states->dispatch_in_background(
        "do_out_dispatch", conn, [this] {
      return do_out_dispatch(*shard_states);
    });
  }
}

seastar::future<>
IOHandler::read_message(
    shard_states_t &ctx,
    utime_t throttle_stamp,
    std::size_t msg_size)
{
  return frame_assembler->read_frame_payload<false>(
  ).then([this, throttle_stamp, msg_size, &ctx](auto payload) {
    if (unlikely(ctx.get_io_state() != io_state_t::open)) {
      logger().debug("{} triggered {} during read_message()",
                     conn, ctx.get_io_state());
      abort_protocol();
    }

    utime_t recv_stamp{seastar::lowres_system_clock::now()};

    // we need to get the size before std::moving segments data
    auto msg_frame = MessageFrame::Decode(*payload);
    // XXX: paranoid copy just to avoid oops
    ceph_msg_header2 current_header = msg_frame.header();

    logger().trace("{} got {} + {} + {} byte message,"
                   " envelope type={} src={} off={} seq={}",
                   conn,
                   msg_frame.front_len(),
                   msg_frame.middle_len(),
                   msg_frame.data_len(),
                   current_header.type,
                   conn.get_peer_name(),
                   current_header.data_off,
                   current_header.seq);

    ceph_msg_header header{current_header.seq,
                           current_header.tid,
                           current_header.type,
                           current_header.priority,
                           current_header.version,
                           ceph_le32(msg_frame.front_len()),
                           ceph_le32(msg_frame.middle_len()),
                           ceph_le32(msg_frame.data_len()),
                           current_header.data_off,
                           conn.get_peer_name(),
                           current_header.compat_version,
                           current_header.reserved,
                           ceph_le32(0)};
    ceph_msg_footer footer{ceph_le32(0), ceph_le32(0),
                           ceph_le32(0), ceph_le64(0), current_header.flags};

    Message *message = decode_message(nullptr, 0, header, footer,
        msg_frame.front(), msg_frame.middle(), msg_frame.data(), nullptr);
    if (!message) {
      logger().warn("{} decode message failed", conn);
      abort_in_fault();
    }

    // store reservation size in message, so we don't get confused
    // by messages entering the dispatch queue through other paths.
    message->set_dispatch_throttle_size(msg_size);

    message->set_throttle_stamp(throttle_stamp);
    message->set_recv_stamp(recv_stamp);
    message->set_recv_complete_stamp(utime_t{seastar::lowres_system_clock::now()});

    // check received seq#.  if it is old, drop the message.
    // note that incoming messages may skip ahead.  this is convenient for the
    // client side queueing because messages can't be renumbered, but the (kernel)
    // client will occasionally pull a message out of the sent queue to send
    // elsewhere.  in that case it doesn't matter if we "got" it or not.
    uint64_t cur_seq = in_seq;
    if (message->get_seq() <= cur_seq) {
      logger().error("{} got old message {} <= {} {}, discarding",
                     conn, message->get_seq(), cur_seq, *message);
      if (HAVE_FEATURE(conn.features, RECONNECT_SEQ) &&
          local_conf()->ms_die_on_old_message) {
        ceph_assert(0 == "old msgs despite reconnect_seq feature");
      }
      return seastar::now();
    } else if (message->get_seq() > cur_seq + 1) {
      logger().error("{} missed message? skipped from seq {} to {}",
                     conn, cur_seq, message->get_seq());
      if (local_conf()->ms_die_on_skipped_message) {
        ceph_assert(0 == "skipped incoming seq");
      }
    }

    // note last received message.
    in_seq = message->get_seq();
    if (conn.policy.lossy) {
      logger().debug("{} <== #{} === {} ({})",
                     conn,
                     message->get_seq(),
                     *message,
                     message->get_type());
    } else {
      logger().debug("{} <== #{},{} === {} ({})",
                     conn,
                     message->get_seq(),
                     current_header.ack_seq,
                     *message,
                     message->get_type());
    }

    // notify ack
    if (!conn.policy.lossy) {
      ++ack_left;
      notify_out_dispatch();
    }

    ack_out_sent(current_header.ack_seq);

    // TODO: change MessageRef with seastar::shared_ptr
    auto msg_ref = MessageRef{message, false};
    assert(ctx.get_io_state() == io_state_t::open);
    assert(get_io_state() == io_state_t::open);
    ceph_assert_always(conn_ref);

    // throttle the reading process by the returned future
    return dispatchers.ms_dispatch(conn_ref, std::move(msg_ref));
    // user can make changes
  });
}

void IOHandler::do_in_dispatch()
{
  shard_states->enter_in_dispatching();
  shard_states->dispatch_in_background(
      "do_in_dispatch", conn, [this, &ctx=*shard_states] {
    return seastar::keep_doing([this, &ctx] {
      return frame_assembler->read_main_preamble<false>(
      ).then([this, &ctx](auto ret) {
        switch (ret.tag) {
          case Tag::MESSAGE: {
            size_t msg_size = get_msg_size(*ret.rx_frame_asm);
            return seastar::futurize_invoke([this] {
              // throttle_message() logic
              if (!conn.policy.throttler_messages) {
                return seastar::now();
              }
              // TODO: message throttler
              ceph_abort("TODO");
              return seastar::now();
            }).then([this, msg_size] {
              // throttle_bytes() logic
              if (!conn.policy.throttler_bytes) {
                return seastar::now();
              }
              if (!msg_size) {
                return seastar::now();
              }
              logger().trace("{} wants {} bytes from policy throttler {}/{}",
                             conn, msg_size,
                             conn.policy.throttler_bytes->get_current(),
                             conn.policy.throttler_bytes->get_max());
              return conn.policy.throttler_bytes->get(msg_size);
            }).then([this, msg_size, &ctx] {
              // TODO: throttle_dispatch_queue() logic
              utime_t throttle_stamp{seastar::lowres_system_clock::now()};
              return read_message(ctx, throttle_stamp, msg_size);
            });
          }
          case Tag::ACK:
            return frame_assembler->read_frame_payload<false>(
            ).then([this](auto payload) {
              // handle_message_ack() logic
              auto ack = AckFrame::Decode(payload->back());
              logger().debug("{} GOT AckFrame: seq={}", conn, ack.seq());
              ack_out_sent(ack.seq());
            });
          case Tag::KEEPALIVE2:
            return frame_assembler->read_frame_payload<false>(
            ).then([this](auto payload) {
              // handle_keepalive2() logic
              auto keepalive_frame = KeepAliveFrame::Decode(payload->back());
              logger().debug("{} GOT KeepAliveFrame: timestamp={}",
                             conn, keepalive_frame.timestamp());
              // notify keepalive ack
              next_keepalive_ack = keepalive_frame.timestamp();
              if (seastar::this_shard_id() == get_shard_id()) {
                notify_out_dispatch();
              }

              last_keepalive = seastar::lowres_system_clock::now();
            });
          case Tag::KEEPALIVE2_ACK:
            return frame_assembler->read_frame_payload<false>(
            ).then([this](auto payload) {
              // handle_keepalive2_ack() logic
              auto keepalive_ack_frame = KeepAliveFrameAck::Decode(payload->back());
              auto _last_keepalive_ack =
                seastar::lowres_system_clock::time_point{keepalive_ack_frame.timestamp()};
              set_last_keepalive_ack(_last_keepalive_ack);
              logger().debug("{} GOT KeepAliveFrameAck: timestamp={}",
                             conn, _last_keepalive_ack);
            });
          default: {
            logger().warn("{} do_in_dispatch() received unexpected tag: {}",
                          conn, static_cast<uint32_t>(ret.tag));
            abort_in_fault();
          }
        }
      });
    }).handle_exception([this, &ctx](std::exception_ptr eptr) {
      const char *e_what;
      try {
        std::rethrow_exception(eptr);
      } catch (std::exception &e) {
        e_what = e.what();
      }

      auto io_state = ctx.get_io_state();
      if (io_state == io_state_t::open) {
        auto cc_seq = proto_crosscore.prepare_submit();
        logger().info("{} do_in_dispatch(): fault at {}, {}, going to delay -- {}, "
                      "send {} notify_out_fault()",
                      conn, io_state, io_stat_printer{*this}, e_what, cc_seq);
        do_set_io_state(io_state_t::delay);
        shard_states->dispatch_in_background(
            "notify_out_fault(in)", conn, [this, cc_seq, eptr] {
          auto states = get_states();
          return seastar::smp::submit_to(
              conn.get_messenger_shard_id(), [this, cc_seq, eptr, states] {
            return handshake_listener->notify_out_fault(
                cc_seq, "do_in_dispatch", eptr, states);
          });
        });
      } else {
        if (io_state != io_state_t::switched) {
          logger().info("{} do_in_dispatch(): fault at {}, {} -- {}",
                        conn, io_state, io_stat_printer{*this}, e_what);
        } else {
          logger().info("{} do_in_dispatch(): fault at {} -- {}",
                        conn, io_state, e_what);
        }
      }
    }).finally([&ctx] {
      ctx.exit_in_dispatching();
    });
  });
}

seastar::future<>
IOHandler::close_io(
    cc_seq_t cc_seq,
    bool is_dispatch_reset,
    bool is_replace)
{
  ceph_assert_always(seastar::this_shard_id() == get_shard_id());
  if (!proto_crosscore.proceed_or_wait(cc_seq)) {
    logger().debug("{} got {} close_io(), wait at {}",
                   conn, cc_seq, proto_crosscore.get_in_seq());
    return proto_crosscore.wait(cc_seq
    ).then([this, cc_seq, is_dispatch_reset, is_replace] {
      return close_io(cc_seq, is_dispatch_reset, is_replace);
    });
  }

  logger().debug("{} got {} close_io(reset={}, replace={})",
                 conn, cc_seq, is_dispatch_reset, is_replace);
  ceph_assert_always(get_io_state() == io_state_t::drop);

  if (is_dispatch_reset) {
    dispatch_reset(is_replace);
  }

  ceph_assert_always(conn_ref);
  conn_ref.reset();

  // cannot be running in parallel with to_new_sid()
  if (maybe_dropped_sid.has_value()) {
    assert(shard_states->assert_closed_and_exit());
    auto prv_sid = *maybe_dropped_sid;
    return cleanup_prv_shard(prv_sid);
  } else {
    return shard_states->close(
    ).then([this] {
      assert(shard_states->assert_closed_and_exit());
    });
  }
}

/*
 * IOHandler::shard_states_t
 */

void
IOHandler::shard_states_t::notify_out_dispatching_stopped(
    const char *what, SocketConnection &conn)
{
  assert(seastar::this_shard_id() == sid);
  if (unlikely(out_exit_dispatching.has_value())) {
    out_exit_dispatching->set_value();
    out_exit_dispatching = std::nullopt;
    logger().info("{} do_out_dispatch: stop({}) at {}, set out_exit_dispatching",
                  conn, what, io_state);
  } else {
    if (unlikely(io_state != io_state_t::open)) {
      logger().info("{} do_out_dispatch: stop({}) at {}, no out_exit_dispatching",
                    conn, what, io_state);
    }
  }
}

seastar::future<>
IOHandler::shard_states_t::wait_io_exit_dispatching()
{
  assert(seastar::this_shard_id() == sid);
  assert(io_state != io_state_t::open);
  assert(!gate.is_closed());
  return seastar::when_all(
    [this] {
      if (out_exit_dispatching) {
        return out_exit_dispatching->get_future();
      } else {
        return seastar::now();
      }
    }(),
    [this] {
      if (in_exit_dispatching) {
        return in_exit_dispatching->get_future();
      } else {
        return seastar::now();
      }
    }()
  ).discard_result();
}

IOHandler::shard_states_ref_t
IOHandler::shard_states_t::create_from_previous(
    shard_states_t &prv_states,
    seastar::shard_id new_sid)
{
  auto io_state = prv_states.io_state;
  assert(io_state != io_state_t::open);
  auto ret = shard_states_t::create(new_sid, io_state);
  if (io_state == io_state_t::drop) {
    // the new gate should not never be used
    auto fut = ret->gate.close();
    ceph_assert_always(fut.available());
  }
  prv_states.set_io_state(io_state_t::switched);
  return ret;
}

} // namespace crimson::net
