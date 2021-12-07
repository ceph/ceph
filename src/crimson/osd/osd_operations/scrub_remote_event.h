// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <variant>

#include "crimson/osd/osd_operations/scrub_event.h"

namespace crimson::osd {
class ScrubRemoteEvent;
}  // namespace crimson::osd

namespace fmt {
template <>
struct formatter<crimson::osd::ScrubRemoteEvent>;
}  // namespace fmt

namespace crimson::osd {

using namespace ::std::chrono;
using namespace ::std::chrono_literals;

class OSD;
class ShardServices;
class PG;

/**
 *  ScrubRemoteEvent is used for all inter-OSD scrub messages:
 *  - reserving replicas' scrub resources
 *  - requesting & receiving scrub maps
 *
 * These event are derived from PhasedOperationT<>; they are expected to be used
 * via start_pg_operation(), which means they implement with_pg()
 */

class ScrubRemoteEvent : public PhasedOperationT<ScrubRemoteEvent> {
 public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_event;
  static constexpr bool can_create() { return false; }
  friend class OSD;

  template <typename T = void>
  using interruptible_future = ::crimson::interruptible::
    interruptible_future<::crimson::osd::IOInterruptCondition, T>;
  using ScrubRmtEventFwd =
    seastar::future<> (ScrubPgIF::*)(crimson::net::ConnectionRef conn,
				     Ref<MOSDFastDispatchOp> msg,
				     epoch_t epoch,
				     pg_shard_t from);

 private:
  Ref<PG> pg;
  ScrubRmtEventFwd event_fwd_func;
  Scrub::act_token_t act_token;
  static ScrubEvent::PGPipeline& pp(PG& pg);
  crimson::net::ConnectionRef conn;
  Ref<MOSDFastDispatchOp> scrub_op;  // MOSDFastDispatchOp or Message
  std::optional<ScrubEvent> scrub_event;
  ShardServices& shard_services;
  pg_shard_t from;
  spg_t pgid;
  epoch_t epoch_queued;
  std::chrono::milliseconds delay{0s};

  PipelineHandle handle;

  // fix to get the PGID from the OP message
  spg_t get_pgid() const { return scrub_op ? pgid : pgid; }
  epoch_t get_epoch() const { return scrub_op->get_map_epoch(); }

  void on_pg_absent();
  seastar::future<> complete_rctx_no_pg() { return seastar::now(); }
  seastar::future<Ref<PG>> get_pg();

  const pg_shard_t get_from() const { return from; }

  /**
   * populate some scrub_remote_event data members based on the contents
   * of the arriving op message
   */
  void parse_into_event(ShardServices& shard_services, Ref<PG> pg);

 public:
  virtual ~ScrubRemoteEvent();

  ScrubRemoteEvent(crimson::net::ConnectionRef conn,
		   Ref<Message> scrub_op,
		   ShardServices& shard_services,
		   const pg_shard_t& from,
		   std::chrono::milliseconds delay);

  void print(std::ostream&) const final;
  void dump_detail(ceph::Formatter* f) const final;

  seastar::future<> with_pg(ShardServices& shard_services, Ref<PG> pg);

  std::tuple<StartEvent,
	     ConnectionPipeline::AwaitActive::BlockingEvent,
	     ConnectionPipeline::AwaitMap::BlockingEvent,
	     OSD_OSDMapGate::OSDMapBlocker::BlockingEvent,
	     ConnectionPipeline::GetPG::BlockingEvent,
	     ScrubEvent::PGPipeline::WaitForActive::BlockingEvent,
	     PGActivationBlocker::BlockingEvent,
	     PGMap::PGCreationBlockingEvent,
	     ScrubEvent::PGPipeline::AwaitMap::BlockingEvent,
	     PG_OSDMapGate::OSDMapBlocker::BlockingEvent,
	     ScrubEvent::PGPipeline::Process::BlockingEvent,
	     ScrubEvent::PGPipeline::SendReply::BlockingEvent,
	     CompletionEvent>
    tracking_events;

  ConnectionPipeline& get_connection_pipeline();
  PipelineHandle& get_handle() { return handle; }
  friend fmt::formatter<ScrubRemoteEvent>;
};

}  // namespace crimson::osd

template <>
struct fmt::formatter<crimson::osd::ScrubRemoteEvent> {

  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const crimson::osd::ScrubRemoteEvent& levt, FormatContext& ctx)
  {
    return format_to(ctx.out(),
		     "ScrubRemoteEvent(pgid={}, epoch={}, delay={}, token={})",
		     levt.get_pgid(),
		     levt.epoch_queued,
		     levt.delay,
		     levt.act_token);
  }
};
