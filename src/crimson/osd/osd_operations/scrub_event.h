// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <iostream>
#include <variant>

#include <seastar/core/future.hh>

#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operations/client_request_common.h"
#include "crimson/osd/osd_operations/common/pg_pipeline.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/pg_activation_blocker.h"
#include "crimson/osd/pg_map.h"
#include "messages/MOSDOp.h"
#include "osd/PeeringState.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"


namespace crimson::osd {
class ScrubEvent;
class ScrubRemoteEvent;	 // uses the same pipeline
}  // namespace crimson::osd

namespace fmt {
template <>
struct formatter<crimson::osd::ScrubEvent>;
}  // namespace fmt

namespace crimson::osd {

using namespace ::std::chrono;
using namespace ::std::chrono_literals;

class OSD;
class ShardServices;
class PG;


class ScrubEvent : public PhasedOperationT<ScrubEvent> {
 public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_event;

  using ScrubEventFwdFut = interruptible_future<> (
    ScrubPgIF::*)(epoch_t, [[maybe_unused]] Scrub::act_token_t);
  using ScrubEventFwdImm =
    void (ScrubPgIF::*)(epoch_t, [[maybe_unused]] Scrub::act_token_t);
  using ScrubEventFwd = std::variant<ScrubEventFwdFut, ScrubEventFwdImm>;


  class PGPipeline : public CommonPGPipeline {
    struct AwaitMap : OrderedExclusivePhaseT<AwaitMap> {
      static constexpr auto type_name = "ScrubEvent::PGPipeline::await_map";
    } await_map;

    //  do we need a pipe phase to lock the PG against other
    //  types of client operations?

    // a local synchronier, to enable us to finish an operation after creating
    // a new event.
    struct LocalSync : OrderedExclusivePhaseT<LocalSync> {
      static constexpr auto type_name = "ScrubEvent::PGPipeline::local_sync";
    } local_sync;

    struct SendReply : OrderedExclusivePhaseT<SendReply> {
      static constexpr auto type_name = "ScrubEvent::PGPipeline::send_reply";
    } send_reply;

    friend class ScrubEvent;
    friend class ScrubRemoteEvent;  // uses the same pipeline
  };

 private:
  Ref<PG> pg;
  ScrubEventFwd event_fwd_func;
  Scrub::act_token_t act_token;

  PipelineHandle handle;
  static PGPipeline& pp(PG& pg);

  ShardServices& shard_services;
  spg_t pgid;
  epoch_t epoch_queued;
  std::chrono::milliseconds delay{0s};

  const spg_t get_pgid() const { return pgid; }

  void on_pg_absent();
  seastar::future<> complete_rctx_no_pg() { return seastar::now(); }
  seastar::future<Ref<PG>> get_pg();

 public:
  virtual ~ScrubEvent();

  ScrubEvent(Ref<PG> pg,
	     ShardServices& shard_services,
	     const spg_t& pgid,
	     ScrubEventFwd func,
	     epoch_t epoch_queued,
	     Scrub::act_token_t tkn,
	     std::chrono::milliseconds delay);

  ScrubEvent(Ref<PG> pg,
	     ShardServices& shard_services,
	     const spg_t& pgid,
	     ScrubEventFwd func,
	     epoch_t epoch_queued,
	     Scrub::act_token_t tkn);

  void print(std::ostream&) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> start();

  std::tuple<StartEvent,
	     PGPipeline::AwaitMap::BlockingEvent,
	     PG_OSDMapGate::OSDMapBlocker::BlockingEvent,
	     PGPipeline::Process::BlockingEvent,
	     PGPipeline::SendReply::BlockingEvent,
	     CompletionEvent>
    tracking_events;

  PipelineHandle& get_handle() { return handle; }

  friend fmt::formatter<ScrubEvent>;
};

}  // namespace crimson::osd

template <>
struct fmt::formatter<crimson::osd::ScrubEvent> {

  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const crimson::osd::ScrubEvent& levt, FormatContext& ctx)
  {
    return format_to(ctx.out(),
		     "ScrubEvent(pgid={}, epoch={}, delay={}, token={})",
		     levt.get_pgid(),
		     levt.epoch_queued,
		     levt.delay,
		     levt.act_token);
  }
};
