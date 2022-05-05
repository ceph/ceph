// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request_common.h"
#include "crimson/osd/osd_operations/common/pg_pipeline.h"
#include "crimson/osd/pg_activation_blocker.h"
#include "crimson/osd/pg_map.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDOp.h"

namespace crimson::osd {
class PG;
class OSD;

class ClientRequest final : public PhasedOperationT<ClientRequest>,
                            private CommonClientRequest {
  OSD &osd;
  crimson::net::ConnectionRef conn;
  Ref<MOSDOp> m;
  OpInfo op_info;

public:
  class ConnectionPipeline {
    struct AwaitMap : OrderedExclusivePhaseT<AwaitMap> {
      static constexpr auto type_name =
        "ClientRequest::ConnectionPipeline::await_map";
    } await_map;

    struct GetPG : OrderedExclusivePhaseT<GetPG> {
      static constexpr auto type_name =
        "ClientRequest::ConnectionPipeline::get_pg";
    } get_pg;

    friend class ClientRequest;
    friend class LttngBackend;
  };

  class PGPipeline : public CommonPGPipeline {
    struct AwaitMap : OrderedExclusivePhaseT<AwaitMap> {
      static constexpr auto type_name = "ClientRequest::PGPipeline::await_map";
    } await_map;
    struct WaitRepop : OrderedConcurrentPhaseT<WaitRepop> {
      static constexpr auto type_name = "ClientRequest::PGPipeline::wait_repop";
    } wait_repop;
    struct SendReply : OrderedExclusivePhaseT<SendReply> {
      static constexpr auto type_name = "ClientRequest::PGPipeline::send_reply";
    } send_reply;
    friend class ClientRequest;
    friend class LttngBackend;
  };

  static constexpr OperationTypeCode type = OperationTypeCode::client_request;

  ClientRequest(OSD &osd, crimson::net::ConnectionRef, Ref<MOSDOp> &&m);
  ~ClientRequest();

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;

public:
  seastar::future<> start();
  bool same_session_and_pg(const ClientRequest& other_op) const;

private:
  template <typename FuncT>
  interruptible_future<> with_sequencer(FuncT&& func);
  auto reply_op_error(Ref<PG>& pg, int err);

  enum class seq_mode_t {
    IN_ORDER,
    OUT_OF_ORDER
  };

  interruptible_future<seq_mode_t> do_process(
    Ref<PG>& pg,
    crimson::osd::ObjectContextRef obc);
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition> process_pg_op(
    Ref<PG> &pg);
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition, seq_mode_t> process_op(
    Ref<PG> &pg);
  bool is_pg_op() const;

  ConnectionPipeline &cp();
  PGPipeline &pp(PG &pg);

  class OpSequencer& sequencer;
  // a tombstone used currently by OpSequencer. In the future it's supposed
  // to be replaced with a reusage of OpTracking facilities.
  bool finished = false;
  friend class OpSequencer;

  template <typename Errorator>
  using interruptible_errorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      Errorator>;

  bool is_misdirected(const PG& pg) const;

public:
  std::tuple<
    StartEvent,
    ConnectionPipeline::AwaitMap::BlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent,
    ConnectionPipeline::GetPG::BlockingEvent,
    PGMap::PGCreationBlockingEvent,
    PGPipeline::AwaitMap::BlockingEvent,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent,
    PGPipeline::WaitForActive::BlockingEvent,
    PGActivationBlocker::BlockingEvent,
    PGPipeline::RecoverMissing::BlockingEvent,
    PGPipeline::GetOBC::BlockingEvent,
    PGPipeline::Process::BlockingEvent,
    PGPipeline::WaitRepop::BlockingEvent,
    PGPipeline::SendReply::BlockingEvent,
    CompletionEvent
  > tracking_events;

  friend class LttngBackend;
  friend class HistoricBackend;
};

}
