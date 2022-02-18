// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request_common.h"
#include "crimson/osd/osd_operations/common/pg_pipeline.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDOp.h"

namespace crimson::osd {
class PG;
class OSD;

class ClientRequest final : public OperationT<ClientRequest>,
                            private CommonClientRequest {
  OSD &osd;
  crimson::net::ConnectionRef conn;
  Ref<MOSDOp> m;
  OpInfo op_info;
  PipelineHandle handle;

public:
  class ConnectionPipeline {
    OrderedExclusivePhase await_map = {
      "ClientRequest::ConnectionPipeline::await_map"
    };
    OrderedExclusivePhase get_pg = {
      "ClientRequest::ConnectionPipeline::get_pg"
    };
    friend class ClientRequest;
  };
  class PGPipeline : public CommonPGPipeline {
    OrderedExclusivePhase await_map = {
      "ClientRequest::PGPipeline::await_map"
    };
    OrderedConcurrentPhase wait_repop = {
      "ClientRequest::PGPipeline::wait_repop"
    };
    OrderedExclusivePhase send_reply = {
      "ClientRequest::PGPipeline::send_reply"
    };
    friend class ClientRequest;
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
private:
  bool is_misdirected(const PG& pg) const;
};

}
