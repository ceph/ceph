// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDOp.h"

namespace crimson::osd {
class PG;
class OSD;

class ClientRequest final : public OperationT<ClientRequest> {
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
  class PGPipeline {
    OrderedExclusivePhase await_map = {
      "ClientRequest::PGPipeline::await_map"
    };
    OrderedExclusivePhase wait_for_active = {
      "ClientRequest::PGPipeline::wait_for_active"
    };
    OrderedExclusivePhase recover_missing = {
      "ClientRequest::PGPipeline::recover_missing"
    };
    OrderedExclusivePhase get_obc = {
      "ClientRequest::PGPipeline::get_obc"
    };
    OrderedExclusivePhase process = {
      "ClientRequest::PGPipeline::process"
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

private:
  interruptible_future<> do_recover_missing(Ref<PG>& pgref);
  interruptible_future<> do_process(
      Ref<PG>& pg,
      crimson::osd::ObjectContextRef obc);
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition> process_pg_op(
    Ref<PG> &pg);
  ::crimson::interruptible::interruptible_future<
    ::crimson::osd::IOInterruptCondition> process_op(
    Ref<PG> &pg);
  bool is_pg_op() const;

  ConnectionPipeline &cp();
  PGPipeline &pp(PG &pg);

  OpSequencer& sequencer;
  const uint64_t prev_op_id;

  template <typename Errorator>
  using interruptible_errorator =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      Errorator>;
private:
  bool is_misdirected(const PG& pg) const;
};

}
