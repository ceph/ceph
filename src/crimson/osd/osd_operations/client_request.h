// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/net/Connection.h"
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
  OrderedPipelinePhase::Handle handle;

public:
  class ConnectionPipeline {
    OrderedPipelinePhase await_map = {
      "ClientRequest::ConnectionPipeline::await_map"
    };
    OrderedPipelinePhase get_pg = {
      "ClientRequest::ConnectionPipeline::get_pg"
    };
    friend class ClientRequest;
  };
  class PGPipeline {
    OrderedPipelinePhase await_map = {
      "ClientRequest::PGPipeline::await_map"
    };
    OrderedPipelinePhase wait_for_active = {
      "ClientRequest::PGPipeline::wait_for_active"
    };
    OrderedPipelinePhase recover_missing = {
      "ClientRequest::PGPipeline::recover_missing"
    };
    OrderedPipelinePhase get_obc = {
      "ClientRequest::PGPipeline::get_obc"
    };
    OrderedPipelinePhase process = {
      "ClientRequest::PGPipeline::process"
    };
    friend class ClientRequest;
  };

  static constexpr OperationTypeCode type = OperationTypeCode::client_request;

  ClientRequest(OSD &osd, crimson::net::ConnectionRef, Ref<MOSDOp> &&m);

  void print(std::ostream &) const final;
  void dump_detail(Formatter *f) const final;

public:
  seastar::future<> start();

private:
  seastar::future<> process_pg_op(
    Ref<PG> &pg);
  seastar::future<> process_op(
    Ref<PG> &pg);
  bool is_pg_op() const;

  ConnectionPipeline &cp();
  PGPipeline &pp(PG &pg);
};

}
