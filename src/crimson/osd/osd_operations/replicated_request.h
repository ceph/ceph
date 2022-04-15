// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/net/Connection.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/pg_map.h"
#include "crimson/common/type_helpers.h"

class MOSDRepOp;

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class OSD;
class PG;

class RepRequest final : public PhasedOperationT<RepRequest> {
public:
  class ConnectionPipeline {
    struct AwaitMap : OrderedExclusivePhaseT<AwaitMap> {
      static constexpr auto type_name =
	"RepRequest::ConnectionPipeline::await_map";
    } await_map;
    struct GetPG : OrderedExclusivePhaseT<GetPG> {
      static constexpr auto type_name =
	"RepRequest::ConnectionPipeline::get_pg";
    } get_pg;
    friend RepRequest;
  };
  class PGPipeline {
    struct AwaitMap : OrderedExclusivePhaseT<AwaitMap> {
      static constexpr auto type_name = "RepRequest::PGPipeline::await_map";
    } await_map;
    struct Process : OrderedExclusivePhaseT<Process> {
      static constexpr auto type_name = "RepRequest::PGPipeline::process";
    } process;
    friend RepRequest;
  };
  static constexpr OperationTypeCode type = OperationTypeCode::replicated_request;
  RepRequest(OSD&, crimson::net::ConnectionRef&&, Ref<MOSDRepOp>&&);

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> start();

  std::tuple<
    ConnectionPipeline::AwaitMap::BlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent,
    ConnectionPipeline::GetPG::BlockingEvent,
    PGMap::PGCreationBlockingEvent
  > tracking_events;

private:
  ConnectionPipeline &cp();
  PGPipeline &pp(PG &pg);

  OSD &osd;
  crimson::net::ConnectionRef conn;
  Ref<MOSDRepOp> req;
};

}
