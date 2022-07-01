// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/net/Connection.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/replicated_request.h"
#include "crimson/osd/pg_map.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDPGUpdateLogMissingReply.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class ShardServices;

class OSD;
class PG;

class LogMissingRequestReply final : public PhasedOperationT<LogMissingRequestReply> {
public:
  class PGPipeline {
    struct AwaitMap : OrderedExclusivePhaseT<AwaitMap> {
      static constexpr auto type_name = "LogMissingRequestReply::PGPipeline::await_map";
    } await_map;
    struct Process : OrderedExclusivePhaseT<Process> {
      static constexpr auto type_name = "LogMissingRequestReply::PGPipeline::process";
    } process;
    friend LogMissingRequestReply;
  };
  static constexpr OperationTypeCode type = OperationTypeCode::logmissing_request_reply;
  LogMissingRequestReply(crimson::net::ConnectionRef&&, Ref<MOSDPGUpdateLogMissingReply>&&);

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;

  static constexpr bool can_create() { return false; }
  spg_t get_pgid() const {
    return req->get_spg();
  }
  ConnectionPipeline &get_connection_pipeline();
  PipelineHandle &get_handle() { return handle; }
  epoch_t get_epoch() const { return req->get_min_epoch(); }

  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

  std::tuple<
    ConnectionPipeline::AwaitActive::BlockingEvent,
    ConnectionPipeline::AwaitMap::BlockingEvent,
    ConnectionPipeline::GetPG::BlockingEvent,
    PGMap::PGCreationBlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent
  > tracking_events;

private:
  RepRequest::PGPipeline &pp(PG &pg);

  crimson::net::ConnectionRef conn;
  Ref<MOSDPGUpdateLogMissingReply> req;
};

}
