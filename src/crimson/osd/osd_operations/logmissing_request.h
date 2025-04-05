// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/net/Connection.h"
#include "crimson/osd/osdmap_gate.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/client_request.h"
#include "crimson/osd/pg_map.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDPGUpdateLogMissing.h"

namespace ceph {
  class Formatter;
}

namespace crimson::osd {

class ShardServices;

class OSD;
class PG;

class LogMissingRequest final :
    public PhasedOperationT<LogMissingRequest>,
    public RemoteOperation {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::logmissing_request;
  LogMissingRequest(crimson::net::ConnectionRef&&, Ref<MOSDPGUpdateLogMissing>&&);

  void print(std::ostream &) const final;
  void dump_detail(ceph::Formatter* f) const final;

  static constexpr bool can_create() { return false; }
  spg_t get_pgid() const {
    return req->get_spg();
  }
  PipelineHandle &get_handle() { return handle; }
  epoch_t get_epoch() const { return req->get_min_epoch(); }
  epoch_t get_epoch_sent_at() const {
    return req->get_map_epoch();
  }

  ConnectionPipeline &get_connection_pipeline();

  PerShardPipeline &get_pershard_pipeline(ShardServices &);

  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

  std::tuple<
    StartEvent,
    ConnectionPipeline::AwaitActive::BlockingEvent,
    ConnectionPipeline::AwaitMap::BlockingEvent,
    ConnectionPipeline::GetPGMapping::BlockingEvent,
    PerShardPipeline::CreateOrWaitPG::BlockingEvent,
    PGRepopPipeline::Process::BlockingEvent,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent,
    PGMap::PGCreationBlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent
  > tracking_events;

private:
  PGRepopPipeline &repop_pipeline(PG &pg);

  // must be after `conn` to ensure the ConnectionPipeline's is alive
  PipelineHandle handle;
  Ref<MOSDPGUpdateLogMissing> req;
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::LogMissingRequest> : fmt::ostream_formatter {};
#endif
