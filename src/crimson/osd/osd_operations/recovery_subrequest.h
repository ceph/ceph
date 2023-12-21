// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/net/Connection.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/pg.h"
#include "crimson/common/type_helpers.h"
#include "messages/MOSDFastDispatchOp.h"

namespace crimson::osd {

class PG;

class RecoverySubRequest final : public PhasedOperationT<RecoverySubRequest> {
public:
  static constexpr OperationTypeCode type =
    OperationTypeCode::background_recovery_sub;

  RecoverySubRequest(
    crimson::net::ConnectionRef conn,
    Ref<MOSDFastDispatchOp>&& m)
    : conn(conn), m(m) {}

  void print(std::ostream& out) const final
  {
    out << *m;
  }

  void dump_detail(Formatter *f) const final
  {
  }

  static constexpr bool can_create() { return false; }
  spg_t get_pgid() const {
    return m->get_spg();
  }
  PipelineHandle &get_handle() { return handle; }
  epoch_t get_epoch() const { return m->get_min_epoch(); }

  ConnectionPipeline &get_connection_pipeline();

  PerShardPipeline &get_pershard_pipeline(ShardServices &);

  crimson::net::Connection &get_connection() {
    assert(conn);
    return *conn;
  };

  seastar::future<crimson::net::ConnectionFRef> prepare_remote_submission() {
    assert(conn);
    return conn.get_foreign(
    ).then([this](auto f_conn) {
      conn.reset();
      return f_conn;
    });
  }
  void finish_remote_submission(crimson::net::ConnectionFRef _conn) {
    assert(!conn);
    conn = make_local_shared_foreign(std::move(_conn));
  }

  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

  std::tuple<
    StartEvent,
    ConnectionPipeline::AwaitActive::BlockingEvent,
    ConnectionPipeline::AwaitMap::BlockingEvent,
    ConnectionPipeline::GetPGMapping::BlockingEvent,
    PerShardPipeline::CreateOrWaitPG::BlockingEvent,
    PGMap::PGCreationBlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent,
    CompletionEvent
  > tracking_events;

private:
  crimson::net::ConnectionRef conn;
  // must be after `conn` to ensure the ConnectionPipeline's is alive
  PipelineHandle handle;
  Ref<MOSDFastDispatchOp> m;
};

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::RecoverySubRequest> : fmt::ostream_formatter {};
#endif
