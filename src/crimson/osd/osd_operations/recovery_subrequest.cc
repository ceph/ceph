// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "crimson/osd/osd_operations/recovery_subrequest.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osd_connection_priv.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson {
  template <>
  struct EventBackendRegistry<osd::RecoverySubRequest> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
}

SET_SUBSYS(osd);

namespace crimson::osd {

seastar::future<> RecoverySubRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pgref)
{
  track_event<StartEvent>();
  IRef opref = this;
  return interruptor::with_interruption([this, pgref] {
    LOG_PREFIX(RecoverySubRequest::with_pg);
    DEBUGI("{}: {}", "RecoverySubRequest::with_pg", *this);
    return pgref->get_recovery_backend()->handle_recovery_op(m, r_conn
    ).then_interruptible([this] {
      LOG_PREFIX(RecoverySubRequest::with_pg);
      DEBUGI("{}: complete", *this);
      return handle.complete();
    });
  }, [](std::exception_ptr) {
    return seastar::now();
  }, pgref).finally([this, opref=std::move(opref), pgref] {
    logger().debug("{}: exit", *this);
    track_event<CompletionEvent>();
    handle.exit();
  });
}

ConnectionPipeline &RecoverySubRequest::get_connection_pipeline()
{
  return get_osd_priv(&get_local_connection()
         ).client_request_conn_pipeline;
}

PerShardPipeline &RecoverySubRequest::get_pershard_pipeline(
    ShardServices &shard_services)
{
  return shard_services.get_peering_request_pipeline();
}

}
