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

namespace crimson::osd {

seastar::future<> RecoverySubRequest::with_pg(
  ShardServices &shard_services, Ref<PG> pgref)
{
  logger().debug("{}: {}", "RecoverySubRequest::with_pg", *this);

  track_event<StartEvent>();
  IRef opref = this;
  return interruptor::with_interruption([this, pgref] {
    return pgref->get_recovery_backend()->handle_recovery_op(m, conn
    ).then_interruptible([this] {
      logger().debug("{}: complete", *this);
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
  return get_osd_priv(conn.get()).peering_request_conn_pipeline;
}

}
