// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/osd/watch.h"
#include "crimson/osd/osd_operations/internal_client_request.h"

#include <fmt/ostream.h>

// This translation unit holds the only part of the Watch machinery that is
// coupled to the PG operation framework: the watch-timeout operation and the
// Watch::do_watch_timeout() member that starts it. Keeping it out of watch.cc
// lets watch.cc (the Notify/Watch bookkeeping) be compiled and unit-tested
// without pulling in the entire OSD operation stack (InternalClientRequest ->
// PG::run_executer/submit_executer -> OpsExecuter -> ObjectContextLoader ...).

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {
class WatchTimeoutRequest;
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::WatchTimeoutRequest> : fmt::ostream_formatter {};
#endif

namespace crimson::osd {

// a watcher can remove itself if it has not seen a notification after a period of time.
// in the case, we need to drop it also from the persisted `ObjectState` instance.
// this operation resembles a bit the `_UNWATCH` subop.
class WatchTimeoutRequest final : public InternalClientRequest {
public:
  WatchTimeoutRequest(WatchRef watch, Ref<PG> pg)
    : InternalClientRequest(std::move(pg)),
      watch(std::move(watch)) {
  }

  const hobject_t& get_target_oid() const final;
  PG::do_osd_ops_params_t get_do_osd_ops_params() const final;
  std::vector<OSDOp> create_osd_ops() final;

private:
  WatchRef watch;
};

const hobject_t& WatchTimeoutRequest::get_target_oid() const
{
  assert(watch->obc);
  return watch->obc->get_oid();
}

PG::do_osd_ops_params_t
WatchTimeoutRequest::get_do_osd_ops_params() const
{
  osd_reqid_t reqid;
  reqid.name = watch->entity_name;
  PG::do_osd_ops_params_t params{
    watch->conn,
    reqid,
    ceph_clock_now(),
    get_pg().get_osdmap_epoch(),
    entity_inst_t{ watch->entity_name, watch->winfo.addr },
    0
  };
  logger().debug("{}: params.reqid={}", __func__, params.reqid);
  return params;
}

std::vector<OSDOp> WatchTimeoutRequest::create_osd_ops()
{
  logger().debug("{}", __func__);
  assert(watch);
  OSDOp osd_op;
  osd_op.op.op = CEPH_OSD_OP_WATCH;
  osd_op.op.flags = 0;
  osd_op.op.watch.op = CEPH_OSD_WATCH_OP_UNWATCH;
  osd_op.op.watch.cookie = watch->winfo.cookie;
  return std::vector{std::move(osd_op)};
}

// ///////////////////////////////////////////////////////////////////////////
// a 'Watch' timeout handler

void Watch::do_watch_timeout()
{
  assert(pg);
  auto [op, fut] = pg->get_shard_services().start_operation<WatchTimeoutRequest>(
    shared_from_this(), pg);
  std::ignore = std::move(fut).then([op=std::move(op), this] {
    return send_disconnect_msg();
  });
}

} // namespace crimson::osd
