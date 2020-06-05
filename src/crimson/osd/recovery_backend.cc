// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/recovery_backend.h"
#include "crimson/osd/pg.h"

#include "osd/osd_types.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

hobject_t RecoveryBackend::get_temp_recovery_object(
  const hobject_t& target,
  eversion_t version)
{
  ostringstream ss;
  ss << "temp_recovering_" << pg.get_info().pgid << "_" << version
    << "_" << pg.get_info().history.same_interval_since << "_" << target.snap;
  hobject_t hoid = target.make_temp_hobject(ss.str());
  logger().debug("{} {}", __func__, hoid);
  return hoid;
}

void RecoveryBackend::clean_up(ceph::os::Transaction& t,
			       const std::string& why)
{
  for (auto& soid : temp_contents) {
    t.remove(pg.get_collection_ref()->get_cid(),
	      ghobject_t(soid, ghobject_t::NO_GEN, pg.get_pg_whoami().shard));
  }
  temp_contents.clear();

  for (auto& [soid, recovery_waiter] : recovering) {
    if (recovery_waiter.obc && recovery_waiter.obc->obs.exists) {
      recovery_waiter.obc->drop_recovery_read();
      recovery_waiter.interrupt(why);
    }
  }
  recovering.clear();
}

