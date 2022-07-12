// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/pg_shard_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

PGShardManager::PGShardManager(
  OSDMapService &osdmap_service,
  const int whoami,
  crimson::net::Messenger &cluster_msgr,
  crimson::net::Messenger &public_msgr,
  crimson::mon::Client &monc,
  crimson::mgr::Client &mgrc,
  crimson::os::FuturizedStore &store)
  : core_state(whoami, osdmap_service, cluster_msgr, public_msgr,
	       monc, mgrc, store),
    local_state(whoami),
    shard_services(core_state, local_state)
{}

}
