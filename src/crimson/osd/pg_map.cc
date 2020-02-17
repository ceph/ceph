// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/pg_map.h"

#include "crimson/osd/pg.h"
#include "common/Formatter.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

PGMap::PGCreationState::PGCreationState(spg_t pgid) : pgid(pgid) {}
PGMap::PGCreationState::~PGCreationState() {}

void PGMap::PGCreationState::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pgid;
  f->dump_bool("creating", creating);
}

std::pair<blocking_future<Ref<PG>>, bool> PGMap::get_pg(spg_t pgid, bool wait)
{
  if (auto pg = pgs.find(pgid); pg != pgs.end()) {
    return make_pair(make_ready_blocking_future<Ref<PG>>(pg->second), true);
  } else if (!wait) {
    return make_pair(make_ready_blocking_future<Ref<PG>>(nullptr), true);
  } else {
    auto &state = pgs_creating.emplace(pgid, pgid).first->second;
    return make_pair(
      state.make_blocking_future(state.promise.get_shared_future()),
      state.creating);
  }
}

void PGMap::set_creating(spg_t pgid)
{
  logger().debug("Creating {}", pgid);
  ceph_assert(pgs.count(pgid) == 0);
  auto pg = pgs_creating.find(pgid);
  ceph_assert(pg != pgs_creating.end());
  ceph_assert(pg->second.creating == false);
  pg->second.creating = true;
}

void PGMap::pg_created(spg_t pgid, Ref<PG> pg)
{
  logger().debug("Created {}", pgid);
  ceph_assert(!pgs.count(pgid));
  pgs.emplace(pgid, pg);

  auto state = pgs_creating.find(pgid);
  ceph_assert(state != pgs_creating.end());
  state->second.promise.set_value(pg);
  pgs_creating.erase(pgid);
}

void PGMap::pg_loaded(spg_t pgid, Ref<PG> pg)
{
  ceph_assert(!pgs.count(pgid));
  pgs.emplace(pgid, pg);
}

PGMap::~PGMap() {}

}
