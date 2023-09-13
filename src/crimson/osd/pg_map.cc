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

using std::make_pair;

namespace crimson::osd {

PGMap::PGCreationState::PGCreationState(spg_t pgid) : pgid(pgid) {}
PGMap::PGCreationState::~PGCreationState() {}

void PGMap::PGCreationState::dump_detail(Formatter *f) const
{
  f->dump_stream("pgid") << pgid;
  f->dump_bool("creating", creating);
}

PGMap::wait_for_pg_ret
PGMap::wait_for_pg(PGCreationBlockingEvent::TriggerI&& trigger, spg_t pgid)
{
  if (auto pg = get_pg(pgid)) {
    return make_pair(
      wait_for_pg_fut(wait_for_pg_ertr::ready_future_marker{}, pg),
      true);
  } else {
    auto &state = pgs_creating.emplace(pgid, pgid).first->second;
    return make_pair(
      wait_for_pg_fut(
	trigger.maybe_record_blocking(state.promise.get_shared_future(), state)
      ), state.creating);
  }
}

void PGMap::remove_pg(spg_t pgid) {
  ceph_assert(pgs.erase(pgid) == 1);
}

Ref<PG> PGMap::get_pg(spg_t pgid)
{
  if (auto pg = pgs.find(pgid); pg != pgs.end()) {
    return pg->second;
  } else {
    return nullptr;
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

  auto creating_iter = pgs_creating.find(pgid);
  ceph_assert(creating_iter != pgs_creating.end());
  auto promise = std::move(creating_iter->second.promise);
  pgs_creating.erase(creating_iter);
  promise.set_value(pg);
}

void PGMap::pg_loaded(spg_t pgid, Ref<PG> pg)
{
  ceph_assert(!pgs.count(pgid));
  pgs.emplace(pgid, pg);
}

void PGMap::pg_creation_canceled(spg_t pgid)
{
  logger().debug("PGMap::pg_creation_canceled: {}", pgid);
  ceph_assert(!pgs.count(pgid));

  auto creating_iter = pgs_creating.find(pgid);
  ceph_assert(creating_iter != pgs_creating.end());
  auto promise = std::move(creating_iter->second.promise);
  pgs_creating.erase(creating_iter);
  promise.set_exception(
    crimson::ct_error::ecanceled::exception_ptr()
  );
}

PGMap::~PGMap() {}

}
