// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/pg_map.h"
#include "crimson/common/log.h"
#include "crimson/osd/pg.h"
#include "common/Formatter.h"

SET_SUBSYS(osd);

using std::make_pair;

namespace crimson::osd {

seastar::future<core_id_t> PGShardMapping::get_or_create_pg_mapping(
  spg_t pgid,
  core_id_t core)
{
  LOG_PREFIX(PGShardMapping::get_or_create_pg_mapping);
  auto find_iter = pg_to_core.find(pgid);
  if (find_iter != pg_to_core.end()) {
    ceph_assert_always(find_iter->second != NULL_CORE);
    if (core != NULL_CORE) {
      ceph_assert_always(find_iter->second == core);
    }
    return seastar::make_ready_future<core_id_t>(find_iter->second);
  } else {
    return container().invoke_on(0,[pgid, core, FNAME]
      (auto &primary_mapping) {
      auto [insert_iter, inserted] = primary_mapping.pg_to_core.emplace(pgid, core);
      ceph_assert_always(inserted);
      ceph_assert_always(primary_mapping.core_to_num_pgs.size() > 0);
      std::map<core_id_t, unsigned>::iterator core_iter;
      if (core == NULL_CORE) {
        core_iter = std::min_element(
          primary_mapping.core_to_num_pgs.begin(),
          primary_mapping.core_to_num_pgs.end(),
            [](const auto &left, const auto &right) {
            return left.second < right.second;
        });
      } else {
        core_iter = primary_mapping.core_to_num_pgs.find(core);
      }
      ceph_assert_always(primary_mapping.core_to_num_pgs.end() != core_iter);
      insert_iter->second = core_iter->first;
      core_iter->second++;
      DEBUG("mapping pg {} to core: {} with num_pgs of: {}",
            pgid, insert_iter->second, core_iter->second);
      return primary_mapping.container().invoke_on_others(
        [pgid = insert_iter->first, core = insert_iter->second]
        (auto &other_mapping) {
        ceph_assert_always(core != NULL_CORE);
        auto [insert_iter, inserted] = other_mapping.pg_to_core.emplace(pgid, core);
        ceph_assert_always(inserted);
      });
    }).then([this, pgid] {
      auto find_iter = pg_to_core.find(pgid);
      return seastar::make_ready_future<core_id_t>(find_iter->second);
    });
  }
}

seastar::future<> PGShardMapping::remove_pg_mapping(spg_t pgid) {
  LOG_PREFIX(PGShardMapping::remove_pg_mapping);
  DEBUG("{}", pgid);
  return container().invoke_on(0, [pgid](auto &primary_mapping) {
    auto iter = primary_mapping.pg_to_core.find(pgid);
    ceph_assert_always(iter != primary_mapping.pg_to_core.end());
    ceph_assert_always(iter->second != NULL_CORE);
    auto count_iter = primary_mapping.core_to_num_pgs.find(iter->second);
    ceph_assert_always(count_iter != primary_mapping.core_to_num_pgs.end());
    ceph_assert_always(count_iter->second > 0);
    --(count_iter->second);
    primary_mapping.pg_to_core.erase(iter);
    return primary_mapping.container().invoke_on_others(
      [pgid](auto &other_mapping) {
      auto iter = other_mapping.pg_to_core.find(pgid);
      ceph_assert_always(iter != other_mapping.pg_to_core.end());
      ceph_assert_always(iter->second != NULL_CORE);
      other_mapping.pg_to_core.erase(iter);
    });
  });
}

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
  LOG_PREFIX(PGMap::set_creating);
  DEBUG("Creating {}", pgid);
  ceph_assert(pgs.count(pgid) == 0);
  auto pg = pgs_creating.find(pgid);
  ceph_assert(pg != pgs_creating.end());
  ceph_assert(pg->second.creating == false);
  pg->second.creating = true;
}

void PGMap::pg_created(spg_t pgid, Ref<PG> pg)
{
  LOG_PREFIX(PGMap::pg_created);
  DEBUG("Created {}", pgid);
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
  LOG_PREFIX(PGMap::pg_creation_canceled);
  DEBUG("{}", pgid);
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
