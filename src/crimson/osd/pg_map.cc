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
  core_id_t core_expected)
{
  LOG_PREFIX(PGShardMapping::get_or_create_pg_mapping);
  auto find_iter = pg_to_core.find(pgid);
  if (find_iter != pg_to_core.end()) {
    auto core_found = find_iter->second;
    assert(core_found != NULL_CORE);
    if (core_expected != NULL_CORE && core_expected != core_found) {
      ERROR("the mapping is inconsistent for pg {}: core {}, expected {}",
            pgid, core_found, core_expected);
      ceph_abort("The pg mapping is inconsistent!");
    }
    return seastar::make_ready_future<core_id_t>(core_found);
  } else {
    DEBUG("calling primary to add mapping for pg {} to the expected core {}",
          pgid, core_expected);
    return container().invoke_on(
        0, [pgid, core_expected, FNAME](auto &primary_mapping) {
      auto core_to_update = core_expected;
      auto find_iter = primary_mapping.pg_to_core.find(pgid);
      if (find_iter != primary_mapping.pg_to_core.end()) {
        // this pgid was already mapped within primary_mapping, assert that the
        // mapping is consistent and avoid emplacing once again.
        auto core_found = find_iter->second;
        assert(core_found != NULL_CORE);
        if (core_expected != NULL_CORE) {
          if (core_expected != core_found) {
            ERROR("the mapping is inconsistent for pg {} (primary): core {}, expected {}",
                  pgid, core_found, core_expected);
            ceph_abort("The pg mapping is inconsistent!");
          }
          // core_expected == core_found
          DEBUG("mapping pg {} to core {} (primary): already mapped and expected",
                pgid, core_to_update);
        } else { // core_expected == NULL_CORE
          core_to_update = core_found;
          DEBUG("mapping pg {} to core {} (primary): already mapped",
                pgid, core_to_update);
        }
        // proceed to broadcast core_to_update
      } else { // find_iter == primary_mapping.pg_to_core.end()
        // this pgid isn't mapped within primary_mapping,
        // add the mapping and ajust core_to_num_pgs
        ceph_assert_always(primary_mapping.core_to_num_pgs.size() > 0);
        std::map<core_id_t, unsigned>::iterator count_iter;
        if (core_expected == NULL_CORE) {
          count_iter = std::min_element(
            primary_mapping.core_to_num_pgs.begin(),
            primary_mapping.core_to_num_pgs.end(),
            [](const auto &left, const auto &right) {
              return left.second < right.second;
            }
          );
          core_to_update = count_iter->first;
        } else { // core_expected != NULL_CORE
          count_iter = primary_mapping.core_to_num_pgs.find(core_to_update);
        }
        ceph_assert_always(primary_mapping.core_to_num_pgs.end() != count_iter);
        ++(count_iter->second);
        auto [insert_iter, inserted] =
          primary_mapping.pg_to_core.emplace(pgid, core_to_update);
        assert(inserted);
        DEBUG("mapping pg {} to core {} (primary): num_pgs {}",
              pgid, core_to_update, count_iter->second);
      }
      assert(core_to_update != NULL_CORE);
      return primary_mapping.container().invoke_on_others(
          [pgid, core_to_update, FNAME](auto &other_mapping) {
        auto find_iter = other_mapping.pg_to_core.find(pgid);
        if (find_iter == other_mapping.pg_to_core.end()) {
          DEBUG("mapping pg {} to core {} (others)",
                pgid, core_to_update);
          auto [insert_iter, inserted] =
            other_mapping.pg_to_core.emplace(pgid, core_to_update);
          assert(inserted);
        } else {
          auto core_found = find_iter->second;
          if (core_found != core_to_update) {
            ERROR("the mapping is inconsistent for pg {} (others): core {}, expected {}",
                  pgid, core_found, core_to_update);
            ceph_abort("The pg mapping is inconsistent!");
          }
          DEBUG("mapping pg {} to core {} (others): already mapped",
                pgid, core_to_update);
        }
      });
    }).then([this, pgid, core_expected, FNAME] {
      auto find_iter = pg_to_core.find(pgid);
      if (find_iter == pg_to_core.end()) {
        ERROR("the mapping is inconsistent for pg {}: core not found, expected {}",
              pgid, core_expected);
        ceph_abort("The pg mapping is inconsistent!");
      }
      auto core_found = find_iter->second;
      if (core_expected != NULL_CORE && core_found != core_expected) {
        ERROR("the mapping is inconsistent for pg {}: core {}, expected {}",
              pgid, core_found, core_expected);
        ceph_abort("The pg mapping is inconsistent!");
      }
      DEBUG("returning pg {} mapping to core {} after broadcasted",
            pgid, core_found);
      return seastar::make_ready_future<core_id_t>(core_found);
    });
  }
}

seastar::future<> PGShardMapping::remove_pg_mapping(spg_t pgid) {
  LOG_PREFIX(PGShardMapping::remove_pg_mapping);
  auto find_iter = pg_to_core.find(pgid);
  if (find_iter == pg_to_core.end()) {
    ERROR("trying to remove non-exist mapping for pg {}", pgid);
    ceph_abort("The pg mapping is inconsistent!");
  }
  DEBUG("calling primary to remove mapping for pg {}", pgid);
  return container().invoke_on(
      0, [pgid, FNAME](auto &primary_mapping) {
    auto find_iter = primary_mapping.pg_to_core.find(pgid);
    if (find_iter == primary_mapping.pg_to_core.end()) {
      ERROR("trying to remove non-exist mapping for pg {} (primary)", pgid);
      ceph_abort("The pg mapping is inconsistent!");
    }
    assert(find_iter->second != NULL_CORE);
    auto count_iter = primary_mapping.core_to_num_pgs.find(find_iter->second);
    assert(count_iter != primary_mapping.core_to_num_pgs.end());
    assert(count_iter->second > 0);
    --(count_iter->second);
    primary_mapping.pg_to_core.erase(find_iter);
    DEBUG("pg {} mapping erased (primary)", pgid);
    return primary_mapping.container().invoke_on_others(
      [pgid, FNAME](auto &other_mapping) {
      auto find_iter = other_mapping.pg_to_core.find(pgid);
      if (find_iter == other_mapping.pg_to_core.end()) {
        ERROR("trying to remove non-exist mapping for pg {} (others)", pgid);
        ceph_abort("The pg mapping is inconsistent!");
      }
      assert(find_iter->second != NULL_CORE);
      other_mapping.pg_to_core.erase(find_iter);
      DEBUG("pg {} mapping erased (others)", pgid);
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
