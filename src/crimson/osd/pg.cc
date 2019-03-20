#include "pg.h"

#include "osd/OSDMap.h"

#include "crimson/os/cyan_store.h"
#include "crimson/osd/pg_meta.h"


PG::PG(spg_t pgid,
       pg_shard_t pg_shard,
       pg_pool_t&& pool,
       std::string&& name,
       ec_profile_t&& ec_profile,
       cached_map_t osdmap,
       ceph::net::Messenger& msgr)
  : pgid{pgid},
    whoami{pg_shard},
    pool{std::move(pool)},
    info{pgid},
    osdmap{osdmap},
    msgr{msgr}
{
  // TODO
}

seastar::future<> PG::read_state(ceph::os::CyanStore* store)
{
  return PGMeta{store, pgid}.load().then(
    [this](pg_info_t pg_info_, PastIntervals past_intervals_) {
      info = std::move(pg_info_);
      last_written_info = info;
      past_intervals = std::move(past_intervals_);
      // initialize current mapping
      {
        vector<int> new_up, new_acting;
        int new_up_primary, new_acting_primary;
        osdmap->pg_to_up_acting_osds(pgid.pgid,
                                     &new_up, &new_up_primary,
                                     &new_acting, &new_acting_primary);
        update_primary_state(new_up, new_up_primary,
                             new_acting, new_acting_primary);
      }
      return seastar::now();
    });
}

void
PG::update_primary_state(const std::vector<int>& new_up,
                         int new_up_primary,
                         const std::vector<int>& new_acting,
                         int new_acting_primary)
{
  auto collect_pg_shards =
    [is_erasure=pool.is_erasure()](const std::vector<int>& osds,
                                   int osd_primary) {
      int8_t index = 0;
      pg_shard_set_t collected;
      pg_shard_t pg_primary;
      for (auto osd : osds) {
        if (osd != CRUSH_ITEM_NONE) {
          pg_shard_t pg_shard{
            osd, is_erasure ? shard_id_t{index} : shard_id_t::NO_SHARD};
          if (osd == osd_primary) {
            pg_primary = pg_shard;
          }
          collected.insert(pg_shard);
        }
        index++;
      }
      return std::make_pair(collected, pg_primary);
    };
  acting = new_acting;
  std::tie(actingset, primary) = collect_pg_shards(acting, new_acting_primary);
  ceph_assert(primary.osd == new_acting_primary);
  up = new_up;
  std::tie(upset, up_primary) = collect_pg_shards(up, new_up_primary);
  ceph_assert(up_primary.osd == new_up_primary);
}

epoch_t PG::get_osdmap_epoch() const
{
  return osdmap->get_epoch();
}

pg_shard_t PG::get_whoami() const
{
  return whoami;
}

const pg_info_t& PG::get_info() const
{
  return info;
}

const PastIntervals& PG::get_past_intervals() const
{
  return past_intervals;
}

pg_shard_t PG::get_primary() const
{
  return primary;
}

bool PG::is_primary() const
{
  return whoami == primary;
}
