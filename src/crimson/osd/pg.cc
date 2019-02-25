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
       ceph::net::Messenger* msgr)
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
      last_written_info = std::move(pg_info_);
      past_intervals = std::move(past_intervals_);
      return seastar::now();
    });
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
