#include "pg.h"

#include "osd/OSDMap.h"


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
    osdmap{osdmap},
    msgr{msgr}
{
  // TODO
}

epoch_t PG::get_osdmap_epoch() const
{
  return osdmap->get_epoch();
}

pg_shard_t PG::get_whoami() const
{
  return whoami;
}
