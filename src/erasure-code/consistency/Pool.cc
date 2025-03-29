#include "Pool.h"

using Pool = ceph::consistency::Pool;

Pool::Pool(const std::string& pool_name, const ceph::ErasureCodeProfile& profile) :
  pool_name(pool_name),
  profile(profile)
{
}

ceph::ErasureCodeProfile Pool::get_ec_profile() { return profile; }
std::string Pool::get_pool_name() { return pool_name; }
