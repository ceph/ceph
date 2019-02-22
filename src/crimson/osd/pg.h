// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <seastar/core/future.hh>

#include "osd/osd_types.h"

template<typename T> using Ref = boost::intrusive_ptr<T>;
class OSDMap;

namespace ceph::net {
  class Messenger;
}

class PG : public boost::intrusive_ref_counter<
  PG,
  boost::thread_unsafe_counter>
{
  using ec_profile_t = std::map<std::string,std::string>;
  using cached_map_t = boost::local_shared_ptr<OSDMap>;

public:
  PG(spg_t pgid,
     pg_shard_t pg_shard,
     pg_pool_t&& pool,
     std::string&& name,
     ec_profile_t&& ec_profile,
     cached_map_t osdmap,
     ceph::net::Messenger* msgr);

  epoch_t get_osdmap_epoch() const;
  pg_shard_t get_whoami() const;

private:
  const spg_t pgid;
  pg_shard_t whoami;
  pg_pool_t pool;
  cached_map_t osdmap;
  ceph::net::Messenger* msgr = nullptr;
};
