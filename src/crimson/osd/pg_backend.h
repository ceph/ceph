// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <memory>
#include <string>
#include <boost/smart_ptr/local_shared_ptr.hpp>

#include "crimson/common/shared_lru.h"
#include "osd/osd_types.h"

struct hobject_t;
namespace ceph::os {
  class Collection;
  class CyanStore;
}

class PGBackend
{
protected:
  using CollectionRef = boost::intrusive_ptr<ceph::os::Collection>;
  using ec_profile_t = std::map<std::string, std::string>;

public:
  PGBackend(shard_id_t shard, CollectionRef coll, ceph::os::CyanStore* store);
  virtual ~PGBackend() = default;
  static std::unique_ptr<PGBackend> create(const spg_t pgid,
					   const pg_pool_t& pool,
					   ceph::os::CyanStore* store,
					   const ec_profile_t& ec_profile);
  using cached_oi_t = boost::local_shared_ptr<object_info_t>;
  seastar::future<cached_oi_t> get_object(const hobject_t& oid);
  seastar::future<bufferlist> read(const object_info_t& oi,
				   uint64_t off,
				   uint64_t len,
				   size_t truncate_size,
				   uint32_t truncate_seq,
				   uint32_t flags);
protected:
  const shard_id_t shard;
  CollectionRef coll;
  ceph::os::CyanStore* store;

private:
  using cached_ss_t = boost::local_shared_ptr<SnapSet>;
  SharedLRU<hobject_t, SnapSet> ss_cache;
  seastar::future<cached_ss_t> _load_ss(const hobject_t& oid);
  SharedLRU<hobject_t, object_info_t> oi_cache;
  seastar::future<cached_oi_t> _load_oi(const hobject_t& oid);
  virtual seastar::future<bufferlist> _read(const hobject_t& hoid,
					    size_t offset,
					    size_t length,
					    uint32_t flags) = 0;
};
