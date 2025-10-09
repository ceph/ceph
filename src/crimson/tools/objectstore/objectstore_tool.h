// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "common/hobject.h"
#include "crimson/os/alienstore/alien_store.h"
#include "crimson/os/futurized_store.h"
#include "include/expected.hpp"
#include "osd/osd_types.h"
#include "seastar/core/future.hh"

using crimson::os::FuturizedStore;

namespace crimson {
namespace tools {
namespace kvstore {

class StoreTool
{
public:
  StoreTool(std::unique_ptr<crimson::os::FuturizedStore> store): store(std::move(store)) {}

  seastar::future<> stop();
  void set_shard_id(seastar::shard_id shard_id) { this->shard_id = shard_id; }
  
  // PG operations
  seastar::future<std::vector<crimson::os::coll_core_t>> list_pgs();
  seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
    const coll_t& cid,
    ghobject_t next);
  
  // PG-level operations
  seastar::future<pg_info_t> get_pg_info(const coll_t& cid);
  
  // Omap operations
  seastar::future<>
  omap_iterate(
    const coll_t &cid,
    const ghobject_t &oid,
    const std::optional<std::string> &start,
    FuturizedStore::Shard::omap_iterate_cb_t callback);
  seastar::future<std::string> get_omap(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::string& key);
  seastar::future<bool> set_omap(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::string& key,
    const std::string& value);
  seastar::future<bool> remove_omap(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::string& key);
  
  // Object data operations
  seastar::future<std::string> get_bytes(
    const coll_t& cid,
    const ghobject_t& oid);
  seastar::future<bool> set_bytes(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::string& data);
  
  // Object attribute operations
  seastar::future<tl::expected<FuturizedStore::Shard::attrs_t, std::string>> get_attrs(
    const coll_t& cid,
    const ghobject_t& oid);
  seastar::future<tl::expected<std::string, std::string>> get_attr(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::string& key);
  seastar::future<bool> set_attr(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::string& key,
    const std::string& value);
  seastar::future<bool> remove_attr(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::string& key);

  // Object management operations
  seastar::future<bool> remove_object(
    const coll_t& cid,
    const ghobject_t& oid,
    bool all = false,
    bool force = false);
  
  // Object info operations
  seastar::future<std::string> dump_object_info(
    const coll_t& cid,
    const ghobject_t& oid);
  seastar::future<bool> set_object_size(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t size);
  seastar::future<bool> clear_data_digest(
    const coll_t& cid,
    const ghobject_t& oid);

private:
  seastar::shard_id shard_id = 0;
  std::unique_ptr<crimson::os::FuturizedStore> store;
};

} // namespace kvstore
} // namespace tools
} // namespace crimson
