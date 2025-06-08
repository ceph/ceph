// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/hobject.h"
#include "crimson/os/alienstore/alien_store.h"
#include "crimson/os/futurized_store.h"
#include "osd/osd_types.h"
#include "seastar/core/future.hh"
#include <optional>
#include <string>
#include <tuple>
#include <vector>

using crimson::os::FuturizedStore;

class StoreTool
{
public:
  StoreTool(std::unique_ptr<crimson::os::FuturizedStore> store): store(std::move(store)) {}

  seastar::future<> stop();
  seastar::future<std::vector<crimson::os::coll_core_t>> list_pgs();
  seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
    const coll_t& cid,
    unsigned int shard_id,
    ghobject_t next);
  seastar::future<FuturizedStore::Shard::omap_values_t>
  omap_get_values(
    const coll_t &cid,
    unsigned int shard_id,
    const ghobject_t &oid,
    const std::optional<std::string> &start);
  seastar::future<std::string> get_omap(
    const coll_t& cid,
    unsigned int shard_id,
    const ghobject_t& oid,
    const std::string& key);
  seastar::future<bool> set_omap(
    const coll_t& cid,
    unsigned int shard_id,
    const ghobject_t& oid,
    const std::string& key,
    const std::string& value);
  seastar::future<bool> remove_omap(
    const coll_t& cid,
    unsigned int shard_id,
    const ghobject_t& oid,
    const std::string& key);
private:
  std::unique_ptr<crimson::os::FuturizedStore> store;
};