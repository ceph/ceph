// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "objectstore_tool.h"
#include "common/hobject.h"
#include "crimson/os/alienstore/alien_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "seastar/core/future.hh"
#include "seastar/core/smp.hh"
#include <boost/smart_ptr/intrusive_ptr.hpp>

using crimson::os::FuturizedStore;

seastar::future<> StoreTool::stop()
{
  return store->umount().then([this] {
    return store->stop();
  });
}

seastar::future<std::vector<crimson::os::coll_core_t>> StoreTool::list_pgs()
{
  return store->list_collections();
}


seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
StoreTool::list_objects(const coll_t& cid, unsigned int shard_id, ghobject_t next)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, next] {
      return store->get_sharded_store().open_collection(cid)
        .handle_exception([this, cid](std::exception_ptr) {
          // If collection doesn't exist, create it
          return store->get_sharded_store().create_new_collection(cid);
        })
        .then([this, next] (auto coll) {
          return store->get_sharded_store().list_objects(coll, next, ghobject_t::get_max(), 100);
        });
    }
  );
}

seastar::future<FuturizedStore::Shard::omap_values_t>
StoreTool::omap_get_values(
      const coll_t &cid,
      unsigned int shard_id,
      const ghobject_t &oid,
      const std::optional<std::string> &start)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, start] {
      return store->get_sharded_store().open_collection(cid)
        .handle_exception([](std::exception_ptr) {
          return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
        })
        .then([this, oid, start] (auto coll) {
          if (!coll) {
            fmt::print(std::cerr, "Failed to open collection: collection does not exist\n");
            return seastar::make_ready_future<FuturizedStore::Shard::omap_values_t>(FuturizedStore::Shard::omap_values_t());
          }
          return store->get_sharded_store().omap_get_values(coll, oid, start)
            .safe_then_unpack([](bool success, FuturizedStore::Shard::omap_values_t vals) {
                if (!success) {
                    fmt::print(std::cerr, "omap_get_values failed");
                    return FuturizedStore::Shard::omap_values_t();
                }
                return vals;
            }, FuturizedStore::Shard::read_errorator::all_same_way([] {
                return FuturizedStore::Shard::omap_values_t();
            }));
        });
    }
  );
}

seastar::future<std::string> StoreTool::get_omap(
  const coll_t& cid,
  unsigned int shard_id,
  const ghobject_t& oid,
  const std::string& key)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, key] {
      return store->get_sharded_store().open_collection(cid)
        .handle_exception([](std::exception_ptr) {
          return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
        })
        .then([this, oid, key] (auto coll) {
          if (!coll) {
            fmt::print(std::cerr, "Failed to open collection: collection does not exist\n");
            return seastar::make_ready_future<std::string>(std::string());
          }
          return store->get_sharded_store().omap_get_values(coll, oid, std::nullopt)
            .safe_then_unpack([key](bool success, FuturizedStore::Shard::omap_values_t vals) {
                if (!success) {
                    fmt::print(std::cerr, "omap_get_values failed\n");
                    return std::string();
                }
                auto it = vals.find(key);
                if (it != vals.end()) {
                    return it->second.to_str();
                }
                return std::string();
            }, FuturizedStore::Shard::read_errorator::all_same_way([] {
                return std::string();
            }));
        });
    }
  );
}

seastar::future<bool> StoreTool::set_omap(
  const coll_t& cid,
  unsigned int shard_id,
  const ghobject_t& oid,
  const std::string& key,
  const std::string& value)
{
  // Create copies to ensure safe capture
  coll_t cid_copy = cid;
  ghobject_t oid_copy = oid;
  std::string key_copy = key;
  std::string value_copy = value;
  
  return seastar::smp::submit_to(
    shard_id,
    [this, cid_copy = std::move(cid_copy), oid_copy = std::move(oid_copy), 
     key_copy = std::move(key_copy), value_copy = std::move(value_copy)] () mutable {
      return store->get_sharded_store().open_collection(cid_copy)
        .handle_exception([](std::exception_ptr) {
          return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
        })
        .then([this, cid_copy = std::move(cid_copy), oid_copy = std::move(oid_copy), 
               key_copy = std::move(key_copy), value_copy = std::move(value_copy)] (auto coll) mutable {
          if (!coll) {
            fmt::print(std::cerr, "Failed to open collection: collection does not exist\n");
            return seastar::make_ready_future<bool>(false);
          }
          ceph::os::Transaction txn;
          std::map<std::string, ceph::bufferlist> omap_values;
          ceph::bufferlist bl;
          bl.append(value_copy.c_str(), value_copy.length());
          omap_values[key_copy] = std::move(bl);
          txn.omap_setkeys(cid_copy, oid_copy, omap_values);
          return store->get_sharded_store().do_transaction(coll, std::move(txn))
            .then([] {
                return true;
            }).handle_exception([] (std::exception_ptr) {
                return false;
            });
        });
    }
  );
}

seastar::future<bool> StoreTool::remove_omap(
  const coll_t& cid,
  unsigned int shard_id,
  const ghobject_t& oid,
  const std::string& key)
{
  coll_t cid_copy = cid;
  ghobject_t oid_copy = oid;
  std::string key_copy = key;
  return seastar::smp::submit_to(
    shard_id,
    [this, cid_copy = std::move(cid_copy), oid_copy = std::move(oid_copy), key_copy = std::move(key_copy)] () mutable {
      return store->get_sharded_store().open_collection(cid_copy)
        .handle_exception([](std::exception_ptr) {
          return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
        })
        .then([this, cid_copy = std::move(cid_copy), oid_copy = std::move(oid_copy), key_copy = std::move(key_copy)] (auto coll) mutable {
          if (!coll) {
            fmt::print(std::cerr, "Failed to open collection: collection does not exist\n");
            return seastar::make_ready_future<bool>(false);
          }
          ceph::os::Transaction txn;
          txn.omap_rmkey(cid_copy, oid_copy, key_copy);
          return store->get_sharded_store().do_transaction(coll, std::move(txn))
            .then([] {
                return true;
            }).handle_exception([] (std::exception_ptr) {
                return false;
            });
        });
    }
  );
}