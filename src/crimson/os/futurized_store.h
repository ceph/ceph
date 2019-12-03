// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <unordered_map>
#include <map>
#include <typeinfo>
#include <vector>

#include <seastar/core/future.hh>

#include "crimson/osd/exceptions.h"
#include "include/buffer_fwd.h"
#include "include/uuid.h"
#include "osd/osd_types.h"

namespace ceph::os {
class Transaction;
}

namespace crimson::os {
class FuturizedCollection;

class FuturizedStore {

public:
  static std::unique_ptr<FuturizedStore> create(const std::string& type,
                                                const std::string& data);
  FuturizedStore() = default;
  virtual ~FuturizedStore() = default;

  // no copying
  explicit FuturizedStore(const FuturizedStore& o) = delete;
  const FuturizedStore& operator=(const FuturizedStore& o) = delete;

  virtual seastar::future<> mount() = 0;
  virtual seastar::future<> umount() = 0;

  virtual seastar::future<> mkfs(uuid_d new_osd_fsid) = 0;
  virtual store_statfs_t stat() const = 0;

  using CollectionRef = boost::intrusive_ptr<FuturizedCollection>;
  using read_errorator = crimson::errorator<crimson::ct_error::enoent>;
  virtual read_errorator::future<ceph::bufferlist> read(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    uint32_t op_flags = 0) = 0;

  using get_attr_errorator = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::enodata>;
  virtual get_attr_errorator::future<ceph::bufferptr> get_attr(
    CollectionRef c,
    const ghobject_t& oid,
    std::string_view name) const = 0;

  using get_attrs_ertr = crimson::errorator<
    crimson::ct_error::enoent>;
  using attrs_t = std::map<std::string, ceph::bufferptr, std::less<>>;
  virtual get_attrs_ertr::future<attrs_t> get_attrs(
    CollectionRef c,
    const ghobject_t& oid) = 0;

  using omap_values_t = std::map<std::string, bufferlist, std::less<>>;
  using omap_keys_t = std::set<std::string>;
  virtual seastar::future<omap_values_t> omap_get_values(
                                         CollectionRef c,
                                         const ghobject_t& oid,
                                         const omap_keys_t& keys) = 0;
  virtual seastar::future<std::vector<ghobject_t>, ghobject_t> list_objects(
                                         CollectionRef c,
                                         const ghobject_t& start,
                                         const ghobject_t& end,
                                         uint64_t limit) const = 0;
  virtual seastar::future<bool, omap_values_t> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) = 0; ///< @return <done, values> values.empty() iff done

  virtual seastar::future<CollectionRef> create_new_collection(const coll_t& cid) = 0;
  virtual seastar::future<CollectionRef> open_collection(const coll_t& cid) = 0;
  virtual seastar::future<std::vector<coll_t>> list_collections() = 0;

  virtual seastar::future<> do_transaction(CollectionRef ch,
					   ceph::os::Transaction&& txn) = 0;

  virtual seastar::future<> write_meta(const std::string& key,
				       const std::string& value) = 0;
  virtual seastar::future<int, std::string> read_meta(const std::string& key) = 0;
  virtual uuid_d get_fsid() const  = 0;
  virtual unsigned get_max_attr_name_length() const = 0;
};

}
