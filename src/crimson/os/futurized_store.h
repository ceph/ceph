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
  class OmapIterator {
  public:
    virtual seastar::future<int> seek_to_first() {
      return seastar::make_ready_future<int>(0);
    }
    virtual seastar::future<int> upper_bound(const std::string &after) {
      return seastar::make_ready_future<int>(0);
    }
    virtual seastar::future<int> lower_bound(const std::string &to) {
      return seastar::make_ready_future<int>(0);
    }
    virtual bool valid() const {
      return false;
    }
    virtual seastar::future<int> next() {
      return seastar::make_ready_future<int>(0);
    }
    virtual std::string key() {
      return {};
    }
    virtual seastar::future<std::string> tail_key() {
      return seastar::make_ready_future<std::string>();
    }
    virtual ceph::buffer::list value() {
      return {};
    }
    virtual int status() const {
      return 0;
    }
    virtual ~OmapIterator() {}
  private:
    unsigned count = 0;
    friend void intrusive_ptr_add_ref(FuturizedStore::OmapIterator* iter);
    friend void intrusive_ptr_release(FuturizedStore::OmapIterator* iter);
  };
  using OmapIteratorRef = boost::intrusive_ptr<OmapIterator>;

  static std::unique_ptr<FuturizedStore> create(const std::string& type,
                                                const std::string& data,
                                                const ConfigValues& values);
  FuturizedStore() = default;
  virtual ~FuturizedStore() = default;

  // no copying
  explicit FuturizedStore(const FuturizedStore& o) = delete;
  const FuturizedStore& operator=(const FuturizedStore& o) = delete;

  virtual seastar::future<> start() {
    return seastar::now();
  }
  virtual seastar::future<> stop() = 0;
  virtual seastar::future<> mount() = 0;
  virtual seastar::future<> umount() = 0;

  virtual seastar::future<> mkfs(uuid_d new_osd_fsid) = 0;
  virtual seastar::future<store_statfs_t> stat() const = 0;

  using CollectionRef = boost::intrusive_ptr<FuturizedCollection>;
  using read_errorator = crimson::errorator<crimson::ct_error::enoent,
                                            crimson::ct_error::input_output_error>;
  virtual read_errorator::future<ceph::bufferlist> read(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    uint32_t op_flags = 0) = 0;
  virtual read_errorator::future<ceph::bufferlist> readv(
    CollectionRef c,
    const ghobject_t& oid,
    interval_set<uint64_t>& m,
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
  virtual seastar::future<struct stat> stat(
    CollectionRef c,
    const ghobject_t& oid) = 0;

  using omap_values_t = std::map<std::string, bufferlist, std::less<>>;
  using omap_keys_t = std::set<std::string>;
  virtual seastar::future<omap_values_t> omap_get_values(
                                         CollectionRef c,
                                         const ghobject_t& oid,
                                         const omap_keys_t& keys) = 0;
  virtual seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
    CollectionRef c,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit) const = 0;
  virtual seastar::future<std::tuple<bool, omap_values_t>> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) = 0; ///< @return <done, values> values.empty() iff done

  virtual seastar::future<bufferlist> omap_get_header(
    CollectionRef c,
    const ghobject_t& oid) = 0;

  virtual seastar::future<CollectionRef> create_new_collection(const coll_t& cid) = 0;
  virtual seastar::future<CollectionRef> open_collection(const coll_t& cid) = 0;
  virtual seastar::future<std::vector<coll_t>> list_collections() = 0;

  virtual seastar::future<> do_transaction(CollectionRef ch,
					   ceph::os::Transaction&& txn) = 0;
  virtual seastar::future<OmapIteratorRef> get_omap_iterator(
    CollectionRef ch,
    const ghobject_t& oid) = 0;
  virtual seastar::future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef ch,
    const ghobject_t& oid,
    uint64_t off,
    uint64_t len) = 0;

  virtual seastar::future<> write_meta(const std::string& key,
				       const std::string& value) = 0;
  virtual seastar::future<std::tuple<int, std::string>> read_meta(
    const std::string& key) = 0;
  virtual uuid_d get_fsid() const  = 0;
  virtual unsigned get_max_attr_name_length() const = 0;
};

inline void intrusive_ptr_add_ref(FuturizedStore::OmapIterator* iter) {
  assert(iter);
  iter->count++;
}

inline void intrusive_ptr_release(FuturizedStore::OmapIterator* iter) {
  assert(iter);
  assert(iter->count > 0);
  if ((--iter->count) == 0) {
    delete iter;
  }
}

}
