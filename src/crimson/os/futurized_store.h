// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <map>
#include <optional>
#include <vector>

#include <seastar/core/future.hh>

#include "os/Transaction.h"
#include "crimson/common/smp_helpers.h"
#include "crimson/common/smp_helpers.h"
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
  class Shard {
  public:
    Shard() = default;
    virtual ~Shard() = default;
    // no copying
    explicit Shard(const Shard& o) = delete;
    const Shard& operator=(const Shard& o) = delete;

    using CollectionRef = boost::intrusive_ptr<FuturizedCollection>;
    using base_errorator = crimson::errorator<crimson::ct_error::input_output_error>;
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

    virtual base_errorator::future<bool> exists(
      CollectionRef c,
      const ghobject_t& oid) = 0;

    using get_attr_errorator = crimson::errorator<
      crimson::ct_error::enoent,
      crimson::ct_error::enodata>;
    virtual get_attr_errorator::future<ceph::bufferlist> get_attr(
      CollectionRef c,
      const ghobject_t& oid,
      std::string_view name) const = 0;

    using get_attrs_ertr = crimson::errorator<
      crimson::ct_error::enoent>;
    using attrs_t = std::map<std::string, ceph::bufferlist, std::less<>>;
    virtual get_attrs_ertr::future<attrs_t> get_attrs(
      CollectionRef c,
      const ghobject_t& oid) = 0;

    virtual seastar::future<struct stat> stat(
      CollectionRef c,
      const ghobject_t& oid) = 0;

    using omap_values_t = std::map<std::string, ceph::bufferlist, std::less<>>;
    using omap_keys_t = std::set<std::string>;
    virtual read_errorator::future<omap_values_t> omap_get_values(
      CollectionRef c,
      const ghobject_t& oid,
      const omap_keys_t& keys) = 0;

    virtual read_errorator::future<std::tuple<bool, omap_values_t>> omap_get_values(
      CollectionRef c,           ///< [in] collection
      const ghobject_t &oid,     ///< [in] oid
      const std::optional<std::string> &start ///< [in] start, empty for begin
      ) = 0; ///< @return <done, values> values.empty() only if done

    virtual get_attr_errorator::future<bufferlist> omap_get_header(
      CollectionRef c,
      const ghobject_t& oid) = 0;

    virtual seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
      CollectionRef c,
      const ghobject_t& start,
      const ghobject_t& end,
      uint64_t limit) const = 0;

    virtual seastar::future<CollectionRef> create_new_collection(const coll_t& cid) = 0;

    virtual seastar::future<CollectionRef> open_collection(const coll_t& cid) = 0;

  protected:
    virtual seastar::future<> do_transaction_no_callbacks(
      CollectionRef ch,
      ceph::os::Transaction&& txn) = 0;

  public:
    seastar::future<> do_transaction(
      CollectionRef ch,
      ceph::os::Transaction&& txn) {
      std::unique_ptr<Context> on_commit(
	ceph::os::Transaction::collect_all_contexts(txn));
      return do_transaction_no_callbacks(
	std::move(ch), std::move(txn)
      ).then([on_commit=std::move(on_commit)]() mutable {
	auto c = on_commit.release();
	if (c) c->complete(0);
	return seastar::now();
      });
    }


    /**
     * flush
     *
     * Flushes outstanding transactions on ch, returned future resolves
     * after any previously submitted transactions on ch have committed.
     *
     * @param ch [in] collection on which to flush
     */
    virtual seastar::future<> flush(CollectionRef ch) {
      return do_transaction(ch, ceph::os::Transaction{});
    }

    // error injection
    virtual seastar::future<> inject_data_error(const ghobject_t& o) {
      return seastar::now();
    }

    virtual seastar::future<> inject_mdata_error(const ghobject_t& o) {
      return seastar::now();
    }

    virtual read_errorator::future<std::map<uint64_t, uint64_t>> fiemap(
      CollectionRef ch,
      const ghobject_t& oid,
      uint64_t off,
      uint64_t len) = 0;

    virtual unsigned get_max_attr_name_length() const = 0;
  };

public:
  static std::unique_ptr<FuturizedStore> create(const std::string& type,
                                                const std::string& data,
                                                const ConfigValues& values);
  FuturizedStore()
  : primary_core(seastar::this_shard_id())
  {}

  virtual ~FuturizedStore() = default;

  // no copying
  explicit FuturizedStore(const FuturizedStore& o) = delete;
  const FuturizedStore& operator=(const FuturizedStore& o) = delete;

  virtual seastar::future<> start() = 0;

  virtual seastar::future<> stop() = 0;

  using mount_ertr = crimson::errorator<crimson::stateful_ec>;
  virtual mount_ertr::future<> mount() = 0;

  virtual seastar::future<> umount() = 0;

  using mkfs_ertr = crimson::errorator<crimson::stateful_ec>;
  virtual mkfs_ertr::future<> mkfs(uuid_d new_osd_fsid) = 0;

  virtual seastar::future<store_statfs_t> stat() const = 0;

  virtual uuid_d get_fsid() const  = 0;

  virtual seastar::future<> write_meta(const std::string& key,
				       const std::string& value) = 0;
  // called on the shard and get this FuturizedStore::shard;
  virtual Shard& get_sharded_store() = 0;

  virtual seastar::future<std::tuple<int, std::string>> read_meta(
    const std::string& key) = 0;

  using coll_core_t = std::pair<coll_t, core_id_t>;
  virtual seastar::future<std::vector<coll_core_t>> list_collections() = 0;

protected:
  const core_id_t primary_core;
};
}
