// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <unordered_map>
#include <map>
#include <typeinfo>
#include <vector>

#include <optional>
#include <seastar/core/future.hh>

#include "osd/osd_types.h"
#include "include/uuid.h"

#include "os/Transaction.h"
#include "crimson/os/futurized_store.h"

namespace crimson::os::seastore {

class SeastoreCollection;
class SegmentManager;
class OnodeManager;
class Onode;
using OnodeRef = boost::intrusive_ptr<Onode>;
class Journal;
class LBAManager;
class TransactionManager;
class Transaction;
using TransactionRef = std::unique_ptr<Transaction>;
class Cache;

class SeaStore final : public FuturizedStore {
  uuid_d osd_fsid;

public:

  SeaStore(const std::string& path);
  ~SeaStore() final;

  seastar::future<> stop() final;
  seastar::future<> mount() final;
  seastar::future<> umount() final;

  seastar::future<> mkfs(uuid_d new_osd_fsid) final;
  seastar::future<store_statfs_t> stat() const final;

  read_errorator::future<ceph::bufferlist> read(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    uint32_t op_flags = 0) final;
  read_errorator::future<ceph::bufferlist> readv(
    CollectionRef c,
    const ghobject_t& oid,
    interval_set<uint64_t>& m,
    uint32_t op_flags = 0) final;
  get_attr_errorator::future<ceph::bufferptr> get_attr(
    CollectionRef c,
    const ghobject_t& oid,
    std::string_view name) const final;
  get_attrs_ertr::future<attrs_t> get_attrs(
    CollectionRef c,
    const ghobject_t& oid) final;

  seastar::future<struct stat> stat(
    CollectionRef c,
    const ghobject_t& oid) final;

  seastar::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    const omap_keys_t& keys) final;

  seastar::future<bufferlist> omap_get_header(
    CollectionRef c,
    const ghobject_t& oid) final;

  seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
    CollectionRef c,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit) const final;

  /// Retrieves paged set of values > start (if present)
  seastar::future<std::tuple<bool, omap_values_t>> omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) final; ///< @return <done, values> values.empty() iff done

  seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final;
  seastar::future<CollectionRef> open_collection(const coll_t& cid) final;
  seastar::future<std::vector<coll_t>> list_collections() final;

  seastar::future<> do_transaction(
    CollectionRef ch,
    ceph::os::Transaction&& txn) final;

  seastar::future<OmapIteratorRef> get_omap_iterator(
    CollectionRef ch,
    const ghobject_t& oid) final;
  seastar::future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef ch,
    const ghobject_t& oid,
    uint64_t off,
    uint64_t len) final;

  seastar::future<> write_meta(const std::string& key,
		  const std::string& value) final;
  seastar::future<std::tuple<int, std::string>> read_meta(const std::string& key) final;
  uuid_d get_fsid() const final;

  unsigned get_max_attr_name_length() const final {
    return 256;
  }

private:
  std::unique_ptr<SegmentManager> segment_manager;
  std::unique_ptr<Cache> cache;
  std::unique_ptr<Journal> journal;
  std::unique_ptr<LBAManager> lba_manager;
  std::unique_ptr<TransactionManager> transaction_manager;
  std::unique_ptr<OnodeManager> onode_manager;


  using write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  write_ertr::future<> _do_transaction_step(
    TransactionRef &trans,
    CollectionRef &col,
    std::vector<OnodeRef> &onodes,
    ceph::os::Transaction::iterator &i);

  write_ertr::future<> _remove(
    TransactionRef &trans,
    OnodeRef &onode);
  write_ertr::future<> _touch(
    TransactionRef &trans,
    OnodeRef &onode);
  write_ertr::future<> _write(
    TransactionRef &trans,
    OnodeRef &onode,
    uint64_t offset, size_t len, const ceph::bufferlist& bl,
    uint32_t fadvise_flags);
  write_ertr::future<> _omap_set_values(
    TransactionRef &trans,
    OnodeRef &onode,
    std::map<std::string, ceph::bufferlist> &&aset);
  write_ertr::future<> _omap_set_header(
    TransactionRef &trans,
    OnodeRef &onode,
    const ceph::bufferlist &header);
  write_ertr::future<> _omap_rmkeys(
    TransactionRef &trans,
    OnodeRef &onode,
    const omap_keys_t& aset);
  write_ertr::future<> _omap_rmkeyrange(
    TransactionRef &trans,
    OnodeRef &onode,
    const std::string &first,
    const std::string &last);
  write_ertr::future<> _truncate(
    TransactionRef &trans,
    OnodeRef &onode, uint64_t size);
  write_ertr::future<> _setattrs(
    TransactionRef &trans,
    OnodeRef &onode,
    std::map<std::string,bufferptr>& aset);
  write_ertr::future<> _create_collection(
    TransactionRef &trans,
    const coll_t& cid, int bits);

  boost::intrusive_ptr<SeastoreCollection> _get_collection(const coll_t& cid);
};

}
