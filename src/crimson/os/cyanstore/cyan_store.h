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
#include <seastar/core/future-util.hh>

#include "osd/osd_types.h"
#include "include/uuid.h"

#include "crimson/os/cyanstore/cyan_object.h"
#include "crimson/os/futurized_store.h"

namespace ceph::os {
class Transaction;
}

namespace crimson::os {
class Collection;

class CyanStore final : public FuturizedStore {
  constexpr static unsigned MAX_KEYS_PER_OMAP_GET_CALL = 32;

  const std::string path;
  std::unordered_map<coll_t, boost::intrusive_ptr<Collection>> coll_map;
  std::map<coll_t, boost::intrusive_ptr<Collection>> new_coll_map;
  uint64_t used_bytes = 0;
  uuid_d osd_fsid;

public:
  class CyanOmapIterator final : public OmapIterator {
  public:
    CyanOmapIterator() {}
    CyanOmapIterator(ObjectRef obj) : obj(obj) {
      iter = obj->omap.begin();
    }
    virtual seastar::future<int> seek_to_first();
    virtual seastar::future<int> upper_bound(const std::string &after);
    virtual seastar::future<int> lower_bound(const std::string &to);
    virtual bool valid() const;
    virtual seastar::future<int> next();
    virtual std::string key() {
      return iter->first;
    }
    virtual seastar::future<std::string> tail_key() {
      return seastar::make_ready_future<std::string>((++obj->omap.end())->first);
    }
    virtual ceph::buffer::list value() {
      return iter->second;
    }
    virtual int status() const {
      return iter != obj->omap.end() ? 0 : -1;
    }
    virtual ~CyanOmapIterator() {}
  private:
    std::map<std::string, bufferlist>::const_iterator iter;
    ObjectRef obj;
  };

  CyanStore(const std::string& path);
  ~CyanStore() final;

  seastar::future<> stop() final {
    return seastar::now();
  }
  seastar::future<> mount() final;
  seastar::future<> umount() final;

  seastar::future<> mkfs(uuid_d new_osd_fsid) final;
  seastar::future<store_statfs_t> stat() const final;
  seastar::future<struct stat> stat(
    CollectionRef c,
    const ghobject_t& oid) final;

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
    const ghobject_t& oid);

  seastar::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    const omap_keys_t& keys) final;

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

  seastar::future<ceph::bufferlist> omap_get_header(
    CollectionRef c,
    const ghobject_t& oid) final;

  seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final;
  seastar::future<CollectionRef> open_collection(const coll_t& cid) final;
  seastar::future<std::vector<coll_t>> list_collections() final;

  seastar::future<> do_transaction(CollectionRef ch,
				   ceph::os::Transaction&& txn) final;

  seastar::future<> write_meta(const std::string& key,
		  const std::string& value) final;
  seastar::future<std::tuple<int, std::string>>
  read_meta(const std::string& key) final;
  uuid_d get_fsid() const final;
  unsigned get_max_attr_name_length() const final;

  seastar::future<OmapIteratorRef> get_omap_iterator(
    CollectionRef c,
    const ghobject_t& oid);

  seastar::future<std::map<uint64_t, uint64_t>> fiemap(CollectionRef c,
						       const ghobject_t& oid,
						       uint64_t off,
						       uint64_t len);

private:
  int _remove(const coll_t& cid, const ghobject_t& oid);
  int _touch(const coll_t& cid, const ghobject_t& oid);
  int _write(const coll_t& cid, const ghobject_t& oid,
	     uint64_t offset, size_t len, const ceph::bufferlist& bl,
	     uint32_t fadvise_flags);
  int _omap_set_values(
    const coll_t& cid,
    const ghobject_t& oid,
    std::map<std::string, ceph::bufferlist> &&aset);
  int _omap_set_header(
    const coll_t& cid,
    const ghobject_t& oid,
    const ceph::bufferlist &header);
  int _omap_rmkeys(
    const coll_t& cid,
    const ghobject_t& oid,
    const omap_keys_t& aset);
  int _omap_rmkeyrange(
    const coll_t& cid,
    const ghobject_t& oid,
    const std::string &first,
    const std::string &last);
  int _truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size);
  int _setattrs(const coll_t& cid, const ghobject_t& oid,
                std::map<std::string,bufferptr>& aset);
  int _create_collection(const coll_t& cid, int bits);
  boost::intrusive_ptr<Collection> _get_collection(const coll_t& cid);
};

}
