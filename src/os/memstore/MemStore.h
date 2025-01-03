// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013- Sage Weil <sage@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_MEMSTORE_H
#define CEPH_MEMSTORE_H

#include <atomic>
#include <mutex>
#include <boost/intrusive_ptr.hpp>

#include "include/unordered_map.h"
#include "common/Finisher.h"
#include "common/RefCountedObj.h"
#include "os/ObjectStore.h"
#include "PageSet.h"
#include "include/ceph_assert.h"

class MemStore : public ObjectStore {
public:
  struct Object : public RefCountedObject {
    ceph::mutex xattr_mutex{ceph::make_mutex("MemStore::Object::xattr_mutex")};
    ceph::mutex omap_mutex{ceph::make_mutex("MemStore::Object::omap_mutex")};
    std::map<std::string,ceph::buffer::ptr,std::less<>> xattr;
    ceph::buffer::list omap_header;
    std::map<std::string,ceph::buffer::list> omap;

    using Ref = ceph::ref_t<Object>;

    // interface for object data
    virtual size_t get_size() const = 0;
    virtual int read(uint64_t offset, uint64_t len, ceph::buffer::list &bl) = 0;
    virtual int write(uint64_t offset, const ceph::buffer::list &bl) = 0;
    virtual int clone(Object *src, uint64_t srcoff, uint64_t len,
                      uint64_t dstoff) = 0;
    virtual int truncate(uint64_t offset) = 0;
    virtual void encode(ceph::buffer::list& bl) const = 0;
    virtual void decode(ceph::buffer::list::const_iterator& p) = 0;

    void encode_base(ceph::buffer::list& bl) const {
      using ceph::encode;
      encode(xattr, bl);
      encode(omap_header, bl);
      encode(omap, bl);
    }
    void decode_base(ceph::buffer::list::const_iterator& p) {
      using ceph::decode;
      decode(xattr, p);
      decode(omap_header, p);
      decode(omap, p);
    }

    void dump(ceph::Formatter *f) const {
      f->dump_int("data_len", get_size());
      f->dump_int("omap_header_len", omap_header.length());

      f->open_array_section("xattrs");
      for (auto p = xattr.begin(); p != xattr.end(); ++p) {
	f->open_object_section("xattr");
	f->dump_string("name", p->first);
	f->dump_int("length", p->second.length());
	f->close_section();
      }
      f->close_section();

      f->open_array_section("omap");
      for (auto p = omap.begin(); p != omap.end(); ++p) {
	f->open_object_section("pair");
	f->dump_string("key", p->first);
	f->dump_int("length", p->second.length());
	f->close_section();
      }
      f->close_section();
    }
  protected:
    Object() = default;
  };
  using ObjectRef = Object::Ref;

  struct PageSetObject;
  struct Collection : public CollectionImpl {
    int bits = 0;
    CephContext *cct;
    bool use_page_set;
    ceph::unordered_map<ghobject_t, ObjectRef> object_hash;  ///< for lookup
    std::map<ghobject_t, ObjectRef> object_map;        ///< for iteration
    std::map<std::string,ceph::buffer::ptr> xattr;
    /// for object_{map,hash}
    ceph::shared_mutex lock{
      ceph::make_shared_mutex("MemStore::Collection::lock", true, false)};

    bool exists = true;
    ceph::mutex sequencer_mutex{
      ceph::make_mutex("MemStore::Collection::sequencer_mutex")};

    typedef boost::intrusive_ptr<Collection> Ref;

    ObjectRef create_object() const;

    // NOTE: The lock only needs to protect the object_map/hash, not the
    // contents of individual objects.  The osd is already sequencing
    // reads and writes, so we will never see them concurrently at this
    // level.

    ObjectRef get_object(ghobject_t oid) {
      std::shared_lock l{lock};
      auto o = object_hash.find(oid);
      if (o == object_hash.end())
	return ObjectRef();
      return o->second;
    }

    ObjectRef get_or_create_object(ghobject_t oid) {
      std::lock_guard l{lock};
      auto result = object_hash.emplace(oid, ObjectRef());
      if (result.second)
        object_map[oid] = result.first->second = create_object();
      return result.first->second;
    }

    void encode(ceph::buffer::list& bl) const {
      ENCODE_START(1, 1, bl);
      encode(xattr, bl);
      encode(use_page_set, bl);
      uint32_t s = object_map.size();
      encode(s, bl);
      for (auto p = object_map.begin(); p != object_map.end(); ++p) {
	encode(p->first, bl);
	p->second->encode(bl);
      }
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator& p) {
      DECODE_START(1, p);
      decode(xattr, p);
      decode(use_page_set, p);
      uint32_t s;
      decode(s, p);
      while (s--) {
	ghobject_t k;
	decode(k, p);
	auto o = create_object();
	o->decode(p);
	object_map.insert(std::make_pair(k, o));
	object_hash.insert(std::make_pair(k, o));
      }
      DECODE_FINISH(p);
    }

    uint64_t used_bytes() const {
      uint64_t result = 0;
      for (auto p = object_map.begin(); p != object_map.end(); ++p) {
        result += p->second->get_size();
      }

      return result;
    }

    void flush() override {
    }
    bool flush_commit(Context *c) override {
      return true;
    }

  private:
    FRIEND_MAKE_REF(Collection);
    explicit Collection(CephContext *cct, coll_t c)
      : CollectionImpl(cct, c),
	cct(cct),
	use_page_set(cct->_conf->memstore_page_set) {}
  };
  typedef Collection::Ref CollectionRef;

private:
  class OmapIteratorImpl;


  ceph::unordered_map<coll_t, CollectionRef> coll_map;
  /// rwlock to protect coll_map
  ceph::shared_mutex coll_lock{
    ceph::make_shared_mutex("MemStore::coll_lock")};
  std::map<coll_t,CollectionRef> new_coll_map;

  CollectionRef get_collection(const coll_t& cid);

  Finisher finisher;

  std::atomic<uint64_t> used_bytes;

  void _do_transaction(Transaction& t);

  int _touch(const coll_t& cid, const ghobject_t& oid);
  int _write(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len,
	      const ceph::buffer::list& bl, uint32_t fadvise_flags = 0);
  int _zero(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len);
  int _truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size);
  int _remove(const coll_t& cid, const ghobject_t& oid);
  int _setattrs(const coll_t& cid, const ghobject_t& oid, std::map<std::string,ceph::buffer::ptr>& aset);
  int _rmattr(const coll_t& cid, const ghobject_t& oid, const char *name);
  int _rmattrs(const coll_t& cid, const ghobject_t& oid);
  int _clone(const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid);
  int _clone_range(const coll_t& cid, const ghobject_t& oldoid,
		   const ghobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _omap_clear(const coll_t& cid, const ghobject_t &oid);
  int _omap_setkeys(const coll_t& cid, const ghobject_t &oid, ceph::buffer::list& aset_bl);
  int _omap_rmkeys(const coll_t& cid, const ghobject_t &oid, ceph::buffer::list& keys_bl);
  int _omap_rmkeyrange(const coll_t& cid, const ghobject_t &oid,
		       const std::string& first, const std::string& last);
  int _omap_setheader(const coll_t& cid, const ghobject_t &oid, const ceph::buffer::list &bl);

  int _collection_hint_expected_num_objs(const coll_t& cid, uint32_t pg_num,
      uint64_t num_objs) const { return 0; }
  int _create_collection(const coll_t& c, int bits);
  int _destroy_collection(const coll_t& c);
  int _collection_add(const coll_t& cid, const coll_t& ocid, const ghobject_t& oid);
  int _collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid,
			      coll_t cid, const ghobject_t& o);
  int _split_collection(const coll_t& cid, uint32_t bits, uint32_t rem, coll_t dest);
  int _merge_collection(const coll_t& cid, uint32_t bits, coll_t dest);

  int _save();
  int _load();

  void dump(ceph::Formatter *f);
  void dump_all();

public:
  MemStore(CephContext *cct, const std::string& path)
    : ObjectStore(cct, path),
      finisher(cct),
      used_bytes(0) {}
  ~MemStore() override { }

  std::string get_type() override {
    return "memstore";
  }

  bool test_mount_in_use() override {
    return false;
  }

  int mount() override;
  int umount() override;

  int fsck(bool deep) override {
    return 0;
  }

  int validate_hobject_key(const hobject_t &obj) const override {
    return 0;
  }
  unsigned get_max_attr_name_length() override {
    return 256;  // arbitrary; there is no real limit internally
  }

  int mkfs() override;
  int mkjournal() override {
    return 0;
  }
  bool wants_journal() override {
    return false;
  }
  bool allows_journal() override {
    return false;
  }
  bool needs_journal() override {
    return false;
  }

  int get_devices(std::set<std::string> *ls) override {
    // no devices for us!
    return 0;
  }

  int statfs(struct store_statfs_t *buf,
             osd_alert_list_t* alerts = nullptr) override;
  int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
		  bool *per_pool_omap) override;

  bool exists(CollectionHandle &c, const ghobject_t& oid) override;
  int stat(CollectionHandle &c, const ghobject_t& oid,
	   struct stat *st, bool allow_eio = false) override;
  int set_collection_opts(
    CollectionHandle& c,
    const pool_opts_t& opts) override;
  int read(
    CollectionHandle &c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    ceph::buffer::list& bl,
    uint32_t op_flags = 0) override;
  using ObjectStore::fiemap;
  int fiemap(CollectionHandle& c, const ghobject_t& oid,
	     uint64_t offset, size_t len, ceph::buffer::list& bl) override;
  int fiemap(CollectionHandle& c, const ghobject_t& oid, uint64_t offset,
	     size_t len, std::map<uint64_t, uint64_t>& destmap) override;
  int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name,
	      ceph::buffer::ptr& value) override;
  int getattrs(CollectionHandle &c, const ghobject_t& oid,
	       std::map<std::string,ceph::buffer::ptr,std::less<>>& aset) override;

  int list_collections(std::vector<coll_t>& ls) override;

  CollectionHandle open_collection(const coll_t& c) override {
    return get_collection(c);
  }
  CollectionHandle create_new_collection(const coll_t& c) override;

  void set_collection_commit_queue(const coll_t& cid,
				   ContextQueue *commit_queue) override {
  }

  bool collection_exists(const coll_t& c) override;
  int collection_empty(CollectionHandle& c, bool *empty) override;
  int collection_bits(CollectionHandle& c) override;
  int collection_list(CollectionHandle& cid,
		      const ghobject_t& start, const ghobject_t& end, int max,
		      std::vector<ghobject_t> *ls, ghobject_t *next) override;

  using ObjectStore::omap_get;
  int omap_get(
    CollectionHandle& c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    std::map<std::string, ceph::buffer::list> *out /// < [out] Key to value map
    ) override;

  using ObjectStore::omap_get_header;
  /// Get omap header
  int omap_get_header(
    CollectionHandle& c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) override;

  using ObjectStore::omap_get_keys;
  /// Get keys defined on oid
  int omap_get_keys(
    CollectionHandle& c,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    std::set<std::string> *keys      ///< [out] Keys defined on oid
    ) override;

  using ObjectStore::omap_get_values;
  /// Get key values
  int omap_get_values(
    CollectionHandle& c,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const std::set<std::string> &keys,     ///< [in] Keys to get
    std::map<std::string, ceph::buffer::list> *out ///< [out] Returned keys and values
    ) override;
#ifdef WITH_SEASTAR
  int omap_get_values(
    CollectionHandle &c,         ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const std::optional<std::string> &start_after,     ///< [in] Keys to get
    std::map<std::string, ceph::buffer::list> *out ///< [out] Returned keys and values
    ) override;
#endif

  using ObjectStore::omap_check_keys;
  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    CollectionHandle& c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const std::set<std::string> &keys, ///< [in] Keys to check
    std::set<std::string> *out         ///< [out] Subset of keys defined on oid
    ) override;

  using ObjectStore::get_omap_iterator;
  ObjectMap::ObjectMapIterator get_omap_iterator(
    CollectionHandle& c,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    ) override;

  void set_fsid(uuid_d u) override;
  uuid_d get_fsid() override;

  uint64_t estimate_objects_overhead(uint64_t num_objects) override {
    return 0; //do not care
  }

  objectstore_perf_stat_t get_cur_stats() override;
  void refresh_perf_counters() override {
  }
  const PerfCounters* get_perf_counters() const override {
    return nullptr;
  }


  int queue_transactions(
    CollectionHandle& ch,
    std::vector<Transaction>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL) override;
};




#endif
