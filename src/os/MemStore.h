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

#include "include/assert.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/Finisher.h"
#include "common/RWLock.h"
#include "ObjectStore.h"

class MemStore : public ObjectStore {
public:
  struct Object {
    bufferlist data;
    map<string,bufferptr> xattr;
    bufferlist omap_header;
    map<string,bufferlist> omap;

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(data, bl);
      ::encode(xattr, bl);
      ::encode(omap_header, bl);
      ::encode(omap, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& p) {
      DECODE_START(1, p);
      ::decode(data, p);
      ::decode(xattr, p);
      ::decode(omap_header, p);
      ::decode(omap, p);
      DECODE_FINISH(p);
    }
    void dump(Formatter *f) const {
      f->dump_int("data_len", data.length());
      f->dump_int("omap_header_len", omap_header.length());

      f->open_array_section("xattrs");
      for (map<string,bufferptr>::const_iterator p = xattr.begin();
	   p != xattr.end();
	   ++p) {
	f->open_object_section("xattr");
	f->dump_string("name", p->first);
	f->dump_int("length", p->second.length());
	f->close_section();	
      }
      f->close_section();

      f->open_array_section("omap");
      for (map<string,bufferlist>::const_iterator p = omap.begin();
	   p != omap.end();
	   ++p) {
	f->open_object_section("pair");
	f->dump_string("key", p->first);
	f->dump_int("length", p->second.length());
	f->close_section();	
      }
      f->close_section();
    }
  };
  typedef ceph::shared_ptr<Object> ObjectRef;

  struct Collection {
    ceph::unordered_map<ghobject_t, ObjectRef> object_hash;  ///< for lookup
    map<ghobject_t, ObjectRef> object_map;        ///< for iteration
    map<string,bufferptr> xattr;
    RWLock lock;   ///< for object_{map,hash}

    // NOTE: The lock only needs to protect the object_map/hash, not the
    // contents of individual objects.  The osd is already sequencing
    // reads and writes, so we will never see them concurrently at this
    // level.

    ObjectRef get_object(ghobject_t oid) {
      ceph::unordered_map<ghobject_t,ObjectRef>::iterator o = object_hash.find(oid);
      if (o == object_hash.end())
	return ObjectRef();
      return o->second;
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(xattr, bl);
      uint32_t s = object_map.size();
      ::encode(s, bl);
      for (map<ghobject_t, ObjectRef>::const_iterator p = object_map.begin();
	   p != object_map.end();
	   ++p) {
	::encode(p->first, bl);
	p->second->encode(bl);
      }
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& p) {
      DECODE_START(1, p);
      ::decode(xattr, p);
      uint32_t s;
      ::decode(s, p);
      while (s--) {
	ghobject_t k;
	::decode(k, p);
	ObjectRef o(new Object);
	o->decode(p);
	object_map.insert(make_pair(k, o));
	object_hash.insert(make_pair(k, o));
      }
      DECODE_FINISH(p);
    }

    Collection() : lock("MemStore::Collection::lock") {}
  };
  typedef ceph::shared_ptr<Collection> CollectionRef;

private:
  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    CollectionRef c;
    ObjectRef o;
    map<string,bufferlist>::iterator it;
  public:
    OmapIteratorImpl(CollectionRef c, ObjectRef o)
      : c(c), o(o), it(o->omap.begin()) {}

    int seek_to_first() {
      RWLock::RLocker l(c->lock);
      it = o->omap.begin();
      return 0;
    }
    int upper_bound(const string &after) {
      RWLock::RLocker l(c->lock);
      it = o->omap.upper_bound(after);
      return 0;
    }
    int lower_bound(const string &to) {
      RWLock::RLocker l(c->lock);
      it = o->omap.lower_bound(to);
      return 0;
    }
    bool valid() {
      RWLock::RLocker l(c->lock);
      return it != o->omap.end();      
    }
    int next() {
      RWLock::RLocker l(c->lock);
      ++it;
      return 0;
    }
    string key() {
      RWLock::RLocker l(c->lock);
      return it->first;
    }
    bufferlist value() {
      RWLock::RLocker l(c->lock);
      return it->second;
    }
    int status() {
      return 0;
    }
  };


  ceph::unordered_map<coll_t, CollectionRef> coll_map;
  RWLock coll_lock;    ///< rwlock to protect coll_map
  Mutex apply_lock;    ///< serialize all updates

  CollectionRef get_collection(coll_t cid);

  Finisher finisher;

  void _do_transaction(Transaction& t);

  void _write_into_bl(const bufferlist& src, unsigned offset, bufferlist *dst);

  int _touch(coll_t cid, const ghobject_t& oid);
  int _write(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, const bufferlist& bl,
      bool replica = false);
  int _zero(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len);
  int _truncate(coll_t cid, const ghobject_t& oid, uint64_t size);
  int _remove(coll_t cid, const ghobject_t& oid);
  int _setattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset);
  int _rmattr(coll_t cid, const ghobject_t& oid, const char *name);
  int _rmattrs(coll_t cid, const ghobject_t& oid);
  int _clone(coll_t cid, const ghobject_t& oldoid, const ghobject_t& newoid);
  int _clone_range(coll_t cid, const ghobject_t& oldoid,
		   const ghobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _omap_clear(coll_t cid, const ghobject_t &oid);
  int _omap_setkeys(coll_t cid, const ghobject_t &oid,
		    const map<string, bufferlist> &aset);
  int _omap_rmkeys(coll_t cid, const ghobject_t &oid, const set<string> &keys);
  int _omap_rmkeyrange(coll_t cid, const ghobject_t &oid,
		       const string& first, const string& last);
  int _omap_setheader(coll_t cid, const ghobject_t &oid, const bufferlist &bl);

  int _collection_hint_expected_num_objs(coll_t cid, uint32_t pg_num,
      uint64_t num_objs) const { return 0; }
  int _create_collection(coll_t c);
  int _destroy_collection(coll_t c);
  int _collection_add(coll_t cid, coll_t ocid, const ghobject_t& oid);
  int _collection_move_rename(coll_t oldcid, const ghobject_t& oldoid,
			      coll_t cid, const ghobject_t& o);
  int _collection_setattr(coll_t cid, const char *name, const void *value,
			  size_t size);
  int _collection_setattrs(coll_t cid, map<string,bufferptr> &aset);
  int _collection_rmattr(coll_t cid, const char *name);
  int _split_collection(coll_t cid, uint32_t bits, uint32_t rem, coll_t dest);

  int _save();
  int _load();

  void dump(Formatter *f);
  void dump_all();

public:
  MemStore(CephContext *cct, const string& path)
    : ObjectStore(path),
      coll_lock("MemStore::coll_lock"),
      apply_lock("MemStore::apply_lock"),
      finisher(cct),
      sharded(false) { }
  ~MemStore() { }

  bool need_journal() { return false; };
  int peek_journal_fsid(uuid_d *fsid);

  bool test_mount_in_use() {
    return false;
  }

  int mount();
  int umount();

  unsigned get_max_object_name_length() {
    return 4096;
  }
  unsigned get_max_attr_name_length() {
    return 256;  // arbitrary; there is no real limit internally
  }

  int mkfs();
  int mkjournal() {
    return 0;
  }

  bool sharded;
  void set_allow_sharded_objects() {
    sharded = true;
  }
  bool get_allow_sharded_objects() {
    return sharded;
  }

  int statfs(struct statfs *buf);

  bool exists(coll_t cid, const ghobject_t& oid);
  int stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false); // struct stat?
  int read(
    coll_t cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio = false);
  int fiemap(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
  int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr& value);
  int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset);

  int list_collections(vector<coll_t>& ls);
  bool collection_exists(coll_t c);
  int collection_getattr(coll_t cid, const char *name,
			 void *value, size_t size);
  int collection_getattr(coll_t cid, const char *name, bufferlist& bl);
  int collection_getattrs(coll_t cid, map<string,bufferptr> &aset);
  bool collection_empty(coll_t c);
  int collection_list(coll_t cid, vector<ghobject_t>& o);
  int collection_list_partial(coll_t cid, ghobject_t start,
			      int min, int max, snapid_t snap, 
			      vector<ghobject_t> *ls, ghobject_t *next);
  int collection_list_range(coll_t cid, ghobject_t start, ghobject_t end,
			    snapid_t seq, vector<ghobject_t> *ls);

  int omap_get(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    );

  /// Get omap header
  int omap_get_header(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    );

  /// Get keys defined on oid
  int omap_get_keys(
    coll_t cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    );

  /// Get key values
  int omap_get_values(
    coll_t cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    );

  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    coll_t cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    );

  ObjectMap::ObjectMapIterator get_omap_iterator(
    coll_t cid,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    );

  void set_fsid(uuid_d u);
  uuid_d get_fsid();

  objectstore_perf_stat_t get_cur_stats();

  int queue_transactions(
    Sequencer *osr, list<Transaction*>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL);
};




#endif
