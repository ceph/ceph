// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_GENERICOBJECTMAP_H
#define CEPH_GENERICOBJECTMAP_H

#include "include/buffer.h"
#include <set>
#include <map>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "include/memory.h"
#include "ObjectMap.h"
#include "kv/KeyValueDB.h"
#include "osd/osd_types.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/simple_cache.hpp"


/**
 * Genericobjectmap: Provide with key/value associated to ghobject_t APIs to caller
 * and avoid concerning too much. Wrap and combine KeyValueDB/ObjectMap APIs
 * with ghobject_t and adding clone capacity.
 *
 * Prefix space structure:
 *
 * - GHOBJECT_TO_SEQ: Contains leaf mapping from ghobject_t->Header(including
 *                    hobj.seq and related metadata)
 * - INTERN_PREFIX: GLOBAL_STATE_KEY - contains the global state
 *                                  @see State
 *                                  @see write_state
 *                                  @see init
 *                                  @see generate_new_header
 * - INTERN_PREFIX + header_key(header->seq) + COMPLETE_PREFIX: see below
 * - INTERN_PREFIX + header_key(header->seq) + PARENT_KEY
 *              : used to store parent header(same as headers in GHOBJECT_TO_SEQ)
 * - USER_PREFIX + header_key(header->seq) + [CUSTOM_PREFIX]
 *              : key->value which set by callers
 *
 * For each node (represented by a header), we
 * store three mappings: the key mapping, the complete mapping, and the parent.
 * The complete mapping (COMPLETE_PREFIX space) is key->key.  Each x->y entry in
 * this mapping indicates that the key mapping contains all entries on [x,y).
 * Note, max string is represented by "", so ""->"" indicates that the parent
 * is unnecessary (@see rm_keys).  When looking up a key not contained in the
 * the complete set, we have to check the parent if we don't find it in the
 * key set.  During rm_keys, we copy keys from the parent and update the
 * complete set to reflect the change @see rm_keys.
 */

// This class only provide basic read capacity, suggest inherit it to
// implement write transaction to use it. @see StripObjectMap
class GenericObjectMap {
 public:
  boost::scoped_ptr<KeyValueDB> db;

  /**
   * Serializes access to next_seq as well as the in_use set
   */
  Mutex header_lock;

  GenericObjectMap(KeyValueDB *db) : db(db), header_lock("GenericObjectMap") {}

  int get(
    const coll_t &cid,
    const ghobject_t &oid,
    const string &prefix,
    map<string, bufferlist> *out
    );

  int get_keys(
    const coll_t &cid,
    const ghobject_t &oid,
    const string &prefix,
    set<string> *keys
    );

  int get_values(
    const coll_t &cid,
    const ghobject_t &oid,
    const string &prefix,
    const set<string> &keys,
    map<string, bufferlist> *out
    );

  int check_keys(
    const coll_t &cid,
    const ghobject_t &oid,
    const string &prefix,
    const set<string> &keys,
    set<string> *out
    );

  /// Read initial state from backing store
  int init(bool upgrade = false);

  /// Upgrade store to current version
  int upgrade() {return 0;}

  /// Consistency check, debug, there must be no parallel writes
  bool check(std::ostream &out);

  /// Util, list all objects, there must be no other concurrent access
  int list_objects(const coll_t &cid, ghobject_t start, ghobject_t end, int max,
                   vector<ghobject_t> *objs, ///< [out] objects
                   ghobject_t *next);

  ObjectMap::ObjectMapIterator get_iterator(const coll_t &cid,
                                            const ghobject_t &oid,
                                            const string &prefix);

  KeyValueDB::Transaction get_transaction() { return db->get_transaction(); }
  int submit_transaction(KeyValueDB::Transaction t) {
    return db->submit_transaction(t);
  }
  int submit_transaction_sync(KeyValueDB::Transaction t) {
    return db->submit_transaction_sync(t);
  }

  /// persistent state for store @see generate_header
  struct State {
    __u8 v;
    uint64_t seq;
    State() : v(0), seq(1) {}
    State(uint64_t seq) : v(0), seq(seq) {}

    void encode(bufferlist &bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(v, bl);
      ::encode(seq, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator &bl) {
      DECODE_START(1, bl);
      ::decode(v, bl);
      ::decode(seq, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const {
      f->dump_unsigned("seq", seq);
    }

    static void generate_test_instances(list<State*> &o) {
      o.push_back(new State(0));
      o.push_back(new State(20));
    }
  } state;

  struct _Header {
    uint64_t seq;
    uint64_t parent;
    uint64_t num_children;

    coll_t cid;
    ghobject_t oid;

    // Used by successor
    bufferlist data;

    void encode(bufferlist &bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(seq, bl);
      ::encode(parent, bl);
      ::encode(num_children, bl);
      ::encode(cid, bl);
      ::encode(oid, bl);
      ::encode(data, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator &bl) {
      DECODE_START(1, bl);
      ::decode(seq, bl);
      ::decode(parent, bl);
      ::decode(num_children, bl);
      ::decode(cid, bl);
      ::decode(oid, bl);
      ::decode(data, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter *f) const {
      f->dump_unsigned("seq", seq);
      f->dump_unsigned("parent", parent);
      f->dump_unsigned("num_children", num_children);
      f->dump_stream("coll") << cid;
      f->dump_stream("oid") << oid;
    }

    _Header() : seq(0), parent(0), num_children(1) {}
  };

  typedef ceph::shared_ptr<_Header> Header;

  Header lookup_header(const coll_t &cid, const ghobject_t &oid) {
    Mutex::Locker l(header_lock);
    return _lookup_header(cid, oid);
  }

  /// Lookup or create header for c oid
  Header lookup_create_header(const coll_t &cid, const ghobject_t &oid,
    KeyValueDB::Transaction t);

  /// Set leaf node for c and oid to the value of header
  void set_header(const coll_t &cid, const ghobject_t &oid, _Header &header,
    KeyValueDB::Transaction t);

  // Move all modify member function to "protect", in order to indicate these
  // should be made use of by sub-class
  void set_keys(
    const Header header,
    const string &prefix,
    const map<string, bufferlist> &set,
    KeyValueDB::Transaction t
    );

  int clear(
    const Header header,
    KeyValueDB::Transaction t
    );

  int rm_keys(
    const Header header,
    const string &prefix,
    const set<string> &buffered_keys,
    const set<string> &to_clear,
    KeyValueDB::Transaction t
    );

  void clone(
    const Header origin_header,
    const coll_t &cid,
    const ghobject_t &target,
    KeyValueDB::Transaction t,
    Header *old_header,
    Header *new_header
    );

  void rename(
    const Header header,
    const coll_t &cid,
    const ghobject_t &target,
    KeyValueDB::Transaction t
    );

  static const string GLOBAL_STATE_KEY;
  static const string PARENT_KEY;

  static const string USER_PREFIX;
  static const string INTERN_PREFIX;
  static const string PARENT_PREFIX;
  static const string COMPLETE_PREFIX;
  static const string GHOBJECT_TO_SEQ_PREFIX;

  static const string GHOBJECT_KEY_SEP_S;
  static const char GHOBJECT_KEY_SEP_C;
  static const char GHOBJECT_KEY_ENDING;

private:
  /// Implicit lock on Header->seq

  static string header_key(const coll_t &cid);
  static string header_key(const coll_t &cid, const ghobject_t &oid);
  static bool parse_header_key(const string &in, coll_t *c, ghobject_t *oid);

  string seq_key(uint64_t seq) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%.*" PRId64, (int)(2*sizeof(seq)), seq);
    return string(buf);
  }

  string user_prefix(Header header, const string &prefix);
  string complete_prefix(Header header);
  string parent_seq_prefix(uint64_t seq);

  class EmptyIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
  public:
    int seek_to_first() { return 0; }
    int seek_to_last() { return 0; }
    int upper_bound(const string &after) { return 0; }
    int lower_bound(const string &to) { return 0; }
    bool valid() { return false; }
    int next(bool validate=true) { assert(0); return 0; }
    string key() { assert(0); return ""; }
    bufferlist value() { assert(0); return bufferlist(); }
    int status() { return 0; }
  };


  /// Iterator
  class GenericObjectMapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
  public:
    GenericObjectMap *map;

    /// NOTE: implicit lock on header->seq AND for all ancestors
    Header header;

    /// parent_iter == NULL iff no parent
    ceph::shared_ptr<GenericObjectMapIteratorImpl> parent_iter;
    KeyValueDB::Iterator key_iter;
    KeyValueDB::Iterator complete_iter;

    /// cur_iter points to currently valid iterator
    ceph::shared_ptr<ObjectMap::ObjectMapIteratorImpl> cur_iter;
    int r;

    /// init() called, key_iter, complete_iter, parent_iter filled in
    bool ready;
    /// past end
    bool invalid;

    string prefix;

    GenericObjectMapIteratorImpl(GenericObjectMap *map, Header header,
        const string &_prefix) : map(map), header(header), r(0), ready(false),
                                 invalid(true), prefix(_prefix) { }
    int seek_to_first();
    int seek_to_last();
    int upper_bound(const string &after);
    int lower_bound(const string &to);
    bool valid();
    int next(bool validate=true);
    string key();
    bufferlist value();
    int status();

    bool on_parent() {
      return cur_iter == parent_iter;
    }

    /// skips to next valid parent entry
    int next_parent();

    /// Tests whether to_test is in complete region
    int in_complete_region(const string &to_test, ///[in] key to test
                           string *begin,         ///[out] beginning of region
                           string *end            ///[out] end of region
      ); ///< @returns true if to_test is in the complete region, else false

  private:
    int init();
    bool valid_parent();
    int adjust();
  };

protected:
  typedef ceph::shared_ptr<GenericObjectMapIteratorImpl> GenericObjectMapIterator;
  GenericObjectMapIterator _get_iterator(Header header, string prefix) {
    return GenericObjectMapIterator(new GenericObjectMapIteratorImpl(this, header, prefix));
  }

  Header generate_new_header(const coll_t &cid, const ghobject_t &oid,
                             Header parent, KeyValueDB::Transaction t) {
    Mutex::Locker l(header_lock);
    return _generate_new_header(cid, oid, parent, t);
  }

  // Scan keys in header into out_keys and out_values (if nonnull)
  int scan(Header header, const string &prefix, const set<string> &in_keys,
           set<string> *out_keys, map<string, bufferlist> *out_values);

 private:

  /// Removes node corresponding to header
  void clear_header(Header header, KeyValueDB::Transaction t);

  /// Set node containing input to new contents
  void set_parent_header(Header input, KeyValueDB::Transaction t);

    /// Remove leaf node corresponding to oid in c
  void remove_header(const coll_t &cid, const ghobject_t &oid, Header header,
      KeyValueDB::Transaction t);

  /**
   * Generate new header for c oid with new seq number
   *
   * Has the side effect of syncronously saving the new GenericObjectMap state
   */
  Header _generate_new_header(const coll_t &cid, const ghobject_t &oid,
                              Header parent, KeyValueDB::Transaction t);

  // Lookup leaf header for c oid
  Header _lookup_header(const coll_t &cid, const ghobject_t &oid);

  // Lookup header node for input
  Header lookup_parent(Header input);

  // Remove header and all related prefixes
  int _clear(Header header, KeyValueDB::Transaction t);

  // Adds to t operations necessary to add new_complete to the complete set
  int merge_new_complete(Header header, const map<string, string> &new_complete,
      GenericObjectMapIterator iter, KeyValueDB::Transaction t);

  // Writes out State (mainly next_seq)
  int write_state(KeyValueDB::Transaction _t);

  // 0 if the complete set now contains all of key space, < 0 on error, 1 else
  int need_parent(GenericObjectMapIterator iter);

  // Copies header entry from parent @see rm_keys
  int copy_up_header(Header header, KeyValueDB::Transaction t);

  // Sets header @see set_header
  void _set_header(Header header, const bufferlist &bl,
                   KeyValueDB::Transaction t);
};
WRITE_CLASS_ENCODER(GenericObjectMap::_Header)
WRITE_CLASS_ENCODER(GenericObjectMap::State)

#endif
