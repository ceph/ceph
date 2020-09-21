// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#ifndef DBOBJECTMAP_DB_H
#define DBOBJECTMAP_DB_H

#include "include/buffer_fwd.h"
#include <set>
#include <map>
#include <string>

#include <vector>
#include <boost/scoped_ptr.hpp>

#include "os/ObjectMap.h"
#include "kv/KeyValueDB.h"
#include "osd/osd_types.h"
#include "common/ceph_mutex.h"
#include "common/simple_cache.hpp"
#include <boost/optional/optional_io.hpp>

#include "SequencerPosition.h"

/**
 * DBObjectMap: Implements ObjectMap in terms of KeyValueDB
 *
 * Prefix space structure:
 *
 * @see complete_prefix
 * @see user_prefix
 * @see sys_prefix
 *
 * - HOBJECT_TO_SEQ: Contains leaf mapping from ghobject_t->header.seq and
 *                   corresponding omap header
 * - SYS_PREFIX: GLOBAL_STATE_KEY - contains next seq number
 *                                  @see State
 *                                  @see write_state
 *                                  @see init
 *                                  @see generate_new_header
 * - USER_PREFIX + header_key(header->seq) + USER_PREFIX
 *              : key->value for header->seq
 * - USER_PREFIX + header_key(header->seq) + COMPLETE_PREFIX: see below
 * - USER_PREFIX + header_key(header->seq) + XATTR_PREFIX: xattrs
 * - USER_PREFIX + header_key(header->seq) + SYS_PREFIX
 *              : USER_HEADER_KEY - omap header for header->seq
 *              : HEADER_KEY - encoding of header for header->seq
 *
 * For each node (represented by a header), we
 * store three mappings: the key mapping, the complete mapping, and the parent.
 * The complete mapping (COMPLETE_PREFIX space) is key->key.  Each x->y entry in
 * this mapping indicates that the key mapping contains all entries on [x,y).
 * Note, max std::string is represented by "", so ""->"" indicates that the parent
 * is unnecessary (@see rm_keys).  When looking up a key not contained in the
 * the complete std::set, we have to check the parent if we don't find it in the
 * key std::set.  During rm_keys, we copy keys from the parent and update the
 * complete std::set to reflect the change @see rm_keys.
 */
class DBObjectMap : public ObjectMap {
public:

  KeyValueDB *get_db() override { return db.get(); }

  /**
   * Serializes access to next_seq as well as the in_use std::set
   */
  ceph::mutex header_lock = ceph::make_mutex("DBOBjectMap");
  ceph::condition_variable header_cond;
  ceph::condition_variable map_header_cond;

  /**
   * Std::Set of headers currently in use
   */
  std::set<uint64_t> in_use;
  std::set<ghobject_t> map_header_in_use;

  /**
   * Takes the map_header_in_use entry in constructor, releases in
   * destructor
   */
  class MapHeaderLock {
    DBObjectMap *db;
    boost::optional<ghobject_t> locked;

    MapHeaderLock(const MapHeaderLock &);
    MapHeaderLock &operator=(const MapHeaderLock &);
  public:
    explicit MapHeaderLock(DBObjectMap *db) : db(db) {}
    MapHeaderLock(DBObjectMap *db, const ghobject_t &oid) : db(db), locked(oid) {
      std::unique_lock l{db->header_lock};
      db->map_header_cond.wait(l, [db, this] {
        return !db->map_header_in_use.count(*locked);
      });
      db->map_header_in_use.insert(*locked);
    }

    const ghobject_t &get_locked() const {
      ceph_assert(locked);
      return *locked;
    }

    void swap(MapHeaderLock &o) {
      ceph_assert(db == o.db);

      // centos6's boost optional doesn't seem to have swap :(
      boost::optional<ghobject_t> _locked = o.locked;
      o.locked = locked;
      locked = _locked;
    }

    ~MapHeaderLock() {
      if (locked) {
	std::lock_guard l{db->header_lock};
	ceph_assert(db->map_header_in_use.count(*locked));
	db->map_header_cond.notify_all();
	db->map_header_in_use.erase(*locked);
      }
    }
  };

  DBObjectMap(CephContext* cct, KeyValueDB *db)
    : ObjectMap(cct, db),
      caches(cct->_conf->filestore_omap_header_cache_size)
    {}

  int set_keys(
    const ghobject_t &oid,
    const std::map<std::string, ceph::buffer::list> &set,
    const SequencerPosition *spos=0
    ) override;

  int set_header(
    const ghobject_t &oid,
    const ceph::buffer::list &bl,
    const SequencerPosition *spos=0
    ) override;

  int get_header(
    const ghobject_t &oid,
    ceph::buffer::list *bl
    ) override;

  int clear(
    const ghobject_t &oid,
    const SequencerPosition *spos=0
    ) override;

  int clear_keys_header(
    const ghobject_t &oid,
    const SequencerPosition *spos=0
    ) override;

  int rm_keys(
    const ghobject_t &oid,
    const std::set<std::string> &to_clear,
    const SequencerPosition *spos=0
    ) override;

  int get(
    const ghobject_t &oid,
    ceph::buffer::list *header,
    std::map<std::string, ceph::buffer::list> *out
    ) override;

  int get_keys(
    const ghobject_t &oid,
    std::set<std::string> *keys
    ) override;

  int get_values(
    const ghobject_t &oid,
    const std::set<std::string> &keys,
    std::map<std::string, ceph::buffer::list> *out
    ) override;

  int check_keys(
    const ghobject_t &oid,
    const std::set<std::string> &keys,
    std::set<std::string> *out
    ) override;

  int get_xattrs(
    const ghobject_t &oid,
    const std::set<std::string> &to_get,
    std::map<std::string, ceph::buffer::list> *out
    ) override;

  int get_all_xattrs(
    const ghobject_t &oid,
    std::set<std::string> *out
    ) override;

  int set_xattrs(
    const ghobject_t &oid,
    const std::map<std::string, ceph::buffer::list> &to_set,
    const SequencerPosition *spos=0
    ) override;

  int remove_xattrs(
    const ghobject_t &oid,
    const std::set<std::string> &to_remove,
    const SequencerPosition *spos=0
    ) override;

  int clone(
    const ghobject_t &oid,
    const ghobject_t &target,
    const SequencerPosition *spos=0
    ) override;

  int rename(
    const ghobject_t &from,
    const ghobject_t &to,
    const SequencerPosition *spos=0
    ) override;

  int legacy_clone(
    const ghobject_t &oid,
    const ghobject_t &target,
    const SequencerPosition *spos=0
    ) override;

  /// Read initial state from backing store
  int get_state();
  /// Write current state settings to DB
  void set_state();
  /// Read initial state and upgrade or initialize state
  int init(bool upgrade = false);

  /// Upgrade store to current version
  int upgrade_to_v2();

  /// Consistency check, debug, there must be no parallel writes
  int check(std::ostream &out, bool repair = false, bool force = false) override;

  /// Ensure that all previous operations are durable
  int sync(const ghobject_t *oid=0, const SequencerPosition *spos=0) override;

  void compact() override {
    ceph_assert(db);
    db->compact();
  }

  /// Util, get all objects, there must be no other concurrent access
  int list_objects(std::vector<ghobject_t> *objs ///< [out] objects
    );

  struct _Header;
  // Util, get all object headers, there must be no other concurrent access
  int list_object_headers(std::vector<_Header> *out ///< [out] headers
    );

  ObjectMapIterator get_iterator(const ghobject_t &oid) override;

  static const std::string USER_PREFIX;
  static const std::string XATTR_PREFIX;
  static const std::string SYS_PREFIX;
  static const std::string COMPLETE_PREFIX;
  static const std::string HEADER_KEY;
  static const std::string USER_HEADER_KEY;
  static const std::string GLOBAL_STATE_KEY;
  static const std::string HOBJECT_TO_SEQ;

  /// Legacy
  static const std::string LEAF_PREFIX;
  static const std::string REVERSE_LEAF_PREFIX;

  /// persistent state for store @see generate_header
  struct State {
    static const __u8 CUR_VERSION = 3;
    __u8 v;
    uint64_t seq;
    // legacy is false when complete regions never used
    bool legacy;
    State() : v(0), seq(1), legacy(false) {}
    explicit State(uint64_t seq) : v(0), seq(seq), legacy(false) {}

    void encode(ceph::buffer::list &bl) const {
      ENCODE_START(3, 1, bl);
      encode(v, bl);
      encode(seq, bl);
      encode(legacy, bl);
      ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator &bl) {
      DECODE_START(3, bl);
      if (struct_v >= 2)
	decode(v, bl);
      else
	v = 0;
      decode(seq, bl);
      if (struct_v >= 3)
	decode(legacy, bl);
      else
	legacy = false;
      DECODE_FINISH(bl);
    }

    void dump(ceph::Formatter *f) const {
      f->dump_unsigned("v", v);
      f->dump_unsigned("seq", seq);
      f->dump_bool("legacy", legacy);
    }

    static void generate_test_instances(std::list<State*> &o) {
      o.push_back(new State(0));
      o.push_back(new State(20));
    }
  } state;

  struct _Header {
    uint64_t seq;
    uint64_t parent;
    uint64_t num_children;

    ghobject_t oid;

    SequencerPosition spos;

    void encode(ceph::buffer::list &bl) const {
      coll_t unused;
      ENCODE_START(2, 1, bl);
      encode(seq, bl);
      encode(parent, bl);
      encode(num_children, bl);
      encode(unused, bl);
      encode(oid, bl);
      encode(spos, bl);
      ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator &bl) {
      coll_t unused;
      DECODE_START(2, bl);
      decode(seq, bl);
      decode(parent, bl);
      decode(num_children, bl);
      decode(unused, bl);
      decode(oid, bl);
      if (struct_v >= 2)
	decode(spos, bl);
      DECODE_FINISH(bl);
    }

    void dump(ceph::Formatter *f) const {
      f->dump_unsigned("seq", seq);
      f->dump_unsigned("parent", parent);
      f->dump_unsigned("num_children", num_children);
      f->dump_stream("oid") << oid;
    }

    static void generate_test_instances(std::list<_Header*> &o) {
      o.push_back(new _Header);
      o.push_back(new _Header);
      o.back()->parent = 20;
      o.back()->seq = 30;
    }

    size_t length() {
      return sizeof(_Header);
    }

    _Header() : seq(0), parent(0), num_children(1) {}
  };

  /// Std::String munging (public for testing)
  static std::string ghobject_key(const ghobject_t &oid);
  static std::string ghobject_key_v0(coll_t c, const ghobject_t &oid);
  static int is_buggy_ghobject_key_v1(CephContext* cct,
				      const std::string &in);
private:
  /// Implicit lock on Header->seq
  typedef std::shared_ptr<_Header> Header;
  ceph::mutex cache_lock = ceph::make_mutex("DBObjectMap::CacheLock");
  SimpleLRU<ghobject_t, _Header> caches;

  std::string map_header_key(const ghobject_t &oid);
  std::string header_key(uint64_t seq);
  std::string complete_prefix(Header header);
  std::string user_prefix(Header header);
  std::string sys_prefix(Header header);
  std::string xattr_prefix(Header header);
  std::string sys_parent_prefix(_Header header);
  std::string sys_parent_prefix(Header header) {
    return sys_parent_prefix(*header);
  }

  class EmptyIteratorImpl : public ObjectMapIteratorImpl {
  public:
    int seek_to_first() override { return 0; }
    int seek_to_last() { return 0; }
    int upper_bound(const std::string &after) override { return 0; }
    int lower_bound(const std::string &to) override { return 0; }
    bool valid() override { return false; }
    int next() override { ceph_abort(); return 0; }
    std::string key() override { ceph_abort(); return ""; }
    ceph::buffer::list value() override { ceph_abort(); return ceph::buffer::list(); }
    int status() override { return 0; }
  };


  /// Iterator
  class DBObjectMapIteratorImpl : public ObjectMapIteratorImpl {
  public:
    DBObjectMap *map;

    /// NOTE: implicit lock hlock->get_locked() when returned out of the class
    MapHeaderLock hlock;
    /// NOTE: implicit lock on header->seq AND for all ancestors
    Header header;

    /// parent_iter == NULL iff no parent
    std::shared_ptr<DBObjectMapIteratorImpl> parent_iter;
    KeyValueDB::Iterator key_iter;
    KeyValueDB::Iterator complete_iter;

    /// cur_iter points to currently valid iterator
    std::shared_ptr<ObjectMapIteratorImpl> cur_iter;
    int r;

    /// init() called, key_iter, complete_iter, parent_iter filled in
    bool ready;
    /// past end
    bool invalid;

    DBObjectMapIteratorImpl(DBObjectMap *map, Header header) :
      map(map), hlock(map), header(header), r(0), ready(false), invalid(true) {}
    int seek_to_first() override;
    int seek_to_last();
    int upper_bound(const std::string &after) override;
    int lower_bound(const std::string &to) override;
    bool valid() override;
    int next() override;
    std::string key() override;
    ceph::buffer::list value() override;
    int status() override;

    bool on_parent() {
      return cur_iter == parent_iter;
    }

    /// skips to next valid parent entry
    int next_parent();
    
    /// first parent() >= to
    int lower_bound_parent(const std::string &to);

    /**
     * Tests whether to_test is in complete region
     *
     * postcondition: complete_iter will be max s.t. complete_iter->value > to_test
     */
    int in_complete_region(const std::string &to_test, ///< [in] key to test
			   std::string *begin,         ///< [out] beginning of region
			   std::string *end            ///< [out] end of region
      ); ///< @returns true if to_test is in the complete region, else false

  private:
    int init();
    bool valid_parent();
    int adjust();
  };

  typedef std::shared_ptr<DBObjectMapIteratorImpl> DBObjectMapIterator;
  DBObjectMapIterator _get_iterator(Header header) {
    return std::make_shared<DBObjectMapIteratorImpl>(this, header);
  }

  /// sys

  /// Removes node corresponding to header
  void clear_header(Header header, KeyValueDB::Transaction t);

  /// Std::Set node containing input to new contents
  void set_header(Header input, KeyValueDB::Transaction t);

  /// Remove leaf node corresponding to oid in c
  void remove_map_header(
    const MapHeaderLock &l,
    const ghobject_t &oid,
    Header header,
    KeyValueDB::Transaction t);

  /// Std::Set leaf node for c and oid to the value of header
  void set_map_header(
    const MapHeaderLock &l,
    const ghobject_t &oid, _Header header,
    KeyValueDB::Transaction t);

  /// Std::Set leaf node for c and oid to the value of header
  bool check_spos(const ghobject_t &oid,
		  Header header,
		  const SequencerPosition *spos);

  /// Lookup or create header for c oid
  Header lookup_create_map_header(
    const MapHeaderLock &l,
    const ghobject_t &oid,
    KeyValueDB::Transaction t);

  /**
   * Generate new header for c oid with new seq number
   *
   * Has the side effect of synchronously saving the new DBObjectMap state
   */
  Header _generate_new_header(const ghobject_t &oid, Header parent);
  Header generate_new_header(const ghobject_t &oid, Header parent) {
    std::lock_guard l{header_lock};
    return _generate_new_header(oid, parent);
  }

  /// Lookup leaf header for c oid
  Header _lookup_map_header(
    const MapHeaderLock &l,
    const ghobject_t &oid);
  Header lookup_map_header(
    const MapHeaderLock &l2,
    const ghobject_t &oid) {
    std::lock_guard l{header_lock};
    return _lookup_map_header(l2, oid);
  }

  /// Lookup header node for input
  Header lookup_parent(Header input);


  /// Helpers
  int _get_header(Header header, ceph::buffer::list *bl);

  /// Scan keys in header into out_keys and out_values (if nonnull)
  int scan(Header header,
	   const std::set<std::string> &in_keys,
	   std::set<std::string> *out_keys,
	   std::map<std::string, ceph::buffer::list> *out_values);

  /// Remove header and all related prefixes
  int _clear(Header header,
	     KeyValueDB::Transaction t);

  /* Scan complete region bumping *begin to the beginning of any
   * containing region and adding all complete region keys between
   * the updated begin and end to the complete_keys_to_remove std::set */
  int merge_new_complete(DBObjectMapIterator &iter,
			 std::string *begin,
			 const std::string &end,
			 std::set<std::string> *complete_keys_to_remove);

  /// Writes out State (mainly next_seq)
  int write_state(KeyValueDB::Transaction _t =
		  KeyValueDB::Transaction());

  /// Copies header entry from parent @see rm_keys
  int copy_up_header(Header header,
		     KeyValueDB::Transaction t);

  /// Sets header @see set_header
  void _set_header(Header header, const ceph::buffer::list &bl,
		   KeyValueDB::Transaction t);

  /**
   * Removes header seq lock and possibly object lock
   * once Header is out of scope
   * @see lookup_parent
   * @see generate_new_header
   */
  class RemoveOnDelete {
  public:
    DBObjectMap *db;
    explicit RemoveOnDelete(DBObjectMap *db) :
      db(db) {}
    void operator() (_Header *header) {
      std::lock_guard l{db->header_lock};
      ceph_assert(db->in_use.count(header->seq));
      db->in_use.erase(header->seq);
      db->header_cond.notify_all();
      delete header;
    }
  };
  friend class RemoveOnDelete;
};
WRITE_CLASS_ENCODER(DBObjectMap::_Header)
WRITE_CLASS_ENCODER(DBObjectMap::State)

std::ostream& operator<<(std::ostream& out, const DBObjectMap::_Header& h);

#endif
