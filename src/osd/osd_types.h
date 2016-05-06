// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_OSD_TYPES_H
#define CEPH_OSD_TYPES_H

#include <sstream>
#include <stdio.h>
#include <memory>
#include <boost/scoped_ptr.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/variant.hpp>

#include "include/rados/rados_types.hpp"

#include "msg/msg_types.h"
#include "include/types.h"
#include "include/utime.h"
#include "include/CompatSet.h"
#include "common/histogram.h"
#include "include/interval_set.h"
#include "include/inline_memory.h"
#include "common/Formatter.h"
#include "common/bloom_filter.hpp"
#include "common/hobject.h"
#include "common/snap_types.h"
#include "HitSet.h"
#include "Watch.h"
#include "OpRequest.h"
#include "include/cmp.h"
#include "librados/ListObjectImpl.h"

#define CEPH_OSD_ONDISK_MAGIC "ceph osd volume v026"

#define CEPH_OSD_FEATURE_INCOMPAT_BASE CompatSet::Feature(1, "initial feature set(~v.18)")
#define CEPH_OSD_FEATURE_INCOMPAT_PGINFO CompatSet::Feature(2, "pginfo object")
#define CEPH_OSD_FEATURE_INCOMPAT_OLOC CompatSet::Feature(3, "object locator")
#define CEPH_OSD_FEATURE_INCOMPAT_LEC  CompatSet::Feature(4, "last_epoch_clean")
#define CEPH_OSD_FEATURE_INCOMPAT_CATEGORIES  CompatSet::Feature(5, "categories")
#define CEPH_OSD_FEATURE_INCOMPAT_HOBJECTPOOL  CompatSet::Feature(6, "hobjectpool")
#define CEPH_OSD_FEATURE_INCOMPAT_BIGINFO CompatSet::Feature(7, "biginfo")
#define CEPH_OSD_FEATURE_INCOMPAT_LEVELDBINFO CompatSet::Feature(8, "leveldbinfo")
#define CEPH_OSD_FEATURE_INCOMPAT_LEVELDBLOG CompatSet::Feature(9, "leveldblog")
#define CEPH_OSD_FEATURE_INCOMPAT_SNAPMAPPER CompatSet::Feature(10, "snapmapper")
#define CEPH_OSD_FEATURE_INCOMPAT_SHARDS CompatSet::Feature(11, "sharded objects")
#define CEPH_OSD_FEATURE_INCOMPAT_HINTS CompatSet::Feature(12, "transaction hints")
#define CEPH_OSD_FEATURE_INCOMPAT_PGMETA CompatSet::Feature(13, "pg meta object")


/// max recovery priority for MBackfillReserve
#define OSD_RECOVERY_PRIORITY_MAX 255u

/// base recovery priority for MBackfillReserve
#define OSD_RECOVERY_PRIORITY_BASE 230u

/// base backfill priority for MBackfillReserve (degraded PG)
#define OSD_BACKFILL_DEGRADED_PRIORITY_BASE 200u

/// base backfill priority for MBackfillReserve
#define OSD_BACKFILL_PRIORITY_BASE 1u

typedef hobject_t collection_list_handle_t;

/// convert a single CPEH_OSD_FLAG_* to a string
const char *ceph_osd_flag_name(unsigned flag);
/// convert a single CEPH_OSD_OF_FLAG_* to a string
const char *ceph_osd_op_flag_name(unsigned flag);

/// convert CEPH_OSD_FLAG_* op flags to a string
string ceph_osd_flag_string(unsigned flags);
/// conver CEPH_OSD_OP_FLAG_* op flags to a string
string ceph_osd_op_flag_string(unsigned flags);

struct pg_shard_t {
  int32_t osd;
  shard_id_t shard;
  pg_shard_t() : osd(-1), shard(shard_id_t::NO_SHARD) {}
  explicit pg_shard_t(int osd) : osd(osd), shard(shard_id_t::NO_SHARD) {}
  pg_shard_t(int osd, shard_id_t shard) : osd(osd), shard(shard) {}
  static pg_shard_t undefined_shard() {
    return pg_shard_t(-1, shard_id_t::NO_SHARD);
  }
  bool is_undefined() const {
    return osd == -1;
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
};
WRITE_CLASS_ENCODER(pg_shard_t)
WRITE_EQ_OPERATORS_2(pg_shard_t, osd, shard)
WRITE_CMP_OPERATORS_2(pg_shard_t, osd, shard)
ostream &operator<<(ostream &lhs, const pg_shard_t &rhs);

class IsPGRecoverablePredicate {
public:
  /**
   * have encodes the shards available
   */
  virtual bool operator()(const set<pg_shard_t> &have) const = 0;
  virtual ~IsPGRecoverablePredicate() {}
};

class IsPGReadablePredicate {
public:
  /**
   * have encodes the shards available
   */
  virtual bool operator()(const set<pg_shard_t> &have) const = 0;
  virtual ~IsPGReadablePredicate() {}
};

inline ostream& operator<<(ostream& out, const osd_reqid_t& r) {
  return out << r.name << "." << r.inc << ":" << r.tid;
}

inline bool operator==(const osd_reqid_t& l, const osd_reqid_t& r) {
  return (l.name == r.name) && (l.inc == r.inc) && (l.tid == r.tid);
}
inline bool operator!=(const osd_reqid_t& l, const osd_reqid_t& r) {
  return (l.name != r.name) || (l.inc != r.inc) || (l.tid != r.tid);
}
inline bool operator<(const osd_reqid_t& l, const osd_reqid_t& r) {
  return (l.name < r.name) || (l.inc < r.inc) || 
    (l.name == r.name && l.inc == r.inc && l.tid < r.tid);
}
inline bool operator<=(const osd_reqid_t& l, const osd_reqid_t& r) {
  return (l.name < r.name) || (l.inc < r.inc) ||
    (l.name == r.name && l.inc == r.inc && l.tid <= r.tid);
}
inline bool operator>(const osd_reqid_t& l, const osd_reqid_t& r) { return !(l <= r); }
inline bool operator>=(const osd_reqid_t& l, const osd_reqid_t& r) { return !(l < r); }

namespace std {
  template<> struct hash<osd_reqid_t> {
    size_t operator()(const osd_reqid_t &r) const { 
      static hash<uint64_t> H;
      return H(r.name.num() ^ r.tid ^ r.inc);
    }
  };
} // namespace std


// -----

// a locator constrains the placement of an object.  mainly, which pool
// does it go in.
struct object_locator_t {
  // You specify either the hash or the key -- not both
  int64_t pool;     ///< pool id
  string key;       ///< key string (if non-empty)
  string nspace;    ///< namespace
  int64_t hash;     ///< hash position (if >= 0)

  explicit object_locator_t()
    : pool(-1), hash(-1) {}
  explicit object_locator_t(int64_t po)
    : pool(po), hash(-1)  {}
  explicit object_locator_t(int64_t po, int64_t ps)
    : pool(po), hash(ps)  {}
  explicit object_locator_t(int64_t po, string ns)
    : pool(po), nspace(ns), hash(-1) {}
  explicit object_locator_t(int64_t po, string ns, int64_t ps)
    : pool(po), nspace(ns), hash(ps) {}
  explicit object_locator_t(int64_t po, string ns, string s)
    : pool(po), key(s), nspace(ns), hash(-1) {}
  explicit object_locator_t(const hobject_t& soid)
    : pool(soid.pool), key(soid.get_key()), nspace(soid.nspace), hash(-1) {}

  int64_t get_pool() const {
    return pool;
  }

  void clear() {
    pool = -1;
    key = "";
    nspace = "";
    hash = -1;
  }

  bool empty() const {
    return pool == -1;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<object_locator_t*>& o);
};
WRITE_CLASS_ENCODER(object_locator_t)

inline bool operator==(const object_locator_t& l, const object_locator_t& r) {
  return l.pool == r.pool && l.key == r.key && l.nspace == r.nspace && l.hash == r.hash;
}
inline bool operator!=(const object_locator_t& l, const object_locator_t& r) {
  return !(l == r);
}

inline ostream& operator<<(ostream& out, const object_locator_t& loc)
{
  out << "@" << loc.pool;
  if (loc.nspace.length())
    out << ";" << loc.nspace;
  if (loc.key.length())
    out << ":" << loc.key;
  return out;
}

struct request_redirect_t {
private:
  object_locator_t redirect_locator; ///< this is authoritative
  string redirect_object; ///< If non-empty, the request goes to this object name
  bufferlist osd_instructions; ///< a bufferlist for the OSDs, passed but not interpreted by clients

  friend ostream& operator<<(ostream& out, const request_redirect_t& redir);
public:

  request_redirect_t() {}
  explicit request_redirect_t(const object_locator_t& orig, int64_t rpool) :
      redirect_locator(orig) { redirect_locator.pool = rpool; }
  explicit request_redirect_t(const object_locator_t& rloc) :
      redirect_locator(rloc) {}
  explicit request_redirect_t(const object_locator_t& orig,
                              const string& robj) :
      redirect_locator(orig), redirect_object(robj) {}

  void set_instructions(const bufferlist& bl) { osd_instructions = bl; }
  const bufferlist& get_instructions() { return osd_instructions; }

  bool empty() const { return redirect_locator.empty() &&
			      redirect_object.empty(); }

  void combine_with_locator(object_locator_t& orig, string& obj) const {
    orig = redirect_locator;
    if (!redirect_object.empty())
      obj = redirect_object;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<request_redirect_t*>& o);
};
WRITE_CLASS_ENCODER(request_redirect_t)

inline ostream& operator<<(ostream& out, const request_redirect_t& redir) {
  out << "object " << redir.redirect_object << ", locator{" << redir.redirect_locator << "}";
  return out;
}

// Internal OSD op flags - set by the OSD based on the op types
enum {
  CEPH_OSD_RMW_FLAG_READ        = (1 << 1),
  CEPH_OSD_RMW_FLAG_WRITE       = (1 << 2),
  CEPH_OSD_RMW_FLAG_CLASS_READ  = (1 << 3),
  CEPH_OSD_RMW_FLAG_CLASS_WRITE = (1 << 4),
  CEPH_OSD_RMW_FLAG_PGOP        = (1 << 5),
  CEPH_OSD_RMW_FLAG_CACHE       = (1 << 6),
  CEPH_OSD_RMW_FLAG_FORCE_PROMOTE   = (1 << 7),
  CEPH_OSD_RMW_FLAG_SKIP_HANDLE_CACHE = (1 << 8),
  CEPH_OSD_RMW_FLAG_SKIP_PROMOTE      = (1 << 9),
};


// pg stuff

#define OSD_SUPERBLOCK_GOBJECT ghobject_t(hobject_t(sobject_t(object_t("osd_superblock"), 0)))

// placement seed (a hash value)
typedef uint32_t ps_t;

// old (v1) pg_t encoding (wrap old struct ceph_pg)
struct old_pg_t {
  ceph_pg v;
  void encode(bufferlist& bl) const {
    ::encode_raw(v, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode_raw(v, bl);
  }
};
WRITE_CLASS_ENCODER(old_pg_t)

// placement group id
struct pg_t {
  uint64_t m_pool;
  uint32_t m_seed;
  int32_t m_preferred;

  pg_t() : m_pool(0), m_seed(0), m_preferred(-1) {}
  pg_t(ps_t seed, uint64_t pool, int pref=-1) :
    m_pool(pool), m_seed(seed), m_preferred(pref) {}
  // cppcheck-suppress noExplicitConstructor
  pg_t(const ceph_pg& cpg) :
    m_pool(cpg.pool), m_seed(cpg.ps), m_preferred((__s16)cpg.preferred) {}

  // cppcheck-suppress noExplicitConstructor
  pg_t(const old_pg_t& opg) {
    *this = opg.v;
  }

  old_pg_t get_old_pg() const {
    old_pg_t o;
    assert(m_pool < 0xffffffffull);
    o.v.pool = m_pool;
    o.v.ps = m_seed;
    o.v.preferred = (__s16)m_preferred;
    return o;
  }

  ps_t ps() const {
    return m_seed;
  }
  uint64_t pool() const {
    return m_pool;
  }
  int32_t preferred() const {
    return m_preferred;
  }

  static const uint8_t calc_name_buf_size = 36;  // max length for max values len("18446744073709551615.ffffffff") + future suffix len("_head") + '\0'
  char *calc_name(char *buf, const char *suffix_backwords) const;

  void set_ps(ps_t p) {
    m_seed = p;
  }
  void set_pool(uint64_t p) {
    m_pool = p;
  }
  void set_preferred(int32_t osd) {
    m_preferred = osd;
  }

  pg_t get_parent() const;
  pg_t get_ancestor(unsigned old_pg_num) const;

  int print(char *o, int maxlen) const;
  bool parse(const char *s);

  bool is_split(unsigned old_pg_num, unsigned new_pg_num, set<pg_t> *pchildren) const;

  /**
   * Returns b such that for all object o:
   *   ~((~0)<<b) & o.hash) == 0 iff o is in the pg for *this
   */
  unsigned get_split_bits(unsigned pg_num) const;

  bool contains(int bits, const ghobject_t& oid) {
    return oid.match(bits, ps());
  }

  hobject_t get_hobj_start() const;
  hobject_t get_hobj_end(unsigned pg_num) const;

  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(m_pool, bl);
    ::encode(m_seed, bl);
    ::encode(m_preferred, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(m_pool, bl);
    ::decode(m_seed, bl);
    ::decode(m_preferred, bl);
  }
  void decode_old(bufferlist::iterator& bl) {
    old_pg_t opg;
    ::decode(opg, bl);
    *this = opg;
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_t*>& o);
};
WRITE_CLASS_ENCODER(pg_t)

inline bool operator<(const pg_t& l, const pg_t& r) {
  return l.pool() < r.pool() ||
    (l.pool() == r.pool() && (l.preferred() < r.preferred() ||
			      (l.preferred() == r.preferred() && (l.ps() < r.ps()))));
}
inline bool operator<=(const pg_t& l, const pg_t& r) {
  return l.pool() < r.pool() ||
    (l.pool() == r.pool() && (l.preferred() < r.preferred() ||
			      (l.preferred() == r.preferred() && (l.ps() <= r.ps()))));
}
inline bool operator==(const pg_t& l, const pg_t& r) {
  return l.pool() == r.pool() &&
    l.preferred() == r.preferred() &&
    l.ps() == r.ps();
}
inline bool operator!=(const pg_t& l, const pg_t& r) {
  return l.pool() != r.pool() ||
    l.preferred() != r.preferred() ||
    l.ps() != r.ps();
}
inline bool operator>(const pg_t& l, const pg_t& r) {
  return l.pool() > r.pool() ||
    (l.pool() == r.pool() && (l.preferred() > r.preferred() ||
			      (l.preferred() == r.preferred() && (l.ps() > r.ps()))));
}
inline bool operator>=(const pg_t& l, const pg_t& r) {
  return l.pool() > r.pool() ||
    (l.pool() == r.pool() && (l.preferred() > r.preferred() ||
			      (l.preferred() == r.preferred() && (l.ps() >= r.ps()))));
}

ostream& operator<<(ostream& out, const pg_t &pg);

namespace std {
  template<> struct hash< pg_t >
  {
    size_t operator()( const pg_t& x ) const
    {
      static hash<uint32_t> H;
      return H((x.pool() & 0xffffffff) ^ (x.pool() >> 32) ^ x.ps() ^ x.preferred());
    }
  };
} // namespace std

struct spg_t {
  pg_t pgid;
  shard_id_t shard;
  spg_t() : shard(shard_id_t::NO_SHARD) {}
  spg_t(pg_t pgid, shard_id_t shard) : pgid(pgid), shard(shard) {}
  explicit spg_t(pg_t pgid) : pgid(pgid), shard(shard_id_t::NO_SHARD) {}
  unsigned get_split_bits(unsigned pg_num) const {
    return pgid.get_split_bits(pg_num);
  }
  spg_t get_parent() const {
    return spg_t(pgid.get_parent(), shard);
  }
  ps_t ps() const {
    return pgid.ps();
  }
  uint64_t pool() const {
    return pgid.pool();
  }
  int32_t preferred() const {
    return pgid.preferred();
  }

  static const uint8_t calc_name_buf_size = pg_t::calc_name_buf_size + 4; // 36 + len('s') + len("255");
  char *calc_name(char *buf, const char *suffix_backwords) const;
 
  bool parse(const char *s);
  bool parse(const std::string& s) {
    return parse(s.c_str());
  }
  bool is_split(unsigned old_pg_num, unsigned new_pg_num,
		set<spg_t> *pchildren) const {
    set<pg_t> _children;
    set<pg_t> *children = pchildren ? &_children : NULL;
    bool is_split = pgid.is_split(old_pg_num, new_pg_num, children);
    if (pchildren && is_split) {
      for (set<pg_t>::iterator i = _children.begin();
	   i != _children.end();
	   ++i) {
	pchildren->insert(spg_t(*i, shard));
      }
    }
    return is_split;
  }
  bool is_no_shard() const {
    return shard == shard_id_t::NO_SHARD;
  }

  ghobject_t make_pgmeta_oid() const {
    return ghobject_t::make_pgmeta(pgid.pool(), pgid.ps(), shard);
  }

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(pgid, bl);
    ::encode(shard, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(pgid, bl);
    ::decode(shard, bl);
    DECODE_FINISH(bl);
  }

  hobject_t make_temp_hobject(const string& name) const {
    return hobject_t(object_t(name), "", CEPH_NOSNAP,
		     pgid.ps(),
		     hobject_t::POOL_TEMP_START - pgid.pool(), "");
  }

  ghobject_t make_temp_ghobject(const string& name) const {
    return ghobject_t(
      hobject_t(object_t(name), "", CEPH_NOSNAP,
		pgid.ps(),
		hobject_t::POOL_TEMP_START - pgid.pool(), ""),
      ghobject_t::NO_GEN,
      shard);
  }
};
WRITE_CLASS_ENCODER(spg_t)
WRITE_EQ_OPERATORS_2(spg_t, pgid, shard)
WRITE_CMP_OPERATORS_2(spg_t, pgid, shard)

namespace std {
  template<> struct hash< spg_t >
  {
    size_t operator()( const spg_t& x ) const
      {
      static hash<uint32_t> H;
      return H(hash<pg_t>()(x.pgid) ^ x.shard);
    }
  };
} // namespace std

ostream& operator<<(ostream& out, const spg_t &pg);

// ----------------------

class coll_t {
  enum type_t {
    TYPE_META = 0,
    TYPE_LEGACY_TEMP = 1,  /* no longer used */
    TYPE_PG = 2,
    TYPE_PG_TEMP = 3,
  };
  type_t type;
  spg_t pgid;
  uint64_t removal_seq;  // note: deprecated, not encoded

  char _str_buff[spg_t::calc_name_buf_size];
  char *_str;

  void calc_str();

  coll_t(type_t t, spg_t p, uint64_t r)
    : type(t), pgid(p), removal_seq(r) {
    calc_str();
  }

public:
  coll_t() : type(TYPE_META), removal_seq(0)
  {
    calc_str();
  }

  coll_t(const coll_t& other)
    : type(other.type), pgid(other.pgid), removal_seq(other.removal_seq) {
    calc_str();
  }

  explicit coll_t(spg_t pgid)
    : type(TYPE_PG), pgid(pgid), removal_seq(0)
  {
    calc_str();
  }

  coll_t& operator=(const coll_t& rhs)
  {
    this->type = rhs.type;
    this->pgid = rhs.pgid;
    this->removal_seq = rhs.removal_seq;
    this->calc_str();
    return *this;
  }

  // named constructors
  static coll_t meta() {
    return coll_t();
  }
  static coll_t pg(spg_t p) {
    return coll_t(p);
  }

  const std::string to_str() const {
    return string(_str);
  }
  const char *c_str() const {
    return _str;
  }

  bool parse(const std::string& s);

  int operator<(const coll_t &rhs) const {
    return type < rhs.type ||
		  (type == rhs.type && pgid < rhs.pgid);
  }

  bool is_meta() const {
    return type == TYPE_META;
  }
  bool is_pg_prefix(spg_t *pgid_) const {
    if (type == TYPE_PG || type == TYPE_PG_TEMP) {
      *pgid_ = pgid;
      return true;
    }
    return false;
  }
  bool is_pg() const {
    return type == TYPE_PG;
  }
  bool is_pg(spg_t *pgid_) const {
    if (type == TYPE_PG) {
      *pgid_ = pgid;
      return true;
    }
    return false;
  }
  bool is_temp() const {
    return type == TYPE_PG_TEMP;
  }
  bool is_temp(spg_t *pgid_) const {
    if (type == TYPE_PG_TEMP) {
      *pgid_ = pgid;
      return true;
    }
    return false;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  size_t encoded_size() const;

  inline bool operator==(const coll_t& rhs) const {
    // only compare type if meta
    if (type != rhs.type)
      return false;
    if (type == TYPE_META)
      return true;
    return type == rhs.type && pgid == rhs.pgid;
  }
  inline bool operator!=(const coll_t& rhs) const {
    return !(*this == rhs);
  }

  // get a TEMP collection that corresponds to the current collection,
  // which we presume is a pg collection.
  coll_t get_temp() const {
    assert(type == TYPE_PG);
    return coll_t(TYPE_PG_TEMP, pgid, 0);
  }

  ghobject_t get_min_hobj() const {
    ghobject_t o;
    switch (type) {
    case TYPE_PG:
      o.hobj.pool = pgid.pool();
      o.set_shard(pgid.shard);
      break;
    case TYPE_META:
      o.hobj.pool = -1;
      break;
    default:
      break;
    }
    return o;
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<coll_t*>& o);
};

WRITE_CLASS_ENCODER(coll_t)

inline ostream& operator<<(ostream& out, const coll_t& c) {
  out << c.to_str();
  return out;
}

namespace std {
  template<> struct hash<coll_t> {
    size_t operator()(const coll_t &c) const { 
      size_t h = 0;
      string str(c.to_str());
      std::string::const_iterator end(str.end());
      for (std::string::const_iterator s = str.begin(); s != end; ++s) {
	h += *s;
	h += (h << 10);
	h ^= (h >> 6);
      }
      h += (h << 3);
      h ^= (h >> 11);
      h += (h << 15);
      return h;
    }
  };
} // namespace std

inline ostream& operator<<(ostream& out, const ceph_object_layout &ol)
{
  out << pg_t(ol.ol_pgid);
  int su = ol.ol_stripe_unit;
  if (su)
    out << ".su=" << su;
  return out;
}



// compound rados version type
/* WARNING: If add member in eversion_t, please make sure the encode/decode function
 * work well. For little-endian machine, we should make sure there is no padding
 * in 32-bit machine and 64-bit machine.
 */
class eversion_t {
public:
  version_t version;
  epoch_t epoch;
  __u32 __pad;
  eversion_t() : version(0), epoch(0), __pad(0) {}
  eversion_t(epoch_t e, version_t v) : version(v), epoch(e), __pad(0) {}

  // cppcheck-suppress noExplicitConstructor
  eversion_t(const ceph_eversion& ce) : 
    version(ce.version),
    epoch(ce.epoch),
    __pad(0) { }

  explicit eversion_t(bufferlist& bl) : __pad(0) { decode(bl); }

  static eversion_t max() {
    eversion_t max;
    max.version -= 1;
    max.epoch -= 1;
    return max;
  }

  operator ceph_eversion() {
    ceph_eversion c;
    c.epoch = epoch;
    c.version = version;
    return c;
  }

  string get_key_name() const;

  void encode(bufferlist &bl) const {
#if defined(CEPH_LITTLE_ENDIAN)
    bl.append((char *)this, sizeof(version_t) + sizeof(epoch_t));
#else
    ::encode(version, bl);
    ::encode(epoch, bl);
#endif
  }
  void decode(bufferlist::iterator &bl) {
#if defined(CEPH_LITTLE_ENDIAN)
    bl.copy(sizeof(version_t) + sizeof(epoch_t), (char *)this);
#else
    ::decode(version, bl);
    ::decode(epoch, bl);
#endif
  }
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }
};
WRITE_CLASS_ENCODER(eversion_t)

inline bool operator==(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) && (l.version == r.version);
}
inline bool operator!=(const eversion_t& l, const eversion_t& r) {
  return (l.epoch != r.epoch) || (l.version != r.version);
}
inline bool operator<(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version < r.version):(l.epoch < r.epoch);
}
inline bool operator<=(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version <= r.version):(l.epoch <= r.epoch);
}
inline bool operator>(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version > r.version):(l.epoch > r.epoch);
}
inline bool operator>=(const eversion_t& l, const eversion_t& r) {
  return (l.epoch == r.epoch) ? (l.version >= r.version):(l.epoch >= r.epoch);
}
inline ostream& operator<<(ostream& out, const eversion_t& e) {
  return out << e.epoch << "'" << e.version;
}

/**
 * objectstore_perf_stat_t
 *
 * current perf information about the osd
 */
struct objectstore_perf_stat_t {
  // cur_op_latency is in ms since double add/sub are not associative
  uint32_t filestore_commit_latency;
  uint32_t filestore_apply_latency;

  objectstore_perf_stat_t() :
    filestore_commit_latency(0), filestore_apply_latency(0) {}

  bool operator==(const objectstore_perf_stat_t &r) const {
    return filestore_commit_latency == r.filestore_commit_latency &&
      filestore_apply_latency == r.filestore_apply_latency;
  }

  void add(const objectstore_perf_stat_t &o) {
    filestore_commit_latency += o.filestore_commit_latency;
    filestore_apply_latency += o.filestore_apply_latency;
  }
  void sub(const objectstore_perf_stat_t &o) {
    filestore_commit_latency -= o.filestore_commit_latency;
    filestore_apply_latency -= o.filestore_apply_latency;
  }
  void dump(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(std::list<objectstore_perf_stat_t*>& o);
};
WRITE_CLASS_ENCODER(objectstore_perf_stat_t)

/** osd_stat
 * aggregate stats for an osd
 */
struct osd_stat_t {
  int64_t kb, kb_used, kb_avail;
  vector<int> hb_in, hb_out;
  int32_t snap_trim_queue_len, num_snap_trimming;

  pow2_hist_t op_queue_age_hist;

  objectstore_perf_stat_t fs_perf_stat;

  osd_stat_t() : kb(0), kb_used(0), kb_avail(0),
		 snap_trim_queue_len(0), num_snap_trimming(0) {}

  void add(const osd_stat_t& o) {
    kb += o.kb;
    kb_used += o.kb_used;
    kb_avail += o.kb_avail;
    snap_trim_queue_len += o.snap_trim_queue_len;
    num_snap_trimming += o.num_snap_trimming;
    op_queue_age_hist.add(o.op_queue_age_hist);
    fs_perf_stat.add(o.fs_perf_stat);
  }
  void sub(const osd_stat_t& o) {
    kb -= o.kb;
    kb_used -= o.kb_used;
    kb_avail -= o.kb_avail;
    snap_trim_queue_len -= o.snap_trim_queue_len;
    num_snap_trimming -= o.num_snap_trimming;
    op_queue_age_hist.sub(o.op_queue_age_hist);
    fs_perf_stat.sub(o.fs_perf_stat);
  }

  void dump(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(std::list<osd_stat_t*>& o);
};
WRITE_CLASS_ENCODER(osd_stat_t)

inline bool operator==(const osd_stat_t& l, const osd_stat_t& r) {
  return l.kb == r.kb &&
    l.kb_used == r.kb_used &&
    l.kb_avail == r.kb_avail &&
    l.snap_trim_queue_len == r.snap_trim_queue_len &&
    l.num_snap_trimming == r.num_snap_trimming &&
    l.hb_in == r.hb_in &&
    l.hb_out == r.hb_out &&
    l.op_queue_age_hist == r.op_queue_age_hist &&
    l.fs_perf_stat == r.fs_perf_stat;
}
inline bool operator!=(const osd_stat_t& l, const osd_stat_t& r) {
  return !(l == r);
}



inline ostream& operator<<(ostream& out, const osd_stat_t& s) {
  return out << "osd_stat(" << kb_t(s.kb_used) << " used, "
	     << kb_t(s.kb_avail) << " avail, "
	     << kb_t(s.kb) << " total, "
	     << "peers " << s.hb_in << "/" << s.hb_out
	     << " op hist " << s.op_queue_age_hist.h
	     << ")";
}


/*
 * pg states
 */
#define PG_STATE_CREATING     (1<<0)  // creating
#define PG_STATE_ACTIVE       (1<<1)  // i am active.  (primary: replicas too)
#define PG_STATE_CLEAN        (1<<2)  // peers are complete, clean of stray replicas.
#define PG_STATE_DOWN         (1<<4)  // a needed replica is down, PG offline
#define PG_STATE_REPLAY       (1<<5)  // crashed, waiting for replay
//#define PG_STATE_STRAY      (1<<6)  // i must notify the primary i exist.
#define PG_STATE_SPLITTING    (1<<7)  // i am splitting
#define PG_STATE_SCRUBBING    (1<<8)  // scrubbing
//#define PG_STATE_SCRUBQ       (1<<9)  // queued for scrub
#define PG_STATE_DEGRADED     (1<<10) // pg contains objects with reduced redundancy
#define PG_STATE_INCONSISTENT (1<<11) // pg replicas are inconsistent (but shouldn't be)
#define PG_STATE_PEERING      (1<<12) // pg is (re)peering
#define PG_STATE_REPAIR       (1<<13) // pg should repair on next scrub
#define PG_STATE_RECOVERING   (1<<14) // pg is recovering/migrating objects
#define PG_STATE_BACKFILL_WAIT     (1<<15) // [active] reserving backfill
#define PG_STATE_INCOMPLETE   (1<<16) // incomplete content, peering failed.
#define PG_STATE_STALE        (1<<17) // our state for this pg is stale, unknown.
#define PG_STATE_REMAPPED     (1<<18) // pg is explicitly remapped to different OSDs than CRUSH
#define PG_STATE_DEEP_SCRUB   (1<<19) // deep scrub: check CRC32 on files
#define PG_STATE_BACKFILL  (1<<20) // [active] backfilling pg content
#define PG_STATE_BACKFILL_TOOFULL (1<<21) // backfill can't proceed: too full
#define PG_STATE_RECOVERY_WAIT (1<<22) // waiting for recovery reservations
#define PG_STATE_UNDERSIZED    (1<<23) // pg acting < pool size
#define PG_STATE_ACTIVATING   (1<<24) // pg is peered but not yet active
#define PG_STATE_PEERED        (1<<25) // peered, cannot go active, can recover

std::string pg_state_string(int state);
std::string pg_vector_string(const vector<int32_t> &a);
int pg_string_state(const std::string& state);


/*
 * pool_snap_info_t
 *
 * attributes for a single pool snapshot.  
 */
struct pool_snap_info_t {
  snapid_t snapid;
  utime_t stamp;
  string name;

  void dump(Formatter *f) const;
  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<pool_snap_info_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pool_snap_info_t)

inline ostream& operator<<(ostream& out, const pool_snap_info_t& si) {
  return out << si.snapid << '(' << si.name << ' ' << si.stamp << ')';
}


/*
 * pool_opts_t
 *
 * pool options.
 */

class pool_opts_t {
public:
  enum key_t {
    SCRUB_MIN_INTERVAL,
    SCRUB_MAX_INTERVAL,
    DEEP_SCRUB_INTERVAL,
    RECOVERY_PRIORITY,
    RECOVERY_OP_PRIORITY,
    SCRUB_PRIORITY
  };

  enum type_t {
    STR,
    INT,
    DOUBLE,
  };

  struct opt_desc_t {
    key_t key;
    type_t type;

    opt_desc_t(key_t k, type_t t) : key(k), type(t) {}

    bool operator==(const opt_desc_t& rhs) const {
      return key == rhs.key && type == rhs.type;
    }
  };

  typedef boost::variant<std::string,int,double> value_t;

  static bool is_opt_name(const std::string& name);
  static opt_desc_t get_opt_desc(const std::string& name);

  pool_opts_t() : opts() {}

  bool is_set(key_t key) const;

  template<typename T>
  void set(key_t key, const T &val) {
    value_t value = val;
    opts[key] = value;
  }

  template<typename T>
  bool get(key_t key, T *val) const {
    opts_t::const_iterator i = opts.find(key);
    if (i == opts.end()) {
      return false;
    }
    *val = boost::get<T>(i->second);
    return true;
  }

  const value_t& get(key_t key) const;

  bool unset(key_t key);

  void dump(const std::string& name, Formatter *f) const;

  void dump(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);

private:
  typedef std::map<key_t, value_t> opts_t;
  opts_t opts;

  friend ostream& operator<<(ostream& out, const pool_opts_t& opts);
};
WRITE_CLASS_ENCODER(pool_opts_t)

/*
 * pg_pool
 */
struct pg_pool_t {
  enum {
    TYPE_REPLICATED = 1,     // replication
    //TYPE_RAID4 = 2,   // raid4 (never implemented)
    TYPE_ERASURE = 3,      // erasure-coded
  };
  static const char *get_type_name(int t) {
    switch (t) {
    case TYPE_REPLICATED: return "replicated";
      //case TYPE_RAID4: return "raid4";
    case TYPE_ERASURE: return "erasure";
    default: return "???";
    }
  }
  const char *get_type_name() const {
    return get_type_name(type);
  }
  static const char* get_default_type() {
    return "replicated";
  }

  enum {
    FLAG_HASHPSPOOL = 1<<0, // hash pg seed and pool together (instead of adding)
    FLAG_FULL       = 1<<1, // pool is full
    FLAG_DEBUG_FAKE_EC_POOL = 1<<2, // require ReplicatedPG to act like an EC pg
    FLAG_INCOMPLETE_CLONES = 1<<3, // may have incomplete clones (bc we are/were an overlay)
    FLAG_NODELETE = 1<<4, // pool can't be deleted
    FLAG_NOPGCHANGE = 1<<5, // pool's pg and pgp num can't be changed
    FLAG_NOSIZECHANGE = 1<<6, // pool's size and min size can't be changed
    FLAG_WRITE_FADVISE_DONTNEED = 1<<7, // write mode with LIBRADOS_OP_FLAG_FADVISE_DONTNEED
    FLAG_NOSCRUB = 1<<8, // block periodic scrub
    FLAG_NODEEP_SCRUB = 1<<9, // block periodic deep-scrub
  };

  static const char *get_flag_name(int f) {
    switch (f) {
    case FLAG_HASHPSPOOL: return "hashpspool";
    case FLAG_FULL: return "full";
    case FLAG_DEBUG_FAKE_EC_POOL: return "require_local_rollback";
    case FLAG_INCOMPLETE_CLONES: return "incomplete_clones";
    case FLAG_NODELETE: return "nodelete";
    case FLAG_NOPGCHANGE: return "nopgchange";
    case FLAG_NOSIZECHANGE: return "nosizechange";
    case FLAG_WRITE_FADVISE_DONTNEED: return "write_fadvise_dontneed";
    case FLAG_NOSCRUB: return "noscrub";
    case FLAG_NODEEP_SCRUB: return "nodeep-scrub";
    default: return "???";
    }
  }
  static string get_flags_string(uint64_t f) {
    string s;
    for (unsigned n=0; f && n<64; ++n) {
      if (f & (1ull << n)) {
	if (s.length())
	  s += ",";
	s += get_flag_name(1ull << n);
      }
    }
    return s;
  }
  string get_flags_string() const {
    return get_flags_string(flags);
  }
  static uint64_t get_flag_by_name(const string& name) {
    if (name == "hashpspool")
      return FLAG_HASHPSPOOL;
    if (name == "full")
      return FLAG_FULL;
    if (name == "require_local_rollback")
      return FLAG_DEBUG_FAKE_EC_POOL;
    if (name == "incomplete_clones")
      return FLAG_INCOMPLETE_CLONES;
    if (name == "nodelete")
      return FLAG_NODELETE;
    if (name == "nopgchange")
      return FLAG_NOPGCHANGE;
    if (name == "nosizechange")
      return FLAG_NOSIZECHANGE;
    if (name == "write_fadvise_dontneed")
      return FLAG_WRITE_FADVISE_DONTNEED;
    if (name == "noscrub")
      return FLAG_NOSCRUB;
    if (name == "nodeep-scrub")
      return FLAG_NODEEP_SCRUB;
    return 0;
  }

  /// converts the acting/up vector to a set of pg shards
  void convert_to_pg_shards(const vector<int> &from, set<pg_shard_t>* to) const;

  typedef enum {
    CACHEMODE_NONE = 0,                  ///< no caching
    CACHEMODE_WRITEBACK = 1,             ///< write to cache, flush later
    CACHEMODE_FORWARD = 2,               ///< forward if not in cache
    CACHEMODE_READONLY = 3,              ///< handle reads, forward writes [not strongly consistent]
    CACHEMODE_READFORWARD = 4,           ///< forward reads, write to cache flush later
    CACHEMODE_READPROXY = 5,             ///< proxy reads, write to cache flush later
    CACHEMODE_PROXY = 6,                 ///< proxy if not in cache
  } cache_mode_t;
  static const char *get_cache_mode_name(cache_mode_t m) {
    switch (m) {
    case CACHEMODE_NONE: return "none";
    case CACHEMODE_WRITEBACK: return "writeback";
    case CACHEMODE_FORWARD: return "forward";
    case CACHEMODE_READONLY: return "readonly";
    case CACHEMODE_READFORWARD: return "readforward";
    case CACHEMODE_READPROXY: return "readproxy";
    case CACHEMODE_PROXY: return "proxy";
    default: return "unknown";
    }
  }
  static cache_mode_t get_cache_mode_from_str(const string& s) {
    if (s == "none")
      return CACHEMODE_NONE;
    if (s == "writeback")
      return CACHEMODE_WRITEBACK;
    if (s == "forward")
      return CACHEMODE_FORWARD;
    if (s == "readonly")
      return CACHEMODE_READONLY;
    if (s == "readforward")
      return CACHEMODE_READFORWARD;
    if (s == "readproxy")
      return CACHEMODE_READPROXY;
    if (s == "proxy")
      return CACHEMODE_PROXY;
    return (cache_mode_t)-1;
  }
  const char *get_cache_mode_name() const {
    return get_cache_mode_name(cache_mode);
  }
  bool cache_mode_requires_hit_set() const {
    switch (cache_mode) {
    case CACHEMODE_NONE:
    case CACHEMODE_FORWARD:
    case CACHEMODE_READONLY:
    case CACHEMODE_PROXY:
      return false;
    case CACHEMODE_WRITEBACK:
    case CACHEMODE_READFORWARD:
    case CACHEMODE_READPROXY:
      return true;
    default:
      assert(0 == "implement me");
    }
  }

  uint64_t flags;           ///< FLAG_*
  __u8 type;                ///< TYPE_*
  __u8 size, min_size;      ///< number of osds in each pg
  __u8 crush_ruleset;       ///< crush placement rule set
  __u8 object_hash;         ///< hash mapping object name to ps
private:
  __u32 pg_num, pgp_num;    ///< number of pgs


public:
  map<string,string> properties;  ///< OBSOLETE
  string erasure_code_profile; ///< name of the erasure code profile in OSDMap
  epoch_t last_change;      ///< most recent epoch changed, exclusing snapshot changes
  epoch_t last_force_op_resend; ///< last epoch that forced clients to resend
  snapid_t snap_seq;        ///< seq for per-pool snapshot
  epoch_t snap_epoch;       ///< osdmap epoch of last snap
  uint64_t auid;            ///< who owns the pg
  __u32 crash_replay_interval; ///< seconds to allow clients to replay ACKed but unCOMMITted requests

  uint64_t quota_max_bytes; ///< maximum number of bytes for this pool
  uint64_t quota_max_objects; ///< maximum number of objects for this pool

  /*
   * Pool snaps (global to this pool).  These define a SnapContext for
   * the pool, unless the client manually specifies an alternate
   * context.
   */
  map<snapid_t, pool_snap_info_t> snaps;
  /*
   * Alternatively, if we are defining non-pool snaps (e.g. via the
   * Ceph MDS), we must track @removed_snaps (since @snaps is not
   * used).  Snaps and removed_snaps are to be used exclusive of each
   * other!
   */
  interval_set<snapid_t> removed_snaps;

  unsigned pg_num_mask, pgp_num_mask;

  set<uint64_t> tiers;      ///< pools that are tiers of us
  int64_t tier_of;         ///< pool for which we are a tier
  // Note that write wins for read+write ops
  int64_t read_tier;       ///< pool/tier for objecter to direct reads to
  int64_t write_tier;      ///< pool/tier for objecter to direct writes to
  cache_mode_t cache_mode;  ///< cache pool mode

  bool is_tier() const { return tier_of >= 0; }
  bool has_tiers() const { return !tiers.empty(); }
  void clear_tier() {
    tier_of = -1;
    clear_read_tier();
    clear_write_tier();
    clear_tier_tunables();
  }
  bool has_read_tier() const { return read_tier >= 0; }
  void clear_read_tier() { read_tier = -1; }
  bool has_write_tier() const { return write_tier >= 0; }
  void clear_write_tier() { write_tier = -1; }
  void clear_tier_tunables() {
    if (cache_mode != CACHEMODE_NONE)
      flags |= FLAG_INCOMPLETE_CLONES;
    cache_mode = CACHEMODE_NONE;

    target_max_bytes = 0;
    target_max_objects = 0;
    cache_target_dirty_ratio_micro = 0;
    cache_target_dirty_high_ratio_micro = 0;
    cache_target_full_ratio_micro = 0;
    hit_set_params = HitSet::Params();
    hit_set_period = 0;
    hit_set_count = 0;
    hit_set_grade_decay_rate = 0;
    hit_set_search_last_n = 0;
    grade_table.resize(0);
  }

  uint64_t target_max_bytes;   ///< tiering: target max pool size
  uint64_t target_max_objects; ///< tiering: target max pool size

  uint32_t cache_target_dirty_ratio_micro; ///< cache: fraction of target to leave dirty
  uint32_t cache_target_dirty_high_ratio_micro; ///<cache: fraction of  target to flush with high speed
  uint32_t cache_target_full_ratio_micro;  ///< cache: fraction of target to fill before we evict in earnest

  uint32_t cache_min_flush_age;  ///< minimum age (seconds) before we can flush
  uint32_t cache_min_evict_age;  ///< minimum age (seconds) before we can evict

  HitSet::Params hit_set_params; ///< The HitSet params to use on this pool
  uint32_t hit_set_period;      ///< periodicity of HitSet segments (seconds)
  uint32_t hit_set_count;       ///< number of periods to retain
  bool use_gmt_hitset;	        ///< use gmt to name the hitset archive object
  uint32_t min_read_recency_for_promote;   ///< minimum number of HitSet to check before promote on read
  uint32_t min_write_recency_for_promote;  ///< minimum number of HitSet to check before promote on write
  uint32_t hit_set_grade_decay_rate;   ///< current hit_set has highest priority on objects
                                       ///temperature count,the follow hit_set's priority decay 
                                       ///by this params than pre hit_set
  uint32_t hit_set_search_last_n;   ///<accumulate atmost N hit_sets for temperature

  uint32_t stripe_width;        ///< erasure coded stripe size in bytes

  uint64_t expected_num_objects; ///< expected number of objects on this pool, a value of 0 indicates
                                 ///< user does not specify any expected value
  bool fast_read;            ///< whether turn on fast read on the pool or not

  pool_opts_t opts; ///< options

private:
  vector<uint32_t> grade_table;

public:
  uint32_t get_grade(unsigned i) const {
    if (grade_table.size() <= i)
      return 0;
    return grade_table[i];
  }
  void calc_grade_table() {
    unsigned v = 1000000;
    grade_table.resize(hit_set_count);
    for (unsigned i = 0; i < hit_set_count; i++) {
      v = v * (1 - (hit_set_grade_decay_rate / 100.0));
      grade_table[i] = v;
    }
  }

  pg_pool_t()
    : flags(0), type(0), size(0), min_size(0),
      crush_ruleset(0), object_hash(0),
      pg_num(0), pgp_num(0),
      last_change(0),
      last_force_op_resend(0),
      snap_seq(0), snap_epoch(0),
      auid(0),
      crash_replay_interval(0),
      quota_max_bytes(0), quota_max_objects(0),
      pg_num_mask(0), pgp_num_mask(0),
      tier_of(-1), read_tier(-1), write_tier(-1),
      cache_mode(CACHEMODE_NONE),
      target_max_bytes(0), target_max_objects(0),
      cache_target_dirty_ratio_micro(0),
      cache_target_dirty_high_ratio_micro(0),
      cache_target_full_ratio_micro(0),
      cache_min_flush_age(0),
      cache_min_evict_age(0),
      hit_set_params(),
      hit_set_period(0),
      hit_set_count(0),
      use_gmt_hitset(true),
      min_read_recency_for_promote(0),
      min_write_recency_for_promote(0),
      hit_set_grade_decay_rate(0),
      hit_set_search_last_n(0),
      stripe_width(0),
      expected_num_objects(0),
      fast_read(false),
      opts()
  { }

  void dump(Formatter *f) const;

  uint64_t get_flags() const { return flags; }
  bool has_flag(uint64_t f) const { return flags & f; }
  void set_flag(uint64_t f) { flags |= f; }
  void unset_flag(uint64_t f) { flags &= ~f; }

  /// This method will later return true for ec pools as well
  bool ec_pool() const {
    return type == TYPE_ERASURE;
  }
  bool require_rollback() const {
    return ec_pool() || flags & FLAG_DEBUG_FAKE_EC_POOL;
  }

  /// true if incomplete clones may be present
  bool allow_incomplete_clones() const {
    return cache_mode != CACHEMODE_NONE || has_flag(FLAG_INCOMPLETE_CLONES);
  }

  unsigned get_type() const { return type; }
  unsigned get_size() const { return size; }
  unsigned get_min_size() const { return min_size; }
  int get_crush_ruleset() const { return crush_ruleset; }
  int get_object_hash() const { return object_hash; }
  const char *get_object_hash_name() const {
    return ceph_str_hash_name(get_object_hash());
  }
  epoch_t get_last_change() const { return last_change; }
  epoch_t get_last_force_op_resend() const { return last_force_op_resend; }
  epoch_t get_snap_epoch() const { return snap_epoch; }
  snapid_t get_snap_seq() const { return snap_seq; }
  uint64_t get_auid() const { return auid; }
  unsigned get_crash_replay_interval() const { return crash_replay_interval; }

  void set_snap_seq(snapid_t s) { snap_seq = s; }
  void set_snap_epoch(epoch_t e) { snap_epoch = e; }

  void set_stripe_width(uint32_t s) { stripe_width = s; }
  uint32_t get_stripe_width() const { return stripe_width; }

  bool is_replicated()   const { return get_type() == TYPE_REPLICATED; }
  bool is_erasure() const { return get_type() == TYPE_ERASURE; }

  bool supports_omap() const {
    return !(get_type() == TYPE_ERASURE || has_flag(FLAG_DEBUG_FAKE_EC_POOL));
  }

  bool requires_aligned_append() const { return is_erasure(); }
  uint64_t required_alignment() const { return stripe_width; }

  bool can_shift_osds() const {
    switch (get_type()) {
    case TYPE_REPLICATED:
      return true;
    case TYPE_ERASURE:
      return false;
    default:
      assert(0 == "unhandled pool type");
    }
  }

  unsigned get_pg_num() const { return pg_num; }
  unsigned get_pgp_num() const { return pgp_num; }

  unsigned get_pg_num_mask() const { return pg_num_mask; }
  unsigned get_pgp_num_mask() const { return pgp_num_mask; }

  // if pg_num is not a multiple of two, pgs are not equally sized.
  // return, for a given pg, the fraction (denominator) of the total
  // pool size that it represents.
  unsigned get_pg_num_divisor(pg_t pgid) const;

  void set_pg_num(int p) {
    pg_num = p;
    calc_pg_masks();
  }
  void set_pgp_num(int p) {
    pgp_num = p;
    calc_pg_masks();
  }

  void set_quota_max_bytes(uint64_t m) {
    quota_max_bytes = m;
  }
  uint64_t get_quota_max_bytes() {
    return quota_max_bytes;
  }

  void set_quota_max_objects(uint64_t m) {
    quota_max_objects = m;
  }
  uint64_t get_quota_max_objects() {
    return quota_max_objects;
  }

  static int calc_bits_of(int t);
  void calc_pg_masks();

  /*
   * we have two snap modes:
   *  - pool global snaps
   *    - snap existence/non-existence defined by snaps[] and snap_seq
   *  - user managed snaps
   *    - removal governed by removed_snaps
   *
   * we know which mode we're using based on whether removed_snaps is empty.
   * If nothing has been created, both functions report false.
   */
  bool is_pool_snaps_mode() const;
  bool is_unmanaged_snaps_mode() const;
  bool is_removed_snap(snapid_t s) const;

  /*
   * build set of known-removed sets from either pool snaps or
   * explicit removed_snaps set.
   */
  void build_removed_snaps(interval_set<snapid_t>& rs) const;
  snapid_t snap_exists(const char *s) const;
  void add_snap(const char *n, utime_t stamp);
  void add_unmanaged_snap(uint64_t& snapid);
  void remove_snap(snapid_t s);
  void remove_unmanaged_snap(snapid_t s);

  SnapContext get_snap_context() const;

  /// hash a object name+namespace key to a hash position
  uint32_t hash_key(const string& key, const string& ns) const;

  /// round a hash position down to a pg num
  uint32_t raw_hash_to_pg(uint32_t v) const;

  /*
   * map a raw pg (with full precision ps) into an actual pg, for storage
   */
  pg_t raw_pg_to_pg(pg_t pg) const;
  
  /*
   * map raw pg (full precision ps) into a placement seed.  include
   * pool id in that value so that different pools don't use the same
   * seeds.
   */
  ps_t raw_pg_to_pps(pg_t pg) const;

  /// choose a random hash position within a pg
  uint32_t get_random_pg_position(pg_t pgid, uint32_t seed) const;

  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& bl);

  static void generate_test_instances(list<pg_pool_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pg_pool_t)

ostream& operator<<(ostream& out, const pg_pool_t& p);


/**
 * a summation of object stats
 *
 * This is just a container for object stats; we don't know what for.
 *
 * If you add members in object_stat_sum_t, you should make sure there are
 * not padding among these members.
 * You should also modify the padding_check function.

 */
struct object_stat_sum_t {
  /**************************************************************************
   * WARNING: be sure to update operator==, floor, and split when
   * adding/removing fields!
   **************************************************************************/
  int64_t num_bytes;    // in bytes
  int64_t num_objects;
  int64_t num_object_clones;
  int64_t num_object_copies;  // num_objects * num_replicas
  int64_t num_objects_missing_on_primary;
  int64_t num_objects_degraded;
  int64_t num_objects_unfound;
  int64_t num_rd;
  int64_t num_rd_kb;
  int64_t num_wr;
  int64_t num_wr_kb;
  int64_t num_scrub_errors;	// total deep and shallow scrub errors
  int64_t num_objects_recovered;
  int64_t num_bytes_recovered;
  int64_t num_keys_recovered;
  int64_t num_shallow_scrub_errors;
  int64_t num_deep_scrub_errors;
  int64_t num_objects_dirty;
  int64_t num_whiteouts;
  int64_t num_objects_omap;
  int64_t num_objects_hit_set_archive;
  int64_t num_objects_misplaced;
  int64_t num_bytes_hit_set_archive;
  int64_t num_flush;
  int64_t num_flush_kb;
  int64_t num_evict;
  int64_t num_evict_kb;
  int64_t num_promote;
  int32_t num_flush_mode_high;  // 1 when in high flush mode, otherwise 0
  int32_t num_flush_mode_low;   // 1 when in low flush mode, otherwise 0
  int32_t num_evict_mode_some;  // 1 when in evict some mode, otherwise 0
  int32_t num_evict_mode_full;  // 1 when in evict full mode, otherwise 0
  int64_t num_objects_pinned;
  int64_t num_objects_missing;

  object_stat_sum_t()
    : num_bytes(0),
      num_objects(0), num_object_clones(0), num_object_copies(0),
      num_objects_missing_on_primary(0), num_objects_degraded(0),
      num_objects_unfound(0),
      num_rd(0), num_rd_kb(0), num_wr(0), num_wr_kb(0),
      num_scrub_errors(0),
      num_objects_recovered(0),
      num_bytes_recovered(0),
      num_keys_recovered(0),
      num_shallow_scrub_errors(0),
      num_deep_scrub_errors(0),
      num_objects_dirty(0),
      num_whiteouts(0),
      num_objects_omap(0),
      num_objects_hit_set_archive(0),
      num_objects_misplaced(0),
      num_bytes_hit_set_archive(0),
      num_flush(0),
      num_flush_kb(0),
      num_evict(0),
      num_evict_kb(0),
      num_promote(0),
      num_flush_mode_high(0), num_flush_mode_low(0),
      num_evict_mode_some(0), num_evict_mode_full(0),
      num_objects_pinned(0),
      num_objects_missing(0)
  {}

  void floor(int64_t f) {
#define FLOOR(x) if (x < f) x = f
    FLOOR(num_bytes);
    FLOOR(num_objects);
    FLOOR(num_object_clones);
    FLOOR(num_object_copies);
    FLOOR(num_objects_missing_on_primary);
    FLOOR(num_objects_missing);
    FLOOR(num_objects_degraded);
    FLOOR(num_objects_misplaced);
    FLOOR(num_objects_unfound);
    FLOOR(num_rd);
    FLOOR(num_rd_kb);
    FLOOR(num_wr);
    FLOOR(num_wr_kb);
    FLOOR(num_scrub_errors);
    FLOOR(num_shallow_scrub_errors);
    FLOOR(num_deep_scrub_errors);
    FLOOR(num_objects_recovered);
    FLOOR(num_bytes_recovered);
    FLOOR(num_keys_recovered);
    FLOOR(num_objects_dirty);
    FLOOR(num_whiteouts);
    FLOOR(num_objects_omap);
    FLOOR(num_objects_hit_set_archive);
    FLOOR(num_bytes_hit_set_archive);
    FLOOR(num_flush);
    FLOOR(num_flush_kb);
    FLOOR(num_evict);
    FLOOR(num_evict_kb);
    FLOOR(num_promote);
    FLOOR(num_flush_mode_high);
    FLOOR(num_flush_mode_low);
    FLOOR(num_evict_mode_some);
    FLOOR(num_evict_mode_full);
    FLOOR(num_objects_pinned);
#undef FLOOR
  }

  void split(vector<object_stat_sum_t> &out) const {
#define SPLIT(PARAM)                            \
    for (unsigned i = 0; i < out.size(); ++i) { \
      out[i].PARAM = PARAM / out.size();        \
      if (i < (PARAM % out.size())) {           \
	out[i].PARAM++;                         \
      }                                         \
    }                                           \

    SPLIT(num_bytes);
    SPLIT(num_objects);
    SPLIT(num_object_clones);
    SPLIT(num_object_copies);
    SPLIT(num_objects_missing_on_primary);
    SPLIT(num_objects_missing);
    SPLIT(num_objects_degraded);
    SPLIT(num_objects_misplaced);
    SPLIT(num_objects_unfound);
    SPLIT(num_rd);
    SPLIT(num_rd_kb);
    SPLIT(num_wr);
    SPLIT(num_wr_kb);
    SPLIT(num_scrub_errors);
    SPLIT(num_shallow_scrub_errors);
    SPLIT(num_deep_scrub_errors);
    SPLIT(num_objects_recovered);
    SPLIT(num_bytes_recovered);
    SPLIT(num_keys_recovered);
    SPLIT(num_objects_dirty);
    SPLIT(num_whiteouts);
    SPLIT(num_objects_omap);
    SPLIT(num_objects_hit_set_archive);
    SPLIT(num_bytes_hit_set_archive);
    SPLIT(num_flush);
    SPLIT(num_flush_kb);
    SPLIT(num_evict);
    SPLIT(num_evict_kb);
    SPLIT(num_promote);
    SPLIT(num_flush_mode_high);
    SPLIT(num_flush_mode_low);
    SPLIT(num_evict_mode_some);
    SPLIT(num_evict_mode_full);
    SPLIT(num_objects_pinned);
#undef SPLIT
  }

  void clear() {
    memset(this, 0, sizeof(*this));
  }

  void calc_copies(int nrep) {
    num_object_copies = nrep * num_objects;
  }

  bool is_zero() const {
    return mem_is_zero((char*)this, sizeof(*this));
  }

  void add(const object_stat_sum_t& o);
  void sub(const object_stat_sum_t& o);

  void dump(Formatter *f) const;
  void padding_check() {
    static_assert(
      sizeof(object_stat_sum_t) ==
        sizeof(num_bytes) +
        sizeof(num_objects) +
        sizeof(num_object_clones) +
        sizeof(num_object_copies) +
        sizeof(num_objects_missing_on_primary) +
        sizeof(num_objects_degraded) +
        sizeof(num_objects_unfound) +
        sizeof(num_rd) +
        sizeof(num_rd_kb) +
        sizeof(num_wr) +
        sizeof(num_wr_kb) +
        sizeof(num_scrub_errors) +
        sizeof(num_objects_recovered) +
        sizeof(num_bytes_recovered) +
        sizeof(num_keys_recovered) +
        sizeof(num_shallow_scrub_errors) +
        sizeof(num_deep_scrub_errors) +
        sizeof(num_objects_dirty) +
        sizeof(num_whiteouts) +
        sizeof(num_objects_omap) +
        sizeof(num_objects_hit_set_archive) +
        sizeof(num_objects_misplaced) +
        sizeof(num_bytes_hit_set_archive) +
        sizeof(num_flush) +
        sizeof(num_flush_kb) +
        sizeof(num_evict) +
        sizeof(num_evict_kb) +
        sizeof(num_promote) +
        sizeof(num_flush_mode_high) +
        sizeof(num_flush_mode_low) +
        sizeof(num_evict_mode_some) +
        sizeof(num_evict_mode_full) +
        sizeof(num_objects_pinned) +
        sizeof(num_objects_missing)
      ,
      "object_stat_sum_t have padding");
  }
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<object_stat_sum_t*>& o);
};
WRITE_CLASS_ENCODER(object_stat_sum_t)

bool operator==(const object_stat_sum_t& l, const object_stat_sum_t& r);

/**
 * a collection of object stat sums
 *
 * This is a collection of stat sums over different categories.
 */
struct object_stat_collection_t {
  /**************************************************************************
   * WARNING: be sure to update the operator== when adding/removing fields! *
   **************************************************************************/
  object_stat_sum_t sum;

  void calc_copies(int nrep) {
    sum.calc_copies(nrep);
  }

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<object_stat_collection_t*>& o);

  bool is_zero() const {
    return sum.is_zero();
  }

  void clear() {
    sum.clear();
  }

  void floor(int64_t f) {
    sum.floor(f);
  }

  void add(const object_stat_sum_t& o) {
    sum.add(o);
  }

  void add(const object_stat_collection_t& o) {
    sum.add(o.sum);
  }
  void sub(const object_stat_collection_t& o) {
    sum.sub(o.sum);
  }
};
WRITE_CLASS_ENCODER(object_stat_collection_t)

inline bool operator==(const object_stat_collection_t& l,
		       const object_stat_collection_t& r) {
  return l.sum == r.sum;
}


/** pg_stat
 * aggregate stats for a single PG.
 */
struct pg_stat_t {
  /**************************************************************************
   * WARNING: be sure to update the operator== when adding/removing fields! *
   **************************************************************************/
  eversion_t version;
  version_t reported_seq;  // sequence number
  epoch_t reported_epoch;  // epoch of this report
  __u32 state;
  utime_t last_fresh;   // last reported
  utime_t last_change;  // new state != previous state
  utime_t last_active;  // state & PG_STATE_ACTIVE
  utime_t last_peered;  // state & PG_STATE_ACTIVE || state & PG_STATE_PEERED
  utime_t last_clean;   // state & PG_STATE_CLEAN
  utime_t last_unstale; // (state & PG_STATE_STALE) == 0
  utime_t last_undegraded; // (state & PG_STATE_DEGRADED) == 0
  utime_t last_fullsized; // (state & PG_STATE_UNDERSIZED) == 0

  eversion_t log_start;         // (log_start,version]
  eversion_t ondisk_log_start;  // there may be more on disk

  epoch_t created;
  epoch_t last_epoch_clean;
  pg_t parent;
  __u32 parent_split_bits;

  eversion_t last_scrub;
  eversion_t last_deep_scrub;
  utime_t last_scrub_stamp;
  utime_t last_deep_scrub_stamp;
  utime_t last_clean_scrub_stamp;

  object_stat_collection_t stats;

  int64_t log_size;
  int64_t ondisk_log_size;    // >= active_log_size

  vector<int32_t> up, acting;
  epoch_t mapping_epoch;

  vector<int32_t> blocked_by;  ///< osds on which the pg is blocked

  utime_t last_became_active;
  utime_t last_became_peered;

  /// up, acting primaries
  int32_t up_primary;
  int32_t acting_primary;

  bool stats_invalid:1;
  /// true if num_objects_dirty is not accurate (because it was not
  /// maintained starting from pool creation)
  bool dirty_stats_invalid:1;
  bool omap_stats_invalid:1;
  bool hitset_stats_invalid:1;
  bool hitset_bytes_stats_invalid:1;
  bool pin_stats_invalid:1;

  pg_stat_t()
    : reported_seq(0),
      reported_epoch(0),
      state(0),
      created(0), last_epoch_clean(0),
      parent_split_bits(0),
      log_size(0), ondisk_log_size(0),
      mapping_epoch(0),
      up_primary(-1),
      acting_primary(-1),
      stats_invalid(false),
      dirty_stats_invalid(false),
      omap_stats_invalid(false),
      hitset_stats_invalid(false),
      hitset_bytes_stats_invalid(false),
      pin_stats_invalid(false)
  { }

  epoch_t get_effective_last_epoch_clean() const {
    if (state & PG_STATE_CLEAN) {
      // we are clean as of this report, and should thus take the
      // reported epoch
      return reported_epoch;
    } else {
      return last_epoch_clean;
    }
  }

  pair<epoch_t, version_t> get_version_pair() const {
    return make_pair(reported_epoch, reported_seq);
  }

  void floor(int64_t f) {
    stats.floor(f);
    if (log_size < f)
      log_size = f;
    if (ondisk_log_size < f)
      ondisk_log_size = f;
  }

  void add(const pg_stat_t& o) {
    stats.add(o.stats);
    log_size += o.log_size;
    ondisk_log_size += o.ondisk_log_size;
  }
  void sub(const pg_stat_t& o) {
    stats.sub(o.stats);
    log_size -= o.log_size;
    ondisk_log_size -= o.ondisk_log_size;
  }

  bool is_acting_osd(int32_t osd, bool primary) const;
  void dump(Formatter *f) const;
  void dump_brief(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(list<pg_stat_t*>& o);
};
WRITE_CLASS_ENCODER(pg_stat_t)

bool operator==(const pg_stat_t& l, const pg_stat_t& r);

/*
 * summation over an entire pool
 */
struct pool_stat_t {
  object_stat_collection_t stats;
  int64_t log_size;
  int64_t ondisk_log_size;    // >= active_log_size
  int32_t up;       ///< number of up replicas or shards
  int32_t acting;   ///< number of acting replicas or shards

  pool_stat_t() : log_size(0), ondisk_log_size(0), up(0), acting(0)
  { }

  void floor(int64_t f) {
    stats.floor(f);
    if (log_size < f)
      log_size = f;
    if (ondisk_log_size < f)
      ondisk_log_size = f;
    if (up < f)
      up = f;
    if (acting < f)
      acting = f;
  }

  void add(const pg_stat_t& o) {
    stats.add(o.stats);
    log_size += o.log_size;
    ondisk_log_size += o.ondisk_log_size;
    up += o.up.size();
    acting += o.acting.size();
  }
  void sub(const pg_stat_t& o) {
    stats.sub(o.stats);
    log_size -= o.log_size;
    ondisk_log_size -= o.ondisk_log_size;
    up -= o.up.size();
    acting -= o.acting.size();
  }

  bool is_zero() const {
    return (stats.is_zero() &&
	    log_size == 0 &&
	    ondisk_log_size == 0 &&
	    up == 0 &&
	    acting == 0);
  }

  void dump(Formatter *f) const;
  void encode(bufferlist &bl, uint64_t features) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(list<pool_stat_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pool_stat_t)


// -----------------------------------------

/**
 * pg_hit_set_info_t - information about a single recorded HitSet
 *
 * Track basic metadata about a HitSet, like the nubmer of insertions
 * and the time range it covers.
 */
struct pg_hit_set_info_t {
  utime_t begin, end;   ///< time interval
  eversion_t version;   ///< version this HitSet object was written
  bool using_gmt;	///< use gmt for creating the hit_set archive object name
  explicit pg_hit_set_info_t(bool using_gmt = true)
    : using_gmt(using_gmt) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_hit_set_info_t*>& o);
};
WRITE_CLASS_ENCODER(pg_hit_set_info_t)

/**
 * pg_hit_set_history_t - information about a history of hitsets
 *
 * Include information about the currently accumulating hit set as well
 * as archived/historical ones.
 */
struct pg_hit_set_history_t {
  eversion_t current_last_update;  ///< last version inserted into current set
  list<pg_hit_set_info_t> history; ///< archived sets, sorted oldest -> newest

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_hit_set_history_t*>& o);
};
WRITE_CLASS_ENCODER(pg_hit_set_history_t)


// -----------------------------------------

/**
 * pg_history_t - information about recent pg peering/mapping history
 *
 * This is aggressively shared between OSDs to bound the amount of past
 * history they need to worry about.
 */
struct pg_history_t {
  epoch_t epoch_created;       // epoch in which PG was created
  epoch_t last_epoch_started;  // lower bound on last epoch started (anywhere, not necessarily locally)
  epoch_t last_epoch_clean;    // lower bound on last epoch the PG was completely clean.
  epoch_t last_epoch_split;    // as parent
  epoch_t last_epoch_marked_full;  // pool or cluster
  
  /**
   * In the event of a map discontinuity, same_*_since may reflect the first
   * map the osd has seen in the new map sequence rather than the actual start
   * of the interval.  This is ok since a discontinuity at epoch e means there
   * must have been a clean interval between e and now and that we cannot be
   * in the active set during the interval containing e.
   */
  epoch_t same_up_since;       // same acting set since
  epoch_t same_interval_since;   // same acting AND up set since
  epoch_t same_primary_since;  // same primary at least back through this epoch.

  eversion_t last_scrub;
  eversion_t last_deep_scrub;
  utime_t last_scrub_stamp;
  utime_t last_deep_scrub_stamp;
  utime_t last_clean_scrub_stamp;
  utime_t last_flush_evict_stamp; // last cache tier flush/evict timestamp

  pg_history_t()
    : epoch_created(0),
      last_epoch_started(0), last_epoch_clean(0), last_epoch_split(0),
      last_epoch_marked_full(0),
      same_up_since(0), same_interval_since(0), same_primary_since(0),
      last_flush_evict_stamp(utime_t()) {}
  
  bool merge(const pg_history_t &other) {
    // Here, we only update the fields which cannot be calculated from the OSDmap.
    bool modified = false;
    if (epoch_created < other.epoch_created) {
      epoch_created = other.epoch_created;
      modified = true;
    }
    if (last_epoch_started < other.last_epoch_started) {
      last_epoch_started = other.last_epoch_started;
      modified = true;
    }
    if (last_epoch_clean < other.last_epoch_clean) {
      last_epoch_clean = other.last_epoch_clean;
      modified = true;
    }
    if (last_epoch_split < other.last_epoch_split) {
      last_epoch_split = other.last_epoch_split; 
      modified = true;
    }
    if (last_epoch_marked_full < other.last_epoch_marked_full) {
      last_epoch_marked_full = other.last_epoch_marked_full;
      modified = true;
    }
    if (other.last_scrub > last_scrub) {
      last_scrub = other.last_scrub;
      modified = true;
    }
    if (other.last_scrub_stamp > last_scrub_stamp) {
      last_scrub_stamp = other.last_scrub_stamp;
      modified = true;
    }
    if (other.last_deep_scrub > last_deep_scrub) {
      last_deep_scrub = other.last_deep_scrub;
      modified = true;
    }
    if (other.last_deep_scrub_stamp > last_deep_scrub_stamp) {
      last_deep_scrub_stamp = other.last_deep_scrub_stamp;
      modified = true;
    }
    if (other.last_clean_scrub_stamp > last_clean_scrub_stamp) {
      last_clean_scrub_stamp = other.last_clean_scrub_stamp;
      modified = true;
    }
    if (other.last_flush_evict_stamp > last_flush_evict_stamp) {
      last_flush_evict_stamp = other.last_flush_evict_stamp;
      modified = true;
    }
    return modified;
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_history_t*>& o);
};
WRITE_CLASS_ENCODER(pg_history_t)

inline ostream& operator<<(ostream& out, const pg_history_t& h) {
  return out << "ec=" << h.epoch_created
	     << " les/c/f " << h.last_epoch_started << "/" << h.last_epoch_clean
	     << "/" << h.last_epoch_marked_full
	     << " " << h.same_up_since << "/" << h.same_interval_since << "/" << h.same_primary_since;
}


/**
 * pg_info_t - summary of PG statistics.
 *
 * some notes: 
 *  - last_complete implies we have all objects that existed as of that
 *    stamp, OR a newer object, OR have already applied a later delete.
 *  - if last_complete >= log.bottom, then we know pg contents thru log.head.
 *    otherwise, we have no idea what the pg is supposed to contain.
 */
struct pg_info_t {
  spg_t pgid;
  eversion_t last_update;      ///< last object version applied to store.
  eversion_t last_complete;    ///< last version pg was complete through.
  epoch_t last_epoch_started;  ///< last epoch at which this pg started on this osd
  
  version_t last_user_version; ///< last user object version applied to store

  eversion_t log_tail;         ///< oldest log entry.

  hobject_t last_backfill;     ///< objects >= this and < last_complete may be missing
  bool last_backfill_bitwise;  ///< true if last_backfill reflects a bitwise (vs nibblewise) sort

  interval_set<snapid_t> purged_snaps;

  pg_stat_t stats;

  pg_history_t history;
  pg_hit_set_history_t hit_set;

  pg_info_t()
    : last_epoch_started(0), last_user_version(0),
      last_backfill(hobject_t::get_max()),
      last_backfill_bitwise(false)
  { }
  // cppcheck-suppress noExplicitConstructor
  pg_info_t(spg_t p)
    : pgid(p),
      last_epoch_started(0), last_user_version(0),
      last_backfill(hobject_t::get_max()),
      last_backfill_bitwise(false)
  { }
  
  void set_last_backfill(hobject_t pos, bool sort) {
    last_backfill = pos;
    last_backfill_bitwise = sort;
  }

  bool is_empty() const { return last_update.version == 0; }
  bool dne() const { return history.epoch_created == 0; }

  bool is_incomplete() const { return !last_backfill.is_max(); }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  bool overlaps_with(const pg_info_t &oinfo) const {
    return last_update > oinfo.log_tail ?
      oinfo.last_update >= log_tail :
      last_update >= oinfo.log_tail;
  }
  static void generate_test_instances(list<pg_info_t*>& o);
};
WRITE_CLASS_ENCODER(pg_info_t)

inline ostream& operator<<(ostream& out, const pg_info_t& pgi) 
{
  out << pgi.pgid << "(";
  if (pgi.dne())
    out << " DNE";
  if (pgi.is_empty())
    out << " empty";
  else {
    out << " v " << pgi.last_update;
    if (pgi.last_complete != pgi.last_update)
      out << " lc " << pgi.last_complete;
    out << " (" << pgi.log_tail << "," << pgi.last_update << "]";
  }
  if (pgi.is_incomplete())
    out << " lb " << pgi.last_backfill
	<< (pgi.last_backfill_bitwise ? " (bitwise)" : " (NIBBLEWISE)");
  //out << " c " << pgi.epoch_created;
  out << " local-les=" << pgi.last_epoch_started;
  out << " n=" << pgi.stats.stats.sum.num_objects;
  out << " " << pgi.history
      << ")";
  return out;
}

struct pg_notify_t {
  epoch_t query_epoch;
  epoch_t epoch_sent;
  pg_info_t info;
  shard_id_t to;
  shard_id_t from;
  pg_notify_t() :
    query_epoch(0), epoch_sent(0), to(shard_id_t::NO_SHARD),
    from(shard_id_t::NO_SHARD) {}
  pg_notify_t(
    shard_id_t to,
    shard_id_t from,
    epoch_t query_epoch,
    epoch_t epoch_sent,
    const pg_info_t &info)
    : query_epoch(query_epoch),
      epoch_sent(epoch_sent),
      info(info), to(to), from(from) {
    assert(from == info.pgid.shard);
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_notify_t*> &o);
};
WRITE_CLASS_ENCODER(pg_notify_t)
ostream &operator<<(ostream &lhs, const pg_notify_t &notify);


/**
 * pg_interval_t - information about a past interval
 */
class OSDMap;
struct pg_interval_t {
  vector<int32_t> up, acting;
  epoch_t first, last;
  bool maybe_went_rw;
  int32_t primary;
  int32_t up_primary;

  pg_interval_t()
    : first(0), last(0),
      maybe_went_rw(false),
      primary(-1),
      up_primary(-1)
  {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_interval_t*>& o);

  /**
   * Determines whether there is an interval change
   */
  static bool is_new_interval(
    int old_acting_primary,
    int new_acting_primary,
    const vector<int> &old_acting,
    const vector<int> &new_acting,
    int old_up_primary,
    int new_up_primary,
    const vector<int> &old_up,
    const vector<int> &new_up,
    int old_size,
    int new_size,
    int old_min_size,
    int new_min_size,
    unsigned old_pg_num,
    unsigned new_pg_num,
    bool old_sort_bitwise,
    bool new_sort_bitwise,
    pg_t pgid
    );

  /**
   * Determines whether there is an interval change
   */
  static bool is_new_interval(
    int old_acting_primary,                     ///< [in] primary as of lastmap
    int new_acting_primary,                     ///< [in] primary as of lastmap
    const vector<int> &old_acting,              ///< [in] acting as of lastmap
    const vector<int> &new_acting,              ///< [in] acting as of osdmap
    int old_up_primary,                         ///< [in] up primary of lastmap
    int new_up_primary,                         ///< [in] up primary of osdmap
    const vector<int> &old_up,                  ///< [in] up as of lastmap
    const vector<int> &new_up,                  ///< [in] up as of osdmap
    ceph::shared_ptr<const OSDMap> osdmap,  ///< [in] current map
    ceph::shared_ptr<const OSDMap> lastmap, ///< [in] last map
    pg_t pgid                                   ///< [in] pgid for pg
    );

  /**
   * Integrates a new map into *past_intervals, returns true
   * if an interval was closed out.
   */
  static bool check_new_interval(
    int old_acting_primary,                     ///< [in] primary as of lastmap
    int new_acting_primary,                     ///< [in] primary as of osdmap
    const vector<int> &old_acting,              ///< [in] acting as of lastmap
    const vector<int> &new_acting,              ///< [in] acting as of osdmap
    int old_up_primary,                         ///< [in] up primary of lastmap
    int new_up_primary,                         ///< [in] up primary of osdmap
    const vector<int> &old_up,                  ///< [in] up as of lastmap
    const vector<int> &new_up,                  ///< [in] up as of osdmap
    epoch_t same_interval_since,                ///< [in] as of osdmap
    epoch_t last_epoch_clean,                   ///< [in] current
    ceph::shared_ptr<const OSDMap> osdmap,  ///< [in] current map
    ceph::shared_ptr<const OSDMap> lastmap, ///< [in] last map
    pg_t pgid,                                  ///< [in] pgid for pg
    IsPGRecoverablePredicate *could_have_gone_active, /// [in] predicate whether the pg can be active
    map<epoch_t, pg_interval_t> *past_intervals,///< [out] intervals
    ostream *out = 0                            ///< [out] debug ostream
    );
};
WRITE_CLASS_ENCODER(pg_interval_t)

ostream& operator<<(ostream& out, const pg_interval_t& i);

typedef map<epoch_t, pg_interval_t> pg_interval_map_t;


/** 
 * pg_query_t - used to ask a peer for information about a pg.
 *
 * note: if version=0, type=LOG, then we just provide our full log.
 */
struct pg_query_t {
  enum {
    INFO = 0,
    LOG = 1,
    MISSING = 4,
    FULLLOG = 5,
  };
  const char *get_type_name() const {
    switch (type) {
    case INFO: return "info";
    case LOG: return "log";
    case MISSING: return "missing";
    case FULLLOG: return "fulllog";
    default: return "???";
    }
  }

  __s32 type;
  eversion_t since;
  pg_history_t history;
  epoch_t epoch_sent;
  shard_id_t to;
  shard_id_t from;

  pg_query_t() : type(-1), epoch_sent(0), to(shard_id_t::NO_SHARD),
		 from(shard_id_t::NO_SHARD) {}
  pg_query_t(
    int t,
    shard_id_t to,
    shard_id_t from,
    const pg_history_t& h,
    epoch_t epoch_sent)
    : type(t),
      history(h),
      epoch_sent(epoch_sent),
      to(to), from(from) {
    assert(t != LOG);
  }
  pg_query_t(
    int t,
    shard_id_t to,
    shard_id_t from,
    eversion_t s,
    const pg_history_t& h,
    epoch_t epoch_sent)
    : type(t), since(s), history(h),
      epoch_sent(epoch_sent), to(to), from(from) {
    assert(t == LOG);
  }
  
  void encode(bufferlist &bl, uint64_t features) const;
  void decode(bufferlist::iterator &bl);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_query_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pg_query_t)

inline ostream& operator<<(ostream& out, const pg_query_t& q) {
  out << "query(" << q.get_type_name() << " " << q.since;
  if (q.type == pg_query_t::LOG)
    out << " " << q.history;
  out << ")";
  return out;
}

class PGBackend;
class ObjectModDesc {
  bool can_local_rollback;
  bool rollback_info_completed;
public:
  class Visitor {
  public:
    virtual void append(uint64_t old_offset) {}
    virtual void setattrs(map<string, boost::optional<bufferlist> > &attrs) {}
    virtual void rmobject(version_t old_version) {}
    /**
     * Used to support the unfound_lost_delete log event: if the stashed
     * version exists, we unstash it, otherwise, we do nothing.  This way
     * each replica rolls back to whatever state it had prior to the attempt
     * at mark unfound lost delete
     */
    virtual void try_rmobject(version_t old_version) {
      rmobject(old_version);
    }
    virtual void create() {}
    virtual void update_snaps(set<snapid_t> &old_snaps) {}
    virtual ~Visitor() {}
  };
  void visit(Visitor *visitor) const;
  mutable bufferlist bl;
  enum ModID {
    APPEND = 1,
    SETATTRS = 2,
    DELETE = 3,
    CREATE = 4,
    UPDATE_SNAPS = 5,
    TRY_DELETE = 6
  };
  ObjectModDesc() : can_local_rollback(true), rollback_info_completed(false) {}
  void claim(ObjectModDesc &other) {
    bl.clear();
    bl.claim(other.bl);
    can_local_rollback = other.can_local_rollback;
    rollback_info_completed = other.rollback_info_completed;
  }
  void claim_append(ObjectModDesc &other) {
    if (!can_local_rollback || rollback_info_completed)
      return;
    if (!other.can_local_rollback) {
      mark_unrollbackable();
      return;
    }
    bl.claim_append(other.bl);
    rollback_info_completed = other.rollback_info_completed;
  }
  void swap(ObjectModDesc &other) {
    bl.swap(other.bl);

    bool temp = other.can_local_rollback;
    other.can_local_rollback = can_local_rollback;
    can_local_rollback = temp;

    temp = other.rollback_info_completed;
    other.rollback_info_completed = rollback_info_completed;
    rollback_info_completed = temp;
  }
  void append_id(ModID id) {
    uint8_t _id(id);
    ::encode(_id, bl);
  }
  void append(uint64_t old_size) {
    if (!can_local_rollback || rollback_info_completed)
      return;
    ENCODE_START(1, 1, bl);
    append_id(APPEND);
    ::encode(old_size, bl);
    ENCODE_FINISH(bl);
  }
  void setattrs(map<string, boost::optional<bufferlist> > &old_attrs) {
    if (!can_local_rollback || rollback_info_completed)
      return;
    ENCODE_START(1, 1, bl);
    append_id(SETATTRS);
    ::encode(old_attrs, bl);
    ENCODE_FINISH(bl);
  }
  bool rmobject(version_t deletion_version) {
    if (!can_local_rollback || rollback_info_completed)
      return false;
    ENCODE_START(1, 1, bl);
    append_id(DELETE);
    ::encode(deletion_version, bl);
    ENCODE_FINISH(bl);
    rollback_info_completed = true;
    return true;
  }
  bool try_rmobject(version_t deletion_version) {
    if (!can_local_rollback || rollback_info_completed)
      return false;
    ENCODE_START(1, 1, bl);
    append_id(TRY_DELETE);
    ::encode(deletion_version, bl);
    ENCODE_FINISH(bl);
    rollback_info_completed = true;
    return true;
  }
  void create() {
    if (!can_local_rollback || rollback_info_completed)
      return;
    rollback_info_completed = true;
    ENCODE_START(1, 1, bl);
    append_id(CREATE);
    ENCODE_FINISH(bl);
  }
  void update_snaps(set<snapid_t> &old_snaps) {
    if (!can_local_rollback || rollback_info_completed)
      return;
    ENCODE_START(1, 1, bl);
    append_id(UPDATE_SNAPS);
    ::encode(old_snaps, bl);
    ENCODE_FINISH(bl);
  }

  // cannot be rolled back
  void mark_unrollbackable() {
    can_local_rollback = false;
    bl.clear();
  }
  bool can_rollback() const {
    return can_local_rollback;
  }
  bool empty() const {
    return can_local_rollback && (bl.length() == 0);
  }

  /**
   * Create fresh copy of bl bytes to avoid keeping large buffers around
   * in the case that bl contains ptrs which point into a much larger
   * message buffer
   */
  void trim_bl() {
    if (bl.length() > 0)
      bl.rebuild();
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ObjectModDesc*>& o);
};
WRITE_CLASS_ENCODER(ObjectModDesc)


/**
 * pg_log_entry_t - single entry/event in pg log
 *
 */
struct pg_log_entry_t {
  enum {
    MODIFY = 1,   // some unspecified modification (but not *all* modifications)
    CLONE = 2,    // cloned object from head
    DELETE = 3,   // deleted object
    BACKLOG = 4,  // event invented by generate_backlog [deprecated]
    LOST_REVERT = 5, // lost new version, revert to an older version.
    LOST_DELETE = 6, // lost new version, revert to no object (deleted).
    LOST_MARK = 7,   // lost new version, now EIO
    PROMOTE = 8,     // promoted object from another tier
    CLEAN = 9,       // mark an object clean
  };
  static const char *get_op_name(int op) {
    switch (op) {
    case MODIFY:
      return "modify  ";
    case PROMOTE:
      return "promote ";
    case CLONE:
      return "clone   ";
    case DELETE:
      return "delete  ";
    case BACKLOG:
      return "backlog ";
    case LOST_REVERT:
      return "l_revert";
    case LOST_DELETE:
      return "l_delete";
    case LOST_MARK:
      return "l_mark  ";
    case CLEAN:
      return "clean   ";
    default:
      return "unknown ";
    }
  }
  const char *get_op_name() const {
    return get_op_name(op);
  }

  // describes state for a locally-rollbackable entry
  ObjectModDesc mod_desc;
  bufferlist snaps;   // only for clone entries
  hobject_t  soid;
  osd_reqid_t reqid;  // caller+tid to uniquely identify request
  vector<pair<osd_reqid_t, version_t> > extra_reqids;
  eversion_t version, prior_version, reverting_to;
  version_t user_version; // the user version for this entry
  utime_t     mtime;  // this is the _user_ mtime, mind you

  __s32      op;
  bool invalid_hash; // only when decoding sobject_t based entries
  bool invalid_pool; // only when decoding pool-less hobject based entries

  pg_log_entry_t()
   : user_version(0), op(0),
     invalid_hash(false), invalid_pool(false) {}
  pg_log_entry_t(int _op, const hobject_t& _soid,
                const eversion_t& v, const eversion_t& pv,
                version_t uv,
                const osd_reqid_t& rid, const utime_t& mt)
   : soid(_soid), reqid(rid), version(v), prior_version(pv), user_version(uv),
     mtime(mt), op(_op), invalid_hash(false), invalid_pool(false)
     {}
      
  bool is_clone() const { return op == CLONE; }
  bool is_modify() const { return op == MODIFY; }
  bool is_promote() const { return op == PROMOTE; }
  bool is_clean() const { return op == CLEAN; }
  bool is_backlog() const { return op == BACKLOG; }
  bool is_lost_revert() const { return op == LOST_REVERT; }
  bool is_lost_delete() const { return op == LOST_DELETE; }
  bool is_lost_mark() const { return op == LOST_MARK; }

  bool is_update() const {
    return
      is_clone() || is_modify() || is_promote() || is_clean() ||
      is_backlog() || is_lost_revert() || is_lost_mark();
  }
  bool is_delete() const {
    return op == DELETE || op == LOST_DELETE;
  }
      
  bool reqid_is_indexed() const {
    return reqid != osd_reqid_t() && (op == MODIFY || op == DELETE);
  }

  string get_key_name() const;
  void encode_with_checksum(bufferlist& bl) const;
  void decode_with_checksum(bufferlist::iterator& p);

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_log_entry_t*>& o);

};
WRITE_CLASS_ENCODER(pg_log_entry_t)

ostream& operator<<(ostream& out, const pg_log_entry_t& e);



/**
 * pg_log_t - incremental log of recent pg changes.
 *
 *  serves as a recovery queue for recent changes.
 */
struct pg_log_t {
  /*
   *   head - newest entry (update|delete)
   *   tail - entry previous to oldest (update|delete) for which we have
   *          complete negative information.  
   * i.e. we can infer pg contents for any store whose last_update >= tail.
   */
  eversion_t head;    // newest entry
  eversion_t tail;    // version prior to oldest

  // We can rollback rollback-able entries > can_rollback_to
  eversion_t can_rollback_to;

  // always <= can_rollback_to, indicates how far stashed rollback
  // data can be found
  eversion_t rollback_info_trimmed_to;

  list<pg_log_entry_t> log;  // the actual log.
  
  pg_log_t() {}

  void clear() {
    eversion_t z;
    can_rollback_to = head = tail = z;
    log.clear();
  }

  bool empty() const {
    return log.empty();
  }

  bool null() const {
    return head.version == 0 && head.epoch == 0;
  }

  size_t approx_size() const {
    return head.version - tail.version;
  }

  static void filter_log(spg_t import_pgid, const OSDMap &curmap,
    const string &hit_set_namespace, const pg_log_t &in,
    pg_log_t &out, pg_log_t &reject);

  /**
   * copy entries from the tail of another pg_log_t
   *
   * @param other pg_log_t to copy from
   * @param from copy entries after this version
   */
  void copy_after(const pg_log_t &other, eversion_t from);

  /**
   * copy a range of entries from another pg_log_t
   *
   * @param other pg_log_t to copy from
   * @param from copy entries after this version
   * @param to up to and including this version
   */
  void copy_range(const pg_log_t &other, eversion_t from, eversion_t to);

  /**
   * copy up to N entries
   *
   * @param other source log
   * @param max max number of entries to copy
   */
  void copy_up_to(const pg_log_t &other, int max);

  ostream& print(ostream& out) const;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl, int64_t pool = -1);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_log_t*>& o);
};
WRITE_CLASS_ENCODER(pg_log_t)

inline ostream& operator<<(ostream& out, const pg_log_t& log) 
{
  out << "log((" << log.tail << "," << log.head << "], crt="
      << log.can_rollback_to << ")";
  return out;
}


/**
 * pg_missing_t - summary of missing objects.
 *
 *  kept in memory, as a supplement to pg_log_t
 *  also used to pass missing info in messages.
 */
struct pg_missing_t {
  struct item {
    eversion_t need, have;
    item() {}
    explicit item(eversion_t n) : need(n) {}  // have no old version
    item(eversion_t n, eversion_t h) : need(n), have(h) {}

    void encode(bufferlist& bl) const {
      ::encode(need, bl);
      ::encode(have, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(need, bl);
      ::decode(have, bl);
    }
    void dump(Formatter *f) const {
      f->dump_stream("need") << need;
      f->dump_stream("have") << have;
    }
    static void generate_test_instances(list<item*>& o) {
      o.push_back(new item);
      o.push_back(new item);
      o.back()->need = eversion_t(1, 2);
      o.back()->have = eversion_t(1, 1);
    }
  }; 
  WRITE_CLASS_ENCODER(item)

  map<hobject_t, item, hobject_t::ComparatorWithDefault> missing;  // oid -> (need v, have v)
  map<version_t, hobject_t> rmissing;  // v -> oid

  unsigned int num_missing() const;
  bool have_missing() const;
  void swap(pg_missing_t& o);
  bool is_missing(const hobject_t& oid) const;
  bool is_missing(const hobject_t& oid, eversion_t v) const;
  eversion_t have_old(const hobject_t& oid) const;
  void add_next_event(const pg_log_entry_t& e);
  void revise_need(hobject_t oid, eversion_t need);
  void revise_have(hobject_t oid, eversion_t have);
  void add(const hobject_t& oid, eversion_t need, eversion_t have);
  void rm(const hobject_t& oid, eversion_t v);
  void rm(const std::map<hobject_t, pg_missing_t::item, hobject_t::ComparatorWithDefault>::iterator &m);
  void got(const hobject_t& oid, eversion_t v);
  void got(const std::map<hobject_t, pg_missing_t::item, hobject_t::ComparatorWithDefault>::iterator &m);
  void split_into(pg_t child_pgid, unsigned split_bits, pg_missing_t *omissing);

  void clear() {
    missing.clear();
    rmissing.clear();
  }

  void resort(bool sort_bitwise);

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl, int64_t pool = -1);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_missing_t*>& o);
};
WRITE_CLASS_ENCODER(pg_missing_t::item)
WRITE_CLASS_ENCODER(pg_missing_t)

ostream& operator<<(ostream& out, const pg_missing_t::item& i);
ostream& operator<<(ostream& out, const pg_missing_t& missing);

/**
 * pg list objects response format
 *
 */
struct pg_nls_response_t {
  collection_list_handle_t handle;
  list<librados::ListObjectImpl> entries;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(handle, bl);
    __u32 n = (__u32)entries.size();
    ::encode(n, bl);
    for (list<librados::ListObjectImpl>::const_iterator i = entries.begin(); i != entries.end(); ++i) {
      ::encode(i->nspace, bl);
      ::encode(i->oid, bl);
      ::encode(i->locator, bl);
    }
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(handle, bl);
    __u32 n;
    ::decode(n, bl);
    entries.clear();
    while (n--) {
      librados::ListObjectImpl i;
      ::decode(i.nspace, bl);
      ::decode(i.oid, bl);
      ::decode(i.locator, bl);
      entries.push_back(i);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->dump_stream("handle") << handle;
    f->open_array_section("entries");
    for (list<librados::ListObjectImpl>::const_iterator p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("object");
      f->dump_string("namespace", p->nspace);
      f->dump_string("object", p->oid);
      f->dump_string("key", p->locator);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(list<pg_nls_response_t*>& o) {
    o.push_back(new pg_nls_response_t);
    o.push_back(new pg_nls_response_t);
    o.back()->handle = hobject_t(object_t("hi"), "key", 1, 2, -1, "");
    o.back()->entries.push_back(librados::ListObjectImpl("", "one", ""));
    o.back()->entries.push_back(librados::ListObjectImpl("", "two", "twokey"));
    o.back()->entries.push_back(librados::ListObjectImpl("", "three", ""));
    o.push_back(new pg_nls_response_t);
    o.back()->handle = hobject_t(object_t("hi"), "key", 3, 4, -1, "");
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1one", ""));
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1two", "n1twokey"));
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1three", ""));
    o.push_back(new pg_nls_response_t);
    o.back()->handle = hobject_t(object_t("hi"), "key", 5, 6, -1, "");
    o.back()->entries.push_back(librados::ListObjectImpl("", "one", ""));
    o.back()->entries.push_back(librados::ListObjectImpl("", "two", "twokey"));
    o.back()->entries.push_back(librados::ListObjectImpl("", "three", ""));
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1one", ""));
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1two", "n1twokey"));
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1three", ""));
  }
};

WRITE_CLASS_ENCODER(pg_nls_response_t)

// For backwards compatibility with older OSD requests
struct pg_ls_response_t {
  collection_list_handle_t handle; 
  list<pair<object_t, string> > entries;

  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(handle, bl);
    ::encode(entries, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    assert(v == 1);
    ::decode(handle, bl);
    ::decode(entries, bl);
  }
  void dump(Formatter *f) const {
    f->dump_stream("handle") << handle;
    f->open_array_section("entries");
    for (list<pair<object_t, string> >::const_iterator p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("object");
      f->dump_stream("object") << p->first;
      f->dump_string("key", p->second);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(list<pg_ls_response_t*>& o) {
    o.push_back(new pg_ls_response_t);
    o.push_back(new pg_ls_response_t);
    o.back()->handle = hobject_t(object_t("hi"), "key", 1, 2, -1, "");
    o.back()->entries.push_back(make_pair(object_t("one"), string()));
    o.back()->entries.push_back(make_pair(object_t("two"), string("twokey")));
  }
};

WRITE_CLASS_ENCODER(pg_ls_response_t)

/**
 * object_copy_cursor_t
 */
struct object_copy_cursor_t {
  uint64_t data_offset;
  string omap_offset;
  bool attr_complete;
  bool data_complete;
  bool omap_complete;

  object_copy_cursor_t()
    : data_offset(0),
      attr_complete(false),
      data_complete(false),
      omap_complete(false)
  {}

  bool is_initial() const {
    return !attr_complete && data_offset == 0 && omap_offset.empty();
  }
  bool is_complete() const {
    return attr_complete && data_complete && omap_complete;
  }

  static void generate_test_instances(list<object_copy_cursor_t*>& o);
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(object_copy_cursor_t)

/**
 * object_copy_data_t
 *
 * Return data from a copy request. The semantics are a little strange
 * as a result of the encoding's heritage.
 *
 * In particular, the sender unconditionally fills in the cursor (from what
 * it receives and sends), the size, and the mtime, but is responsible for
 * figuring out whether it should put any data in the attrs, data, or
 * omap members (corresponding to xattrs, object data, and the omap entries)
 * based on external data (the client includes a max amount to return with
 * the copy request). The client then looks into the attrs, data, and/or omap
 * based on the contents of the cursor.
 */
struct object_copy_data_t {
  enum {
    FLAG_DATA_DIGEST = 1<<0,
    FLAG_OMAP_DIGEST = 1<<1,
  };
  object_copy_cursor_t cursor;
  uint64_t size;
  utime_t mtime;
  uint32_t data_digest, omap_digest;
  uint32_t flags;
  map<string, bufferlist> attrs;
  bufferlist data;
  bufferlist omap_header;
  bufferlist omap_data;

  /// which snaps we are defined for (if a snap and not the head)
  vector<snapid_t> snaps;
  ///< latest snap seq for the object (if head)
  snapid_t snap_seq;

  ///< recent reqids on this object
  vector<pair<osd_reqid_t, version_t> > reqids;

  uint64_t truncate_seq;
  uint64_t truncate_size;

public:
  object_copy_data_t() :
    size((uint64_t)-1), data_digest(-1),
    omap_digest(-1), flags(0),
    truncate_seq(0),
    truncate_size(0) {}

  static void generate_test_instances(list<object_copy_data_t*>& o);
  void encode_classic(bufferlist& bl) const;
  void decode_classic(bufferlist::iterator& bl);
  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER_FEATURES(object_copy_data_t)

/**
 * pg creation info
 */
struct pg_create_t {
  epoch_t created;   // epoch pg created
  pg_t parent;       // split from parent (if != pg_t())
  __s32 split_bits;

  pg_create_t()
    : created(0), split_bits(0) {}
  pg_create_t(unsigned c, pg_t p, int s)
    : created(c), parent(p), split_bits(s) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_create_t*>& o);
};
WRITE_CLASS_ENCODER(pg_create_t)

// -----------------------------------------

struct osd_peer_stat_t {
  utime_t stamp;

  osd_peer_stat_t() { }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<osd_peer_stat_t*>& o);
};
WRITE_CLASS_ENCODER(osd_peer_stat_t)

ostream& operator<<(ostream& out, const osd_peer_stat_t &stat);


// -----------------------------------------

class ObjectExtent {
 public:
  object_t    oid;       // object id
  uint64_t    objectno;
  uint64_t    offset;    // in object
  uint64_t    length;    // in object
  uint64_t    truncate_size;	// in object

  object_locator_t oloc;   // object locator (pool etc)

  vector<pair<uint64_t,uint64_t> >  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : objectno(0), offset(0), length(0), truncate_size(0) {}
  ObjectExtent(object_t o, uint64_t ono, uint64_t off, uint64_t l, uint64_t ts) :
    oid(o), objectno(ono), offset(off), length(l), truncate_size(ts) { }
};

inline ostream& operator<<(ostream& out, const ObjectExtent &ex)
{
  return out << "extent(" 
             << ex.oid << " (" << ex.objectno << ") in " << ex.oloc
             << " " << ex.offset << "~" << ex.length
	     << " -> " << ex.buffer_extents
             << ")";
}






// ---------------------------------------

class OSDSuperblock {
public:
  uuid_d cluster_fsid, osd_fsid;
  int32_t whoami;    // my role in this fs.
  epoch_t current_epoch;             // most recent epoch
  epoch_t oldest_map, newest_map;    // oldest/newest maps we have.
  double weight;

  CompatSet compat_features;

  // last interval over which i mounted and was then active
  epoch_t mounted;     // last epoch i mounted
  epoch_t clean_thru;  // epoch i was active and clean thru

  OSDSuperblock() : 
    whoami(-1), 
    current_epoch(0), oldest_map(0), newest_map(0), weight(0),
    mounted(0), clean_thru(0) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<OSDSuperblock*>& o);
};
WRITE_CLASS_ENCODER(OSDSuperblock)

inline ostream& operator<<(ostream& out, const OSDSuperblock& sb)
{
  return out << "sb(" << sb.cluster_fsid
             << " osd." << sb.whoami
	     << " " << sb.osd_fsid
             << " e" << sb.current_epoch
             << " [" << sb.oldest_map << "," << sb.newest_map << "]"
	     << " lci=[" << sb.mounted << "," << sb.clean_thru << "]"
             << ")";
}


// -------

WRITE_CLASS_ENCODER(interval_set<uint64_t>)





/*
 * attached to object head.  describes most recent snap context, and
 * set of existing clones.
 */
struct SnapSet {
  snapid_t seq;
  bool head_exists;
  vector<snapid_t> snaps;    // descending
  vector<snapid_t> clones;   // ascending
  map<snapid_t, interval_set<uint64_t> > clone_overlap;  // overlap w/ next newest
  map<snapid_t, uint64_t> clone_size;

  SnapSet() : seq(0), head_exists(false) {}
  explicit SnapSet(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }

  /// populate SnapSet from a librados::snap_set_t
  void from_snap_set(const librados::snap_set_t& ss);

  /// get space accounted to clone
  uint64_t get_clone_bytes(snapid_t clone) const;
    
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<SnapSet*>& o);  

  SnapContext get_ssc_as_of(snapid_t as_of) const {
    SnapContext out;
    out.seq = as_of;
    for (vector<snapid_t>::const_iterator i = snaps.begin();
	 i != snaps.end();
	 ++i) {
      if (*i <= as_of)
	out.snaps.push_back(*i);
    }
    return out;
  }

  // return min element of snaps > after, return max if no such element
  snapid_t get_first_snap_after(snapid_t after, snapid_t max) const {
    for (vector<snapid_t>::const_reverse_iterator i = snaps.rbegin();
	 i != snaps.rend();
	 ++i) {
      if (*i > after)
	return *i;
    }
    return max;
  }

  SnapSet get_filtered(const pg_pool_t &pinfo) const;
  void filter(const pg_pool_t &pinfo);
};
WRITE_CLASS_ENCODER(SnapSet)

ostream& operator<<(ostream& out, const SnapSet& cs);



#define OI_ATTR "_"
#define SS_ATTR "snapset"

struct watch_info_t {
  uint64_t cookie;
  uint32_t timeout_seconds;
  entity_addr_t addr;

  watch_info_t() : cookie(0), timeout_seconds(0) { }
  watch_info_t(uint64_t c, uint32_t t, const entity_addr_t& a) : cookie(c), timeout_seconds(t), addr(a) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<watch_info_t*>& o);
};
WRITE_CLASS_ENCODER(watch_info_t)

static inline bool operator==(const watch_info_t& l, const watch_info_t& r) {
  return l.cookie == r.cookie && l.timeout_seconds == r.timeout_seconds
	    && l.addr == r.addr;
}

static inline ostream& operator<<(ostream& out, const watch_info_t& w) {
  return out << "watch(cookie " << w.cookie << " " << w.timeout_seconds << "s"
    << " " << w.addr << ")";
}

struct notify_info_t {
  uint64_t cookie;
  uint64_t notify_id;
  uint32_t timeout;
  bufferlist bl;
};

static inline ostream& operator<<(ostream& out, const notify_info_t& n) {
  return out << "notify(cookie " << n.cookie
	     << " notify" << n.notify_id
	     << " " << n.timeout << "s)";
}


struct object_info_t {
  hobject_t soid;
  eversion_t version, prior_version;
  version_t user_version;
  osd_reqid_t last_reqid;

  uint64_t size;
  utime_t mtime;
  utime_t local_mtime; // local mtime

  // note: these are currently encoded into a total 16 bits; see
  // encode()/decode() for the weirdness.
  typedef enum {
    FLAG_LOST     = 1<<0,
    FLAG_WHITEOUT = 1<<1,  // object logically does not exist
    FLAG_DIRTY    = 1<<2,  // object has been modified since last flushed or undirtied
    FLAG_OMAP     = 1 << 3,  // has (or may have) some/any omap data
    FLAG_DATA_DIGEST = 1 << 4,  // has data crc
    FLAG_OMAP_DIGEST = 1 << 5,  // has omap crc
    FLAG_CACHE_PIN = 1 << 6,    // pin the object in cache tier
    // ...
    FLAG_USES_TMAP = 1<<8,  // deprecated; no longer used.
  } flag_t;

  flag_t flags;

  static string get_flag_string(flag_t flags) {
    string s;
    if (flags & FLAG_LOST)
      s += "|lost";
    if (flags & FLAG_WHITEOUT)
      s += "|whiteout";
    if (flags & FLAG_DIRTY)
      s += "|dirty";
    if (flags & FLAG_USES_TMAP)
      s += "|uses_tmap";
    if (flags & FLAG_OMAP)
      s += "|omap";
    if (flags & FLAG_DATA_DIGEST)
      s += "|data_digest";
    if (flags & FLAG_OMAP_DIGEST)
      s += "|omap_digest";
    if (flags & FLAG_CACHE_PIN)
      s += "|cache_pin";
    if (s.length())
      return s.substr(1);
    return s;
  }
  string get_flag_string() const {
    return get_flag_string(flags);
  }

  vector<snapid_t> snaps;  // [clone]

  uint64_t truncate_seq, truncate_size;

  map<pair<uint64_t, entity_name_t>, watch_info_t> watchers;

  // opportunistic checksums; may or may not be present
  __u32 data_digest;  ///< data crc32c
  __u32 omap_digest;  ///< omap crc32c

  void copy_user_bits(const object_info_t& other);

  static ps_t legacy_object_locator_to_ps(const object_t &oid, 
					  const object_locator_t &loc);

  bool test_flag(flag_t f) const {
    return (flags & f) == f;
  }
  void set_flag(flag_t f) {
    flags = (flag_t)(flags | f);
  }
  void clear_flag(flag_t f) {
    flags = (flag_t)(flags & ~f);
  }
  bool is_lost() const {
    return test_flag(FLAG_LOST);
  }
  bool is_whiteout() const {
    return test_flag(FLAG_WHITEOUT);
  }
  bool is_dirty() const {
    return test_flag(FLAG_DIRTY);
  }
  bool is_omap() const {
    return test_flag(FLAG_OMAP);
  }
  bool is_data_digest() const {
    return test_flag(FLAG_DATA_DIGEST);
  }
  bool is_omap_digest() const {
    return test_flag(FLAG_OMAP_DIGEST);
  }
  bool is_cache_pinned() const {
    return test_flag(FLAG_CACHE_PIN);
  }

  void set_data_digest(__u32 d) {
    set_flag(FLAG_DATA_DIGEST);
    data_digest = d;
  }
  void set_omap_digest(__u32 d) {
    set_flag(FLAG_OMAP_DIGEST);
    omap_digest = d;
  }
  void clear_data_digest() {
    clear_flag(FLAG_DATA_DIGEST);
    data_digest = -1;
  }
  void clear_omap_digest() {
    clear_flag(FLAG_OMAP_DIGEST);
    omap_digest = -1;
  }
  void new_object() {
    set_data_digest(-1);
    set_omap_digest(-1);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<object_info_t*>& o);

  explicit object_info_t()
    : user_version(0), size(0), flags((flag_t)0),
      truncate_seq(0), truncate_size(0),
      data_digest(-1), omap_digest(-1)
  {}

  explicit object_info_t(const hobject_t& s)
    : soid(s),
      user_version(0), size(0), flags((flag_t)0),
      truncate_seq(0), truncate_size(0),
      data_digest(-1), omap_digest(-1)
  {}

  explicit object_info_t(bufferlist& bl) {
    decode(bl);
  }
  object_info_t operator=(bufferlist& bl) {
    decode(bl);
    return *this;
  }
};
WRITE_CLASS_ENCODER(object_info_t)

struct ObjectState {
  object_info_t oi;
  bool exists;         ///< the stored object exists (i.e., we will remember the object_info_t)

  ObjectState() : exists(false) {}

  ObjectState(const object_info_t &oi_, bool exists_)
    : oi(oi_), exists(exists_) {}
};

struct SnapSetContext {
  hobject_t oid;
  SnapSet snapset;
  int ref;
  bool registered : 1;
  bool exists : 1;

  explicit SnapSetContext(const hobject_t& o) :
    oid(o), ref(0), registered(false), exists(true) { }
};

/*
  * keep tabs on object modifications that are in flight.
  * we need to know the projected existence, size, snapset,
  * etc., because we don't send writes down to disk until after
  * replicas ack.
  */

struct ObjectContext;

typedef ceph::shared_ptr<ObjectContext> ObjectContextRef;

struct ObjectContext {
  ObjectState obs;

  SnapSetContext *ssc;  // may be null

  Context *destructor_callback;

private:
  Mutex lock;
public:
  Cond cond;
  int unstable_writes, readers, writers_waiting, readers_waiting;


  // set if writes for this object are blocked on another objects recovery
  ObjectContextRef blocked_by;      // object blocking our writes
  set<ObjectContextRef> blocking;   // objects whose writes we block

  // any entity in obs.oi.watchers MUST be in either watchers or unconnected_watchers.
  map<pair<uint64_t, entity_name_t>, WatchRef> watchers;

  // attr cache
  map<string, bufferlist> attr_cache;

  void fill_in_setattrs(const set<string> &changing, ObjectModDesc *mod) {
    map<string, boost::optional<bufferlist> > to_set;
    for (set<string>::const_iterator i = changing.begin();
	 i != changing.end();
	 ++i) {
      map<string, bufferlist>::iterator iter = attr_cache.find(*i);
      if (iter != attr_cache.end()) {
	to_set[*i] = iter->second;
      } else {
	to_set[*i];
      }
    }
    mod->setattrs(to_set);
  }
  
  struct RWState {
    enum State {
      RWNONE,
      RWREAD,
      RWWRITE,
      RWEXCL,
    };
    static const char *get_state_name(State s) {
      switch (s) {
      case RWNONE: return "none";
      case RWREAD: return "read";
      case RWWRITE: return "write";
      case RWEXCL: return "excl";
      default: return "???";
      }
    }
    const char *get_state_name() const {
      return get_state_name(state);
    }

    list<OpRequestRef> waiters;  ///< ops waiting on state change
    int count;              ///< number of readers or writers

    State state:4;               ///< rw state
    /// if set, restart backfill when we can get a read lock
    bool recovery_read_marker:1;
    /// if set, requeue snaptrim on lock release
    bool snaptrimmer_write_marker:1;

    RWState()
      : count(0),
	state(RWNONE),
	recovery_read_marker(false),
	snaptrimmer_write_marker(false)
    {}
    bool get_read(OpRequestRef op) {
      if (get_read_lock()) {
	return true;
      } // else
      waiters.push_back(op);
      return false;
    }
    /// this function adjusts the counts if necessary
    bool get_read_lock() {
      // don't starve anybody!
      if (!waiters.empty()) {
	return false;
      }
      switch (state) {
      case RWNONE:
	assert(count == 0);
	state = RWREAD;
	// fall through
      case RWREAD:
	count++;
	return true;
      case RWWRITE:
	return false;
      case RWEXCL:
	return false;
      default:
	assert(0 == "unhandled case");
	return false;
      }
    }

    bool get_write(OpRequestRef op, bool greedy=false) {
      if (get_write_lock(greedy)) {
	return true;
      } // else
      if (op)
	waiters.push_back(op);
      return false;
    }
    bool get_write_lock(bool greedy=false) {
      if (!greedy) {
	// don't starve anybody!
	if (!waiters.empty() ||
	    recovery_read_marker) {
	  return false;
	}
      }
      switch (state) {
      case RWNONE:
	assert(count == 0);
	state = RWWRITE;
	// fall through
      case RWWRITE:
	count++;
	return true;
      case RWREAD:
	return false;
      case RWEXCL:
	return false;
      default:
	assert(0 == "unhandled case");
	return false;
      }
    }
    bool get_excl_lock() {
      switch (state) {
      case RWNONE:
	assert(count == 0);
	state = RWEXCL;
	count = 1;
	return true;
      case RWWRITE:
	return false;
      case RWREAD:
	return false;
      case RWEXCL:
	return false;
      default:
	assert(0 == "unhandled case");
	return false;
      }
    }
    bool get_excl(OpRequestRef op) {
      if (get_excl_lock()) {
	return true;
      } // else
      if (op)
	waiters.push_back(op);
      return false;
    }
    /// same as get_write_lock, but ignore starvation
    bool take_write_lock() {
      if (state == RWWRITE) {
	count++;
	return true;
      }
      return get_write_lock();
    }
    void dec(list<OpRequestRef> *requeue) {
      assert(count > 0);
      assert(requeue);
      count--;
      if (count == 0) {
	state = RWNONE;
	requeue->splice(requeue->end(), waiters);
      }
    }
    void put_read(list<OpRequestRef> *requeue) {
      assert(state == RWREAD);
      dec(requeue);
    }
    void put_write(list<OpRequestRef> *requeue) {
      assert(state == RWWRITE);
      dec(requeue);
    }
    void put_excl(list<OpRequestRef> *requeue) {
      assert(state == RWEXCL);
      dec(requeue);
    }
    bool empty() const { return state == RWNONE; }
  } rwstate;

  bool get_read(OpRequestRef op) {
    return rwstate.get_read(op);
  }
  bool get_write(OpRequestRef op) {
    return rwstate.get_write(op, false);
  }
  bool get_excl(OpRequestRef op) {
    return rwstate.get_excl(op);
  }
  bool get_lock_type(OpRequestRef op, RWState::State type) {
    switch (type) {
    case RWState::RWWRITE:
      return get_write(op);
    case RWState::RWREAD:
      return get_read(op);
    case RWState::RWEXCL:
      return get_excl(op);
    default:
      assert(0 == "invalid lock type");
      return true;
    }
  }
  bool get_write_greedy(OpRequestRef op) {
    return rwstate.get_write(op, true);
  }
  bool get_snaptrimmer_write() {
    if (rwstate.get_write_lock()) {
      return true;
    } else {
      rwstate.snaptrimmer_write_marker = true;
      return false;
    }
  }
  bool get_recovery_read() {
    rwstate.recovery_read_marker = true;
    if (rwstate.get_read_lock()) {
      return true;
    }
    return false;
  }
  void drop_recovery_read(list<OpRequestRef> *ls) {
    assert(rwstate.recovery_read_marker);
    rwstate.put_read(ls);
    rwstate.recovery_read_marker = false;
  }
  void put_read(list<OpRequestRef> *to_wake) {
    rwstate.put_read(to_wake);
  }
  void put_excl(list<OpRequestRef> *to_wake,
		 bool *requeue_recovery,
		 bool *requeue_snaptrimmer) {
    rwstate.put_excl(to_wake);
    if (rwstate.empty() && rwstate.recovery_read_marker) {
      rwstate.recovery_read_marker = false;
      *requeue_recovery = true;
    }
    if (rwstate.empty() && rwstate.snaptrimmer_write_marker) {
      rwstate.snaptrimmer_write_marker = false;
      *requeue_snaptrimmer = true;
    }
  }
  void put_write(list<OpRequestRef> *to_wake,
		 bool *requeue_recovery,
		 bool *requeue_snaptrimmer) {
    rwstate.put_write(to_wake);
    if (rwstate.empty() && rwstate.recovery_read_marker) {
      rwstate.recovery_read_marker = false;
      *requeue_recovery = true;
    }
    if (rwstate.empty() && rwstate.snaptrimmer_write_marker) {
      rwstate.snaptrimmer_write_marker = false;
      *requeue_snaptrimmer = true;
    }
  }
  void put_lock_type(
    ObjectContext::RWState::State type,
    list<OpRequestRef> *to_wake,
    bool *requeue_recovery,
    bool *requeue_snaptrimmer) {
    switch (type) {
    case ObjectContext::RWState::RWWRITE:
      return put_write(to_wake, requeue_recovery, requeue_snaptrimmer);
    case ObjectContext::RWState::RWREAD:
      return put_read(to_wake);
    case ObjectContext::RWState::RWEXCL:
      return put_excl(to_wake, requeue_recovery, requeue_snaptrimmer);
    default:
      assert(0 == "invalid lock type");
    }
  }
  bool is_request_pending() {
    return (rwstate.count > 0);
  }

  ObjectContext()
    : ssc(NULL),
      destructor_callback(0),
      lock("ReplicatedPG::ObjectContext::lock"),
      unstable_writes(0), readers(0), writers_waiting(0), readers_waiting(0),
      blocked(false), requeue_scrub_on_unblock(false) {}

  ~ObjectContext() {
    assert(rwstate.empty());
    if (destructor_callback)
      destructor_callback->complete(0);
  }

  void start_block() {
    assert(!blocked);
    blocked = true;
  }
  void stop_block() {
    assert(blocked);
    blocked = false;
  }
  bool is_blocked() const {
    return blocked;
  }

  // do simple synchronous mutual exclusion, for now.  no waitqueues or anything fancy.
  void ondisk_write_lock() {
    lock.Lock();
    writers_waiting++;
    while (readers_waiting || readers)
      cond.Wait(lock);
    writers_waiting--;
    unstable_writes++;
    lock.Unlock();
  }
  void ondisk_write_unlock() {
    lock.Lock();
    assert(unstable_writes > 0);
    unstable_writes--;
    if (!unstable_writes && readers_waiting)
      cond.Signal();
    lock.Unlock();
  }
  void ondisk_read_lock() {
    lock.Lock();
    readers_waiting++;
    while (unstable_writes)
      cond.Wait(lock);
    readers_waiting--;
    readers++;
    lock.Unlock();
  }
  void ondisk_read_unlock() {
    lock.Lock();
    assert(readers > 0);
    readers--;
    if (!readers && writers_waiting)
      cond.Signal();
    lock.Unlock();
  }

  /// in-progress copyfrom ops for this object
  bool blocked:1;
  bool requeue_scrub_on_unblock:1;    // true if we need to requeue scrub on unblock

};

inline ostream& operator<<(ostream& out, const ObjectState& obs)
{
  out << obs.oi.soid;
  if (!obs.exists)
    out << "(dne)";
  return out;
}

inline ostream& operator<<(ostream& out, const ObjectContext::RWState& rw)
{
  return out << "rwstate(" << rw.get_state_name()
	     << " n=" << rw.count
	     << " w=" << rw.waiters.size()
	     << ")";
}

inline ostream& operator<<(ostream& out, const ObjectContext& obc)
{
  return out << "obc(" << obc.obs << " " << obc.rwstate << ")";
}


ostream& operator<<(ostream& out, const object_info_t& oi);

class ObcLockManager {
  struct ObjectLockState {
    ObjectContextRef obc;
    ObjectContext::RWState::State type;
    ObjectLockState(
      ObjectContextRef obc,
      ObjectContext::RWState::State type)
      : obc(obc), type(type) {}
  };
  map<hobject_t, ObjectLockState, hobject_t::BitwiseComparator> locks;
public:
  ObcLockManager() = default;
  ObcLockManager(ObcLockManager &&) = default;
  ObcLockManager(const ObcLockManager &) = delete;
  bool empty() const {
    return locks.empty();
  }
  bool get_lock_type(
    ObjectContext::RWState::State type,
    const hobject_t &hoid,
    ObjectContextRef obc,
    OpRequestRef op) {
    assert(locks.find(hoid) == locks.end());
    if (obc->get_lock_type(op, type)) {
      locks.insert(make_pair(hoid, ObjectLockState(obc, type)));
      return true;
    } else {
      return false;
    }
  }
  /// Get write lock, ignore starvation
  bool take_write_lock(
    const hobject_t &hoid,
    ObjectContextRef obc) {
    assert(locks.find(hoid) == locks.end());
    if (obc->rwstate.take_write_lock()) {
      locks.insert(
	make_pair(
	  hoid, ObjectLockState(obc, ObjectContext::RWState::RWWRITE)));
      return true;
    } else {
      return false;
    }
  }
  /// Get write lock for snap trim
  bool get_snaptrimmer_write(
    const hobject_t &hoid,
    ObjectContextRef obc) {
    assert(locks.find(hoid) == locks.end());
    if (obc->get_snaptrimmer_write()) {
      locks.insert(
	make_pair(
	  hoid, ObjectLockState(obc, ObjectContext::RWState::RWWRITE)));
      return true;
    } else {
      return false;
    }
  }
  /// Get write lock greedy
  bool get_write_greedy(
    const hobject_t &hoid,
    ObjectContextRef obc,
    OpRequestRef op) {
    assert(locks.find(hoid) == locks.end());
    if (obc->get_write_greedy(op)) {
      locks.insert(
	make_pair(
	  hoid, ObjectLockState(obc, ObjectContext::RWState::RWWRITE)));
      return true;
    } else {
      return false;
    }
  }
  void put_locks(
    list<pair<hobject_t, list<OpRequestRef> > > *to_requeue,
    bool *requeue_recovery,
    bool *requeue_snaptrimmer) {
    for (auto p: locks) {
      list<OpRequestRef> _to_requeue;
      p.second.obc->put_lock_type(
	p.second.type,
	&_to_requeue,
	requeue_recovery,
	requeue_snaptrimmer);
      if (to_requeue) {
	to_requeue->push_back(
	  make_pair(
	    p.second.obc->obs.oi.soid,
	    std::move(_to_requeue)));
      }
    }
    locks.clear();
  }
  ~ObcLockManager() {
    assert(locks.empty());
  }
};



// Object recovery
struct ObjectRecoveryInfo {
  hobject_t soid;
  eversion_t version;
  uint64_t size;
  object_info_t oi;
  SnapSet ss;
  interval_set<uint64_t> copy_subset;
  map<hobject_t, interval_set<uint64_t>, hobject_t::BitwiseComparator> clone_subset;

  ObjectRecoveryInfo() : size(0) { }

  static void generate_test_instances(list<ObjectRecoveryInfo*>& o);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl, int64_t pool = -1);
  ostream &print(ostream &out) const;
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(ObjectRecoveryInfo)
ostream& operator<<(ostream& out, const ObjectRecoveryInfo &inf);

struct ObjectRecoveryProgress {
  uint64_t data_recovered_to;
  string omap_recovered_to;
  bool first;
  bool data_complete;
  bool omap_complete;

  ObjectRecoveryProgress()
    : data_recovered_to(0),
      first(true),
      data_complete(false), omap_complete(false) { }

  bool is_complete(const ObjectRecoveryInfo& info) const {
    return (data_recovered_to >= (
      info.copy_subset.empty() ?
      0 : info.copy_subset.range_end())) &&
      omap_complete;
  }

  static void generate_test_instances(list<ObjectRecoveryProgress*>& o);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  ostream &print(ostream &out) const;
  void dump(Formatter *f) const;
};
WRITE_CLASS_ENCODER(ObjectRecoveryProgress)
ostream& operator<<(ostream& out, const ObjectRecoveryProgress &prog);

struct PushReplyOp {
  hobject_t soid;

  static void generate_test_instances(list<PushReplyOp*>& o);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  ostream &print(ostream &out) const;
  void dump(Formatter *f) const;

  uint64_t cost(CephContext *cct) const;
};
WRITE_CLASS_ENCODER(PushReplyOp)
ostream& operator<<(ostream& out, const PushReplyOp &op);

struct PullOp {
  hobject_t soid;

  ObjectRecoveryInfo recovery_info;
  ObjectRecoveryProgress recovery_progress;

  static void generate_test_instances(list<PullOp*>& o);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  ostream &print(ostream &out) const;
  void dump(Formatter *f) const;

  uint64_t cost(CephContext *cct) const;
};
WRITE_CLASS_ENCODER(PullOp)
ostream& operator<<(ostream& out, const PullOp &op);

struct PushOp {
  hobject_t soid;
  eversion_t version;
  bufferlist data;
  interval_set<uint64_t> data_included;
  bufferlist omap_header;
  map<string, bufferlist> omap_entries;
  map<string, bufferlist> attrset;

  ObjectRecoveryInfo recovery_info;
  ObjectRecoveryProgress before_progress;
  ObjectRecoveryProgress after_progress;

  static void generate_test_instances(list<PushOp*>& o);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  ostream &print(ostream &out) const;
  void dump(Formatter *f) const;

  uint64_t cost(CephContext *cct) const;
};
WRITE_CLASS_ENCODER(PushOp)
ostream& operator<<(ostream& out, const PushOp &op);


/*
 * summarize pg contents for purposes of a scrub
 */
struct ScrubMap {
  struct object {
    map<string,bufferptr> attrs;
    set<snapid_t> snapcolls;
    uint64_t size;
    __u32 omap_digest;         ///< omap crc32c
    __u32 digest;              ///< data crc32c
    uint32_t nlinks;
    bool negative:1;
    bool digest_present:1;
    bool omap_digest_present:1;
    bool read_error:1;
    bool stat_error:1;

    object() :
      // Init invalid size so it won't match if we get a stat EIO error
      size(-1), omap_digest(0), digest(0), nlinks(0), 
      negative(false), digest_present(false), omap_digest_present(false), 
      read_error(false), stat_error(false) {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<object*>& o);
  };
  WRITE_CLASS_ENCODER(object)

  map<hobject_t,object, hobject_t::BitwiseComparator> objects;
  eversion_t valid_through;
  eversion_t incr_since;

  void merge_incr(const ScrubMap &l);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl, int64_t pool=-1);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<ScrubMap*>& o);
};
WRITE_CLASS_ENCODER(ScrubMap::object)
WRITE_CLASS_ENCODER(ScrubMap)


struct OSDOp {
  ceph_osd_op op;
  sobject_t soid;

  bufferlist indata, outdata;
  int32_t rval;

  OSDOp() : rval(0) {
    memset(&op, 0, sizeof(ceph_osd_op));
  }

  /**
   * split a bufferlist into constituent indata nembers of a vector of OSDOps
   *
   * @param ops [out] vector of OSDOps
   * @param in  [in] combined data buffer
   */
  static void split_osd_op_vector_in_data(vector<OSDOp>& ops, bufferlist& in);

  /**
   * merge indata nembers of a vector of OSDOp into a single bufferlist
   *
   * Notably this also encodes certain other OSDOp data into the data
   * buffer, including the sobject_t soid.
   *
   * @param ops [in] vector of OSDOps
   * @param out [out] combined data buffer
   */
  static void merge_osd_op_vector_in_data(vector<OSDOp>& ops, bufferlist& out);

  /**
   * split a bufferlist into constituent outdata members of a vector of OSDOps
   *
   * @param ops [out] vector of OSDOps
   * @param in  [in] combined data buffer
   */
  static void split_osd_op_vector_out_data(vector<OSDOp>& ops, bufferlist& in);

  /**
   * merge outdata members of a vector of OSDOps into a single bufferlist
   *
   * @param ops [in] vector of OSDOps
   * @param out [out] combined data buffer
   */
  static void merge_osd_op_vector_out_data(vector<OSDOp>& ops, bufferlist& out);
};

ostream& operator<<(ostream& out, const OSDOp& op);

struct watch_item_t {
  entity_name_t name;
  uint64_t cookie;
  uint32_t timeout_seconds;
  entity_addr_t addr;

  watch_item_t() : cookie(0), timeout_seconds(0) { }
  watch_item_t(entity_name_t name, uint64_t cookie, uint32_t timeout,
     const entity_addr_t& addr)
    : name(name), cookie(cookie), timeout_seconds(timeout),
    addr(addr) { }

  void encode(bufferlist &bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(name, bl);
    ::encode(cookie, bl);
    ::encode(timeout_seconds, bl);
    ::encode(addr, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(2, bl);
    ::decode(name, bl);
    ::decode(cookie, bl);
    ::decode(timeout_seconds, bl);
    if (struct_v >= 2) {
      ::decode(addr, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(watch_item_t)

struct obj_watch_item_t {
  hobject_t obj;
  watch_item_t wi;
};

/**
 * obj list watch response format
 *
 */
struct obj_list_watch_response_t {
  list<watch_item_t> entries;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(entries, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(entries, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->open_array_section("entries");
    for (list<watch_item_t>::const_iterator p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("watch");
      f->dump_stream("watcher") << p->name;
      f->dump_int("cookie", p->cookie);
      f->dump_int("timeout", p->timeout_seconds);
      f->open_object_section("addr");
      p->addr.dump(f);
      f->close_section();
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(list<obj_list_watch_response_t*>& o) {
    entity_addr_t ea;
    o.push_back(new obj_list_watch_response_t);
    o.push_back(new obj_list_watch_response_t);
    ea.set_nonce(1000);
    ea.set_family(AF_INET);
    ea.set_in4_quad(0, 127);
    ea.set_in4_quad(1, 0);
    ea.set_in4_quad(2, 0);
    ea.set_in4_quad(3, 1);
    ea.set_port(1024);
    o.back()->entries.push_back(watch_item_t(entity_name_t(entity_name_t::TYPE_CLIENT, 1), 10, 30, ea));
    ea.set_nonce(1001);
    ea.set_in4_quad(3, 2);
    ea.set_port(1025);
    o.back()->entries.push_back(watch_item_t(entity_name_t(entity_name_t::TYPE_CLIENT, 2), 20, 60, ea));
  }
};

WRITE_CLASS_ENCODER(obj_list_watch_response_t)

struct clone_info {
  snapid_t cloneid;
  vector<snapid_t> snaps;  // ascending
  vector< pair<uint64_t,uint64_t> > overlap;
  uint64_t size;

  clone_info() : cloneid(CEPH_NOSNAP), size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(cloneid, bl);
    ::encode(snaps, bl);
    ::encode(overlap, bl);
    ::encode(size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(cloneid, bl);
    ::decode(snaps, bl);
    ::decode(overlap, bl);
    ::decode(size, bl);
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    if (cloneid == CEPH_NOSNAP)
      f->dump_string("cloneid", "HEAD");
    else
      f->dump_unsigned("cloneid", cloneid.val);
    f->open_array_section("snapshots");
    for (vector<snapid_t>::const_iterator p = snaps.begin(); p != snaps.end(); ++p) {
      f->open_object_section("snap");
      f->dump_unsigned("id", p->val);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("overlaps");
    for (vector< pair<uint64_t,uint64_t> >::const_iterator q = overlap.begin();
          q != overlap.end(); ++q) {
      f->open_object_section("overlap");
      f->dump_unsigned("offset", q->first);
      f->dump_unsigned("length", q->second);
      f->close_section();
    }
    f->close_section();
    f->dump_unsigned("size", size);
  }
  static void generate_test_instances(list<clone_info*>& o) {
    o.push_back(new clone_info);
    o.push_back(new clone_info);
    o.back()->cloneid = 1;
    o.back()->snaps.push_back(1);
    o.back()->overlap.push_back(pair<uint64_t,uint64_t>(0,4096));
    o.back()->overlap.push_back(pair<uint64_t,uint64_t>(8192,4096));
    o.back()->size = 16384;
    o.push_back(new clone_info);
    o.back()->cloneid = CEPH_NOSNAP;
    o.back()->size = 32768;
  }
};
WRITE_CLASS_ENCODER(clone_info)

/**
 * obj list snaps response format
 *
 */
struct obj_list_snap_response_t {
  vector<clone_info> clones;   // ascending
  snapid_t seq;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    ::encode(clones, bl);
    ::encode(seq, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(2, bl);
    ::decode(clones, bl);
    if (struct_v >= 2)
      ::decode(seq, bl);
    else
      seq = CEPH_NOSNAP;
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const {
    f->open_array_section("clones");
    for (vector<clone_info>::const_iterator p = clones.begin(); p != clones.end(); ++p) {
      f->open_object_section("clone");
      p->dump(f);
      f->close_section();
    }
    f->dump_unsigned("seq", seq);
    f->close_section();
  }
  static void generate_test_instances(list<obj_list_snap_response_t*>& o) {
    o.push_back(new obj_list_snap_response_t);
    o.push_back(new obj_list_snap_response_t);
    clone_info cl;
    cl.cloneid = 1;
    cl.snaps.push_back(1);
    cl.overlap.push_back(pair<uint64_t,uint64_t>(0,4096));
    cl.overlap.push_back(pair<uint64_t,uint64_t>(8192,4096));
    cl.size = 16384;
    o.back()->clones.push_back(cl);
    cl.cloneid = CEPH_NOSNAP;
    cl.snaps.clear();
    cl.overlap.clear();
    cl.size = 32768;
    o.back()->clones.push_back(cl);
    o.back()->seq = 123;
  }
};

WRITE_CLASS_ENCODER(obj_list_snap_response_t)

enum scrub_error_type {
  CLEAN,
  DEEP_ERROR,
  SHALLOW_ERROR
};

// PromoteCounter

struct PromoteCounter {
  atomic64_t attempts, objects, bytes;

  void attempt() {
    attempts.inc();
  }

  void finish(uint64_t size) {
    objects.inc();
    bytes.add(size);
  }

  void sample_and_attenuate(uint64_t *a, uint64_t *o, uint64_t *b) {
    *a = attempts.read();
    *o = objects.read();
    *b = bytes.read();
    attempts.set(*a / 2);
    objects.set(*o / 2);
    bytes.set(*b / 2);
  }
};

#endif
