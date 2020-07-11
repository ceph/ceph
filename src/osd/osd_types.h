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

#include <atomic>
#include <sstream>
#include <cstdio>
#include <memory>
#include <string_view>

#include <boost/scoped_ptr.hpp>
#include <boost/optional/optional_io.hpp>
#include <boost/variant.hpp>
#include <boost/smart_ptr/local_shared_ptr.hpp>

#include "include/rados/rados_types.hpp"
#include "include/mempool.h"

#include "msg/msg_types.h"
#include "include/compat.h"
#include "include/types.h"
#include "include/utime.h"
#include "include/CompatSet.h"
#include "common/ceph_context.h"
#include "common/histogram.h"
#include "include/interval_set.h"
#include "include/inline_memory.h"
#include "common/Formatter.h"
#include "common/bloom_filter.hpp"
#include "common/hobject.h"
#include "common/snap_types.h"
#include "HitSet.h"
#include "Watch.h"
#include "include/cmp.h"
#include "librados/ListObjectImpl.h"
#include "compressor/Compressor.h"
#include "osd_perf_counters.h"

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
#define CEPH_OSD_FEATURE_INCOMPAT_MISSING CompatSet::Feature(14, "explicit missing set")
#define CEPH_OSD_FEATURE_INCOMPAT_FASTINFO CompatSet::Feature(15, "fastinfo pg attr")
#define CEPH_OSD_FEATURE_INCOMPAT_RECOVERY_DELETES CompatSet::Feature(16, "deletes in missing set")
#define CEPH_OSD_FEATURE_INCOMPAT_SNAPMAPPER2 CompatSet::Feature(17, "new snapmapper key structure")


/// pool priority range set by user
#define OSD_POOL_PRIORITY_MAX 10
#define OSD_POOL_PRIORITY_MIN -OSD_POOL_PRIORITY_MAX

/// min recovery priority for MBackfillReserve
#define OSD_RECOVERY_PRIORITY_MIN 0

/// base backfill priority for MBackfillReserve
#define OSD_BACKFILL_PRIORITY_BASE 100

/// base backfill priority for MBackfillReserve (degraded PG)
#define OSD_BACKFILL_DEGRADED_PRIORITY_BASE 140

/// base recovery priority for MBackfillReserve
#define OSD_RECOVERY_PRIORITY_BASE 180

/// base backfill priority for MBackfillReserve (inactive PG)
#define OSD_BACKFILL_INACTIVE_PRIORITY_BASE 220

/// base recovery priority for MRecoveryReserve (inactive PG)
#define OSD_RECOVERY_INACTIVE_PRIORITY_BASE 220

/// max manually/automatically set recovery priority for MBackfillReserve
#define OSD_RECOVERY_PRIORITY_MAX 253

/// backfill priority for MBackfillReserve, when forced manually
#define OSD_BACKFILL_PRIORITY_FORCED 254

/// recovery priority for MRecoveryReserve, when forced manually
#define OSD_RECOVERY_PRIORITY_FORCED 255

/// priority for pg deletion when osd is not fullish
#define OSD_DELETE_PRIORITY_NORMAL 179

/// priority for pg deletion when osd is approaching full
#define OSD_DELETE_PRIORITY_FULLISH 219

/// priority when more full
#define OSD_DELETE_PRIORITY_FULL 255

static std::map<int, int> max_prio_map = {
	{OSD_BACKFILL_PRIORITY_BASE, OSD_BACKFILL_DEGRADED_PRIORITY_BASE - 1},
	{OSD_BACKFILL_DEGRADED_PRIORITY_BASE, OSD_RECOVERY_PRIORITY_BASE - 1},
	{OSD_RECOVERY_PRIORITY_BASE, OSD_BACKFILL_INACTIVE_PRIORITY_BASE - 1},
	{OSD_RECOVERY_INACTIVE_PRIORITY_BASE, OSD_RECOVERY_PRIORITY_MAX},
	{OSD_BACKFILL_INACTIVE_PRIORITY_BASE, OSD_RECOVERY_PRIORITY_MAX}
};

typedef hobject_t collection_list_handle_t;

/// convert a single CPEH_OSD_FLAG_* to a std::string
const char *ceph_osd_flag_name(unsigned flag);
/// convert a single CEPH_OSD_OF_FLAG_* to a std::string
const char *ceph_osd_op_flag_name(unsigned flag);

/// convert CEPH_OSD_FLAG_* op flags to a std::string
std::string ceph_osd_flag_string(unsigned flags);
/// conver CEPH_OSD_OP_FLAG_* op flags to a std::string
std::string ceph_osd_op_flag_string(unsigned flags);
/// conver CEPH_OSD_ALLOC_HINT_FLAG_* op flags to a std::string
std::string ceph_osd_alloc_hint_flag_string(unsigned flags);

typedef std::map<std::string,std::string> osd_alert_list_t;
/// map osd id -> alert_list_t
typedef std::map<int, osd_alert_list_t> osd_alerts_t;
void dump(ceph::Formatter* f, const osd_alerts_t& alerts);


typedef interval_set<
  snapid_t,
  mempool::osdmap::flat_map> snap_interval_set_t;


/**
 * osd request identifier
 *
 * caller name + incarnation# + tid to unique identify this request.
 */
struct osd_reqid_t {
  entity_name_t name; // who
  ceph_tid_t    tid;
  int32_t       inc;  // incarnation

  osd_reqid_t()
    : tid(0), inc(0)
  {}
  osd_reqid_t(const entity_name_t& a, int i, ceph_tid_t t)
    : name(a), tid(t), inc(i)
  {}

  DENC(osd_reqid_t, v, p) {
    DENC_START(2, 2, p);
    denc(v.name, p);
    denc(v.tid, p);
    denc(v.inc, p);
    DENC_FINISH(p);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<osd_reqid_t*>& o);
};
WRITE_CLASS_DENC(osd_reqid_t)



struct pg_shard_t {
  static const int32_t NO_OSD = 0x7fffffff;
  int32_t osd;
  shard_id_t shard;
  pg_shard_t() : osd(-1), shard(shard_id_t::NO_SHARD) {}
  explicit pg_shard_t(int osd) : osd(osd), shard(shard_id_t::NO_SHARD) {}
  pg_shard_t(int osd, shard_id_t shard) : osd(osd), shard(shard) {}
  bool is_undefined() const {
    return osd == -1;
  }
  std::string get_osd() const { return (osd == NO_OSD ? "NONE" : std::to_string(osd)); }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("osd", osd);
    if (shard != shard_id_t::NO_SHARD) {
      f->dump_unsigned("shard", shard);
    }
  }
};
WRITE_CLASS_ENCODER(pg_shard_t)
WRITE_EQ_OPERATORS_2(pg_shard_t, osd, shard)
WRITE_CMP_OPERATORS_2(pg_shard_t, osd, shard)
std::ostream& operator<<(std::ostream &lhs, const pg_shard_t &rhs);

class IsPGRecoverablePredicate {
public:
  /**
   * have encodes the shards available
   */
  virtual bool operator()(const std::set<pg_shard_t> &have) const = 0;
  virtual ~IsPGRecoverablePredicate() {}
};

class IsPGReadablePredicate {
public:
  /**
   * have encodes the shards available
   */
  virtual bool operator()(const std::set<pg_shard_t> &have) const = 0;
  virtual ~IsPGReadablePredicate() {}
};

inline std::ostream& operator<<(std::ostream& out, const osd_reqid_t& r) {
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
  std::int64_t pool;     ///< pool id
  std::string key;       ///< key string (if non-empty)
  std::string nspace;    ///< namespace
  std::int64_t hash;     ///< hash position (if >= 0)

  explicit object_locator_t()
    : pool(-1), hash(-1) {}
  explicit object_locator_t(int64_t po)
    : pool(po), hash(-1)  {}
  explicit object_locator_t(int64_t po, int64_t ps)
    : pool(po), hash(ps)  {}
  explicit object_locator_t(int64_t po, std::string_view ns)
    : pool(po), nspace(ns), hash(-1) {}
  explicit object_locator_t(int64_t po, std::string_view ns, int64_t ps)
    : pool(po), nspace(ns), hash(ps) {}
  explicit object_locator_t(int64_t po, std::string_view ns, std::string_view s)
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

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<object_locator_t*>& o);
};
WRITE_CLASS_ENCODER(object_locator_t)

inline bool operator==(const object_locator_t& l, const object_locator_t& r) {
  return l.pool == r.pool && l.key == r.key && l.nspace == r.nspace && l.hash == r.hash;
}
inline bool operator!=(const object_locator_t& l, const object_locator_t& r) {
  return !(l == r);
}

inline std::ostream& operator<<(std::ostream& out, const object_locator_t& loc)
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
  std::string redirect_object; ///< If non-empty, the request goes to this object name

  friend std::ostream& operator<<(std::ostream& out, const request_redirect_t& redir);
public:

  request_redirect_t() {}
  explicit request_redirect_t(const object_locator_t& orig, int64_t rpool) :
      redirect_locator(orig) { redirect_locator.pool = rpool; }
  explicit request_redirect_t(const object_locator_t& rloc) :
      redirect_locator(rloc) {}
  explicit request_redirect_t(const object_locator_t& orig,
                              const std::string& robj) :
      redirect_locator(orig), redirect_object(robj) {}

  bool empty() const { return redirect_locator.empty() &&
			      redirect_object.empty(); }

  void combine_with_locator(object_locator_t& orig, std::string& obj) const {
    orig = redirect_locator;
    if (!redirect_object.empty())
      obj = redirect_object;
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<request_redirect_t*>& o);
};
WRITE_CLASS_ENCODER(request_redirect_t)

inline std::ostream& operator<<(std::ostream& out, const request_redirect_t& redir) {
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
  CEPH_OSD_RMW_FLAG_RWORDERED         = (1 << 10),
  CEPH_OSD_RMW_FLAG_RETURNVEC = (1 << 11),
};


// pg stuff

#define OSD_SUPERBLOCK_GOBJECT ghobject_t(hobject_t(sobject_t(object_t("osd_superblock"), 0)))

// placement seed (a hash value)
typedef uint32_t ps_t;

// old (v1) pg_t encoding (wrap old struct ceph_pg)
struct old_pg_t {
  ceph_pg v;
  void encode(ceph::buffer::list& bl) const {
    ceph::encode_raw(v, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    ceph::decode_raw(v, bl);
  }
};
WRITE_CLASS_ENCODER(old_pg_t)

// placement group id
struct pg_t {
  uint64_t m_pool;
  uint32_t m_seed;

  pg_t() : m_pool(0), m_seed(0) {}
  pg_t(ps_t seed, uint64_t pool) :
    m_pool(pool), m_seed(seed) {}
  // cppcheck-suppress noExplicitConstructor
  pg_t(const ceph_pg& cpg) :
    m_pool(cpg.pool), m_seed(cpg.ps) {}

  // cppcheck-suppress noExplicitConstructor
  pg_t(const old_pg_t& opg) {
    *this = opg.v;
  }

  old_pg_t get_old_pg() const {
    old_pg_t o;
    ceph_assert(m_pool < 0xffffffffull);
    o.v.pool = m_pool;
    o.v.ps = m_seed;
    o.v.preferred = (__s16)-1;
    return o;
  }

  ps_t ps() const {
    return m_seed;
  }
  int64_t pool() const {
    return m_pool;
  }

  static const uint8_t calc_name_buf_size = 36;  // max length for max values len("18446744073709551615.ffffffff") + future suffix len("_head") + '\0'
  char *calc_name(char *buf, const char *suffix_backwords) const;

  void set_ps(ps_t p) {
    m_seed = p;
  }
  void set_pool(uint64_t p) {
    m_pool = p;
  }

  pg_t get_parent() const;
  pg_t get_ancestor(unsigned old_pg_num) const;

  int print(char *o, int maxlen) const;
  bool parse(const char *s);

  bool is_split(unsigned old_pg_num, unsigned new_pg_num, std::set<pg_t> *pchildren) const;

  bool is_merge_source(unsigned old_pg_num, unsigned new_pg_num, pg_t *parent) const;
  bool is_merge_target(unsigned old_pg_num, unsigned new_pg_num) const {
    return ps() < new_pg_num && is_split(new_pg_num, old_pg_num, nullptr);
  }

  /**
   * Returns b such that for all object o:
   *   ~((~0)<<b) & o.hash) == 0 iff o is in the pg for *this
   */
  unsigned get_split_bits(unsigned pg_num) const;

  bool contains(int bits, const ghobject_t& oid) const {
    return
      (int64_t)m_pool == oid.hobj.get_logical_pool() &&
      oid.match(bits, ps());
  }
  bool contains(int bits, const hobject_t& oid) const {
    return
      (int64_t)m_pool == oid.get_logical_pool() &&
      oid.match(bits, ps());
  }

  hobject_t get_hobj_start() const;
  hobject_t get_hobj_end(unsigned pg_num) const;

  // strong ordering is supported
  inline int compare(const pg_t& p) const noexcept {
    if (auto delta = pool() - p.pool(); delta != 0) {
      return delta;
    } else if (ps() < p.ps()) {
      return -1;
    } else if (ps() > p.ps()) {
      return 1;
    } else {
      return 0;
    }
  }

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 v = 1;
    encode(v, bl);
    encode(m_pool, bl);
    encode(m_seed, bl);
    encode((int32_t)-1, bl); // was preferred
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 v;
    decode(v, bl);
    decode(m_pool, bl);
    decode(m_seed, bl);
    bl += sizeof(int32_t); // was preferred
  }
  void decode_old(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    old_pg_t opg;
    decode(opg, bl);
    *this = opg;
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_t*>& o);
};
WRITE_CLASS_ENCODER(pg_t)

inline bool operator<(const pg_t& l, const pg_t& r) {
  return l.compare(r) < 0;
}
inline bool operator<=(const pg_t& l, const pg_t& r) {
  return l.compare(r) <= 0;
}
inline bool operator==(const pg_t& l, const pg_t& r) {
  return l.compare(r) == 0;
}
inline bool operator!=(const pg_t& l, const pg_t& r) {
  return l.compare(r) != 0;
}
inline bool operator>(const pg_t& l, const pg_t& r) {
  return l.compare(r) > 0;
}
inline bool operator>=(const pg_t& l, const pg_t& r) {
  return l.compare(r) >= 0;
}

std::ostream& operator<<(std::ostream& out, const pg_t &pg);

namespace std {
  template<> struct hash< pg_t >
  {
    size_t operator()( const pg_t& x ) const
    {
      static hash<uint32_t> H;
      // xor (s32)-1 in there to preserve original m_preferred result (paranoia!)
      return H((x.pool() & 0xffffffff) ^ (x.pool() >> 32) ^ x.ps() ^ (int32_t)(-1));
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
  void reset_shard(shard_id_t s) {
    shard = s;
  }

  static const uint8_t calc_name_buf_size = pg_t::calc_name_buf_size + 4; // 36 + len('s') + len("255");
  char *calc_name(char *buf, const char *suffix_backwords) const;
 
  bool parse(const char *s);
  bool parse(const std::string& s) {
    return parse(s.c_str());
  }

  spg_t get_ancestor(unsigned old_pg_num) const {
    return spg_t(pgid.get_ancestor(old_pg_num), shard);
  }

  bool is_split(unsigned old_pg_num, unsigned new_pg_num,
		std::set<spg_t> *pchildren) const {
    std::set<pg_t> _children;
    std::set<pg_t> *children = pchildren ? &_children : NULL;
    bool is_split = pgid.is_split(old_pg_num, new_pg_num, children);
    if (pchildren && is_split) {
      for (std::set<pg_t>::iterator i = _children.begin();
	   i != _children.end();
	   ++i) {
	pchildren->insert(spg_t(*i, shard));
      }
    }
    return is_split;
  }
  bool is_merge_target(unsigned old_pg_num, unsigned new_pg_num) const {
    return pgid.is_merge_target(old_pg_num, new_pg_num);
  }
  bool is_merge_source(unsigned old_pg_num, unsigned new_pg_num,
		       spg_t *parent) const {
    spg_t out = *this;
    bool r = pgid.is_merge_source(old_pg_num, new_pg_num, &out.pgid);
    if (r && parent) {
      *parent = out;
    }
    return r;
  }

  bool is_no_shard() const {
    return shard == shard_id_t::NO_SHARD;
  }

  ghobject_t make_pgmeta_oid() const {
    return ghobject_t::make_pgmeta(pgid.pool(), pgid.ps(), shard);
  }

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(pgid, bl);
    encode(shard, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(pgid, bl);
    decode(shard, bl);
    DECODE_FINISH(bl);
  }

  ghobject_t make_temp_ghobject(const std::string& name) const {
    return ghobject_t(
      hobject_t(object_t(name), "", CEPH_NOSNAP,
		pgid.ps(),
		hobject_t::get_temp_pool(pgid.pool()),
		""),
      ghobject_t::NO_GEN,
      shard);
  }

  unsigned hash_to_shard(unsigned num_shards) const {
    return ps() % num_shards;
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

std::ostream& operator<<(std::ostream& out, const spg_t &pg);

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
    return std::string(_str);
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
  int64_t pool() const {
    return pgid.pool();
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
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
    ceph_assert(type == TYPE_PG);
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

  unsigned hash_to_shard(unsigned num_shards) const {
    if (type == TYPE_PG)
      return pgid.hash_to_shard(num_shards);
    return 0;  // whatever.
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<coll_t*>& o);
};

WRITE_CLASS_ENCODER(coll_t)

inline std::ostream& operator<<(std::ostream& out, const coll_t& c) {
  out << c.to_str();
  return out;
}

namespace std {
  template<> struct hash<coll_t> {
    size_t operator()(const coll_t &c) const { 
      size_t h = 0;
      std::string str(c.to_str());
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

inline std::ostream& operator<<(std::ostream& out, const ceph_object_layout &ol)
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

  explicit eversion_t(ceph::buffer::list& bl) : __pad(0) { decode(bl); }

  static const eversion_t& max() {
    static const eversion_t max(-1,-1);
    return max;
  }

  operator ceph_eversion() {
    ceph_eversion c;
    c.epoch = epoch;
    c.version = version;
    return c;
  }

  std::string get_key_name() const;

  // key must point to the beginning of a block of 32 chars
  inline void get_key_name(char* key) const {
    // Below is equivalent of sprintf("%010u.%020llu");
    key[31] = 0;
    ritoa<uint64_t, 10, 20>(version, key + 31);
    key[10] = '.';
    ritoa<uint32_t, 10, 10>(epoch, key + 10);
  }

  void encode(ceph::buffer::list &bl) const {
#if defined(CEPH_LITTLE_ENDIAN)
    bl.append((char *)this, sizeof(version_t) + sizeof(epoch_t));
#else
    using ceph::encode;
    encode(version, bl);
    encode(epoch, bl);
#endif
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
#if defined(CEPH_LITTLE_ENDIAN)
    bl.copy(sizeof(version_t) + sizeof(epoch_t), (char *)this);
#else
    using ceph::decode;
    decode(version, bl);
    decode(epoch, bl);
#endif
  }
  void decode(ceph::buffer::list& bl) {
    auto p = std::cbegin(bl);
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
inline std::ostream& operator<<(std::ostream& out, const eversion_t& e) {
  return out << e.epoch << "'" << e.version;
}

/**
 * objectstore_perf_stat_t
 *
 * current perf information about the osd
 */
struct objectstore_perf_stat_t {
  // cur_op_latency is in ns since double add/sub are not associative
  uint64_t os_commit_latency_ns;
  uint64_t os_apply_latency_ns;

  objectstore_perf_stat_t() :
    os_commit_latency_ns(0), os_apply_latency_ns(0) {}

  bool operator==(const objectstore_perf_stat_t &r) const {
    return os_commit_latency_ns == r.os_commit_latency_ns &&
      os_apply_latency_ns == r.os_apply_latency_ns;
  }

  void add(const objectstore_perf_stat_t &o) {
    os_commit_latency_ns += o.os_commit_latency_ns;
    os_apply_latency_ns += o.os_apply_latency_ns;
  }
  void sub(const objectstore_perf_stat_t &o) {
    os_commit_latency_ns -= o.os_commit_latency_ns;
    os_apply_latency_ns -= o.os_apply_latency_ns;
  }
  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static void generate_test_instances(std::list<objectstore_perf_stat_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(objectstore_perf_stat_t)

/*
 * pg states
 */
#define PG_STATE_CREATING           (1ULL << 0)  // creating
#define PG_STATE_ACTIVE             (1ULL << 1)  // i am active.  (primary: replicas too)
#define PG_STATE_CLEAN              (1ULL << 2)  // peers are complete, clean of stray replicas.
#define PG_STATE_DOWN               (1ULL << 4)  // a needed replica is down, PG offline
#define PG_STATE_RECOVERY_UNFOUND   (1ULL << 5)  // recovery stopped due to unfound
#define PG_STATE_BACKFILL_UNFOUND   (1ULL << 6)  // backfill stopped due to unfound
#define PG_STATE_PREMERGE           (1ULL << 7)  // i am prepare to merging
#define PG_STATE_SCRUBBING          (1ULL << 8)  // scrubbing
//#define PG_STATE_SCRUBQ           (1ULL << 9)  // queued for scrub
#define PG_STATE_DEGRADED           (1ULL << 10) // pg contains objects with reduced redundancy
#define PG_STATE_INCONSISTENT       (1ULL << 11) // pg replicas are inconsistent (but shouldn't be)
#define PG_STATE_PEERING            (1ULL << 12) // pg is (re)peering
#define PG_STATE_REPAIR             (1ULL << 13) // pg should repair on next scrub
#define PG_STATE_RECOVERING         (1ULL << 14) // pg is recovering/migrating objects
#define PG_STATE_BACKFILL_WAIT      (1ULL << 15) // [active] reserving backfill
#define PG_STATE_INCOMPLETE         (1ULL << 16) // incomplete content, peering failed.
#define PG_STATE_STALE              (1ULL << 17) // our state for this pg is stale, unknown.
#define PG_STATE_REMAPPED           (1ULL << 18) // pg is explicitly remapped to different OSDs than CRUSH
#define PG_STATE_DEEP_SCRUB         (1ULL << 19) // deep scrub: check CRC32 on files
#define PG_STATE_BACKFILLING        (1ULL << 20) // [active] backfilling pg content
#define PG_STATE_BACKFILL_TOOFULL   (1ULL << 21) // backfill can't proceed: too full
#define PG_STATE_RECOVERY_WAIT      (1ULL << 22) // waiting for recovery reservations
#define PG_STATE_UNDERSIZED         (1ULL << 23) // pg acting < pool size
#define PG_STATE_ACTIVATING         (1ULL << 24) // pg is peered but not yet active
#define PG_STATE_PEERED             (1ULL << 25) // peered, cannot go active, can recover
#define PG_STATE_SNAPTRIM           (1ULL << 26) // trimming snaps
#define PG_STATE_SNAPTRIM_WAIT      (1ULL << 27) // queued to trim snaps
#define PG_STATE_RECOVERY_TOOFULL   (1ULL << 28) // recovery can't proceed: too full
#define PG_STATE_SNAPTRIM_ERROR     (1ULL << 29) // error stopped trimming snaps
#define PG_STATE_FORCED_RECOVERY    (1ULL << 30) // force recovery of this pg before any other
#define PG_STATE_FORCED_BACKFILL    (1ULL << 31) // force backfill of this pg before any other
#define PG_STATE_FAILED_REPAIR      (1ULL << 32) // A repair failed to fix all errors
#define PG_STATE_LAGGY              (1ULL << 33) // PG is laggy/unreabable due to slow/delayed pings
#define PG_STATE_WAIT               (1ULL << 34) // PG is waiting for prior intervals' readable period to expire

std::string pg_state_string(uint64_t state);
std::string pg_vector_string(const std::vector<int32_t> &a);
std::optional<uint64_t> pg_string_state(const std::string& state);


/*
 * pool_snap_info_t
 *
 * attributes for a single pool snapshot.  
 */
struct pool_snap_info_t {
  snapid_t snapid;
  utime_t stamp;
  std::string name;

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  static void generate_test_instances(std::list<pool_snap_info_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pool_snap_info_t)

inline std::ostream& operator<<(std::ostream& out, const pool_snap_info_t& si) {
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
    SCRUB_PRIORITY,
    COMPRESSION_MODE,
    COMPRESSION_ALGORITHM,
    COMPRESSION_REQUIRED_RATIO,
    COMPRESSION_MAX_BLOB_SIZE,
    COMPRESSION_MIN_BLOB_SIZE,
    CSUM_TYPE,
    CSUM_MAX_BLOCK,
    CSUM_MIN_BLOCK,
    FINGERPRINT_ALGORITHM,
    PG_NUM_MIN,         // min pg_num
    TARGET_SIZE_BYTES,  // total bytes in pool
    TARGET_SIZE_RATIO,  // fraction of total cluster
    PG_AUTOSCALE_BIAS,
    READ_LEASE_INTERVAL,
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

  typedef boost::variant<std::string,int64_t,double> value_t;

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

  void dump(const std::string& name, ceph::Formatter *f) const;

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);

private:
  typedef std::map<key_t, value_t> opts_t;
  opts_t opts;

  friend std::ostream& operator<<(std::ostream& out, const pool_opts_t& opts);
};
WRITE_CLASS_ENCODER_FEATURES(pool_opts_t)

struct pg_merge_meta_t {
  pg_t source_pgid;
  epoch_t ready_epoch = 0;
  epoch_t last_epoch_started = 0;
  epoch_t last_epoch_clean = 0;
  eversion_t source_version;
  eversion_t target_version;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source_pgid, bl);
    encode(ready_epoch, bl);
    encode(last_epoch_started, bl);
    encode(last_epoch_clean, bl);
    encode(source_version, bl);
    encode(target_version, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(source_pgid, p);
    decode(ready_epoch, p);
    decode(last_epoch_started, p);
    decode(last_epoch_clean, p);
    decode(source_version, p);
    decode(target_version, p);
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("source_pgid") << source_pgid;
    f->dump_unsigned("ready_epoch", ready_epoch);
    f->dump_unsigned("last_epoch_started", last_epoch_started);
    f->dump_unsigned("last_epoch_clean", last_epoch_clean);
    f->dump_stream("source_version") << source_version;
    f->dump_stream("target_version") << target_version;
  }
};
WRITE_CLASS_ENCODER(pg_merge_meta_t)

/*
 * pg_pool
 */
struct pg_pool_t {
  static const char *APPLICATION_NAME_CEPHFS;
  static const char *APPLICATION_NAME_RBD;
  static const char *APPLICATION_NAME_RGW;

  enum {
    TYPE_REPLICATED = 1,     // replication
    //TYPE_RAID4 = 2,   // raid4 (never implemented)
    TYPE_ERASURE = 3,      // erasure-coded
  };
  static std::string_view get_type_name(int t) {
    switch (t) {
    case TYPE_REPLICATED: return "replicated";
      //case TYPE_RAID4: return "raid4";
    case TYPE_ERASURE: return "erasure";
    default: return "???";
    }
  }
  std::string_view get_type_name() const {
    return get_type_name(type);
  }

  enum {
    FLAG_HASHPSPOOL = 1<<0, // hash pg seed and pool together (instead of adding)
    FLAG_FULL       = 1<<1, // pool is full
    FLAG_EC_OVERWRITES = 1<<2, // enables overwrites, once enabled, cannot be disabled
    FLAG_INCOMPLETE_CLONES = 1<<3, // may have incomplete clones (bc we are/were an overlay)
    FLAG_NODELETE = 1<<4, // pool can't be deleted
    FLAG_NOPGCHANGE = 1<<5, // pool's pg and pgp num can't be changed
    FLAG_NOSIZECHANGE = 1<<6, // pool's size and min size can't be changed
    FLAG_WRITE_FADVISE_DONTNEED = 1<<7, // write mode with LIBRADOS_OP_FLAG_FADVISE_DONTNEED
    FLAG_NOSCRUB = 1<<8, // block periodic scrub
    FLAG_NODEEP_SCRUB = 1<<9, // block periodic deep-scrub
    FLAG_FULL_QUOTA = 1<<10, // pool is currently running out of quota, will set FLAG_FULL too
    FLAG_NEARFULL = 1<<11, // pool is nearfull
    FLAG_BACKFILLFULL = 1<<12, // pool is backfillfull
    FLAG_SELFMANAGED_SNAPS = 1<<13, // pool uses selfmanaged snaps
    FLAG_POOL_SNAPS = 1<<14,        // pool has pool snaps
    FLAG_CREATING = 1<<15,          // initial pool PGs are being created
  };

  static const char *get_flag_name(int f) {
    switch (f) {
    case FLAG_HASHPSPOOL: return "hashpspool";
    case FLAG_FULL: return "full";
    case FLAG_EC_OVERWRITES: return "ec_overwrites";
    case FLAG_INCOMPLETE_CLONES: return "incomplete_clones";
    case FLAG_NODELETE: return "nodelete";
    case FLAG_NOPGCHANGE: return "nopgchange";
    case FLAG_NOSIZECHANGE: return "nosizechange";
    case FLAG_WRITE_FADVISE_DONTNEED: return "write_fadvise_dontneed";
    case FLAG_NOSCRUB: return "noscrub";
    case FLAG_NODEEP_SCRUB: return "nodeep-scrub";
    case FLAG_FULL_QUOTA: return "full_quota";
    case FLAG_NEARFULL: return "nearfull";
    case FLAG_BACKFILLFULL: return "backfillfull";
    case FLAG_SELFMANAGED_SNAPS: return "selfmanaged_snaps";
    case FLAG_POOL_SNAPS: return "pool_snaps";
    case FLAG_CREATING: return "creating";
    default: return "???";
    }
  }
  static std::string get_flags_string(uint64_t f) {
    std::string s;
    for (unsigned n=0; f && n<64; ++n) {
      if (f & (1ull << n)) {
	if (s.length())
	  s += ",";
	s += get_flag_name(1ull << n);
      }
    }
    return s;
  }
  std::string get_flags_string() const {
    return get_flags_string(flags);
  }
  static uint64_t get_flag_by_name(const std::string& name) {
    if (name == "hashpspool")
      return FLAG_HASHPSPOOL;
    if (name == "full")
      return FLAG_FULL;
    if (name == "ec_overwrites")
      return FLAG_EC_OVERWRITES;
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
    if (name == "full_quota")
      return FLAG_FULL_QUOTA;
    if (name == "nearfull")
      return FLAG_NEARFULL;
    if (name == "backfillfull")
      return FLAG_BACKFILLFULL;
    if (name == "selfmanaged_snaps")
      return FLAG_SELFMANAGED_SNAPS;
    if (name == "pool_snaps")
      return FLAG_POOL_SNAPS;
    if (name == "creating")
      return FLAG_CREATING;
    return 0;
  }

  /// converts the acting/up vector to a set of pg shards
  void convert_to_pg_shards(const std::vector<int> &from, std::set<pg_shard_t>* to) const;

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
  static cache_mode_t get_cache_mode_from_str(const std::string& s) {
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
      ceph_abort_msg("implement me");
    }
  }

  enum class pg_autoscale_mode_t : uint8_t {
    OFF = 0,
    WARN = 1,
    ON = 2,
    UNKNOWN = UINT8_MAX,
  };
  static const char *get_pg_autoscale_mode_name(pg_autoscale_mode_t m) {
    switch (m) {
    case pg_autoscale_mode_t::OFF: return "off";
    case pg_autoscale_mode_t::ON: return "on";
    case pg_autoscale_mode_t::WARN: return "warn";
    default: return "???";
    }
  }
  static pg_autoscale_mode_t get_pg_autoscale_mode_by_name(const std::string& m) {
    if (m == "off") {
      return pg_autoscale_mode_t::OFF;
    }
    if (m == "warn") {
      return pg_autoscale_mode_t::WARN;
    }
    if (m == "on") {
      return pg_autoscale_mode_t::ON;
    }
    return pg_autoscale_mode_t::UNKNOWN;
  }

  utime_t create_time;
  uint64_t flags = 0;           ///< FLAG_*
  __u8 type = 0;                ///< TYPE_*
  __u8 size = 0, min_size = 0;  ///< number of osds in each pg
  __u8 crush_rule = 0;          ///< crush placement rule
  __u8 object_hash = 0;         ///< hash mapping object name to ps
  pg_autoscale_mode_t pg_autoscale_mode = pg_autoscale_mode_t::UNKNOWN;

private:
  __u32 pg_num = 0, pgp_num = 0;  ///< number of pgs
  __u32 pg_num_pending = 0;       ///< pg_num we are about to merge down to
  __u32 pg_num_target = 0;        ///< pg_num we should converge toward
  __u32 pgp_num_target = 0;       ///< pgp_num we should converge toward

public:
  std::map<std::string, std::string> properties;  ///< OBSOLETE
  std::string erasure_code_profile; ///< name of the erasure code profile in OSDMap
  epoch_t last_change = 0;      ///< most recent epoch changed, exclusing snapshot changes

  /// last epoch that forced clients to resend
  epoch_t last_force_op_resend = 0;
  /// last epoch that forced clients to resend (pre-nautilus clients only)
  epoch_t last_force_op_resend_prenautilus = 0;
  /// last epoch that forced clients to resend (pre-luminous clients only)
  epoch_t last_force_op_resend_preluminous = 0;

  /// metadata for the most recent PG merge
  pg_merge_meta_t last_pg_merge_meta;
  
  snapid_t snap_seq = 0;        ///< seq for per-pool snapshot
  epoch_t snap_epoch = 0;       ///< osdmap epoch of last snap
  uint64_t auid = 0;            ///< who owns the pg

  uint64_t quota_max_bytes = 0; ///< maximum number of bytes for this pool
  uint64_t quota_max_objects = 0; ///< maximum number of objects for this pool

  /*
   * Pool snaps (global to this pool).  These define a SnapContext for
   * the pool, unless the client manually specifies an alternate
   * context.
   */
  std::map<snapid_t, pool_snap_info_t> snaps;
  /*
   * Alternatively, if we are defining non-pool snaps (e.g. via the
   * Ceph MDS), we must track @removed_snaps (since @snaps is not
   * used).  Snaps and removed_snaps are to be used exclusive of each
   * other!
   */
  interval_set<snapid_t> removed_snaps;

  unsigned pg_num_mask = 0, pgp_num_mask = 0;

  std::set<uint64_t> tiers;      ///< pools that are tiers of us
  int64_t tier_of = -1;         ///< pool for which we are a tier
  // Note that write wins for read+write ops
  int64_t read_tier = -1;       ///< pool/tier for objecter to direct reads to
  int64_t write_tier = -1;      ///< pool/tier for objecter to direct writes to
  cache_mode_t cache_mode = CACHEMODE_NONE;  ///< cache pool mode

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

  uint64_t target_max_bytes = 0;   ///< tiering: target max pool size
  uint64_t target_max_objects = 0; ///< tiering: target max pool size

  uint32_t cache_target_dirty_ratio_micro = 0; ///< cache: fraction of target to leave dirty
  uint32_t cache_target_dirty_high_ratio_micro = 0; ///< cache: fraction of  target to flush with high speed
  uint32_t cache_target_full_ratio_micro = 0;  ///< cache: fraction of target to fill before we evict in earnest

  uint32_t cache_min_flush_age = 0;  ///< minimum age (seconds) before we can flush
  uint32_t cache_min_evict_age = 0;  ///< minimum age (seconds) before we can evict

  HitSet::Params hit_set_params; ///< The HitSet params to use on this pool
  uint32_t hit_set_period = 0;   ///< periodicity of HitSet segments (seconds)
  uint32_t hit_set_count = 0;    ///< number of periods to retain
  bool use_gmt_hitset = true;	 ///< use gmt to name the hitset archive object
  uint32_t min_read_recency_for_promote = 0;   ///< minimum number of HitSet to check before promote on read
  uint32_t min_write_recency_for_promote = 0;  ///< minimum number of HitSet to check before promote on write
  uint32_t hit_set_grade_decay_rate = 0; ///< current hit_set has highest priority on objects
                                         ///< temperature count,the follow hit_set's priority decay
                                         ///< by this params than pre hit_set
  uint32_t hit_set_search_last_n = 0;    ///< accumulate atmost N hit_sets for temperature

  uint32_t stripe_width = 0;        ///< erasure coded stripe size in bytes

  uint64_t expected_num_objects = 0; ///< expected number of objects on this pool, a value of 0 indicates
                                     ///< user does not specify any expected value
  bool fast_read = false;            ///< whether turn on fast read on the pool or not

  pool_opts_t opts; ///< options

  typedef enum {
    TYPE_FINGERPRINT_NONE = 0,
    TYPE_FINGERPRINT_SHA1 = 1,     
    TYPE_FINGERPRINT_SHA256 = 2,     
    TYPE_FINGERPRINT_SHA512 = 3,     
  } fingerprint_t;
  static fingerprint_t get_fingerprint_from_str(const std::string& s) {
    if (s == "none")
      return TYPE_FINGERPRINT_NONE;
    if (s == "sha1")
      return TYPE_FINGERPRINT_SHA1;
    if (s == "sha256")
      return TYPE_FINGERPRINT_SHA256;
    if (s == "sha512")
      return TYPE_FINGERPRINT_SHA512;
    return (fingerprint_t)-1;
  }
  const fingerprint_t get_fingerprint_type() const {
    std::string fp_str;
    opts.get(pool_opts_t::FINGERPRINT_ALGORITHM, &fp_str);
    return get_fingerprint_from_str(fp_str);
  }
  const char *get_fingerprint_name() const {
    std::string fp_str;
    fingerprint_t fp_t;
    opts.get(pool_opts_t::FINGERPRINT_ALGORITHM, &fp_str);
    fp_t = get_fingerprint_from_str(fp_str);
    return get_fingerprint_name(fp_t);
  }
  static const char *get_fingerprint_name(fingerprint_t m) {
    switch (m) {
    case TYPE_FINGERPRINT_NONE: return "none";
    case TYPE_FINGERPRINT_SHA1: return "sha1";
    case TYPE_FINGERPRINT_SHA256: return "sha256";
    case TYPE_FINGERPRINT_SHA512: return "sha512";
    default: return "unknown";
    }
  }

  /// application -> key/value metadata
  std::map<std::string, std::map<std::string, std::string>> application_metadata;

private:
  std::vector<uint32_t> grade_table;

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

  pg_pool_t() = default;

  void dump(ceph::Formatter *f) const;

  const utime_t &get_create_time() const { return create_time; }
  uint64_t get_flags() const { return flags; }
  bool has_flag(uint64_t f) const { return flags & f; }
  void set_flag(uint64_t f) { flags |= f; }
  void unset_flag(uint64_t f) { flags &= ~f; }

  bool require_rollback() const {
    return is_erasure();
  }

  /// true if incomplete clones may be present
  bool allow_incomplete_clones() const {
    return cache_mode != CACHEMODE_NONE || has_flag(FLAG_INCOMPLETE_CLONES);
  }

  unsigned get_type() const { return type; }
  unsigned get_size() const { return size; }
  unsigned get_min_size() const { return min_size; }
  int get_crush_rule() const { return crush_rule; }
  int get_object_hash() const { return object_hash; }
  const char *get_object_hash_name() const {
    return ceph_str_hash_name(get_object_hash());
  }
  epoch_t get_last_change() const { return last_change; }
  epoch_t get_last_force_op_resend() const { return last_force_op_resend; }
  epoch_t get_last_force_op_resend_prenautilus() const {
    return last_force_op_resend_prenautilus;
  }
  epoch_t get_last_force_op_resend_preluminous() const {
    return last_force_op_resend_preluminous;
  }
  epoch_t get_snap_epoch() const { return snap_epoch; }
  snapid_t get_snap_seq() const { return snap_seq; }
  uint64_t get_auid() const { return auid; }

  void set_snap_seq(snapid_t s) { snap_seq = s; }
  void set_snap_epoch(epoch_t e) { snap_epoch = e; }

  void set_stripe_width(uint32_t s) { stripe_width = s; }
  uint32_t get_stripe_width() const { return stripe_width; }

  bool is_replicated()   const { return get_type() == TYPE_REPLICATED; }
  bool is_erasure() const { return get_type() == TYPE_ERASURE; }

  bool supports_omap() const {
    return !(get_type() == TYPE_ERASURE);
  }

  bool requires_aligned_append() const {
    return is_erasure() && !has_flag(FLAG_EC_OVERWRITES);
  }
  uint64_t required_alignment() const { return stripe_width; }

  bool allows_ecoverwrites() const {
    return has_flag(FLAG_EC_OVERWRITES);
  }

  bool can_shift_osds() const {
    switch (get_type()) {
    case TYPE_REPLICATED:
      return true;
    case TYPE_ERASURE:
      return false;
    default:
      ceph_abort_msg("unhandled pool type");
    }
  }

  unsigned get_pg_num() const { return pg_num; }
  unsigned get_pgp_num() const { return pgp_num; }
  unsigned get_pg_num_target() const { return pg_num_target; }
  unsigned get_pgp_num_target() const { return pgp_num_target; }
  unsigned get_pg_num_pending() const { return pg_num_pending; }

  unsigned get_pg_num_mask() const { return pg_num_mask; }
  unsigned get_pgp_num_mask() const { return pgp_num_mask; }

  // if pg_num is not a multiple of two, pgs are not equally sized.
  // return, for a given pg, the fraction (denominator) of the total
  // pool size that it represents.
  unsigned get_pg_num_divisor(pg_t pgid) const;

  bool is_pending_merge(pg_t pgid, bool *target) const;

  void set_pg_num(int p) {
    pg_num = p;
    pg_num_pending = p;
    calc_pg_masks();
  }
  void set_pgp_num(int p) {
    pgp_num = p;
    calc_pg_masks();
  }
  void set_pg_num_pending(int p) {
    pg_num_pending = p;
    calc_pg_masks();
  }
  void set_pg_num_target(int p) {
    pg_num_target = p;
  }
  void set_pgp_num_target(int p) {
    pgp_num_target = p;
  }
  void dec_pg_num(pg_t source_pgid,
		  epoch_t ready_epoch,
		  eversion_t source_version,
		  eversion_t target_version,
		  epoch_t last_epoch_started,
		  epoch_t last_epoch_clean) {
    --pg_num;
    last_pg_merge_meta.source_pgid = source_pgid;
    last_pg_merge_meta.ready_epoch = ready_epoch;
    last_pg_merge_meta.source_version = source_version;
    last_pg_merge_meta.target_version = target_version;
    last_pg_merge_meta.last_epoch_started = last_epoch_started;
    last_pg_merge_meta.last_epoch_clean = last_epoch_clean;
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

  void set_last_force_op_resend(uint64_t t) {
    last_force_op_resend = t;
    last_force_op_resend_prenautilus = t;
    last_force_op_resend_preluminous = t;
  }

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

  snapid_t snap_exists(std::string_view s) const;
  void add_snap(const char *n, utime_t stamp);
  uint64_t add_unmanaged_snap(bool preoctopus_compat);
  void remove_snap(snapid_t s);
  void remove_unmanaged_snap(snapid_t s, bool preoctopus_compat);

  SnapContext get_snap_context() const;

  /// hash a object name+namespace key to a hash position
  uint32_t hash_key(const std::string& key, const std::string& ns) const;

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

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);

  static void generate_test_instances(std::list<pg_pool_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pg_pool_t)

std::ostream& operator<<(std::ostream& out, const pg_pool_t& p);


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
  int64_t num_legacy_snapsets; ///< upper bound on pre-luminous-style SnapSets
  int64_t num_large_omap_objects = 0;
  int64_t num_objects_manifest = 0;
  int64_t num_omap_bytes = 0;
  int64_t num_omap_keys = 0;
  int64_t num_objects_repaired = 0;

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
      num_objects_missing(0),
      num_legacy_snapsets(0)
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
    FLOOR(num_large_omap_objects);
    FLOOR(num_objects_manifest);
    FLOOR(num_omap_bytes);
    FLOOR(num_omap_keys);
    FLOOR(num_shallow_scrub_errors);
    FLOOR(num_deep_scrub_errors);
    num_scrub_errors = num_shallow_scrub_errors + num_deep_scrub_errors;
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
    FLOOR(num_legacy_snapsets);
    FLOOR(num_objects_repaired);
#undef FLOOR
  }

  void split(std::vector<object_stat_sum_t> &out) const {
#define SPLIT(PARAM)                            \
    for (unsigned i = 0; i < out.size(); ++i) { \
      out[i].PARAM = PARAM / out.size();        \
      if (i < (PARAM % out.size())) {           \
	out[i].PARAM++;                         \
      }                                         \
    }
#define SPLIT_PRESERVE_NONZERO(PARAM)		\
    for (unsigned i = 0; i < out.size(); ++i) { \
      if (PARAM)				\
	out[i].PARAM = 1 + PARAM / out.size();	\
      else					\
	out[i].PARAM = 0;			\
    }

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
    SPLIT(num_large_omap_objects);
    SPLIT(num_objects_manifest);
    SPLIT(num_omap_bytes);
    SPLIT(num_omap_keys);
    SPLIT(num_objects_repaired);
    SPLIT_PRESERVE_NONZERO(num_shallow_scrub_errors);
    SPLIT_PRESERVE_NONZERO(num_deep_scrub_errors);
    for (unsigned i = 0; i < out.size(); ++i) {
      out[i].num_scrub_errors = out[i].num_shallow_scrub_errors +
				out[i].num_deep_scrub_errors;
    }
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
    SPLIT_PRESERVE_NONZERO(num_legacy_snapsets);
#undef SPLIT
#undef SPLIT_PRESERVE_NONZERO
  }

  void clear() {
    // FIPS zeroization audit 20191117: this memset is not security related.
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

  void dump(ceph::Formatter *f) const;
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
        sizeof(num_large_omap_objects) +
        sizeof(num_objects_manifest) +
        sizeof(num_omap_bytes) +
        sizeof(num_omap_keys) +
        sizeof(num_objects_repaired) +
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
        sizeof(num_objects_missing) +
        sizeof(num_legacy_snapsets)
      ,
      "object_stat_sum_t have padding");
  }
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  static void generate_test_instances(std::list<object_stat_sum_t*>& o);
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

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  static void generate_test_instances(std::list<object_stat_collection_t*>& o);

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
  uint64_t state;
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

  std::vector<int32_t> up, acting;
  std::vector<pg_shard_t> avail_no_missing;
  std::map< std::set<pg_shard_t>, int32_t > object_location_counts;
  epoch_t mapping_epoch;

  std::vector<int32_t> blocked_by;  ///< osds on which the pg is blocked

  interval_set<snapid_t> purged_snaps;  ///< recently removed snaps that we've purged

  utime_t last_became_active;
  utime_t last_became_peered;

  /// up, acting primaries
  int32_t up_primary;
  int32_t acting_primary;

  // snaptrimq.size() is 64bit, but let's be serious - anything over 50k is
  // absurd already, so cap it to 2^32 and save 4 bytes at  the same time
  uint32_t snaptrimq_len;

  bool stats_invalid:1;
  /// true if num_objects_dirty is not accurate (because it was not
  /// maintained starting from pool creation)
  bool dirty_stats_invalid:1;
  bool omap_stats_invalid:1;
  bool hitset_stats_invalid:1;
  bool hitset_bytes_stats_invalid:1;
  bool pin_stats_invalid:1;
  bool manifest_stats_invalid:1;

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
      snaptrimq_len(0),
      stats_invalid(false),
      dirty_stats_invalid(false),
      omap_stats_invalid(false),
      hitset_stats_invalid(false),
      hitset_bytes_stats_invalid(false),
      pin_stats_invalid(false),
      manifest_stats_invalid(false)
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

  std::pair<epoch_t, version_t> get_version_pair() const {
    return { reported_epoch, reported_seq };
  }

  void floor(int64_t f) {
    stats.floor(f);
    if (log_size < f)
      log_size = f;
    if (ondisk_log_size < f)
      ondisk_log_size = f;
    if (snaptrimq_len < f)
      snaptrimq_len = f;
  }

  void add_sub_invalid_flags(const pg_stat_t& o) {
    // adding (or subtracting!) invalid stats render our stats invalid too
    stats_invalid |= o.stats_invalid;
    dirty_stats_invalid |= o.dirty_stats_invalid;
    omap_stats_invalid |= o.omap_stats_invalid;
    hitset_stats_invalid |= o.hitset_stats_invalid;
    hitset_bytes_stats_invalid |= o.hitset_bytes_stats_invalid;
    pin_stats_invalid |= o.pin_stats_invalid;
    manifest_stats_invalid |= o.manifest_stats_invalid;
  }
  void add(const pg_stat_t& o) {
    stats.add(o.stats);
    log_size += o.log_size;
    ondisk_log_size += o.ondisk_log_size;
    snaptrimq_len = std::min((uint64_t)snaptrimq_len + o.snaptrimq_len,
                             (uint64_t)(1ull << 31));
    add_sub_invalid_flags(o);
  }
  void sub(const pg_stat_t& o) {
    stats.sub(o.stats);
    log_size -= o.log_size;
    ondisk_log_size -= o.ondisk_log_size;
    if (o.snaptrimq_len < snaptrimq_len) {
      snaptrimq_len -= o.snaptrimq_len;
    } else {
      snaptrimq_len = 0;
    }
    add_sub_invalid_flags(o);
  }

  bool is_acting_osd(int32_t osd, bool primary) const;
  void dump(ceph::Formatter *f) const;
  void dump_brief(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static void generate_test_instances(std::list<pg_stat_t*>& o);
};
WRITE_CLASS_ENCODER(pg_stat_t)

bool operator==(const pg_stat_t& l, const pg_stat_t& r);

/** store_statfs_t
 * ObjectStore full statfs information
 */
struct store_statfs_t
{
  uint64_t total = 0;                  ///< Total bytes
  uint64_t available = 0;              ///< Free bytes available
  uint64_t internally_reserved = 0;    ///< Bytes reserved for internal purposes

  int64_t allocated = 0;               ///< Bytes allocated by the store

  int64_t data_stored = 0;                ///< Bytes actually stored by the user
  int64_t data_compressed = 0;            ///< Bytes stored after compression
  int64_t data_compressed_allocated = 0;  ///< Bytes allocated for compressed data
  int64_t data_compressed_original = 0;   ///< Bytes that were compressed

  int64_t omap_allocated = 0;         ///< approx usage of omap data
  int64_t internal_metadata = 0;      ///< approx usage of internal metadata

  void reset() {
    *this = store_statfs_t();
  }
  void floor(int64_t f) {
#define FLOOR(x) if (int64_t(x) < f) x = f
    FLOOR(total);
    FLOOR(available);
    FLOOR(internally_reserved);
    FLOOR(allocated);
    FLOOR(data_stored);
    FLOOR(data_compressed);
    FLOOR(data_compressed_allocated);
    FLOOR(data_compressed_original);

    FLOOR(omap_allocated);
    FLOOR(internal_metadata);
#undef FLOOR
  }

  bool operator ==(const store_statfs_t& other) const;
  bool is_zero() const {
    return *this == store_statfs_t();
  }

  uint64_t get_used() const {
    return total - available - internally_reserved;
  }

  // this accumulates both actually used and statfs's internally_reserved
  uint64_t get_used_raw() const {
    return total - available;
  }

  float get_used_raw_ratio() const {
    if (total) {
      return (float)get_used_raw() / (float)total;
    } else {
      return 0.0;
    }
  }

  // helpers to ease legacy code porting
  uint64_t kb_avail() const {
    return available >> 10;
  }
  uint64_t kb() const {
    return total >> 10;
  }
  uint64_t kb_used() const {
    return (total - available - internally_reserved) >> 10;
  }
  uint64_t kb_used_raw() const {
    return get_used_raw() >> 10;
  }

  uint64_t kb_used_data() const {
    return allocated >> 10;
  }
  uint64_t kb_used_omap() const {
    return omap_allocated >> 10;
  }

  uint64_t kb_used_internal_metadata() const {
    return internal_metadata >> 10;
  }

  void add(const store_statfs_t& o) {
    total += o.total;
    available += o.available;
    internally_reserved += o.internally_reserved;
    allocated += o.allocated;
    data_stored += o.data_stored;
    data_compressed += o.data_compressed;
    data_compressed_allocated += o.data_compressed_allocated;
    data_compressed_original += o.data_compressed_original;
    omap_allocated += o.omap_allocated;
    internal_metadata += o.internal_metadata;
  }
  void sub(const store_statfs_t& o) {
    total -= o.total;
    available -= o.available;
    internally_reserved -= o.internally_reserved;
    allocated -= o.allocated;
    data_stored -= o.data_stored;
    data_compressed -= o.data_compressed;
    data_compressed_allocated -= o.data_compressed_allocated;
    data_compressed_original -= o.data_compressed_original;
    omap_allocated -= o.omap_allocated;
    internal_metadata -= o.internal_metadata;
  }
  void dump(ceph::Formatter *f) const;
  DENC(store_statfs_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.total, p);
    denc(v.available, p);
    denc(v.internally_reserved, p);
    denc(v.allocated, p);
    denc(v.data_stored, p);
    denc(v.data_compressed, p);
    denc(v.data_compressed_allocated, p);
    denc(v.data_compressed_original, p);
    denc(v.omap_allocated, p);
    denc(v.internal_metadata, p);
    DENC_FINISH(p);
  }
  static void generate_test_instances(std::list<store_statfs_t*>& o);
};
WRITE_CLASS_DENC(store_statfs_t)

std::ostream &operator<<(std::ostream &lhs, const store_statfs_t &rhs);

/** osd_stat
 * aggregate stats for an osd
 */
struct osd_stat_t {
  store_statfs_t statfs;
  std::vector<int> hb_peers;
  int32_t snap_trim_queue_len, num_snap_trimming;
  uint64_t num_shards_repaired;

  pow2_hist_t op_queue_age_hist;

  objectstore_perf_stat_t os_perf_stat;
  osd_alerts_t os_alerts;

  epoch_t up_from = 0;
  uint64_t seq = 0;

  uint32_t num_pgs = 0;

  uint32_t num_osds = 0;
  uint32_t num_per_pool_osds = 0;
  uint32_t num_per_pool_omap_osds = 0;

  struct Interfaces {
    uint32_t last_update;  // in seconds
    uint32_t back_pingtime[3];
    uint32_t back_min[3];
    uint32_t back_max[3];
    uint32_t back_last;
    uint32_t front_pingtime[3];
    uint32_t front_min[3];
    uint32_t front_max[3];
    uint32_t front_last;
  };
  std::map<int, Interfaces> hb_pingtime;  ///< map of osd id to Interfaces

  osd_stat_t() : snap_trim_queue_len(0), num_snap_trimming(0),
       num_shards_repaired(0)	{}

 void add(const osd_stat_t& o) {
    statfs.add(o.statfs);
    snap_trim_queue_len += o.snap_trim_queue_len;
    num_snap_trimming += o.num_snap_trimming;
    num_shards_repaired += o.num_shards_repaired;
    op_queue_age_hist.add(o.op_queue_age_hist);
    os_perf_stat.add(o.os_perf_stat);
    num_pgs += o.num_pgs;
    num_osds += o.num_osds;
    num_per_pool_osds += o.num_per_pool_osds;
    num_per_pool_omap_osds += o.num_per_pool_omap_osds;
    for (const auto& a : o.os_alerts) {
      auto& target = os_alerts[a.first];
      for (auto& i : a.second) {
	target.emplace(i.first, i.second);
      }
    }
  }
  void sub(const osd_stat_t& o) {
    statfs.sub(o.statfs);
    snap_trim_queue_len -= o.snap_trim_queue_len;
    num_snap_trimming -= o.num_snap_trimming;
    num_shards_repaired -= o.num_shards_repaired;
    op_queue_age_hist.sub(o.op_queue_age_hist);
    os_perf_stat.sub(o.os_perf_stat);
    num_pgs -= o.num_pgs;
    num_osds -= o.num_osds;
    num_per_pool_osds -= o.num_per_pool_osds;
    num_per_pool_omap_osds -= o.num_per_pool_omap_osds;
    for (const auto& a : o.os_alerts) {
      auto& target = os_alerts[a.first];
      for (auto& i : a.second) {
        target.erase(i.first);
      }
      if (target.empty()) {
	os_alerts.erase(a.first);
      }
    }
  }
  void dump(ceph::Formatter *f, bool with_net = true) const;
  void dump_ping_time(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static void generate_test_instances(std::list<osd_stat_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(osd_stat_t)

inline bool operator==(const osd_stat_t& l, const osd_stat_t& r) {
  return l.statfs == r.statfs &&
    l.snap_trim_queue_len == r.snap_trim_queue_len &&
    l.num_snap_trimming == r.num_snap_trimming &&
    l.num_shards_repaired == r.num_shards_repaired &&
    l.hb_peers == r.hb_peers &&
    l.op_queue_age_hist == r.op_queue_age_hist &&
    l.os_perf_stat == r.os_perf_stat &&
    l.num_pgs == r.num_pgs &&
    l.num_osds == r.num_osds &&
    l.num_per_pool_osds == r.num_per_pool_osds &&
    l.num_per_pool_omap_osds == r.num_per_pool_omap_osds;
}
inline bool operator!=(const osd_stat_t& l, const osd_stat_t& r) {
  return !(l == r);
}

inline std::ostream& operator<<(std::ostream& out, const osd_stat_t& s) {
  return out << "osd_stat(" << s.statfs << ", "
	     << "peers " << s.hb_peers
	     << " op hist " << s.op_queue_age_hist.h
	     << ")";
}

/*
 * summation over an entire pool
 */
struct pool_stat_t {
  object_stat_collection_t stats;
  store_statfs_t store_stats;
  int64_t log_size;
  int64_t ondisk_log_size;    // >= active_log_size
  int32_t up;       ///< number of up replicas or shards
  int32_t acting;   ///< number of acting replicas or shards
  int32_t num_store_stats; ///< amount of store_stats accumulated

  pool_stat_t() : log_size(0), ondisk_log_size(0), up(0), acting(0),
    num_store_stats(0)
  { }

  void floor(int64_t f) {
    stats.floor(f);
    store_stats.floor(f);
    if (log_size < f)
      log_size = f;
    if (ondisk_log_size < f)
      ondisk_log_size = f;
    if (up < f)
      up = f;
    if (acting < f)
      acting = f;
    if (num_store_stats < f)
      num_store_stats = f;
  }

  void add(const store_statfs_t& o) {
    store_stats.add(o);
    ++num_store_stats;
  }
  void sub(const store_statfs_t& o) {
    store_stats.sub(o);
    --num_store_stats;
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
            store_stats.is_zero() &&
	    log_size == 0 &&
	    ondisk_log_size == 0 &&
	    up == 0 &&
	    acting == 0 &&
	    num_store_stats == 0);
  }

  // helper accessors to retrieve used/netto bytes depending on the
  // collection method: new per-pool objectstore report or legacy PG
  // summation at OSD.
  // In legacy mode used and netto values are the same. But for new per-pool
  // collection 'used' provides amount of space ALLOCATED at all related OSDs 
  // and 'netto' is amount of stored user data.
  uint64_t get_allocated_data_bytes(bool per_pool) const {
    if (per_pool) {
      return store_stats.allocated;
    } else {
      // legacy mode, use numbers from 'stats'
      return stats.sum.num_bytes + stats.sum.num_bytes_hit_set_archive;
    }
  }
  uint64_t get_allocated_omap_bytes(bool per_pool_omap) const {
    if (per_pool_omap) {
      return store_stats.omap_allocated;
    } else {
      // omap is not broken out by pool by nautilus bluestore; report the
      // scrub value.  this will be imprecise in that it won't account for
      // any storage overhead/efficiency.
      return stats.sum.num_omap_bytes;
    }
  }
  uint64_t get_user_data_bytes(float raw_used_rate, ///< space amp factor
			       bool per_pool) const {
    // NOTE: we need the space amp factor so that we can work backwards from
    // the raw utilization to the amount of data that the user actually stored.
    if (per_pool) {
      return raw_used_rate ? store_stats.data_stored / raw_used_rate : 0;
    } else {
      // legacy mode, use numbers from 'stats'.  note that we do NOT use the
      // raw_used_rate factor here because we are working from the PG stats
      // directly.
      return stats.sum.num_bytes + stats.sum.num_bytes_hit_set_archive;
    }
  }
  uint64_t get_user_omap_bytes(float raw_used_rate, ///< space amp factor
			       bool per_pool_omap) const {
    if (per_pool_omap) {
      return raw_used_rate ? store_stats.omap_allocated / raw_used_rate : 0;
    } else {
      // omap usage is lazily reported during scrub; this value may lag.
      return stats.sum.num_omap_bytes;
    }
  }

  void dump(ceph::Formatter *f) const;
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  static void generate_test_instances(std::list<pool_stat_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pool_stat_t)


// -----------------------------------------

/**
 * pg_hit_set_info_t - information about a single recorded HitSet
 *
 * Track basic metadata about a HitSet, like the number of insertions
 * and the time range it covers.
 */
struct pg_hit_set_info_t {
  utime_t begin, end;   ///< time interval
  eversion_t version;   ///< version this HitSet object was written
  bool using_gmt;	///< use gmt for creating the hit_set archive object name

  friend bool operator==(const pg_hit_set_info_t& l,
			 const pg_hit_set_info_t& r) {
    return
      l.begin == r.begin &&
      l.end == r.end &&
      l.version == r.version &&
      l.using_gmt == r.using_gmt;
  }

  explicit pg_hit_set_info_t(bool using_gmt = true)
    : using_gmt(using_gmt) {}

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_hit_set_info_t*>& o);
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
  std::list<pg_hit_set_info_t> history; ///< archived sets, sorted oldest -> newest

  friend bool operator==(const pg_hit_set_history_t& l,
			 const pg_hit_set_history_t& r) {
    return
      l.current_last_update == r.current_last_update &&
      l.history == r.history;
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_hit_set_history_t*>& o);
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
  epoch_t epoch_created = 0;       // epoch in which *pg* was created (pool or pg)
  epoch_t epoch_pool_created = 0;  // epoch in which *pool* was created
			       // (note: may be pg creation epoch for
			       // pre-luminous clusters)
  epoch_t last_epoch_started = 0;;  // lower bound on last epoch started (anywhere, not necessarily locally)
  epoch_t last_interval_started = 0;; // first epoch of last_epoch_started interval
  epoch_t last_epoch_clean = 0;;    // lower bound on last epoch the PG was completely clean.
  epoch_t last_interval_clean = 0;; // first epoch of last_epoch_clean interval
  epoch_t last_epoch_split = 0;;    // as parent or child
  epoch_t last_epoch_marked_full = 0;;  // pool or cluster

  /**
   * In the event of a map discontinuity, same_*_since may reflect the first
   * map the osd has seen in the new map sequence rather than the actual start
   * of the interval.  This is ok since a discontinuity at epoch e means there
   * must have been a clean interval between e and now and that we cannot be
   * in the active set during the interval containing e.
   */
  epoch_t same_up_since = 0;;       // same acting set since
  epoch_t same_interval_since = 0;;   // same acting AND up set since
  epoch_t same_primary_since = 0;;  // same primary at least back through this epoch.

  eversion_t last_scrub;
  eversion_t last_deep_scrub;
  utime_t last_scrub_stamp;
  utime_t last_deep_scrub_stamp;
  utime_t last_clean_scrub_stamp;

  /// upper bound on how long prior interval readable (relative to encode time)
  ceph::timespan prior_readable_until_ub = ceph::timespan::zero();

  friend bool operator==(const pg_history_t& l, const pg_history_t& r) {
    return
      l.epoch_created == r.epoch_created &&
      l.epoch_pool_created == r.epoch_pool_created &&
      l.last_epoch_started == r.last_epoch_started &&
      l.last_interval_started == r.last_interval_started &&
      l.last_epoch_clean == r.last_epoch_clean &&
      l.last_interval_clean == r.last_interval_clean &&
      l.last_epoch_split == r.last_epoch_split &&
      l.last_epoch_marked_full == r.last_epoch_marked_full &&
      l.same_up_since == r.same_up_since &&
      l.same_interval_since == r.same_interval_since &&
      l.same_primary_since == r.same_primary_since &&
      l.last_scrub == r.last_scrub &&
      l.last_deep_scrub == r.last_deep_scrub &&
      l.last_scrub_stamp == r.last_scrub_stamp &&
      l.last_deep_scrub_stamp == r.last_deep_scrub_stamp &&
      l.last_clean_scrub_stamp == r.last_clean_scrub_stamp &&
      l.prior_readable_until_ub == r.prior_readable_until_ub;
  }

  pg_history_t() {}
  pg_history_t(epoch_t created, utime_t stamp)
    : epoch_created(created),
      epoch_pool_created(created),
      same_up_since(created),
      same_interval_since(created),
      same_primary_since(created),
      last_scrub_stamp(stamp),
      last_deep_scrub_stamp(stamp),
      last_clean_scrub_stamp(stamp) {}
  
  bool merge(const pg_history_t &other) {
    // Here, we only update the fields which cannot be calculated from the OSDmap.
    bool modified = false;
    if (epoch_created < other.epoch_created) {
      epoch_created = other.epoch_created;
      modified = true;
    }
    if (epoch_pool_created < other.epoch_pool_created) {
      // FIXME: for jewel compat only; this should either be 0 or always the
      // same value across all pg instances.
      epoch_pool_created = other.epoch_pool_created;
      modified = true;
    }
    if (last_epoch_started < other.last_epoch_started) {
      last_epoch_started = other.last_epoch_started;
      modified = true;
    }
    if (last_interval_started < other.last_interval_started) {
      last_interval_started = other.last_interval_started;
      // if we are learning about a newer *started* interval, our
      // readable_until_ub is obsolete
      prior_readable_until_ub = other.prior_readable_until_ub;
      modified = true;
    } else if (other.last_interval_started == last_interval_started &&
	       other.prior_readable_until_ub < prior_readable_until_ub) {
      // if other is the *same* interval, than pull our upper bound in
      // if they have a tighter bound.
      prior_readable_until_ub = other.prior_readable_until_ub;
      modified = true;
    }
    if (last_epoch_clean < other.last_epoch_clean) {
      last_epoch_clean = other.last_epoch_clean;
      modified = true;
    }
    if (last_interval_clean < other.last_interval_clean) {
      last_interval_clean = other.last_interval_clean;
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
    return modified;
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_history_t*>& o);

  ceph::signedspan refresh_prior_readable_until_ub(
    ceph::signedspan now,  ///< now, relative to osd startup_time
    ceph::signedspan ub) { ///< ub, relative to osd startup_time
    if (now >= ub) {
      // prior interval(s) are unreadable; we can zero the upper bound
      prior_readable_until_ub = ceph::signedspan::zero();
      return ceph::signedspan::zero();
    } else {
      prior_readable_until_ub = ub - now;
      return ub;
    }
  }
  ceph::signedspan get_prior_readable_until_ub(ceph::signedspan now) {
    if (prior_readable_until_ub == ceph::signedspan::zero()) {
      return ceph::signedspan::zero();
    }
    return now + prior_readable_until_ub;
  }
};
WRITE_CLASS_ENCODER(pg_history_t)

inline std::ostream& operator<<(std::ostream& out, const pg_history_t& h) {
  out << "ec=" << h.epoch_created << "/" << h.epoch_pool_created
      << " lis/c=" << h.last_interval_started
      << "/" << h.last_interval_clean
      << " les/c/f=" << h.last_epoch_started << "/" << h.last_epoch_clean
      << "/" << h.last_epoch_marked_full
      << " sis=" << h.same_interval_since;
  if (h.prior_readable_until_ub != ceph::timespan::zero()) {
    out << " pruub=" << h.prior_readable_until_ub;
  }
  return out;
}


/**
 * pg_info_t - summary of PG statistics.
 *
 * some notes: 
 *  - last_complete implies we have all objects that existed as of that
 *    stamp, OR a newer object, OR have already applied a later delete.
 *  - if last_complete >= log.tail, then we know pg contents thru log.head.
 *    otherwise, we have no idea what the pg is supposed to contain.
 */
struct pg_info_t {
  spg_t pgid;
  eversion_t last_update;      ///< last object version applied to store.
  eversion_t last_complete;    ///< last version pg was complete through.
  epoch_t last_epoch_started;  ///< last epoch at which this pg started on this osd
  epoch_t last_interval_started; ///< first epoch of last_epoch_started interval
  
  version_t last_user_version; ///< last user object version applied to store

  eversion_t log_tail;         ///< oldest log entry.

  hobject_t last_backfill;     ///< objects >= this and < last_complete may be missing

  interval_set<snapid_t> purged_snaps;

  pg_stat_t stats;

  pg_history_t history;
  pg_hit_set_history_t hit_set;

  friend bool operator==(const pg_info_t& l, const pg_info_t& r) {
    return
      l.pgid == r.pgid &&
      l.last_update == r.last_update &&
      l.last_complete == r.last_complete &&
      l.last_epoch_started == r.last_epoch_started &&
      l.last_interval_started == r.last_interval_started &&
      l.last_user_version == r.last_user_version &&
      l.log_tail == r.log_tail &&
      l.last_backfill == r.last_backfill &&
      l.purged_snaps == r.purged_snaps &&
      l.stats == r.stats &&
      l.history == r.history &&
      l.hit_set == r.hit_set;
  }

  pg_info_t()
    : last_epoch_started(0),
      last_interval_started(0),
      last_user_version(0),
      last_backfill(hobject_t::get_max())
  { }
  // cppcheck-suppress noExplicitConstructor
  pg_info_t(spg_t p)
    : pgid(p),
      last_epoch_started(0),
      last_interval_started(0),
      last_user_version(0),
      last_backfill(hobject_t::get_max())
  { }
  
  void set_last_backfill(hobject_t pos) {
    last_backfill = pos;
  }

  bool is_empty() const { return last_update.version == 0; }
  bool dne() const { return history.epoch_created == 0; }

  bool has_missing() const { return last_complete != last_update; }
  bool is_incomplete() const { return !last_backfill.is_max(); }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_info_t*>& o);
};
WRITE_CLASS_ENCODER(pg_info_t)

inline std::ostream& operator<<(std::ostream& out, const pg_info_t& pgi) 
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
    out << " lb " << pgi.last_backfill;
  //out << " c " << pgi.epoch_created;
  out << " local-lis/les=" << pgi.last_interval_started
      << "/" << pgi.last_epoch_started;
  out << " n=" << pgi.stats.stats.sum.num_objects;
  out << " " << pgi.history
      << ")";
  return out;
}

/**
 * pg_fast_info_t - common pg_info_t fields
 *
 * These are the fields of pg_info_t (and children) that are updated for
 * most IO operations.
 *
 * ** WARNING **
 * Because we rely on these fields to be applied to the normal
 * info struct, adding a new field here that is not also new in info
 * means that we must set an incompat OSD feature bit!
 */
struct pg_fast_info_t {
  eversion_t last_update;
  eversion_t last_complete;
  version_t last_user_version;
  struct { // pg_stat_t stats
    eversion_t version;
    version_t reported_seq;
    utime_t last_fresh;
    utime_t last_active;
    utime_t last_peered;
    utime_t last_clean;
    utime_t last_unstale;
    utime_t last_undegraded;
    utime_t last_fullsized;
    int64_t log_size;  // (also ondisk_log_size, which has the same value)
    struct { // object_stat_collection_t stats;
      struct { // objct_stat_sum_t sum
	int64_t num_bytes;    // in bytes
	int64_t num_objects;
	int64_t num_object_copies;
	int64_t num_rd;
	int64_t num_rd_kb;
	int64_t num_wr;
	int64_t num_wr_kb;
	int64_t num_objects_dirty;
      } sum;
    } stats;
  } stats;

  void populate_from(const pg_info_t& info) {
    last_update = info.last_update;
    last_complete = info.last_complete;
    last_user_version = info.last_user_version;
    stats.version = info.stats.version;
    stats.reported_seq = info.stats.reported_seq;
    stats.last_fresh = info.stats.last_fresh;
    stats.last_active = info.stats.last_active;
    stats.last_peered = info.stats.last_peered;
    stats.last_clean = info.stats.last_clean;
    stats.last_unstale = info.stats.last_unstale;
    stats.last_undegraded = info.stats.last_undegraded;
    stats.last_fullsized = info.stats.last_fullsized;
    stats.log_size = info.stats.log_size;
    stats.stats.sum.num_bytes = info.stats.stats.sum.num_bytes;
    stats.stats.sum.num_objects = info.stats.stats.sum.num_objects;
    stats.stats.sum.num_object_copies = info.stats.stats.sum.num_object_copies;
    stats.stats.sum.num_rd = info.stats.stats.sum.num_rd;
    stats.stats.sum.num_rd_kb = info.stats.stats.sum.num_rd_kb;
    stats.stats.sum.num_wr = info.stats.stats.sum.num_wr;
    stats.stats.sum.num_wr_kb = info.stats.stats.sum.num_wr_kb;
    stats.stats.sum.num_objects_dirty = info.stats.stats.sum.num_objects_dirty;
  }

  bool try_apply_to(pg_info_t* info) {
    if (last_update <= info->last_update)
      return false;
    info->last_update = last_update;
    info->last_complete = last_complete;
    info->last_user_version = last_user_version;
    info->stats.version = stats.version;
    info->stats.reported_seq = stats.reported_seq;
    info->stats.last_fresh = stats.last_fresh;
    info->stats.last_active = stats.last_active;
    info->stats.last_peered = stats.last_peered;
    info->stats.last_clean = stats.last_clean;
    info->stats.last_unstale = stats.last_unstale;
    info->stats.last_undegraded = stats.last_undegraded;
    info->stats.last_fullsized = stats.last_fullsized;
    info->stats.log_size = stats.log_size;
    info->stats.ondisk_log_size = stats.log_size;
    info->stats.stats.sum.num_bytes = stats.stats.sum.num_bytes;
    info->stats.stats.sum.num_objects = stats.stats.sum.num_objects;
    info->stats.stats.sum.num_object_copies = stats.stats.sum.num_object_copies;
    info->stats.stats.sum.num_rd = stats.stats.sum.num_rd;
    info->stats.stats.sum.num_rd_kb = stats.stats.sum.num_rd_kb;
    info->stats.stats.sum.num_wr = stats.stats.sum.num_wr;
    info->stats.stats.sum.num_wr_kb = stats.stats.sum.num_wr_kb;
    info->stats.stats.sum.num_objects_dirty = stats.stats.sum.num_objects_dirty;
    return true;
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(last_update, bl);
    encode(last_complete, bl);
    encode(last_user_version, bl);
    encode(stats.version, bl);
    encode(stats.reported_seq, bl);
    encode(stats.last_fresh, bl);
    encode(stats.last_active, bl);
    encode(stats.last_peered, bl);
    encode(stats.last_clean, bl);
    encode(stats.last_unstale, bl);
    encode(stats.last_undegraded, bl);
    encode(stats.last_fullsized, bl);
    encode(stats.log_size, bl);
    encode(stats.stats.sum.num_bytes, bl);
    encode(stats.stats.sum.num_objects, bl);
    encode(stats.stats.sum.num_object_copies, bl);
    encode(stats.stats.sum.num_rd, bl);
    encode(stats.stats.sum.num_rd_kb, bl);
    encode(stats.stats.sum.num_wr, bl);
    encode(stats.stats.sum.num_wr_kb, bl);
    encode(stats.stats.sum.num_objects_dirty, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(last_update, p);
    decode(last_complete, p);
    decode(last_user_version, p);
    decode(stats.version, p);
    decode(stats.reported_seq, p);
    decode(stats.last_fresh, p);
    decode(stats.last_active, p);
    decode(stats.last_peered, p);
    decode(stats.last_clean, p);
    decode(stats.last_unstale, p);
    decode(stats.last_undegraded, p);
    decode(stats.last_fullsized, p);
    decode(stats.log_size, p);
    decode(stats.stats.sum.num_bytes, p);
    decode(stats.stats.sum.num_objects, p);
    decode(stats.stats.sum.num_object_copies, p);
    decode(stats.stats.sum.num_rd, p);
    decode(stats.stats.sum.num_rd_kb, p);
    decode(stats.stats.sum.num_wr, p);
    decode(stats.stats.sum.num_wr_kb, p);
    decode(stats.stats.sum.num_objects_dirty, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(pg_fast_info_t)


class OSDMap;
/**
 * PastIntervals -- information needed to determine the PriorSet and
 * the might_have_unfound set
 */
class PastIntervals {
#ifdef WITH_SEASTAR
  using OSDMapRef = boost::local_shared_ptr<const OSDMap>;
#else
  using OSDMapRef = std::shared_ptr<const OSDMap>;
#endif
public:
  struct pg_interval_t {
    std::vector<int32_t> up, acting;
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

    pg_interval_t(
      std::vector<int32_t> &&up,
      std::vector<int32_t> &&acting,
      epoch_t first,
      epoch_t last,
      bool maybe_went_rw,
      int32_t primary,
      int32_t up_primary)
      : up(up), acting(acting), first(first), last(last),
	maybe_went_rw(maybe_went_rw), primary(primary), up_primary(up_primary)
      {}

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<pg_interval_t*>& o);
  };

  PastIntervals();
  PastIntervals(PastIntervals &&rhs) = default;
  PastIntervals &operator=(PastIntervals &&rhs) = default;

  PastIntervals(const PastIntervals &rhs);
  PastIntervals &operator=(const PastIntervals &rhs);

  class interval_rep {
  public:
    virtual size_t size() const = 0;
    virtual bool empty() const = 0;
    virtual void clear() = 0;
    virtual std::pair<epoch_t, epoch_t> get_bounds() const = 0;
    virtual std::set<pg_shard_t> get_all_participants(
      bool ec_pool) const = 0;
    virtual void add_interval(bool ec_pool, const pg_interval_t &interval) = 0;
    virtual std::unique_ptr<interval_rep> clone() const = 0;
    virtual std::ostream &print(std::ostream &out) const = 0;
    virtual void encode(ceph::buffer::list &bl) const = 0;
    virtual void decode(ceph::buffer::list::const_iterator &bl) = 0;
    virtual void dump(ceph::Formatter *f) const = 0;
    virtual void iterate_mayberw_back_to(
      epoch_t les,
      std::function<void(epoch_t, const std::set<pg_shard_t> &)> &&f) const = 0;

    virtual bool has_full_intervals() const { return false; }
    virtual void iterate_all_intervals(
      std::function<void(const pg_interval_t &)> &&f) const {
      ceph_assert(!has_full_intervals());
      ceph_abort_msg("not valid for this implementation");
    }
    virtual void adjust_start_backwards(epoch_t last_epoch_clean) = 0;

    virtual ~interval_rep() {}
  };
  friend class pi_compact_rep;
private:

  std::unique_ptr<interval_rep> past_intervals;

  explicit PastIntervals(interval_rep *rep) : past_intervals(rep) {}

public:
  void add_interval(bool ec_pool, const pg_interval_t &interval) {
    ceph_assert(past_intervals);
    return past_intervals->add_interval(ec_pool, interval);
  }

  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    if (past_intervals) {
      __u8 type = 2;
      encode(type, bl);
      past_intervals->encode(bl);
    } else {
      encode((__u8)0, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator &bl);

  void dump(ceph::Formatter *f) const {
    ceph_assert(past_intervals);
    past_intervals->dump(f);
  }
  static void generate_test_instances(std::list<PastIntervals *> & o);

  /**
   * Determines whether there is an interval change
   */
  static bool is_new_interval(
    int old_acting_primary,
    int new_acting_primary,
    const std::vector<int> &old_acting,
    const std::vector<int> &new_acting,
    int old_up_primary,
    int new_up_primary,
    const std::vector<int> &old_up,
    const std::vector<int> &new_up,
    int old_size,
    int new_size,
    int old_min_size,
    int new_min_size,
    unsigned old_pg_num,
    unsigned new_pg_num,
    unsigned old_pg_num_pending,
    unsigned new_pg_num_pending,
    bool old_sort_bitwise,
    bool new_sort_bitwise,
    bool old_recovery_deletes,
    bool new_recovery_deletes,
    pg_t pgid
    );

  /**
   * Determines whether there is an interval change
   */
  static bool is_new_interval(
    int old_acting_primary,                     ///< [in] primary as of lastmap
    int new_acting_primary,                     ///< [in] primary as of lastmap
    const std::vector<int> &old_acting,              ///< [in] acting as of lastmap
    const std::vector<int> &new_acting,              ///< [in] acting as of osdmap
    int old_up_primary,                         ///< [in] up primary of lastmap
    int new_up_primary,                         ///< [in] up primary of osdmap
    const std::vector<int> &old_up,                  ///< [in] up as of lastmap
    const std::vector<int> &new_up,                  ///< [in] up as of osdmap
    const OSDMap *osdmap,  ///< [in] current map
    const OSDMap *lastmap, ///< [in] last map
    pg_t pgid                                   ///< [in] pgid for pg
    );

  /**
   * Integrates a new map into *past_intervals, returns true
   * if an interval was closed out.
   */
  static bool check_new_interval(
    int old_acting_primary,                     ///< [in] primary as of lastmap
    int new_acting_primary,                     ///< [in] primary as of osdmap
    const std::vector<int> &old_acting,              ///< [in] acting as of lastmap
    const std::vector<int> &new_acting,              ///< [in] acting as of osdmap
    int old_up_primary,                         ///< [in] up primary of lastmap
    int new_up_primary,                         ///< [in] up primary of osdmap
    const std::vector<int> &old_up,                  ///< [in] up as of lastmap
    const std::vector<int> &new_up,                  ///< [in] up as of osdmap
    epoch_t same_interval_since,                ///< [in] as of osdmap
    epoch_t last_epoch_clean,                   ///< [in] current
    const OSDMap *osdmap,      ///< [in] current map
    const OSDMap *lastmap,     ///< [in] last map
    pg_t pgid,                                  ///< [in] pgid for pg
    const IsPGRecoverablePredicate &could_have_gone_active, ///< [in] predicate whether the pg can be active
    PastIntervals *past_intervals,              ///< [out] intervals
    std::ostream *out = 0                            ///< [out] debug ostream
    );
  static bool check_new_interval(
    int old_acting_primary,                     ///< [in] primary as of lastmap
    int new_acting_primary,                     ///< [in] primary as of osdmap
    const std::vector<int> &old_acting,              ///< [in] acting as of lastmap
    const std::vector<int> &new_acting,              ///< [in] acting as of osdmap
    int old_up_primary,                         ///< [in] up primary of lastmap
    int new_up_primary,                         ///< [in] up primary of osdmap
    const std::vector<int> &old_up,                  ///< [in] up as of lastmap
    const std::vector<int> &new_up,                  ///< [in] up as of osdmap
    epoch_t same_interval_since,                ///< [in] as of osdmap
    epoch_t last_epoch_clean,                   ///< [in] current
    OSDMapRef osdmap,      ///< [in] current map
    OSDMapRef lastmap,     ///< [in] last map
    pg_t pgid,                                  ///< [in] pgid for pg
    const IsPGRecoverablePredicate &could_have_gone_active, ///< [in] predicate whether the pg can be active
    PastIntervals *past_intervals,              ///< [out] intervals
    std::ostream *out = 0                            ///< [out] debug ostream
    ) {
    return check_new_interval(
      old_acting_primary, new_acting_primary,
      old_acting, new_acting,
      old_up_primary, new_up_primary,
      old_up, new_up,
      same_interval_since, last_epoch_clean,
      osdmap.get(), lastmap.get(),
      pgid,
      could_have_gone_active,
      past_intervals,
      out);
  }

  friend std::ostream& operator<<(std::ostream& out, const PastIntervals &i);

  template <typename F>
  void iterate_mayberw_back_to(
    epoch_t les,
    F &&f) const {
    ceph_assert(past_intervals);
    past_intervals->iterate_mayberw_back_to(les, std::forward<F>(f));
  }
  void clear() {
    ceph_assert(past_intervals);
    past_intervals->clear();
  }

  /**
   * Should return a value which gives an indication of the amount
   * of state contained
   */
  size_t size() const {
    ceph_assert(past_intervals);
    return past_intervals->size();
  }

  bool empty() const {
    ceph_assert(past_intervals);
    return past_intervals->empty();
  }

  void swap(PastIntervals &other) {
    using std::swap;
    swap(other.past_intervals, past_intervals);
  }

  /**
   * Return all shards which have been in the acting set back to the
   * latest epoch to which we have trimmed except for pg_whoami
   */
  std::set<pg_shard_t> get_might_have_unfound(
    pg_shard_t pg_whoami,
    bool ec_pool) const {
    ceph_assert(past_intervals);
    auto ret = past_intervals->get_all_participants(ec_pool);
    ret.erase(pg_whoami);
    return ret;
  }

  /**
   * Return all shards which we might want to talk to for peering
   */
  std::set<pg_shard_t> get_all_probe(
    bool ec_pool) const {
    ceph_assert(past_intervals);
    return past_intervals->get_all_participants(ec_pool);
  }

  /* Return the set of epochs [start, end) represented by the
   * past_interval set.
   */
  std::pair<epoch_t, epoch_t> get_bounds() const {
    ceph_assert(past_intervals);
    return past_intervals->get_bounds();
  }

  void adjust_start_backwards(epoch_t last_epoch_clean) {
    ceph_assert(past_intervals);
    past_intervals->adjust_start_backwards(last_epoch_clean);
  }

  enum osd_state_t {
    UP,
    DOWN,
    DNE,
    LOST
  };
  struct PriorSet {
    bool ec_pool = false;
    std::set<pg_shard_t> probe; ///< current+prior OSDs we need to probe.
    std::set<int> down;  ///< down osds that would normally be in @a probe and might be interesting.
    std::map<int, epoch_t> blocked_by;  ///< current lost_at values for any OSDs in cur set for which (re)marking them lost would affect cur set

    bool pg_down = false;   ///< some down osds are included in @a cur; the DOWN pg state bit should be set.
    const IsPGRecoverablePredicate* pcontdec = nullptr;

    PriorSet() = default;
    PriorSet(PriorSet &&) = default;
    PriorSet &operator=(PriorSet &&) = default;

    PriorSet &operator=(const PriorSet &) = delete;
    PriorSet(const PriorSet &) = delete;

    bool operator==(const PriorSet &rhs) const {
      return (ec_pool == rhs.ec_pool) &&
	(probe == rhs.probe) &&
	(down == rhs.down) &&
	(blocked_by == rhs.blocked_by) &&
	(pg_down == rhs.pg_down);
    }

    bool affected_by_map(
      const OSDMap &osdmap,
      const DoutPrefixProvider *dpp) const;

    // For verifying tests
    PriorSet(
      bool ec_pool,
      std::set<pg_shard_t> probe,
      std::set<int> down,
      std::map<int, epoch_t> blocked_by,
      bool pg_down,
      const IsPGRecoverablePredicate *pcontdec)
      : ec_pool(ec_pool), probe(probe), down(down), blocked_by(blocked_by),
	pg_down(pg_down), pcontdec(pcontdec) {}

  private:
    template <typename F>
    PriorSet(
      const PastIntervals &past_intervals,
      bool ec_pool,
      epoch_t last_epoch_started,
      const IsPGRecoverablePredicate *c,
      F f,
      const std::vector<int> &up,
      const std::vector<int> &acting,
      const DoutPrefixProvider *dpp);

    friend class PastIntervals;
  };

  template <typename... Args>
  PriorSet get_prior_set(Args&&... args) const {
    return PriorSet(*this, std::forward<Args>(args)...);
  }
};
WRITE_CLASS_ENCODER(PastIntervals)

std::ostream& operator<<(std::ostream& out, const PastIntervals::pg_interval_t& i);
std::ostream& operator<<(std::ostream& out, const PastIntervals &i);
std::ostream& operator<<(std::ostream& out, const PastIntervals::PriorSet &i);

template <typename F>
PastIntervals::PriorSet::PriorSet(
  const PastIntervals &past_intervals,
  bool ec_pool,
  epoch_t last_epoch_started,
  const IsPGRecoverablePredicate *c,
  F f,
  const std::vector<int> &up,
  const std::vector<int> &acting,
  const DoutPrefixProvider *dpp)
  : ec_pool(ec_pool), pg_down(false), pcontdec(c)
{
  /*
   * We have to be careful to gracefully deal with situations like
   * so. Say we have a power outage or something that takes out both
   * OSDs, but the monitor doesn't mark them down in the same epoch.
   * The history may look like
   *
   *  1: A B
   *  2:   B
   *  3:       let's say B dies for good, too (say, from the power spike)
   *  4: A
   *
   * which makes it look like B may have applied updates to the PG
   * that we need in order to proceed.  This sucks...
   *
   * To minimize the risk of this happening, we CANNOT go active if
   * _any_ OSDs in the prior set are down until we send an MOSDAlive
   * to the monitor such that the OSDMap sets osd_up_thru to an epoch.
   * Then, we have something like
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:
   *  4: A
   *
   * -> we can ignore B, bc it couldn't have gone active (alive_thru
   *    still 0).
   *
   * or,
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:   B   up_thru[B]=2
   *  4:
   *  5: A
   *
   * -> we must wait for B, bc it was alive through 2, and could have
   *    written to the pg.
   *
   * If B is really dead, then an administrator will need to manually
   * intervene by marking the OSD as "lost."
   */

  // Include current acting and up nodes... not because they may
  // contain old data (this interval hasn't gone active, obviously),
  // but because we want their pg_info to inform choose_acting(), and
  // so that we know what they do/do not have explicitly before
  // sending them any new info/logs/whatever.
  for (unsigned i = 0; i < acting.size(); i++) {
    if (acting[i] != 0x7fffffff /* CRUSH_ITEM_NONE, can't import crush.h here */)
      probe.insert(pg_shard_t(acting[i], ec_pool ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }
  // It may be possible to exclude the up nodes, but let's keep them in
  // there for now.
  for (unsigned i = 0; i < up.size(); i++) {
    if (up[i] != 0x7fffffff /* CRUSH_ITEM_NONE, can't import crush.h here */)
      probe.insert(pg_shard_t(up[i], ec_pool ? shard_id_t(i) : shard_id_t::NO_SHARD));
  }

  std::set<pg_shard_t> all_probe = past_intervals.get_all_probe(ec_pool);
  ldpp_dout(dpp, 10) << "build_prior all_probe " << all_probe << dendl;
  for (auto &&i: all_probe) {
    switch (f(0, i.osd, nullptr)) {
    case UP: {
      probe.insert(i);
      break;
    }
    case DNE:
    case LOST:
    case DOWN: {
      down.insert(i.osd);
      break;
    }
    }
  }

  past_intervals.iterate_mayberw_back_to(
    last_epoch_started,
    [&](epoch_t start, const std::set<pg_shard_t> &acting) {
      ldpp_dout(dpp, 10) << "build_prior maybe_rw interval:" << start
			 << ", acting: " << acting << dendl;

      // look at candidate osds during this interval.  each falls into
      // one of three categories: up, down (but potentially
      // interesting), or lost (down, but we won't wait for it).
      std::set<pg_shard_t> up_now;
      std::map<int, epoch_t> candidate_blocked_by;
      // any candidates down now (that might have useful data)
      bool any_down_now = false;

      // consider ACTING osds
      for (auto &&so: acting) {
	epoch_t lost_at = 0;
	switch (f(start, so.osd, &lost_at)) {
	case UP: {
	  // include past acting osds if they are up.
	  up_now.insert(so);
	  break;
	}
	case DNE: {
	  ldpp_dout(dpp, 10) << "build_prior  prior osd." << so.osd
			     << " no longer exists" << dendl;
	  break;
	}
	case LOST: {
	  ldpp_dout(dpp, 10) << "build_prior  prior osd." << so.osd
			     << " is down, but lost_at " << lost_at << dendl;
	  up_now.insert(so);
	  break;
	}
	case DOWN: {
	  ldpp_dout(dpp, 10) << "build_prior  prior osd." << so.osd
			     << " is down" << dendl;
	  candidate_blocked_by[so.osd] = lost_at;
	  any_down_now = true;
	  break;
	}
	}
      }

      // if not enough osds survived this interval, and we may have gone rw,
      // then we need to wait for one of those osds to recover to
      // ensure that we haven't lost any information.
      if (!(*pcontdec)(up_now) && any_down_now) {
	// fixme: how do we identify a "clean" shutdown anyway?
	ldpp_dout(dpp, 10) << "build_prior  possibly went active+rw,"
			   << " insufficient up; including down osds" << dendl;
	ceph_assert(!candidate_blocked_by.empty());
	pg_down = true;
	blocked_by.insert(
	  candidate_blocked_by.begin(),
	  candidate_blocked_by.end());
      }
    });

  ldpp_dout(dpp, 10) << "build_prior final: probe " << probe
	   << " down " << down
	   << " blocked_by " << blocked_by
	   << (pg_down ? " pg_down":"")
	   << dendl;
}

struct pg_notify_t {
  epoch_t query_epoch;
  epoch_t epoch_sent;
  pg_info_t info;
  shard_id_t to;
  shard_id_t from;
  PastIntervals past_intervals;
  pg_notify_t() :
    query_epoch(0), epoch_sent(0), to(shard_id_t::NO_SHARD),
    from(shard_id_t::NO_SHARD) {}
  pg_notify_t(
    shard_id_t to,
    shard_id_t from,
    epoch_t query_epoch,
    epoch_t epoch_sent,
    const pg_info_t &info,
    const PastIntervals& pi)
    : query_epoch(query_epoch),
      epoch_sent(epoch_sent),
      info(info), to(to), from(from),
      past_intervals(pi) {
    ceph_assert(from == info.pgid.shard);
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &p);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_notify_t*> &o);
};
WRITE_CLASS_ENCODER(pg_notify_t)
std::ostream &operator<<(std::ostream &lhs, const pg_notify_t &notify);


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
  std::string_view get_type_name() const {
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
    ceph_assert(t != LOG);
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
    ceph_assert(t == LOG);
  }
  
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_query_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pg_query_t)

inline std::ostream& operator<<(std::ostream& out, const pg_query_t& q) {
  out << "query(" << q.get_type_name() << " " << q.since;
  if (q.type == pg_query_t::LOG)
    out << " " << q.history;
  out << " epoch_sent " << q.epoch_sent;
  out << ")";
  return out;
}

/**
 * pg_lease_t - readable lease metadata, from primary -> non-primary
 *
 * This metadata serves to increase either or both of the lease expiration
 * and upper bound on the non-primary.
 */
struct pg_lease_t {
  /// pg readable_until value; replicas must not be readable beyond this
  ceph::signedspan readable_until = ceph::signedspan::zero();

  /// upper bound on any acting osd's readable_until
  ceph::signedspan readable_until_ub = ceph::signedspan::zero();

  /// duration of the lease (in case clock deltas aren't available)
  ceph::signedspan interval = ceph::signedspan::zero();

  pg_lease_t() {}
  pg_lease_t(ceph::signedspan ru, ceph::signedspan ruub,
	     ceph::signedspan i)
    : readable_until(ru),
      readable_until_ub(ruub),
      interval(i) {}

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_lease_t*>& o);

  friend std::ostream& operator<<(std::ostream& out, const pg_lease_t& l) {
    return out << "pg_lease(ru " << l.readable_until
	       << " ub " << l.readable_until_ub
	       << " int " << l.interval << ")";
  }
};
WRITE_CLASS_ENCODER(pg_lease_t)

/**
 * pg_lease_ack_t - lease ack, from non-primary -> primary
 *
 * This metadata acknowledges to the primary what a non-primary's noted
 * upper bound is.
 */
struct pg_lease_ack_t {
  /// highest upper bound non-primary has recorded (primary's clock)
  ceph::signedspan readable_until_ub = ceph::signedspan::zero();

  pg_lease_ack_t() {}
  pg_lease_ack_t(ceph::signedspan ub)
    : readable_until_ub(ub) {}

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_lease_ack_t*>& o);

  friend std::ostream& operator<<(std::ostream& out, const pg_lease_ack_t& l) {
    return out << "pg_lease_ack(ruub " << l.readable_until_ub << ")";
  }
};
WRITE_CLASS_ENCODER(pg_lease_ack_t)



class PGBackend;
class ObjectModDesc {
  bool can_local_rollback;
  bool rollback_info_completed;

  // version required to decode, reflected in encode/decode version
  __u8 max_required_version = 1;
public:
  class Visitor {
  public:
    virtual void append(uint64_t old_offset) {}
    virtual void setattrs(std::map<std::string, std::optional<ceph::buffer::list>> &attrs) {}
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
    virtual void update_snaps(const std::set<snapid_t> &old_snaps) {}
    virtual void rollback_extents(
      version_t gen,
      const std::vector<std::pair<uint64_t, uint64_t> > &extents) {}
    virtual ~Visitor() {}
  };
  void visit(Visitor *visitor) const;
  mutable ceph::buffer::list bl;
  enum ModID {
    APPEND = 1,
    SETATTRS = 2,
    DELETE = 3,
    CREATE = 4,
    UPDATE_SNAPS = 5,
    TRY_DELETE = 6,
    ROLLBACK_EXTENTS = 7
  };
  ObjectModDesc() : can_local_rollback(true), rollback_info_completed(false) {
    bl.reassign_to_mempool(mempool::mempool_osd_pglog);
  }
  void claim(ObjectModDesc &other) {
    bl = std::move(other.bl);
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

    using std::swap;
    swap(other.can_local_rollback, can_local_rollback);
    swap(other.rollback_info_completed, rollback_info_completed);
    swap(other.max_required_version, max_required_version);
  }
  void append_id(ModID id) {
    using ceph::encode;
    uint8_t _id(id);
    encode(_id, bl);
  }
  void append(uint64_t old_size) {
    if (!can_local_rollback || rollback_info_completed)
      return;
    ENCODE_START(1, 1, bl);
    append_id(APPEND);
    encode(old_size, bl);
    ENCODE_FINISH(bl);
  }
  void setattrs(std::map<std::string, std::optional<ceph::buffer::list>> &old_attrs) {
    if (!can_local_rollback || rollback_info_completed)
      return;
    ENCODE_START(1, 1, bl);
    append_id(SETATTRS);
    encode(old_attrs, bl);
    ENCODE_FINISH(bl);
  }
  bool rmobject(version_t deletion_version) {
    if (!can_local_rollback || rollback_info_completed)
      return false;
    ENCODE_START(1, 1, bl);
    append_id(DELETE);
    encode(deletion_version, bl);
    ENCODE_FINISH(bl);
    rollback_info_completed = true;
    return true;
  }
  bool try_rmobject(version_t deletion_version) {
    if (!can_local_rollback || rollback_info_completed)
      return false;
    ENCODE_START(1, 1, bl);
    append_id(TRY_DELETE);
    encode(deletion_version, bl);
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
  void update_snaps(const std::set<snapid_t> &old_snaps) {
    if (!can_local_rollback || rollback_info_completed)
      return;
    ENCODE_START(1, 1, bl);
    append_id(UPDATE_SNAPS);
    encode(old_snaps, bl);
    ENCODE_FINISH(bl);
  }
  void rollback_extents(
    version_t gen, const std::vector<std::pair<uint64_t, uint64_t> > &extents) {
    ceph_assert(can_local_rollback);
    ceph_assert(!rollback_info_completed);
    if (max_required_version < 2)
      max_required_version = 2;
    ENCODE_START(2, 2, bl);
    append_id(ROLLBACK_EXTENTS);
    encode(gen, bl);
    encode(extents, bl);
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

  bool requires_kraken() const {
    return max_required_version >= 2;
  }

  /**
   * Create fresh copy of bl bytes to avoid keeping large buffers around
   * in the case that bl contains ptrs which point into a much larger
   * message buffer
   */
  void trim_bl() const {
    if (bl.length() > 0)
      bl.rebuild();
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ObjectModDesc*>& o);
};
WRITE_CLASS_ENCODER(ObjectModDesc)

class ObjectCleanRegions {
private:
  bool new_object;
  bool clean_omap;
  interval_set<uint64_t> clean_offsets;
  static std::atomic<uint32_t> max_num_intervals;

  /**
   * trim the number of intervals if clean_offsets.num_intervals()
   * exceeds the given upbound max_num_intervals
   * etc. max_num_intervals=2, clean_offsets:{[5~10], [20~5]}
   * then new interval [30~10] will evict out the shortest one [20~5]
   * finally, clean_offsets becomes {[5~10], [30~10]}
   */
  void trim();
  friend std::ostream& operator<<(std::ostream& out, const ObjectCleanRegions& ocr);
public:
  ObjectCleanRegions() : new_object(false), clean_omap(true) {
    clean_offsets.insert(0, (uint64_t)-1);
  }
  ObjectCleanRegions(uint64_t offset, uint64_t len, bool co)
    : new_object(false), clean_omap(co) {
    clean_offsets.insert(offset, len);
  }
  bool operator==(const ObjectCleanRegions &orc) const {
    return new_object == orc.new_object && clean_omap == orc.clean_omap && clean_offsets == orc.clean_offsets;
  }
  static void set_max_num_intervals(uint32_t num);
  void merge(const ObjectCleanRegions &other);
  void mark_data_region_dirty(uint64_t offset, uint64_t len);
  void mark_omap_dirty();
  void mark_object_new();
  void mark_fully_dirty();
  interval_set<uint64_t> get_dirty_regions() const;
  bool omap_is_dirty() const;
  bool object_is_exist() const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ObjectCleanRegions*>& o);
};
WRITE_CLASS_ENCODER(ObjectCleanRegions)
std::ostream& operator<<(std::ostream& out, const ObjectCleanRegions& ocr);


struct OSDOp {
  ceph_osd_op op;
  sobject_t soid;

  ceph::buffer::list indata, outdata;
  errorcode32_t rval = 0;

  OSDOp() {
    // FIPS zeroization audit 20191115: this memset clean for security
    memset(&op, 0, sizeof(ceph_osd_op));
  }

  OSDOp(const int op_code) {
    // FIPS zeroization audit 20191115: this memset clean for security
    memset(&op, 0, sizeof(ceph_osd_op));
    op.op = op_code;
  }

  /**
   * split a ceph::buffer::list into constituent indata members of a vector of OSDOps
   *
   * @param ops [out] vector of OSDOps
   * @param in  [in] combined data buffer
   */
  template<typename V>
  static void split_osd_op_vector_in_data(V& ops,
					  ceph::buffer::list& in) {
    ceph::buffer::list::iterator datap = in.begin();
    for (unsigned i = 0; i < ops.size(); i++) {
      if (ops[i].op.payload_len) {
	datap.copy(ops[i].op.payload_len, ops[i].indata);
      }
    }
  }

  /**
   * merge indata members of a vector of OSDOp into a single ceph::buffer::list
   *
   * Notably this also encodes certain other OSDOp data into the data
   * buffer, including the sobject_t soid.
   *
   * @param ops [in] vector of OSDOps
   * @param out [out] combined data buffer
   */
  template<typename V>
  static void merge_osd_op_vector_in_data(V& ops, ceph::buffer::list& out) {
    for (unsigned i = 0; i < ops.size(); i++) {
      if (ops[i].indata.length()) {
	ops[i].op.payload_len = ops[i].indata.length();
	out.append(ops[i].indata);
      }
    }
  }

  /**
   * split a ceph::buffer::list into constituent outdata members of a vector of OSDOps
   *
   * @param ops [out] vector of OSDOps
   * @param in  [in] combined data buffer
   */
  static void split_osd_op_vector_out_data(std::vector<OSDOp>& ops, ceph::buffer::list& in);

  /**
   * merge outdata members of a vector of OSDOps into a single ceph::buffer::list
   *
   * @param ops [in] vector of OSDOps
   * @param out [out] combined data buffer
   */
  static void merge_osd_op_vector_out_data(std::vector<OSDOp>& ops, ceph::buffer::list& out);

  /**
   * Clear data as much as possible, leave minimal data for historical op dump
   *
   * @param ops [in] vector of OSDOps
   */
  template<typename V>
  static void clear_data(V& ops) {
    for (unsigned i = 0; i < ops.size(); i++) {
      OSDOp& op = ops[i];
      op.outdata.clear();
      if (ceph_osd_op_type_attr(op.op.op) &&
	  op.op.xattr.name_len &&
	  op.indata.length() >= op.op.xattr.name_len) {
	ceph::buffer::list bl;
	bl.push_back(ceph::buffer::ptr_node::create(op.op.xattr.name_len));
	bl.begin().copy_in(op.op.xattr.name_len, op.indata);
	op.indata = std::move(bl);
      } else if (ceph_osd_op_type_exec(op.op.op) &&
		 op.op.cls.class_len &&
		 op.indata.length() >
	         (op.op.cls.class_len + op.op.cls.method_len)) {
	__u8 len = op.op.cls.class_len + op.op.cls.method_len;
	ceph::buffer::list bl;
	bl.push_back(ceph::buffer::ptr_node::create(len));
	bl.begin().copy_in(len, op.indata);
	op.indata = std::move(bl);
      } else {
	op.indata.clear();
      }
    }
  }
};
std::ostream& operator<<(std::ostream& out, const OSDOp& op);

struct pg_log_op_return_item_t {
  int32_t rval;
  ceph::buffer::list bl;
  void encode(ceph::buffer::list& p) const {
    using ceph::encode;
    encode(rval, p);
    encode(bl, p);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    decode(rval, p);
    decode(bl, p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_int("rval", rval);
    f->dump_unsigned("bl_length", bl.length());
  }
  friend bool operator==(const pg_log_op_return_item_t& lhs,
			 const pg_log_op_return_item_t& rhs) {
    return lhs.rval == rhs.rval &&
      lhs.bl.contents_equal(rhs.bl);
  }
  friend bool operator!=(const pg_log_op_return_item_t& lhs,
			 const pg_log_op_return_item_t& rhs) {
    return !(lhs == rhs);
  }
  friend std::ostream& operator<<(std::ostream& out, const pg_log_op_return_item_t& i) {
    return out << "r=" << i.rval << "+" << i.bl.length() << "b";
  }
};
WRITE_CLASS_ENCODER(pg_log_op_return_item_t)

/**
 * pg_log_entry_t - single entry/event in pg log
 *
 */
struct pg_log_entry_t {
  enum {
    MODIFY = 1,   // some unspecified modification (but not *all* modifications)
    CLONE = 2,    // cloned object from head
    DELETE = 3,   // deleted object
    //BACKLOG = 4,  // event invented by generate_backlog [obsolete]
    LOST_REVERT = 5, // lost new version, revert to an older version.
    LOST_DELETE = 6, // lost new version, revert to no object (deleted).
    LOST_MARK = 7,   // lost new version, now EIO
    PROMOTE = 8,     // promoted object from another tier
    CLEAN = 9,       // mark an object clean
    ERROR = 10,      // write that returned an error
  };
  static const char *get_op_name(int op) {
    switch (op) {
    case MODIFY:
      return "modify";
    case PROMOTE:
      return "promote";
    case CLONE:
      return "clone";
    case DELETE:
      return "delete";
    case LOST_REVERT:
      return "l_revert";
    case LOST_DELETE:
      return "l_delete";
    case LOST_MARK:
      return "l_mark";
    case CLEAN:
      return "clean";
    case ERROR:
      return "error";
    default:
      return "unknown";
    }
  }
  const char *get_op_name() const {
    return get_op_name(op);
  }

  // describes state for a locally-rollbackable entry
  ObjectModDesc mod_desc;
  ceph::buffer::list snaps;   // only for clone entries
  hobject_t  soid;
  osd_reqid_t reqid;  // caller+tid to uniquely identify request
  mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > extra_reqids;

  /// map extra_reqids by index to error return code (if any)
  mempool::osd_pglog::map<uint32_t, int> extra_reqid_return_codes;

  eversion_t version, prior_version, reverting_to;
  version_t user_version; // the user version for this entry
  utime_t     mtime;  // this is the _user_ mtime, mind you
  int32_t return_code; // only stored for ERRORs for dup detection

  std::vector<pg_log_op_return_item_t> op_returns;

  __s32      op;
  bool invalid_hash; // only when decoding sobject_t based entries
  bool invalid_pool; // only when decoding pool-less hobject based entries
  ObjectCleanRegions clean_regions;

  pg_log_entry_t()
   : user_version(0), return_code(0), op(0),
     invalid_hash(false), invalid_pool(false) {
    snaps.reassign_to_mempool(mempool::mempool_osd_pglog);
  }
  pg_log_entry_t(int _op, const hobject_t& _soid,
                const eversion_t& v, const eversion_t& pv,
                version_t uv,
                const osd_reqid_t& rid, const utime_t& mt,
                int return_code)
   : soid(_soid), reqid(rid), version(v), prior_version(pv), user_version(uv),
     mtime(mt), return_code(return_code), op(_op),
     invalid_hash(false), invalid_pool(false) {
    snaps.reassign_to_mempool(mempool::mempool_osd_pglog);
  }
      
  bool is_clone() const { return op == CLONE; }
  bool is_modify() const { return op == MODIFY; }
  bool is_promote() const { return op == PROMOTE; }
  bool is_clean() const { return op == CLEAN; }
  bool is_lost_revert() const { return op == LOST_REVERT; }
  bool is_lost_delete() const { return op == LOST_DELETE; }
  bool is_lost_mark() const { return op == LOST_MARK; }
  bool is_error() const { return op == ERROR; }

  bool is_update() const {
    return
      is_clone() || is_modify() || is_promote() || is_clean() ||
      is_lost_revert() || is_lost_mark();
  }
  bool is_delete() const {
    return op == DELETE || op == LOST_DELETE;
  }

  bool can_rollback() const {
    return mod_desc.can_rollback();
  }

  void mark_unrollbackable() {
    mod_desc.mark_unrollbackable();
  }

  bool requires_kraken() const {
    return mod_desc.requires_kraken();
  }

  // Errors are only used for dup detection, whereas
  // the index by objects is used by recovery, copy_get,
  // and other facilities that don't expect or need to
  // be aware of error entries.
  bool object_is_indexed() const {
    return !is_error();
  }

  bool reqid_is_indexed() const {
    return reqid != osd_reqid_t() &&
      (op == MODIFY || op == DELETE || op == ERROR);
  }

  void set_op_returns(const std::vector<OSDOp>& ops) {
    op_returns.resize(ops.size());
    for (unsigned i = 0; i < ops.size(); ++i) {
      op_returns[i].rval = ops[i].rval;
      op_returns[i].bl = ops[i].outdata;
    }
  }

  std::string get_key_name() const;
  void encode_with_checksum(ceph::buffer::list& bl) const;
  void decode_with_checksum(ceph::buffer::list::const_iterator& p);

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_log_entry_t*>& o);

};
WRITE_CLASS_ENCODER(pg_log_entry_t)

std::ostream& operator<<(std::ostream& out, const pg_log_entry_t& e);

struct pg_log_dup_t {
  osd_reqid_t reqid;  // caller+tid to uniquely identify request
  eversion_t version;
  version_t user_version; // the user version for this entry
  int32_t return_code; // only stored for ERRORs for dup detection

  std::vector<pg_log_op_return_item_t> op_returns;

  pg_log_dup_t()
    : user_version(0), return_code(0)
  {}
  explicit pg_log_dup_t(const pg_log_entry_t& entry)
    : reqid(entry.reqid), version(entry.version),
      user_version(entry.user_version),
      return_code(entry.return_code),
      op_returns(entry.op_returns)
  {}
  pg_log_dup_t(const eversion_t& v, version_t uv,
	       const osd_reqid_t& rid, int return_code)
    : reqid(rid), version(v), user_version(uv),
      return_code(return_code)
  {}

  std::string get_key_name() const;
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_log_dup_t*>& o);

  bool operator==(const pg_log_dup_t &rhs) const {
    return reqid == rhs.reqid &&
      version == rhs.version &&
      user_version == rhs.user_version &&
      return_code == rhs.return_code &&
      op_returns == rhs.op_returns;
  }
  bool operator!=(const pg_log_dup_t &rhs) const {
    return !(*this == rhs);
  }

  friend std::ostream& operator<<(std::ostream& out, const pg_log_dup_t& e);
};
WRITE_CLASS_ENCODER(pg_log_dup_t)

std::ostream& operator<<(std::ostream& out, const pg_log_dup_t& e);

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

protected:
  // We can rollback rollback-able entries > can_rollback_to
  eversion_t can_rollback_to;

  // always <= can_rollback_to, indicates how far stashed rollback
  // data can be found
  eversion_t rollback_info_trimmed_to;

public:
  // the actual log
  mempool::osd_pglog::list<pg_log_entry_t> log;

  // entries just for dup op detection ordered oldest to newest
  mempool::osd_pglog::list<pg_log_dup_t> dups;

  pg_log_t() = default;
  pg_log_t(const eversion_t &last_update,
	   const eversion_t &log_tail,
	   const eversion_t &can_rollback_to,
	   const eversion_t &rollback_info_trimmed_to,
	   mempool::osd_pglog::list<pg_log_entry_t> &&entries,
	   mempool::osd_pglog::list<pg_log_dup_t> &&dup_entries)
    : head(last_update), tail(log_tail), can_rollback_to(can_rollback_to),
      rollback_info_trimmed_to(rollback_info_trimmed_to),
      log(std::move(entries)), dups(std::move(dup_entries)) {}
  pg_log_t(const eversion_t &last_update,
	   const eversion_t &log_tail,
	   const eversion_t &can_rollback_to,
	   const eversion_t &rollback_info_trimmed_to,
	   const std::list<pg_log_entry_t> &entries,
	   const std::list<pg_log_dup_t> &dup_entries)
    : head(last_update), tail(log_tail), can_rollback_to(can_rollback_to),
      rollback_info_trimmed_to(rollback_info_trimmed_to) {
    for (auto &&entry: entries) {
      log.push_back(entry);
    }
    for (auto &&entry: dup_entries) {
      dups.push_back(entry);
    }
  }

  void clear() {
    eversion_t z;
    rollback_info_trimmed_to = can_rollback_to = head = tail = z;
    log.clear();
    dups.clear();
  }

  eversion_t get_rollback_info_trimmed_to() const {
    return rollback_info_trimmed_to;
  }
  eversion_t get_can_rollback_to() const {
    return can_rollback_to;
  }


  pg_log_t split_out_child(pg_t child_pgid, unsigned split_bits) {
    mempool::osd_pglog::list<pg_log_entry_t> oldlog, childlog;
    oldlog.swap(log);

    eversion_t old_tail;
    unsigned mask = ~((~0)<<split_bits);
    for (auto i = oldlog.begin();
	 i != oldlog.end();
      ) {
      if ((i->soid.get_hash() & mask) == child_pgid.m_seed) {
	childlog.push_back(*i);
      } else {
	log.push_back(*i);
      }
      oldlog.erase(i++);
    }

    // osd_reqid is unique, so it doesn't matter if there are extra
    // dup entries in each pg. To avoid storing oid with the dup
    // entries, just copy the whole list.
    auto childdups(dups);

    return pg_log_t(
      head,
      tail,
      can_rollback_to,
      rollback_info_trimmed_to,
      std::move(childlog),
      std::move(childdups));
    }

  mempool::osd_pglog::list<pg_log_entry_t> rewind_from_head(eversion_t newhead) {
    ceph_assert(newhead >= tail);

    mempool::osd_pglog::list<pg_log_entry_t>::iterator p = log.end();
    mempool::osd_pglog::list<pg_log_entry_t> divergent;
    while (true) {
      if (p == log.begin()) {
	// yikes, the whole thing is divergent!
	using std::swap;
	swap(divergent, log);
	break;
      }
      --p;
      if (p->version.version <= newhead.version) {
	/*
	 * look at eversion.version here.  we want to avoid a situation like:
	 *  our log: 100'10 (0'0) m 10000004d3a.00000000/head by client4225.1:18529
	 *  new log: 122'10 (0'0) m 10000004d3a.00000000/head by client4225.1:18529
	 *  lower_bound = 100'9
	 * i.e, same request, different version.  If the eversion.version is > the
	 * lower_bound, we it is divergent.
	 */
	++p;
	divergent.splice(divergent.begin(), log, p, log.end());
	break;
      }
      ceph_assert(p->version > newhead);
    }
    head = newhead;

    if (can_rollback_to > newhead)
      can_rollback_to = newhead;

    if (rollback_info_trimmed_to > newhead)
      rollback_info_trimmed_to = newhead;

    return divergent;
  }

  void merge_from(const std::vector<pg_log_t*>& slogs, eversion_t last_update) {
    log.clear();

    // sort and merge dups
    std::multimap<eversion_t,pg_log_dup_t> sorted;
    for (auto& d : dups) {
      sorted.emplace(d.version, d);
    }
    for (auto l : slogs) {
      for (auto& d : l->dups) {
	sorted.emplace(d.version, d);
      }
    }
    dups.clear();
    for (auto& i : sorted) {
      dups.push_back(i.second);
    }

    head = last_update;
    tail = last_update;
    can_rollback_to = last_update;
    rollback_info_trimmed_to = last_update;
  }

  bool empty() const {
    return log.empty();
  }

  bool null() const {
    return head.version == 0 && head.epoch == 0;
  }

  uint64_t approx_size() const {
    return head.version - tail.version;
  }

  static void filter_log(spg_t import_pgid, const OSDMap &curmap,
    const std::string &hit_set_namespace, const pg_log_t &in,
    pg_log_t &out, pg_log_t &reject);

  /**
   * copy entries from the tail of another pg_log_t
   *
   * @param other pg_log_t to copy from
   * @param from copy entries after this version
   */
  void copy_after(CephContext* cct, const pg_log_t &other, eversion_t from);

  /**
   * copy up to N entries
   *
   * @param other source log
   * @param max max number of entries to copy
   */
  void copy_up_to(CephContext* cct, const pg_log_t &other, int max);

  std::ostream& print(std::ostream& out) const;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl, int64_t pool = -1);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_log_t*>& o);
};
WRITE_CLASS_ENCODER(pg_log_t)

inline std::ostream& operator<<(std::ostream& out, const pg_log_t& log)
{
  out << "log((" << log.tail << "," << log.head << "], crt="
      << log.get_can_rollback_to() << ")";
  return out;
}


/**
 * pg_missing_t - summary of missing objects.
 *
 *  kept in memory, as a supplement to pg_log_t
 *  also used to pass missing info in messages.
 */
struct pg_missing_item {
  eversion_t need, have;
  ObjectCleanRegions clean_regions;
  enum missing_flags_t {
    FLAG_NONE = 0,
    FLAG_DELETE = 1,
  } flags;
  pg_missing_item() : flags(FLAG_NONE) {}
  explicit pg_missing_item(eversion_t n) : need(n), flags(FLAG_NONE) {}  // have no old version
  pg_missing_item(eversion_t n, eversion_t h, bool is_delete=false, bool old_style = false) :
    need(n), have(h) {
    set_delete(is_delete);
    if (old_style)
      clean_regions.mark_fully_dirty();
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const {
    using ceph::encode;
    if (HAVE_FEATURE(features, SERVER_OCTOPUS)) {
      // encoding a zeroed eversion_t to differentiate between OSD_RECOVERY_DELETES、
      // SERVER_OCTOPUS and legacy unversioned encoding - a need value of 0'0 is not
      // possible. This can be replaced with the legacy encoding
      encode(eversion_t(), bl);
      encode(eversion_t(-1, -1), bl);
      encode(need, bl);
      encode(have, bl);   
      encode(static_cast<uint8_t>(flags), bl);
      encode(clean_regions, bl);
    } else {
      encode(eversion_t(), bl);
      encode(need, bl);
      encode(have, bl);
      encode(static_cast<uint8_t>(flags), bl);
    }
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    eversion_t e, l;
    decode(e, bl);
    decode(l, bl);
    if(l == eversion_t(-1, -1)) {
      // support all
      decode(need, bl);
      decode(have, bl);
      uint8_t f;
      decode(f, bl);
      flags = static_cast<missing_flags_t>(f);
      decode(clean_regions, bl);
     } else {
      // support OSD_RECOVERY_DELETES
      need = l;
      decode(have, bl);
      uint8_t f;
      decode(f, bl);
      flags = static_cast<missing_flags_t>(f); 
      clean_regions.mark_fully_dirty();
    }
  }

  void set_delete(bool is_delete) {
    flags = is_delete ? FLAG_DELETE : FLAG_NONE;
  }

  bool is_delete() const {
    return (flags & FLAG_DELETE) == FLAG_DELETE;
  }

  std::string flag_str() const {
    if (flags == FLAG_NONE) {
      return "none";
    } else {
      return "delete";
    }
  }

  void dump(ceph::Formatter *f) const {
    f->dump_stream("need") << need;
    f->dump_stream("have") << have;
    f->dump_stream("flags") << flag_str();
    f->dump_stream("clean_regions") << clean_regions;
  }
  static void generate_test_instances(std::list<pg_missing_item*>& o) {
    o.push_back(new pg_missing_item);
    o.push_back(new pg_missing_item);
    o.back()->need = eversion_t(1, 2);
    o.back()->have = eversion_t(1, 1);
    o.push_back(new pg_missing_item);
    o.back()->need = eversion_t(3, 5);
    o.back()->have = eversion_t(3, 4);
    o.back()->clean_regions.mark_data_region_dirty(4096, 8192);
    o.back()->clean_regions.mark_omap_dirty();
    o.back()->flags = FLAG_DELETE;
  }
  bool operator==(const pg_missing_item &rhs) const {
    return need == rhs.need && have == rhs.have && flags == rhs.flags;
  }
  bool operator!=(const pg_missing_item &rhs) const {
    return !(*this == rhs);
  }
};
WRITE_CLASS_ENCODER_FEATURES(pg_missing_item)
std::ostream& operator<<(std::ostream& out, const pg_missing_item &item);

class pg_missing_const_i {
public:
  virtual const std::map<hobject_t, pg_missing_item> &
    get_items() const = 0;
  virtual const std::map<version_t, hobject_t> &get_rmissing() const = 0;
  virtual bool get_may_include_deletes() const = 0;
  virtual unsigned int num_missing() const = 0;
  virtual bool have_missing() const = 0;
  virtual bool is_missing(const hobject_t& oid, pg_missing_item *out = nullptr) const = 0;
  virtual bool is_missing(const hobject_t& oid, eversion_t v) const = 0;
  virtual ~pg_missing_const_i() {}
};


template <bool Track>
class ChangeTracker {
public:
  void changed(const hobject_t &obj) {}
  template <typename F>
  void get_changed(F &&f) const {}
  void flush() {}
  bool is_clean() const {
    return true;
  }
};
template <>
class ChangeTracker<true> {
  std::set<hobject_t> _changed;
public:
  void changed(const hobject_t &obj) {
    _changed.insert(obj);
  }
  template <typename F>
  void get_changed(F &&f) const {
    for (auto const &i: _changed) {
      f(i);
    }
  }
  void flush() {
    _changed.clear();
  }
  bool is_clean() const {
    return _changed.empty();
  }
};

template <bool TrackChanges>
class pg_missing_set : public pg_missing_const_i {
  using item = pg_missing_item;
  std::map<hobject_t, item> missing;  // oid -> (need v, have v)
  std::map<version_t, hobject_t> rmissing;  // v -> oid
  ChangeTracker<TrackChanges> tracker;

public:
  pg_missing_set() = default;

  template <typename missing_type>
  pg_missing_set(const missing_type &m) {
    missing = m.get_items();
    rmissing = m.get_rmissing();
    may_include_deletes = m.get_may_include_deletes();
    for (auto &&i: missing)
      tracker.changed(i.first);
  }

  bool may_include_deletes = false;

  const std::map<hobject_t, item> &get_items() const override {
    return missing;
  }
  const std::map<version_t, hobject_t> &get_rmissing() const override {
    return rmissing;
  }
  bool get_may_include_deletes() const override {
    return may_include_deletes;
  }
  unsigned int num_missing() const override {
    return missing.size();
  }
  bool have_missing() const override {
    return !missing.empty();
  }
  void merge(const pg_log_entry_t& e) {
    auto miter = missing.find(e.soid);
    if (miter != missing.end() && miter->second.have != eversion_t() && e.version > miter->second.have)
      miter->second.clean_regions.merge(e.clean_regions);
  }
  bool is_missing(const hobject_t& oid, pg_missing_item *out = nullptr) const override {
    auto iter = missing.find(oid);
    if (iter == missing.end())
      return false;
    if (out)
      *out = iter->second;
    return true;
  }
  bool is_missing(const hobject_t& oid, eversion_t v) const override {
    std::map<hobject_t, item>::const_iterator m =
      missing.find(oid);
    if (m == missing.end())
      return false;
    const item &item(m->second);
    if (item.need > v)
      return false;
    return true;
  }
  eversion_t get_oldest_need() const {
    if (missing.empty()) {
      return eversion_t();
    }
    auto it = missing.find(rmissing.begin()->second);
    ceph_assert(it != missing.end());
    return it->second.need;
  }

  void claim(pg_missing_set& o) {
    static_assert(!TrackChanges, "Can't use claim with TrackChanges");
    missing.swap(o.missing);
    rmissing.swap(o.rmissing);
  }

  /*
   * this needs to be called in log order as we extend the log.  it
   * assumes missing is accurate up through the previous log entry.
   */
  void add_next_event(const pg_log_entry_t& e) {
    std::map<hobject_t, item>::iterator missing_it;
    missing_it = missing.find(e.soid);
    bool is_missing_divergent_item = missing_it != missing.end();
    if (e.prior_version == eversion_t() || e.is_clone()) {
      // new object.
      if (is_missing_divergent_item) {  // use iterator
        rmissing.erase(missing_it->second.need.version);
        // .have = nil
        missing_it->second = item(e.version, eversion_t(), e.is_delete());
        missing_it->second.clean_regions.mark_fully_dirty();
      } else {
         // create new element in missing map
         // .have = nil
        missing[e.soid] = item(e.version, eversion_t(), e.is_delete());
        missing[e.soid].clean_regions.mark_fully_dirty();
      }
    } else if (is_missing_divergent_item) {
      // already missing (prior).
      rmissing.erase((missing_it->second).need.version);
      missing_it->second.need = e.version;  // leave .have unchanged.
      missing_it->second.set_delete(e.is_delete());
      if (e.is_lost_revert())
        missing_it->second.clean_regions.mark_fully_dirty();
      else
        missing_it->second.clean_regions.merge(e.clean_regions);
    } else {
      // not missing, we must have prior_version (if any)
      ceph_assert(!is_missing_divergent_item);
      missing[e.soid] = item(e.version, e.prior_version, e.is_delete());
      if (e.is_lost_revert())
        missing[e.soid].clean_regions.mark_fully_dirty();
      else
        missing[e.soid].clean_regions = e.clean_regions;
    }
    rmissing[e.version.version] = e.soid;
    tracker.changed(e.soid);
  }

  void revise_need(hobject_t oid, eversion_t need, bool is_delete) {
    auto p = missing.find(oid);
    if (p != missing.end()) {
      rmissing.erase((p->second).need.version);
      p->second.need = need;          // do not adjust .have
      p->second.set_delete(is_delete);
      p->second.clean_regions.mark_fully_dirty();
    } else {
      missing[oid] = item(need, eversion_t(), is_delete);
      missing[oid].clean_regions.mark_fully_dirty();
    }
    rmissing[need.version] = oid;

    tracker.changed(oid);
  }

  void revise_have(hobject_t oid, eversion_t have) {
    auto p = missing.find(oid);
    if (p != missing.end()) {
      tracker.changed(oid);
      (p->second).have = have;
    }
  }

  void mark_fully_dirty(const hobject_t& oid) {
    auto p = missing.find(oid);
    if (p != missing.end()) {
      tracker.changed(oid);
      (p->second).clean_regions.mark_fully_dirty();
    }
  }

  void add(const hobject_t& oid, eversion_t need, eversion_t have,
	   bool is_delete) {
    missing[oid] = item(need, have, is_delete, true);
    rmissing[need.version] = oid;
    tracker.changed(oid);
  }

  void add(const hobject_t& oid, pg_missing_item&& item) {
    rmissing[item.need.version] = oid;
    missing.insert({oid, std::move(item)});
    tracker.changed(oid);
  }

  void rm(const hobject_t& oid, eversion_t v) {
    std::map<hobject_t, item>::iterator p = missing.find(oid);
    if (p != missing.end() && p->second.need <= v)
      rm(p);
  }

  void rm(std::map<hobject_t, item>::const_iterator m) {
    tracker.changed(m->first);
    rmissing.erase(m->second.need.version);
    missing.erase(m);
  }

  void got(const hobject_t& oid, eversion_t v) {
    std::map<hobject_t, item>::iterator p = missing.find(oid);
    ceph_assert(p != missing.end());
    ceph_assert(p->second.need <= v || p->second.is_delete());
    got(p);
  }

  void got(std::map<hobject_t, item>::const_iterator m) {
    tracker.changed(m->first);
    rmissing.erase(m->second.need.version);
    missing.erase(m);
  }

  void split_into(
    pg_t child_pgid,
    unsigned split_bits,
    pg_missing_set *omissing) {
    omissing->may_include_deletes = may_include_deletes;
    unsigned mask = ~((~0)<<split_bits);
    for (std::map<hobject_t, item>::iterator i = missing.begin();
	 i != missing.end();
      ) {
      if ((i->first.get_hash() & mask) == child_pgid.m_seed) {
	omissing->add(i->first, i->second.need, i->second.have,
		      i->second.is_delete());
	rm(i++);
      } else {
	++i;
      }
    }
  }

  void clear() {
    for (auto const &i: missing)
      tracker.changed(i.first);
    missing.clear();
    rmissing.clear();
  }

  void encode(ceph::buffer::list &bl, uint64_t features) const {
    ENCODE_START(5, 2, bl)
    encode(missing, bl, features);
    encode(may_include_deletes, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl, int64_t pool = -1) {
    for (auto const &i: missing)
      tracker.changed(i.first);
    DECODE_START_LEGACY_COMPAT_LEN(5, 2, 2, bl);
    decode(missing, bl);
    if (struct_v >= 4) {
      decode(may_include_deletes, bl);
    }
    DECODE_FINISH(bl);

    if (struct_v < 3) {
      // Handle hobject_t upgrade
      std::map<hobject_t, item> tmp;
      for (std::map<hobject_t, item>::iterator i =
	     missing.begin();
	   i != missing.end();
	) {
	if (!i->first.is_max() && i->first.pool == -1) {
	  hobject_t to_insert(i->first);
	  to_insert.pool = pool;
	  tmp[to_insert] = i->second;
	  missing.erase(i++);
	} else {
	  ++i;
	}
      }
      missing.insert(tmp.begin(), tmp.end());
    }

    for (std::map<hobject_t,item>::iterator it =
	   missing.begin();
	 it != missing.end();
	 ++it)
      rmissing[it->second.need.version] = it->first;
    for (auto const &i: missing)
      tracker.changed(i.first);
  }
  void dump(ceph::Formatter *f) const {
    f->open_array_section("missing");
    for (std::map<hobject_t,item>::const_iterator p =
	   missing.begin(); p != missing.end(); ++p) {
      f->open_object_section("item");
      f->dump_stream("object") << p->first;
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
    f->dump_bool("may_include_deletes", may_include_deletes);
  }
  template <typename F>
  void filter_objects(F &&f) {
    for (auto i = missing.begin(); i != missing.end();) {
      if (f(i->first)) {
	rm(i++);
      } else {
        ++i;
      }
    }
  }
  static void generate_test_instances(std::list<pg_missing_set*>& o) {
    o.push_back(new pg_missing_set);
    o.back()->may_include_deletes = true;
    o.push_back(new pg_missing_set);
    o.back()->add(
      hobject_t(object_t("foo"), "foo", 123, 456, 0, ""),
      eversion_t(5, 6), eversion_t(5, 1), false);
    o.back()->may_include_deletes = true;
    o.push_back(new pg_missing_set);
    o.back()->add(
      hobject_t(object_t("foo"), "foo", 123, 456, 0, ""),
      eversion_t(5, 6), eversion_t(5, 1), true);
    o.back()->may_include_deletes = true;
  }
  template <typename F>
  void get_changed(F &&f) const {
    tracker.get_changed(f);
  }
  void flush() {
    tracker.flush();
  }
  bool is_clean() const {
    return tracker.is_clean();
  }
  template <typename missing_t>
  bool debug_verify_from_init(
    const missing_t &init_missing,
    std::ostream *oss) const {
    if (!TrackChanges)
      return true;
    auto check_missing(init_missing.get_items());
    tracker.get_changed([&](const hobject_t &hoid) {
	check_missing.erase(hoid);
	if (missing.count(hoid)) {
	  check_missing.insert(*(missing.find(hoid)));
	}
      });
    bool ok = true;
    if (check_missing.size() != missing.size()) {
      if (oss) {
	*oss << "Size mismatch, check: " << check_missing.size()
	     << ", actual: " << missing.size() << "\n";
      }
      ok = false;
    }
    for (auto &i: missing) {
      if (!check_missing.count(i.first)) {
	if (oss)
	  *oss << "check_missing missing " << i.first << "\n";
	ok = false;
      } else if (check_missing[i.first] != i.second) {
	if (oss)
	  *oss << "check_missing missing item mismatch on " << i.first
	       << ", check: " << check_missing[i.first]
	       << ", actual: " << i.second << "\n";
	ok = false;
      }
    }
    if (oss && !ok) {
      *oss << "check_missing: " << check_missing << "\n";
      std::set<hobject_t> changed;
      tracker.get_changed([&](const hobject_t &hoid) { changed.insert(hoid); });
      *oss << "changed: " << changed << "\n";
    }
    return ok;
  }
};
template <bool TrackChanges>
void encode(
  const pg_missing_set<TrackChanges> &c, ceph::buffer::list &bl, uint64_t features=0) {
  ENCODE_DUMP_PRE();
  c.encode(bl, features);
  ENCODE_DUMP_POST(cl);
}
template <bool TrackChanges>
void decode(pg_missing_set<TrackChanges> &c, ceph::buffer::list::const_iterator &p) {
  c.decode(p);
}
template <bool TrackChanges>
std::ostream& operator<<(std::ostream& out, const pg_missing_set<TrackChanges> &missing)
{
  out << "missing(" << missing.num_missing()
      << " may_include_deletes = " << missing.may_include_deletes;
  //if (missing.num_lost()) out << ", " << missing.num_lost() << " lost";
  out << ")";
  return out;
}

using pg_missing_t = pg_missing_set<false>;
using pg_missing_tracker_t = pg_missing_set<true>;




/**
 * pg list objects response format
 *
 */

template<typename T>
struct pg_nls_response_template {
  collection_list_handle_t handle;
  std::vector<T> entries;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(handle, bl);
    __u32 n = (__u32)entries.size();
    encode(n, bl);
    for (auto i = entries.begin(); i != entries.end(); ++i) {
      encode(i->nspace, bl);
      encode(i->oid, bl);
      encode(i->locator, bl);
    }
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(handle, bl);
    __u32 n;
    decode(n, bl);
    entries.clear();
    while (n--) {
      T i;
      decode(i.nspace, bl);
      decode(i.oid, bl);
      decode(i.locator, bl);
      entries.push_back(i);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("handle") << handle;
    f->open_array_section("entries");
    for (auto p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("object");
      f->dump_string("namespace", p->nspace);
      f->dump_string("object", p->oid);
      f->dump_string("key", p->locator);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(std::list<pg_nls_response_template<T>*>& o) {
    o.push_back(new pg_nls_response_template<T>);
    o.push_back(new pg_nls_response_template<T>);
    o.back()->handle = hobject_t(object_t("hi"), "key", 1, 2, -1, "");
    o.back()->entries.push_back(librados::ListObjectImpl("", "one", ""));
    o.back()->entries.push_back(librados::ListObjectImpl("", "two", "twokey"));
    o.back()->entries.push_back(librados::ListObjectImpl("", "three", ""));
    o.push_back(new pg_nls_response_template<T>);
    o.back()->handle = hobject_t(object_t("hi"), "key", 3, 4, -1, "");
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1one", ""));
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1two", "n1twokey"));
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1three", ""));
    o.push_back(new pg_nls_response_template<T>);
    o.back()->handle = hobject_t(object_t("hi"), "key", 5, 6, -1, "");
    o.back()->entries.push_back(librados::ListObjectImpl("", "one", ""));
    o.back()->entries.push_back(librados::ListObjectImpl("", "two", "twokey"));
    o.back()->entries.push_back(librados::ListObjectImpl("", "three", ""));
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1one", ""));
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1two", "n1twokey"));
    o.back()->entries.push_back(librados::ListObjectImpl("n1", "n1three", ""));
  }
};

using pg_nls_response_t = pg_nls_response_template<librados::ListObjectImpl>;

WRITE_CLASS_ENCODER(pg_nls_response_t)

// For backwards compatibility with older OSD requests
struct pg_ls_response_t {
  collection_list_handle_t handle; 
  std::list<std::pair<object_t, std::string> > entries;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    __u8 v = 1;
    encode(v, bl);
    encode(handle, bl);
    encode(entries, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 v;
    decode(v, bl);
    ceph_assert(v == 1);
    decode(handle, bl);
    decode(entries, bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("handle") << handle;
    f->open_array_section("entries");
    for (std::list<std::pair<object_t, std::string> >::const_iterator p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("object");
      f->dump_stream("object") << p->first;
      f->dump_string("key", p->second);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(std::list<pg_ls_response_t*>& o) {
    o.push_back(new pg_ls_response_t);
    o.push_back(new pg_ls_response_t);
    o.back()->handle = hobject_t(object_t("hi"), "key", 1, 2, -1, "");
    o.back()->entries.push_back(std::make_pair(object_t("one"), std::string()));
    o.back()->entries.push_back(std::make_pair(object_t("two"), std::string("twokey")));
  }
};

WRITE_CLASS_ENCODER(pg_ls_response_t)

/**
 * object_copy_cursor_t
 */
struct object_copy_cursor_t {
  uint64_t data_offset;
  std::string omap_offset;
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

  static void generate_test_instances(std::list<object_copy_cursor_t*>& o);
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
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
  std::map<std::string, ceph::buffer::list> attrs;
  ceph::buffer::list data;
  ceph::buffer::list omap_header;
  ceph::buffer::list omap_data;

  /// which snaps we are defined for (if a snap and not the head)
  std::vector<snapid_t> snaps;
  /// latest snap seq for the object (if head)
  snapid_t snap_seq;

  /// recent reqids on this object
  mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > reqids;

  /// map reqids by index to error return code (if any)
  mempool::osd_pglog::map<uint32_t, int> reqid_return_codes;

  uint64_t truncate_seq;
  uint64_t truncate_size;

public:
  object_copy_data_t() :
    size((uint64_t)-1), data_digest(-1),
    omap_digest(-1), flags(0),
    truncate_seq(0),
    truncate_size(0) {}

  static void generate_test_instances(std::list<object_copy_data_t*>& o);
  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
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

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<pg_create_t*>& o);
};
WRITE_CLASS_ENCODER(pg_create_t)

// -----------------------------------------

class ObjectExtent {
  /**
   * ObjectExtents are used for specifying IO behavior against RADOS
   * objects when one is using the ObjectCacher.
   *
   * To use this in a real system, *every member* must be filled
   * out correctly. In particular, make sure to initialize the
   * oloc correctly, as its default values are deliberate poison
   * and will cause internal ObjectCacher asserts.
   *
   * Similarly, your buffer_extents vector *must* specify a total
   * size equal to your length. If the buffer_extents inadvertently
   * contain less space than the length member specifies, you
   * will get unintelligible asserts deep in the ObjectCacher.
   *
   * If you are trying to do testing and don't care about actual
   * RADOS function, the simplest thing to do is to initialize
   * the ObjectExtent (truncate_size can be 0), create a single entry
   * in buffer_extents matching the length, and set oloc.pool to 0.
   */
 public:
  object_t    oid;       // object id
  uint64_t    objectno;
  uint64_t    offset;    // in object
  uint64_t    length;    // in object
  uint64_t    truncate_size;	// in object

  object_locator_t oloc;   // object locator (pool etc)

  std::vector<std::pair<uint64_t,uint64_t> >  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : objectno(0), offset(0), length(0), truncate_size(0) {}
  ObjectExtent(object_t o, uint64_t ono, uint64_t off, uint64_t l, uint64_t ts) :
    oid(o), objectno(ono), offset(off), length(l), truncate_size(ts) { }
};

inline std::ostream& operator<<(std::ostream& out, const ObjectExtent &ex)
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
  int32_t whoami = -1;    // my role in this fs.
  epoch_t current_epoch = 0;             // most recent epoch
  epoch_t oldest_map = 0, newest_map = 0;    // oldest/newest maps we have.
  double weight = 0.0;

  CompatSet compat_features;

  // last interval over which i mounted and was then active
  epoch_t mounted = 0;     // last epoch i mounted
  epoch_t clean_thru = 0;  // epoch i was active and clean thru

  epoch_t purged_snaps_last = 0;
  utime_t last_purged_snaps_scrub;

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<OSDSuperblock*>& o);
};
WRITE_CLASS_ENCODER(OSDSuperblock)

inline std::ostream& operator<<(std::ostream& out, const OSDSuperblock& sb)
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






/*
 * attached to object head.  describes most recent snap context, and
 * set of existing clones.
 */
struct SnapSet {
  snapid_t seq;
  // NOTE: this is for pre-octopus compatibility only! remove in Q release
  std::vector<snapid_t> snaps;    // descending
  std::vector<snapid_t> clones;   // ascending
  std::map<snapid_t, interval_set<uint64_t> > clone_overlap;  // overlap w/ next newest
  std::map<snapid_t, uint64_t> clone_size;
  std::map<snapid_t, std::vector<snapid_t>> clone_snaps; // descending

  SnapSet() : seq(0) {}
  explicit SnapSet(ceph::buffer::list& bl) {
    auto p = std::cbegin(bl);
    decode(p);
  }

  /// populate SnapSet from a librados::snap_set_t
  void from_snap_set(const librados::snap_set_t& ss, bool legacy);

  /// get space accounted to clone
  uint64_t get_clone_bytes(snapid_t clone) const;
    
  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<SnapSet*>& o);  

  SnapContext get_ssc_as_of(snapid_t as_of) const {
    SnapContext out;
    out.seq = as_of;
    for (auto p = clone_snaps.rbegin();
	 p != clone_snaps.rend();
	 ++p) {
      for (auto snap : p->second) {
	if (snap <= as_of) {
	  out.snaps.push_back(snap);
	}
      }
    }
    return out;
  }


  SnapSet get_filtered(const pg_pool_t &pinfo) const;
  void filter(const pg_pool_t &pinfo);
};
WRITE_CLASS_ENCODER(SnapSet)

std::ostream& operator<<(std::ostream& out, const SnapSet& cs);



#define OI_ATTR "_"
#define SS_ATTR "snapset"

struct watch_info_t {
  uint64_t cookie;
  uint32_t timeout_seconds;
  entity_addr_t addr;

  watch_info_t() : cookie(0), timeout_seconds(0) { }
  watch_info_t(uint64_t c, uint32_t t, const entity_addr_t& a) : cookie(c), timeout_seconds(t), addr(a) {}

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<watch_info_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(watch_info_t)

static inline bool operator==(const watch_info_t& l, const watch_info_t& r) {
  return l.cookie == r.cookie && l.timeout_seconds == r.timeout_seconds
	    && l.addr == r.addr;
}

static inline std::ostream& operator<<(std::ostream& out, const watch_info_t& w) {
  return out << "watch(cookie " << w.cookie << " " << w.timeout_seconds << "s"
    << " " << w.addr << ")";
}

struct notify_info_t {
  uint64_t cookie;
  uint64_t notify_id;
  uint32_t timeout;
  ceph::buffer::list bl;
};

static inline std::ostream& operator<<(std::ostream& out, const notify_info_t& n) {
  return out << "notify(cookie " << n.cookie
	     << " notify" << n.notify_id
	     << " " << n.timeout << "s)";
}

struct chunk_info_t {
  typedef enum {
    FLAG_DIRTY = 1, 
    FLAG_MISSING = 2,
    FLAG_HAS_REFERENCE = 4,
    FLAG_HAS_FINGERPRINT = 8,
  } cflag_t;
  uint32_t offset;
  uint32_t length;
  hobject_t oid;
  cflag_t flags;   // FLAG_*

  chunk_info_t() : offset(0), length(0), flags((cflag_t)0) { }

  static std::string get_flag_string(uint64_t flags) {
    std::string r;
    if (flags & FLAG_DIRTY) {
      r += "|dirty";
    }
    if (flags & FLAG_MISSING) {
      r += "|missing";
    }
    if (flags & FLAG_HAS_REFERENCE) {
      r += "|has_reference";
    }
    if (flags & FLAG_HAS_FINGERPRINT) {
      r += "|has_fingerprint";
    }
    if (r.length())
      return r.substr(1);
    return r;
  }
  bool test_flag(cflag_t f) const {
    return (flags & f) == f;
  }
  void set_flag(cflag_t f) {
    flags = (cflag_t)(flags | f);
  }
  void set_flags(cflag_t f) {
    flags = f;
  }
  void clear_flag(cflag_t f) {
    flags = (cflag_t)(flags & ~f);
  }
  void clear_flags() {
    flags = (cflag_t)0;
  }
  bool is_dirty() const {
    return test_flag(FLAG_DIRTY);
  }
  bool is_missing() const {
    return test_flag(FLAG_MISSING);
  }
  bool has_reference() const {
    return test_flag(FLAG_HAS_REFERENCE);
  }
  bool has_fingerprint() const {
    return test_flag(FLAG_HAS_FINGERPRINT);
  }
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& out, const chunk_info_t& ci);
};
WRITE_CLASS_ENCODER(chunk_info_t)
std::ostream& operator<<(std::ostream& out, const chunk_info_t& ci);

struct object_info_t;
struct object_manifest_t {
  enum {
    TYPE_NONE = 0,
    TYPE_REDIRECT = 1, 
    TYPE_CHUNKED = 2, 
  };
  uint8_t type;  // redirect, chunked, ...
  hobject_t redirect_target;
  std::map<uint64_t, chunk_info_t> chunk_map;

  object_manifest_t() : type(0) { }
  object_manifest_t(uint8_t type, const hobject_t& redirect_target) 
    : type(type), redirect_target(redirect_target) { }

  bool is_empty() const {
    return type == TYPE_NONE;
  }
  bool is_redirect() const {
    return type == TYPE_REDIRECT;
  }
  bool is_chunked() const {
    return type == TYPE_CHUNKED;
  }
  static std::string_view get_type_name(uint8_t m) {
    switch (m) {
    case TYPE_NONE: return "none";
    case TYPE_REDIRECT: return "redirect";
    case TYPE_CHUNKED: return "chunked";
    default: return "unknown";
    }
  }
  std::string_view get_type_name() const {
    return get_type_name(type);
  }
  void clear() {
    type = 0;
    redirect_target = hobject_t();
    chunk_map.clear();
  }
  static void generate_test_instances(std::list<object_manifest_t*>& o);
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& out, const object_info_t& oi);
};
WRITE_CLASS_ENCODER(object_manifest_t)
std::ostream& operator<<(std::ostream& out, const object_manifest_t& oi);

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
    FLAG_LOST        = 1<<0,
    FLAG_WHITEOUT    = 1<<1, // object logically does not exist
    FLAG_DIRTY       = 1<<2, // object has been modified since last flushed or undirtied
    FLAG_OMAP        = 1<<3, // has (or may have) some/any omap data
    FLAG_DATA_DIGEST = 1<<4, // has data crc
    FLAG_OMAP_DIGEST = 1<<5, // has omap crc
    FLAG_CACHE_PIN   = 1<<6, // pin the object in cache tier
    FLAG_MANIFEST    = 1<<7, // has manifest
    FLAG_USES_TMAP   = 1<<8, // deprecated; no longer used
    FLAG_REDIRECT_HAS_REFERENCE = 1<<9, // has reference
  } flag_t;

  flag_t flags;

  static std::string get_flag_string(flag_t flags) {
    std::string s;
    std::vector<std::string> sv = get_flag_vector(flags);
    for (auto ss : sv) {
      s += std::string("|") + ss;
    }
    if (s.length())
      return s.substr(1);
    return s;
  }
  static std::vector<std::string> get_flag_vector(flag_t flags) {
    std::vector<std::string> sv;
    if (flags & FLAG_LOST)
      sv.insert(sv.end(), "lost");
    if (flags & FLAG_WHITEOUT)
      sv.insert(sv.end(), "whiteout");
    if (flags & FLAG_DIRTY)
      sv.insert(sv.end(), "dirty");
    if (flags & FLAG_USES_TMAP)
      sv.insert(sv.end(), "uses_tmap");
    if (flags & FLAG_OMAP)
      sv.insert(sv.end(), "omap");
    if (flags & FLAG_DATA_DIGEST)
      sv.insert(sv.end(), "data_digest");
    if (flags & FLAG_OMAP_DIGEST)
      sv.insert(sv.end(), "omap_digest");
    if (flags & FLAG_CACHE_PIN)
      sv.insert(sv.end(), "cache_pin");
    if (flags & FLAG_MANIFEST)
      sv.insert(sv.end(), "manifest");
    if (flags & FLAG_REDIRECT_HAS_REFERENCE)
      sv.insert(sv.end(), "redirect_has_reference");
    return sv;
  }
  std::string get_flag_string() const {
    return get_flag_string(flags);
  }

  uint64_t truncate_seq, truncate_size;

  std::map<std::pair<uint64_t, entity_name_t>, watch_info_t> watchers;

  // opportunistic checksums; may or may not be present
  __u32 data_digest;  ///< data crc32c
  __u32 omap_digest;  ///< omap crc32c
  
  // alloc hint attribute
  uint64_t expected_object_size, expected_write_size;
  uint32_t alloc_hint_flags;

  struct object_manifest_t manifest;

  void copy_user_bits(const object_info_t& other);

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
  bool has_manifest() const {
    return test_flag(FLAG_MANIFEST);
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
    clear_data_digest();
    clear_omap_digest();
  }

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void decode(ceph::buffer::list& bl) {
    auto p = std::cbegin(bl);
    decode(p);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<object_info_t*>& o);

  explicit object_info_t()
    : user_version(0), size(0), flags((flag_t)0),
      truncate_seq(0), truncate_size(0),
      data_digest(-1), omap_digest(-1),
      expected_object_size(0), expected_write_size(0),
      alloc_hint_flags(0)
  {}

  explicit object_info_t(const hobject_t& s)
    : soid(s),
      user_version(0), size(0), flags((flag_t)0),
      truncate_seq(0), truncate_size(0),
      data_digest(-1), omap_digest(-1),
      expected_object_size(0), expected_write_size(0),
      alloc_hint_flags(0)
  {}

  explicit object_info_t(ceph::buffer::list& bl) {
    decode(bl);
  }
};
WRITE_CLASS_ENCODER_FEATURES(object_info_t)

std::ostream& operator<<(std::ostream& out, const object_info_t& oi);



// Object recovery
struct ObjectRecoveryInfo {
  hobject_t soid;
  eversion_t version;
  uint64_t size;
  object_info_t oi;
  SnapSet ss;   // only populated if soid is_snap()
  interval_set<uint64_t> copy_subset;
  std::map<hobject_t, interval_set<uint64_t>> clone_subset;
  bool object_exist;

  ObjectRecoveryInfo() : size(0), object_exist(true) { }

  static void generate_test_instances(std::list<ObjectRecoveryInfo*>& o);
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl, int64_t pool = -1);
  std::ostream &print(std::ostream &out) const;
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER_FEATURES(ObjectRecoveryInfo)
std::ostream& operator<<(std::ostream& out, const ObjectRecoveryInfo &inf);

struct ObjectRecoveryProgress {
  uint64_t data_recovered_to;
  std::string omap_recovered_to;
  bool first;
  bool data_complete;
  bool omap_complete;
  bool error = false;

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

  static void generate_test_instances(std::list<ObjectRecoveryProgress*>& o);
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  std::ostream &print(std::ostream &out) const;
  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(ObjectRecoveryProgress)
std::ostream& operator<<(std::ostream& out, const ObjectRecoveryProgress &prog);

struct PushReplyOp {
  hobject_t soid;

  static void generate_test_instances(std::list<PushReplyOp*>& o);
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  std::ostream &print(std::ostream &out) const;
  void dump(ceph::Formatter *f) const;

  uint64_t cost(CephContext *cct) const;
};
WRITE_CLASS_ENCODER(PushReplyOp)
std::ostream& operator<<(std::ostream& out, const PushReplyOp &op);

struct PullOp {
  hobject_t soid;

  ObjectRecoveryInfo recovery_info;
  ObjectRecoveryProgress recovery_progress;

  static void generate_test_instances(std::list<PullOp*>& o);
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  std::ostream &print(std::ostream &out) const;
  void dump(ceph::Formatter *f) const;

  uint64_t cost(CephContext *cct) const;
};
WRITE_CLASS_ENCODER_FEATURES(PullOp)
std::ostream& operator<<(std::ostream& out, const PullOp &op);

struct PushOp {
  hobject_t soid;
  eversion_t version;
  ceph::buffer::list data;
  interval_set<uint64_t> data_included;
  ceph::buffer::list omap_header;
  std::map<std::string, ceph::buffer::list> omap_entries;
  std::map<std::string, ceph::buffer::list> attrset;

  ObjectRecoveryInfo recovery_info;
  ObjectRecoveryProgress before_progress;
  ObjectRecoveryProgress after_progress;

  static void generate_test_instances(std::list<PushOp*>& o);
  void encode(ceph::buffer::list &bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  std::ostream &print(std::ostream &out) const;
  void dump(ceph::Formatter *f) const;

  uint64_t cost(CephContext *cct) const;
};
WRITE_CLASS_ENCODER_FEATURES(PushOp)
std::ostream& operator<<(std::ostream& out, const PushOp &op);


/*
 * summarize pg contents for purposes of a scrub
 */
struct ScrubMap {
  struct object {
    std::map<std::string, ceph::buffer::ptr> attrs;
    uint64_t size;
    __u32 omap_digest;         ///< omap crc32c
    __u32 digest;              ///< data crc32c
    bool negative:1;
    bool digest_present:1;
    bool omap_digest_present:1;
    bool read_error:1;
    bool stat_error:1;
    bool ec_hash_mismatch:1;
    bool ec_size_mismatch:1;
    bool large_omap_object_found:1;
    uint64_t large_omap_object_key_count = 0;
    uint64_t large_omap_object_value_size = 0;
    uint64_t object_omap_bytes = 0;
    uint64_t object_omap_keys = 0;

    object() :
      // Init invalid size so it won't match if we get a stat EIO error
      size(-1), omap_digest(0), digest(0),
      negative(false), digest_present(false), omap_digest_present(false),
      read_error(false), stat_error(false), ec_hash_mismatch(false),
      ec_size_mismatch(false), large_omap_object_found(false) {}

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<object*>& o);
  };
  WRITE_CLASS_ENCODER(object)

  std::map<hobject_t,object> objects;
  eversion_t valid_through;
  eversion_t incr_since;
  bool has_large_omap_object_errors:1;
  bool has_omap_keys:1;

  void merge_incr(const ScrubMap &l);
  void clear_from(const hobject_t& start) {
    objects.erase(objects.lower_bound(start), objects.end());
  }
  void insert(const ScrubMap &r) {
    objects.insert(r.objects.begin(), r.objects.end());
  }
  void swap(ScrubMap &r) {
    using std::swap;
    swap(objects, r.objects);
    swap(valid_through, r.valid_through);
    swap(incr_since, r.incr_since);
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl, int64_t pool=-1);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<ScrubMap*>& o);
};
WRITE_CLASS_ENCODER(ScrubMap::object)
WRITE_CLASS_ENCODER(ScrubMap)

struct ScrubMapBuilder {
  bool deep = false;
  std::vector<hobject_t> ls;
  size_t pos = 0;
  int64_t data_pos = 0;
  std::string omap_pos;
  int ret = 0;
  ceph::buffer::hash data_hash, omap_hash;  ///< accumulatinng hash value
  uint64_t omap_keys = 0;
  uint64_t omap_bytes = 0;

  bool empty() {
    return ls.empty();
  }
  bool done() {
    return pos >= ls.size();
  }
  void reset() {
    *this = ScrubMapBuilder();
  }

  bool data_done() {
    return data_pos < 0;
  }

  void next_object() {
    ++pos;
    data_pos = 0;
    omap_pos.clear();
    omap_keys = 0;
    omap_bytes = 0;
  }

  friend std::ostream& operator<<(std::ostream& out, const ScrubMapBuilder& pos) {
    out << "(" << pos.pos << "/" << pos.ls.size();
    if (pos.pos < pos.ls.size()) {
      out << " " << pos.ls[pos.pos];
    }
    if (pos.data_pos < 0) {
      out << " byte " << pos.data_pos;
    }
    if (!pos.omap_pos.empty()) {
      out << " key " << pos.omap_pos;
    }
    if (pos.deep) {
      out << " deep";
    }
    if (pos.ret) {
      out << " ret " << pos.ret;
    }
    return out << ")";
  }
};

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

  void encode(ceph::buffer::list &bl, uint64_t features) const {
    ENCODE_START(2, 1, bl);
    encode(name, bl);
    encode(cookie, bl);
    encode(timeout_seconds, bl);
    encode(addr, bl, features);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START(2, bl);
    decode(name, bl);
    decode(cookie, bl);
    decode(timeout_seconds, bl);
    if (struct_v >= 2) {
      decode(addr, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_stream("watcher") << name;
    f->dump_int("cookie", cookie);
    f->dump_int("timeout", timeout_seconds);
    f->open_object_section("addr");
    addr.dump(f);
    f->close_section();
  }
  static void generate_test_instances(std::list<watch_item_t*>& o) {
    entity_addr_t ea;
    ea.set_type(entity_addr_t::TYPE_LEGACY);
    ea.set_nonce(1000);
    ea.set_family(AF_INET);
    ea.set_in4_quad(0, 127);
    ea.set_in4_quad(1, 0);
    ea.set_in4_quad(2, 0);
    ea.set_in4_quad(3, 1);
    ea.set_port(1024);
    o.push_back(new watch_item_t(entity_name_t(entity_name_t::TYPE_CLIENT, 1), 10, 30, ea));
    ea.set_nonce(1001);
    ea.set_in4_quad(3, 2);
    ea.set_port(1025);
    o.push_back(new watch_item_t(entity_name_t(entity_name_t::TYPE_CLIENT, 2), 20, 60, ea));
  }
};
WRITE_CLASS_ENCODER_FEATURES(watch_item_t)

struct obj_watch_item_t {
  hobject_t obj;
  watch_item_t wi;
};

/**
 * obj list watch response format
 *
 */
struct obj_list_watch_response_t {
  std::list<watch_item_t> entries;

  void encode(ceph::buffer::list& bl, uint64_t features) const {
    ENCODE_START(1, 1, bl);
    encode(entries, bl, features);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(entries, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->open_array_section("entries");
    for (std::list<watch_item_t>::const_iterator p = entries.begin(); p != entries.end(); ++p) {
      f->open_object_section("watch");
      p->dump(f);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(std::list<obj_list_watch_response_t*>& o) {
    entity_addr_t ea;
    o.push_back(new obj_list_watch_response_t);
    o.push_back(new obj_list_watch_response_t);
    std::list<watch_item_t*> test_watchers;
    watch_item_t::generate_test_instances(test_watchers);
    for (auto &e : test_watchers) {
      o.back()->entries.push_back(*e);
      delete e;
    }
  }
};
WRITE_CLASS_ENCODER_FEATURES(obj_list_watch_response_t)

struct clone_info {
  snapid_t cloneid;
  std::vector<snapid_t> snaps;  // ascending
  std::vector< std::pair<uint64_t,uint64_t> > overlap;
  uint64_t size;

  clone_info() : cloneid(CEPH_NOSNAP), size(0) {}

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(cloneid, bl);
    encode(snaps, bl);
    encode(overlap, bl);
    encode(size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(cloneid, bl);
    decode(snaps, bl);
    decode(overlap, bl);
    decode(size, bl);
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    if (cloneid == CEPH_NOSNAP)
      f->dump_string("cloneid", "HEAD");
    else
      f->dump_unsigned("cloneid", cloneid.val);
    f->open_array_section("snapshots");
    for (std::vector<snapid_t>::const_iterator p = snaps.begin(); p != snaps.end(); ++p) {
      f->open_object_section("snap");
      f->dump_unsigned("id", p->val);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("overlaps");
    for (std::vector< std::pair<uint64_t,uint64_t> >::const_iterator q = overlap.begin();
          q != overlap.end(); ++q) {
      f->open_object_section("overlap");
      f->dump_unsigned("offset", q->first);
      f->dump_unsigned("length", q->second);
      f->close_section();
    }
    f->close_section();
    f->dump_unsigned("size", size);
  }
  static void generate_test_instances(std::list<clone_info*>& o) {
    o.push_back(new clone_info);
    o.push_back(new clone_info);
    o.back()->cloneid = 1;
    o.back()->snaps.push_back(1);
    o.back()->overlap.push_back(std::pair<uint64_t,uint64_t>(0,4096));
    o.back()->overlap.push_back(std::pair<uint64_t,uint64_t>(8192,4096));
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
  std::vector<clone_info> clones;   // ascending
  snapid_t seq;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(clones, bl);
    encode(seq, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(clones, bl);
    if (struct_v >= 2)
      decode(seq, bl);
    else
      seq = CEPH_NOSNAP;
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter *f) const {
    f->open_array_section("clones");
    for (std::vector<clone_info>::const_iterator p = clones.begin(); p != clones.end(); ++p) {
      f->open_object_section("clone");
      p->dump(f);
      f->close_section();
    }
    f->dump_unsigned("seq", seq);
    f->close_section();
  }
  static void generate_test_instances(std::list<obj_list_snap_response_t*>& o) {
    o.push_back(new obj_list_snap_response_t);
    o.push_back(new obj_list_snap_response_t);
    clone_info cl;
    cl.cloneid = 1;
    cl.snaps.push_back(1);
    cl.overlap.push_back(std::pair<uint64_t,uint64_t>(0,4096));
    cl.overlap.push_back(std::pair<uint64_t,uint64_t>(8192,4096));
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

// PromoteCounter

struct PromoteCounter {
  std::atomic<unsigned long long>  attempts{0};
  std::atomic<unsigned long long>  objects{0};
  std::atomic<unsigned long long>  bytes{0};

  void attempt() {
    attempts++;
  }

  void finish(uint64_t size) {
    objects++;
    bytes += size;
  }

  void sample_and_attenuate(uint64_t *a, uint64_t *o, uint64_t *b) {
    *a = attempts;
    *o = objects;
    *b = bytes;
    attempts = *a / 2;
    objects = *o / 2;
    bytes = *b / 2;
  }
};

struct pool_pg_num_history_t {
  /// last epoch updated
  epoch_t epoch = 0;
  /// poolid -> epoch -> pg_num
  std::map<int64_t, std::map<epoch_t,uint32_t>> pg_nums;
  /// pair(epoch, poolid)
  std::set<std::pair<epoch_t,int64_t>> deleted_pools;

  void log_pg_num_change(epoch_t epoch, int64_t pool, uint32_t pg_num) {
    pg_nums[pool][epoch] = pg_num;
  }
  void log_pool_delete(epoch_t epoch, int64_t pool) {
    deleted_pools.insert(std::make_pair(epoch, pool));
  }

  /// prune history based on oldest osdmap epoch in the cluster
  void prune(epoch_t oldest_epoch) {
    auto i = deleted_pools.begin();
    while (i != deleted_pools.end()) {
      if (i->first >= oldest_epoch) {
	break;
      }
      pg_nums.erase(i->second);
      i = deleted_pools.erase(i);
    }
    for (auto& j : pg_nums) {
      auto k = j.second.lower_bound(oldest_epoch);
      // keep this and the entry before it (just to be paranoid)
      if (k != j.second.begin()) {
	--k;
	j.second.erase(j.second.begin(), k);
      }
    }
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(epoch, bl);
    encode(pg_nums, bl);
    encode(deleted_pools, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    decode(epoch, p);
    decode(pg_nums, p);
    decode(deleted_pools, p);
    DECODE_FINISH(p);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("epoch", epoch);
    f->open_object_section("pools");
    for (auto& i : pg_nums) {
      f->open_object_section("pool");
      f->dump_unsigned("pool_id", i.first);
      f->open_array_section("changes");
      for (auto& j : i.second) {
	f->open_object_section("change");
	f->dump_unsigned("epoch", j.first);
	f->dump_unsigned("pg_num", j.second);
	f->close_section();
      }
      f->close_section();
      f->close_section();
    }
    f->close_section();
    f->open_array_section("deleted_pools");
    for (auto& i : deleted_pools) {
      f->open_object_section("deletion");
      f->dump_unsigned("pool_id", i.second);
      f->dump_unsigned("epoch", i.first);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(std::list<pool_pg_num_history_t*>& ls) {
    ls.push_back(new pool_pg_num_history_t);
  }
  friend std::ostream& operator<<(std::ostream& out, const pool_pg_num_history_t& h) {
    return out << "pg_num_history(e" << h.epoch
	       << " pg_nums " << h.pg_nums
	       << " deleted_pools " << h.deleted_pools
	       << ")";
  }
};
WRITE_CLASS_ENCODER(pool_pg_num_history_t)

// prefix pgmeta_oid keys with _ so that PGLog::read_log_and_missing() can
// easily skip them
static const std::string_view infover_key = "_infover";
static const std::string_view info_key = "_info";
static const std::string_view biginfo_key = "_biginfo";
static const std::string_view epoch_key = "_epoch";
static const std::string_view fastinfo_key = "_fastinfo";

static const __u8 pg_latest_struct_v = 10;
// v10 is the new past_intervals encoding
// v9 was fastinfo_key addition
// v8 was the move to a per-pg pgmeta object
// v7 was SnapMapper addition in 86658392516d5175b2756659ef7ffaaf95b0f8ad
// (first appeared in cuttlefish).
static const __u8 pg_compat_struct_v = 10;

int prepare_info_keymap(
  CephContext* cct,
  std::map<std::string,ceph::buffer::list> *km,
  std::string *key_to_remove,
  epoch_t epoch,
  pg_info_t &info,
  pg_info_t &last_written_info,
  PastIntervals &past_intervals,
  bool dirty_big_info,
  bool dirty_epoch,
  bool try_fast_info,
  PerfCounters *logger = nullptr,
  DoutPrefixProvider *dpp = nullptr);

namespace ceph::os {
  class Transaction;
};

void create_pg_collection(
  ceph::os::Transaction& t, spg_t pgid, int bits);

void init_pg_ondisk(
  ceph::os::Transaction& t, spg_t pgid, const pg_pool_t *pool);

// omap specific stats
struct omap_stat_t {
 int large_omap_objects;
 int64_t omap_bytes;
 int64_t omap_keys;
};

// filter for pg listings
class PGLSFilter {
  CephContext* cct;
protected:
  std::string xattr;
public:
  PGLSFilter();
  virtual ~PGLSFilter();
  virtual bool filter(const hobject_t &obj,
                      const ceph::buffer::list& xattr_data) const = 0;

  /**
   * Arguments passed from the RADOS client.  Implementations must
   * handle any encoding errors, and return an appropriate error code,
   * or 0 on valid input.
   */
  virtual int init(ceph::buffer::list::const_iterator &params) = 0;

  /**
   * xattr key, or empty string.  If non-empty, this xattr will be fetched
   * and the value passed into ::filter
   */
  virtual const std::string& get_xattr() const { return xattr; }

  /**
   * If true, objects without the named xattr (if xattr name is not empty)
   * will be rejected without calling ::filter
   */
  virtual bool reject_empty_xattr() const { return true; }
};

class PGLSPlainFilter : public PGLSFilter {
  std::string val;
public:
  int init(ceph::buffer::list::const_iterator &params) override;
  ~PGLSPlainFilter() override {}
  bool filter(const hobject_t& obj,
              const ceph::buffer::list& xattr_data) const override;
};


#endif
