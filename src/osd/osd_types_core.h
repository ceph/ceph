// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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

#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "include/mempool.h"
#include "common/fmt_common.h"
#include "msg/msg_types.h"
#include "include/common_fwd.h"
#include "include/types.h"
#include "include/utime.h"
#include "include/interval_set.h"
#include "common/Formatter.h"
#include "common/hobject.h"
#include "common/snap_types.h"
#include "common/strtol.h"
#include "osd/ECTypes.h"
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

using shard_id_set = bitset_set<128, shard_id_t>;
WRITE_CLASS_DENC(shard_id_set)

enum class OmapUpdateType : uint8_t {Remove, Insert, RemoveRange};

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
    DENC_START_OSD_REQID(2, 2, p);
    denc(v.name, p);
    denc(v.tid, p);
    denc(v.inc, p);
    DENC_FINISH(p);
  }
  void dump(ceph::Formatter *f) const;
  static std::list<osd_reqid_t> generate_test_instances();
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
      f->dump_unsigned("shard", static_cast<unsigned>(shard));
    }
  }
  static std::list<pg_shard_t> generate_test_instances() {
    std::list<pg_shard_t> o;
    o.emplace_back();
    o.push_back(pg_shard_t(1));
    o.push_back(pg_shard_t(1, shard_id_t(2)));
    return o;
  }
  auto operator<=>(const pg_shard_t&) const = default;
};
WRITE_CLASS_ENCODER(pg_shard_t)
std::ostream& operator<<(std::ostream &lhs, const pg_shard_t &rhs);

using HobjToShardSetMapping = std::map<hobject_t, std::set<pg_shard_t>>;

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
  return std::tie(l.name, l.inc, l.tid) < std::tie(r.name, r.inc, r.tid);
}
inline bool operator<=(const osd_reqid_t& l, const osd_reqid_t& r) {
  return std::tie(l.name, l.inc, l.tid) <= std::tie(r.name, r.inc, r.tid);
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
  static std::list<object_locator_t> generate_test_instances();
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
  if (!loc.nspace.empty())
    out << ";" << loc.nspace;
  if (!loc.key.empty())
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
  static std::list<request_redirect_t> generate_test_instances();
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
  CEPH_OSD_RMW_FLAG_READ_DATA  = (1 << 12),
  CEPH_OSD_RMW_FLAG_EC_DIRECT_READ  = (1 << 13),
  CEPH_OSD_RMW_FLAG_EC_SYNC_READ    = (1 << 14),
  CEPH_OSD_RMW_FLAG_CLASS_READ_DATA = (1 << 15),
  CEPH_OSD_RMW_FLAG_PRIMARY_ONLY    = (1 << 16),
};


// pg stuff

#define OSD_SUPERBLOCK_GOBJECT ghobject_t(hobject_t(sobject_t(object_t("osd_superblock"), 0)))
#define OSD_SUPERBLOCK_OMAP_KEY "osd_superblock"

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
  constexpr pg_t(ps_t seed, uint64_t pool) :
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
  auto operator<=>(const pg_t&) const noexcept = default;

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
  static std::list<pg_t> generate_test_instances();
};
WRITE_CLASS_ENCODER(pg_t)

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
  constexpr spg_t(pg_t pgid, shard_id_t shard) : pgid(pgid), shard(shard) {}
  explicit spg_t(pg_t pgid) : pgid(pgid), shard(shard_id_t::NO_SHARD) {}
  auto operator<=>(const spg_t&) const = default;
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
  // and a (limited) version that uses an internal buffer:
  std::string calc_name_sring() const;
 
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

  ghobject_t make_snapmapper_oid() const {
    return ghobject_t::make_snapmapper(pgid.pool(), pgid.ps(), shard);
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
  void dump(ceph::Formatter *f) const {
    f->dump_stream("pgid") << pgid;
    f->dump_unsigned("shard", static_cast<unsigned>(shard));
  }
  static std::list<spg_t> generate_test_instances() {
    std::list<spg_t> o;
    o.emplace_back();
    o.push_back(spg_t(pg_t(1, 2), shard_id_t(3)));
    return o;
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

namespace std {
  template<> struct hash< spg_t >
  {
    size_t operator()( const spg_t& x ) const
      {
      static hash<uint32_t> H;
      // Historically a "shard" was an int8_t, hence the unexpected cast in
      // this XOR.
      return H(hash<pg_t>()(x.pgid) ^ static_cast<int8_t>(x.shard));
    }
  };
} // namespace std

std::ostream& operator<<(std::ostream& out, const spg_t &pg);

// ----------------------

class coll_t {
  enum type_t : uint8_t {
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

  friend class denc_coll_t;
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
  static std::list<coll_t> generate_test_instances();
};

WRITE_CLASS_ENCODER(coll_t)

inline std::ostream& operator<<(std::ostream& out, const coll_t& c) {
  out << c.to_str();
  return out;
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<coll_t> : fmt::ostream_formatter {};
#endif

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

struct denc_coll_t {
  coll_t coll;

  auto &get_type() const { return coll.type; }
  auto &get_type() { return coll.type; }
  auto &get_pgid() const { return coll.pgid; }
  auto &get_pgid() { return coll.pgid; }

  denc_coll_t() = default;
  denc_coll_t(const denc_coll_t &) = default;
  denc_coll_t(denc_coll_t &&) = default;

  denc_coll_t &operator=(const denc_coll_t &) = default;
  denc_coll_t &operator=(denc_coll_t &&) = default;

  explicit denc_coll_t(const coll_t &coll) : coll(coll) {}
  operator coll_t() const {
    return coll;
  }

  bool operator<(const denc_coll_t &rhs) const {
    return coll < rhs.coll;
  }

  DENC(denc_coll_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.get_type(), p);
    denc(v.get_pgid().pgid.m_pool, p);
    denc(v.get_pgid().pgid.m_seed, p);
    denc(v.get_pgid().shard.id, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(denc_coll_t)


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
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("version", version);
    f->dump_unsigned("epoch", epoch);
  }
  static std::list<eversion_t> generate_test_instances() {
    std::list<eversion_t> o;
    o.emplace_back();
    o.push_back(eversion_t(1, 2));
    return o;
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

namespace std {
template <>
struct hash<eversion_t> {
  size_t operator()(const eversion_t& ev) const noexcept
  {
    // Combine epoch and version with a simple shift-based mix
    // This is fast and works well when differences are small
    return (size_t)ev.epoch ^ ((size_t)ev.version << 8 |
			       (size_t)ev.version >> (sizeof(size_t) * 8 - 8));
  }
};
}  // namespace std

/**
 * op_queue_type_t
 *
 * Supported op queue types
 */
enum class op_queue_type_t : uint8_t {
  WeightedPriorityQueue = 0,
  mClockScheduler,
  PrioritizedQueue
};
std::string_view get_op_queue_type_name(const op_queue_type_t &q);
std::optional<op_queue_type_t> get_op_queue_type_by_name(
  const std::string_view &s);
