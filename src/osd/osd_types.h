// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
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

#include "msg/msg_types.h"
#include "include/types.h"
#include "include/utime.h"
#include "include/CompatSet.h"
#include "include/interval_set.h"
#include "common/snap_types.h"
#include "common/Formatter.h"
#include "os/hobject.h"
#include "Watch.h"

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


typedef hobject_t collection_list_handle_t;


/**
 * osd request identifier
 *
 * caller name + incarnation# + tid to unique identify this request.
 */
struct osd_reqid_t {
  entity_name_t name; // who
  tid_t         tid;
  int32_t       inc;  // incarnation

  osd_reqid_t()
    : tid(0), inc(0) {}
  osd_reqid_t(const entity_name_t& a, int i, tid_t t)
    : name(a), tid(t), inc(i) {}

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<osd_reqid_t*>& o);
};
WRITE_CLASS_ENCODER(osd_reqid_t)

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

namespace __gnu_cxx {
  template<> struct hash<osd_reqid_t> {
    size_t operator()(const osd_reqid_t &r) const { 
      static hash<uint64_t> H;
      return H(r.name.num() ^ r.tid ^ r.inc);
    }
  };
}


// -----

// a locator constrains the placement of an object.  mainly, which pool
// does it go in.
struct object_locator_t {
  int64_t pool;
  string key;

  explicit object_locator_t()
    : pool(-1) {}
  explicit object_locator_t(int64_t po)
    : pool(po) {}
  explicit object_locator_t(int64_t po, string s)
    : pool(po), key(s) {}

  int get_pool() const {
    return pool;
  }

  void clear() {
    pool = -1;
    key = "";
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<object_locator_t*>& o);
};
WRITE_CLASS_ENCODER(object_locator_t)

inline bool operator==(const object_locator_t& l, const object_locator_t& r) {
  return l.pool == r.pool && l.key == r.key;
}
inline bool operator!=(const object_locator_t& l, const object_locator_t& r) {
  return !(l == r);
}

inline ostream& operator<<(ostream& out, const object_locator_t& loc)
{
  out << "@" << loc.pool;
  if (loc.key.length())
    out << ":" << loc.key;
  return out;
}


// Internal OSD op flags - set by the OSD based on the op types
enum {
  CEPH_OSD_RMW_FLAG_READ        = (1 << 1),
  CEPH_OSD_RMW_FLAG_WRITE       = (1 << 2),
  CEPH_OSD_RMW_FLAG_CLASS_READ  = (1 << 3),
  CEPH_OSD_RMW_FLAG_CLASS_WRITE = (1 << 4),
  CEPH_OSD_RMW_FLAG_PGOP        = (1 << 5),
};


// pg stuff

// object namespaces
#define CEPH_METADATA_NS       1
#define CEPH_DATA_NS           2
#define CEPH_CAS_NS            3
#define CEPH_OSDMETADATA_NS 0xff

// poolsets
enum {
  CEPH_DATA_RULE,
  CEPH_METADATA_RULE,
  CEPH_RBD_RULE,
};

#define OSD_SUPERBLOCK_POBJECT hobject_t(sobject_t(object_t("osd_superblock"), 0))

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
  pg_t(ps_t seed, uint64_t pool, int pref) {
    m_seed = seed;
    m_pool = pool;
    m_preferred = pref;
  }

  pg_t(const ceph_pg& cpg) {
    m_pool = cpg.pool;
    m_seed = cpg.ps;
    m_preferred = (__s16)cpg.preferred;
  }
  old_pg_t get_old_pg() const {
    old_pg_t o;
    assert(m_pool < 0xffffffffull);
    o.v.pool = m_pool;
    o.v.ps = m_seed;
    o.v.preferred = (__s16)m_preferred;
    return o;
  }
  pg_t(const old_pg_t& opg) {
    *this = opg.v;
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

  int print(char *o, int maxlen) const;
  bool parse(const char *s);

  bool is_split(unsigned old_pg_num, unsigned new_pg_num, set<pg_t> *pchildren) const;

  /**
   * Returns b such that for all object o:
   *   ~((~0)<<b) & o.hash) == 0 iff o is in the pg for *this
   */
  unsigned get_split_bits(unsigned pg_num) const;

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

namespace __gnu_cxx {
  template<> struct hash< pg_t >
  {
    size_t operator()( const pg_t& x ) const
    {
      static hash<uint32_t> H;
      return H((x.pool() & 0xffffffff) ^ (x.pool() >> 32) ^ x.ps() ^ x.preferred());
    }
  };
}


// ----------------------

class coll_t {
public:
  const static coll_t META_COLL;

  coll_t()
    : str("meta")
  { }

  explicit coll_t(const std::string &str_)
    : str(str_)
  { }

  explicit coll_t(pg_t pgid, snapid_t snap = CEPH_NOSNAP)
    : str(pg_and_snap_to_str(pgid, snap))
  { }

  static coll_t make_temp_coll(pg_t pgid) {
    return coll_t(pg_to_tmp_str(pgid));
  }

  static coll_t make_removal_coll(uint64_t seq, pg_t pgid) {
    return coll_t(seq_to_removal_str(seq, pgid));
  }

  const std::string& to_str() const {
    return str;
  }

  const char* c_str() const {
    return str.c_str();
  }

  int operator<(const coll_t &rhs) const {
    return str < rhs.str;
  }

  bool is_pg_prefix(pg_t& pgid) const;
  bool is_pg(pg_t& pgid, snapid_t& snap) const;
  bool is_temp(pg_t& pgid) const;
  bool is_removal(uint64_t *seq, pg_t *pgid) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  inline bool operator==(const coll_t& rhs) const {
    return str == rhs.str;
  }
  inline bool operator!=(const coll_t& rhs) const {
    return str != rhs.str;
  }

  void dump(Formatter *f) const;
  static void generate_test_instances(list<coll_t*>& o);

private:
  static std::string pg_and_snap_to_str(pg_t p, snapid_t s) {
    std::ostringstream oss;
    oss << p << "_" << s;
    return oss.str();
  }
  static std::string pg_to_tmp_str(pg_t p) {
    std::ostringstream oss;
    oss << p << "_TEMP";
    return oss.str();
  }
  static std::string seq_to_removal_str(uint64_t seq, pg_t pgid) {
    std::ostringstream oss;
    oss << "FORREMOVAL_" << seq << "_" << pgid;
    return oss.str();
  }

  std::string str;
};

WRITE_CLASS_ENCODER(coll_t)

inline ostream& operator<<(ostream& out, const coll_t& c) {
  out << c.to_str();
  return out;
}

namespace __gnu_cxx {
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
}

inline ostream& operator<<(ostream& out, const ceph_object_layout &ol)
{
  out << pg_t(ol.ol_pgid);
  int su = ol.ol_stripe_unit;
  if (su)
    out << ".su=" << su;
  return out;
}



// compound rados version type
class eversion_t {
public:
  version_t version;
  epoch_t epoch;
  __u32 __pad;
  eversion_t() : version(0), epoch(0), __pad(0) {}
  eversion_t(epoch_t e, version_t v) : version(v), epoch(e), __pad(0) {}

  eversion_t(const ceph_eversion& ce) : 
    version(ce.version),
    epoch(ce.epoch),
    __pad(0) { }

  eversion_t(bufferlist& bl) : __pad(0) { decode(bl); }

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

  void inc(epoch_t e) {
    if (epoch < e)
      epoch = e;
    version++;
  }

  void encode(bufferlist &bl) const {
    ::encode(version, bl);
    ::encode(epoch, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(version, bl);
    ::decode(epoch, bl);
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
inline ostream& operator<<(ostream& out, const eversion_t e) {
  return out << e.epoch << "'" << e.version;
}



/** osd_stat
 * aggregate stats for an osd
 */
struct osd_stat_t {
  int64_t kb, kb_used, kb_avail;
  vector<int> hb_in, hb_out;
  int32_t snap_trim_queue_len, num_snap_trimming;

  osd_stat_t() : kb(0), kb_used(0), kb_avail(0),
		 snap_trim_queue_len(0), num_snap_trimming(0) {}

  void add(const osd_stat_t& o) {
    kb += o.kb;
    kb_used += o.kb_used;
    kb_avail += o.kb_avail;
    snap_trim_queue_len += o.snap_trim_queue_len;
    num_snap_trimming += o.num_snap_trimming;
  }
  void sub(const osd_stat_t& o) {
    kb -= o.kb;
    kb_used -= o.kb_used;
    kb_avail -= o.kb_avail;
    snap_trim_queue_len -= o.snap_trim_queue_len;
    num_snap_trimming -= o.num_snap_trimming;
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
    l.hb_out == r.hb_out;
}
inline bool operator!=(const osd_stat_t& l, const osd_stat_t& r) {
  return !(l == r);
}



inline ostream& operator<<(ostream& out, const osd_stat_t& s) {
  return out << "osd_stat(" << kb_t(s.kb_used) << " used, "
	     << kb_t(s.kb_avail) << " avail, "
	     << kb_t(s.kb) << " total, "
	     << "peers " << s.hb_in << "/" << s.hb_out << ")";
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
#define PG_STATE_SCRUBQ       (1<<9)  // queued for scrub
#define PG_STATE_DEGRADED     (1<<10) // pg membership not complete
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

std::string pg_state_string(int state);


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
 * pg_pool
 */
struct pg_pool_t {
  enum {
    TYPE_REP = 1,     // replication
    TYPE_RAID4 = 2,   // raid4 (never implemented)
  };
  enum {
    FLAG_HASHPSPOOL = 1, // hash pg seed and pool together (instead of adding)
    FLAG_FULL       = 2, // pool is full
  };

  static const char *get_type_name(int t) {
    switch (t) {
    case TYPE_REP: return "rep";
    case TYPE_RAID4: return "raid4";
    default: return "???";
    }
  }
  const char *get_type_name() const {
    return get_type_name(type);
  }

  uint64_t flags;           /// FLAG_* 
  __u8 type;                /// TYPE_*
  __u8 size, min_size;      /// number of osds in each pg
  __u8 crush_ruleset;       /// crush placement rule set
  __u8 object_hash;         /// hash mapping object name to ps
private:
  __u32 pg_num, pgp_num;    /// number of pgs
public:
  epoch_t last_change;      /// most recent epoch changed, exclusing snapshot changes
  snapid_t snap_seq;        /// seq for per-pool snapshot
  epoch_t snap_epoch;       /// osdmap epoch of last snap
  uint64_t auid;            /// who owns the pg
  __u32 crash_replay_interval; /// seconds to allow clients to replay ACKed but unCOMMITted requests

  uint64_t quota_max_bytes; /// maximum number of bytes for this pool
  uint64_t quota_max_objects; /// maximum number of objects for this pool

  /*
   * Pool snaps (global to this pool).  These define a SnapContext for
   * the pool, unless the client manually specifies an alternate
   * context.
   */
  map<snapid_t, pool_snap_info_t> snaps;
  /*
   * Alternatively, if we are definining non-pool snaps (e.g. via the
   * Ceph MDS), we must track @removed_snaps (since @snaps is not
   * used).  Snaps and removed_snaps are to be used exclusive of each
   * other!
   */
  interval_set<snapid_t> removed_snaps;

  int pg_num_mask, pgp_num_mask;

  pg_pool_t()
    : flags(0), type(0), size(0), min_size(0),
      crush_ruleset(0), object_hash(0),
      pg_num(0), pgp_num(0),
      last_change(0),
      snap_seq(0), snap_epoch(0),
      auid(0),
      crash_replay_interval(0),
      quota_max_bytes(0), quota_max_objects(0),
      pg_num_mask(0), pgp_num_mask(0) { }

  void dump(Formatter *f) const;

  uint64_t get_flags() const { return flags; }
  unsigned get_type() const { return type; }
  unsigned get_size() const { return size; }
  unsigned get_min_size() const { return min_size; }
  int get_crush_ruleset() const { return crush_ruleset; }
  int get_object_hash() const { return object_hash; }
  const char *get_object_hash_name() const {
    return ceph_str_hash_name(get_object_hash());
  }
  epoch_t get_last_change() const { return last_change; }
  epoch_t get_snap_epoch() const { return snap_epoch; }
  snapid_t get_snap_seq() const { return snap_seq; }
  uint64_t get_auid() const { return auid; }
  unsigned get_crash_replay_interval() const { return crash_replay_interval; }

  void set_snap_seq(snapid_t s) { snap_seq = s; }
  void set_snap_epoch(epoch_t e) { snap_epoch = e; }

  bool is_rep()   const { return get_type() == TYPE_REP; }
  bool is_raid4() const { return get_type() == TYPE_RAID4; }

  unsigned get_pg_num() const { return pg_num; }
  unsigned get_pgp_num() const { return pgp_num; }

  unsigned get_pg_num_mask() const { return pg_num_mask; }
  unsigned get_pgp_num_mask() const { return pgp_num_mask; }

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
 */
struct object_stat_sum_t {
  int64_t num_bytes;    // in bytes
  int64_t num_objects;
  int64_t num_object_clones;
  int64_t num_object_copies;  // num_objects * num_replicas
  int64_t num_objects_missing_on_primary;
  int64_t num_objects_degraded;
  int64_t num_objects_unfound;
  int64_t num_rd, num_rd_kb;
  int64_t num_wr, num_wr_kb;
  int64_t num_scrub_errors;	// total deep and shallow scrub errors
  int64_t num_shallow_scrub_errors;
  int64_t num_deep_scrub_errors;
  int64_t num_objects_recovered;
  int64_t num_bytes_recovered;
  int64_t num_keys_recovered;

  object_stat_sum_t()
    : num_bytes(0),
      num_objects(0), num_object_clones(0), num_object_copies(0),
      num_objects_missing_on_primary(0), num_objects_degraded(0), num_objects_unfound(0),
      num_rd(0), num_rd_kb(0), num_wr(0), num_wr_kb(0),
      num_scrub_errors(0), num_shallow_scrub_errors(0),
      num_deep_scrub_errors(0),
      num_objects_recovered(0),
      num_bytes_recovered(0),
      num_keys_recovered(0)
  {}

  void clear() {
    memset(this, 0, sizeof(*this));
  }

  void calc_copies(int nrep) {
    num_object_copies = nrep * num_objects;
  }

  bool is_zero() const {
    object_stat_sum_t zero;
    return memcmp(this, &zero, sizeof(zero)) == 0;
  }

  void add(const object_stat_sum_t& o);
  void sub(const object_stat_sum_t& o);

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<object_stat_sum_t*>& o);
};
WRITE_CLASS_ENCODER(object_stat_sum_t)

/**
 * a collection of object stat sums
 *
 * This is a collection of stat sums over different categories.
 */
struct object_stat_collection_t {
  object_stat_sum_t sum;
  map<string,object_stat_sum_t> cat_sum;

  void calc_copies(int nrep) {
    sum.calc_copies(nrep);
    for (map<string,object_stat_sum_t>::iterator p = cat_sum.begin(); p != cat_sum.end(); ++p)
      p->second.calc_copies(nrep);
  }

  void dump(Formatter *f) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  static void generate_test_instances(list<object_stat_collection_t*>& o);

  bool is_zero() const {
    return (cat_sum.empty() && sum.is_zero());
  }

  void clear() {
    sum.clear();
    cat_sum.clear();
  }

  void add(const object_stat_sum_t& o, const string& cat) {
    sum.add(o);
    if (cat.length())
      cat_sum[cat].add(o);
  }

  void add(const object_stat_collection_t& o) {
    sum.add(o.sum);
    for (map<string,object_stat_sum_t>::const_iterator p = o.cat_sum.begin();
	 p != o.cat_sum.end();
	 ++p)
      cat_sum[p->first].add(p->second);
  }
  void sub(const object_stat_collection_t& o) {
    sum.sub(o.sum);
    for (map<string,object_stat_sum_t>::const_iterator p = o.cat_sum.begin();
	 p != o.cat_sum.end();
	 ++p) {
      object_stat_sum_t& s = cat_sum[p->first];
      s.sub(p->second);
      if (s.is_zero())
	cat_sum.erase(p->first);
    }
  }
};
WRITE_CLASS_ENCODER(object_stat_collection_t)

/** pg_stat
 * aggregate stats for a single PG.
 */
struct pg_stat_t {
  eversion_t version;
  eversion_t reported;
  __u32 state;
  utime_t last_fresh;   // last reported
  utime_t last_change;  // new state != previous state
  utime_t last_active;  // state & PG_STATE_ACTIVE
  utime_t last_clean;   // state & PG_STATE_CLEAN
  utime_t last_unstale; // (state & PG_STATE_STALE) == 0

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
  bool stats_invalid;

  int64_t log_size;
  int64_t ondisk_log_size;    // >= active_log_size

  vector<int> up, acting;
  epoch_t mapping_epoch;

  utime_t last_became_active;

  pg_stat_t()
    : state(0),
      created(0), last_epoch_clean(0),
      parent_split_bits(0),
      stats_invalid(false),
      log_size(0), ondisk_log_size(0),
      mapping_epoch(0)
  { }

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

  void dump(Formatter *f) const;
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(list<pg_stat_t*>& o);
};
WRITE_CLASS_ENCODER(pg_stat_t)

/*
 * summation over an entire pool
 */
struct pool_stat_t {
  object_stat_collection_t stats;
  uint64_t log_size;
  uint64_t ondisk_log_size;    // >= active_log_size

  pool_stat_t() : log_size(0), ondisk_log_size(0)
  { }

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

  bool is_zero() const {
    return (stats.is_zero() &&
	    log_size == 0 &&
	    ondisk_log_size == 0);
  }

  void dump(Formatter *f) const;
  void encode(bufferlist &bl, uint64_t features) const;
  void decode(bufferlist::iterator &bl);
  static void generate_test_instances(list<pool_stat_t*>& o);
};
WRITE_CLASS_ENCODER_FEATURES(pool_stat_t)


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
  
  epoch_t same_up_since;       // same acting set since
  epoch_t same_interval_since;   // same acting AND up set since
  epoch_t same_primary_since;  // same primary at least back through this epoch.

  eversion_t last_scrub;
  eversion_t last_deep_scrub;
  utime_t last_scrub_stamp;
  utime_t last_deep_scrub_stamp;
  utime_t last_clean_scrub_stamp;

  pg_history_t()
    : epoch_created(0),
      last_epoch_started(0), last_epoch_clean(0), last_epoch_split(0),
      same_up_since(0), same_interval_since(0), same_primary_since(0) {}
  
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

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_history_t*>& o);
};
WRITE_CLASS_ENCODER(pg_history_t)

inline ostream& operator<<(ostream& out, const pg_history_t& h) {
  return out << "ec=" << h.epoch_created
	     << " les/c " << h.last_epoch_started << "/" << h.last_epoch_clean
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
  pg_t pgid;
  eversion_t last_update;    // last object version applied to store.
  eversion_t last_complete;  // last version pg was complete through.
  epoch_t last_epoch_started;// last epoch at which this pg started on this osd
  
  eversion_t log_tail;     // oldest log entry.

  hobject_t last_backfill;   // objects >= this and < last_complete may be missing

  interval_set<snapid_t> purged_snaps;

  pg_stat_t stats;

  pg_history_t history;

  pg_info_t()
    : last_epoch_started(0), last_backfill(hobject_t::get_max())
  { }
  pg_info_t(pg_t p)
    : pgid(p),
      last_epoch_started(0), last_backfill(hobject_t::get_max())
  { }
  
  bool is_empty() const { return last_update.version == 0; }
  bool dne() const { return history.epoch_created == 0; }

  bool is_incomplete() const { return last_backfill != hobject_t::get_max(); }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);
  void dump(Formatter *f) const;
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
    if (pgi.is_incomplete())
      out << " lb " << pgi.last_backfill;
  }
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
  pg_notify_t() : query_epoch(0), epoch_sent(0) {}
  pg_notify_t(epoch_t query_epoch,
	      epoch_t epoch_sent,
	      const pg_info_t &info)
    : query_epoch(query_epoch),
      epoch_sent(epoch_sent),
      info(info) {}
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
  vector<int> up, acting;
  epoch_t first, last;
  bool maybe_went_rw;

  pg_interval_t() : first(0), last(0), maybe_went_rw(false) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<pg_interval_t*>& o);

  /**
   * Integrates a new map into *past_intervals, returns true
   * if an interval was closed out.
   */
  static bool check_new_interval(
    const vector<int> &old_acting,              ///< [in] acting as of lastmap
    const vector<int> &new_acting,              ///< [in] acting as of osdmap
    const vector<int> &old_up,                  ///< [in] up as of lastmap
    const vector<int> &new_up,                  ///< [in] up as of osdmap
    epoch_t same_interval_since,                ///< [in] as of osdmap
    epoch_t last_epoch_clean,                   ///< [in] current
    std::tr1::shared_ptr<const OSDMap> osdmap,  ///< [in] current map
    std::tr1::shared_ptr<const OSDMap> lastmap, ///< [in] last map
    int64_t poolid,                             ///< [in] pool for pg
    pg_t pgid,                                  ///< [in] pgid for pg
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

  pg_query_t() : type(-1), epoch_sent(0) {}
  pg_query_t(int t, const pg_history_t& h,
	     epoch_t epoch_sent)
    : type(t), history(h),
      epoch_sent(epoch_sent) {
    assert(t != LOG);
  }
  pg_query_t(int t, eversion_t s, const pg_history_t& h,
	     epoch_t epoch_sent)
    : type(t), since(s), history(h),
      epoch_sent(epoch_sent) {
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


/**
 * pg_log_entry_t - single entry/event in pg log
 *
 */
struct pg_log_entry_t {
  enum {
    MODIFY = 1,
    CLONE = 2,
    DELETE = 3,
    BACKLOG = 4,  // event invented by generate_backlog [deprecated]
    LOST_REVERT = 5, // lost new version, revert to an older version.
    LOST_DELETE = 6, // lost new version, revert to no object (deleted).
    LOST_MARK = 7,   // lost new version, now EIO
  };
  static const char *get_op_name(int op) {
    switch (op) {
    case MODIFY:
      return "modify  ";
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
    default:
      return "unknown ";
    }
  }
  const char *get_op_name() const {
    return get_op_name(op);
  }

  __s32      op;
  hobject_t  soid;
  eversion_t version, prior_version, reverting_to;
  osd_reqid_t reqid;  // caller+tid to uniquely identify request
  utime_t     mtime;  // this is the _user_ mtime, mind you
  bufferlist snaps;   // only for clone entries
  bool invalid_hash; // only when decoding sobject_t based entries
  bool invalid_pool; // only when decoding pool-less hobject based entries

  uint64_t offset;   // [soft state] my offset on disk
      
  pg_log_entry_t()
    : op(0), invalid_hash(false), invalid_pool(false), offset(0) {}
  pg_log_entry_t(int _op, const hobject_t& _soid, 
		 const eversion_t& v, const eversion_t& pv,
		 const osd_reqid_t& rid, const utime_t& mt)
    : op(_op), soid(_soid), version(v),
      prior_version(pv),
      reqid(rid), mtime(mt), invalid_hash(false), invalid_pool(false),
      offset(0) {}
      
  bool is_clone() const { return op == CLONE; }
  bool is_modify() const { return op == MODIFY; }
  bool is_backlog() const { return op == BACKLOG; }
  bool is_lost_revert() const { return op == LOST_REVERT; }
  bool is_lost_delete() const { return op == LOST_DELETE; }
  bool is_lost_mark() const { return op == LOST_MARK; }

  bool is_update() const {
    return is_clone() || is_modify() || is_backlog() || is_lost_revert() || is_lost_mark();
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

  list<pg_log_entry_t> log;  // the actual log.
  
  pg_log_t() {}

  void clear() {
    eversion_t z;
    head = tail = z;
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

  list<pg_log_entry_t>::const_iterator find_entry(eversion_t v) const {
    int fromhead = head.version - v.version;
    int fromtail = v.version - tail.version;
    list<pg_log_entry_t>::const_iterator p;
    if (fromhead < fromtail) {
      p = log.end();
      --p;
      while (p->version > v)
	--p;
      return p;
    } else {
      p = log.begin();
      while (p->version < v)
	++p;
      return p;
    }      
  }

  list<pg_log_entry_t>::iterator find_entry(eversion_t v) {
    int fromhead = head.version - v.version;
    int fromtail = v.version - tail.version;
    list<pg_log_entry_t>::iterator p;
    if (fromhead < fromtail) {
      p = log.end();
      --p;
      while (p->version > v)
	--p;
      return p;
    } else {
      p = log.begin();
      while (p->version < v)
	++p;
      return p;
    }      
  }

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
   * @parem to up to and including this version
   */
  void copy_range(const pg_log_t &other, eversion_t from, eversion_t to);

  /**
   * copy up to N entries
   *
   * @param o source log
   * @param max max number of entreis to copy
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
  out << "log(" << log.tail << "," << log.head << "]";
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
    item(eversion_t n) : need(n) {}  // have no old version
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

  map<hobject_t, item> missing;         // oid -> (need v, have v)
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
  void rm(const std::map<hobject_t, pg_missing_t::item>::iterator &m);
  void got(const hobject_t& oid, eversion_t v);
  void got(const std::map<hobject_t, pg_missing_t::item>::iterator &m);
  void split_into(pg_t child_pgid, unsigned split_bits, pg_missing_t *omissing);

  void clear() {
    missing.clear();
    rmissing.clear();
  }

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
    o.back()->handle = hobject_t(object_t("hi"), "key", 1, 2, -1);
    o.back()->entries.push_back(make_pair(object_t("one"), string()));
    o.back()->entries.push_back(make_pair(object_t("two"), string("twokey")));
  }
};

WRITE_CLASS_ENCODER(pg_ls_response_t)


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

  object_locator_t oloc;   // object locator (pool etc)

  vector<pair<uint64_t,uint64_t> >  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : objectno(0), offset(0), length(0) {}
  ObjectExtent(object_t o, uint64_t ono, uint64_t off, uint64_t l) : oid(o), objectno(ono), offset(off), length(l) { }
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

  SnapSet() : head_exists(false) {}
  SnapSet(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }
    
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<SnapSet*>& o);  
};
WRITE_CLASS_ENCODER(SnapSet)

ostream& operator<<(ostream& out, const SnapSet& cs);



#define OI_ATTR "_"
#define SS_ATTR "snapset"

struct watch_info_t {
  uint64_t cookie;
  uint32_t timeout_seconds;

  watch_info_t(uint64_t c=0, uint32_t t=0) : cookie(c), timeout_seconds(t) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<watch_info_t*>& o);
};
WRITE_CLASS_ENCODER(watch_info_t)

static inline bool operator==(const watch_info_t& l, const watch_info_t& r) {
  return l.cookie == r.cookie && l.timeout_seconds == r.timeout_seconds;
}

static inline ostream& operator<<(ostream& out, const watch_info_t& w) {
  return out << "watch(cookie " << w.cookie << " " << w.timeout_seconds << "s)";
}

struct notify_info_t {
  uint64_t cookie;
  uint32_t timeout;
  bufferlist bl;
};

static inline ostream& operator<<(ostream& out, const notify_info_t& n) {
  return out << "notify(cookie " << n.cookie << " " << n.timeout << "s)";
}


struct object_info_t {
  hobject_t soid;
  object_locator_t oloc;
  string category;

  eversion_t version, prior_version;
  eversion_t user_version;
  osd_reqid_t last_reqid;

  uint64_t size;
  utime_t mtime;
  bool lost;

  osd_reqid_t wrlock_by;   // [head]
  vector<snapid_t> snaps;  // [clone]

  uint64_t truncate_seq, truncate_size;


  map<pair<uint64_t, entity_name_t>, watch_info_t> watchers;
  bool uses_tmap;

  void copy_user_bits(const object_info_t& other);

  static ps_t legacy_object_locator_to_ps(const object_t &oid, 
					  const object_locator_t &loc);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(list<object_info_t*>& o);

  explicit object_info_t()
    : size(0), lost(false),
      truncate_seq(0), truncate_size(0), uses_tmap(false)
  {}

  object_info_t(const hobject_t& s, const object_locator_t& o)
    : soid(s), oloc(o), size(0),
      lost(false), truncate_seq(0), truncate_size(0), uses_tmap(false) {}

  object_info_t(bufferlist& bl) {
    decode(bl);
  }
};
WRITE_CLASS_ENCODER(object_info_t)

struct ObjectState {
  object_info_t oi;
  bool exists;

  ObjectState(const object_info_t &oi_, bool exists_)
    : oi(oi_), exists(exists_) {}
};


struct SnapSetContext {
  object_t oid;
  int ref;
  bool registered;
  SnapSet snapset;

  SnapSetContext(const object_t& o) : oid(o), ref(0), registered(false) { }
};


/*
  * keep tabs on object modifications that are in flight.
  * we need to know the projected existence, size, snapset,
  * etc., because we don't send writes down to disk until after
  * replicas ack.
  */
struct ObjectContext {
  int ref;
  bool registered;
  ObjectState obs;

  SnapSetContext *ssc;  // may be null

private:
  Mutex lock;
public:
  Cond cond;
  int unstable_writes, readers, writers_waiting, readers_waiting;

  // set if writes for this object are blocked on another objects recovery
  ObjectContext *blocked_by;      // object blocking our writes
  set<ObjectContext*> blocking;   // objects whose writes we block

  // any entity in obs.oi.watchers MUST be in either watchers or unconnected_watchers.
  map<pair<uint64_t, entity_name_t>, WatchRef> watchers;

  ObjectContext(const object_info_t &oi_, bool exists_, SnapSetContext *ssc_)
    : ref(0), registered(false), obs(oi_, exists_), ssc(ssc_),
      lock("ReplicatedPG::ObjectContext::lock"),
      unstable_writes(0), readers(0), writers_waiting(0), readers_waiting(0),
      blocked_by(0) {}

  void get() { ++ref; }

  // do simple synchronous mutual exclusion, for now.  now waitqueues or anything fancy.
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
};

inline ostream& operator<<(ostream& out, ObjectState& obs)
{
  out << obs.oi.soid;
  if (!obs.exists)
    out << "(dne)";
  return out;
}

inline ostream& operator<<(ostream& out, ObjectContext& obc)
{
  return out << "obc(" << obc.obs << ")";
}


ostream& operator<<(ostream& out, const object_info_t& oi);



// Object recovery
struct ObjectRecoveryInfo {
  hobject_t soid;
  eversion_t version;
  uint64_t size;
  object_info_t oi;
  SnapSet ss;
  interval_set<uint64_t> copy_subset;
  map<hobject_t, interval_set<uint64_t> > clone_subset;

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
  bool first;
  uint64_t data_recovered_to;
  bool data_complete;
  string omap_recovered_to;
  bool omap_complete;

  ObjectRecoveryProgress()
    : first(true),
      data_recovered_to(0),
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


/*
 * summarize pg contents for purposes of a scrub
 */
struct ScrubMap {
  struct object {
    uint64_t size;
    bool negative;
    map<string,bufferptr> attrs;
    __u32 digest;
    bool digest_present;
    uint32_t nlinks;
    set<snapid_t> snapcolls;
    __u32 omap_digest;
    bool omap_digest_present;
    bool read_error;

    object() :
      // Init invalid size so it won't match if we get a stat EIO error
      size(-1), negative(false), digest(0), digest_present(false),
      nlinks(0), omap_digest(0), omap_digest_present(false),
      read_error(false) {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<object*>& o);
  };
  WRITE_CLASS_ENCODER(object)

  map<hobject_t,object> objects;
  map<string,bufferptr> attrs;
  bufferlist logbl;
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
   * @param in  [out] combined data buffer
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
   * @param in  [out] combined data buffer
   */
  static void merge_osd_op_vector_out_data(vector<OSDOp>& ops, bufferlist& out);
};

ostream& operator<<(ostream& out, const OSDOp& op);

struct watch_item_t {
  entity_name_t name;
  uint64_t cookie;
  uint32_t timeout_seconds;

  watch_item_t() : cookie(0), timeout_seconds(0) { }
  watch_item_t(entity_name_t name, uint64_t cookie, uint32_t timeout)
    : name(name), cookie(cookie), timeout_seconds(timeout) { }

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(name, bl);
    ::encode(cookie, bl);
    ::encode(timeout_seconds, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(name, bl);
    ::decode(cookie, bl);
    ::decode(timeout_seconds, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(watch_item_t)

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
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(list<obj_list_watch_response_t*>& o) {
    o.push_back(new obj_list_watch_response_t);
    o.push_back(new obj_list_watch_response_t);
    o.back()->entries.push_back(watch_item_t(entity_name_t(entity_name_t::TYPE_CLIENT, 1), 10, 30));
    o.back()->entries.push_back(watch_item_t(entity_name_t(entity_name_t::TYPE_CLIENT, 2), 20, 60));
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

#endif
