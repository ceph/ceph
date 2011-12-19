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
#include <stdexcept>

#include "msg/msg_types.h"
#include "include/types.h"
#include "include/utime.h"
#include "include/CompatSet.h"
#include "include/interval_set.h"
#include "common/Formatter.h"



#define CEPH_OSD_ONDISK_MAGIC "ceph osd volume v026"

#define CEPH_OSD_FEATURE_INCOMPAT_BASE CompatSet::Feature(1, "initial feature set(~v.18)")
#define CEPH_OSD_FEATURE_INCOMPAT_PGINFO CompatSet::Feature(2, "pginfo object")
#define CEPH_OSD_FEATURE_INCOMPAT_OLOC CompatSet::Feature(3, "object locator")
#define CEPH_OSD_FEATURE_INCOMPAT_LEC  CompatSet::Feature(4, "last_epoch_clean")
#define CEPH_OSD_FEATURE_INCOMPAT_CATEGORIES  CompatSet::Feature(5, "categories")


/* osdreqid_t - caller name + incarnation# + tid to unique identify this request
 * use for metadata and osd ops.
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

  int print(char *o, int maxlen) const;
  bool parse(const char *s);

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
  const static coll_t TEMP_COLL;

  coll_t()
    : str("meta")
  { }

  explicit coll_t(const std::string &str_)
    : str(str_)
  { }

  explicit coll_t(pg_t pgid, snapid_t snap = CEPH_NOSNAP)
    : str(pg_and_snap_to_str(pgid, snap))
  { }

  const std::string& to_str() const {
    return str;
  }

  const char* c_str() const {
    return str.c_str();
  }

  int operator<(const coll_t &rhs) const {
    return str < rhs.str;
  }

  bool is_pg(pg_t& pgid, snapid_t& snap) const;
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  inline bool operator==(const coll_t& rhs) const {
    return str == rhs.str;
  }
  inline bool operator!=(const coll_t& rhs) const {
    return str != rhs.str;
  }

private:
  static std::string pg_and_snap_to_str(pg_t p, snapid_t s) {
    std::ostringstream oss;
    oss << p << "_" << s;
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

  void dump(Formatter *f) const {
    f->dump_unsigned("kb", kb);
    f->dump_unsigned("kb_used", kb_used);
    f->dump_unsigned("kb_avail", kb_avail);
    f->open_array_section("hb_in");
    for (vector<int>::const_iterator p = hb_in.begin(); p != hb_in.end(); ++p)
      f->dump_int("osd", *p);
    f->close_section();
    f->open_array_section("hb_out");
    for (vector<int>::const_iterator p = hb_out.begin(); p != hb_out.end(); ++p)
      f->dump_int("osd", *p);
    f->close_section();
    f->dump_int("snap_trim_queue_len", snap_trim_queue_len);
    f->dump_int("num_snap_trimming", num_snap_trimming);
  }

  void encode(bufferlist &bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(kb, bl);
    ::encode(kb_used, bl);
    ::encode(kb_avail, bl);
    ::encode(snap_trim_queue_len, bl);
    ::encode(num_snap_trimming, bl);
    ::encode(hb_in, bl);
    ::encode(hb_out, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(kb, bl);
    ::decode(kb_used, bl);
    ::decode(kb_avail, bl);
    ::decode(snap_trim_queue_len, bl);
    ::decode(num_snap_trimming, bl);
    ::decode(hb_in, bl);
    ::decode(hb_out, bl);
  }
  
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
#define PG_STATE_STRAY        (1<<6)  // i must notify the primary i exist.
#define PG_STATE_SPLITTING    (1<<7)  // i am splitting
#define PG_STATE_SCRUBBING    (1<<8)  // scrubbing
#define PG_STATE_SCRUBQ       (1<<9)  // queued for scrub
#define PG_STATE_DEGRADED     (1<<10) // pg membership not complete
#define PG_STATE_INCONSISTENT (1<<11) // pg replicas are inconsistent (but shouldn't be)
#define PG_STATE_PEERING      (1<<12) // pg is (re)peering
#define PG_STATE_REPAIR       (1<<13) // pg should repair on next scrub
#define PG_STATE_SCANNING     (1<<14) // scanning content to generate backlog

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
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
};
WRITE_CLASS_ENCODER(pool_snap_info_t)

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
  __u8 size;                /// number of osds in each pg
  __u8 crush_ruleset;       /// crush placement rule set
  __u8 object_hash;         /// hash mapping object name to ps
  __u32 pg_num, pgp_num;    /// number of pgs
  __u32 lpg_num, lpgp_num;  /// number of localized pgs
  epoch_t last_change;      /// most recent epoch changed, exclusing snapshot changes
  snapid_t snap_seq;        /// seq for per-pool snapshot
  epoch_t snap_epoch;       /// osdmap epoch of last snap
  uint64_t auid;            /// who owns the pg
  __u32 crash_replay_interval; /// seconds to allow clients to replay ACKed but unCOMMITted requests

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

  int pg_num_mask, pgp_num_mask, lpg_num_mask, lpgp_num_mask;

  pg_pool_t()
    : flags(0), type(0), size(0), crush_ruleset(0), object_hash(0),
      pg_num(0), pgp_num(0), lpg_num(0), lpgp_num(0),
      last_change(0),
      snap_seq(0), snap_epoch(0),
      auid(0),
      crash_replay_interval(0),
      pg_num_mask(0), pgp_num_mask(0), lpg_num_mask(0), lpgp_num_mask(0) { }

  void dump(Formatter *f) const;

  uint64_t get_flags() const { return flags; }
  unsigned get_type() const { return type; }
  unsigned get_size() const { return size; }
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
  unsigned get_lpg_num() const { return lpg_num; }
  unsigned get_lpgp_num() const { return lpgp_num; }

  unsigned get_pg_num_mask() const { return pg_num_mask; }
  unsigned get_pgp_num_mask() const { return pgp_num_mask; }
  unsigned get_lpg_num_mask() const { return lpg_num_mask; }
  unsigned get_lpgp_num_mask() const { return lpgp_num_mask; }

  int calc_bits_of(int t);
  void calc_pg_masks();

  /*
   * we have two snap modes:
   *  - pool global snaps
   *    - snap existence/non-existence defined by snaps[] and snap_seq
   *  - user managed snaps
   *    - removal governed by removed_snaps
   *
   * we know which mode we're using based on whether removed_snaps is empty.
   */
  bool is_pool_snaps_mode() const;
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
};
WRITE_CLASS_ENCODER_FEATURES(pg_pool_t)

ostream& operator<<(ostream& out, const pg_pool_t& p);


/*
 * object_stat_sum_t - a summation of object stats
 */
struct object_stat_sum_t {
  int64_t num_bytes;    // in bytes
  int64_t num_kb;       // in KB
  int64_t num_objects;
  int64_t num_object_clones;
  int64_t num_object_copies;  // num_objects * num_replicas
  int64_t num_objects_missing_on_primary;
  int64_t num_objects_degraded;
  int64_t num_objects_unfound;
  int64_t num_rd, num_rd_kb;
  int64_t num_wr, num_wr_kb;

  object_stat_sum_t()
    : num_bytes(0), num_kb(0),
      num_objects(0), num_object_clones(0), num_object_copies(0),
      num_objects_missing_on_primary(0), num_objects_degraded(0), num_objects_unfound(0),
      num_rd(0), num_rd_kb(0), num_wr(0), num_wr_kb(0)
  {}

  void calc_copies_degraded(int nrep, int acting_nrep) {
    num_object_copies = nrep * num_objects;
    num_objects_degraded = (nrep - acting_nrep) * num_objects;
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("num_bytes", num_bytes);
    f->dump_unsigned("num_kb", num_kb);
    f->dump_unsigned("num_objects", num_objects);
    f->dump_unsigned("num_object_clones", num_object_clones);
    f->dump_unsigned("num_object_copies", num_object_copies);
    f->dump_unsigned("num_objects_missing_on_primary", num_objects_missing_on_primary);
    f->dump_unsigned("num_objects_degraded", num_objects_degraded);
    f->dump_unsigned("num_objects_unfound", num_objects_unfound);
    f->dump_unsigned("num_read", num_rd);
    f->dump_unsigned("num_read_kb", num_rd_kb);
    f->dump_unsigned("num_write", num_wr);
    f->dump_unsigned("num_write_kb", num_wr_kb);
  }
  void encode(bufferlist& bl) const {
    __u8 v = 2;
    ::encode(v, bl);
    ::encode(num_bytes, bl);
    ::encode(num_kb, bl);
    ::encode(num_objects, bl);
    ::encode(num_object_clones, bl);
    ::encode(num_object_copies, bl);
    ::encode(num_objects_missing_on_primary, bl);
    ::encode(num_objects_degraded, bl);
    ::encode(num_objects_unfound, bl);
    ::encode(num_rd, bl);
    ::encode(num_rd_kb, bl);
    ::encode(num_wr, bl);
    ::encode(num_wr_kb, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(num_bytes, bl);
    ::decode(num_kb, bl);
    ::decode(num_objects, bl);
    ::decode(num_object_clones, bl);
    ::decode(num_object_copies, bl);
    ::decode(num_objects_missing_on_primary, bl);
    ::decode(num_objects_degraded, bl);
    if (v >= 2)
      ::decode(num_objects_unfound, bl);
    ::decode(num_rd, bl);
    ::decode(num_rd_kb, bl);
    ::decode(num_wr, bl);
    ::decode(num_wr_kb, bl);
  }
  void add(const object_stat_sum_t& o) {
    num_bytes += o.num_bytes;
    num_kb += o.num_kb;
    num_objects += o.num_objects;
    num_object_clones += o.num_object_clones;
    num_object_copies += o.num_object_copies;
    num_objects_missing_on_primary += o.num_objects_missing_on_primary;
    num_objects_degraded += o.num_objects_degraded;
    num_rd += o.num_rd;
    num_rd_kb += o.num_rd_kb;
    num_wr += o.num_wr;
    num_wr_kb += o.num_wr_kb;
    num_objects_unfound += o.num_objects_unfound;
  }
  void sub(const object_stat_sum_t& o) {
    num_bytes -= o.num_bytes;
    num_kb -= o.num_kb;
    num_objects -= o.num_objects;
    num_object_clones -= o.num_object_clones;
    num_object_copies -= o.num_object_copies;
    num_objects_missing_on_primary -= o.num_objects_missing_on_primary;
    num_objects_degraded -= o.num_objects_degraded;
    num_rd -= o.num_rd;
    num_rd_kb -= o.num_rd_kb;
    num_wr -= o.num_wr;
    num_wr_kb -= o.num_wr_kb;
    num_objects_unfound -= o.num_objects_unfound;
  }
};
WRITE_CLASS_ENCODER(object_stat_sum_t)

struct object_stat_collection_t {
  object_stat_sum_t sum;
  map<string,object_stat_sum_t> cat_sum;

  void calc_copies_degraded(int nrep, int acting_nrep) {
    sum.calc_copies_degraded(nrep, acting_nrep);
    for (map<string,object_stat_sum_t>::iterator p = cat_sum.begin(); p != cat_sum.end(); ++p)
      p->second.calc_copies_degraded(nrep, acting_nrep);
  }

  void dump(Formatter *f) const {
    f->open_object_section("stat_sum");
    sum.dump(f);
    f->close_section();
    f->open_object_section("stat_cat_sum");
    for (map<string,object_stat_sum_t>::const_iterator p = cat_sum.begin(); p != cat_sum.end(); ++p) {
      f->open_object_section(p->first.c_str());
      p->second.dump(f);
      f->close_section();
    }
    f->close_section();
  }
  void encode(bufferlist& bl) const {
    __u8 v = 1;
    ::encode(v, bl);
    ::encode(sum, bl);
    ::encode(cat_sum, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 v;
    ::decode(v, bl);
    ::decode(sum, bl);
    ::decode(cat_sum, bl);
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
	 ++p)
      cat_sum[p->first].sub(p->second);
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

  eversion_t log_start;         // (log_start,version]
  eversion_t ondisk_log_start;  // there may be more on disk

  epoch_t created;
  epoch_t last_epoch_clean;
  pg_t parent;
  __u32 parent_split_bits;

  eversion_t last_scrub;
  utime_t last_scrub_stamp;

  object_stat_collection_t stats;

  int64_t log_size;
  int64_t ondisk_log_size;    // >= active_log_size

  vector<int> up, acting;


  pg_stat_t() : state(0),
		created(0), parent_split_bits(0), 
		log_size(0), ondisk_log_size(0)
  { }

  void dump(Formatter *f) const {
    f->dump_stream("version") << version;
    f->dump_stream("reported") << reported;
    f->dump_string("state", pg_state_string(state));
    f->dump_stream("log_start") << log_start;
    f->dump_stream("ondisk_log_start") << ondisk_log_start;
    f->dump_unsigned("created", created);
    f->dump_unsigned("last_epoch_clean", created);
    f->dump_stream("parent") << parent;
    f->dump_unsigned("parent_split_bits", parent_split_bits);
    f->dump_stream("last_scrub") << last_scrub;
    f->dump_stream("last_scrub_stamp") << last_scrub_stamp;
    f->dump_unsigned("log_size", log_size);
    f->dump_unsigned("ondisk_log_size", ondisk_log_size);
    stats.dump(f);
    f->open_array_section("up");
    for (vector<int>::const_iterator p = up.begin(); p != up.end(); ++p)
      f->dump_int("osd", *p);
    f->close_section();
    f->open_array_section("acting");
    for (vector<int>::const_iterator p = acting.begin(); p != acting.end(); ++p)
      f->dump_int("osd", *p);
    f->close_section();
  }

  void encode(bufferlist &bl) const {
    __u8 v = 7;
    ::encode(v, bl);

    ::encode(version, bl);
    ::encode(reported, bl);
    ::encode(state, bl);
    ::encode(log_start, bl);
    ::encode(ondisk_log_start, bl);
    ::encode(created, bl);
    ::encode(last_epoch_clean, bl);
    ::encode(parent, bl);
    ::encode(parent_split_bits, bl);
    ::encode(last_scrub, bl);
    ::encode(last_scrub_stamp, bl);
    ::encode(stats, bl);
    ::encode(log_size, bl);
    ::encode(ondisk_log_size, bl);
    ::encode(up, bl);
    ::encode(acting, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 v;
    ::decode(v, bl);
    if (v > 7)
      throw buffer::malformed_input("unknown pg_stat_t encoding version > 4");

    ::decode(version, bl);
    ::decode(reported, bl);
    ::decode(state, bl);
    ::decode(log_start, bl);
    ::decode(ondisk_log_start, bl);
    ::decode(created, bl);
    if (v >= 7)
      ::decode(last_epoch_clean, bl);
    else
      last_epoch_clean = 0;
    if (v < 6) {
      old_pg_t opgid;
      ::decode(opgid, bl);
      parent = opgid;
    } else {
      ::decode(parent, bl);
    }
    ::decode(parent_split_bits, bl);
    ::decode(last_scrub, bl);
    ::decode(last_scrub_stamp, bl);
    if (v <= 4) {
      ::decode(stats.sum.num_bytes, bl);
      ::decode(stats.sum.num_kb, bl);
      ::decode(stats.sum.num_objects, bl);
      ::decode(stats.sum.num_object_clones, bl);
      ::decode(stats.sum.num_object_copies, bl);
      ::decode(stats.sum.num_objects_missing_on_primary, bl);
      ::decode(stats.sum.num_objects_degraded, bl);
      ::decode(log_size, bl);
      ::decode(ondisk_log_size, bl);
      if (v >= 2) {
	::decode(stats.sum.num_rd, bl);
	::decode(stats.sum.num_rd_kb, bl);
	::decode(stats.sum.num_wr, bl);
	::decode(stats.sum.num_wr_kb, bl);
      }
      if (v >= 3) {
	::decode(up, bl);
      }
      if (v == 4) {
	::decode(stats.sum.num_objects_unfound, bl);  // sigh.
      }
      ::decode(acting, bl);
    } else {
      ::decode(stats, bl);
      ::decode(log_size, bl);
      ::decode(ondisk_log_size, bl);
      ::decode(up, bl);
      ::decode(acting, bl);
    }
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

  void dump(Formatter *f) const {
    stats.dump(f);
    f->dump_unsigned("log_size", log_size);
    f->dump_unsigned("ondisk_log_size", ondisk_log_size);
  }
  void encode(bufferlist &bl) const {
    __u8 v = 4;
    ::encode(v, bl);
    ::encode(stats, bl);
    ::encode(log_size, bl);
    ::encode(ondisk_log_size, bl);
  }
  void decode(bufferlist::iterator &bl) {
    __u8 v;
    ::decode(v, bl);
    if (v >= 4) {
      ::decode(stats, bl);
      ::decode(log_size, bl);
      ::decode(ondisk_log_size, bl);
    } else {
      ::decode(stats.sum.num_bytes, bl);
      ::decode(stats.sum.num_kb, bl);
      ::decode(stats.sum.num_objects, bl);
      ::decode(stats.sum.num_object_clones, bl);
      ::decode(stats.sum.num_object_copies, bl);
      ::decode(stats.sum.num_objects_missing_on_primary, bl);
      ::decode(stats.sum.num_objects_degraded, bl);
      ::decode(log_size, bl);
      ::decode(ondisk_log_size, bl);
      if (v >= 2) {
	::decode(stats.sum.num_rd, bl);
	::decode(stats.sum.num_rd_kb, bl);
	::decode(stats.sum.num_wr, bl);
	::decode(stats.sum.num_wr_kb, bl);
      }
      if (v >= 3) {
	::decode(stats.sum.num_objects_unfound, bl);
      }
    }
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
};
WRITE_CLASS_ENCODER(pool_stat_t)






struct osd_peer_stat_t {
  struct ceph_timespec stamp;
  float oprate;
  float qlen;            // current
  float recent_qlen;     // moving average
  float read_latency;
  float read_latency_mine;
  float frac_rd_ops_shed_in;
  float frac_rd_ops_shed_out;
} __attribute__ ((packed));

WRITE_RAW_ENCODER(osd_peer_stat_t)

inline ostream& operator<<(ostream& out, const osd_peer_stat_t &stat) {
  return out << "stat(" << stat.stamp
	     << " oprate=" << stat.oprate
    	     << " qlen=" << stat.qlen 
    	     << " recent_qlen=" << stat.recent_qlen
	     << " rdlat=" << stat.read_latency_mine << " / " << stat.read_latency
	     << " fshedin=" << stat.frac_rd_ops_shed_in
	     << ")";
}




// -----------------------------------------

class ObjectExtent {
 public:
  object_t    oid;       // object id
  __u32      offset;    // in object
  __u32      length;    // in object

  object_locator_t oloc;   // object locator (pool etc)

  map<__u32, __u32>  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : offset(0), length(0) {}
  ObjectExtent(object_t o, __u32 off=0, __u32 l=0) : oid(o), offset(off), length(l) { }
};

inline ostream& operator<<(ostream& out, const ObjectExtent &ex)
{
  return out << "extent(" 
             << ex.oid << " in " << ex.oloc
             << " " << ex.offset << "~" << ex.length
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
  vector<snapid_t> snaps;    // ascending
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
};
WRITE_CLASS_ENCODER(SnapSet)

ostream& operator<<(ostream& out, const SnapSet& cs);



#define OI_ATTR "_"
#define SS_ATTR "snapset"

struct watch_info_t {
  uint64_t cookie;
  uint32_t timeout_seconds;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
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


  map<entity_name_t, watch_info_t> watchers;

  void copy_user_bits(const object_info_t& other);

  static ps_t legacy_object_locator_to_ps(const object_t &oid, 
					  const object_locator_t &loc);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }

  object_info_t(const hobject_t& s, const object_locator_t& o)
    : soid(s), oloc(o), size(0),
      lost(false), truncate_seq(0), truncate_size(0) {}

  object_info_t(bufferlist& bl) {
    decode(bl);
  }
};
WRITE_CLASS_ENCODER(object_info_t)


ostream& operator<<(ostream& out, const object_info_t& oi);



/*
 * summarize pg contents for purposes of a scrub
 */
struct ScrubMap {
  struct object {
    uint64_t size;
    bool negative;
    map<string,bufferptr> attrs;

    object(): size(0),negative(0),attrs() {}

    void encode(bufferlist& bl) const {
      __u8 struct_v = 1;
      ::encode(struct_v, bl);
      ::encode(size, bl);
      ::encode(negative, bl);
      ::encode(attrs, bl);
    }
    void decode(bufferlist::iterator& bl) {
      __u8 struct_v;
      ::decode(struct_v, bl);
      ::decode(size, bl);
      ::decode(negative, bl);
      ::decode(attrs, bl);
    }
  };
  WRITE_CLASS_ENCODER(object)

  map<hobject_t,object> objects;
  map<string,bufferptr> attrs;
  bufferlist logbl;
  eversion_t valid_through;
  eversion_t incr_since;

  void merge_incr(const ScrubMap &l);

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);
};
WRITE_CLASS_ENCODER(ScrubMap::object)
WRITE_CLASS_ENCODER(ScrubMap)


struct OSDOp {
  ceph_osd_op op;
  bufferlist data;
  sobject_t soid;

  OSDOp() {
    memset(&op, 0, sizeof(ceph_osd_op));
  }
};

ostream& operator<<(ostream& out, const OSDOp& op);

#endif
