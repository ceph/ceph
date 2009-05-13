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

#ifndef __OSD_TYPES_H
#define __OSD_TYPES_H

#include <stdio.h>

#include "msg/msg_types.h"
#include "include/types.h"
#include "include/pobject.h"
#include "include/interval_set.h"
#include "include/nstring.h"



#define CEPH_OSD_ONDISK_MAGIC "ceph osd volume v014"





/* osdreqid_t - caller name + incarnation# + tid to unique identify this request
 * use for metadata and osd ops.
 */
struct osd_reqid_t {
  entity_name_t name; // who
  tid_t         tid;
  int32_t       inc;  // incarnation
  osd_reqid_t() : tid(0), inc(0) {}
  osd_reqid_t(const entity_name_t& a, int i, tid_t t) : name(a), tid(t), inc(i) {}
  void encode(bufferlist &bl) const {
    ::encode(name, bl);
    ::encode(tid, bl);
    ::encode(inc, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(name, bl);
    ::decode(tid, bl);
    ::decode(inc, bl);
  }
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
      static hash<__u64> H;
      return H(r.name.num() ^ r.tid ^ r.inc);
    }
  };
}




// pg stuff

typedef uint16_t ps_t;

// object namespaces
#define CEPH_METADATA_NS       1
#define CEPH_DATA_NS           2
#define CEPH_CAS_NS            3
#define CEPH_OSDMETADATA_NS 0xff

// poolsets
enum {
  CEPH_DATA_RULE,
  CEPH_METADATA_RULE,
  CEPH_CASDATA_RULE,
};

//#define CEPH_POOL(poolset, size) (((poolset) << 8) + (size))

#define OSD_SUPERBLOCK_POBJECT pobject_t(CEPH_OSDMETADATA_NS, 0, object_t(0,0))

// placement group id
struct pg_t {
  union ceph_pg u;

  pg_t() { u.pg64 = 0; }
  pg_t(const pg_t& o) { u.pg64 = o.u.pg64; }
  pg_t(ps_t seed, int pool, int pref) {
    u.pg64 = 0;
    u.pg.ps = seed;
    u.pg.pool = pool;
    u.pg.preferred = pref;   // hack: avoid negative.
    assert(sizeof(u.pg) == sizeof(u.pg64));
  }
  pg_t(uint64_t v) { u.pg64 = v; }
  pg_t(const ceph_pg& cpg) {
    u = cpg;
  }

  ps_t ps() { return u.pg.ps; }
  int pool() { return u.pg.pool; }
  int preferred() { return u.pg.preferred; }   // hack: avoid negative.
  
  operator uint64_t() const { return u.pg64; }

  pobject_t to_log_pobject() const { 
    return pobject_t(CEPH_OSDMETADATA_NS,
		     0,
		     object_t(u.pg64, 0));
  }

  coll_t to_coll() const {
    return coll_t(u.pg64, 0); 
  }
  coll_t to_snap_coll(snapid_t sn) const {
    return coll_t(u.pg64, sn);
  }

  bool parse(const char *s) {
    int pool;
    int ps;
    int r = sscanf(s, "%d.%x", &pool, &ps);
    if (r < 3)
      return false;
    u.pg.pool = pool;
    u.pg.ps = ps;
    u.pg.preferred = -1;
    return true;
  }

} __attribute__ ((packed));

inline void encode(pg_t pgid, bufferlist& bl) { encode_raw(pgid.u.pg64, bl); }
inline void decode(pg_t &pgid, bufferlist::iterator& p) { 
  __u64 v;
  decode_raw(v, p); 
  pgid.u.pg64 = v;
}


inline ostream& operator<<(ostream& out, pg_t pg) 
{
  out << pg.pool() << '.';
  out << hex << pg.ps() << dec;

  if (pg.preferred() >= 0)
    out << 'p' << pg.preferred();

  //out << "=" << hex << (__uint64_t)pg << dec;
  return out;
}

namespace __gnu_cxx {
  template<> struct hash< pg_t >
  {
    size_t operator()( const pg_t& x ) const
    {
      static rjhash<uint64_t> H;
      return H(x);
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

  void encode(bufferlist &bl) const {
    ::encode(kb, bl);
    ::encode(kb_used, bl);
    ::encode(kb_avail, bl);
    ::encode(snap_trim_queue_len, bl);
    ::encode(num_snap_trimming, bl);
    ::encode(hb_in, bl);
    ::encode(hb_out, bl);
  }
  void decode(bufferlist::iterator &bl) {
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
  return out << "osd_stat(" << (s.kb_used) << "/" << s.kb << " KB used, " 
	     << s.kb_avail << " avail, "
	     << "peers " << s.hb_in << "/" << s.hb_out << ")";
}


/*
 * pg states
 */
#define PG_STATE_CREATING     (1<<0)  // creating
#define PG_STATE_ACTIVE       (1<<1)  // i am active.  (primary: replicas too)
#define PG_STATE_CLEAN        (1<<2)  // peers are complete, clean of stray replicas.
#define PG_STATE_CRASHED      (1<<3)  // all replicas went down, clients needs to replay
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

static inline std::string pg_state_string(int state) {
  std::string st;
  if (state & PG_STATE_CREATING) st += "creating+";
  if (state & PG_STATE_ACTIVE) st += "active+";
  if (state & PG_STATE_CLEAN) st += "clean+";
  if (state & PG_STATE_CRASHED) st += "crashed+";
  if (state & PG_STATE_DOWN) st += "down+";
  if (state & PG_STATE_REPLAY) st += "replay+";
  if (state & PG_STATE_STRAY) st += "stray+";
  if (state & PG_STATE_SPLITTING) st += "splitting+";
  if (state & PG_STATE_DEGRADED) st += "degraded+";
  if (state & PG_STATE_SCRUBBING) st += "scrubbing+";
  if (state & PG_STATE_SCRUBQ) st += "scrubq+";
  if (state & PG_STATE_INCONSISTENT) st += "inconsistent+";
  if (state & PG_STATE_PEERING) st += "peering+";
  if (state & PG_STATE_REPAIR) st += "repair+";
  if (state & PG_STATE_SCANNING) st += "scanning+";
  if (!st.length()) 
    st = "inactive";
  else 
    st.resize(st.length()-1);
  return st;
}




/*
 * pg_pool
 */
struct pg_pool_t {
  ceph_pg_pool v;
  int pg_num_mask, pgp_num_mask, lpg_num_mask, lpgp_num_mask;

  int get_pg_num() const { return v.pg_num; }
  int get_pgp_num() const { return v.pgp_num; }
  int get_lpg_num() const { return v.lpg_num; }
  int get_lpgp_num() const { return v.lpgp_num; }

  int get_pg_num_mask() const { return pg_num_mask; }
  int get_pgp_num_mask() const { return pgp_num_mask; }
  int get_lpg_num_mask() const { return lpg_num_mask; }
  int get_lpgp_num_mask() const { return lpgp_num_mask; }

  int calc_bits_of(int t) {
    int b = 0;
    while (t > 0) {
      t = t >> 1;
      b++;
    }
    return b;
  }

  unsigned get_type() const { return v.type; }
  unsigned get_size() const { return v.size; }
  int get_crush_ruleset() const { return v.crush_ruleset; }
  epoch_t get_last_change() const { return v.last_change; }

  bool is_rep()   const { return get_type() == CEPH_PG_TYPE_REP; }
  bool is_raid4() const { return get_type() == CEPH_PG_TYPE_RAID4; }

  void calc_pg_masks() {
    pg_num_mask = (1 << calc_bits_of(v.pg_num-1)) - 1;
    pgp_num_mask = (1 << calc_bits_of(v.pgp_num-1)) - 1;
    lpg_num_mask = (1 << calc_bits_of(v.lpg_num-1)) - 1;
    lpgp_num_mask = (1 << calc_bits_of(v.lpgp_num-1)) - 1;
  }

  /*
   * map a raw pg (with full precision ps) into an actual pg, for storage
   */
  pg_t raw_pg_to_pg(pg_t pg) const {
    if (pg.preferred() >= 0 && v.lpg_num)
      pg.u.pg.ps = ceph_stable_mod(pg.ps(), v.lpg_num, lpg_num_mask);
    else
      pg.u.pg.ps = ceph_stable_mod(pg.ps(), v.pg_num, pg_num_mask);
    return pg;
  }
  
  /*
   * map raw pg (full precision ps) into a placement ps
   */
  ps_t raw_pg_to_pps(pg_t pg) const {
    if (pg.preferred() >= 0 && v.lpgp_num)
      return ceph_stable_mod(pg.ps(), v.lpgp_num, lpgp_num_mask);
    else
      return ceph_stable_mod(pg.ps(), v.pgp_num, pgp_num_mask);
  }

  void encode(bufferlist& bl) const {
    ::encode(v, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(v, bl);
    calc_pg_masks();
  }
};
WRITE_CLASS_ENCODER(pg_pool_t)

inline ostream& operator<<(ostream& out, const pg_pool_t& p) {
  out << "pg_pool(";
  switch (p.get_type()) {
  case CEPH_PG_TYPE_REP: out << "rep"; break;
  default: out << "type " << p.get_type();
  }
  out << " size " << p.get_size()
      << " ruleset " << p.get_crush_ruleset()
      << " pg_num " << p.get_pg_num()
      << " pgp_num " << p.get_pgp_num()
      << " lpg_num " << p.get_lpg_num()
      << " lpgp_num " << p.get_lpgp_num()
      << " last_change " << p.get_last_change()
      << ")";
  return out;
}

/** pg_stat
 * aggregate stats for a single PG.
 */
struct pg_stat_t {
  eversion_t version;
  eversion_t reported;
  __u32 state;

  epoch_t created;
  pg_t parent;
  __u32 parent_split_bits;

  eversion_t last_scrub;
  utime_t last_scrub_stamp;

  __u64 num_bytes;    // in bytes
  __u64 num_kb;       // in KB
  __u64 num_objects;
  __u64 num_object_clones;
  __u64 num_object_copies;  // num_objects * num_replicas
  __u64 num_objects_missing_on_primary;
  __u64 num_objects_degraded;

  vector<int> acting;

  pg_stat_t() : state(0),
		created(0), parent_split_bits(0), 
		num_bytes(0), num_kb(0), 
		num_objects(0), num_object_clones(0), num_object_copies(0),
		num_objects_missing_on_primary(0), num_objects_degraded(0)
  { }

  void encode(bufferlist &bl) const {
    ::encode(version, bl);
    ::encode(reported, bl);
    ::encode(state, bl);
    ::encode(created, bl);
    ::encode(parent, bl);
    ::encode(parent_split_bits, bl);
    ::encode(last_scrub, bl);
    ::encode(last_scrub_stamp, bl);
    ::encode(num_bytes, bl);
    ::encode(num_kb, bl);
    ::encode(num_objects, bl);
    ::encode(num_object_clones, bl);
    ::encode(num_object_copies, bl);
    ::encode(num_objects_missing_on_primary, bl);
    ::encode(num_objects_degraded, bl);
    ::encode(acting, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(version, bl);
    ::decode(reported, bl);
    ::decode(state, bl);
    ::decode(created, bl);
    ::decode(parent, bl);
    ::decode(parent_split_bits, bl);
    ::decode(last_scrub, bl);
    ::decode(last_scrub_stamp, bl);
    ::decode(num_bytes, bl);
    ::decode(num_kb, bl);
    ::decode(num_objects, bl);
    ::decode(num_object_clones, bl);
    ::decode(num_object_copies, bl);
    ::decode(num_objects_missing_on_primary, bl);
    ::decode(num_objects_degraded, bl);
    ::decode(acting, bl);
  }

  void add(const pg_stat_t& o) {
    num_bytes += o.num_bytes;
    num_kb += o.num_kb;
    num_objects += o.num_objects;
    num_object_clones += o.num_object_clones;
    num_object_copies += o.num_object_copies;
    num_objects_missing_on_primary += o.num_objects_missing_on_primary;
    num_objects_degraded += o.num_objects_degraded;
  }
  void sub(const pg_stat_t& o) {
    num_bytes -= o.num_bytes;
    num_kb -= o.num_kb;
    num_objects -= o.num_objects;
    num_object_clones -= o.num_object_clones;
    num_object_copies -= o.num_object_copies;
    num_objects_missing_on_primary -= o.num_objects_missing_on_primary;
    num_objects_degraded -= o.num_objects_degraded;
  }
};
WRITE_CLASS_ENCODER(pg_stat_t)

struct osd_peer_stat_t {
	struct ceph_timespec stamp;
	float oprate;
	float qlen;
	float recent_qlen;
	float read_latency;
	float read_latency_mine;
	float frac_rd_ops_shed_in;
	float frac_rd_ops_shed_out;
} __attribute__ ((packed));

WRITE_RAW_ENCODER(osd_peer_stat_t)

inline ostream& operator<<(ostream& out, const osd_peer_stat_t &stat) {
  return out << "stat(" << stat.stamp
    //<< " oprate=" << stat.oprate
    //	     << " qlen=" << stat.qlen 
    //	     << " recent_qlen=" << stat.recent_qlen
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

  ceph_object_layout layout;   // object layout (pgid, etc.)

  map<__u32, __u32>  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : offset(0), length(0) {}
  ObjectExtent(object_t o, __u32 off=0, __u32 l=0) : oid(o), offset(off), length(l) { }
};

inline ostream& operator<<(ostream& out, ObjectExtent &ex)
{
  return out << "extent(" 
             << ex.oid << " in " << ex.layout
             << " " << ex.offset << "~" << ex.length
             << ")";
}






// ---------------------------------------

class OSDSuperblock {
public:
  nstring magic;
  ceph_fsid_t fsid;
  int32_t whoami;    // my role in this fs.
  epoch_t current_epoch;             // most recent epoch
  epoch_t oldest_map, newest_map;    // oldest/newest maps we have.
  double weight;

  // last interval over which i mounted and was then active
  epoch_t mounted;     // last epoch i mounted
  epoch_t clean_thru;  // epoch i was active and clean thru

  OSDSuperblock() : 
    whoami(-1), 
    current_epoch(0), oldest_map(0), newest_map(0), weight(0),
    mounted(0), clean_thru(0) {
    memset(&fsid, 0, sizeof(fsid));
  }

  void encode(bufferlist &bl) const {
    ::encode(magic, bl);
    ::encode(fsid, bl);
    ::encode(whoami, bl);
    ::encode(current_epoch, bl);
    ::encode(oldest_map, bl);
    ::encode(newest_map, bl);
    ::encode(weight, bl);
    ::encode(clean_thru, bl);
    ::encode(mounted, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(magic, bl);
    ::decode(fsid, bl);
    ::decode(whoami, bl);
    ::decode(current_epoch, bl);
    ::decode(oldest_map, bl);
    ::decode(newest_map, bl);
    ::decode(weight, bl);
    ::decode(clean_thru, bl);
    ::decode(mounted, bl);
  }
};
WRITE_CLASS_ENCODER(OSDSuperblock)

inline ostream& operator<<(ostream& out, OSDSuperblock& sb)
{
  return out << "sb('" << sb.magic << "' fsid " << sb.fsid
             << " osd" << sb.whoami
             << " e" << sb.current_epoch
             << " [" << sb.oldest_map << "," << sb.newest_map << "]"
	     << " lci=[" << sb.mounted << "," << sb.clean_thru << "]"
             << ")";
}


// -------

WRITE_CLASS_ENCODER(interval_set<__u64>)





/*
 * attached to object head.  describes most recent snap context, and
 * set of existing clones.
 */
struct SnapSet {
  snapid_t seq;
  bool head_exists;
  vector<snapid_t> snaps;    // ascending
  vector<snapid_t> clones;   // ascending
  map<snapid_t, interval_set<__u64> > clone_overlap;  // overlap w/ next newest
  map<snapid_t, __u64> clone_size;

  SnapSet() : head_exists(false) {}

  void encode(bufferlist& bl) const {
    ::encode(seq, bl);
    ::encode(head_exists, bl);
    ::encode(snaps, bl);
    ::encode(clones, bl);
    ::encode(clone_overlap, bl);
    ::encode(clone_size, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(seq, bl);
    ::decode(head_exists, bl);
    ::decode(snaps, bl);
    ::decode(clones, bl);
    ::decode(clone_overlap, bl);
    ::decode(clone_size, bl);
  }
};
WRITE_CLASS_ENCODER(SnapSet)

inline ostream& operator<<(ostream& out, const SnapSet& cs) {
  return out << cs.seq << "=" << cs.snaps << ":"
	     << cs.clones
	     << (cs.head_exists ? "+head":"");
}



#define OI_ATTR "_"

struct object_info_t {
  pobject_t poid;

  eversion_t version, prior_version;
  osd_reqid_t last_reqid;
  utime_t mtime;

  osd_reqid_t wrlock_by;   // [head]
  SnapSet snapset;         // [head]
  vector<snapid_t> snaps;  // [clone]

  bufferlist truncate_info;  // bah.. messy layering.

  void encode(bufferlist& bl) const {
    ::encode(poid, bl);
    ::encode(version, bl);
    ::encode(prior_version, bl);
    ::encode(last_reqid, bl);
    ::encode(mtime, bl);
    if (poid.oid.snap == CEPH_NOSNAP) {
      ::encode(snapset, bl);
      ::encode(wrlock_by, bl);
    } else
      ::encode(snaps, bl);
    ::encode(truncate_info, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(poid, bl);
    ::decode(version, bl);
    ::decode(prior_version, bl);
    ::decode(last_reqid, bl);
    ::decode(mtime, bl);
    if (poid.oid.snap == CEPH_NOSNAP) {
      ::decode(snapset, bl);
      ::decode(wrlock_by, bl);
    } else
      ::decode(snaps, bl);
    ::decode(truncate_info, bl);
  }
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }

  object_info_t(pobject_t p) : poid(p) {}
  object_info_t(bufferlist& bl) {
    decode(bl);
  }
};
WRITE_CLASS_ENCODER(object_info_t)


inline ostream& operator<<(ostream& out, const object_info_t& oi) {
  out << oi.poid << "(" << oi.version
      << " " << oi.last_reqid;
  if (oi.wrlock_by.tid)
    out << " wrlock_by=" << oi.wrlock_by;
  if (oi.poid.oid.snap == CEPH_NOSNAP)
    out << " " << oi.snapset << ")";
  else
    out << " " << oi.snaps << ")";
  return out;
}



/*
 * summarize pg contents for purposes of a scrub
 */
struct ScrubMap {
  struct object {
    pobject_t poid;
    __u64 size;
    map<nstring,bufferptr> attrs;

    void encode(bufferlist& bl) const {
      ::encode(poid, bl);
      ::encode(size, bl);
      ::encode(attrs, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(poid, bl);
      ::decode(size, bl);
      ::decode(attrs, bl);
    }
  };
  WRITE_CLASS_ENCODER(object)

  vector<object> objects;
  map<nstring,bufferptr> attrs;
  bufferlist logbl;

  void encode(bufferlist& bl) const {
    ::encode(objects, bl);
    ::encode(attrs, bl);
    ::encode(logbl, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(objects, bl);
    ::decode(attrs, bl);
    ::decode(logbl, bl);
  }
};
WRITE_CLASS_ENCODER(ScrubMap::object)
WRITE_CLASS_ENCODER(ScrubMap)

#endif
