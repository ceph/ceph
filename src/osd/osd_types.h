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

#include "msg/msg_types.h"
#include "include/types.h"
#include "include/pobject.h"

/* osdreqid_t - caller name + incarnation# + tid to unique identify this request
 * use for metadata and osd ops.
 */
struct osd_reqid_t {
  entity_name_t name; // who
  int32_t       inc;  // incarnation
  tid_t         tid;
  osd_reqid_t() : inc(0), tid(0) {}
  osd_reqid_t(const entity_name_t& a, int i, tid_t t) : name(a), inc(i), tid(t) {}
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

namespace __gnu_cxx {
  template<> struct hash<osd_reqid_t> {
    size_t operator()(const osd_reqid_t &r) const { 
      static blobhash H;
      return H((const char*)&r, sizeof(r));
    }
  };
}



// osd types
typedef uint64_t coll_t;        // collection id

// pg stuff

typedef uint16_t ps_t;
typedef uint8_t pruleset_t;



// placement group id
struct pg_t {
public:
  static const int TYPE_REP   = CEPH_PG_TYPE_REP;
  static const int TYPE_RAID4 = CEPH_PG_TYPE_RAID4;

  //private:
  union ceph_pg u;

public:
  pg_t() { u.pg64 = 0; }
  pg_t(const pg_t& o) { u.pg64 = o.u.pg64; }
  pg_t(int type, int size, ps_t seed, int pref) {//, pruleset_t r=0) {
    u.pg.type = type;
    u.pg.size = size;
    u.pg.ps = seed;
    u.pg.preferred = pref;   // hack: avoid negative.
    //u.pg.ruleset = r;
    assert(sizeof(u.pg) == sizeof(u.pg64));
  }
  pg_t(uint64_t v) { u.pg64 = v; }
  pg_t(const ceph_pg& cpg) {
    u = cpg;
  }

  int type()      { return u.pg.type; }
  bool is_rep()   { return type() == TYPE_REP; }
  bool is_raid4() { return type() == TYPE_RAID4; }

  int size() { return u.pg.size; }
  ps_t ps() { return u.pg.ps; }
  //pruleset_t ruleset() { return u.pg.ruleset; }
  int preferred() { return u.pg.preferred; }   // hack: avoid negative.
  
  /*
  pg_t operator=(uint64_t v) { u.val = v; return *this; }
  pg_t operator&=(uint64_t v) { u.val &= v; return *this; }
  pg_t operator+=(pg_t o) { u.val += o.val; return *this; }
  pg_t operator-=(pg_t o) { u.val -= o.val; return *this; }
  pg_t operator++() { ++u.val; return *this; }
  */
  operator uint64_t() const { return u.pg64; }

  pobject_t to_object() const { 
    return pobject_t(1,  // volume 1 == osd metadata, for now
		     0,
		     object_t(u.pg64, 0));
  }
};

inline ostream& operator<<(ostream& out, pg_t pg) 
{
  if (pg.is_rep()) 
    out << pg.size() << 'x';
  else if (pg.is_raid4()) 
    out << pg.size() << 'r';
  else 
    out << pg.size() << '?';
  
  //if (pg.ruleset())
  //out << (int)pg.ruleset() << 's';
  
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
  out << "pg" << ol.ol_pgid;
  if (ol.ol_stripe_unit)
    out << ".su=" << ol.ol_stripe_unit;
  return out;
}



// compound rados version type
class eversion_t {
public:
  epoch_t epoch;
  version_t version;
  eversion_t() : epoch(0), version(0) {}
  eversion_t(epoch_t e, version_t v) : epoch(e), version(v) {}

  eversion_t(const ceph_eversion& ce) : epoch(ce.epoch), version(ce.version) {}    
  operator ceph_eversion() {
    ceph_eversion c;
    c.epoch = epoch;
    c.version = version;
    return c;
  }
};

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
  int64_t num_blocks;
  int64_t num_blocks_avail;
  int64_t num_objects;

  osd_stat_t() : num_blocks(0), num_blocks_avail(0), num_objects(0) {}
};


/** pg_stat
 * aggregate stats for a single PG.
 */
struct pg_stat_t {
  eversion_t reported;
  
  int32_t state;
  int64_t num_bytes;    // in bytes
  int64_t num_blocks;   // in 4k blocks
  int64_t num_objects;
  
  pg_stat_t() : state(0), num_bytes(0), num_blocks(0), num_objects(0) {}
};

typedef struct ceph_osd_peer_stat osd_peer_stat_t;

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
  off_t       start;     // in object
  size_t      length;    // in object

  ceph_object_layout layout;   // object layout (pgid, etc.)

  map<size_t, size_t>  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : start(0), length(0) {}
  ObjectExtent(object_t o, off_t s=0, size_t l=0) : oid(o), start(s), length(l) { }
};

inline ostream& operator<<(ostream& out, ObjectExtent &ex)
{
  return out << "extent(" 
             << ex.oid << " in " << ex.layout
             << " " << ex.start << "~" << ex.length
             << ")";
}



// ---------------------------------------

class OSDSuperblock {
public:
  const static uint64_t MAGIC = 0xeb0f505dULL;
  uint64_t magic;
  ceph_fsid fsid;
  int32_t whoami;    // my role in this fs.
  epoch_t current_epoch;             // most recent epoch
  epoch_t oldest_map, newest_map;    // oldest/newest maps we have.
  double weight;
  OSDSuperblock(int w=0) : 
    magic(MAGIC), whoami(w), 
    current_epoch(0), oldest_map(0), newest_map(0), weight(0) {
    memset(&fsid, 0, sizeof(fsid));
  }
};

inline ostream& operator<<(ostream& out, OSDSuperblock& sb)
{
  return out << "sb(fsid " << sb.fsid
             << " osd" << sb.whoami
             << " e" << sb.current_epoch
             << " [" << sb.oldest_map << "," << sb.newest_map
             << "])";
}


#endif
