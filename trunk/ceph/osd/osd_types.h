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

/* osdreqid_t - caller name + incarnation# + tid to unique identify this request
 * use for metadata and osd ops.
 */
class osdreqid_t {
public:
  entity_name_t name; // who
  int           inc;  // incarnation
  tid_t         tid;
  osdreqid_t() : inc(0), tid(0) {}
  osdreqid_t(const entity_name_t& a, int i, tid_t t) : name(a), inc(i), tid(t) {}
};

inline ostream& operator<<(ostream& out, const osdreqid_t& r) {
  return out << r.name << "." << r.inc << ":" << r.tid;
}

inline bool operator==(const osdreqid_t& l, const osdreqid_t& r) {
  return (l.name == r.name) && (l.inc == r.inc) && (l.tid == r.tid);
}
inline bool operator!=(const osdreqid_t& l, const osdreqid_t& r) {
  return (l.name != r.name) || (l.inc != r.inc) || (l.tid != r.tid);
}
inline bool operator<(const osdreqid_t& l, const osdreqid_t& r) {
  return (l.name < r.name) || (l.inc < r.inc) || 
    (l.name == r.name && l.inc == r.inc && l.tid < r.tid);
}
inline bool operator<=(const osdreqid_t& l, const osdreqid_t& r) {
  return (l.name < r.name) || (l.inc < r.inc) ||
    (l.name == r.name && l.inc == r.inc && l.tid <= r.tid);
}
inline bool operator>(const osdreqid_t& l, const osdreqid_t& r) { return !(l <= r); }
inline bool operator>=(const osdreqid_t& l, const osdreqid_t& r) { return !(l < r); }

namespace __gnu_cxx {
  template<> struct hash<osdreqid_t> {
    size_t operator()(const osdreqid_t &r) const { 
      static blobhash H;
      return H((const char*)&r, sizeof(r));
    }
  };
}



// osd types
typedef uint64_t coll_t;        // collection id

// pg stuff

#define PG_INO 1

typedef uint16_t ps_t;
typedef uint8_t pruleset_t;


// crush rule ids
#define CRUSH_REP_RULE(nrep) (100+nrep)  // replication
#define CRUSH_RAID_RULE(num) (200+num)   // raid



// placement group id
struct pg_t {
public:
  static const int TYPE_REP   = 1;
  static const int TYPE_RAID4 = 2;

private:
  union {
    struct {
      int32_t preferred;
      uint8_t type;
      uint8_t size;
      uint16_t ps;
    } fields;
    uint64_t val;          // 64
  } u;

public:
  pg_t() { u.val = 0; }
  pg_t(const pg_t& o) { u.val = o.u.val; }
  pg_t(int type, int size, ps_t seed, int pref) {//, pruleset_t r=0) {
    u.fields.type = type;
    u.fields.size = size;
    u.fields.ps = seed;
    u.fields.preferred = pref;   // hack: avoid negative.
    //u.fields.ruleset = r;
    assert(sizeof(u.fields) == sizeof(u.val));
  }
  pg_t(uint64_t v) { u.val = v; }

  int type()      { return u.fields.type; }
  bool is_rep()   { return type() == TYPE_REP; }
  bool is_raid4() { return type() == TYPE_RAID4; }

  int size() { return u.fields.size; }
  ps_t ps() { return u.fields.ps; }
  //pruleset_t ruleset() { return u.fields.ruleset; }
  int preferred() { return u.fields.preferred; }   // hack: avoid negative.
  
  /*
  pg_t operator=(uint64_t v) { u.val = v; return *this; }
  pg_t operator&=(uint64_t v) { u.val &= v; return *this; }
  pg_t operator+=(pg_t o) { u.val += o.val; return *this; }
  pg_t operator-=(pg_t o) { u.val -= o.val; return *this; }
  pg_t operator++() { ++u.val; return *this; }
  */
  operator uint64_t() const { return u.val; }

  object_t to_object() const { return object_t(PG_INO, u.val >> 32, u.val & 0xffffffff); }
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
  
  if (pg.preferred() >= 0)
    out << pg.preferred() << 'p';
  out << hex << pg.ps() << dec;

  //out << "=" << hex << (__uint64_t)pg << dec;
  return out;
}

namespace __gnu_cxx {
  template<> struct hash< pg_t >
  {
    size_t operator()( const pg_t& x ) const
    {
      static hash<uint64_t> H;
      return H(x);
    }
  };
}


/** ObjectLayout
 *
 * describes an object's placement and layout in the storage cluster.  
 * most importatly, which pg it belongs to.
 * if that pg is raided, it also specifies the object's stripe_unit.
 */
struct ObjectLayout {
  pg_t pgid;            // what pg do i belong to
  int  stripe_unit;     // for object raid in raid pgs

  ObjectLayout() : pgid(0), stripe_unit(0) { }
  ObjectLayout(pg_t p, int su=0) : pgid(p), stripe_unit(su) { }
};

inline ostream& operator<<(ostream& out, const ObjectLayout &ol)
{
  out << "pg" << ol.pgid;
  if (ol.stripe_unit)
    out << ".su=" << ol.stripe_unit;
  return out;
}



// compound rados version type
class eversion_t {
public:
  epoch_t epoch;
  version_t version;
  eversion_t(epoch_t e=0, version_t v=0) : epoch(e), version(v) {}
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





// -----------------------------------------

class ObjectExtent {
 public:
  object_t    oid;       // object id
  off_t       start;     // in object
  size_t      length;    // in object

  objectrev_t rev;       // which revision?

  ObjectLayout layout;   // object layout (pgid, etc.)

  map<size_t, size_t>  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)
  
  ObjectExtent() : start(0), length(0), rev(0) {}
  ObjectExtent(object_t o, off_t s=0, size_t l=0) : oid(o), start(s), length(l), rev(0) { }
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
  uint64_t fsid;      // unique fs id (random number)
  int        whoami;    // my role in this fs.
  epoch_t    current_epoch;             // most recent epoch
  epoch_t    oldest_map, newest_map;    // oldest/newest maps we have.
  OSDSuperblock(uint64_t f=0, int w=0) : 
    magic(MAGIC), fsid(f), whoami(w), 
    current_epoch(0), oldest_map(0), newest_map(0) {}
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
