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

#ifndef __CEPH_TYPES_H
#define __CEPH_TYPES_H

// this is needed for ceph_fs to compile in userland
#include <netinet/in.h>
#define _LINUX_TYPES_H /* we don't want linux/types.h's __u32, __le32, etc. */
#include "inttypes.h"
#include "byteorder.h"
#include <fcntl.h>
#include <string.h>

#include "ceph_fs.h"

#define _BACKWARD_BACKWARD_WARNING_H   /* make gcc 4.3 shut up about hash_*. */

extern "C" {
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "statlite.h"
}

#include <string>
#include <list>
#include <set>
#include <map>
#include <vector>
#include <iostream>
#include <iomanip>

using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;


#include "assert.h"
#include "object.h"
#include "utime.h"
#include "intarith.h"

#include "../acconfig.h"

// DARWIN compatibility
#ifdef DARWIN
typedef long long loff_t;
typedef long long off64_t;
#define O_DIRECT 00040000
#endif

// -- stl crap --

namespace __gnu_cxx {
  template<> struct hash< std::string >
  {
    size_t operator()( const std::string& x ) const
    {
      static hash<const char*> H;
      return H(x.c_str());
    }
  };

#ifndef __LP64__
  template<> struct hash<int64_t> {
    size_t operator()(int64_t __x) const { 
      static hash<int32_t> H;
      return H((__x >> 32) ^ (__x & 0xffffffff)); 
    }
  };
  template<> struct hash<uint64_t> {
    size_t operator()(uint64_t __x) const { 
      static hash<uint32_t> H;
      return H((__x >> 32) ^ (__x & 0xffffffff)); 
    }
  };
#endif

}



// -- io helpers --

template<class A, class B>
inline ostream& operator<<(ostream& out, const pair<A,B> v) {
  return out << v.first << "," << v.second;
}

template<class A>
inline ostream& operator<<(ostream& out, const vector<A>& v) {
  out << "[";
  for (typename vector<A>::const_iterator p = v.begin(); p != v.end(); p++) {
    if (p != v.begin()) out << ",";
    out << *p;
  }
  out << "]";
  return out;
}
template<class A>
inline ostream& operator<<(ostream& out, const deque<A>& v) {
  out << "<";
  for (typename deque<A>::const_iterator p = v.begin(); p != v.end(); p++) {
    if (p != v.begin()) out << ",";
    out << *p;
  }
  out << ">";
  return out;
}

template<class A>
inline ostream& operator<<(ostream& out, const list<A>& ilist) {
  for (typename list<A>::const_iterator it = ilist.begin();
       it != ilist.end();
       it++) {
    if (it != ilist.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A>
inline ostream& operator<<(ostream& out, const set<A>& iset) {
  for (typename set<A>::const_iterator it = iset.begin();
       it != iset.end();
       it++) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A>
inline ostream& operator<<(ostream& out, const multiset<A>& iset) {
  for (typename multiset<A>::const_iterator it = iset.begin();
       it != iset.end();
       it++) {
    if (it != iset.begin()) out << ",";
    out << *it;
  }
  return out;
}

template<class A,class B>
inline ostream& operator<<(ostream& out, const map<A,B>& m) 
{
  out << "{";
  for (typename map<A,B>::const_iterator it = m.begin();
       it != m.end();
       it++) {
    if (it != m.begin()) out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}";
  return out;
}




/*
 * comparators for stl containers
 */
// for hash_map:
//   hash_map<const char*, long, hash<const char*>, eqstr> vals;
struct eqstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) == 0;
  }
};

// for set, map
struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
    return strcmp(s1, s2) < 0;
  }
};



#include "encoding.h"

WRITE_RAW_ENCODER(ceph_fsid_t)
WRITE_RAW_ENCODER(ceph_file_layout)
WRITE_RAW_ENCODER(ceph_pg_pool)
WRITE_RAW_ENCODER(ceph_client_ticket)
WRITE_RAW_ENCODER(ceph_mds_session_head)
WRITE_RAW_ENCODER(ceph_mds_request_head)
WRITE_RAW_ENCODER(ceph_mds_request_release)
WRITE_RAW_ENCODER(ceph_mds_caps)
WRITE_RAW_ENCODER(ceph_mds_cap_release)
WRITE_RAW_ENCODER(ceph_mds_cap_item)
WRITE_RAW_ENCODER(ceph_mds_lease)
WRITE_RAW_ENCODER(ceph_mds_snap_head)
WRITE_RAW_ENCODER(ceph_mds_snap_realm)
WRITE_RAW_ENCODER(ceph_mds_reply_head)
WRITE_RAW_ENCODER(ceph_mds_reply_inode)
WRITE_RAW_ENCODER(ceph_mds_cap_reconnect)
WRITE_RAW_ENCODER(ceph_mds_snaprealm_reconnect)
WRITE_RAW_ENCODER(ceph_frag_tree_split)
WRITE_RAW_ENCODER(ceph_osd_request_head)
WRITE_RAW_ENCODER(ceph_osd_reply_head)
WRITE_RAW_ENCODER(ceph_osd_op)

WRITE_RAW_ENCODER(ceph_mon_statfs)
WRITE_RAW_ENCODER(ceph_mon_statfs_reply)

// ----------------------
// some basic types

// NOTE: these must match ceph_fs.h typedefs
typedef __u64 tid_t;         // transaction id
typedef __u64 version_t;
typedef __u32 epoch_t;       // map epoch  (32bits -> 13 epochs/second for 10 years)


#define O_LAZY 01000000



// --------------------------------------
// ino

typedef __u64 _inodeno_t;

struct inodeno_t {
  _inodeno_t val;
  inodeno_t() : val(0) {}
  inodeno_t(_inodeno_t v) : val(v) {}
  inodeno_t operator+=(inodeno_t o) { val += o.val; return *this; }
  operator _inodeno_t() const { return val; }
};

inline void encode(inodeno_t i, bufferlist &bl) { encode(i.val, bl); }
inline void decode(inodeno_t &i, bufferlist::iterator &p) { decode(i.val, p); }

inline ostream& operator<<(ostream& out, inodeno_t ino) {
  return out << hex << ino.val << dec;
}

namespace __gnu_cxx {
  template<> struct hash< inodeno_t >
  {
    size_t operator()( const inodeno_t& x ) const
    {
      static rjhash<uint64_t> H;
      return H(x.val);
    }
  };
}


// file modes

static inline bool file_mode_is_readonly(int mode) {
  return (mode & CEPH_FILE_MODE_WR) == 0;
}

inline int DT_TO_MODE(int dt) {
  return dt << 12;
}

inline unsigned char MODE_TO_DT(int mode) {
  return mode >> 12;
}



struct SnapRealmInfo {
  mutable ceph_mds_snap_realm h;
  vector<snapid_t> my_snaps;
  vector<snapid_t> prior_parent_snaps;  // before parent_since

  SnapRealmInfo() {
    memset(&h, 0, sizeof(h));
  }
  SnapRealmInfo(inodeno_t ino, snapid_t created, snapid_t seq, snapid_t current_parent_since) {
    memset(&h, 0, sizeof(h));
    h.ino = ino;
    h.created = created;
    h.seq = seq;
    h.parent_since = current_parent_since;
  }
  
  inodeno_t ino() { return inodeno_t(h.ino); }
  inodeno_t parent() { return inodeno_t(h.parent); }
  snapid_t seq() { return snapid_t(h.seq); }
  snapid_t parent_since() { return snapid_t(h.parent_since); }
  snapid_t created() { return snapid_t(h.created); }

  void encode(bufferlist& bl) const {
    h.num_snaps = my_snaps.size();
    h.num_prior_parent_snaps = prior_parent_snaps.size();
    ::encode(h, bl);
    ::encode_nohead(my_snaps, bl);
    ::encode_nohead(prior_parent_snaps, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(h, bl);
    ::decode_nohead(h.num_snaps, my_snaps, bl);
    ::decode_nohead(h.num_prior_parent_snaps, prior_parent_snaps, bl);
  }
};
WRITE_CLASS_ENCODER(SnapRealmInfo)


struct SnapContext {
  snapid_t seq;            // 'time' stamp
  vector<snapid_t> snaps;  // existent snaps, in descending order

  SnapContext() {}
  SnapContext(snapid_t s, vector<snapid_t>& v) : seq(s), snaps(v) {}    

  void clear() {
    seq = 0;
    snaps.clear();
  }
  bool empty() { return seq == 0; }

  void encode(bufferlist& bl) const {
    ::encode(seq, bl);
    ::encode(snaps, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(seq, bl);
    ::decode(snaps, bl);
  }
};
WRITE_CLASS_ENCODER(SnapContext)

inline ostream& operator<<(ostream& out, const SnapContext& snapc) {
  return out << snapc.seq << "=" << snapc.snaps;
}

// dentries
#define MAX_DENTRY_LEN 255


// --

inline ostream& operator<<(ostream& out, const ceph_fsid_t& f) {
  char b[37];
  sprintf(b, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
	  f.fsid[0], f.fsid[1], f.fsid[2], f.fsid[3], f.fsid[4], f.fsid[5], f.fsid[6], f.fsid[7],
	  f.fsid[8], f.fsid[9], f.fsid[10], f.fsid[11], f.fsid[12], f.fsid[13], f.fsid[14], f.fsid[15]);
  return out << b;
}

inline ostream& operator<<(ostream& out, const ceph_osd_op& op) {
  out << ceph_osd_op_name(op.op);
  if (ceph_osd_op_type_data(op.op)) {
    switch (op.op) {
    case CEPH_OSD_OP_DELETE:
      break;
    case CEPH_OSD_OP_TRUNCATE:
      out << " " << op.offset;
      break;
    case CEPH_OSD_OP_SETTRUNC:
    case CEPH_OSD_OP_MASKTRUNC:
    case CEPH_OSD_OP_TRIMTRUNC:
      out << " " << op.truncate_seq << "@" << op.truncate_size;      
      break;
    default:
      out << " " << op.offset << "~" << op.length;
    }
  } else if (ceph_osd_op_type_attr(op.op))
    out << " " << op.name_len << "+" << op.value_len;
  return out;
}





struct kb_t {
  uint64_t v;
  kb_t(uint64_t _v) : v(_v) {}
};

inline ostream& operator<<(ostream& out, const kb_t& kb)
{
  __u64 bump_after = 100;
  if (kb.v > bump_after << 40)
    return out << (kb.v >> 40) << " PB";    
  if (kb.v > bump_after << 30)
    return out << (kb.v >> 30) << " TB";    
  if (kb.v > bump_after << 20)
    return out << (kb.v >> 20) << " GB";    
  if (kb.v > bump_after << 10)
    return out << (kb.v >> 10) << " MB";
  return out << kb.v << " KB";
}



#endif
