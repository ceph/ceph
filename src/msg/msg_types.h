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

#ifndef __MSG_TYPES_H
#define __MSG_TYPES_H

#include "include/types.h"
#include "include/blobhash.h"
#include "tcp.h"
#include "include/encoding.h"

class entity_name_t {
public:
  __u8 _type;
  __s64 _num;

public:
  static const int TYPE_MON = CEPH_ENTITY_TYPE_MON;
  static const int TYPE_MDS = CEPH_ENTITY_TYPE_MDS;
  static const int TYPE_OSD = CEPH_ENTITY_TYPE_OSD;
  static const int TYPE_CLIENT = CEPH_ENTITY_TYPE_CLIENT;
  static const int TYPE_ADMIN = CEPH_ENTITY_TYPE_ADMIN;

  static const int NEW = -1;

  // cons
  entity_name_t() : _type(0), _num(0) { }
  entity_name_t(int t, __s64 n) : _type(t), _num(n) { }
  entity_name_t(const ceph_entity_name &n) : 
    _type(n.type), _num(n.num) { }

  // static cons
  static entity_name_t MON(int i=NEW) { return entity_name_t(TYPE_MON, i); }
  static entity_name_t MDS(int i=NEW) { return entity_name_t(TYPE_MDS, i); }
  static entity_name_t OSD(int i=NEW) { return entity_name_t(TYPE_OSD, i); }
  static entity_name_t CLIENT(int i=NEW) { return entity_name_t(TYPE_CLIENT, i); }
  static entity_name_t ADMIN(int i=NEW) { return entity_name_t(TYPE_ADMIN, i); }
  
  __s64 num() const { return _num; }
  int type() const { return _type; }
  const char *type_str() const {
    switch (type()) {
    case TYPE_MDS: return "mds"; 
    case TYPE_OSD: return "osd"; 
    case TYPE_MON: return "mon"; 
    case TYPE_CLIENT: return "client"; 
    case TYPE_ADMIN: return "admin";
    default: return "unknown";
    }    
  }

  bool is_new() const { return num() < 0; }

  bool is_client() const { return type() == TYPE_CLIENT; }
  bool is_mds() const { return type() == TYPE_MDS; }
  bool is_osd() const { return type() == TYPE_OSD; }
  bool is_mon() const { return type() == TYPE_MON; }
  bool is_admin() const { return type() == TYPE_ADMIN; }

  operator ceph_entity_name() const {
    ceph_entity_name n = { _type, init_le64(_num) };
    return n;
  }

};

inline void encode(const entity_name_t &a, bufferlist& bl) {
  encode(a._type, bl);
  encode(a._num, bl);
}

inline void decode(entity_name_t &a, bufferlist::iterator& p) {
  decode(a._type, p);
  decode(a._num, p);
}

inline bool operator== (const entity_name_t& l, const entity_name_t& r) { 
  return (l.type() == r.type()) && (l.num() == r.num()); }
inline bool operator!= (const entity_name_t& l, const entity_name_t& r) { 
  return (l.type() != r.type()) || (l.num() != r.num()); }
inline bool operator< (const entity_name_t& l, const entity_name_t& r) { 
  return (l.type() < r.type()) || (l.type() == r.type() && l.num() < r.num()); }

inline std::ostream& operator<<(std::ostream& out, const entity_name_t& addr) {
  //if (addr.is_namer()) return out << "namer";
  if (addr.is_new() || addr.num() < 0)
    return out << addr.type_str() << "?";
  else
    return out << addr.type_str() << addr.num();
}
inline std::ostream& operator<<(std::ostream& out, const ceph_entity_name& addr) {
  return out << *(const entity_name_t*)&addr;
}

namespace __gnu_cxx {
  template<> struct hash< entity_name_t >
  {
    size_t operator()( const entity_name_t m ) const
    {
      return rjhash32(m.type() ^ m.num());
    }
  };
}



/*
 * an entity's network address.
 * includes a random value that prevents it from being reused.
 * thus identifies a particular process instance.
 * ipv4 for now.
 */
WRITE_RAW_ENCODER(ceph_entity_addr)
WRITE_RAW_ENCODER(sockaddr_storage)

struct entity_addr_t {
  struct ceph_entity_addr v;

  entity_addr_t() { 
    memset(&v, 0, sizeof(v));
  }
  entity_addr_t(const ceph_entity_addr &o) {
    memcpy(&v, &o, sizeof(v));
  }

  __u32 get_nonce() const { return v.nonce; }
  void set_nonce(__u32 n) { v.nonce = n; }
  
  __u32 get_erank() const { return v.erank; }
  void set_erank(__u32 r) { v.erank = r; }

  sockaddr_storage &in_addr() {
    return *(sockaddr_storage *)&v.in_addr;
  }
  sockaddr_in &in4_addr() {
    return *(sockaddr_in *)&v.in_addr;
  }
  sockaddr_in6 &in6_addr() {
    return *(sockaddr_in6 *)&v.in_addr;
  }

  void set_in4_quad(int pos, int val) {
    sockaddr_in *in4 = (sockaddr_in *)&v.in_addr;
    in4->sin_family = AF_INET;
    unsigned char *ipq = (unsigned char*)&in4->sin_addr.s_addr;
    ipq[pos] = val;
  }
  void set_port(int port) {
    switch (v.in_addr.ss_family) {
    case AF_INET:
      ((sockaddr_in *)&v.in_addr)->sin_port = htons(port);
      break;
    case AF_INET6:
      ((sockaddr_in6 *)&v.in_addr)->sin6_port = htons(port);
      break;
    default:
      assert(0);
    }
  }
  int get_port() const {
    switch (v.in_addr.ss_family) {
    case AF_INET:
      return ntohs(((const sockaddr_in *)&v.in_addr)->sin_port);
      break;
    case AF_INET6:
      return ntohs(((const sockaddr_in6 *)&v.in_addr)->sin6_port);
      break;
    }
    return 0;
  }

  operator ceph_entity_addr() const { return v; }

  bool is_local_to(const entity_addr_t &other) const {
    return ceph_entity_addr_is_local(&v, &other.v);
  }
  bool is_same_host(const entity_addr_t &other) const {
    const ceph_entity_addr *a = &v;
    const ceph_entity_addr *b = &other.v;
    if (a->in_addr.ss_family != b->in_addr.ss_family)
      return false;
    if (a->in_addr.ss_family == AF_INET)
      return ((struct sockaddr_in *)&a->in_addr)->sin_addr.s_addr ==
	((struct sockaddr_in *)&b->in_addr)->sin_addr.s_addr;
    if (a->in_addr.ss_family == AF_INET6)
      return memcmp(((struct sockaddr_in6 *)&a->in_addr)->sin6_addr.s6_addr,
		    ((struct sockaddr_in6 *)&b->in_addr)->sin6_addr.s6_addr,
		    sizeof(((struct sockaddr_in6 *)&a->in_addr)->sin6_addr.s6_addr));
    return false;
  }

  bool is_blank_addr() {
    switch (v.in_addr.ss_family) {
    case AF_INET:
      return ((sockaddr_in *)&v.in_addr)->sin_addr.s_addr == INADDR_ANY;
    case AF_INET6:
      {
	sockaddr_in6 *in6 = (sockaddr_in6 *)&v.in_addr;
	return in6->sin6_addr.s6_addr32[0] == 0 &&
	  in6->sin6_addr.s6_addr32[1] == 0 &&
	  in6->sin6_addr.s6_addr32[2] == 0 &&
	  in6->sin6_addr.s6_addr32[3] == 0;
      }
    default:
      return true;
    }
  }

  void encode(bufferlist& bl) const {
    ::encode(v, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(v, bl);
  }
};
WRITE_CLASS_ENCODER(entity_addr_t)

inline ostream& operator<<(ostream& out, const sockaddr_storage &ss)
{
  switch (ss.ss_family) {
  case AF_INET:
    {
      sockaddr_in *a = (sockaddr_in *)&ss;
      unsigned char *addr = (unsigned char*)&a->sin_addr.s_addr;
      return out << (unsigned)addr[0] << "."
		 << (unsigned)addr[1] << "."
		 << (unsigned)addr[2] << "."
		 << (unsigned)addr[3] << ":"
		 << ntohs(a->sin_port);
    }
    break;

  case AF_INET6:
    {
      sockaddr_in6& in6 = *(sockaddr_in6 *)&ss;
      if (in6.sin6_addr.s6_addr32[0] == 0 &&
	  in6.sin6_addr.s6_addr32[1] == 0 &&
	  in6.sin6_addr.s6_addr16[4] == 0 &&
	  in6.sin6_addr.s6_addr16[5] == 0xffff)
	return out << "::ffff:"
		   << in6.sin6_addr.s6_addr[12] << '.' 
		   << in6.sin6_addr.s6_addr[13] << '.' 
		   << in6.sin6_addr.s6_addr[14] << '.' 
		   << in6.sin6_addr.s6_addr[15]
		   << ':' << ntohs(in6.sin6_port);
      char b[40];
      sprintf(b, "%04x:%04x:%04x:%04x:%04x:%04x:%04x:%04x",
	      in6.sin6_addr.s6_addr16[0],
	      in6.sin6_addr.s6_addr16[1],
	      in6.sin6_addr.s6_addr16[2],
	      in6.sin6_addr.s6_addr16[3],
	      in6.sin6_addr.s6_addr16[4],
	      in6.sin6_addr.s6_addr16[5],
	      in6.sin6_addr.s6_addr16[6],
	      in6.sin6_addr.s6_addr16[7]);
      return out << b << ':' << ntohs(in6.sin6_port);
    }
    break;

  case 0:
    return out << "(null sockaddr_storage)";
  }
  return out << "(unrecognized sockaddr_storage family " << ss.ss_family << ")";  
}

inline ostream& operator<<(ostream& out, const entity_addr_t &addr)
{
  return out << addr.v.in_addr << '/' << addr.v.nonce << '/' << addr.v.erank;
}

inline bool operator==(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) == 0; }
inline bool operator!=(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) != 0; }
inline bool operator<(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) < 0; }
inline bool operator<=(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) <= 0; }
inline bool operator>(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) > 0; }
inline bool operator>=(const entity_addr_t& a, const entity_addr_t& b) { return memcmp(&a, &b, sizeof(a)) >= 0; }

namespace __gnu_cxx {
  template<> struct hash< entity_addr_t >
  {
    size_t operator()( const entity_addr_t& x ) const
    {
      static blobhash H;
      return H((const char*)&x.v, sizeof(x.v));
    }
  };
}


/*
 * a particular entity instance
 */
struct entity_inst_t {
  entity_name_t name;
  entity_addr_t addr;
  entity_inst_t() {}
  entity_inst_t(entity_name_t n, const entity_addr_t& a) : name(n), addr(a) {}
  entity_inst_t(const ceph_entity_inst& i) : name(i.name), addr(i.addr) { }
  entity_inst_t(const ceph_entity_name& n, const ceph_entity_addr &a) : name(n), addr(a) {}
  operator ceph_entity_inst() {
    ceph_entity_inst i = {name, addr};
    return i;
  }
};

inline void encode(const entity_inst_t &i, bufferlist& bl) {
  encode(i.name, bl);
  encode(i.addr, bl);
}
inline void decode(entity_inst_t &i, bufferlist::iterator& p) {
  decode(i.name, p);
  decode(i.addr, p);
}

inline bool operator==(const entity_inst_t& a, const entity_inst_t& b) { 
  return a.name == b.name && a.addr == b.addr;
}
inline bool operator!=(const entity_inst_t& a, const entity_inst_t& b) { 
  return a.name != b.name || a.addr != b.addr;
}
inline bool operator<(const entity_inst_t& a, const entity_inst_t& b) { 
  return a.name < b.name || (a.name == b.name && a.addr < b.addr);
}
inline bool operator<=(const entity_inst_t& a, const entity_inst_t& b) {
  return a.name < b.name || (a.name == b.name && a.addr <= b.addr);
}
inline bool operator>(const entity_inst_t& a, const entity_inst_t& b) { return b < a; }
inline bool operator>=(const entity_inst_t& a, const entity_inst_t& b) { return b <= a; }

namespace __gnu_cxx {
  template<> struct hash< entity_inst_t >
  {
    size_t operator()( const entity_inst_t& x ) const
    {
      static hash< entity_name_t > H;
      static hash< entity_addr_t > I;
      return H(x.name) ^ I(x.addr);
    }
  };
}


inline ostream& operator<<(ostream& out, const entity_inst_t &i)
{
  return out << i.name << " " << i.addr;
}
inline ostream& operator<<(ostream& out, const ceph_entity_inst &i)
{
  return out << *(const entity_inst_t*)&i;
}



#endif
