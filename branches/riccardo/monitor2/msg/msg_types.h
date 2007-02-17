// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include "tcp.h"

// new typed msg_addr_t way!
class entity_name_t {
  int _type;
  int _num;

public:
  static const int TYPE_MON = 1;
  static const int TYPE_MDS = 2;
  static const int TYPE_OSD = 3;
  static const int TYPE_CLIENT = 4;

  static const int NEW = -1;

  // cons
  entity_name_t() : _type(0), _num(0) {}
  entity_name_t(int t, int n) : _type(t), _num(n) {}
  
  int num() const { return _num; }
  int type() const { return _type; }
  const char *type_str() const {
    switch (type()) {
    case TYPE_MDS: return "mds"; 
    case TYPE_OSD: return "osd"; 
    case TYPE_MON: return "mon"; 
    case TYPE_CLIENT: return "client"; 
    default: return "unknown";
    }    
  }

  bool is_new() const { return num() == NEW; }

  bool is_client() const { return type() == TYPE_CLIENT; }
  bool is_mds() const { return type() == TYPE_MDS; }
  bool is_osd() const { return type() == TYPE_OSD; }
  bool is_mon() const { return type() == TYPE_MON; }
};

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

namespace __gnu_cxx {
  template<> struct hash< entity_name_t >
  {
    size_t operator()( const entity_name_t m ) const
    {
      static blobhash H;
      return H((const char*)&m, sizeof(m));
    }
  };
}

// get rid of these
#define MSG_ADDR_MDS(x)     entity_name_t(entity_name_t::TYPE_MDS,x)
#define MSG_ADDR_OSD(x)     entity_name_t(entity_name_t::TYPE_OSD,x)
#define MSG_ADDR_MON(x)     entity_name_t(entity_name_t::TYPE_MON,x)
#define MSG_ADDR_CLIENT(x)  entity_name_t(entity_name_t::TYPE_CLIENT,x)

#define MSG_ADDR_RANK_NEW    MSG_ADDR_RANK(entity_name_t::NEW)
#define MSG_ADDR_MDS_NEW     MSG_ADDR_MDS(entity_name_t::NEW)
#define MSG_ADDR_OSD_NEW     MSG_ADDR_OSD(entity_name_t::NEW)
#define MSG_ADDR_CLIENT_NEW  MSG_ADDR_CLIENT(entity_name_t::NEW)


/*
 * an entity's network address.
 * includes a random value that prevents it from being reused.
 * thus identifies a particular process instance.
 * ipv4 for now.
 */
struct entity_addr_t {
  __uint8_t  ipq[4];
  __uint32_t port;
  __uint32_t nonce;  // bind time, or pid, or something unique!

  entity_addr_t() : port(0), nonce(0) {
    ipq[0] = ipq[1] = ipq[2] = ipq[3] = 0;
  }

  void set_addr(tcpaddr_t a) {
    memcpy((char*)ipq, (char*)&a.sin_addr.s_addr, 4);
    port = ntohs(a.sin_port);
  }
  void make_addr(tcpaddr_t& a) const {
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    memcpy((char*)&a.sin_addr.s_addr, (char*)ipq, 4);
    a.sin_port = htons(port);
  }
};

inline ostream& operator<<(ostream& out, const entity_addr_t &addr)
{
  return out << (int)addr.ipq[0]
	     << '.' << (int)addr.ipq[1]
	     << '.' << (int)addr.ipq[2]
	     << '.' << (int)addr.ipq[3]
	     << ':' << addr.port
	     << '.' << addr.nonce;
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
      return H((const char*)&x, sizeof(x));
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
};


inline bool operator==(const entity_inst_t& a, const entity_inst_t& b) { return memcmp(&a, &b, sizeof(a)) == 0; }
inline bool operator!=(const entity_inst_t& a, const entity_inst_t& b) { return memcmp(&a, &b, sizeof(a)) != 0; }
inline bool operator<(const entity_inst_t& a, const entity_inst_t& b) { return memcmp(&a, &b, sizeof(a)) < 0; }
inline bool operator<=(const entity_inst_t& a, const entity_inst_t& b) { return memcmp(&a, &b, sizeof(a)) <= 0; }
inline bool operator>(const entity_inst_t& a, const entity_inst_t& b) { return memcmp(&a, &b, sizeof(a)) > 0; }
inline bool operator>=(const entity_inst_t& a, const entity_inst_t& b) { return memcmp(&a, &b, sizeof(a)) >= 0; }

namespace __gnu_cxx {
  template<> struct hash< entity_inst_t >
  {
    size_t operator()( const entity_inst_t& x ) const
    {
      static blobhash H;
      return H((const char*)&x, sizeof(x));
    }
  };
}

inline ostream& operator<<(ostream& out, const entity_inst_t &i)
{
  return out << i.name << " " << i.addr;
}


#endif
