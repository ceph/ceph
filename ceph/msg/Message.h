// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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



#ifndef __MESSAGE_H
#define __MESSAGE_H
 
#define MSG_NS_CONNECT     1
#define MSG_NS_CONNECTACK  2
#define MSG_NS_REGISTER    3
#define MSG_NS_REGISTERACK 4
#define MSG_NS_STARTED     5
#define MSG_NS_UNREGISTER  6
#define MSG_NS_LOOKUP      7
#define MSG_NS_LOOKUPREPLY 8
#define MSG_NS_FAILURE     9


#define MSG_PING        10
#define MSG_PING_ACK    11

#define MSG_FAILURE     12
#define MSG_FAILURE_ACK 13

#define MSG_SHUTDOWN    99999


#define MSG_OSD_OP           20    // delete, etc.
#define MSG_OSD_OPREPLY      21    // delete, etc.
#define MSG_OSD_PING         22

#define MSG_OSD_GETMAP       23
#define MSG_OSD_MAP          24

#define MSG_OSD_BOOT         25
#define MSG_OSD_MKFS_ACK     26

#define MSG_OSD_FAILURE      27

#define MSG_OSD_PG_NOTIFY      50
#define MSG_OSD_PG_QUERY       51
#define MSG_OSD_PG_SUMMARY     52
#define MSG_OSD_PG_LOG         53
#define MSG_OSD_PG_REMOVE      54

#define MSG_CLIENT_REQUEST         60
#define MSG_CLIENT_REPLY           61
//#define MSG_CLIENT_DONE            62
#define MSG_CLIENT_FILECAPS        63
#define MSG_CLIENT_INODEAUTHUPDATE 64

#define MSG_CLIENT_MOUNT           70
#define MSG_CLIENT_MOUNTACK        71
#define MSG_CLIENT_UNMOUNT         72

#define MSG_MDS_HEARTBEAT          100
#define MSG_MDS_DISCOVER           110
#define MSG_MDS_DISCOVERREPLY      111

#define MSG_MDS_INODEGETREPLICA    112
#define MSG_MDS_INODEGETREPLICAACK 113

#define MSG_MDS_INODEFILECAPS      115

#define MSG_MDS_INODEUPDATE  120
#define MSG_MDS_DIRUPDATE    121
#define MSG_MDS_INODEEXPIRE  122
#define MSG_MDS_DIREXPIRE    123

#define MSG_MDS_DIREXPIREREQ 124

#define MSG_MDS_CACHEEXPIRE  125

#define MSG_MDS_ANCHORREQUEST 130
#define MSG_MDS_ANCHORREPLY   131

#define MSG_MDS_INODELINK       140
#define MSG_MDS_INODELINKACK    141
#define MSG_MDS_INODEUNLINK     142
#define MSG_MDS_INODEUNLINKACK  143

#define MSG_MDS_EXPORTDIRDISCOVER      150
#define MSG_MDS_EXPORTDIRDISCOVERACK   151
#define MSG_MDS_EXPORTDIRPREP      152
#define MSG_MDS_EXPORTDIRPREPACK   153
#define MSG_MDS_EXPORTDIRWARNING   154
#define MSG_MDS_EXPORTDIR          155
#define MSG_MDS_EXPORTDIRNOTIFY    156
#define MSG_MDS_EXPORTDIRNOTIFYACK 157
#define MSG_MDS_EXPORTDIRFINISH    158


#define MSG_MDS_HASHDIRDISCOVER    160
#define MSG_MDS_HASHDIRDISCOVERACK 161
#define MSG_MDS_HASHDIRPREP        162
#define MSG_MDS_HASHDIRPREPACK     163
#define MSG_MDS_HASHDIR            164
#define MSG_MDS_HASHDIRACK         165
#define MSG_MDS_HASHDIRNOTIFY      166

#define MSG_MDS_HASHREADDIR        168
#define MSG_MDS_HASHREADDIRREPLY   169

#define MSG_MDS_UNHASHDIRPREP      170
#define MSG_MDS_UNHASHDIRPREPACK   171
#define MSG_MDS_UNHASHDIR          172
#define MSG_MDS_UNHASHDIRACK       173
#define MSG_MDS_UNHASHDIRNOTIFY    174
#define MSG_MDS_UNHASHDIRNOTIFYACK 175

#define MSG_MDS_DENTRYUNLINK      200

#define MSG_MDS_RENAMEWARNING    300   // sent from src to bystanders
#define MSG_MDS_RENAMENOTIFY     301   // sent from dest to bystanders
#define MSG_MDS_RENAMENOTIFYACK  302   // sent back to src
#define MSG_MDS_RENAMEACK        303   // sent from src to initiator, to xlock_finish

#define MSG_MDS_RENAMEPREP       304   // sent from initiator to dest auth (if dir)
#define MSG_MDS_RENAMEREQ        305   // sent from initiator (or dest if dir) to src auth
#define MSG_MDS_RENAME           306   // sent from src to dest, includes inode

#define MSG_MDS_LOCK             500

#define MSG_MDS_SHUTDOWNSTART  900
#define MSG_MDS_SHUTDOWNFINISH 901



#include "include/bufferlist.h"


// use fixed offsets and static entity -> logical addr mapping!
#define MSG_ADDR_NAMER_BASE   0
#define MSG_ADDR_RANK_BASE    0x10000000    // per-rank messenger services
#define MSG_ADDR_MDS_BASE     0x20000000
#define MSG_ADDR_OSD_BASE     0x30000000
#define MSG_ADDR_MON_BASE     0x40000000
#define MSG_ADDR_CLIENT_BASE  0x50000000

#define MSG_ADDR_TYPE_MASK    0xf0000000
#define MSG_ADDR_NUM_MASK     0x0fffffff

#define MSG_ADDR_NEW          0x0fffffff
#define MSG_ADDR_UNDEF_BASE   0xffffffff


/* old int way, which lacked type safety...
typedef int  msg_addr_t;

#define MSG_ADDR_RANK(x)    (MSG_ADDR_RANK_BASE + (x))
#define MSG_ADDR_MDS(x)     (MSG_ADDR_MDS_BASE + (x))
#define MSG_ADDR_OSD(x)     (MSG_ADDR_OSD_BASE + (x))
#define MSG_ADDR_CLIENT(x)  (MSG_ADDR_CLIENT_BASE + (x))

#define MSG_ADDR_DIRECTORY   0
#define MSG_ADDR_RANK_NEW    MSG_ADDR_RANK(MSG_ADDR_NEW)
#define MSG_ADDR_MDS_NEW     MSG_ADDR_MDS(MSG_ADDR_NEW)
#define MSG_ADDR_OSD_NEW     MSG_ADDR_OSD(MSG_ADDR_NEW)
#define MSG_ADDR_CLIENT_NEW  MSG_ADDR_CLIENT(MSG_ADDR_NEW)

#define MSG_ADDR_ISCLIENT(x)  ((x) >= MSG_ADDR_CLIENT_BASE)
#define MSG_ADDR_TYPE(x)    (((x) & MSG_ADDR_TYPE_MASK) == MSG_ADDR_RANK_BASE ? "rank": \
							 (((x) & MSG_ADDR_TYPE_MASK) == MSG_ADDR_CLIENT_BASE ? "client": \
							  (((x) & MSG_ADDR_TYPE_MASK) == MSG_ADDR_OSD_BASE ? "osd": \
							   (((x) & MSG_ADDR_TYPE_MASK) == MSG_ADDR_MDS_BASE ? "mds": \
                                ((x) == MSG_ADDR_DIRECTORY ? "namer":"unknown")))))
#define MSG_ADDR_NUM(x)    ((x) & MSG_ADDR_NUM_MASK)
#define MSG_ADDR_NICE(x)   MSG_ADDR_TYPE(x) << MSG_ADDR_NUM(x)
*/

// new typed msg_addr_t way!
class msg_addr_t {
public:
  int _addr;

  msg_addr_t() : _addr(MSG_ADDR_UNDEF_BASE) {}
  msg_addr_t(int t, int n) : _addr(t | n) {}
  
  int num() const { return _addr & MSG_ADDR_NUM_MASK; }
  int type() const { return _addr & MSG_ADDR_TYPE_MASK; }
  const char *type_str() const {
	switch (type()) {
	case MSG_ADDR_RANK_BASE: return "rank";
	case MSG_ADDR_MDS_BASE: return "mds"; 
	case MSG_ADDR_OSD_BASE: return "osd"; 
	case MSG_ADDR_MON_BASE: return "mon"; 
	case MSG_ADDR_CLIENT_BASE: return "client"; 
	case MSG_ADDR_NAMER_BASE: return "namer";
	}
	return "unknown";
  }

  bool is_new() const { return num() == MSG_ADDR_NEW; }

  bool is_client() const { return type() == MSG_ADDR_CLIENT_BASE; }
  bool is_mds() const { return type() == MSG_ADDR_MDS_BASE; }
  bool is_osd() const { return type() == MSG_ADDR_OSD_BASE; }
  bool is_mon() const { return type() == MSG_ADDR_MON_BASE; }
  bool is_namer() const { return type() == MSG_ADDR_NAMER_BASE; }
};

inline bool operator== (const msg_addr_t& l, const msg_addr_t& r) { return l._addr == r._addr; }
inline bool operator!= (const msg_addr_t& l, const msg_addr_t& r) { return l._addr != r._addr; }
inline bool operator< (const msg_addr_t& l, const msg_addr_t& r) { return l._addr < r._addr; }

//typedef struct msg_addr msg_addr_t;

inline ostream& operator<<(ostream& out, const msg_addr_t& addr) {
  //if (addr.is_namer()) return out << "namer";
  return out << addr.type_str() << addr.num();
}

namespace __gnu_cxx {
  template<> struct hash< msg_addr_t >
  {
    size_t operator()( const msg_addr_t m ) const
    {
	  static hash<int> H;
      return H(m._addr);
    }
  };
}

#define MSG_ADDR_RANK(x)    msg_addr_t(MSG_ADDR_RANK_BASE,x)
#define MSG_ADDR_MDS(x)     msg_addr_t(MSG_ADDR_MDS_BASE,x)
#define MSG_ADDR_OSD(x)     msg_addr_t(MSG_ADDR_OSD_BASE,x)
#define MSG_ADDR_MON(x)     msg_addr_t(MSG_ADDR_MON_BASE,x)
#define MSG_ADDR_CLIENT(x)  msg_addr_t(MSG_ADDR_CLIENT_BASE,x)
#define MSG_ADDR_NAMER(x)   msg_addr_t(MSG_ADDR_NAMER_BASE,x)

#define MSG_ADDR_UNDEF       msg_addr_t()
#define MSG_ADDR_DIRECTORY   MSG_ADDR_NAMER(0)

#define MSG_ADDR_RANK_NEW    MSG_ADDR_RANK(MSG_ADDR_NEW)
#define MSG_ADDR_MDS_NEW     MSG_ADDR_MDS(MSG_ADDR_NEW)
#define MSG_ADDR_OSD_NEW     MSG_ADDR_OSD(MSG_ADDR_NEW)
#define MSG_ADDR_CLIENT_NEW  MSG_ADDR_CLIENT(MSG_ADDR_NEW)
#define MSG_ADDR_NAMER_NEW   MSG_ADDR_NAMER(MSG_ADDR_NEW)

#define MSG_ADDR_ISCLIENT(x)  x.is_client()
#define MSG_ADDR_TYPE(x)      x.type_str()
#define MSG_ADDR_NUM(x)       x.num()
#define MSG_ADDR_NICE(x)      x.type_str() << x.num()



#include <stdlib.h>
#include <cassert>

#include <iostream>
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;


#include "tcp.h"


class entity_inst_t {
 public:
  tcpaddr_t addr;
  int       rank;

  entity_inst_t() : rank(-1) {}
  entity_inst_t(tcpaddr_t& a, int r) : addr(a), rank(r) {}
};

inline bool operator==(entity_inst_t& a, entity_inst_t& b) { return a.rank == b.rank && a.addr == b.addr; }
inline bool operator!=(entity_inst_t& a, entity_inst_t& b) { return !(a == b); }
inline bool operator>(entity_inst_t& a, entity_inst_t& b) { return a.rank > b.rank; }
inline bool operator>=(entity_inst_t& a, entity_inst_t& b) { return a.rank >= b.rank; }
inline bool operator<(entity_inst_t& a, entity_inst_t& b) { return a.rank < b.rank; }
inline bool operator<=(entity_inst_t& a, entity_inst_t& b) { return a.rank <= b.rank; }

inline ostream& operator<<(ostream& out, entity_inst_t &i)
{
  return out << "rank" << i.rank << "_" << i.addr;
}


// abstract Message class



typedef struct {
  int type;
  msg_addr_t source, dest;
  entity_inst_t source_inst;
  int source_port, dest_port;
  int nchunks;
  __uint64_t lamport_send_stamp;
  __uint64_t lamport_recv_stamp;
} msg_envelope_t;

#define MSG_ENVELOPE_LEN  sizeof(msg_envelope_t)


class Message {
 private:
  
 protected:
  msg_envelope_t  env;    // envelope
  bufferlist      payload;        // payload
  
  friend class Messenger;
public:

 public:
  Message() { 
	env.source_port = env.dest_port = -1;
	env.source = env.dest = MSG_ADDR_UNDEF;
	env.nchunks = 0;
	env.lamport_send_stamp = 0;	
	env.lamport_recv_stamp = 0;
  };
  Message(int t) {
	env.source_port = env.dest_port = -1;
	env.source = env.dest = MSG_ADDR_UNDEF;
	env.nchunks = 0;
	env.type = t;
	env.lamport_send_stamp = 0;
	env.lamport_recv_stamp = 0;
  }
  virtual ~Message() {
  }

  void set_lamport_send_stamp(__uint64_t t) { env.lamport_send_stamp = t; }
  void set_lamport_recv_stamp(__uint64_t t) { env.lamport_recv_stamp = t; }
  __uint64_t get_lamport_send_stamp() { return env.lamport_send_stamp; }
  __uint64_t get_lamport_recv_stamp() { return env.lamport_recv_stamp; }


  // for rpc-type procedural messages (pcid = procedure call id)
  virtual long get_pcid() { return 0; }
  virtual void set_pcid(long t) { assert(0); }  // overload me

  void clear_payload() { payload.clear(); }
  bool empty_payload() { return payload.length() == 0; }
  bufferlist& get_payload() {
	return payload;
  }
  void set_payload(bufferlist& bl) {
	payload.claim(bl);
  }
  msg_envelope_t& get_envelope() {
	return env;
  }
  void set_envelope(msg_envelope_t& env) {
	this->env = env;
  }


  // ENVELOPE ----

  // type
  int get_type() { return env.type; }
  void set_type(int t) { env.type = t; }
  virtual char *get_type_name() = 0;

  // source/dest
  msg_addr_t& get_dest() { return env.dest; }
  void set_dest(msg_addr_t a, int p) { env.dest = a; env.dest_port = p; }
  int get_dest_port() { return env.dest_port; }
  
  msg_addr_t& get_source() { return env.source; }
  void set_source(msg_addr_t a, int p) { env.source = a; env.source_port = p; }
  int get_source_port() { return env.source_port; }

  entity_inst_t& get_source_inst() { return env.source_inst; }
  void set_source_inst(entity_inst_t &i) { env.source_inst = i; }

  // PAYLOAD ----
  void reset_payload() {
	payload.clear();
  }

  // overload either the rope version (easier!)
  virtual void encode_payload(crope& s)           { assert(0); }
  virtual void decode_payload(crope& s, int& off) { assert(0); }
 
  // of the bufferlist versions (faster!)
  virtual void decode_payload() {
	// use a crope for convenience, small messages, etc.  FIXME someday.
	crope ser;
	payload._rope(ser);
	
	int off = 0;
	decode_payload(ser, off);
	assert((unsigned)off == payload.length());
  }
  virtual void encode_payload() {
	assert(payload.length() == 0);  // caller should reset payload

	// use crope for convenience, small messages. FIXME someday.
	crope r;
	encode_payload(r);

	// copy payload
	payload.push_back( new buffer(r.c_str(), r.length()) );
  }
  
};

extern Message *decode_message(msg_envelope_t &env, bufferlist& bl);
ostream& operator<<(ostream& out, Message& m);

#endif
