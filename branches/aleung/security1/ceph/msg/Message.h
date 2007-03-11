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

#ifndef __MESSAGE_H
#define __MESSAGE_H
 
#define MSG_CLOSE 0

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



#define MSG_MON_ELECTION_ACK       15
#define MSG_MON_ELECTION_PROPOSE   16
#define MSG_MON_ELECTION_VICTORY   17

#define MSG_MON_OSDMAP_INFO            20
#define MSG_MON_OSDMAP_LEASE           21
#define MSG_MON_OSDMAP_LEASE_ACK       22
#define MSG_MON_OSDMAP_UPDATE_PREPARE  23
#define MSG_MON_OSDMAP_UPDATE_ACK      24
#define MSG_MON_OSDMAP_UPDATE_COMMIT   25

#define MSG_MON_PAXOS              30

#define MSG_OSD_OP           40    // delete, etc.
#define MSG_OSD_OPREPLY      41    // delete, etc.
#define MSG_OSD_PING         42

#define MSG_OSD_GETMAP       43
#define MSG_OSD_MAP          44

#define MSG_OSD_BOOT         45
#define MSG_OSD_MKFS_ACK     46

#define MSG_OSD_FAILURE      47

#define MSG_OSD_IN           48
#define MSG_OSD_OUT          49



#define MSG_OSD_PG_NOTIFY      50
#define MSG_OSD_PG_QUERY       51
#define MSG_OSD_PG_SUMMARY     52
#define MSG_OSD_PG_LOG         53
#define MSG_OSD_PG_REMOVE      54

// osd group update
#define MSG_OSD_UPDATE         55
#define MSG_OSD_UPDATE_REPLY   56

#define MSG_CLIENT_REQUEST         60
#define MSG_CLIENT_REPLY           61
//#define MSG_CLIENT_DONE            62
#define MSG_CLIENT_FILECAPS        63
#define MSG_CLIENT_INODEAUTHUPDATE 64
#define MSG_CLIENT_UPDATE          65
#define MSG_CLIENT_UPDATE_REPLY    66
#define MSG_CLIENT_RENEWAL         67

#define MSG_CLIENT_BOOT            70
#define MSG_CLIENT_MOUNT           71
#define MSG_CLIENT_MOUNTACK        72
#define MSG_CLIENT_UNMOUNT         73
#define MSG_CLIENT_AUTH_USER       74
#define MSG_CLIENT_AUTH_USER_ACK   75


// *** MDS ***

#define MSG_MDS_GETMAP             102
#define MSG_MDS_MAP                103
#define MSG_MDS_HEARTBEAT          104  // for mds load balancer
#define MSG_MDS_BEACON             105  // to monitor

#define MSG_MDS_IMPORTMAP          106
#define MSG_MDS_CACHEREJOIN        107
#define MSG_MDS_CACHEREJOINACK     108

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


#include <stdlib.h>
#include <cassert>

#include <iostream>
#include <list>
using std::list;

#include <ext/hash_map>
#include <ext/rope>

using __gnu_cxx::crope;

#include "include/types.h"
#include "include/buffer.h"
#include "msg_types.h"




// ======================================================

// abstract Message class



typedef struct {
  int type;
  entity_inst_t src, dst;
  int source_port, dest_port;
  int nchunks;
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
    env.nchunks = 0;
  };
  Message(int t) {
    env.source_port = env.dest_port = -1;
    env.nchunks = 0;
    env.type = t;
  }
  virtual ~Message() {
  }


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
  entity_inst_t& get_dest_inst() { return env.dst; }
  void set_dest_inst(entity_inst_t& inst) { env.dst = inst; }

  entity_inst_t& get_source_inst() { return env.src; }
  void set_source_inst(entity_inst_t& inst) { env.src = inst; }

  entity_name_t& get_dest() { return env.dst.name; }
  void set_dest(entity_name_t a, int p) { env.dst.name = a; env.dest_port = p; }
  int get_dest_port() { return env.dest_port; }
  void set_dest_port(int p) { env.dest_port = p; }
  
  entity_name_t& get_source() { return env.src.name; }
  void set_source(entity_name_t a, int p) { env.src.name = a; env.source_port = p; }
  int get_source_port() { return env.source_port; }

  entity_addr_t& get_source_addr() { return env.src.addr; }
  void set_source_addr(const entity_addr_t &i) { env.src.addr = i; }

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
    for (list<bufferptr>::const_iterator it = payload.buffers().begin();
         it != payload.buffers().end();
         it++)
      ser.append((*it).c_str(), (*it).length());
    
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
    payload.push_back( buffer::copy(r.c_str(), r.length()) );
  }

  virtual void print(ostream& out) {
    out << get_type_name();
  }
  
};

extern Message *decode_message(msg_envelope_t &env, bufferlist& bl);
inline ostream& operator<<(ostream& out, Message& m) {
  m.print(out);
  return out;
}

#endif
