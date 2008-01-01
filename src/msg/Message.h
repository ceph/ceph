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

#ifndef __MESSAGE_H
#define __MESSAGE_H
 
/* public message types */
#include "include/ceph_fs.h"

// monitor internal
#define MSG_MON_ELECTION           60
#define MSG_MON_PAXOS              61

/* monitor <-> mon admin tool */
#define MSG_MON_COMMAND            50
#define MSG_MON_COMMAND_ACK        51

// osd internal
#define MSG_OSD_PING         70
#define MSG_OSD_BOOT         71
#define MSG_OSD_FAILURE      72
#define MSG_OSD_IN           73
#define MSG_OSD_OUT          74

#define MSG_OSD_SUBOP        75
#define MSG_OSD_SUBOPREPLY   76

#define MSG_OSD_PG_NOTIFY      80
#define MSG_OSD_PG_QUERY       81
#define MSG_OSD_PG_SUMMARY     82
#define MSG_OSD_PG_LOG         83
#define MSG_OSD_PG_REMOVE      84
#define MSG_OSD_PG_ACTIVATE_SET 85

#define MSG_PGSTATS    86



// *** MDS ***

#define MSG_MDS_RESOLVE            0x200
#define MSG_MDS_RESOLVEACK         0x201
#define MSG_MDS_CACHEREJOIN        0x202
#define MSG_MDS_DISCOVER           0x203
#define MSG_MDS_DISCOVERREPLY      0x204
#define MSG_MDS_INODEUPDATE  0x205
#define MSG_MDS_DIRUPDATE    0x206
#define MSG_MDS_CACHEEXPIRE  0x207
#define MSG_MDS_DENTRYUNLINK      0x208
#define MSG_MDS_FRAGMENTNOTIFY 0x209

#define MSG_MDS_LOCK             0x300
#define MSG_MDS_INODEFILECAPS    0x301

#define MSG_MDS_EXPORTDIRDISCOVER     0x449
#define MSG_MDS_EXPORTDIRDISCOVERACK  0x450
#define MSG_MDS_EXPORTDIRCANCEL       0x451
#define MSG_MDS_EXPORTDIRPREP         0x452
#define MSG_MDS_EXPORTDIRPREPACK      0x453
#define MSG_MDS_EXPORTDIRWARNING      0x454
#define MSG_MDS_EXPORTDIRWARNINGACK   0x455
#define MSG_MDS_EXPORTDIR             0x456
#define MSG_MDS_EXPORTDIRACK          0x457
#define MSG_MDS_EXPORTDIRNOTIFY       0x458
#define MSG_MDS_EXPORTDIRNOTIFYACK    0x459
#define MSG_MDS_EXPORTDIRFINISH       0x460

#define MSG_MDS_EXPORTCAPS            0x470
#define MSG_MDS_EXPORTCAPSACK         0x471

#define MSG_MDS_BEACON             90  // to monitor
#define MSG_MDS_SLAVE_REQUEST      91

#define MSG_MDS_ANCHOR             0x100
#define MSG_MDS_HEARTBEAT          0x500  // for mds load balancer



#include <stdlib.h>
#include <cassert>

#include <iostream>
#include <list>
using std::list;

#include <ext/hash_map>


#include "include/types.h"
#include "include/buffer.h"
#include "msg_types.h"




// ======================================================

// abstract Message class


class Message {
protected:
  ceph_msg_header  env;      // envelope
  bufferlist       payload;  // "front" unaligned blob
  bufferlist       data;     // data payload (page-alignment will be preserved where possible)
  
  utime_t recv_stamp;

  friend class Messenger;

public:
  Message() { };
  Message(int t) {
    env.type = t;
    env.data_off = 0;
  }
  virtual ~Message() { }

  ceph_msg_header &get_env() { return env; }
  void set_env(const ceph_msg_header &e) { env = e; }

  void clear_payload() { payload.clear(); }
  bool empty_payload() { return payload.length() == 0; }
  bufferlist& get_payload() { return payload; }
  void set_payload(bufferlist& bl) { payload.claim(bl); }
  void copy_payload(const bufferlist& bl) { payload = bl; }

  void set_data(bufferlist &d) { data.claim(d); }
  void copy_data(const bufferlist &d) { data = d; }
  bufferlist& get_data() { return data; }
  off_t get_data_len() { return data.length(); }

  void set_recv_stamp(utime_t t) { recv_stamp = t; }
  utime_t get_recv_stamp() { return recv_stamp; }

  // ENVELOPE ----

  // type
  int get_type() { return env.type; }
  void set_type(int t) { env.type = t; }

  unsigned get_seq() { return env.seq; }
  void set_seq(unsigned s) { env.seq = s; }

  // source/dest
  entity_inst_t& get_dest_inst() { return *(entity_inst_t*)&env.dst; }
  void set_dest_inst(entity_inst_t& inst) { env.dst = *(ceph_entity_inst*)&inst; }

  entity_inst_t& get_source_inst() { return *(entity_inst_t*)&env.src; }
  void set_source_inst(entity_inst_t& inst) { env.src = *(ceph_entity_inst*)&inst; }

  entity_name_t& get_dest() { return *(entity_name_t*)&env.dst.name; }
  void set_dest(entity_name_t a) { env.dst.name = *(ceph_entity_name*)&a; }
  
  entity_name_t& get_source() { return *(entity_name_t*)&env.src.name; }
  void set_source(entity_name_t a) { env.src.name = *(ceph_entity_name*)&a; }

  entity_addr_t& get_source_addr() { return *(entity_addr_t*)&env.src.addr; }
  void set_source_addr(const entity_addr_t &i) { env.src.addr = *(ceph_entity_addr*)&i; }

  // virtual bits
  virtual void decode_payload() = 0;
  virtual void encode_payload() = 0;
  virtual const char *get_type_name() = 0;
  virtual void print(ostream& out) {
    out << get_type_name();
  }
  
};

extern Message *decode_message(ceph_msg_header &env, bufferlist& front, bufferlist& data);
inline ostream& operator<<(ostream& out, Message& m) {
  m.print(out);
  return out;
}

#endif
