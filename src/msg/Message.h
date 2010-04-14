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
#include "include/types.h"
#include "config.h"

// monitor internal
#define MSG_MON_ELECTION           65
#define MSG_MON_PAXOS              66

/* monitor <-> mon admin tool */
#define MSG_MON_COMMAND            50
#define MSG_MON_COMMAND_ACK        51
#define MSG_LOG                    52
#define MSG_LOGACK                 53
#define MSG_MON_OBSERVE            54
#define MSG_MON_OBSERVE_NOTIFY     55
#define MSG_CLASS                  56
#define MSG_CLASS_ACK              57

#define MSG_GETPOOLSTATS           58
#define MSG_GETPOOLSTATSREPLY      59

#define MSG_MON_GLOBAL_ID          60

#define MSG_POOLOP                 49
#define MSG_POOLOPREPLY            48

#define MSG_ROUTE                  47
#define MSG_FORWARD                46

#define MSG_PAXOS                  40


// osd internal
#define MSG_OSD_PING         70
#define MSG_OSD_BOOT         71
#define MSG_OSD_FAILURE      72
#define MSG_OSD_ALIVE        73

#define MSG_OSD_SUBOP        76
#define MSG_OSD_SUBOPREPLY   77

#define MSG_OSD_PGTEMP       78

#define MSG_OSD_PG_NOTIFY      80
#define MSG_OSD_PG_QUERY       81
#define MSG_OSD_PG_SUMMARY     82
#define MSG_OSD_PG_LOG         83
#define MSG_OSD_PG_REMOVE      84
#define MSG_OSD_PG_INFO        85
#define MSG_OSD_PG_TRIM        86

#define MSG_PGSTATS            87
#define MSG_PGSTATSACK         88

#define MSG_OSD_PG_CREATE      89
#define MSG_REMOVE_SNAPS       90

#define MSG_OSD_SCRUB          91



// *** MDS ***

#define MSG_MDS_BEACON             100  // to monitor
#define MSG_MDS_SLAVE_REQUEST      101
#define MSG_MDS_TABLE_REQUEST      102

#define MSG_MDS_RESOLVE            0x200
#define MSG_MDS_RESOLVEACK         0x201
#define MSG_MDS_CACHEREJOIN        0x202
#define MSG_MDS_DISCOVER           0x203
#define MSG_MDS_DISCOVERREPLY      0x204
#define MSG_MDS_INODEUPDATE        0x205
#define MSG_MDS_DIRUPDATE          0x206
#define MSG_MDS_CACHEEXPIRE        0x207
#define MSG_MDS_DENTRYUNLINK       0x208
#define MSG_MDS_FRAGMENTNOTIFY     0x209
#define MSG_MDS_OFFLOAD_TARGETS    0x20a
#define MSG_MDS_OFFLOAD_COMPLETE   0x20b
#define MSG_MDS_DENTRYLINK         0x20c

#define MSG_MDS_LOCK               0x300
#define MSG_MDS_INODEFILECAPS      0x301

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

#define MSG_MDS_HEARTBEAT          0x500  // for mds load balancer



#include <stdlib.h>

#include <iostream>
#include <list>
using std::list;

#include <ext/hash_map>


#include "include/types.h"
#include "include/buffer.h"
#include "msg_types.h"

#include "common/debug.h"



// ======================================================

// abstract Connection, for keeping per-connection state

struct RefCountedObject {
  atomic_t nref;
  RefCountedObject() : nref(1) {}
  virtual ~RefCountedObject() {}
  
  RefCountedObject *get() {
    //generic_dout(0) << "RefCountedObject::get " << this << " " << nref.test() << " -> " << (nref.test() + 1) << dendl;
    nref.inc();
    return this;
  }
  void put() {
    //generic_dout(0) << "RefCountedObject::put " << this << " " << nref.test() << " -> " << (nref.test() - 1) << dendl;
    if (nref.dec() == 0)
      delete this;
  }
};

struct Connection : public RefCountedObject {
  atomic_t nref;
  Mutex lock;
  RefCountedObject *priv;
  int peer_type;
  entity_addr_t peer_addr;
  unsigned features;

public:
  Connection() : nref(1), lock("Connection::lock"), priv(NULL), peer_type(-1), features(0) {}
  ~Connection() {
    //generic_dout(0) << "~Connection " << this << dendl;
    if (priv) {
      //generic_dout(0) << "~Connection " << this << " dropping priv " << priv << dendl;
      priv->put();
    }
  }

  Connection *get() {
    nref.inc();
    return this;
  }
  void put() {
    if (nref.dec() == 0)
      delete this;
  }

  void set_priv(RefCountedObject *o) {
    Mutex::Locker l(lock);
    if (priv)
      priv->put();
    priv = o;
  }
  RefCountedObject *get_priv() {
    Mutex::Locker l(lock);
    if (priv)
      return priv->get();
    return NULL;
  }

  int get_peer_type() { return peer_type; }
  void set_peer_type(int t) { peer_type = t; }
  
  bool peer_is_mon() { return peer_type == CEPH_ENTITY_TYPE_MON; }
  bool peer_is_mds() { return peer_type == CEPH_ENTITY_TYPE_MDS; }
  bool peer_is_osd() { return peer_type == CEPH_ENTITY_TYPE_OSD; }
  bool peer_is_client() { return peer_type == CEPH_ENTITY_TYPE_CLIENT; }

  const entity_addr_t& get_peer_addr() { return peer_addr; }
  void set_peer_addr(const entity_addr_t& a) { peer_addr = a; }

  int get_features() const { return features; }
  bool has_feature(int f) const { return features & f; }
  void set_features(unsigned f) { features = f; }
  void set_feature(unsigned f) { features |= f; }
};



// abstract Message class

class Message {
protected:
  ceph_msg_header  header;      // headerelope
  ceph_msg_footer  footer;
  bufferlist       payload;  // "front" unaligned blob
  bufferlist       middle;   // "middle" unaligned blob
  bufferlist       data;     // data payload (page-alignment will be preserved where possible)
  
  utime_t recv_stamp;
  Connection *connection;

  friend class Messenger;

  bool _forwarded;
  entity_inst_t _orig_source_inst;
  
public:
  atomic_t nref;

  Message() : connection(NULL), _forwarded(false), nref(0) {
    memset(&header, 0, sizeof(header));
    memset(&footer, 0, sizeof(footer));
  };
  Message(int t) : connection(NULL), _forwarded(false), nref(0) {
    memset(&header, 0, sizeof(header));
    header.type = t;
    header.version = 1;
    header.priority = 0;  // undef
    header.data_off = 0;
    memset(&footer, 0, sizeof(footer));
  }
  virtual ~Message() { 
    assert(nref.read() == 0);
    if (connection)
      connection->put();
  }

  Message *get() {
    //int r = 
    nref.inc();
    //*_dout << dbeginl << "message(" << this << ").get " << (r-1) << " -> " << r << std::endl;
    //_dout_end_line();
    return this;
  }
  void put() {
    int r = nref.dec();
    //*_dout << dbeginl << "message(" << this << ").put " << (r+1) << " -> " << r << std::endl;
    //_dout_end_line();
    if (r == 0)
      delete this;
  }

  Connection *get_connection() { return connection; }
  void set_connection(Connection *c) { connection = c; }
 
  ceph_msg_header &get_header() { return header; }
  void set_header(const ceph_msg_header &e) { header = e; }
  void set_footer(const ceph_msg_footer &e) { footer = e; }
  ceph_msg_footer &get_footer() { return footer; }

  void clear_payload() { payload.clear(); middle.clear(); }
  bool empty_payload() { return payload.length() == 0; }
  bufferlist& get_payload() { return payload; }
  void set_payload(bufferlist& bl) { payload.claim(bl); }
  void copy_payload(const bufferlist& bl) { payload = bl; }

  void set_middle(bufferlist& bl) { middle.claim(bl); }
  bufferlist& get_middle() { return middle; }

  void set_data(const bufferlist &d) { data = d; }
  void copy_data(const bufferlist &d) { data = d; }
  bufferlist& get_data() { return data; }
  off_t get_data_len() { return data.length(); }

  void set_recv_stamp(utime_t t) { recv_stamp = t; }
  utime_t get_recv_stamp() { return recv_stamp; }

  void calc_header_crc() {
    header.crc = ceph_crc32c_le(0, (unsigned char*)&header,
			   sizeof(header) - sizeof(header.crc));
  }
  void calc_front_crc() {
    footer.front_crc = payload.crc32c(0);
    footer.middle_crc = middle.crc32c(0);
  }
  void calc_data_crc() {
    footer.data_crc = data.crc32c(0);
  }

  // type
  int get_type() { return header.type; }
  void set_type(int t) { header.type = t; }

  __u64 get_tid() { return header.tid; }
  void set_tid(__u64 t) { header.tid = t; }

  unsigned get_seq() { return header.seq; }
  void set_seq(unsigned s) { header.seq = s; }

  unsigned get_priority() { return header.priority; }
  void set_priority(__s16 p) { header.priority = p; }

  // source/dest
  entity_inst_t get_source_inst() {
    return entity_inst_t(get_source(), get_source_addr());
  }
  entity_name_t get_source() {
    return entity_name_t(header.src);
  }
  entity_addr_t get_source_addr() {
    if (connection)
      return connection->get_peer_addr();
    return entity_addr_t();
  }

  // forwarded?
  entity_inst_t get_orig_source_inst() {
    if (_forwarded)
      return _orig_source_inst;
    return get_source_inst();
  }
  entity_name_t get_orig_source() {
    return get_orig_source_inst().name;
  }
  entity_addr_t get_orig_source_addr() {
    return get_orig_source_inst().addr;
  }
  void set_orig_source_inst(entity_inst_t& i) {
    _forwarded = true;
    _orig_source_inst = i;
  }

  // virtual bits
  virtual void decode_payload() = 0;
  virtual void encode_payload() = 0;
  virtual const char *get_type_name() = 0;
  virtual void print(ostream& out) {
    out << get_type_name();
  }

  void encode();
};

extern Message *decode_message(ceph_msg_header &header, ceph_msg_footer& footer,
			       bufferlist& front, bufferlist& middle, bufferlist& data);
inline ostream& operator<<(ostream& out, Message& m) {
  m.print(out);
  return out;
}

extern void encode_message(Message *m, bufferlist& bl);
extern Message *decode_message(bufferlist::iterator& bl);

#endif
