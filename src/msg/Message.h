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

#ifndef CEPH_MESSAGE_H
#define CEPH_MESSAGE_H
 
/* public message types */
#include "include/types.h"
#include "common/config.h"

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

// #define MSG_POOLOP                 49
// #define MSG_POOLOPREPLY            48

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
#define MSG_OSD_PG_MISSING     92
#define MSG_OSD_REP_SCRUB      93



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
#include "common/Throttle.h"
#include "msg_types.h"

#include "common/debug.h"



// ======================================================

// abstract Connection, for keeping per-connection state

struct RefCountedObject {
  atomic_t nref;
  RefCountedObject() : nref(1) {}
  virtual ~RefCountedObject() {}
  
  RefCountedObject *get() {
    //generic_dout(0) << "RefCountedObject::get " << this << " " << nref.read() << " -> " << (nref.read() + 1) << dendl;
    nref.inc();
    return this;
  }
  void put() {
    //generic_dout(0) << "RefCountedObject::put " << this << " " << nref.read() << " -> " << (nref.read() - 1) << dendl;
    if (nref.dec() == 0)
      delete this;
  }
};

struct Connection : public RefCountedObject {
  Mutex lock;
  RefCountedObject *priv;
  int peer_type;
  entity_addr_t peer_addr;
  unsigned features;
  RefCountedObject *pipe;

  int rx_buffers_version;
  map<tid_t,pair<bufferlist,int> > rx_buffers;

public:
  Connection() : lock("Connection::lock"), priv(NULL), peer_type(-1), features(0), pipe(NULL),
		 rx_buffers_version(0) {}
  ~Connection() {
    //generic_dout(0) << "~Connection " << this << dendl;
    if (priv) {
      //generic_dout(0) << "~Connection " << this << " dropping priv " << priv << dendl;
      priv->put();
    }
    if (pipe)
      pipe->put();
  }

  Connection *get() {
    return (Connection *)RefCountedObject::get();
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

  RefCountedObject *get_pipe() {
    Mutex::Locker l(lock);
    if (pipe)
      return pipe->get();
    return NULL;
  }
  void clear_pipe() {
    Mutex::Locker l(lock);
    if (pipe) {
      pipe->put();
      pipe = NULL;
    }
  }
  void reset_pipe(RefCountedObject *p) {
    Mutex::Locker l(lock);
    if (pipe)
      pipe->put();
    pipe = p->get();
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

  void post_rx_buffer(tid_t tid, bufferlist& bl) {
    Mutex::Locker l(lock);
    ++rx_buffers_version;
    rx_buffers[tid] = pair<bufferlist,int>(bl, rx_buffers_version);
  }
  void revoke_rx_buffer(tid_t tid) {
    Mutex::Locker l(lock);
    rx_buffers.erase(tid);
  }
};



// abstract Message class

class Message : public RefCountedObject {
protected:
  ceph_msg_header  header;      // headerelope
  ceph_msg_footer  footer;
  bufferlist       payload;  // "front" unaligned blob
  bufferlist       middle;   // "middle" unaligned blob
  bufferlist       data;     // data payload (page-alignment will be preserved where possible)
  
  utime_t recv_stamp;
  Connection *connection;

  // release our size in bytes back to this throttler when our payload
  // is adjusted or when we are destroyed.
  Throttle *throttler;

  // keep track of how big this message was when we reserved space in
  // the msgr dispatch_throttler, so that we can properly release it
  // later.  this is necessary because messages can enter the dispatch
  // queue locally (not via read_message()), and those are not
  // currently throttled.
  uint64_t dispatch_throttle_size;

  friend class Messenger;

public:
  Message() : connection(NULL), dispatch_throttle_size(0) {
    memset(&header, 0, sizeof(header));
    memset(&footer, 0, sizeof(footer));
    throttler = NULL;
  };
  Message(int t) : connection(NULL), dispatch_throttle_size(0) {
    memset(&header, 0, sizeof(header));
    header.type = t;
    header.version = 1;
    header.priority = 0;  // undef
    header.data_off = 0;
    memset(&footer, 0, sizeof(footer));
    throttler = NULL;
  }

  Message *get() {
    return (Message *)RefCountedObject::get();
  }

protected:
  virtual ~Message() { 
    assert(nref.read() == 0);
    if (connection)
      connection->put();
    if (throttler)
      throttler->put(payload.length() + middle.length() + data.length());
  }
public:
  Connection *get_connection() { return connection; }
  void set_connection(Connection *c) {
    if (connection)
      connection->put();
    connection = c;
  }
  void set_throttler(Throttle *t) { throttler = t; }
  Throttle *get_throttler() { return throttler; }
 
  void set_dispatch_throttle_size(uint64_t s) { dispatch_throttle_size = s; }
  uint64_t get_dispatch_throttle_size() { return dispatch_throttle_size; }

  ceph_msg_header &get_header() { return header; }
  void set_header(const ceph_msg_header &e) { header = e; }
  void set_footer(const ceph_msg_footer &e) { footer = e; }
  ceph_msg_footer &get_footer() { return footer; }

  /*
   * If you use get_[data, middle, payload] you shouldn't
   * use it to change those bufferlists unless you KNOW
   * there is no throttle being used. The other
   * functions are throttling-aware as appropriate.
   */

  void clear_payload() {
    if (throttler) throttler->put(payload.length() + middle.length());
    payload.clear();
    middle.clear();
  }
  void clear_data() {
    if (throttler) throttler->put(data.length());
    data.clear();
  }

  bool empty_payload() { return payload.length() == 0; }
  bufferlist& get_payload() { return payload; }
  void set_payload(bufferlist& bl) {
    if (throttler) throttler->put(payload.length());
    payload.claim(bl);
    if (throttler) throttler->take(payload.length());
  }

  void set_middle(bufferlist& bl) {
    if (throttler) throttler->put(payload.length());
    middle.claim(bl);
    if (throttler) throttler->take(payload.length());
  }
  bufferlist& get_middle() { return middle; }

  void set_data(const bufferlist &d) {
    if (throttler) throttler->put(data.length());
    data = d;
    if (throttler) throttler->take(data.length());
  }

  bufferlist& get_data() { return data; }
  void claim_data(bufferlist& bl) {
    if (throttler) throttler->put(data.length());
    bl.claim(data);
  }
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

  uint64_t get_tid() { return header.tid; }
  void set_tid(uint64_t t) { header.tid = t; }

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
    return get_source_inst();
  }
  entity_name_t get_orig_source() {
    return get_orig_source_inst().name;
  }
  entity_addr_t get_orig_source_addr() {
    return get_orig_source_inst().addr;
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
  if (m.get_header().version)
    out << " v" << m.get_header().version;
  return out;
}

extern void encode_message(Message *m, bufferlist& bl);
extern Message *decode_message(bufferlist::iterator& bl);

#endif
