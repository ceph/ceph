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
 
#include <stdlib.h>
#include <ostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/list.hpp>
// Because intusive_ptr clobbers our assert...
#include "include/assert.h"

#include "include/types.h"
#include "include/buffer.h"
#include "common/Throttle.h"
#include "msg_types.h"

#include "common/RefCountedObj.h"
#include "msg/Connection.h"

#include "common/debug.h"
#include "common/config.h"
#include "MessageFactory.h"
// monitor internal
#define MSG_MON_SCRUB              64
#define MSG_MON_ELECTION           65
#define MSG_MON_PAXOS              66
#define MSG_MON_PROBE              67
#define MSG_MON_JOIN               68
#define MSG_MON_SYNC		   69

/* monitor <-> mon admin tool */
#define MSG_MON_COMMAND            50
#define MSG_MON_COMMAND_ACK        51
#define MSG_LOG                    52
#define MSG_LOGACK                 53
//#define MSG_MON_OBSERVE            54
//#define MSG_MON_OBSERVE_NOTIFY     55
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
#define MSG_OSD_MARK_ME_DOWN 74

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

#define MSG_OSD_PG_SCAN        94
#define MSG_OSD_PG_BACKFILL    95

#define MSG_COMMAND            97
#define MSG_COMMAND_REPLY      98

#define MSG_OSD_BACKFILL_RESERVE 99
#define MSG_OSD_RECOVERY_RESERVE 150

#define MSG_OSD_PG_PUSH        105
#define MSG_OSD_PG_PULL        106
#define MSG_OSD_PG_PUSH_REPLY  107

#define MSG_OSD_EC_WRITE       108
#define MSG_OSD_EC_WRITE_REPLY 109
#define MSG_OSD_EC_READ        110
#define MSG_OSD_EC_READ_REPLY  111

#define MSG_OSD_REPOP         112
#define MSG_OSD_REPOPREPLY    113
#define MSG_OSD_PG_UPDATE_LOG_MISSING  114
#define MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY  115


// *** MDS ***

#define MSG_MDS_BEACON             100  // to monitor
#define MSG_MDS_SLAVE_REQUEST      101
#define MSG_MDS_TABLE_REQUEST      102

                                // 150 already in use (MSG_OSD_RECOVERY_RESERVE)

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
#define MSG_MDS_FINDINO            0x20d
#define MSG_MDS_FINDINOREPLY       0x20e
#define MSG_MDS_OPENINO            0x20f
#define MSG_MDS_OPENINOREPLY       0x210

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
#define MSG_MDS_GATHERCAPS            0x472

#define MSG_MDS_HEARTBEAT          0x500  // for mds load balancer

// *** generic ***
#define MSG_TIMECHECK             0x600
#define MSG_MON_HEALTH            0x601

// *** Message::encode() crcflags bits ***
#define MSG_CRC_DATA           (1 << 0)
#define MSG_CRC_HEADER         (1 << 1)
#define MSG_CRC_ALL            (MSG_CRC_DATA | MSG_CRC_HEADER)

// Xio Testing
#define MSG_DATA_PING		  0x602

// Xio intends to define messages 0x603..0x606

// Special
#define MSG_NOP                   0x607

// ======================================================

// abstract Message class

namespace bi = boost::intrusive;

// XioMessenger conditional trace flags
#define MSG_MAGIC_XIO          0x0002
#define MSG_MAGIC_TRACE_XCON   0x0004
#define MSG_MAGIC_TRACE_DTOR   0x0008
#define MSG_MAGIC_TRACE_HDR    0x0010
#define MSG_MAGIC_TRACE_XIO    0x0020
#define MSG_MAGIC_TRACE_XMSGR  0x0040
#define MSG_MAGIC_TRACE_CTR    0x0080

// XioMessenger diagnostic "ping pong" flag (resend msg when send completes)
#define MSG_MAGIC_REDUPE       0x0100

class Message : public RefCountedObject {
protected:
  ceph_msg_header  header;      // headerelope
  ceph_msg_footer  footer;
  bufferlist       payload;  // "front" unaligned blob
  bufferlist       middle;   // "middle" unaligned blob
  bufferlist       data;     // data payload (page-alignment will be preserved where possible)

  /* recv_stamp is set when the Messenger starts reading the
   * Message off the wire */
  utime_t recv_stamp;
  /* dispatch_stamp is set when the Messenger starts calling dispatch() on
   * its endpoints */
  utime_t dispatch_stamp;
  /* throttle_stamp is the point at which we got throttle */
  utime_t throttle_stamp;
  /* time at which message was fully read */
  utime_t recv_complete_stamp;

  ConnectionRef connection;

  uint32_t magic;

  bi::list_member_hook<> dispatch_q;

public:
  class CompletionHook : public Context {
  protected:
    Message *m;
    friend class Message;
  public:
    explicit CompletionHook(Message *_m) : m(_m) {}
    virtual void set_message(Message *_m) { m = _m; }
  };

  typedef bi::list< Message,
		    bi::member_hook< Message,
				     bi::list_member_hook<>,
				     &Message::dispatch_q > > Queue;

protected:
  CompletionHook* completion_hook; // owned by Messenger

  // release our size in bytes back to this throttler when our payload
  // is adjusted or when we are destroyed.
  Throttle *byte_throttler;

  // release a count back to this throttler when we are destroyed
  Throttle *msg_throttler;

  // keep track of how big this message was when we reserved space in
  // the msgr dispatch_throttler, so that we can properly release it
  // later.  this is necessary because messages can enter the dispatch
  // queue locally (not via read_message()), and those are not
  // currently throttled.
  uint64_t dispatch_throttle_size;

  friend class Messenger;

public:
  Message()
    : connection(NULL),
      magic(0),
      completion_hook(NULL),
      byte_throttler(NULL),
      msg_throttler(NULL),
      dispatch_throttle_size(0) {
    memset(&header, 0, sizeof(header));
    memset(&footer, 0, sizeof(footer));
  }
  Message(int t, int version=1, int compat_version=0)
    : connection(NULL),
      magic(0),
      completion_hook(NULL),
      byte_throttler(NULL),
      msg_throttler(NULL),
      dispatch_throttle_size(0) {
    memset(&header, 0, sizeof(header));
    header.type = t;
    header.version = version;
    header.compat_version = compat_version;
    header.priority = 0;  // undef
    header.data_off = 0;
    memset(&footer, 0, sizeof(footer));
  }

  Message *get() {
    return static_cast<Message *>(RefCountedObject::get());
  }

protected:
  virtual ~Message() {
    if (byte_throttler)
      byte_throttler->put(payload.length() + middle.length() + data.length());
    release_message_throttle();
    /* call completion hooks (if any) */
    if (completion_hook)
      completion_hook->complete(0);
  }
public:
  const ConnectionRef& get_connection() const { return connection; }
  void set_connection(const ConnectionRef& c) {
    connection = c;
  }
  CompletionHook* get_completion_hook() { return completion_hook; }
  void set_completion_hook(CompletionHook *hook) { completion_hook = hook; }
  void set_byte_throttler(Throttle *t) { byte_throttler = t; }
  Throttle *get_byte_throttler() { return byte_throttler; }
  void set_message_throttler(Throttle *t) { msg_throttler = t; }
  Throttle *get_message_throttler() { return msg_throttler; }

  void set_dispatch_throttle_size(uint64_t s) { dispatch_throttle_size = s; }
  uint64_t get_dispatch_throttle_size() const { return dispatch_throttle_size; }

  const ceph_msg_header &get_header() const { return header; }
  ceph_msg_header &get_header() { return header; }
  void set_header(const ceph_msg_header &e) { header = e; }
  void set_footer(const ceph_msg_footer &e) { footer = e; }
  const ceph_msg_footer &get_footer() const { return footer; }
  ceph_msg_footer &get_footer() { return footer; }
  void set_src(const entity_name_t& src) { header.src = src; }

  uint32_t get_magic() const { return magic; }
  void set_magic(int _magic) { magic = _magic; }

  /*
   * If you use get_[data, middle, payload] you shouldn't
   * use it to change those bufferlists unless you KNOW
   * there is no throttle being used. The other
   * functions are throttling-aware as appropriate.
   */

  void clear_payload() {
    if (byte_throttler)
      byte_throttler->put(payload.length() + middle.length());
    payload.clear();
    middle.clear();
  }

  virtual void clear_buffers() {}
  void clear_data() {
    if (byte_throttler)
      byte_throttler->put(data.length());
    data.clear();
    clear_buffers(); // let subclass drop buffers as well
  }
  void release_message_throttle() {
    if (msg_throttler)
      msg_throttler->put();
    msg_throttler = nullptr;
  }

  bool empty_payload() const { return payload.length() == 0; }
  bufferlist& get_payload() { return payload; }
  void set_payload(bufferlist& bl) {
    if (byte_throttler)
      byte_throttler->put(payload.length());
    payload.claim(bl, buffer::list::CLAIM_ALLOW_NONSHAREABLE);
    if (byte_throttler)
      byte_throttler->take(payload.length());
  }

  void set_middle(bufferlist& bl) {
    if (byte_throttler)
      byte_throttler->put(payload.length());
    middle.claim(bl, buffer::list::CLAIM_ALLOW_NONSHAREABLE);
    if (byte_throttler)
      byte_throttler->take(payload.length());
  }
  bufferlist& get_middle() { return middle; }

  void set_data(const bufferlist &bl) {
    if (byte_throttler)
      byte_throttler->put(data.length());
    data.share(bl);
    if (byte_throttler)
      byte_throttler->take(data.length());
  }

  bufferlist& get_data() { return data; }
  void claim_data(bufferlist& bl,
		  unsigned int flags = buffer::list::CLAIM_DEFAULT) {
    if (byte_throttler)
      byte_throttler->put(data.length());
    bl.claim(data, flags);
  }
  off_t get_data_len() { return data.length(); }

  void set_recv_stamp(utime_t t) { recv_stamp = t; }
  const utime_t& get_recv_stamp() const { return recv_stamp; }
  void set_dispatch_stamp(utime_t t) { dispatch_stamp = t; }
  const utime_t& get_dispatch_stamp() const { return dispatch_stamp; }
  void set_throttle_stamp(utime_t t) { throttle_stamp = t; }
  const utime_t& get_throttle_stamp() const { return throttle_stamp; }
  void set_recv_complete_stamp(utime_t t) { recv_complete_stamp = t; }
  const utime_t& get_recv_complete_stamp() const { return recv_complete_stamp; }

  void calc_header_crc() {
    header.crc = ceph_crc32c(0, (unsigned char*)&header,
			     sizeof(header) - sizeof(header.crc));
  }
  void calc_front_crc() {
    footer.front_crc = payload.crc32c(0);
    footer.middle_crc = middle.crc32c(0);
  }
  void calc_data_crc() {
    footer.data_crc = data.crc32c(0);
  }

  virtual int get_cost() const {
    return data.length();
  }

  // type
  int get_type() const { return header.type; }
  void set_type(int t) { header.type = t; }

  uint64_t get_tid() const { return header.tid; }
  void set_tid(uint64_t t) { header.tid = t; }

  unsigned get_seq() const { return header.seq; }
  void set_seq(unsigned s) { header.seq = s; }

  unsigned get_priority() const { return header.priority; }
  void set_priority(__s16 p) { header.priority = p; }

  // source/dest
  entity_inst_t get_source_inst() const {
    return entity_inst_t(get_source(), get_source_addr());
  }
  entity_name_t get_source() const {
    return entity_name_t(header.src);
  }
  entity_addr_t get_source_addr() const {
    if (connection)
      return connection->get_peer_addr();
    return entity_addr_t();
  }

  // forwarded?
  entity_inst_t get_orig_source_inst() const {
    return get_source_inst();
  }
  entity_name_t get_orig_source() const {
    return get_orig_source_inst().name;
  }
  entity_addr_t get_orig_source_addr() const {
    return get_orig_source_inst().addr;
  }

  // virtual bits
  virtual void decode_payload() = 0;
  virtual void encode_payload(uint64_t features) = 0;
  virtual const char *get_type_name() const = 0;
  virtual void print(ostream& out) const {
    out << get_type_name() << " magic: " << magic;
  }

  virtual void dump(Formatter *f) const;

  void encode(uint64_t features, int crcflags);
};
typedef boost::intrusive_ptr<Message> MessageRef;

extern Message *decode_message(CephContext *cct, int crcflags,
			       ceph_msg_header &header,
			       ceph_msg_footer& footer, bufferlist& front,
			       bufferlist& middle, bufferlist& data);
inline ostream& operator<<(ostream& out, Message& m) {
  m.print(out);
  if (m.get_header().version)
    out << " v" << m.get_header().version;
  return out;
}

extern void encode_message(Message *m, uint64_t features, bufferlist& bl);
extern Message *decode_message(CephContext *cct, int crcflags,
                               bufferlist::iterator& bl);

#endif
