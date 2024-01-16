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

#ifndef CEPH_LOGEVENT_H
#define CEPH_LOGEVENT_H

#define EVENT_NEW_ENCODING 0 // indicates that the encoding is versioned
#define EVENT_UNUSED       1 // was previously EVENT_STRING

#define EVENT_SUBTREEMAP   2
#define EVENT_EXPORT       3
#define EVENT_IMPORTSTART  4
#define EVENT_IMPORTFINISH 5
#define EVENT_FRAGMENT     6

#define EVENT_RESETJOURNAL 9

#define EVENT_SESSION      10
#define EVENT_SESSIONS_OLD 11
#define EVENT_SESSIONS     12

#define EVENT_UPDATE       20
#define EVENT_PEERUPDATE   21
#define EVENT_OPEN         22
#define EVENT_COMMITTED    23
#define EVENT_PURGED       24

#define EVENT_TABLECLIENT  42
#define EVENT_TABLESERVER  43

#define EVENT_SUBTREEMAP_TEST   50
#define EVENT_NOOP        51


#include "include/buffer_fwd.h"
#include "include/Context.h"
#include "include/utime.h"

class MDSRank;
class LogSegment;
class EMetaBlob;

// generic log event
class LogEvent {
public:
  typedef __u32 EventType;
  friend class MDLog;

  LogEvent() = delete;
  explicit LogEvent(int t) : _type(t) {}
  LogEvent(const LogEvent&) = delete;
  LogEvent& operator=(const LogEvent&) = delete;
  virtual ~LogEvent() {}

  std::string_view get_type_str() const;
  static EventType str_to_type(std::string_view str);
  EventType get_type() const { return _type; }
  void set_type(EventType t) { _type = t; }

  uint64_t get_start_off() const { return _start_off; }
  void set_start_off(uint64_t o) { _start_off = o; }

  utime_t get_stamp() const { return stamp; }
  void set_stamp(utime_t t) { stamp = t; }

  // encoding
  virtual void encode(bufferlist& bl, uint64_t features) const = 0;
  virtual void decode(bufferlist::const_iterator &) = 0;
  static std::unique_ptr<LogEvent> decode_event(bufferlist::const_iterator);
  virtual void dump(Formatter *f) const = 0;

  void encode_with_header(bufferlist& bl, uint64_t features) {
    using ceph::encode;
    encode(EVENT_NEW_ENCODING, bl);
    ENCODE_START(1, 1, bl)
    encode(_type, bl);
    this->encode(bl, features);
    ENCODE_FINISH(bl);
  }

  virtual void print(std::ostream& out) const {
    out << "event(" << _type << ")";
  }

  /*** live journal ***/
  /* update_segment() - adjust any state we need to in the LogSegment 
   */
  virtual void update_segment() { }

  /*** recovery ***/
  /* replay() - replay given event.  this is idempotent.
   */
  virtual void replay(MDSRank *m) { ceph_abort(); }

  /**
   * If the subclass embeds a MetaBlob, return it here so that
   * tools can examine metablobs while traversing lists of LogEvent.
   */
  virtual EMetaBlob *get_metablob() { return NULL; }

protected:
  LogSegment* get_segment() { return _segment; }
  LogSegment const* get_segment() const { return _segment; }

  utime_t stamp;

private:
  static const std::map<std::string, LogEvent::EventType> types;

  static std::unique_ptr<LogEvent> decode_event(bufferlist::const_iterator&, EventType);

  EventType _type = 0;
  uint64_t _start_off = 0;
  LogSegment *_segment = nullptr;
};

#endif
