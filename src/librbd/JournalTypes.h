// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_TYPES_H
#define CEPH_LIBRBD_JOURNAL_TYPES_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include <iosfwd>
#include <boost/variant.hpp>

namespace ceph {
class Formatter;
}

namespace librbd {
namespace journal {

enum EventType {
  EVENT_TYPE_AIO_DISCARD = 0,
  EVENT_TYPE_AIO_WRITE   = 1,
  EVENT_TYPE_AIO_FLUSH   = 2
};

struct AioDiscardEvent {
  static const EventType EVENT_TYPE = EVENT_TYPE_AIO_DISCARD;

  uint64_t offset;
  size_t length;

  AioDiscardEvent() : offset(0), length(0) {
  }
  AioDiscardEvent(uint64_t _offset, size_t _length)
    : offset(_offset), length(_length) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

struct AioWriteEvent {
  static const EventType EVENT_TYPE = EVENT_TYPE_AIO_WRITE;

  uint64_t offset;
  size_t length;
  bufferlist data;

  AioWriteEvent() : offset(0), length(0) {
  }
  AioWriteEvent(uint64_t _offset, size_t _length, const bufferlist &_data)
    : offset(_offset), length(_length), data(_data) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

struct UnknownEvent {
  static const EventType EVENT_TYPE = static_cast<EventType>(-1);

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

struct AioFlushEvent {
  static const EventType EVENT_TYPE = EVENT_TYPE_AIO_FLUSH;

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

typedef boost::variant<AioDiscardEvent,
                       AioWriteEvent,
                       AioFlushEvent,
                       UnknownEvent> Event;

struct EventEntry {
  EventEntry() : event(UnknownEvent()) {
  }
  EventEntry(const Event &_event) : event(_event) {
  }

  Event event;

  EventType get_event_type() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<EventEntry *> &o);
};

} // namespace journal
} // namespace librbd

std::ostream &operator<<(std::ostream &out,
                         const librbd::journal::EventType &type);

WRITE_CLASS_ENCODER(librbd::journal::EventEntry);

#endif // CEPH_LIBRBD_JOURNAL_TYPES_H
