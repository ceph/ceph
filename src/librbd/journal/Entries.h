// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_ENTRIES_H
#define CEPH_LIBRBD_JOURNAL_ENTRIES_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/types.h"
#include <iosfwd>
#include <boost/variant.hpp>

namespace ceph {
class Formatter;
}

namespace librbd {
namespace journal {

enum EventType {
  EVENT_TYPE_AIO_DISCARD    = 0,
  EVENT_TYPE_AIO_WRITE      = 1,
  EVENT_TYPE_AIO_FLUSH      = 2,
  EVENT_TYPE_OP_FINISH      = 3,
  EVENT_TYPE_SNAP_CREATE    = 4,
  EVENT_TYPE_SNAP_REMOVE    = 5,
  EVENT_TYPE_SNAP_RENAME    = 6,
  EVENT_TYPE_SNAP_PROTECT   = 7,
  EVENT_TYPE_SNAP_UNPROTECT = 8,
  EVENT_TYPE_SNAP_ROLLBACK  = 9,
  EVENT_TYPE_RENAME         = 10,
  EVENT_TYPE_RESIZE         = 11,
  EVENT_TYPE_FLATTEN        = 12
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

struct AioFlushEvent {
  static const EventType EVENT_TYPE = EVENT_TYPE_AIO_FLUSH;

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

struct OpEventBase {
  uint64_t op_tid;

protected:
  OpEventBase() : op_tid(0) {
  }
  OpEventBase(uint64_t op_tid) : op_tid(op_tid) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

struct OpFinishEvent : public OpEventBase {
  static const EventType EVENT_TYPE = EVENT_TYPE_OP_FINISH;

  int r;

  OpFinishEvent() : r(0) {
  }
  OpFinishEvent(uint64_t op_tid, int r) : OpEventBase(op_tid), r(r) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

struct SnapEventBase : public OpEventBase {
  std::string snap_name;

protected:
  SnapEventBase() {
  }
  SnapEventBase(uint64_t op_tid, const std::string &_snap_name)
    : OpEventBase(op_tid), snap_name(_snap_name) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

struct SnapCreateEvent : public SnapEventBase {
  static const EventType EVENT_TYPE = EVENT_TYPE_SNAP_CREATE;

  SnapCreateEvent() {
  }
  SnapCreateEvent(uint64_t op_tid, const std::string &snap_name)
    : SnapEventBase(op_tid, snap_name) {
  }

  using SnapEventBase::encode;
  using SnapEventBase::decode;
  using SnapEventBase::dump;
};

struct SnapRemoveEvent : public SnapEventBase {
  static const EventType EVENT_TYPE = EVENT_TYPE_SNAP_REMOVE;

  SnapRemoveEvent() {
  }
  SnapRemoveEvent(uint64_t op_tid, const std::string &snap_name)
    : SnapEventBase(op_tid, snap_name) {
  }

  using SnapEventBase::encode;
  using SnapEventBase::decode;
  using SnapEventBase::dump;
};

struct SnapRenameEvent : public SnapEventBase {
  static const EventType EVENT_TYPE = EVENT_TYPE_SNAP_RENAME;

  uint64_t snap_id;

  SnapRenameEvent() : snap_id(CEPH_NOSNAP) {
  }
  SnapRenameEvent(uint64_t op_tid, uint64_t src_snap_id,
                  const std::string &dest_snap_name)
    : SnapEventBase(op_tid, dest_snap_name), snap_id(src_snap_id) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

struct SnapProtectEvent : public SnapEventBase {
  static const EventType EVENT_TYPE = EVENT_TYPE_SNAP_PROTECT;

  SnapProtectEvent() {
  }
  SnapProtectEvent(uint64_t op_tid, const std::string &snap_name)
    : SnapEventBase(op_tid, snap_name) {
  }

  using SnapEventBase::encode;
  using SnapEventBase::decode;
  using SnapEventBase::dump;
};

struct SnapUnprotectEvent : public SnapEventBase {
  static const EventType EVENT_TYPE = EVENT_TYPE_SNAP_UNPROTECT;

  SnapUnprotectEvent() {
  }
  SnapUnprotectEvent(uint64_t op_tid, const std::string &snap_name)
    : SnapEventBase(op_tid, snap_name) {
  }

  using SnapEventBase::encode;
  using SnapEventBase::decode;
  using SnapEventBase::dump;
};

struct SnapRollbackEvent : public SnapEventBase {
  static const EventType EVENT_TYPE = EVENT_TYPE_SNAP_ROLLBACK;

  SnapRollbackEvent() {
  }
  SnapRollbackEvent(uint64_t op_tid, const std::string &snap_name)
    : SnapEventBase(op_tid, snap_name) {
  }

  using SnapEventBase::encode;
  using SnapEventBase::decode;
  using SnapEventBase::dump;
};

struct RenameEvent : public OpEventBase {
  static const EventType EVENT_TYPE = EVENT_TYPE_RENAME;

  std::string image_name;

  RenameEvent() {
  }
  RenameEvent(uint64_t op_tid, const std::string &_image_name)
    : OpEventBase(op_tid), image_name(_image_name) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

struct ResizeEvent : public OpEventBase {
  static const EventType EVENT_TYPE = EVENT_TYPE_RESIZE;

  uint64_t size;

  ResizeEvent() : size(0) {
  }
  ResizeEvent(uint64_t op_tid, uint64_t _size)
    : OpEventBase(op_tid), size(_size) {
  }

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

struct FlattenEvent : public OpEventBase {
  static const EventType EVENT_TYPE = EVENT_TYPE_FLATTEN;

  FlattenEvent() {
  }
  FlattenEvent(uint64_t op_tid) : OpEventBase(op_tid) {
  }

  using OpEventBase::encode;
  using OpEventBase::decode;
  using OpEventBase::dump;
};

struct UnknownEvent {
  static const EventType EVENT_TYPE = static_cast<EventType>(-1);

  void encode(bufferlist& bl) const;
  void decode(__u8 version, bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

typedef boost::variant<AioDiscardEvent,
                       AioWriteEvent,
                       AioFlushEvent,
                       OpFinishEvent,
                       SnapCreateEvent,
                       SnapRemoveEvent,
                       SnapRenameEvent,
                       SnapProtectEvent,
                       SnapUnprotectEvent,
                       SnapRollbackEvent,
                       RenameEvent,
                       ResizeEvent,
                       FlattenEvent,
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

#endif // CEPH_LIBRBD_JOURNAL_ENTRIES_H
