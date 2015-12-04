// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/JournalTypes.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "common/Formatter.h"

namespace librbd {
namespace journal {

namespace {

class GetEventTypeVistor : public boost::static_visitor<EventType> {
public:
  template <typename Event>
  inline EventType operator()(const Event &event) const {
    return Event::EVENT_TYPE;
  }
};

class EncodeEventVisitor : public boost::static_visitor<void> {
public:
  EncodeEventVisitor(bufferlist &bl) : m_bl(bl) {
  }

  template <typename Event>
  inline void operator()(const Event &event) const {
    ::encode(static_cast<uint32_t>(Event::EVENT_TYPE), m_bl);
    event.encode(m_bl);
  }
private:
  bufferlist &m_bl;
};

class DecodeEventVisitor : public boost::static_visitor<void> {
public:
  DecodeEventVisitor(__u8 version, bufferlist::iterator &iter)
    : m_version(version), m_iter(iter) {
  }

  template <typename Event>
  inline void operator()(Event &event) const {
    event.decode(m_version, m_iter);
  }
private:
  __u8 m_version;
  bufferlist::iterator &m_iter;
};

class DumpEventVisitor : public boost::static_visitor<void> {
public:
  DumpEventVisitor(Formatter *formatter) : m_formatter(formatter) {}

  template <typename Event>
  inline void operator()(const Event &event) const {
    EventType event_type = Event::EVENT_TYPE;
    m_formatter->dump_string("event_type", stringify(event_type));
    event.dump(m_formatter);
  }
private:
  ceph::Formatter *m_formatter;
};

} // anonymous namespace

void AioDiscardEvent::encode(bufferlist& bl) const {
  ::encode(offset, bl);
  ::encode(length, bl);
}

void AioDiscardEvent::decode(__u8 version, bufferlist::iterator& it) {
  ::decode(offset, it);
  ::decode(length, it);
}

void AioDiscardEvent::dump(Formatter *f) const {
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

void AioWriteEvent::encode(bufferlist& bl) const {
  ::encode(offset, bl);
  ::encode(length, bl);
  ::encode(data, bl);
}

void AioWriteEvent::decode(__u8 version, bufferlist::iterator& it) {
  ::decode(offset, it);
  ::decode(length, it);
  ::decode(data, it);
}

void AioWriteEvent::dump(Formatter *f) const {
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("length", length);
}

void AioFlushEvent::encode(bufferlist& bl) const {
}

void AioFlushEvent::decode(__u8 version, bufferlist::iterator& it) {
}

void AioFlushEvent::dump(Formatter *f) const {
}

void OpEventBase::encode(bufferlist& bl) const {
  ::encode(tid, bl);
}

void OpEventBase::decode(__u8 version, bufferlist::iterator& it) {
  ::decode(tid, it);
}

void OpEventBase::dump(Formatter *f) const {
  f->dump_unsigned("tid", tid);
}

void SnapEventBase::encode(bufferlist& bl) const {
  OpStartEventBase::encode(bl);
  ::encode(snap_name, bl);
}

void SnapEventBase::decode(__u8 version, bufferlist::iterator& it) {
  OpStartEventBase::decode(version, it);
  ::decode(snap_name, it);
}

void SnapEventBase::dump(Formatter *f) const {
  OpStartEventBase::dump(f);
  f->dump_string("snap_name", snap_name);
}

void SnapRenameEvent::encode(bufferlist& bl) const {
  SnapEventBase::encode(bl);
  ::encode(snap_id, bl);
}

void SnapRenameEvent::decode(__u8 version, bufferlist::iterator& it) {
  SnapEventBase::decode(version, it);
  ::decode(snap_id, it);
}

void SnapRenameEvent::dump(Formatter *f) const {
  OpStartEventBase::dump(f);
  f->dump_unsigned("src_snap_id", snap_id);
  f->dump_string("dest_snap_name", snap_name);
}

void RenameEvent::encode(bufferlist& bl) const {
  OpStartEventBase::encode(bl);
  ::encode(image_name, bl);
}

void RenameEvent::decode(__u8 version, bufferlist::iterator& it) {
  OpStartEventBase::decode(version, it);
  ::decode(image_name, it);
}

void RenameEvent::dump(Formatter *f) const {
  OpStartEventBase::dump(f);
  f->dump_string("image_name", image_name);
}

void ResizeEvent::encode(bufferlist& bl) const {
  OpStartEventBase::encode(bl);
  ::encode(size, bl);
}

void ResizeEvent::decode(__u8 version, bufferlist::iterator& it) {
  OpStartEventBase::decode(version, it);
  ::decode(size, it);
}

void ResizeEvent::dump(Formatter *f) const {
  OpStartEventBase::dump(f);
  f->dump_unsigned("size", size);
}

void UnknownEvent::encode(bufferlist& bl) const {
  assert(false);
}

void UnknownEvent::decode(__u8 version, bufferlist::iterator& it) {
}

void UnknownEvent::dump(Formatter *f) const {
}

EventType EventEntry::get_event_type() const {
  return boost::apply_visitor(GetEventTypeVistor(), event);
}

void EventEntry::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  boost::apply_visitor(EncodeEventVisitor(bl), event);
  ENCODE_FINISH(bl);
}

void EventEntry::decode(bufferlist::iterator& it) {
  DECODE_START(1, it);

  uint32_t event_type;
  ::decode(event_type, it);

  // select the correct payload variant based upon the encoded op
  switch (event_type) {
  case EVENT_TYPE_AIO_DISCARD:
    event = AioDiscardEvent();
    break;
  case EVENT_TYPE_AIO_WRITE:
    event = AioWriteEvent();
    break;
  case EVENT_TYPE_AIO_FLUSH:
    event = AioFlushEvent();
    break;
  case EVENT_TYPE_OP_FINISH:
    event = OpFinishEvent();
    break;
  case EVENT_TYPE_SNAP_CREATE:
    event = SnapCreateEvent();
    break;
  case EVENT_TYPE_SNAP_REMOVE:
    event = SnapRemoveEvent();
    break;
  case EVENT_TYPE_SNAP_RENAME:
    event = SnapRenameEvent();
    break;
  case EVENT_TYPE_SNAP_PROTECT:
    event = SnapProtectEvent();
    break;
  case EVENT_TYPE_SNAP_UNPROTECT:
    event = SnapUnprotectEvent();
    break;
  case EVENT_TYPE_SNAP_ROLLBACK:
    event = SnapRollbackEvent();
    break;
  case EVENT_TYPE_RENAME:
    event = RenameEvent();
    break;
  case EVENT_TYPE_RESIZE:
    event = ResizeEvent();
    break;
  case EVENT_TYPE_FLATTEN:
    event = FlattenEvent();
    break;
  default:
    event = UnknownEvent();
    break;
  }

  boost::apply_visitor(DecodeEventVisitor(struct_v, it), event);
  DECODE_FINISH(it);
}

void EventEntry::dump(Formatter *f) const {
  boost::apply_visitor(DumpEventVisitor(f), event);
}

void EventEntry::generate_test_instances(std::list<EventEntry *> &o) {
  o.push_back(new EventEntry(AioDiscardEvent()));
  o.push_back(new EventEntry(AioDiscardEvent(123, 345)));

  bufferlist bl;
  bl.append(std::string(32, '1'));
  o.push_back(new EventEntry(AioWriteEvent()));
  o.push_back(new EventEntry(AioWriteEvent(123, 456, bl)));

  o.push_back(new EventEntry(AioFlushEvent()));

  o.push_back(new EventEntry(OpFinishEvent(123, -1)));

  o.push_back(new EventEntry(SnapCreateEvent()));
  o.push_back(new EventEntry(SnapCreateEvent(234, "snap")));

  o.push_back(new EventEntry(SnapRemoveEvent()));
  o.push_back(new EventEntry(SnapRemoveEvent(345, "snap")));

  o.push_back(new EventEntry(SnapRenameEvent()));
  o.push_back(new EventEntry(SnapRenameEvent(345, 1, "snap")));

  o.push_back(new EventEntry(SnapProtectEvent()));
  o.push_back(new EventEntry(SnapProtectEvent(456, "snap")));

  o.push_back(new EventEntry(SnapUnprotectEvent()));
  o.push_back(new EventEntry(SnapUnprotectEvent(567, "snap")));

  o.push_back(new EventEntry(SnapRollbackEvent()));
  o.push_back(new EventEntry(SnapRollbackEvent(678, "snap")));

  o.push_back(new EventEntry(RenameEvent()));
  o.push_back(new EventEntry(RenameEvent(789, "image name")));

  o.push_back(new EventEntry(ResizeEvent()));
  o.push_back(new EventEntry(ResizeEvent(890, 1234)));

  o.push_back(new EventEntry(FlattenEvent(901)));
}

} // namespace journal
} // namespace librbd

std::ostream &operator<<(std::ostream &out,
                         const librbd::journal::EventType &type) {
  using namespace librbd::journal;

  switch (type) {
  case EVENT_TYPE_AIO_DISCARD:
    out << "AioDiscard";
    break;
  case EVENT_TYPE_AIO_WRITE:
    out << "AioWrite";
    break;
  case EVENT_TYPE_AIO_FLUSH:
    out << "AioFlush";
    break;
  case EVENT_TYPE_OP_FINISH:
    out << "OpFinish";
    break;
  case EVENT_TYPE_SNAP_CREATE:
    out << "SnapCreate";
    break;
  case EVENT_TYPE_SNAP_REMOVE:
    out << "SnapRemove";
    break;
  case EVENT_TYPE_SNAP_RENAME:
    out << "SnapRename";
    break;
  case EVENT_TYPE_SNAP_PROTECT:
    out << "SnapProtect";
    break;
  case EVENT_TYPE_SNAP_UNPROTECT:
    out << "SnapUnprotect";
    break;
  case EVENT_TYPE_SNAP_ROLLBACK:
    out << "SnapRollback";
    break;
  case EVENT_TYPE_RENAME:
    out << "Rename";
    break;
  case EVENT_TYPE_RESIZE:
    out << "Resize";
    break;
  case EVENT_TYPE_FLATTEN:
    out << "Flatten";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(type) << ")";
    break;
  }
  return out;
}
