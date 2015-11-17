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
  default:
    out << "Unknown (" << static_cast<uint32_t>(type) << ")";
    break;
  }
  return out;
}
