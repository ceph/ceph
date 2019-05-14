// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_JOURNAL_TYPES_H
#define CEPH_CLS_JOURNAL_TYPES_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include <iosfwd>
#include <list>
#include <string>

namespace ceph {
class Formatter;
}

namespace cls {
namespace journal {

static const uint64_t JOURNAL_MAX_RETURN = 256;

struct ObjectPosition {
  uint64_t object_number;
  uint64_t tag_tid;
  uint64_t entry_tid;

  ObjectPosition() : object_number(0), tag_tid(0), entry_tid(0) {}
  ObjectPosition(uint64_t _object_number, uint64_t _tag_tid,
                 uint64_t _entry_tid)
    : object_number(_object_number), tag_tid(_tag_tid), entry_tid(_entry_tid) {}

  inline bool operator==(const ObjectPosition& rhs) const {
    return (object_number == rhs.object_number &&
            tag_tid == rhs.tag_tid &&
            entry_tid == rhs.entry_tid);
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& iter);
  void dump(ceph::Formatter *f) const;

  inline bool operator<(const ObjectPosition &rhs) const {
    if (object_number != rhs.object_number) {
      return object_number < rhs.object_number;
    } else if (tag_tid != rhs.tag_tid) {
      return tag_tid < rhs.tag_tid;
    }
    return entry_tid < rhs.entry_tid;
  }

  static void generate_test_instances(std::list<ObjectPosition *> &o);
};

typedef std::list<ObjectPosition> ObjectPositions;

struct ObjectSetPosition {
  // stored in most-recent -> least recent committed entry order
  ObjectPositions object_positions;

  ObjectSetPosition() {}
  ObjectSetPosition(const ObjectPositions &_object_positions)
    : object_positions(_object_positions) {}

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& iter);
  void dump(ceph::Formatter *f) const;

  inline bool operator==(const ObjectSetPosition &rhs) const {
    return (object_positions == rhs.object_positions);
  }

  static void generate_test_instances(std::list<ObjectSetPosition *> &o);
};

enum ClientState {
  CLIENT_STATE_CONNECTED = 0,
  CLIENT_STATE_DISCONNECTED = 1
};

struct Client {
  std::string id;
  ceph::buffer::list data;
  ObjectSetPosition commit_position;
  ClientState state;

  Client() : state(CLIENT_STATE_CONNECTED) {}
  Client(const std::string& _id, const ceph::buffer::list &_data,
         const ObjectSetPosition &_commit_position = ObjectSetPosition(),
         ClientState _state = CLIENT_STATE_CONNECTED)
    : id(_id), data(_data), commit_position(_commit_position), state(_state) {}

  inline bool operator==(const Client &rhs) const {
    return (id == rhs.id &&
            data.contents_equal(rhs.data) &&
            commit_position == rhs.commit_position &&
            state == rhs.state);
  }
  inline bool operator<(const Client &rhs) const {
    return (id < rhs.id);
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& iter);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<Client *> &o);
};

struct Tag {
  static const uint64_t TAG_CLASS_NEW = static_cast<uint64_t>(-1);

  uint64_t tid;
  uint64_t tag_class;
  ceph::buffer::list data;

  Tag() : tid(0), tag_class(0) {}
  Tag(uint64_t tid, uint64_t tag_class, const ceph::buffer::list &data)
    : tid(tid), tag_class(tag_class), data(data) {}

  inline bool operator==(const Tag &rhs) const {
    return (tid == rhs.tid &&
            tag_class == rhs.tag_class &&
            data.contents_equal(rhs.data));
  }
  inline bool operator<(const Tag &rhs) const {
    return (tid < rhs.tid);
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& iter);
  void dump(ceph::Formatter *f) const;

  static void generate_test_instances(std::list<Tag *> &o);
};

WRITE_CLASS_ENCODER(ObjectPosition);
WRITE_CLASS_ENCODER(ObjectSetPosition);
WRITE_CLASS_ENCODER(Client);
WRITE_CLASS_ENCODER(Tag);

std::ostream &operator<<(std::ostream &os, const ClientState &state);
std::ostream &operator<<(std::ostream &os,
                         const ObjectPosition &object_position);
std::ostream &operator<<(std::ostream &os,
                         const ObjectSetPosition &object_set_position);
std::ostream &operator<<(std::ostream &os,
			 const Client &client);
std::ostream &operator<<(std::ostream &os, const Tag &tag);

} // namespace journal
} // namespace cls

#endif // CEPH_CLS_JOURNAL_TYPES_H
