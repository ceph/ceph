// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_JOURNAL_TYPES_H
#define CEPH_CLS_JOURNAL_TYPES_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include <string>
#include <vector>

namespace ceph {
class Formatter;
}

namespace cls {
namespace journal {

struct EntryPosition {
  std::string tag;
  uint64_t tid;

  EntryPosition() : tid(0) {}
  EntryPosition(const std::string& _tag, uint64_t _tid)
    : tag(_tag), tid(_tid) {}

  inline bool operator==(const EntryPosition& rhs) const {
    return (tag == rhs.tag && tid == rhs.tid);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& iter);
  void dump(Formatter *f);

  static void generate_test_instances(std::list<EntryPosition *> &o);
};

typedef std::vector<EntryPosition> EntryPositions;

struct ObjectSetPosition {
  uint64_t object_number;
  EntryPositions entry_positions;

  ObjectSetPosition() : object_number(0) {}
  ObjectSetPosition(uint64_t _object_number,
                    const EntryPositions &_entry_positions)
    : object_number(_object_number), entry_positions(_entry_positions) {}

  inline bool operator==(const ObjectSetPosition &rhs) const {
    return (object_number == rhs.object_number &&
            entry_positions == rhs.entry_positions);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& iter);
  void dump(Formatter *f);

  static void generate_test_instances(std::list<ObjectSetPosition *> &o);
};

struct Client {
  std::string id;
  std::string description;
  ObjectSetPosition commit_position;

  Client() {}
  Client(const std::string& _id, const std::string& _description,
         const ObjectSetPosition &_commit_position = ObjectSetPosition())
    : id(_id), description(_description), commit_position(_commit_position) {}

  inline bool operator==(const Client &rhs) const {
    return (id == rhs.id && description == rhs.description &&
            commit_position == rhs.commit_position);
  }
  inline bool operator<(const Client &rhs) const {
    return (id < rhs.id);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& iter);
  void dump(Formatter *f);

  static void generate_test_instances(std::list<Client *> &o);
};

WRITE_CLASS_ENCODER(EntryPosition);
WRITE_CLASS_ENCODER(ObjectSetPosition);
WRITE_CLASS_ENCODER(Client);

} // namespace journal
} // namespace cls

using cls::journal::encode;
using cls::journal::decode;

#endif // CEPH_CLS_JOURNAL_TYPES_H
