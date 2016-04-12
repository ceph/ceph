// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/journal/cls_journal_types.h"
#include "include/stringify.h"
#include "common/Formatter.h"

namespace cls {
namespace journal {

void ObjectPosition::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(object_number, bl);
  ::encode(tag_tid, bl);
  ::encode(entry_tid, bl);
  ENCODE_FINISH(bl);
}

void ObjectPosition::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);
  ::decode(object_number, iter);
  ::decode(tag_tid, iter);
  ::decode(entry_tid, iter);
  DECODE_FINISH(iter);
}

void ObjectPosition::dump(Formatter *f) const {
  f->dump_unsigned("object_number", object_number);
  f->dump_unsigned("tag_tid", tag_tid);
  f->dump_unsigned("entry_tid", entry_tid);
}

void ObjectPosition::generate_test_instances(std::list<ObjectPosition *> &o) {
  o.push_back(new ObjectPosition());
  o.push_back(new ObjectPosition(1, 2, 3));
}

void ObjectSetPosition::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(object_positions, bl);
  ENCODE_FINISH(bl);
}

void ObjectSetPosition::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);
  ::decode(object_positions, iter);
  DECODE_FINISH(iter);
}

void ObjectSetPosition::dump(Formatter *f) const {
  f->open_array_section("object_positions");
  for (auto &pos : object_positions) {
    f->open_object_section("object_position");
    pos.dump(f);
    f->close_section();
  }
  f->close_section();
}

void ObjectSetPosition::generate_test_instances(
    std::list<ObjectSetPosition *> &o) {
  o.push_back(new ObjectSetPosition());
  o.push_back(new ObjectSetPosition({{0, 1, 120}, {121, 2, 121}}));
}

void Client::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(id, bl);
  ::encode(data, bl);
  ::encode(commit_position, bl);
  ::encode(static_cast<uint8_t>(state), bl);
  ENCODE_FINISH(bl);
}

void Client::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);
  ::decode(id, iter);
  ::decode(data, iter);
  ::decode(commit_position, iter);

  uint8_t state_raw;
  ::decode(state_raw, iter);
  state = static_cast<ClientState>(state_raw);
  DECODE_FINISH(iter);
}

void Client::dump(Formatter *f) const {
  f->dump_string("id", id);

  std::stringstream data_ss;
  data.hexdump(data_ss);
  f->dump_string("data", data_ss.str());

  f->open_object_section("commit_position");
  commit_position.dump(f);
  f->close_section();

  f->dump_string("state", stringify(state));
}

void Client::generate_test_instances(std::list<Client *> &o) {
  bufferlist data;
  data.append(std::string('1', 128));

  o.push_back(new Client());
  o.push_back(new Client("id", data));
  o.push_back(new Client("id", data, {{{1, 2, 120}, {2, 3, 121}}}));
}

void Tag::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(tid, bl);
  ::encode(tag_class, bl);
  ::encode(data, bl);
  ENCODE_FINISH(bl);
}

void Tag::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);
  ::decode(tid, iter);
  ::decode(tag_class, iter);
  ::decode(data, iter);
  DECODE_FINISH(iter);
}

void Tag::dump(Formatter *f) const {
  f->dump_unsigned("tid", tid);
  f->dump_unsigned("tag_class", tag_class);

  std::stringstream data_ss;
  data.hexdump(data_ss);
  f->dump_string("data", data_ss.str());
}

void Tag::generate_test_instances(std::list<Tag *> &o) {
  o.push_back(new Tag());

  bufferlist data;
  data.append(std::string('1', 128));
  o.push_back(new Tag(123, 234, data));
}

std::ostream &operator<<(std::ostream &os, const ClientState &state) {
  switch (state) {
  case CLIENT_STATE_CONNECTED:
    os << "connected";
    break;
  case CLIENT_STATE_DISCONNECTED:
    os << "disconnected";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(state) << ")";
    break;
  }
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const ObjectPosition &object_position) {
  os << "["
     << "object_number=" << object_position.object_number << ", "
     << "tag_tid=" << object_position.tag_tid << ", "
     << "entry_tid=" << object_position.entry_tid << "]";
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const ObjectSetPosition &object_set_position) {
  os << "[positions=[";
  std::string delim;
  for (auto &object_position : object_set_position.object_positions) {
    os << delim << object_position;
    delim = ", ";
  }
  os << "]]";
  return os;
}

std::ostream &operator<<(std::ostream &os, const Client &client) {
  os << "[id=" << client.id << ", "
     << "commit_position=" << client.commit_position << ", "
     << "state=" << client.state << "]";
  return os;
}

std::ostream &operator<<(std::ostream &os, const Tag &tag) {
  os << "[tid=" << tag.tid << ", "
     << "tag_class=" << tag.tag_class << ", "
     << "data=";
  tag.data.hexdump(os);
  os << "]";
  return os;
}

} // namespace journal
} // namespace cls
