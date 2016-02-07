// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/journal/cls_journal_types.h"
#include "common/Formatter.h"

namespace cls {
namespace journal {

void EntryPosition::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(tag_tid, bl);
  ::encode(entry_tid, bl);
  ENCODE_FINISH(bl);
}

void EntryPosition::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);
  ::decode(tag_tid, iter);
  ::decode(entry_tid, iter);
  DECODE_FINISH(iter);
}

void EntryPosition::dump(Formatter *f) const {
  f->dump_unsigned("tag_tid", tag_tid);
  f->dump_unsigned("entry_tid", entry_tid);
}

void EntryPosition::generate_test_instances(std::list<EntryPosition *> &o) {
  o.push_back(new EntryPosition());
  o.push_back(new EntryPosition(1, 2));
}

void ObjectSetPosition::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(object_number, bl);
  ::encode(entry_positions, bl);
  ENCODE_FINISH(bl);
}

void ObjectSetPosition::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);
  ::decode(object_number, iter);
  ::decode(entry_positions, iter);
  DECODE_FINISH(iter);
}

void ObjectSetPosition::dump(Formatter *f) const {
  f->dump_unsigned("object_number", object_number);
  f->open_array_section("entry_positions");
  for (EntryPositions::const_iterator it = entry_positions.begin();
       it != entry_positions.end(); ++it) {
    f->open_object_section("entry_position");
    it->dump(f);
    f->close_section();
  }
  f->close_section();
}

void ObjectSetPosition::generate_test_instances(
    std::list<ObjectSetPosition *> &o) {
  o.push_back(new ObjectSetPosition());
  o.push_back(new ObjectSetPosition(1, {{1, 120}, {2, 121}}));
}

void Client::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(id, bl);
  ::encode(data, bl);
  ::encode(commit_position, bl);
  ENCODE_FINISH(bl);
}

void Client::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);
  ::decode(id, iter);
  ::decode(data, iter);
  ::decode(commit_position, iter);
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
}

void Client::generate_test_instances(std::list<Client *> &o) {
  bufferlist data;
  data.append(std::string('1', 128));

  o.push_back(new Client());
  o.push_back(new Client("id", data));
  o.push_back(new Client("id", data, {1, {{1, 120}, {2, 121}}}));
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

std::ostream &operator<<(std::ostream &os,
                         const EntryPosition &entry_position) {
  os << "[tag_tid=" << entry_position.tag_tid << ", entry_tid="
     << entry_position.entry_tid << "]";
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const ObjectSetPosition &object_set_position) {
  os << "[object_number=" << object_set_position.object_number << ", "
     << "positions=[";
  std::string delim;
  for (auto &entry_position : object_set_position.entry_positions) {
    os << entry_position << delim;
    delim = ", ";
  }
  os << "]]";
  return os;
}

std::ostream &operator<<(std::ostream &os, const Client &client) {
  os << "[id=" << client.id << ", "
     << "commit_position=" << client.commit_position << "]";
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
