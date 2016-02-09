// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/Entry.h"
#include "include/encoding.h"
#include "include/stringify.h"
#include "common/Formatter.h"
#include <strstream>

#define dout_subsys ceph_subsys_journaler
#undef dout_prefix
#define dout_prefix *_dout << "Entry: "

namespace journal {

namespace {

const uint32_t HEADER_FIXED_SIZE = 25; /// preamble, version, entry tid, tag id

} // anonymous namespace

void Entry::encode(bufferlist &bl) const {
  bufferlist data_bl;
  ::encode(preamble, data_bl);
  ::encode(static_cast<uint8_t>(1), data_bl);
  ::encode(m_entry_tid, data_bl);
  ::encode(m_tag_tid, data_bl);
  assert(HEADER_FIXED_SIZE == data_bl.length());

  ::encode(m_data, data_bl);

  uint32_t crc = data_bl.crc32c(0);
  bl.claim_append(data_bl);
  ::encode(crc, bl);
}

void Entry::decode(bufferlist::iterator &iter) {
  uint32_t start_offset = iter.get_off();
  uint64_t bl_preamble;
  ::decode(bl_preamble, iter);
  if (bl_preamble != preamble) {
    throw buffer::malformed_input("incorrect preamble: " +
                                  stringify(bl_preamble));
  }

  uint8_t version;
  ::decode(version, iter);
  if (version != 1) {
    throw buffer::malformed_input("unknown version: " + stringify(version));
  }

  ::decode(m_entry_tid, iter);
  ::decode(m_tag_tid, iter);
  ::decode(m_data, iter);
  uint32_t end_offset = iter.get_off();

  uint32_t crc;
  ::decode(crc, iter);

  bufferlist data_bl;
  data_bl.substr_of(iter.get_bl(), start_offset, end_offset - start_offset);
  uint32_t actual_crc = data_bl.crc32c(0);
  if (crc != actual_crc) {
    throw buffer::malformed_input("crc mismatch: " + stringify(crc) +
                                  " != " + stringify(actual_crc));
  }
}

void Entry::dump(Formatter *f) const {
  f->dump_unsigned("tag_tid", m_tag_tid);
  f->dump_unsigned("entry_tid", m_entry_tid);

  std::stringstream data;
  m_data.hexdump(data);
  f->dump_string("data", data.str());
}

bool Entry::is_readable(bufferlist::iterator iter, uint32_t *bytes_needed) {
  uint32_t start_off = iter.get_off();
  if (iter.get_remaining() < HEADER_FIXED_SIZE) {
    *bytes_needed = HEADER_FIXED_SIZE - iter.get_remaining();
    return false;
  }
  uint64_t bl_preamble;
  ::decode(bl_preamble, iter);
  if (bl_preamble != preamble) {
    *bytes_needed = 0;
    return false;
  }
  iter.advance(HEADER_FIXED_SIZE - sizeof(bl_preamble));

  if (iter.get_remaining() < sizeof(uint32_t)) {
    *bytes_needed = sizeof(uint32_t) - iter.get_remaining();
    return false;
  }
  uint32_t data_size;
  ::decode(data_size, iter);

  if (iter.get_remaining() < data_size) {
    *bytes_needed = data_size - iter.get_remaining();
    return false;
  }
  iter.advance(data_size);
  uint32_t end_off = iter.get_off();

  if (iter.get_remaining() < sizeof(uint32_t)) {
    *bytes_needed = sizeof(uint32_t) - iter.get_remaining();
    return false;
  }

  bufferlist crc_bl;
  crc_bl.substr_of(iter.get_bl(), start_off, end_off - start_off);

  *bytes_needed = 0;
  uint32_t crc;
  ::decode(crc, iter);
  if (crc != crc_bl.crc32c(0)) {
    return false;
  }
  return true;
}

void Entry::generate_test_instances(std::list<Entry *> &o) {
  o.push_back(new Entry(1, 123, bufferlist()));

  bufferlist bl;
  bl.append("data");
  o.push_back(new Entry(2, 123, bl));
}

bool Entry::operator==(const Entry& rhs) const {
  return (m_tag_tid == rhs.m_tag_tid && m_entry_tid == rhs.m_entry_tid &&
          const_cast<bufferlist&>(m_data).contents_equal(
            const_cast<bufferlist&>(rhs.m_data)));
}

std::ostream &operator<<(std::ostream &os, const Entry &entry) {
  os << "Entry[tag_tid=" << entry.get_tag_tid() << ", "
     << "entry_tid=" << entry.get_entry_tid() << ", "
     << "data size=" << entry.get_data().length() << "]";
  return os;
}

} // namespace journal
