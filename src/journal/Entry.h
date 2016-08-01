// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_ENTRY_H
#define CEPH_JOURNAL_ENTRY_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include <iosfwd>
#include <string>

namespace ceph {
class Formatter;
}

namespace journal {

class Entry {
public:
  Entry() : m_tag_tid(0), m_entry_tid() {}
  Entry(uint64_t tag_tid, uint64_t entry_tid, const bufferlist &data)
    : m_tag_tid(tag_tid), m_entry_tid(entry_tid), m_data(data)
  {
  }

  static uint32_t get_fixed_size();

  inline uint64_t get_tag_tid() const {
    return m_tag_tid;
  }
  inline uint64_t get_entry_tid() const {
    return m_entry_tid;
  }
  inline const bufferlist &get_data() const {
    return m_data;
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &iter);
  void dump(ceph::Formatter *f) const;

  bool operator==(const Entry& rhs) const;

  static bool is_readable(bufferlist::iterator iter, uint32_t *bytes_needed);
  static void generate_test_instances(std::list<Entry *> &o);

private:
  static const uint64_t preamble = 0x3141592653589793;

  uint64_t m_tag_tid;
  uint64_t m_entry_tid;
  bufferlist m_data;
};

std::ostream &operator<<(std::ostream &os, const Entry &entry);

} // namespace journal

using journal::operator<<;

WRITE_CLASS_ENCODER(journal::Entry)

#endif // CEPH_JOURNAL_ENTRY_H
