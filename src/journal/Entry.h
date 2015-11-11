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
  Entry() : m_tid() {}
  Entry(const std::string &tag, uint64_t tid, const bufferlist &data)
    : m_tag(tag), m_tid(tid), m_data(data)
  {
  }

  inline const std::string &get_tag() const {
    return m_tag;
  }
  inline uint64_t get_tid() const {
    return m_tid;
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

  std::string m_tag;
  uint64_t m_tid;
  bufferlist m_data;
};

std::ostream &operator<<(std::ostream &os, const Entry &entry);

} // namespace journal

using journal::operator<<;

WRITE_CLASS_ENCODER(journal::Entry)

#endif // CEPH_JOURNAL_ENTRY_H
