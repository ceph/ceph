// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_REPLAY_ENTRY_H
#define CEPH_JOURNAL_REPLAY_ENTRY_H

#include "include/int_types.h"
#include "include/buffer.h"

namespace journal {

class ReplayEntry {
public:
  ReplayEntry() : m_commit_tid(0) {
  }
  ReplayEntry(const bufferlist &data, uint64_t commit_tid)
    : m_data(data), m_commit_tid(commit_tid) {
  }

  inline const bufferlist &get_data() const {
    return m_data;
  }
  inline uint64_t get_commit_tid() const {
    return m_commit_tid;
  }

private:
  bufferlist m_data;
  uint64_t m_commit_tid;
};

} // namespace journal

#endif // CEPH_JOURNAL_REPLAY_ENTRY_H
