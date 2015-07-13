// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_REPLAY_HANDLER_H
#define CEPH_JOURNAL_REPLAY_HANDLER_H

namespace journal {

struct ReplayHandler  {
  virtual ~ReplayHandler() {}

  virtual void handle_entries_available() = 0;
  virtual void handle_complete(int r) = 0;
};

} // namespace journal

#endif // CEPH_JOURNAL_REPLAY_HANDLER_H
