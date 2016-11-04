// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_SETTINGS_H
#define CEPH_JOURNAL_SETTINGS_H

#include "include/int_types.h"

namespace journal {

struct Settings {
  double commit_interval = 5;         ///< commit position throttle (in secs)
  uint64_t max_fetch_bytes = 0;       ///< 0 implies no limit
  uint64_t max_payload_bytes = 0;     ///< 0 implies object size limit
  int max_concurrent_object_sets = 0; ///< 0 implies no limit
  std::set<std::string> whitelisted_laggy_clients;
                                      ///< clients that mustn't be disconnected
};

} // namespace journal

#endif // # CEPH_JOURNAL_SETTINGS_H
