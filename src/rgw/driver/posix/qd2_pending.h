// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cerrno>
#include <future>
#include <utility>
#include "include/buffer.h"

namespace rgw::sal {

/*
 * One in-flight File::write(), overlapping the next socket read in the
 * PUT process() loop (QD2).  The write runs on a helper thread so
 * complete()/destruction can join without deadlocking the request
 * strand — unlike GET iterate(), process() returns with I/O still
 * outstanding.
 */
class QD2PendingWrite {
  ceph::bufferlist pending_bl;
  std::future<int> inflight;

public:
  QD2PendingWrite() = default;
  QD2PendingWrite(const QD2PendingWrite&) = delete;
  QD2PendingWrite& operator=(const QD2PendingWrite&) = delete;

  ~QD2PendingWrite() {
    try {
      drain();
    } catch (...) {}
  }

  int drain() {
    if (!inflight.valid()) {
      return 0;
    }
    int r;
    try {
      r = inflight.get();
    } catch (...) {
      r = -EIO;
    }
    inflight = {};
    pending_bl.clear();
    return r;
  }

  template <typename Write>
  int process(ceph::bufferlist&& data, uint64_t offset, bool pipeline,
              Write&& do_write) {
    int r = drain();
    if (r < 0) {
      return r;
    }
    if (data.length() == 0) {
      return 0;
    }
    if (!pipeline) {
      return do_write(offset, data);
    }
    pending_bl = std::move(data);
    inflight = std::async(std::launch::async,
        [this, offset, do_write = std::forward<Write>(do_write)]() mutable {
          return do_write(offset, pending_bl);
        });
    return 0;
  }
};

} // namespace rgw::sal
