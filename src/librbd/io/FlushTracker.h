// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_FLUSH_TRACKER_H
#define CEPH_LIBRBD_IO_FLUSH_TRACKER_H

#include "include/int_types.h"
#include "common/ceph_mutex.h"
#include <atomic>
#include <list>
#include <map>
#include <set>
#include <unordered_map>

struct Context;

namespace librbd {

struct ImageCtx;

namespace io {

struct AioCompletion;

template <typename ImageCtxT>
class FlushTracker {
public:
  FlushTracker(ImageCtxT* image_ctx);
  ~FlushTracker();

  void shut_down();

  uint64_t start_io(uint64_t tid);
  void finish_io(uint64_t tid);

  void flush(Context* on_finish);

private:
  typedef std::list<Context*> Contexts;
  typedef std::map<uint64_t, Contexts> FlushContexts;
  typedef std::set<uint64_t> Tids;
  typedef std::unordered_map<uint64_t, uint64_t> TidToFlushTid;

  ImageCtxT* m_image_ctx;

  std::atomic<uint32_t> m_next_flush_tid{0};

  mutable ceph::shared_mutex m_lock;
  TidToFlushTid m_tid_to_flush_tid;

  Tids m_in_flight_flush_tids;
  FlushContexts m_flush_contexts;

};

} // namespace io
} // namespace librbd

extern template class librbd::io::FlushTracker<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_FLUSH_TRACKER_H
