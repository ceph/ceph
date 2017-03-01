// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_PERF_REPORTER_H
#define CEPH_LIBRBD_PERF_REPORTER_H

#include "cls/rbd/cls_rbd_types.h"

class Context;
class Mutex;
class PerfCounters;
class SafeTimer;

namespace librbd {

struct ImageCtx;

template <typename ImageCtxT = ImageCtx>
class PerfReporter {
public:
  PerfReporter(const PerfReporter&) = delete;
  PerfReporter& operator=(const PerfReporter&) = delete;
  explicit PerfReporter(ImageCtxT &image_ctx);
  ~PerfReporter();

  void init(PerfCounters *raw_counters);
  void shutdown();

  void report();
  void handle_report(int r);

private:
  ImageCtxT &m_image_ctx;
  PerfCounters *m_raw_counters = nullptr;
  cls::rbd::PerfCounters m_counters = {};
  SafeTimer *m_timer = nullptr;
  Mutex *m_timer_lock = nullptr;
  Context *m_report_ctx = nullptr;
};

} // namespace librbd

extern template class librbd::PerfReporter<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_PERF_REPORTER_H
