// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_client.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/PerfReporter.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::PerfReporter: " << this << " " \
                           << __func__ << ": "

namespace librbd {

using util::create_rados_callback;

template <typename I>
PerfReporter<I>::PerfReporter(I &image_ctx) : m_image_ctx(image_ctx) {
  CephContext *cct = m_image_ctx.cct;

  ImageCtx::get_timer_instance(cct, &m_timer, &m_timer_lock);
}

template <typename I>
PerfReporter<I>::~PerfReporter() {
}

template <typename I>
void PerfReporter<I>::init(PerfCounters *raw_counters) {
  CephContext *cct = m_image_ctx.cct;

  m_raw_counters = raw_counters;
  m_report_ctx = new FunctionContext([this](int r) {
      report();
    });

  Mutex::Locker timer_locker(*m_timer_lock);
  m_timer->add_event_after(cct->_conf->rbd_perf_report_interval, m_report_ctx);
}

template <typename I>
void PerfReporter<I>::shutdown() {
  Mutex::Locker timer_locker(*m_timer_lock);

  if (m_report_ctx != nullptr) {
    m_timer->cancel_event(m_report_ctx);
    m_report_ctx = nullptr;
  }
}

template <typename I>
void PerfReporter<I>::report() {
  CephContext *cct = m_image_ctx.cct;

  assert(m_timer_lock->is_locked());

  bool toreport = false;
  auto &counters = m_counters.counters;

  // TODO: report only part of the counters
  m_raw_counters->with_counters([this, cct, &counters, &toreport](
      const PerfCounters::perf_counter_data_vec_t &data) {
      for (const auto &c : data) {
        cls::rbd::PerfCounter counter;

        if (c.type & PERFCOUNTER_HISTOGRAM) {
          continue;
        }

        auto a = c.read_avg();

        counter.type = static_cast<uint32_t>(c.type);
        counter.u64 = a.first;
        counter.avgcount = a.second;

        if (counters.count(c.name)) {
          if (counters[c.name] == counter)
            continue;

          toreport = true;
          counters[c.name] = counter;
        }

        toreport = true;
        counters.insert({c.name, counter});
      }
    });

  if (!toreport) {
    m_report_ctx = new FunctionContext([this](int r) {
        report();
      });
    m_timer->add_event_after(cct->_conf->rbd_perf_report_interval, m_report_ctx);
    return;
  }

  librados::ObjectWriteOperation op;
  librbd::cls_client::image_perf_update(&op, m_counters);
  using klass = PerfReporter<I>;
  librados::AioCompletion *rados_completion = create_rados_callback<
      klass, &klass::handle_report>(this);
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
      rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void PerfReporter<I>::handle_report(int r) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "r=" << r << dendl;

  Mutex::Locker timer_locker(*m_timer_lock);

  m_report_ctx = new FunctionContext([this](int r) {
      report();
    });
  m_timer->add_event_after(cct->_conf->rbd_perf_report_interval, m_report_ctx);
}

} // namespace librbd

template class librbd::PerfReporter<librbd::ImageCtx>;
