// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_SIMPLE_SCHEDULER_OBJECT_DISPATCH_H
#define CEPH_LIBRBD_IO_SIMPLE_SCHEDULER_OBJECT_DISPATCH_H

#include "common/ceph_mutex.h"
#include "common/snap_types.h"
#include "include/interval_set.h"
#include "include/utime.h"

#include "librbd/io/ObjectDispatchInterface.h"
#include "librbd/io/TypeTraits.h"

#include <list>
#include <map>
#include <memory>

namespace librbd {

class ImageCtx;

namespace io {

class LatencyStats;

/**
 * Simple scheduler plugin for object dispatcher layer.
 */
template <typename ImageCtxT = ImageCtx>
class SimpleSchedulerObjectDispatch : public ObjectDispatchInterface {
private:
  // mock unit testing support
  typedef ::librbd::io::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::SafeTimer SafeTimer;
public:
  static SimpleSchedulerObjectDispatch* create(ImageCtxT* image_ctx) {
    return new SimpleSchedulerObjectDispatch(image_ctx);
  }

  SimpleSchedulerObjectDispatch(ImageCtxT* image_ctx);
  ~SimpleSchedulerObjectDispatch() override;

  ObjectDispatchLayer get_dispatch_layer() const override {
    return OBJECT_DISPATCH_LAYER_SCHEDULER;
  }

  void init();
  void shut_down(Context* on_finish) override;

  bool read(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      librados::snap_t snap_id, int op_flags,
      const ZTracer::Trace &parent_trace, ceph::bufferlist* read_data,
      Extents* extent_map, int* object_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool discard(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      const ::SnapContext &snapc, int discard_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;

  bool write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
      const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;

  bool write_same(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
      const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;

  bool compare_and_write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
      ceph::bufferlist&& write_data, const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
      int* object_dispatch_flags, uint64_t* journal_tid,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool flush(
      FlushSource flush_source, const ZTracer::Trace &parent_trace,
      uint64_t* journal_tid, DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;

  bool invalidate_cache(Context* on_finish) override {
    return false;
  }
  bool reset_existence_cache(Context* on_finish) override {
    return false;
  }

  void extent_overwritten(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      uint64_t journal_tid, uint64_t new_journal_tid) override {
  }

private:
  struct MergedRequests {
    ceph::bufferlist data;
    std::list<Context *> requests;
  };

  class ObjectRequests {
  public:
    using clock_t = ceph::real_clock;

    ObjectRequests(uint64_t object_no) : m_object_no(object_no) {
    }

    uint64_t get_object_no() const {
      return m_object_no;
    }

    void set_dispatch_seq(uint64_t dispatch_seq) {
      m_dispatch_seq = dispatch_seq;
    }

    uint64_t get_dispatch_seq() const {
      return m_dispatch_seq;
    }

    clock_t::time_point get_dispatch_time() const {
      return m_dispatch_time;
    }

    void set_scheduled_dispatch(const clock_t::time_point &dispatch_time) {
      m_dispatch_time = dispatch_time;
    }

    bool is_scheduled_dispatch() const {
      return !clock_t::is_zero(m_dispatch_time);
    }

    size_t delayed_requests_size() const {
      return m_delayed_requests.size();
    }

    bool intersects(uint64_t object_off, uint64_t len) const {
      return m_delayed_request_extents.intersects(object_off, len);
    }

    bool try_delay_request(uint64_t object_off, ceph::bufferlist&& data,
                           const ::SnapContext &snapc, int op_flags,
                           int object_dispatch_flags, Context* on_dispatched);

    void dispatch_delayed_requests(ImageCtxT *image_ctx,
                                   LatencyStats *latency_stats,
                                   ceph::mutex *latency_stats_lock);

  private:
    uint64_t m_object_no;
    uint64_t m_dispatch_seq = 0;
    clock_t::time_point m_dispatch_time;
    SnapContext m_snapc = {0, {}};
    int m_op_flags = 0;
    int m_object_dispatch_flags = 0;
    std::map<uint64_t, MergedRequests> m_delayed_requests;
    interval_set<uint64_t> m_delayed_request_extents;

    void try_merge_delayed_requests(
        typename std::map<uint64_t, MergedRequests>::iterator &iter,
        typename std::map<uint64_t, MergedRequests>::iterator &iter2);
  };

  typedef std::shared_ptr<ObjectRequests> ObjectRequestsRef;
  typedef std::map<uint64_t, ObjectRequestsRef> Requests;

  ImageCtxT *m_image_ctx;

  ceph::mutex m_lock;
  SafeTimer *m_timer;
  ceph::mutex *m_timer_lock;
  uint64_t m_max_delay;
  uint64_t m_dispatch_seq = 0;

  Requests m_requests;
  std::list<ObjectRequestsRef> m_dispatch_queue;
  Context *m_timer_task = nullptr;
  std::unique_ptr<LatencyStats> m_latency_stats;

  bool try_delay_write(uint64_t object_no, uint64_t object_off,
                       ceph::bufferlist&& data, const ::SnapContext &snapc,
                       int op_flags, int object_dispatch_flags,
                       Context* on_dispatched);
  bool intersects(uint64_t object_no, uint64_t object_off, uint64_t len) const;

  void dispatch_all_delayed_requests();
  void dispatch_delayed_requests(uint64_t object_no);
  void dispatch_delayed_requests(ObjectRequestsRef object_requests);
  void register_in_flight_request(uint64_t object_no, const utime_t &start_time,
                                  Context** on_finish);

  void schedule_dispatch_delayed_requests();
};

} // namespace io
} // namespace librbd

extern template class librbd::io::SimpleSchedulerObjectDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_SIMPLE_SCHEDULER_OBJECT_DISPATCH_H
