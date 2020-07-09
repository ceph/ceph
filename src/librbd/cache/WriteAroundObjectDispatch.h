// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_WRITE_AROUND_OBJECT_DISPATCH_H
#define CEPH_LIBRBD_CACHE_WRITE_AROUND_OBJECT_DISPATCH_H

#include "librbd/io/ObjectDispatchInterface.h"
#include "include/interval_set.h"
#include "common/ceph_mutex.h"
#include "librbd/io/Types.h"
#include <map>
#include <set>
#include <string>

struct Context;

namespace librbd {

struct ImageCtx;

namespace cache {

template <typename ImageCtxT = ImageCtx>
class WriteAroundObjectDispatch : public io::ObjectDispatchInterface {
public:
  static WriteAroundObjectDispatch* create(ImageCtxT* image_ctx,
                                           size_t max_dirty,
                                          bool writethrough_until_flush) {
    return new WriteAroundObjectDispatch(image_ctx, max_dirty,
                                         writethrough_until_flush);
  }

  WriteAroundObjectDispatch(ImageCtxT* image_ctx, size_t max_dirty,
                            bool writethrough_until_flush);
  ~WriteAroundObjectDispatch() override;

  io::ObjectDispatchLayer get_dispatch_layer() const override {
    return io::OBJECT_DISPATCH_LAYER_CACHE;
  }

  void init();
  void shut_down(Context* on_finish) override;

  bool read(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      librados::snap_t snap_id, int op_flags,
      const ZTracer::Trace &parent_trace, ceph::bufferlist* read_data,
      io::Extents* extent_map, int* object_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool discard(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      const ::SnapContext &snapc, int discard_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context**on_finish, Context* on_dispatched) override;

  bool write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
      const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context**on_finish, Context* on_dispatched) override;

  bool write_same(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      io::LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
      const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
      Context**on_finish, Context* on_dispatched) override;

  bool compare_and_write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
      ceph::bufferlist&& write_data, const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
      int* object_dispatch_flags, uint64_t* journal_tid,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool flush(
      io::FlushSource flush_source, const ZTracer::Trace &parent_trace,
      uint64_t* journal_tid, io::DispatchResult* dispatch_result,
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
  struct QueuedIO {
    QueuedIO(uint64_t length, Context* on_finish, Context* on_dispatched)
      : length(length), on_finish(on_finish), on_dispatched(on_dispatched) {
    }

    uint64_t length;
    Context* on_finish;
    Context* on_dispatched;
  };

  struct QueuedFlush {
    QueuedFlush(Context* on_finish, Context* on_dispatched)
      : on_finish(on_finish), on_dispatched(on_dispatched) {
    }

    Context* on_finish;
    Context* on_dispatched;
  };


  struct BlockedIO : public QueuedIO {
    BlockedIO(uint64_t offset, uint64_t length, Context* on_finish,
              Context* on_dispatched)
      : QueuedIO(length, on_finish, on_dispatched), offset(offset) {
    }

    uint64_t offset;
  };

  typedef std::map<uint64_t, QueuedIO> QueuedIOs;
  typedef std::map<uint64_t, QueuedFlush> QueuedFlushes;

  typedef std::map<uint64_t, BlockedIO> BlockedObjectIOs;
  typedef std::map<uint64_t, BlockedObjectIOs> BlockedIOs;

  typedef std::map<uint64_t, Context*> Contexts;
  typedef std::set<uint64_t> Tids;
  typedef interval_set<uint64_t> InFlightObjectExtents;
  typedef std::map<uint64_t, InFlightObjectExtents> InFlightExtents;

  ImageCtxT* m_image_ctx;
  size_t m_init_max_dirty;
  size_t m_max_dirty;

  ceph::mutex m_lock;
  bool m_user_flushed = false;

  uint64_t m_last_tid = 0;
  uint64_t m_in_flight_bytes = 0;

  Tids m_in_flight_io_tids;
  InFlightExtents m_in_flight_extents;

  BlockedIOs m_blocked_ios;
  QueuedIOs m_queued_ios;
  Tids m_queued_or_blocked_io_tids;

  BlockedIOs m_blocked_unoptimized_ios;

  QueuedFlushes m_queued_flushes;
  Contexts m_in_flight_flushes;
  Contexts m_pending_flushes;
  int m_pending_flush_error = 0;

  bool dispatch_unoptimized_io(uint64_t object_no, uint64_t object_off,
                               uint64_t object_len,
                               io::DispatchResult* dispatch_result,
                               Context* on_dispatched);
  bool dispatch_io(uint64_t object_no, uint64_t object_off,
                   uint64_t object_len, int op_flags,
                   io::DispatchResult* dispatch_result, Context** on_finish,
                   Context* on_dispatch);

  bool block_overlapping_io(InFlightObjectExtents* in_flight_object_extents,
                            uint64_t object_off, uint64_t object_len);
  void unblock_overlapping_ios(uint64_t object_no, uint64_t object_off,
                               uint64_t object_len,
                               Contexts* unoptimized_io_dispatches);

  bool can_dispatch_io(uint64_t tid, uint64_t length);

  void handle_in_flight_io_complete(int r, uint64_t tid, uint64_t object_no,
                                    uint64_t object_off, uint64_t object_len);
  void handle_in_flight_flush_complete(int r, uint64_t tid);

  QueuedIOs collect_ready_ios();
  Contexts collect_ready_flushes();
  Contexts collect_finished_flushes();

};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::WriteAroundObjectDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_WRITE_AROUND_OBJECT_DISPATCH_H
