// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IO_OBJECT_DISPATCH_H
#define CEPH_TEST_LIBRBD_MOCK_IO_OBJECT_DISPATCH_H

#include "gmock/gmock.h"
#include "common/ceph_mutex.h"
#include "librbd/io/ObjectDispatchInterface.h"
#include "librbd/io/Types.h"

class Context;

namespace librbd {
namespace io {

struct MockObjectDispatch : public ObjectDispatchInterface {
public:
  ceph::shared_mutex lock = ceph::make_shared_mutex("MockObjectDispatch::lock");

  MockObjectDispatch() {}

  MOCK_CONST_METHOD0(get_dispatch_layer, ObjectDispatchLayer());

  MOCK_METHOD1(shut_down, void(Context*));

  MOCK_METHOD6(execute_read,
               bool(uint64_t, ReadExtents*, IOContext io_context, uint64_t*,
                    DispatchResult*, Context*));
  bool read(
      uint64_t object_no, ReadExtents* extents, IOContext io_context,
      int op_flags, int read_flags, const ZTracer::Trace& parent_trace,
      uint64_t* version, int* dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) {
    return execute_read(object_no, extents, io_context, version,
                        dispatch_result, on_dispatched);
  }

  MOCK_METHOD9(execute_discard,
               bool(uint64_t, uint64_t, uint64_t, IOContext, int,
                    int*, uint64_t*, DispatchResult*, Context*));
  bool discard(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      IOContext io_context, int discard_flags,
      const ZTracer::Trace &parent_trace, int* dispatch_flags,
      uint64_t* journal_tid, DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) {
    return execute_discard(object_no, object_off, object_len, io_context,
                           discard_flags, dispatch_flags, journal_tid,
                           dispatch_result, on_dispatched);
  }

  MOCK_METHOD10(execute_write,
               bool(uint64_t, uint64_t, const ceph::bufferlist&,
                    IOContext, int, std::optional<uint64_t>, int*,
                    uint64_t*, DispatchResult*, Context *));
  bool write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
      IOContext io_context, int op_flags, int write_flags,
      std::optional<uint64_t> assert_version,
      const ZTracer::Trace &parent_trace, int* dispatch_flags,
      uint64_t* journal_tid, DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override {
    return execute_write(object_no, object_off, data, io_context, write_flags,
                         assert_version, dispatch_flags, journal_tid,
                         dispatch_result, on_dispatched);
  }

  MOCK_METHOD10(execute_write_same,
                bool(uint64_t, uint64_t, uint64_t,
                     const LightweightBufferExtents&,
                     const ceph::bufferlist&, IOContext, int*,
                     uint64_t*, DispatchResult*, Context *));
  bool write_same(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
      IOContext io_context, int op_flags,
      const ZTracer::Trace &parent_trace, int* dispatch_flags,
      uint64_t* journal_tid, DispatchResult* dispatch_result,
      Context* *on_finish, Context* on_dispatched) override {
    return execute_write_same(object_no, object_off, object_len, buffer_extents,
                              data, io_context, dispatch_flags, journal_tid,
                              dispatch_result, on_dispatched);
  }

  MOCK_METHOD9(execute_compare_and_write,
               bool(uint64_t, uint64_t, const ceph::bufferlist&,
                    const ceph::bufferlist&, uint64_t*, int*, uint64_t*,
                    DispatchResult*, Context *));
  bool compare_and_write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
      ceph::bufferlist&& write_data, IOContext io_context, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
      int* dispatch_flags, uint64_t* journal_tid,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return execute_compare_and_write(object_no, object_off, cmp_data,
                                     write_data, mismatch_offset,
                                     dispatch_flags, journal_tid,
                                     dispatch_result, on_dispatched);
  }

  MOCK_METHOD4(execute_flush, bool(FlushSource, uint64_t*, DispatchResult*,
                                   Context*));
  bool flush(FlushSource flush_source, const ZTracer::Trace &parent_trace,
             uint64_t* journal_tid, DispatchResult* dispatch_result,
             Context** on_finish, Context* on_dispatched) {
    return execute_flush(flush_source, journal_tid, dispatch_result,
                         on_dispatched);
  }

  MOCK_METHOD7(execute_list_snaps, bool(uint64_t, const Extents&,
                                        const SnapIds&, int, SnapshotDelta*,
                                        DispatchResult*, Context*));
  bool list_snaps(
      uint64_t object_no, io::Extents&& extents, SnapIds&& snap_ids,
      int list_snaps_flags, const ZTracer::Trace &parent_trace,
      SnapshotDelta* snapshot_delta, int* object_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return execute_list_snaps(object_no, extents, snap_ids, list_snaps_flags,
                              snapshot_delta, dispatch_result, on_dispatched);
  }

  MOCK_METHOD1(invalidate_cache, bool(Context*));
  MOCK_METHOD1(reset_existence_cache, bool(Context*));

  MOCK_METHOD5(extent_overwritten, void(uint64_t, uint64_t, uint64_t, uint64_t,
                                        uint64_t));
  MOCK_METHOD2(prepare_copyup, int(uint64_t, SnapshotSparseBufferlist*));
};

} // namespace io
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IO_OBJECT_DISPATCH_H
