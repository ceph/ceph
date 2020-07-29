// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IO_OBJECT_DISPATCH_H
#define CEPH_TEST_LIBRBD_MOCK_IO_OBJECT_DISPATCH_H

#include "gmock/gmock.h"
#include "librbd/io/ObjectDispatchInterface.h"
#include "librbd/io/Types.h"

class Context;

namespace librbd {
namespace io {

struct MockObjectDispatch : public ObjectDispatchInterface {
public:
  RWLock lock;

  MockObjectDispatch() : lock("MockObjectDispatch::lock", true, false) {
  }

  MOCK_CONST_METHOD0(get_dispatch_layer, ObjectDispatchLayer());

  MOCK_METHOD1(shut_down, void(Context*));

  MOCK_METHOD8(execute_read,
               bool(uint64_t, uint64_t, uint64_t, librados::snap_t,
                    ceph::bufferlist*, Extents*, DispatchResult*, Context*));
  bool read(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      librados::snap_t snap_id, int op_flags,
      const ZTracer::Trace& parent_trace, ceph::bufferlist* read_data,
      Extents* extent_map, int* dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) {
    return execute_read(object_no, object_off, object_len, snap_id, read_data,
                        extent_map, dispatch_result, on_dispatched);
  }

  MOCK_METHOD9(execute_discard,
               bool(uint64_t, uint64_t, uint64_t, const ::SnapContext &, int,
                    int*, uint64_t*, DispatchResult*, Context*));
  bool discard(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      const ::SnapContext &snapc, int discard_flags,
      const ZTracer::Trace &parent_trace, int* dispatch_flags,
      uint64_t* journal_tid, DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) {
    return execute_discard(object_no, object_off, object_len, snapc,
                           discard_flags, dispatch_flags, journal_tid,
                           dispatch_result, on_dispatched);
  }

  MOCK_METHOD8(execute_write,
               bool(uint64_t, uint64_t, const ceph::bufferlist&,
                    const ::SnapContext &, int*, uint64_t*, DispatchResult*,
                    Context *));
  bool write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
      const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, int* dispatch_flags,
      uint64_t* journal_tid, DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override {
    return execute_write(object_no, object_off, data, snapc, dispatch_flags,
                         journal_tid, dispatch_result, on_dispatched);
  }

  MOCK_METHOD10(execute_write_same,
                bool(uint64_t, uint64_t, uint64_t,
                     const LightweightBufferExtents&,
                     const ceph::bufferlist&, const ::SnapContext &, int*,
                     uint64_t*, DispatchResult*, Context *));
  bool write_same(
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
      const ::SnapContext &snapc, int op_flags,
      const ZTracer::Trace &parent_trace, int* dispatch_flags,
      uint64_t* journal_tid, DispatchResult* dispatch_result,
      Context* *on_finish, Context* on_dispatched) override {
    return execute_write_same(object_no, object_off, object_len, buffer_extents,
                              data, snapc, dispatch_flags, journal_tid,
                              dispatch_result, on_dispatched);
  }

  MOCK_METHOD9(execute_compare_and_write,
               bool(uint64_t, uint64_t, const ceph::bufferlist&,
                    const ceph::bufferlist&, uint64_t*, int*, uint64_t*,
                    DispatchResult*, Context *));
  bool compare_and_write(
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
      ceph::bufferlist&& write_data, const ::SnapContext &snapc, int op_flags,
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

  MOCK_METHOD1(invalidate_cache, bool(Context*));
  MOCK_METHOD1(reset_existence_cache, bool(Context*));

  MOCK_METHOD5(extent_overwritten, void(uint64_t, uint64_t, uint64_t, uint64_t,
                                        uint64_t));
};

} // namespace io
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IO_OBJECT_DISPATCH_H
