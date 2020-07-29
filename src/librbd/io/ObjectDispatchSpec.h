// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_OBJECT_DISPATCH_SPEC_H
#define CEPH_LIBRBD_IO_OBJECT_DISPATCH_SPEC_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/snap_types.h"
#include "common/zipkin_trace.h"
#include "librbd/io/Types.h"
#include <boost/variant/variant.hpp>

namespace librbd {
namespace io {

struct ObjectDispatcherInterface;

struct ObjectDispatchSpec {
private:
  // helper to avoid extra heap allocation per object IO
  struct C_Dispatcher : public Context {
    ObjectDispatchSpec* object_dispatch_spec;
    Context* on_finish;

    C_Dispatcher(ObjectDispatchSpec* object_dispatch_spec, Context* on_finish)
      : object_dispatch_spec(object_dispatch_spec), on_finish(on_finish) {
    }

    void complete(int r) override;
    void finish(int r) override;
  };

public:
  struct RequestBase {
    uint64_t object_no;
    uint64_t object_off;

    RequestBase(uint64_t object_no, uint64_t object_off)
      : object_no(object_no), object_off(object_off) {
    }
  };

  struct ReadRequest : public RequestBase {
    uint64_t object_len;
    librados::snap_t snap_id;
    ceph::bufferlist* read_data;
    Extents* extent_map;

    ReadRequest(uint64_t object_no, uint64_t object_off, uint64_t object_len,
                librados::snap_t snap_id, ceph::bufferlist* read_data,
                Extents* extent_map)
      : RequestBase(object_no, object_off),
        object_len(object_len), snap_id(snap_id), read_data(read_data),
        extent_map(extent_map) {
    }
  };

  struct WriteRequestBase : public RequestBase {
    ::SnapContext snapc;
    uint64_t journal_tid;

    WriteRequestBase(uint64_t object_no, uint64_t object_off,
                     const ::SnapContext& snapc, uint64_t journal_tid)
      : RequestBase(object_no, object_off), snapc(snapc),
        journal_tid(journal_tid) {
    }
  };

  struct DiscardRequest : public WriteRequestBase {
    uint64_t object_len;
    int discard_flags;

    DiscardRequest(uint64_t object_no, uint64_t object_off, uint64_t object_len,
                   int discard_flags, const ::SnapContext& snapc,
                   uint64_t journal_tid)
      : WriteRequestBase(object_no, object_off, snapc, journal_tid),
        object_len(object_len), discard_flags(discard_flags) {
    }
  };

  struct WriteRequest : public WriteRequestBase {
    ceph::bufferlist data;

    WriteRequest(uint64_t object_no, uint64_t object_off,
                 ceph::bufferlist&& data, const ::SnapContext& snapc,
                 uint64_t journal_tid)
      : WriteRequestBase(object_no, object_off, snapc, journal_tid),
        data(std::move(data)) {
    }
  };

  struct WriteSameRequest : public WriteRequestBase {
    uint64_t object_len;
    LightweightBufferExtents buffer_extents;
    ceph::bufferlist data;

    WriteSameRequest(uint64_t object_no, uint64_t object_off,
                     uint64_t object_len,
                     LightweightBufferExtents&& buffer_extents,
                     ceph::bufferlist&& data, const ::SnapContext& snapc,
                     uint64_t journal_tid)
    : WriteRequestBase(object_no, object_off, snapc, journal_tid),
      object_len(object_len), buffer_extents(std::move(buffer_extents)),
      data(std::move(data)) {
    }
  };

  struct CompareAndWriteRequest : public WriteRequestBase {
    ceph::bufferlist cmp_data;
    ceph::bufferlist data;
    uint64_t* mismatch_offset;

    CompareAndWriteRequest(uint64_t object_no, uint64_t object_off,
                           ceph::bufferlist&& cmp_data, ceph::bufferlist&& data,
                           uint64_t* mismatch_offset,
                           const ::SnapContext& snapc, uint64_t journal_tid)
      : WriteRequestBase(object_no, object_off, snapc, journal_tid),
        cmp_data(std::move(cmp_data)), data(std::move(data)),
        mismatch_offset(mismatch_offset) {
    }
  };

  struct FlushRequest {
    FlushSource flush_source;
    uint64_t journal_tid;

    FlushRequest(FlushSource flush_source, uint64_t journal_tid)
      : flush_source(flush_source), journal_tid(journal_tid) {
    }
  };

  typedef boost::variant<ReadRequest,
                         DiscardRequest,
                         WriteRequest,
                         WriteSameRequest,
                         CompareAndWriteRequest,
                         FlushRequest> Request;

  C_Dispatcher dispatcher_ctx;

  ObjectDispatcherInterface* object_dispatcher;
  ObjectDispatchLayer dispatch_layer;
  int object_dispatch_flags = 0;
  DispatchResult dispatch_result = DISPATCH_RESULT_INVALID;

  Request request;
  int op_flags;
  ZTracer::Trace parent_trace;

  template <typename ImageCtxT>
  static ObjectDispatchSpec* create_read(
      ImageCtxT* image_ctx, ObjectDispatchLayer object_dispatch_layer,
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      librados::snap_t snap_id, int op_flags,
      const ZTracer::Trace &parent_trace, ceph::bufferlist* read_data,
      Extents* extent_map, Context* on_finish) {
    return new ObjectDispatchSpec(image_ctx->io_object_dispatcher,
                                  object_dispatch_layer,
                                  ReadRequest{object_no, object_off,
                                              object_len, snap_id, read_data,
                                              extent_map},
                                  op_flags, parent_trace, on_finish);
  }

  template <typename ImageCtxT>
  static ObjectDispatchSpec* create_discard(
      ImageCtxT* image_ctx, ObjectDispatchLayer object_dispatch_layer,
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      const ::SnapContext &snapc, int discard_flags, uint64_t journal_tid,
      const ZTracer::Trace &parent_trace, Context *on_finish) {
    return new ObjectDispatchSpec(image_ctx->io_object_dispatcher,
                                  object_dispatch_layer,
                                  DiscardRequest{object_no, object_off,
                                                 object_len, discard_flags,
                                                 snapc, journal_tid},
                                  0, parent_trace, on_finish);
  }

  template <typename ImageCtxT>
  static ObjectDispatchSpec* create_write(
      ImageCtxT* image_ctx, ObjectDispatchLayer object_dispatch_layer,
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
      const ::SnapContext &snapc, int op_flags, uint64_t journal_tid,
      const ZTracer::Trace &parent_trace, Context *on_finish) {
    return new ObjectDispatchSpec(image_ctx->io_object_dispatcher,
                                  object_dispatch_layer,
                                  WriteRequest{object_no, object_off,
                                               std::move(data), snapc,
                                               journal_tid},
                                  op_flags, parent_trace, on_finish);
  }

  template <typename ImageCtxT>
  static ObjectDispatchSpec* create_write_same(
      ImageCtxT* image_ctx, ObjectDispatchLayer object_dispatch_layer,
      uint64_t object_no, uint64_t object_off, uint64_t object_len,
      LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
      const ::SnapContext &snapc, int op_flags, uint64_t journal_tid,
      const ZTracer::Trace &parent_trace, Context *on_finish) {
    return new ObjectDispatchSpec(image_ctx->io_object_dispatcher,
                                  object_dispatch_layer,
                                  WriteSameRequest{object_no, object_off,
                                                   object_len,
                                                   std::move(buffer_extents),
                                                   std::move(data), snapc,
                                                   journal_tid},
                                  op_flags, parent_trace, on_finish);
  }

  template <typename ImageCtxT>
  static ObjectDispatchSpec* create_compare_and_write(
      ImageCtxT* image_ctx, ObjectDispatchLayer object_dispatch_layer,
      uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
      ceph::bufferlist&& write_data, const ::SnapContext &snapc,
      uint64_t *mismatch_offset, int op_flags, uint64_t journal_tid,
      const ZTracer::Trace &parent_trace, Context *on_finish) {
    return new ObjectDispatchSpec(image_ctx->io_object_dispatcher,
                                  object_dispatch_layer,
                                  CompareAndWriteRequest{object_no,
                                                         object_off,
                                                         std::move(cmp_data),
                                                         std::move(write_data),
                                                         mismatch_offset,
                                                         snapc, journal_tid},
                                  op_flags, parent_trace, on_finish);
  }

  template <typename ImageCtxT>
  static ObjectDispatchSpec* create_flush(
      ImageCtxT* image_ctx, ObjectDispatchLayer object_dispatch_layer,
      FlushSource flush_source, uint64_t journal_tid,
      const ZTracer::Trace &parent_trace, Context *on_finish) {
    return new ObjectDispatchSpec(image_ctx->io_object_dispatcher,
                                  object_dispatch_layer,
                                  FlushRequest{flush_source, journal_tid}, 0,
                                  parent_trace, on_finish);
  }

  void send();
  void fail(int r);

private:
  template <typename> friend class ObjectDispatcher;

  ObjectDispatchSpec(ObjectDispatcherInterface* object_dispatcher,
                     ObjectDispatchLayer object_dispatch_layer,
                     Request&& request, int op_flags,
                     const ZTracer::Trace& parent_trace, Context* on_finish)
    : dispatcher_ctx(this, on_finish), object_dispatcher(object_dispatcher),
      dispatch_layer(object_dispatch_layer), request(std::move(request)),
      op_flags(op_flags), parent_trace(parent_trace) {
  }

};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_OBJECT_DISPATCH_SPEC_H
