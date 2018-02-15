// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ObjectDispatcher.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectRequest.h"
#include <boost/variant.hpp>

namespace librbd {
namespace io {

// TODO temporary implementation
template <typename I>
struct ObjectDispatcher<I>::SendVisitor
  : public boost::static_visitor<void> {
  ObjectDispatcher* object_dispatcher;
  ObjectDispatchSpec* object_dispatch_spec;

  SendVisitor(ObjectDispatcher* object_dispatcher,
              ObjectDispatchSpec* object_dispatch_spec)
    : object_dispatcher(object_dispatcher),
      object_dispatch_spec(object_dispatch_spec)  {
  }

  void operator()(ObjectDispatchSpec::ReadRequest& read) const {
    auto req = ObjectReadRequest<I>::create(
      object_dispatcher->m_image_ctx, read.oid, read.object_no, read.object_off,
      read.object_len, read.snap_id, object_dispatch_spec->op_flags, false,
      object_dispatch_spec->parent_trace, read.read_data, read.extent_map,
      &object_dispatch_spec->dispatcher_ctx);
    req->send();
  }

  void operator()(ObjectDispatchSpec::DiscardRequest& discard) const {
    auto req = ObjectRequest<I>::create_discard(
      object_dispatcher->m_image_ctx, discard.oid, discard.object_no,
      discard.object_off, discard.object_len, discard.snapc, true, true,
      object_dispatch_spec->parent_trace,
      &object_dispatch_spec->dispatcher_ctx);
    req->send();
  }

  void operator()(ObjectDispatchSpec::WriteRequest& write) const {
    auto req = ObjectRequest<I>::create_write(
      object_dispatcher->m_image_ctx, write.oid, write.object_no,
      write.object_off, std::move(write.data), write.snapc,
      object_dispatch_spec->op_flags, object_dispatch_spec->parent_trace,
      &object_dispatch_spec->dispatcher_ctx);
    req->send();
  }

  void operator()(ObjectDispatchSpec::WriteSameRequest& write_same) const {
    auto req = ObjectRequest<I>::create_write_same(
      object_dispatcher->m_image_ctx, write_same.oid, write_same.object_no,
      write_same.object_off, write_same.object_len, std::move(write_same.data),
      write_same.snapc, object_dispatch_spec->op_flags,
      object_dispatch_spec->parent_trace,
      &object_dispatch_spec->dispatcher_ctx);
    req->send();
  }

  void operator()(
      ObjectDispatchSpec::CompareAndWriteRequest& compare_and_write) const {
    auto req = ObjectRequest<I>::create_compare_and_write(
      object_dispatcher->m_image_ctx, compare_and_write.oid,
      compare_and_write.object_no, compare_and_write.object_off,
      std::move(compare_and_write.cmp_data), std::move(compare_and_write.data),
      compare_and_write.snapc, compare_and_write.mismatch_offset,
      object_dispatch_spec->op_flags, object_dispatch_spec->parent_trace,
      &object_dispatch_spec->dispatcher_ctx);
    req->send();
  }

  void operator()(
      ObjectDispatchSpec::FlushRequest& flush) const {
  }
};

template <typename I>
void ObjectDispatcher<I>::send(ObjectDispatchSpec* object_dispatch_spec) {
  // TODO temporary implementation
  object_dispatch_spec->dispatch_result = DISPATCH_RESULT_COMPLETE;
  boost::apply_visitor(SendVisitor{this, object_dispatch_spec},
                       object_dispatch_spec->request);
}

} // namespace io
} // namespace librbd

template class librbd::io::ObjectDispatcher<librbd::ImageCtx>;
