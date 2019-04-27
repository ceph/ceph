// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"
#include <boost/variant.hpp>

namespace librbd {
namespace io {

template <typename I>
struct ImageDispatchSpec<I>::SendVisitor
  : public boost::static_visitor<void> {
  ImageDispatchSpec* spec;

  explicit SendVisitor(ImageDispatchSpec* spec)
    : spec(spec) {
  }

  void operator()(Read& read) const {
    ImageRequest<I>::aio_read(
      &spec->m_image_ctx, spec->m_aio_comp, std::move(spec->m_image_extents),
      std::move(read.read_result), spec->m_op_flags, spec->m_parent_trace);
  }

  void operator()(Discard& discard) const {
    ImageRequest<I>::aio_discard(
      &spec->m_image_ctx, spec->m_aio_comp, std::move(spec->m_image_extents),
      discard.discard_granularity_bytes, spec->m_parent_trace);
  }

  void operator()(Write& write) const {
    ImageRequest<I>::aio_write(
      &spec->m_image_ctx, spec->m_aio_comp, std::move(spec->m_image_extents),
      std::move(write.bl), spec->m_op_flags, spec->m_parent_trace);
  }

  void operator()(WriteSame& write_same) const {
    ImageRequest<I>::aio_writesame(
      &spec->m_image_ctx, spec->m_aio_comp, std::move(spec->m_image_extents),
      std::move(write_same.bl), spec->m_op_flags, spec->m_parent_trace);
  }

  void operator()(CompareAndWrite& compare_and_write) const {
    ImageRequest<I>::aio_compare_and_write(
      &spec->m_image_ctx, spec->m_aio_comp, std::move(spec->m_image_extents),
      std::move(compare_and_write.cmp_bl), std::move(compare_and_write.bl),
      compare_and_write.mismatch_offset, spec->m_op_flags,
      spec->m_parent_trace);
  }

  void operator()(Flush& flush) const {
    ImageRequest<I>::aio_flush(
      &spec->m_image_ctx, spec->m_aio_comp, flush.flush_source,
      spec->m_parent_trace);
  }
};

template <typename I>
struct ImageDispatchSpec<I>::IsWriteOpVisitor
  : public boost::static_visitor<bool> {
  bool operator()(const Read&) const {
    return false;
  }

  template <typename T>
  bool operator()(const T&) const {
    return true;
  }
};

template <typename I>
struct ImageDispatchSpec<I>::TokenRequestedVisitor
  : public boost::static_visitor<uint64_t> {
  ImageDispatchSpec* spec;
  uint64_t flag;
  uint64_t *tokens;

  TokenRequestedVisitor(ImageDispatchSpec* spec, uint64_t _flag,
                        uint64_t *tokens)
    : spec(spec), flag(_flag), tokens(tokens) {
  }

  uint64_t operator()(const Read&) const {
    if (flag & RBD_QOS_WRITE_MASK) {
      *tokens = 0;
      return false;
    }

    *tokens = (flag & RBD_QOS_BPS_MASK) ? spec->extents_length() : 1;
    return true;
  }

  uint64_t operator()(const Flush&) const {
    *tokens = 0;
    return true;
  }

  template <typename T>
  uint64_t operator()(const T&) const {
    if (flag & RBD_QOS_READ_MASK) {
      *tokens = 0;
      return false;
    }

    *tokens = (flag & RBD_QOS_BPS_MASK) ? spec->extents_length() : 1;
    return true;
  }
};

template <typename I>
void ImageDispatchSpec<I>::send() {
  boost::apply_visitor(SendVisitor{this}, m_request);
}

template <typename I>
void ImageDispatchSpec<I>::fail(int r) {
  m_aio_comp->get();
  m_aio_comp->fail(r);
}

template <typename I>
uint64_t ImageDispatchSpec<I>::extents_length() {
  uint64_t length = 0;
  auto &extents = this->m_image_extents;

  for (auto &extent : extents) {
    length += extent.second;
  }
  return length;
}

template <typename I>
bool ImageDispatchSpec<I>::is_write_op() const {
  return boost::apply_visitor(IsWriteOpVisitor(), m_request);
}

template <typename I>
bool ImageDispatchSpec<I>::tokens_requested(uint64_t flag, uint64_t *tokens) {
  return boost::apply_visitor(TokenRequestedVisitor{this, flag, tokens},
                              m_request);
}

template <typename I>
void ImageDispatchSpec<I>::start_op() {
  m_aio_comp->start_op();
}

} // namespace io
} // namespace librbd

template class librbd::io::ImageDispatchSpec<librbd::ImageCtx>;
