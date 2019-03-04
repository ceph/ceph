// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/ValidatePoolRequest.h"
#include "include/rados/librados.hpp"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::ValidatePoolRequest: " \
                           << __func__ << ": "

namespace librbd {
namespace image {

namespace {

const std::string OVERWRITE_VALIDATED("overwrite validated");
const std::string VALIDATE("validate");

} // anonymous namespace

using util::create_rados_callback;
using util::create_context_callback;
using util::create_async_context_callback;

template <typename I>
ValidatePoolRequest<I>::ValidatePoolRequest(librados::IoCtx& io_ctx,
                                            ContextWQ *op_work_queue,
                                            Context *on_finish)
    : m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_op_work_queue(op_work_queue), m_on_finish(on_finish) {
    // validatation should occur in default namespace
    m_io_ctx.dup(io_ctx);
    m_io_ctx.set_namespace("");
  }

template <typename I>
void ValidatePoolRequest<I>::send() {
  read_rbd_info();
}

template <typename I>
void ValidatePoolRequest<I>::read_rbd_info() {
  ldout(m_cct, 5) << dendl;

  auto comp = create_rados_callback<
    ValidatePoolRequest<I>,
    &ValidatePoolRequest<I>::handle_read_rbd_info>(this);

  librados::ObjectReadOperation op;
  op.read(0, 0, nullptr, nullptr);

  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(RBD_INFO, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ValidatePoolRequest<I>::handle_read_rbd_info(int r) {
  ldout(m_cct, 5) << "r=" << r << dendl;

  if (r >= 0) {
    bufferlist validated_bl;
    validated_bl.append(OVERWRITE_VALIDATED);

    bufferlist validate_bl;
    validate_bl.append(VALIDATE);

    if (m_out_bl.contents_equal(validated_bl)) {
      // already validated pool
      finish(0);
      return;
    } else if (m_out_bl.contents_equal(validate_bl)) {
      // implies snapshot was already successfully created
      overwrite_rbd_info();
      return;
    }
  } else if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to read RBD info: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  create_snapshot();
}

template <typename I>
void ValidatePoolRequest<I>::create_snapshot() {
  ldout(m_cct, 5) << dendl;

  // allocate a self-managed snapshot id if this a new pool to force
  // self-managed snapshot mode
  auto ctx = new FunctionContext([this](int r) {
      r = m_io_ctx.selfmanaged_snap_create(&m_snap_id);
      handle_create_snapshot(r);
    });
  m_op_work_queue->queue(ctx, 0);
}

template <typename I>
void ValidatePoolRequest<I>::handle_create_snapshot(int r) {
  ldout(m_cct, 5) << "r=" << r << dendl;

  if (r == -EINVAL) {
    lderr(m_cct) << "pool not configured for self-managed RBD snapshot support"
                 << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to allocate self-managed snapshot: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  write_rbd_info();
}

template <typename I>
void ValidatePoolRequest<I>::write_rbd_info() {
  ldout(m_cct, 5) << dendl;

  bufferlist bl;
  bl.append(VALIDATE);

  librados::ObjectWriteOperation op;
  op.create(true);
  op.write(0, bl);

  auto comp = create_rados_callback<
    ValidatePoolRequest<I>,
    &ValidatePoolRequest<I>::handle_write_rbd_info>(this);
  int r = m_io_ctx.aio_operate(RBD_INFO, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ValidatePoolRequest<I>::handle_write_rbd_info(int r) {
  ldout(m_cct, 5) << "r=" << r << dendl;

  if (r == -EOPNOTSUPP) {
    lderr(m_cct) << "pool missing required overwrite support" << dendl;
    m_ret_val = -EINVAL;
  } else if (r < 0 && r != -EEXIST) {
    lderr(m_cct) << "failed to write RBD info: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
  }

  remove_snapshot();
}

template <typename I>
void ValidatePoolRequest<I>::remove_snapshot() {
  ldout(m_cct, 5) << dendl;

  auto ctx = new FunctionContext([this](int r) {
      r = m_io_ctx.selfmanaged_snap_remove(m_snap_id);
      handle_remove_snapshot(r);
    });
  m_op_work_queue->queue(ctx, 0);
}

template <typename I>
void ValidatePoolRequest<I>::handle_remove_snapshot(int r) {
  ldout(m_cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    // not a fatal error
    lderr(m_cct) << "failed to remove validation snapshot: " << cpp_strerror(r)
                 << dendl;
  }

  if (m_ret_val < 0) {
    finish(m_ret_val);
    return;
  }

  overwrite_rbd_info();
}

template <typename I>
void ValidatePoolRequest<I>::overwrite_rbd_info() {
  ldout(m_cct, 5) << dendl;

  bufferlist bl;
  bl.append(OVERWRITE_VALIDATED);

  librados::ObjectWriteOperation op;
  op.write(0, bl);

  auto comp = create_rados_callback<
    ValidatePoolRequest<I>,
    &ValidatePoolRequest<I>::handle_overwrite_rbd_info>(this);
  int r = m_io_ctx.aio_operate(RBD_INFO, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ValidatePoolRequest<I>::handle_overwrite_rbd_info(int r) {
  ldout(m_cct, 5) << "r=" << r << dendl;

  if (r == -EOPNOTSUPP) {
    lderr(m_cct) << "pool missing required overwrite support" << dendl;
    finish(-EINVAL);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to validate overwrite support: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void ValidatePoolRequest<I>::finish(int r) {
  ldout(m_cct, 5) << "r=" << r << dendl;
  m_on_finish->complete(r);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::ValidatePoolRequest<librbd::ImageCtx>;
