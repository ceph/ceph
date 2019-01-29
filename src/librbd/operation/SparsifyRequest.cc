// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SparsifyRequest.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Types.h"
#include "librbd/io/ObjectRequest.h"
#include "osdc/Striper.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd

namespace librbd {
namespace operation {

using util::create_context_callback;
using util::create_rados_callback;

#undef dout_prefix
#define dout_prefix *_dout << "librbd::operation::SparsifyObject: " << this \
                           << " " << m_oid << " " << __func__ << ": "

template <typename I>
class C_SparsifyObject : public C_AsyncObjectThrottle<I> {
public:

  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v                    (object map disabled)
   * SPARSIFY -----------------------\
   *    |                            |
   *    | (object map enabled)       |
   *    v                            |
   * PRE UPDATE OBJECT MAP           |
   *    |                            |
   *    v                            |
   * CHECK EXISTS                    |
   *    |                            |
   *    v                            |
   * POST UPDATE OBJECT MAP          |
   *    |                            |
   *    v                            |
   * <finish> <----------------------/
   *
   * @endverbatim
   *
   */

  C_SparsifyObject(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                   uint64_t object_no, size_t sparse_size)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_cct(image_ctx->cct),
      m_object_no(object_no), m_sparse_size(sparse_size),
      m_oid(image_ctx->get_object_name(object_no)) {
  }

  int send() override {
    I &image_ctx = this->m_image_ctx;
    ceph_assert(image_ctx.owner_lock.is_locked());

    ldout(m_cct, 20) << dendl;

    if (image_ctx.exclusive_lock != nullptr &&
        !image_ctx.exclusive_lock->is_lock_owner()) {
      ldout(m_cct, 1) << "lost exclusive lock during sparsify" << dendl;
      return -ERESTART;
    }

    {
      RWLock::RLocker snap_locker(image_ctx.snap_lock);
      if (image_ctx.object_map != nullptr &&
          !image_ctx.object_map->object_may_exist(m_object_no)) {
        // can skip because the object does not exist
        return 1;
      }

      RWLock::RLocker parent_locker(image_ctx.parent_lock);
      uint64_t overlap_objects = 0;
      uint64_t overlap;
      int r = image_ctx.get_parent_overlap(CEPH_NOSNAP, &overlap);
      if (r == 0 && overlap > 0) {
        overlap_objects = Striper::get_num_objects(image_ctx.layout, overlap);
      }
      m_remove_empty = (m_object_no >= overlap_objects);
    }

    send_sparsify();
    return 0;
  }

  void send_sparsify() {
    I &image_ctx = this->m_image_ctx;
    ldout(m_cct, 20) << dendl;

    librados::ObjectWriteOperation op;
    cls_client::sparsify(&op, m_sparse_size, m_remove_empty);
    auto comp = create_rados_callback<
      C_SparsifyObject, &C_SparsifyObject::handle_sparsify>(this);
    int r = image_ctx.data_ctx.aio_operate(m_oid, comp, &op);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_sparsify(int r) {
    ldout(m_cct, 20) << "r=" << r << dendl;

    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "failed to sparsify: " << cpp_strerror(r) << dendl;
    }

    if (r == -ENOENT) {
      this->complete(0);
      return;
    }

    send_pre_update_object_map();
  }

  void send_pre_update_object_map() {
    I *image_ctx = &this->m_image_ctx;

    if (!m_remove_empty || !image_ctx->test_features(RBD_FEATURE_OBJECT_MAP)) {
      this->complete(0);
      return;
    }

    ldout(m_cct, 20) << dendl;

    image_ctx->owner_lock.get_read();
    image_ctx->snap_lock.get_read();
    if (image_ctx->object_map == nullptr) {
      // possible that exclusive lock was lost in background
      lderr(m_cct) << "object map is not initialized" << dendl;

      image_ctx->snap_lock.put_read();
      image_ctx->owner_lock.put_read();
      this->complete(-EINVAL);
      return;
    }

    int r;
    m_finish_op_ctx = image_ctx->exclusive_lock->start_op(&r);
    if (m_finish_op_ctx == nullptr) {
      lderr(m_cct) << "lost exclusive lock" << dendl;
      image_ctx->snap_lock.put_read();
      image_ctx->owner_lock.put_read();
      this->complete(r);
      return;
    }

    auto ctx = create_context_callback<
      C_SparsifyObject<I>,
      &C_SparsifyObject<I>::handle_pre_update_object_map>(this);

    image_ctx->object_map_lock.get_write();
    bool sent = image_ctx->object_map->template aio_update<
      Context, &Context::complete>(CEPH_NOSNAP, m_object_no, OBJECT_PENDING,
                                   OBJECT_EXISTS, {}, false, ctx);

    // NOTE: state machine might complete before we reach here
    image_ctx->object_map_lock.put_write();
    image_ctx->snap_lock.put_read();
    image_ctx->owner_lock.put_read();
    if (!sent) {
      ctx->complete(0);
    }
  }

  void handle_pre_update_object_map(int r) {
    ldout(m_cct, 20) << "r=" << r << dendl;

    if (r < 0) {
      lderr(m_cct) << "failed to update object map: " << cpp_strerror(r)
                   << dendl;
      finish_op(r);
      return;
    }

    send_check_exists();
  }

  void send_check_exists() {
    I &image_ctx = this->m_image_ctx;

    ldout(m_cct, 20) << dendl;

    librados::ObjectReadOperation op;
    op.stat(NULL, NULL, NULL);
    m_out_bl.clear();
    auto comp = create_rados_callback<
      C_SparsifyObject, &C_SparsifyObject::handle_check_exists>(this);
    int r = image_ctx.data_ctx.aio_operate(m_oid, comp, &op, &m_out_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_check_exists(int r) {
    ldout(m_cct, 20) << "r=" << r << dendl;

    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "stat failed: " << cpp_strerror(r) << dendl;
      finish_op(r);
      return;
    }

    send_post_update_object_map(r == 0);
  }

  void send_post_update_object_map(bool exists) {
    I &image_ctx = this->m_image_ctx;

    auto ctx = create_context_callback<
      C_SparsifyObject<I>,
      &C_SparsifyObject<I>::handle_post_update_object_map>(this);
    bool sent;
    {
      RWLock::RLocker owner_locker(image_ctx.owner_lock);
      RWLock::RLocker snap_locker(image_ctx.snap_lock);

      assert(image_ctx.exclusive_lock->is_lock_owner());
      assert(image_ctx.object_map != nullptr);

      RWLock::WLocker object_map_locker(image_ctx.object_map_lock);

      sent = image_ctx.object_map->template aio_update<
        Context, &Context::complete>(CEPH_NOSNAP, m_object_no,
                                     exists ? OBJECT_EXISTS : OBJECT_NONEXISTENT,
                                     OBJECT_PENDING, {}, false, ctx);
    }
    if (!sent) {
      ctx->complete(0);
    }
  }

  void handle_post_update_object_map(int r) {
    ldout(m_cct, 20) << "r=" << r << dendl;

    if (r < 0) {
      lderr(m_cct) << "failed to update object map: " << cpp_strerror(r)
                   << dendl;
      finish_op(r);
      return;
    }

    finish_op(0);
  }

  void finish_op(int r) {
    ldout(m_cct, 20) << "r=" << r << dendl;

    m_finish_op_ctx->complete(0);
    this->complete(r);
  }

private:
  CephContext *m_cct;
  uint64_t m_object_no;
  size_t m_sparse_size;
  std::string m_oid;

  bool m_remove_empty = false;
  bufferlist m_out_bl;
  Context *m_finish_op_ctx = nullptr;
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::operation::SparsifyRequest: " << this \
                           << " " << __func__ << ": "

template <typename I>
bool SparsifyRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void SparsifyRequest<I>::send_op() {
  sparsify_objects();
}

template <typename I>
void SparsifyRequest<I>::sparsify_objects() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  assert(image_ctx.owner_lock.is_locked());

  uint64_t objects = 0;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    objects = image_ctx.get_object_count(CEPH_NOSNAP);
  }

  auto ctx = create_context_callback<
    SparsifyRequest<I>,
    &SparsifyRequest<I>::handle_sparsify_objects>(this);
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_SparsifyObject<I> >(),
      boost::lambda::_1, &image_ctx, boost::lambda::_2, m_sparse_size));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, image_ctx, context_factory, ctx, &m_prog_ctx, 0, objects);
  throttle->start_ops(
    image_ctx.config.template get_val<uint64_t>("rbd_concurrent_management_ops"));
}

template <typename I>
void SparsifyRequest<I>::handle_sparsify_objects(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r == -ERESTART) {
    ldout(cct, 5) << "sparsify operation interrupted" << dendl;
    this->complete(r);
    return;
  } else if (r < 0) {
    lderr(cct) << "sparsify encountered an error: " << cpp_strerror(r) << dendl;
    this->complete(r);
    return;
  }

  this->complete(0);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::SparsifyRequest<librbd::ImageCtx>;
