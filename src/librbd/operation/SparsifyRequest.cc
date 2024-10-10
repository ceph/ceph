// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/SparsifyRequest.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/err.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Types.h"
#include "librbd/io/ObjectRequest.h"
#include "librbd/io/Utils.h"
#include "osdc/Striper.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd

namespace librbd {
namespace operation {

namespace {

bool may_be_trimmed(const std::map<uint64_t,uint64_t> &extent_map,
                    const bufferlist &bl, size_t sparse_size,
                    uint64_t *new_end_ptr) {
  if (extent_map.empty()) {
    *new_end_ptr = 0;
    return true;
  }

  uint64_t end = extent_map.rbegin()->first + extent_map.rbegin()->second;
  uint64_t new_end = end;
  uint64_t bl_off = bl.length();

  for (auto it = extent_map.rbegin(); it != extent_map.rend(); it++) {
    auto off = it->first;
    auto len = it->second;

    new_end = p2roundup<uint64_t>(off + len, sparse_size);

    uint64_t extent_left = len;
    uint64_t sub_len = len % sparse_size;
    if (sub_len == 0) {
      sub_len = sparse_size;
    }
    while (extent_left > 0) {
      ceph_assert(bl_off >= sub_len);
      bl_off -= sub_len;
      bufferlist sub_bl;
      sub_bl.substr_of(bl, bl_off, sub_len);
      if (!sub_bl.is_zero()) {
        break;
      }
      new_end -= sparse_size;
      extent_left -= sub_len;
      sub_len = sparse_size;
    }
    if (extent_left > 0) {
      break;
    }
  }

  if (new_end < end) {
    *new_end_ptr = new_end;
    return true;
  }

  return false;
}

} // anonymous namespace

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
   *    v      (not supported)
   * SPARSIFY * * * * * * * * * * * * > READ < * * * * * * * * * * (concurrent
   *    |                                 |                      *  update is
   *    |  (object map disabled)          | (can trim)           *  detected)
   *    |------------------------\        V                      *
   *    |                        |      PRE UPDATE OBJECT MAP    *
   *    | (object map enabled)   |        |         (if needed)  *
   *    v                        |        V                      *
   * PRE UPDATE OBJECT MAP       |      TRIM * * * * * * * * * * *
   *    |                        |        |
   *    v                        |        V
   * CHECK EXISTS                |      POST UPDATE OBJECT MAP
   *    |                        |        |          (if needed)
   *    v                        |        |
   * POST UPDATE OBJECT MAP      |        |
   *    |                        |        |
   *    v                        |        |
   * <finish> <------------------/<-------/
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
    ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

    ldout(m_cct, 20) << dendl;

    if (!image_ctx.data_ctx.is_valid()) {
      lderr(m_cct) << "missing data pool" << dendl;
      return -ENODEV;
    }

    if (image_ctx.exclusive_lock != nullptr &&
        !image_ctx.exclusive_lock->is_lock_owner()) {
      ldout(m_cct, 1) << "lost exclusive lock during sparsify" << dendl;
      return -ERESTART;
    }

    {
      std::shared_lock image_locker{image_ctx.image_lock};
      if (image_ctx.object_map != nullptr &&
          !image_ctx.object_map->object_may_exist(m_object_no)) {
        // can skip because the object does not exist
        return 1;
      }

      uint64_t raw_overlap = 0;
      uint64_t object_overlap = 0;
      int r = image_ctx.get_parent_overlap(CEPH_NOSNAP, &raw_overlap);
      ceph_assert(r == 0);
      if (raw_overlap > 0) {
        auto [parent_extents, area] = io::util::object_to_area_extents(
            &image_ctx, m_object_no, {{0, image_ctx.layout.object_size}});
        object_overlap = image_ctx.prune_parent_extents(parent_extents, area,
                                                        raw_overlap, false);
      }
      m_remove_empty = object_overlap == 0;
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

    if (r == -EOPNOTSUPP) {
      m_trying_trim = true;
      send_read();
      return;
    }

    if (r == -ENOENT) {
      finish_op(0);
      return;
    }

    if (r < 0) {
      lderr(m_cct) << "failed to sparsify: " << cpp_strerror(r) << dendl;
      finish_op(r);
      return;
    }

    send_pre_update_object_map();
  }

  void send_pre_update_object_map() {
    I &image_ctx = this->m_image_ctx;

    if (m_trying_trim) {
      if (!m_remove_empty || m_new_end != 0 ||
          !image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
        send_trim();
        return;
      }
    } else if (!m_remove_empty ||
               !image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
      finish_op(0);
      return;
    }

    ldout(m_cct, 20) << dendl;

    image_ctx.owner_lock.lock_shared();
    image_ctx.image_lock.lock_shared();
    if (image_ctx.object_map == nullptr) {
      // possible that exclusive lock was lost in background
      lderr(m_cct) << "object map is not initialized" << dendl;

      image_ctx.image_lock.unlock_shared();
      image_ctx.owner_lock.unlock_shared();
      finish_op(-EINVAL);
      return;
    }

    int r;
    m_finish_op_ctx = image_ctx.exclusive_lock->start_op(&r);
    if (m_finish_op_ctx == nullptr) {
      lderr(m_cct) << "lost exclusive lock" << dendl;
      image_ctx.image_lock.unlock_shared();
      image_ctx.owner_lock.unlock_shared();
      finish_op(r);
      return;
    }

    auto ctx = create_context_callback<
      C_SparsifyObject<I>,
      &C_SparsifyObject<I>::handle_pre_update_object_map>(this);

    bool sent = image_ctx.object_map->template aio_update<
      Context, &Context::complete>(CEPH_NOSNAP, m_object_no, OBJECT_PENDING,
                                   OBJECT_EXISTS, {}, false, false, ctx);

    // NOTE: state machine might complete before we reach here
    image_ctx.image_lock.unlock_shared();
    image_ctx.owner_lock.unlock_shared();
    if (!sent) {
      finish_op(0);
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

    if (m_trying_trim) {
      send_trim();
    } else {
      send_check_exists();
    }
  }

  void send_check_exists() {
    I &image_ctx = this->m_image_ctx;

    ldout(m_cct, 20) << dendl;

    librados::ObjectReadOperation op;
    op.stat(NULL, NULL, NULL);
    m_bl.clear();
    auto comp = create_rados_callback<
      C_SparsifyObject, &C_SparsifyObject::handle_check_exists>(this);
    int r = image_ctx.data_ctx.aio_operate(m_oid, comp, &op, &m_bl);
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

    ldout(m_cct, 20) << dendl;

    auto ctx = create_context_callback<
      C_SparsifyObject<I>,
      &C_SparsifyObject<I>::handle_post_update_object_map>(this);
    bool sent;
    {
      std::shared_lock owner_locker{image_ctx.owner_lock};
      std::shared_lock image_locker{image_ctx.image_lock};

      assert(image_ctx.exclusive_lock->is_lock_owner());
      assert(image_ctx.object_map != nullptr);

      sent = image_ctx.object_map->template aio_update<
        Context, &Context::complete>(CEPH_NOSNAP, m_object_no,
                                     exists ? OBJECT_EXISTS : OBJECT_NONEXISTENT,
                                     OBJECT_PENDING, {}, false, false, ctx);
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

  void send_read() {
    I &image_ctx = this->m_image_ctx;

    ldout(m_cct, 20) << dendl;

    librados::ObjectReadOperation op;
    m_bl.clear();
    op.sparse_read(0, image_ctx.layout.object_size, &m_extent_map, &m_bl,
                   nullptr);
    auto comp = create_rados_callback<
      C_SparsifyObject, &C_SparsifyObject::handle_read>(this);
    int r = image_ctx.data_ctx.aio_operate(m_oid, comp, &op, &m_bl);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_read(int r) {
    ldout(m_cct, 20) << "r=" << r << dendl;

    if (r < 0) {
      if (r == -ENOENT) {
        r = 0;
      } else {
        lderr(m_cct) << "failed to read object: " << cpp_strerror(r) << dendl;
      }
      finish_op(r);
      return;
    }

    if (!may_be_trimmed(m_extent_map, m_bl, m_sparse_size, &m_new_end)) {
      finish_op(0);
      return;
    }

    send_pre_update_object_map();
  }

  void send_trim() {
    I &image_ctx = this->m_image_ctx;

    ldout(m_cct, 20) << dendl;

    ceph_assert(m_new_end < image_ctx.layout.object_size);

    librados::ObjectWriteOperation op;
    m_bl.clear();
    m_bl.append_zero(image_ctx.layout.object_size - m_new_end);
    op.cmpext(m_new_end, m_bl, nullptr);
    if (m_new_end == 0 && m_remove_empty) {
      op.remove();
    } else {
      op.truncate(m_new_end);
    }

    auto comp = create_rados_callback<
      C_SparsifyObject, &C_SparsifyObject::handle_trim>(this);
    int r = image_ctx.data_ctx.aio_operate(m_oid, comp, &op);
    ceph_assert(r == 0);
    comp->release();
  }

  void handle_trim(int r) {
    I &image_ctx = this->m_image_ctx;

    ldout(m_cct, 20) << "r=" << r << dendl;

    if (r <= -MAX_ERRNO) {
      m_finish_op_ctx->complete(0);
      m_finish_op_ctx = nullptr;
      send_read();
      return;
    }

    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "failed to trim: " << cpp_strerror(r) << dendl;
      finish_op(r);
      return;
    }

    if (!m_remove_empty || m_new_end != 0 ||
        !image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
      finish_op(0);
      return;
    }

    send_post_update_object_map(false);
  }

  void finish_op(int r) {
    ldout(m_cct, 20) << "r=" << r << dendl;

    if (m_finish_op_ctx != nullptr) {
      m_finish_op_ctx->complete(0);
    }
    this->complete(r);
  }

private:
  CephContext *m_cct;
  uint64_t m_object_no;
  size_t m_sparse_size;
  std::string m_oid;

  bool m_remove_empty = false;
  bool m_trying_trim = false;
  bufferlist m_bl;
  std::map<uint64_t,uint64_t> m_extent_map;
  uint64_t m_new_end = 0;
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
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  uint64_t objects = 0;
  {
    std::shared_lock image_locker{image_ctx.image_lock};
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
