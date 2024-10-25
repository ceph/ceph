// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/MigrateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/deep_copy/ObjectCopyRequest.h"
#include "librbd/io/AsyncOperation.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/io/ObjectRequest.h"
#include "osdc/Striper.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::MigrateRequest: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace operation {

using util::create_context_callback;
using util::create_async_context_callback;

namespace {

template <typename I>
class C_MigrateObject : public C_AsyncObjectThrottle<I> {
public:
  C_MigrateObject(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                  IOContext io_context, uint64_t object_no)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_io_context(io_context),
      m_object_no(object_no) {
  }

  int send() override {
    I &image_ctx = this->m_image_ctx;
    ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
    CephContext *cct = image_ctx.cct;

    if (image_ctx.exclusive_lock != nullptr &&
        !image_ctx.exclusive_lock->is_lock_owner()) {
      ldout(cct, 1) << "lost exclusive lock during migrate" << dendl;
      return -ERESTART;
    }

    start_async_op();
    return 0;
  }

private:
  IOContext m_io_context;
  uint64_t m_object_no;

  io::AsyncOperation *m_async_op = nullptr;

  void start_async_op() {
    I &image_ctx = this->m_image_ctx;
    ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
    CephContext *cct = image_ctx.cct;
    ldout(cct, 10) << dendl;

    ceph_assert(m_async_op == nullptr);
    m_async_op = new io::AsyncOperation();
    m_async_op->start_op(image_ctx);

    if (!image_ctx.io_image_dispatcher->writes_blocked()) {
      migrate_object();
      return;
    }

    auto ctx = create_async_context_callback(
      image_ctx, create_context_callback<
        C_MigrateObject<I>, &C_MigrateObject<I>::handle_start_async_op>(this));
    m_async_op->finish_op();
    delete m_async_op;
    m_async_op = nullptr;
    image_ctx.io_image_dispatcher->wait_on_writes_unblocked(ctx);
  }

  void handle_start_async_op(int r) {
    I &image_ctx = this->m_image_ctx;
    CephContext *cct = image_ctx.cct;
    ldout(cct, 10) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to start async op: " << cpp_strerror(r) << dendl;
      this->complete(r);
      return;
    }

    std::shared_lock owner_locker{image_ctx.owner_lock};
    start_async_op();
  }

  bool is_within_overlap_bounds() {
    I &image_ctx = this->m_image_ctx;
    std::shared_lock image_locker{image_ctx.image_lock};

    auto overlap = std::min(image_ctx.size, image_ctx.migration_info.overlap);
    return overlap > 0 &&
      Striper::get_num_objects(image_ctx.layout, overlap) > m_object_no;
  }

  void migrate_object() {
    I &image_ctx = this->m_image_ctx;
    ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
    CephContext *cct = image_ctx.cct;

    auto ctx = create_context_callback<
      C_MigrateObject<I>, &C_MigrateObject<I>::handle_migrate_object>(this);

    if (is_within_overlap_bounds()) {
      bufferlist bl;
      auto req = new io::ObjectWriteRequest<I>(&image_ctx, m_object_no, 0,
                                               std::move(bl), m_io_context, 0,
                                               0, std::nullopt, {}, ctx);

      ldout(cct, 20) << "copyup object req " << req << ", object_no "
                     << m_object_no << dendl;

      req->send();
    } else {
      ceph_assert(image_ctx.parent != nullptr);

      uint32_t flags = deep_copy::OBJECT_COPY_REQUEST_FLAG_MIGRATION;
      if (image_ctx.migration_info.flatten) {
        flags |= deep_copy::OBJECT_COPY_REQUEST_FLAG_FLATTEN;
      }

      auto req = deep_copy::ObjectCopyRequest<I>::create(
        image_ctx.parent, &image_ctx, 0, 0, image_ctx.migration_info.snap_map,
        m_object_no, flags, nullptr, ctx);

      ldout(cct, 20) << "deep copy object req " << req << ", object_no "
                     << m_object_no << dendl;
      req->send();
    }
  }

  void handle_migrate_object(int r) {
    CephContext *cct = this->m_image_ctx.cct;
    ldout(cct, 10) << "r=" << r << dendl;

    if (r == -ENOENT) {
      r = 0;
    }

    m_async_op->finish_op();
    delete m_async_op;
    this->complete(r);
  }
};

} // anonymous namespace

template <typename I>
void MigrateRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << dendl;

  migrate_objects();
}

template <typename I>
bool MigrateRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }

  return true;
}

template <typename I>
void MigrateRequest<I>::migrate_objects() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  uint64_t overlap_objects = get_num_overlap_objects();

  ldout(cct, 10) << "from 0 to " << overlap_objects << dendl;

  auto ctx = create_context_callback<
    MigrateRequest<I>, &MigrateRequest<I>::handle_migrate_objects>(this);

  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_MigrateObject<I> >(),
      boost::lambda::_1, &image_ctx, image_ctx.get_data_io_context(),
      boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, image_ctx, context_factory, ctx, &m_prog_ctx, 0, overlap_objects);
  throttle->start_ops(
    image_ctx.config.template get_val<uint64_t>("rbd_concurrent_management_ops"));
}

template <typename I>
void MigrateRequest<I>::handle_migrate_objects(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to migrate objects: " << cpp_strerror(r) << dendl;
  }

  this->complete(r);
}

template <typename I>
uint64_t MigrateRequest<I>::get_num_overlap_objects() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << dendl;

  std::shared_lock image_locker{image_ctx.image_lock};

  auto overlap = image_ctx.migration_info.overlap;

  return overlap > 0 ?
    Striper::get_num_objects(image_ctx.layout, overlap) : 0;
}

} // namespace operation
} // namespace librbd

template class librbd::operation::MigrateRequest<librbd::ImageCtx>;
