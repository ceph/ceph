// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/ObjectCacherObjectDispatch.h"
#include "include/neorados/RADOS.hpp"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/cache/ObjectCacherWriteback.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Types.h"
#include "librbd/io/Utils.h"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ObjectCacherObjectDispatch: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace cache {

using librbd::util::data_object_name;

namespace {

typedef std::vector<ObjectExtent> ObjectExtents;

} // anonymous namespace

template <typename I>
struct ObjectCacherObjectDispatch<I>::C_InvalidateCache : public Context {
  ObjectCacherObjectDispatch* dispatcher;
  bool purge_on_error;
  Context *on_finish;

  C_InvalidateCache(ObjectCacherObjectDispatch* dispatcher,
                    bool purge_on_error, Context *on_finish)
    : dispatcher(dispatcher), purge_on_error(purge_on_error),
      on_finish(on_finish) {
  }

  void finish(int r) override {
    ceph_assert(ceph_mutex_is_locked(dispatcher->m_cache_lock));
    auto cct = dispatcher->m_image_ctx->cct;

    if (r == -EBLOCKLISTED) {
      lderr(cct) << "blocklisted during flush (purging)" << dendl;
      dispatcher->m_object_cacher->purge_set(dispatcher->m_object_set);
    } else if (r < 0 && purge_on_error) {
      lderr(cct) << "failed to invalidate cache (purging): "
                 << cpp_strerror(r) << dendl;
      dispatcher->m_object_cacher->purge_set(dispatcher->m_object_set);
    } else if (r != 0) {
      lderr(cct) << "failed to invalidate cache: " << cpp_strerror(r) << dendl;
    }

    auto unclean = dispatcher->m_object_cacher->release_set(
      dispatcher->m_object_set);
    if (unclean == 0) {
      r = 0;
    } else {
      lderr(cct) << "could not release all objects from cache: "
                 << unclean << " bytes remain" << dendl;
      if (r == 0) {
        r = -EBUSY;
      }
    }

    on_finish->complete(r);
  }
};

template <typename I>
ObjectCacherObjectDispatch<I>::ObjectCacherObjectDispatch(
    I* image_ctx, size_t max_dirty, bool writethrough_until_flush)
  : m_image_ctx(image_ctx), m_max_dirty(max_dirty),
    m_writethrough_until_flush(writethrough_until_flush),
    m_cache_lock(ceph::make_mutex(util::unique_lock_name(
      "librbd::cache::ObjectCacherObjectDispatch::cache_lock", this))) {
  ceph_assert(m_image_ctx->data_ctx.is_valid());
}

template <typename I>
ObjectCacherObjectDispatch<I>::~ObjectCacherObjectDispatch() {
  delete m_object_cacher;
  delete m_object_set;

  delete m_writeback_handler;
}

template <typename I>
void ObjectCacherObjectDispatch<I>::init() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  m_cache_lock.lock();
  ldout(cct, 5) << "enabling caching..." << dendl;
  m_writeback_handler = new ObjectCacherWriteback(m_image_ctx, m_cache_lock);

  auto init_max_dirty = m_max_dirty;
  if (m_writethrough_until_flush) {
    init_max_dirty = 0;
  }

  auto cache_size =
    m_image_ctx->config.template get_val<Option::size_t>("rbd_cache_size");
  auto target_dirty =
    m_image_ctx->config.template get_val<Option::size_t>("rbd_cache_target_dirty");
  auto max_dirty_age =
    m_image_ctx->config.template get_val<double>("rbd_cache_max_dirty_age");
  auto block_writes_upfront =
    m_image_ctx->config.template get_val<bool>("rbd_cache_block_writes_upfront");
  auto max_dirty_object =
    m_image_ctx->config.template get_val<uint64_t>("rbd_cache_max_dirty_object");

  ldout(cct, 5) << "Initial cache settings:"
                << " size=" << cache_size
                << " num_objects=" << 10
                << " max_dirty=" << init_max_dirty
                << " target_dirty=" << target_dirty
                << " max_dirty_age=" << max_dirty_age << dendl;

  m_object_cacher = new ObjectCacher(cct, m_image_ctx->perfcounter->get_name(),
                                     *m_writeback_handler, m_cache_lock,
                                     nullptr, nullptr, cache_size,
                                     10,  /* reset this in init */
                                     init_max_dirty, target_dirty,
                                     max_dirty_age, block_writes_upfront);

  // size object cache appropriately
  if (max_dirty_object == 0) {
    max_dirty_object = std::min<uint64_t>(
      2000, std::max<uint64_t>(10, cache_size / 100 /
                                 sizeof(ObjectCacher::Object)));
  }
  ldout(cct, 5) << " cache bytes " << cache_size
                << " -> about " << max_dirty_object << " objects" << dendl;
  m_object_cacher->set_max_objects(max_dirty_object);

  m_object_set = new ObjectCacher::ObjectSet(nullptr,
                                             m_image_ctx->data_ctx.get_id(), 0);
  m_object_cacher->start();
  m_cache_lock.unlock();

  // add ourself to the IO object dispatcher chain
  if (m_max_dirty > 0) {
    m_image_ctx->disable_zero_copy = true;
  }
  m_image_ctx->io_object_dispatcher->register_dispatch(this);
}

template <typename I>
void ObjectCacherObjectDispatch<I>::shut_down(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  // chain shut down in reverse order

  // shut down the cache
  on_finish = new LambdaContext([this, on_finish](int r) {
      m_object_cacher->stop();
      on_finish->complete(r);
    });

  // ensure we aren't holding the cache lock post-flush
  on_finish = util::create_async_context_callback(*m_image_ctx, on_finish);

  // invalidate any remaining cache entries
  on_finish = new C_InvalidateCache(this, true, on_finish);

  // flush all pending writeback state
  std::lock_guard locker{m_cache_lock};
  m_object_cacher->release_set(m_object_set);
  m_object_cacher->flush_set(m_object_set, on_finish);
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::read(
    uint64_t object_no, io::ReadExtents* extents, IOContext io_context,
    int op_flags, int read_flags, const ZTracer::Trace &parent_trace,
    uint64_t* version, int* object_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  // IO chained in reverse order
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << *extents << dendl;

  if (extents->size() == 0) {
    ldout(cct, 20) << "no extents to read" << dendl;
    return false;
  }

  if (version != nullptr) {
    // we currently don't cache read versions
    // and don't support reading more than one extent
    return false;
  }

  // ensure we aren't holding the cache lock post-read
  on_dispatched = util::create_async_context_callback(*m_image_ctx,
                                                      on_dispatched);

  // embed the RBD-internal read flags in the generic RADOS op_flags and
  op_flags = ((op_flags & ~ObjectCacherWriteback::READ_FLAGS_MASK) |
              ((read_flags << ObjectCacherWriteback::READ_FLAGS_SHIFT) &
               ObjectCacherWriteback::READ_FLAGS_MASK));

  ceph::bufferlist* bl;
  if (extents->size() > 1) {
    auto req = new io::ReadResult::C_ObjectReadMergedExtents(
            cct, extents, on_dispatched);
    on_dispatched = req;
    bl = &req->bl;
  } else {
    bl = &extents->front().bl;
  }

  m_image_ctx->image_lock.lock_shared();
  auto rd = m_object_cacher->prepare_read(
    io_context->get_read_snap(), bl, op_flags);
  m_image_ctx->image_lock.unlock_shared();

  uint64_t off = 0;
  for (auto& read_extent: *extents) {
    ObjectExtent extent(data_object_name(m_image_ctx, object_no), object_no,
                        read_extent.offset, read_extent.length, 0);
    extent.oloc.pool = m_image_ctx->data_ctx.get_id();
    extent.buffer_extents.push_back({off, read_extent.length});
    rd->extents.push_back(extent);
    off += read_extent.length;
  }

  ZTracer::Trace trace(parent_trace);
  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;

  m_cache_lock.lock();
  int r = m_object_cacher->readx(rd, m_object_set, on_dispatched, &trace);
  m_cache_lock.unlock();
  if (r != 0) {
    on_dispatched->complete(r);
  }
  return true;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::discard(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    IOContext io_context, int discard_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << object_len << dendl;

  ObjectExtents object_extents;
  object_extents.emplace_back(data_object_name(m_image_ctx, object_no),
                              object_no, object_off, object_len, 0);

  // discard the cache state after changes are committed to disk (and to
  // prevent races w/ readahead)
  auto ctx = *on_finish;
  *on_finish = new LambdaContext(
    [this, object_extents, ctx](int r) {
      m_cache_lock.lock();
      m_object_cacher->discard_set(m_object_set, object_extents);
      m_cache_lock.unlock();

      ctx->complete(r);
    });

  // ensure we aren't holding the cache lock post-write
  on_dispatched = util::create_async_context_callback(*m_image_ctx,
                                                      on_dispatched);

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;

  // ensure any in-flight writeback is complete before advancing
  // the discard request
  std::lock_guard locker{m_cache_lock};
  m_object_cacher->discard_writeback(m_object_set, object_extents,
                                     on_dispatched);
  return true;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& data,
    IOContext io_context, int op_flags, int write_flags,
    std::optional<uint64_t> assert_version,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << data.length() << dendl;

  // ensure we aren't holding the cache lock post-write
  on_dispatched = util::create_async_context_callback(*m_image_ctx,
                                                      on_dispatched);

  // cache layer does not handle version checking
  if (assert_version.has_value() ||
      (write_flags & io::OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE) != 0) {
    ObjectExtents object_extents;
    object_extents.emplace_back(data_object_name(m_image_ctx, object_no),
                                object_no, object_off, data.length(), 0);

    *dispatch_result = io::DISPATCH_RESULT_CONTINUE;

    // ensure any in-flight writeback is complete before advancing
    // the write request
    std::lock_guard locker{m_cache_lock};
    m_object_cacher->discard_writeback(m_object_set, object_extents,
                                       on_dispatched);
    return true;
  }

  SnapContext snapc;
  if (io_context->get_write_snap_context()) {
    auto write_snap_context = *io_context->get_write_snap_context();
    snapc = SnapContext(write_snap_context.first,
                        {write_snap_context.second.begin(),
                         write_snap_context.second.end()});
  }

  m_image_ctx->image_lock.lock_shared();
  ObjectCacher::OSDWrite *wr = m_object_cacher->prepare_write(
    snapc, data, ceph::real_clock::zero(), op_flags, *journal_tid);
  m_image_ctx->image_lock.unlock_shared();

  ObjectExtent extent(data_object_name(m_image_ctx, object_no),
                      object_no, object_off, data.length(), 0);
  extent.oloc.pool = m_image_ctx->data_ctx.get_id();
  extent.buffer_extents.push_back({0, data.length()});
  wr->extents.push_back(extent);

  ZTracer::Trace trace(parent_trace);
  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;

  std::lock_guard locker{m_cache_lock};
  m_object_cacher->writex(wr, m_object_set, on_dispatched, &trace);
  return true;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::write_same(
    uint64_t object_no, uint64_t object_off, uint64_t object_len,
    io::LightweightBufferExtents&& buffer_extents, ceph::bufferlist&& data,
    IOContext io_context, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << object_len << dendl;

  // ObjectCacher doesn't support write-same so convert to regular write
  io::LightweightObjectExtent extent(object_no, object_off, object_len, 0);
  extent.buffer_extents = std::move(buffer_extents);

  bufferlist ws_data;
  io::util::assemble_write_same_extent(extent, data, &ws_data, true);

  return write(object_no, object_off, std::move(ws_data), io_context, op_flags,
               0, std::nullopt, parent_trace, object_dispatch_flags,
               journal_tid, dispatch_result, on_finish, on_dispatched);
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::compare_and_write(
    uint64_t object_no, uint64_t object_off, ceph::bufferlist&& cmp_data,
    ceph::bufferlist&& write_data, IOContext io_context, int op_flags,
    const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
    int* object_dispatch_flags, uint64_t* journal_tid,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << cmp_data.length() << dendl;

  // pass-through the compare-and-write request since it's not a supported
  // operation of the ObjectCacher

  ObjectExtents object_extents;
  object_extents.emplace_back(data_object_name(m_image_ctx, object_no),
                              object_no, object_off, cmp_data.length(), 0);

  // if compare succeeds, discard the cache state after changes are
  // committed to disk
  auto ctx = *on_finish;
  *on_finish = new LambdaContext(
    [this, object_extents, ctx](int r) {
      // ObjectCacher doesn't provide a way to reliably invalidate
      // extents: in case of a racing read (if the bh is in RX state),
      // release_set() just returns while discard_set() populates the
      // extent with zeroes.  Neither is OK but the latter is better
      // because it is at least deterministic...
      if (r == 0) {
        m_cache_lock.lock();
        m_object_cacher->discard_set(m_object_set, object_extents);
        m_cache_lock.unlock();
      }

      ctx->complete(r);
    });

  // ensure we aren't holding the cache lock post-flush
  on_dispatched = util::create_async_context_callback(*m_image_ctx,
                                                      on_dispatched);

  // flush any pending writes from the cache before compare
  ZTracer::Trace trace(parent_trace);
  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;

  std::lock_guard cache_locker{m_cache_lock};
  m_object_cacher->flush_set(m_object_set, object_extents, &trace,
                             on_dispatched);
  return true;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::flush(
    io::FlushSource flush_source, const ZTracer::Trace &parent_trace,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  // ensure we aren't holding the cache lock post-flush
  on_dispatched = util::create_async_context_callback(*m_image_ctx,
                                                      on_dispatched);

  std::lock_guard locker{m_cache_lock};
  if (flush_source == io::FLUSH_SOURCE_USER && !m_user_flushed) {
    m_user_flushed = true;
    if (m_writethrough_until_flush && m_max_dirty > 0) {
      m_object_cacher->set_max_dirty(m_max_dirty);
      ldout(cct, 5) << "saw first user flush, enabling writeback" << dendl;
    }
  }

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  m_object_cacher->flush_set(m_object_set, on_dispatched);
  return true;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::invalidate_cache(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  // ensure we aren't holding the cache lock post-flush
  on_finish = util::create_async_context_callback(*m_image_ctx, on_finish);

  // invalidate any remaining cache entries
  on_finish = new C_InvalidateCache(this, false, on_finish);

  std::lock_guard locker{m_cache_lock};
  m_object_cacher->release_set(m_object_set);
  m_object_cacher->flush_set(m_object_set, on_finish);
  return true;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::reset_existence_cache(
    Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  std::lock_guard locker{m_cache_lock};
  m_object_cacher->clear_nonexistence(m_object_set);
  return false;
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ObjectCacherObjectDispatch<librbd::ImageCtx>;
