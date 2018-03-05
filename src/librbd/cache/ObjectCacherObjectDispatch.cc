// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/ObjectCacherObjectDispatch.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/LibrbdWriteback.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/ObjectDispatcher.h"
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
    assert(dispatcher->m_cache_lock.is_locked());
    auto cct = dispatcher->m_image_ctx->cct;

    if (r == -EBLACKLISTED) {
      lderr(cct) << "blacklisted during flush (purging)" << dendl;
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
    I* image_ctx)
  : m_image_ctx(image_ctx),
    m_cache_lock(util::unique_lock_name(
      "librbd::cache::ObjectCacherObjectDispatch::cache_lock", this)) {
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

  m_cache_lock.Lock();
  ldout(cct, 5) << "enabling caching..." << dendl;
  m_writeback_handler = new LibrbdWriteback(m_image_ctx, m_cache_lock);

  uint64_t init_max_dirty = m_image_ctx->cache_max_dirty;
  if (m_image_ctx->cache_writethrough_until_flush) {
    init_max_dirty = 0;
  }

  ldout(cct, 5) << "Initial cache settings:"
                << " size=" << m_image_ctx->cache_size
                << " num_objects=" << 10
                << " max_dirty=" << init_max_dirty
                << " target_dirty=" << m_image_ctx->cache_target_dirty
                << " max_dirty_age="
                << m_image_ctx->cache_max_dirty_age << dendl;

  m_object_cacher = new ObjectCacher(cct, m_image_ctx->perfcounter->get_name(),
                                     *m_writeback_handler, m_cache_lock,
                                     nullptr, nullptr, m_image_ctx->cache_size,
    			             10,  /* reset this in init */
    			             init_max_dirty,
    			             m_image_ctx->cache_target_dirty,
    			             m_image_ctx->cache_max_dirty_age,
                                     m_image_ctx->cache_block_writes_upfront);

  // size object cache appropriately
  uint64_t obj = m_image_ctx->cache_max_dirty_object;
  if (!obj) {
    obj = std::min<uint64_t>(2000,
                             std::max<uint64_t>(
                               10, m_image_ctx->cache_size / 100 /
                                 sizeof(ObjectCacher::Object)));
  }
  ldout(cct, 5) << " cache bytes " << m_image_ctx->cache_size
                << " -> about " << obj << " objects" << dendl;
  m_object_cacher->set_max_objects(obj);

  m_object_set = new ObjectCacher::ObjectSet(nullptr,
                                             m_image_ctx->data_ctx.get_id(), 0);
  m_object_set->return_enoent = true;
  m_object_cacher->start();
  m_cache_lock.Unlock();

  // add ourself to the IO object dispatcher chain
  m_image_ctx->io_object_dispatcher->register_object_dispatch(this);
}

template <typename I>
void ObjectCacherObjectDispatch<I>::shut_down(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  // chain shut down in reverse order

  // shut down the cache
  on_finish = new FunctionContext([this, on_finish](int r) {
      m_object_cacher->stop();
      on_finish->complete(r);
    });

  // ensure we aren't holding the cache lock post-flush
  on_finish = util::create_async_context_callback(*m_image_ctx, on_finish);

  // invalidate any remaining cache entries
  on_finish = new C_InvalidateCache(this, true, on_finish);

  // flush all pending writeback state
  m_cache_lock.Lock();
  m_object_cacher->release_set(m_object_set);
  m_object_cacher->flush_set(m_object_set, on_finish);
  m_cache_lock.Unlock();
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::read(
    const std::string &oid, uint64_t object_no, uint64_t object_off,
    uint64_t object_len, librados::snap_t snap_id, int op_flags,
    const ZTracer::Trace &parent_trace, ceph::bufferlist* read_data,
    io::ExtentMap* extent_map, int* object_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  // IO chained in reverse order
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << object_len << dendl;

  // ensure we aren't holding the cache lock post-read
  on_dispatched = util::create_async_context_callback(*m_image_ctx,
                                                      on_dispatched);

  m_image_ctx->snap_lock.get_read();
  auto rd = m_object_cacher->prepare_read(snap_id, read_data, op_flags);
  m_image_ctx->snap_lock.put_read();

  ObjectExtent extent(oid, object_no, object_off, object_len, 0);
  extent.oloc.pool = m_image_ctx->data_ctx.get_id();
  extent.buffer_extents.push_back({0, object_len});
  rd->extents.push_back(extent);

  ZTracer::Trace trace(parent_trace);
  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;

  m_cache_lock.Lock();
  int r = m_object_cacher->readx(rd, m_object_set, on_dispatched, &trace);
  m_cache_lock.Unlock();
  if (r != 0) {
    on_dispatched->complete(r);
  }
  return true;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::discard(
    const std::string &oid, uint64_t object_no, uint64_t object_off,
    uint64_t object_len, const ::SnapContext &snapc, int discard_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << object_len << dendl;

  // discard the cache state after changes are committed to disk
  auto ctx = *on_finish;
  *on_finish = new FunctionContext(
    [this, oid, object_no, object_off, object_len, ctx](int r) {
      ObjectExtents object_extents;
      object_extents.emplace_back(oid, object_no, object_off, object_len, 0);

      m_cache_lock.Lock();
      m_object_cacher->discard_set(m_object_set, object_extents);
      m_cache_lock.Unlock();

      ctx->complete(r);
    });

  // pass-through the discard request since ObjectCacher won't
  // writeback discards.
  return false;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::write(
    const std::string &oid, uint64_t object_no, uint64_t object_off,
    ceph::bufferlist&& data, const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << data.length() << dendl;

  // ensure we aren't holding the cache lock post-write
  on_dispatched = util::create_async_context_callback(*m_image_ctx,
                                                      on_dispatched);

  m_image_ctx->snap_lock.get_read();
  ObjectCacher::OSDWrite *wr = m_object_cacher->prepare_write(
    snapc, data, ceph::real_time::min(), op_flags, *journal_tid);
  m_image_ctx->snap_lock.put_read();

  ObjectExtent extent(oid, 0, object_off, data.length(), 0);
  extent.oloc.pool = m_image_ctx->data_ctx.get_id();
  extent.buffer_extents.push_back({0, data.length()});
  wr->extents.push_back(extent);

  ZTracer::Trace trace(parent_trace);
  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;

  m_cache_lock.Lock();
  m_object_cacher->writex(wr, m_object_set, on_dispatched, &trace);
  m_cache_lock.Unlock();
  return true;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::write_same(
    const std::string &oid, uint64_t object_no, uint64_t object_off,
    uint64_t object_len, io::Extents&& buffer_extents, ceph::bufferlist&& data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, int* object_dispatch_flags,
    uint64_t* journal_tid, io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << object_len << dendl;

  // ObjectCacher doesn't support write-same so convert to regular write
  ObjectExtent extent(oid, 0, object_off, object_len, 0);
  extent.buffer_extents = std::move(buffer_extents);

  bufferlist ws_data;
  io::util::assemble_write_same_extent(extent, data, &ws_data, true);

  return write(oid, object_no, object_off, std::move(ws_data), snapc,
               op_flags, parent_trace, object_dispatch_flags, journal_tid,
               dispatch_result, on_finish, on_dispatched);
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::compare_and_write(
    const std::string &oid, uint64_t object_no, uint64_t object_off,
    ceph::bufferlist&& cmp_data, ceph::bufferlist&& write_data,
    const ::SnapContext &snapc, int op_flags,
    const ZTracer::Trace &parent_trace, uint64_t* mismatch_offset,
    int* object_dispatch_flags, uint64_t* journal_tid,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_no=" << object_no << " " << object_off << "~"
                 << cmp_data.length() << dendl;

  // pass-through the compare-and-write request since it's not a supported
  // operation of the ObjectCacher

  // ensure we aren't holding the cache lock post-flush
  on_dispatched = util::create_async_context_callback(*m_image_ctx,
                                                      on_dispatched);

  // flush any pending writes from the cache
  ZTracer::Trace trace(parent_trace);
  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;

  ObjectExtents object_extents;
  object_extents.emplace_back(oid, object_no, object_off, cmp_data.length(),
                              0);

  Mutex::Locker cache_locker(m_cache_lock);
  m_object_cacher->flush_set(m_object_set, object_extents, &trace,
                             on_dispatched);
  return true;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::flush(
    io::FlushSource flush_source, const ZTracer::Trace &parent_trace,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  // ensure we aren't holding the cache lock post-flush
  on_dispatched = util::create_async_context_callback(*m_image_ctx,
                                                      on_dispatched);

  m_cache_lock.Lock();
  if (flush_source == io::FLUSH_SOURCE_USER && !m_user_flushed &&
      m_image_ctx->cache_writethrough_until_flush &&
      m_image_ctx->cache_max_dirty > 0) {
    m_user_flushed = true;
    m_object_cacher->set_max_dirty(m_image_ctx->cache_max_dirty);
    ldout(cct, 5) << "saw first user flush, enabling writeback" << dendl;
  }

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  m_object_cacher->flush_set(m_object_set, on_dispatched);
  m_cache_lock.Unlock();
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

  m_cache_lock.Lock();
  m_object_cacher->release_set(m_object_set);
  m_object_cacher->flush_set(m_object_set, on_finish);
  m_cache_lock.Unlock();
  return true;
}

template <typename I>
bool ObjectCacherObjectDispatch<I>::reset_existence_cache(
    Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  m_cache_lock.Lock();
  m_object_cacher->clear_nonexistence(m_object_set);
  m_cache_lock.Unlock();

  return false;
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ObjectCacherObjectDispatch<librbd::ImageCtx>;
