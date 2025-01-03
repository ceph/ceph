// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/CopyupRequest.h"
#include "include/neorados/RADOS.hpp"
#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsioEngine.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/asio/Utils.h"
#include "librbd/deep_copy/ObjectCopyRequest.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/io/ObjectRequest.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Utils.h"

#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::CopyupRequest: " << this            \
                           << " " << __func__ << ": "                          \
                           << data_object_name(m_image_ctx, m_object_no) << " "

namespace librbd {
namespace io {

using librbd::util::data_object_name;

namespace {

template <typename I>
class C_UpdateObjectMap : public C_AsyncObjectThrottle<I> {
public:
  C_UpdateObjectMap(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                    uint64_t object_no, uint8_t head_object_map_state,
                    const std::vector<uint64_t> *snap_ids,
                    bool first_snap_is_clean, const ZTracer::Trace &trace,
                    size_t snap_id_idx)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_object_no(object_no),
      m_head_object_map_state(head_object_map_state), m_snap_ids(*snap_ids),
      m_first_snap_is_clean(first_snap_is_clean), m_trace(trace),
      m_snap_id_idx(snap_id_idx)
  {
  }

  int send() override {
    auto& image_ctx = this->m_image_ctx;
    ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
    if (image_ctx.exclusive_lock == nullptr) {
      return 1;
    }
    ceph_assert(image_ctx.exclusive_lock->is_lock_owner());

    std::shared_lock image_locker{image_ctx.image_lock};
    if (image_ctx.object_map == nullptr) {
      return 1;
    }

    uint64_t snap_id = m_snap_ids[m_snap_id_idx];
    if (snap_id == CEPH_NOSNAP) {
      return update_head();
    } else {
      return update_snapshot(snap_id);
    }
  }

  int update_head() {
    auto& image_ctx = this->m_image_ctx;
    ceph_assert(ceph_mutex_is_locked(image_ctx.image_lock));

    bool sent = image_ctx.object_map->template aio_update<Context>(
      CEPH_NOSNAP, m_object_no, m_head_object_map_state, {}, m_trace, false,
      this);
    return (sent ? 0 : 1);
  }

  int update_snapshot(uint64_t snap_id) {
    auto& image_ctx = this->m_image_ctx;
    ceph_assert(ceph_mutex_is_locked(image_ctx.image_lock));

    uint8_t state = OBJECT_EXISTS;
    if (image_ctx.test_features(RBD_FEATURE_FAST_DIFF, image_ctx.image_lock) &&
        (m_snap_id_idx > 0 || m_first_snap_is_clean)) {
      // first snapshot should be exists+dirty since it contains
      // the copyup data -- later snapshots inherit the data.
      state = OBJECT_EXISTS_CLEAN;
    }

    bool sent = image_ctx.object_map->template aio_update<Context>(
      snap_id, m_object_no, state, {}, m_trace, true, this);
    ceph_assert(sent);
    return 0;
  }

private:
  uint64_t m_object_no;
  uint8_t m_head_object_map_state;
  const std::vector<uint64_t> &m_snap_ids;
  bool m_first_snap_is_clean;
  const ZTracer::Trace &m_trace;
  size_t m_snap_id_idx;
};

} // anonymous namespace

template <typename I>
CopyupRequest<I>::CopyupRequest(I *ictx, uint64_t objectno,
                                Extents &&image_extents, ImageArea area,
                                const ZTracer::Trace &parent_trace)
  : m_image_ctx(ictx), m_object_no(objectno),
    m_image_extents(std::move(image_extents)), m_image_area(area),
    m_trace(librbd::util::create_trace(*m_image_ctx, "copy-up", parent_trace))
{
  ceph_assert(m_image_ctx->data_ctx.is_valid());
  m_async_op.start_op(*librbd::util::get_image_ctx(m_image_ctx));
}

template <typename I>
CopyupRequest<I>::~CopyupRequest() {
  ceph_assert(m_pending_requests.empty());
  m_async_op.finish_op();
}

template <typename I>
void CopyupRequest<I>::append_request(AbstractObjectWriteRequest<I> *req,
                                      const Extents& object_extents) {
  std::lock_guard locker{m_lock};

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "object_request=" << req << ", "
                 << "append=" << m_append_request_permitted << dendl;
  if (m_append_request_permitted) {
    m_pending_requests.push_back(req);

    for (auto [offset, length] : object_extents) {
      if (length > 0) {
        m_write_object_extents.union_insert(offset, length);
      }
    }
  } else {
    m_restart_requests.push_back(req);
  }
}

template <typename I>
void CopyupRequest<I>::send() {
  read_from_parent();
}

template <typename I>
void CopyupRequest<I>::read_from_parent() {
  auto cct = m_image_ctx->cct;
  std::shared_lock image_locker{m_image_ctx->image_lock};

  if (m_image_ctx->parent == nullptr) {
    ldout(cct, 5) << "parent detached" << dendl;

    m_image_ctx->asio_engine->post(
      [this]() { handle_read_from_parent(-ENOENT); });
    return;
  } else if (is_deep_copy()) {
    deep_copy();
    return;
  }

  auto comp = AioCompletion::create_and_start<
    CopyupRequest<I>,
    &CopyupRequest<I>::handle_read_from_parent>(
      this, librbd::util::get_image_ctx(m_image_ctx->parent), AIO_TYPE_READ);

  ldout(cct, 20) << "completion=" << comp
                 << " image_extents=" << m_image_extents
                 << " area=" << m_image_area << dendl;
  auto req = io::ImageDispatchSpec::create_read(
    *m_image_ctx->parent, io::IMAGE_DISPATCH_LAYER_INTERNAL_START, comp,
    std::move(m_image_extents), m_image_area,
    ReadResult{&m_copyup_extent_map, &m_copyup_data},
    m_image_ctx->parent->get_data_io_context(), 0, 0, m_trace);
  req->send();
}

template <typename I>
void CopyupRequest<I>::handle_read_from_parent(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    m_lock.lock();
    disable_append_requests();
    m_lock.unlock();

    lderr(cct) << "error reading from parent: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  convert_copyup_extent_map();

  m_image_ctx->image_lock.lock_shared();
  m_lock.lock();
  disable_append_requests();

  r = prepare_copyup_data();
  if (r < 0) {
    m_lock.unlock();
    m_image_ctx->image_lock.unlock_shared();

    lderr(m_image_ctx->cct) << "failed to prepare copyup data: "
                            << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_copyup_is_zero = m_copyup_data.is_zero();
  m_copyup_required = is_copyup_required();
  if (!m_copyup_required) {
    m_lock.unlock();
    m_image_ctx->image_lock.unlock_shared();

    ldout(cct, 20) << "no-op, skipping" << dendl;
    finish(0);
    return;
  }

  // copyup() will affect snapshots only if parent data is not all
  // zeros.
  if (!m_copyup_is_zero) {
    m_snap_ids.insert(m_snap_ids.end(), m_image_ctx->snaps.rbegin(),
                      m_image_ctx->snaps.rend());
  }

  m_lock.unlock();
  m_image_ctx->image_lock.unlock_shared();

  update_object_maps();
}

template <typename I>
void CopyupRequest<I>::deep_copy() {
  auto cct = m_image_ctx->cct;
  ceph_assert(ceph_mutex_is_locked(m_image_ctx->image_lock));
  ceph_assert(m_image_ctx->parent != nullptr);

  m_lock.lock();
  m_deep_copied = true;
  m_flatten = is_copyup_required() ? true : m_image_ctx->migration_info.flatten;
  m_lock.unlock();

  ldout(cct, 20) << "flatten=" << m_flatten << dendl;

  uint32_t flags = deep_copy::OBJECT_COPY_REQUEST_FLAG_MIGRATION;
  if (m_flatten) {
    flags |= deep_copy::OBJECT_COPY_REQUEST_FLAG_FLATTEN;
  }

  auto ctx = librbd::util::create_context_callback<
    CopyupRequest<I>, &CopyupRequest<I>::handle_deep_copy>(this);
  auto req = deep_copy::ObjectCopyRequest<I>::create(
    m_image_ctx->parent, m_image_ctx, 0, 0,
    m_image_ctx->migration_info.snap_map, m_object_no, flags, nullptr, ctx);

  req->send();
}

template <typename I>
void CopyupRequest<I>::handle_deep_copy(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_image_ctx->image_lock.lock_shared();
  m_lock.lock();
  m_copyup_required = is_copyup_required();
  if (r == -ENOENT && !m_flatten && m_copyup_required) {
    m_lock.unlock();
    m_image_ctx->image_lock.unlock_shared();

    ldout(cct, 10) << "restart deep-copy with flatten" << dendl;
    send();
    return;
  }

  disable_append_requests();

  if (r < 0 && r != -ENOENT) {
    m_lock.unlock();
    m_image_ctx->image_lock.unlock_shared();

    lderr(cct) << "error encountered during deep-copy: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (!m_copyup_required && !is_update_object_map_required(r)) {
    m_lock.unlock();
    m_image_ctx->image_lock.unlock_shared();

    if (r == -ENOENT) {
      r = 0;
    }

    ldout(cct, 20) << "skipping" << dendl;
    finish(r);
    return;
  }

  // For deep-copy, copyup() will never affect snapshots.  However,
  // this state machine is responsible for updating object maps for
  // snapshots that have been created on destination image after
  // migration started.
  if (r != -ENOENT) {
    compute_deep_copy_snap_ids();
  }

  m_lock.unlock();
  m_image_ctx->image_lock.unlock_shared();

  update_object_maps();
}

template <typename I>
void CopyupRequest<I>::update_object_maps() {
  std::shared_lock owner_locker{m_image_ctx->owner_lock};
  std::shared_lock image_locker{m_image_ctx->image_lock};
  if (m_image_ctx->object_map == nullptr) {
    image_locker.unlock();
    owner_locker.unlock();

    copyup();
    return;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  bool copy_on_read = m_pending_requests.empty();
  uint8_t head_object_map_state = OBJECT_EXISTS;
  if (copy_on_read && !m_snap_ids.empty() &&
      m_image_ctx->test_features(RBD_FEATURE_FAST_DIFF,
                                 m_image_ctx->image_lock)) {
    // HEAD is non-dirty since data is tied to first snapshot
    head_object_map_state = OBJECT_EXISTS_CLEAN;
  }

  auto r_it = m_pending_requests.rbegin();
  if (r_it != m_pending_requests.rend()) {
    // last write-op determines the final object map state
    head_object_map_state = (*r_it)->get_pre_write_object_map_state();
  }

  if ((*m_image_ctx->object_map)[m_object_no] != head_object_map_state) {
    // (maybe) need to update the HEAD object map state
    m_snap_ids.push_back(CEPH_NOSNAP);
  }
  image_locker.unlock();

  ceph_assert(m_image_ctx->exclusive_lock->is_lock_owner());
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_UpdateObjectMap<I>>(),
    boost::lambda::_1, m_image_ctx, m_object_no, head_object_map_state,
    &m_snap_ids, m_first_snap_is_clean, m_trace, boost::lambda::_2));
  auto ctx = librbd::util::create_context_callback<
    CopyupRequest<I>, &CopyupRequest<I>::handle_update_object_maps>(this);
  auto throttle = new AsyncObjectThrottle<I>(
    nullptr, *m_image_ctx, context_factory, ctx, nullptr, 0, m_snap_ids.size());
  throttle->start_ops(
    m_image_ctx->config.template get_val<uint64_t>("rbd_concurrent_management_ops"));
}

template <typename I>
void CopyupRequest<I>::handle_update_object_maps(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to update object map: "
                            << cpp_strerror(r) << dendl;

    finish(r);
    return;
  }

  copyup();
}

template <typename I>
void CopyupRequest<I>::copyup() {
  auto cct = m_image_ctx->cct;
  m_image_ctx->image_lock.lock_shared();
  auto snapc = m_image_ctx->snapc;
  auto io_context = m_image_ctx->get_data_io_context();
  m_image_ctx->image_lock.unlock_shared();

  m_lock.lock();
  if (!m_copyup_required) {
    m_lock.unlock();

    ldout(cct, 20) << "skipping copyup" << dendl;
    finish(0);
    return;
  }

  ldout(cct, 20) << dendl;

  bool copy_on_read = m_pending_requests.empty() && !m_deep_copied;
  bool deep_copyup = !snapc.snaps.empty() && !m_copyup_is_zero;
  if (m_copyup_is_zero) {
    m_copyup_data.clear();
    m_copyup_extent_map.clear();
  }

  neorados::WriteOp copyup_op;
  neorados::WriteOp write_op;
  neorados::WriteOp* op;
  if (copy_on_read || deep_copyup) {
    // copyup-op will use its own request issued to the initial object revision
    op = &copyup_op;
    ++m_pending_copyups;
  } else {
    // copyup-op can be combined with the write-ops (if any)
    op = &write_op;
  }

  if (m_image_ctx->enable_sparse_copyup) {
    cls_client::sparse_copyup(op, m_copyup_extent_map, m_copyup_data);
  } else {
    // convert the sparse read back into a standard (thick) read
    Striper::StripedReadResult destriper;
    destriper.add_partial_sparse_result(
      cct, std::move(m_copyup_data), m_copyup_extent_map, 0,
      {{0, m_image_ctx->layout.object_size}});

    bufferlist thick_bl;
    destriper.assemble_result(cct, thick_bl, false);
    cls_client::copyup(op, thick_bl);
  }
  ObjectRequest<I>::add_write_hint(*m_image_ctx, op);

  if (!copy_on_read) {
    // merge all pending write ops into this single RADOS op
    for (auto req : m_pending_requests) {
      ldout(cct, 20) << "add_copyup_ops " << req << dendl;
      req->add_copyup_ops(&write_op);
    }

    if (write_op.size() > 0) {
      ++m_pending_copyups;
    }
  }
  m_lock.unlock();

  // issue librados ops at the end to simplify test cases
  auto object = neorados::Object{data_object_name(m_image_ctx, m_object_no)};
  if (copyup_op.size() > 0) {
    // send only the copyup request with a blank snapshot context so that
    // all snapshots are detected from the parent for this object.  If
    // this is a CoW request, a second request will be created for the
    // actual modification.
    ldout(cct, 20) << "copyup with empty snapshot context" << dendl;

    auto copyup_io_context = *io_context;
    copyup_io_context.set_write_snap_context({});

    m_image_ctx->rados_api.execute(
      object, copyup_io_context, std::move(copyup_op),
      librbd::asio::util::get_callback_adapter(
        [this](int r) { handle_copyup(r); }), nullptr,
        (this->m_trace.valid() ? this->m_trace.get_info() : nullptr));
  }

  if (write_op.size() > 0) {
    // compare-and-write doesn't add any write ops (copyup+cmpext+write
    // can't be executed in the same RADOS op because, unless the object
    // was already present in the clone, cmpext wouldn't see it)
    ldout(cct, 20) << (!deep_copyup && write_op.size() > 2 ?
                        "copyup + ops" : !deep_copyup ? "copyup" : "ops")
                   << " with current snapshot context" << dendl;

    m_image_ctx->rados_api.execute(
      object, *io_context, std::move(write_op),
      librbd::asio::util::get_callback_adapter(
        [this](int r) { handle_copyup(r); }), nullptr,
        (this->m_trace.valid() ? this->m_trace.get_info() : nullptr));
  }
}

template <typename I>
void CopyupRequest<I>::handle_copyup(int r) {
  auto cct = m_image_ctx->cct;
  unsigned pending_copyups;
  int copyup_ret_val = r;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_pending_copyups > 0);
    pending_copyups = --m_pending_copyups;
    if (m_copyup_ret_val < 0) {
      copyup_ret_val = m_copyup_ret_val;
    } else if (r < 0) {
      m_copyup_ret_val = r;
    }
  }

  ldout(cct, 20) << "r=" << r << ", "
                 << "pending=" << pending_copyups << dendl;

  if (pending_copyups == 0) {
    if (copyup_ret_val < 0 && copyup_ret_val != -ENOENT) {
      lderr(cct) << "failed to copyup object: " << cpp_strerror(copyup_ret_val)
                 << dendl;
      complete_requests(false, copyup_ret_val);
    }

    finish(0);
  }
}

template <typename I>
void CopyupRequest<I>::finish(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  complete_requests(true, r);
  delete this;
}

template <typename I>
void CopyupRequest<I>::complete_requests(bool override_restart_retval, int r) {
  auto cct = m_image_ctx->cct;
  remove_from_list();

  while (!m_pending_requests.empty()) {
    auto it = m_pending_requests.begin();
    auto req = *it;
    ldout(cct, 20) << "completing request " << req << dendl;
    req->handle_copyup(r);
    m_pending_requests.erase(it);
  }

  if (override_restart_retval) {
    r = -ERESTART;
  }

  while (!m_restart_requests.empty()) {
    auto it = m_restart_requests.begin();
    auto req = *it;
    ldout(cct, 20) << "restarting request " << req << dendl;
    req->handle_copyup(r);
    m_restart_requests.erase(it);
  }
}

template <typename I>
void CopyupRequest<I>::disable_append_requests() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  m_append_request_permitted = false;
}

template <typename I>
void CopyupRequest<I>::remove_from_list() {
  std::lock_guard copyup_list_locker{m_image_ctx->copyup_list_lock};

  auto it = m_image_ctx->copyup_list.find(m_object_no);
  if (it != m_image_ctx->copyup_list.end()) {
    m_image_ctx->copyup_list.erase(it);
  }
}

template <typename I>
bool CopyupRequest<I>::is_copyup_required() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  bool copy_on_read = m_pending_requests.empty();
  if (copy_on_read) {
    // always force a copyup if CoR enabled
    return true;
  }

  if (!m_copyup_is_zero) {
    return true;
  }

  for (auto req : m_pending_requests) {
    if (!req->is_empty_write_op()) {
      return true;
    }
  }
  return false;
}

template <typename I>
bool CopyupRequest<I>::is_deep_copy() const {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx->image_lock));
  return !m_image_ctx->migration_info.empty();
}

template <typename I>
bool CopyupRequest<I>::is_update_object_map_required(int r) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx->image_lock));

  if (r < 0) {
    return false;
  }

  if (m_image_ctx->object_map == nullptr) {
    return false;
  }

  if (m_image_ctx->migration_info.empty()) {
    // migration might have completed while IO was in-flight,
    // assume worst-case and perform an object map update
    return true;
  }

  auto it = m_image_ctx->migration_info.snap_map.find(CEPH_NOSNAP);
  ceph_assert(it != m_image_ctx->migration_info.snap_map.end());
  return it->second[0] != CEPH_NOSNAP;
}

template <typename I>
void CopyupRequest<I>::compute_deep_copy_snap_ids() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx->image_lock));

  // don't copy ids for the snaps updated by object deep copy or
  // that don't overlap
  std::set<uint64_t> deep_copied;
  for (auto &it : m_image_ctx->migration_info.snap_map) {
    if (it.first != CEPH_NOSNAP) {
      deep_copied.insert(it.second.front());
    }
  }
  ldout(m_image_ctx->cct, 15) << "deep_copied=" << deep_copied << dendl;

  std::copy_if(m_image_ctx->snaps.rbegin(), m_image_ctx->snaps.rend(),
               std::back_inserter(m_snap_ids),
               [this, cct=m_image_ctx->cct, &deep_copied](uint64_t snap_id) {
      if (deep_copied.count(snap_id)) {
        m_first_snap_is_clean = true;
        return false;
      }

      uint64_t raw_overlap = 0;
      uint64_t object_overlap = 0;
      int r = m_image_ctx->get_parent_overlap(snap_id, &raw_overlap);
      if (r < 0) {
        ldout(cct, 5) << "failed getting parent overlap for snap_id: "
                      << snap_id << ": " << cpp_strerror(r) << dendl;
      } else if (raw_overlap > 0) {
        auto [parent_extents, area] = util::object_to_area_extents(
            m_image_ctx, m_object_no, {{0, m_image_ctx->layout.object_size}});
        object_overlap = m_image_ctx->prune_parent_extents(parent_extents, area,
                                                           raw_overlap, false);
      }
      return object_overlap > 0;
    });
}

template <typename I>
void CopyupRequest<I>::convert_copyup_extent_map() {
  auto cct = m_image_ctx->cct;

  Extents image_extent_map;
  image_extent_map.swap(m_copyup_extent_map);
  m_copyup_extent_map.reserve(image_extent_map.size());

  // convert the image-extent extent map to object-extents
  for (auto [image_offset, image_length] : image_extent_map) {
    striper::LightweightObjectExtents object_extents;
    util::area_to_object_extents(m_image_ctx, image_offset, image_length,
                                 m_image_area, 0, &object_extents);
    for (auto& object_extent : object_extents) {
      m_copyup_extent_map.emplace_back(
        object_extent.offset, object_extent.length);
    }
  }

  ldout(cct, 20) << "image_extents=" << image_extent_map << ", "
                 << "object_extents=" << m_copyup_extent_map << dendl;
}

template <typename I>
int CopyupRequest<I>::prepare_copyup_data() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx->image_lock));
  auto cct = m_image_ctx->cct;

  SnapshotSparseBufferlist snapshot_sparse_bufferlist;
  auto& sparse_bufferlist = snapshot_sparse_bufferlist[0];

  bool copy_on_read = m_pending_requests.empty();
  bool maybe_deep_copyup = !m_image_ctx->snapc.snaps.empty();
  if (copy_on_read || maybe_deep_copyup) {
    // stand-alone copyup that will not be overwritten until HEAD revision
    ldout(cct, 20) << "processing full copy-up" << dendl;

    uint64_t buffer_offset = 0;
    for (auto [object_offset, object_length] : m_copyup_extent_map) {
      bufferlist sub_bl;
      sub_bl.substr_of(m_copyup_data, buffer_offset, object_length);
      buffer_offset += object_length;

      sparse_bufferlist.insert(
        object_offset, object_length,
        {SPARSE_EXTENT_STATE_DATA, object_length, std::move(sub_bl)});
    }
  } else {
    // copyup that will concurrently written to the HEAD revision with the
    // associated write-ops so only process partial extents
    uint64_t buffer_offset = 0;
    for (auto [object_offset, object_length] : m_copyup_extent_map) {
      interval_set<uint64_t> copyup_object_extents;
      copyup_object_extents.insert(object_offset, object_length);

      interval_set<uint64_t> intersection;
      intersection.intersection_of(copyup_object_extents,
                                   m_write_object_extents);

      // extract only portions of the parent copyup data that have not
      // been overwritten by write-ops
      copyup_object_extents.subtract(intersection);
      for (auto [copyup_offset, copyup_length] : copyup_object_extents) {
        bufferlist sub_bl;
        sub_bl.substr_of(
          m_copyup_data, buffer_offset + (copyup_offset - object_offset),
          copyup_length);
        ceph_assert(sub_bl.length() == copyup_length);

        sparse_bufferlist.insert(
          copyup_offset, copyup_length,
          {SPARSE_EXTENT_STATE_DATA, copyup_length, std::move(sub_bl)});
      }
      buffer_offset += object_length;
    }

    ldout(cct, 20) << "processing partial copy-up: " << sparse_bufferlist
                   << dendl;
  }

  // Let dispatch layers have a chance to process the data
  auto r = m_image_ctx->io_object_dispatcher->prepare_copyup(
    m_object_no, &snapshot_sparse_bufferlist);
  if (r < 0) {
    return r;
  }

  // Convert sparse extents back to extent map
  m_copyup_data.clear();
  m_copyup_extent_map.clear();
  m_copyup_extent_map.reserve(sparse_bufferlist.ext_count());
  for (auto& extent : sparse_bufferlist) {
    auto& sbe = extent.get_val();
    if (sbe.state == SPARSE_EXTENT_STATE_DATA) {
      m_copyup_extent_map.emplace_back(extent.get_off(), extent.get_len());
      m_copyup_data.append(sbe.bl);
    }
  }

  return 0;
}

} // namespace io
} // namespace librbd

template class librbd::io::CopyupRequest<librbd::ImageCtx>;
