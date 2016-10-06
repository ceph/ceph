// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/CreateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "include/assert.h"
#include "librbd/Utils.h"
#include "common/ceph_context.h"
#include "librbd/AioCompletion.h"
#include "librbd/Journal.h"
#include "librbd/journal/CreateRequest.h"
#include "librbd/journal/RemoveRequest.h"
#include "journal/Journaler.h"
#include "librbd/MirroringWatcher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::CreateRequest: "

namespace librbd {
namespace image {

using util::create_rados_ack_callback;
using util::create_context_callback;

namespace {

bool validate_features(CephContext *cct, uint64_t features, bool force_non_primary) {
  if ((features & RBD_FEATURE_FAST_DIFF) != 0 &&
      (features & RBD_FEATURE_OBJECT_MAP) == 0) {
    lderr(cct) << "cannot use fast diff without object map" << dendl;
    return false;
  }
  if ((features & RBD_FEATURE_OBJECT_MAP) != 0 &&
      (features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
    lderr(cct) << "cannot use object map without exclusive lock" << dendl;
    return false;
  }
  if ((features & RBD_FEATURE_JOURNALING) != 0) {
    if ((features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
      lderr(cct) << "cannot use journaling without exclusive lock" << dendl;
      return false;
    }
  } else if (force_non_primary) {
    assert(false);
  }

  return true;
}

bool validate_layout(CephContext *cct, uint64_t size, file_layout_t &layout) {
  if (!librbd::ObjectMap::is_compatible(layout, size)) {
    lderr(cct) << "image size not compatible with object map" << dendl;
    return false;
  }

  return true;
}

} // anonymous namespace

// TODO: do away with @m_op_work_queue
// This is used as a temporary measure to execute synchronous calls in
// worker thread (see callers of ->queue()). Once everything is made
// fully asynchronous this can be done away with.
template<typename I>
CreateRequest<I>::CreateRequest(IoCtx &ioctx, std::string &imgname, std::string &imageid,
                                uint64_t size, int order, uint64_t features,
                                uint64_t stripe_unit, uint64_t stripe_count,
                                uint8_t journal_order, uint8_t journal_splay_width,
                                const std::string &journal_pool,
                                const std::string &non_primary_global_image_id,
                                const std::string &primary_mirror_uuid,
                                ContextWQ *op_work_queue, Context *on_finish) :
  m_image_name(imgname), m_image_id(imageid), m_size(size), m_order(order),
  m_features(features), m_stripe_unit(stripe_unit), m_stripe_count(stripe_count),
  m_journal_order(journal_order), m_journal_splay_width(journal_splay_width),
  m_journal_pool(journal_pool), m_non_primary_global_image_id(non_primary_global_image_id),
  m_primary_mirror_uuid(primary_mirror_uuid),
  m_op_work_queue(op_work_queue), m_on_finish(on_finish) {

  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());

  m_id_obj = util::id_obj_name(m_image_name);
  m_header_obj = util::header_name(m_image_id);
  m_objmap_name = ObjectMap::object_map_name(m_image_id, CEPH_NOSNAP);

  m_layout.object_size = 1ull << m_order;
  if (m_stripe_unit == 0 || m_stripe_count == 0) {
    m_layout.stripe_unit = m_layout.object_size;
    m_layout.stripe_count = 1;
  } else {
    m_layout.stripe_unit = m_stripe_unit;
    m_layout.stripe_count = m_stripe_count;
  }

  m_force_non_primary = !non_primary_global_image_id.empty();

  // TODO
  m_features &= ~RBD_FEATURE_DATA_POOL;
}

template<typename I>
void CreateRequest<I>::send() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  if (!validate_features(m_cct, m_features, m_force_non_primary)) {
    complete(-EINVAL);
    return;
  }
  if (!validate_layout(m_cct, m_size, m_layout)) {
    complete(-EINVAL);
    return;
  }

  validate_pool();
}

template<typename I>
void CreateRequest<I>::validate_pool() {
  if (!m_cct->_conf->rbd_validate_pool) {
    create_id_object();
    return;
  }

  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_validate_pool>(this);

  librados::ObjectReadOperation op;
  op.stat(NULL, NULL, NULL);

  int r = m_ioctx.aio_operate(RBD_DIRECTORY, comp, &op, &m_outbl);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context* CreateRequest<I>::handle_validate_pool(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    create_id_object();
    return nullptr;
  } else if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "failed to stat RBD directory: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  // allocate a self-managed snapshot id if this a new pool to force
  // self-managed snapshot mode
  // This call is executed just once per (fresh) pool, hence we do not
  // try hard to make it asynchronous (and it's pretty safe not to cause
  // deadlocks).

  uint64_t snap_id;
  int r = m_ioctx.selfmanaged_snap_create(&snap_id);
  if (r == -EINVAL) {
    lderr(m_cct) << "pool not configured for self-managed RBD snapshot support"
                 << dendl;
    *result = r;
    return m_on_finish;
  } else if (r < 0) {
    lderr(m_cct) << "failed to allocate self-managed snapshot: "
                 << cpp_strerror(r) << dendl;
    *result = r;
    return m_on_finish;
  }

  r = m_ioctx.selfmanaged_snap_remove(snap_id);
  if (r < 0) {
    // we've already switced to self-managed snapshots -- no need to
    // error out in case of failure here.
    ldout(m_cct, 10) << "failed to release self-managed snapshot " << snap_id
                     << ": " << cpp_strerror(r) << dendl;
  }

  create_id_object();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::create_id_object() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  op.create(true);
  cls_client::set_id(&op, m_image_id);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_create_id_object>(this);
  int r = m_ioctx.aio_operate(m_id_obj, comp, &op);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_create_id_object(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error creating RBD id object: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  add_image_to_directory();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::add_image_to_directory() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::dir_add_image(&op, m_image_name, m_image_id);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_add_image_to_directory>(this);
  int r = m_ioctx.aio_operate(RBD_DIRECTORY, comp, &op);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_add_image_to_directory(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error adding image to directory: " << cpp_strerror(*result) << dendl;

    m_r_saved = *result;
    remove_id_object();
    return nullptr;
  }

  create_image();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::create_image() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  ostringstream oss;
  oss << RBD_DATA_PREFIX << m_image_id;

  librados::ObjectWriteOperation op;
  op.create(true);
  cls_client::create_image(&op, m_size, m_order, m_features, oss.str(),
                           /* TODO */-1);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_create_image>(this);
  int r = m_ioctx.aio_operate(m_header_obj, comp, &op);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_create_image(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error writing header: " << cpp_strerror(*result) << dendl;
    m_r_saved = *result;
    remove_from_dir();
    return nullptr;
  }

  set_stripe_unit_count();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::set_stripe_unit_count() {
  if ((!m_stripe_unit && !m_stripe_count) ||
      ((m_stripe_count == 1) && (m_stripe_unit == (1ull << m_order)))) {
    object_map_resize();
    return;
  }

  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::set_stripe_unit_count(&op, m_stripe_unit, m_stripe_count);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_set_stripe_unit_count>(this);
  int r = m_ioctx.aio_operate(m_header_obj, comp, &op);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_set_stripe_unit_count(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error setting stripe unit/count: " << cpp_strerror(*result) << dendl;
    m_r_saved = *result;
    remove_header_object();
    return nullptr;
  }

  object_map_resize();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::object_map_resize() {
  if ((m_features & RBD_FEATURE_OBJECT_MAP) == 0) {
    fetch_mirror_mode();
    return;
  }

  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  op.create(true);
  cls_client::object_map_resize(&op, Striper::get_num_objects(m_layout, m_size),
                                OBJECT_NONEXISTENT);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_object_map_resize>(this);
  int r = m_ioctx.aio_operate(m_objmap_name, comp, &op);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_object_map_resize(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error creating initial object map: " << cpp_strerror(*result) << dendl;

    m_r_saved = *result;
    remove_header_object();
    return nullptr;
  }

  fetch_mirror_mode();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::fetch_mirror_mode() {
  if ((m_features & RBD_FEATURE_JOURNALING) == 0) {
    complete(0);
    return;
  }

  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_mode_get_start(&op);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_fetch_mirror_mode>(this);
  m_outbl.clear();
  int r = m_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_outbl);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_fetch_mirror_mode(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "failed to retrieve mirror mode: " << cpp_strerror(*result) << dendl;

    m_r_saved = *result;
    remove_object_map();
    return nullptr;
  }

  cls::rbd::MirrorMode mirror_mode_internal = cls::rbd::MIRROR_MODE_DISABLED;
  if (*result == 0) {
    bufferlist::iterator it = m_outbl.begin();
    *result = cls_client::mirror_mode_get_finish(&it, &mirror_mode_internal);
    if (*result < 0) {
      lderr(m_cct) << "Failed to retrieve mirror mode" << dendl;

      m_r_saved = *result;
      remove_object_map();
      return nullptr;
    }
  }

  // TODO: remove redundant code...
  switch (mirror_mode_internal) {
  case cls::rbd::MIRROR_MODE_DISABLED:
  case cls::rbd::MIRROR_MODE_IMAGE:
  case cls::rbd::MIRROR_MODE_POOL:
    m_mirror_mode = static_cast<rbd_mirror_mode_t>(mirror_mode_internal);
    break;
  default:
    lderr(m_cct) << "Unknown mirror mode ("
               << static_cast<uint32_t>(mirror_mode_internal) << ")"
               << dendl;
    *result = -EINVAL;
    remove_object_map();
    return nullptr;
  }

  journal_create();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::journal_create() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = CreateRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_journal_create>(this);

  librbd::journal::TagData tag_data;
  tag_data.mirror_uuid = (m_force_non_primary ? m_primary_mirror_uuid :
                          librbd::Journal<I>::LOCAL_MIRROR_UUID);

  librbd::journal::CreateRequest<I> *req =
    librbd::journal::CreateRequest<I>::create(
      m_ioctx, m_image_id, m_journal_order, m_journal_splay_width, m_journal_pool,
      cls::journal::Tag::TAG_CLASS_NEW, tag_data, librbd::Journal<I>::IMAGE_CLIENT_ID,
      m_op_work_queue, ctx);
  req->send();
}

template<typename I>
Context* CreateRequest<I>::handle_journal_create(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error creating journal: " << cpp_strerror(*result) << dendl;

    m_r_saved = *result;
    remove_object_map();
    return nullptr;
  }

  fetch_mirror_image();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::fetch_mirror_image() {
  if ((m_mirror_mode != RBD_MIRROR_MODE_POOL) && !m_force_non_primary) {
    complete(0);
    return;
  }

  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_id);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_fetch_mirror_image>(this);
  m_outbl.clear();
  int r = m_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_outbl);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_fetch_mirror_image(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "cannot enable mirroring: " << cpp_strerror(*result) << dendl;

    m_r_saved = *result;
    journal_remove();
    return nullptr;
  }

  if (*result == 0) {
    bufferlist::iterator it = m_outbl.begin();
    *result = cls_client::mirror_image_get_finish(&it, &m_mirror_image_internal);
    if (*result < 0) {
      lderr(m_cct) << "cannot enable mirroring: " << cpp_strerror(*result) << dendl;

      m_r_saved = *result;
      journal_remove();
      return nullptr;
    }

    if (m_mirror_image_internal.state == cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
      return m_on_finish;
    }
  }

  // enable image mirroring (-ENOENT or disabled earlier)
  mirror_image_enable();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::mirror_image_enable() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  m_mirror_image_internal.state = cls::rbd::MIRROR_IMAGE_STATE_ENABLED;
  if (m_non_primary_global_image_id.empty()) {
    uuid_d uuid_gen;
    uuid_gen.generate_random();
    m_mirror_image_internal.global_image_id = uuid_gen.to_string();
  } else {
    m_mirror_image_internal.global_image_id = m_non_primary_global_image_id;
  }

  librados::ObjectWriteOperation op;
  cls_client::mirror_image_set(&op, m_image_id, m_mirror_image_internal);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_mirror_image_enable>(this);
  int r = m_ioctx.aio_operate(RBD_MIRRORING, comp, &op);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_mirror_image_enable(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "cannot enable mirroring: " << cpp_strerror(*result) << dendl;

    m_r_saved = *result;
    journal_remove();
    return nullptr;
  }

  send_watcher_notification();
  return nullptr;
}

// TODO: make this *really* async
template<typename I>
void CreateRequest<I>::send_watcher_notification() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  Context *ctx = new FunctionContext([this](int r) {
      r = MirroringWatcher<>::notify_image_updated(
        m_ioctx, cls::rbd::MIRROR_IMAGE_STATE_ENABLED,
        m_image_id, m_mirror_image_internal.global_image_id);
      handle_watcher_notify(r);
    });

    m_op_work_queue->queue(ctx, 0);
}

template<typename I>
void CreateRequest<I>::handle_watcher_notify(int r) {
  ldout(m_cct, 20) << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    // non fatal error -- watchers would cope up upon noticing missing
    // updates. just log and move on...
    ldout(m_cct, 10) << "failed to send update notification: " << cpp_strerror(r)
                     << dendl;
  } else {
    ldout(m_cct, 20) << "image mirroring is enabled: global_id=" <<
      m_mirror_image_internal.global_image_id << dendl;
  }

  complete(0);
}

template<typename I>
void CreateRequest<I>::complete(int r) {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  if (r == 0) {
    ldout(m_cct, 20) << "done." << dendl;
  }

  m_on_finish->complete(r);
  delete this;
}

// cleanup
template<typename I>
void CreateRequest<I>::journal_remove() {
  if ((m_features & RBD_FEATURE_JOURNALING) == 0) {
    remove_object_map();
    return;
  }

  ldout(m_cct, 20) << this << " " <<__func__ << dendl;

  using klass = CreateRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_journal_remove>(this);

  librbd::journal::RemoveRequest<I> *req =
    librbd::journal::RemoveRequest<I>::create(
      m_ioctx, m_image_id, librbd::Journal<I>::IMAGE_CLIENT_ID, m_op_work_queue, ctx);
  req->send();
}

template<typename I>
Context *CreateRequest<I>::handle_journal_remove(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error cleaning up journal after creation failed: "
                 << cpp_strerror(*result) << dendl;
  }

  remove_object_map();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::remove_object_map() {
  if ((m_features & RBD_FEATURE_OBJECT_MAP) == 0) {
    remove_header_object();
    return;
  }

  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_remove_object_map>(this);
  int r = m_ioctx.aio_remove(m_objmap_name, comp);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_remove_object_map(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error cleaning up object map after creation failed: "
                 << cpp_strerror(*result) << dendl;
  }

  remove_header_object();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::remove_header_object() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_remove_header_object>(this);
  int r = m_ioctx.aio_remove(m_header_obj, comp);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_remove_header_object(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error cleaning up image header after creation failed: "
                 << cpp_strerror(*result) << dendl;
  }

  remove_from_dir();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::remove_from_dir() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::dir_remove_image(&op, m_image_name, m_image_id);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_remove_from_dir>(this);
  int r = m_ioctx.aio_operate(RBD_DIRECTORY, comp, &op);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_remove_from_dir(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error cleaning up image from rbd_directory object "
                 << "after creation failed: " << cpp_strerror(*result) << dendl;
  }

  remove_id_object();
  return nullptr;
}

template<typename I>
void CreateRequest<I>::remove_id_object() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_remove_id_object>(this);
  int r = m_ioctx.aio_remove(m_id_obj, comp);
  assert(r == 0);
  comp->release();
}

template<typename I>
Context *CreateRequest<I>::handle_remove_id_object(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "error cleaning up id object after creation failed: "
                 << cpp_strerror(*result) << dendl;
  }

  *result = m_r_saved;
  return m_on_finish;
}

} //namespace image
} //namespace librbd

template class librbd::image::CreateRequest<librbd::ImageCtx>;
