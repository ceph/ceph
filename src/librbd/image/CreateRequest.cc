// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/CreateRequest.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "osdc/Striper.h"
#include "librbd/Features.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/image/Types.h"
#include "librbd/image/ValidatePoolRequest.h"
#include "librbd/journal/CreateRequest.h"
#include "librbd/journal/RemoveRequest.h"
#include "librbd/journal/TypeTraits.h"
#include "librbd/mirror/EnableRequest.h"
#include "journal/Journaler.h"


#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::CreateRequest: " << __func__ \
                           << ": "

namespace librbd {
namespace image {

using util::create_rados_callback;
using util::create_context_callback;

namespace {

int validate_features(CephContext *cct, uint64_t features) {
  if (features & ~RBD_FEATURES_ALL) {
    lderr(cct) << "librbd does not support requested features." << dendl;
    return -ENOSYS;
  }
  if ((features & RBD_FEATURE_OPERATIONS) != 0) {
    lderr(cct) << "cannot use internally controlled features" << dendl;
    return -EINVAL;
  }
  if ((features & RBD_FEATURE_FAST_DIFF) != 0 &&
      (features & RBD_FEATURE_OBJECT_MAP) == 0) {
    lderr(cct) << "cannot use fast diff without object map" << dendl;
    return -EINVAL;
  }
  if ((features & RBD_FEATURE_OBJECT_MAP) != 0 &&
      (features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
    lderr(cct) << "cannot use object map without exclusive lock" << dendl;
    return -EINVAL;
  }
  if ((features & RBD_FEATURE_JOURNALING) != 0 &&
      (features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
    lderr(cct) << "cannot use journaling without exclusive lock" << dendl;
    return -EINVAL;
  }

  return 0;
}

int validate_striping(CephContext *cct, uint8_t order, uint64_t stripe_unit,
                      uint64_t stripe_count) {
  if ((stripe_unit && !stripe_count) ||
      (!stripe_unit && stripe_count)) {
    lderr(cct) << "must specify both (or neither) of stripe-unit and "
               << "stripe-count" << dendl;
    return -EINVAL;
  } else if (stripe_unit && ((1ull << order) % stripe_unit || stripe_unit > (1ull << order))) {
    lderr(cct) << "stripe unit is not a factor of the object size" << dendl;
    return -EINVAL;
  }
  return 0;
}

bool validate_layout(CephContext *cct, uint64_t size, file_layout_t &layout) {
  if (!librbd::ObjectMap<>::is_compatible(layout, size)) {
    lderr(cct) << "image size not compatible with object map" << dendl;
    return false;
  }

  return true;
}

int get_image_option(const ImageOptions &image_options, int option,
                     uint8_t *value) {
  uint64_t large_value;
  int r = image_options.get(option, &large_value);
  if (r < 0) {
    return r;
  }
  *value = static_cast<uint8_t>(large_value);
  return 0;
}

} // anonymous namespace

template<typename I>
int CreateRequest<I>::validate_order(CephContext *cct, uint8_t order) {
  if (order > 25 || order < 12) {
    lderr(cct) << "order must be in the range [12, 25]" << dendl;
    return -EDOM;
  }
  return 0;
}

#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::CreateRequest: " << this << " " \
                           << __func__ << ": "

template<typename I>
CreateRequest<I>::CreateRequest(const ConfigProxy& config, IoCtx &ioctx,
                                const std::string &image_name,
                                const std::string &image_id, uint64_t size,
                                const ImageOptions &image_options,
                                uint32_t create_flags,
                                cls::rbd::MirrorImageMode mirror_image_mode,
                                const std::string &non_primary_global_image_id,
                                const std::string &primary_mirror_uuid,
                                asio::ContextWQ *op_work_queue,
                                Context *on_finish)
  : m_config(config), m_image_name(image_name), m_image_id(image_id),
    m_size(size), m_create_flags(create_flags),
    m_mirror_image_mode(mirror_image_mode),
    m_non_primary_global_image_id(non_primary_global_image_id),
    m_primary_mirror_uuid(primary_mirror_uuid),
    m_op_work_queue(op_work_queue), m_on_finish(on_finish) {

  m_io_ctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_io_ctx.cct());

  m_id_obj = util::id_obj_name(m_image_name);
  m_header_obj = util::header_name(m_image_id);
  m_objmap_name = ObjectMap<>::object_map_name(m_image_id, CEPH_NOSNAP);
  if (!non_primary_global_image_id.empty() &&
      (m_create_flags & CREATE_FLAG_MIRROR_ENABLE_MASK) == 0) {
    m_create_flags |= CREATE_FLAG_FORCE_MIRROR_ENABLE;
  }

  if (image_options.get(RBD_IMAGE_OPTION_FEATURES, &m_features) != 0) {
    m_features = librbd::rbd_features_from_string(
      m_config.get_val<std::string>("rbd_default_features"), nullptr);
    m_negotiate_features = true;
  }

  uint64_t features_clear = 0;
  uint64_t features_set = 0;
  image_options.get(RBD_IMAGE_OPTION_FEATURES_CLEAR, &features_clear);
  image_options.get(RBD_IMAGE_OPTION_FEATURES_SET, &features_set);

  uint64_t features_conflict = features_clear & features_set;
  features_clear &= ~features_conflict;
  features_set &= ~features_conflict;
  m_features |= features_set;
  m_features &= ~features_clear;

  if ((m_features & RBD_FEATURE_OBJECT_MAP) == RBD_FEATURE_OBJECT_MAP) {
      m_features |= RBD_FEATURE_FAST_DIFF;
  }

  if (image_options.get(RBD_IMAGE_OPTION_STRIPE_UNIT, &m_stripe_unit) != 0 ||
      m_stripe_unit == 0) {
    m_stripe_unit = m_config.get_val<Option::size_t>("rbd_default_stripe_unit");
  }
  if (image_options.get(RBD_IMAGE_OPTION_STRIPE_COUNT, &m_stripe_count) != 0 ||
      m_stripe_count == 0) {
    m_stripe_count = m_config.get_val<uint64_t>("rbd_default_stripe_count");
  }
  if (get_image_option(image_options, RBD_IMAGE_OPTION_ORDER, &m_order) != 0 ||
      m_order == 0) {
    m_order = config.get_val<uint64_t>("rbd_default_order");
  }
  if (get_image_option(image_options, RBD_IMAGE_OPTION_JOURNAL_ORDER,
      &m_journal_order) != 0) {
    m_journal_order = m_config.get_val<uint64_t>("rbd_journal_order");
  }
  if (get_image_option(image_options, RBD_IMAGE_OPTION_JOURNAL_SPLAY_WIDTH,
                       &m_journal_splay_width) != 0) {
    m_journal_splay_width = m_config.get_val<uint64_t>(
      "rbd_journal_splay_width");
  }
  if (image_options.get(RBD_IMAGE_OPTION_JOURNAL_POOL, &m_journal_pool) != 0) {
    m_journal_pool = m_config.get_val<std::string>("rbd_journal_pool");
  }
  if (image_options.get(RBD_IMAGE_OPTION_DATA_POOL, &m_data_pool) != 0) {
    m_data_pool = m_config.get_val<std::string>("rbd_default_data_pool");
  }

  m_layout.object_size = 1ull << m_order;
  if (m_stripe_unit == 0 || m_stripe_count == 0) {
    m_layout.stripe_unit = m_layout.object_size;
    m_layout.stripe_count = 1;
  } else {
    m_layout.stripe_unit = m_stripe_unit;
    m_layout.stripe_count = m_stripe_count;
  }

  if (!m_data_pool.empty() && m_data_pool != ioctx.get_pool_name()) {
    m_features |= RBD_FEATURE_DATA_POOL;
  } else {
    m_data_pool.clear();
    m_features &= ~RBD_FEATURE_DATA_POOL;
  }

  if ((m_stripe_unit != 0 && m_stripe_unit != (1ULL << m_order)) ||
      (m_stripe_count != 0 && m_stripe_count != 1)) {
    m_features |= RBD_FEATURE_STRIPINGV2;
  } else {
    m_features &= ~RBD_FEATURE_STRIPINGV2;
  }

  ldout(m_cct, 10) << "name=" << m_image_name << ", "
                   << "id=" << m_image_id << ", "
                   << "size=" << m_size << ", "
                   << "features=" << m_features << ", "
                   << "order=" << (uint64_t)m_order << ", "
                   << "stripe_unit=" << m_stripe_unit << ", "
                   << "stripe_count=" << m_stripe_count << ", "
                   << "journal_order=" << (uint64_t)m_journal_order << ", "
                   << "journal_splay_width="
                   << (uint64_t)m_journal_splay_width << ", "
                   << "journal_pool=" << m_journal_pool << ", "
                   << "data_pool=" << m_data_pool << dendl;
}

template<typename I>
void CreateRequest<I>::send() {
  ldout(m_cct, 20) << dendl;

  int r = validate_features(m_cct, m_features);
  if (r < 0) {
    complete(r);
    return;
  }

  r = validate_order(m_cct, m_order);
  if (r < 0) {
    complete(r);
    return;
  }

  r = validate_striping(m_cct, m_order, m_stripe_unit, m_stripe_count);
  if (r < 0) {
    complete(r);
    return;
  }

  if (((m_features & RBD_FEATURE_OBJECT_MAP) != 0) &&
      (!validate_layout(m_cct, m_size, m_layout))) {
    complete(-EINVAL);
    return;
  }

  validate_data_pool();
}

template <typename I>
void CreateRequest<I>::validate_data_pool() {
  m_data_io_ctx = m_io_ctx;
  if ((m_features & RBD_FEATURE_DATA_POOL) != 0) {
    librados::Rados rados(m_io_ctx);
    int r = rados.ioctx_create(m_data_pool.c_str(), m_data_io_ctx);
    if (r < 0) {
      lderr(m_cct) << "data pool " << m_data_pool << " does not exist" << dendl;
      complete(r);
      return;
    }
    m_data_pool_id = m_data_io_ctx.get_id();
    m_data_io_ctx.set_namespace(m_io_ctx.get_namespace());
  }

  if (!m_config.get_val<bool>("rbd_validate_pool")) {
    add_image_to_directory();
    return;
  }

  ldout(m_cct, 15) << dendl;

  auto ctx = create_context_callback<
    CreateRequest<I>, &CreateRequest<I>::handle_validate_data_pool>(this);
  auto req = ValidatePoolRequest<I>::create(m_data_io_ctx, m_op_work_queue,
                                            ctx);
  req->send();
}

template <typename I>
void CreateRequest<I>::handle_validate_data_pool(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r == -EINVAL) {
    lderr(m_cct) << "pool does not support RBD images" << dendl;
    complete(r);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to validate pool: " << cpp_strerror(r) << dendl;
    complete(r);
    return;
  }

  add_image_to_directory();
}

template<typename I>
void CreateRequest<I>::add_image_to_directory() {
  ldout(m_cct, 15) << dendl;

  librados::ObjectWriteOperation op;
  if (!m_io_ctx.get_namespace().empty()) {
    cls_client::dir_state_assert(&op, cls::rbd::DIRECTORY_STATE_READY);
  }
  cls_client::dir_add_image(&op, m_image_name, m_image_id);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_add_image_to_directory>(this);
  int r = m_io_ctx.aio_operate(RBD_DIRECTORY, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_add_image_to_directory(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r == -EEXIST) {
    ldout(m_cct, 5) << "directory entry for image " << m_image_name
                    << " already exists" << dendl;
    complete(r);
    return;
  } else if (!m_io_ctx.get_namespace().empty() && r == -ENOENT) {
    ldout(m_cct, 5) << "namespace " << m_io_ctx.get_namespace()
                    << " does not exist" << dendl;
    complete(r);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "error adding image to directory: " << cpp_strerror(r)
                 << dendl;
    complete(r);
    return;
  }

  create_id_object();
}

template<typename I>
void CreateRequest<I>::create_id_object() {
  ldout(m_cct, 15) << dendl;

  librados::ObjectWriteOperation op;
  op.create(true);
  cls_client::set_id(&op, m_image_id);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_create_id_object>(this);
  int r = m_io_ctx.aio_operate(m_id_obj, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_create_id_object(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r == -EEXIST) {
    ldout(m_cct, 5) << "id object for  " << m_image_name << " already exists"
                    << dendl;
    m_r_saved = r;
    remove_from_dir();
    return;
  } else if (r < 0) {
    lderr(m_cct) << "error creating RBD id object: " << cpp_strerror(r)
                 << dendl;
    m_r_saved = r;
    remove_from_dir();
    return;
  }

  negotiate_features();
}

template<typename I>
void CreateRequest<I>::negotiate_features() {
  if (!m_negotiate_features) {
    create_image();
    return;
  }

  ldout(m_cct, 15) << dendl;

  librados::ObjectReadOperation op;
  cls_client::get_all_features_start(&op);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_negotiate_features>(this);

  m_outbl.clear();
  int r = m_io_ctx.aio_operate(RBD_DIRECTORY, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_negotiate_features(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  uint64_t all_features;
  if (r >= 0) {
    auto it = m_outbl.cbegin();
    r = cls_client::get_all_features_finish(&it, &all_features);
  }
  if (r < 0) {
    ldout(m_cct, 10) << "error retrieving server supported features set: "
                     << cpp_strerror(r) << dendl;
  } else if ((m_features & all_features) != m_features) {
    m_features &= all_features;
    ldout(m_cct, 10) << "limiting default features set to server supported: "
		     << m_features << dendl;
  }

  create_image();
}

template<typename I>
void CreateRequest<I>::create_image() {
  ldout(m_cct, 15) << dendl;
  ceph_assert(m_data_pool.empty() || m_data_pool_id != -1);

  ostringstream oss;
  oss << RBD_DATA_PREFIX;
  if (m_data_pool_id != -1) {
    oss << stringify(m_io_ctx.get_id()) << ".";
  }
  oss << m_image_id;
  if (oss.str().length() > RBD_MAX_BLOCK_NAME_PREFIX_LENGTH) {
    lderr(m_cct) << "object prefix '" << oss.str() << "' too large" << dendl;
    m_r_saved = -EINVAL;
    remove_id_object();
    return;
  }

  librados::ObjectWriteOperation op;
  op.create(true);
  cls_client::create_image(&op, m_size, m_order, m_features, oss.str(),
                           m_data_pool_id);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_create_image>(this);
  int r = m_io_ctx.aio_operate(m_header_obj, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_create_image(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r == -EEXIST) {
    ldout(m_cct, 5) << "image id already in-use" << dendl;
    complete(-EBADF);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "error writing header: " << cpp_strerror(r) << dendl;
    m_r_saved = r;
    remove_id_object();
    return;
  }

  set_stripe_unit_count();
}

template<typename I>
void CreateRequest<I>::set_stripe_unit_count() {
  if ((!m_stripe_unit && !m_stripe_count) ||
      ((m_stripe_count == 1) && (m_stripe_unit == (1ull << m_order)))) {
    object_map_resize();
    return;
  }

  ldout(m_cct, 15) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::set_stripe_unit_count(&op, m_stripe_unit, m_stripe_count);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_set_stripe_unit_count>(this);
  int r = m_io_ctx.aio_operate(m_header_obj, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_set_stripe_unit_count(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "error setting stripe unit/count: "
                 << cpp_strerror(r) << dendl;
    m_r_saved = r;
    remove_header_object();
    return;
  }

  object_map_resize();
}

template<typename I>
void CreateRequest<I>::object_map_resize() {
  if ((m_features & RBD_FEATURE_OBJECT_MAP) == 0) {
    fetch_mirror_mode();
    return;
  }

  ldout(m_cct, 15) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::object_map_resize(&op, Striper::get_num_objects(m_layout, m_size),
                                OBJECT_NONEXISTENT);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_object_map_resize>(this);
  int r = m_io_ctx.aio_operate(m_objmap_name, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_object_map_resize(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "error creating initial object map: "
                 << cpp_strerror(r) << dendl;

    m_r_saved = r;
    remove_header_object();
    return;
  }

  fetch_mirror_mode();
}

template<typename I>
void CreateRequest<I>::fetch_mirror_mode() {
  if ((m_features & RBD_FEATURE_JOURNALING) == 0) {
    mirror_image_enable();
    return;
  }

  ldout(m_cct, 15) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_mode_get_start(&op);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_fetch_mirror_mode>(this);
  m_outbl.clear();
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_fetch_mirror_mode(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if ((r < 0) && (r != -ENOENT)) {
    lderr(m_cct) << "failed to retrieve mirror mode: " << cpp_strerror(r)
                 << dendl;

    m_r_saved = r;
    remove_object_map();
    return;
  }

  m_mirror_mode = cls::rbd::MIRROR_MODE_DISABLED;
  if (r == 0) {
    auto it = m_outbl.cbegin();
    r = cls_client::mirror_mode_get_finish(&it, &m_mirror_mode);
    if (r < 0) {
      lderr(m_cct) << "Failed to retrieve mirror mode" << dendl;

      m_r_saved = r;
      remove_object_map();
      return;
    }
  }

  journal_create();
}

template<typename I>
void CreateRequest<I>::journal_create() {
  ldout(m_cct, 15) << dendl;

  using klass = CreateRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_journal_create>(
    this);

  // only link to remote primary mirror uuid if in journal-based
  // mirroring mode
  bool use_primary_mirror_uuid = (
    !m_non_primary_global_image_id.empty() &&
    m_mirror_image_mode == cls::rbd::MIRROR_IMAGE_MODE_JOURNAL);

  librbd::journal::TagData tag_data;
  tag_data.mirror_uuid = (use_primary_mirror_uuid ? m_primary_mirror_uuid :
                          librbd::Journal<I>::LOCAL_MIRROR_UUID);

  typename journal::TypeTraits<I>::ContextWQ* context_wq;
  Journal<>::get_work_queue(m_cct, &context_wq);

  auto req = librbd::journal::CreateRequest<I>::create(
    m_io_ctx, m_image_id, m_journal_order, m_journal_splay_width,
    m_journal_pool, cls::journal::Tag::TAG_CLASS_NEW, tag_data,
    librbd::Journal<I>::IMAGE_CLIENT_ID, context_wq, ctx);
  req->send();
}

template<typename I>
void CreateRequest<I>::handle_journal_create(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "error creating journal: " << cpp_strerror(r)
                 << dendl;

    m_r_saved = r;
    remove_object_map();
    return;
  }

  mirror_image_enable();
}

template<typename I>
void CreateRequest<I>::mirror_image_enable() {
  auto mirror_enable_flag = (m_create_flags & CREATE_FLAG_MIRROR_ENABLE_MASK);

  if ((m_mirror_mode != cls::rbd::MIRROR_MODE_POOL &&
       mirror_enable_flag != CREATE_FLAG_FORCE_MIRROR_ENABLE) ||
      (mirror_enable_flag == CREATE_FLAG_SKIP_MIRROR_ENABLE)) {
    complete(0);
    return;
  }

  ldout(m_cct, 15) << dendl;
  auto ctx = create_context_callback<
    CreateRequest<I>, &CreateRequest<I>::handle_mirror_image_enable>(this);

  auto req = mirror::EnableRequest<I>::create(
    m_io_ctx, m_image_id, m_mirror_image_mode,
    m_non_primary_global_image_id, true, m_op_work_queue, ctx);
  req->send();
}

template<typename I>
void CreateRequest<I>::handle_mirror_image_enable(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "cannot enable mirroring: " << cpp_strerror(r)
                 << dendl;

    m_r_saved = r;
    journal_remove();
    return;
  }

  complete(0);
}

template<typename I>
void CreateRequest<I>::complete(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_data_io_ctx.close();
  auto on_finish = m_on_finish;
  delete this;
  on_finish->complete(r);
}

// cleanup
template<typename I>
void CreateRequest<I>::journal_remove() {
  if ((m_features & RBD_FEATURE_JOURNALING) == 0) {
    remove_object_map();
    return;
  }

  ldout(m_cct, 15) << dendl;

  using klass = CreateRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_journal_remove>(
    this);

  typename journal::TypeTraits<I>::ContextWQ* context_wq;
  Journal<>::get_work_queue(m_cct, &context_wq);

  librbd::journal::RemoveRequest<I> *req =
    librbd::journal::RemoveRequest<I>::create(
      m_io_ctx, m_image_id, librbd::Journal<I>::IMAGE_CLIENT_ID, context_wq,
      ctx);
  req->send();
}

template<typename I>
void CreateRequest<I>::handle_journal_remove(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "error cleaning up journal after creation failed: "
                 << cpp_strerror(r) << dendl;
  }

  remove_object_map();
}

template<typename I>
void CreateRequest<I>::remove_object_map() {
  if ((m_features & RBD_FEATURE_OBJECT_MAP) == 0) {
    remove_header_object();
    return;
  }

  ldout(m_cct, 15) << dendl;

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_remove_object_map>(this);
  int r = m_io_ctx.aio_remove(m_objmap_name, comp);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_remove_object_map(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "error cleaning up object map after creation failed: "
                 << cpp_strerror(r) << dendl;
  }

  remove_header_object();
}

template<typename I>
void CreateRequest<I>::remove_header_object() {
  ldout(m_cct, 15) << dendl;

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_remove_header_object>(this);
  int r = m_io_ctx.aio_remove(m_header_obj, comp);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_remove_header_object(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "error cleaning up image header after creation failed: "
                 << cpp_strerror(r) << dendl;
  }

  remove_id_object();
}

template<typename I>
void CreateRequest<I>::remove_id_object() {
  ldout(m_cct, 15) << dendl;

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_remove_id_object>(this);
  int r = m_io_ctx.aio_remove(m_id_obj, comp);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_remove_id_object(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "error cleaning up id object after creation failed: "
                 << cpp_strerror(r) << dendl;
  }

  remove_from_dir();
}

template<typename I>
void CreateRequest<I>::remove_from_dir() {
  ldout(m_cct, 15) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::dir_remove_image(&op, m_image_name, m_image_id);

  using klass = CreateRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_remove_from_dir>(this);
  int r = m_io_ctx.aio_operate(RBD_DIRECTORY, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template<typename I>
void CreateRequest<I>::handle_remove_from_dir(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "error cleaning up image from rbd_directory object "
                 << "after creation failed: " << cpp_strerror(r) << dendl;
  }

  complete(m_r_saved);
}

} //namespace image
} //namespace librbd

template class librbd::image::CreateRequest<librbd::ImageCtx>;
