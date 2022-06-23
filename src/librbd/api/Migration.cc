// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Migration.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/AsioEngine.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/api/Config.h"
#include "librbd/api/Group.h"
#include "librbd/api/Image.h"
#include "librbd/api/Snapshot.h"
#include "librbd/api/Trash.h"
#include "librbd/deep_copy/Handler.h"
#include "librbd/deep_copy/ImageCopyRequest.h"
#include "librbd/deep_copy/MetadataCopyRequest.h"
#include "librbd/deep_copy/SnapshotCopyRequest.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/image/AttachChildRequest.h"
#include "librbd/image/AttachParentRequest.h"
#include "librbd/image/CloneRequest.h"
#include "librbd/image/CreateRequest.h"
#include "librbd/image/DetachChildRequest.h"
#include "librbd/image/DetachParentRequest.h"
#include "librbd/image/ListWatchersRequest.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/image/Types.h"
#include "librbd/internal.h"
#include "librbd/migration/FormatInterface.h"
#include "librbd/migration/OpenSourceImageRequest.h"
#include "librbd/migration/NativeFormat.h"
#include "librbd/mirror/DisableRequest.h"
#include "librbd/mirror/EnableRequest.h"

#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Migration: " << __func__ << ": "

namespace librbd {

inline bool operator==(const linked_image_spec_t& rhs,
                       const linked_image_spec_t& lhs) {
  bool result = (rhs.pool_id == lhs.pool_id &&
                 rhs.pool_namespace == lhs.pool_namespace &&
                 rhs.image_id == lhs.image_id);
  return result;
}

namespace api {

using util::create_rados_callback;

namespace {

class MigrationProgressContext : public ProgressContext {
public:
  MigrationProgressContext(librados::IoCtx& io_ctx,
                           const std::string &header_oid,
                           cls::rbd::MigrationState state,
                           ProgressContext *prog_ctx)
    : m_io_ctx(io_ctx), m_header_oid(header_oid), m_state(state),
      m_prog_ctx(prog_ctx), m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())),
      m_lock(ceph::make_mutex(
	util::unique_lock_name("librbd::api::MigrationProgressContext",
			       this))) {
    ceph_assert(m_prog_ctx != nullptr);
  }

  ~MigrationProgressContext() {
    wait_for_in_flight_updates();
  }

  int update_progress(uint64_t offset, uint64_t total) override {
    ldout(m_cct, 20) << "offset=" << offset << ", total=" << total << dendl;

    m_prog_ctx->update_progress(offset, total);

    std::string description = stringify(offset * 100 / total) + "% complete";

    send_state_description_update(description);

    return 0;
  }

private:
  librados::IoCtx& m_io_ctx;
  std::string m_header_oid;
  cls::rbd::MigrationState m_state;
  ProgressContext *m_prog_ctx;

  CephContext* m_cct;
  mutable ceph::mutex m_lock;
  ceph::condition_variable m_cond;
  std::string m_state_description;
  bool m_pending_update = false;
  int m_in_flight_state_updates = 0;

  void send_state_description_update(const std::string &description) {
    std::lock_guard locker{m_lock};

    if (description == m_state_description) {
      return;
    }

    m_state_description = description;

    if (m_in_flight_state_updates > 0) {
      m_pending_update = true;
      return;
    }

    set_state_description();
  }

  void set_state_description() {
    ldout(m_cct, 20) << "state_description=" << m_state_description << dendl;

    ceph_assert(ceph_mutex_is_locked(m_lock));

    librados::ObjectWriteOperation op;
    cls_client::migration_set_state(&op, m_state, m_state_description);

    using klass = MigrationProgressContext;
    librados::AioCompletion *comp =
      create_rados_callback<klass, &klass::handle_set_state_description>(this);
    int r = m_io_ctx.aio_operate(m_header_oid, comp, &op);
    ceph_assert(r == 0);
    comp->release();

    m_in_flight_state_updates++;
  }

  void handle_set_state_description(int r) {
    ldout(m_cct, 20) << "r=" << r << dendl;

    std::lock_guard locker{m_lock};

    m_in_flight_state_updates--;

    if (r < 0) {
      lderr(m_cct) << "failed to update migration state: " << cpp_strerror(r)
                   << dendl;
    } else if (m_pending_update) {
      set_state_description();
      m_pending_update = false;
    } else {
      m_cond.notify_all();
    }
  }

  void wait_for_in_flight_updates() {
    std::unique_lock locker{m_lock};

    ldout(m_cct, 20) << "m_in_flight_state_updates="
                     << m_in_flight_state_updates << dendl;
    m_pending_update = false;
    m_cond.wait(locker, [this] { return m_in_flight_state_updates <= 0; });
  }
};

int trash_search(librados::IoCtx &io_ctx, rbd_trash_image_source_t source,
                 const std::string &image_name, std::string *image_id) {
  std::vector<trash_image_info_t> entries;

  int r = Trash<>::list(io_ctx, entries, false);
  if (r < 0) {
    return r;
  }

  for (auto &entry : entries) {
    if (entry.source == source && entry.name == image_name) {
      *image_id = entry.id;
      return 0;
    }
  }

  return -ENOENT;
}

template <typename I>
int open_images(librados::IoCtx& io_ctx, const std::string &image_name,
                I **src_image_ctx, I **dst_image_ctx,
                cls::rbd::MigrationSpec* src_migration_spec,
                cls::rbd::MigrationSpec* dst_migration_spec,
                bool skip_open_dst_image) {
  CephContext* cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  *src_image_ctx = nullptr;
  *dst_image_ctx = nullptr;

  ldout(cct, 10) << "trying to open image by name " << io_ctx.get_pool_name()
                 << "/" << image_name << dendl;
  auto image_ctx = I::create(image_name, "", nullptr, io_ctx, false);
  int r = image_ctx->state->open(OPEN_FLAG_IGNORE_MIGRATING);
  if (r == -ENOENT) {
    // presume user passed the source image so we need to search the trash
    ldout(cct, 10) << "Source image is not found. Trying trash" << dendl;

    std::string src_image_id;
    r = trash_search(io_ctx, RBD_TRASH_IMAGE_SOURCE_MIGRATION, image_name,
                     &src_image_id);
    if (r < 0) {
      lderr(cct) << "failed to determine image id: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    ldout(cct, 10) << "source image id from trash: " << src_image_id << dendl;
    image_ctx = I::create(image_name, src_image_id, nullptr, io_ctx, false);
    r = image_ctx->state->open(OPEN_FLAG_IGNORE_MIGRATING);
  }

  if (r < 0) {
    if (r != -ENOENT) {
      lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
      return r;
    }
    image_ctx = nullptr;
  }

  BOOST_SCOPE_EXIT_TPL(&r, &image_ctx, src_image_ctx, dst_image_ctx) {
    if (r != 0) {
      if (*src_image_ctx != nullptr) {
        (*src_image_ctx)->state->close();
      }
      if (*dst_image_ctx != nullptr) {
        (*dst_image_ctx)->state->close();
      }
      if (image_ctx != nullptr) {
        image_ctx->state->close();
      }
    }
  } BOOST_SCOPE_EXIT_END;

  // The opened image is either a source or destination
  cls::rbd::MigrationSpec migration_spec;
  r = cls_client::migration_get(&image_ctx->md_ctx, image_ctx->header_oid,
                                &migration_spec);
  if (r < 0) {
    lderr(cct) << "failed retrieving migration header: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  ldout(cct, 10) << "migration spec: " << migration_spec << dendl;
  if (migration_spec.header_type == cls::rbd::MIGRATION_HEADER_TYPE_SRC) {
    ldout(cct, 10) << "the source image is opened" << dendl;
    *src_image_ctx = image_ctx;
    *src_migration_spec = migration_spec;
    image_ctx = nullptr;
  } else if (migration_spec.header_type ==
               cls::rbd::MIGRATION_HEADER_TYPE_DST) {
    ldout(cct, 10) << "the destination image is opened" << dendl;
    std::string image_id = image_ctx->id;
    image_ctx->state->close();
    image_ctx = I::create(image_name, image_id, nullptr, io_ctx, false);

    if (!skip_open_dst_image) {
      ldout(cct, 10) << "re-opening the destination image" << dendl;
      r = image_ctx->state->open(0);
      if (r < 0) {
        image_ctx = nullptr;
        lderr(cct) << "failed to re-open destination image: " << cpp_strerror(r)
                   << dendl;
        return r;
      }
    }

    *dst_image_ctx = image_ctx;
    *dst_migration_spec = migration_spec;
    image_ctx = nullptr;
  } else {
    lderr(cct) << "unexpected migration header type: "
               << migration_spec.header_type << dendl;
    r = -EINVAL;
    return r;
  }

  // attempt to open the other (paired) image
  I** other_image_ctx = nullptr;
  std::string other_image_type;
  std::string other_image_name;
  std::string other_image_id;
  cls::rbd::MigrationSpec* other_migration_spec = nullptr;
  librados::IoCtx other_io_ctx;

  int flags = OPEN_FLAG_IGNORE_MIGRATING;
  if (*src_image_ctx == nullptr &&
      dst_migration_spec->source_spec.empty()) {
    r = util::create_ioctx(io_ctx, "source image", migration_spec.pool_id,
                           migration_spec.pool_namespace, &other_io_ctx);
    if (r < 0) {
      return r;
    }

    other_image_type = "source";
    other_image_ctx = src_image_ctx;
    other_migration_spec = src_migration_spec;
    other_image_name = migration_spec.image_name;
    other_image_id = migration_spec.image_id;

    if (other_image_id.empty()) {
      ldout(cct, 20) << "trying to open v1 image by name "
                     << other_io_ctx.get_pool_name() << "/"
                     << other_image_name << dendl;
      flags |= OPEN_FLAG_OLD_FORMAT;
    } else {
      ldout(cct, 20) << "trying to open v2 image by id "
                     << other_io_ctx.get_pool_name() << "/"
                     << other_image_id << dendl;
    }

    *src_image_ctx = I::create(other_image_name, other_image_id, nullptr,
                               other_io_ctx, false);
  } else if (*dst_image_ctx == nullptr) {
    r = util::create_ioctx(io_ctx, "destination image", migration_spec.pool_id,
                           migration_spec.pool_namespace, &other_io_ctx);
    if (r < 0) {
      return r;
    }

    other_image_name = migration_spec.image_name;
    if (skip_open_dst_image) {
      other_image_id = migration_spec.image_id;
    } else {
      other_image_type = "destination";
      other_image_ctx = dst_image_ctx;
      other_migration_spec = dst_migration_spec;
      other_image_id = migration_spec.image_id;
    }

    *dst_image_ctx = I::create(other_image_name, other_image_id, nullptr,
                               other_io_ctx, false);
  }

  if (other_image_ctx != nullptr) {
    r = (*other_image_ctx)->state->open(flags);
    if (r < 0) {
      lderr(cct) << "failed to open " << other_image_type << " image "
                 << other_io_ctx.get_pool_name()
                 << "/" << (other_image_id.empty() ?
                              other_image_name : other_image_id)
                 << ": " << cpp_strerror(r) << dendl;
      *other_image_ctx = nullptr;
      return r;
    }

    r = cls_client::migration_get(&(*other_image_ctx)->md_ctx,
                                  (*other_image_ctx)->header_oid,
                                  other_migration_spec);
    if (r < 0) {
      lderr(cct) << "failed retrieving migration header: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    ldout(cct, 20) << other_image_type << " migration spec: "
                   << *other_migration_spec << dendl;
  }

  if (!skip_open_dst_image) {
    // legacy clients will only store status in the source images
    if (dst_migration_spec->source_spec.empty()) {
      dst_migration_spec->state = migration_spec.state;
      dst_migration_spec->state_description =
        migration_spec.state_description;
    }
  }

  return 0;
}

class SteppedProgressContext : public ProgressContext {
public:
  SteppedProgressContext(ProgressContext* progress_ctx, size_t total_steps)
    : m_progress_ctx(progress_ctx), m_total_steps(total_steps) {
  }

  void next_step() {
    ceph_assert(m_current_step < m_total_steps);
    ++m_current_step;
  }

  int update_progress(uint64_t object_number,
                      uint64_t object_count) override {
    return m_progress_ctx->update_progress(
      object_number + (object_count * (m_current_step - 1)),
      object_count * m_total_steps);
  }

private:
  ProgressContext* m_progress_ctx;
  size_t m_total_steps;
  size_t m_current_step = 1;
};

} // anonymous namespace

template <typename I>
int Migration<I>::prepare(librados::IoCtx& io_ctx,
                          const std::string &image_name,
                          librados::IoCtx& dest_io_ctx,
                          const std::string &dest_image_name_,
                          ImageOptions& opts) {
  CephContext* cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  std::string dest_image_name = dest_image_name_.empty() ? image_name :
    dest_image_name_;

  ldout(cct, 10) << io_ctx.get_pool_name() << "/" << image_name << " -> "
                 << dest_io_ctx.get_pool_name() << "/" << dest_image_name
                 << ", opts=" << opts << dendl;

  auto src_image_ctx = I::create(image_name, "", nullptr, io_ctx, false);
  int r = src_image_ctx->state->open(0);
  if (r < 0) {
    lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
    return r;
  }
  BOOST_SCOPE_EXIT_TPL(src_image_ctx) {
    src_image_ctx->state->close();
  } BOOST_SCOPE_EXIT_END;

  std::list<obj_watch_t> watchers;
  int flags = librbd::image::LIST_WATCHERS_FILTER_OUT_MY_INSTANCE |
              librbd::image::LIST_WATCHERS_FILTER_OUT_MIRROR_INSTANCES;
  C_SaferCond on_list_watchers;
  auto list_watchers_request = librbd::image::ListWatchersRequest<I>::create(
      *src_image_ctx, flags, &watchers, &on_list_watchers);
  list_watchers_request->send();
  r = on_list_watchers.wait();
  if (r < 0) {
    lderr(cct) << "failed listing watchers:" << cpp_strerror(r) << dendl;
    return r;
  }
  if (!watchers.empty()) {
    lderr(cct) << "image has watchers - not migrating" << dendl;
    return -EBUSY;
  }

  uint64_t format = 2;
  if (opts.get(RBD_IMAGE_OPTION_FORMAT, &format) != 0) {
    opts.set(RBD_IMAGE_OPTION_FORMAT, format);
  }
  if (format != 2) {
    lderr(cct) << "unsupported destination image format: " << format << dendl;
    return -EINVAL;
  }

  uint64_t features;
  {
    std::shared_lock image_locker{src_image_ctx->image_lock};
    features = src_image_ctx->features;
  }
  opts.get(RBD_IMAGE_OPTION_FEATURES, &features);
  if ((features & ~RBD_FEATURES_ALL) != 0) {
    lderr(cct) << "librbd does not support requested features" << dendl;
    return -ENOSYS;
  }
  opts.set(RBD_IMAGE_OPTION_FEATURES, features);

  uint64_t order = src_image_ctx->order;
  if (opts.get(RBD_IMAGE_OPTION_ORDER, &order) != 0) {
    opts.set(RBD_IMAGE_OPTION_ORDER, order);
  }
  r = image::CreateRequest<I>::validate_order(cct, order);
  if (r < 0) {
    return r;
  }

  uint64_t stripe_unit = src_image_ctx->stripe_unit;
  if (opts.get(RBD_IMAGE_OPTION_STRIPE_UNIT, &stripe_unit) != 0) {
    opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit);
  }
  uint64_t stripe_count = src_image_ctx->stripe_count;
  if (opts.get(RBD_IMAGE_OPTION_STRIPE_COUNT, &stripe_count) != 0) {
    opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count);
  }

  uint64_t flatten = 0;
  if (opts.get(RBD_IMAGE_OPTION_FLATTEN, &flatten) == 0) {
    opts.unset(RBD_IMAGE_OPTION_FLATTEN);
  }

  ldout(cct, 20) << "updated opts=" << opts << dendl;

  auto dst_image_ctx = I::create(
    dest_image_name, util::generate_image_id(dest_io_ctx), nullptr,
    dest_io_ctx, false);
  src_image_ctx->image_lock.lock_shared();
  cls::rbd::MigrationSpec dst_migration_spec{
    cls::rbd::MIGRATION_HEADER_TYPE_DST,
    src_image_ctx->md_ctx.get_id(), src_image_ctx->md_ctx.get_namespace(),
    src_image_ctx->name, src_image_ctx->id, "", {}, 0, false,
    cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, flatten > 0,
    cls::rbd::MIGRATION_STATE_PREPARING, ""};
  src_image_ctx->image_lock.unlock_shared();

  Migration migration(src_image_ctx, dst_image_ctx, dst_migration_spec,
                      opts, nullptr);
  r = migration.prepare();

  return r;
}

template <typename I>
int Migration<I>::prepare_import(
    const std::string& source_spec, librados::IoCtx& dest_io_ctx,
    const std::string &dest_image_name, ImageOptions& opts) {
  if (source_spec.empty() || !dest_io_ctx.is_valid() ||
      dest_image_name.empty()) {
    return -EINVAL;
  }

  auto cct = reinterpret_cast<CephContext *>(dest_io_ctx.cct());
  ldout(cct, 10) << source_spec << " -> "
                 << dest_io_ctx.get_pool_name() << "/"
                 << dest_image_name << ", opts=" << opts << dendl;

  I* src_image_ctx = nullptr;
  C_SaferCond open_ctx;
  auto req = migration::OpenSourceImageRequest<I>::create(
    dest_io_ctx, nullptr, CEPH_NOSNAP,
    {-1, "", "", "", source_spec, {}, 0, false}, &src_image_ctx, &open_ctx);
  req->send();

  int r = open_ctx.wait();
  if (r < 0) {
    lderr(cct) << "failed to open source image: " << cpp_strerror(r) << dendl;
    return r;
  }

  auto asio_engine = src_image_ctx->asio_engine;
  BOOST_SCOPE_EXIT_TPL(src_image_ctx) {
    src_image_ctx->state->close();
  } BOOST_SCOPE_EXIT_END;

  uint64_t image_format = 2;
  if (opts.get(RBD_IMAGE_OPTION_FORMAT, &image_format) != 0) {
    opts.set(RBD_IMAGE_OPTION_FORMAT, image_format);
  }
  if (image_format != 2) {
    lderr(cct) << "unsupported destination image format: " << image_format
               << dendl;
    return -EINVAL;
  }

  ldout(cct, 20) << "updated opts=" << opts << dendl;

  // use json-spirit to clean-up json formatting
  json_spirit::mObject source_spec_object;
  json_spirit::mValue json_root;
  if(json_spirit::read(source_spec, json_root)) {
    try {
      source_spec_object = json_root.get_obj();
    } catch (std::runtime_error&) {
      lderr(cct) << "failed to clean source spec" << dendl;
      return -EINVAL;
    }
  }

  auto dst_image_ctx = I::create(
    dest_image_name, util::generate_image_id(dest_io_ctx), nullptr,
    dest_io_ctx, false);
  cls::rbd::MigrationSpec dst_migration_spec{
    cls::rbd::MIGRATION_HEADER_TYPE_DST, -1, "", "", "",
    json_spirit::write(source_spec_object), {},
    0, false, cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, true,
    cls::rbd::MIGRATION_STATE_PREPARING, ""};

  Migration migration(src_image_ctx, dst_image_ctx, dst_migration_spec,
                      opts, nullptr);
  return migration.prepare_import();
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::execute(librados::IoCtx& io_ctx,
                          const std::string &image_name,
                          ProgressContext &prog_ctx) {
  CephContext* cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  ldout(cct, 10) << io_ctx.get_pool_name() << "/" << image_name << dendl;

  I *src_image_ctx;
  I *dst_image_ctx;
  cls::rbd::MigrationSpec src_migration_spec;
  cls::rbd::MigrationSpec dst_migration_spec;
  int r = open_images(io_ctx, image_name, &src_image_ctx, &dst_image_ctx,
                      &src_migration_spec, &dst_migration_spec, false);
  if (r < 0) {
    return r;
  }

  // ensure the destination loads the migration info
  dst_image_ctx->ignore_migrating = false;
  r = dst_image_ctx->state->refresh();
  if (r < 0) {
    lderr(cct) << "failed to refresh destination image: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  BOOST_SCOPE_EXIT_TPL(src_image_ctx, dst_image_ctx) {
    dst_image_ctx->state->close();
    if (src_image_ctx != nullptr) {
      src_image_ctx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;

  if (dst_migration_spec.state != cls::rbd::MIGRATION_STATE_PREPARED &&
      dst_migration_spec.state != cls::rbd::MIGRATION_STATE_EXECUTING) {
    lderr(cct) << "current migration state is '" << dst_migration_spec.state
               << "' (should be 'prepared')" << dendl;
    return -EINVAL;
  }

  ldout(cct, 5) << "migrating ";
  if (!dst_migration_spec.source_spec.empty()) {
    *_dout << dst_migration_spec.source_spec;
  } else {
    *_dout << src_image_ctx->md_ctx.get_pool_name() << "/"
           << src_image_ctx->name;
  }
  *_dout << " -> " << dst_image_ctx->md_ctx.get_pool_name() << "/"
         << dst_image_ctx->name << dendl;

  ImageOptions opts;
  Migration migration(src_image_ctx, dst_image_ctx, dst_migration_spec,
                      opts, &prog_ctx);
  r = migration.execute();
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::abort(librados::IoCtx& io_ctx, const std::string &image_name,
                        ProgressContext &prog_ctx) {
  CephContext* cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  ldout(cct, 10) << io_ctx.get_pool_name() << "/" << image_name << dendl;

  I *src_image_ctx;
  I *dst_image_ctx;
  cls::rbd::MigrationSpec src_migration_spec;
  cls::rbd::MigrationSpec dst_migration_spec;
  int r = open_images(io_ctx, image_name, &src_image_ctx, &dst_image_ctx,
                      &src_migration_spec, &dst_migration_spec, true);
  if (r < 0) {
    return r;
  }

  ldout(cct, 5) << "canceling incomplete migration ";
  if (!dst_migration_spec.source_spec.empty()) {
    *_dout << dst_migration_spec.source_spec;
  } else {
    *_dout << src_image_ctx->md_ctx.get_pool_name() << "/"
           << src_image_ctx->name;
  }
  *_dout << " -> " << dst_image_ctx->md_ctx.get_pool_name() << "/"
         << dst_image_ctx->name << dendl;

  ImageOptions opts;
  Migration migration(src_image_ctx, dst_image_ctx, dst_migration_spec,
                      opts, &prog_ctx);
  r = migration.abort();

  if (src_image_ctx != nullptr) {
    src_image_ctx->state->close();
  }

  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::commit(librados::IoCtx& io_ctx,
                         const std::string &image_name,
                         ProgressContext &prog_ctx) {
  CephContext* cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  ldout(cct, 10) << io_ctx.get_pool_name() << "/" << image_name << dendl;

  I *src_image_ctx;
  I *dst_image_ctx;
  cls::rbd::MigrationSpec src_migration_spec;
  cls::rbd::MigrationSpec dst_migration_spec;
  int r = open_images(io_ctx, image_name, &src_image_ctx, &dst_image_ctx,
                      &src_migration_spec, &dst_migration_spec, false);
  if (r < 0) {
    return r;
  }

  if (dst_migration_spec.state != cls::rbd::MIGRATION_STATE_EXECUTED) {
    lderr(cct) << "current migration state is '" << dst_migration_spec.state
              << "' (should be 'executed')" << dendl;
    dst_image_ctx->state->close();
    if (src_image_ctx != nullptr) {
      src_image_ctx->state->close();
    }
    return -EINVAL;
  }

  // ensure the destination loads the migration info
  dst_image_ctx->ignore_migrating = false;
  r = dst_image_ctx->state->refresh();
  if (r < 0) {
    lderr(cct) << "failed to refresh destination image: " << cpp_strerror(r)
               << dendl;
    return r;
  }

  ldout(cct, 5) << "migrating ";
  if (!dst_migration_spec.source_spec.empty()) {
    *_dout << dst_migration_spec.source_spec;
  } else {
    *_dout << src_image_ctx->md_ctx.get_pool_name() << "/"
           << src_image_ctx->name;
  }
  *_dout << " -> " << dst_image_ctx->md_ctx.get_pool_name() << "/"
         << dst_image_ctx->name << dendl;

  ImageOptions opts;
  Migration migration(src_image_ctx, dst_image_ctx, dst_migration_spec,
                      opts, &prog_ctx);
  r = migration.commit();

  // image_ctx is closed in commit when removing src image
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::status(librados::IoCtx& io_ctx,
                         const std::string &image_name,
                         image_migration_status_t *status) {
  CephContext* cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  ldout(cct, 10) << io_ctx.get_pool_name() << "/" << image_name << dendl;

  I *src_image_ctx;
  I *dst_image_ctx;
  cls::rbd::MigrationSpec src_migration_spec;
  cls::rbd::MigrationSpec dst_migration_spec;
  int r = open_images(io_ctx, image_name, &src_image_ctx, &dst_image_ctx,
                      &src_migration_spec, &dst_migration_spec, false);
  if (r < 0) {
    return r;
  }

  ldout(cct, 5) << "migrating ";
  if (!dst_migration_spec.source_spec.empty()) {
    *_dout << dst_migration_spec.source_spec;
  } else {
    *_dout << src_image_ctx->md_ctx.get_pool_name() << "/"
           << src_image_ctx->name;
  }
  *_dout << " -> " << dst_image_ctx->md_ctx.get_pool_name() << "/"
         << dst_image_ctx->name << dendl;

  ImageOptions opts;
  Migration migration(src_image_ctx, dst_image_ctx, dst_migration_spec,
                      opts, nullptr);
  r = migration.status(status);

  dst_image_ctx->state->close();
  if (src_image_ctx != nullptr) {
    src_image_ctx->state->close();
  }

  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::get_source_spec(I* image_ctx, std::string* source_spec) {
  auto cct = image_ctx->cct;
  ldout(cct, 10) << dendl;

  image_ctx->image_lock.lock_shared();
  auto migration_info = image_ctx->migration_info;
  image_ctx->image_lock.unlock_shared();

  if (migration_info.empty()) {
    // attempt to directly read the spec in case the state is EXECUTED
    cls::rbd::MigrationSpec migration_spec;
    int r = cls_client::migration_get(&image_ctx->md_ctx, image_ctx->header_oid,
                                      &migration_spec);
    if (r == -ENOENT) {
      return r;
    } else if (r < 0) {
      lderr(cct) << "failed retrieving migration header: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    migration_info = {
      migration_spec.pool_id, migration_spec.pool_namespace,
      migration_spec.image_name, migration_spec.image_id,
      migration_spec.source_spec, {}, 0, false};
  }

  if (!migration_info.source_spec.empty()) {
    *source_spec = migration_info.source_spec;
  } else {
    // legacy migration source
    *source_spec = migration::NativeFormat<I>::build_source_spec(
      migration_info.pool_id,
      migration_info.pool_namespace,
      migration_info.image_name,
      migration_info.image_id);
  }

  return 0;
}

template <typename I>
Migration<I>::Migration(ImageCtx* src_image_ctx,
                        ImageCtx* dst_image_ctx,
                        const cls::rbd::MigrationSpec& dst_migration_spec,
                        ImageOptions& opts, ProgressContext *prog_ctx)
  : m_cct(dst_image_ctx->cct),
    m_src_image_ctx(src_image_ctx), m_dst_image_ctx(dst_image_ctx),
    m_dst_io_ctx(dst_image_ctx->md_ctx), m_dst_image_name(dst_image_ctx->name),
    m_dst_image_id(dst_image_ctx->id),
    m_dst_header_oid(util::header_name(m_dst_image_id)),
    m_image_options(opts), m_flatten(dst_migration_spec.flatten),
    m_mirroring(dst_migration_spec.mirroring),
    m_mirror_image_mode(dst_migration_spec.mirror_image_mode),
    m_prog_ctx(prog_ctx),
    m_src_migration_spec(cls::rbd::MIGRATION_HEADER_TYPE_SRC,
                         m_dst_io_ctx.get_id(), m_dst_io_ctx.get_namespace(),
                         m_dst_image_name, m_dst_image_id, "", {}, 0,
                         m_mirroring, m_mirror_image_mode, m_flatten,
                         dst_migration_spec.state,
                         dst_migration_spec.state_description),
    m_dst_migration_spec(dst_migration_spec) {
  m_dst_io_ctx.dup(dst_image_ctx->md_ctx);
}

template <typename I>
int Migration<I>::prepare() {
  ldout(m_cct, 10) << dendl;

  BOOST_SCOPE_EXIT_TPL(&m_dst_image_ctx) {
    if (m_dst_image_ctx != nullptr) {
      m_dst_image_ctx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;

  int r = validate_src_snaps(m_src_image_ctx);
  if (r < 0) {
    return r;
  }

  r = disable_mirroring(m_src_image_ctx, &m_mirroring, &m_mirror_image_mode);
  if (r < 0) {
    return r;
  }

  r = unlink_src_image(m_src_image_ctx);
  if (r < 0) {
    enable_mirroring(m_src_image_ctx, m_mirroring, m_mirror_image_mode);
    return r;
  }

  r = set_src_migration(m_src_image_ctx);
  if (r < 0) {
    relink_src_image(m_src_image_ctx);
    enable_mirroring(m_src_image_ctx, m_mirroring, m_mirror_image_mode);
    return r;
  }

  r = create_dst_image(&m_dst_image_ctx);
  if (r < 0) {
    abort();
    return r;
  }

  ldout(m_cct, 10) << "succeeded" << dendl;

  return 0;
}

template <typename I>
int Migration<I>::prepare_import() {
  ldout(m_cct, 10) << dendl;

  BOOST_SCOPE_EXIT_TPL(&m_dst_image_ctx) {
    if (m_dst_image_ctx != nullptr) {
      m_dst_image_ctx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;

  int r = create_dst_image(&m_dst_image_ctx);
  if (r < 0) {
    abort();
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::execute() {
  ldout(m_cct, 10) << dendl;

  int r = set_state(cls::rbd::MIGRATION_STATE_EXECUTING, "");
  if (r < 0) {
    return r;
  }

  {
    MigrationProgressContext dst_prog_ctx(
      m_dst_image_ctx->md_ctx, m_dst_image_ctx->header_oid,
      cls::rbd::MIGRATION_STATE_EXECUTING, m_prog_ctx);
    std::optional<MigrationProgressContext> src_prog_ctx;
    if (m_src_image_ctx != nullptr) {
      src_prog_ctx.emplace(m_src_image_ctx->md_ctx, m_src_image_ctx->header_oid,
                           cls::rbd::MIGRATION_STATE_EXECUTING, &dst_prog_ctx);
    }

    while (true) {
      r = m_dst_image_ctx->operations->migrate(
        *(src_prog_ctx ? &src_prog_ctx.value() : &dst_prog_ctx));
      if (r == -EROFS) {
        std::shared_lock owner_locker{m_dst_image_ctx->owner_lock};
        if (m_dst_image_ctx->exclusive_lock != nullptr &&
          !m_dst_image_ctx->exclusive_lock->accept_ops()) {
          ldout(m_cct, 5) << "lost exclusive lock, retrying remote" << dendl;
          continue;
        }
      }
      break;
    }
  }

  if (r < 0) {
    lderr(m_cct) << "migration failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = set_state(cls::rbd::MIGRATION_STATE_EXECUTED, "");
  if (r < 0) {
    return r;
  }

  m_dst_image_ctx->notify_update();

  ldout(m_cct, 10) << "succeeded" << dendl;

  return 0;
}

template <typename I>
int Migration<I>::abort() {
  ldout(m_cct, 10) << dendl;

  int r;
  if (m_src_image_ctx != nullptr) {
    m_src_image_ctx->owner_lock.lock_shared();
    if (m_src_image_ctx->exclusive_lock != nullptr &&
        !m_src_image_ctx->exclusive_lock->is_lock_owner()) {
      C_SaferCond ctx;
      m_src_image_ctx->exclusive_lock->acquire_lock(&ctx);
      m_src_image_ctx->owner_lock.unlock_shared();
      r = ctx.wait();
      if (r < 0) {
        lderr(m_cct) << "error acquiring exclusive lock: " << cpp_strerror(r)
                     << dendl;
        return r;
      }
    } else {
      m_src_image_ctx->owner_lock.unlock_shared();
    }
  }

  group_info_t group_info;
  group_info.pool = -1;

  r = m_dst_image_ctx->state->open(OPEN_FLAG_IGNORE_MIGRATING);
  if (r < 0) {
    ldout(m_cct, 1) << "failed to open destination image: " << cpp_strerror(r)
                    << dendl;
    m_dst_image_ctx = nullptr;
  } else {
    BOOST_SCOPE_EXIT_TPL(&m_dst_image_ctx) {
      if (m_dst_image_ctx != nullptr) {
        m_dst_image_ctx->state->close();
      }
    } BOOST_SCOPE_EXIT_END;

    std::list<obj_watch_t> watchers;
    int flags = librbd::image::LIST_WATCHERS_FILTER_OUT_MY_INSTANCE |
                librbd::image::LIST_WATCHERS_FILTER_OUT_MIRROR_INSTANCES;
    C_SaferCond on_list_watchers;
    auto list_watchers_request = librbd::image::ListWatchersRequest<I>::create(
        *m_dst_image_ctx, flags, &watchers, &on_list_watchers);
    list_watchers_request->send();
    r = on_list_watchers.wait();
    if (r < 0) {
      lderr(m_cct) << "failed listing watchers:" << cpp_strerror(r) << dendl;
      return r;
    }
    if (!watchers.empty()) {
      lderr(m_cct) << "image has watchers - cannot abort migration" << dendl;
      return -EBUSY;
    }

    // ensure destination image is now read-only
    r = set_state(cls::rbd::MIGRATION_STATE_ABORTING, "");
    if (r < 0) {
      return r;
    }

    SteppedProgressContext progress_ctx(
      m_prog_ctx, (m_src_image_ctx != nullptr ? 2 : 1));
    if (m_src_image_ctx != nullptr) {
      // copy dst HEAD -> src HEAD
      revert_data(m_dst_image_ctx, m_src_image_ctx, &progress_ctx);
      progress_ctx.next_step();

      ldout(m_cct, 10) << "relinking children" << dendl;
      r = relink_children(m_dst_image_ctx, m_src_image_ctx);
      if (r < 0) {
        return r;
      }
    }

    ldout(m_cct, 10) << "removing dst image snapshots" << dendl;
    std::vector<librbd::snap_info_t> snaps;
    r = Snapshot<I>::list(m_dst_image_ctx, snaps);
    if (r < 0) {
      lderr(m_cct) << "failed listing snapshots: " << cpp_strerror(r)
                   << dendl;
      return r;
    }

    for (auto &snap : snaps) {
      librbd::NoOpProgressContext prog_ctx;
      int r = Snapshot<I>::remove(m_dst_image_ctx, snap.name.c_str(),
                                  RBD_SNAP_REMOVE_UNPROTECT, prog_ctx);
      if (r < 0) {
        lderr(m_cct) << "failed removing snapshot: " << cpp_strerror(r)
                     << dendl;
        return r;
      }
    }

    ldout(m_cct, 10) << "removing group" << dendl;

    r = remove_group(m_dst_image_ctx, &group_info);
    if (r < 0 && r != -ENOENT) {
      return r;
    }

    ldout(m_cct, 10) << "removing dst image" << dendl;

    ceph_assert(m_dst_image_ctx->ignore_migrating);

    auto asio_engine = m_dst_image_ctx->asio_engine;
    librados::IoCtx dst_io_ctx(m_dst_image_ctx->md_ctx);

    C_SaferCond on_remove;
    auto req = librbd::image::RemoveRequest<>::create(
      dst_io_ctx, m_dst_image_ctx, false, false, progress_ctx,
      asio_engine->get_work_queue(), &on_remove);
    req->send();
    r = on_remove.wait();

    m_dst_image_ctx = nullptr;

    if (r < 0) {
      lderr(m_cct) << "failed removing destination image '"
                   << dst_io_ctx.get_pool_name() << "/" << m_dst_image_name
                   << " (" << m_dst_image_id << ")': " << cpp_strerror(r)
                   << dendl;
      return r;
    }
  }

  if (m_src_image_ctx != nullptr) {
    r = relink_src_image(m_src_image_ctx);
    if (r < 0) {
      return r;
    }

    r = add_group(m_src_image_ctx, group_info);
    if (r < 0) {
      return r;
    }

    r = remove_migration(m_src_image_ctx);
    if (r < 0) {
      return r;
    }

    r = enable_mirroring(m_src_image_ctx, m_mirroring, m_mirror_image_mode);
    if (r < 0) {
      return r;
    }
  }

  ldout(m_cct, 10) << "succeeded" << dendl;

  return 0;
}

template <typename I>
int Migration<I>::commit() {
  ldout(m_cct, 10) << dendl;

  BOOST_SCOPE_EXIT_TPL(&m_dst_image_ctx, &m_src_image_ctx) {
    m_dst_image_ctx->state->close();
    if (m_src_image_ctx != nullptr) {
      m_src_image_ctx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;

  int r = remove_migration(m_dst_image_ctx);
  if (r < 0) {
    return r;
  }

  if (m_src_image_ctx != nullptr) {
    r = remove_src_image(&m_src_image_ctx);
    if (r < 0) {
      return r;
    }
  }

  r = enable_mirroring(m_dst_image_ctx, m_mirroring, m_mirror_image_mode);
  if (r < 0) {
    return r;
  }

  ldout(m_cct, 10) << "succeeded" << dendl;

  return 0;
}

template <typename I>
int Migration<I>::status(image_migration_status_t *status) {
  ldout(m_cct, 10) << dendl;

  status->source_pool_id = m_dst_migration_spec.pool_id;
  status->source_pool_namespace = m_dst_migration_spec.pool_namespace;
  status->source_image_name = m_dst_migration_spec.image_name;
  status->source_image_id = m_dst_migration_spec.image_id;
  status->dest_pool_id = m_src_migration_spec.pool_id;
  status->dest_pool_namespace = m_src_migration_spec.pool_namespace;
  status->dest_image_name = m_src_migration_spec.image_name;
  status->dest_image_id = m_src_migration_spec.image_id;

  switch (m_src_migration_spec.state) {
  case cls::rbd::MIGRATION_STATE_ERROR:
    status->state = RBD_IMAGE_MIGRATION_STATE_ERROR;
    break;
  case cls::rbd::MIGRATION_STATE_PREPARING:
    status->state = RBD_IMAGE_MIGRATION_STATE_PREPARING;
    break;
  case cls::rbd::MIGRATION_STATE_PREPARED:
    status->state = RBD_IMAGE_MIGRATION_STATE_PREPARED;
    break;
  case cls::rbd::MIGRATION_STATE_EXECUTING:
    status->state = RBD_IMAGE_MIGRATION_STATE_EXECUTING;
    break;
  case cls::rbd::MIGRATION_STATE_EXECUTED:
    status->state = RBD_IMAGE_MIGRATION_STATE_EXECUTED;
    break;
  default:
    status->state = RBD_IMAGE_MIGRATION_STATE_UNKNOWN;
    break;
  }

  status->state_description = m_src_migration_spec.state_description;

  return 0;
}

template <typename I>
int Migration<I>::set_state(I* image_ctx, const std::string& image_description,
                            cls::rbd::MigrationState state,
                            const std::string &description) {
  int r = cls_client::migration_set_state(&image_ctx->md_ctx,
                                          image_ctx->header_oid,
                                          state, description);
  if (r < 0) {
    lderr(m_cct) << "failed to set " << image_description << " "
                 << "migration header: " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

template <typename I>
int Migration<I>::set_state(cls::rbd::MigrationState state,
                            const std::string &description) {
  int r;
  if (m_src_image_ctx != nullptr) {
    r = set_state(m_src_image_ctx, "source", state, description);
    if (r < 0) {
      return r;
    }
  }

  r = set_state(m_dst_image_ctx, "destination", state, description);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::list_src_snaps(I* image_ctx,
                                 std::vector<librbd::snap_info_t> *snaps) {
  ldout(m_cct, 10) << dendl;

  int r = Snapshot<I>::list(image_ctx, *snaps);
  if (r < 0) {
    lderr(m_cct) << "failed listing snapshots: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto &snap : *snaps) {
    librbd::snap_namespace_type_t namespace_type;
    r = Snapshot<I>::get_namespace_type(image_ctx, snap.id,
                                        &namespace_type);
    if (r < 0) {
      lderr(m_cct) << "error getting snap namespace type: " << cpp_strerror(r)
                   << dendl;
      return r;
    }

    if (namespace_type != RBD_SNAP_NAMESPACE_TYPE_USER) {
      if (namespace_type == RBD_SNAP_NAMESPACE_TYPE_TRASH) {
        lderr(m_cct) << "image has snapshots with linked clones that must be "
                     << "deleted or flattened before the image can be migrated"
                     << dendl;
      } else {
        lderr(m_cct) << "image has non-user type snapshots "
                     << "that are not supported by migration" << dendl;
      }
      return -EBUSY;
    }
  }

  return 0;
}

template <typename I>
int Migration<I>::validate_src_snaps(I* image_ctx) {
  ldout(m_cct, 10) << dendl;

  std::vector<librbd::snap_info_t> snaps;
  int r = list_src_snaps(image_ctx, &snaps);
  if (r < 0) {
    return r;
  }

  uint64_t dst_features = 0;
  r = m_image_options.get(RBD_IMAGE_OPTION_FEATURES, &dst_features);
  ceph_assert(r == 0);

  if (!image_ctx->test_features(RBD_FEATURE_LAYERING)) {
    return 0;
  }

  for (auto &snap : snaps) {
    std::shared_lock image_locker{image_ctx->image_lock};
    cls::rbd::ParentImageSpec parent_spec{image_ctx->md_ctx.get_id(),
                                          image_ctx->md_ctx.get_namespace(),
                                          image_ctx->id, snap.id};
    std::vector<librbd::linked_image_spec_t> child_images;
    r = api::Image<I>::list_children(image_ctx, parent_spec,
                                     &child_images);
    if (r < 0) {
      lderr(m_cct) << "failed listing children: " << cpp_strerror(r)
                   << dendl;
      return r;
    }
    if (!child_images.empty()) {
      ldout(m_cct, 1) << image_ctx->name << "@" << snap.name
                      << " has children" << dendl;

      if ((dst_features & RBD_FEATURE_LAYERING) == 0) {
        lderr(m_cct) << "can't migrate to destination without layering feature: "
                     << "image has children" << dendl;
        return -EINVAL;
      }
    }
  }

  return 0;
}


template <typename I>
int Migration<I>::set_src_migration(I* image_ctx) {
  ldout(m_cct, 10) << dendl;

  image_ctx->ignore_migrating = true;

  int r = cls_client::migration_set(&image_ctx->md_ctx, image_ctx->header_oid,
                                    m_src_migration_spec);
  if (r < 0) {
    lderr(m_cct) << "failed to set source migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  image_ctx->notify_update();

  return 0;
}

template <typename I>
int Migration<I>::remove_migration(I *image_ctx) {
  ldout(m_cct, 10) << dendl;

  int r;

  r = cls_client::migration_remove(&image_ctx->md_ctx, image_ctx->header_oid);
  if (r == -ENOENT) {
    r = 0;
  }
  if (r < 0) {
    lderr(m_cct) << "failed removing migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  image_ctx->notify_update();

  return 0;
}

template <typename I>
int Migration<I>::unlink_src_image(I* image_ctx) {
  if (image_ctx->old_format) {
    return v1_unlink_src_image(image_ctx);
  } else {
    return v2_unlink_src_image(image_ctx);
  }
}

template <typename I>
int Migration<I>::v1_unlink_src_image(I* image_ctx) {
  ldout(m_cct, 10) << dendl;

  std::shared_lock image_locker{image_ctx->image_lock};
  int r = tmap_rm(image_ctx->md_ctx, image_ctx->name);
  if (r < 0) {
    lderr(m_cct) << "failed removing " << image_ctx->name << " from tmap: "
                 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::v2_unlink_src_image(I* image_ctx) {
  ldout(m_cct, 10) << dendl;

  image_ctx->owner_lock.lock_shared();
  if (image_ctx->exclusive_lock != nullptr &&
      image_ctx->exclusive_lock->is_lock_owner()) {
    C_SaferCond ctx;
    image_ctx->exclusive_lock->release_lock(&ctx);
    image_ctx->owner_lock.unlock_shared();
    int r = ctx.wait();
     if (r < 0) {
      lderr(m_cct) << "error releasing exclusive lock: " << cpp_strerror(r)
                   << dendl;
      return r;
     }
  } else {
    image_ctx->owner_lock.unlock_shared();
  }

  int r = Trash<I>::move(image_ctx->md_ctx, RBD_TRASH_IMAGE_SOURCE_MIGRATION,
                         image_ctx->name, 0);
  if (r < 0) {
    lderr(m_cct) << "failed moving image to trash: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::relink_src_image(I* image_ctx) {
  if (image_ctx->old_format) {
    return v1_relink_src_image(image_ctx);
  } else {
    return v2_relink_src_image(image_ctx);
  }
}

template <typename I>
int Migration<I>::v1_relink_src_image(I* image_ctx) {
  ldout(m_cct, 10) << dendl;

  std::shared_lock image_locker{image_ctx->image_lock};
  int r = tmap_set(image_ctx->md_ctx, image_ctx->name);
  if (r < 0) {
    lderr(m_cct) << "failed adding " << image_ctx->name << " to tmap: "
                 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::v2_relink_src_image(I* image_ctx) {
  ldout(m_cct, 10) << dendl;

  std::shared_lock image_locker{image_ctx->image_lock};
  int r = Trash<I>::restore(image_ctx->md_ctx,
                            {cls::rbd::TRASH_IMAGE_SOURCE_MIGRATION},
                            image_ctx->id, image_ctx->name);
  if (r < 0) {
    lderr(m_cct) << "failed restoring image from trash: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::create_dst_image(I** image_ctx) {
  ldout(m_cct, 10) << dendl;

  uint64_t size;
  cls::rbd::ParentImageSpec parent_spec;
  {
    std::shared_lock image_locker{m_src_image_ctx->image_lock};
    size = m_src_image_ctx->size;

    // use oldest snapshot or HEAD for parent spec
    if (!m_src_image_ctx->snap_info.empty()) {
      parent_spec = m_src_image_ctx->snap_info.begin()->second.parent.spec;
    } else {
      parent_spec = m_src_image_ctx->parent_md.spec;
    }
  }

  ConfigProxy config{m_cct->_conf};
  api::Config<I>::apply_pool_overrides(m_dst_io_ctx, &config);

  uint64_t mirror_image_mode;
  if (m_image_options.get(RBD_IMAGE_OPTION_MIRROR_IMAGE_MODE,
                          &mirror_image_mode) == 0) {
    m_mirroring = true;
    m_mirror_image_mode = static_cast<cls::rbd::MirrorImageMode>(
      mirror_image_mode);
    m_image_options.unset(RBD_IMAGE_OPTION_MIRROR_IMAGE_MODE);
  }

  int r;
  C_SaferCond on_create;
  librados::IoCtx parent_io_ctx;
  if (parent_spec.pool_id == -1) {
    auto *req = image::CreateRequest<I>::create(
      config, m_dst_io_ctx, m_dst_image_name, m_dst_image_id, size,
      m_image_options, image::CREATE_FLAG_SKIP_MIRROR_ENABLE,
      cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
      m_src_image_ctx->op_work_queue, &on_create);
    req->send();
  } else {
    r = util::create_ioctx(m_src_image_ctx->md_ctx, "parent image",
                           parent_spec.pool_id, parent_spec.pool_namespace,
                           &parent_io_ctx);
    if (r < 0) {
      return r;
    }

    auto *req = image::CloneRequest<I>::create(
      config, parent_io_ctx, parent_spec.image_id, "", {}, parent_spec.snap_id,
      m_dst_io_ctx, m_dst_image_name, m_dst_image_id, m_image_options,
      cls::rbd::MIRROR_IMAGE_MODE_JOURNAL, "", "",
      m_src_image_ctx->op_work_queue, &on_create);
    req->send();
  }

  r = on_create.wait();
  if (r < 0) {
    lderr(m_cct) << "header creation failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  auto dst_image_ctx = *image_ctx;
  dst_image_ctx->id = m_dst_image_id;
  *image_ctx = nullptr; // prevent prepare from cleaning up the ImageCtx

  r = dst_image_ctx->state->open(OPEN_FLAG_IGNORE_MIGRATING);
  if (r < 0) {
    lderr(m_cct) << "failed to open newly created header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  BOOST_SCOPE_EXIT_TPL(dst_image_ctx) {
    dst_image_ctx->state->close();
  } BOOST_SCOPE_EXIT_END;

  {
    std::shared_lock owner_locker{dst_image_ctx->owner_lock};
    r = dst_image_ctx->operations->prepare_image_update(
      exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL, true);
    if (r < 0) {
      lderr(m_cct) << "cannot obtain exclusive lock" << dendl;
      return r;
    }
    if (dst_image_ctx->exclusive_lock != nullptr) {
      dst_image_ctx->exclusive_lock->block_requests(0);
    }
  }

  SnapSeqs snap_seqs;

  C_SaferCond on_snapshot_copy;
  auto snapshot_copy_req = librbd::deep_copy::SnapshotCopyRequest<I>::create(
      m_src_image_ctx, dst_image_ctx, 0, CEPH_NOSNAP, 0, m_flatten,
      m_src_image_ctx->op_work_queue, &snap_seqs, &on_snapshot_copy);
  snapshot_copy_req->send();
  r = on_snapshot_copy.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to copy snapshots: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (!m_src_image_ctx->header_oid.empty()) {
    C_SaferCond on_metadata_copy;
    auto metadata_copy_req = librbd::deep_copy::MetadataCopyRequest<I>::create(
        m_src_image_ctx, dst_image_ctx, &on_metadata_copy);
    metadata_copy_req->send();
    r = on_metadata_copy.wait();
    if (r < 0) {
      lderr(m_cct) << "failed to copy metadata: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  m_dst_migration_spec.snap_seqs = snap_seqs;
  m_dst_migration_spec.overlap = size;
  m_dst_migration_spec.mirroring = m_mirroring;
  m_dst_migration_spec.mirror_image_mode = m_mirror_image_mode;
  m_dst_migration_spec.flatten = m_flatten;
  r = cls_client::migration_set(&m_dst_io_ctx, m_dst_header_oid,
                                m_dst_migration_spec);
  if (r < 0) {
    lderr(m_cct) << "failed to set migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  if (m_dst_migration_spec.source_spec.empty()) {
    r = update_group(m_src_image_ctx, dst_image_ctx);
    if (r < 0) {
      return r;
    }

    r = set_state(m_src_image_ctx, "source",
                  cls::rbd::MIGRATION_STATE_PREPARED, "");
    if (r < 0) {
      return r;
    }
  }

  r = set_state(dst_image_ctx, "destination",
                cls::rbd::MIGRATION_STATE_PREPARED, "");
  if (r < 0) {
    return r;
  }

  if (m_dst_migration_spec.source_spec.empty()) {
    r = dst_image_ctx->state->refresh();
    if (r < 0) {
      lderr(m_cct) << "failed to refresh destination image: " << cpp_strerror(r)
                   << dendl;
      return r;
    }

    r = relink_children(m_src_image_ctx, dst_image_ctx);
    if (r < 0) {
      return r;
    }
  }

  return 0;
}

template <typename I>
int Migration<I>::remove_group(I *image_ctx, group_info_t *group_info) {
  int r = librbd::api::Group<I>::image_get_group(image_ctx, group_info);
  if (r < 0) {
    lderr(m_cct) << "failed to get image group: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (group_info->pool == -1) {
    return -ENOENT;
  }

  ceph_assert(!image_ctx->id.empty());

  ldout(m_cct, 10) << dendl;

  IoCtx group_ioctx;
  r = util::create_ioctx(image_ctx->md_ctx, "group", group_info->pool, {},
                         &group_ioctx);
  if (r < 0) {
    return r;
  }

  r = librbd::api::Group<I>::image_remove_by_id(group_ioctx,
                                                group_info->name.c_str(),
                                                image_ctx->md_ctx,
                                                image_ctx->id.c_str());
  if (r < 0) {
    lderr(m_cct) << "failed to remove image from group: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::add_group(I *image_ctx, group_info_t &group_info) {
  if (group_info.pool == -1) {
    return 0;
  }

  ldout(m_cct, 10) << dendl;

  IoCtx group_ioctx;
  int r = util::create_ioctx(image_ctx->md_ctx, "group", group_info.pool, {},
                             &group_ioctx);
  if (r < 0) {
    return r;
  }

  r = librbd::api::Group<I>::image_add(group_ioctx, group_info.name.c_str(),
                                       image_ctx->md_ctx,
                                       image_ctx->name.c_str());
  if (r < 0) {
    lderr(m_cct) << "failed to add image to group: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::update_group(I *from_image_ctx, I *to_image_ctx) {
  ldout(m_cct, 10) << dendl;

  group_info_t group_info;

  int r = remove_group(from_image_ctx, &group_info);
  if (r < 0) {
    return r == -ENOENT ? 0 : r;
  }

  r = add_group(to_image_ctx, group_info);
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::disable_mirroring(
    I *image_ctx, bool *was_enabled,
    cls::rbd::MirrorImageMode *mirror_image_mode) {
  *was_enabled = false;

  cls::rbd::MirrorImage mirror_image;
  int r = cls_client::mirror_image_get(&image_ctx->md_ctx, image_ctx->id,
                                       &mirror_image);
  if (r == -ENOENT) {
    ldout(m_cct, 10) << "mirroring is not enabled for this image" << dendl;
    return 0;
  }

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirror image: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  if (mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
    *was_enabled = true;
    *mirror_image_mode = mirror_image.mode;
  }

  ldout(m_cct, 10) << dendl;

  C_SaferCond ctx;
  auto req = mirror::DisableRequest<I>::create(image_ctx, false, true, &ctx);
  req->send();
  r = ctx.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to disable mirroring: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  m_src_migration_spec.mirroring = true;

  return 0;
}

template <typename I>
int Migration<I>::enable_mirroring(
    I *image_ctx, bool was_enabled,
    cls::rbd::MirrorImageMode mirror_image_mode) {
  cls::rbd::MirrorMode mirror_mode;
  int r = cls_client::mirror_mode_get(&image_ctx->md_ctx, &mirror_mode);
  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to retrieve mirror mode: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  if (mirror_mode == cls::rbd::MIRROR_MODE_DISABLED) {
    ldout(m_cct, 10) << "mirroring is not enabled for destination pool"
                     << dendl;
    return 0;
  }
  if (mirror_mode == cls::rbd::MIRROR_MODE_IMAGE && !was_enabled) {
    ldout(m_cct, 10) << "mirroring is not enabled for image" << dendl;
    return 0;
  }

  ldout(m_cct, 10) << dendl;

  C_SaferCond ctx;
  auto req = mirror::EnableRequest<I>::create(
    image_ctx, mirror_image_mode, "", false, &ctx);
  req->send();
  r = ctx.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to enable mirroring: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

// When relinking children we should be careful as it my be interrupted
// at any moment by some reason and we may end up in an inconsistent
// state, which we have to be able to fix with "migration abort". Below
// are all possible states during migration (P1 - source parent, P2 -
// destination parent, C - child):
//
//   P1  P2    P1  P2    P1  P2    P1  P2
//   ^\         \  ^      \ /^        /^
//    \v         v/        v/        v/
//     C         C         C         C
//
//     1         2         3         4
//
// (1) and (4) are the initial and the final consistent states. (2)
// and (3) are intermediate inconsistent states that have to be fixed
// by relink_children running in "migration abort" mode. For this, it
// scans P2 for all children attached and relinks (fixes) states (3)
// and (4) to state (1). Then it scans P1 for remaining children and
// fixes the states (2).

template <typename I>
int Migration<I>::relink_children(I *from_image_ctx, I *to_image_ctx) {
  ldout(m_cct, 10) << dendl;

  bool migration_abort = (to_image_ctx == m_src_image_ctx);

  std::vector<librbd::snap_info_t> snaps;
  int r = list_src_snaps(
    migration_abort ? to_image_ctx : from_image_ctx, &snaps);
  if (r < 0) {
    return r;
  }

  for (auto it = snaps.begin(); it != snaps.end(); it++) {
    auto &snap = *it;
    std::vector<librbd::linked_image_spec_t> src_child_images;

    if (from_image_ctx != m_src_image_ctx) {
      ceph_assert(migration_abort);

      // We run list snaps against the src image to get only those snapshots
      // that are migrated. If the "from" image is not the src image
      // (abort migration case), we need to remap snap ids.
      // Also collect the list of the children currently attached to the
      // source, so we could make a proper decision later about relinking.

      std::shared_lock src_image_locker{to_image_ctx->image_lock};
      cls::rbd::ParentImageSpec src_parent_spec{to_image_ctx->md_ctx.get_id(),
                                                to_image_ctx->md_ctx.get_namespace(),
                                                to_image_ctx->id, snap.id};
      r = api::Image<I>::list_children(to_image_ctx, src_parent_spec,
                                       &src_child_images);
      if (r < 0) {
        lderr(m_cct) << "failed listing children: " << cpp_strerror(r)
                     << dendl;
        return r;
      }

      std::shared_lock image_locker{from_image_ctx->image_lock};
      snap.id = from_image_ctx->get_snap_id(cls::rbd::UserSnapshotNamespace(),
                                            snap.name);
      if (snap.id == CEPH_NOSNAP) {
        ldout(m_cct, 5) << "skipping snapshot " << snap.name << dendl;
        continue;
      }
    }

    std::vector<librbd::linked_image_spec_t> child_images;
    {
      std::shared_lock image_locker{from_image_ctx->image_lock};
      cls::rbd::ParentImageSpec parent_spec{from_image_ctx->md_ctx.get_id(),
                                            from_image_ctx->md_ctx.get_namespace(),
                                            from_image_ctx->id, snap.id};
      r = api::Image<I>::list_children(from_image_ctx, parent_spec,
                                       &child_images);
      if (r < 0) {
        lderr(m_cct) << "failed listing children: " << cpp_strerror(r)
                     << dendl;
        return r;
      }
    }

    for (auto &child_image : child_images) {
      r = relink_child(from_image_ctx, to_image_ctx, snap, child_image,
                       migration_abort, true);
      if (r < 0) {
        return r;
      }

      src_child_images.erase(std::remove(src_child_images.begin(),
                                         src_child_images.end(), child_image),
                             src_child_images.end());
    }

    for (auto &child_image : src_child_images) {
      r = relink_child(from_image_ctx, to_image_ctx, snap, child_image,
                       migration_abort, false);
      if (r < 0) {
        return r;
      }
    }
  }

  return 0;
}

template <typename I>
int Migration<I>::relink_child(I *from_image_ctx, I *to_image_ctx,
                               const librbd::snap_info_t &from_snap,
                               const librbd::linked_image_spec_t &child_image,
                               bool migration_abort, bool reattach_child) {
  ldout(m_cct, 10) << from_snap.name << " " << child_image.pool_name << "/"
                   << child_image.pool_namespace << "/"
                   << child_image.image_name << " (migration_abort="
                   << migration_abort << ", reattach_child=" << reattach_child
                   << ")" << dendl;

  librados::snap_t to_snap_id;
  {
    std::shared_lock image_locker{to_image_ctx->image_lock};
    to_snap_id = to_image_ctx->get_snap_id(cls::rbd::UserSnapshotNamespace(),
                                             from_snap.name);
    if (to_snap_id == CEPH_NOSNAP) {
      lderr(m_cct) << "no snapshot " << from_snap.name << " on destination image"
                   << dendl;
      return -ENOENT;
    }
  }

  librados::IoCtx child_io_ctx;
  int r = util::create_ioctx(to_image_ctx->md_ctx,
                             "child image " + child_image.image_name,
                             child_image.pool_id, child_image.pool_namespace,
                             &child_io_ctx);
  if (r < 0) {
    return r;
  }

  I *child_image_ctx = I::create("", child_image.image_id, nullptr,
                                 child_io_ctx, false);
  r = child_image_ctx->state->open(OPEN_FLAG_SKIP_OPEN_PARENT);
  if (r < 0) {
    lderr(m_cct) << "failed to open child image: " << cpp_strerror(r) << dendl;
    return r;
  }
  BOOST_SCOPE_EXIT_TPL(child_image_ctx) {
    child_image_ctx->state->close();
  } BOOST_SCOPE_EXIT_END;

  uint32_t clone_format = 1;
  if (child_image_ctx->test_op_features(RBD_OPERATION_FEATURE_CLONE_CHILD)) {
    clone_format = 2;
  }

  cls::rbd::ParentImageSpec parent_spec;
  uint64_t parent_overlap;
  {
    std::shared_lock image_locker{child_image_ctx->image_lock};

    // use oldest snapshot or HEAD for parent spec
    if (!child_image_ctx->snap_info.empty()) {
      parent_spec = child_image_ctx->snap_info.begin()->second.parent.spec;
      parent_overlap = child_image_ctx->snap_info.begin()->second.parent.overlap;
    } else {
      parent_spec = child_image_ctx->parent_md.spec;
      parent_overlap = child_image_ctx->parent_md.overlap;
    }
  }

  if (migration_abort &&
      parent_spec.pool_id == to_image_ctx->md_ctx.get_id() &&
      parent_spec.pool_namespace == to_image_ctx->md_ctx.get_namespace() &&
      parent_spec.image_id == to_image_ctx->id &&
      parent_spec.snap_id == to_snap_id) {
    ldout(m_cct, 10) << "no need for parent re-attach" << dendl;
  } else {
    if (parent_spec.pool_id != from_image_ctx->md_ctx.get_id() ||
        parent_spec.pool_namespace != from_image_ctx->md_ctx.get_namespace() ||
        parent_spec.image_id != from_image_ctx->id ||
        parent_spec.snap_id != from_snap.id) {
      lderr(m_cct) << "parent is not source image: " << parent_spec.pool_id
                   << "/" << parent_spec.pool_namespace << "/"
                   << parent_spec.image_id << "@" << parent_spec.snap_id
                   << dendl;
      return -ESTALE;
    }

    parent_spec.pool_id = to_image_ctx->md_ctx.get_id();
    parent_spec.pool_namespace = to_image_ctx->md_ctx.get_namespace();
    parent_spec.image_id = to_image_ctx->id;
    parent_spec.snap_id = to_snap_id;

    C_SaferCond on_reattach_parent;
    auto reattach_parent_req = image::AttachParentRequest<I>::create(
      *child_image_ctx, parent_spec, parent_overlap, true, &on_reattach_parent);
    reattach_parent_req->send();
    r = on_reattach_parent.wait();
    if (r < 0) {
      lderr(m_cct) << "failed to re-attach parent: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if (reattach_child) {
    C_SaferCond on_reattach_child;
    auto reattach_child_req = image::AttachChildRequest<I>::create(
      child_image_ctx, to_image_ctx, to_snap_id, from_image_ctx, from_snap.id,
      clone_format, &on_reattach_child);
    reattach_child_req->send();
    r = on_reattach_child.wait();
    if (r < 0) {
      lderr(m_cct) << "failed to re-attach child: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  child_image_ctx->notify_update();

  return 0;
}

template <typename I>
int Migration<I>::remove_src_image(I** image_ctx) {
  ldout(m_cct, 10) << dendl;

  auto src_image_ctx = *image_ctx;

  std::vector<librbd::snap_info_t> snaps;
  int r = list_src_snaps(src_image_ctx, &snaps);
  if (r < 0) {
    return r;
  }

  for (auto it = snaps.rbegin(); it != snaps.rend(); it++) {
    auto &snap = *it;

    librbd::NoOpProgressContext prog_ctx;
    int r = Snapshot<I>::remove(src_image_ctx, snap.name.c_str(),
                                RBD_SNAP_REMOVE_UNPROTECT, prog_ctx);
    if (r < 0) {
      lderr(m_cct) << "failed removing source image snapshot '" << snap.name
                   << "': " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  ceph_assert(src_image_ctx->ignore_migrating);

  auto asio_engine = src_image_ctx->asio_engine;
  auto src_image_id = src_image_ctx->id;
  librados::IoCtx src_io_ctx(src_image_ctx->md_ctx);

  C_SaferCond on_remove;
  auto req = librbd::image::RemoveRequest<I>::create(
      src_io_ctx, src_image_ctx, false, true, *m_prog_ctx,
      asio_engine->get_work_queue(), &on_remove);
  req->send();
  r = on_remove.wait();

  *image_ctx = nullptr;

  // For old format image it will return -ENOENT due to expected
  // tmap_rm failure at the end.
  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed removing source image: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  if (!src_image_id.empty()) {
    r = cls_client::trash_remove(&src_io_ctx, src_image_id);
    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "error removing image " << src_image_id
                   << " from rbd_trash object" << dendl;
    }
  }

  return 0;
}

template <typename I>
int Migration<I>::revert_data(I* src_image_ctx, I* dst_image_ctx,
                              ProgressContext* prog_ctx) {
  ldout(m_cct, 10) << dendl;

  cls::rbd::MigrationSpec migration_spec;
  int r = cls_client::migration_get(&src_image_ctx->md_ctx,
                                    src_image_ctx->header_oid,
                                    &migration_spec);

  if (r < 0) {
    lderr(m_cct) << "failed retrieving migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  if (migration_spec.header_type != cls::rbd::MIGRATION_HEADER_TYPE_DST) {
    lderr(m_cct) << "unexpected migration header type: "
                 << migration_spec.header_type << dendl;
    return -EINVAL;
  }

  uint64_t src_snap_id_start = 0;
  uint64_t src_snap_id_end = CEPH_NOSNAP;
  uint64_t dst_snap_id_start = 0;
  if (!migration_spec.snap_seqs.empty()) {
    src_snap_id_start = migration_spec.snap_seqs.rbegin()->second;
  }

  // we only care about the HEAD revision so only add a single mapping to
  // represent the most recent state
  SnapSeqs snap_seqs;
  snap_seqs[CEPH_NOSNAP] = CEPH_NOSNAP;

  ldout(m_cct, 20) << "src_snap_id_start=" << src_snap_id_start << ", "
                   << "src_snap_id_end=" << src_snap_id_end << ", "
                   << "dst_snap_id_start=" << dst_snap_id_start << ", "
                   << "snap_seqs=" << snap_seqs << dendl;

  C_SaferCond ctx;
  deep_copy::ProgressHandler progress_handler(prog_ctx);
  auto request = deep_copy::ImageCopyRequest<I>::create(
    src_image_ctx, dst_image_ctx, src_snap_id_start, src_snap_id_end,
    dst_snap_id_start, false, {}, snap_seqs, &progress_handler, &ctx);
  request->send();

  r = ctx.wait();
  if (r < 0) {
    lderr(m_cct) << "error reverting destination image data blocks back to "
                 << "source image: " << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Migration<librbd::ImageCtx>;
