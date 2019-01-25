// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Migration.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
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
#include "librbd/deep_copy/MetadataCopyRequest.h"
#include "librbd/deep_copy/SnapshotCopyRequest.h"
#include "librbd/image/CloneRequest.h"
#include "librbd/image/CreateRequest.h"
#include "librbd/image/ListWatchersRequest.h"
#include "librbd/image/RemoveRequest.h"
#include "librbd/internal.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/mirror/DisableRequest.h"
#include "librbd/mirror/EnableRequest.h"

#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Migration: " << __func__ << ": "

namespace librbd {
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
      m_lock(util::unique_lock_name("librbd::api::MigrationProgressContext",
                                    this)) {
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
  mutable Mutex m_lock;
  Cond m_cond;
  std::string m_state_description;
  bool m_pending_update = false;
  int m_in_flight_state_updates = 0;

  void send_state_description_update(const std::string &description) {
    Mutex::Locker locker(m_lock);

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

    ceph_assert(m_lock.is_locked());

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

    Mutex::Locker locker(m_lock);

    m_in_flight_state_updates--;

    if (r < 0) {
      lderr(m_cct) << "failed to update migration state: " << cpp_strerror(r)
                   << dendl;
    } else if (m_pending_update) {
      set_state_description();
      m_pending_update = false;
    } else {
      m_cond.Signal();
    }
  }

  void wait_for_in_flight_updates() {
    Mutex::Locker locker(m_lock);

    ldout(m_cct, 20) << "m_in_flight_state_updates="
                     << m_in_flight_state_updates << dendl;

    m_pending_update = false;
    while (m_in_flight_state_updates > 0) {
      m_cond.Wait(m_lock);
    }
  }
};

int trash_search(librados::IoCtx &io_ctx, rbd_trash_image_source_t source,
                 const std::string &image_name, std::string *image_id) {
  std::vector<trash_image_info_t> entries;

  int r = Trash<>::list(io_ctx, entries);
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
int open_source_image(librados::IoCtx& io_ctx, const std::string &image_name,
                      I **src_image_ctx, librados::IoCtx *dst_io_ctx,
                      std::string *dst_image_name, std::string *dst_image_id,
                      bool *flatten, bool *mirroring,
                      cls::rbd::MigrationState *state,
                      std::string *state_description) {
  CephContext* cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  librados::IoCtx src_io_ctx;
  std::string src_image_name;
  std::string src_image_id;
  cls::rbd::MigrationSpec migration_spec;
  I *image_ctx = I::create(image_name, "", nullptr, io_ctx, false);

  ldout(cct, 10) << "trying to open image by name " << io_ctx.get_pool_name()
                 << "/" << image_name << dendl;

  int r = image_ctx->state->open(OPEN_FLAG_IGNORE_MIGRATING);
  if (r < 0) {
    if (r != -ENOENT) {
      lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
      return r;
    }
    image_ctx = nullptr;
  }

  BOOST_SCOPE_EXIT_TPL(&r, &image_ctx) {
    if (r != 0 && image_ctx != nullptr) {
      image_ctx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;

  if (r == 0) {
    // The opened image is either a source (then just proceed) or a
    // destination (then look for the source image id in the migration
    // header).

    r = cls_client::migration_get(&image_ctx->md_ctx, image_ctx->header_oid,
                                  &migration_spec);

    if (r < 0) {
      lderr(cct) << "failed retrieving migration header: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    ldout(cct, 10) << "migration spec: " << migration_spec << dendl;

    if (migration_spec.header_type != cls::rbd::MIGRATION_HEADER_TYPE_SRC &&
        migration_spec.header_type != cls::rbd::MIGRATION_HEADER_TYPE_DST) {
        lderr(cct) << "unexpected migration header type: "
                   << migration_spec.header_type << dendl;
        r = -EINVAL;
        return r;
    }

    if (migration_spec.header_type == cls::rbd::MIGRATION_HEADER_TYPE_DST) {
      ldout(cct, 10) << "the destination image is opened" << dendl;

      // Close and look for the source image.
      r = image_ctx->state->close();
      image_ctx = nullptr;
      if (r < 0) {
        lderr(cct) << "failed closing image: " << cpp_strerror(r)
                   << dendl;
        return r;
      }

      r = util::create_ioctx(io_ctx, "source image", migration_spec.pool_id,
                             migration_spec.pool_namespace, &src_io_ctx);
      if (r < 0) {
        return r;
      }

      src_image_name = migration_spec.image_name;
      src_image_id = migration_spec.image_id;
    } else {
      ldout(cct, 10) << "the source image is opened" << dendl;
    }
  } else {
    assert (r == -ENOENT);

    ldout(cct, 10) << "source image is not found. Trying trash" << dendl;

    r = trash_search(io_ctx, RBD_TRASH_IMAGE_SOURCE_MIGRATION, image_name,
                     &src_image_id);
    if (r < 0) {
      lderr(cct) << "failed to determine image id: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    ldout(cct, 10) << "source image id from trash: " << src_image_id << dendl;

    src_io_ctx.dup(io_ctx);
  }

  if (image_ctx == nullptr) {
    int flags = OPEN_FLAG_IGNORE_MIGRATING;

    if (src_image_id.empty()) {
      ldout(cct, 20) << "trying to open v1 image by name "
                     << src_io_ctx.get_pool_name() << "/" << src_image_name
                     << dendl;

      flags |= OPEN_FLAG_OLD_FORMAT;
    } else {
      ldout(cct, 20) << "trying to open v2 image by id "
                     << src_io_ctx.get_pool_name() << "/" << src_image_id
                     << dendl;
    }

    image_ctx = I::create(src_image_name, src_image_id, nullptr, src_io_ctx,
                          false);
    r = image_ctx->state->open(flags);
    if (r < 0) {
      lderr(cct) << "failed to open source image " << src_io_ctx.get_pool_name()
                 << "/" << (src_image_id.empty() ? src_image_name : src_image_id)
                 << ": " << cpp_strerror(r) << dendl;
      image_ctx = nullptr;
      return r;
    }

    r = cls_client::migration_get(&image_ctx->md_ctx, image_ctx->header_oid,
                                  &migration_spec);
    if (r < 0) {
      lderr(cct) << "failed retrieving migration header: " << cpp_strerror(r)
                 << dendl;
      return r;
    }

    ldout(cct, 20) << "migration spec: " << migration_spec << dendl;
  }

  r = util::create_ioctx(image_ctx->md_ctx, "source image",
                         migration_spec.pool_id, migration_spec.pool_namespace,
                         dst_io_ctx);
  if (r < 0) {
    return r;
  }

  *src_image_ctx = image_ctx;
  *dst_image_name = migration_spec.image_name;
  *dst_image_id = migration_spec.image_id;
  *flatten = migration_spec.flatten;
  *mirroring = migration_spec.mirroring;
  *state = migration_spec.state;
  *state_description = migration_spec.state_description;

  return 0;
}

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

  auto image_ctx = I::create(image_name, "", nullptr, io_ctx, false);
  int r = image_ctx->state->open(0);
  if (r < 0) {
    lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
    return r;
  }
  BOOST_SCOPE_EXIT_TPL(image_ctx) {
    image_ctx->state->close();
  } BOOST_SCOPE_EXIT_END;

  std::list<obj_watch_t> watchers;
  int flags = librbd::image::LIST_WATCHERS_FILTER_OUT_MY_INSTANCE |
              librbd::image::LIST_WATCHERS_FILTER_OUT_MIRROR_INSTANCES;
  C_SaferCond on_list_watchers;
  auto list_watchers_request = librbd::image::ListWatchersRequest<I>::create(
      *image_ctx, flags, &watchers, &on_list_watchers);
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
    RWLock::RLocker snap_locker(image_ctx->snap_lock);
    features = image_ctx->features;
  }
  opts.get(RBD_IMAGE_OPTION_FEATURES, &features);
  if ((features & ~RBD_FEATURES_ALL) != 0) {
    lderr(cct) << "librbd does not support requested features" << dendl;
    return -ENOSYS;
  }
  features &= ~RBD_FEATURES_INTERNAL;
  features |= RBD_FEATURE_MIGRATING;
  opts.set(RBD_IMAGE_OPTION_FEATURES, features);

  uint64_t order = image_ctx->order;
  if (opts.get(RBD_IMAGE_OPTION_ORDER, &order) != 0) {
    opts.set(RBD_IMAGE_OPTION_ORDER, order);
  }
  r = image::CreateRequest<I>::validate_order(cct, order);
  if (r < 0) {
    return r;
  }

  uint64_t stripe_unit = image_ctx->stripe_unit;
  if (opts.get(RBD_IMAGE_OPTION_STRIPE_UNIT, &stripe_unit) != 0) {
    opts.set(RBD_IMAGE_OPTION_STRIPE_UNIT, stripe_unit);
  }
  uint64_t stripe_count = image_ctx->stripe_count;
  if (opts.get(RBD_IMAGE_OPTION_STRIPE_COUNT, &stripe_count) != 0) {
    opts.set(RBD_IMAGE_OPTION_STRIPE_COUNT, stripe_count);
  }

  uint64_t flatten = 0;
  if (opts.get(RBD_IMAGE_OPTION_FLATTEN, &flatten) == 0) {
    opts.unset(RBD_IMAGE_OPTION_FLATTEN);
  }

  ldout(cct, 20) << "updated opts=" << opts << dendl;

  Migration migration(image_ctx, dest_io_ctx, dest_image_name, "", opts, flatten > 0,
                      false, cls::rbd::MIGRATION_STATE_PREPARING, "", nullptr);
  r = migration.prepare();

  features &= ~RBD_FEATURE_MIGRATING;
  opts.set(RBD_IMAGE_OPTION_FEATURES, features);

  return r;
}

template <typename I>
int Migration<I>::execute(librados::IoCtx& io_ctx,
                          const std::string &image_name,
                          ProgressContext &prog_ctx) {
  CephContext* cct = reinterpret_cast<CephContext *>(io_ctx.cct());

  ldout(cct, 10) << io_ctx.get_pool_name() << "/" << image_name << dendl;

  I *image_ctx;
  librados::IoCtx dest_io_ctx;
  std::string dest_image_name;
  std::string dest_image_id;
  bool flatten;
  bool mirroring;
  cls::rbd::MigrationState state;
  std::string state_description;

  int r = open_source_image(io_ctx, image_name, &image_ctx, &dest_io_ctx,
                            &dest_image_name, &dest_image_id, &flatten,
                            &mirroring, &state, &state_description);
  if (r < 0) {
    return r;
  }

  BOOST_SCOPE_EXIT_TPL(image_ctx) {
    image_ctx->state->close();
  } BOOST_SCOPE_EXIT_END;

  if (state != cls::rbd::MIGRATION_STATE_PREPARED) {
    lderr(cct) << "current migration state is '" << state << "'"
               << " (should be 'prepared')" << dendl;
    return -EINVAL;
  }

  ldout(cct, 5) << "migrating " << image_ctx->md_ctx.get_pool_name() << "/"
                << image_ctx->name << " -> " << dest_io_ctx.get_pool_name()
                << "/" << dest_image_name << dendl;

  ImageOptions opts;
  Migration migration(image_ctx, dest_io_ctx, dest_image_name, dest_image_id,
                      opts, flatten, mirroring, state, state_description,
                      &prog_ctx);
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

  I *image_ctx;
  librados::IoCtx dest_io_ctx;
  std::string dest_image_name;
  std::string dest_image_id;
  bool flatten;
  bool mirroring;
  cls::rbd::MigrationState state;
  std::string state_description;

  int r = open_source_image(io_ctx, image_name, &image_ctx, &dest_io_ctx,
                            &dest_image_name, &dest_image_id, &flatten,
                            &mirroring, &state, &state_description);
  if (r < 0) {
    return r;
  }

  ldout(cct, 5) << "canceling incomplete migration "
                << image_ctx->md_ctx.get_pool_name() << "/" << image_ctx->name
                << " -> " << dest_io_ctx.get_pool_name() << "/" << dest_image_name
                << dendl;

  ImageOptions opts;
  Migration migration(image_ctx, dest_io_ctx, dest_image_name, dest_image_id,
                      opts, flatten, mirroring, state, state_description,
                      &prog_ctx);
  r = migration.abort();

  image_ctx->state->close();

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

  I *image_ctx;
  librados::IoCtx dest_io_ctx;
  std::string dest_image_name;
  std::string dest_image_id;
  bool flatten;
  bool mirroring;
  cls::rbd::MigrationState state;
  std::string state_description;

  int r = open_source_image(io_ctx, image_name, &image_ctx, &dest_io_ctx,
                            &dest_image_name, &dest_image_id, &flatten,
                            &mirroring, &state, &state_description);
  if (r < 0) {
    return r;
  }

  if (state != cls::rbd::MIGRATION_STATE_EXECUTED) {
    lderr(cct) << "current migration state is '" << state << "'"
               << " (should be 'executed')" << dendl;
    image_ctx->state->close();
    return -EINVAL;
  }

  ldout(cct, 5) << "migrating " << image_ctx->md_ctx.get_pool_name() << "/"
                << image_ctx->name << " -> " << dest_io_ctx.get_pool_name()
                << "/" << dest_image_name << dendl;

  ImageOptions opts;
  Migration migration(image_ctx, dest_io_ctx, dest_image_name, dest_image_id,
                      opts, flatten, mirroring, state, state_description,
                      &prog_ctx);
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

  I *image_ctx;
  librados::IoCtx dest_io_ctx;
  std::string dest_image_name;
  std::string dest_image_id;
  bool flatten;
  bool mirroring;
  cls::rbd::MigrationState state;
  std::string state_description;

  int r = open_source_image(io_ctx, image_name, &image_ctx, &dest_io_ctx,
                            &dest_image_name, &dest_image_id, &flatten,
                            &mirroring, &state, &state_description);
  if (r < 0) {
    return r;
  }

  ldout(cct, 5) << "migrating " << image_ctx->md_ctx.get_pool_name() << "/"
                << image_ctx->name << " -> " << dest_io_ctx.get_pool_name()
                << "/" << dest_image_name << dendl;

  ImageOptions opts;
  Migration migration(image_ctx, dest_io_ctx, dest_image_name, dest_image_id,
                      opts, flatten, mirroring, state, state_description,
                      nullptr);
  r = migration.status(status);

  image_ctx->state->close();

  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
Migration<I>::Migration(I *src_image_ctx, librados::IoCtx& dst_io_ctx,
                        const std::string &dstname,
                        const std::string &dst_image_id,
                        ImageOptions& opts, bool flatten, bool mirroring,
                        cls::rbd::MigrationState state,
                        const std::string &state_description,
                        ProgressContext *prog_ctx)
  : m_cct(static_cast<CephContext *>(dst_io_ctx.cct())),
    m_src_image_ctx(src_image_ctx), m_dst_io_ctx(dst_io_ctx),
    m_src_old_format(m_src_image_ctx->old_format),
    m_src_image_name(m_src_image_ctx->old_format ? m_src_image_ctx->name : ""),
    m_src_image_id(m_src_image_ctx->id),
    m_src_header_oid(m_src_image_ctx->header_oid), m_dst_image_name(dstname),
    m_dst_image_id(dst_image_id.empty() ?
                   util::generate_image_id(m_dst_io_ctx) : dst_image_id),
    m_dst_header_oid(util::header_name(m_dst_image_id)), m_image_options(opts),
    m_flatten(flatten), m_mirroring(mirroring), m_prog_ctx(prog_ctx),
    m_src_migration_spec(cls::rbd::MIGRATION_HEADER_TYPE_SRC,
                         m_dst_io_ctx.get_id(), m_dst_io_ctx.get_namespace(),
                         m_dst_image_name, m_dst_image_id, {}, 0, flatten,
                         mirroring, state, state_description),
    m_dst_migration_spec(cls::rbd::MIGRATION_HEADER_TYPE_DST,
                         src_image_ctx->md_ctx.get_id(),
                         src_image_ctx->md_ctx.get_namespace(),
                         m_src_image_ctx->name, m_src_image_ctx->id, {}, 0,
                         flatten, mirroring, state, state_description) {
  m_src_io_ctx.dup(src_image_ctx->md_ctx);
}

template <typename I>
int Migration<I>::prepare() {
  ldout(m_cct, 10) << dendl;

  int r = list_snaps();
  if (r < 0) {
    return r;
  }

  r = disable_mirroring(m_src_image_ctx, &m_mirroring);
  if (r < 0) {
    return r;
  }

  r = unlink_src_image();
  if (r < 0) {
    enable_mirroring(m_src_image_ctx, m_mirroring);
    return r;
  }

  r = set_migration();
  if (r < 0) {
    relink_src_image();
    enable_mirroring(m_src_image_ctx, m_mirroring);
    return r;
  }

  r = create_dst_image();
  if (r < 0) {
    abort();
    return r;
  }

  r = set_state(cls::rbd::MIGRATION_STATE_PREPARED, "");
  if (r < 0) {
    return r;
  }

  ldout(m_cct, 10) << "succeeded" << dendl;

  return 0;
}

template <typename I>
int Migration<I>::execute() {
  ldout(m_cct, 10) << dendl;

  auto dst_image_ctx = I::create(m_dst_image_name, m_dst_image_id, nullptr,
                                 m_dst_io_ctx, false);
  int r = dst_image_ctx->state->open(0);
  if (r < 0) {
    lderr(m_cct) << "failed to open destination image: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  BOOST_SCOPE_EXIT_TPL(dst_image_ctx) {
    dst_image_ctx->state->close();
  } BOOST_SCOPE_EXIT_END;

  r = set_state(cls::rbd::MIGRATION_STATE_EXECUTING, "");
  if (r < 0) {
    return r;
  }

  while (true) {
    MigrationProgressContext prog_ctx(m_src_io_ctx, m_src_header_oid,
                                      cls::rbd::MIGRATION_STATE_EXECUTING,
                                      m_prog_ctx);
    r = dst_image_ctx->operations->migrate(prog_ctx);
    if (r == -EROFS) {
      RWLock::RLocker owner_locker(dst_image_ctx->owner_lock);
      if (dst_image_ctx->exclusive_lock != nullptr &&
          !dst_image_ctx->exclusive_lock->accept_ops()) {
        ldout(m_cct, 5) << "lost exclusive lock, retrying remote" << dendl;
        continue;
      }
    }
    break;
  }
  if (r < 0) {
    lderr(m_cct) << "migration failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = set_state(cls::rbd::MIGRATION_STATE_EXECUTED, "");
  if (r < 0) {
    return r;
  }

  dst_image_ctx->notify_update();

  ldout(m_cct, 10) << "succeeded" << dendl;

  return 0;
}

template <typename I>
int Migration<I>::abort() {
  ldout(m_cct, 10) << dendl;

  int r;

  m_src_image_ctx->owner_lock.get_read();
  if (m_src_image_ctx->exclusive_lock != nullptr &&
      !m_src_image_ctx->exclusive_lock->is_lock_owner()) {
    C_SaferCond ctx;
    m_src_image_ctx->exclusive_lock->acquire_lock(&ctx);
    m_src_image_ctx->owner_lock.put_read();
    r = ctx.wait();
    if (r < 0) {
      lderr(m_cct) << "error acquiring exclusive lock: " << cpp_strerror(r)
                   << dendl;
      return r;
    }
  } else {
    m_src_image_ctx->owner_lock.put_read();
  }

  group_info_t group_info;
  group_info.pool = -1;

  auto dst_image_ctx = I::create(m_dst_image_name, m_dst_image_id, nullptr,
                                 m_dst_io_ctx, false);
  r = dst_image_ctx->state->open(OPEN_FLAG_IGNORE_MIGRATING);
  if (r < 0) {
    ldout(m_cct, 1) << "failed to open destination image: " << cpp_strerror(r)
                    << dendl;
  } else {
    ldout(m_cct, 10) << "removing dst image snapshots" << dendl;

    BOOST_SCOPE_EXIT_TPL(&dst_image_ctx) {
      if (dst_image_ctx != nullptr) {
        dst_image_ctx->state->close();
      }
    } BOOST_SCOPE_EXIT_END;

    std::vector<librbd::snap_info_t> snaps;
    r = snap_list(dst_image_ctx, snaps);
    if (r < 0) {
      lderr(m_cct) << "failed listing snapshots: " << cpp_strerror(r)
                   << dendl;
      return r;
    }

    for (auto &snap : snaps) {
      librbd::NoOpProgressContext prog_ctx;
      int r = snap_remove(dst_image_ctx, snap.name.c_str(), 0, prog_ctx);
      if (r < 0) {
        lderr(m_cct) << "failed removing snapshot: " << cpp_strerror(r)
                     << dendl;
        return r;
      }
    }

    ldout(m_cct, 10) << "removing group" << dendl;

    r = remove_group(dst_image_ctx, &group_info);
    if (r < 0 && r != -ENOENT) {
      return r;
    }

    ldout(m_cct, 10) << "removing dst image" << dendl;

    ceph_assert(dst_image_ctx->ignore_migrating);

    ThreadPool *thread_pool;
    ContextWQ *op_work_queue;
    ImageCtx::get_thread_pool_instance(m_cct, &thread_pool, &op_work_queue);
    C_SaferCond on_remove;
    auto req = librbd::image::RemoveRequest<>::create(
      m_dst_io_ctx, dst_image_ctx, false, false, *m_prog_ctx, op_work_queue,
      &on_remove);
    req->send();
    r = on_remove.wait();

    dst_image_ctx = nullptr;

    if (r < 0) {
      lderr(m_cct) << "failed removing destination image '"
                   << m_dst_io_ctx.get_pool_name() << "/" << m_dst_image_name
                   << " (" << m_dst_image_id << ")': " << cpp_strerror(r)
                   << dendl;
      // not fatal
    }
  }

  r = relink_src_image();
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

  r = enable_mirroring(m_src_image_ctx, m_mirroring);
  if (r < 0) {
    return r;
  }

  ldout(m_cct, 10) << "succeeded" << dendl;

  return 0;
}

template <typename I>
int Migration<I>::commit() {
  ldout(m_cct, 10) << dendl;

  BOOST_SCOPE_EXIT_TPL(&m_src_image_ctx) {
    if (m_src_image_ctx != nullptr) {
      m_src_image_ctx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;

  auto dst_image_ctx = I::create(m_dst_image_name, m_dst_image_id, nullptr,
                                 m_dst_io_ctx, false);
  int r = dst_image_ctx->state->open(0);
  if (r < 0) {
    lderr(m_cct) << "failed to open destination image: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  BOOST_SCOPE_EXIT_TPL(dst_image_ctx) {
    dst_image_ctx->state->close();
  } BOOST_SCOPE_EXIT_END;

  r = remove_migration(dst_image_ctx);
  if (r < 0) {
    return r;
  }

  r = remove_src_image();

  if (r < 0) {
    return r;
  }

  r = enable_mirroring(dst_image_ctx, m_mirroring);
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
int Migration<I>::set_state(cls::rbd::MigrationState state,
                            const std::string &description) {
  int r = cls_client::migration_set_state(&m_src_io_ctx, m_src_header_oid,
                                          state, description);
  if (r < 0) {
    lderr(m_cct) << "failed to set source migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  r = cls_client::migration_set_state(&m_dst_io_ctx, m_dst_header_oid, state,
                                      description);
  if (r < 0) {
    lderr(m_cct) << "failed to set destination migration header: "
                 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::list_snaps(std::vector<librbd::snap_info_t> *snapsptr) {
  ldout(m_cct, 10) << dendl;

  std::vector<librbd::snap_info_t> snaps;

  int r = snap_list(m_src_image_ctx, snaps);
  if (r < 0) {
    lderr(m_cct) << "failed listing snapshots: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto &snap : snaps) {
    librbd::snap_namespace_type_t namespace_type;
    r = Snapshot<I>::get_namespace_type(m_src_image_ctx, snap.id, &namespace_type);
    if (r < 0) {
      lderr(m_cct) << "error getting snap namespace type: " << cpp_strerror(r)
                   << dendl;
      return r;
    }

    if (namespace_type == RBD_SNAP_NAMESPACE_TYPE_GROUP ||
        namespace_type == RBD_SNAP_NAMESPACE_TYPE_TRASH) {
      lderr(m_cct) << "image has group or trash snapshot '" << snap.name << "'"
                   << dendl;
      return -EBUSY;
    } else {
      bool is_protected;
      r = snap_is_protected(m_src_image_ctx, snap.name.c_str(), &is_protected);
      if (r < 0) {
        lderr(m_cct) << "failed retrieving snapshot status: " << cpp_strerror(r)
                     << dendl;
        return r;
      }
      if (is_protected) {
        lderr(m_cct) << "image has protected snapshot '" << snap.name << "'"
                     << dendl;
        return -EBUSY;
      }

      RWLock::RLocker l(m_src_image_ctx->snap_lock);
      cls::rbd::ParentImageSpec parent_spec{m_src_image_ctx->md_ctx.get_id(),
                                            m_src_image_ctx->md_ctx.get_namespace(),
                                            m_src_image_ctx->id, snap.id};
      std::vector<librbd::linked_image_spec_t> child_images;
      r = api::Image<I>::list_children(m_src_image_ctx, parent_spec, &child_images);
      if (r < 0) {
        lderr(m_cct) << "failed listing children: " << cpp_strerror(r)
                     << dendl;
        return r;
      }

      size_t size = child_images.size();
      if (size > 0) {
        lderr(m_cct) << "image has snapshot '" << snap.name << "' with linked clones"
                     << dendl;
        return -EBUSY;
      }
    }
  }

  if (snapsptr != nullptr) {
    *snapsptr = snaps;
  }

  return 0;
}

template <typename I>
int Migration<I>::set_migration() {
  ldout(m_cct, 10) << dendl;

  m_src_image_ctx->ignore_migrating = true;

  int r = cls_client::migration_set(&m_src_io_ctx, m_src_header_oid,
                                    m_src_migration_spec);
  if (r < 0) {
    lderr(m_cct) << "failed to set migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  m_src_image_ctx->notify_update();

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
int Migration<I>::unlink_src_image() {
  if (m_src_old_format) {
    return v1_unlink_src_image();
  } else {
    return v2_unlink_src_image();
  }
}

template <typename I>
int Migration<I>::v1_unlink_src_image() {
  ldout(m_cct, 10) << dendl;

  int r = tmap_rm(m_src_io_ctx, m_src_image_name);
  if (r < 0) {
    lderr(m_cct) << "failed removing " << m_src_image_name << " from tmap: "
                 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::v2_unlink_src_image() {
  ldout(m_cct, 10) << dendl;

  m_src_image_ctx->owner_lock.get_read();
  if (m_src_image_ctx->exclusive_lock != nullptr &&
      m_src_image_ctx->exclusive_lock->is_lock_owner()) {
    C_SaferCond ctx;
    m_src_image_ctx->exclusive_lock->release_lock(&ctx);
    m_src_image_ctx->owner_lock.put_read();
    int r = ctx.wait();
     if (r < 0) {
      lderr(m_cct) << "error releasing exclusive lock: " << cpp_strerror(r)
                   << dendl;
      return r;
     }
  } else {
    m_src_image_ctx->owner_lock.put_read();
  }

  int r = Trash<I>::move(m_src_io_ctx, RBD_TRASH_IMAGE_SOURCE_MIGRATION,
                         m_src_image_ctx->name, 0);
  if (r < 0) {
    lderr(m_cct) << "failed moving image to trash: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::relink_src_image() {
  if (m_src_old_format) {
    return v1_relink_src_image();
  } else {
    return v2_relink_src_image();
  }
}

template <typename I>
int Migration<I>::v1_relink_src_image() {
  ldout(m_cct, 10) << dendl;

  int r = tmap_set(m_src_io_ctx, m_src_image_name);
  if (r < 0) {
    lderr(m_cct) << "failed adding " << m_src_image_name << " to tmap: "
                 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::v2_relink_src_image() {
  ldout(m_cct, 10) << dendl;

  int r = Trash<I>::restore(m_src_io_ctx, RBD_TRASH_IMAGE_SOURCE_MIGRATION,
                            m_src_image_ctx->id, m_src_image_ctx->name);
  if (r < 0) {
    lderr(m_cct) << "failed restoring image from trash: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::create_dst_image() {
  ldout(m_cct, 10) << dendl;

  uint64_t size;
  cls::rbd::ParentImageSpec parent_spec;
  {
    RWLock::RLocker snap_locker(m_src_image_ctx->snap_lock);
    RWLock::RLocker parent_locker(m_src_image_ctx->parent_lock);
    size = m_src_image_ctx->size;

    // use oldest snapshot or HEAD for parent spec
    if (!m_src_image_ctx->snap_info.empty()) {
      parent_spec = m_src_image_ctx->snap_info.begin()->second.parent.spec;
    } else {
      parent_spec = m_src_image_ctx->parent_md.spec;
    }
  }

  ThreadPool *thread_pool;
  ContextWQ *op_work_queue;
  ImageCtx::get_thread_pool_instance(m_cct, &thread_pool, &op_work_queue);

  ConfigProxy config{m_cct->_conf};
  api::Config<I>::apply_pool_overrides(m_dst_io_ctx, &config);

  int r;
  C_SaferCond on_create;
  librados::IoCtx parent_io_ctx;
  if (parent_spec.pool_id == -1) {
    auto *req = image::CreateRequest<I>::create(
      config, m_dst_io_ctx, m_dst_image_name, m_dst_image_id, size,
      m_image_options, "", "", true /* skip_mirror_enable */, op_work_queue,
      &on_create);
    req->send();
  } else {
    r = util::create_ioctx(m_src_image_ctx->md_ctx, "destination image",
                           parent_spec.pool_id, parent_spec.pool_namespace,
                           &parent_io_ctx);
    if (r < 0) {
      return r;
    }

    auto *req = image::CloneRequest<I>::create(
      config, parent_io_ctx, parent_spec.image_id, "", parent_spec.snap_id,
      m_dst_io_ctx, m_dst_image_name, m_dst_image_id, m_image_options, "", "",
      op_work_queue, &on_create);
    req->send();
  }

  r = on_create.wait();
  if (r < 0) {
    lderr(m_cct) << "header creation failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  auto dst_image_ctx = I::create(m_dst_image_name, m_dst_image_id, nullptr,
                                 m_dst_io_ctx, false);

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
    RWLock::RLocker owner_locker(dst_image_ctx->owner_lock);
    r = dst_image_ctx->operations->prepare_image_update(true);
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
      m_src_image_ctx, dst_image_ctx, CEPH_NOSNAP, m_flatten,
      m_src_image_ctx->op_work_queue, &snap_seqs, &on_snapshot_copy);
  snapshot_copy_req->send();
  r = on_snapshot_copy.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to copy snapshots: " << cpp_strerror(r) << dendl;
    return r;
  }

  C_SaferCond on_metadata_copy;
  auto metadata_copy_req = librbd::deep_copy::MetadataCopyRequest<I>::create(
      m_src_image_ctx, dst_image_ctx, &on_metadata_copy);
  metadata_copy_req->send();
  r = on_metadata_copy.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to copy metadata: " << cpp_strerror(r) << dendl;
    return r;
  }

  m_dst_migration_spec = {cls::rbd::MIGRATION_HEADER_TYPE_DST,
                          m_src_io_ctx.get_id(), m_src_io_ctx.get_namespace(),
                          m_src_image_name, m_src_image_id, snap_seqs, size,
                          m_flatten, m_mirroring,
                          cls::rbd::MIGRATION_STATE_PREPARING, ""};

  r = cls_client::migration_set(&m_dst_io_ctx, m_dst_header_oid,
                                m_dst_migration_spec);
  if (r < 0) {
    lderr(m_cct) << "failed to set migration header: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  r = update_group(m_src_image_ctx, dst_image_ctx);
  if (r < 0) {
    return r;
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
int Migration<I>::disable_mirroring(I *image_ctx, bool *was_enabled) {
  *was_enabled = false;

  if (!image_ctx->test_features(RBD_FEATURE_JOURNALING)) {
    return 0;
  }

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
int Migration<I>::enable_mirroring(I *image_ctx, bool was_enabled) {

  if (!image_ctx->test_features(RBD_FEATURE_JOURNALING)) {
    return 0;
  }

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
  auto req = mirror::EnableRequest<I>::create(image_ctx->md_ctx, image_ctx->id,
                                              "", image_ctx->op_work_queue,
                                              &ctx);
  req->send();
  r = ctx.wait();
  if (r < 0) {
    lderr(m_cct) << "failed to enable mirroring: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  return 0;
}

template <typename I>
int Migration<I>::remove_src_image() {
  ldout(m_cct, 10) << dendl;

  std::vector<librbd::snap_info_t> snaps;
  int r = list_snaps(&snaps);
  if (r < 0) {
    return r;
  }

  for (auto it = snaps.rbegin(); it != snaps.rend(); it++) {
    auto &snap = *it;
    librbd::NoOpProgressContext prog_ctx;
    int r = snap_remove(m_src_image_ctx, snap.name.c_str(), 0, prog_ctx);
    if (r < 0) {
      lderr(m_cct) << "failed removing snapshot '" << snap.name << "': "
                   << cpp_strerror(r) << dendl;
      return r;
    }
  }

  ceph_assert(m_src_image_ctx->ignore_migrating);

  ThreadPool *thread_pool;
  ContextWQ *op_work_queue;
  ImageCtx::get_thread_pool_instance(m_cct, &thread_pool, &op_work_queue);
  C_SaferCond on_remove;
  auto req = librbd::image::RemoveRequest<I>::create(
      m_src_io_ctx, m_src_image_ctx, false, true, *m_prog_ctx, op_work_queue,
      &on_remove);
  req->send();
  r = on_remove.wait();

  m_src_image_ctx = nullptr;

  // For old format image it will return -ENOENT due to expected
  // tmap_rm failure at the end.
  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed removing source image: " << cpp_strerror(r)
                 << dendl;
    return r;
  }

  if (!m_src_image_id.empty()) {
    r = cls_client::trash_remove(&m_src_io_ctx, m_src_image_id);
    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "error removing image " << m_src_image_id
                   << " from rbd_trash object" << dendl;
    }
  }

  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::Migration<librbd::ImageCtx>;
