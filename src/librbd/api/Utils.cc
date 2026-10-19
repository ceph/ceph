// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/api/Utils.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/Cond.h"
#include "common/dout.h"

#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
#include <algorithm>
#include <set>
#include <shared_mutex>
#if defined(HAVE_LIBCRYPTSETUP)
#include "librbd/crypto/luks/LUKSEncryptionFormat.h"
#endif

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::api::util: " << __func__ << ": "

namespace librbd {
namespace api {
namespace util {

int cleanup_group_snapshot_dependencies(ImageCtx *ictx) {
  struct GroupSnapshotRef {
    cls::rbd::GroupSpec group_spec;
    std::string group_snap_id;

    bool operator<(const GroupSnapshotRef &rhs) const {
      if (group_spec != rhs.group_spec) {
        return group_spec < rhs.group_spec;
      }
      return group_snap_id < rhs.group_snap_id;
    }
  };

  std::set<GroupSnapshotRef> group_snapshot_refs;
  {
    std::shared_lock image_locker{ictx->image_lock};
    for (const auto &[snap_id, snap_info] : ictx->snap_info) {
      if (auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
            &snap_info.snap_namespace);
        mirror_ns != nullptr && mirror_ns->group_spec.is_valid()) {
        if (mirror_ns->group_snap_id.empty() ||
            !mirror_ns->mirror_peer_uuids.empty()) {
          return -EBUSY;
        }
        group_snapshot_refs.insert({mirror_ns->group_spec,
          mirror_ns->group_snap_id});
      }
    }
  }

  if (group_snapshot_refs.empty()) {
    return 0;
  }

  for (const auto &ref : group_snapshot_refs) {
    librados::IoCtx group_ioctx;
    int r = librbd::util::create_ioctx(ictx->md_ctx, "group", ref.group_spec.pool_id,
                              {}, &group_ioctx);
    if (r < 0) {
      return r == -ENOENT ? -EBUSY : r;
    }

    cls::rbd::GroupSnapshot group_snap;
    r = cls_client::group_snap_get_by_id(
      &group_ioctx, librbd::util::group_header_name(ref.group_spec.group_id),
      ref.group_snap_id, &group_snap);
    if (r == -ENOENT) {
      continue; // Another group cleanup already removed this snapshot.
    } else if (r < 0) {
      return r;
    }
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
      &group_snap.snapshot_namespace);
    if (ns == nullptr || ns->is_non_primary() ||
        !ns->mirror_peer_uuids.empty()) {
      return -EBUSY;
    }
    if (std::none_of(group_snap.snaps.begin(), group_snap.snaps.end(),
          [ictx](const auto& snap) {
            return snap.pool == ictx->md_ctx.get_id() &&
                   snap.image_id == ictx->id;
          })) {
      // Refresh below still checks for remaining group-linked snapshots.
      continue;
    }
    std::vector<ImageCtx*> image_ctxs{ictx};
    C_SaferCond ctx;
    auto req = mirror::snapshot::RemoveGroupSnapshotRequest<>::create(
      group_ioctx, ref.group_spec.group_id, &group_snap, &image_ctxs, &ctx);
    req->send();
    r = ctx.wait();
    if (r < 0) {
      return r;
    }
  }

  int r = ictx->state->refresh();
  if (r < 0) {
    return r;
  }
  std::shared_lock image_locker{ictx->image_lock};
  for (const auto &[snap_id, snap_info] : ictx->snap_info) {
    auto ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
      &snap_info.snap_namespace);
    if (ns != nullptr && ns->group_spec.is_valid()) {
      // Keep snapshots still owned by a group until group cleanup removes them.
      return -EBUSY;
    }
  }
  return 0;
}

template <typename I>
int create_encryption_format(
        CephContext* cct, encryption_format_t format,
        encryption_options_t opts, size_t opts_size, bool c_api,
        crypto::EncryptionFormat<I>** result_format) {
  size_t expected_opts_size;
  switch (format) {
#if defined(HAVE_LIBCRYPTSETUP)
    case RBD_ENCRYPTION_FORMAT_LUKS1: {
      if (c_api) {
        expected_opts_size = sizeof(rbd_encryption_luks1_format_options_t);
        if (expected_opts_size == opts_size) {
          auto c_opts = (rbd_encryption_luks1_format_options_t*)opts;
          *result_format = new crypto::luks::LUKS1EncryptionFormat<I>(
                  c_opts->alg, {c_opts->passphrase, c_opts->passphrase_size});
        }
      } else {
        expected_opts_size = sizeof(encryption_luks1_format_options_t);
        if (expected_opts_size == opts_size) {
          auto cpp_opts = (encryption_luks1_format_options_t*)opts;
          *result_format = new crypto::luks::LUKS1EncryptionFormat<I>(
                  cpp_opts->alg, cpp_opts->passphrase);
        }
      }
      break;
    }
    case RBD_ENCRYPTION_FORMAT_LUKS2: {
      if (c_api) {
        expected_opts_size = sizeof(rbd_encryption_luks2_format_options_t);
        if (expected_opts_size == opts_size) {
          auto c_opts = (rbd_encryption_luks2_format_options_t*)opts;
          *result_format = new crypto::luks::LUKS2EncryptionFormat<I>(
                  c_opts->alg, {c_opts->passphrase, c_opts->passphrase_size});
        }
      } else {
        expected_opts_size = sizeof(encryption_luks2_format_options_t);
        if (expected_opts_size == opts_size) {
          auto cpp_opts = (encryption_luks2_format_options_t*)opts;
          *result_format = new crypto::luks::LUKS2EncryptionFormat<I>(
                  cpp_opts->alg, cpp_opts->passphrase);
        }
      }
      break;
    }
    case RBD_ENCRYPTION_FORMAT_LUKS: {
      if (c_api) {
        expected_opts_size = sizeof(rbd_encryption_luks_format_options_t);
        if (expected_opts_size == opts_size) {
          auto c_opts = (rbd_encryption_luks_format_options_t*)opts;
          *result_format = new crypto::luks::LUKSEncryptionFormat<I>(
                  {c_opts->passphrase, c_opts->passphrase_size});
        }
      } else {
        expected_opts_size = sizeof(encryption_luks_format_options_t);
        if (expected_opts_size == opts_size) {
          auto cpp_opts = (encryption_luks_format_options_t*)opts;
          *result_format = new crypto::luks::LUKSEncryptionFormat<I>(
                  cpp_opts->passphrase);
        }
      }
      break;
    }
#endif
    default:
      lderr(cct) << "unsupported encryption format: " << format << dendl;
      return -ENOTSUP;
  }

  if (expected_opts_size != opts_size) {
    lderr(cct) << "expected opts_size: " << expected_opts_size << dendl;
    return -EINVAL;
  }

  return 0;
}

template <typename I>
int notify_quiesce(std::vector<I *> &ictxs, ProgressContext &prog_ctx,
                   std::vector<uint64_t> *requests) {
  int image_count = ictxs.size();
  if (image_count == 0) {
    return 0;
  }

  std::vector<C_SaferCond> on_finishes(image_count);

  requests->resize(image_count);
  for (int i = 0; i < image_count; ++i) {
    auto ictx = ictxs[i];

    ictx->image_watcher->notify_quiesce(&(*requests)[i], prog_ctx,
                                        &on_finishes[i]);
  }

  int ret_code = 0;
  for (int i = 0; i < image_count; ++i) {
    int r = on_finishes[i].wait();
    if (r < 0) {
      ret_code = r;
    }
  }

  if (ret_code != 0) {
    notify_unquiesce(ictxs, *requests);
  }

  return ret_code;
}

template <typename I>
void notify_unquiesce(std::vector<I *> &ictxs,
                      const std::vector<uint64_t> &requests) {
  if (requests.empty()) {
    return;
  }

  ceph_assert(requests.size() == ictxs.size());
  int image_count = ictxs.size();
  std::vector<C_SaferCond> on_finishes(image_count);

  for (int i = 0; i < image_count; ++i) {
    ImageCtx *ictx = ictxs[i];

    ictx->image_watcher->notify_unquiesce(requests[i], &on_finishes[i]);
  }

  for (int i = 0; i < image_count; ++i) {
    on_finishes[i].wait();
  }
}

template <typename I>
librados::snap_t get_group_snap_id(
    I *ictx, const cls::rbd::SnapshotNamespace& in_snap_namespace) {
  ceph_assert(ceph_mutex_is_locked(ictx->image_lock));
  auto it = ictx->snap_ids.lower_bound({cls::rbd::ImageSnapshotNamespaceGroup{},
                                        ""});
  for (; it != ictx->snap_ids.end(); ++it) {
    if (it->first.first == in_snap_namespace) {
      return it->second;
    } else if (std::get_if<cls::rbd::ImageSnapshotNamespaceGroup>(
                   &it->first.first) == nullptr) {
      break;
    }
  }
  return CEPH_NOSNAP;
}

int group_snap_remove(librados::IoCtx& group_ioctx, const std::string& group_id,
                      const cls::rbd::GroupSnapshot& group_snap) {
  CephContext *cct = (CephContext *)group_ioctx.cct();
  std::string group_header_oid = librbd::util::group_header_name(group_id);
  std::vector<C_SaferCond*> on_finishes;
  int r, ret_code;

  std::vector<librbd::ImageCtx*> ictxs;

  cls::rbd::ImageSnapshotNamespaceGroup snap_namespace{group_ioctx.get_id(),
                                                       group_id, group_snap.id};

  ldout(cct, 20) << "Removing snapshot with group snap_id: " << group_snap.id
                 << ", and group_id: " << group_id << dendl;
  int snap_count = group_snap.snaps.size();

  for (int i = 0; i < snap_count; ++i) {
    librbd::IoCtx image_io_ctx;
    r = librbd::util::create_ioctx(group_ioctx, "image",
                                   group_snap.snaps[i].pool, {}, &image_io_ctx);
    if (r < 0) {
      ret_code = r;
      goto finish;
    }

    librbd::ImageCtx* image_ctx = new ImageCtx("", group_snap.snaps[i].image_id,
					       nullptr, image_io_ctx, false);

    C_SaferCond* on_finish = new C_SaferCond;

    image_ctx->state->open(0, on_finish);

    ictxs.push_back(image_ctx);
    on_finishes.push_back(on_finish);
  }

  ret_code = 0;
  for (int i = 0; i < snap_count; ++i) {
    r = on_finishes[i]->wait();
    delete on_finishes[i];
    if (r < 0) {
      ictxs[i] = nullptr;
      if (r != -ENOENT) {
        ret_code = r;
      }
    }
  }
  if (ret_code != 0) {
    goto finish;
  }

  ldout(cct, 20) << "Opened participating images. " <<
		    "Deleting snapshots themselves." << dendl;

  for (int i = 0; i < snap_count; ++i) {
    ImageCtx *ictx = ictxs[i];
    on_finishes[i] = new C_SaferCond;

    if (ictx == nullptr) {
      on_finishes[i]->complete(0);
      continue;
    }

    std::string snap_name;
    ictx->image_lock.lock_shared();
    auto snap_id = get_group_snap_id(ictx, snap_namespace);
    r = ictx->get_snap_name(snap_id, &snap_name);
    ictx->image_lock.unlock_shared();

    if (r >= 0) {
      ldout(cct, 20) << "removing individual snapshot: " << snap_name
                     << ", on image: " << ictx->name << dendl;
      ictx->operations->snap_remove(snap_namespace, snap_name, on_finishes[i]);
    } else {
      // We are ok to ignore missing image snapshots. The snapshot could have
      // been inconsistent in the first place.
      on_finishes[i]->complete(0);
    }
  }

  for (int i = 0; i < snap_count; ++i) {
    r = on_finishes[i]->wait();
    delete on_finishes[i];
    if (r < 0 && r != -ENOENT) {
      // if previous attempts to remove this snapshot failed then the image's
      // snapshot may not exist
      lderr(cct) << "Failed deleting image snapshot. Ret code: " << r << dendl;
      ret_code = r;
    }
  }

  if (ret_code != 0) {
    goto finish;
  }

  ldout(cct, 20) << "Removed images snapshots removing snapshot record."
                 << dendl;

  r = cls_client::group_snap_remove(&group_ioctx, group_header_oid,
      group_snap.id);
  if (r < 0) {
    ret_code = r;
    goto finish;
  }

finish:
  for (int i = 0; i < snap_count; ++i) {
    if (ictxs[i] != nullptr) {
      ictxs[i]->state->close();
    }
  }
  return ret_code;
}

} // namespace util
} // namespace api
} // namespace librbd

template int librbd::api::util::create_encryption_format(
    CephContext* cct, encryption_format_t format, encryption_options_t opts,
    size_t opts_size, bool c_api,
    crypto::EncryptionFormat<librbd::ImageCtx>** result_format);

template int librbd::api::util::notify_quiesce(
    std::vector<librbd::ImageCtx *> &ictxs, librbd::ProgressContext &prog_ctx,
    std::vector<uint64_t> *requests);
template void librbd::api::util::notify_unquiesce(
    std::vector<librbd::ImageCtx *> &ictxs,
    const std::vector<uint64_t> &requests);

template librados::snap_t librbd::api::util::get_group_snap_id(
    librbd::ImageCtx *ictx,
    const cls::rbd::SnapshotNamespace &in_snap_namespace);
