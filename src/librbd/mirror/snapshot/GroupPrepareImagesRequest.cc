// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "include/ceph_assert.h"
#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/Context.h"
#include "librbd/api/Group.h"
#include "librbd/api/Image.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/internal.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::GroupPrepareImagesRequest: " \
                           <<  this << " " << __func__ << ": "
namespace librbd {
namespace mirror {
namespace snapshot {

namespace {

const uint32_t MAX_RETURN = 1024;

} // anonymous namespace

using librbd::util::create_rados_callback;
using librbd::util::create_context_callback;

template <typename I>
GroupPrepareImagesRequest<I>::GroupPrepareImagesRequest(
    librados::IoCtx& group_ioctx, const std::string& group_id,
    std::vector<librbd::ImageCtx *>& image_ctxs,
    std::vector<cls::rbd::GroupImageStatus>& images,
    std::vector<cls::rbd::MirrorImage>* mirror_images,
    std::vector<bool>* is_primary,
    std::vector<bool>* is_image_disabled,
    std::set<std::string>* mirror_peer_uuids,
    const std::string& api_name, bool force, Context *on_finish)
     : m_group_ioctx(group_ioctx), m_group_id(group_id),
       m_image_ctxs(image_ctxs), m_images(images),
       m_mirror_images(mirror_images), m_is_primary(is_primary),
       m_is_image_disabled(is_image_disabled),
       m_mirror_peer_uuids(mirror_peer_uuids), m_api_name(api_name),
       m_force(force), m_on_finish(on_finish) {
  m_cct = reinterpret_cast<CephContext*>(m_group_ioctx.cct());
  ldout(m_cct, 10) << "group_id=" << m_group_id
                   << dendl;
}

template <typename I>
void GroupPrepareImagesRequest<I>::send() {
  ldout(m_cct, 10) << dendl;

  if (m_api_name == "disable") {
    list_group_images();
  } else {
    get_mirror_peer_list();
  }
}

template <typename I>
void GroupPrepareImagesRequest<I>::get_mirror_peer_list() {
  ldout(m_cct, 10) << dendl;

  m_default_ns_ioctx.dup(m_group_ioctx);
  m_default_ns_ioctx.set_namespace("");

  librados::ObjectReadOperation op;
  cls_client::mirror_peer_list_start(&op);

  auto comp = create_rados_callback<
      GroupPrepareImagesRequest<I>,
      &GroupPrepareImagesRequest<I>::handle_get_mirror_peer_list>(this);

  m_outbl.clear();
  int r = m_default_ns_ioctx.aio_operate(RBD_MIRRORING, comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_get_mirror_peer_list(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::MirrorPeer> peers;
  if (r == 0) {
    auto it = m_outbl.cbegin();
    r = cls_client::mirror_peer_list_finish(&it, &peers);
  }

  if (r < 0) {
    lderr(m_cct) << "error listing mirror peers" << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  for (auto &peer : peers) {
    if (peer.mirror_peer_direction == cls::rbd::MIRROR_PEER_DIRECTION_RX) {
      continue;
    }
    m_mirror_peer_uuids->insert(peer.uuid);
  }

  if (m_mirror_peer_uuids->empty()) {
    lderr(m_cct) << "no mirror tx peers configured for the pool" << dendl;
    finish(-EINVAL);
    return;
  }

  list_group_images();
}

template <typename I>
void GroupPrepareImagesRequest<I>::list_group_images() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::group_image_list_start(&op, m_start_after, MAX_RETURN);

  auto comp = create_rados_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_list_group_images>(this);

  m_outbl.clear();
  int r = m_group_ioctx.aio_operate(
    librbd::util::group_header_name(m_group_id), comp, &op, &m_outbl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_list_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::vector<cls::rbd::GroupImageStatus> images;
  if (r == 0) {
    auto iter = m_outbl.cbegin();
    r = cls_client::group_image_list_finish(&iter, &images);
  }

  if (r < 0) {
    lderr(m_cct) << "error listing images in group: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  auto image_count = images.size();
  m_images.insert(m_images.end(), images.begin(), images.end());
  if (image_count == MAX_RETURN) {
    m_start_after = images.rbegin()->spec;
    list_group_images();
    return;
  }

  if (m_images.empty()) {
    finish(0);
    return;
  }
  if (m_api_name == "enable") {
    check_mirror_images_disabled();
  } else {
    open_group_images();
  }
}

template <typename I>
void GroupPrepareImagesRequest<I>::check_mirror_images_disabled() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_check_mirror_images_disabled>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  m_mirror_images->resize(m_images.size());
  m_out_bls.resize(m_images.size());
  for (size_t i = 0; i < m_images.size(); i++) {
    librados::ObjectReadOperation op;
    cls_client::mirror_image_get_start(&op, m_images[i].spec.image_id);

    auto on_mirror_image_get = new LambdaContext(
      [this, i, new_sub_ctx=gather_ctx->new_sub()](int r) {
        if (r == 0) {
          auto iter = m_out_bls[i].cbegin();
          r = cls_client::mirror_image_get_finish(&iter, &(*m_mirror_images)[i]);
        }

        if (r == -ENOENT) {
          // image is disabled for mirroring as required
          r = 0;
        } else if (r == 0) {
          lderr(m_cct) << "image_id=" << m_images[i].spec.image_id
                       << " is not disabled for mirroring" << dendl;
          r = -EINVAL;
        } else {
          lderr(m_cct) << "failed to get mirror image info for image_id="
                       << m_images[i].spec.image_id << dendl;
        }

        new_sub_ctx->complete(r);
      });

    auto comp = create_rados_callback(on_mirror_image_get);

    int r = m_group_ioctx.aio_operate(RBD_MIRRORING, comp, &op,
                                      &m_out_bls[i]);
    ceph_assert(r == 0);
    comp->release();
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_check_mirror_images_disabled(int r) {
  ldout(m_cct, 10) << "r=" << r <<  dendl;

  m_out_bls.clear();

  if (r < 0) {
    lderr(m_cct) << "images not disabled for mirroring: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  open_group_images();
}

template <typename I>
void GroupPrepareImagesRequest<I>::open_group_images() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_open_group_images>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);
  int r = 0;
  std::vector<librados::IoCtx> ioctxs;

  for (const auto& image: m_images) {
    librados::IoCtx image_io_ctx;
    r = librbd::util::create_ioctx(m_group_ioctx, "image",
                                   image.spec.pool_id, {},
                                   &image_io_ctx);
    if (r < 0) {
      finish(r);
      return;
    }

    ioctxs.push_back(std::move(image_io_ctx));
  }

  for (size_t i = 0; i < m_images.size(); i++) {
    m_image_ctxs.push_back(
      new ImageCtx("", m_images[i].spec.image_id.c_str(), nullptr, ioctxs[i],
                   false));

    auto on_open = new LambdaContext(
      [this, i, new_sub_ctx=gather_ctx->new_sub()](int r) {
        // If asynchronous ImageState::open() fails, ImageState together with
        // ImageCtx is destroyed. Simply NULL-out the respective image_ctxs[i]
        // pointer to record that it's no longer valid.
        if (r < 0) {
          m_image_ctxs[i] = nullptr;
        }
        new_sub_ctx->complete(r);
      });

    // Open parent as well to check if the image is a clone
    m_image_ctxs[i]->state->open(0, on_open);
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_open_group_images(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to open group images: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  if (m_api_name == "enable") {
    finish(0);
  } else if (m_api_name == "disable") {
    refresh_images();
  } else {
    get_images_mirror_info();
  }
}

template <typename I>
void GroupPrepareImagesRequest<I>::refresh_images() {
  ldout(m_cct, 10) << dendl;

  for (auto ictx : m_image_ctxs) {
    int r = ictx->state->refresh_if_required();
    if (r < 0) {
      CephContext *cct = ictx->cct;
      lderr(cct) << "refresh request failed: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
  }

  get_images_mirror_mode();
}

template <typename I>
void GroupPrepareImagesRequest<I>::get_images_mirror_mode() {
  ldout(m_cct, 10) << dendl;

  for (auto ictx : m_image_ctxs) {
    cls::rbd::MirrorMode mirror_mode;
    CephContext *cct = ictx->cct;
    int r = cls_client::mirror_mode_get(&ictx->md_ctx, &mirror_mode);
    if (r < 0) {
      lderr(cct) << "cannot disable mirroring: failed to retrieve pool "
        "mirroring mode: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    if (mirror_mode != cls::rbd::MIRROR_MODE_IMAGE) {
      lderr(cct) << "cannot disable mirroring in the current pool mirroring "
        "mode" << dendl;
      finish(-EINVAL);
      return;
    }
  }

  get_images_mirror_info();
}

template <typename I>
void GroupPrepareImagesRequest<I>::get_images_mirror_info() {
  ldout(m_cct, 10) << dendl;
  auto ctx = create_context_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_get_images_mirror_info>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  m_mirror_images->resize(m_images.size());
  m_images_promotion_states.resize(m_images.size());
  m_images_primary_mirror_uuids.resize(m_images.size());
  for (size_t i = 0; i < m_images.size(); i++) {
    auto info_ctx = new LambdaContext(
      [this, i, new_sub_ctx=gather_ctx->new_sub()](int r) {
        CephContext *image_cct = m_image_ctxs[i]->cct;
        if (r < 0 && r != -ENOENT) {
          lderr(image_cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
                           << dendl;
          new_sub_ctx->complete(r);
          return;
        } else if ((*m_mirror_images)[i].state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
          lderr(image_cct) << "mirroring is not currently enabled" << dendl;
          if (m_api_name == "disable") {
            m_is_image_disabled->resize(m_images.size());
            (*m_is_image_disabled)[i] = true;
          }
          new_sub_ctx->complete(-EINVAL);
          return;
        }

        if (m_api_name == "disable") {
          m_is_primary->resize(m_images.size());
          (*m_is_primary)[i] = (m_images_promotion_states[i] == PROMOTION_STATE_PRIMARY ||
                                m_images_promotion_states[i] == PROMOTION_STATE_UNKNOWN);

          if (!(*m_is_primary)[i] && !m_force) {
            lderr(image_cct) << "mirrored image is not primary, "
                             << "add force option to disable mirroring" << dendl;
            new_sub_ctx->complete(-EINVAL);
            return;
          }
        } else if (m_api_name == "demote") {
          if (m_images_promotion_states[i] != PROMOTION_STATE_PRIMARY) {
            lderr(image_cct) << "image is not primary" << dendl;
            new_sub_ctx->complete(-EINVAL);
            return;
          }
        } else if (m_api_name == "promote") {
          if (m_images_promotion_states[i] == PROMOTION_STATE_PRIMARY) {
            lderr(image_cct) << "image is already primary" << dendl;
            new_sub_ctx->complete(-EINVAL);
            return;
          } else if (m_images_promotion_states[i] == PROMOTION_STATE_NON_PRIMARY && !m_force) {
            lderr(image_cct) << "image is primary within a remote cluster or demotion is not propagated yet"
                             << dendl;
            new_sub_ctx->complete(-EBUSY);
            return;
          }
        }

        new_sub_ctx->complete(0);
      });
    auto req = GetInfoRequest<I>::create(*m_image_ctxs[i], &(*m_mirror_images)[i],
                                         &m_images_promotion_states[i],
                                         &m_images_primary_mirror_uuids[i], info_ctx);
    req->send();

  }
  gather_ctx->activate();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_get_images_mirror_info(int r) {
  ldout(m_cct, 10) << "r=" << r <<  dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to get mirror images info: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_api_name == "create") {
    finish(0);
  } else if (m_api_name == "disable") {
    check_images_child_mirroring();
  } else {
    update_images_read_only_mask();
  }
}

template <typename I>
void GroupPrepareImagesRequest<I>::check_images_child_mirroring() {
  ldout(m_cct, 10) <<  dendl;

  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    auto ictx = m_image_ctxs[i];
    auto image_cct = ictx->cct;
    std::shared_lock image_locker{ictx->image_lock};
    for (const auto& info : ictx->snap_info) {
      cls::rbd::ParentImageSpec parent_spec{ictx->md_ctx.get_id(),
                                            ictx->md_ctx.get_namespace(),
                                            ictx->id, info.first};
      std::vector<librbd::linked_image_spec_t> child_images;
      int r = api::Image<I>::list_children(ictx, parent_spec, &child_images);
      if (r < 0) {
        lderr (image_cct) << "failed to list children of image" << dendl;
        finish(r);
        return;
      }

      if (child_images.empty()) {
        continue;
      }

      librados::IoCtx child_io_ctx;
      int64_t child_pool_id = -1;
      for (auto &child_image : child_images){
        std::string pool = child_image.pool_name;
        if (child_pool_id == -1 ||
            child_pool_id != child_image.pool_id ||
            child_io_ctx.get_namespace() != child_image.pool_namespace) {
          r = librbd::util::create_ioctx(ictx->md_ctx, "child image",
                                         child_image.pool_id,
                                         child_image.pool_namespace,
                                         &child_io_ctx);
          if (r < 0) {
            finish(r);
            return;
          }

          child_pool_id = child_image.pool_id;
        }

        cls::rbd::MirrorImage child_mirror_image_internal;
        r = cls_client::mirror_image_get(&child_io_ctx, child_image.image_id,
                                        &child_mirror_image_internal);
        if (r != -ENOENT) {
          lderr(image_cct) << "mirroring is enabled on one or more children "
                           << dendl;
          finish(-EBUSY);
          return;
        }
      }
    }
    image_locker.unlock();
  }

  update_images_read_only_mask();
}

template <typename I>
void GroupPrepareImagesRequest<I>::update_images_read_only_mask() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    GroupPrepareImagesRequest<I>,
    &GroupPrepareImagesRequest<I>::handle_update_images_read_only_mask>(this);
  auto gather_ctx = new C_Gather(m_cct, ctx);

  // don't let the non-primary feature bit prevent image updates
  for (size_t i = 0; i < m_image_ctxs.size(); i++) {
    m_image_ctxs[i]->image_lock.lock();
    m_image_ctxs[i]->read_only_mask &= ~IMAGE_READ_ONLY_FLAG_NON_PRIMARY;
    m_image_ctxs[i]->image_lock.unlock();
    m_image_ctxs[i]->state->refresh(gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

template <typename I>
void GroupPrepareImagesRequest<I>::handle_update_images_read_only_mask(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update images read only mask: "
                 << cpp_strerror(r) << dendl;
  }

  finish(r);
}

template <typename I>
void GroupPrepareImagesRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::GroupPrepareImagesRequest<librbd::ImageCtx>;
