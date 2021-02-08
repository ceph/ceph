// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/DisableRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/journal/cls_journal_client.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/journal/PromoteRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/mirror/ImageRemoveRequest.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"
#include "librbd/mirror/snapshot/PromoteRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::DisableRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using util::create_rados_callback;

template <typename I>
DisableRequest<I>::DisableRequest(I *image_ctx, bool force, bool remove,
                                  Context *on_finish)
  : m_image_ctx(image_ctx), m_force(force), m_remove(remove),
    m_on_finish(on_finish) {
}

template <typename I>
void DisableRequest<I>::send() {
  send_get_mirror_info();
}

template <typename I>
void DisableRequest<I>::send_get_mirror_info() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;


  using klass = DisableRequest<I>;
  Context *ctx = util::create_context_callback<
      klass, &klass::handle_get_mirror_info>(this);

  auto req = GetInfoRequest<I>::create(*m_image_ctx, &m_mirror_image,
                                       &m_promotion_state,
                                       &m_primary_mirror_uuid, ctx);
  req->send();
}

template <typename I>
Context *DisableRequest<I>::handle_get_mirror_info(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << *result << dendl;

  if (*result < 0) {
    if (*result == -ENOENT) {
      ldout(cct, 20) << "mirroring is not enabled for this image" << dendl;
      *result = 0;
    } else {
      lderr(cct) << "failed to get mirroring info: " << cpp_strerror(*result)
                 << dendl;
    }
    return m_on_finish;
  }

  m_is_primary = (m_promotion_state == PROMOTION_STATE_PRIMARY ||
                  m_promotion_state == PROMOTION_STATE_UNKNOWN);

  if (!m_is_primary && !m_force) {
    lderr(cct) << "mirrored image is not primary, "
               << "add force option to disable mirroring" << dendl;
    *result = -EINVAL;
    return m_on_finish;
  }

  send_image_state_update();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_image_state_update() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    DisableRequest<I>,
    &DisableRequest<I>::handle_image_state_update>(this);
  auto req = ImageStateUpdateRequest<I>::create(
    m_image_ctx->md_ctx, m_image_ctx->id,
    cls::rbd::MIRROR_IMAGE_STATE_DISABLING, m_mirror_image, ctx);
  req->send();
}

template <typename I>
Context *DisableRequest<I>::handle_image_state_update(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to disable mirroring: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_promote_image();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_promote_image() {
  if (m_is_primary) {
    clean_mirror_state();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    DisableRequest<I>, &DisableRequest<I>::handle_promote_image>(this);
  if (m_mirror_image.mode == cls::rbd::MIRROR_IMAGE_MODE_JOURNAL) {
    // Not primary -- shouldn't have the journal open
    ceph_assert(m_image_ctx->journal == nullptr);

    auto req = journal::PromoteRequest<I>::create(m_image_ctx, true, ctx);
    req->send();
  } else if (m_mirror_image.mode == cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
    auto req = mirror::snapshot::PromoteRequest<I>::create(
      m_image_ctx, m_mirror_image.global_image_id, ctx);
    req->send();
  } else {
    lderr(cct) << "unknown image mirror mode: " << m_mirror_image.mode << dendl;
    ctx->complete(-EOPNOTSUPP);
  }
}

template <typename I>
Context *DisableRequest<I>::handle_promote_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to promote image: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  send_refresh_image();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_refresh_image() {
  if (!m_image_ctx->state->is_refresh_required()) {
    clean_mirror_state();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    DisableRequest<I>,
    &DisableRequest<I>::handle_refresh_image>(this);
  m_image_ctx->state->refresh(ctx);
}

template <typename I>
Context *DisableRequest<I>::handle_refresh_image(int* result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to refresh image: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  clean_mirror_state();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::clean_mirror_state() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  if (m_mirror_image.mode == cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
    remove_mirror_snapshots();
  } else {
    send_get_clients();
  }
}

template <typename I>
void DisableRequest<I>::send_get_clients() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  using klass = DisableRequest<I>;
  Context *ctx = util::create_context_callback<
    klass, &klass::handle_get_clients>(this);

  std::string header_oid = ::journal::Journaler::header_oid(m_image_ctx->id);
  m_clients.clear();
  cls::journal::client::client_list(m_image_ctx->md_ctx, header_oid, &m_clients,
                                    ctx);
}

template <typename I>
Context *DisableRequest<I>::handle_get_clients(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << *result << dendl;

  std::unique_lock locker{m_lock};
  ceph_assert(m_current_ops.empty());

  if (*result < 0) {
    lderr(cct) << "failed to get registered clients: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  for (auto client : m_clients) {
    journal::ClientData client_data;
    auto bl_it = client.data.cbegin();
    try {
      using ceph::decode;
      decode(client_data, bl_it);
    } catch (const buffer::error &err) {
      lderr(cct) << "failed to decode client data" << dendl;
      m_error_result = -EBADMSG;
      continue;
    }

    journal::ClientMetaType type = client_data.get_client_meta_type();
    if (type != journal::ClientMetaType::MIRROR_PEER_CLIENT_META_TYPE) {
      continue;
    }

    if (m_current_ops.find(client.id) != m_current_ops.end()) {
      // Should not happen.
      lderr(cct) << "clients with the same id "
                 << client.id << dendl;
      continue;
    }

    m_current_ops[client.id] = 0;
    m_ret[client.id] = 0;

    journal::MirrorPeerClientMeta client_meta =
      boost::get<journal::MirrorPeerClientMeta>(client_data.client_meta);

    for (const auto& sync : client_meta.sync_points) {
      send_remove_snap(client.id, sync.snap_namespace, sync.snap_name);
    }

    if (m_current_ops[client.id] == 0) {
      // no snaps to remove
      send_unregister_client(client.id);
    }
  }

  if (m_current_ops.empty()) {
    if (m_error_result < 0) {
      *result = m_error_result;
      return m_on_finish;
    } else if (!m_remove) {
      return m_on_finish;
    }
    locker.unlock();

    // no mirror clients to unregister
    send_remove_mirror_image();
  }

  return nullptr;
}

template <typename I>
void DisableRequest<I>::remove_mirror_snapshots() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  // remove snapshot-based mirroring snapshots
  bool removing_snapshots = false;
  {
    std::lock_guard locker{m_lock};
    std::shared_lock image_locker{m_image_ctx->image_lock};

    for (auto &it : m_image_ctx->snap_info) {
      auto &snap_info = it.second;
      auto type = cls::rbd::get_snap_namespace_type(
        snap_info.snap_namespace);
      if (type == cls::rbd::SNAPSHOT_NAMESPACE_TYPE_MIRROR) {
        send_remove_snap("", snap_info.snap_namespace, snap_info.name);
        removing_snapshots = true;
      }
    }
  }

  if (!removing_snapshots) {
    send_remove_mirror_image();
  }
}

template <typename I>
void DisableRequest<I>::send_remove_snap(
    const std::string &client_id,
    const cls::rbd::SnapshotNamespace &snap_namespace,
    const std::string &snap_name) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "client_id=" << client_id
                 << ", snap_name=" << snap_name << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));

  m_current_ops[client_id]++;

  Context *ctx = create_context_callback(
    &DisableRequest<I>::handle_remove_snap, client_id);

  ctx = new LambdaContext([this, snap_namespace, snap_name, ctx](int r) {
      m_image_ctx->operations->snap_remove(snap_namespace,
                                           snap_name.c_str(),
                                           ctx);
    });

  m_image_ctx->op_work_queue->queue(ctx, 0);
}

template <typename I>
Context *DisableRequest<I>::handle_remove_snap(int *result,
    const std::string &client_id) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << *result << dendl;

  std::unique_lock locker{m_lock};

  ceph_assert(m_current_ops[client_id] > 0);
  m_current_ops[client_id]--;

  if (*result < 0 && *result != -ENOENT) {
    lderr(cct) << "failed to remove mirroring snapshot: "
               << cpp_strerror(*result) << dendl;
    m_ret[client_id] = *result;
  }

  if (m_current_ops[client_id] == 0) {
    if (m_mirror_image.mode == cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
      ceph_assert(client_id.empty());
      m_current_ops.erase(client_id);
      if (m_ret[client_id] < 0) {
        return m_on_finish;
      }
      locker.unlock();

      send_remove_mirror_image();
      return nullptr;
    }

    send_unregister_client(client_id);
  }

  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_unregister_client(
  const std::string &client_id) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_lock));
  ceph_assert(m_current_ops[client_id] == 0);

  Context *ctx = create_context_callback(
    &DisableRequest<I>::handle_unregister_client, client_id);

  if (m_ret[client_id] < 0) {
    m_image_ctx->op_work_queue->queue(ctx, m_ret[client_id]);
    return;
  }

  librados::ObjectWriteOperation op;
  cls::journal::client::client_unregister(&op, client_id);
  std::string header_oid = ::journal::Journaler::header_oid(m_image_ctx->id);
  librados::AioCompletion *comp = create_rados_callback(ctx);

  int r = m_image_ctx->md_ctx.aio_operate(header_oid, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *DisableRequest<I>::handle_unregister_client(
  int *result, const std::string &client_id) {

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << *result << dendl;

  std::unique_lock locker{m_lock};
  ceph_assert(m_current_ops[client_id] == 0);
  m_current_ops.erase(client_id);

  if (*result < 0 && *result != -ENOENT) {
    lderr(cct) << "failed to unregister remote journal client: "
               << cpp_strerror(*result) << dendl;
    m_error_result = *result;
  }

  if (!m_current_ops.empty()) {
    return nullptr;
  }

  if (m_error_result < 0) {
    *result = m_error_result;
    return m_on_finish;
  }
  locker.unlock();

  send_get_clients();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_remove_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    DisableRequest<I>,
    &DisableRequest<I>::handle_remove_mirror_image>(this);
  auto req = ImageRemoveRequest<I>::create(
    m_image_ctx->md_ctx, m_mirror_image.global_image_id, m_image_ctx->id,
    ctx);
  req->send();
}

template <typename I>
Context *DisableRequest<I>::handle_remove_mirror_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to remove mirror image: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  ldout(cct, 20) << "removed image state from rbd_mirroring object" << dendl;
  return m_on_finish;
}

template <typename I>
Context *DisableRequest<I>::create_context_callback(
  Context*(DisableRequest<I>::*handle)(int*, const std::string &client_id),
  const std::string &client_id) {

  return new LambdaContext([this, handle, client_id](int r) {
      Context *on_finish = (this->*handle)(&r, client_id);
      if (on_finish != nullptr) {
        on_finish->complete(r);
        delete this;
      }
    });
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::DisableRequest<librbd::ImageCtx>;
