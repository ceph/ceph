// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/DisableRequest.h"
#include "common/WorkQueue.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/journal/cls_journal_client.h"
#include "cls/rbd/cls_rbd_client.h"
#include "journal/Journaler.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/journal/PromoteRequest.h"
#include "librbd/mirror/GetInfoRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::DisableRequest: "

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
  ldout(cct, 10) << this << " " << __func__ << dendl;


  using klass = DisableRequest<I>;
  Context *ctx = util::create_context_callback<
      klass, &klass::handle_get_mirror_info>(this);

  auto req = GetInfoRequest<I>::create(*m_image_ctx, &m_mirror_image,
                                       &m_promotion_state, ctx);
  req->send();
}

template <typename I>
Context *DisableRequest<I>::handle_get_mirror_info(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    if (*result == -ENOENT) {
      ldout(cct, 20) << this << " " << __func__
                     << ": mirroring is not enabled for this image" << dendl;
      *result = 0;
    } else {
      lderr(cct) << "failed to get mirroring info: " << cpp_strerror(*result)
                 << dendl;
    }
    return m_on_finish;
  }

  m_is_primary = (m_promotion_state == PROMOTION_STATE_PRIMARY);

  if (!m_is_primary && !m_force) {
    lderr(cct) << "mirrored image is not primary, "
               << "add force option to disable mirroring" << dendl;
    *result = -EINVAL;
    return m_on_finish;
  }

  send_set_mirror_image();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_set_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_mirror_image.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;

  librados::ObjectWriteOperation op;
  cls_client::mirror_image_set(&op, m_image_ctx->id, m_mirror_image);

  using klass = DisableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_set_mirror_image>(this);
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *DisableRequest<I>::handle_set_mirror_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to disable mirroring: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_notify_mirroring_watcher();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_notify_mirroring_watcher() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = DisableRequest<I>;
  Context *ctx = util::create_context_callback<
    klass, &klass::handle_notify_mirroring_watcher>(this);

  MirroringWatcher<I>::notify_image_updated(
    m_image_ctx->md_ctx, cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
    m_image_ctx->id, m_mirror_image.global_image_id, ctx);
}

template <typename I>
Context *DisableRequest<I>::handle_notify_mirroring_watcher(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to send update notification: "
               << cpp_strerror(*result) << dendl;
    *result = 0;
  }

  if (m_mirror_image.mode == cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
    // remove mirroring snapshots

    bool removing_snapshots = false;
    {
      std::lock_guard locker{m_lock};
      std::shared_lock image_locker{m_image_ctx->image_lock};

      for (auto &it : m_image_ctx->snap_info) {
        auto &snap_info = it.second;
        auto type = cls::rbd::get_snap_namespace_type(
          snap_info.snap_namespace);
        if (type == cls::rbd::SNAPSHOT_NAMESPACE_TYPE_MIRROR_PRIMARY ||
            type == cls::rbd::SNAPSHOT_NAMESPACE_TYPE_MIRROR_NON_PRIMARY) {
          send_remove_snap("", snap_info.snap_namespace, snap_info.name);
          removing_snapshots = true;
        }
      }
    }

    if (!removing_snapshots) {
      send_remove_mirror_image();
    }

    return nullptr;
  }

  send_promote_image();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_promote_image() {
  if (m_is_primary) {
    send_get_clients();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // Not primary -- shouldn't have the journal open
  ceph_assert(m_image_ctx->journal == nullptr);

  using klass = DisableRequest<I>;
  Context *ctx = util::create_context_callback<
    klass, &klass::handle_promote_image>(this);
  auto req = journal::PromoteRequest<I>::create(m_image_ctx, true, ctx);
  req->send();
}

template <typename I>
Context *DisableRequest<I>::handle_promote_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to promote image: " << cpp_strerror(*result) << dendl;
    return m_on_finish;
  }

  send_get_clients();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_get_clients() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

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
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to get registered clients: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  std::lock_guard locker{m_lock};

  ceph_assert(m_current_ops.empty());

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
      lderr(cct) << this << " " << __func__ << ": clients with the same id "
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

    // no mirror clients to unregister
    send_remove_mirror_image();
  }

  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_remove_snap(const std::string &client_id,
					 const cls::rbd::SnapshotNamespace &snap_namespace,
					 const std::string &snap_name) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": client_id=" << client_id
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
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  std::lock_guard locker{m_lock};

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
  ldout(cct, 10) << this << " " << __func__ << dendl;

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
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  std::lock_guard locker{m_lock};
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

  send_get_clients();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_remove_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::mirror_image_remove(&op, m_image_ctx->id);

  using klass = DisableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_remove_mirror_image>(this);
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *DisableRequest<I>::handle_remove_mirror_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == -ENOENT) {
    *result = 0;
  }

  if (*result < 0) {
    lderr(cct) << "failed to remove mirror image: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  ldout(cct, 20) << this << " " << __func__
                 <<  ": removed image state from rbd_mirroring object" << dendl;

  send_notify_mirroring_watcher_removed();
  return nullptr;
}

template <typename I>
void DisableRequest<I>::send_notify_mirroring_watcher_removed() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = DisableRequest<I>;
  Context *ctx = util::create_context_callback<
    klass, &klass::handle_notify_mirroring_watcher_removed>(this);

  MirroringWatcher<I>::notify_image_updated(
    m_image_ctx->md_ctx, cls::rbd::MIRROR_IMAGE_STATE_DISABLED, m_image_ctx->id,
    m_mirror_image.global_image_id, ctx);
}

template <typename I>
Context *DisableRequest<I>::handle_notify_mirroring_watcher_removed(
  int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to send update notification: "
               << cpp_strerror(*result) << dendl;
    *result = 0;
  }

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
