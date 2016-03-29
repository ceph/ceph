// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SyncPointCreateRequest.h"
#include "include/uuid.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_sync::SyncPointCreateRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_sync {

namespace {

static const std::string SNAP_NAME_PREFIX(".rbd-mirror");

} // anonymous namespace

using librbd::util::create_context_callback;

template <typename I>
SyncPointCreateRequest<I>::SyncPointCreateRequest(I *remote_image_ctx,
                                                  const std::string &mirror_uuid,
                                                  Journaler *journaler,
                                                  MirrorPeerClientMeta *client_meta,
                                                  Context *on_finish)
  : m_remote_image_ctx(remote_image_ctx), m_mirror_uuid(mirror_uuid),
    m_journaler(journaler), m_client_meta(client_meta), m_on_finish(on_finish),
    m_client_meta_copy(*client_meta) {
  assert(m_client_meta->sync_points.size() < 2);

  // initialize the updated client meta with the new sync point
  m_client_meta_copy.sync_points.emplace_back();
  if (m_client_meta_copy.sync_points.size() > 1) {
    m_client_meta_copy.sync_points.back().from_snap_name =
      m_client_meta_copy.sync_points.front().snap_name;
  }
}

template <typename I>
void SyncPointCreateRequest<I>::send() {
  send_update_client();
}

template <typename I>
void SyncPointCreateRequest<I>::send_update_client() {
  uuid_d uuid_gen;
  uuid_gen.generate_random();

  MirrorPeerSyncPoint &sync_point = m_client_meta_copy.sync_points.back();
  sync_point.snap_name = SNAP_NAME_PREFIX + "." + m_mirror_uuid + "." +
                         uuid_gen.to_string();

  CephContext *cct = m_remote_image_ctx->cct;
  ldout(cct, 20) << ": sync_point=" << sync_point << dendl;

  bufferlist client_data_bl;
  librbd::journal::ClientData client_data(m_client_meta_copy);
  ::encode(client_data, client_data_bl);

  Context *ctx = create_context_callback<
    SyncPointCreateRequest<I>, &SyncPointCreateRequest<I>::handle_update_client>(
      this);
  m_journaler->update_client(client_data_bl, ctx);
}

template <typename I>
void SyncPointCreateRequest<I>::handle_update_client(int r) {
  CephContext *cct = m_remote_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to update client data: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  // update provided meta structure to reflect reality
  *m_client_meta = m_client_meta_copy;

  send_refresh_image();
}

template <typename I>
void SyncPointCreateRequest<I>::send_refresh_image() {
  CephContext *cct = m_remote_image_ctx->cct;
  ldout(cct, 20) << dendl;

  Context *ctx = create_context_callback<
    SyncPointCreateRequest<I>, &SyncPointCreateRequest<I>::handle_refresh_image>(
      this);
  m_remote_image_ctx->state->refresh(ctx);
}

template <typename I>
void SyncPointCreateRequest<I>::handle_refresh_image(int r) {
  CephContext *cct = m_remote_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "remote image refresh failed: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_create_snap();
}

template <typename I>
void SyncPointCreateRequest<I>::send_create_snap() {
  CephContext *cct = m_remote_image_ctx->cct;
  ldout(cct, 20) << dendl;

  MirrorPeerSyncPoint &sync_point = m_client_meta_copy.sync_points.back();

  Context *ctx = create_context_callback<
    SyncPointCreateRequest<I>, &SyncPointCreateRequest<I>::handle_create_snap>(
      this);
  m_remote_image_ctx->operations->snap_create(
    sync_point.snap_name.c_str(), ctx);
}

template <typename I>
void SyncPointCreateRequest<I>::handle_create_snap(int r) {
  CephContext *cct = m_remote_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r == -EEXIST) {
    send_update_client();
    return;
  } else if (r < 0) {
    lderr(cct) << "failed to create snapshot: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void SyncPointCreateRequest<I>::finish(int r) {
  CephContext *cct = m_remote_image_ctx->cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_sync::SyncPointCreateRequest<librbd::ImageCtx>;
