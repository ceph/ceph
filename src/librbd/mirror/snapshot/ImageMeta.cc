// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/ImageMeta.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "json_spirit/json_spirit.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/WatchNotifyTypes.h"
#include "librbd/mirror/snapshot/Utils.h"
#include "librbd/watcher/Notifier.h"

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::ImageMeta: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_rados_callback;
using librbd::mirror::snapshot::util::get_image_meta_key;

template <typename I>
ImageMeta<I>::ImageMeta(I* image_ctx, const std::string& mirror_uuid)
  : m_image_ctx(image_ctx), m_mirror_uuid(mirror_uuid) {
}

template <typename I>
void ImageMeta<I>::load(Context* on_finish) {
  ldout(m_image_ctx->cct, 15) << "oid=" << m_image_ctx->header_oid << ", "
                              << "key=" << get_image_meta_key(m_mirror_uuid)
                              << dendl;

  librados::ObjectReadOperation op;
  cls_client::metadata_get_start(&op, get_image_meta_key(m_mirror_uuid));

  m_out_bl.clear();
  auto ctx = new LambdaContext([this, on_finish](int r) {
      handle_load(on_finish, r);
    });
  auto aio_comp = create_rados_callback(ctx);
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, aio_comp,
                                          &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ImageMeta<I>::handle_load(Context* on_finish, int r) {
  ldout(m_image_ctx->cct, 15) << "r=" << r << dendl;

  std::string data;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = cls_client::metadata_get_finish(&it, &data);
  }

  if (r == -ENOENT) {
    ldout(m_image_ctx->cct, 15) << "no snapshot-based mirroring image-meta: "
                                << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  } else if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to load snapshot-based mirroring "
                            << "image-meta: " << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  bool json_valid = false;
  json_spirit::mValue json_root;
  if (json_spirit::read(data, json_root)) {
    try {
      auto& json_obj = json_root.get_obj();
      resync_requested = json_obj["resync_requested"].get_bool();
      json_valid = true;
    } catch (std::runtime_error&) {
    }
  }

  if (!json_valid) {
    lderr(m_image_ctx->cct) << "invalid image-meta JSON received" << dendl;
    on_finish->complete(-EBADMSG);
    return;
  }

  on_finish->complete(0);
}

template <typename I>
void ImageMeta<I>::save(Context* on_finish) {
  ldout(m_image_ctx->cct, 15) << "oid=" << m_image_ctx->header_oid << ", "
                              << "key=" << get_image_meta_key(m_mirror_uuid)
                              << dendl;

  // simple implementation for now
  std::string json = "{\"resync_requested\": " +
                     std::string(resync_requested ? "true" : "false") + "}";

  bufferlist bl;
  bl.append(json);

  // avoid using built-in metadata_set operation since that would require
  // opening the non-primary image in read/write mode which isn't supported
  librados::ObjectWriteOperation op;
  cls_client::metadata_set(&op, {{get_image_meta_key(m_mirror_uuid), bl}});

  auto ctx = new LambdaContext([this, on_finish](int r) {
      handle_save(on_finish, r);
    });
  auto aio_comp = create_rados_callback(ctx);
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, aio_comp,
                                          &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ImageMeta<I>::handle_save(Context* on_finish, int r) {
  ldout(m_image_ctx->cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to save snapshot-based mirroring "
                            << "image-meta: " << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  notify_update(on_finish);
}

template <typename I>
void ImageMeta<I>::notify_update(Context* on_finish) {
  ldout(m_image_ctx->cct, 15) << dendl;

  // directly send header notification on image since you cannot
  // open a non-primary image read/write and therefore cannot re-use
  // the ImageWatcher to send the notification
  bufferlist bl;
  encode(watch_notify::NotifyMessage(new watch_notify::HeaderUpdatePayload()),
         bl);

  m_out_bl.clear();
  auto ctx = new LambdaContext([this, on_finish](int r) {
      handle_notify_update(on_finish, r);
    });
  auto aio_comp = create_rados_callback(ctx);
  int r = m_image_ctx->md_ctx.aio_notify(
    m_image_ctx->header_oid, aio_comp, bl, watcher::Notifier::NOTIFY_TIMEOUT,
    &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ImageMeta<I>::handle_notify_update(Context* on_finish, int r) {
  ldout(m_image_ctx->cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to notify image update: "
                            << cpp_strerror(r) << dendl;
  }
  on_finish->complete(r);
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::ImageMeta<librbd::ImageCtx>;
