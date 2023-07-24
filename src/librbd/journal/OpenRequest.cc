// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/OpenRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::OpenRequest: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace journal {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using util::C_DecodeTags;

template <typename I>
OpenRequest<I>::OpenRequest(I *image_ctx, Journaler *journaler, ceph::mutex *lock,
                            journal::ImageClientMeta *client_meta,
                            uint64_t *tag_tid, journal::TagData *tag_data,
                            Context *on_finish)
  : m_image_ctx(image_ctx), m_journaler(journaler), m_lock(lock),
    m_client_meta(client_meta), m_tag_tid(tag_tid), m_tag_data(tag_data),
    m_on_finish(on_finish) {
}

template <typename I>
void OpenRequest<I>::send() {
  send_init();
}

template <typename I>
void OpenRequest<I>::send_init() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  m_journaler->init(create_async_context_callback(
    *m_image_ctx, create_context_callback<
      OpenRequest<I>, &OpenRequest<I>::handle_init>(this)));
}

template <typename I>
void OpenRequest<I>::handle_init(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to initialize journal: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  // locate the master image client record
  cls::journal::Client client;
  r = m_journaler->get_cached_client(Journal<ImageCtx>::IMAGE_CLIENT_ID,
                                     &client);
  if (r < 0) {
    lderr(cct) << "failed to locate master image client" << dendl;
    finish(r);
    return;
  }

  librbd::journal::ClientData client_data;
  auto bl = client.data.cbegin();
  try {
    decode(client_data, bl);
  } catch (const buffer::error &err) {
    lderr(cct) << "failed to decode client meta data: " << err.what()
               << dendl;
    finish(-EINVAL);
    return;
  }

  journal::ImageClientMeta *image_client_meta =
    boost::get<journal::ImageClientMeta>(&client_data.client_meta);
  if (image_client_meta == nullptr) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to extract client meta data" << dendl;
    finish(-EINVAL);
    return;
  }

  ldout(cct, 20) << this << " " << __func__ << ": "
                 << "client: " << client << ", "
                 << "image meta: " << *image_client_meta << dendl;

  m_tag_class = image_client_meta->tag_class;
  {
    std::lock_guard locker{*m_lock};
    *m_client_meta = *image_client_meta;
  }

  send_get_tags();
}

template <typename I>
void OpenRequest<I>::send_get_tags() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  C_DecodeTags *tags_ctx = new C_DecodeTags(
    cct, m_lock, m_tag_tid, m_tag_data, create_async_context_callback(
      *m_image_ctx, create_context_callback<
        OpenRequest<I>, &OpenRequest<I>::handle_get_tags>(this)));
  m_journaler->get_tags(m_tag_class, &tags_ctx->tags, tags_ctx);
}

template <typename I>
void OpenRequest<I>::handle_get_tags(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << this << " " << __func__ << ": "
               << "failed to decode journal tags: " << cpp_strerror(r) << dendl;
  }

  finish(r);
}

template <typename I>
void OpenRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace journal
} // namespace librbd

template class librbd::journal::OpenRequest<librbd::ImageCtx>;
