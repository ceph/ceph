// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/PromoteRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "journal/Journaler.h"
#include "journal/Settings.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/journal/OpenRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::PromoteRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace journal {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;

template <typename I>
PromoteRequest<I>::PromoteRequest(I *image_ctx, bool force, Context *on_finish)
  : m_image_ctx(image_ctx), m_force(force), m_on_finish(on_finish),
    m_lock(ceph::make_mutex("PromoteRequest::m_lock")) {
}

template <typename I>
void PromoteRequest<I>::send() {
  send_open();
}

template <typename I>
void PromoteRequest<I>::send_open() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  m_journaler = new Journaler(m_image_ctx->md_ctx, m_image_ctx->id,
                              Journal<>::IMAGE_CLIENT_ID, {}, nullptr);
  Context *ctx = create_async_context_callback(
    *m_image_ctx, create_context_callback<
      PromoteRequest<I>, &PromoteRequest<I>::handle_open>(this));
  auto open_req = OpenRequest<I>::create(m_image_ctx, m_journaler,
                                         &m_lock, &m_client_meta,
                                         &m_tag_tid, &m_tag_data, ctx);
  open_req->send();
}

template <typename I>
void PromoteRequest<I>::handle_open(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    m_ret_val = r;
    lderr(cct) << "failed to open journal: " << cpp_strerror(r) << dendl;
    shut_down();
    return;
  }

  allocate_tag();
}

template <typename I>
void PromoteRequest<I>::allocate_tag() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  journal::TagPredecessor predecessor;
  if (!m_force && m_tag_data.mirror_uuid == Journal<>::ORPHAN_MIRROR_UUID) {
    // orderly promotion -- demotion epoch will have a single entry
    // so link to our predecessor (demotion) epoch
    predecessor = TagPredecessor{Journal<>::ORPHAN_MIRROR_UUID, true, m_tag_tid,
                                 1};
  } else {
    // forced promotion -- create an epoch no peers can link against
    predecessor = TagPredecessor{Journal<>::LOCAL_MIRROR_UUID, true, m_tag_tid,
                                 0};
  }

  TagData tag_data;
  tag_data.mirror_uuid = Journal<>::LOCAL_MIRROR_UUID;
  tag_data.predecessor = predecessor;

  bufferlist tag_bl;
  encode(tag_data, tag_bl);

  Context *ctx = create_context_callback<
    PromoteRequest<I>, &PromoteRequest<I>::handle_allocate_tag>(this);
  m_journaler->allocate_tag(m_client_meta.tag_class, tag_bl, &m_tag, ctx);
}

template <typename I>
void PromoteRequest<I>::handle_allocate_tag(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    m_ret_val = r;
    lderr(cct) << "failed to allocate tag: " << cpp_strerror(r) << dendl;
    shut_down();
    return;
  }

  m_tag_tid = m_tag.tid;
  append_event();
}

template <typename I>
void PromoteRequest<I>::append_event() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  EventEntry event_entry{DemotePromoteEvent{}, {}};
  bufferlist event_entry_bl;
  encode(event_entry, event_entry_bl);

  m_journaler->start_append(0);
  m_future = m_journaler->append(m_tag_tid, event_entry_bl);

  auto ctx = create_context_callback<
    PromoteRequest<I>, &PromoteRequest<I>::handle_append_event>(this);
  m_future.flush(ctx);
}

template <typename I>
void PromoteRequest<I>::handle_append_event(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    m_ret_val = r;
    lderr(cct) << "failed to append promotion journal event: "
               << cpp_strerror(r) << dendl;
    stop_append();
    return;
  }

  commit_event();
}

template <typename I>
void PromoteRequest<I>::commit_event() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  m_journaler->committed(m_future);

  auto ctx = create_context_callback<
    PromoteRequest<I>, &PromoteRequest<I>::handle_commit_event>(this);
  m_journaler->flush_commit_position(ctx);
}

template <typename I>
void PromoteRequest<I>::handle_commit_event(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    m_ret_val = r;
    lderr(cct) << "failed to flush promote commit position: "
               << cpp_strerror(r) << dendl;
  }

  stop_append();
}

template <typename I>
void PromoteRequest<I>::stop_append() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    PromoteRequest<I>, &PromoteRequest<I>::handle_stop_append>(this);
  m_journaler->stop_append(ctx);
}

template <typename I>
void PromoteRequest<I>::handle_stop_append(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
    lderr(cct) << "failed to stop journal append: " << cpp_strerror(r) << dendl;
  }

  shut_down();
}

template <typename I>
void PromoteRequest<I>::shut_down() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  Context *ctx = create_async_context_callback(
    *m_image_ctx, create_context_callback<
      PromoteRequest<I>, &PromoteRequest<I>::handle_shut_down>(this));
  m_journaler->shut_down(ctx);
}

template <typename I>
void PromoteRequest<I>::handle_shut_down(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to shut down journal: " << cpp_strerror(r) << dendl;
  }

  delete m_journaler;
  finish(r);
}

template <typename I>
void PromoteRequest<I>::finish(int r) {
  if (m_ret_val < 0) {
    r = m_ret_val;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace journal
} // namespace librbd

template class librbd::journal::PromoteRequest<librbd::ImageCtx>;
