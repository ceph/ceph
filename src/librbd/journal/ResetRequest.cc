// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/journal/ResetRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "journal/Journaler.h"
#include "journal/Settings.h"
#include "include/assert.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/journal/CreateRequest.h"
#include "librbd/journal/RemoveRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::journal::ResetRequest: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace journal {

using util::create_async_context_callback;
using util::create_context_callback;

template<typename I>
void ResetRequest<I>::send() {
   init_journaler();
}

template<typename I>
void ResetRequest<I>::init_journaler() {
  ldout(m_cct, 10) << dendl;

  m_journaler = new Journaler(m_io_ctx, m_image_id, m_client_id, {});
  Context *ctx = create_context_callback<
     ResetRequest<I>, &ResetRequest<I>::handle_init_journaler>(this);
  m_journaler->init(ctx);
}

template<typename I>
void ResetRequest<I>::handle_init_journaler(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    ldout(m_cct, 5) << "journal does not exist" << dendl;
    m_ret_val = r;
  } else if (r < 0) {
    lderr(m_cct) << "failed to init journaler: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
  } else {
    int64_t pool_id;
    m_journaler->get_metadata(&m_order, &m_splay_width, &pool_id);

    if (pool_id != -1) {
      librados::Rados rados(m_io_ctx);
      r = rados.pool_reverse_lookup(pool_id, &m_object_pool_name);
      if (r < 0) {
        lderr(m_cct) << "failed to lookup data pool: " << cpp_strerror(r)
                     << dendl;
        m_ret_val = r;
      }
    }
  }

  shut_down_journaler();
}

template<typename I>
void ResetRequest<I>::shut_down_journaler() {
  ldout(m_cct, 10) << dendl;

  Context *ctx = create_async_context_callback(
    m_op_work_queue, create_context_callback<
      ResetRequest<I>, &ResetRequest<I>::handle_journaler_shutdown>(this));
  m_journaler->shut_down(ctx);
}

template<typename I>
void ResetRequest<I>::handle_journaler_shutdown(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  delete m_journaler;
  if (r < 0) {
    lderr(m_cct) << "failed to shut down journaler: " << cpp_strerror(r)
                 << dendl;
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }

  if (m_ret_val < 0) {
    finish(m_ret_val);
    return;
  }

  remove_journal();
}

template<typename I>
void ResetRequest<I>::remove_journal() {
  ldout(m_cct, 10) << dendl;

  Context *ctx = create_context_callback<
    ResetRequest<I>, &ResetRequest<I>::handle_remove_journal>(this);
  auto req = RemoveRequest<I>::create(m_io_ctx, m_image_id, m_client_id,
                                      m_op_work_queue, ctx);
  req->send();
}

template<typename I>
void ResetRequest<I>::handle_remove_journal(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove journal: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  create_journal();
}

template<typename I>
void ResetRequest<I>::create_journal() {
  ldout(m_cct, 10) << dendl;

  Context *ctx = create_context_callback<
    ResetRequest<I>, &ResetRequest<I>::handle_create_journal>(this);
  journal::TagData tag_data(m_mirror_uuid);
  auto req = CreateRequest<I>::create(m_io_ctx, m_image_id, m_order,
                                      m_splay_width, m_object_pool_name,
                                      cls::journal::Tag::TAG_CLASS_NEW,
                                      tag_data, m_client_id, m_op_work_queue,
                                      ctx);
  req->send();
}

template<typename I>
void ResetRequest<I>::handle_create_journal(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to create journal: " << cpp_strerror(r) << dendl;
  }
  finish(r);
}

template<typename I>
void ResetRequest<I>::finish(int r) {
   ldout(m_cct, 10) << "r=" << r << dendl;

   m_on_finish->complete(r);
   delete this;
}

} // namespace journal
} // namespace librbd

template class librbd::journal::ResetRequest<librbd::ImageCtx>;
