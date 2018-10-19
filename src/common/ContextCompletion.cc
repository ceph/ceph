// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "common/ContextCompletion.h"

namespace ceph
{

ContextCompletion::ContextCompletion(Context *ctx, bool ignore_enoent)
  : m_ctx(ctx),
    m_ignore_enoent(ignore_enoent), m_ret(0), m_building(true), m_current_ops(0)
{
}

void ContextCompletion::finish_adding_requests() {
  bool complete;
  {
    std::lock_guard l(m_lock);
    m_building = false;
    complete = (m_current_ops == 0);
  }
  if (complete) {
    m_ctx->complete(m_ret);
    delete this;
  }
}

void ContextCompletion::start_op() {
  std::lock_guard l(m_lock);
  ++m_current_ops;
}

void ContextCompletion::finish_op(int r) {
  bool complete;
  {
    std::lock_guard l(m_lock);
    if (r < 0 && m_ret == 0 && (!m_ignore_enoent || r != -ENOENT)) {
      m_ret = r;
    }

    --m_current_ops;
    complete = (m_current_ops == 0 && !m_building);
  }
  if (complete) {
    m_ctx->complete(m_ret);
    delete this;
  }
}

} // namespace ceph
