// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_ASYNC_COMPLETION_H
#define CEPH_ASYNC_COMPLETION_H

#include "include/int_types.h"
#include "include/Context.h"

namespace ceph {

class ContextCompletion {
public:
  ContextCompletion(Context *ctx, bool ignore_enoent);

  void finish_adding_requests();

  void start_op();
  void finish_op(int r);

private:
  Mutex m_lock;
  Context *m_ctx;
  bool m_ignore_enoent;
  int m_ret;
  bool m_building;
  uint64_t m_current_ops;
};

class C_ContextCompletion : public Context {
public:
  C_ContextCompletion(ContextCompletion &context_completion)
    : m_context_completion(context_completion)
  {
    m_context_completion.start_op();
  }

  virtual void finish(int r) {
    m_context_completion.finish_op(r);
  }

private:
  ContextCompletion &m_context_completion;
};

} // namespace ceph

#endif // CEPH_ASYNC_COMPLETION_H
