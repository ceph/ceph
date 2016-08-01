// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef LIBRBD_ASYNC_OPERATION_H
#define LIBRBD_ASYNC_OPERATION_H

#include "include/assert.h"
#include "include/xlist.h"
#include <list>

class Context;

namespace librbd {

class ImageCtx;

class AsyncOperation {
public:

  AsyncOperation()
    : m_image_ctx(NULL), m_xlist_item(this)
  {
  }

  ~AsyncOperation()
  {
    assert(!m_xlist_item.is_on_list());
  }

  inline bool started() const {
    return m_xlist_item.is_on_list();
  }

  void start_op(ImageCtx &image_ctx);
  void finish_op();

  void add_flush_context(Context *on_finish);

private:

  ImageCtx *m_image_ctx;
  xlist<AsyncOperation *>::item m_xlist_item;
  std::list<Context *> m_flush_contexts;

};

} // namespace librbd

#endif // LIBRBD_ASYNC_OPERATION_H
