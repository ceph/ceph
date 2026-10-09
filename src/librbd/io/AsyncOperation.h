// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef LIBRBD_IO_ASYNC_OPERATION_H
#define LIBRBD_IO_ASYNC_OPERATION_H

#include "include/ceph_assert.h"
#include "include/xlist.h"
#include <list>

class Context;

namespace librbd {

class ImageCtx;

namespace io {

class AsyncOperation {
public:

  AsyncOperation()
    : m_image_ctx(NULL), m_xlist_item(this)
  {
  }

  ~AsyncOperation()
  {
    ceph_assert(!m_xlist_item.is_on_list());
  }

  inline bool started() const {
    return m_xlist_item.is_on_list();
  }

  // pass writes=false for an op that never changes the image, so that
  // flush_writes() doesn't wait for it
  void start_op(ImageCtx &image_ctx, bool writes = true);
  void finish_op();

  // wait for all older operations to finish
  void flush(Context *on_finish);
  // wait for all older operations that may write to finish
  void flush_writes(Context *on_finish);

private:

  ImageCtx *m_image_ctx;
  bool m_writes = true;
  xlist<AsyncOperation *>::item m_xlist_item;
  std::list<Context *> m_flush_contexts;
  std::list<Context *> m_flush_writes_contexts;

  // the next older op, or nullptr. requires async_ops_lock
  AsyncOperation* find_older_op(bool writes_only);

};

} // namespace io
} // namespace librbd

#endif // LIBRBD_IO_ASYNC_OPERATION_H
