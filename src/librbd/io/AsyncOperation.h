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

  // pass false for reads and list-snaps, which diff-iterate can skip
  void start_op(ImageCtx &image_ctx, bool writes = true);
  void finish_op();

  // Older writes and flushes are always waited on.
  void flush(Context *on_finish, bool skip_reads = false);

private:

  ImageCtx *m_image_ctx;
  bool m_writes = true;
  xlist<AsyncOperation *>::item m_xlist_item;
  using FlushContexts = std::list<Context *>;
  FlushContexts m_flush_contexts;
  FlushContexts m_flush_writes_contexts;

  // Waiter queue on the next older matching op. Requires async_ops_lock.
  FlushContexts* find_older_waiters(bool skip_reads);

};

} // namespace io
} // namespace librbd

#endif // LIBRBD_IO_ASYNC_OPERATION_H
