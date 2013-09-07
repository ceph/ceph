// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_WATCHCTX_H
#define CEPH_LIBRBD_WATCHCTX_H

#include "include/int_types.h"

#include "common/Mutex.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"

class ImageCtx;

namespace librbd {

  class WatchCtx : public librados::WatchCtx {
    ImageCtx *ictx;
    bool valid;
    Mutex lock;
  public:
    uint64_t cookie;
    WatchCtx(ImageCtx *ctx) : ictx(ctx),
			      valid(true),
			      lock("librbd::WatchCtx"),
			      cookie(0) {}
    virtual ~WatchCtx() {}
    void invalidate();
    virtual void notify(uint8_t opcode, uint64_t ver, ceph::bufferlist& bl);
  };
}

#endif
