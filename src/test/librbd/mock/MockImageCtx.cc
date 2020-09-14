// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/neorados/RADOS.hpp"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockSafeTimer.h"
#include "librbd/io/AsyncOperation.h"

static MockSafeTimer *s_timer;
static ceph::mutex *s_timer_lock;

namespace librbd {

MockImageCtx* MockImageCtx::s_instance = nullptr;

void MockImageCtx::set_timer_instance(MockSafeTimer *timer,
                                      ceph::mutex *timer_lock) {
  s_timer = timer;
  s_timer_lock = timer_lock;
}

void MockImageCtx::get_timer_instance(CephContext *cct, MockSafeTimer **timer,
                                      ceph::mutex **timer_lock) {
  *timer = s_timer;
  *timer_lock = s_timer_lock;
}

void MockImageCtx::wait_for_async_ops() {
  io::AsyncOperation async_op;
  async_op.start_op(*image_ctx);

  C_SaferCond ctx;
  async_op.flush(&ctx);
  ctx.wait();

  async_op.finish_op();
}

IOContext MockImageCtx::get_data_io_context() {
  auto ctx = std::make_shared<neorados::IOContext>(
    data_ctx.get_id(), data_ctx.get_namespace());
  ctx->read_snap(snap_id);
  ctx->write_snap_context(
    {{snapc.seq, {snapc.snaps.begin(), snapc.snaps.end()}}});
  return ctx;
}

} // namespace librbd
