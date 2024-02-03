// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/neorados/RADOS.hpp"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockSafeTimer.h"
#include "test/librbd/mock/crypto/MockEncryptionFormat.h"
#include "librbd/io/AsyncOperation.h"

static MockSafeTimer *s_timer;
static ceph::mutex *s_timer_lock;

namespace librbd {

MockImageCtx* MockImageCtx::s_instance = nullptr;

MockImageCtx::MockImageCtx(librbd::ImageCtx &image_ctx)
  : image_ctx(&image_ctx),
    cct(image_ctx.cct),
    perfcounter(image_ctx.perfcounter),
    snap_namespace(image_ctx.snap_namespace),
    snap_name(image_ctx.snap_name),
    snap_id(image_ctx.snap_id),
    snap_exists(image_ctx.snap_exists),
    snapc(image_ctx.snapc),
    snaps(image_ctx.snaps),
    snap_info(image_ctx.snap_info),
    snap_ids(image_ctx.snap_ids),
    old_format(image_ctx.old_format),
    read_only(image_ctx.read_only),
    read_only_flags(image_ctx.read_only_flags),
    read_only_mask(image_ctx.read_only_mask),
    clone_copy_on_read(image_ctx.clone_copy_on_read),
    lockers(image_ctx.lockers),
    exclusive_locked(image_ctx.exclusive_locked),
    lock_tag(image_ctx.lock_tag),
    asio_engine(image_ctx.asio_engine),
    rados_api(image_ctx.rados_api),
    owner_lock(image_ctx.owner_lock),
    image_lock(image_ctx.image_lock),
    timestamp_lock(image_ctx.timestamp_lock),
    async_ops_lock(image_ctx.async_ops_lock),
    copyup_list_lock(image_ctx.copyup_list_lock),
    order(image_ctx.order),
    size(image_ctx.size),
    features(image_ctx.features),
    flags(image_ctx.flags),
    op_features(image_ctx.op_features),
    operations_disabled(image_ctx.operations_disabled),
    stripe_unit(image_ctx.stripe_unit),
    stripe_count(image_ctx.stripe_count),
    object_prefix(image_ctx.object_prefix),
    header_oid(image_ctx.header_oid),
    id(image_ctx.id),
    name(image_ctx.name),
    parent_md(image_ctx.parent_md),
    format_string(image_ctx.format_string),
    group_spec(image_ctx.group_spec),
    layout(image_ctx.layout),
    io_image_dispatcher(new io::MockImageDispatcher()),
    io_object_dispatcher(new io::MockObjectDispatcher()),
    op_work_queue(new MockContextWQ()),
    plugin_registry(new MockPluginRegistry()),
    readahead_max_bytes(image_ctx.readahead_max_bytes),
    event_socket(image_ctx.event_socket),
    parent(NULL), operations(new MockOperations()),
    state(new MockImageState()),
    image_watcher(NULL), object_map(NULL),
    exclusive_lock(NULL), journal(NULL),
    trace_endpoint(image_ctx.trace_endpoint),
    sparse_read_threshold_bytes(image_ctx.sparse_read_threshold_bytes),
    discard_granularity_bytes(image_ctx.discard_granularity_bytes),
    mirroring_replay_delay(image_ctx.mirroring_replay_delay),
    non_blocking_aio(image_ctx.non_blocking_aio),
    blkin_trace_all(image_ctx.blkin_trace_all),
    enable_alloc_hint(image_ctx.enable_alloc_hint),
    alloc_hint_flags(image_ctx.alloc_hint_flags),
    read_flags(image_ctx.read_flags),
    ignore_migrating(image_ctx.ignore_migrating),
    enable_sparse_copyup(image_ctx.enable_sparse_copyup),
    mtime_update_interval(image_ctx.mtime_update_interval),
    atime_update_interval(image_ctx.atime_update_interval),
    cache(image_ctx.cache),
    config(image_ctx.config)
{
  md_ctx.dup(image_ctx.md_ctx);
  data_ctx.dup(image_ctx.data_ctx);

  if (image_ctx.image_watcher != NULL) {
    image_watcher = new MockImageWatcher();
  }
}

MockImageCtx::~MockImageCtx() {
  wait_for_async_requests();
  wait_for_async_ops();
  image_ctx->md_ctx.aio_flush();
  image_ctx->data_ctx.aio_flush();
  image_ctx->op_work_queue->drain();
  delete state;
  delete operations;
  delete image_watcher;
  delete op_work_queue;
  delete plugin_registry;
  delete io_image_dispatcher;
  delete io_object_dispatcher;
}

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
  if (snap_id != CEPH_NOSNAP) {
    ctx->set_read_snap(snap_id);
  }
  if (!snapc.snaps.empty()) {
    ctx->set_write_snap_context(
      {{snapc.seq, {snapc.snaps.begin(), snapc.snaps.end()}}});
  }
  return ctx;
}

IOContext MockImageCtx::duplicate_data_io_context() {
  return std::make_shared<neorados::IOContext>(*get_data_io_context());
}

uint64_t MockImageCtx::get_data_offset() const {
  if (encryption_format != nullptr) {
    return encryption_format->get_crypto()->get_data_offset();
  }
  return 0;
}

} // namespace librbd
