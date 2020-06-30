// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_fixture.h"
#include "include/stringify.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "journal/Journaler.h"
#include "journal/Settings.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Journal.h"
#include "librbd/Operations.h"
#include "librbd/api/Io.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ReadResult.h"
#include "librbd/journal/Types.h"
#include "tools/rbd_mirror/ImageSync.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/Throttler.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"

void register_test_image_sync() {
}

namespace rbd {
namespace mirror {

namespace {

int flush(librbd::ImageCtx *image_ctx) {
  C_SaferCond ctx;
  auto aio_comp = librbd::io::AioCompletion::create_and_start(
    &ctx, image_ctx, librbd::io::AIO_TYPE_FLUSH);
  auto req = librbd::io::ImageDispatchSpec<>::create_flush(
    *image_ctx, librbd::io::IMAGE_DISPATCH_LAYER_INTERNAL_START, aio_comp,
    librbd::io::FLUSH_SOURCE_INTERNAL, {});
  req->send();
  return ctx.wait();
}

void scribble(librbd::ImageCtx *image_ctx, int num_ops, uint64_t max_size)
{
  max_size = std::min<uint64_t>(image_ctx->size, max_size);
  for (int i=0; i<num_ops; i++) {
    uint64_t off = rand() % (image_ctx->size - max_size + 1);
    uint64_t len = 1 + rand() % max_size;

    if (rand() % 4 == 0) {
      ASSERT_EQ((int)len,
                librbd::api::Io<>::discard(
                  *image_ctx, off, len, image_ctx->discard_granularity_bytes));
    } else {
      bufferlist bl;
      bl.append(std::string(len, '1'));
      ASSERT_EQ((int)len, librbd::api::Io<>::write(
                  *image_ctx, off, len, std::move(bl), 0));
    }
  }

  std::shared_lock owner_locker{image_ctx->owner_lock};
  ASSERT_EQ(0, flush(image_ctx));
}

} // anonymous namespace
class TestImageSync : public TestFixture {
public:

  void SetUp() override {
    TestFixture::SetUp();
    create_and_open(m_local_io_ctx, &m_local_image_ctx);
    create_and_open(m_remote_io_ctx, &m_remote_image_ctx);

    auto cct = reinterpret_cast<CephContext*>(m_local_io_ctx.cct());
    m_image_sync_throttler = rbd::mirror::Throttler<>::create(
        cct, "rbd_mirror_concurrent_image_syncs");

    m_instance_watcher = rbd::mirror::InstanceWatcher<>::create(
        m_local_io_ctx, m_threads->work_queue, nullptr, m_image_sync_throttler);
    m_instance_watcher->handle_acquire_leader();

    ContextWQ* context_wq;
    librbd::Journal<>::get_work_queue(cct, &context_wq);

    m_remote_journaler = new ::journal::Journaler(
      context_wq, m_threads->timer, &m_threads->timer_lock,
      m_remote_io_ctx, m_remote_image_ctx->id, "mirror-uuid", {}, nullptr);

    m_client_meta = {"image-id"};

    librbd::journal::ClientData client_data(m_client_meta);
    bufferlist client_data_bl;
    encode(client_data, client_data_bl);

    ASSERT_EQ(0, m_remote_journaler->register_client(client_data_bl));

    m_state_builder = rbd::mirror::image_replayer::journal::StateBuilder<
      librbd::ImageCtx>::create("global image id");
    m_state_builder->remote_journaler = m_remote_journaler;
    m_state_builder->remote_client_meta = m_client_meta;
    m_sync_point_handler = m_state_builder->create_sync_point_handler();
  }

  void TearDown() override {
    m_instance_watcher->handle_release_leader();

    m_state_builder->remote_journaler = nullptr;
    m_state_builder->destroy_sync_point_handler();
    m_state_builder->destroy();

    delete m_remote_journaler;
    delete m_instance_watcher;
    delete m_image_sync_throttler;

    TestFixture::TearDown();
  }

  void create_and_open(librados::IoCtx &io_ctx, librbd::ImageCtx **image_ctx) {
    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(io_ctx, m_image_name, image_ctx));

    C_SaferCond ctx;
    {
      std::shared_lock owner_locker{(*image_ctx)->owner_lock};
      (*image_ctx)->exclusive_lock->try_acquire_lock(&ctx);
    }
    ASSERT_EQ(0, ctx.wait());
    ASSERT_TRUE((*image_ctx)->exclusive_lock->is_lock_owner());
  }

  ImageSync<> *create_request(Context *ctx) {
    return new ImageSync<>(m_threads, m_local_image_ctx, m_remote_image_ctx,
                           "mirror-uuid", m_sync_point_handler,
                           m_instance_watcher, nullptr, ctx);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx;
  rbd::mirror::Throttler<> *m_image_sync_throttler;
  rbd::mirror::InstanceWatcher<> *m_instance_watcher;
  ::journal::Journaler *m_remote_journaler;
  librbd::journal::MirrorPeerClientMeta m_client_meta;
  rbd::mirror::image_replayer::journal::StateBuilder<librbd::ImageCtx>* m_state_builder = nullptr;
  rbd::mirror::image_sync::SyncPointHandler* m_sync_point_handler = nullptr;
};

TEST_F(TestImageSync, Empty) {
  C_SaferCond ctx;
  ImageSync<> *request = create_request(&ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(0U, m_client_meta.sync_points.size());
  ASSERT_EQ(0, m_remote_image_ctx->state->refresh());
  ASSERT_EQ(0U, m_remote_image_ctx->snap_ids.size());
  ASSERT_EQ(0, m_local_image_ctx->state->refresh());
  ASSERT_EQ(1U, m_local_image_ctx->snap_ids.size()); // deleted on journal replay
}

TEST_F(TestImageSync, Simple) {
  scribble(m_remote_image_ctx, 10, 102400);

  C_SaferCond ctx;
  ImageSync<> *request = create_request(&ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  int64_t object_size = std::min<int64_t>(
    m_remote_image_ctx->size, 1 << m_remote_image_ctx->order);
  bufferlist read_remote_bl;
  read_remote_bl.append(std::string(object_size, '1'));
  bufferlist read_local_bl;
  read_local_bl.append(std::string(object_size, '1'));

  for (uint64_t offset = 0; offset < m_remote_image_ctx->size;
       offset += object_size) {
    ASSERT_LE(0, librbd::api::Io<>::read(
                   *m_remote_image_ctx, offset, object_size,
                   librbd::io::ReadResult{&read_remote_bl}, 0));
    ASSERT_LE(0, librbd::api::Io<>::read(
                   *m_local_image_ctx, offset, object_size,
                   librbd::io::ReadResult{&read_local_bl}, 0));
    ASSERT_TRUE(read_remote_bl.contents_equal(read_local_bl));
  }
}

TEST_F(TestImageSync, Resize) {
  int64_t object_size = std::min<int64_t>(
    m_remote_image_ctx->size, 1 << m_remote_image_ctx->order);

  uint64_t off = 0;
  uint64_t len = object_size / 10;

  bufferlist bl;
  bl.append(std::string(len, '1'));
  ASSERT_EQ((int)len, librbd::api::Io<>::write(
                        *m_remote_image_ctx, off, len, std::move(bl), 0));
  {
    std::shared_lock owner_locker{m_remote_image_ctx->owner_lock};
    ASSERT_EQ(0, flush(m_remote_image_ctx));
  }

  ASSERT_EQ(0, create_snap(m_remote_image_ctx, "snap", nullptr));

  uint64_t size = object_size - 1;
  librbd::NoOpProgressContext no_op_progress_ctx;
  ASSERT_EQ(0, m_remote_image_ctx->operations->resize(size, true,
                                                      no_op_progress_ctx));

  C_SaferCond ctx;
  ImageSync<> *request = create_request(&ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  bufferlist read_remote_bl;
  read_remote_bl.append(std::string(len, '\0'));
  bufferlist read_local_bl;
  read_local_bl.append(std::string(len, '\0'));

  ASSERT_LE(0, librbd::api::Io<>::read(
              *m_remote_image_ctx, off, len,
              librbd::io::ReadResult{&read_remote_bl}, 0));
  ASSERT_LE(0, librbd::api::Io<>::read(
              *m_local_image_ctx, off, len,
              librbd::io::ReadResult{&read_local_bl}, 0));

  ASSERT_TRUE(read_remote_bl.contents_equal(read_local_bl));
}

TEST_F(TestImageSync, Discard) {
  int64_t object_size = std::min<int64_t>(
    m_remote_image_ctx->size, 1 << m_remote_image_ctx->order);

  uint64_t off = 0;
  uint64_t len = object_size / 10;

  bufferlist bl;
  bl.append(std::string(len, '1'));
  ASSERT_EQ((int)len, librbd::api::Io<>::write(
              *m_remote_image_ctx, off, len, std::move(bl), 0));
  {
    std::shared_lock owner_locker{m_remote_image_ctx->owner_lock};
    ASSERT_EQ(0, flush(m_remote_image_ctx));
  }

  ASSERT_EQ(0, create_snap(m_remote_image_ctx, "snap", nullptr));

  ASSERT_EQ((int)len - 2,
            librbd::api::Io<>::discard(
              *m_remote_image_ctx, off + 1, len - 2,
              m_remote_image_ctx->discard_granularity_bytes));
  {
    std::shared_lock owner_locker{m_remote_image_ctx->owner_lock};
    ASSERT_EQ(0, flush(m_remote_image_ctx));
  }

  C_SaferCond ctx;
  ImageSync<> *request = create_request(&ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  bufferlist read_remote_bl;
  read_remote_bl.append(std::string(object_size, '\0'));
  bufferlist read_local_bl;
  read_local_bl.append(std::string(object_size, '\0'));

  ASSERT_LE(0, librbd::api::Io<>::read(
              *m_remote_image_ctx, off, len,
              librbd::io::ReadResult{&read_remote_bl}, 0));
  ASSERT_LE(0, librbd::api::Io<>::read(
              *m_local_image_ctx, off, len,
              librbd::io::ReadResult{&read_local_bl}, 0));

  ASSERT_TRUE(read_remote_bl.contents_equal(read_local_bl));
}

TEST_F(TestImageSync, SnapshotStress) {
  std::list<std::string> snap_names;

  const int num_snaps = 4;
  for (int idx = 0; idx <= num_snaps; ++idx) {
    scribble(m_remote_image_ctx, 10, 102400);

    librbd::NoOpProgressContext no_op_progress_ctx;
    uint64_t size = 1 + rand() % m_image_size;
    ASSERT_EQ(0, m_remote_image_ctx->operations->resize(size, true,
                                                        no_op_progress_ctx));
    ASSERT_EQ(0, m_remote_image_ctx->state->refresh());

    if (idx < num_snaps) {
      snap_names.push_back("snap" + stringify(idx + 1));
      ASSERT_EQ(0, create_snap(m_remote_image_ctx, snap_names.back().c_str(),
                               nullptr));
    } else {
      snap_names.push_back("");
    }
  }

  C_SaferCond ctx;
  ImageSync<> *request = create_request(&ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  int64_t object_size = std::min<int64_t>(
    m_remote_image_ctx->size, 1 << m_remote_image_ctx->order);
  bufferlist read_remote_bl;
  read_remote_bl.append(std::string(object_size, '1'));
  bufferlist read_local_bl;
  read_local_bl.append(std::string(object_size, '1'));

  for (auto &snap_name : snap_names) {
    uint64_t remote_snap_id;
    {
      std::shared_lock remote_image_locker{m_remote_image_ctx->image_lock};
      remote_snap_id = m_remote_image_ctx->get_snap_id(
        cls::rbd::UserSnapshotNamespace{}, snap_name);
    }

    uint64_t remote_size;
    {
      C_SaferCond ctx;
      m_remote_image_ctx->state->snap_set(remote_snap_id, &ctx);
      ASSERT_EQ(0, ctx.wait());

      std::shared_lock remote_image_locker{m_remote_image_ctx->image_lock};
      remote_size = m_remote_image_ctx->get_image_size(
        m_remote_image_ctx->snap_id);
    }

    uint64_t local_snap_id;
    {
      std::shared_lock image_locker{m_local_image_ctx->image_lock};
      local_snap_id = m_local_image_ctx->get_snap_id(
        cls::rbd::UserSnapshotNamespace{}, snap_name);
    }

    uint64_t local_size;
    {
      C_SaferCond ctx;
      m_local_image_ctx->state->snap_set(local_snap_id, &ctx);
      ASSERT_EQ(0, ctx.wait());

      std::shared_lock image_locker{m_local_image_ctx->image_lock};
      local_size = m_local_image_ctx->get_image_size(
        m_local_image_ctx->snap_id);
      bool flags_set;
      ASSERT_EQ(0, m_local_image_ctx->test_flags(m_local_image_ctx->snap_id,
                                                 RBD_FLAG_OBJECT_MAP_INVALID,
                                                 m_local_image_ctx->image_lock,
                                                 &flags_set));
      ASSERT_FALSE(flags_set);
    }

    ASSERT_EQ(remote_size, local_size);

    for (uint64_t offset = 0; offset < remote_size; offset += object_size) {
      ASSERT_LE(0, librbd::api::Io<>::read(
                     *m_remote_image_ctx, offset, object_size,
                     librbd::io::ReadResult{&read_remote_bl}, 0));
      ASSERT_LE(0, librbd::api::Io<>::read(
                     *m_local_image_ctx, offset, object_size,
                     librbd::io::ReadResult{&read_local_bl}, 0));
      ASSERT_TRUE(read_remote_bl.contents_equal(read_local_bl));
    }
  }
}

} // namespace mirror
} // namespace rbd
