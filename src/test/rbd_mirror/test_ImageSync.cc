// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_fixture.h"
#include "include/rbd/librbd.hpp"
#include "journal/Journaler.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "tools/rbd_mirror/ImageSync.h"
#include "tools/rbd_mirror/Threads.h"

void register_test_image_sync() {
}

namespace rbd {
namespace mirror {

namespace {

void scribble(librbd::ImageCtx *image_ctx, int num_ops, size_t max_size)
{
  for (int i=0; i<num_ops; i++) {
    uint64_t off = rand() % (image_ctx->size - max_size + 1);
    uint64_t len = 1 + rand() % max_size;

    if (rand() % 4 == 0) {
      ASSERT_EQ((int)len, image_ctx->aio_work_queue->discard(off, len));
    } else {
      std::string str(len, '1');
      ASSERT_EQ((int)len, image_ctx->aio_work_queue->write(off, len,
                                                           str.c_str(), 0));
    }
  }
}

} // anonymous namespace
class TestImageSync : public TestFixture {
public:

  virtual void SetUp() {
    TestFixture::SetUp();
    create_and_open(m_local_io_ctx, &m_local_image_ctx);
    create_and_open(m_remote_io_ctx, &m_remote_image_ctx);

    m_remote_journaler = new ::journal::Journaler(
      m_threads->work_queue, m_threads->timer, &m_threads->timer_lock,
      m_remote_io_ctx, m_remote_image_ctx->id, "mirror-uuid", 5);

    m_client_meta = {"image-id"};

    librbd::journal::ClientData client_data(m_client_meta);
    bufferlist client_data_bl;
    ::encode(client_data, client_data_bl);

    ASSERT_EQ(0, m_remote_journaler->register_client(client_data_bl));
  }

  void create_and_open(librados::IoCtx &io_ctx, librbd::ImageCtx **image_ctx) {
    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(io_ctx, m_image_name, image_ctx));

    C_SaferCond ctx;
    {
      RWLock::RLocker owner_locker((*image_ctx)->owner_lock);
      (*image_ctx)->exclusive_lock->try_lock(&ctx);
    }
    ASSERT_EQ(0, ctx.wait());
    ASSERT_TRUE((*image_ctx)->exclusive_lock->is_lock_owner());
  }

  ImageSync<> *create_request(Context *ctx) {
    return new ImageSync<>(m_local_image_ctx, m_remote_image_ctx,
                           m_threads->timer, &m_threads->timer_lock,
                           "mirror-uuid", m_remote_journaler, &m_client_meta,
                           ctx);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx;
  ::journal::Journaler *m_remote_journaler;
  librbd::journal::MirrorPeerClientMeta m_client_meta;
};

TEST_F(TestImageSync, Empty) {
  C_SaferCond ctx;
  ImageSync<> *request = create_request(&ctx);
  request->start();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(0U, m_client_meta.sync_points.size());
  ASSERT_EQ(0, m_remote_image_ctx->state->refresh());
  ASSERT_EQ(0U, m_remote_image_ctx->snap_ids.size());
  ASSERT_EQ(0, m_local_image_ctx->state->refresh());
  ASSERT_EQ(1U, m_local_image_ctx->snap_ids.size()); // deleted on journal replay
}

TEST_F(TestImageSync, Simple) {
  scribble(m_remote_image_ctx, 10, 102400);
  {
    RWLock::RLocker owner_locker(m_remote_image_ctx->owner_lock);
    ASSERT_EQ(0, m_remote_image_ctx->flush());
  }

  C_SaferCond ctx;
  ImageSync<> *request = create_request(&ctx);
  request->start();
  ASSERT_EQ(0, ctx.wait());

  int64_t object_size = std::min<int64_t>(
    m_remote_image_ctx->size, 1 << m_remote_image_ctx->order);
  bufferlist read_remote_bl;
  read_remote_bl.append(std::string(object_size, '1'));
  bufferlist read_local_bl;
  read_local_bl.append(std::string(object_size, '1'));

  for (uint64_t offset = 0; offset < m_remote_image_ctx->size;
       offset += object_size) {
    ASSERT_LE(0, m_remote_image_ctx->aio_work_queue->read(
                   offset, object_size, read_remote_bl.c_str(), 0));
    ASSERT_LE(0, m_local_image_ctx->aio_work_queue->read(
                   offset, object_size, read_local_bl.c_str(), 0));
    ASSERT_TRUE(read_remote_bl.contents_equal(read_local_bl));
  }
}

} // namespace mirror
} // namespace rbd
