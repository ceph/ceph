// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/interval_set.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/image_sync/ObjectCopyRequest.h"

// template definitions
#include "tools/rbd_mirror/image_sync/ObjectCopyRequest.cc"
template class rbd::mirror::image_sync::ObjectCopyRequest<librbd::MockImageCtx>;

namespace rbd {
namespace mirror {
namespace image_sync {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Return;

namespace {

void scribble(librados::IoCtx &io_ctx, const std::string &oid, int num_ops,
              size_t max_size, interval_set<uint64_t> *what)
{
  uint64_t object_size = 1 << 22;
  librados::ObjectWriteOperation op;
  for (int i=0; i<num_ops; i++) {
    uint64_t off = rand() % (object_size - max_size + 1);
    uint64_t len = 1 + rand() % max_size;

    bufferlist bl;
    bl.append(std::string(len, '1'));

    op.write(off, bl);

    interval_set<uint64_t> w;
    w.insert(off, len);
    what->union_of(w);
  }

  ASSERT_EQ(0, io_ctx.operate(oid, &op));
  std::cout << " wrote " << *what << std::endl;
}

} // anonymous namespace

class TestMockImageSyncObjectCopyRequest : public TestMockFixture {
public:
  typedef ObjectCopyRequest<librbd::MockImageCtx> MockObjectCopyRequest;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_list_snaps(librbd::MockImageCtx &mock_image_ctx,
                         librados::MockTestMemIoCtxImpl &mock_io_ctx, int r) {
    auto &expect = EXPECT_CALL(mock_io_ctx,
                               list_snaps(mock_image_ctx.image_ctx->get_object_name(0),
                                          _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_get_object_name(librbd::MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, get_object_name(0))
                  .WillOnce(Return(mock_image_ctx.image_ctx->get_object_name(0)));
  }

  MockObjectCopyRequest *create_request(librbd::MockImageCtx &mock_remote_image_ctx,
                                        librbd::MockImageCtx &mock_local_image_ctx,
                                        Context *on_finish) {
    expect_get_object_name(mock_local_image_ctx);
    expect_get_object_name(mock_remote_image_ctx);
    return new MockObjectCopyRequest(&mock_local_image_ctx,
                                     &mock_remote_image_ctx, &m_snap_map,
                                     0, on_finish);
  }

  void expect_read(librados::MockTestMemIoCtxImpl &mock_io_ctx, uint64_t offset,
                   uint64_t length, int r) {
    auto &expect = EXPECT_CALL(mock_io_ctx, read(_, length, offset, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_read(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                   const interval_set<uint64_t> &extents, int r) {
    for (auto extent : extents) {
      expect_read(mock_io_ctx, extent.first, extent.second, r);
      if (r < 0) {
        break;
      }
    }
  }

  void expect_write(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                    uint64_t offset, uint64_t length, int r) {
    auto &expect = EXPECT_CALL(mock_io_ctx, write(_, _, length, offset, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_write(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                    const interval_set<uint64_t> &extents, int r) {
    for (auto extent : extents) {
      expect_write(mock_io_ctx, extent.first, extent.second, r);
      if (r < 0) {
        break;
      }
    }
  }

  void expect_truncate(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                       uint64_t offset, int r) {
    auto &expect = EXPECT_CALL(mock_io_ctx, truncate(_, offset, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_remove(librados::MockTestMemIoCtxImpl &mock_io_ctx, int r) {
    auto &expect = EXPECT_CALL(mock_io_ctx, remove(_, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  int create_snap(librbd::ImageCtx *image_ctx, const char* snap_name,
                  librados::snap_t *snap_id) {
    int r = image_ctx->operations->snap_create(snap_name);
    if (r < 0) {
      return r;
    }

    r = image_ctx->state->refresh();
    if (r < 0) {
      return r;
    }

    if (image_ctx->snap_ids.count(snap_name) == 0) {
      return -ENOENT;
    }
    *snap_id = image_ctx->snap_ids[snap_name];
    return 0;
  }

  int create_snap(const char* snap_name) {
    librados::snap_t remote_snap_id;
    int r = create_snap(m_remote_image_ctx, snap_name, &remote_snap_id);
    if (r < 0) {
      return r;
    }

    librados::snap_t local_snap_id;
    r = create_snap(m_local_image_ctx, snap_name, &local_snap_id);
    if (r < 0) {
      return r;
    }

    // collection of all existing snaps in local image
    MockObjectCopyRequest::SnapIds local_snap_ids({local_snap_id});
    if (!m_snap_map.empty()) {
      local_snap_ids.insert(local_snap_ids.end(),
                            m_snap_map.rbegin()->second.begin(),
                            m_snap_map.rbegin()->second.end());
    }
    m_snap_map[remote_snap_id] = local_snap_ids;
    return 0;
  }

  int compare_objects() {
    MockObjectCopyRequest::SnapMap snap_map(m_snap_map);
    if (snap_map.empty()) {
      return -ENOENT;
    }

    std::string remote_oid(m_remote_image_ctx->get_object_name(0));
    std::string local_oid(m_local_image_ctx->get_object_name(0));

    librados::IoCtx remote_io_ctx;
    remote_io_ctx.dup(m_remote_image_ctx->data_ctx);

    librados::IoCtx local_io_ctx;
    local_io_ctx.dup(m_local_image_ctx->data_ctx);

    while (!snap_map.empty()) {
      librados::snap_t remote_snap_id = snap_map.begin()->first;
      librados::snap_t local_snap_id = *snap_map.begin()->second.begin();
      snap_map.erase(snap_map.begin());
      std::cout << "comparing " << remote_snap_id << " to " << local_snap_id
                << std::endl;

      remote_io_ctx.snap_set_read(remote_snap_id);
      bufferlist remote_bl;
      int remote_r = remote_io_ctx.read(remote_oid, remote_bl, 1<<22, 0);

      local_io_ctx.snap_set_read(local_snap_id);
      bufferlist local_bl;
      int local_r = local_io_ctx.read(local_oid, local_bl, 1<<22, 0);

      if (remote_r != local_r) {
        return remote_r < 0 ? remote_r : local_r;
      }

      if (!remote_bl.contents_equal(local_bl)) {
        return -EBADMSG;
      }
    }
    return 0;
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx;

  MockObjectCopyRequest::SnapMap m_snap_map;

};

TEST_F(TestMockImageSyncObjectCopyRequest, DNE) {
  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, -ENOENT);

  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncObjectCopyRequest, Write) {
  // scribble some data
  std::string remote_oid(m_remote_image_ctx->get_object_name(0));
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx->data_ctx, remote_oid, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(), 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockImageSyncObjectCopyRequest, ReadError) {
  // scribble some data
  std::string remote_oid(m_remote_image_ctx->get_object_name(0));
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx->data_ctx, remote_oid, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), -EINVAL);

  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageSyncObjectCopyRequest, WriteError) {
  // scribble some data
  std::string remote_oid(m_remote_image_ctx->get_object_name(0));
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx->data_ctx, remote_oid, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(), -EINVAL);

  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageSyncObjectCopyRequest, WriteSnaps) {
  // scribble some data
  std::string remote_oid(m_remote_image_ctx->get_object_name(0));
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx->data_ctx, remote_oid, 10, 1024, &one);
  ASSERT_EQ(0, create_snap("one"));

  interval_set<uint64_t> two;
  scribble(m_remote_image_ctx->data_ctx, remote_oid, 10, 1024, &two);
  ASSERT_EQ(0, create_snap("two"));

  if (one.range_end() < two.range_end()) {
    interval_set<uint64_t> resize_diff;
    resize_diff.insert(one.range_end(), two.range_end() - one.range_end());
    two.union_of(resize_diff);
  }

  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(), 0);
  expect_read(mock_remote_io_ctx, two, 0);
  expect_write(mock_local_io_ctx, two, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockImageSyncObjectCopyRequest, Trim) {
  // scribble some data
  std::string remote_oid(m_remote_image_ctx->get_object_name(0));
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx->data_ctx, remote_oid, 10, 1024, &one);
  ASSERT_EQ(0, create_snap("one"));

  // trim the object
  uint64_t trim_offset = rand() % one.range_end();
  ASSERT_EQ(0, m_remote_image_ctx->data_ctx.trunc(remote_oid, trim_offset));
  ASSERT_EQ(0, create_snap("sync"));

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(), 0);
  expect_truncate(mock_local_io_ctx, trim_offset, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockImageSyncObjectCopyRequest, Remove) {
  // scribble some data
  std::string remote_oid(m_remote_image_ctx->get_object_name(0));
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx->data_ctx, remote_oid, 10, 1024, &one);
  ASSERT_EQ(0, create_snap("one"));

  // remove the object
  ASSERT_EQ(0, m_remote_image_ctx->data_ctx.remove(remote_oid));
  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(), 0);
  expect_remove(mock_local_io_ctx, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd
