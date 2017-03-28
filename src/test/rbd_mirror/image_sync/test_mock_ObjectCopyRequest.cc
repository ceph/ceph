// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/interval_set.h"
#include "include/rbd/librbd.hpp"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_sync/ObjectCopyRequest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

// template definitions
#include "tools/rbd_mirror/image_sync/ObjectCopyRequest.cc"
template class rbd::mirror::image_sync::ObjectCopyRequest<librbd::MockTestImageCtx>;

bool operator==(const SnapContext& rhs, const SnapContext& lhs) {
  return (rhs.seq == lhs.seq && rhs.snaps == lhs.snaps);
}

namespace rbd {
namespace mirror {
namespace image_sync {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

namespace {

void scribble(librbd::ImageCtx *image_ctx, int num_ops, size_t max_size,
              interval_set<uint64_t> *what)
{
  uint64_t object_size = 1 << image_ctx->order;
  for (int i=0; i<num_ops; i++) {
    uint64_t off = rand() % (object_size - max_size + 1);
    uint64_t len = 1 + rand() % max_size;

    bufferlist bl;
    bl.append(std::string(len, '1'));

    int r = image_ctx->aio_work_queue->write(off, len, bl.c_str(), 0);
    ASSERT_EQ(static_cast<int>(len), r);

    interval_set<uint64_t> w;
    w.insert(off, len);
    what->union_of(w);
  }
  std::cout << " wrote " << *what << std::endl;
}

} // anonymous namespace

class TestMockImageSyncObjectCopyRequest : public TestMockFixture {
public:
  typedef ObjectCopyRequest<librbd::MockTestImageCtx> MockObjectCopyRequest;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_list_snaps(librbd::MockTestImageCtx &mock_image_ctx,
                         librados::MockTestMemIoCtxImpl &mock_io_ctx,
                         const librados::snap_set_t &snap_set) {
    expect_set_snap_read(mock_io_ctx, CEPH_SNAPDIR);
    EXPECT_CALL(mock_io_ctx,
                list_snaps(mock_image_ctx.image_ctx->get_object_name(0), _))
      .WillOnce(DoAll(WithArg<1>(Invoke([&snap_set](librados::snap_set_t *out_snap_set) {
                          *out_snap_set = snap_set;
                        })),
                      Return(0)));
  }

  void expect_list_snaps(librbd::MockTestImageCtx &mock_image_ctx,
                         librados::MockTestMemIoCtxImpl &mock_io_ctx, int r) {
    expect_set_snap_read(mock_io_ctx, CEPH_SNAPDIR);
    auto &expect = EXPECT_CALL(mock_io_ctx,
                               list_snaps(mock_image_ctx.image_ctx->get_object_name(0),
                                          _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_get_object_name(librbd::MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, get_object_name(0))
                  .WillOnce(Return(mock_image_ctx.image_ctx->get_object_name(0)));
  }

  MockObjectCopyRequest *create_request(librbd::MockTestImageCtx &mock_remote_image_ctx,
                                        librbd::MockTestImageCtx &mock_local_image_ctx,
                                        Context *on_finish) {
    expect_get_object_name(mock_local_image_ctx);
    expect_get_object_name(mock_remote_image_ctx);
    return new MockObjectCopyRequest(&mock_local_image_ctx,
                                     &mock_remote_image_ctx, &m_snap_map,
                                     0, on_finish);
  }

  void expect_set_snap_read(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                            uint64_t snap_id) {
    EXPECT_CALL(mock_io_ctx, set_snap_read(snap_id));
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
                    uint64_t offset, uint64_t length,
                    const SnapContext &snapc, int r) {
    auto &expect = EXPECT_CALL(mock_io_ctx, write(_, _, length, offset, snapc));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_write(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                    const interval_set<uint64_t> &extents,
                    const SnapContext &snapc, int r) {
    for (auto extent : extents) {
      expect_write(mock_io_ctx, extent.first, extent.second, snapc, r);
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

  void expect_update_object_map(librbd::MockTestImageCtx &mock_image_ctx,
                                librbd::MockObjectMap &mock_object_map,
                                librados::snap_t snap_id, uint8_t state,
                                int r) {
    if (mock_image_ctx.image_ctx->object_map != nullptr) {
      auto &expect = EXPECT_CALL(mock_object_map, aio_update(snap_id, 0, 1, state, _, _));
      if (r < 0) {
        expect.WillOnce(DoAll(WithArg<5>(Invoke([this, r](Context *ctx) {
                                  m_threads->work_queue->queue(ctx, r);
                                })),
                              Return(true)));
      } else {
        expect.WillOnce(DoAll(WithArg<5>(Invoke([&mock_image_ctx, snap_id, state, r](Context *ctx) {
                                  assert(mock_image_ctx.image_ctx->snap_lock.is_locked());
                                  assert(mock_image_ctx.image_ctx->object_map_lock.is_wlocked());
                                  mock_image_ctx.image_ctx->object_map->aio_update<Context>(
                                    snap_id, 0, 1, state, boost::none, ctx);
                                })),
                              Return(true)));
      }
    }
  }

  using TestFixture::create_snap;
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
    m_remote_snap_ids.push_back(remote_snap_id);
    m_local_snap_ids.push_back(local_snap_id);
    return 0;
  }

  std::string get_snap_name(librbd::ImageCtx *image_ctx,
                            librados::snap_t snap_id) {
    auto it = std::find_if(image_ctx->snap_ids.begin(),
                           image_ctx->snap_ids.end(),
                           [snap_id](const std::pair<std::string, librados::snap_t> &pair) {
        return (pair.second == snap_id);
      });
    if (it == image_ctx->snap_ids.end()) {
      return "";
    }
    return it->first;
  }

  int compare_objects() {
    MockObjectCopyRequest::SnapMap snap_map(m_snap_map);
    if (snap_map.empty()) {
      return -ENOENT;
    }

    int r;
    uint64_t object_size = 1 << m_remote_image_ctx->order;
    while (!snap_map.empty()) {
      librados::snap_t remote_snap_id = snap_map.begin()->first;
      librados::snap_t local_snap_id = *snap_map.begin()->second.begin();
      snap_map.erase(snap_map.begin());

      std::string snap_name = get_snap_name(m_remote_image_ctx, remote_snap_id);
      if (snap_name.empty()) {
        return -ENOENT;
      }

      std::cout << "comparing '" << snap_name << " (" << remote_snap_id
                << " to " << local_snap_id << ")" << std::endl;

      r = librbd::snap_set(m_remote_image_ctx, snap_name.c_str());
      if (r < 0) {
        return r;
      }

      r = librbd::snap_set(m_local_image_ctx, snap_name.c_str());
      if (r < 0) {
        return r;
      }

      bufferlist remote_bl;
      remote_bl.append(std::string(object_size, '1'));
      r = m_remote_image_ctx->aio_work_queue->read(0, object_size,
                                                   remote_bl.c_str(), 0);
      if (r < 0) {
        return r;
      }

      bufferlist local_bl;
      local_bl.append(std::string(object_size, '1'));
      r = m_local_image_ctx->aio_work_queue->read(0, object_size,
                                                  local_bl.c_str(), 0);
      if (r < 0) {
        return r;
      }

      if (!remote_bl.contents_equal(local_bl)) {
        return -EBADMSG;
      }
    }

    r = librbd::snap_set(m_remote_image_ctx, nullptr);
    if (r < 0) {
      return r;
    }
    r = librbd::snap_set(m_local_image_ctx, nullptr);
    if (r < 0) {
      return r;
    }

    return 0;
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx;

  MockObjectCopyRequest::SnapMap m_snap_map;
  std::vector<librados::snap_t> m_remote_snap_ids;
  std::vector<librados::snap_t> m_local_snap_ids;
};

TEST_F(TestMockImageSyncObjectCopyRequest, DNE) {
  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockObjectMap mock_object_map;
  mock_local_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_local_image_ctx);

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
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockObjectMap mock_object_map;
  mock_local_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[0]);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_update_object_map(mock_local_image_ctx, mock_object_map,
                           m_local_snap_ids[0], OBJECT_EXISTS, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockImageSyncObjectCopyRequest, ReadMissingStaleSnapSet) {
  ASSERT_EQ(0, create_snap("one"));
  ASSERT_EQ(0, create_snap("two"));

  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx, 10, 102400, &one);
  ASSERT_EQ(0, create_snap("three"));

  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockObjectMap mock_object_map;
  mock_local_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  librados::clone_info_t dummy_clone_info;
  dummy_clone_info.cloneid = librados::SNAP_HEAD;
  dummy_clone_info.size = 123;

  librados::snap_set_t dummy_snap_set1;
  dummy_snap_set1.clones.push_back(dummy_clone_info);

  dummy_clone_info.size = 234;
  librados::snap_set_t dummy_snap_set2;
  dummy_snap_set2.clones.push_back(dummy_clone_info);

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, dummy_snap_set1);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[3]);
  expect_read(mock_remote_io_ctx, 0, 123, -ENOENT);
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, dummy_snap_set2);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[3]);
  expect_read(mock_remote_io_ctx, 0, 234, -ENOENT);
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[3]);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(),
               {m_local_snap_ids[1], {m_local_snap_ids[1],
                                      m_local_snap_ids[0]}},
                0);
  expect_update_object_map(mock_local_image_ctx, mock_object_map,
                           m_local_snap_ids[2], OBJECT_EXISTS, 0);
  expect_update_object_map(mock_local_image_ctx, mock_object_map,
                           m_local_snap_ids[3], OBJECT_EXISTS_CLEAN, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockImageSyncObjectCopyRequest, ReadMissingUpToDateSnapMap) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockObjectMap mock_object_map;
  mock_local_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[0]);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), -ENOENT);
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);

  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockImageSyncObjectCopyRequest, ReadError) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockObjectMap mock_object_map;
  mock_local_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[0]);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), -EINVAL);

  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageSyncObjectCopyRequest, WriteError) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockObjectMap mock_object_map;
  mock_local_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[0]);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(), {0, {}}, -EINVAL);

  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageSyncObjectCopyRequest, WriteSnaps) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx, 10, 102400, &one);
  ASSERT_EQ(0, create_snap("one"));

  interval_set<uint64_t> two;
  scribble(m_remote_image_ctx, 10, 102400, &two);
  ASSERT_EQ(0, create_snap("two"));

  if (one.range_end() < two.range_end()) {
    interval_set<uint64_t> resize_diff;
    resize_diff.insert(one.range_end(), two.range_end() - one.range_end());
    two.union_of(resize_diff);
  }

  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockObjectMap mock_object_map;
  mock_local_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[0]);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[2]);
  expect_read(mock_remote_io_ctx, two, 0);
  expect_write(mock_local_io_ctx, two,
               {m_local_snap_ids[0], {m_local_snap_ids[0]}}, 0);
  expect_update_object_map(mock_local_image_ctx, mock_object_map,
                           m_local_snap_ids[0], OBJECT_EXISTS, 0);
  expect_update_object_map(mock_local_image_ctx, mock_object_map,
                           m_local_snap_ids[1], OBJECT_EXISTS, 0);
  expect_update_object_map(mock_local_image_ctx, mock_object_map,
                           m_local_snap_ids[2], OBJECT_EXISTS_CLEAN, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockImageSyncObjectCopyRequest, Trim) {
  ASSERT_EQ(0, metadata_set(m_remote_image_ctx,
			    "conf_rbd_skip_partial_discard", "false"));
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx, 10, 102400, &one);
  ASSERT_EQ(0, create_snap("one"));

  // trim the object
  uint64_t trim_offset = rand() % one.range_end();
  ASSERT_LE(0, m_remote_image_ctx->aio_work_queue->discard(
    trim_offset, one.range_end() - trim_offset));
  ASSERT_EQ(0, create_snap("sync"));

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockObjectMap mock_object_map;
  mock_local_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[0]);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_truncate(mock_local_io_ctx, trim_offset, 0);
  expect_update_object_map(mock_local_image_ctx, mock_object_map,
                           m_local_snap_ids[0], OBJECT_EXISTS, 0);
  expect_update_object_map(mock_local_image_ctx, mock_object_map,
                           m_local_snap_ids[1], OBJECT_EXISTS, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockImageSyncObjectCopyRequest, Remove) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_remote_image_ctx, 10, 102400, &one);
  ASSERT_EQ(0, create_snap("one"));
  ASSERT_EQ(0, create_snap("two"));

  // remove the object
  uint64_t object_size = 1 << m_remote_image_ctx->order;
  ASSERT_LE(0, m_remote_image_ctx->aio_work_queue->discard(0, object_size));
  ASSERT_EQ(0, create_snap("sync"));
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockObjectMap mock_object_map;
  mock_local_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_local_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_remote_image_ctx,
                                                  mock_local_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_remote_io_ctx(get_mock_io_ctx(
    request->get_remote_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_local_io_ctx(get_mock_io_ctx(
    request->get_local_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_remote_image_ctx, mock_remote_io_ctx, 0);
  expect_set_snap_read(mock_remote_io_ctx, m_remote_snap_ids[1]);
  expect_read(mock_remote_io_ctx, 0, one.range_end(), 0);
  expect_write(mock_local_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_remove(mock_local_io_ctx, 0);
  expect_update_object_map(mock_local_image_ctx, mock_object_map,
                           m_local_snap_ids[0], OBJECT_EXISTS, 0);
  expect_update_object_map(mock_local_image_ctx, mock_object_map,
                           m_local_snap_ids[1], OBJECT_EXISTS_CLEAN, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd
