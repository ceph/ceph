// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/interval_set.h"
#include "include/rbd/librbd.hpp"
#include "include/rbd/object_map_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/api/Image.h"
#include "librbd/deep_copy/ObjectCopyRequest.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/io/ReadResult.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/test_support.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }

  MockTestImageCtx *parent = nullptr;
};

} // anonymous namespace

namespace util {

inline ImageCtx* get_image_ctx(MockTestImageCtx* image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util

namespace io {

template <>
struct ImageRequest<MockTestImageCtx> {
  static ImageRequest *s_instance;

  static void aio_read(MockTestImageCtx *ictx, AioCompletion *c,
                       Extents &&image_extents, ReadResult &&read_result,
                       int op_flags, const ZTracer::Trace &parent_trace) {
    ceph_assert(s_instance != nullptr);
    s_instance->aio_read(c, image_extents);
  }
  MOCK_METHOD2(aio_read, void(AioCompletion *, const Extents&));
};

ImageRequest<MockTestImageCtx> *ImageRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace io

} // namespace librbd

// template definitions
#include "librbd/deep_copy/ObjectCopyRequest.cc"
template class librbd::deep_copy::ObjectCopyRequest<librbd::MockTestImageCtx>;

bool operator==(const SnapContext& rhs, const SnapContext& lhs) {
  return (rhs.seq == lhs.seq && rhs.snaps == lhs.snaps);
}

namespace librbd {
namespace deep_copy {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnNew;
using ::testing::WithArg;

namespace {

void scribble(librbd::ImageCtx *image_ctx, int num_ops, size_t max_size,
              interval_set<uint64_t> *what)
{
  uint64_t object_size = 1 << image_ctx->order;
  for (int i = 0; i < num_ops; i++) {
    uint64_t off = rand() % (object_size - max_size + 1);
    uint64_t len = 1 + rand() % max_size;
    std::cout << __func__ << ": off=" << off << ", len=" << len << std::endl;

    bufferlist bl;
    bl.append(std::string(len, '1'));

    int r = image_ctx->io_work_queue->write(off, len, std::move(bl), 0);
    ASSERT_EQ(static_cast<int>(len), r);

    interval_set<uint64_t> w;
    w.insert(off, len);
    what->union_of(w);
  }
  std::cout << " wrote " << *what << std::endl;
}

} // anonymous namespace

class TestMockDeepCopyObjectCopyRequest : public TestMockFixture {
public:
  typedef ObjectCopyRequest<librbd::MockTestImageCtx> MockObjectCopyRequest;

  librbd::ImageCtx *m_src_image_ctx;
  librbd::ImageCtx *m_dst_image_ctx;
  ThreadPool *m_thread_pool;
  ContextWQ *m_work_queue;

  SnapMap m_snap_map;
  std::vector<librados::snap_t> m_src_snap_ids;
  std::vector<librados::snap_t> m_dst_snap_ids;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_src_image_ctx));

    librbd::NoOpProgressContext no_op;
    m_image_size = 1 << m_src_image_ctx->order;
    ASSERT_EQ(0, m_src_image_ctx->operations->resize(m_image_size, true, no_op));

    librbd::RBD rbd;
    std::string dst_image_name = get_temp_image_name();
    ASSERT_EQ(0, create_image_pp(rbd, m_ioctx, dst_image_name, m_image_size));
    ASSERT_EQ(0, open_image(dst_image_name, &m_dst_image_ctx));

    librbd::ImageCtx::get_thread_pool_instance(m_src_image_ctx->cct,
                                               &m_thread_pool, &m_work_queue);
  }

  bool is_fast_diff(librbd::MockImageCtx &mock_image_ctx) {
    return (mock_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0;
  }

  void prepare_exclusive_lock(librbd::MockImageCtx &mock_image_ctx,
                              librbd::MockExclusiveLock &mock_exclusive_lock) {
    if ((mock_image_ctx.features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
      return;
    }
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  void expect_get_object_count(librbd::MockImageCtx& mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, get_object_count(_))
      .WillRepeatedly(Invoke([&mock_image_ctx](librados::snap_t snap_id) {
          return mock_image_ctx.image_ctx->get_object_count(snap_id);
        }));
  }

  void expect_test_features(librbd::MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, test_features(_))
      .WillRepeatedly(WithArg<0>(Invoke([&mock_image_ctx](uint64_t features) {
              return (mock_image_ctx.features & features) != 0;
            })));
  }

  void expect_start_op(librbd::MockExclusiveLock &mock_exclusive_lock) {
    if ((m_src_image_ctx->features & RBD_FEATURE_EXCLUSIVE_LOCK) == 0) {
      return;
    }
    EXPECT_CALL(mock_exclusive_lock, start_op(_)).WillOnce(
      ReturnNew<FunctionContext>([](int) {}));
  }

  void expect_list_snaps(librbd::MockTestImageCtx &mock_image_ctx,
                         librados::MockTestMemIoCtxImpl &mock_io_ctx,
                         const librados::snap_set_t &snap_set) {
    expect_get_object_name(mock_image_ctx);
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
    expect_get_object_name(mock_image_ctx);
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

  MockObjectCopyRequest *create_request(
      librbd::MockTestImageCtx &mock_src_image_ctx,
      librbd::MockTestImageCtx &mock_dst_image_ctx, Context *on_finish) {
    expect_get_object_name(mock_dst_image_ctx);
    expect_get_object_count(mock_dst_image_ctx);
    return new MockObjectCopyRequest(&mock_src_image_ctx, &mock_dst_image_ctx,
                                     m_snap_map, 0, false, on_finish);
  }

  void expect_set_snap_read(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                            uint64_t snap_id) {
    EXPECT_CALL(mock_io_ctx, set_snap_read(snap_id));
  }

  void expect_sparse_read(librados::MockTestMemIoCtxImpl &mock_io_ctx, uint64_t offset,
                          uint64_t length, int r) {

    auto &expect = EXPECT_CALL(mock_io_ctx, sparse_read(_, offset, length, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_sparse_read(librados::MockTestMemIoCtxImpl &mock_io_ctx,
                   const interval_set<uint64_t> &extents, int r) {
    for (auto extent : extents) {
      expect_sparse_read(mock_io_ctx, extent.first, extent.second, r);
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
      auto &expect = EXPECT_CALL(mock_object_map, aio_update(snap_id, 0, 1, state, _, _, false, _));
      if (r < 0) {
        expect.WillOnce(DoAll(WithArg<7>(Invoke([this, r](Context *ctx) {
                                  m_work_queue->queue(ctx, r);
                                })),
                              Return(true)));
      } else {
        expect.WillOnce(DoAll(WithArg<7>(Invoke([&mock_image_ctx, snap_id, state](Context *ctx) {
                                  ceph_assert(mock_image_ctx.image_ctx->snap_lock.is_locked());
                                  ceph_assert(mock_image_ctx.image_ctx->object_map_lock.is_wlocked());
                                  mock_image_ctx.image_ctx->object_map->aio_update<Context>(
                                    snap_id, 0, 1, state, boost::none, {}, false, ctx);
                                })),
                              Return(true)));
      }
    }
  }

  int create_snap(librbd::ImageCtx *image_ctx, const char* snap_name,
                  librados::snap_t *snap_id) {
    int r = image_ctx->operations->snap_create(
        cls::rbd::UserSnapshotNamespace(), snap_name);
    if (r < 0) {
      return r;
    }

    r = image_ctx->state->refresh();
    if (r < 0) {
      return r;
    }

    if (image_ctx->snap_ids.count({cls::rbd::UserSnapshotNamespace(),
                                   snap_name}) == 0) {
      return -ENOENT;
    }

    if (snap_id != nullptr) {
      *snap_id = image_ctx->snap_ids[{cls::rbd::UserSnapshotNamespace(),
                                      snap_name}];
    }
    return 0;
  }

  int create_snap(const char* snap_name) {
    librados::snap_t src_snap_id;
    int r = create_snap(m_src_image_ctx, snap_name, &src_snap_id);
    if (r < 0) {
      return r;
    }

    librados::snap_t dst_snap_id;
    r = create_snap(m_dst_image_ctx, snap_name, &dst_snap_id);
    if (r < 0) {
      return r;
    }

    // collection of all existing snaps in dst image
    SnapIds dst_snap_ids({dst_snap_id});
    if (!m_snap_map.empty()) {
      dst_snap_ids.insert(dst_snap_ids.end(),
                            m_snap_map.rbegin()->second.begin(),
                            m_snap_map.rbegin()->second.end());
    }
    m_snap_map[src_snap_id] = dst_snap_ids;
    m_src_snap_ids.push_back(src_snap_id);
    m_dst_snap_ids.push_back(dst_snap_id);

    return 0;
  }

  std::string get_snap_name(librbd::ImageCtx *image_ctx,
                            librados::snap_t snap_id) {
    auto it = std::find_if(image_ctx->snap_ids.begin(),
                           image_ctx->snap_ids.end(),
                           [snap_id](const std::pair<std::pair<cls::rbd::SnapshotNamespace,
							       std::string>,
						     librados::snap_t> &pair) {
        return (pair.second == snap_id);
      });
    if (it == image_ctx->snap_ids.end()) {
      return "";
    }
    return it->first.second;
  }

  int compare_objects() {
    SnapMap snap_map(m_snap_map);
    if (snap_map.empty()) {
      return -ENOENT;
    }

    int r;
    uint64_t object_size = 1 << m_src_image_ctx->order;
    while (!snap_map.empty()) {
      librados::snap_t src_snap_id = snap_map.begin()->first;
      librados::snap_t dst_snap_id = *snap_map.begin()->second.begin();
      snap_map.erase(snap_map.begin());

      std::string snap_name = get_snap_name(m_src_image_ctx, src_snap_id);
      if (snap_name.empty()) {
        return -ENOENT;
      }

      std::cout << "comparing '" << snap_name << " (" << src_snap_id
                << " to " << dst_snap_id << ")" << std::endl;

      r = librbd::api::Image<>::snap_set(m_src_image_ctx,
			                 cls::rbd::UserSnapshotNamespace(),
			                 snap_name.c_str());
      if (r < 0) {
        return r;
      }

      r = librbd::api::Image<>::snap_set(m_dst_image_ctx,
			                 cls::rbd::UserSnapshotNamespace(),
			                 snap_name.c_str());
      if (r < 0) {
        return r;
      }

      bufferlist src_bl;
      src_bl.append(std::string(object_size, '1'));
      r = m_src_image_ctx->io_work_queue->read(
        0, object_size, librbd::io::ReadResult{&src_bl}, 0);
      if (r < 0) {
        return r;
      }

      bufferlist dst_bl;
      dst_bl.append(std::string(object_size, '1'));
      r = m_dst_image_ctx->io_work_queue->read(
        0, object_size, librbd::io::ReadResult{&dst_bl}, 0);
      if (r < 0) {
        return r;
      }

      if (!src_bl.contents_equal(dst_bl)) {
        return -EBADMSG;
      }
    }

    r = librbd::api::Image<>::snap_set(m_src_image_ctx,
			               cls::rbd::UserSnapshotNamespace(),
			               nullptr);
    if (r < 0) {
      return r;
    }
    r = librbd::api::Image<>::snap_set(m_dst_image_ctx,
			               cls::rbd::UserSnapshotNamespace(),
			               nullptr);
    if (r < 0) {
      return r;
    }

    return 0;
  }
};

TEST_F(TestMockDeepCopyObjectCopyRequest, DNE) {
  ASSERT_EQ(0, create_snap("copy"));
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;
  expect_test_features(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_src_io_ctx(get_mock_io_ctx(
    request->get_src_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, -ENOENT);

  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, Write) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("copy"));
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_src_io_ctx(get_mock_io_ctx(
    request->get_src_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, 0);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[0]);
  expect_sparse_read(mock_src_io_ctx, 0, one.range_end(), 0);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], OBJECT_EXISTS, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, ReadMissingStaleSnapSet) {
  ASSERT_EQ(0, create_snap("one"));
  ASSERT_EQ(0, create_snap("two"));

  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);
  ASSERT_EQ(0, create_snap("three"));

  ASSERT_EQ(0, create_snap("copy"));
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_src_io_ctx(get_mock_io_ctx(
    request->get_src_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  librados::clone_info_t dummy_clone_info;
  dummy_clone_info.cloneid = librados::SNAP_HEAD;
  dummy_clone_info.size = 123;

  librados::snap_set_t dummy_snap_set1;
  dummy_snap_set1.clones.push_back(dummy_clone_info);

  dummy_clone_info.size = 234;
  librados::snap_set_t dummy_snap_set2;
  dummy_snap_set2.clones.push_back(dummy_clone_info);

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, dummy_snap_set1);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[3]);
  expect_sparse_read(mock_src_io_ctx, 0, 123, -ENOENT);
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, dummy_snap_set2);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[3]);
  expect_sparse_read(mock_src_io_ctx, 0, 234, -ENOENT);
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, 0);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[3]);
  expect_sparse_read(mock_src_io_ctx, 0, one.range_end(), 0);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(),
               {m_dst_snap_ids[1], {m_dst_snap_ids[1],
                                    m_dst_snap_ids[0]}},
               0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[2], OBJECT_EXISTS, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[3], is_fast_diff(mock_dst_image_ctx) ?
                           OBJECT_EXISTS_CLEAN : OBJECT_EXISTS, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, ReadMissingUpToDateSnapMap) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("copy"));
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_src_io_ctx(get_mock_io_ctx(
    request->get_src_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, 0);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[0]);
  expect_sparse_read(mock_src_io_ctx, 0, one.range_end(), -ENOENT);
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, 0);

  request->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, ReadError) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("copy"));
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_src_io_ctx(get_mock_io_ctx(
    request->get_src_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, 0);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[0]);
  expect_sparse_read(mock_src_io_ctx, 0, one.range_end(),  -EINVAL);

  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, WriteError) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("copy"));
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_src_io_ctx(get_mock_io_ctx(
    request->get_src_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, 0);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[0]);
  expect_sparse_read(mock_src_io_ctx, 0, one.range_end(), 0);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, -EINVAL);

  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, WriteSnaps) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);
  ASSERT_EQ(0, create_snap("one"));

  interval_set<uint64_t> two;
  scribble(m_src_image_ctx, 10, 102400, &two);
  ASSERT_EQ(0, create_snap("two"));

  if (one.range_end() < two.range_end()) {
    interval_set<uint64_t> resize_diff;
    resize_diff.insert(one.range_end(), two.range_end() - one.range_end());
    two.union_of(resize_diff);
  }

  ASSERT_EQ(0, create_snap("copy"));
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_src_io_ctx(get_mock_io_ctx(
    request->get_src_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, 0);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[0]);
  expect_sparse_read(mock_src_io_ctx, 0, one.range_end(), 0);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[2]);
  expect_sparse_read(mock_src_io_ctx, two, 0);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, two,
               {m_dst_snap_ids[0], {m_dst_snap_ids[0]}}, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], OBJECT_EXISTS, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[1], OBJECT_EXISTS, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[2], is_fast_diff(mock_dst_image_ctx) ?
                           OBJECT_EXISTS_CLEAN : OBJECT_EXISTS, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, Trim) {
  ASSERT_EQ(0, m_src_image_ctx->operations->metadata_set(
              "conf_rbd_skip_partial_discard", "false"));
  m_src_image_ctx->discard_granularity_bytes = 0;

  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);
  ASSERT_EQ(0, create_snap("one"));

  // trim the object
  uint64_t trim_offset = rand() % one.range_end();
  ASSERT_LE(0, m_src_image_ctx->io_work_queue->discard(
    trim_offset, one.range_end() - trim_offset,
    m_src_image_ctx->discard_granularity_bytes));
  ASSERT_EQ(0, create_snap("copy"));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_src_io_ctx(get_mock_io_ctx(
    request->get_src_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, 0);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[0]);
  expect_sparse_read(mock_src_io_ctx, 0, one.range_end(), 0);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_start_op(mock_exclusive_lock);
  expect_truncate(mock_dst_io_ctx, trim_offset, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], OBJECT_EXISTS, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[1], OBJECT_EXISTS, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, Remove) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);
  ASSERT_EQ(0, create_snap("one"));
  ASSERT_EQ(0, create_snap("two"));

  // remove the object
  uint64_t object_size = 1 << m_src_image_ctx->order;
  ASSERT_LE(0, m_src_image_ctx->io_work_queue->discard(
    0, object_size, m_src_image_ctx->discard_granularity_bytes));
  ASSERT_EQ(0, create_snap("copy"));
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_src_io_ctx(get_mock_io_ctx(
    request->get_src_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, 0);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[1]);
  expect_sparse_read(mock_src_io_ctx, 0, one.range_end(), 0);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_start_op(mock_exclusive_lock);
  expect_remove(mock_dst_io_ctx, 0);
  expect_start_op(mock_exclusive_lock);
  uint8_t state = OBJECT_EXISTS;
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], state, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[1], is_fast_diff(mock_dst_image_ctx) ?
                           OBJECT_EXISTS_CLEAN : OBJECT_EXISTS, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, ObjectMapUpdateError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);

  ASSERT_EQ(0, create_snap("copy"));
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_test_features(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, &ctx);

  librados::MockTestMemIoCtxImpl &mock_src_io_ctx(get_mock_io_ctx(
    request->get_src_io_ctx()));
  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, mock_src_io_ctx, 0);
  expect_set_snap_read(mock_src_io_ctx, m_src_snap_ids[0]);
  expect_sparse_read(mock_src_io_ctx, 0, one.range_end(), 0);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], OBJECT_EXISTS, -EBLACKLISTED);

  request->send();
  ASSERT_EQ(-EBLACKLISTED, ctx.wait());
}

} // namespace deep_copy
} // namespace librbd
