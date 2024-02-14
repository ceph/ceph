// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "include/interval_set.h"
#include "include/neorados/RADOS.hpp"
#include "include/rbd/librbd.hpp"
#include "include/rbd/object_map_types.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/api/Image.h"
#include "librbd/api/Io.h"
#include "librbd/deep_copy/ObjectCopyRequest.h"
#include "librbd/deep_copy/Utils.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Utils.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/test_support.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx,
       MockTestImageCtx* mock_parent_image_ctx = nullptr)
    : librbd::MockImageCtx(image_ctx) {
    parent = mock_parent_image_ctx;
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
namespace util {


namespace {
struct Mock {
    static Mock* s_instance;

    Mock() {
      s_instance = this;
    }

    MOCK_METHOD4(trigger_copyup,
            bool(MockImageCtx*, uint64_t, IOContext io_context,
                 Context*));
};

Mock *Mock::s_instance = nullptr;

} // anonymous namespace


template <>
void area_to_object_extents(MockTestImageCtx* image_ctx, uint64_t offset,
                            uint64_t length, ImageArea area,
                            uint64_t buffer_offset,
                            striper::LightweightObjectExtents* object_extents) {
  Striper::file_to_extents(image_ctx->cct, &image_ctx->layout, offset, length,
                           0, buffer_offset, object_extents);
}

template <>
std::pair<Extents, ImageArea> object_to_area_extents(
    MockTestImageCtx* image_ctx, uint64_t object_no,
    const Extents& object_extents) {
  Extents extents;
  for (auto [off, len] : object_extents) {
    Striper::extent_to_file(image_ctx->cct, &image_ctx->layout, object_no, off,
                            len, extents);
  }
  return {std::move(extents), ImageArea::DATA};
}

template <>
bool librbd::io::util::trigger_copyup(
        MockTestImageCtx *image_ctx, uint64_t object_no, IOContext io_context,
        Context* on_finish){
  return trigger_copyup(image_ctx->image_ctx, object_no, io_context, on_finish);
  //return Mock::s_instance->trigger_copyup(image_ctx, object_no, io_context, on_finish);
}


} // namespace util
} // namespace io

} // namespace librbd

// template definitions
#include "librbd/deep_copy/ObjectCopyRequest.cc"
template class librbd::deep_copy::ObjectCopyRequest<librbd::MockTestImageCtx>;

static bool operator==(const SnapContext& rhs, const SnapContext& lhs) {
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

    int r = api::Io<>::write(*image_ctx, off, len, std::move(bl), 0);
    ASSERT_EQ(static_cast<int>(len), r);

    interval_set<uint64_t> w;
    w.insert(off, len);
    what->union_of(w);
  }
  std::cout << " wrote " << *what << std::endl;
}

} // anonymous namespace


MATCHER(IsListSnaps, "") {
  auto req = boost::get<io::ImageDispatchSpec::ListSnaps>(&arg->request);
  return (req != nullptr);
}

MATCHER_P2(IsRead, snap_id, image_interval, "") {
  auto req = boost::get<io::ImageDispatchSpec::Read>(&arg->request);
  if (req == nullptr ||
      arg->io_context->get_read_snap() != snap_id) {
    return false;
  }

  // ensure the read request encloses the full snapshot delta
  interval_set<uint64_t> expected_interval(image_interval);
  interval_set<uint64_t> read_interval;
  for (auto &image_extent : arg->image_extents) {
    read_interval.insert(image_extent.first, image_extent.second);
  }

  interval_set<uint64_t> intersection;
  intersection.intersection_of(expected_interval, read_interval);
  expected_interval.subtract(intersection);
  return expected_interval.empty();
}

class TestMockDeepCopyObjectCopyRequest : public TestMockFixture {
public:
  typedef ObjectCopyRequest<librbd::MockTestImageCtx> MockObjectCopyRequest;
//  typedef librbd::io::util::Mock MockUtils;

  librbd::ImageCtx *m_src_image_ctx;
  librbd::ImageCtx *m_dst_image_ctx;
  librbd::ImageCtx *m_parent_image_ctx;

  std::shared_ptr<librbd::AsioEngine> m_asio_engine;
  asio::ContextWQ *m_work_queue;

  SnapMap m_snap_map;
  SnapSeqs m_snap_seqs;
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

    m_asio_engine = std::make_shared<librbd::AsioEngine>(
      m_src_image_ctx->md_ctx);
    m_work_queue = m_asio_engine->get_work_queue();
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
    EXPECT_CALL(mock_exclusive_lock, start_op(_)).WillOnce(Return(new LambdaContext([](int){})));
  }

  void expect_list_snaps(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, send(IsListSnaps()))
      .WillOnce(Invoke(
        [&mock_image_ctx, r](io::ImageDispatchSpec* spec) {
          if (r < 0) {
            spec->fail(r);
            return;
          }

          spec->image_dispatcher =
            mock_image_ctx.image_ctx->io_image_dispatcher;
          mock_image_ctx.image_ctx->io_image_dispatcher->send(spec);
        }));
  }

  void expect_get_object_name(librbd::MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, get_object_name(0))
                  .WillOnce(Return(mock_image_ctx.image_ctx->get_object_name(0)));
  }

  MockObjectCopyRequest *create_request(
      librbd::MockTestImageCtx &mock_src_image_ctx,
      librbd::MockTestImageCtx &mock_dst_image_ctx,
      librados::snap_t src_snap_id_start,
      librados::snap_t src_snap_id_end,
      librados::snap_t dst_snap_id_start,
      uint32_t flags, Context *on_finish) {
    SnapMap snap_map;
    util::compute_snap_map(mock_dst_image_ctx.cct, src_snap_id_start,
                           src_snap_id_end, m_dst_snap_ids, m_snap_seqs,
                           &snap_map);

    expect_get_object_name(mock_dst_image_ctx);
    return new MockObjectCopyRequest(&mock_src_image_ctx, &mock_dst_image_ctx,
                                     src_snap_id_start, dst_snap_id_start,
                                     snap_map, 0, flags, nullptr, on_finish);
  }

  void expect_read(librbd::MockTestImageCtx& mock_image_ctx,
                   uint64_t snap_id, uint64_t offset, uint64_t length, int r) {
    interval_set<uint64_t> extents;
    extents.insert(offset, length);
    expect_read(mock_image_ctx, snap_id, extents, r);
  }

  void expect_read(librbd::MockTestImageCtx& mock_image_ctx, uint64_t snap_id,
                   const interval_set<uint64_t> &extents, int r) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher,
                send(IsRead(snap_id, extents)))
      .WillOnce(Invoke(
        [&mock_image_ctx, r](io::ImageDispatchSpec* spec) {
          if (r < 0) {
            spec->fail(r);
            return;
          }

          spec->image_dispatcher =
            mock_image_ctx.image_ctx->io_image_dispatcher;
          mock_image_ctx.image_ctx->io_image_dispatcher->send(spec);
        }));
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
                                  ceph_assert(ceph_mutex_is_locked(mock_image_ctx.image_ctx->image_lock));
                                  mock_image_ctx.image_ctx->object_map->aio_update<Context>(
                                    snap_id, 0, 1, state, boost::none, {}, false, ctx);
                                })),
                              Return(true)));
      }
    }
  }

  void expect_prepare_copyup(MockTestImageCtx& mock_image_ctx, int r = 0) {
    EXPECT_CALL(*mock_image_ctx.io_object_dispatcher,
            prepare_copyup(_, _)).WillOnce(Return(r));
  }

  int create_snap(librbd::ImageCtx *image_ctx, const char* snap_name,
                  librados::snap_t *snap_id) {
    NoOpProgressContext prog_ctx;
    int r = image_ctx->operations->snap_create(
        cls::rbd::UserSnapshotNamespace(), snap_name, 0, prog_ctx);
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
    std::cout << "Snap-name : " << snap_name << " " << src_snap_id << "->" << dst_snap_id << std::endl;

    // collection of all existing snaps in dst image
    SnapIds dst_snap_ids({dst_snap_id});
    if (!m_snap_map.empty()) {
      dst_snap_ids.insert(dst_snap_ids.end(),
                            m_snap_map.rbegin()->second.begin(),
                            m_snap_map.rbegin()->second.end());
    }
    m_snap_map[src_snap_id] = dst_snap_ids;
    m_snap_seqs[src_snap_id] = dst_snap_id;
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

  int copy_objects() {
    int r;
    uint64_t object_size = 1 << m_src_image_ctx->order;

    bufferlist bl;
    bl.append(std::string(object_size, '1'));
    r = api::Io<>::read(*m_src_image_ctx, 0, object_size,
                        librbd::io::ReadResult{&bl}, 0);
    if (r < 0) {
      return r;
    }

    r = api::Io<>::write(*m_dst_image_ctx, 0, object_size, std::move(bl), 0);
    if (r < 0) {
      return r;
    }

    return 0;
  }

  int snap_protect(librbd::ImageCtx *ictx,
                   const std::string &snap_name) {
    return ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
                                       snap_name.c_str());
}

  void create_clone(const std::string &snap_name) {
    ASSERT_EQ(0, snap_protect(m_src_image_ctx, snap_name));


    int order = m_src_image_ctx->order;
    uint64_t features;
    ASSERT_EQ(0, librbd::get_features(m_src_image_ctx, &features));

    std::string clone_name = get_temp_image_name();
    std::string dst_name = get_temp_image_name();
    std::cout << "clone " << clone_name << std::endl;

    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_image_ctx->name.c_str(),
			       snap_name.c_str(), m_ioctx, clone_name.c_str(),
			       features, &order, 0, 0));

    ASSERT_EQ(0, librbd::clone(m_ioctx, m_src_image_ctx->name.c_str(),
			       snap_name.c_str(), m_ioctx, dst_name.c_str(),
			       features, &order, 0, 0));
    close_image(m_dst_image_ctx);
    ASSERT_EQ(0, open_image(dst_name, &m_dst_image_ctx));

    close_image(m_src_image_ctx);
    ASSERT_EQ(0, open_image(clone_name, &m_src_image_ctx));

    ASSERT_EQ(0, open_image(m_image_name, &m_parent_image_ctx));

    std::cout << "src_parent" << (m_src_image_ctx->parent) << std::endl;
    std::cout << "dst_parent" << (m_dst_image_ctx->parent) << std::endl;

    m_snap_map.clear();
    m_snap_seqs.clear();
    m_src_snap_ids.clear();
    m_dst_snap_ids.clear();
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
      r = api::Io<>::read(
        *m_src_image_ctx, 0, object_size, librbd::io::ReadResult{&src_bl}, 0);
      if (r < 0) {
        return r;
      }

      bufferlist dst_bl;
      dst_bl.append(std::string(object_size, '1'));
      r = api::Io<>::read(
        *m_dst_image_ctx, 0, object_size, librbd::io::ReadResult{&dst_bl}, 0);
      if (r < 0) {
        return r;
      }

      if (!src_bl.contents_equal(dst_bl)) {
        std::cout << "src block: " << std::endl; src_bl.hexdump(std::cout);
        std::cout << "dst block: " << std::endl; dst_bl.hexdump(std::cout);
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
  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, 0,
                                                  CEPH_NOSNAP, 0, 0, &ctx);

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, -ENOENT);

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

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, 0,
                                                  CEPH_NOSNAP, 0, 0, &ctx);

  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[0], 0, one.range_end(), 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], OBJECT_EXISTS, 0);
  expect_prepare_copyup(mock_dst_image_ctx);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
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

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, 0,
                                                  CEPH_NOSNAP, 0, 0, &ctx);

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[0], 0, one.range_end(),
              -EINVAL);

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

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, 0,
                                                  CEPH_NOSNAP, 0, 0, &ctx);

  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[0], 0, one.range_end(), 0);

  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], OBJECT_EXISTS, 0);

  expect_prepare_copyup(mock_dst_image_ctx);
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

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, 0,
                                                  CEPH_NOSNAP, 0, 0, &ctx);

  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[0], 0, one.range_end(), 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[2], two, 0);
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
  expect_prepare_copyup(mock_dst_image_ctx);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, two,
               {m_dst_snap_ids[0], {m_dst_snap_ids[0]}}, 0);

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
  ASSERT_LE(0, api::Io<>::discard(
    *m_src_image_ctx, trim_offset, one.range_end() - trim_offset,
    m_src_image_ctx->discard_granularity_bytes));
  ASSERT_EQ(0, create_snap("copy"));

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, 0,
                                                  CEPH_NOSNAP, 0, 0, &ctx);

  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[0], 0, one.range_end(), 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], OBJECT_EXISTS, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[1], OBJECT_EXISTS, 0);
  expect_prepare_copyup(mock_dst_image_ctx);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_start_op(mock_exclusive_lock);
  expect_truncate(mock_dst_io_ctx, trim_offset, 0);

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
  ASSERT_LE(0, api::Io<>::discard(
    *m_src_image_ctx, 0, object_size,
    m_src_image_ctx->discard_granularity_bytes));
  ASSERT_EQ(0, create_snap("copy"));
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, 0,
                                                  CEPH_NOSNAP, 0, 0, &ctx);

  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[1], 0, one.range_end(), 0);

  expect_start_op(mock_exclusive_lock);
  uint8_t state = OBJECT_EXISTS;
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], state, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[1], is_fast_diff(mock_dst_image_ctx) ?
                           OBJECT_EXISTS_CLEAN : OBJECT_EXISTS, 0);

  expect_prepare_copyup(mock_dst_image_ctx);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0);
  expect_start_op(mock_exclusive_lock);
  expect_remove(mock_dst_io_ctx, 0);

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

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, 0,
                                                  CEPH_NOSNAP, 0, 0, &ctx);

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[0], 0, one.range_end(), 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], OBJECT_EXISTS, -EBLOCKLISTED);

  request->send();
  ASSERT_EQ(-EBLOCKLISTED, ctx.wait());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, PrepareCopyupError) {
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

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx, 0,
                                                  CEPH_NOSNAP, 0, 0, &ctx);

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[0], 0, one.range_end(), 0);

  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
          m_dst_snap_ids[0], OBJECT_EXISTS, 0);

  expect_prepare_copyup(mock_dst_image_ctx, -EIO);

  request->send();
  ASSERT_EQ(-EIO, ctx.wait());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, WriteSnapsStart) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);
  ASSERT_EQ(0, copy_objects());
  ASSERT_EQ(0, create_snap("one"));

  auto src_snap_id_start = m_src_image_ctx->snaps[0];
  auto dst_snap_id_start = m_dst_image_ctx->snaps[0];

  interval_set<uint64_t> two;
  scribble(m_src_image_ctx, 10, 102400, &two);
  ASSERT_EQ(0, create_snap("two"));

  interval_set<uint64_t> three;
  scribble(m_src_image_ctx, 10, 102400, &three);
  ASSERT_EQ(0, create_snap("three"));

  auto max_extent = one.range_end();
  if (max_extent < two.range_end()) {
    interval_set<uint64_t> resize_diff;
    resize_diff.insert(max_extent, two.range_end() - max_extent);
    two.union_of(resize_diff);
  }

  max_extent = std::max(max_extent, two.range_end());
  if (max_extent < three.range_end()) {
    interval_set<uint64_t> resize_diff;
    resize_diff.insert(max_extent, three.range_end() - max_extent);
    three.union_of(resize_diff);
  }

  interval_set<uint64_t> four;
  scribble(m_src_image_ctx, 10, 102400, &four);

  // map should begin after src start and src end's dst snap seqs should
  // point to HEAD revision
  m_snap_seqs.rbegin()->second = CEPH_NOSNAP;
  m_dst_snap_ids.pop_back();

  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  C_SaferCond ctx;
  MockObjectCopyRequest *request = create_request(mock_src_image_ctx,
                                                  mock_dst_image_ctx,
                                                  src_snap_id_start,
                                                  CEPH_NOSNAP,
                                                  dst_snap_id_start,
                                                  0, &ctx);

  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request->get_dst_io_ctx()));

  InSequence seq;
  expect_list_snaps(mock_src_image_ctx, 0);

  expect_read(mock_src_image_ctx, m_src_snap_ids[1], two, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[2], three, 0);

  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[1], OBJECT_EXISTS, 0);

  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           CEPH_NOSNAP, OBJECT_EXISTS, 0);

  expect_prepare_copyup(mock_dst_image_ctx);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, two,
               {m_dst_snap_ids[0], {m_dst_snap_ids[0]}}, 0);

  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, three,
               {m_dst_snap_ids[1], {m_dst_snap_ids[1], m_dst_snap_ids[0]}}, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, Incremental) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 10, 102400, &one);
  ASSERT_EQ(0, create_snap("snap1"));
  mock_dst_image_ctx.snaps = m_dst_image_ctx->snaps;

  InSequence seq;

  C_SaferCond ctx1;
  auto request1 = create_request(mock_src_image_ctx, mock_dst_image_ctx,
                                 0, m_src_snap_ids[0], 0, 0, &ctx1);

  expect_list_snaps(mock_src_image_ctx, 0);

  expect_read(mock_src_image_ctx, m_src_snap_ids[0], 0, one.range_end(), 0);

  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], OBJECT_EXISTS, 0);

  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request1->get_dst_io_ctx()));
  expect_prepare_copyup(mock_dst_image_ctx);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0);

  request1->send();
  ASSERT_EQ(0, ctx1.wait());

  // clean (no-updates) snapshots
  ASSERT_EQ(0, create_snap("snap2"));
  ASSERT_EQ(0, create_snap("snap3"));
  mock_dst_image_ctx.snaps = m_dst_image_ctx->snaps;

  C_SaferCond ctx2;
  auto request2 = create_request(mock_src_image_ctx, mock_dst_image_ctx,
                                 m_src_snap_ids[0], m_src_snap_ids[2],
                                 m_dst_snap_ids[0], 0, &ctx2);

  expect_list_snaps(mock_src_image_ctx, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[1],
                           is_fast_diff(mock_dst_image_ctx) ?
                             OBJECT_EXISTS_CLEAN : OBJECT_EXISTS, 0);
  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[2],
                           is_fast_diff(mock_dst_image_ctx) ?
                             OBJECT_EXISTS_CLEAN : OBJECT_EXISTS, 0);

  request2->send();
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(0, compare_objects());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, SkipSnapList) {
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  ASSERT_EQ(0, create_snap("snap1"));
  mock_dst_image_ctx.snaps = m_dst_image_ctx->snaps;

  InSequence seq;

  // clean (no-updates) snapshots
  ASSERT_EQ(0, create_snap("snap2"));
  mock_dst_image_ctx.snaps = m_dst_image_ctx->snaps;

  C_SaferCond ctx;
  auto request = create_request(mock_src_image_ctx, mock_dst_image_ctx,
                                m_src_snap_ids[0], m_src_snap_ids[1],
                                m_dst_snap_ids[0],
                                OBJECT_COPY_REQUEST_FLAG_EXISTS_CLEAN, &ctx);

  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[1],
                           is_fast_diff(mock_dst_image_ctx) ?
                             OBJECT_EXISTS_CLEAN : OBJECT_EXISTS, 0);

  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopyObjectCopyRequest, CloneIncremental) {
  // scribble some data
  interval_set<uint64_t> one;
  scribble(m_src_image_ctx, 3, 102400, &one);
  ASSERT_EQ(0, snap_create(*m_src_image_ctx, "snap1"));

  create_clone("snap1");

  librbd::MockTestImageCtx mock_parent_image_ctx(*m_parent_image_ctx);
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx, &mock_parent_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx, &mock_parent_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  ASSERT_EQ(0, create_snap("snap2")); // No changes
  std::cout << m_src_image_ctx->name << std::endl;

  interval_set<uint64_t> two;
  scribble(m_src_image_ctx, 3, 102400, &two); // Should trigger src copyup
  ASSERT_EQ(0, create_snap("snap3"));

  std::cout << "m_src_snap_ids[0]:" << m_src_snap_ids[0] << std::endl;

  if (one.range_end() < two.range_end()) {
    interval_set<uint64_t> resize_diff;
    resize_diff.insert(one.range_end(), two.range_end() - one.range_end());
    two.union_of(resize_diff);
  }
  mock_dst_image_ctx.snaps = m_dst_image_ctx->snaps;
  InSequence seq;

 // MockUtils mock_utils;
  C_SaferCond ctx1;
  auto request1 = create_request(mock_src_image_ctx, mock_dst_image_ctx,
                                 0, m_src_snap_ids[0], 0, 0, &ctx1);

  expect_list_snaps(mock_src_image_ctx, 0);

  expect_read(mock_src_image_ctx, m_src_snap_ids[0], 0, one.range_end(), 0);

  expect_start_op(mock_exclusive_lock);
  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[0], OBJECT_EXISTS, 0);

  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx(get_mock_io_ctx(
    request1->get_dst_io_ctx()));
  expect_prepare_copyup(mock_dst_image_ctx);

  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx, 0, one.range_end(), {0, {}}, 0); // This is because the copyup should have updated the old snap
  request1->send();
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  auto request2 = create_request(mock_src_image_ctx, mock_dst_image_ctx,
                                 m_src_snap_ids[0], m_src_snap_ids[1],
                                 m_dst_snap_ids[0], 0, &ctx2);

  expect_list_snaps(mock_src_image_ctx, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[1], two, 0);
  expect_start_op(mock_exclusive_lock);

  expect_update_object_map(mock_dst_image_ctx, mock_object_map,
                           m_dst_snap_ids[1],
                           OBJECT_EXISTS, 0);

  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx2(get_mock_io_ctx(
    request2->get_dst_io_ctx()));
  expect_prepare_copyup(mock_dst_image_ctx);
  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx2, two,
               {m_dst_snap_ids[0], {m_dst_snap_ids[0]}}, 0);
//	  {m_dst_snap_ids[1], {m_dst_snap_ids[1], m_dst_snap_ids[0]}}, 0);
//{m_dst_snap_ids[1], {m_dst_snap_ids[1], m_dst_snap_ids[0]}}, 0); // This is because the copyup should have updated the old snap
  request2->send();
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(0, compare_objects());
}


TEST_F(TestMockDeepCopyObjectCopyRequest, CloneIncremental2) {

  int len = 3145728; //3MB
  bufferlist bl;
  bl.append(std::string(len, '1'));

  int r = api::Io<>::write(*m_src_image_ctx, 0, len, std::move(bl), 0);
  ASSERT_EQ(static_cast<int>(len), r);

  ASSERT_EQ(0, snap_create(*m_src_image_ctx, "snap"));

  create_clone("snap");
  librbd::MockTestImageCtx mock_parent_image_ctx(*m_parent_image_ctx);
  librbd::MockTestImageCtx mock_src_image_ctx(*m_src_image_ctx, &mock_parent_image_ctx);
  librbd::MockTestImageCtx mock_dst_image_ctx(*m_dst_image_ctx, &mock_parent_image_ctx);

  librbd::MockExclusiveLock mock_exclusive_lock;
  prepare_exclusive_lock(mock_dst_image_ctx, mock_exclusive_lock);

  librbd::MockExclusiveLock mock_parent_exclusive_lock;
  prepare_exclusive_lock(mock_parent_image_ctx, mock_parent_exclusive_lock);

  librbd::MockObjectMap mock_object_map;
  mock_dst_image_ctx.object_map = &mock_object_map;

  expect_op_work_queue(mock_src_image_ctx);
  expect_test_features(mock_dst_image_ctx);
  expect_get_object_count(mock_dst_image_ctx);

  ASSERT_EQ(0, create_snap("snap1")); // No changes
  std::cout << m_src_image_ctx->name << std::endl;
  mock_dst_image_ctx.snaps = m_dst_image_ctx->snaps;



  C_SaferCond ctx1;
  auto request1 = create_request(mock_src_image_ctx, mock_dst_image_ctx,
                                 0, m_src_snap_ids[0], 0, 0, &ctx1);


  expect_list_snaps(mock_src_image_ctx, 0);

  request1->send();
  ASSERT_EQ(-ENOENT, ctx1.wait());

  int len1 = 1048576; //1MB
  bufferlist bl1;
  bl1.append(std::string(len1, '2'));

  r = api::Io<>::write(*m_src_image_ctx, len1, len1, std::move(bl1), 0);
  ASSERT_EQ(static_cast<int>(len1), r);
  ASSERT_EQ(0, create_snap("snap2"));

  m_snap_seqs.rbegin()->second = CEPH_NOSNAP;
  mock_dst_image_ctx.snaps = m_dst_image_ctx->snaps;
  InSequence seq;

  C_SaferCond ctx2;
  auto request2 = create_request(mock_src_image_ctx, mock_dst_image_ctx,
                                 m_src_snap_ids[0], CEPH_NOSNAP,
                                 m_dst_snap_ids[0], 0, &ctx2);

  librados::MockTestMemIoCtxImpl &mock_dst_io_ctx2(get_mock_io_ctx(
    request2->get_dst_io_ctx()));
  expect_list_snaps(mock_src_image_ctx, 0);
  expect_read(mock_src_image_ctx, m_src_snap_ids[1], len1, len1, 0);

  expect_start_op(mock_exclusive_lock);
  expect_prepare_copyup(mock_dst_image_ctx);

  expect_start_op(mock_exclusive_lock);
  expect_write(mock_dst_io_ctx2, len1, len1,
               {m_dst_snap_ids[1], {m_dst_snap_ids[1], m_dst_snap_ids[0]}}, 0);

  request2->send();
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(0, compare_objects());

}

} // namespace deep_copy
} // namespace librbd
