// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "common/Cond.h"
#include "common/Throttle.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include <list>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/rolling_sum.hpp>

void register_test_object_map() {
}

class TestObjectMap : public TestFixture {
public:

  int when_open_object_map(librbd::ImageCtx *ictx) {
    C_SaferCond ctx;
    librbd::ObjectMap<> *object_map = new librbd::ObjectMap<>(*ictx, ictx->snap_id);
    object_map->open(&ctx);
    int r = ctx.wait();
    object_map->put();

    return r;
  }
};

TEST_F(TestObjectMap, RefreshInvalidatesWhenCorrupt) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_FALSE(flags_set);

  C_SaferCond lock_ctx;
  {
    std::unique_lock owner_locker{ictx->owner_lock};
    ictx->exclusive_lock->try_acquire_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  std::string oid = librbd::ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP);
  bufferlist bl;
  bl.append("corrupt");
  ASSERT_EQ(0, ictx->md_ctx.write_full(oid, bl));

  ASSERT_EQ(0, when_open_object_map(ictx));
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);
}

TEST_F(TestObjectMap, RefreshInvalidatesWhenTooSmall) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_FALSE(flags_set);

  C_SaferCond lock_ctx;
  {
    std::unique_lock owner_locker{ictx->owner_lock};
    ictx->exclusive_lock->try_acquire_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  librados::ObjectWriteOperation op;
  librbd::cls_client::object_map_resize(&op, 0, OBJECT_NONEXISTENT);

  std::string oid = librbd::ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP);
  ASSERT_EQ(0, ictx->md_ctx.operate(oid, &op));

  ASSERT_EQ(0, when_open_object_map(ictx));
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);
}

TEST_F(TestObjectMap, InvalidateFlagOnDisk) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_FALSE(flags_set);

  C_SaferCond lock_ctx;
  {
    std::unique_lock owner_locker{ictx->owner_lock};
    ictx->exclusive_lock->try_acquire_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  std::string oid = librbd::ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP);
  bufferlist bl;
  bl.append("corrupt");
  ASSERT_EQ(0, ictx->md_ctx.write_full(oid, bl));

  ASSERT_EQ(0, when_open_object_map(ictx));
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);
}

TEST_F(TestObjectMap, AcquireLockInvalidatesWhenTooSmall) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_FALSE(flags_set);

  librados::ObjectWriteOperation op;
  librbd::cls_client::object_map_resize(&op, 0, OBJECT_NONEXISTENT);

  std::string oid = librbd::ObjectMap<>::object_map_name(ictx->id, CEPH_NOSNAP);
  ASSERT_EQ(0, ictx->md_ctx.operate(oid, &op));

  C_SaferCond lock_ctx;
  {
    std::unique_lock owner_locker{ictx->owner_lock};
    ictx->exclusive_lock->try_acquire_lock(&lock_ctx);
  }
  ASSERT_EQ(0, lock_ctx.wait());

  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);

  // Test the flag is stored on disk
  ASSERT_EQ(0, ictx->state->refresh());
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_TRUE(flags_set);
}

namespace chrono = std::chrono;

TEST_F(TestObjectMap, DISABLED_StressTest) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  uint64_t object_count = cls::rbd::MAX_OBJECT_MAP_OBJECT_COUNT;
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, resize(ictx, ictx->layout.object_size * object_count));

  bool flags_set;
  ASSERT_EQ(0, ictx->test_flags(CEPH_NOSNAP, RBD_FLAG_OBJECT_MAP_INVALID,
                                &flags_set));
  ASSERT_FALSE(flags_set);

  srand(time(NULL) % (unsigned long) -1);

  coarse_mono_time start = coarse_mono_clock::now();
  chrono::duration<double> last = chrono::duration<double>::zero();

  const int WINDOW_SIZE = 5;
  typedef boost::accumulators::accumulator_set<
    double, boost::accumulators::stats<
      boost::accumulators::tag::rolling_sum> > RollingSum;

  RollingSum time_acc(
    boost::accumulators::tag::rolling_window::window_size = WINDOW_SIZE);
  RollingSum ios_acc(
    boost::accumulators::tag::rolling_window::window_size = WINDOW_SIZE);

  uint32_t io_threads = 16;
  uint64_t cur_ios = 0;
  SimpleThrottle throttle(io_threads, false);
  for (uint64_t ios = 0; ios < 100000;) {
    if (throttle.pending_error()) {
      break;
    }

    throttle.start_op();
    uint64_t object_no = (rand() % object_count);
    auto ctx = new LambdaContext([&throttle, object_no](int r) {
        ASSERT_EQ(0, r) << "object_no=" << object_no;
        throttle.end_op(r);
      });

    std::shared_lock owner_locker{ictx->owner_lock};
    std::shared_lock image_locker{ictx->image_lock};
    ASSERT_TRUE(ictx->object_map != nullptr);

    if (!ictx->object_map->aio_update<
          Context, &Context::complete>(CEPH_NOSNAP, object_no,
                                       OBJECT_EXISTS, {}, {}, true,
                                       false, ctx)) {
      ctx->complete(0);
    } else {
      ++cur_ios;
      ++ios;
    }

    coarse_mono_time now = coarse_mono_clock::now();
    chrono::duration<double> elapsed = now - start;
    if (last == chrono::duration<double>::zero()) {
      last = elapsed;
    } else if ((int)elapsed.count() != (int)last.count()) {
      time_acc((elapsed - last).count());
      ios_acc(static_cast<double>(cur_ios));
      cur_ios = 0;

      double time_sum = boost::accumulators::rolling_sum(time_acc);
      std::cerr << std::setw(5) << (int)elapsed.count() << "\t"
                << std::setw(8) << (int)ios << "\t"
                << std::fixed << std::setw(8) << std::setprecision(2)
                << boost::accumulators::rolling_sum(ios_acc) / time_sum
                << std::endl;
      last = elapsed;
    }
  }

  ASSERT_EQ(0, throttle.wait_for_ret());
}
