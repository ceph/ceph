#include <chrono>
#include <list>
#include <vector>

//#include "common/config.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "osdc/Objecter.h"

#include "gtest/gtest.h"

TEST(Librados, CreateShutdown) {
  rados_t cluster;
  int err;
  err = rados_create(&cluster, "someid");
  EXPECT_EQ(err, 0);

  rados_shutdown(cluster);
}

TEST(Librados, HitSetListAndVectorDecode) {
  using real_interval = std::pair<ceph::real_time, ceph::real_time>;
  using time_interval = std::pair<time_t, time_t>;

  const auto one_nanosecond = std::chrono::nanoseconds {1};
  const std::vector<real_interval> encoded {
    {ceph::real_clock::from_time_t(10) + one_nanosecond,
     ceph::real_clock::from_time_t(20)},
    {ceph::real_clock::from_time_t(30), {}}
  };
  const std::list<real_interval> legacy(std::begin(encoded), std::end(encoded));
  ceph::buffer::list encoded_vector;
  ceph::buffer::list encoded_list;
  ceph::encode(encoded, encoded_vector);
  ceph::encode(legacy, encoded_list);

  ASSERT_TRUE(encoded_vector.contents_equal(encoded_list));

  const std::vector<time_interval> expected {{11, 20}, {30, 0}};
  std::vector<time_interval> decoded {{99, 99}};
  int result = -1;
  ObjectOperation operation;
  operation.hit_set_ls(&decoded, &result);
  ceph::encode(encoded, *operation.out_bl.front());

  ceph::buffer::list ignored;
  auto handler = std::move(operation.out_handler.front());
  std::move(handler)(boost::system::error_code {}, 0, ignored);

  EXPECT_EQ(0, result);
  EXPECT_EQ(expected, decoded);

  const std::list<time_interval> expected_list(std::begin(expected), std::end(expected));
  std::list<time_interval> legacy_decoded {{99, 99}};
  result = -1;
  ObjectOperation legacy_operation;
  legacy_operation.hit_set_ls(&legacy_decoded, &result);
  ceph::encode(encoded, *legacy_operation.out_bl.front());

  auto legacy_handler = std::move(legacy_operation.out_handler.front());
  std::move(legacy_handler)(boost::system::error_code {}, 0, ignored);

  EXPECT_EQ(0, result);
  EXPECT_EQ(expected_list, legacy_decoded);
}

TEST(Librados, HitSetVectorDecodeFailurePreservesOutput) {
  using time_interval = std::pair<time_t, time_t>;

  const std::vector<time_interval> expected {{99, 99}};
  auto decoded = expected;
  int result = -1;
  ObjectOperation operation;
  operation.hit_set_ls(&decoded, &result);
  operation.out_bl.front()->append("x", 1);

  ceph::buffer::list ignored;
  auto handler = std::move(operation.out_handler.front());
  std::move(handler)(boost::system::error_code {}, 0, ignored);

  EXPECT_EQ(-EIO, result);
  EXPECT_EQ(expected, decoded);
}
