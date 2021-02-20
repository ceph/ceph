// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include "cls/2pc_queue/cls_2pc_queue_types.h"
#include "cls/2pc_queue/cls_2pc_queue_client.h"
#include "cls/queue/cls_queue_client.h"
#include "cls/2pc_queue/cls_2pc_queue_types.h"

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <chrono>
#include <atomic>

class TestCls2PCQueue : public ::testing::Test {
protected:
  librados::Rados rados;
  std::string pool_name;
  librados::IoCtx ioctx;

  void SetUp() override {
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }

  void TearDown() override {
    ioctx.close();
    ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
  }
};

TEST_F(TestCls2PCQueue, GetCapacity)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 8*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  uint64_t size;

  const int ret = cls_queue_get_capacity(ioctx, queue_name, size);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(max_size, size);
}

TEST_F(TestCls2PCQueue, AsyncGetCapacity)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 8*1024;
  librados::ObjectWriteOperation wop;
  wop.create(true);
  cls_2pc_queue_init(wop, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &wop));

  librados::ObjectReadOperation rop;
  bufferlist bl;
  int rc;
  cls_2pc_queue_get_capacity(rop, &bl, &rc);
  ASSERT_EQ(0, ioctx.operate(queue_name, &rop, nullptr));
  ASSERT_EQ(0, rc);
  uint64_t size;
  ASSERT_EQ(cls_2pc_queue_get_capacity_result(bl, size), 0);
  ASSERT_EQ(max_size, size);
}

TEST_F(TestCls2PCQueue, Reserve)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024U*1024U;
  const auto number_of_ops = 10U;
  const auto number_of_elements = 23U;
  const auto size_to_reserve = 250U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  for (auto i = 0U; i < number_of_ops; ++i) {
    cls_2pc_reservation::id_t res_id;
    ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
    ASSERT_EQ(res_id, i+1);
  }
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), number_of_ops);
  for (const auto& r : reservations) {
      ASSERT_NE(r.first, cls_2pc_reservation::NO_ID);
      ASSERT_GT(r.second.timestamp.time_since_epoch().count(), 0);
  }
}

TEST_F(TestCls2PCQueue, AsyncReserve)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024U*1024U;
  constexpr auto number_of_ops = 10U;
  constexpr auto number_of_elements = 23U;
  const auto size_to_reserve = 250U;
  librados::ObjectWriteOperation wop;
  wop.create(true);
  cls_2pc_queue_init(wop, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &wop));

  for (auto i = 0U; i < number_of_ops; ++i) {
    bufferlist res_bl;
    int res_rc;
    cls_2pc_queue_reserve(wop, size_to_reserve, number_of_elements, &res_bl, &res_rc);
    ASSERT_EQ(0, ioctx.operate(queue_name, &wop, librados::OPERATION_RETURNVEC));
    ASSERT_EQ(res_rc, 0);
    cls_2pc_reservation::id_t res_id;
    ASSERT_EQ(0, cls_2pc_queue_reserve_result(res_bl, res_id));
    ASSERT_EQ(res_id, i+1);
  }

  bufferlist bl;
  int rc;
  librados::ObjectReadOperation rop;
  cls_2pc_queue_list_reservations(rop, &bl, &rc);
  ASSERT_EQ(0, ioctx.operate(queue_name, &rop, nullptr));
  ASSERT_EQ(0, rc);
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations_result(bl, reservations));
  ASSERT_EQ(reservations.size(), number_of_ops);
  for (const auto& r : reservations) {
      ASSERT_NE(r.first, cls_2pc_reservation::NO_ID);
      ASSERT_GT(r.second.timestamp.time_since_epoch().count(), 0);
  }
}

TEST_F(TestCls2PCQueue, Commit)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024*128;
  const auto number_of_ops = 200U;
  const auto number_of_elements = 23U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  for (auto i = 0U; i < number_of_ops; ++i) {
    const std::string element_prefix("op-" +to_string(i) + "-element-");
    auto total_size = 0UL;
    std::vector<bufferlist> data(number_of_elements);
    // create vector of buffer lists
    std::generate(data.begin(), data.end(), [j = 0, &element_prefix, &total_size] () mutable {
          bufferlist bl;
          bl.append(element_prefix + to_string(j++));
          total_size += bl.length();
          return bl;
        });

    cls_2pc_reservation::id_t res_id;
    ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, total_size, number_of_elements, res_id), 0);
    ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
    cls_2pc_queue_commit(op, data, res_id);
    ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  }
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
}

TEST_F(TestCls2PCQueue, Abort)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024U*1024U;
  const auto number_of_ops = 17U;
  const auto number_of_elements = 23U;
  const auto size_to_reserve = 250U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  for (auto i = 0U; i < number_of_ops; ++i) {
    cls_2pc_reservation::id_t res_id;
    ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
    ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
    cls_2pc_queue_abort(op, res_id);
    ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  }
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
}

TEST_F(TestCls2PCQueue, ReserveError)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 256U*1024U;
  const auto number_of_ops = 254U;
  const auto number_of_elements = 1U;
  const auto size_to_reserve = 1024U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  cls_2pc_reservation::id_t res_id;
  for (auto i = 0U; i < number_of_ops-1; ++i) {
    ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
    ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
  }
  res_id = cls_2pc_reservation::NO_ID;
  // this one is failing because it exceeds the queue size
  ASSERT_NE(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
  ASSERT_EQ(res_id, cls_2pc_reservation::NO_ID);

  // this one is failing because it tries to reserve 0 entries
  ASSERT_NE(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, 0, res_id), 0);
  // this one is failing because it tries to reserve 0 bytes
  ASSERT_NE(cls_2pc_queue_reserve(ioctx, queue_name, 0, number_of_elements, res_id), 0);

  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), number_of_ops-1);
  for (const auto& r : reservations) {
      ASSERT_NE(r.first, cls_2pc_reservation::NO_ID);
      ASSERT_GT(r.second.timestamp.time_since_epoch().count(), 0);
  }
}

TEST_F(TestCls2PCQueue, CommitError)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024;
  const auto number_of_ops = 17U;
  const auto number_of_elements = 23U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  const auto invalid_reservation_op = 8;
  const auto invalid_elements_op = 11;
  std::vector<bufferlist> invalid_data(number_of_elements+3);
  // create vector of buffer lists
  std::generate(invalid_data.begin(), invalid_data.end(), [j = 0] () mutable {
      bufferlist bl;
      bl.append("invalid data is larger that regular data" + to_string(j++));
      return bl;
    });
  for (auto i = 0U; i < number_of_ops; ++i) {
    const std::string element_prefix("op-" +to_string(i) + "-element-");
    std::vector<bufferlist> data(number_of_elements);
    auto total_size = 0UL;
    // create vector of buffer lists
    std::generate(data.begin(), data.end(), [j = 0, &element_prefix, &total_size] () mutable {
          bufferlist bl;
          bl.append(element_prefix + to_string(j++));
          total_size += bl.length();
          return bl;
        });

    cls_2pc_reservation::id_t res_id;
    ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, total_size, number_of_elements, res_id), 0);
    ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
    if (i == invalid_reservation_op) {
      // fail on a commits with invalid reservation id
      cls_2pc_queue_commit(op, data, res_id+999);
      ASSERT_NE(0, ioctx.operate(queue_name, &op));
    } else if (i == invalid_elements_op) {
      // fail on a commits when data size is larger than the reserved one
      cls_2pc_queue_commit(op, invalid_data, res_id);
      ASSERT_NE(0, ioctx.operate(queue_name, &op));
    } else {
      cls_2pc_queue_commit(op, data, res_id);
      ASSERT_EQ(0, ioctx.operate(queue_name, &op));
    }
  }
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  // 2 reservations were not comitted
  ASSERT_EQ(reservations.size(), 2);
}

TEST_F(TestCls2PCQueue, AbortError)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024;
  const auto number_of_ops = 17U;
  const auto number_of_elements = 23U;
  const auto size_to_reserve = 250U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  const auto invalid_reservation_op = 8;

  for (auto i = 0U; i < number_of_ops; ++i) {
    cls_2pc_reservation::id_t res_id;
    ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
    ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
    if (i == invalid_reservation_op) {
      // aborting a reservation which does not exists
      // is a no-op, not an error
      cls_2pc_queue_abort(op, res_id+999);
    } else {
      cls_2pc_queue_abort(op, res_id);
    }
    ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  }
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  // 1 reservation was not aborted
  ASSERT_EQ(reservations.size(), 1);
}

TEST_F(TestCls2PCQueue, MultiReserve)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024;
  const auto number_of_ops = 11U;
  const auto number_of_elements = 23U;
  const auto max_producer_count = 10U;
  const auto size_to_reserve = 250U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  std::vector<std::thread> producers(max_producer_count);
  for (auto& p : producers) {
    p = std::thread([this, &queue_name] {
      librados::ObjectWriteOperation op;
  	  for (auto i = 0U; i < number_of_ops; ++i) {
        cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
        ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
        ASSERT_NE(res_id, 0);
      }
    });
  }

  std::for_each(producers.begin(), producers.end(), [](auto& p) { p.join(); });

  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), number_of_ops*max_producer_count);
  auto total_reservations = 0U;
  for (const auto& r : reservations) {
    total_reservations += r.second.size;
  }
  ASSERT_EQ(total_reservations, number_of_ops*max_producer_count*size_to_reserve);
}

TEST_F(TestCls2PCQueue, MultiCommit)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024;
  const auto number_of_ops = 11U;
  const auto number_of_elements = 23U;
  const auto max_producer_count = 10U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  std::vector<std::thread> producers(max_producer_count);
  for (auto& p : producers) {
    p = std::thread([this, &queue_name] {
      librados::ObjectWriteOperation op;
  	  for (auto i = 0U; i < number_of_ops; ++i) {
        const std::string element_prefix("op-" +to_string(i) + "-element-");
        std::vector<bufferlist> data(number_of_elements);
        auto total_size = 0UL;
        // create vector of buffer lists
        std::generate(data.begin(), data.end(), [j = 0, &element_prefix, &total_size] () mutable {
            bufferlist bl;
            bl.append(element_prefix + to_string(j++));
            total_size += bl.length();
            return bl;
          });
        cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
        ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, total_size, number_of_elements, res_id), 0);
        ASSERT_NE(res_id, 0);
        cls_2pc_queue_commit(op, data, res_id);
        ASSERT_EQ(0, ioctx.operate(queue_name, &op));
      }
    });
  }

  std::for_each(producers.begin(), producers.end(), [](auto& p) { p.join(); });

  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
}

TEST_F(TestCls2PCQueue, MultiAbort)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024;
  const auto number_of_ops = 11U;
  const auto number_of_elements = 23U;
  const auto max_producer_count = 10U;
  const auto size_to_reserve = 250U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  std::vector<std::thread> producers(max_producer_count);
  for (auto& p : producers) {
    p = std::thread([this, &queue_name] {
      librados::ObjectWriteOperation op;
  	  for (auto i = 0U; i < number_of_ops; ++i) {
        cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
        ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
        ASSERT_NE(res_id, 0);
        cls_2pc_queue_abort(op, res_id);
        ASSERT_EQ(0, ioctx.operate(queue_name, &op));
      }
    });
  }

  std::for_each(producers.begin(), producers.end(), [](auto& p) { p.join(); });

  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
}

TEST_F(TestCls2PCQueue, ReserveCommit)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024;
  const auto number_of_ops = 11U;
  const auto number_of_elements = 23U;
  const auto max_workers = 10U;
  const auto size_to_reserve = 512U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  std::vector<std::thread> reservers(max_workers);
  for (auto& r : reservers) {
    r = std::thread([this, &queue_name] {
      librados::ObjectWriteOperation op;
  	  for (auto i = 0U; i < number_of_ops; ++i) {
        cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
        ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
        ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
      }
    });
  }

  auto committer = std::thread([this, &queue_name] {
    librados::ObjectWriteOperation op;
    int remaining_ops = number_of_ops*max_workers;
    while (remaining_ops > 0) {
      const std::string element_prefix("op-" +to_string(remaining_ops) + "-element-");
      std::vector<bufferlist> data(number_of_elements);
      // create vector of buffer lists
      std::generate(data.begin(), data.end(), [j = 0, &element_prefix] () mutable {
          bufferlist bl;
          bl.append(element_prefix + to_string(j++));
          return bl;
        });
      cls_2pc_reservations reservations;
      ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
      for (const auto& r : reservations) {
        cls_2pc_queue_commit(op, data, r.first);
        ASSERT_EQ(0, ioctx.operate(queue_name, &op));
        --remaining_ops;
      }
    }
  });

  std::for_each(reservers.begin(), reservers.end(), [](auto& r) { r.join(); });
  committer.join();

  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
}

TEST_F(TestCls2PCQueue, ReserveAbort)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024;
  const auto number_of_ops = 17U;
  const auto number_of_elements = 23U;
  const auto max_workers = 10U;
  const auto size_to_reserve = 250U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  std::vector<std::thread> reservers(max_workers);
  for (auto& r : reservers) {
    r = std::thread([this, &queue_name] {
      librados::ObjectWriteOperation op;
  	  for (auto i = 0U; i < number_of_ops; ++i) {
        cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
        ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
        ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
      }
    });
  }
  
  auto aborter = std::thread([this, &queue_name] {
    librados::ObjectWriteOperation op;
    int remaining_ops = number_of_ops*max_workers;
    while (remaining_ops > 0) {
      cls_2pc_reservations reservations;
      ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
      for (const auto& r : reservations) {
        cls_2pc_queue_abort(op, r.first);
        ASSERT_EQ(0, ioctx.operate(queue_name, &op));
        --remaining_ops;
      }
    }
  });

  std::for_each(reservers.begin(), reservers.end(), [](auto& r) { r.join(); });
  aborter.join();

  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
}

TEST_F(TestCls2PCQueue, ManualCleanup)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 128*1024*1024;
  const auto number_of_ops = 17U;
  const auto number_of_elements = 23U;
  const auto max_workers = 10U;
  const auto size_to_reserve = 512U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  // anything older than 100ms is cosidered stale
  ceph::coarse_real_time stale_time = ceph::coarse_real_clock::now() + std::chrono::milliseconds(100);

  std::vector<std::thread> reservers(max_workers);
  for (auto& r : reservers) {
    r = std::thread([this, &queue_name] {
      librados::ObjectWriteOperation op;
  	  for (auto i = 0U; i < number_of_ops; ++i) {
        cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
        ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
        ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
        // wait for 10ms between each reservation to make sure at least some are stale
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    });
  }

  auto cleaned_reservations = 0U;
  auto committed_reservations = 0U;
  auto aborter = std::thread([this, &queue_name, &stale_time, &cleaned_reservations, &committed_reservations] {
    librados::ObjectWriteOperation op;
    int remaining_ops = number_of_ops*max_workers;
    while (remaining_ops > 0) {
      cls_2pc_reservations reservations;
      ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
      for (const auto& r : reservations) {
        if (r.second.timestamp > stale_time) {
          // abort stale reservations
          cls_2pc_queue_abort(op, r.first);
          ASSERT_EQ(0, ioctx.operate(queue_name, &op));
          ++cleaned_reservations;
        } else {
          // commit good reservations
          const std::string element_prefix("op-" +to_string(remaining_ops) + "-element-");
          std::vector<bufferlist> data(number_of_elements);
          // create vector of buffer lists
          std::generate(data.begin(), data.end(), [j = 0, &element_prefix] () mutable {
              bufferlist bl;
              bl.append(element_prefix + to_string(j++));
              return bl;
            });
          cls_2pc_queue_commit(op, data, r.first);
          ASSERT_EQ(0, ioctx.operate(queue_name, &op));
          ++committed_reservations;
        }
        --remaining_ops;
      }
    }
  });


  std::for_each(reservers.begin(), reservers.end(), [](auto& r) { r.join(); });
  aborter.join();

  ASSERT_GT(cleaned_reservations, 0);
  ASSERT_EQ(committed_reservations + cleaned_reservations, number_of_ops*max_workers);
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
}

TEST_F(TestCls2PCQueue, Cleanup)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 128*1024*1024;
  const auto number_of_ops = 15U;
  const auto number_of_elements = 23U;
  const auto max_workers = 10U;
  const auto size_to_reserve = 512U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  // anything older than 100ms is cosidered stale
  ceph::coarse_real_time stale_time = ceph::coarse_real_clock::now() + std::chrono::milliseconds(100);

  std::vector<std::thread> reservers(max_workers);
  for (auto& r : reservers) {
    r = std::thread([this, &queue_name] {
      librados::ObjectWriteOperation op;
  	  for (auto i = 0U; i < number_of_ops; ++i) {
        cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
        ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
        ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
        // wait for 10ms between each reservation to make sure at least some are stale
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    });
  }

  std::for_each(reservers.begin(), reservers.end(), [](auto& r) { r.join(); });

  cls_2pc_reservations all_reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, all_reservations));
  ASSERT_EQ(all_reservations.size(), number_of_ops*max_workers);
  
  cls_2pc_queue_expire_reservations(op, stale_time);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  
  cls_2pc_reservations good_reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, good_reservations));

  for (const auto& r : all_reservations) {
    if (good_reservations.find(r.first) == good_reservations.end()) {
      // not in the "good" list
      ASSERT_GE(stale_time.time_since_epoch().count(), 
          r.second.timestamp.time_since_epoch().count());
    }
  }
  for (const auto& r : good_reservations) {
   ASSERT_LT(stale_time.time_since_epoch().count(), 
       r.second.timestamp.time_since_epoch().count());
  }
}

TEST_F(TestCls2PCQueue, MultiProducer)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 128*1024*1024;
  const auto number_of_ops = 300U;
  const auto number_of_elements = 23U;
  const auto max_producer_count = 10U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  auto producer_count = max_producer_count;

  std::vector<std::thread> producers(max_producer_count);
  for (auto& p : producers) {
    p = std::thread([this, &queue_name, &producer_count] {
      librados::ObjectWriteOperation op;
  	  for (auto i = 0U; i < number_of_ops; ++i) {
        const std::string element_prefix("op-" +to_string(i) + "-element-");
        std::vector<bufferlist> data(number_of_elements);
        auto total_size = 0UL;
        // create vector of buffer lists
        std::generate(data.begin(), data.end(), [j = 0, &element_prefix, &total_size] () mutable {
            bufferlist bl;
            bl.append(element_prefix + to_string(j++));
            total_size += bl.length();
            return bl;
          });
        cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
        ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, total_size, number_of_elements, res_id), 0);
        ASSERT_NE(res_id, 0);
        cls_2pc_queue_commit(op, data, res_id);
        ASSERT_EQ(0, ioctx.operate(queue_name, &op));
      }
      --producer_count;
    });
  }

  auto consume_count = 0U;
  std::thread consumer([this, &queue_name, &consume_count, &producer_count] {
          librados::ObjectWriteOperation op;
          const auto max_elements = 42;
          const std::string marker;
          bool truncated = false;
          std::string end_marker;
          std::vector<cls_queue_entry> entries;
          while (producer_count > 0 || truncated) {
            const auto ret = cls_2pc_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
            ASSERT_EQ(0, ret);
            consume_count += entries.size();
            cls_2pc_queue_remove_entries(op, end_marker); 
            ASSERT_EQ(0, ioctx.operate(queue_name, &op));
          }
       });

  std::for_each(producers.begin(), producers.end(), [](auto& p) { p.join(); });
  consumer.join();
  ASSERT_EQ(consume_count, number_of_ops*number_of_elements*max_producer_count);
}

TEST_F(TestCls2PCQueue, AsyncConsumer)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  constexpr auto max_size = 128*1024*1024;
  constexpr auto number_of_ops = 250U;
  constexpr auto number_of_elements = 23U;
  librados::ObjectWriteOperation wop;
  wop.create(true);
  cls_2pc_queue_init(wop, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &wop));


  for (auto i = 0U; i < number_of_ops; ++i) {
    const std::string element_prefix("op-" +to_string(i) + "-element-");
    std::vector<bufferlist> data(number_of_elements);
    auto total_size = 0UL;
    // create vector of buffer lists
    std::generate(data.begin(), data.end(), [j = 0, &element_prefix, &total_size] () mutable {
        bufferlist bl;
        bl.append(element_prefix + to_string(j++));
        total_size += bl.length();
        return bl;
        });
    cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
    ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, total_size, number_of_elements, res_id), 0);
    ASSERT_NE(res_id, 0);
    cls_2pc_queue_commit(wop, data, res_id);
    ASSERT_EQ(0, ioctx.operate(queue_name, &wop));
  }

  constexpr auto max_elements = 42;
  std::string marker;
  std::string end_marker;
  librados::ObjectReadOperation rop;
  auto consume_count = 0U;
  std::vector<cls_queue_entry> entries;
 	bool truncated = true;
  while (truncated) {
		bufferlist bl;
		int rc;
    cls_2pc_queue_list_entries(rop, marker, max_elements, &bl, &rc);
    ASSERT_EQ(0, ioctx.operate(queue_name, &rop, nullptr));
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(cls_2pc_queue_list_entries_result(bl, entries, &truncated, end_marker), 0);
    consume_count += entries.size();
    cls_2pc_queue_remove_entries(wop, end_marker); 
		marker = end_marker;
  }

  ASSERT_EQ(consume_count, number_of_ops*number_of_elements);
	// execute all delete operations in a batch
  ASSERT_EQ(0, ioctx.operate(queue_name, &wop));
  // make sure that queue is empty
  ASSERT_EQ(cls_2pc_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker), 0);
  ASSERT_EQ(entries.size(), 0);
}

TEST_F(TestCls2PCQueue, MultiProducerConsumer)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024;
  const auto number_of_ops = 300U;
  const auto number_of_elements = 23U;
  const auto max_workers = 10U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  auto producer_count = max_workers;

  auto retry_happened = false;

  std::vector<std::thread> producers(max_workers);
  for (auto& p : producers) {
    p = std::thread([this, &queue_name, &producer_count, &retry_happened] {
      librados::ObjectWriteOperation op;
  	  for (auto i = 0U; i < number_of_ops; ++i) {
        const std::string element_prefix("op-" +to_string(i) + "-element-");
        std::vector<bufferlist> data(number_of_elements);
        auto total_size = 0UL;
        // create vector of buffer lists
        std::generate(data.begin(), data.end(), [j = 0, &element_prefix, &total_size] () mutable {
            bufferlist bl;
            bl.append(element_prefix + to_string(j++));
            total_size += bl.length();
            return bl;
          });
        cls_2pc_reservation::id_t res_id = cls_2pc_reservation::NO_ID;
        auto rc = cls_2pc_queue_reserve(ioctx, queue_name, total_size, number_of_elements, res_id);
        while (rc != 0) {
          // other errors should cause test to fail
          ASSERT_EQ(rc, -ENOSPC);
          ASSERT_EQ(res_id, 0);
          // queue is full, sleep and retry
          retry_happened = true;
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
          rc = cls_2pc_queue_reserve(ioctx, queue_name, total_size, number_of_elements, res_id);
        };
        ASSERT_NE(res_id, 0);
        cls_2pc_queue_commit(op, data, res_id);
        ASSERT_EQ(0, ioctx.operate(queue_name, &op));
      }
      --producer_count;
    });
  }

  const auto max_elements = 128;
  std::vector<std::thread> consumers(max_workers/2);
  for (auto& c : consumers) {
    c = std::thread([this, &queue_name, &producer_count] {
          librados::ObjectWriteOperation op;
          const std::string marker;
          bool truncated = false;
          std::string end_marker;
          std::vector<cls_queue_entry> entries;
          while (producer_count > 0 || truncated) {
            const auto ret = cls_2pc_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
            ASSERT_EQ(0, ret);
            if (entries.empty()) {
              // queue is empty, let it fill
              std::this_thread::sleep_for(std::chrono::milliseconds(100));
            } else {
              cls_2pc_queue_remove_entries(op, end_marker); 
              ASSERT_EQ(0, ioctx.operate(queue_name, &op));
            }
          }
       });
  }

  std::for_each(producers.begin(), producers.end(), [](auto& p) { p.join(); });
  std::for_each(consumers.begin(), consumers.end(), [](auto& c) { c.join(); });
  if (!retry_happened) {
      std::cerr << "Queue was never full - all reservations were sucessfull." <<
          "Please decrease the amount of consumer threads" << std::endl;
  }
  // make sure that queue is empty and no reservations remain
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
  const std::string marker;
  bool truncated = false;
  std::string end_marker;
  std::vector<cls_queue_entry> entries;
  ASSERT_EQ(0, cls_2pc_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker));
  ASSERT_EQ(entries.size(), 0);
}

TEST_F(TestCls2PCQueue, ReserveSpillover)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024U*1024U;
  const auto number_of_ops = 1024U;
  const auto number_of_elements = 8U;
  const auto size_to_reserve = 64U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  for (auto i = 0U; i < number_of_ops; ++i) {
    cls_2pc_reservation::id_t res_id;
    ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
    ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
  }
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), number_of_ops);
  for (const auto& r : reservations) {
      ASSERT_NE(r.first, cls_2pc_reservation::NO_ID);
      ASSERT_GT(r.second.timestamp.time_since_epoch().count(), 0);
  }
}

TEST_F(TestCls2PCQueue, CommitSpillover)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024U*1024U;
  const auto number_of_ops = 1024U;
  const auto number_of_elements = 4U;
  const auto size_to_reserve = 128U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  for (auto i = 0U; i < number_of_ops; ++i) {
    cls_2pc_reservation::id_t res_id;
    ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
    ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
  }
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  for (const auto& r : reservations) {
    const std::string element_prefix("foo");
        std::vector<bufferlist> data(number_of_elements);
        auto total_size = 0UL;
        // create vector of buffer lists
        std::generate(data.begin(), data.end(), [j = 0, &element_prefix, &total_size] () mutable {
            bufferlist bl;
            bl.append(element_prefix + to_string(j++));
            total_size += bl.length();
            return bl;
          });
      ASSERT_NE(r.first, cls_2pc_reservation::NO_ID);
      cls_2pc_queue_commit(op, data, r.first);
      ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  }
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
}

TEST_F(TestCls2PCQueue, AbortSpillover)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024U*1024U;
  const auto number_of_ops = 1024U;
  const auto number_of_elements = 4U;
  const auto size_to_reserve = 128U;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  for (auto i = 0U; i < number_of_ops; ++i) {
    cls_2pc_reservation::id_t res_id;
    ASSERT_EQ(cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, number_of_elements, res_id), 0);
    ASSERT_NE(res_id, cls_2pc_reservation::NO_ID);
  }
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  for (const auto& r : reservations) {
      ASSERT_NE(r.first, cls_2pc_reservation::NO_ID);
      cls_2pc_queue_abort(op, r.first);
      ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  }
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
}

