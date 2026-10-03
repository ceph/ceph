// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/types.h"

#include "cls/2pc_queue/cls_2pc_queue_types.h"
#include "cls/2pc_queue/cls_2pc_queue_client.h"
#include "cls/queue/cls_queue_client.h"
#include "cls/2pc_queue/cls_2pc_queue_types.h"

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "test/librados/test_pool_types.h"
#include "global/global_context.h"
#include "cls/2pc_queue/cls_2pc_queue_const.h"

#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <chrono>
#include <atomic>

using namespace std;
using ceph::test::PoolType;
using ceph::test::pool_type_name;
using ceph::test::create_pool_by_type;
using ceph::test::destroy_pool_by_type;

class TestCls2PCQueue : public ceph::test::ClsTestFixture {
  // Inherits: rados, ioctx, pool_name, pool_type, SetUp(), TearDown()
};

TEST_P(TestCls2PCQueue, GetCapacity)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 8*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_2pc_queue_init(op, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  uint64_t size;

  const int ret = cls_2pc_queue_get_capacity(ioctx, queue_name, size);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(max_size, size);
}

TEST_P(TestCls2PCQueue, AsyncGetCapacity)
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

TEST_P(TestCls2PCQueue, Reserve)
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

TEST_P(TestCls2PCQueue, AsyncReserve)
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

TEST_P(TestCls2PCQueue, Commit)
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

TEST_P(TestCls2PCQueue, Stats)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024*128;
  const auto number_of_ops = 200U;
  const auto number_of_elements = 23U;
  auto total_committed_elements = 0U;
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

    total_committed_elements += number_of_elements;
    uint32_t committed_entries;
    uint64_t size;

    ASSERT_EQ(cls_2pc_queue_get_topic_stats(ioctx, queue_name, committed_entries, size), 0);
    ASSERT_EQ(committed_entries, total_committed_elements);
  }
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);
}

TEST_P(TestCls2PCQueue, UpgradeFromReef)
{
  const std::string queue_name = __PRETTY_FUNCTION__;
  const auto max_size = 1024*1024*128;
  const auto number_of_ops = 200U;
  const auto number_of_elements = 23U;
  auto total_committed_elements = 0U;
  librados::ObjectWriteOperation wop;
  wop.create(true);
  cls_2pc_queue_init(wop, queue_name, max_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &wop));

  for (auto i = 0U; i < number_of_ops; ++i) {
    const std::string element_prefix("wop-" +to_string(i) + "-element-");
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
    cls_2pc_queue_commit(wop, data, res_id);
    ASSERT_EQ(0, ioctx.operate(queue_name, &wop));

    total_committed_elements += number_of_elements;
    uint32_t committed_entries;
    uint64_t size;

    ASSERT_EQ(cls_2pc_queue_get_topic_stats(ioctx, queue_name, committed_entries, size), 0);
    ASSERT_EQ(committed_entries, total_committed_elements);
  }
  cls_2pc_reservations reservations;
  ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  ASSERT_EQ(reservations.size(), 0);

  constexpr auto max_elements = 42U;
  std::string marker;
  std::string end_marker;
  librados::ObjectReadOperation rop;
  auto consume_count = 0U;
  std::vector<cls_queue_entry> entries;
  bool truncated = true;

  auto simulate_reef_cls_2pc_queue_remove_entries = [](librados::ObjectWriteOperation& wop, const std::string& end_marker) {
    bufferlist in;
    cls_queue_remove_op rem_op;
    rem_op.end_marker = end_marker;
    encode(rem_op, in);
    wop.exec(cls::tpc_queue::method::remove_entries, in);
  };

  while (truncated) {
    bufferlist bl;
    int rc;
    cls_2pc_queue_list_entries(rop, marker, max_elements, &bl, &rc);
    ASSERT_EQ(0, ioctx.operate(queue_name, &rop, nullptr));
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(cls_2pc_queue_list_entries_result(bl, entries, &truncated, end_marker), 0);

    consume_count += entries.size();
    // simulating reef cls_2pc_queue_remove_entries with cls_queue_remove_op
    simulate_reef_cls_2pc_queue_remove_entries(wop, end_marker);
    marker = end_marker;
    total_committed_elements -= entries.size();
  }

  // execute all delete operations in a batch
  ASSERT_EQ(0, ioctx.operate(queue_name, &wop));
  ASSERT_EQ(consume_count, number_of_ops*number_of_elements);

  uint32_t entries_number;
  uint64_t size;
  ASSERT_EQ(cls_2pc_queue_get_topic_stats(ioctx, queue_name, entries_number, size), 0);
  ASSERT_EQ(total_committed_elements, 0);
  ASSERT_EQ(entries_number, 0);
}

TEST_P(TestCls2PCQueue, Abort)
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

TEST_P(TestCls2PCQueue, ReserveError)
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

TEST_P(TestCls2PCQueue, CommitError)
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
  // 2 reservations were not committed
  ASSERT_EQ(reservations.size(), 2);
}

TEST_P(TestCls2PCQueue, AbortError)
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

TEST_P(TestCls2PCQueue, MultiReserve)
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

TEST_P(TestCls2PCQueue, MultiCommit)
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

TEST_P(TestCls2PCQueue, MultiAbort)
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

TEST_P(TestCls2PCQueue, ReserveCommit)
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

TEST_P(TestCls2PCQueue, ReserveAbort)
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

TEST_P(TestCls2PCQueue, ManualCleanup)
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

  // anything older than 100ms is considered stale
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

TEST_P(TestCls2PCQueue, Cleanup)
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

  // anything older than 100ms is considered stale
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

TEST_P(TestCls2PCQueue, MultiProducer)
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

  std::atomic<int>  producer_count = max_producer_count;

  std::vector<std::thread> producers(max_producer_count);
  for (auto& p : producers) {
    p = std::thread([this, &queue_name, &producer_count] {
      for (auto i = 0U; i < number_of_ops; ++i) {
        librados::ObjectWriteOperation op;
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
          const auto max_elements = 42;
          const std::string marker;
          bool truncated = true;
          std::string end_marker;
          std::vector<cls_queue_entry> entries;
          while (producer_count > 0 || truncated) {
            const auto ret = cls_2pc_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
            if (ret != 0) {
              // transient error, retry
              std::this_thread::sleep_for(std::chrono::milliseconds(10));
              continue;
            }
            if (entries.empty()) {
              // queue is empty, let it fill
              std::this_thread::sleep_for(std::chrono::milliseconds(100));
            } else {
              consume_count += entries.size();
              librados::ObjectWriteOperation op;
              cls_2pc_queue_remove_entries(op, end_marker, max_elements);
              ASSERT_EQ(0, ioctx.operate(queue_name, &op));
            }
          }
       });

  std::for_each(producers.begin(), producers.end(), [](auto& p) { p.join(); });
  consumer.join();
  ASSERT_EQ(consume_count, number_of_ops*number_of_elements*max_producer_count);
}

TEST_P(TestCls2PCQueue, AsyncConsumer)
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
  auto consume_count = 0U;
  std::vector<cls_queue_entry> entries;
  bool truncated = true;
  while (truncated) {
    librados::ObjectReadOperation rop;
    bufferlist bl;
    int rc;
    cls_2pc_queue_list_entries(rop, marker, max_elements, &bl, &rc);
    ASSERT_EQ(0, ioctx.operate(queue_name, &rop, nullptr));
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(cls_2pc_queue_list_entries_result(bl, entries, &truncated, end_marker), 0);
    consume_count += entries.size();
    cls_2pc_queue_remove_entries(wop, end_marker, max_elements);
    marker = end_marker;
  }

  ASSERT_EQ(consume_count, number_of_ops*number_of_elements);
  // execute all delete operations in a batch
  ASSERT_EQ(0, ioctx.operate(queue_name, &wop));
  // make sure that queue is empty
  ASSERT_EQ(cls_2pc_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker), 0);
  ASSERT_EQ(entries.size(), 0);
}

TEST_P(TestCls2PCQueue, MultiProducerConsumer)
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

  std::atomic<int> producer_count = max_workers;

  std::atomic<bool> retry_happened = false;

  std::vector<std::thread> producers(max_workers);
  for (auto& p : producers) {
    p = std::thread([this, &queue_name, &producer_count, &retry_happened] {
      for (auto i = 0U; i < number_of_ops; ++i) {
        librados::ObjectWriteOperation op;
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
  std::vector<std::thread> readers(max_workers/2);
  for (auto& c : readers) {
    c = std::thread([this, &queue_name, &producer_count, &retry_happened] {
          const std::string marker;
          bool truncated = true;
          std::string end_marker;
          std::vector<cls_queue_entry> entries;
          while (producer_count > 0 || truncated) {
            if (!retry_happened) {
              // queue was never full, let it fill
              std::this_thread::sleep_for(std::chrono::milliseconds(100));
              continue;
            }
            const auto ret = cls_2pc_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
            if (ret != 0) {
              // transient error, retry
              std::this_thread::sleep_for(std::chrono::milliseconds(10));
              continue;
            }
            if (entries.empty()) {
              // queue is empty, let it fill
              std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
          }
       });
  }
  
  auto deleter = std::thread([this, &queue_name, &producer_count, &retry_happened] {
      const std::string marker;
      bool truncated = true;
      std::string end_marker;
      std::vector<cls_queue_entry> entries;
      while (producer_count > 0 || truncated) {
        if (!retry_happened) {
          // queue was never full, let it fill
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          continue;
        }
        const auto ret = cls_2pc_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
        if (ret != 0) {
          // transient error, retry
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
          continue;
        }
        if (entries.empty()) {
          // queue is empty, let it fill
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        } else {
          librados::ObjectWriteOperation op;
          cls_2pc_queue_remove_entries(op, end_marker, max_elements);
          ASSERT_EQ(0, ioctx.operate(queue_name, &op));
        }
      }
  });

  std::for_each(producers.begin(), producers.end(), [](auto& p) { p.join(); });
  std::for_each(readers.begin(), readers.end(), [](auto& c) { c.join(); });
  deleter.join();
  ASSERT_TRUE(retry_happened);
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

TEST_P(TestCls2PCQueue, ReserveSpillover)
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

TEST_P(TestCls2PCQueue, CommitSpillover)
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

TEST_P(TestCls2PCQueue, AbortSpillover)
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

namespace {
// read/write the queue head the same way queue_read_head()/queue_write_head() do
void read_queue_head(librados::IoCtx& ioctx, const std::string& queue_name, cls_queue_head& head)
{
  constexpr auto prefix_len = sizeof(uint16_t) + sizeof(uint64_t);
  bufferlist bl;
  ASSERT_EQ(ioctx.read(queue_name, bl, prefix_len, 0), static_cast<int>(prefix_len));
  auto it = bl.cbegin();
  uint16_t queue_head_start;
  decode(queue_head_start, it);
  ASSERT_EQ(queue_head_start, QUEUE_HEAD_START);
  uint64_t encoded_len;
  decode(encoded_len, it);
  bl.clear();
  ASSERT_EQ(ioctx.read(queue_name, bl, encoded_len, prefix_len), static_cast<int>(encoded_len));
  it = bl.cbegin();
  decode(head, it);
}

void write_queue_head(librados::IoCtx& ioctx, const std::string& queue_name, const cls_queue_head& head)
{
  bufferlist bl;
  encode(static_cast<uint16_t>(QUEUE_HEAD_START), bl);
  bufferlist bl_head;
  encode(head, bl_head);
  encode(static_cast<uint64_t>(bl_head.length()), bl);
  bl.claim_append(bl_head);
  ASSERT_LE(bl.length(), head.max_head_size);
  ASSERT_EQ(0, ioctx.write(queue_name, bl, bl.length(), 0));
}

void read_urgent_data(librados::IoCtx& ioctx, const std::string& queue_name,
                      cls_queue_head& head, cls_2pc_urgent_data& urgent_data)
{
  ASSERT_NO_FATAL_FAILURE(read_queue_head(ioctx, queue_name, head));
  auto it = head.bl_urgent_data.cbegin();
  decode(urgent_data, it);
}

// encode the urgent data as a pre-v4 cls would
bufferlist encode_old_urgent_data(const cls_2pc_urgent_data& urgent_data, uint8_t version)
{
  bufferlist bl;
  ENCODE_START(version, 1, bl);
  encode(urgent_data.reserved_size, bl);
  encode(urgent_data.last_id, bl);
  encode(urgent_data.reservations, bl);
  encode(urgent_data.has_xattrs, bl);
  if (version >= 2) {
    encode(urgent_data.committed_entries, bl);
  }
  ENCODE_FINISH(bl);
  return bl;
}

void drift_reserved_size(librados::IoCtx& ioctx, const std::string& queue_name,
                         uint64_t drifted_reserved_size, uint8_t version)
{
  cls_queue_head head;
  cls_2pc_urgent_data urgent_data;
  ASSERT_NO_FATAL_FAILURE(read_urgent_data(ioctx, queue_name, head, urgent_data));
  urgent_data.reserved_size = drifted_reserved_size;
  head.bl_urgent_data = encode_old_urgent_data(urgent_data, version);
  ASSERT_NO_FATAL_FAILURE(write_queue_head(ioctx, queue_name, head));
}

uint64_t pending_reservations_size(librados::IoCtx& ioctx, const std::string& queue_name)
{
  cls_2pc_reservations reservations;
  EXPECT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
  uint64_t total = 0;
  for (const auto& [id, res] : reservations) {
    total += res.size + res.entries*QUEUE_ENTRY_OVERHEAD;
  }
  return total;
}
}

// the first write of any kind must fix a drifted reserved_size of a pre-v4 queue
TEST_P(TestCls2PCQueue, RecalcDriftedReservedSize)
{
  const auto max_size = 64U*1024U;
  const auto size_to_reserve = 1000U;
  const uint64_t reservation_size = size_to_reserve + QUEUE_ENTRY_OVERHEAD;
  const std::vector<std::string> first_writes = {"reserve", "commit", "abort", "expire", "remove_entries"};

  for (const uint8_t version : {1, 2, 3}) {
    for (const auto& first_write : first_writes) {
      const std::string queue_name = std::string(__PRETTY_FUNCTION__) + "-v" + to_string(version) +
        "-" + first_write;
      SCOPED_TRACE(queue_name);
      librados::ObjectWriteOperation op;
      op.create(true);
      cls_2pc_queue_init(op, queue_name, max_size);
      ASSERT_EQ(0, ioctx.operate(queue_name, &op));

      // one committed entry and one pending reservation
      bufferlist bl_data;
      bl_data.append(std::string(size_to_reserve, 'a'));
      cls_2pc_reservation::id_t res_id;
      ASSERT_EQ(0, cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, 1, res_id));
      cls_2pc_queue_commit(op, {bl_data}, res_id);
      ASSERT_EQ(0, ioctx.operate(queue_name, &op));
      cls_2pc_reservation::id_t pending_id;
      ASSERT_EQ(0, cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, 1, pending_id));

      // drift reserved_size so that no new reservation fits
      ASSERT_NO_FATAL_FAILURE(drift_reserved_size(ioctx, queue_name, max_size, version));

      uint64_t expected_reserved_size = 0;
      if (first_write == "reserve") {
        ASSERT_EQ(0, cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, 1, res_id));
        expected_reserved_size = 2*reservation_size;
      } else {
        if (first_write == "commit") {
          cls_2pc_queue_commit(op, {bl_data}, pending_id);
        } else if (first_write == "abort") {
          cls_2pc_queue_abort(op, pending_id);
        } else if (first_write == "expire") {
          cls_2pc_queue_expire_reservations(op, ceph::coarse_real_clock::now() + std::chrono::seconds(60));
        } else {
          std::vector<cls_queue_entry> entries;
          bool truncated;
          std::string end_marker;
          ASSERT_EQ(0, cls_2pc_queue_list_entries(ioctx, queue_name, "", 1, entries, &truncated, end_marker));
          ASSERT_EQ(entries.size(), 1);
          cls_2pc_queue_remove_entries(op, end_marker, 1);
          // the pending reservation is untouched
          expected_reserved_size = reservation_size;
        }
        ASSERT_EQ(0, ioctx.operate(queue_name, &op));
      }

      cls_queue_head head;
      cls_2pc_urgent_data urgent_data;
      ASSERT_NO_FATAL_FAILURE(read_urgent_data(ioctx, queue_name, head, urgent_data));
      ASSERT_EQ(urgent_data.decoded_struct_v, 4);
      ASSERT_EQ(urgent_data.reserved_size, expected_reserved_size);

      ASSERT_EQ(0, cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, 1, res_id));
    }
  }
}

// same, with reservations spilled over to xattrs
TEST_P(TestCls2PCQueue, RecalcDriftedReservedSizeSpillover)
{
  const auto max_size = 1024U*1024U;
  const auto number_of_ops = 1024U;
  const auto size_to_reserve = 64U;
  const std::vector<std::string> first_writes = {"reserve", "commit", "abort", "expire"};

  for (const uint8_t version : {2, 3}) {
    for (const auto& first_write : first_writes) {
      const std::string queue_name = std::string(__PRETTY_FUNCTION__) + "-v" + to_string(version) +
        "-" + first_write;
      SCOPED_TRACE(queue_name);
      librados::ObjectWriteOperation op;
      op.create(true);
      cls_2pc_queue_init(op, queue_name, max_size);
      ASSERT_EQ(0, ioctx.operate(queue_name, &op));

      cls_2pc_reservation::id_t res_id;
      for (auto i = 0U; i < number_of_ops; ++i) {
        ASSERT_EQ(0, cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, 1, res_id));
      }
      // pick a reservation that was spilled over to xattrs
      cls_queue_head head;
      cls_2pc_urgent_data urgent_data;
      ASSERT_NO_FATAL_FAILURE(read_urgent_data(ioctx, queue_name, head, urgent_data));
      ASSERT_TRUE(urgent_data.has_xattrs);
      cls_2pc_reservations reservations;
      ASSERT_EQ(0, cls_2pc_queue_list_reservations(ioctx, queue_name, reservations));
      ASSERT_EQ(reservations.size(), number_of_ops);
      auto spilled = std::find_if(reservations.begin(), reservations.end(), [&urgent_data](const auto& r) {
          return urgent_data.reservations.count(r.first) == 0;
        });
      ASSERT_NE(spilled, reservations.end());
      const auto spilled_id = spilled->first;

      ASSERT_NO_FATAL_FAILURE(drift_reserved_size(ioctx, queue_name, max_size, version));

      if (first_write == "reserve") {
        ASSERT_EQ(0, cls_2pc_queue_reserve(ioctx, queue_name, size_to_reserve, 1, res_id));
      } else {
        if (first_write == "commit") {
          bufferlist bl_data;
          bl_data.append(std::string(size_to_reserve, 'a'));
          cls_2pc_queue_commit(op, {bl_data}, spilled_id);
        } else if (first_write == "abort") {
          cls_2pc_queue_abort(op, spilled_id);
        } else {
          cls_2pc_queue_expire_reservations(op, ceph::coarse_real_clock::now() + std::chrono::seconds(60));
        }
        ASSERT_EQ(0, ioctx.operate(queue_name, &op));
      }

      ASSERT_NO_FATAL_FAILURE(read_urgent_data(ioctx, queue_name, head, urgent_data));
      ASSERT_EQ(urgent_data.decoded_struct_v, 4);
      ASSERT_EQ(urgent_data.reserved_size, pending_reservations_size(ioctx, queue_name));
      if (first_write == "expire") {
        ASSERT_EQ(urgent_data.reserved_size, 0);
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(, TestCls2PCQueue,
  ::testing::Values(PoolType::REPLICATED, PoolType::FAST_EC),
  [](const ::testing::TestParamInfo<PoolType>& info) {
  return pool_type_name(info.param);
  }
);
