// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_queue_client.h"
#include "cls/queue/cls_queue_ops.h"

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include <string>
#include <vector>
#include <algorithm>

class TestClsQueue : public ::testing::Test {
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

  void test_enqueue(const std::string& queue_name, 
          librados::ObjectWriteOperation& op, 
          int number_of_ops, 
          int number_of_elements,
          int expected_rc) {
    // test multiple enqueues
    for (auto i = 0; i < number_of_ops; ++i) {
      const std::string element_prefix("op-" +to_string(i) + "-element-");
      std::vector<bufferlist> data(number_of_elements);
      // create vector of buffer lists
      std::generate(data.begin(), data.end(), [j = 0, &element_prefix] () mutable {
            bufferlist bl;
            bl.append(element_prefix + to_string(j++));
            return bl;
          });

      // enqueue vector
      cls_queue_enqueue(op, 0, data);
      ASSERT_EQ(expected_rc, ioctx.operate(queue_name, &op));
      }
  }
};

TEST_F(TestClsQueue, GetCapacity)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  uint64_t size;
  const int ret = cls_queue_get_capacity(ioctx, queue_name, size);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(queue_size, size);
}

TEST_F(TestClsQueue, Enqueue)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  // test multiple enqueues
  // 10 iterations, 100 elelemts each
  // expect 0 (OK)
  test_enqueue(queue_name, op, 10, 100, 0);
}

TEST_F(TestClsQueue, QueueFull)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  // 8 iterations, 5 elelemts each
  // expect 0 (OK)
  test_enqueue(queue_name, op, 8, 5, 0);
  // 2 iterations, 5 elelemts each
  // expect -28 (Q FULL)
  test_enqueue(queue_name, op, 2, 5, -28);
}

TEST_F(TestClsQueue, List)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  // test multiple enqueues
  test_enqueue(queue_name, op, 10, 100, 0);

  const auto max_elements = 50;
  std::string marker;
  bool truncated = false;
  std::string next_marker;
  do {
    std::vector<cls_queue_entry> entries;
    const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, next_marker);
    ASSERT_EQ(0, ret);
    if (truncated) {
	  ASSERT_NE(marker, next_marker);
    } else {
	  ASSERT_EQ(marker, next_marker);
    }
    marker = next_marker;
  } while (truncated);
}

TEST_F(TestClsQueue, Dequeue)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  // test multiple enqueues
  test_enqueue(queue_name, op, 10, 100, 0);

  // get the end marker for 42 elements
  const auto remove_elements = 42;
  const std::string marker;
  bool truncated;
  std::string end_marker;
  std::vector<cls_queue_entry> entries;
  const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, remove_elements, entries, &truncated, end_marker);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(truncated, true);
  // remove up to end marker
  cls_queue_remove_entries(op, end_marker); 
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));
}

TEST_F(TestClsQueue, ListEmpty)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  const auto max_elements = 50;
  const std::string marker;
  bool truncated;
  std::string next_marker;
  std::vector<cls_queue_entry> entries;
  const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, next_marker);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(truncated, false);
  ASSERT_EQ(entries.size(), 0);
}

TEST_F(TestClsQueue, DequeueEmpty)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  const auto max_elements = 50;
  const std::string marker;
  bool truncated;
  std::string end_marker;
  std::vector<cls_queue_entry> entries;
  const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
  ASSERT_EQ(0, ret);
  cls_queue_remove_entries(op, end_marker); 
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));
}

TEST_F(TestClsQueue, ListAll)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  // test multiple enqueues
  test_enqueue(queue_name, op, 10, 100, 0);

  const auto total_elements = 10*100;
  std::string marker;
  bool truncated;
  std::string next_marker;
  std::vector<cls_queue_entry> entries;
  const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, total_elements, entries, &truncated, next_marker);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(entries.size(), total_elements);
  ASSERT_EQ(truncated, false);
}

TEST_F(TestClsQueue, DeleteAll)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  // test multiple enqueues
  test_enqueue(queue_name, op, 10, 100, 0);

  const auto total_elements = 10*100;
  const std::string marker;
  bool truncated;
  std::string end_marker;
  std::vector<cls_queue_entry> entries;
  auto ret = cls_queue_list_entries(ioctx, queue_name, marker, total_elements, entries, &truncated, end_marker);
  ASSERT_EQ(0, ret);
  cls_queue_remove_entries(op, end_marker); 
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  // list again to make sure that queue is empty
  ret = cls_queue_list_entries(ioctx, queue_name, marker, 10, entries, &truncated, end_marker);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(truncated, false);
  ASSERT_EQ(entries.size(), 0);
}

