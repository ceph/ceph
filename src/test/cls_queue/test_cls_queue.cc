// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
#include <thread>
#include <chrono>
#include <atomic>

using namespace std;

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
          int number_of_ops, 
          int number_of_elements,
          int expected_rc) {
    librados::ObjectWriteOperation op;
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
  test_enqueue(queue_name, 10, 100, 0);
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
  test_enqueue(queue_name, 8, 5, 0);
  // 2 iterations, 5 elelemts each
  // expect -28 (Q FULL)
  test_enqueue(queue_name, 2, 5, -28);
}

TEST_F(TestClsQueue, List)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  const auto number_of_ops = 10;
  const auto number_of_elements = 100;

  // test multiple enqueues
  test_enqueue(queue_name, number_of_ops, number_of_elements, 0);

  const auto max_elements = 42;
  std::string marker;
  bool truncated = false;
  std::string next_marker;
  auto total_elements = 0;
  do {
    std::vector<cls_queue_entry> entries;
    const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, next_marker);
    ASSERT_EQ(0, ret);
    marker = next_marker;
    total_elements += entries.size();
  } while (truncated);

  ASSERT_EQ(total_elements, number_of_ops*number_of_elements);
}

TEST_F(TestClsQueue, ListByEndMarker)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  const auto number_of_ops = 10;
  const auto number_of_elements = 100;

  // test multiple enqueues
  test_enqueue(queue_name, number_of_ops, number_of_elements, 0);

  const auto max_elements = 42;
  std::string marker, end_marker;
  bool truncated = false;
  std::string max_op_next_marker;
  auto total_elements = 0;
  do {
    std::vector<cls_queue_entry> entries;
    auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, max_op_next_marker);
    ASSERT_EQ(0, ret);
    end_marker = max_op_next_marker;

    std::vector<cls_queue_entry> end_marker_entries;
    std::string end_marker_next_marker;
    bool end_marker_truncated = false;
    ret = cls_queue_list_entries(ioctx, queue_name, marker, end_marker, end_marker_entries,
                                 &end_marker_truncated, end_marker_next_marker);
    ASSERT_EQ(0, ret);

    ASSERT_EQ(end_marker_next_marker, end_marker);
    ASSERT_EQ(end_marker_entries.size(), entries.size());
    for (auto i = 0U; i < end_marker_entries.size() && i < entries.size(); ++i) {
      ASSERT_EQ(end_marker_entries[i].marker, entries[i].marker);
    }

    marker = max_op_next_marker;
    total_elements += entries.size();
  } while (truncated);

  ASSERT_EQ(total_elements, number_of_ops*number_of_elements);
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
  test_enqueue(queue_name, 10, 100, 0);

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

TEST_F(TestClsQueue, DequeueMarker)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  // test multiple enqueues
  test_enqueue(queue_name, 10, 1000, 0);

  const auto remove_elements = 1024;
  const std::string marker;
  bool truncated;
  std::string end_marker;
  std::vector<cls_queue_entry> entries;
  auto ret = cls_queue_list_entries(ioctx, queue_name, marker, remove_elements, entries, &truncated, end_marker);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(truncated, true);
  cls_queue_marker after_deleted_marker;
  // remove specific markers
  for (const auto& entry : entries) {
    cls_queue_marker marker;
    marker.from_str(entry.marker.c_str());
    ASSERT_EQ(marker.from_str(entry.marker.c_str()), 0);
    if (marker.offset > 0 && marker.offset % 2 == 0) {
      after_deleted_marker = marker;
      cls_queue_remove_entries(op, marker.to_str());
    }
  }
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  entries.clear();
  ret = cls_queue_list_entries(ioctx, queue_name, marker, remove_elements, entries, &truncated, end_marker);
  ASSERT_EQ(0, ret);
  for (const auto& entry : entries) {
    cls_queue_marker marker;
    marker.from_str(entry.marker.c_str());
    ASSERT_EQ(marker.from_str(entry.marker.c_str()), 0);
    ASSERT_GE(marker.gen, after_deleted_marker.gen);    
    ASSERT_GE(marker.offset, after_deleted_marker.offset);    
  }
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
  test_enqueue(queue_name, 10, 100, 0);

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
  test_enqueue(queue_name, 10, 100, 0);

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

TEST_F(TestClsQueue, EnqueueDequeue)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  bool done = false;
  const int number_of_ops = 10;
  const int number_of_elements = 100;

  std::thread producer([this, &queue_name, &done] {
          test_enqueue(queue_name, number_of_ops, number_of_elements, 0);
          done = true;
          });

  auto consume_count = 0U;
  std::thread consumer([this, &queue_name, &consume_count, &done] {
          librados::ObjectWriteOperation op;
          const auto max_elements = 42;
          const std::string marker;
          bool truncated = false;
          std::string end_marker;
          std::vector<cls_queue_entry> entries;
          while (!done || truncated) {
            const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
            ASSERT_EQ(0, ret);
            consume_count += entries.size();
            cls_queue_remove_entries(op, end_marker);
            ASSERT_EQ(0, ioctx.operate(queue_name, &op));
          }
       });

  producer.join();
  consumer.join();
  ASSERT_EQ(consume_count, number_of_ops*number_of_elements);
}

TEST_F(TestClsQueue, QueueFullDequeue)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 4096;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  bool done = false;
  const auto number_of_ops = 100;
  const auto number_of_elements = 50;

  std::thread producer([this, &queue_name, &done] {
          librados::ObjectWriteOperation op;
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
            if (ioctx.operate(queue_name, &op) == -28) {
              // queue is full - wait and retry
              --i;
              std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
          }
          done = true;
        });

  auto consume_count = 0;
  std::thread consumer([this, &queue_name, &consume_count, &done] {
          librados::ObjectWriteOperation op;
          const auto max_elements = 42;
          std::string marker;
          bool truncated = false;
          std::string end_marker;
          std::vector<cls_queue_entry> entries;
          while (!done || truncated) {
            auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
            ASSERT_EQ(0, ret);
            consume_count += entries.size();
            cls_queue_remove_entries(op, end_marker);
            ASSERT_EQ(0, ioctx.operate(queue_name, &op));
          }
       });

  producer.join();
  consumer.join();
  ASSERT_EQ(consume_count, number_of_ops*number_of_elements);
}

TEST_F(TestClsQueue, MultiProducer)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  const int max_producer_count = 10;
  int producer_count = max_producer_count;
  const int number_of_ops = 10;
  const int number_of_elements = 100;

  std::vector<std::thread> producers(max_producer_count);
  for (auto& p : producers) {
    p = std::thread([this, &queue_name, &producer_count] {
		      test_enqueue(queue_name, number_of_ops, number_of_elements, 0);
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
            const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
            ASSERT_EQ(0, ret);
            consume_count += entries.size();
            cls_queue_remove_entries(op, end_marker);
            ASSERT_EQ(0, ioctx.operate(queue_name, &op));
          }
       });

  for (auto& p : producers) {
      p.join();
  }
  consumer.join();
  ASSERT_EQ(consume_count, number_of_ops*number_of_elements*max_producer_count);
}

TEST_F(TestClsQueue, MultiConsumer)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  bool done = false;
  const int number_of_ops = 10;
  const int number_of_elements = 100;

  std::thread producer([this, &queue_name, &done] {
          test_enqueue(queue_name, number_of_ops, number_of_elements, 0);
          done = true;
          });

  int consume_count = 0;
  std::mutex list_and_remove_lock;

  std::vector<std::thread> consumers(10);
  for (auto& c : consumers) {
    c = std::thread([this, &queue_name, &consume_count, &done, &list_and_remove_lock] {
          librados::ObjectWriteOperation op;
          const auto max_elements = 42;
          const std::string marker;
          bool truncated = false;
          std::string end_marker;
          std::vector<cls_queue_entry> entries;
          while (!done || truncated) {
            std::lock_guard lock(list_and_remove_lock);
            const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
            ASSERT_EQ(0, ret);
            consume_count += entries.size();
            cls_queue_remove_entries(op, end_marker);
            ASSERT_EQ(0, ioctx.operate(queue_name, &op));
          }
    });
  }

  producer.join();
  for (auto& c : consumers) {
      c.join();
  }
  ASSERT_EQ(consume_count, number_of_ops*number_of_elements);
}

TEST_F(TestClsQueue, NoLockMultiConsumer)
{
  const std::string queue_name = "my-queue";
  const uint64_t queue_size = 1024*1024;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  bool done = false;
  const int number_of_ops = 10;
  const int number_of_elements = 100;

  std::thread producer([this, &queue_name, &done] {
          test_enqueue(queue_name, number_of_ops, number_of_elements, 0);
          done = true;
          });

  std::vector<std::thread> consumers(5);
  for (auto& c : consumers) {
    c = std::thread([this, &queue_name, &done] {
          librados::ObjectWriteOperation op;
          const auto max_elements = 42;
          const std::string marker;
          bool truncated = false;
          std::string end_marker;
          std::vector<cls_queue_entry> entries;
          while (!done || truncated) {
            const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
            ASSERT_EQ(0, ret);
            cls_queue_remove_entries(op, end_marker);
            ASSERT_EQ(0, ioctx.operate(queue_name, &op));
          }
    });
  }

  producer.join();
  for (auto& c : consumers) {
      c.join();
  }

  // make sure queue is empty
  const auto max_elements = 1000;
  const std::string marker;
  bool truncated = false;
  std::string end_marker;
  std::vector<cls_queue_entry> entries;
  const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
  ASSERT_EQ(0, ret);
  ASSERT_EQ(entries.size(), 0);
  ASSERT_EQ(truncated, false);
}

TEST_F(TestClsQueue, WrapAround)
{
  const std::string queue_name = "my-queue";
  const auto number_of_entries = 10U;
  const auto max_entry_size = 2000;
  const auto min_entry_size = 1000;
  const uint64_t queue_size = number_of_entries*max_entry_size;
  const auto entry_overhead = 10;
  librados::ObjectWriteOperation op;
  op.create(true);
  cls_queue_init(op, queue_name, queue_size);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  std::list<bufferlist> total_bl;

  // fill up the queue
  for (auto i = 0U; i < number_of_entries; ++i) {
    const auto entry_size = rand()%(max_entry_size - min_entry_size + 1) + min_entry_size;
    std::string entry_str(entry_size-entry_overhead, 0);
    std::generate_n(entry_str.begin(), entry_str.size(), [](){return (char)(rand());});
    bufferlist entry_bl;
    entry_bl.append(entry_str);
    std::vector<bufferlist> data{{entry_bl}};
    // enqueue vector
    cls_queue_enqueue(op, 0, data);
    ASSERT_EQ(0, ioctx.operate(queue_name, &op));
    total_bl.push_back(entry_bl);
  }

  std::string marker;
  for (auto j = 0; j < 10; ++j) {
    // empty half+1 of the queue
    const auto max_elements = number_of_entries/2 + 1;
    bool truncated;
    std::string end_marker;
    std::vector<cls_queue_entry> entries;
    const auto ret = cls_queue_list_entries(ioctx, queue_name, marker, max_elements, entries, &truncated, end_marker);
    ASSERT_EQ(0, ret);
    for (auto& entry : entries) {
      ASSERT_EQ(entry.data, total_bl.front());
      total_bl.pop_front();
    }
    marker = end_marker;
    cls_queue_remove_entries(op, end_marker);
    ASSERT_EQ(0, ioctx.operate(queue_name, &op));
   
    // fill half+1 of the queue
    for (auto i = 0U; i < number_of_entries/2 + 1; ++i) {
      const auto entry_size = rand()%(max_entry_size - min_entry_size + 1) + min_entry_size;
      std::string entry_str(entry_size-entry_overhead, 0);
      std::generate_n(entry_str.begin(), entry_str.size(), [](){return (char)(rand());});
      bufferlist entry_bl;
      entry_bl.append(entry_str);
      std::vector<bufferlist> data{{entry_bl}};
      cls_queue_enqueue(op, 0, data);
      ASSERT_EQ(0, ioctx.operate(queue_name, &op));
      total_bl.push_back(entry_bl);
    }
  }
}

