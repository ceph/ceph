// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/log.h"
#include "crimson/os/poseidonstore/cache.h"
#include "crimson/os/poseidonstore/device_manager/memory.h"
#include "crimson/os/poseidonstore/transaction.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::poseidonstore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}
struct cache_test_t : public seastar_test_suite_t {
  std::unique_ptr<DeviceManager> device_manager;
  Cache cache;
  paddr_t current = 0;

  cache_test_t()
    : device_manager(create_memory(device_manager::DEFAULT_TEST_MEMORY)),
      cache(*device_manager) {}

  seastar::future<std::optional<paddr_t>> submit_transaction(
    TransactionRef t) {

    auto record = cache.try_construct_record(*t);
    if (!record) {
      return seastar::make_ready_future<std::optional<paddr_t>>(
	std::nullopt);
    }

    bufferlist bl;
    auto items = (*record)->get_write_items();
    for (auto &&block : items) {
      bl.append(block.bl);
    }

    auto prev = current;
    auto size = bl.length();

    return device_manager->write(
      prev,
      std::move(bl)
    ).safe_then(
      [this, prev, t=std::move(t), len=size]() mutable {
	cache.complete_commit(*t);
	current = prev + len;
	return seastar::make_ready_future<std::optional<paddr_t>>(prev);
      },
      crimson::ct_error::all_same_way([](auto e) {
	ASSERT_FALSE("failed to submit");
      })
     );
  }

  void read_verify(paddr_t addr, size_t len, char c) {
    auto bp = ceph::bufferptr(
      buffer::create_page_aligned(len));
    ::memset(bp.c_str(), c, len);
    bufferlist bl, buf;
    bl.push_back(bp);
    auto test = device_manager->read(
      addr,
      len).unsafe_get0();
    buf.push_back(test);
    ASSERT_EQ(
      bl.begin().crc32c(bl.length(), 1),
      buf.begin().crc32c(buf.length(), 1));
  }

  auto get_transaction() {
    return make_transaction();
  }

  seastar::future<> set_up_fut() final {
    return device_manager->init(
    ).safe_then([this] {
      return seastar::now();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ceph_assert(0 == "error");
	})
      );
  }
  seastar::future<> tear_down_fut() final {
    return seastar::now();
  }
};

TEST_F(cache_test_t, extent_split)
{
  run_async([this] {
    auto extent1 = cache.alloc_new_extent<TestBlock>(
      TestBlock::SIZE*2);
    extent1->set_laddr(TestBlock::SIZE*2);
    extent1->set_contents('d');
    auto extent2 = cache.split_extent(TestBlock::SIZE, TestBlock::SIZE, extent1);
    ASSERT_EQ(TestBlock::SIZE, extent1->get_length());
    ASSERT_EQ(TestBlock::SIZE, extent2->get_length());
    for (uint64_t i = 0; i < TestBlock::SIZE; i++) {
      ASSERT_EQ('d', *(extent1->get_bptr().c_str() + i));
      ASSERT_EQ('d', *(extent2->get_bptr().c_str() + i));
    }
    extent1 = cache.alloc_new_extent<TestBlock>(TestBlock::SIZE*4);
    extent2 = cache.split_extent(TestBlock::SIZE*3, TestBlock::SIZE, extent1);
    auto extent3 = cache.split_extent(TestBlock::SIZE*2, TestBlock::SIZE, extent1);
    ASSERT_EQ(TestBlock::SIZE*2, extent1->get_length());
    ASSERT_EQ(TestBlock::SIZE, extent2->get_length());
    ASSERT_EQ(TestBlock::SIZE, extent3->get_length());

    extent1 = cache.alloc_new_extent<TestBlock>(TestBlock::SIZE*4);
    extent2 = cache.split_extent(TestBlock::SIZE*2, TestBlock::SIZE*2, extent1);
    ASSERT_EQ(TestBlock::SIZE*2, extent2->get_length());

    extent1 = cache.alloc_new_extent<TestBlock>(TestBlock::SIZE*4);
    extent2 = cache.split_extent(0, 8000, extent1);
    ASSERT_EQ(nullptr, extent2);
  });
}

TEST_F(cache_test_t, extent_merge_split)
{
  run_async([this] {
    auto extent1 = cache.alloc_new_extent<TestBlock>(TestBlock::SIZE);
    extent1->set_laddr(TestBlock::SIZE*2);
    extent1->set_contents('d');
    auto extent2 = cache.alloc_new_extent<TestBlock>(TestBlock::SIZE*2);
    extent2->set_laddr(TestBlock::SIZE*3);
    extent2->set_contents('c');
    auto extent3 = cache.merge_extent<TestBlock>(extent1, extent2);
    ASSERT_EQ(TestBlock::SIZE*3, extent3->get_length());
    for (uint64_t i = 0; i < TestBlock::SIZE; i++) {
      ASSERT_EQ('d', *(extent3->get_bptr().c_str() + i));
      ASSERT_EQ('c', *(extent3->get_bptr().c_str() + i + TestBlock::SIZE));
    }

    extent1 = cache.alloc_new_extent<TestBlock>(TestBlock::SIZE);
    extent1->set_laddr(TestBlock::SIZE);
    extent2 = cache.alloc_new_extent<TestBlock>(TestBlock::SIZE*2);
    extent2->set_laddr(TestBlock::SIZE*2);
    extent3 = cache.merge_extent(extent1, extent2);
    ASSERT_EQ(extent3->get_laddr(), TestBlock::SIZE);

    extent1 = cache.split_extent(TestBlock::SIZE*2, TestBlock::SIZE, extent3);
    ASSERT_EQ(extent3->get_laddr(), TestBlock::SIZE);
    ASSERT_EQ(extent1->get_laddr(), TestBlock::SIZE*3);
  });
}

TEST_F(cache_test_t, normal_submit)
{
  run_async([this] {
    {
      auto t = get_transaction();
      auto extent1 = cache.alloc_new_extent<TestBlock>(
	TestBlock::SIZE);
      extent1->set_laddr(0);
      extent1->set_contents('c');
      auto extent2 = cache.alloc_new_extent<TestBlock>(
	TestBlock::SIZE);
      extent2->set_laddr(TestBlock::SIZE);
      extent2->set_contents('d');
      t->add_to_read_set(extent1);
      t->add_to_write_set(extent2);
      cache.fill_cache(*t);
      auto ret = submit_transaction(std::move(t)).get0();
      read_verify(*ret, TestBlock::SIZE, 'd');
    }
    {
      auto t = get_transaction();
      auto extent1 = cache.alloc_new_extent<TestBlock>(
	TestBlock::SIZE);
      extent1->set_laddr(0);
      extent1->set_contents('c');
      auto extent2 = cache.alloc_new_extent<TestBlock>(
	TestBlock::SIZE);
      extent2->set_laddr(TestBlock::SIZE);
      extent2->set_contents('d');
      auto extent3 = cache.alloc_new_extent<TestBlock>(
	TestBlock::SIZE);
      extent3->set_laddr(TestBlock::SIZE*3);
      extent3->set_contents('e');
      t->add_to_read_set(extent1);
      t->add_to_write_set(extent2);
      t->add_to_write_set(extent3);
      cache.fill_cache(*t);
      auto ret = submit_transaction(std::move(t)).get0();
      read_verify(*ret, TestBlock::SIZE, 'd');
      read_verify(*ret + TestBlock::SIZE, TestBlock::SIZE, 'e');
    }
    {
      auto t = get_transaction();
      auto extent1 = cache.alloc_new_extent<TestBlock>(
	TestBlock::SIZE);
      extent1->set_laddr(0);
      extent1->set_contents('c');
      auto extent2 = cache.alloc_new_extent<TestBlock>(
	TestBlock::SIZE);
      extent2->set_laddr(TestBlock::SIZE);
      extent2->set_contents('d');
      auto extent3 = cache.alloc_new_extent<TestBlock>(
	TestBlock::SIZE);
      extent3->set_laddr(TestBlock::SIZE*2);
      extent3->set_contents('e');

      auto extent4 = cache.merge_extent(extent1, extent2);
      auto extent5 = cache.merge_extent(extent4, extent3);
      t->add_to_write_set(extent5);
      cache.fill_cache(*t);
      auto ret = submit_transaction(std::move(t)).get0();
      read_verify(*ret, TestBlock::SIZE, 'c');
      read_verify(*ret + TestBlock::SIZE, TestBlock::SIZE, 'd');
      read_verify(*ret + TestBlock::SIZE*2, TestBlock::SIZE, 'e');
    }
  });
}
