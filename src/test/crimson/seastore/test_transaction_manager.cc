// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"

#include "test/crimson/seastore/test_block.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

struct test_extent_record_t {
  test_extent_desc_t desc;
  unsigned refcount = 0;
  test_extent_record_t() = default;
  test_extent_record_t(
    const test_extent_desc_t &desc,
    unsigned refcount) : desc(desc), refcount(refcount) {}

  void update(const test_extent_desc_t &to) {
    desc = to;
  }

  bool operator==(const test_extent_desc_t &rhs) const {
    return desc == rhs;
  }
  bool operator!=(const test_extent_desc_t &rhs) const {
    return desc != rhs;
  }
};

std::ostream &operator<<(std::ostream &lhs, const test_extent_record_t &rhs) {
  return lhs << "test_extent_record_t(" << rhs.desc
	     << ", refcount=" << rhs.refcount << ")";
}

struct transaction_manager_test_t : public seastar_test_suite_t {
  std::unique_ptr<SegmentManager> segment_manager;
  Journal journal;
  Cache cache;
  LBAManagerRef lba_manager;
  TransactionManager tm;

  transaction_manager_test_t()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      journal(*segment_manager),
      cache(*segment_manager),
      lba_manager(
	lba_manager::create_lba_manager(*segment_manager, cache)),
      tm(*segment_manager, journal, cache, *lba_manager) {}

  seastar::future<> set_up_fut() final {
    return segment_manager->init().safe_then([this] {
      return tm.mkfs();
    }).safe_then([this] {
      return tm.mount();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to mount");
      })
    );
  }

  seastar::future<> tear_down_fut() final {
    return tm.close(
    ).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to close");
      })
    );
  }

  struct test_extents_t : std::map<laddr_t, test_extent_record_t> {
  private:
    void check_available(laddr_t addr, extent_len_t len) {
      auto iter = upper_bound(addr);
      if (iter != begin()) {
	auto liter = iter;
	liter--;
	EXPECT_FALSE(liter->first + liter->second.desc.len > addr);
      }
      if (iter != end()) {
	EXPECT_FALSE(iter->first < addr + len);
      }
    }
    void check_hint(laddr_t hint, laddr_t addr, extent_len_t len) {
      auto iter = lower_bound(hint);
      laddr_t last = hint;
      while (true) {
	if (iter == end() || iter->first > addr) {
	  EXPECT_EQ(addr, last);
	  break;
	}
	EXPECT_FALSE(iter->first - last > len);
	last = iter->first + iter->second.desc.len;
	++iter;
      }
    }
  public:
    void insert(TestBlock &extent) {
      check_available(extent.get_laddr(), extent.get_length());
      emplace(
	std::make_pair(
	  extent.get_laddr(),
	  test_extent_record_t{extent.get_desc(), 1}
	));
    }
    void alloced(laddr_t hint, TestBlock &extent) {
      check_hint(hint, extent.get_laddr(), extent.get_length());
      insert(extent);
    }
  } test_mappings;

  struct test_transaction_t {
    TransactionRef t;
    test_extents_t mappings;
  };

  test_transaction_t create_transaction() {
    return { tm.create_transaction(), test_mappings };
  }

  TestBlockRef alloc_extent(
    test_transaction_t &t,
    laddr_t hint,
    extent_len_t len,
    char contents) {
    auto extent = tm.alloc_extent<TestBlock>(
      *(t.t),
      hint,
      len).unsafe_get0();
    extent->set_contents(contents);
    EXPECT_FALSE(t.mappings.count(extent->get_laddr()));
    EXPECT_EQ(len, extent->get_length());
    t.mappings.alloced(hint, *extent);
    return extent;
  }

  void replay() {
    tm.close().unsafe_get();
    tm.mount().unsafe_get();
  }

  void check_mappings() {
    auto t = create_transaction();
    check_mappings(t);
  }

  TestBlockRef get_extent(
    test_transaction_t &t,
    laddr_t addr,
    extent_len_t len) {
    ceph_assert(t.mappings.count(addr));
    ceph_assert(t.mappings[addr].desc.len == len);

    auto ret_list = tm.read_extents<TestBlock>(
      *t.t, addr, len
    ).unsafe_get0();
    EXPECT_EQ(ret_list.size(), 1);
    auto &ext = ret_list.begin()->second;
    auto &laddr = ret_list.begin()->first;
    EXPECT_EQ(addr, laddr);
    EXPECT_EQ(addr, ext->get_laddr());
    return ext;
  }

  test_block_mutator_t mutator;
  TestBlockRef mutate_extent(
    test_transaction_t &t,
    TestBlockRef ref) {
    ceph_assert(t.mappings.count(ref->get_laddr()));
    ceph_assert(t.mappings[ref->get_laddr()].desc.len == ref->get_length());
    auto ext = tm.get_mutable_extent(*t.t, ref)->cast<TestBlock>();
    EXPECT_EQ(ext->get_laddr(), ref->get_laddr());
    EXPECT_EQ(ext->get_desc(), ref->get_desc());
    mutator.mutate(*ext);
    t.mappings[ext->get_laddr()].update(ext->get_desc());
    return ext;
  }

  void inc_ref(test_transaction_t &t, laddr_t offset) {
    ceph_assert(t.mappings.count(offset));
    ceph_assert(t.mappings[offset].refcount > 0);
    auto refcnt = tm.inc_ref(*t.t, offset).unsafe_get0();
    t.mappings[offset].refcount++;
    EXPECT_EQ(refcnt, t.mappings[offset].refcount);
  }

  void dec_ref(test_transaction_t &t, laddr_t offset) {
    ceph_assert(t.mappings.count(offset));
    ceph_assert(t.mappings[offset].refcount > 0);
    auto refcnt = tm.dec_ref(*t.t, offset).unsafe_get0();
    t.mappings[offset].refcount--;
    EXPECT_EQ(refcnt, t.mappings[offset].refcount);
    if (t.mappings[offset].refcount == 0) {
      t.mappings.erase(offset);
    }
  }

  void check_mappings(test_transaction_t &t) {
    for (auto &i: t.mappings) {
      logger().debug("check_mappings: {}->{}", i.first, i.second);
      auto ext = get_extent(t, i.first, i.second.desc.len);
      EXPECT_EQ(i.second, ext->get_desc());
    }
  }

  void submit_transaction(test_transaction_t t) {
    tm.submit_transaction(std::move(t.t)).unsafe_get();
    test_mappings = t.mappings;
  }
};

TEST_F(transaction_manager_test_t, basic)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    constexpr laddr_t ADDR = 0xFF * SIZE;
    {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	ADDR,
	SIZE,
	'a');
      ASSERT_EQ(ADDR, extent->get_laddr());
      check_mappings(t);
      check_mappings();
      submit_transaction(std::move(t));
      check_mappings();
    }
  });
}

TEST_F(transaction_manager_test_t, mutate)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    constexpr laddr_t ADDR = 0xFF * SIZE;
    {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	ADDR,
	SIZE,
	'a');
      ASSERT_EQ(ADDR, extent->get_laddr());
      check_mappings(t);
      check_mappings();
      submit_transaction(std::move(t));
      check_mappings();
    }
    replay();
    {
      auto t = create_transaction();
      auto ext = get_extent(
	t,
	ADDR,
	SIZE);
      auto mut = mutate_extent(t, ext);
      check_mappings(t);
      check_mappings();
      submit_transaction(std::move(t));
      check_mappings();
    }
    replay();
    check_mappings();
  });
}

TEST_F(transaction_manager_test_t, inc_dec_ref)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    constexpr laddr_t ADDR = 0xFF * SIZE;
    {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	ADDR,
	SIZE,
	'a');
      ASSERT_EQ(ADDR, extent->get_laddr());
      check_mappings(t);
      check_mappings();
      submit_transaction(std::move(t));
      check_mappings();
    }
    replay();
    {
      auto t = create_transaction();
      inc_ref(t, ADDR);
      check_mappings(t);
      check_mappings();
      submit_transaction(std::move(t));
      check_mappings();
    }
    {
      auto t = create_transaction();
      dec_ref(t, ADDR);
      check_mappings(t);
      check_mappings();
      submit_transaction(std::move(t));
      check_mappings();
    }
    replay();
    {
      auto t = create_transaction();
      dec_ref(t, ADDR);
      check_mappings(t);
      check_mappings();
      submit_transaction(std::move(t));
      check_mappings();
    }
  });
}
