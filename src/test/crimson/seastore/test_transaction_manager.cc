// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <random>

#include <boost/iterator/counting_iterator.hpp>

#include "test/crimson/gtest_seastar.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"
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

template<>
struct fmt::formatter<test_extent_record_t> : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const test_extent_record_t& r, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "test_extent_record_t({}, refcount={})",
			  r.desc, r.refcount);
  }
};

struct transaction_manager_test_t :
  public seastar_test_suite_t,
  TMTestState {

  std::random_device rd;
  std::mt19937 gen;

  transaction_manager_test_t(std::size_t num_main_devices, std::size_t num_cold_devices)
    : TMTestState(num_main_devices, num_cold_devices), gen(rd()) {
  }

  laddr_t get_random_laddr(size_t block_size, laddr_t limit) {
    return block_size *
      std::uniform_int_distribution<>(0, (limit / block_size) - 1)(gen);
  }

  char get_random_contents() {
    return static_cast<char>(std::uniform_int_distribution<>(0, 255)(gen));
  }

  seastar::future<> set_up_fut() final {
    return tm_setup();
  }

  seastar::future<> tear_down_fut() final {
    return tm_teardown();
  }

  struct test_extents_t : std::map<laddr_t, test_extent_record_t> {
    using delta_t = std::map<laddr_t, std::optional<test_extent_record_t>>;
    std::map<laddr_t, uint64_t> laddr_write_seq;

    struct delta_overlay_t {
      const test_extents_t &extents;
      const delta_t &delta;

      delta_overlay_t(
	const test_extents_t &extents,
	const delta_t &delta)
	: extents(extents), delta(delta) {}


      class iterator {
	friend class test_extents_t;

	const delta_overlay_t &parent;
	test_extents_t::const_iterator biter;
	delta_t::const_iterator oiter;
	std::optional<std::pair<laddr_t, test_extent_record_t>> cur;

	iterator(
	  const delta_overlay_t &parent,
	  test_extents_t::const_iterator biter,
	  delta_t::const_iterator oiter)
	  : parent(parent), biter(biter), oiter(oiter) {}

	laddr_t get_bkey() {
	  return biter == parent.extents.end() ? L_ADDR_MAX : biter->first;
	}

	laddr_t get_okey() {
	  return oiter == parent.delta.end() ? L_ADDR_MAX : oiter->first;
	}

	bool is_end() {
	  return oiter == parent.delta.end() && biter == parent.extents.end();
	}

	bool is_valid() {
	  return is_end() ||
	    ((get_okey() < get_bkey()) && (oiter->second)) ||
	    (get_okey() > get_bkey());
	}

	auto get_pair() {
	  assert(is_valid());
	  assert(!is_end());
	  auto okey = get_okey();
	  auto bkey = get_bkey();
	  return (
	    bkey < okey ?
	    std::pair<laddr_t, test_extent_record_t>(*biter) :
	    std::make_pair(okey, *(oiter->second)));
	}

	void adjust() {
	  while (!is_valid()) {
	    if (get_okey() < get_bkey()) {
	      assert(!oiter->second);
	      ++oiter;
	    } else {
	      assert(get_okey() == get_bkey());
	      ++biter;
	    }
	  }
	  assert(is_valid());
	  if (!is_end()) {
	    cur = get_pair();
	  } else {
	    cur = std::nullopt;
	  }
	}

      public:
	iterator(const iterator &) = default;
	iterator(iterator &&) = default;

	iterator &operator++() {
	  assert(is_valid());
	  assert(!is_end());
	  if (get_bkey() < get_okey()) {
	    ++biter;
	  } else {
	    ++oiter;
	  }
	  adjust();
	  return *this;
	}

	bool operator==(const iterator &o) const {
	  return o.biter == biter && o.oiter == oiter;
	}
	bool operator!=(const iterator &o) const {
	  return !(*this == o);
	}

	auto operator*() {
	  assert(!is_end());
	  return *cur;
	}
	auto operator->() {
	  assert(!is_end());
	  return &*cur;
	}
      };

      iterator begin() {
	auto ret = iterator{*this, extents.begin(), delta.begin()};
	ret.adjust();
	return ret;
      }

      iterator end() {
	auto ret = iterator{*this, extents.end(), delta.end()};
	// adjust unnecessary
	return ret;
      }

      iterator lower_bound(laddr_t l) {
	auto ret = iterator{*this, extents.lower_bound(l), delta.lower_bound(l)};
	ret.adjust();
	return ret;
      }

      iterator upper_bound(laddr_t l) {
	auto ret = iterator{*this, extents.upper_bound(l), delta.upper_bound(l)};
	ret.adjust();
	return ret;
      }

      iterator find(laddr_t l) {
	auto ret = lower_bound(l);
	if (ret == end() || ret->first != l) {
	  return end();
	} else {
	  return ret;
	}
      }
    };
  private:
    void check_available(
      laddr_t addr, extent_len_t len, const delta_t &delta
    ) const {
      delta_overlay_t overlay(*this, delta);
      for (const auto &i: overlay) {
	if (i.first < addr) {
	  EXPECT_FALSE(i.first + i.second.desc.len > addr);
	} else {
	  EXPECT_FALSE(addr + len > i.first);
	}
      }
    }

    void check_hint(
      laddr_t hint,
      laddr_t intermediate_hint,
      laddr_t addr,
      extent_len_t len,
      delta_t &delta) const {
      delta_overlay_t overlay(*this, delta);
      auto real_hint = intermediate_hint == L_ADDR_NULL
	? hint : intermediate_hint;
      auto iter = overlay.lower_bound(real_hint);
      laddr_t last = real_hint;
      while (true) {
	if (iter == overlay.end() || iter->first > addr) {
	  EXPECT_EQ(addr, last);
	  break;
	}
	EXPECT_FALSE(iter->first - last > len);
	last = iter->first + iter->second.desc.len;
	++iter;
      }
    }

    std::optional<test_extent_record_t> &populate_delta(
      laddr_t addr, delta_t &delta, const test_extent_desc_t *desc) const {
      auto diter = delta.find(addr);
      if (diter != delta.end())
	return diter->second;

      auto iter = find(addr);
      if (iter == end()) {
	assert(desc);
	auto ret = delta.emplace(
	  std::make_pair(addr, test_extent_record_t{*desc, 0}));
	assert(ret.second);
	return ret.first->second;
      } else {
	auto ret = delta.emplace(*iter);
	assert(ret.second);
	return ret.first->second;
      }
    }
  public:
    delta_overlay_t get_overlay(const delta_t &delta) const {
      return delta_overlay_t{*this, delta};
    }

    void insert(TestBlock &extent, delta_t &delta) const {
      check_available(extent.get_laddr(), extent.get_length(), delta);
      delta[extent.get_laddr()] =
	test_extent_record_t{extent.get_desc(), 1};
    }

    void alloced(
      laddr_t hint,
      TestBlock &extent,
      delta_t &delta,
      laddr_t intermediate_hint = L_ADDR_NULL) const
    {
      check_hint(
	hint,
	intermediate_hint,
	extent.get_laddr(),
	extent.get_length(),
	delta);
      insert(extent, delta);
    }

    bool contains(laddr_t addr, const delta_t &delta) const {
      delta_overlay_t overlay(*this, delta);
      return overlay.find(addr) != overlay.end();
    }

    test_extent_record_t get(laddr_t addr, const delta_t &delta) const {
      delta_overlay_t overlay(*this, delta);
      auto iter = overlay.find(addr);
      assert(iter != overlay.end());
      return iter->second;
    }

    void update(
      laddr_t addr,
      const test_extent_desc_t &desc,
      delta_t &delta) const {
      auto &rec = populate_delta(addr, delta, &desc);
      assert(rec);
      rec->desc = desc;
    }

    int inc_ref(
      laddr_t addr,
      delta_t &delta) const {
      auto &rec = populate_delta(addr, delta, nullptr);
      assert(rec);
      return ++rec->refcount;
    }

    int dec_ref(
      laddr_t addr,
      delta_t &delta) const {
      auto &rec = populate_delta(addr, delta, nullptr);
      assert(rec);
      assert(rec->refcount > 0);
      rec->refcount--;
      if (rec->refcount == 0) {
	delta[addr] = std::nullopt;
	return 0;
      } else {
	return rec->refcount;
      }
    }

    void consume(const delta_t &delta, const uint64_t write_seq = 0) {
      for (const auto &i : delta) {
	if (i.second) {
	  if (laddr_write_seq.find(i.first) == laddr_write_seq.end() ||
	      laddr_write_seq[i.first] <= write_seq) {
	    (*this)[i.first] = *i.second;
	    laddr_write_seq[i.first] = write_seq;
	  }
	} else {
	  erase(i.first);
	}
      }
    }

  } test_mappings;

  struct test_transaction_t {
    TransactionRef t;
    test_extents_t::delta_t mapping_delta;
  };

  test_transaction_t create_transaction() {
    return { create_mutate_transaction(), {} };
  }

  test_transaction_t create_read_test_transaction() {
    return {create_read_transaction(), {} };
  }

  test_transaction_t create_weak_test_transaction() {
    return { create_weak_transaction(), {} };
  }

  TestBlockRef alloc_extent(
    test_transaction_t &t,
    laddr_t hint,
    extent_len_t len,
    char contents) {
    auto extents = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->alloc_data_extents<TestBlock>(trans, hint, len);
    }).unsafe_get0();
    assert(extents.size() == 1);
    auto extent = extents.front();
    extent->set_contents(contents);
    EXPECT_FALSE(test_mappings.contains(extent->get_laddr(), t.mapping_delta));
    EXPECT_EQ(len, extent->get_length());
    test_mappings.alloced(hint, *extent, t.mapping_delta);
    return extent;
  }

  std::vector<TestBlockRef> alloc_extents(
    test_transaction_t &t,
    laddr_t hint,
    extent_len_t len,
    char contents) {
    auto extents = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->alloc_data_extents<TestBlock>(trans, hint, len);
    }).unsafe_get0();
    size_t length = 0;
    for (auto &extent : extents) {
      extent->set_contents(contents);
      length += extent->get_length();
      EXPECT_FALSE(test_mappings.contains(extent->get_laddr(), t.mapping_delta));
      test_mappings.alloced(hint, *extent, t.mapping_delta);
    }
    EXPECT_EQ(len, length);
    return extents;
  }

  void alloc_extents_deemed_fail(
    test_transaction_t &t,
    laddr_t hint,
    extent_len_t len,
    char contents)
  {
    std::cout << __func__ << std::endl;
    auto fut = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->alloc_data_extents<TestBlock>(trans, hint, len);
    });
    fut.unsafe_wait();
    assert(fut.failed());
    (void)fut.get_exception();
  }

  TestBlockRef alloc_extent(
    test_transaction_t &t,
    laddr_t hint,
    extent_len_t len) {
    return alloc_extent(
      t,
      hint,
      len,
      get_random_contents());
  }

  bool check_usage() {
    return epm->check_usage();
  }

  void replay() {
    EXPECT_TRUE(check_usage());
    restart();
  }

  void check() {
    check_mappings();
    check_usage();
  }

  void check_mappings() {
    auto t = create_weak_test_transaction();
    check_mappings(t);
  }

  TestBlockRef read_pin(
    test_transaction_t &t,
    LBAMappingRef pin) {
    auto addr = pin->is_indirect()
      ? pin->get_intermediate_base()
      : pin->get_key();
    auto len = pin->is_indirect()
      ? pin->get_intermediate_length()
      : pin->get_length();
    ceph_assert(test_mappings.contains(addr, t.mapping_delta));
    ceph_assert(test_mappings.get(addr, t.mapping_delta).desc.len == len);

    auto ext = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->read_pin<TestBlock>(trans, std::move(pin));
    }).unsafe_get0();
    EXPECT_EQ(addr, ext->get_laddr());
    return ext;
  }

  TestBlockRef get_extent(
    test_transaction_t &t,
    laddr_t addr,
    extent_len_t len) {
    ceph_assert(test_mappings.contains(addr, t.mapping_delta));
    ceph_assert(test_mappings.get(addr, t.mapping_delta).desc.len == len);

    auto ext = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->read_extent<TestBlock>(trans, addr, len);
    }).unsafe_get0();
    EXPECT_EQ(addr, ext->get_laddr());
    return ext;
  }

  TestBlockRef try_get_extent(
    test_transaction_t &t,
    laddr_t addr) {
    ceph_assert(test_mappings.contains(addr, t.mapping_delta));

    using ertr = with_trans_ertr<TransactionManager::read_extent_iertr>;
    using ret = ertr::future<TestBlockRef>;
    auto ext = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->read_extent<TestBlock>(trans, addr);
    }).safe_then([](auto ext) -> ret {
      return ertr::make_ready_future<TestBlockRef>(ext);
    }).handle_error(
      [](const crimson::ct_error::eagain &e) {
	return seastar::make_ready_future<TestBlockRef>();
      },
      crimson::ct_error::assert_all{
	"get_extent got invalid error"
      }
    ).get0();
    if (ext) {
      EXPECT_EQ(addr, ext->get_laddr());
    }
    return ext;
  }

  TestBlockRef try_get_extent(
    test_transaction_t &t,
    laddr_t addr,
    extent_len_t len) {
    ceph_assert(test_mappings.contains(addr, t.mapping_delta));
    ceph_assert(test_mappings.get(addr, t.mapping_delta).desc.len == len);

    using ertr = with_trans_ertr<TransactionManager::read_extent_iertr>;
    using ret = ertr::future<TestBlockRef>;
    auto ext = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->read_extent<TestBlock>(trans, addr, len);
    }).safe_then([](auto ext) -> ret {
      return ertr::make_ready_future<TestBlockRef>(ext);
    }).handle_error(
      [](const crimson::ct_error::eagain &e) {
	return seastar::make_ready_future<TestBlockRef>();
      },
      crimson::ct_error::assert_all{
	"get_extent got invalid error"
      }
    ).get0();
    if (ext) {
      EXPECT_EQ(addr, ext->get_laddr());
    }
    return ext;
  }

  TestBlockRef try_read_pin(
    test_transaction_t &t,
    LBAMappingRef &&pin) {
    using ertr = with_trans_ertr<TransactionManager::base_iertr>;
    using ret = ertr::future<TestBlockRef>;
    bool indirect = pin->is_indirect();
    auto addr = pin->get_key();
    auto im_addr = indirect ? pin->get_intermediate_base() : L_ADDR_NULL;
    auto ext = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->read_pin<TestBlock>(trans, std::move(pin));
    }).safe_then([](auto ext) -> ret {
      return ertr::make_ready_future<TestBlockRef>(ext);
    }).handle_error(
      [](const crimson::ct_error::eagain &e) {
	return seastar::make_ready_future<TestBlockRef>();
      },
      crimson::ct_error::assert_all{
	"read_pin got invalid error"
      }
    ).get0();
    if (ext) {
      if (indirect) {
	EXPECT_EQ(im_addr, ext->get_laddr());
      } else {
	EXPECT_EQ(addr, ext->get_laddr());
      }
    }
    if (t.t->is_conflicted()) {
      return nullptr;
    }
    return ext;
  }

  test_block_mutator_t mutator;
  TestBlockRef mutate_extent(
    test_transaction_t &t,
    TestBlockRef ref) {
    ceph_assert(test_mappings.contains(ref->get_laddr(), t.mapping_delta));
    ceph_assert(
      test_mappings.get(ref->get_laddr(), t.mapping_delta).desc.len ==
      ref->get_length());

    auto ext = tm->get_mutable_extent(*t.t, ref)->cast<TestBlock>();
    EXPECT_EQ(ext->get_laddr(), ref->get_laddr());
    EXPECT_EQ(ext->get_desc(), ref->get_desc());
    mutator.mutate(*ext, gen);

    test_mappings.update(ext->get_laddr(), ext->get_desc(), t.mapping_delta);
    return ext;
  }

  TestBlockRef mutate_addr(
    test_transaction_t &t,
    laddr_t offset,
    size_t length) {
    auto ext = get_extent(t, offset, length);
    mutate_extent(t, ext);
    return ext;
  }

  LBAMappingRef get_pin(
    test_transaction_t &t,
    laddr_t offset) {
    ceph_assert(test_mappings.contains(offset, t.mapping_delta));
    auto pin = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->get_pin(trans, offset);
    }).unsafe_get0();
    EXPECT_EQ(offset, pin->get_key());
    return pin;
  }

  LBAMappingRef clone_pin(
    test_transaction_t &t,
    laddr_t offset,
    const LBAMapping &mapping) {
    auto pin = with_trans_intr(*(t.t), [&](auto &trans) {
      return tm->clone_pin(trans, offset, mapping);
    }).unsafe_get0();
    EXPECT_EQ(offset, pin->get_key());
    EXPECT_EQ(mapping.get_key(), pin->get_intermediate_key());
    EXPECT_EQ(mapping.get_key(), pin->get_intermediate_base());
    test_mappings.inc_ref(pin->get_intermediate_key(), t.mapping_delta);
    return pin;
  }

  LBAMappingRef try_get_pin(
    test_transaction_t &t,
    laddr_t offset) {
    ceph_assert(test_mappings.contains(offset, t.mapping_delta));
    using ertr = with_trans_ertr<TransactionManager::get_pin_iertr>;
    using ret = ertr::future<LBAMappingRef>;
    auto pin = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->get_pin(trans, offset);
    }).safe_then([](auto pin) -> ret {
      return ertr::make_ready_future<LBAMappingRef>(std::move(pin));
    }).handle_error(
      [](const crimson::ct_error::eagain &e) {
	return seastar::make_ready_future<LBAMappingRef>();
      },
      crimson::ct_error::assert_all{
	"get_extent got invalid error"
      }
    ).get0();
    if (pin) {
      EXPECT_EQ(offset, pin->get_key());
    }
    return pin;
  }

  void inc_ref(test_transaction_t &t, laddr_t offset) {
    ceph_assert(test_mappings.contains(offset, t.mapping_delta));
    ceph_assert(test_mappings.get(offset, t.mapping_delta).refcount > 0);

    auto refcnt = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->inc_ref(trans, offset);
    }).unsafe_get0();
    auto check_refcnt = test_mappings.inc_ref(offset, t.mapping_delta);
    EXPECT_EQ(refcnt, check_refcnt);
  }

  void dec_ref(test_transaction_t &t, laddr_t offset) {
    ceph_assert(test_mappings.contains(offset, t.mapping_delta));
    ceph_assert(test_mappings.get(offset, t.mapping_delta).refcount > 0);

    auto refcnt = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->remove(trans, offset);
    }).unsafe_get0();
    auto check_refcnt = test_mappings.dec_ref(offset, t.mapping_delta);
    EXPECT_EQ(refcnt, check_refcnt);
    if (refcnt == 0)
      logger().debug("dec_ref: {} at refcount 0", offset);
  }

  void check_mappings(test_transaction_t &t) {
    auto overlay = test_mappings.get_overlay(t.mapping_delta);
    for (const auto &i: overlay) {
      logger().debug("check_mappings: {}->{}", i.first, i.second);
      auto ext = get_extent(t, i.first, i.second.desc.len);
      assert(i.second == ext->get_desc());
    }
    with_trans_intr(
      *t.t,
      [this, &overlay](auto &t) {
	return lba_manager->scan_mappings(
	  t,
	  0,
	  L_ADDR_MAX,
	  [iter=overlay.begin(), &overlay](auto l, auto p, auto len) mutable {
	    EXPECT_NE(iter, overlay.end());
	    logger().debug(
	      "check_mappings: scan {}",
	      l);
	    EXPECT_EQ(l, iter->first);
	    ++iter;
	  });
      }).unsafe_get0();
    (void)with_trans_intr(
      *t.t,
      [=, this](auto &t) {
	return lba_manager->check_child_trackers(t);
      }).unsafe_get0();
  }

  bool try_submit_transaction(test_transaction_t t) {
    using ertr = with_trans_ertr<TransactionManager::submit_transaction_iertr>;
    using ret = ertr::future<bool>;
    uint64_t write_seq = 0;
    bool success = submit_transaction_fut_with_seq(*t.t
    ).safe_then([&write_seq](auto seq) -> ret {
      write_seq = seq;
      return ertr::make_ready_future<bool>(true);
    }).handle_error(
      [](const crimson::ct_error::eagain &e) {
	return seastar::make_ready_future<bool>(false);
      },
      crimson::ct_error::assert_all{
	"try_submit_transaction hit invalid error"
      }
    ).then([this](auto ret) {
      return epm->run_background_work_until_halt(
      ).then([ret] { return ret; });
    }).get0();

    if (success) {
      test_mappings.consume(t.mapping_delta, write_seq);
    }

    return success;
  }

  void submit_transaction(test_transaction_t &&t) {
    bool success = try_submit_transaction(std::move(t));
    EXPECT_TRUE(success);
  }

  void submit_transaction_expect_conflict(test_transaction_t &&t) {
    bool success = try_submit_transaction(std::move(t));
    EXPECT_FALSE(success);
  }

  auto allocate_sequentially(const size_t size, const int num, bool run_clean = true) {
    return repeat_eagain([this, size, num] {
      return seastar::do_with(
	create_transaction(),
	[this, size, num](auto &t) {
	  return with_trans_intr(
	    *t.t,
	    [&t, this, size, num](auto &) {
	      return trans_intr::do_for_each(
		boost::make_counting_iterator(0),
		boost::make_counting_iterator(num),
		[&t, this, size](auto) {
		  return tm->alloc_data_extents<TestBlock>(
		    *(t.t), L_ADDR_MIN, size
		  ).si_then([&t, this, size](auto extents) {
		    assert(extents.size() == 1);
		    auto extent = extents.front();
		    extent->set_contents(get_random_contents());
		    EXPECT_FALSE(
		      test_mappings.contains(extent->get_laddr(), t.mapping_delta));
		    EXPECT_EQ(size, extent->get_length());
		    test_mappings.alloced(extent->get_laddr(), *extent, t.mapping_delta);
		    return seastar::now();
		  });
		}).si_then([&t, this] {
		  return tm->submit_transaction(*t.t);
		});
	    }).safe_then([&t, this] {
	      test_mappings.consume(t.mapping_delta);
	    });
	});
    }).safe_then([this, run_clean]() {
      if (run_clean) {
        return epm->run_background_work_until_halt();
      } else {
        return epm->background_process.trimmer->trim();
      }
    }).handle_error(
      crimson::ct_error::assert_all{
	"Invalid error in SeaStore::list_collections"
      }
    );
  }

  void test_parallel_extent_read() {
    constexpr size_t TOTAL = 4<<20;
    constexpr size_t BSIZE = 4<<10;
    constexpr size_t BLOCKS = TOTAL / BSIZE;
    run_async([this] {
      for (unsigned i = 0; i < BLOCKS; ++i) {
	auto t = create_transaction();
	auto extent = alloc_extent(
	  t,
	  i * BSIZE,
	  BSIZE);
	ASSERT_EQ(i * BSIZE, extent->get_laddr());
	submit_transaction(std::move(t));
      }

      seastar::do_with(
	create_read_test_transaction(),
	[this](auto &t) {
	return with_trans_intr(*(t.t), [this](auto &t) {
	  return trans_intr::parallel_for_each(
	    boost::make_counting_iterator(0lu),
	    boost::make_counting_iterator(BLOCKS),
	    [this, &t](auto i) {
	    return tm->read_extent<TestBlock>(t, i * BSIZE, BSIZE
	    ).si_then([](auto) {
	      return seastar::now();
	    });
	  });
	});
      }).unsafe_get0();
    });
  }

  void test_random_writes_concurrent() {
    constexpr unsigned WRITE_STREAMS = 256;

    constexpr size_t TOTAL = 4<<20;
    constexpr size_t BSIZE = 4<<10;
    constexpr size_t BLOCKS = TOTAL / BSIZE;
    run_async([this] {
      std::for_each(
        boost::make_counting_iterator(0u),
        boost::make_counting_iterator(WRITE_STREAMS),
        [&](auto idx) {
          for (unsigned i = idx; i < BLOCKS; i += WRITE_STREAMS) {
            while (true) {
              auto t = create_transaction();
              auto extent = alloc_extent(
                t,
                i * BSIZE,
                BSIZE);
              ASSERT_EQ(i * BSIZE, extent->get_laddr());
              if (try_submit_transaction(std::move(t)))
                break;
            }
          }
        });

      int writes = 0;
      unsigned failures = 0;
      seastar::parallel_for_each(
        boost::make_counting_iterator(0u),
        boost::make_counting_iterator(WRITE_STREAMS),
        [&](auto) {
          return seastar::async([&] {
            while (writes < 300) {
              auto t = create_transaction();
              auto ext = try_get_extent(
                t,
                get_random_laddr(BSIZE, TOTAL),
                BSIZE);
              if (!ext){
                failures++;
                continue;
              }
              auto mut = mutate_extent(t, ext);
              auto success = try_submit_transaction(std::move(t));
              writes += success;
              failures += !success;
            }
          });
        }).get0();
      replay();
      logger().info("random_writes_concurrent: checking");
      check();
      logger().info(
        "random_writes_concurrent: {} suceeded, {} failed",
        writes,
        failures
      );
    });
  }

  void test_evict() {
    // only support segmented backend currently
    ASSERT_EQ(epm->get_main_backend_type(), backend_type_t::SEGMENTED);
    ASSERT_TRUE(epm->background_process.has_cold_tier());
    constexpr size_t device_size =
      segment_manager::DEFAULT_TEST_EPHEMERAL.size;
    constexpr size_t block_size =
      segment_manager::DEFAULT_TEST_EPHEMERAL.block_size;
    constexpr size_t segment_size =
      segment_manager::DEFAULT_TEST_EPHEMERAL.segment_size;
    ASSERT_GE(segment_size, block_size * 20);

    run_async([this] {
      // indicates there is no available segments to reclaim
      double stop_ratio = (double)segment_size / (double)device_size / 2;
      // 1 segment
      double default_ratio = stop_ratio * 2;
      // 1.25 segment
      double fast_ratio = stop_ratio * 2.5;

      epm->background_process
        .eviction_state
        .init(stop_ratio, default_ratio, fast_ratio);

      // these variables are described in
      // EPM::BackgroundProcess::eviction_state_t::maybe_update_eviction_mode
      size_t ratio_A_size = segment_size / 2 - block_size * 10;
      size_t ratio_B_size = segment_size / 2 + block_size * 10;
      size_t ratio_C_size = segment_size + block_size;
      size_t ratio_D_size = segment_size * 1.25 + block_size;

      auto run_until = [this](size_t size) -> seastar::future<> {
        return seastar::repeat([this, size] {
          size_t current_size = epm->background_process
                                    .main_cleaner->get_stat().data_stored;
          if (current_size >= size) {
            return seastar::futurize_invoke([] {
              return seastar::stop_iteration::yes;
            });
          } else {
            int num = (size - current_size) / block_size;
            return seastar::do_for_each(
              boost::make_counting_iterator(0),
              boost::make_counting_iterator(num),
              [this](auto) {
	        // don't start background process to test the behavior
                // of generation changes during alloc new extents
                return allocate_sequentially(block_size, 1, false);
              }).then([] {
                return seastar::stop_iteration::no;
              });
          }
        });
      };

      std::vector<extent_types_t> all_extent_types{
        extent_types_t::ROOT,
        extent_types_t::LADDR_INTERNAL,
        extent_types_t::LADDR_LEAF,
        extent_types_t::OMAP_INNER,
        extent_types_t::OMAP_LEAF,
        extent_types_t::ONODE_BLOCK_STAGED,
        extent_types_t::COLL_BLOCK,
        extent_types_t::OBJECT_DATA_BLOCK,
        extent_types_t::RETIRED_PLACEHOLDER,
        extent_types_t::ALLOC_INFO,
        extent_types_t::JOURNAL_TAIL,
        extent_types_t::TEST_BLOCK,
        extent_types_t::TEST_BLOCK_PHYSICAL,
        extent_types_t::BACKREF_INTERNAL,
        extent_types_t::BACKREF_LEAF
      };

      std::vector<rewrite_gen_t> all_generations;
      for (auto i = INIT_GENERATION; i < REWRITE_GENERATIONS; i++) {
        all_generations.push_back(i);
      }

      // input target-generation -> expected generation after the adjustment
      using generation_mapping_t = std::map<rewrite_gen_t, rewrite_gen_t>;
      std::map<extent_types_t, generation_mapping_t> expected_generations;

      // this loop should be consistent with EPM::adjust_generation
      for (auto t : all_extent_types) {
        expected_generations[t] = {};
        if (!is_logical_type(t)) {
          for (auto gen : all_generations) {
            expected_generations[t][gen] = INLINE_GENERATION;
          }
        } else {
	  if (get_extent_category(t) == data_category_t::METADATA) {
	    expected_generations[t][INIT_GENERATION] = INLINE_GENERATION;
	  } else {
	    expected_generations[t][INIT_GENERATION] = OOL_GENERATION;
	  }

          for (auto i = INIT_GENERATION + 1; i < REWRITE_GENERATIONS; i++) {
	    expected_generations[t][i] = i;
          }
        }
      }

      auto update_data_gen_mapping = [&](std::function<rewrite_gen_t(rewrite_gen_t)> func) {
        for (auto t : all_extent_types) {
          if (!is_logical_type(t)) {
            continue;
          }
          for (auto i = INIT_GENERATION + 1; i < REWRITE_GENERATIONS; i++) {
            expected_generations[t][i] = func(i);
          }
        }
        // since background process didn't start in allocate_sequentially
        // we update eviction mode manually.
        epm->background_process.maybe_update_eviction_mode();
      };

      auto test_gen = [&](const char *caller) {
        for (auto t : all_extent_types) {
          for (auto gen : all_generations) {
            auto epm_gen = epm->adjust_generation(
              get_extent_category(t),
              t,
              placement_hint_t::HOT,
              gen);
            if (expected_generations[t][gen] != epm_gen) {
              logger().error("caller: {}, extent type: {}, input generation: {}, "
			     "expected generation : {}, adjust result from EPM: {}",
			     caller, t, gen, expected_generations[t][gen], epm_gen);
            }
            EXPECT_EQ(expected_generations[t][gen], epm_gen);
          }
        }
      };

      // verify that no data should go to the cold tier
      update_data_gen_mapping([](rewrite_gen_t gen) -> rewrite_gen_t {
        if (gen == MIN_COLD_GENERATION) {
          return MIN_COLD_GENERATION - 1;
        } else {
          return gen;
        }
      });
      test_gen("init");

      run_until(ratio_A_size).get();
      EXPECT_TRUE(epm->background_process.eviction_state.is_stop_mode());
      test_gen("exceed ratio A");
      epm->run_background_work_until_halt().get();

      run_until(ratio_B_size).get();
      EXPECT_TRUE(epm->background_process.eviction_state.is_stop_mode());
      test_gen("exceed ratio B");
      epm->run_background_work_until_halt().get();

      // verify that data may go to the cold tier
      run_until(ratio_C_size).get();
      update_data_gen_mapping([](rewrite_gen_t gen) { return gen; });
      EXPECT_TRUE(epm->background_process.eviction_state.is_default_mode());
      test_gen("exceed ratio C");
      epm->run_background_work_until_halt().get();

      // verify that data must go to the cold tier
      run_until(ratio_D_size).get();
      update_data_gen_mapping([](rewrite_gen_t gen) {
        if (gen >= MIN_REWRITE_GENERATION && gen < MIN_COLD_GENERATION) {
          return MIN_COLD_GENERATION;
        } else {
          return gen;
        }
      });
      EXPECT_TRUE(epm->background_process.eviction_state.is_fast_mode());
      test_gen("exceed ratio D");

      auto main_size = epm->background_process.main_cleaner->get_stat().data_stored;
      auto cold_size = epm->background_process.cold_cleaner->get_stat().data_stored;
      EXPECT_EQ(cold_size, 0);
      epm->run_background_work_until_halt().get();
      auto new_main_size = epm->background_process.main_cleaner->get_stat().data_stored;
      auto new_cold_size = epm->background_process.cold_cleaner->get_stat().data_stored;
      EXPECT_GE(main_size, new_main_size);
      EXPECT_NE(new_cold_size, 0);

      update_data_gen_mapping([](rewrite_gen_t gen) { return gen; });
      EXPECT_TRUE(epm->background_process.eviction_state.is_default_mode());
      test_gen("finish evict");
    });
  }

  using remap_entry = TransactionManager::remap_entry;
  LBAMappingRef remap_pin(
    test_transaction_t &t,
    LBAMappingRef &&opin,
    extent_len_t new_offset,
    extent_len_t new_len) {
    if (t.t->is_conflicted()) {
      return nullptr;
    }
    auto o_laddr = opin->get_key();
    auto data_laddr = opin->is_indirect()
      ? opin->get_intermediate_base()
      : o_laddr;
    auto pin = with_trans_intr(*(t.t), [&](auto& trans) {
      return tm->remap_pin<TestBlock>(
        trans, std::move(opin), std::array{
          remap_entry(new_offset, new_len)}
      ).si_then([](auto ret) {
        return std::move(ret[0]);
      });
    }).handle_error(crimson::ct_error::eagain::handle([] {
      LBAMappingRef t = nullptr;
      return t;
    }), crimson::ct_error::pass_further_all{}).unsafe_get0();
    if (t.t->is_conflicted()) {
      return nullptr;
    }
    if (opin->is_indirect()) {
      test_mappings.inc_ref(data_laddr, t.mapping_delta);
    } else {
      test_mappings.dec_ref(data_laddr, t.mapping_delta);
      EXPECT_FALSE(test_mappings.contains(data_laddr, t.mapping_delta));
    }
    EXPECT_TRUE(pin);
    EXPECT_EQ(pin->get_length(), new_len);
    EXPECT_EQ(pin->get_key(), o_laddr + new_offset);

    auto extent = try_read_pin(t, pin->duplicate());
    if (extent) {
      if (!pin->is_indirect()) {
	test_mappings.alloced(pin->get_key(), *extent, t.mapping_delta);
	EXPECT_TRUE(extent->is_exist_clean());
      } else {
	EXPECT_TRUE(extent->is_stable_written());
      }
    } else {
      ceph_assert(t.t->is_conflicted());
      return nullptr;
    }
    return pin;
  }

  using _overwrite_pin_iertr = TransactionManager::get_pin_iertr;
  using _overwrite_pin_ret = _overwrite_pin_iertr::future<
    std::tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>>;
  _overwrite_pin_ret _overwrite_pin(
    Transaction &t,
    LBAMappingRef &&opin,
    extent_len_t new_offset,
    extent_len_t new_len,
    ceph::bufferlist &bl) {
    auto o_laddr = opin->get_key();
    auto o_len = opin->get_length();
    if (new_offset != 0 && o_len != new_offset + new_len) {
      return tm->remap_pin<TestBlock, 2>(
        t,
        std::move(opin),
        std::array{
          remap_entry(
            0,
            new_offset),
          remap_entry(
            new_offset + new_len,
            o_len - new_offset - new_len)
        }
      ).si_then([this, new_offset, new_len, o_laddr, &t, &bl](auto ret) {
        return tm->alloc_data_extents<TestBlock>(t, o_laddr + new_offset, new_len
        ).si_then([this, ret = std::move(ret), new_len,
                   new_offset, o_laddr, &t, &bl](auto extents) mutable {
	  assert(extents.size() == 1);
	  auto ext = extents.front();
          ceph_assert(ret.size() == 2);
          auto iter = bl.cbegin();
          iter.copy(new_len, ext->get_bptr().c_str());
          auto r_laddr = o_laddr + new_offset + new_len;
          // old pins expired after alloc new extent, need to get it.
          return tm->get_pin(t, o_laddr
          ).si_then([this, &t, ext = std::move(ext), r_laddr](auto lpin) mutable {
            return tm->get_pin(t, r_laddr
            ).si_then([lpin = std::move(lpin), ext = std::move(ext)]
            (auto rpin) mutable {
              return _overwrite_pin_iertr::make_ready_future<
                std::tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>>(
                  std::make_tuple(
                    std::move(lpin), std::move(ext), std::move(rpin)));
            });
          });
        }).handle_error_interruptible(
	  crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
	  crimson::ct_error::pass_further_all{}
	);
      });
    } else if (new_offset == 0 && o_len != new_offset + new_len) {
      return tm->remap_pin<TestBlock, 1>(
        t,
        std::move(opin),
        std::array{
          remap_entry(
            new_offset + new_len,
            o_len - new_offset - new_len)
        }
      ).si_then([this, new_offset, new_len, o_laddr, &t, &bl](auto ret) {
        return tm->alloc_data_extents<TestBlock>(t, o_laddr + new_offset, new_len
        ).si_then([this, ret = std::move(ret), new_offset, new_len,
                   o_laddr, &t, &bl](auto extents) mutable {
	  assert(extents.size() == 1);
	  auto ext = extents.front();
          ceph_assert(ret.size() == 1);
          auto iter = bl.cbegin();
          iter.copy(new_len, ext->get_bptr().c_str());
          auto r_laddr = o_laddr + new_offset + new_len;
          return tm->get_pin(t, r_laddr
          ).si_then([ext = std::move(ext)](auto rpin) mutable {
            return _overwrite_pin_iertr::make_ready_future<
              std::tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>>(
                std::make_tuple(
                  nullptr, std::move(ext), std::move(rpin)));
          });
        });
      }).handle_error_interruptible(
	crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
	crimson::ct_error::pass_further_all{}
      );
    } else if (new_offset != 0 && o_len == new_offset + new_len) {
      return tm->remap_pin<TestBlock, 1>(
        t,
        std::move(opin),
        std::array{
          remap_entry(
            0,
            new_offset)
        }
      ).si_then([this, new_offset, new_len, o_laddr, &t, &bl](auto ret) {
        return tm->alloc_data_extents<TestBlock>(t, o_laddr + new_offset, new_len
        ).si_then([this, ret = std::move(ret), new_len, o_laddr, &t, &bl]
          (auto extents) mutable {
	  assert(extents.size() == 1);
	  auto ext = extents.front();
          ceph_assert(ret.size() == 1);
          auto iter = bl.cbegin();
          iter.copy(new_len, ext->get_bptr().c_str());
          return tm->get_pin(t, o_laddr
          ).si_then([ext = std::move(ext)](auto lpin) mutable {
            return _overwrite_pin_iertr::make_ready_future<
              std::tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>>(
                std::make_tuple(
                  std::move(lpin), std::move(ext), nullptr));
          });
        });
      }).handle_error_interruptible(
	crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
	crimson::ct_error::pass_further_all{}
      );
    } else {
      ceph_abort("impossible");
        return _overwrite_pin_iertr::make_ready_future<
          std::tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>>(
            std::make_tuple(nullptr, nullptr, nullptr));
    }
  }

  using overwrite_pin_ret = std::tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>;
  overwrite_pin_ret overwrite_pin(
    test_transaction_t &t,
    LBAMappingRef &&opin,
    extent_len_t new_offset,
    extent_len_t new_len,
    ceph::bufferlist &bl) {
    if (t.t->is_conflicted()) {
      return std::make_tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>(
        nullptr, nullptr, nullptr);
    }
    auto o_laddr = opin->get_key();
    auto o_paddr = opin->get_val();
    auto o_len = opin->get_length();
    auto res = with_trans_intr(*(t.t), [&](auto& trans) {
      return _overwrite_pin(
        trans, std::move(opin), new_offset, new_len, bl);
    }).handle_error(crimson::ct_error::eagain::handle([] {
      return std::make_tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>(
        nullptr, nullptr, nullptr);
    }), crimson::ct_error::pass_further_all{}).unsafe_get0();
    if (t.t->is_conflicted()) {
      return std::make_tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>(
        nullptr, nullptr, nullptr);
    }
    test_mappings.dec_ref(o_laddr, t.mapping_delta);
    EXPECT_FALSE(test_mappings.contains(o_laddr, t.mapping_delta));
    auto &[lpin, ext, rpin] = res;

    EXPECT_TRUE(ext);
    EXPECT_TRUE(lpin || rpin);
    EXPECT_TRUE(o_len > ext->get_length());
    if (lpin) {
      EXPECT_EQ(lpin->get_key(), o_laddr);
      EXPECT_EQ(lpin->get_val(), o_paddr);
      EXPECT_EQ(lpin->get_length(), new_offset);
      auto lext = try_read_pin(t, lpin->duplicate());
      if (lext) {
        test_mappings.alloced(lpin->get_key(), *lext, t.mapping_delta);
        EXPECT_TRUE(lext->is_exist_clean());
      } else {
        ceph_assert(t.t->is_conflicted());
        return std::make_tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>(
          nullptr, nullptr, nullptr);
      }
    }
    EXPECT_EQ(ext->get_laddr(), o_laddr + new_offset);
    EXPECT_EQ(ext->get_length(), new_len);
    test_mappings.alloced(ext->get_laddr(), *ext, t.mapping_delta);
    if (rpin) {
      EXPECT_EQ(rpin->get_key(), o_laddr + new_offset + new_len);
      EXPECT_EQ(rpin->get_val(), o_paddr.add_offset(new_offset)
        .add_offset(new_len));
      EXPECT_EQ(rpin->get_length(), o_len - new_offset - new_len);
      auto rext = try_read_pin(t, rpin->duplicate());
      if (rext) {
        test_mappings.alloced(rpin->get_key(), *rext, t.mapping_delta);
        EXPECT_TRUE(rext->is_exist_clean());
      } else {
        ceph_assert(t.t->is_conflicted());
        return std::make_tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>(
          nullptr, nullptr, nullptr);
      }
    }
    return std::make_tuple<LBAMappingRef, TestBlockRef, LBAMappingRef>(
      std::move(lpin), std::move(ext), std::move(rpin));
  }

  void test_remap_pin() {
    run_async([this] {
      constexpr size_t l_offset = 32 << 10;
      constexpr size_t l_len = 32 << 10;
      constexpr size_t r_offset = 64 << 10;
      constexpr size_t r_len = 32 << 10;
      {
	auto t = create_transaction();
	auto lext = alloc_extent(t, l_offset, l_len);
        lext->set_contents('l', 0, 16 << 10);
        auto rext = alloc_extent(t, r_offset, r_len);
        rext->set_contents('r', 16 << 10, 16 << 10);
	submit_transaction(std::move(t));
      }
      {
	auto t = create_transaction();
        auto lpin = get_pin(t, l_offset);
        auto rpin = get_pin(t, r_offset);
        //split left
        auto pin1 = remap_pin(t, std::move(lpin), 0, 16 << 10);
        ASSERT_TRUE(pin1);
        auto pin2 = remap_pin(t, std::move(pin1), 0, 8 << 10);  
        ASSERT_TRUE(pin2);
        auto pin3 = remap_pin(t, std::move(pin2), 0, 4 << 10);
        ASSERT_TRUE(pin3);
        auto lext = read_pin(t, std::move(pin3));
        EXPECT_EQ('l', lext->get_bptr().c_str()[0]);
	auto mlext = mutate_extent(t, lext);
	ASSERT_TRUE(mlext->is_exist_mutation_pending());
	ASSERT_TRUE(mlext.get() == lext.get());

        //split right
        auto pin4 = remap_pin(t, std::move(rpin), 16 << 10, 16 << 10);
        ASSERT_TRUE(pin4);
        auto pin5 = remap_pin(t, std::move(pin4), 8 << 10, 8 << 10);  
        ASSERT_TRUE(pin5);
        auto pin6 = remap_pin(t, std::move(pin5), 4 << 10, 4 << 10);
        ASSERT_TRUE(pin6);
        auto rext = read_pin(t, std::move(pin6));
        EXPECT_EQ('r', rext->get_bptr().c_str()[0]);
	auto mrext = mutate_extent(t, rext);
	ASSERT_TRUE(mrext->is_exist_mutation_pending());
	ASSERT_TRUE(mrext.get() == rext.get());

	submit_transaction(std::move(t));
	check();
      }
      replay();
      check();
    });
  }

  void test_clone_and_remap_pin() {
    run_async([this] {
      constexpr size_t l_offset = 32 << 10;
      constexpr size_t l_len = 32 << 10;
      constexpr size_t r_offset = 64 << 10;
      constexpr size_t r_len = 32 << 10;
      constexpr size_t l_clone_offset = 96 << 10;
      constexpr size_t r_clone_offset = 128 << 10;
      {
	auto t = create_transaction();
	auto lext = alloc_extent(t, l_offset, l_len);
        lext->set_contents('l', 0, 16 << 10);
	test_mappings.update(l_offset, lext->get_desc(), t.mapping_delta);
        auto rext = alloc_extent(t, r_offset, r_len);
        rext->set_contents('r', 16 << 10, 16 << 10);
	test_mappings.update(r_offset, rext->get_desc(), t.mapping_delta);
	submit_transaction(std::move(t));
      }
      {
	auto t = create_transaction();
        auto lpin = get_pin(t, l_offset);
        auto rpin = get_pin(t, r_offset);
	auto l_clone_pin = clone_pin(t, l_clone_offset, *lpin);
	auto r_clone_pin = clone_pin(t, r_clone_offset, *rpin);
        //split left
        auto pin1 = remap_pin(t, std::move(l_clone_pin), 0, 16 << 10);
        ASSERT_TRUE(pin1);
        auto pin2 = remap_pin(t, std::move(pin1), 0, 8 << 10);  
        ASSERT_TRUE(pin2);
        auto pin3 = remap_pin(t, std::move(pin2), 0, 4 << 10);
        ASSERT_TRUE(pin3);
        auto lext = read_pin(t, std::move(pin3));
        EXPECT_EQ('l', lext->get_bptr().c_str()[0]);

        //split right
        auto pin4 = remap_pin(t, std::move(r_clone_pin), 16 << 10, 16 << 10);
        ASSERT_TRUE(pin4);
        auto pin5 = remap_pin(t, std::move(pin4), 8 << 10, 8 << 10);  
        ASSERT_TRUE(pin5);
        auto pin6 = remap_pin(t, std::move(pin5), 4 << 10, 4 << 10);
        ASSERT_TRUE(pin6);
	auto int_offset = pin6->get_intermediate_offset();
        auto rext = read_pin(t, std::move(pin6));
        EXPECT_EQ('r', rext->get_bptr().c_str()[int_offset]);

	submit_transaction(std::move(t));
	check();
      }
      replay();
      check();
    });
  }

  void test_overwrite_pin() {
    run_async([this] {
      constexpr size_t m_offset = 8 << 10;
      constexpr size_t m_len = 56 << 10;
      constexpr size_t l_offset = 64 << 10;
      constexpr size_t l_len = 64 << 10;
      constexpr size_t r_offset = 128 << 10;
      constexpr size_t r_len = 64 << 10;
      {
	auto t = create_transaction();
	auto m_ext = alloc_extent(t, m_offset, m_len);
        m_ext->set_contents('a', 0 << 10, 8 << 10);
        m_ext->set_contents('b', 16 << 10, 4 << 10);
        m_ext->set_contents('c', 36 << 10, 4 << 10);
        m_ext->set_contents('d', 52 << 10, 4 << 10);

        auto l_ext = alloc_extent(t, l_offset, l_len);
        auto r_ext = alloc_extent(t, r_offset, r_len);
	submit_transaction(std::move(t));
      }
      {
	auto t = create_transaction();
        auto mpin = get_pin(t, m_offset);
        auto lpin = get_pin(t, l_offset);
        auto rpin = get_pin(t, r_offset);

        bufferlist mbl1, mbl2, mbl3;
        mbl1.append(ceph::bufferptr(ceph::buffer::create(8 << 10, 0)));
        mbl2.append(ceph::bufferptr(ceph::buffer::create(16 << 10, 0)));
        mbl3.append(ceph::bufferptr(ceph::buffer::create(12 << 10, 0)));
        auto [mlp1, mext1, mrp1] = overwrite_pin(
          t, std::move(mpin), 8 << 10 , 8 << 10, mbl1);
        auto [mlp2, mext2, mrp2] = overwrite_pin(
          t, std::move(mrp1), 4 << 10 , 16 << 10, mbl2);
        auto [mlpin3, me3, mrpin3] = overwrite_pin(
          t, std::move(mrp2), 4 << 10 , 12 << 10, mbl3);
        auto mlext1 = get_extent(t, mlp1->get_key(), mlp1->get_length());
        auto mlext2 = get_extent(t, mlp2->get_key(), mlp2->get_length());
        auto mlext3 = get_extent(t, mlpin3->get_key(), mlpin3->get_length());
        auto mrext3 = get_extent(t, mrpin3->get_key(), mrpin3->get_length());
        EXPECT_EQ('a', mlext1->get_bptr().c_str()[0]);
        EXPECT_EQ('b', mlext2->get_bptr().c_str()[0]);
        EXPECT_EQ('c', mlext3->get_bptr().c_str()[0]);
        EXPECT_EQ('d', mrext3->get_bptr().c_str()[0]);
        auto mutate_mlext1 = mutate_extent(t, mlext1);
        auto mutate_mlext2 = mutate_extent(t, mlext2);
        auto mutate_mlext3 = mutate_extent(t, mlext3);
	auto mutate_mrext3 = mutate_extent(t, mrext3);
        ASSERT_TRUE(mutate_mlext1->is_exist_mutation_pending());
        ASSERT_TRUE(mutate_mlext2->is_exist_mutation_pending());
	ASSERT_TRUE(mutate_mlext3->is_exist_mutation_pending());
        ASSERT_TRUE(mutate_mrext3->is_exist_mutation_pending());
        ASSERT_TRUE(mutate_mlext1.get() == mlext1.get());
        ASSERT_TRUE(mutate_mlext2.get() == mlext2.get());
	ASSERT_TRUE(mutate_mlext3.get() == mlext3.get());
        ASSERT_TRUE(mutate_mrext3.get() == mrext3.get());

        bufferlist lbl1, rbl1;
        lbl1.append(ceph::bufferptr(ceph::buffer::create(32 << 10, 0)));
        auto [llp1, lext1, lrp1] = overwrite_pin(
          t, std::move(lpin), 0 , 32 << 10, lbl1);
        EXPECT_FALSE(llp1);
        EXPECT_TRUE(lrp1);
        EXPECT_TRUE(lext1);

        rbl1.append(ceph::bufferptr(ceph::buffer::create(32 << 10, 0)));
        auto [rlp1, rext1, rrp1] = overwrite_pin(
          t, std::move(rpin), 32 << 10 , 32 << 10, rbl1);
        EXPECT_TRUE(rlp1);
        EXPECT_TRUE(rext1);
        EXPECT_FALSE(rrp1);

	submit_transaction(std::move(t));
	check();
      }
      replay();
      check();
    });
  }

  void test_remap_pin_concurrent() {
    run_async([this] {
      constexpr unsigned REMAP_NUM = 32;
      constexpr size_t offset = 0;
      constexpr size_t length = 256 << 10;
      {
	auto t = create_transaction();
	auto extent = alloc_extent(t, offset, length);
	ASSERT_EQ(length, extent->get_length());
	submit_transaction(std::move(t));
      }
      int success = 0;
      int early_exit = 0;
      int conflicted = 0;

      seastar::parallel_for_each(
        boost::make_counting_iterator(0u),
	boost::make_counting_iterator(REMAP_NUM),
	[&](auto) {
	  return seastar::async([&] {
	    uint32_t pieces = std::uniform_int_distribution<>(6, 31)(gen);
	    std::set<uint32_t> split_points;
	    for (uint32_t i = 0; i < pieces; i++) {
	      auto p = std::uniform_int_distribution<>(1, 256)(gen);
	      split_points.insert(p - p % 4);
	    }

	    auto t = create_transaction();
            auto pin0 = try_get_pin(t, offset);
	    if (!pin0 || pin0->get_length() != length) {
	      early_exit++;
	      return;
	    }

            auto last_pin = pin0->duplicate();
	    ASSERT_TRUE(!split_points.empty());
	    for (auto off : split_points) {
	      if (off == 0 || off >= 255) {
		continue;
	      }
              auto new_off = (off << 10) - last_pin->get_key();
              auto new_len = last_pin->get_length() - new_off;
              //always remap right extent at new split_point
	      auto pin = remap_pin(t, std::move(last_pin), new_off, new_len);
              if (!pin) {
		conflicted++;
		return;
	      }
              last_pin = pin->duplicate();
	    }
            auto last_ext = try_get_extent(t, last_pin->get_key());
            if (last_ext) {
	      auto last_ext1 = mutate_extent(t, last_ext);
	      ASSERT_TRUE(last_ext1->is_exist_mutation_pending());
            } else {
	      conflicted++;
	      return;
            }

	    if (try_submit_transaction(std::move(t))) {
	      success++;
	      logger().info("transaction {} submit the transction",
                static_cast<void*>(t.t.get()));
	    } else {
	      conflicted++;
	    }
	  });
	}).handle_exception([](std::exception_ptr e) {
	  logger().info("{}", e);
	}).get0();
      logger().info("test_remap_pin_concurrent: "
        "early_exit {} conflicted {} success {}",
        early_exit, conflicted, success);
      ASSERT_TRUE(success == 1);
      ASSERT_EQ(success + conflicted + early_exit, REMAP_NUM);
      replay();
      check();
    });
  }

  void test_overwrite_pin_concurrent() {
    run_async([this] {
      constexpr unsigned REMAP_NUM = 32;
      constexpr size_t offset = 0;
      constexpr size_t length = 256 << 10;
      {
	auto t = create_transaction();
	auto extent = alloc_extent(t, offset, length);
	ASSERT_EQ(length, extent->get_length());
	submit_transaction(std::move(t));
      }
      int success = 0;
      int early_exit = 0;
      int conflicted = 0;

      seastar::parallel_for_each(
        boost::make_counting_iterator(0u),
	boost::make_counting_iterator(REMAP_NUM),
	[&](auto) {
	  return seastar::async([&] {
	    uint32_t pieces = std::uniform_int_distribution<>(6, 31)(gen);
            if (pieces % 2 == 1) {
              pieces++;
            }
	    std::list<uint32_t> split_points;
	    for (uint32_t i = 0; i < pieces; i++) {
	      auto p = std::uniform_int_distribution<>(1, 120)(gen);
	      split_points.push_back(p - p % 4);
	    }
            split_points.sort();

	    auto t = create_transaction();
            auto pin0 = try_get_pin(t, offset);
	    if (!pin0 || pin0->get_length() != length) {
	      early_exit++;
	      return;
	    }

            auto empty_transaction = true;
            auto last_rpin = pin0->duplicate();
	    ASSERT_TRUE(!split_points.empty());
            while(!split_points.empty()) {
              // new overwrite area: start_off ~ end_off
              auto start_off = split_points.front();
              split_points.pop_front();
              auto end_off = split_points.front();
              split_points.pop_front();
              ASSERT_TRUE(start_off <= end_off);
              if (((end_off << 10) == pin0->get_key() + pin0->get_length())
                || (start_off == end_off)) {
                if (split_points.empty() && empty_transaction) {
                  early_exit++;
                  return;
                }
                continue;
              }
              empty_transaction = false;
              auto new_off = (start_off << 10) - last_rpin->get_key();
              auto new_len = (end_off - start_off) << 10;
              bufferlist bl;
              bl.append(ceph::bufferptr(ceph::buffer::create(new_len, 0)));
              auto [lpin, ext, rpin] = overwrite_pin(
                t, last_rpin->duplicate(), new_off, new_len, bl);
	      if (!ext) {
		conflicted++;
		return;
	      }
              // lpin is nullptr might not cause by confliction,
              // it might just not exist.
              if (lpin) {
                auto lext = try_get_extent(t, lpin->get_key());
                if (!lext) {
		  conflicted++;
		  return;
                }
                if (get_random_contents() % 2 == 0) {
		  auto lext1 = mutate_extent(t, lext);
		  ASSERT_TRUE(lext1->is_exist_mutation_pending());
	        }
              }
              ASSERT_TRUE(rpin);
              last_rpin = rpin->duplicate();
	    }
            auto last_rext = try_get_extent(t, last_rpin->get_key());
            if (!last_rext) {
	      conflicted++;
	      return;
            }
	    if (get_random_contents() % 2 == 0) {
              auto last_rext1 = mutate_extent(t, last_rext);
              ASSERT_TRUE(last_rext1->is_exist_mutation_pending());
	    }

	    if (try_submit_transaction(std::move(t))) {
	      success++;
	      logger().info("transaction {} submit the transction",
                static_cast<void*>(t.t.get()));
	    } else {
	      conflicted++;
	    }
	  });
	}).handle_exception([](std::exception_ptr e) {
	  logger().info("{}", e);
	}).get0();
      logger().info("test_overwrite_pin_concurrent: "
        "early_exit {} conflicted {} success {}",
        early_exit, conflicted, success);
      ASSERT_TRUE(success == 1 || early_exit == REMAP_NUM);
      ASSERT_EQ(success + conflicted + early_exit, REMAP_NUM);
      replay();
      check();
    });
  }
};

struct tm_single_device_test_t :
  public transaction_manager_test_t {

  tm_single_device_test_t() : transaction_manager_test_t(1, 0) {}
};

struct tm_multi_device_test_t :
  public transaction_manager_test_t {

  tm_multi_device_test_t() : transaction_manager_test_t(3, 0) {}
};

struct tm_multi_tier_device_test_t :
  public transaction_manager_test_t {

  tm_multi_tier_device_test_t() : transaction_manager_test_t(1, 2) {}
};

struct tm_random_block_device_test_t :
  public transaction_manager_test_t {

  tm_random_block_device_test_t() : transaction_manager_test_t(1, 0) {}
};

TEST_P(tm_random_block_device_test_t, scatter_allocation)
{
  run_async([this] {
    constexpr laddr_t ADDR = 0xFF * 4096;
    epm->prefill_fragmented_devices();
    auto t = create_transaction();
    for (int i = 0; i < 1991; i++) {
      auto extents = alloc_extents(t, ADDR + i * 16384, 16384, 'a');
      std::cout << "num of extents: " << extents.size() << std::endl;
    }
    alloc_extents_deemed_fail(t, ADDR + 1991 * 16384, 16384, 'a');
    check_mappings(t);
    check();
    submit_transaction(std::move(t));
    check();
  });
}

TEST_P(tm_single_device_test_t, basic)
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
      check();
      submit_transaction(std::move(t));
      check();
    }
  });
}

TEST_P(tm_single_device_test_t, mutate)
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
      check();
      submit_transaction(std::move(t));
      check();
    }
    ASSERT_TRUE(check_usage());
    replay();
    {
      auto t = create_transaction();
      auto ext = get_extent(
	t,
	ADDR,
	SIZE);
      auto mut = mutate_extent(t, ext);
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
    ASSERT_TRUE(check_usage());
    replay();
    check();
  });
}

TEST_P(tm_single_device_test_t, allocate_lba_conflict)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    constexpr laddr_t ADDR = 0xFF * SIZE;
    constexpr laddr_t ADDR2 = 0xFE * SIZE;
    auto t = create_transaction();
    auto t2 = create_transaction();

    // These should conflict as they should both modify the lba root
    auto extent = alloc_extent(
      t,
      ADDR,
      SIZE,
      'a');
    ASSERT_EQ(ADDR, extent->get_laddr());
    check_mappings(t);
    check();

    auto extent2 = alloc_extent(
      t2,
      ADDR2,
      SIZE,
      'a');
    ASSERT_EQ(ADDR2, extent2->get_laddr());
    check_mappings(t2);
    extent2.reset();

    submit_transaction(std::move(t2));
    submit_transaction_expect_conflict(std::move(t));
  });
}

TEST_P(tm_single_device_test_t, mutate_lba_conflict)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    {
      auto t = create_transaction();
      for (unsigned i = 0; i < 300; ++i) {
	auto extent = alloc_extent(
	  t,
	  laddr_t(i * SIZE),
	  SIZE);
      }
      check_mappings(t);
      submit_transaction(std::move(t));
      check();
    }

    constexpr laddr_t ADDR = 150 * SIZE;
    {
      auto t = create_transaction();
      auto t2 = create_transaction();

      mutate_addr(t, ADDR, SIZE);
      mutate_addr(t2, ADDR, SIZE);

      submit_transaction(std::move(t));
      submit_transaction_expect_conflict(std::move(t2));
    }
    check();

    {
      auto t = create_transaction();
      mutate_addr(t, ADDR, SIZE);
      submit_transaction(std::move(t));
    }
    check();
  });
}

TEST_P(tm_single_device_test_t, concurrent_mutate_lba_no_conflict)
{
  constexpr laddr_t SIZE = 4096;
  constexpr size_t NUM = 500;
  constexpr laddr_t addr = 0;
  constexpr laddr_t addr2 = SIZE * (NUM - 1);
  run_async([this] {
    {
      auto t = create_transaction();
      for (unsigned i = 0; i < NUM; ++i) {
	auto extent = alloc_extent(
	  t,
	  laddr_t(i * SIZE),
	  SIZE);
      }
      submit_transaction(std::move(t));
    }

    {
      auto t = create_transaction();
      auto t2 = create_transaction();

      mutate_addr(t, addr, SIZE);
      mutate_addr(t2, addr2, SIZE);

      submit_transaction(std::move(t));
      submit_transaction(std::move(t2));
    }
    check();
  });
}

TEST_P(tm_single_device_test_t, create_remove_same_transaction)
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
      dec_ref(t, ADDR);
      check_mappings(t);

      extent = alloc_extent(
	t,
	ADDR,
	SIZE,
	'a');

      submit_transaction(std::move(t));
      check();
    }
    replay();
    check();
  });
}

TEST_P(tm_single_device_test_t, split_merge_read_same_transaction)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    {
      auto t = create_transaction();
      for (unsigned i = 0; i < 300; ++i) {
	auto extent = alloc_extent(
	  t,
	  laddr_t(i * SIZE),
	  SIZE);
      }
      check_mappings(t);
      submit_transaction(std::move(t));
      check();
    }
    {
      auto t = create_transaction();
      for (unsigned i = 0; i < 240; ++i) {
	dec_ref(
	  t,
	  laddr_t(i * SIZE));
      }
      check_mappings(t);
      submit_transaction(std::move(t));
      check();
    }
  });
}

TEST_P(tm_single_device_test_t, inc_dec_ref)
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
      check();
      submit_transaction(std::move(t));
      check();
    }
    replay();
    {
      auto t = create_transaction();
      inc_ref(t, ADDR);
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
    {
      auto t = create_transaction();
      dec_ref(t, ADDR);
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
    replay();
    {
      auto t = create_transaction();
      dec_ref(t, ADDR);
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
  });
}

TEST_P(tm_single_device_test_t, cause_lba_split)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    for (unsigned i = 0; i < 200; ++i) {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	i * SIZE,
	SIZE,
	(char)(i & 0xFF));
      ASSERT_EQ(i * SIZE, extent->get_laddr());
      submit_transaction(std::move(t));
    }
    check();
  });
}

TEST_P(tm_single_device_test_t, random_writes)
{
  constexpr size_t TOTAL = 4<<20;
  constexpr size_t BSIZE = 4<<10;
  constexpr size_t PADDING_SIZE = 256<<10;
  constexpr size_t BLOCKS = TOTAL / BSIZE;
  run_async([this] {
    for (unsigned i = 0; i < BLOCKS; ++i) {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	i * BSIZE,
	BSIZE);
      ASSERT_EQ(i * BSIZE, extent->get_laddr());
      submit_transaction(std::move(t));
    }

    for (unsigned i = 0; i < 4; ++i) {
      for (unsigned j = 0; j < 65; ++j) {
	auto t = create_transaction();
	for (unsigned k = 0; k < 2; ++k) {
	  auto ext = get_extent(
	    t,
	    get_random_laddr(BSIZE, TOTAL),
	    BSIZE);
	  auto mut = mutate_extent(t, ext);
	  // pad out transaction
	  auto padding = alloc_extent(
	    t,
	    TOTAL + (k * PADDING_SIZE),
	    PADDING_SIZE);
	  dec_ref(t, padding->get_laddr());
	}
	submit_transaction(std::move(t));
      }
      replay();
      logger().info("random_writes: {} checking", i);
      check();
      logger().info("random_writes: {} done replaying/checking", i);
    }
  });
}

TEST_P(tm_single_device_test_t, find_hole_assert_trigger)
{
  constexpr unsigned max = 10;
  constexpr size_t BSIZE = 4<<10;
  int num = 40;
  run([&, this] {
    return seastar::parallel_for_each(
      boost::make_counting_iterator(0u),
      boost::make_counting_iterator(max),
      [&, this](auto idx) {
        return allocate_sequentially(BSIZE, num);
    });
  });
}

TEST_P(tm_single_device_test_t, remap_lazy_read) 
{
  constexpr laddr_t offset = 0;
  constexpr size_t length = 256 << 10;
   run_async([this, offset] {
    {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	offset,
	length,
	'a');
      ASSERT_EQ(offset, extent->get_laddr());
      check_mappings(t);
      submit_transaction(std::move(t));
      check();
    }
    replay();
    {
      auto t = create_transaction();
      auto pin = get_pin(t, offset);
      auto rpin = remap_pin(t, std::move(pin), 0, 128 << 10);
      check_mappings(t);
      submit_transaction(std::move(t));
      check();
    }
    replay();
    {
      auto t = create_transaction();
      auto pin = get_pin(t, offset);
      bufferlist bl;
      bl.append(ceph::bufferptr(ceph::buffer::create(64 << 10, 0)));
      auto [lpin, ext, rpin] = overwrite_pin(
        t, std::move(pin), 4 << 10 , 64 << 10, bl);
      check_mappings(t);
      submit_transaction(std::move(t));
      check();
    }
    replay();
   });
}

TEST_P(tm_single_device_test_t, random_writes_concurrent)
{
  test_random_writes_concurrent();
}

TEST_P(tm_multi_device_test_t, random_writes_concurrent)
{
  test_random_writes_concurrent();
}

TEST_P(tm_multi_tier_device_test_t, evict)
{
  test_evict();
}

TEST_P(tm_single_device_test_t, parallel_extent_read)
{
  test_parallel_extent_read();
}

TEST_P(tm_single_device_test_t, test_remap_pin)
{
  test_remap_pin();
}

TEST_P(tm_single_device_test_t, test_clone_and_remap_pin)
{
  test_clone_and_remap_pin();
}

TEST_P(tm_single_device_test_t, test_overwrite_pin)
{
  test_overwrite_pin();
}

TEST_P(tm_single_device_test_t, test_remap_pin_concurrent)
{
  test_remap_pin_concurrent();
}

TEST_P(tm_single_device_test_t, test_overwrite_pin_concurrent)
{
  test_overwrite_pin_concurrent();
}

INSTANTIATE_TEST_SUITE_P(
  transaction_manager_test,
  tm_single_device_test_t,
  ::testing::Values (
    "segmented",
    "circularbounded"
  )
);

INSTANTIATE_TEST_SUITE_P(
  transaction_manager_test,
  tm_multi_device_test_t,
  ::testing::Values (
    "segmented"
  )
);

INSTANTIATE_TEST_SUITE_P(
  transaction_manager_test,
  tm_multi_tier_device_test_t,
  ::testing::Values (
    "segmented"
  )
);

INSTANTIATE_TEST_SUITE_P(
  transaction_manager_test,
  tm_random_block_device_test_t,
  ::testing::Values (
    "circularbounded"
  )
);
