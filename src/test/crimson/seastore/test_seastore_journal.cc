// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include <random>

#include "crimson/common/log.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/segment_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

struct record_validator_t {
  record_t record;
  paddr_t record_final_offset;

  template <typename... T>
  record_validator_t(T&&... record) : record(std::forward<T>(record)...) {}

  void validate(SegmentManager &manager) {
    paddr_t addr = make_record_relative_paddr(0);
    for (auto &&block : record.extents) {
      auto test = manager.read(
	record_final_offset.add_relative(addr),
	block.bl.length()).unsafe_get0();
      addr.offset += block.bl.length();
      bufferlist bl;
      bl.push_back(test);
      ASSERT_EQ(
	bl.length(),
	block.bl.length());
      ASSERT_EQ(
	bl.begin().crc32c(bl.length(), 1),
	block.bl.begin().crc32c(block.bl.length(), 1));
    }
  }

  auto get_replay_handler() {
    auto checker = [this, iter=record.deltas.begin()] (
      paddr_t base,
      const delta_info_t &di) mutable {
      EXPECT_EQ(base, record_final_offset);
      ceph_assert(iter != record.deltas.end());
      EXPECT_EQ(di, *iter++);
      EXPECT_EQ(base, record_final_offset);
      return iter != record.deltas.end();
    };
    if (record.deltas.size()) {
      return std::make_optional(std::move(checker));
    } else {
      return std::optional<decltype(checker)>();
    }
  }
};

struct journal_test_t : seastar_test_suite_t, JournalSegmentProvider {
  std::unique_ptr<SegmentManager> segment_manager;
  std::unique_ptr<Journal> journal;

  std::vector<record_validator_t> records;

  std::default_random_engine generator;

  const segment_off_t block_size;

  journal_test_t()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      block_size(segment_manager->get_block_size())
  {
  }

  segment_id_t next = 0;
  get_segment_ret get_segment() final {
    return get_segment_ret(
      get_segment_ertr::ready_future_marker{},
      next++);
  }

  void put_segment(segment_id_t segment) final {
    return;
  }

  seastar::future<> set_up_fut() final {
    journal.reset(new Journal(*segment_manager));
    journal->set_segment_provider(this);
    return segment_manager->init(
    ).safe_then([this] {
      return journal->open_for_write();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to mount");
      }));
  }

  template <typename T>
  auto replay(T &&f) {
    return journal->close(
    ).safe_then([this, f=std::move(f)]() mutable {
      journal.reset(new Journal(*segment_manager));
      journal->set_segment_provider(this);
      return journal->replay(std::forward<T>(std::move(f)));
    }).safe_then([this] {
      return journal->open_for_write();
    });
  }

  auto replay_and_check() {
    auto record_iter = records.begin();
    decltype(record_iter->get_replay_handler()) delta_checker = std::nullopt;
    auto advance = [this, &record_iter, &delta_checker] {
      ceph_assert(!delta_checker);
      while (record_iter != records.end()) {
	auto checker = record_iter->get_replay_handler();
	record_iter++;
	if (checker) {
	  delta_checker.emplace(std::move(*checker));
	  break;
	}
      }
    };
    advance();
    replay(
      [&advance,
       &delta_checker]
      (auto base, const auto &di) mutable {
	if (!delta_checker) {
	  EXPECT_FALSE("No Deltas Left");
	}
	if (!(*delta_checker)(base, di)) {
	  delta_checker = std::nullopt;
	  advance();
	}
	return Journal::replay_ertr::now();
      }).unsafe_get0();
    ASSERT_EQ(record_iter, records.end());
    for (auto &i : records) {
      i.validate(*segment_manager);
    }
  }

  template <typename... T>
  auto submit_record(T&&... _record) {
    auto record{std::forward<T>(_record)...};
    records.push_back(record);
    auto addr = journal->submit_record(std::move(record)).unsafe_get0();
    records.back().record_final_offset = addr;
    return addr;
  }

  seastar::future<> tear_down_fut() final {
    return seastar::now();
  }

  extent_t generate_extent(size_t blocks) {
    std::uniform_int_distribution<char> distribution(
      std::numeric_limits<char>::min(),
      std::numeric_limits<char>::max()
    );
    char contents = distribution(generator);
    bufferlist bl;
    bl.append(buffer::ptr(buffer::create(blocks * block_size, contents)));
    return extent_t{bl};
  }

  delta_info_t generate_delta(size_t bytes) {
    std::uniform_int_distribution<char> distribution(
      std::numeric_limits<char>::min(),
      std::numeric_limits<char>::max()
    );
    char contents = distribution(generator);
    bufferlist bl;
    bl.append(buffer::ptr(buffer::create(bytes, contents)));
    return delta_info_t{
      extent_types_t::TEST_BLOCK,
      paddr_t{},
      0, 0,
      block_size,
      1,
      bl
    };
  }
};

TEST_F(journal_test_t, replay_one_journal_segment)
{
 run_async([this] {
   submit_record(record_t{
     { generate_extent(1), generate_extent(2) },
     { generate_delta(23), generate_delta(30) }
     });
   replay_and_check();
 });
}

TEST_F(journal_test_t, replay_two_records)
{
 run_async([this] {
   submit_record(record_t{
     { generate_extent(1), generate_extent(2) },
     { generate_delta(23), generate_delta(30) }
     });
   submit_record(record_t{
     { generate_extent(4), generate_extent(1) },
     { generate_delta(23), generate_delta(400) }
     });
   replay_and_check();
 });
}

TEST_F(journal_test_t, replay_twice)
{
 run_async([this] {
   submit_record(record_t{
     { generate_extent(1), generate_extent(2) },
     { generate_delta(23), generate_delta(30) }
     });
   submit_record(record_t{
     { generate_extent(4), generate_extent(1) },
     { generate_delta(23), generate_delta(400) }
     });
   replay_and_check();
   submit_record(record_t{
     { generate_extent(2), generate_extent(5) },
     { generate_delta(230), generate_delta(40) }
     });
   replay_and_check();
 });
}

TEST_F(journal_test_t, roll_journal_and_replay)
{
 run_async([this] {
   paddr_t current = submit_record(
     record_t{
       { generate_extent(1), generate_extent(2) },
       { generate_delta(23), generate_delta(30) }
     });
   auto starting_segment = current.segment;
   unsigned so_far = 0;
   while (current.segment == starting_segment) {
     current = submit_record(record_t{
	 { generate_extent(512), generate_extent(512) },
	 { generate_delta(23), generate_delta(400) }
       });
     ++so_far;
     ASSERT_FALSE(so_far > 10);
   }
   replay_and_check();
 });
}
