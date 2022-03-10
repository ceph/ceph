// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include <random>

#include "crimson/common/log.h"
#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"

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
      addr.as_seg_paddr().set_segment_off(
	addr.as_seg_paddr().get_segment_off()
	+ block.bl.length());
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

struct journal_test_t : seastar_test_suite_t, SegmentProvider {
  segment_manager::EphemeralSegmentManagerRef segment_manager;
  WritePipeline pipeline;
  JournalRef journal;

  std::vector<record_validator_t> records;

  std::default_random_engine generator;

  seastore_off_t block_size;

  ExtentReaderRef scanner;

  segment_id_t next;

  journal_test_t() = default;

  seastar::lowres_system_clock::time_point get_last_modified(
    segment_id_t id) const final {
    return seastar::lowres_system_clock::time_point();
  }

  seastar::lowres_system_clock::time_point get_last_rewritten(
    segment_id_t id) const final {
    return seastar::lowres_system_clock::time_point();
  }

  void update_segment_avail_bytes(paddr_t offset) final {}

  segment_id_t get_segment(device_id_t id, segment_seq_t seq) final {
    auto ret = next;
    next = segment_id_t{
      next.device_id(),
      next.device_segment_id() + 1};
    return ret;
  }

  journal_seq_t get_journal_tail_target() const final { return journal_seq_t{}; }
  void update_journal_tail_committed(journal_seq_t paddr) final {}

  seastar::future<> set_up_fut() final {
    segment_manager = segment_manager::create_test_ephemeral();
    block_size = segment_manager->get_block_size();
    scanner.reset(new ExtentReader());
    next = segment_id_t(segment_manager->get_device_id(), 0);
    journal = journal::make_segmented(*segment_manager, *scanner, *this);
    journal->set_write_pipeline(&pipeline);
    scanner->add_segment_manager(segment_manager.get());
    return segment_manager->init(
    ).safe_then([this] {
      return journal->open_for_write();
    }).safe_then(
      [](auto){},
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to mount");
      }));
  }

  seastar::future<> tear_down_fut() final {
    return journal->close(
    ).safe_then([this] {
      segment_manager.reset();
      scanner.reset();
      journal.reset();
    }).handle_error(
      crimson::ct_error::all_same_way([](auto e) {
        ASSERT_FALSE("Unable to close");
      })
    );
  }

  template <typename T>
  auto replay(T &&f) {
    return journal->close(
    ).safe_then([this, f=std::move(f)]() mutable {
      journal = journal::make_segmented(
	*segment_manager, *scanner, *this);
      journal->set_write_pipeline(&pipeline);
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
      (const auto &offsets, const auto &di, auto t) mutable {
	if (!delta_checker) {
	  EXPECT_FALSE("No Deltas Left");
	}
	if (!(*delta_checker)(offsets.record_block_base, di)) {
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
    OrderingHandle handle = get_dummy_ordering_handle();
    auto [addr, _] = journal->submit_record(
      std::move(record),
      handle).unsafe_get0();
    records.back().record_final_offset = addr;
    return addr;
  }

  extent_t generate_extent(size_t blocks) {
    std::uniform_int_distribution<char> distribution(
      std::numeric_limits<char>::min(),
      std::numeric_limits<char>::max()
    );
    char contents = distribution(generator);
    bufferlist bl;
    bl.append(buffer::ptr(buffer::create(blocks * block_size, contents)));
    return extent_t{
      extent_types_t::TEST_BLOCK,
      L_ADDR_NULL,
      bl};
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
      L_ADDR_NULL,
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
   auto starting_segment = current.as_seg_paddr().get_segment_id();
   unsigned so_far = 0;
   while (current.as_seg_paddr().get_segment_id() == starting_segment) {
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
