// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include <random>

#include "crimson/common/log.h"
#include "crimson/os/seastore/async_cleaner.h"
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
      addr = addr.add_offset(block.bl.length());
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

struct journal_test_t : seastar_test_suite_t, SegmentProvider, JournalTrimmer {
  segment_manager::EphemeralSegmentManagerRef segment_manager;
  WritePipeline pipeline;
  JournalRef journal;

  std::vector<record_validator_t> records;

  std::default_random_engine generator;

  extent_len_t block_size;

  SegmentManagerGroupRef sms;

  segment_id_t next;

  std::map<segment_id_t, segment_seq_t> segment_seqs;
  std::map<segment_id_t, segment_type_t> segment_types;

  journal_seq_t dummy_tail;

  mutable segment_info_t tmp_info;

  journal_test_t() = default;

  /*
   * JournalTrimmer interfaces
   */
  journal_seq_t get_journal_head() const final { return dummy_tail; }

  void set_journal_head(journal_seq_t) final {}

  segment_seq_t get_journal_head_sequence() const final {
    return NULL_SEG_SEQ;
  }

  void set_journal_head_sequence(segment_seq_t) final {}

  journal_seq_t get_dirty_tail() const final { return dummy_tail; }

  journal_seq_t get_alloc_tail() const final { return dummy_tail; }

  void update_journal_tails(journal_seq_t, journal_seq_t) final {}

  bool try_reserve_inline_usage(std::size_t) final { return true; }

  void release_inline_usage(std::size_t) final {}

  std::size_t get_trim_size_per_cycle() const final {
    return 0;
  }

  /*
   * SegmentProvider interfaces
   */
  const segment_info_t& get_seg_info(segment_id_t id) const final {
    tmp_info = {};
    tmp_info.seq = segment_seqs.at(id);
    tmp_info.type = segment_types.at(id);
    return tmp_info;
  }

  segment_id_t allocate_segment(
    segment_seq_t seq,
    segment_type_t type,
    data_category_t,
    rewrite_gen_t
  ) final {
    auto ret = next;
    next = segment_id_t{
      segment_manager->get_device_id(),
      next.device_segment_id() + 1};
    segment_seqs[ret] = seq;
    segment_types[ret] = type;
    return ret;
  }

  void close_segment(segment_id_t) final {}

  void update_segment_avail_bytes(segment_type_t, paddr_t) final {}

  void update_modify_time(segment_id_t, sea_time_point, std::size_t) final {}

  SegmentManagerGroup* get_segment_manager_group() final { return sms.get(); }

  seastar::future<> set_up_fut() final {
    segment_manager = segment_manager::create_test_ephemeral();
    return segment_manager->init(
    ).safe_then([this] {
      return segment_manager->mkfs(
        segment_manager::get_ephemeral_device_config(0, 1, 0));
    }).safe_then([this] {
      block_size = segment_manager->get_block_size();
      sms.reset(new SegmentManagerGroup());
      next = segment_id_t(segment_manager->get_device_id(), 0);
      journal = journal::make_segmented(*this, *this);
      journal->set_write_pipeline(&pipeline);
      sms->add_segment_manager(segment_manager.get());
      return journal->open_for_mkfs();
    }).safe_then([this](auto) {
      dummy_tail = journal_seq_t{0,
        paddr_t::make_seg_paddr(segment_id_t(segment_manager->get_device_id(), 0), 0)};
    }, crimson::ct_error::assert_all{"Unable to mount"});
  }

  seastar::future<> tear_down_fut() final {
    return journal->close(
    ).safe_then([this] {
      segment_manager.reset();
      sms.reset();
      journal.reset();
    }).handle_error(
      crimson::ct_error::assert_all{"Unable to close"}
    );
  }

  template <typename T>
  auto replay(T &&f) {
    return journal->close(
    ).safe_then([this, f=std::move(f)]() mutable {
      journal = journal::make_segmented(*this, *this);
      journal->set_write_pipeline(&pipeline);
      return journal->replay(std::forward<T>(std::move(f)));
    }).safe_then([this] {
      return journal->open_for_mount();
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
      (const auto &offsets,
       const auto &di,
       const journal_seq_t &,
       const journal_seq_t &,
       auto t) mutable {
	if (!delta_checker) {
	  EXPECT_FALSE("No Deltas Left");
	}
	if (!(*delta_checker)(offsets.record_block_base, di)) {
	  delta_checker = std::nullopt;
	  advance();
	}
	return Journal::replay_ertr::make_ready_future<
	  std::pair<bool, CachedExtentRef>>(true, nullptr);
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
      MAX_SEG_SEQ,
      segment_type_t::NULL_SEG,
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
