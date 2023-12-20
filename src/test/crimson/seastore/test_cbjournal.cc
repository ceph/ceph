// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include <random>

#include "crimson/common/log.h"
#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/journal/circular_bounded_journal.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"
#include "crimson/os/seastore/seastore_types.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"
#include "crimson/os/seastore/random_block_manager/block_rb_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace crimson::os::seastore::journal;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

std::optional<record_t> decode_record(
  bufferlist& bl)
{
  record_t record;
  record_group_header_t r_header;
  auto bliter = bl.cbegin();
  decode(r_header, bliter);
  logger().debug(" decode_record mdlength {} records {}",
		  r_header.mdlength, r_header.records);
  device_id_t d_id = 1 << (std::numeric_limits<device_id_t>::digits - 1);

  auto del_infos = try_decode_deltas(r_header, bl,
    paddr_t::make_blk_paddr(d_id, 0));
  for (auto &iter : *del_infos) {
    for (auto r : iter.deltas) {
      record.deltas.push_back(r.second);
    }
  }
  auto ex_infos = try_decode_extent_infos(r_header, bl);
  auto bliter_ex = bl.cbegin();
  bliter_ex += r_header.mdlength;
  for (auto &iter: *ex_infos) {
    for (auto e : iter.extent_infos) {
      extent_t ex;
      auto bptr = bufferptr(ceph::buffer::create_page_aligned(e.len));
      logger().debug(" exten len {} remaining {} ", e.len, bliter_ex.get_remaining());
      bliter_ex.copy(e.len, bptr.c_str());
      ex.bl.append(bptr);
      record.extents.push_back(ex);
    }
  }
  return record;
}

struct entry_validator_t {
  bufferlist bl;
  int entries;
  record_t record;
  segment_nonce_t magic = 0;
  journal_seq_t seq;

  template <typename... T>
  entry_validator_t(T&&... entry) : record(std::forward<T>(entry)...) {}

  void validate(record_t read) {
    auto iter = read.extents.begin();
    for (auto &&block : record.extents) {
      ASSERT_EQ(
	iter->bl.length(),
	block.bl.length());
      ASSERT_EQ(
	iter->bl.begin().crc32c(iter->bl.length(), 1),
	block.bl.begin().crc32c(block.bl.length(), 1));
      ++iter;
    }
    auto iter_delta = read.deltas.begin();
    for (auto &&block : record.deltas) {
      ASSERT_EQ(
	iter_delta->bl.length(),
	block.bl.length());
      ASSERT_EQ(
	iter_delta->bl.begin().crc32c(iter_delta->bl.length(), 1),
	block.bl.begin().crc32c(block.bl.length(), 1));
      ++iter_delta;
    }
  }
  void validate(CircularBoundedJournal &cbj) {
    rbm_abs_addr offset = 0;
    auto cursor = scan_valid_records_cursor(seq);
    cbj.test_initialize_cursor(cursor);
    for (int i = 0; i < entries; i++) {
      paddr_t paddr = seq.offset.add_offset(offset);
      cursor.seq.offset = paddr;
      auto md = cbj.test_read_validate_record_metadata(
	cursor, magic).unsafe_get0();
      assert(md);
      auto& [header, md_bl] = *md;
      auto dbuf = cbj.read(
	paddr.add_offset(header.mdlength),
	header.dlength).unsafe_get0();

      bufferlist bl;
      bl.append(md_bl);
      bl.append(dbuf);
      auto record = decode_record(bl);
      validate(*record);
      offset += header.mdlength + header.dlength;
      cursor.last_committed = header.committed_to;
    }
  }

  rbm_abs_addr get_abs_addr() {
    return convert_paddr_to_abs_addr(seq.offset);
  }

  bool validate_delta(bufferlist bl) {
    for (auto &&block : record.deltas) {
      if (bl.begin().crc32c(bl.length(), 1) ==
	  block.bl.begin().crc32c(block.bl.length(), 1)) {
	return true;
      }
    }
    return false;
  }
};

struct cbjournal_test_t : public seastar_test_suite_t, JournalTrimmer
{
  std::vector<entry_validator_t> entries;
  std::unique_ptr<CircularBoundedJournal> cbj;
  random_block_device::EphemeralRBMDeviceRef device;

  std::default_random_engine generator;
  uint64_t block_size;
  WritePipeline pipeline;

  cbjournal_test_t() = default;

  /*
   * JournalTrimmer interfaces
   */
  journal_seq_t get_journal_head() const {
    return JOURNAL_SEQ_NULL;
  }

  journal_seq_t get_dirty_tail() const final {
    return JOURNAL_SEQ_NULL;
  }

  journal_seq_t get_alloc_tail() const final {
    return JOURNAL_SEQ_NULL;
  }

  void set_journal_head(journal_seq_t head) final {}

  void update_journal_tails(
    journal_seq_t dirty_tail,
    journal_seq_t alloc_tail) final {}

  bool try_reserve_inline_usage(std::size_t) final { return true; }

  void release_inline_usage(std::size_t) final {}

  std::size_t get_trim_size_per_cycle() const final {
    return 0;
  }

  auto submit_record(record_t&& record) {
    entries.push_back(record);
    OrderingHandle handle = get_dummy_ordering_handle();
    auto [addr, w_result] = cbj->submit_record(
	  std::move(record),
	  handle).unsafe_get0();
    entries.back().seq = w_result.start_seq;
    entries.back().entries = 1;
    entries.back().magic = cbj->get_cjs().get_cbj_header().magic;
    logger().debug("submit entry to addr {}", entries.back().seq);
    return convert_paddr_to_abs_addr(entries.back().seq.offset);
  }

  seastar::future<> tear_down_fut() final {
    return close();
  }

  extent_t generate_extent(size_t blocks) {
    std::uniform_int_distribution<char> distribution(
      std::numeric_limits<char>::min(),
      std::numeric_limits<char>::max()
    );
    char contents = distribution(generator);
    bufferlist bl;
    bl.append(buffer::ptr(buffer::create(blocks * block_size, contents)));
    return extent_t{extent_types_t::TEST_BLOCK, L_ADDR_NULL, bl};
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
	device->get_block_size(),
	1,
	0,
	segment_type_t::JOURNAL,
	bl
    };
  }

  auto replay_and_check() {
    for (auto &i : entries) {
      i.validate(*(cbj.get()));
    }
  }

  auto replay() {
    return cbj->replay(
      [this](const auto &offsets,
	     const auto &e,
	     auto &dirty_seq,
	     auto &alloc_seq,
	     auto last_modified) {
      bool found = false;
      for (auto &i : entries) {
	paddr_t base = offsets.write_result.start_seq.offset; 
	rbm_abs_addr addr = convert_paddr_to_abs_addr(base);
	if (addr == i.get_abs_addr()) {
	  logger().debug(" compare addr: {} and i.addr {} ", base, i.get_abs_addr());
	  found = i.validate_delta(e.bl);
	  break;
	}
      }
      assert(found == true);
      return Journal::replay_ertr::make_ready_future<
	std::pair<bool, CachedExtentRef>>(true, nullptr);
    });
  }

  auto mkfs() {
    device_config_t config = get_rbm_ephemeral_device_config(0, 1);
    return device->mkfs(config
    ).safe_then([this]() {
      return device->mount(
      ).safe_then([this]() {
	return cbj->open_for_mkfs(
	).safe_then([](auto q) {
	  return seastar::now();
	});
      });
    }).safe_then([this] {
      return cbj->close();
    });
  }
  auto open() {
    return cbj->open_for_mount(
    ).safe_then([](auto q) {
      return seastar::now();
    });
  }
  seastar::future<> close() {
    return cbj->close().handle_error(crimson::ct_error::assert_all{});
  }
  auto get_records_available_size() {
    return cbj->get_cjs().get_records_available_size();
  }
  auto get_records_total_size() {
    return cbj->get_cjs().get_records_total_size();
  }
  auto get_block_size() {
    return device->get_block_size();
  }
  auto get_written_to_rbm_addr() {
    return cbj->get_rbm_addr(cbj->get_cjs().get_written_to());
  }
  auto get_written_to() {
    return cbj->get_cjs().get_written_to();
  }
  auto get_journal_tail() {
    return cbj->get_dirty_tail();
  }
  auto get_records_used_size() {
    return cbj->get_cjs().get_records_used_size();
  }
  bool is_available_size(uint64_t size) {
    return cbj->get_cjs().is_available_size(size);
  }
  void update_journal_tail(rbm_abs_addr addr, uint32_t len) {
    paddr_t paddr =
      convert_abs_addr_to_paddr(
	  addr + len,
	  cbj->get_device_id());
    journal_seq_t seq = {0, paddr};
    cbj->update_journal_tail(
      seq,
      seq
    ).get0();
  }
  void set_written_to(journal_seq_t seq) {
    cbj->set_written_to(seq);
  }

  seastar::future<> set_up_fut() final {
    device = random_block_device::create_test_ephemeral(
     random_block_device::DEFAULT_TEST_CBJOURNAL_SIZE, 0);
    cbj.reset(new CircularBoundedJournal(*this, device.get(), std::string()));
    block_size = device->get_block_size();
    cbj->set_write_pipeline(&pipeline);
    return mkfs(
    ).safe_then([this] {
      return replay(
      ).safe_then([this] {
	return open(
	).safe_then([this] {
	  return replay();
	});
      });
    }).handle_error(crimson::ct_error::assert_all{});
  }
};

TEST_F(cbjournal_test_t, submit_one_record)
{
  run_async([this] {
    submit_record(
     record_t{
      { generate_extent(1), generate_extent(2) },
      { generate_delta(3), generate_delta(4) }
      });
    replay_and_check();
  });
}

TEST_F(cbjournal_test_t, submit_three_records)
{
  run_async([this] {
    submit_record(
     record_t{
      { generate_extent(1), generate_extent(2) },
      { generate_delta(3), generate_delta(4) }
      });
    submit_record(
     record_t{
      { generate_extent(8), generate_extent(9) },
      { generate_delta(20), generate_delta(21) }
      });
    submit_record(
     record_t{
      { generate_extent(5), generate_extent(6) },
      { generate_delta(200), generate_delta(210) }
      });
    replay_and_check();
  });
}

TEST_F(cbjournal_test_t, submit_full_records)
{
  run_async([this] {
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();

    submit_record(std::move(rec));
    while (is_available_size(record_total_size)) {
     submit_record(
       record_t {
	{ generate_extent(1), generate_extent(2) },
	{ generate_delta(20), generate_delta(21) }
	});
    }

    update_journal_tail(entries.back().get_abs_addr(), record_total_size);
    ASSERT_EQ(get_records_total_size(),
	     get_records_available_size());

    // will be appended at the begining of log
    submit_record(
     record_t {
      { generate_extent(1), generate_extent(2) },
      { generate_delta(20), generate_delta(21) }
      });

    while (is_available_size(record_total_size)) {
     submit_record(
       record_t {
	{ generate_extent(1), generate_extent(2) },
	{ generate_delta(20), generate_delta(21) }
	});
    }
    ASSERT_TRUE(record_total_size > get_records_available_size());
  });
}

TEST_F(cbjournal_test_t, boudary_check_verify)
{
  run_async([this] {
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();
    submit_record(std::move(rec));
    while (is_available_size(record_total_size)) {
     submit_record(
       record_t {
	{ generate_extent(1), generate_extent(2) },
	{ generate_delta(20), generate_delta(21) }
	});
    }

    uint64_t avail = get_records_available_size();
    // forward 2 recod size here because 1 block is reserved between head and tail
    update_journal_tail(entries.front().get_abs_addr(), record_total_size * 2);
    entries.erase(entries.begin());
    entries.erase(entries.begin());
    ASSERT_EQ(avail + (record_total_size * 2), get_records_available_size());
    avail = get_records_available_size();
    // will be appended at the begining of WAL
    submit_record(
     record_t {
      { generate_extent(1), generate_extent(2) },
      { generate_delta(20), generate_delta(21) }
      });
    ASSERT_TRUE(avail - record_total_size >= get_records_available_size());
    replay_and_check();
  });
}

TEST_F(cbjournal_test_t, update_header)
{
  run_async([this] {
    auto [header, _buf] = *(cbj->get_cjs().read_header().unsafe_get0());
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();
    submit_record(std::move(rec));

    update_journal_tail(entries.front().get_abs_addr(), record_total_size);
    cbj->get_cjs().write_header().unsafe_get0();
    auto [update_header, update_buf2] = *(cbj->get_cjs().read_header().unsafe_get0());
    cbj->close().unsafe_get0();
    replay().unsafe_get0();

    ASSERT_EQ(update_header.dirty_tail.offset, update_header.dirty_tail.offset);
  });
}

TEST_F(cbjournal_test_t, replay)
{
  run_async([this] {
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();
    submit_record(std::move(rec));
    while (is_available_size(record_total_size)) {
    submit_record(
      record_t {
       { generate_extent(1), generate_extent(2) },
       { generate_delta(20), generate_delta(21) }
       });
    }
    // will be appended at the begining of WAL
    uint64_t avail = get_records_available_size();
    update_journal_tail(entries.front().get_abs_addr(), record_total_size * 2);
    entries.erase(entries.begin());
    entries.erase(entries.begin());
    ASSERT_EQ(avail + (record_total_size * 2), get_records_available_size());
    avail = get_records_available_size();
    submit_record(
      record_t {
       { generate_extent(1), generate_extent(2) },
       { generate_delta(20), generate_delta(21) }
       });
    ASSERT_TRUE(avail - record_total_size >= get_records_available_size());
    cbj->close().unsafe_get0();
    replay().unsafe_get0();
  });
}

TEST_F(cbjournal_test_t, replay_after_reset)
{
  run_async([this] {
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();
    submit_record(std::move(rec));
    while (is_available_size(record_total_size)) {
    submit_record(
      record_t {
       { generate_extent(1), generate_extent(2) },
       { generate_delta(20), generate_delta(21) }
       });
    }
    auto old_written_to = get_written_to();
    auto old_used_size = get_records_used_size();
    set_written_to(
      journal_seq_t{0,
	convert_abs_addr_to_paddr(
	  cbj->get_records_start(),
	  cbj->get_device_id())});
    cbj->close().unsafe_get0();
    replay().unsafe_get0();
    ASSERT_EQ(old_written_to, get_written_to());
    ASSERT_EQ(old_used_size,
      get_records_used_size());
  });
}

TEST_F(cbjournal_test_t, multiple_submit_at_end)
{
  run_async([this] {
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();
    submit_record(std::move(rec));
    while (is_available_size(record_total_size)) {
    submit_record(
      record_t {
       { generate_extent(1), generate_extent(2) },
       { generate_delta(20), generate_delta(21) }
       });
    }
    update_journal_tail(entries.front().get_abs_addr(), record_total_size * 8);
    for (int i = 0; i < 8; i++) {
      entries.erase(entries.begin());
    }
    seastar::parallel_for_each(
      boost::make_counting_iterator(0u),
      boost::make_counting_iterator(4u),
      [&](auto) {
	return seastar::async([&] {
	  auto writes = 0;
	  while (writes < 2) {
	    record_t rec {
	     { generate_extent(1) },
	     { generate_delta(20) } };
	    submit_record(std::move(rec));
	    writes++;
	  }
	});
      }).get0();
    auto old_written_to = get_written_to();
    cbj->close().unsafe_get0();
    cbj->replay(
      [](const auto &offsets,
	     const auto &e,
	     auto &dirty_seq,
	     auto &alloc_seq,
	     auto last_modified) {
      return Journal::replay_ertr::make_ready_future<
	std::pair<bool, CachedExtentRef>>(true, nullptr);
    }).unsafe_get0();
    assert(get_written_to() == old_written_to);
  });
}
