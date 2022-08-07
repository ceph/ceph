// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include <random>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/journal/circular_bounded_journal.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace crimson::os::seastore::journal;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

constexpr uint64_t CBTEST_DEFAULT_TEST_SIZE = 1 << 20;
constexpr uint64_t CBTEST_DEFAULT_BLOCK_SIZE = 4096;


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
  journal_seq_t last_seq;
  record_t record;
  rbm_abs_addr addr = 0;

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
    for (int i = 0; i < entries; i++) {
      paddr_t paddr = convert_abs_addr_to_paddr(
	addr + offset,
	cbj.get_device_id());
      auto [header, buf] = *(cbj.read_record(paddr, NULL_SEG_SEQ).unsafe_get0());
      auto record = decode_record(buf);
      validate(*record);
      offset += header.mdlength + header.dlength;
    }
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

struct cbjournal_test_t : public seastar_test_suite_t
{
  std::vector<entry_validator_t> entries;
  std::unique_ptr<CircularBoundedJournal> cbj;
  random_block_device::RBMDevice *device;

  std::default_random_engine generator;
  uint64_t block_size;
  CircularBoundedJournal::mkfs_config_t config;
  WritePipeline pipeline;

  cbjournal_test_t() {
    device = new random_block_device::TestMemory(CBTEST_DEFAULT_TEST_SIZE + CBTEST_DEFAULT_BLOCK_SIZE);
    cbj.reset(new CircularBoundedJournal(device, std::string()));
    device_id_t d_id = 1 << (std::numeric_limits<device_id_t>::digits - 1);
    config.block_size = CBTEST_DEFAULT_BLOCK_SIZE;
    config.total_size = CBTEST_DEFAULT_TEST_SIZE;
    config.device_id = d_id;
    block_size = CBTEST_DEFAULT_BLOCK_SIZE;
    cbj->set_write_pipeline(&pipeline);
  }

  seastar::future<> set_up_fut() final {
    return seastar::now();
  }

  auto submit_record(record_t&& record) {
    entries.push_back(record);
    OrderingHandle handle = get_dummy_ordering_handle();
    auto [addr, w_result] = cbj->submit_record(
	  std::move(record),
	  handle).unsafe_get0();
    entries.back().addr = 
      convert_paddr_to_abs_addr(w_result.start_seq.offset);
    entries.back().entries = 1;
    logger().debug("submit entry to addr {}", entries.back().addr);
    return entries.back().addr;
  }

  seastar::future<> tear_down_fut() final {
    if (device) {
      delete device;
    }
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
	CBTEST_DEFAULT_BLOCK_SIZE,
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
    cbj->replay(
      [this](const auto &offsets,
             const auto &e,
             auto &dirty_seq,
             auto &alloc_seq,
             auto last_modified) {
      bool found = false;
      for (auto &i : entries) {
	paddr_t base = offsets.write_result.start_seq.offset; 
	rbm_abs_addr addr = convert_paddr_to_abs_addr(base);
	if (addr == i.addr) {
	  logger().debug(" compare addr: {} and i.addr {} ", base, i.addr);
	  found = i.validate_delta(e.bl);
	  break;
	}
      }
      assert(found == true);
      return Journal::replay_ertr::make_ready_future<bool>(true);
    }).unsafe_get0();
  }

  auto mkfs() {
    return cbj->mkfs(config).unsafe_get0();
  }
  void open() {
    cbj->open_device_read_header().unsafe_get0();
    cbj->open_for_mkfs().unsafe_get0();
  }
  auto get_available_size() {
    return cbj->get_available_size();
  }
  auto get_total_size() {
    return cbj->get_total_size();
  }
  auto get_block_size() {
    return device->get_block_size();
  }
  auto get_written_to() {
    return cbj->get_written_to();
  }
  auto get_journal_tail() {
    return cbj->get_journal_tail();
  }
  auto get_used_size() {
    return cbj->get_used_size();
  }
  void update_journal_tail(rbm_abs_addr addr, uint32_t len) {
    cbj->update_journal_tail(addr + len).unsafe_get0();
  }
  void set_written_to(rbm_abs_addr addr) {
    cbj->set_written_to(addr);
  }
};

TEST_F(cbjournal_test_t, submit_one_record)
{
  run_async([this] {
    mkfs();
    open();
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
    mkfs();
    open();
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
    mkfs();
    open();
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();

    submit_record(std::move(rec));
    while (record_total_size <= get_available_size()) {
     submit_record(
       record_t {
	{ generate_extent(1), generate_extent(2) },
	{ generate_delta(20), generate_delta(21) }
	});
    }

    uint64_t avail = get_available_size();
    update_journal_tail(entries.back().addr, record_total_size);
    ASSERT_EQ(get_total_size(),
	     get_available_size());

    // will be appended at the begining of log
    submit_record(
     record_t {
      { generate_extent(1), generate_extent(2) },
      { generate_delta(20), generate_delta(21) }
      });

    while (record_total_size <= get_available_size()) {
     submit_record(
       record_t {
	{ generate_extent(1), generate_extent(2) },
	{ generate_delta(20), generate_delta(21) }
	});
    }
    ASSERT_EQ(avail, get_available_size());
  });
}

TEST_F(cbjournal_test_t, boudary_check_verify)
{
  run_async([this] {
    mkfs();
    open();
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();
    submit_record(std::move(rec));
    while (record_total_size <= get_available_size()) {
     submit_record(
       record_t {
	{ generate_extent(1), generate_extent(2) },
	{ generate_delta(20), generate_delta(21) }
	});
    }

    uint64_t avail = get_available_size();
    update_journal_tail(entries.front().addr, record_total_size);
    entries.erase(entries.begin());
    ASSERT_EQ(avail + record_total_size, get_available_size());
    avail = get_available_size();
    // will be appended at the begining of WAL
    submit_record(
     record_t {
      { generate_extent(1), generate_extent(2) },
      { generate_delta(20), generate_delta(21) }
      });
    ASSERT_EQ(avail - record_total_size, get_available_size());
    replay_and_check();
  });
}

TEST_F(cbjournal_test_t, update_header)
{
  run_async([this] {
    mkfs();
    open();
    auto [header, _buf] = *(cbj->read_header().unsafe_get0());
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();
    submit_record(std::move(rec));

    update_journal_tail(entries.front().addr, record_total_size);
    cbj->write_header().unsafe_get0();
    auto [update_header, update_buf2] = *(cbj->read_header().unsafe_get0());
    cbj->close().unsafe_get0();
    replay();

    ASSERT_EQ(update_header.journal_tail, update_header.journal_tail);
    ASSERT_EQ(header.block_size, update_header.block_size);
    ASSERT_EQ(header.size, update_header.size);
  });
}

TEST_F(cbjournal_test_t, replay)
{
  run_async([this] {
    mkfs();
    open();
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();
    submit_record(std::move(rec));
    while (record_total_size <= get_available_size()) {
    submit_record(
      record_t {
       { generate_extent(1), generate_extent(2) },
       { generate_delta(20), generate_delta(21) }
       });
    }
    // will be appended at the begining of WAL
    uint64_t avail = get_available_size();
    update_journal_tail(entries.front().addr, record_total_size);
    entries.erase(entries.begin());
    ASSERT_EQ(avail + record_total_size, get_available_size());
    avail = get_available_size();
    submit_record(
      record_t {
       { generate_extent(1), generate_extent(2) },
       { generate_delta(20), generate_delta(21) }
       });
    ASSERT_EQ(avail - record_total_size, get_available_size());
    cbj->close().unsafe_get0();
    replay();
  });
}

TEST_F(cbjournal_test_t, replay_after_reset)
{
  run_async([this] {
    mkfs();
    open();
    record_t rec {
     { generate_extent(1), generate_extent(2) },
     { generate_delta(20), generate_delta(21) }
     };
    auto r_size = record_group_size_t(rec.size, block_size);
    auto record_total_size = r_size.get_encoded_length();
    submit_record(std::move(rec));
    while (record_total_size <= get_available_size()) {
    submit_record(
      record_t {
       { generate_extent(1), generate_extent(2) },
       { generate_delta(20), generate_delta(21) }
       });
    }
    auto old_written_to = get_written_to();
    auto old_used_size = get_used_size();
    set_written_to(4096);
    cbj->close().unsafe_get0();
    replay();
    ASSERT_EQ(old_written_to, get_written_to());
    ASSERT_EQ(old_used_size,
      get_used_size());
  });
}
