// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include <random>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/circular_bounded_journal.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/nvmedevice.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

constexpr uint64_t DEFAULT_TEST_SIZE = 1 << 20;
constexpr uint64_t DEFAULT_BLOCK_SIZE = 4096;


std::optional<record_t> decode_record(
  bufferlist& bl)
{
  record_t record;
  record_header_t header;
  checksum_t crc;
  std::vector<extent_info_t> extent_infos;
  auto bliter = bl.cbegin();
  decode(header, bliter);
  decode(crc, bliter);
  for (int i = 0; i < (int)header.extents; i++) {
    extent_info_t ex;
    decode(ex, bliter);
    extent_infos.push_back(ex);
  }
  for (int i = 0; i < (int)header.deltas; i++) {
    delta_info_t delta;
    decode(delta, bliter);
    record.deltas.push_back(delta);
  }

  if (bl.length() % DEFAULT_BLOCK_SIZE != 0) {
    bl.append_zero(
      DEFAULT_BLOCK_SIZE - (bl.length() % DEFAULT_BLOCK_SIZE));
  }

  bliter = bl.cbegin();
  bliter += header.mdlength;
  logger().debug(" decode_record md len {} extents {}", header.mdlength, header.extents);
  for (auto iter : extent_infos) {
    extent_t ex;
    auto bptr = bufferptr(ceph::buffer::create_page_aligned(iter.len));
    logger().debug(" exten len {} remaining {} ", iter.len, bliter.get_remaining());
    bliter.copy(iter.len, bptr.c_str());
    ex.bl.append(bptr);
    record.extents.push_back(ex);
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

  void validate(CBJournal &cbj) {
    rbm_abs_addr offset = 0;
    for (int i = 0; i < entries; i++) {
      paddr_t paddr = convert_abs_addr_to_paddr(
	addr + offset,
	cbj.get_block_size(),
	cbj.get_device_id());
      auto [header, buf] = *(cbj.read_record(paddr).unsafe_get0());
      auto record = decode_record(buf);
      validate(*record);
      offset += header.mdlength + header.dlength;
    }
  }
};

struct cbjournal_test_t : public seastar_test_suite_t
{
  segment_manager::EphemeralSegmentManagerRef segment_manager; // Need to be deleted, just for Cache
  ExtentReaderRef reader;
  Cache cache;
  std::vector<entry_validator_t> entries;
  std::unique_ptr<CBJournal> cbj;
  nvme_device::NVMeBlockDevice *device;

  std::default_random_engine generator;
  uint64_t block_size;
  CBJournal::mkfs_config_t config;
  WritePipeline pipeline;

  cbjournal_test_t() :
      segment_manager(segment_manager::create_test_ephemeral()),
      reader(new ExtentReader()),
      cache(*reader)
  {
    device = new nvme_device::TestMemory(DEFAULT_TEST_SIZE);
    cbj.reset(new CBJournal(device, std::string()));
    config.start = paddr_t::make_blk_paddr(0, 0, 0);
    config.end = paddr_t::make_blk_paddr(0,
      DEFAULT_TEST_SIZE / DEFAULT_BLOCK_SIZE,
      DEFAULT_TEST_SIZE % DEFAULT_BLOCK_SIZE);
    config.block_size = DEFAULT_BLOCK_SIZE;
    config.total_size = DEFAULT_TEST_SIZE;
    block_size = DEFAULT_BLOCK_SIZE;
    cbj->set_write_pipeline(&pipeline);
  }

  seastar::future<> set_up_fut() final {
    return seastar::now();
  }

  auto submit_record(record_t&& record) {
    entries.push_back(record);
    OrderingHandle handle = get_dummy_ordering_handle();
    auto [addr, seq] = cbj->submit_record(
	  std::move(record),
	  handle).unsafe_get0();
    entries.back().addr = 
      convert_paddr_to_abs_addr(
	addr,
	device->get_block_size());
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
	DEFAULT_BLOCK_SIZE,
	1,
	bl
    };
  }

  auto replay_and_check() {
    for (auto &i : entries) {
      i.validate(*(cbj.get()));
    }
  }

  auto mkfs() {
    return cbj->mkfs(config).unsafe_get0();
  }
  auto open() {
    rbm_abs_addr addr = convert_paddr_to_abs_addr(
      config.start,
      device->get_block_size());
    return cbj->open_for_write(addr).unsafe_get0();
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
  auto get_committed_to() {
    return cbj->get_committed_to();
  }
  auto get_applied_to() {
    return cbj->get_applied_to();
  }
  void update_applied_to(rbm_abs_addr addr, uint32_t len) {
    cbj->update_applied_to(addr, len);
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
   auto record_size = get_encoded_record_length(rec, block_size);
   auto record_total_size = record_size.mdlength + record_size.dlength;
   submit_record(std::move(rec));
   while (record_total_size <= get_available_size()) {
     submit_record(
       record_t {
	  { generate_extent(1), generate_extent(2) },
	  { generate_delta(20), generate_delta(21) }
	  });
   }

   uint64_t avail = get_available_size();
   update_applied_to(entries.back().addr, record_total_size);
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
   auto record_size = get_encoded_record_length(rec, block_size);
   auto record_total_size = record_size.mdlength + record_size.dlength;
   submit_record(std::move(rec));
   while (record_total_size <= get_available_size()) {
     submit_record(
       record_t {
	  { generate_extent(1), generate_extent(2) },
	  { generate_delta(20), generate_delta(21) }
	  });
   }

   uint64_t avail = get_available_size();
   update_applied_to(entries.front().addr, record_total_size);
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

TEST_F(cbjournal_test_t, update_super)
{
 run_async([this] {
   mkfs();
   open();
   auto [header, _buf] = *(cbj->read_super(0).unsafe_get0());
   record_t rec {
      { generate_extent(1), generate_extent(2) },
      { generate_delta(20), generate_delta(21) }
      };
   auto record_size = get_encoded_record_length(rec, block_size);
   auto record_total_size = record_size.mdlength + record_size.dlength;
   submit_record(std::move(rec));

   cbj->sync_super().unsafe_get0();
   auto [update_header, update_buf] = *(cbj->read_super(0).unsafe_get0());

   ASSERT_EQ(header.size, update_header.size);
   ASSERT_EQ(header.used_size + record_total_size, update_header.used_size);

   update_applied_to(entries.front().addr, record_total_size);
   cbj->sync_super().unsafe_get0();
   auto [update_header2, update_buf2] = *(cbj->read_super(0).unsafe_get0());

   ASSERT_EQ(header.used_size, update_header2.used_size);
   ASSERT_EQ(header.written_to + record_total_size, update_header2.written_to);
   ASSERT_EQ(header.committed_to, update_header2.committed_to);
 });
}
