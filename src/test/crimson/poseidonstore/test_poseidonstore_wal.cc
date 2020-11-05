// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include <random>

#include "crimson/common/log.h"
#include "crimson/os/poseidonstore/wal.h"
#include "crimson/os/poseidonstore/device_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::poseidonstore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

struct entry_validator_t {
  bufferlist bl;
  paddr_t addr;
  int entries;
  wal_seq_t last_seq;

  template <typename... T>
  entry_validator_t(T&&... entry) : bl(std::forward<T>(entry)...) {}

  void validate(DeviceManager &manager) {
    bufferlist buf;
    auto test = manager.read(
      addr,
      bl.length()).unsafe_get0();
    buf.push_back(test);
    ASSERT_EQ(
      bl.length(),
      buf.length());
    ASSERT_EQ(
      bl.begin().crc32c(bl.length(), 1),
      buf.begin().crc32c(buf.length(), 1));
  }
  void validate(WAL &wal) {
    paddr_t entry_offset = 0, bl_offset = 0;
    for (int i = 0; i < entries; i++) {
      bufferlist buf, original;
      auto [header, entry_buf] = *(wal.read_entry(addr + entry_offset).unsafe_get0());
      buf.substr_of(entry_buf, header.header_length, header.data_length);
      original.substr_of(bl, bl_offset, header.data_length);
      ASSERT_EQ(
	buf.length(),
	original.length());
      ASSERT_EQ(
	buf.begin().crc32c(buf.length(), 1),
	original.begin().crc32c(original.length(), 1));
      entry_offset += header.total_length;
      bl_offset += header.data_length;
    }
  }
};

struct wal_test_t : seastar_test_suite_t {
  std::unique_ptr<DeviceManager> device_manager;
  std::unique_ptr<WAL> wal;
  std::vector<entry_validator_t> entries;

  std::default_random_engine generator;
  uint64_t block_size;

  wal_test_t()
    : device_manager(create_memory(device_manager::DEFAULT_TEST_MEMORY)),
      block_size(device_manager->get_block_size())
  {}

  seastar::future<> set_up_fut() final {
    wal.reset(new WAL(*device_manager));
    return device_manager->init(
    ).safe_then([this] {
      logger().debug("try to create wal partition");
      return wal->create();
    }).safe_then([this] {
      logger().debug("try to open the wal");
      return wal->open();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ceph_assert(0 == "error");
	})
      );
  }

  auto submit_entry(bufferlist buf) {
    entry_validator_t entry(buf);
    entries.push_back(entry);
    RecordRef record = new Record();
    struct write_item item;
    item.bl.claim_append(buf);
    record->add_write_item(item);
    auto [addr, seq] = *(wal->submit_entry(record).unsafe_get0());
    entries.back().addr = addr;
    entries.back().last_seq = seq;
    entries.back().entries = 1;
    logger().debug("submit entry to addr {}", entries.back().addr);
    return entries.back().addr;
  }

  auto submit_entry(std::vector<bufferlist> &bls) {
    RecordRef record = new Record();
    bufferlist buf;
    for (auto p : bls) {
      buf.append(p);
      struct write_item item;
      item.bl.append(p);
      record->add_write_item(item);
    }
    auto [addr, seq] = *(wal->submit_entry(record).unsafe_get0());
    entry_validator_t entry_val(buf);
    entries.push_back(entry_val);
    entries.back().addr = addr;
    entries.back().last_seq = seq;
    entries.back().entries = bls.size();
    logger().debug("submit entry to addr {}", entries.back().addr);
    return entries.back().addr;
  }

  seastar::future<> tear_down_fut() final {
    return seastar::now();
  }

  bufferlist generate_extent(size_t blocks) {
    std::uniform_int_distribution<char> distribution(
      std::numeric_limits<char>::min(),
      std::numeric_limits<char>::max()
    );
    char contents = distribution(generator);
    bufferlist bl;
    bl.append(buffer::ptr(buffer::create(blocks * block_size, contents)));
    return bl;
  }

  auto replay_and_check() {
    for (auto &i : entries) {
      i.validate(*wal);
    }
  }

  auto get_available_size() {
    return wal->get_available_size();
  }
  auto get_total_wal_size() {
    return wal->get_total_wal_size();
  }
  auto get_block_size() {
    return device_manager->get_block_size();
  }
};

TEST_F(wal_test_t, submit_one_entry)
{
 run_async([this] {
   submit_entry(generate_extent(1));
   replay_and_check();
 });
}

TEST_F(wal_test_t, submit_three_entries)
{
 run_async([this] {
   submit_entry(generate_extent(1));
   submit_entry(generate_extent(4));
   submit_entry(generate_extent(23));
   replay_and_check();
 });
}

TEST_F(wal_test_t, submit_full_entries)
{
 run_async([this] {
   bufferlist buf = generate_extent(23);
   submit_entry(buf);
   while (buf.length() <= get_available_size()) {
    submit_entry(buf);
   }
   uint64_t avail = get_available_size();
   wal->update_last_applied(entries.back().last_seq);
   wal->update_used_size(wal->get_cur_pos(), wal->get_last_applied());
   // call this once again to check that wal handel a corner case.
   wal->update_last_applied(entries.back().last_seq);
   wal->update_used_size(wal->get_cur_pos(), wal->get_last_applied());
   ASSERT_EQ(get_total_wal_size(), get_available_size());
   // will be appended at the begining of log
   submit_entry(buf);
   ASSERT_EQ(wal->get_aligned_entry_size(buf.length()), 
	     get_total_wal_size() - entries.back().last_seq.addr
	     + wal->get_cur_pos().addr);
   ASSERT_EQ(wal->get_aligned_entry_size(buf.length()), 
	     get_total_wal_size() - get_available_size());

   while (buf.length() <= get_available_size()) {
    submit_entry(buf);
   }
   ASSERT_EQ(avail, get_available_size());

 });
}

TEST_F(wal_test_t, boudary_check_verify)
{
 run_async([this] {
   std::vector<bufferlist> bls;
   bufferlist buf = generate_extent(23);
   bls.push_back(buf);
   submit_entry(bls);
   bls.clear();
   while (buf.length() <= get_available_size()) {
    buf = generate_extent(23);
    bls.push_back(buf);
    submit_entry(bls);
    bls.clear();
   }
   uint64_t avail = get_available_size();
   wal->update_last_applied(entries.front().last_seq);
   wal->update_used_size(wal->get_cur_pos(), wal->get_last_applied());
   entries.erase(entries.begin());
   ASSERT_EQ(avail + wal->get_aligned_entry_size(buf.length()),
	     get_available_size());
   avail = get_available_size();
   // will be appended at the begining of WAL
   buf = generate_extent(23);
   bls.push_back(buf);
   submit_entry(bls);
   bls.clear();
   ASSERT_EQ(avail - wal->get_aligned_entry_size(buf.length()),
	     get_available_size());
   replay_and_check();
 });
}

TEST_F(wal_test_t, write_multi_entries)
{
 run_async([this] {
   std::vector<bufferlist> bls;
   bls.push_back(generate_extent(23));
   bls.push_back(generate_extent(4));
   bls.push_back(generate_extent(1));
   size_t size;
   for (auto p : bls) {
    size += p.length();
   }
   submit_entry(bls);
   bls.clear();
   size = wal->get_aligned_entry_size(size);
   while (size <= get_available_size()) {
    bls.push_back(generate_extent(23));
    bls.push_back(generate_extent(4));
    bls.push_back(generate_extent(1));
    submit_entry(bls);
    bls.clear();
   }
   wal->update_last_applied(entries.front().last_seq);
   wal->update_used_size(wal->get_cur_pos(), wal->get_last_applied());
   entries.erase(entries.begin());
   bls.push_back(generate_extent(8));
   bls.push_back(generate_extent(4));
   bls.push_back(generate_extent(1));
   submit_entry(bls);
   bls.clear();
   replay_and_check();
 });
}

TEST_F(wal_test_t, update_super)
{
 run_async([this] {
   auto [init_header, init_buf] = *(wal->read_super(WAL_START_ADDRESS).unsafe_get0());
   bufferlist buf = generate_extent(23);
   submit_entry(buf);
   while (buf.length() <= get_available_size()) {
    submit_entry(buf);
   }
   wal->update_last_applied(entries.back().last_seq);
   wal->update_used_size(wal->get_cur_pos(), wal->get_last_applied());
   wal_seq_t cur_pos = wal->get_cur_pos();
   wal_seq_t last_applied = wal->get_last_applied();
   wal->sync_super().unsafe_get0();
   auto [update_header, update_buf] = *(wal->read_super(WAL_START_ADDRESS).unsafe_get0());
   ASSERT_EQ(init_header.max_size, update_header.max_size);
   ASSERT_EQ(init_header.used_size, update_header.used_size);
   ASSERT_EQ(cur_pos.addr, update_header.cur_pos.addr);
   ASSERT_EQ(last_applied.addr, update_header.applied.addr);
   ASSERT_EQ(last_applied.length, update_header.applied.length);
   ASSERT_EQ(last_applied.id, update_header.applied.id);
 });
}

TEST_F(wal_test_t, read_replay)
{
 run_async([this] {
   bufferlist buf = generate_extent(23);
   submit_entry(buf);
   while (buf.length() <= get_available_size()) {
    submit_entry(buf);
   }
   wal->update_last_applied(entries.back().last_seq);

   /* read forward */
   paddr_t start_addr = 0; 
   while (start_addr != entries.back().last_seq.addr) {
     auto [header, entry_buf] = *(wal->read_entry(start_addr).unsafe_get0());
     ASSERT_EQ(true, wal->check_valid_entry(header));
     start_addr += header.total_length;
   }
 });
}
