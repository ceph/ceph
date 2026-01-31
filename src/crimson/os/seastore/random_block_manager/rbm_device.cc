// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <sys/mman.h>
#include <string.h>

#include <fcntl.h>

#include "crimson/common/log.h"
#include "crimson/common/errorator-loop.h"
#include "crimson/os/seastore/logging.h"

#include "include/buffer.h"
#include "rbm_device.h"
#include "nvme_block_device.h"
#include "block_rb_manager.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore::random_block_device {

RBMDevice::mkfs_ret RBMDevice::do_primary_mkfs(device_config_t config,
  int shard_num, size_t journal_size) {
  LOG_PREFIX(RBMDevice::do_primary_mkfs);
  check_create_device_ret maybe_create = check_create_device_ertr::now();
  using crimson::common::get_conf;
  if (get_conf<bool>("seastore_block_create") && !get_device_path().empty()) {
    auto size = get_conf<Option::size_t>("seastore_device_size");
    maybe_create = check_create_device(get_device_path(), size);
  }


  co_await std::move(maybe_create);
  auto st = co_await stat_device(
  ).safe_then([] (auto st) mutable {
    return std::optional<seastar::stat_data>(st);
  }).handle_error(crimson::ct_error::all_same_way([]{
    return std::nullopt;
  }));

  if (!st) {
    co_return co_await mkfs_ertr::future<>(
      crimson::ct_error::input_output_error::make()
    );
  }

  super.block_size = (*st).block_size;
  super.size = (*st).size;
  super.config = std::move(config);
  super.journal_size = journal_size;
  ceph_assert_always(super.journal_size > 0);
  ceph_assert_always(super.size >= super.journal_size);
  ceph_assert_always(shard_num > 0);

  std::vector<rbm_shard_info_t> shard_infos(shard_num);
  for (int i = 0; i < shard_num; i++) {
    uint64_t aligned_size = 
      (super.size / shard_num) -
      ((super.size / shard_num) % super.block_size);
    shard_infos[i].size = aligned_size;
    shard_infos[i].start_offset = i * aligned_size;
    assert(shard_infos[i].size > super.journal_size);
  }
  super.shard_infos = shard_infos;
  super.shard_num = shard_num;
  shard_info = shard_infos[seastar::this_shard_id()];
  DEBUG("super {} ", super);

  // write super block
  co_await open(get_device_path(),
    seastar::open_flags::rw | seastar::open_flags::dsync
  ).handle_error(
    mkfs_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error open in RBMDevice::do_primary_mkfs"}
  );
  co_await initialize_nvme_features();
  co_await write_rbm_superblock(
  ).handle_error(
    mkfs_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error write_rbm_superblock in RBMDevice::do_primary_mkfs"
  });
  co_await close();
}

write_ertr::future<> RBMDevice::write_rbm_superblock()
{
  bufferlist meta_b_header;
  super.crc = 0;
  encode(super, meta_b_header);
  // If NVMeDevice supports data protection, CRC for checksum is not required
  // NVMeDevice is expected to generate and store checksum internally.
  // CPU overhead for CRC might be saved.
  if (is_end_to_end_data_protection()) {
    super.crc = -1;
  } else {
    super.crc = meta_b_header.crc32c(-1);
  }

  bufferlist bl;
  encode(super, bl);
  auto iter = bl.begin();
  auto bp = bufferptr(ceph::buffer::create_page_aligned(super.block_size));
  assert(bl.length() < super.block_size);
  iter.copy(bl.length(), bp.c_str());
  co_return co_await write(RBM_START_ADDRESS, bp);
}

read_ertr::future<rbm_superblock_t> RBMDevice::read_rbm_superblock(
  rbm_abs_addr addr)
{
  LOG_PREFIX(RBMDevice::read_rbm_superblock);
  assert(super.block_size > 0);
  auto bptr = bufferptr(ceph::buffer::create_page_aligned(super.block_size));
  co_await read(addr, bptr);
  bufferlist bl;
  bl.append(bptr);
  auto p = bl.cbegin();
  rbm_superblock_t super_block;
  bool err = false;
  try {
    decode(super_block, p);
  } catch (ceph::buffer::error& e) {
    DEBUG("read_rbm_superblock: unable to decode rbm super block {}",
	  e.what());
    err = true;
  }
  if (err) {
    co_return co_await read_ertr::future<rbm_superblock_t>(
      crimson::ct_error::input_output_error::make()
    );
  }
  checksum_t crc = super_block.crc;
  bufferlist meta_b_header;
  super_block.crc = 0;
  encode(super_block, meta_b_header);
  assert(ceph::encoded_sizeof<rbm_superblock_t>(super_block) <
      super_block.block_size);

  // Do CRC verification only if data protection is not supported.
  if (super_block.is_end_to_end_data_protection() == false) {
    if (meta_b_header.crc32c(-1) != crc) {
      DEBUG("bad crc on super block, expected {} != actual {} ",
	    meta_b_header.crc32c(-1), crc);
      co_return co_await read_ertr::future<rbm_superblock_t>(
	crimson::ct_error::input_output_error::make()
      );
    }
  } else {
    ceph_assert_always(crc == (checksum_t)-1);
  }
  super_block.crc = crc;
  super = super_block;
  DEBUG("got {} ", super);
  co_return co_await read_ertr::future<rbm_superblock_t>(
    read_ertr::ready_future_marker{},
    super_block
  );
}

RBMDevice::mount_ret RBMDevice::do_shard_mount()
{
  co_await open(get_device_path(),
    seastar::open_flags::rw | seastar::open_flags::dsync
  ).handle_error(
    mkfs_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error open in RBMDevice::do_shard_mount"}
  );

  auto st = co_await stat_device(
  ).safe_then([] (auto st) mutable {
    return std::optional<seastar::stat_data>(st);
  }).handle_error(crimson::ct_error::all_same_way([]{
    return std::nullopt;
  }));
  if (!st) {
    co_await mount_ertr::future<>(
      crimson::ct_error::input_output_error::make()
    );

  }
  assert((*st).block_size > 0);
  super.block_size = (*st).block_size;
  auto s = co_await read_rbm_superblock(RBM_START_ADDRESS
  ).handle_error(
    mount_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error read_rbm_superblock in RBMDevice::do_shard_mount"}
  );
  LOG_PREFIX(RBMDevice::do_shard_mount);
  shard_info = s.shard_infos[seastar::this_shard_id()];
  INFO("{} read {}", device_id_printer_t{get_device_id()}, shard_info);
  s.validate();
}

EphemeralRBMDeviceRef create_test_ephemeral(uint64_t journal_size, uint64_t data_size) {
  return EphemeralRBMDeviceRef(
    new EphemeralRBMDevice(journal_size + data_size + 
	random_block_device::RBMDevice::get_shard_reserved_size(),
	EphemeralRBMDevice::TEST_BLOCK_SIZE));
}

open_ertr::future<> EphemeralRBMDevice::open(
  const std::string &in_path,
   seastar::open_flags mode) {
  LOG_PREFIX(EphemeralRBMDevice::open);
  if (buf) {
    return open_ertr::now();
  }

  DEBUG(
    "Initializing test memory device {}",
    size);

  // memset 0 is not needed: anonymous mapping is zero-filled
  void* addr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE,
    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

  buf = (char*)addr;

  return open_ertr::now();
}

write_ertr::future<> EphemeralRBMDevice::write(
  uint64_t offset,
  bufferptr bptr,
  uint16_t stream) {
  LOG_PREFIX(EphemeralRBMDevice::write);
  ceph_assert(buf);
  DEBUG(
    "EphemeralRBMDevice: write offset {} len {}",
    offset,
    bptr.length());

  ::memcpy(buf + offset, bptr.c_str(), bptr.length());

  return write_ertr::now();
}

read_ertr::future<> EphemeralRBMDevice::_readv(
  uint64_t offset,
  std::vector<bufferptr> ptrs) {
  LOG_PREFIX(EphemeralRBMDevice::_readv);
  ceph_assert(buf);
  DEBUG(
    "EphemeralRBMDevice: read offset {} {} buffers",
    offset,
    ptrs.size());

  for (auto &ptr : ptrs) {
    ptr.copy_in(0, ptr.length(), buf + offset);
    offset += ptr.length();
  }

  return read_ertr::now();
}

read_ertr::future<> EphemeralRBMDevice::read(
  uint64_t offset,
  bufferptr &bptr) {
  LOG_PREFIX(EphemeralRBMDevice::read);
  ceph_assert(buf);
  DEBUG(
    "EphemeralRBMDevice: read offset {} len {}",
    offset,
    bptr.length());

  bptr.copy_in(0, bptr.length(), buf + offset);
  return read_ertr::now();
}

Device::close_ertr::future<> EphemeralRBMDevice::close() {
  LOG_PREFIX(EphemeralRBMDevice::close);
  DEBUG(" close ");
  return close_ertr::now();
}

write_ertr::future<> EphemeralRBMDevice::writev(
  uint64_t offset,
  ceph::bufferlist bl,
  uint16_t stream) {
  LOG_PREFIX(EphemeralRBMDevice::writev);
  ceph_assert(buf);
  DEBUG(
    "EphemeralRBMDevice: write offset {} len {}",
    offset,
    bl.length());

  bl.begin().copy(bl.length(), buf + offset);
  return write_ertr::now();
}

EphemeralRBMDevice::mount_ret EphemeralRBMDevice::mount() {
  return do_shard_mount();
}

EphemeralRBMDevice::mkfs_ret EphemeralRBMDevice::mkfs(device_config_t config) {
  return do_primary_mkfs(config, 1, DEFAULT_TEST_CBJOURNAL_SIZE);
}

}

