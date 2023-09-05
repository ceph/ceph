// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <fcntl.h>

#include "crimson/common/log.h"
#include "crimson/common/errorator-loop.h"

#include "include/buffer.h"
#include "rbm_device.h"
#include "nvme_block_device.h"
#include "block_rb_manager.h"

namespace crimson::os::seastore::random_block_device {
#include "crimson/os/seastore/logging.h"
SET_SUBSYS(seastore_device);

RBMDevice::mkfs_ret RBMDevice::do_primary_mkfs(device_config_t config,
  int shard_num, size_t journal_size) {
  LOG_PREFIX(RBMDevice::do_primary_mkfs);
  return stat_device(
  ).handle_error(
    mkfs_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error stat_device in RBMDevice::do_primary_mkfs"}
  ).safe_then(
    [this, FNAME, config=std::move(config), shard_num, journal_size](auto st) {
    super.block_size = st.block_size;
    super.size = st.size;
    super.feature |= RBM_BITMAP_BLOCK_CRC;
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
    return open(get_device_path(),
      seastar::open_flags::rw | seastar::open_flags::dsync
    ).handle_error(
      mkfs_ertr::pass_further{},
      crimson::ct_error::assert_all{
      "Invalid error open in RBMDevice::do_primary_mkfs"}
    ).safe_then([this] {
      return write_rbm_header(
      ).safe_then([this] {
	return close();
      }).handle_error(
	mkfs_ertr::pass_further{},
	crimson::ct_error::assert_all{
	"Invalid error write_rbm_header in RBMDevice::do_primary_mkfs"
      });
    });
  });
}

write_ertr::future<> RBMDevice::write_rbm_header()
{
  bufferlist meta_b_header;
  super.crc = 0;
  encode(super, meta_b_header);
  // If NVMeDevice supports data protection, CRC for checksum is not required
  // NVMeDevice is expected to generate and store checksum internally.
  // CPU overhead for CRC might be saved.
  if (is_data_protection_enabled()) {
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
  return write(RBM_START_ADDRESS, std::move(bp));
}

read_ertr::future<rbm_metadata_header_t> RBMDevice::read_rbm_header(
  rbm_abs_addr addr)
{
  LOG_PREFIX(RBMDevice::read_rbm_header);
  assert(super.block_size > 0);
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(super.block_size)),
    [this, addr, FNAME](auto &bptr) {
    return read(
      addr,
      bptr
    ).safe_then([length=bptr.length(), this, bptr, FNAME]()
      -> read_ertr::future<rbm_metadata_header_t> {
      bufferlist bl;
      bl.append(bptr);
      auto p = bl.cbegin();
      rbm_metadata_header_t super_block;
      try {
	decode(super_block, p);
      }
      catch (ceph::buffer::error& e) {
	DEBUG("read_rbm_header: unable to decode rbm super block {}",
	      e.what());
	return crimson::ct_error::enoent::make();
      }
      checksum_t crc = super_block.crc;
      bufferlist meta_b_header;
      super_block.crc = 0;
      encode(super_block, meta_b_header);
      assert(ceph::encoded_sizeof<rbm_metadata_header_t>(super_block) <
	  super_block.block_size);

      // Do CRC verification only if data protection is not supported.
      if (is_data_protection_enabled() == false) {
	if (meta_b_header.crc32c(-1) != crc) {
	  DEBUG("bad crc on super block, expected {} != actual {} ",
		meta_b_header.crc32c(-1), crc);
	  return crimson::ct_error::input_output_error::make();
	}
      } else {
	ceph_assert_always(crc == (checksum_t)-1);
      }
      super_block.crc = crc;
      super = super_block;
      DEBUG("got {} ", super);
      return read_ertr::future<rbm_metadata_header_t>(
	read_ertr::ready_future_marker{},
	super_block
      );
    });
  });
}

RBMDevice::mount_ret RBMDevice::do_shard_mount()
{
  return open(get_device_path(),
    seastar::open_flags::rw | seastar::open_flags::dsync
  ).safe_then([this] {
    return stat_device(
    ).handle_error(
      mount_ertr::pass_further{},
      crimson::ct_error::assert_all{
      "Invalid error stat_device in RBMDevice::do_shard_mount"}
    ).safe_then([this](auto st) {
      assert(st.block_size > 0);
      super.block_size = st.block_size;
      return read_rbm_header(RBM_START_ADDRESS
      ).safe_then([this](auto s) {
	LOG_PREFIX(RBMDevice::do_shard_mount);
	shard_info = s.shard_infos[seastar::this_shard_id()];
	INFO("{} read {}", device_id_printer_t{get_device_id()}, shard_info);
	s.validate();
	return seastar::now();
      });
    });
  }).handle_error(
    mount_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error mount in RBMDevice::do_shard_mount"}
  );
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

  void* addr = ::mmap(
    nullptr,
    size,
    PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS,
    -1,
    0);

  buf = (char*)addr;

  ::memset(buf, 0, size);
  return open_ertr::now();
}

write_ertr::future<> EphemeralRBMDevice::write(
  uint64_t offset,
  bufferptr &&bptr,
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

