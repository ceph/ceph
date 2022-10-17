// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/os/seastore/logging.h"

#include "include/buffer.h"
#include "rbm_device.h"
#include "include/interval_set.h"
#include "include/intarith.h"
#include "block_rb_manager.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore {

BlockRBManager::write_ertr::future<> BlockRBManager::rbm_sync_block_bitmap(
    rbm_bitmap_block_t &block, blk_no_t block_no)
{
  LOG_PREFIX(BlockRBManager::rbm_sync_block_bitmap);
  bufferptr bptr;
  try {
    bptr = bufferptr(ceph::buffer::create_page_aligned(block.get_size()));
    bufferlist bl;
    encode(block, bl);
    auto iter = bl.cbegin();
    iter.copy(block.get_size(), bptr.c_str());
  } catch (const std::exception &e) {
    DEBUG("rbm_sync_block_bitmap: exception creating aligned buffer {}", e);
    ceph_assert(0 == "unhandled exception");
  }
  uint64_t bitmap_block_no = convert_block_no_to_bitmap_block(block_no);
  return device->write(super.start_alloc_area +
		       bitmap_block_no * super.block_size,
		       bptr);
}

BlockRBManager::mkfs_ertr::future<> BlockRBManager::initialize_blk_alloc_area()
{
  LOG_PREFIX(BlockRBManager::initialize_blk_alloc_area);
  auto start = super.start_data_area / super.block_size;
  DEBUG("initialize_alloc_area: start to read at {} ", start);

  /* write allocated bitmap info to rbm meta block */
  rbm_bitmap_block_t b_block(super.block_size);
  alloc_rbm_bitmap_block_buf(b_block);
  for (uint64_t i = 0; i < start; i++) {
    b_block.set_bit(i);
  }

  // CRC calculation is offloaded to NVMeDevice if data protection is enabled.
  if (device->is_data_protection_enabled() == false) {
    b_block.set_crc();
  }

  return seastar::do_with(
    b_block,
    [this, start, FNAME](auto &b_block) {
    return rbm_sync_block_bitmap(b_block,
      super.start_alloc_area / super.block_size
    ).safe_then([this, &b_block, start, FNAME]() {

      /* initialize bitmap blocks as unused */
      auto max = max_block_by_bitmap_block();
      auto max_block = super.size / super.block_size;
      blk_no_t end = round_up_to(max_block, max) - 1;
      DEBUG("init start {} end {} ", start, end);
      return rbm_sync_block_bitmap_by_range(
	start,
	end,
	bitmap_op_types_t::ALL_CLEAR
      ).safe_then([this, &b_block, FNAME]() {
	/*
	 * Set rest of the block bitmap, which is not used, to 1
	 * To do so, we only mark 1 to empty bitmap blocks
	 */
	uint64_t na_block_no = super.size/super.block_size;
	uint64_t remain_block = na_block_no % max_block_by_bitmap_block();
	DEBUG("na_block_no: {}, remain_block: {} ",
	      na_block_no, remain_block);
	if (remain_block) {
	  DEBUG("try to remained write alloc info ");
	  if (na_block_no > max_block_by_bitmap_block()) {
	    b_block.buf.clear();
	    alloc_rbm_bitmap_block_buf(b_block);
	  }
	  for (uint64_t i = remain_block; i < max_block_by_bitmap_block(); i++) {
	    b_block.set_bit(i);
	  }
	  b_block.set_crc();
	  return rbm_sync_block_bitmap(b_block, na_block_no
	  ).handle_error(
	    mkfs_ertr::pass_further{},
	    crimson::ct_error::assert_all{
	      "Invalid error rbm_sync_block_bitmap to update \
	      last bitmap block in BlockRBManager::initialize_blk_alloc_area"
	    }
	  );
	}
	return mkfs_ertr::now();
      }).handle_error(
	mkfs_ertr::pass_further{},
	crimson::ct_error::assert_all{
	  "Invalid error rbm_sync_block_bitmap \
	    in BlockRBManager::initialize_blk_alloc_area"
	}
      );
    }).handle_error(
      mkfs_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error rbm_sync_block_bitmap_by_range \
	  in BlockRBManager::initialize_blk_alloc_area"
      }
    );
  });
}

BlockRBManager::mkfs_ertr::future<> BlockRBManager::mkfs(mkfs_config_t config)
{
  LOG_PREFIX(BlockRBManager::mkfs);
  super.uuid = uuid_d(); // TODO
  super.magic = 0xFF; // TODO
  super.start = convert_paddr_to_abs_addr(
    config.start);
  super.end = convert_paddr_to_abs_addr(
    config.end);
  super.block_size = config.block_size;
  super.size = config.total_size;
  super.free_block_count = config.total_size/config.block_size - 2;
  super.alloc_area_size = get_alloc_area_size();
  super.start_alloc_area = RBM_SUPERBLOCK_SIZE;
  super.start_data_area =
    super.start_alloc_area + super.alloc_area_size;
  super.crc = 0;
  super.feature |= RBM_BITMAP_BLOCK_CRC;
  super.device_id = config.device_id;

  DEBUG("super {} ", super);
  // write super block
  return write_rbm_header().safe_then([this] {
    return initialize_blk_alloc_area();
  }).handle_error(
    mkfs_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error write_rbm_header in BlockRBManager::mkfs"
  });
}

BlockRBManager::find_block_ret BlockRBManager::find_free_block(Transaction &t, size_t size)
{
  LOG_PREFIX(BlockRBManager::find_free_block);
  auto bp = bufferptr(ceph::buffer::create_page_aligned(super.block_size));
  return seastar::do_with(uint64_t(0),
    uint64_t(super.start_alloc_area),
    interval_set<blk_no_t>(),
    bp,
    [&, this, FNAME](auto &allocated, auto &addr, auto &alloc_extent, auto &bp) mutable {
    return crimson::repeat(
      [&, this, FNAME]() mutable {
      return device->read(
	addr,
	bp
      ).safe_then(
	[&bp, &addr, size, &allocated, &alloc_extent, this, FNAME]() mutable {
	DEBUG("find_free_list: allocate {}, addr {}", allocated, addr);
	rbm_bitmap_block_t b_block(super.block_size);
	bufferlist bl_bitmap_block;
	bl_bitmap_block.append(bp);
	decode(b_block, bl_bitmap_block);
	auto max = max_block_by_bitmap_block();
	for (uint64_t i = 0;
	    i < max && (uint64_t)size/super.block_size > allocated; i++) {
	  auto block_id = convert_bitmap_block_no_to_block_id(i, addr);
	  if (b_block.is_allocated(i)) {
	    continue;
	  }
	  DEBUG("find_free_list: allocated block no {} i {}",
		convert_bitmap_block_no_to_block_id(i, addr), i);
	  if (allocated != 0 && alloc_extent.range_end() != block_id) {
	    /*
	     * if not continous block, just restart to find continuous blocks
	     * at the next block.
	     * in-memory allocator can handle this efficiently.
	     */
	    allocated = 0;
	    alloc_extent.clear(); // a range of block allocation
	    DEBUG("find_free_list: rety to find continuous blocks");
	    continue;
	  }
	  allocated += 1;
	  alloc_extent.insert(block_id);
	}
	addr += super.block_size;
	DEBUG("find_free_list: allocated: {} alloc_extent {}",
	      allocated, alloc_extent);
	if (((uint64_t)size)/super.block_size == allocated) {
	  return seastar::stop_iteration::yes;
	} else if (addr >= super.start_data_area) {
	  alloc_extent.clear();
	  return seastar::stop_iteration::yes;
	}
	return seastar::stop_iteration::no;
      });
    }).safe_then([&allocated, &alloc_extent, size, this, FNAME]() {
      DEBUG(" allocated: {} size {} ",
	    allocated * super.block_size, size);
      if (allocated * super.block_size < size) {
	alloc_extent.clear();
      }
      return find_block_ret(
	find_block_ertr::ready_future_marker{},
	alloc_extent);
    }).handle_error(
      find_block_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in BlockRBManager::find_free_block"
      }
    );
  });
}

/* TODO : block allocator */
BlockRBManager::allocate_ret BlockRBManager::alloc_extent(
    Transaction &t, size_t size)
{

  /*
   * 1. find free blocks using block allocator
   * 2. add free blocks to transaction
   *    (the free block is reserved state, not stored)
   * 3. link free blocks to onode
   * Due to in-memory block allocator is the next work to do,
   * just read the block bitmap directly to find free blocks.
   *
   */
  LOG_PREFIX(BlockRBManager::alloc_extent);
  return find_free_block(t, size
  ).safe_then([this, FNAME](auto alloc_extent) mutable
    -> allocate_ertr::future<paddr_t> {
    DEBUG("after find_free_block: allocated {}", alloc_extent);
    if (alloc_extent.empty()) {
      return crimson::ct_error::enospc::make();
    }
    paddr_t paddr = convert_abs_addr_to_paddr(
      alloc_extent.range_start() * super.block_size,
      super.device_id);
    return allocate_ret(
      allocate_ertr::ready_future_marker{},
      paddr);
  }).handle_error(
    allocate_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error find_free_block in BlockRBManager::alloc_extent"
    }
  );
}

void BlockRBManager::add_free_extent(
    std::vector<alloc_delta_t>& v, rbm_abs_addr from, size_t len)
{
  ceph_assert(!(len % super.block_size));
  paddr_t paddr = convert_abs_addr_to_paddr(
    from,
    super.device_id);
  alloc_delta_t alloc_info;
  alloc_info.alloc_blk_ranges.emplace_back(
    paddr, L_ADDR_NULL, len, extent_types_t::ROOT);
  alloc_info.op = alloc_delta_t::op_types_t::CLEAR;
  v.push_back(alloc_info);
}

BlockRBManager::write_ertr::future<> BlockRBManager::rbm_sync_block_bitmap_by_range(
    blk_no_t start, blk_no_t end, bitmap_op_types_t op)
{
  LOG_PREFIX(BlockRBManager::rbm_sync_block_bitmap_by_range);
  auto addr = super.start_alloc_area +
	      (start / max_block_by_bitmap_block())
	      * super.block_size;
  // aligned write
  if (start % max_block_by_bitmap_block() == 0 &&
      end % (max_block_by_bitmap_block() - 1) == 0) {
    auto num_block = num_block_between_blk_ids(start, end);
    bufferlist bl_bitmap_block;
    add_cont_bitmap_blocks_to_buf(bl_bitmap_block, num_block, op);
    return write(
      addr,
      bl_bitmap_block);
  }
  auto bp = bufferptr(ceph::buffer::create_page_aligned(super.block_size));
  // try to read first block, then check the block is aligned
  return device->read(
    addr,
    bp
  ).safe_then([bp, start, end, op, addr, this, FNAME]() {
    rbm_bitmap_block_t b_block(super.block_size);
    bufferlist bl_bitmap_block;
    bl_bitmap_block.append(bp);
    decode(b_block, bl_bitmap_block);
    auto max = max_block_by_bitmap_block();
    auto loop_end = end < (start / max + 1) * max ?
		    end % max : max - 1;
    for (uint64_t i = (start % max); i <= loop_end; i++) {
      if (op == bitmap_op_types_t::ALL_SET) {
	b_block.set_bit(i);
      } else {
	b_block.clear_bit(i);
      }
    }
    auto num_block = num_block_between_blk_ids(start, end);
    DEBUG("rbm_sync_block_bitmap_by_range: start {}, end {}, \
	  loop_end {}, num_block {}",
	  start, end, loop_end, num_block);

    bl_bitmap_block.clear();
    encode(b_block, bl_bitmap_block);
    if (num_block == 1) {
      // | front (unaligned) |
      return write(
	  addr,
	  bl_bitmap_block);
    } else if (!((end + 1) % max)) {
      // | front (unaligned) | middle (aligned) |
      add_cont_bitmap_blocks_to_buf(bl_bitmap_block, num_block - 1, op);
      DEBUG("partially aligned write: addr {} length {}",
	    addr, bl_bitmap_block.length());
      return write(
	  addr,
	  bl_bitmap_block);
    } else if (num_block > 2) {
      // | front (unaligned) | middle | end (unaligned) |
      // fill up the middle
      add_cont_bitmap_blocks_to_buf(bl_bitmap_block, num_block - 2, op);
    }

    auto next_addr = super.start_alloc_area +
		(end / max_block_by_bitmap_block())
		* super.block_size;
    auto bptr = bufferptr(ceph::buffer::create_page_aligned(super.block_size));
    // | front (unaligned) | middle | end (unaligned) | or
    // | front (unaligned) | end (unaligned) |
    return device->read(
      next_addr,
      bptr
    ).safe_then(
      [bptr, bl_bitmap_block, end, op, addr, this, FNAME]() mutable {
      rbm_bitmap_block_t b_block(super.block_size);
      bufferlist block;
      block.append(bptr);
      decode(b_block, block);
      auto max = max_block_by_bitmap_block();
      for (uint64_t i = (end - (end % max)) % max;
	  i <= (end % max); i++) {
	if (op == bitmap_op_types_t::ALL_SET) {
	  b_block.set_bit(i);
	} else {
	  b_block.clear_bit(i);
	}
      }
      DEBUG("start {} end {} ", end - (end % max), end);
      bl_bitmap_block.claim_append(block);
      return write(
	addr,
	bl_bitmap_block);
    }).handle_error(
      write_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in BlockRBManager::rbm_sync_block_bitmap_by_range"
      }
    );
  }).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in BlockRBManager::rbm_sync_block_bitmap_by_range"
    }
  );
}

BlockRBManager::abort_allocation_ertr::future<> BlockRBManager::abort_allocation(
    Transaction &t)
{
  /*
   * TODO: clear all allocation infos associated with transaction in in-memory allocator
   */
  return abort_allocation_ertr::now();
}

BlockRBManager::write_ertr::future<> BlockRBManager::complete_allocation(
    Transaction &t)
{
  return write_ertr::now();
}

BlockRBManager::write_ertr::future<> BlockRBManager::sync_allocation(
    std::vector<alloc_delta_t> &alloc_blocks)
{
  LOG_PREFIX(BlockRBManager::sync_allocation);
  if (alloc_blocks.empty()) {
    return write_ertr::now();
  }
  return seastar::do_with(move(alloc_blocks),
    [&, this, FNAME](auto &alloc_blocks) mutable {
    return crimson::do_for_each(alloc_blocks,
      [this, FNAME](auto &alloc) {
      return crimson::do_for_each(alloc.alloc_blk_ranges,
        [this, &alloc, FNAME](auto &range) -> write_ertr::future<> {
        DEBUG("range {} ~ {}", range.paddr, range.len);
	bitmap_op_types_t op =
	  (alloc.op == alloc_delta_t::op_types_t::SET) ?
	  bitmap_op_types_t::ALL_SET :
	  bitmap_op_types_t::ALL_CLEAR;
	rbm_abs_addr addr = convert_paddr_to_abs_addr(
	  range.paddr);
	blk_no_t start = addr / super.block_size;
	blk_no_t end = start +
	  (round_up_to(range.len, super.block_size)) / super.block_size
	   - 1;
	return rbm_sync_block_bitmap_by_range(
	  start,
	  end,
	  op);
      });
    }).safe_then([this, &alloc_blocks, FNAME]() mutable {
      int alloc_block_count = 0;
      for (const auto& b : alloc_blocks) {
	for (auto r : b.alloc_blk_ranges) {
	  if (b.op == alloc_delta_t::op_types_t::SET) {
	    alloc_block_count +=
	      round_up_to(r.len, super.block_size) / super.block_size;
	    DEBUG("complete alloc block: start {} len {} ",
		  r.paddr, r.len);
	  } else {
	    alloc_block_count -=
	      round_up_to(r.len, super.block_size) / super.block_size;
	    DEBUG("complete alloc block: start {} len {} ",
		  r.paddr, r.len);
	  }
	}
      }
      DEBUG("complete_alloction: complete to allocate {} blocks",
	    alloc_block_count);
      super.free_block_count -= alloc_block_count;
      return write_ertr::now();
    });
  });
}

BlockRBManager::open_ertr::future<> BlockRBManager::open()
{
  return read_rbm_header(RBM_START_ADDRESS
  ).safe_then([&](auto s)
    -> open_ertr::future<> {
    if (s.magic != 0xFF) {
      return crimson::ct_error::enoent::make();
    }
    super = s;
    return check_bitmap_blocks().safe_then([]() {
      return open_ertr::now();
    });
  }).handle_error(
    open_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error read_rbm_header in BlockRBManager::open"
    }
  );
}

BlockRBManager::write_ertr::future<> BlockRBManager::write(
  paddr_t paddr,
  bufferptr &bptr)
{
  ceph_assert(device);
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  if (addr > super.end || addr < super.start ||
      bptr.length() > super.end - super.start) {
    return crimson::ct_error::erange::make();
  }
  return device->write(
    addr,
    bptr);
}

BlockRBManager::read_ertr::future<> BlockRBManager::read(
  paddr_t paddr,
  bufferptr &bptr)
{
  ceph_assert(device);
  rbm_abs_addr addr = convert_paddr_to_abs_addr(paddr);
  if (addr > super.end || addr < super.start ||
      bptr.length() > super.end - super.start) {
    return crimson::ct_error::erange::make();
  }
  return device->read(
    addr,
    bptr);
}

BlockRBManager::close_ertr::future<> BlockRBManager::close()
{
  ceph_assert(device);
  return device->close();
}

BlockRBManager::open_ertr::future<> BlockRBManager::_open_device(
    const std::string path)
{
  ceph_assert(device);
  return device->open(path, seastar::open_flags::rw);
}

BlockRBManager::write_ertr::future<> BlockRBManager::write_rbm_header()
{
  bufferlist meta_b_header;
  super.crc = 0;
  encode(super, meta_b_header);
  // If NVMeDevice supports data protection, CRC for checksum is not required
  // NVMeDevice is expected to generate and store checksum internally.
  // CPU overhead for CRC might be saved.
  if (device->is_data_protection_enabled()) {
    super.crc = -1;
  }
  else {
    super.crc = meta_b_header.crc32c(-1);
  }

  bufferlist bl;
  encode(super, bl);
  auto iter = bl.begin();
  auto bp = bufferptr(ceph::buffer::create_page_aligned(super.block_size));
  assert(bl.length() < super.block_size);
  iter.copy(bl.length(), bp.c_str());

  return device->write(super.start, bp);
}

BlockRBManager::read_ertr::future<rbm_metadata_header_t> BlockRBManager::read_rbm_header(
    rbm_abs_addr addr)
{
  LOG_PREFIX(BlockRBManager::read_rbm_header);
  ceph_assert(device);
  bufferptr bptr =
    bufferptr(ceph::buffer::create_page_aligned(RBM_SUPERBLOCK_SIZE));
  bptr.zero();
  return device->read(
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

    // Do CRC verification only if data protection is not supported.
    if (device->is_data_protection_enabled() == false) {
      if (meta_b_header.crc32c(-1) != crc) {
        DEBUG("bad crc on super block, expected {} != actual {} ",
              meta_b_header.crc32c(-1), crc);
        return crimson::ct_error::input_output_error::make();
      }
    }
    DEBUG("got {} ", super);
    return read_ertr::future<rbm_metadata_header_t>(
      read_ertr::ready_future_marker{},
      super_block
    );
  }).handle_error(
    read_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in BlockRBManager::read_rbm_header"
    }
  );
}

BlockRBManager::check_bitmap_blocks_ertr::future<> BlockRBManager::check_bitmap_blocks()
{
  LOG_PREFIX(BlockRBManager::check_bitmap_blocks);
  auto bp = bufferptr(ceph::buffer::create_page_aligned(super.block_size));
  return seastar::do_with(uint64_t(super.start_alloc_area), uint64_t(0), bp,
    [&, this, FNAME](auto &addr, auto &free_blocks, auto &bp) mutable {
    return crimson::repeat([&, this, FNAME]() mutable {
      return device->read(addr, bp
      ).safe_then(
	[&bp, &addr, &free_blocks, this, FNAME]() mutable {
	DEBUG("verify_bitmap_blocks: addr {}", addr);
	rbm_bitmap_block_t b_block(super.block_size);
	bufferlist bl_bitmap_block;
	bl_bitmap_block.append(bp);
	decode(b_block, bl_bitmap_block);
	auto max = max_block_by_bitmap_block();
	for (uint64_t i = 0; i < max; i++) {
	  if (!b_block.is_allocated(i)) {
	    free_blocks++;
	  }
	}
	addr += super.block_size;
	if (addr >= super.start_data_area) {
	  return seastar::stop_iteration::yes;
	}
	return seastar::stop_iteration::no;
      });
    }).safe_then([&free_blocks, this, FNAME]() {
      DEBUG("free_blocks: {} ", free_blocks);
      super.free_block_count = free_blocks;
      return check_bitmap_blocks_ertr::now();
    }).handle_error(
      check_bitmap_blocks_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error in BlockRBManager::find_free_block"
      }
    );
  });
}

BlockRBManager::write_ertr::future<> BlockRBManager::write(
  rbm_abs_addr addr,
  bufferlist &bl)
{
  LOG_PREFIX(BlockRBManager::write);
  ceph_assert(device);
  bufferptr bptr;
  try {
    bptr = bufferptr(ceph::buffer::create_page_aligned(bl.length()));
    auto iter = bl.cbegin();
    iter.copy(bl.length(), bptr.c_str());
  } catch (const std::exception &e) {
    DEBUG("write: exception creating aligned buffer {}", e);
    ceph_assert(0 == "unhandled exception");
  }
  return device->write(
    addr,
    bptr);
}

std::ostream &operator<<(std::ostream &out, const rbm_metadata_header_t &header)
{
  out << " rbm_metadata_header_t(size=" << header.size
       << ", block_size=" << header.block_size
       << ", start=" << header.start
       << ", end=" << header.end
       << ", magic=" << header.magic
       << ", uuid=" << header.uuid
       << ", free_block_count=" << header.free_block_count
       << ", alloc_area_size=" << header.alloc_area_size
       << ", start_alloc_area=" << header.start_alloc_area
       << ", start_data_area=" << header.start_data_area
       << ", flag=" << header.flag
       << ", feature=" << header.feature
       << ", crc=" << header.crc;
  return out << ")";
}

std::ostream &operator<<(std::ostream &out,
    const rbm_bitmap_block_header_t &header)
{
  out << " rbm_bitmap_block_header_t(size=" << header.size
       << ", checksum=" << header.checksum;
  return out << ")";
}

}
