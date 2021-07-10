// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/segment_manager/block.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}


namespace crimson::os::seastore::segment_manager::block {

static write_ertr::future<> do_write(
  seastar::file &device,
  uint64_t offset,
  bufferptr &bptr)
{
  logger().debug(
    "block: do_write offset {} len {}",
    offset,
    bptr.length());
  return device.dma_write(
    offset,
    bptr.c_str(),
    bptr.length()
  ).handle_exception([](auto e) -> write_ertr::future<size_t> {
      logger().error(
	"do_write: dma_write got error {}",
	e);
      return crimson::ct_error::input_output_error::make();
  }).then([length=bptr.length()](auto result)
	       -> write_ertr::future<> {
    if (result != length) {
      return crimson::ct_error::input_output_error::make();
    }
    return write_ertr::now();
  });
}

static write_ertr::future<> do_writev(
  seastar::file &device,
  uint64_t offset,
  bufferlist&& bl,
  size_t block_size)
{
  logger().debug(
    "block: do_writev offset {} len {}",
    offset,
    bl.length());

  // writev requires each buffer to be aligned to the disks' block
  // size, we need to rebuild here
  bl.rebuild_aligned(block_size);

  std::vector<iovec> iov;
  bl.prepare_iov(&iov);
  return device.dma_write(
    offset,
    std::move(iov)
  ).handle_exception([](auto e) -> write_ertr::future<size_t> {
      logger().error(
	"do_writev: dma_write got error {}",
	e);
      return crimson::ct_error::input_output_error::make();
  }).then(
    [bl=std::move(bl)/* hold the buf until the end of io */](size_t written)
	       -> write_ertr::future<> {
    if (written != bl.length()) {
      return crimson::ct_error::input_output_error::make();
    }
    return write_ertr::now();
  });
}

static read_ertr::future<> do_read(
  seastar::file &device,
  uint64_t offset,
  bufferptr &bptr)
{
  logger().debug(
    "block: do_read offset {} len {}",
    offset,
    bptr.length());
  return device.dma_read(
    offset,
    bptr.c_str(),
    bptr.length()
  ).handle_exception([](auto e) -> read_ertr::future<size_t> {
    logger().error(
      "do_read: dma_read got error {}",
      e);
    return crimson::ct_error::input_output_error::make();
  }).then([length=bptr.length()](auto result) -> read_ertr::future<> {
    if (result != length) {
      return crimson::ct_error::input_output_error::make();
    }
    return read_ertr::now();
  });
}

write_ertr::future<>
SegmentStateTracker::write_out(
  seastar::file &device,
  uint64_t offset)
{
  return do_write(device, offset, bptr);
}

write_ertr::future<>
SegmentStateTracker::read_in(
  seastar::file &device,
  uint64_t offset)
{
  return do_read(
    device,
    offset,
    bptr);
}

static
block_sm_superblock_t make_superblock(
  seastore_meta_t meta,
  const seastar::stat_data &data)
{
  using crimson::common::get_conf;

  auto config_size = get_conf<Option::size_t>(
    "seastore_device_size");

  logger().debug(
    "{}: size {}, block_size {}, allocated_size {}, configured_size {}",
    __func__,
    data.size,
    data.block_size,
    data.allocated_size,
    config_size);

  size_t size = (data.size == 0) ? config_size : data.size;

  auto config_segment_size = get_conf<Option::size_t>(
    "seastore_segment_size");
  size_t raw_segments = size / config_segment_size;
  size_t tracker_size = SegmentStateTracker::get_raw_size(
    raw_segments,
    data.block_size);
  size_t segments = (size - tracker_size - data.block_size)
    / config_segment_size;
  return block_sm_superblock_t{
    size,
    config_segment_size,
    data.block_size,
    segments,
    data.block_size,
    tracker_size + data.block_size,
    meta
  };
}

using check_create_device_ertr = BlockSegmentManager::access_ertr;
using check_create_device_ret = check_create_device_ertr::future<>;
static check_create_device_ret check_create_device(
  const std::string &path,
  size_t size)
{
  logger().error(
    "block.cc:check_create_device path {}, size {}",
    path,
    size);
  return seastar::open_file_dma(
    path,
    seastar::open_flags::exclusive |
    seastar::open_flags::rw |
    seastar::open_flags::create
  ).then([size](auto file) {
    return seastar::do_with(
      file,
      [size](auto &f) -> seastar::future<> {
	logger().error(
	  "block.cc:check_create_device: created device, truncating to {}",
	  size);
	ceph_assert(f);
	return f.truncate(
	  size
	).then([&f, size] {
	  return f.allocate(0, size);
	}).finally([&f] {
	  return f.close();
	});
      });
  }).then_wrapped([&path](auto f) -> check_create_device_ret {
    logger().error(
      "block.cc:check_create_device: complete failed: {}",
      f.failed());
    if (f.failed()) {
      try {
	f.get();
	return seastar::now();
      } catch (const std::system_error &e) {
	if (e.code().value() == EEXIST) {
	  logger().error(
	    "block.cc:check_create_device device {} exists",
	    path);
	  return seastar::now();
	} else {
	  logger().error(
	    "block.cc:check_create_device device {} creation error {}",
	    path,
	    e);
	  return crimson::ct_error::input_output_error::make();
	}
      } catch (...) {
	return crimson::ct_error::input_output_error::make();
      }
    } else {
      std::ignore = f.discard_result();
      return seastar::now();
    }
  });
}

using open_device_ret = 
  BlockSegmentManager::access_ertr::future<
  std::pair<seastar::file, seastar::stat_data>
  >;
static
open_device_ret open_device(
  const std::string &path,
  seastar::open_flags mode)
{
  return seastar::file_stat(path, seastar::follow_symlink::yes
  ).then([mode, &path](auto stat) mutable {
    return seastar::open_file_dma(path, mode).then([=](auto file) {
      logger().error(
	"open_device: open successful, size {}",
	stat.size
      );
      return std::make_pair(file, stat);
    });
  }).handle_exception([](auto e) -> open_device_ret {
    logger().error(
      "open_device: got error {}",
      e);
    return crimson::ct_error::input_output_error::make();
  });
}

  
static
BlockSegmentManager::access_ertr::future<>
write_superblock(seastar::file &device, block_sm_superblock_t sb)
{
  assert(ceph::encoded_sizeof_bounded<block_sm_superblock_t>() <
	 sb.block_size);
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sb.block_size)),
    [=, &device](auto &bp) {
      bufferlist bl;
      encode(sb, bl);
      auto iter = bl.begin();
      assert(bl.length() < sb.block_size);
      iter.copy(bl.length(), bp.c_str());
      logger().debug("write_superblock: doing writeout");
      return do_write(device, 0, bp);
    });
}

static
BlockSegmentManager::access_ertr::future<block_sm_superblock_t>
read_superblock(seastar::file &device, seastar::stat_data sd)
{
  assert(ceph::encoded_sizeof_bounded<block_sm_superblock_t>() <
	 sd.block_size);
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sd.block_size)),
    [=, &device](auto &bp) {
      return do_read(
	device,
	0,
	bp
      ).safe_then([=, &bp] {
	  bufferlist bl;
	  bl.push_back(bp);
	  block_sm_superblock_t ret;
	  auto bliter = bl.cbegin();
	  decode(ret, bliter);
	  return BlockSegmentManager::access_ertr::future<block_sm_superblock_t>(
	    BlockSegmentManager::access_ertr::ready_future_marker{},
	    ret);
      });
    });
}

BlockSegment::BlockSegment(
  BlockSegmentManager &manager, segment_id_t id)
  : manager(manager), id(id) {}

segment_off_t BlockSegment::get_write_capacity() const
{
  return manager.get_segment_size();
}

Segment::close_ertr::future<> BlockSegment::close()
{
  return manager.segment_close(id);
}

Segment::write_ertr::future<> BlockSegment::write(
  segment_off_t offset, ceph::bufferlist bl)
{
  if (offset < write_pointer || offset % manager.superblock.block_size != 0)
    return crimson::ct_error::invarg::make();

  if (offset + bl.length() > manager.superblock.segment_size)
    return crimson::ct_error::enospc::make();

  write_pointer = offset + bl.length();
  return manager.segment_write({id, offset}, bl);
}

Segment::close_ertr::future<> BlockSegmentManager::segment_close(segment_id_t id)
{
  assert(tracker);
  tracker->set(id, segment_state_t::CLOSED);
  return tracker->write_out(device, superblock.tracker_offset);
}

Segment::write_ertr::future<> BlockSegmentManager::segment_write(
  paddr_t addr,
  ceph::bufferlist bl,
  bool ignore_check)
{
  assert((bl.length() % superblock.block_size) == 0);
  logger().debug(
    "segment_write to segment {} at offset {}, physical offset {}, len {}",
    addr.segment,
    addr.offset,
    get_offset(addr),
    bl.length());
  return do_writev(device, get_offset(addr), std::move(bl), superblock.block_size);
}

BlockSegmentManager::~BlockSegmentManager()
{
}

BlockSegmentManager::mount_ret BlockSegmentManager::mount()
{
  return open_device(
    device_path, seastar::open_flags::rw
  ).safe_then([=](auto p) {
    device = std::move(p.first);
    auto sd = p.second;
    return read_superblock(device, sd);
  }).safe_then([=](auto sb) {
    superblock = sb;
    tracker = std::make_unique<SegmentStateTracker>(
      superblock.segments,
      superblock.block_size);
    return tracker->read_in(
      device,
      superblock.tracker_offset
    ).safe_then([this] {
      for (segment_id_t i = 0; i < tracker->get_capacity(); ++i) {
	if (tracker->get(i) == segment_state_t::OPEN) {
	  tracker->set(i, segment_state_t::CLOSED);
	}
      }
      return tracker->write_out(device, superblock.tracker_offset);
    });
  });
}

BlockSegmentManager::mkfs_ret BlockSegmentManager::mkfs(seastore_meta_t meta)
{
  return seastar::do_with(
    seastar::file{},
    seastar::stat_data{},
    block_sm_superblock_t{},
    std::unique_ptr<SegmentStateTracker>(),
    [=](auto &device, auto &stat, auto &sb, auto &tracker) {
      logger().error("BlockSegmentManager::mkfs path {}", device_path);
      check_create_device_ret maybe_create = check_create_device_ertr::now();
      using crimson::common::get_conf;
      if (get_conf<bool>("seastore_block_create")) {
	auto size = get_conf<Option::size_t>("seastore_device_size");
	maybe_create = check_create_device(device_path, size);
      }

      return maybe_create.safe_then([this] {
	return open_device(device_path, seastar::open_flags::rw);
      }).safe_then([&, meta](auto p) {
	device = p.first;
	stat = p.second;
	sb = make_superblock(meta, stat);
	return write_superblock(device, sb);
      }).safe_then([&] {
	logger().debug("BlockSegmentManager::mkfs: superblock written");
	tracker.reset(new SegmentStateTracker(sb.segments, sb.block_size));
	return tracker->write_out(device, sb.tracker_offset);
      }).finally([&] {
	return device.close();
      }).safe_then([] {
	logger().debug("BlockSegmentManager::mkfs: complete");
	return mkfs_ertr::now();
      });
    });
}

BlockSegmentManager::close_ertr::future<> BlockSegmentManager::close()
{
  return device.close();
}

SegmentManager::open_ertr::future<SegmentRef> BlockSegmentManager::open(
  segment_id_t id)
{
  if (id >= get_num_segments()) {
    logger().error("BlockSegmentManager::open: invalid segment {}", id);
    return crimson::ct_error::invarg::make();
  }

  if (tracker->get(id) != segment_state_t::EMPTY) {
    logger().error(
      "BlockSegmentManager::open: invalid segment {} state {}",
      id,
      tracker->get(id));
    return crimson::ct_error::invarg::make();
  }

  tracker->set(id, segment_state_t::OPEN);
  return tracker->write_out(device, superblock.tracker_offset
  ).safe_then([this, id] {
    return open_ertr::future<SegmentRef>(
      open_ertr::ready_future_marker{},
      SegmentRef(new BlockSegment(*this, id)));
  });
}

SegmentManager::release_ertr::future<> BlockSegmentManager::release(
  segment_id_t id)
{
  logger().debug("BlockSegmentManager::release: {}", id);

  if (id >= get_num_segments()) {
    logger().error(
      "BlockSegmentManager::release: invalid segment {}",
      id);
    return crimson::ct_error::invarg::make();
  }

  if (tracker->get(id) != segment_state_t::CLOSED) {
    logger().error(
      "BlockSegmentManager::release: invalid segment {} state {}",
      id,
      tracker->get(id));
    return crimson::ct_error::invarg::make();
  }

  tracker->set(id, segment_state_t::EMPTY);
  return tracker->write_out(device, superblock.tracker_offset);
}

SegmentManager::read_ertr::future<> BlockSegmentManager::read(
  paddr_t addr,
  size_t len,
  ceph::bufferptr &out)
{
  if (addr.segment >= get_num_segments()) {
    logger().error(
      "BlockSegmentManager::read: invalid segment {}",
      addr);
    return crimson::ct_error::invarg::make();
  }

  if (addr.offset + len > superblock.segment_size) {
    logger().error(
      "BlockSegmentManager::read: invalid offset {}~{}!",
      addr,
      len);
    return crimson::ct_error::invarg::make();
  }

  if (tracker->get(addr.segment) == segment_state_t::EMPTY) {
    logger().error(
      "BlockSegmentManager::read: read on invalid segment {} state {}",
      addr.segment,
      tracker->get(addr.segment));
    return crimson::ct_error::enoent::make();
  }

  return do_read(
    device,
    get_offset(addr),
    out);
}

}
