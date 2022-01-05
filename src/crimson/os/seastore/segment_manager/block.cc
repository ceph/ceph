// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "include/buffer.h"

#include "crimson/common/config_proxy.h"
#include "crimson/common/errorator-loop.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/segment_manager/block.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore::segment_manager::block {

static write_ertr::future<> do_write(
  seastar::file &device,
  uint64_t offset,
  bufferptr &bptr)
{
  LOG_PREFIX(block_do_write);
  DEBUG("offset {}, len {}", offset, bptr.length());
  return device.dma_write(
    offset,
    bptr.c_str(),
    bptr.length()
  ).handle_exception([FNAME](auto e) -> write_ertr::future<size_t> {
    ERROR("dma_write got error {}", e);
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
  LOG_PREFIX(block_do_writev);
  DEBUG("offset {}, len {}, {} buffers",
        offset, bl.length(), bl.get_num_buffers());

  // writev requires each buffer to be aligned to the disks' block
  // size, we need to rebuild here
  bl.rebuild_aligned(block_size);

  return seastar::do_with(
    bl.prepare_iovs(),
    std::move(bl),
    [&device, offset, FNAME](auto& iovs, auto& bl)
  {
    return write_ertr::parallel_for_each(
      iovs,
      [&device, offset, FNAME](auto& p) mutable
    {
      auto off = offset + p.offset;
      auto len = p.length;
      auto& iov = p.iov;
      DEBUG("dma_write to offset {}, len {}", off, len);
      return device.dma_write(off, std::move(iov)
      ).handle_exception([FNAME](auto e) -> write_ertr::future<size_t> {
        ERROR("dma_write got error {}", e);
	return crimson::ct_error::input_output_error::make();
      }).then([off, len, FNAME](size_t written) -> write_ertr::future<> {
	if (written != len) {
          ERROR("dma_write to offset {} failed, written {} != iov-len {}",
                off, written, len);
	  return crimson::ct_error::input_output_error::make();
	}
	return write_ertr::now();
      });
    });
  });
}

static read_ertr::future<> do_read(
  seastar::file &device,
  uint64_t offset,
  size_t len,
  bufferptr &bptr)
{
  LOG_PREFIX(block_do_read);
  DEBUG("offset {}, len {}", offset, len);
  assert(len <= bptr.length());
  return device.dma_read(
    offset,
    bptr.c_str(),
    len
  ).handle_exception(
    //FIXME: this is a little bit tricky, since seastar::future<T>::handle_exception
    //	returns seastar::future<T>, to return an crimson::ct_error, we have to create
    //	a seastar::future<T> holding that crimson::ct_error. This is not necessary
    //	once seastar::future<T>::handle_exception() returns seastar::futurize_t<T>
    [FNAME](auto e) -> read_ertr::future<size_t>
  {
    ERROR("dma_read got error {}", e);
    return crimson::ct_error::input_output_error::make();
  }).then([len](auto result) -> read_ertr::future<> {
    if (result != len) {
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
    bptr.length(),
    bptr);
}

static
block_sm_superblock_t make_superblock(
  segment_manager_config_t sm_config,
  const seastar::stat_data &data)
{
  LOG_PREFIX(block_make_superblock);

  using crimson::common::get_conf;

  auto config_size = get_conf<Option::size_t>(
    "seastore_device_size");

  size_t size = (data.size == 0) ? config_size : data.size;

  auto config_segment_size = get_conf<Option::size_t>(
    "seastore_segment_size");
  size_t raw_segments = size / config_segment_size;
  size_t tracker_size = SegmentStateTracker::get_raw_size(
    raw_segments,
    data.block_size);
  size_t segments = (size - tracker_size - data.block_size)
    / config_segment_size;

  DEBUG(
    "size {}, block_size {}, allocated_size {}, configured_size {}, "
    "segment_size {}",
    data.size,
    data.block_size,
    data.allocated_size,
    config_size,
    config_segment_size
  );

  return block_sm_superblock_t{
    size,
    config_segment_size,
    data.block_size,
    segments,
    data.block_size,
    tracker_size + data.block_size,
    sm_config.major_dev,
    sm_config.magic,
    sm_config.dtype,
    sm_config.device_id,
    sm_config.meta,
    std::move(sm_config.secondary_devices)
  };
}

using check_create_device_ertr = BlockSegmentManager::access_ertr;
using check_create_device_ret = check_create_device_ertr::future<>;
static check_create_device_ret check_create_device(
  const std::string &path,
  size_t size)
{
  LOG_PREFIX(block_check_create_device);
  ERROR("path {}, size {}", path, size);
  return seastar::open_file_dma(
    path,
    seastar::open_flags::exclusive |
    seastar::open_flags::rw |
    seastar::open_flags::create
  ).then([size, FNAME](auto file) {
    return seastar::do_with(
      file,
      [size, FNAME](auto &f) -> seastar::future<>
    {
      ERROR("created device, truncating to {}", size);
      ceph_assert(f);
      return f.truncate(
        size
      ).then([&f, size] {
        return f.allocate(0, size);
      }).finally([&f] {
        return f.close();
      });
    });
  }).then_wrapped([&path, FNAME](auto f) -> check_create_device_ret {
    ERROR("complete failed: {}", f.failed());
    if (f.failed()) {
      try {
	f.get();
	return seastar::now();
      } catch (const std::system_error &e) {
	if (e.code().value() == EEXIST) {
          ERROR("device {} exists", path);
	  return seastar::now();
	} else {
          ERROR("device {} creation error {}", path, e);
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
  const std::string &path)
{
  LOG_PREFIX(block_open_device);
  return seastar::file_stat(path, seastar::follow_symlink::yes
  ).then([&path, FNAME](auto stat) mutable {
    return seastar::open_file_dma(
      path,
      seastar::open_flags::rw | seastar::open_flags::dsync
    ).then([=](auto file) {
      ERROR("open successful, size {}", stat.size);
      return std::make_pair(file, stat);
    });
  }).handle_exception([FNAME](auto e) -> open_device_ret {
    ERROR("got error {}", e);
    return crimson::ct_error::input_output_error::make();
  });
}


static
BlockSegmentManager::access_ertr::future<>
write_superblock(seastar::file &device, block_sm_superblock_t sb)
{
  LOG_PREFIX(block_write_superblock);
  assert(ceph::encoded_sizeof<block_sm_superblock_t>(sb) <
	 sb.block_size);
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sb.block_size)),
    [=, &device](auto &bp)
  {
    bufferlist bl;
    encode(sb, bl);
    auto iter = bl.begin();
    assert(bl.length() < sb.block_size);
    iter.copy(bl.length(), bp.c_str());
    DEBUG("doing writeout");
    return do_write(device, 0, bp);
  });
}

static
BlockSegmentManager::access_ertr::future<block_sm_superblock_t>
read_superblock(seastar::file &device, seastar::stat_data sd)
{
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sd.block_size)),
    [=, &device](auto &bp) {
      return do_read(
	device,
	0,
	bp.length(),
	bp
      ).safe_then([=, &bp] {
	  bufferlist bl;
	  bl.push_back(bp);
	  block_sm_superblock_t ret;
	  auto bliter = bl.cbegin();
	  try {
	    decode(ret, bliter);
	  } catch (...) {
	    ceph_assert(0 == "invalid superblock");
	  }
	  assert(ceph::encoded_sizeof<block_sm_superblock_t>(ret) <
		 sd.block_size);
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
  return manager.segment_close(id, write_pointer);
}

Segment::write_ertr::future<> BlockSegment::write(
  segment_off_t offset, ceph::bufferlist bl)
{
  LOG_PREFIX(BlockSegment::write);
  if (offset < write_pointer || offset % manager.superblock.block_size != 0) {
    ERROR("invalid segment write on segment {} to offset {}", id, offset);
    return crimson::ct_error::invarg::make();
  }

  if (offset + bl.length() > manager.superblock.segment_size)
    return crimson::ct_error::enospc::make();

  write_pointer = offset + bl.length();
  return manager.segment_write(paddr_t::make_seg_paddr(id, offset), bl);
}

Segment::close_ertr::future<> BlockSegmentManager::segment_close(
    segment_id_t id, segment_off_t write_pointer)
{
  assert(id.device_id() == superblock.device_id);
  auto s_id = id.device_segment_id();
  assert(tracker);
  tracker->set(s_id, segment_state_t::CLOSED);
  ++stats.closed_segments;
  int unused_bytes = get_segment_size() - write_pointer;
  assert(unused_bytes >= 0);
  stats.closed_segments_unused_bytes += unused_bytes;
  stats.metadata_write.increment(tracker->get_size());
  return tracker->write_out(device, superblock.tracker_offset);
}

Segment::write_ertr::future<> BlockSegmentManager::segment_write(
  paddr_t addr,
  ceph::bufferlist bl,
  bool ignore_check)
{
  LOG_PREFIX(BlockSegmentManager::segment_write);
  auto& seg_addr = addr.as_seg_paddr();
  DEBUG("segment_write to segment {} at offset {}, physical offset {}, len {}",
    seg_addr.get_segment_id(),
    seg_addr.get_segment_off(),
    get_offset(addr),
    bl.length());

  assert(addr.get_device_id() == get_device_id());
  assert((bl.length() % superblock.block_size) == 0);
  stats.data_write.increment(bl.length());
  return do_writev(device, get_offset(addr), std::move(bl), superblock.block_size);
}

BlockSegmentManager::~BlockSegmentManager()
{
}

BlockSegmentManager::mount_ret BlockSegmentManager::mount()
{
  LOG_PREFIX(BlockSegmentManager::mount);
  return open_device(
    device_path
  ).safe_then([=](auto p) {
    device = std::move(p.first);
    auto sd = p.second;
    return read_superblock(device, sd);
  }).safe_then([=](auto sb) {
    superblock = sb;
    stats.data_read.increment(
        ceph::encoded_sizeof<block_sm_superblock_t>(superblock));
    tracker = std::make_unique<SegmentStateTracker>(
      superblock.segments,
      superblock.block_size);
    stats.data_read.increment(tracker->get_size());
    return tracker->read_in(
      device,
      superblock.tracker_offset
    ).safe_then([this] {
      for (device_segment_id_t i = 0; i < tracker->get_capacity(); ++i) {
	if (tracker->get(i) == segment_state_t::OPEN) {
	  tracker->set(i, segment_state_t::CLOSED);
	}
      }
      stats.metadata_write.increment(tracker->get_size());
      return tracker->write_out(device, superblock.tracker_offset);
    });
  }).safe_then([this, FNAME] {
    DEBUG("device {} mounted", get_device_id());
    register_metrics();
  });
}

BlockSegmentManager::mkfs_ret BlockSegmentManager::mkfs(
  segment_manager_config_t sm_config)
{
  LOG_PREFIX(BlockSegmentManager::mkfs);
  DEBUG("magic={}, dtype={}, id={}",
    sm_config.magic, sm_config.dtype, sm_config.device_id);
  return seastar::do_with(
    seastar::file{},
    seastar::stat_data{},
    block_sm_superblock_t{},
    std::unique_ptr<SegmentStateTracker>(),
    [=](auto &device, auto &stat, auto &sb, auto &tracker)
  {
    ERROR("path {}", device_path);
    check_create_device_ret maybe_create = check_create_device_ertr::now();
    using crimson::common::get_conf;
    if (get_conf<bool>("seastore_block_create")) {
      auto size = get_conf<Option::size_t>("seastore_device_size");
      maybe_create = check_create_device(device_path, size);
    }

    return maybe_create.safe_then([this] {
      return open_device(device_path);
    }).safe_then([&, sm_config](auto p) {
      device = p.first;
      stat = p.second;
      sb = make_superblock(sm_config, stat);
      stats.metadata_write.increment(
          ceph::encoded_sizeof<block_sm_superblock_t>(sb));
      return write_superblock(device, sb);
    }).safe_then([&, FNAME] {
      DEBUG("superblock written");
      tracker.reset(new SegmentStateTracker(sb.segments, sb.block_size));
      stats.metadata_write.increment(tracker->get_size());
      return tracker->write_out(device, sb.tracker_offset);
    }).finally([&] {
      return device.close();
    }).safe_then([FNAME] {
      DEBUG("complete");
      return mkfs_ertr::now();
    });
  });
}

BlockSegmentManager::close_ertr::future<> BlockSegmentManager::close()
{
  LOG_PREFIX(BlockSegmentManager::close);
  DEBUG("closing device {}", get_device_id());
  metrics.clear();
  return device.close();
}

SegmentManager::open_ertr::future<SegmentRef> BlockSegmentManager::open(
  segment_id_t id)
{
  LOG_PREFIX(BlockSegmentManager::open);
  assert(id.device_id() == superblock.device_id);
  auto s_id = id.device_segment_id();
  if (s_id >= get_num_segments()) {
    ERROR("invalid segment {}", id);
    return crimson::ct_error::invarg::make();
  }

  if (tracker->get(s_id) != segment_state_t::EMPTY) {
    ERROR("invalid segment {} state {}", id, tracker->get(s_id));
    return crimson::ct_error::invarg::make();
  }

  tracker->set(s_id, segment_state_t::OPEN);
  stats.metadata_write.increment(tracker->get_size());
  return tracker->write_out(device, superblock.tracker_offset
  ).safe_then([this, id] {
    ++stats.opened_segments;
    return open_ertr::future<SegmentRef>(
      open_ertr::ready_future_marker{},
      SegmentRef(new BlockSegment(*this, id)));
  });
}

SegmentManager::release_ertr::future<> BlockSegmentManager::release(
  segment_id_t id)
{
  LOG_PREFIX(BlockSegmentManager::release);
  DEBUG("releasing segment {}", id);
  assert(id.device_id() == superblock.device_id);

  auto s_id = id.device_segment_id();
  if (s_id >= get_num_segments()) {
    ERROR("invalid segment {}", id);
    return crimson::ct_error::invarg::make();
  }

  if (tracker->get(s_id) != segment_state_t::CLOSED) {
    ERROR("invalid segment {} state {}", id, tracker->get(s_id));
    return crimson::ct_error::invarg::make();
  }

  tracker->set(s_id, segment_state_t::EMPTY);
  ++stats.released_segments;
  stats.metadata_write.increment(tracker->get_size());
  return tracker->write_out(device, superblock.tracker_offset);
}

SegmentManager::read_ertr::future<> BlockSegmentManager::read(
  paddr_t addr,
  size_t len,
  ceph::bufferptr &out)
{
  LOG_PREFIX(BlockSegmentManager::read);
  assert(addr.get_device_id() == get_device_id());
  auto& seg_addr = addr.as_seg_paddr();
  if (seg_addr.get_segment_id().device_segment_id() >= get_num_segments()) {
    ERROR("invalid segment {}", addr);
    return crimson::ct_error::invarg::make();
  }

  if (seg_addr.get_segment_off() + len > superblock.segment_size) {
    ERROR("invalid offset {}~{}", addr, len);
    return crimson::ct_error::invarg::make();
  }

  if (tracker->get(seg_addr.get_segment_id().device_segment_id()) == 
      segment_state_t::EMPTY) {
    ERROR("invalid segment {} state {}",
      seg_addr.get_segment_id(),
      tracker->get(seg_addr.get_segment_id().device_segment_id()));
    return crimson::ct_error::enoent::make();
  }

  stats.data_read.increment(len);
  return do_read(
    device,
    get_offset(addr),
    len,
    out);
}

void BlockSegmentManager::register_metrics()
{
  LOG_PREFIX(BlockSegmentManager::register_metrics);
  DEBUG("device {}", get_device_id());
  namespace sm = seastar::metrics;
  sm::label label("device_id");
  std::vector<sm::label_instance> label_instances;
  label_instances.push_back(label(get_device_id()));
  stats.reset();
  metrics.add_group(
    "segment_manager",
    {
      sm::make_counter(
        "data_read_num",
        stats.data_read.num,
        sm::description("total number of data read"),
	label_instances
      ),
      sm::make_counter(
        "data_read_bytes",
        stats.data_read.bytes,
        sm::description("total bytes of data read"),
	label_instances
      ),
      sm::make_counter(
        "data_write_num",
        stats.data_write.num,
        sm::description("total number of data write"),
	label_instances
      ),
      sm::make_counter(
        "data_write_bytes",
        stats.data_write.bytes,
        sm::description("total bytes of data write"),
	label_instances
      ),
      sm::make_counter(
        "metadata_write_num",
        stats.metadata_write.num,
        sm::description("total number of metadata write"),
	label_instances
      ),
      sm::make_counter(
        "metadata_write_bytes",
        stats.metadata_write.bytes,
        sm::description("total bytes of metadata write"),
	label_instances
      ),
      sm::make_counter(
        "opened_segments",
        stats.opened_segments,
        sm::description("total segments opened"),
	label_instances
      ),
      sm::make_counter(
        "closed_segments",
        stats.closed_segments,
        sm::description("total segments closed"),
	label_instances
      ),
      sm::make_counter(
        "closed_segments_unused_bytes",
        stats.closed_segments_unused_bytes,
        sm::description("total unused bytes of closed segments"),
	label_instances
      ),
      sm::make_counter(
        "released_segments",
        stats.released_segments,
        sm::description("total segments released"),
	label_instances
      ),
    }
  );
}

}
