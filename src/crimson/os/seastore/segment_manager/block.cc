// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <sys/mman.h>
#include <string.h>

#include <boost/range/irange.hpp>

#include <fmt/format.h>

#include <seastar/core/metrics.hh>
#include <seastar/util/defer.hh>

#include "include/buffer.h"

#include "crimson/common/config_proxy.h"
#include "crimson/common/errorator-utils.h"
#include "crimson/common/coroutine.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/segment_manager/block.h"

SET_SUBSYS(seastore_device);
/*
 * format:
 * - D<device-id> S<segment-id> offset=<off>~<len> poffset=<off> information
 * - D<device-id> poffset=<off>~<len> information
 *
 * levels:
 * - INFO:  major initiation, closing and segment operations
 * - DEBUG: INFO details, major read and write operations
 * - TRACE: DEBUG details
 */

using segment_state_t = crimson::os::seastore::Segment::segment_state_t;

template <> struct fmt::formatter<segment_state_t>: fmt::formatter<std::string_view> {
  // parse is inherited from formatter<string_view>.
  template <typename FormatContext>
  auto format(segment_state_t s, FormatContext& ctx) const {
    std::string_view name = "unknown";
    switch (s) {
    case segment_state_t::EMPTY:
      name = "empty";
      break;
    case segment_state_t::OPEN:
      name = "open";
      break;
    case segment_state_t::CLOSED:
      name = "closed";
      break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};

namespace crimson::os::seastore::segment_manager::block {

write_ertr::future<>
SegmentStateTracker::write_out(
  device_id_t device_id,
  BlockIODriver &driver,
  uint64_t offset)
{
  LOG_PREFIX(SegmentStateTracker::write_out);
  DEBUG("{} poffset=0x{:x}~0x{:x}",
        device_id_printer_t{device_id}, offset, bptr.length());
  return driver.write(device_id, offset, bptr);
}

write_ertr::future<>
SegmentStateTracker::read_in(
  device_id_t device_id,
  BlockIODriver &driver,
  uint64_t offset)
{
  LOG_PREFIX(SegmentStateTracker::read_in);
  DEBUG("{} poffset=0x{:x}~0x{:x}",
        device_id_printer_t{device_id}, offset, bptr.length());
  return driver.read(
    device_id,
    offset,
    bptr.length(),
    bptr);
}
using std::vector;
static
device_superblock_t make_superblock(
  device_id_t device_id,
  device_config_t sm_config,
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
  size_t shard_tracker_size = SegmentStateTracker::get_raw_size(
    raw_segments / seastar::smp::count,
    data.block_size);
  size_t total_tracker_size = shard_tracker_size * seastar::smp::count;
  size_t tracker_off = data.block_size;   //superblock
  size_t segments = (size - tracker_off - total_tracker_size) / config_segment_size;
  size_t segments_per_shard = segments / seastar::smp::count;

  vector<device_shard_info_t> shard_infos(seastar::smp::count);
  for (unsigned int i = 0; i < seastar::smp::count; i++) {
    shard_infos[i].size = segments_per_shard * config_segment_size;
    shard_infos[i].segments = segments_per_shard;
    shard_infos[i].tracker_offset = tracker_off + i * shard_tracker_size;
    shard_infos[i].first_segment_offset = tracker_off + total_tracker_size
                             + i * segments_per_shard * config_segment_size;
  }

  INFO("{} disk_size=0x{:x}, segment_size=0x{:x}, block_size=0x{:x}",
       device_id_printer_t{device_id},
       size,
       uint64_t(config_segment_size),
       data.block_size);
  for (unsigned int i = 0; i < seastar::smp::count; i++) {
    INFO("shard {} infos: {}", i, shard_infos[i]);
  }

  return device_superblock_t::make_segmented(
    seastar::smp::count,
    config_segment_size,
    data.block_size,
    std::move(sm_config),
    std::move(shard_infos));
}

static
BlockSegmentManager::access_ertr::future<>
write_superblock(
    device_id_t device_id,
    BlockIODriver &driver,
    device_superblock_t sb)
{
  LOG_PREFIX(block_write_superblock);
  DEBUG("{} write {}", device_id_printer_t{device_id}, sb);
  sb.validate();
  assert(SUPERBLOCK_HEADER_PREFIX +
	 ceph::encoded_sizeof<device_superblock_t>(sb) < sb.block_size);
  return seastar::do_with(
    bufferptr(driver.alloc_io_buffer(sb.block_size)),
    [=, &driver](auto &bp)
  {
    bp.zero();
    // magic at offset 0, followed by 37 bytes of null padding (already zero)
    std::memcpy(bp.c_str(),
		CRIMSON_DEVICE_SUPERBLOCK_MAGIC.data(), SUPERBLOCK_MAGIC_SIZE);
    // DENC-encoded superblock at offset 60
    bufferlist bl;
    encode(sb, bl);
    auto iter = bl.begin();
    assert(SUPERBLOCK_HEADER_PREFIX + bl.length() < sb.block_size);
    iter.copy(bl.length(), bp.c_str() + SUPERBLOCK_HEADER_PREFIX);
    return driver.write(device_id, 0, bp);
  });
}

static
BlockSegmentManager::access_ertr::future<device_superblock_t>
read_superblock(BlockIODriver &driver, seastar::stat_data sd)
{
  LOG_PREFIX(block_read_superblock);
  DEBUG("reading superblock ...");
  return seastar::do_with(
    bufferptr(driver.alloc_io_buffer(sd.block_size)),
    [=, &driver](auto &bp)
  {
    return driver.read(
      DEVICE_ID_NULL, // unknown
      0,
      bp.length(),
      bp
    ).safe_then([=, &bp] {
      // verify magic at offset 0
      superblock_magic_t disk_magic;
      std::memcpy(disk_magic.data(), bp.c_str(), SUPERBLOCK_MAGIC_SIZE);
      if (disk_magic != CRIMSON_DEVICE_SUPERBLOCK_MAGIC) {
        ERROR("invalid superblock magic, got: {:02x}",
          fmt::join(
            std::views::transform(disk_magic,
              [](std::byte b) { return std::to_integer<uint8_t>(b); }),
            " "));
        ceph_abort_msg("invalid superblock magic");
      }
      // decode DENC superblock from offset 60
      bufferlist bl;
      bl.append(bp.c_str() + SUPERBLOCK_HEADER_PREFIX,
		bp.length() - SUPERBLOCK_HEADER_PREFIX);
      device_superblock_t ret;
      auto bliter = bl.cbegin();
      try {
        decode(ret, bliter);
      } catch (...) {
        ERROR("got decode error!");
        ceph_assert(0 == "invalid superblock");
      }
      assert(SUPERBLOCK_HEADER_PREFIX +
	     ceph::encoded_sizeof<device_superblock_t>(ret) <= sd.block_size);
      return BlockSegmentManager::access_ertr::future<device_superblock_t>(
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
  auto paddr = paddr_t::make_seg_paddr(id, offset);
  DEBUG("{} offset=0x{:x}~0x{:x} poffset=0x{:x} ...",
        id, offset, bl.length(), manager.get_offset(paddr));

  if (offset < write_pointer ||
      offset % manager.superblock.block_size != 0 ||
      bl.length() % manager.superblock.block_size != 0) {
    ERROR("{} offset=0x{:x}~0x{:x} poffset=0x{:x} invalid write",
          id, offset, bl.length(), manager.get_offset(paddr));
    return crimson::ct_error::invarg::make();
  }

  if (offset + bl.length() > manager.superblock.segment_size) {
    ERROR("{} offset=0x{:x}~0x{:x} poffset=0x{:x} write out of the range 0x{:x}",
          id, offset, bl.length(), manager.get_offset(paddr),
          manager.superblock.segment_size);
    return crimson::ct_error::enospc::make();
  }

  write_pointer = offset + bl.length();
  return manager.segment_write(paddr, bl);
}

Segment::write_ertr::future<> BlockSegment::advance_wp(
  segment_off_t offset) {
  return write_ertr::now();
}

Segment::close_ertr::future<> BlockSegmentManager::segment_close(
    segment_id_t id, segment_off_t write_pointer)
{
  LOG_PREFIX(BlockSegmentManager::segment_close);
  auto s_id = id.device_segment_id();
  int unused_bytes = get_segment_size() - write_pointer;
  INFO("{} unused_bytes=0x{:x} ...", id, unused_bytes);

  assert(unused_bytes >= 0);
  assert(id.device_id() == get_device_id());
  assert(tracker);

  tracker->set(s_id, segment_state_t::CLOSED);
  ++stats.closed_segments;
  stats.closed_segments_unused_bytes += unused_bytes;
  stats.metadata_write.increment(tracker->get_size());
  return tracker->write_out(
      get_device_id(), *driver,
      shard_info.tracker_offset);
}

Segment::write_ertr::future<> BlockSegmentManager::segment_write(
  paddr_t addr,
  ceph::bufferlist bl,
  bool ignore_check)
{
  assert(addr.get_device_id() == get_device_id());
  assert((bl.length() % superblock.block_size) == 0);
  stats.data_write.increment(bl.length());
  return driver->writev(
      get_device_id(),
      get_offset(addr),
      std::move(bl),
      superblock.block_size);
}

BlockSegmentManager::~BlockSegmentManager()
{
}

seastar::future<> BlockSegmentManager::start(uint32_t shard_nums)
{
  LOG_PREFIX(BlockSegmentManager::start);
  device_shard_nums = shard_nums;
  auto num_shard_services = (device_shard_nums + seastar::smp::count - 1 ) / seastar::smp::count;
  INFO("device_shard_nums={} seastar::smp={}, num_shard_services={}", device_shard_nums, seastar::smp::count, num_shard_services);
  return shard_devices.start(num_shard_services, device_path, superblock.config.spec.dtype);

}

seastar::future<> BlockSegmentManager::stop()
{
  return shard_devices.stop();
}

Device& BlockSegmentManager::get_sharded_device(store_index_t store_index)
{
  assert(store_index < shard_devices.local().mshard_devices.size());
  return *shard_devices.local().mshard_devices[store_index];
}

BlockSegmentManager::access_ertr::future<seastar::stat_data>
BlockSegmentManager::open_driver()
{
  driver = make_block_io_driver(device_path, superblock.config.spec.dtype);
  return driver->open(
    device_path,
    seastar::open_flags::rw | seastar::open_flags::dsync);
}

SegmentManager::read_ertr::future<uint32_t> BlockSegmentManager::get_shard_nums()
{
  return open_driver(
  ).safe_then([this](auto sd) {
    return read_superblock(*driver, sd);
  }).safe_then([](auto sb) {
    return read_ertr::make_ready_future<uint32_t>(sb.shard_num);
  }).handle_error(
    crimson::ct_error::assert_all(
      "Invalid error in BlockSegmentManager::get_shard_nums"
    )
  );
}

BlockSegmentManager::mount_ret BlockSegmentManager::mount()
{
  return shard_devices.invoke_on_all([](auto &local_device) {
    return seastar::do_for_each(local_device.mshard_devices, [](auto& mshard_device) {
      return mshard_device->shard_mount(
      ).handle_error(
        crimson::ct_error::assert_all(
          "Invalid error in BlockSegmentManager::mount"
      ));
    });
  });
}

BlockSegmentManager::mount_ret BlockSegmentManager::shard_mount()
{
  LOG_PREFIX(BlockSegmentManager::shard_mount);
  return open_driver(
  ).safe_then([=, this](auto sd) {
    return read_superblock(*driver, sd);
  }).safe_then([=, this](auto sb) ->mount_ertr::future<> {
    set_device_id(sb.config.spec.id);
    if(seastar::this_shard_id() + seastar::smp::count * store_index >= sb.shard_num) {
      INFO("{} shard_id {} out of range {}",
      device_id_printer_t{get_device_id()},
        seastar::this_shard_id() + seastar::smp::count * store_index,
        sb.shard_num);
      shard_status = false;
      return mount_ertr::now();
    }
    shard_info = sb.shard_infos[seastar::this_shard_id() + seastar::smp::count * store_index];
    INFO("{} read {}", device_id_printer_t{get_device_id()}, shard_info);
    sb.validate();
    superblock = sb;
    stats.data_read.increment(
        ceph::encoded_sizeof<device_superblock_t>(superblock));
    tracker = std::make_unique<SegmentStateTracker>(
      shard_info.segments,
      superblock.block_size);
    stats.data_read.increment(tracker->get_size());
    return tracker->read_in(
      get_device_id(),
      *driver,
      shard_info.tracker_offset
    ).safe_then([this] {
      for (device_segment_id_t i = 0; i < tracker->get_capacity(); ++i) {
	if (tracker->get(i) == segment_state_t::OPEN) {
	  tracker->set(i, segment_state_t::CLOSED);
	}
      }
      stats.metadata_write.increment(tracker->get_size());
      return tracker->write_out(
          get_device_id(), *driver,
          shard_info.tracker_offset);
    });
  }).safe_then([this, FNAME] {
    INFO("{} complete", device_id_printer_t{get_device_id()});
    register_metrics(store_index);
  });
}

BlockSegmentManager::mkfs_ret BlockSegmentManager::mkfs(
  device_config_t sm_config)
{
  return shard_devices.local().mshard_devices[0]->primary_mkfs(sm_config
  ).safe_then([this] {
    return shard_devices.invoke_on_all([](auto &local_device) {
      return seastar::do_for_each(local_device.mshard_devices, [](auto& mshard_device) {
        return mshard_device->shard_mkfs(
        ).handle_error(
          crimson::ct_error::assert_all(
            "Invalid error in BlockSegmentManager::mkfs"
        ));
      });
    });
  });
}

BlockSegmentManager::mkfs_ret BlockSegmentManager::primary_mkfs(
  device_config_t sm_config)
{
  LOG_PREFIX(BlockSegmentManager::primary_mkfs);
  ceph_assert(sm_config.spec.dtype == superblock.config.spec.dtype);
  set_device_id(sm_config.spec.id);
  INFO("{} path={}, {}",
       device_id_printer_t{get_device_id()}, device_path, sm_config);

  seastar::stat_data stat;
  device_superblock_t sb;

  using crimson::common::get_conf;
  if (get_conf<bool>("seastore_block_create")) {
    auto size = get_conf<Option::size_t>("seastore_device_size");
     co_await check_create_device(device_path, size);
  }
  auto local_driver = make_block_io_driver(
    device_path, superblock.config.spec.dtype);
  stat = co_await local_driver->open(
    device_path,
    seastar::open_flags::rw | seastar::open_flags::dsync);
  auto closer = seastar::defer([&local_driver] {
    std::ignore = local_driver->close();
  });
  sb = make_superblock(get_device_id(), sm_config, stat);
  stats.metadata_write.increment(ceph::encoded_sizeof<device_superblock_t>(sb));
  co_await write_superblock(get_device_id(), *local_driver, sb);
  INFO("{} complete", device_id_printer_t{get_device_id()});
}

BlockSegmentManager::mkfs_ret BlockSegmentManager::shard_mkfs()
{
  LOG_PREFIX(BlockSegmentManager::shard_mkfs);
  return open_driver(
  ).safe_then([this](auto sd) {
    return read_superblock(*driver, sd);
  }).safe_then([this, FNAME](auto sb) {
    set_device_id(sb.config.spec.id);
    shard_info = sb.shard_infos[seastar::this_shard_id()];
    INFO("{} read {}", device_id_printer_t{get_device_id()}, shard_info);
    sb.validate();
    tracker.reset(new SegmentStateTracker(
      shard_info.segments, sb.block_size));
    stats.metadata_write.increment(tracker->get_size());
    return tracker->write_out(
      get_device_id(), *driver,
      shard_info.tracker_offset);
  }).finally([this] {
    return driver->close();
  }).safe_then([FNAME, this] {
    INFO("{} complete", device_id_printer_t{get_device_id()});
    return mkfs_ertr::now();
  });
}

BlockSegmentManager::close_ertr::future<> BlockSegmentManager::close()
{
  LOG_PREFIX(BlockSegmentManager::close);
  INFO("{}", device_id_printer_t{get_device_id()});
  metrics.clear();
  return driver->close();
}

SegmentManager::open_ertr::future<SegmentRef> BlockSegmentManager::open(
  segment_id_t id)
{
  LOG_PREFIX(BlockSegmentManager::open);
  auto s_id = id.device_segment_id();
  INFO("{} ...", id);

  assert(id.device_id() == get_device_id());

  if (s_id >= get_num_segments()) {
    ERROR("{} segment-id out of range {}", id, get_num_segments());
    return crimson::ct_error::invarg::make();
  }

  if (tracker->get(s_id) != segment_state_t::EMPTY) {
    ERROR("{} invalid state {} != EMPTY", id, tracker->get(s_id));
    return crimson::ct_error::invarg::make();
  }

  tracker->set(s_id, segment_state_t::OPEN);
  stats.metadata_write.increment(tracker->get_size());
  return tracker->write_out(
      get_device_id(), *driver,
      shard_info.tracker_offset
  ).safe_then([this, id, FNAME] {
    ++stats.opened_segments;
    DEBUG("{} done", id);
    return open_ertr::future<SegmentRef>(
      open_ertr::ready_future_marker{},
      SegmentRef(new BlockSegment(*this, id)));
  });
}

SegmentManager::release_ertr::future<> BlockSegmentManager::release(
  segment_id_t id)
{
  LOG_PREFIX(BlockSegmentManager::release);
  auto s_id = id.device_segment_id();
  INFO("{} ...", id);

  assert(id.device_id() == get_device_id());

  if (s_id >= get_num_segments()) {
    ERROR("{} segment-id out of range {}", id, get_num_segments());
    return crimson::ct_error::invarg::make();
  }

  if (tracker->get(s_id) != segment_state_t::CLOSED) {
    ERROR("{} invalid state {} != CLOSED", id, tracker->get(s_id));
    return crimson::ct_error::invarg::make();
  }

  tracker->set(s_id, segment_state_t::EMPTY);
  ++stats.released_segments;
  stats.metadata_write.increment(tracker->get_size());
  return tracker->write_out(
      get_device_id(), *driver,
      shard_info.tracker_offset);
}

SegmentManager::read_ertr::future<> BlockSegmentManager::readv(
  paddr_t addr,
  std::vector<bufferptr> ptrs)
{
  LOG_PREFIX(BlockSegmentManager::readv);
  size_t len = 0;
  for (auto &ptr : ptrs) {
    len += ptr.length();
  }
  auto& seg_addr = addr.as_seg_paddr();
  auto id = seg_addr.get_segment_id();
  auto s_id = id.device_segment_id();
  auto s_off = seg_addr.get_segment_off();
  auto p_off = get_offset(addr);
  DEBUG("{} offset=0x{:x}~0x{:x} poffset=0x{:x} ...", id, s_off, len, p_off);

  assert(addr.get_device_id() == get_device_id());

  if (s_off % superblock.block_size != 0 ||
      len % superblock.block_size != 0) {
    ERROR("{} offset=0x{:x}~0x{:x} poffset=0x{:x} invalid read", id, s_off, len, p_off);
    return crimson::ct_error::invarg::make();
  }

  if (s_id >= get_num_segments()) {
    ERROR("{} offset=0x{:x}~0x{:x} poffset=0x{:x} segment-id out of range {}",
          id, s_off, len, p_off, get_num_segments());
    return crimson::ct_error::invarg::make();
  }

  if (s_off + len > superblock.segment_size) {
    ERROR("{} offset=0x{:x}~0x{:x} poffset=0x{:x} read out of range 0x{:x}",
          id, s_off, len, p_off, superblock.segment_size);
    return crimson::ct_error::invarg::make();
  }

  if (tracker->get(s_id) == segment_state_t::EMPTY) {
    // XXX: not an error during scanning,
    // might need refactor to increase the log level
    DEBUG("{} offset=0x{:x}~0x{:x} poffset=0x{:x} invalid state {}",
          id, s_off, len, p_off, tracker->get(s_id));
    return crimson::ct_error::enoent::make();
  }

  stats.data_read.increment(len);
  return driver->readv(
    get_device_id(),
    p_off,
    std::move(ptrs));
}

SegmentManager::read_ertr::future<> BlockSegmentManager::read(
  paddr_t addr,
  size_t len,
  ceph::bufferptr &out)
{
  LOG_PREFIX(BlockSegmentManager::read);
  auto& seg_addr = addr.as_seg_paddr();
  auto id = seg_addr.get_segment_id();
  auto s_id = id.device_segment_id();
  auto s_off = seg_addr.get_segment_off();
  auto p_off = get_offset(addr);
  DEBUG("{} offset=0x{:x}~0x{:x} poffset=0x{:x} ...", id, s_off, len, p_off);

  assert(addr.get_device_id() == get_device_id());

  if (s_off % superblock.block_size != 0 ||
      len % superblock.block_size != 0) {
    ERROR("{} offset=0x{:x}~0x{:x} poffset=0x{:x} invalid read", id, s_off, len, p_off);
    return crimson::ct_error::invarg::make();
  }

  if (s_id >= get_num_segments()) {
    ERROR("{} offset=0x{:x}~0x{:x} poffset=0x{:x} segment-id out of range {}",
          id, s_off, len, p_off, get_num_segments());
    return crimson::ct_error::invarg::make();
  }

  if (s_off + len > superblock.segment_size) {
    ERROR("{} offset=0x{:x}~0x{:x} poffset=0x{:x} read out of range 0x{:x}",
          id, s_off, len, p_off, superblock.segment_size);
    return crimson::ct_error::invarg::make();
  }

  if (tracker->get(s_id) == segment_state_t::EMPTY) {
    // XXX: not an error during scanning,
    // might need refactor to increase the log level
    DEBUG("{} offset=0x{:x}~0x{:x} poffset=0x{:x} invalid state {}",
          id, s_off, len, p_off, tracker->get(s_id));
    return crimson::ct_error::enoent::make();
  }

  stats.data_read.increment(len);
  return driver->read(
    get_device_id(),
    p_off,
    len,
    out);
}

void BlockSegmentManager::register_metrics(store_index_t store_index)
{
  LOG_PREFIX(BlockSegmentManager::register_metrics);
  if (!shard_status) {
    INFO("{} shard {} is not active, skip registering metrics",
         device_id_printer_t{get_device_id()}, store_index);
    return;
  }

  DEBUG("{}", device_id_printer_t{get_device_id()});
  namespace sm = seastar::metrics;
  std::vector<sm::label_instance> label_instances;
  label_instances.push_back(sm::label_instance("device_id", get_device_id()));
  label_instances.push_back(
    sm::label_instance("shard_device_index", std::to_string(store_index)));
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
