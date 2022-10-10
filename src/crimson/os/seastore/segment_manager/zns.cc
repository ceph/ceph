// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>
#include <linux/blkzoned.h>

#include <fmt/format.h>
#include "crimson/os/seastore/segment_manager/zns.h"
#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/logging.h"
#include "include/buffer.h"

SET_SUBSYS(seastore_device);

#define SECT_SHIFT	9
#define RESERVED_ZONES 	1
// limit the max padding buf size to 1MB
#define MAX_PADDING_SIZE 1048576

using z_op = crimson::os::seastore::segment_manager::zns::zone_op;
template <> struct fmt::formatter<z_op>: fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(z_op s, FormatContext& ctx) {
    std::string_view name = "Unknown";
    switch (s) {
      using enum z_op;
        case OPEN:
          name = "BLKOPENZONE";
          break;
        case FINISH:
          name = "BLKFINISHZONE";
          break;
        case CLOSE:
          name = "BLKCLOSEZONE";
          break;
        case RESET:
          name = "BLKRESETZONE";
          break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};

namespace crimson::os::seastore::segment_manager::zns {

using open_device_ret = ZNSSegmentManager::access_ertr::future<
  std::pair<seastar::file, seastar::stat_data>>;
static open_device_ret open_device(
  const std::string &path,
  seastar::open_flags mode)
{
  LOG_PREFIX(ZNSSegmentManager::open_device);
  return seastar::file_stat(
    path, seastar::follow_symlink::yes
  ).then([FNAME, mode, &path](auto stat) mutable {
    return seastar::open_file_dma(path, mode).then([=](auto file) {
       DEBUG("open of device {} successful, size {}",
        path,
        stat.size);
      return std::make_pair(file, stat);
    });
  }).handle_exception(
    [FNAME](auto e) -> open_device_ret {
	ERROR("got error {}",
	e);
      return crimson::ct_error::input_output_error::make();
    }
  );
}

static zns_sm_metadata_t make_metadata(
  uint64_t total_size,
  seastore_meta_t meta,
  const seastar::stat_data &data,
  size_t zone_size_sectors,
  size_t zone_capacity_sectors,
  size_t num_zones)
{
  LOG_PREFIX(ZNSSegmentManager::make_metadata);

  // TODO: support Option::size_t seastore_segment_size
  // to allow zones_per_segment > 1 with striping.
  size_t zone_size = zone_size_sectors << SECT_SHIFT;
  size_t zone_capacity = zone_capacity_sectors << SECT_SHIFT;
  size_t segment_size = zone_size;
  size_t zones_per_segment = segment_size / zone_size;
  size_t segments = (num_zones - RESERVED_ZONES) / zones_per_segment;
  size_t available_size = zone_capacity * segments;

  assert(total_size == num_zones * zone_size);

  WARN("Ignoring configuration values for device and segment size");
  INFO(
    "device size {}, available_size {}, block_size {}, allocated_size {},"
    " total zones {}, zone_size {}, zone_capacity {},"
    " total segments {}, zones per segment {}, segment size {}",
    total_size,
    available_size,
    data.block_size,
    data.allocated_size,
    num_zones,
    zone_size,
    zone_capacity,
    segments,
    zones_per_segment,
    zone_capacity * zones_per_segment);

  zns_sm_metadata_t ret = zns_sm_metadata_t{
    available_size,
    segment_size,
    zone_capacity * zones_per_segment,
    zones_per_segment,
    zone_capacity,
    data.block_size,
    segments,
    zone_size,
    zone_size * RESERVED_ZONES,
    meta};
  ret.validate();
  return ret;
}

struct ZoneReport {
  struct blk_zone_report *hdr;
  ZoneReport(int nr_zones) 
    : hdr((blk_zone_report *)malloc(
	    sizeof(struct blk_zone_report) + nr_zones * sizeof(struct blk_zone))){;}
  ~ZoneReport(){
    free(hdr);
  }
  ZoneReport(const ZoneReport &) = delete;
  ZoneReport(ZoneReport &&rhs) : hdr(rhs.hdr) {
    rhs.hdr = nullptr;
  }
};

static seastar::future<size_t> get_blk_dev_size(
  seastar::file &device)
{
  return seastar::do_with(
    (uint64_t)0,
    [&](auto& size_sects) {
      return device.ioctl(
        BLKGETSIZE,
	(void *)&size_sects
      ).then([&](int ret) {
        ceph_assert(size_sects);
        size_t size = size_sects << SECT_SHIFT;
	return seastar::make_ready_future<size_t>(size);
      });
  });
}

// zone_size should be in 512B sectors
static seastar::future<> reset_device(
  seastar::file &device,
  uint64_t zone_size_sects,
  uint64_t nr_zones)
{
  return seastar::do_with(
    blk_zone_range{},
    ZoneReport(nr_zones),
    [&, nr_zones](auto &range, auto &zr) {
      range.sector = 0;
      range.nr_sectors = zone_size_sects * nr_zones;
      return device.ioctl(
	BLKRESETZONE, 
	&range
      ).then([&](int ret){
	return seastar::now();
      });
    }
  );
}

static seastar::future<size_t> get_zone_capacity(
  seastar::file &device,
  uint32_t nr_zones)
{
  return seastar::do_with(
    ZoneReport(nr_zones),
    [&](auto &zr) {
        zr.hdr->sector = 0;
        zr.hdr->nr_zones = nr_zones;
	return device.ioctl(
          BLKREPORTZONE,
          zr.hdr
        ).then([&](int ret) {
	return seastar::make_ready_future<size_t>(zr.hdr->zones[0].capacity);
      });
    }
  );
}

static write_ertr::future<> do_write(
  seastar::file &device,
  uint64_t offset,
  bufferptr &bptr)
{
  LOG_PREFIX(ZNSSegmentManager::do_write);
  DEBUG("offset {} len {}",
    offset,
    bptr.length());
  return device.dma_write(
    offset,
    bptr.c_str(),
    bptr.length() 
  ).handle_exception(
    [FNAME](auto e) -> write_ertr::future<size_t> {
      ERROR("dma_write got error {}",
        e);
      return crimson::ct_error::input_output_error::make();
    }
  ).then([length = bptr.length()](auto result) -> write_ertr::future<> {
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
  LOG_PREFIX(ZNSSegmentManager::do_writev);
  DEBUG("offset {} len {}",
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
  ).handle_exception(
    [FNAME](auto e) -> write_ertr::future<size_t> {
      ERROR("dma_write got error {}",
	e);
      return crimson::ct_error::input_output_error::make();
    }
  ).then([bl=std::move(bl)/* hold the buf until the end of io */](size_t written)
	 -> write_ertr::future<> {
    if (written != bl.length()) {
      return crimson::ct_error::input_output_error::make();
    }
    return write_ertr::now();
  });
}

static ZNSSegmentManager::access_ertr::future<>
write_metadata(seastar::file &device, zns_sm_metadata_t sb)
{
  assert(ceph::encoded_sizeof_bounded<zns_sm_metadata_t>() <
	 sb.block_size);
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sb.block_size)),
    [=, &device](auto &bp) {
      LOG_PREFIX(ZNSSegmentManager::write_metadata);
      DEBUG("block_size {}", sb.block_size);
      bufferlist bl;
      encode(sb, bl);
      auto iter = bl.begin();
      assert(bl.length() < sb.block_size);
      DEBUG("buffer length {}", bl.length());
      iter.copy(bl.length(), bp.c_str());
      DEBUG("doing writeout");
      return do_write(device, 0, bp);
    });
}

static read_ertr::future<> do_read(
  seastar::file &device,
  uint64_t offset,
  size_t len,
  bufferptr &bptr)
{
  LOG_PREFIX(ZNSSegmentManager::do_read);
  assert(len <= bptr.length());
  DEBUG("offset {} len {}",
    offset,
    len);
  return device.dma_read(
    offset,
    bptr.c_str(),
    len
  ).handle_exception(
    [FNAME](auto e) -> read_ertr::future<size_t> {
      ERROR("dma_read got error {}",
        e);
      return crimson::ct_error::input_output_error::make();
    }
  ).then([len](auto result) -> read_ertr::future<> {
    if (result != len) {
      return crimson::ct_error::input_output_error::make();
    }
    return read_ertr::now();
  });
}

static
ZNSSegmentManager::access_ertr::future<zns_sm_metadata_t>
read_metadata(seastar::file &device, seastar::stat_data sd)
{
  assert(ceph::encoded_sizeof_bounded<zns_sm_metadata_t>() <
	 sd.block_size);
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
	zns_sm_metadata_t ret;
	auto bliter = bl.cbegin();
	decode(ret, bliter);
        ret.validate();
	return ZNSSegmentManager::access_ertr::future<zns_sm_metadata_t>(
	  ZNSSegmentManager::access_ertr::ready_future_marker{},
	  ret);
      });
    });
}

ZNSSegmentManager::mount_ret ZNSSegmentManager::mount() 
{
  return open_device(
    device_path, seastar::open_flags::rw
  ).safe_then([=, this](auto p) {
    device = std::move(p.first);
    auto sd = p.second;
    return read_metadata(device, sd);
  }).safe_then([=, this](auto meta){
    metadata = meta;
    return mount_ertr::now();
  });
}

ZNSSegmentManager::mkfs_ret ZNSSegmentManager::mkfs(
  device_config_t config)
{
  LOG_PREFIX(ZNSSegmentManager::mkfs);
  INFO("starting, device_path {}", device_path);
  return seastar::do_with(
    seastar::file{},
    seastar::stat_data{},
    zns_sm_metadata_t{},
    size_t(),
    size_t(),
    size_t(),
    [=, this](auto &device, auto &stat, auto &sb, auto &zone_size_sects, auto &nr_zones, auto &size) {
      return open_device(
	device_path,
	seastar::open_flags::rw
      ).safe_then([=, this, &device, &stat, &sb, &zone_size_sects, &nr_zones, &size](auto p) {
	device = p.first;
	stat = p.second;
	return device.ioctl(
	  BLKGETNRZONES,
	  (void *)&nr_zones
	).then([&](int ret) {
	  if (nr_zones == 0) {
	    return seastar::make_exception_future<int>(
	      std::system_error(std::make_error_code(std::errc::io_error)));
	  }
	  return device.ioctl(BLKGETZONESZ, (void *)&zone_size_sects);
	}).then([&](int ret) {
          ceph_assert(zone_size_sects);
	  return reset_device(device, zone_size_sects, nr_zones);
        }).then([&] {
          return get_blk_dev_size(device);
	}).then([&](auto devsize) {
          size = devsize;
	  return get_zone_capacity(device, nr_zones);
	}).then([&, FNAME, config](auto zone_capacity_sects) {
          ceph_assert(zone_capacity_sects);
          DEBUG("zone_size in sectors {}, zone_capacity in sectors {}",
                zone_size_sects, zone_capacity_sects);
	  sb = make_metadata(
            size,
	    config.meta,
	    stat,
	    zone_size_sects,
	    zone_capacity_sects,
	    nr_zones);
	  metadata = sb;
	  stats.metadata_write.increment(
	    ceph::encoded_sizeof_bounded<zns_sm_metadata_t>());
	  DEBUG("Wrote to stats.");
	  return write_metadata(device, sb);
	}).finally([&, FNAME] {
	  DEBUG("Closing device.");
	  return device.close(); 
	}).safe_then([FNAME] {
	  DEBUG("Returning from mkfs.");
	  return mkfs_ertr::now();
	});
      });
    });
}

// Return range of sectors to operate on.
struct blk_zone_range make_range(
  segment_id_t id,
  size_t segment_size,
  size_t first_segment_offset)
{
  return blk_zone_range{
    (id.device_segment_id() * (segment_size >> SECT_SHIFT)
                           + (first_segment_offset >> SECT_SHIFT)),
    (segment_size >> SECT_SHIFT)
  };
}

using blk_zone_op_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;
using blk_zone_op_ret = blk_zone_op_ertr::future<>;
blk_zone_op_ret blk_zone_op(seastar::file &device,
		            blk_zone_range &range,
			    zone_op op) {
  LOG_PREFIX(ZNSSegmentManager::blk_zone_op);

  unsigned long ioctl_op = 0;
  switch (op) {
    using enum zone_op;
    case OPEN:
      ioctl_op = BLKOPENZONE;
      break;
    case FINISH:
      ioctl_op = BLKFINISHZONE;
      break;
    case RESET:
      ioctl_op = BLKRESETZONE;
      break;
    case CLOSE:
      ioctl_op = BLKCLOSEZONE;
      break;
    default:
      ERROR("Invalid zone operation {}", op);
      ceph_assert(ioctl_op);
  }

  return device.ioctl(
    ioctl_op,
    &range
  ).then_wrapped([=](auto f) -> blk_zone_op_ret {
    if (f.failed()) {
      ERROR("{} ioctl failed", op);
      return crimson::ct_error::input_output_error::make();
    } else {
      int ret = f.get();
      if (ret == 0) {
	return seastar::now();
      } else {
        ERROR("{} ioctl failed with return code {}", op, ret);
	return crimson::ct_error::input_output_error::make();
      }
    }
  });
}

ZNSSegmentManager::open_ertr::future<SegmentRef> ZNSSegmentManager::open(
  segment_id_t id)
{
  LOG_PREFIX(ZNSSegmentManager::open);
  return seastar::do_with(
    blk_zone_range{},
    [=, this](auto &range) {
      range = make_range(
	id,
	metadata.segment_size,
        metadata.first_segment_offset);
      return blk_zone_op(
	device,
	range,
	zone_op::OPEN
      );
    }
  ).safe_then([=, this] {
    DEBUG("segment {}, open successful", id);
    return open_ertr::future<SegmentRef>(
      open_ertr::ready_future_marker{},
      SegmentRef(new ZNSSegment(*this, id))
    );
  });
}

ZNSSegmentManager::release_ertr::future<> ZNSSegmentManager::release(
  segment_id_t id) 
{
  LOG_PREFIX(ZNSSegmentManager::release);
  DEBUG("Resetting zone/segment {}", id);
  return seastar::do_with(
    blk_zone_range{},
    [=, this](auto &range) {
      range = make_range(
	id,
	metadata.segment_size,
        metadata.first_segment_offset);
      return blk_zone_op(
	device,
	range,
	zone_op::RESET
      );
    }
  ).safe_then([=] {
    DEBUG("segment release successful");
    return release_ertr::now();
  });
}

SegmentManager::read_ertr::future<> ZNSSegmentManager::read(
  paddr_t addr,
  size_t len,
  ceph::bufferptr &out)
{
  LOG_PREFIX(ZNSSegmentManager::read);
  auto& seg_addr = addr.as_seg_paddr();
  if (seg_addr.get_segment_id().device_segment_id() >= get_num_segments()) {
    ERROR("invalid segment {}",
      seg_addr.get_segment_id().device_segment_id());
    return crimson::ct_error::invarg::make();
  }
  
  if (seg_addr.get_segment_off() + len > metadata.segment_capacity) {
    ERROR("invalid read offset {}, len {}",
      addr,
      len);
    return crimson::ct_error::invarg::make();
  }
  return do_read(
    device,
    get_offset(addr),
    len,
    out);
}

Segment::close_ertr::future<> ZNSSegmentManager::segment_close(
  segment_id_t id, segment_off_t write_pointer)
{
  LOG_PREFIX(ZNSSegmentManager::segment_close);
  return seastar::do_with(
    blk_zone_range{},
    [=, this](auto &range) {
      range = make_range(
	id,
	metadata.segment_size,
        metadata.first_segment_offset);
      return blk_zone_op(
	device,
	range,
	zone_op::FINISH
      );
    }
  ).safe_then([=] {
    DEBUG("zone finish successful");
    return Segment::close_ertr::now();
  });
}

Segment::write_ertr::future<> ZNSSegmentManager::segment_write(
  paddr_t addr,
  ceph::bufferlist bl,
  bool ignore_check)
{
  LOG_PREFIX(ZNSSegmentManager::segment_write);
  assert(addr.get_device_id() == get_device_id());
  assert((bl.length() % metadata.block_size) == 0);
  auto& seg_addr = addr.as_seg_paddr();
  DEBUG("write to segment {} at offset {}, physical offset {}, len {}",
    seg_addr.get_segment_id(),
    seg_addr.get_segment_off(),
    get_offset(addr),
    bl.length());
  stats.data_write.increment(bl.length());
  return do_writev(
    device, 
    get_offset(addr), 
    std::move(bl), 
    metadata.block_size);
}

device_id_t ZNSSegmentManager::get_device_id() const
{
  return metadata.device_id;
};

secondary_device_set_t& ZNSSegmentManager::get_secondary_devices()
{
  return metadata.secondary_devices;
};

magic_t ZNSSegmentManager::get_magic() const
{
  return metadata.magic;
};

segment_off_t ZNSSegment::get_write_capacity() const
{
  return manager.get_segment_size();
}

SegmentManager::close_ertr::future<> ZNSSegmentManager::close()
{
  if (device) {
    return device.close();
  }
  return seastar::now();
}

Segment::close_ertr::future<> ZNSSegment::close()
{
  return manager.segment_close(id, write_pointer);
}

Segment::write_ertr::future<> ZNSSegment::write(
  segment_off_t offset, ceph::bufferlist bl)
{
  LOG_PREFIX(ZNSSegment::write);
  if (offset != write_pointer || offset % manager.metadata.block_size != 0) {
    ERROR("Segment offset and zone write pointer mismatch. "
          "segment {} segment-offset {} write pointer {}",
          id, offset, write_pointer);
    return crimson::ct_error::invarg::make();
  }
  if (offset + bl.length() > manager.metadata.segment_capacity) {
    return crimson::ct_error::enospc::make();
  }
  
  write_pointer = offset + bl.length();
  return manager.segment_write(paddr_t::make_seg_paddr(id, offset), bl);
}

Segment::write_ertr::future<> ZNSSegment::write_padding_bytes(
  size_t padding_bytes)
{
  LOG_PREFIX(ZNSSegment::write_padding_bytes);
  DEBUG("Writing {} padding bytes to segment {} at wp {}",
        padding_bytes, id, write_pointer);

  return crimson::repeat([FNAME, padding_bytes, this] () mutable {
    size_t bufsize = 0;
    if (padding_bytes >= MAX_PADDING_SIZE) {
      bufsize = MAX_PADDING_SIZE;
    } else {
      bufsize = padding_bytes;
    }

    padding_bytes -= bufsize;
    bufferptr bp(ceph::buffer::create_page_aligned(bufsize));
    bp.zero();
    bufferlist padd_bl;
    padd_bl.append(bp);
    return write(write_pointer, padd_bl).safe_then([FNAME, padding_bytes, this]() {
      if (padding_bytes == 0) {
        return write_ertr::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
      } else {
        return write_ertr::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
      }
    });
  });
}

// Advance write pointer, to given offset.
Segment::write_ertr::future<> ZNSSegment::advance_wp(
  segment_off_t offset)
{
  LOG_PREFIX(ZNSSegment::advance_wp);

  DEBUG("Advancing write pointer from {} to {}", write_pointer, offset);
  if (offset < write_pointer) {
    return crimson::ct_error::invarg::make();
  }

  size_t padding_bytes = offset - write_pointer;

  if (padding_bytes == 0) {
    return write_ertr::now();
  }

  assert(padding_bytes % manager.metadata.block_size == 0);

  return write_padding_bytes(padding_bytes);
}

}
