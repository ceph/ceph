// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>
#include <linux/blkzoned.h>

#include "crimson/os/seastore/segment_manager/zns.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"
#include "include/buffer.h"

namespace {
seastar::logger &logger(){
  return crimson::get_logger(ceph_subsys_seastore_device);
}
}

namespace crimson::os::seastore::segment_manager::zns {

using open_device_ret = ZNSSegmentManager::access_ertr::future<
  std::pair<seastar::file, seastar::stat_data>>;
static open_device_ret open_device(
  const std::string &path,
  seastar::open_flags mode)
{
  return seastar::file_stat(
    path, seastar::follow_symlink::yes
  ).then([mode, &path](auto stat) mutable{
    return seastar::open_file_dma(path, mode).then([=](auto file){
      logger().error(
	"open_device: open successful, size {}",
	stat.size);
      return std::make_pair(file, stat);
    });
  }).handle_exception(
    [](auto e) -> open_device_ret {
      logger().error(
	"open_device: got error {}",
	e);
      return crimson::ct_error::input_output_error::make();
    }
  );
}

static zns_sm_metadata_t make_metadata(
  seastore_meta_t meta,
  const seastar::stat_data &data,
  size_t zone_size,
  size_t zone_capacity,
  size_t num_zones)
{
  using crimson::common::get_conf;
  
  auto config_size = get_conf<Option::size_t>(
    "seastore_device_size");
  
  size_t size = (data.size == 0) ? config_size : data.size;
  
  auto config_segment_size = get_conf<Option::size_t>(
    "seastore_segment_size");
  logger().error("CONFIG SIZE: {}", config_segment_size);
  size_t zones_per_segment = config_segment_size / zone_capacity;
  
  size_t segments = (num_zones - 1) * zones_per_segment;
  
  logger().debug(
    "{}: size {}, block_size {}, allocated_size {}, configured_size {}, "
    "segment_size {}",
    __func__,
    data.size,
    data.block_size,
    data.allocated_size,
    config_size,
    config_segment_size);
  
  zns_sm_metadata_t ret = zns_sm_metadata_t{
    size,
    config_segment_size,
    zone_capacity * zones_per_segment,
    zones_per_segment,
    zone_capacity,
    data.block_size,
    segments,
    zone_size,
    zone_size,
    meta};
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

static seastar::future<> reset_device(
  seastar::file &device, 
  uint32_t zone_size, 
  uint32_t nr_zones)
{
  return seastar::do_with(
    blk_zone_range{},
    ZoneReport(nr_zones),
    [&, nr_zones] (auto &range, auto &zr){
      range.sector = 0;
      range.nr_sectors = zone_size * nr_zones;
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
  uint32_t zone_size, 
  uint32_t nr_zones)
{
  return seastar::do_with(
    blk_zone_range{},
    ZoneReport(nr_zones),
    [&] (auto &first_zone_range, auto &zr){
      first_zone_range.sector = 0;
      first_zone_range.nr_sectors = zone_size;
      return device.ioctl(
	BLKOPENZONE, 
	&first_zone_range
      ).then([&](int ret){
	return device.ioctl(BLKREPORTZONE, zr.hdr);
      }).then([&] (int ret){
	return device.ioctl(BLKRESETZONE, &first_zone_range);
      }).then([&](int ret){
	return seastar::make_ready_future<size_t>(zr.hdr->zones[0].wp);
      });
    }
  );
}

static write_ertr::future<> do_write(
  seastar::file &device,
  uint64_t offset,
  bufferptr &bptr)
{
  logger().debug(
    "zns: do_write offset {} len {}",
    offset,
    bptr.length());
  return device.dma_write(
    offset,
    bptr.c_str(),
    bptr.length() 
  ).handle_exception(
    [](auto e) -> write_ertr::future<size_t> {
      logger().error(
        "do_write: dma_write got error {}",
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
  logger().error(
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
  ).handle_exception(
    [](auto e) -> write_ertr::future<size_t> {
      logger().error(
	"do_writev: dma_write got error {}",
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
    [=, &device](auto &bp){
      logger().error("BLOCK SIZE: {}", sb.block_size);
      bufferlist bl;
      encode(sb, bl);
      auto iter = bl.begin();
      assert(bl.length() < sb.block_size);
      logger().error("{}", bl.length());
      iter.copy(bl.length(), bp.c_str());
      logger().debug("write_metadata: doing writeout");
      return do_write(device, 0, bp);
    });
}

static read_ertr::future<> do_read(
  seastar::file &device,
  uint64_t offset,
  size_t len,
  bufferptr &bptr)
{
  assert(len <= bptr.length());
  logger().debug(
    "block: do_read offset {} len {}",
    offset,
    len);
  return device.dma_read(
    offset,
    bptr.c_str(),
    len
  ).handle_exception(
    [](auto e) -> read_ertr::future<size_t> {
      logger().error(
        "do_read: dma_read got error {}",
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
  ).safe_then([=](auto p) {
    device = std::move(p.first);
    auto sd = p.second;
    return read_metadata(device, sd);
  }).safe_then([=](auto meta){
    metadata = meta;
    return mount_ertr::now();
  });
}

ZNSSegmentManager::mkfs_ret ZNSSegmentManager::mkfs(
  segment_manager_config_t config)
{
  logger().error("ZNSSegmentManager::mkfs: starting");
  return seastar::do_with(
    seastar::file{},
    seastar::stat_data{},
    zns_sm_metadata_t{},
    size_t(),
    size_t(),
    [=](auto &device, auto &stat, auto &sb, auto &zone_size, auto &nr_zones){
      logger().error("ZNSSegmentManager::mkfs path {}", device_path);
      return open_device(
	device_path, 
	seastar::open_flags::rw
      ).safe_then([=, &device, &stat, &sb, &zone_size, &nr_zones](auto p){
	device = p.first;
	stat = p.second;
	return device.ioctl(
	  BLKGETNRZONES, 
	  (void *)&nr_zones
	).then([&](int ret){
	  if (nr_zones == 0) {
	    return seastar::make_exception_future<int>(
	      std::system_error(std::make_error_code(std::errc::io_error)));
	  }
	  return device.ioctl(BLKGETZONESZ, (void *)&zone_size);
	}).then([&] (int ret){
	  return reset_device(device, zone_size, nr_zones);
	}).then([&] {
	  return get_zone_capacity(device, zone_size, nr_zones); 
	}).then([&, config] (auto zone_capacity){
	  sb = make_metadata(
	    config.meta, 
	    stat, 
	    zone_size, 
	    zone_capacity, 
	    nr_zones);
	  metadata = sb;
	  stats.metadata_write.increment(
	    ceph::encoded_sizeof_bounded<zns_sm_metadata_t>());
	  logger().error("WROTE TO STATS");
	  return write_metadata(device, sb);
	}).finally([&] {
	  logger().error("CLOSING DEVICE");
	  return device.close(); 
	}).safe_then([] {
	  logger().error("RETURNING FROM MKFS");
	  return mkfs_ertr::now();
	});
      });
    });
}

struct blk_zone_range make_range(
  segment_id_t id, 
  size_t segment_size, 
  size_t block_size, 
  size_t first_segment_offset)
{
  return blk_zone_range{
    (id.device_segment_id() * segment_size + first_segment_offset),
    (segment_size)  
  };
}

using blk_open_zone_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;
using blk_open_zone_ret = blk_open_zone_ertr::future<>;
blk_open_zone_ret blk_open_zone(seastar::file &device, blk_zone_range &range){
  return device.ioctl(
    BLKOPENZONE, 
    &range
  ).then_wrapped([=](auto f) -> blk_open_zone_ret{
    if (f.failed()) {
      return crimson::ct_error::input_output_error::make();
    }
    else {
      int ret = f.get();
      if (ret == 0) {
	return seastar::now();
      } else {
	return crimson::ct_error::input_output_error::make();
      }
    }
  });
}

ZNSSegmentManager::open_ertr::future<SegmentRef> ZNSSegmentManager::open(
  segment_id_t id)
{
  return seastar::do_with(
    blk_zone_range{},
    [=] (auto &range){
      range = make_range(
	id, 
	metadata.zone_size, 
	metadata.block_size, 
	metadata.first_segment_offset);
      return blk_open_zone(
	device, 
	range
      );
    }
  ).safe_then([=] {
    logger().error("open _segment: open successful");
    return open_ertr::future<SegmentRef>(
      open_ertr::ready_future_marker{},
      SegmentRef(new ZNSSegment(*this, id))
    );
  });
}

using blk_close_zone_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;
using blk_close_zone_ret = blk_close_zone_ertr::future<>;
blk_close_zone_ret blk_close_zone(
  seastar::file &device, 
  blk_zone_range &range)
{
  return device.ioctl(
    BLKCLOSEZONE, 
    &range
  ).then_wrapped([=](auto f) -> blk_open_zone_ret{
    if (f.failed()) {
      return crimson::ct_error::input_output_error::make();
    }
    else {
      int ret = f.get();
      if (ret == 0) {
	return seastar::now();
      } else {
	return crimson::ct_error::input_output_error::make();
      }
    }
  });
}

ZNSSegmentManager::release_ertr::future<> ZNSSegmentManager::release(
  segment_id_t id) 
{
  return seastar::do_with(
    blk_zone_range{},
    [=] (auto &range){
      range = make_range(
	id, 
	metadata.zone_size, 
	metadata.block_size, 
	metadata.first_segment_offset);
      return blk_close_zone(
	device, 
	range
      );
    }
  ).safe_then([=] {
    logger().error("release _segment: release successful");
    return release_ertr::now();
  });
}

SegmentManager::read_ertr::future<> ZNSSegmentManager::read(
  paddr_t addr,
  size_t len,
  ceph::bufferptr &out)
{
  auto& seg_addr = addr.as_seg_paddr();
  if (seg_addr.get_segment_id().device_segment_id() >= get_num_segments()) {
    logger().error(
      "ZNSSegmentManager::read: invalid segment {}",
      addr);
    return crimson::ct_error::invarg::make();
  }
  
  if (seg_addr.get_segment_off() + len > metadata.zone_size) {
    logger().error(
      "ZNSSegmentManager::read: invalid offset {}~{}!",
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
  segment_id_t id, seastore_off_t write_pointer)
{
  return seastar::do_with(
    blk_zone_range{},
    [=] (auto &range){
      range = make_range(
	id, 
	metadata.zone_size, 
	metadata.block_size, 
	metadata.first_segment_offset);
      return blk_close_zone(
	device, 
	range
      );
    }
  ).safe_then([=] {
    logger().error("open _segment: open successful");
    return Segment::close_ertr::now();
  });
}

Segment::write_ertr::future<> ZNSSegmentManager::segment_write(
  paddr_t addr,
  ceph::bufferlist bl,
  bool ignore_check)
{
  assert(addr.get_device_id() == get_device_id());
  assert((bl.length() % metadata.block_size) == 0);
  auto& seg_addr = addr.as_seg_paddr();
  logger().debug(
    "BlockSegmentManager::segment_write: "
    "segment_write to segment {} at offset {}, physical offset {}, len {}",
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

device_spec_t ZNSSegmentManager::get_device_spec() const
{
  auto spec = device_spec_t();
  spec.magic = metadata.magic;
  spec.dtype = metadata.dtype;
  spec.id = metadata.device_id;
  return spec;
};

magic_t ZNSSegmentManager::get_magic() const
{
  return metadata.magic;
};

seastore_off_t ZNSSegment::get_write_capacity() const
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
  seastore_off_t offset, ceph::bufferlist bl)
{
  if (offset < write_pointer || offset % manager.metadata.block_size != 0) {
    logger().error(
      "ZNSSegmentManager::ZNSSegment::write: "
      "invalid segment write on segment {} to offset {}",
      id,
      offset);
    return crimson::ct_error::invarg::make();
  }
  if (offset + bl.length() > manager.metadata.segment_size)
    return crimson::ct_error::enospc::make();
  
  write_pointer = offset + bl.length();
  return manager.segment_write(paddr_t::make_seg_paddr(id, offset), bl);
}

}
