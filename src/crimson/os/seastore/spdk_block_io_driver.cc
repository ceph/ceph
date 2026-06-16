// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/spdk_block_io_driver.h"

#include <cstring>

#include <seastar/core/do_with.hh>

#include <spdk/env.h>

#include "include/buffer.h"

#include "crimson/common/errorator-utils.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/spdk_dma_buffer.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore {

SPDKBlockIODriver::open_ertr::future<seastar::stat_data>
SPDKBlockIODriver::open(const std::string &path, seastar::open_flags)
{
  LOG_PREFIX(SPDKBlockIODriver::open);
  // The device is the transport id, not `path`.
  return nvme.probe(transport_id
  ).then([this, FNAME] {
    nvme.start_shard();
    seastar::stat_data stat = {};
    stat.size = nvme.size_bytes();
    stat.block_size = nvme.block_size();
    INFO("transport={} size=0x{:x} block_size=0x{:x}",
         transport_id, stat.size, stat.block_size);
    return stat;
  }).handle_exception([FNAME, this](auto e)
      -> open_ertr::future<seastar::stat_data> {
    ERROR("transport={} probe/open failed -- {}", transport_id, e);
    return crimson::ct_error::input_output_error::make();
  });
}

SPDKBlockIODriver::close_ertr::future<> SPDKBlockIODriver::close()
{
  nvme.stop_shard();
  return nvme.detach().then([] {
    return close_ertr::now();
  });
}

SPDKBlockIODriver::write_ertr::future<> SPDKBlockIODriver::write(
  device_id_t, uint64_t offset, bufferptr &bptr)
{
  return nvme.do_io(/*is_write=*/true, offset, bptr.c_str(), bptr.length());
}

SPDKBlockIODriver::write_ertr::future<> SPDKBlockIODriver::writev(
  device_id_t device_id, uint64_t offset, bufferlist &&bl, size_t block_size)
{
  bl.rebuild_aligned(block_size);
  return seastar::do_with(
    std::move(bl),
    [this, device_id, offset](bufferlist &bl)
  {
    // Submit each contiguous buffer of the (now aligned) bufferlist in order.
    return seastar::do_with(
      (uint64_t)offset,
      [this, device_id, &bl](uint64_t &off)
    {
      return write_ertr::parallel_for_each(
        bl.buffers(),
        [this, device_id, &off](const bufferptr &p) {
        auto at = off;
        off += p.length();
        // const_cast: write path only reads from the buffer.
        return nvme.do_io(true, at, const_cast<char*>(p.c_str()), p.length());
      });
    });
  });
}

SPDKBlockIODriver::read_ertr::future<> SPDKBlockIODriver::read(
  device_id_t, uint64_t offset, size_t len, bufferptr &out)
{
  return nvme.do_io(/*is_write=*/false, offset, out.c_str(), len);
}

SPDKBlockIODriver::read_ertr::future<> SPDKBlockIODriver::readv(
  device_id_t device_id, uint64_t offset, std::vector<bufferptr> ptrs)
{
  return seastar::do_with(
    std::move(ptrs), (uint64_t)offset,
    [this, device_id](std::vector<bufferptr> &ptrs, uint64_t &off)
  {
    return read_ertr::parallel_for_each(
      ptrs,
      [this, device_id, &off](bufferptr &p) {
      auto at = off;
      off += p.length();
      return nvme.do_io(false, at, p.c_str(), p.length());
    });
  });
}

ceph::unique_leakable_ptr<ceph::buffer::raw>
SPDKBlockIODriver::alloc_io_buffer(size_t len)
{
  return create_spdk_dma(len);
}

}
