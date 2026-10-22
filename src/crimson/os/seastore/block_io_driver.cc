// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab


#include <stdexcept>

#include <seastar/core/reactor.hh>

#include "include/buffer.h"

#include "crimson/common/config_proxy.h"
#include "crimson/common/errorator-utils.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/block_io_driver.h"
#ifdef HAVE_SPDK
#include "crimson/os/seastore/spdk_block_io_driver.h"
#endif

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore {

ceph::unique_leakable_ptr<ceph::buffer::raw>
BlockIODriver::alloc_io_buffer(size_t len)
{
  return ceph::buffer::create_page_aligned(len);
}

// ---------------------------------------------------------------------------
// Kernel/io_uring raw block I/O helpers.
//
// These are the long-standing BlockSegmentManager do_* helpers, moved here
// unchanged so both backends share a single implementation. They are kept as
// namespace-scope free functions (rather than member functions of the
// anonymous-namespace driver below) so the parallel_for_each lambdas retain
// linkage.
// ---------------------------------------------------------------------------

using write_ertr = BlockIODriver::write_ertr;
using read_ertr = BlockIODriver::read_ertr;

static write_ertr::future<> do_write(
  device_id_t device_id,
  seastar::file &device,
  uint64_t offset,
  bufferptr &bptr)
{
  LOG_PREFIX(block_io_driver_do_write);
  auto len = bptr.length();
  TRACE("{} poffset=0x{:x}~0x{:x} ...",
        device_id_printer_t{device_id}, offset, len);
  return device.dma_write(
    offset,
    bptr.c_str(),
    len
  ).handle_exception(
    [FNAME, device_id, offset, len](auto e) -> write_ertr::future<size_t> {
    ERROR("{} poffset=0x{:x}~0x{:x} got error -- {}",
          device_id_printer_t{device_id}, offset, len, e);
    return crimson::ct_error::input_output_error::make();
  }).then([FNAME, device_id, offset, len](auto result) -> write_ertr::future<> {
    if (result != len) {
      ERROR("{} poffset=0x{:x}~0x{:x} write len=0x{:x} inconsistent",
            device_id_printer_t{device_id}, offset, len, result);
      return crimson::ct_error::input_output_error::make();
    }
    TRACE("{} poffset=0x{:x}~0x{:x} done", device_id_printer_t{device_id}, offset, len);
    return write_ertr::now();
  });
}

static write_ertr::future<> do_writev(
  device_id_t device_id,
  seastar::file &device,
  uint64_t offset,
  bufferlist&& bl,
  size_t block_size)
{
  LOG_PREFIX(block_io_driver_do_writev);
  TRACE("{} poffset=0x{:x}~0x{:x}, {} buffers",
        device_id_printer_t{device_id}, offset, bl.length(), bl.get_num_buffers());

  // writev requires each buffer to be aligned to the disks' block
  // size, we need to rebuild here
  bl.rebuild_aligned(block_size);

  return seastar::do_with(
    bl.prepare_iovs(),
    std::move(bl),
    [&device, device_id, offset, FNAME](auto& iovs, auto& bl)
  {
    return write_ertr::parallel_for_each(
      iovs,
      [&device, device_id, offset, FNAME](auto& p) mutable
    {
      auto off = offset + p.offset;
      auto len = p.length;
      auto& iov = p.iov;
      TRACE("{} poffset=0x{:x}~0x{:x} dma_write ...",
            device_id_printer_t{device_id}, off, len);
      return device.dma_write(off, std::move(iov)
      ).handle_exception(
        [FNAME, device_id, off, len](auto e) -> write_ertr::future<size_t>
      {
        ERROR("{} poffset=0x{:x}~0x{:x} dma_write got error -- {}",
              device_id_printer_t{device_id}, off, len, e);
	return crimson::ct_error::input_output_error::make();
      }).then([FNAME, device_id, off, len](size_t written) -> write_ertr::future<> {
	if (written != len) {
          ERROR("{} poffset=0x{:x}~0x{:x} dma_write len=0x{:x} inconsistent",
                device_id_printer_t{device_id}, off, len, written);
	  return crimson::ct_error::input_output_error::make();
	}
        TRACE("{} poffset=0x{:x}~0x{:x} dma_write done",
              device_id_printer_t{device_id}, off, len);
	return write_ertr::now();
      });
    });
  });
}

static read_ertr::future<> do_read(
  device_id_t device_id,
  seastar::file &device,
  uint64_t offset,
  size_t len,
  bufferptr &bptr)
{
  LOG_PREFIX(block_io_driver_do_read);
  TRACE("{} poffset=0x{:x}~0x{:x} ...", device_id_printer_t{device_id}, offset, len);
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
    [FNAME, device_id, offset, len](auto e) -> read_ertr::future<size_t>
  {
    ERROR("{} poffset=0x{:x}~0x{:x} got error -- {}",
          device_id_printer_t{device_id}, offset, len, e);
    return crimson::ct_error::input_output_error::make();
  }).then([FNAME, device_id, offset, len](auto result) -> read_ertr::future<> {
    if (result != len) {
      ERROR("{} poffset=0x{:x}~0x{:x} read len=0x{:x} inconsistent",
            device_id_printer_t{device_id}, offset, len, result);
      return crimson::ct_error::input_output_error::make();
    }
    TRACE("{} poffset=0x{:x}~0x{:x} done", device_id_printer_t{device_id}, offset, len);
    return read_ertr::now();
  });
}

static read_ertr::future<> do_readv(
  device_id_t device_id,
  seastar::file &device,
  uint64_t offset,
  std::vector<bufferptr> ptrs)
{
  LOG_PREFIX(block_io_driver_do_readv);
  std::vector<iovec> iov;
  size_t len = 0;
  for (auto &ptr : ptrs) {
    iov.emplace_back(ptr.c_str(), ptr.length());
    len += ptr.length();
  }
  TRACE("{} poffset=0x{:x}~0x{:x} {} buffers",
    device_id_printer_t{device_id}, offset, len, ptrs.size());
  return device.dma_read(offset, std::move(iov)
  ).handle_exception(
    [FNAME, device_id, offset, len](auto e) -> read_ertr::future<size_t>
  {
    ERROR("{} poffset=0x{:x}~0x{:x} got error -- {}",
          device_id_printer_t{device_id}, offset, len, e);
    return crimson::ct_error::input_output_error::make();
  }).then([FNAME, device_id, offset, len](auto result) -> read_ertr::future<> {
    if (result != len) {
      ERROR("{} poffset=0x{:x}~0x{:x} read len=0x{:x} inconsistent",
            device_id_printer_t{device_id}, offset, len, result);
      return crimson::ct_error::input_output_error::make();
    }
    TRACE("{} poffset=0x{:x}~0x{:x} done", device_id_printer_t{device_id}, offset, len);
    return read_ertr::now();
  });
}

namespace {

/**
 * KernelBlockIODriver
 *
 * Default transport: byte-addressed DMA against a seastar::file, backed by
 * io_uring (or the configured Seastar reactor backend) and the kernel NVMe
 * driver.
 */
class KernelBlockIODriver final : public BlockIODriver {
  seastar::file device;

public:
  KernelBlockIODriver() = default;

  open_ertr::future<seastar::stat_data> open(
    const std::string &path,
    seastar::open_flags flags) final
  {
    LOG_PREFIX(KernelBlockIODriver::open);
    return seastar::file_stat(path, seastar::follow_symlink::yes
    ).then([this, path, flags, FNAME](auto stat) mutable {
      return seastar::open_file_dma(path, flags
      ).then([this, stat, path, FNAME](auto file) mutable {
        device = file;
        return device.size().then(
            [this, stat, path, FNAME](auto size) mutable {
          stat.size = size;
          // Use Seastar's DMA alignment for optimal I/O alignment; clamp to
          // laddr_t::UNIT_SIZE since SeaStore operates at 4 KiB granularity
          // and rejects smaller device-reported block sizes.
          stat.block_size = std::max<uint64_t>(device.disk_write_dma_alignment(),
                                               laddr_t::UNIT_SIZE);
          INFO("path={} successful, size=0x{:x}, block_size=0x{:x}",
               path, stat.size, stat.block_size);
          return stat;
        });
      });
    }).handle_exception([FNAME, path](auto e)
        -> open_ertr::future<seastar::stat_data> {
      ERROR("path={} got error -- {}", path, e);
      return crimson::ct_error::input_output_error::make();
    });
  }

  close_ertr::future<> close() final
  {
    return device.close().then([] {
      return close_ertr::now();
    });
  }

  write_ertr::future<> write(
    device_id_t device_id,
    uint64_t offset,
    bufferptr &bptr) final
  {
    return do_write(device_id, device, offset, bptr);
  }

  write_ertr::future<> writev(
    device_id_t device_id,
    uint64_t offset,
    bufferlist &&bl,
    size_t block_size) final
  {
    return do_writev(device_id, device, offset, std::move(bl), block_size);
  }

  read_ertr::future<> read(
    device_id_t device_id,
    uint64_t offset,
    size_t len,
    bufferptr &out) final
  {
    return do_read(device_id, device, offset, len, out);
  }

  read_ertr::future<> readv(
    device_id_t device_id,
    uint64_t offset,
    std::vector<bufferptr> ptrs) final
  {
    return do_readv(device_id, device, offset, std::move(ptrs));
  }
};

} // anonymous namespace

BlockIODriverRef make_block_io_driver(
  const std::string &path,
  device_type_t dtype)
{
  auto trid = crimson::common::local_conf().get_val<std::string>(
    "seastore_spdk_transport_id");
  if (trid.empty()) {
    return std::make_unique<KernelBlockIODriver>();
  }
  // SPDK requested (transport id set).
  if (dtype == device_type_t::ZBD) {
    // ZBD needs the ZNS command set, which BlockIODriver does not model — refuse
    // rather than silently fall back or corrupt a zoned namespace. ZBD never
    // routes through this factory in practice (see ZBDSegmentManager), but guard
    // here too for safety.
    throw std::invalid_argument(
      "seastore_spdk_transport_id is set but device type is ZBD; "
      "SPDK is not supported for zoned (ZNS) devices");
  }
#ifdef HAVE_SPDK
  return std::make_unique<SPDKBlockIODriver>(trid);
#else
  throw std::invalid_argument(
    "seastore_spdk_transport_id is set but this build has WITH_SPDK=OFF");
#endif
}

}
