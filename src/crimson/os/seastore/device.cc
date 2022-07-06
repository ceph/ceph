// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/errorator-loop.h"

#include "crimson/os/seastore/device.h"
#include "crimson/os/seastore/segment_manager.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore {

std::ostream& operator<<(std::ostream& out, const device_spec_t& ds)
{
  return out << "device_spec("
             << "magic=" << ds.magic
             << ", dtype=" << ds.dtype
             << ", " << device_id_printer_t{ds.id}
             << ")";
}

std::ostream& operator<<(std::ostream& out, const device_config_t& conf)
{
  out << "device_config_t("
      << "major_dev=" << conf.major_dev
      << ", spec=" << conf.spec
      << ", meta=" << conf.meta
      << ", secondary(";
  for (const auto& [k, v] : conf.secondary_devices) {
    out << device_id_printer_t{k}
        << ": " << v << ", ";
  }
  return out << "))";
}

seastar::future<DeviceRef>
Device::make_device(const std::string& device)
{
  // TODO: configure device type
  return SegmentManager::get_segment_manager(device
  ).then([](DeviceRef ret) {
    return ret;
  });
}

namespace device {

write_ertr::future<> do_write(
  device_id_t device_id,
  seastar::file &device,
  uint64_t offset,
  bufferptr &bptr)
{
  LOG_PREFIX(block_do_write);
  auto len = bptr.length();
  TRACE("{} poffset={}~{} ...",
        device_id_printer_t{device_id}, offset, len);
  return device.dma_write(
    offset,
    bptr.c_str(),
    len
  ).handle_exception(
    [FNAME, device_id, offset, len](auto e) -> write_ertr::future<size_t> {
    ERROR("{} poffset={}~{} got error -- {}",
          device_id_printer_t{device_id}, offset, len, e);
    return crimson::ct_error::input_output_error::make();
  }).then([FNAME, device_id, offset, len](auto result) -> write_ertr::future<> {
    if (result != len) {
      ERROR("{} poffset={}~{} write len={} inconsistent",
            device_id_printer_t{device_id}, offset, len, result);
      return crimson::ct_error::input_output_error::make();
    }
    TRACE("{} poffset={}~{} done", device_id_printer_t{device_id}, offset, len);
    return write_ertr::now();
  });
}

write_ertr::future<> do_writev(
  device_id_t device_id,
  seastar::file &device,
  uint64_t offset,
  bufferlist&& bl,
  size_t block_size)
{
  LOG_PREFIX(block_do_writev);
  TRACE("{} poffset={}~{}, {} buffers",
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
      TRACE("{} poffset={}~{} dma_write ...",
            device_id_printer_t{device_id}, off, len);
      return device.dma_write(off, std::move(iov)
      ).handle_exception(
        [FNAME, device_id, off, len](auto e) -> write_ertr::future<size_t>
      {
        ERROR("{} poffset={}~{} dma_write got error -- {}",
              device_id_printer_t{device_id}, off, len, e);
	return crimson::ct_error::input_output_error::make();
      }).then([FNAME, device_id, off, len](size_t written) -> write_ertr::future<> {
	if (written != len) {
          ERROR("{} poffset={}~{} dma_write len={} inconsistent",
                device_id_printer_t{device_id}, off, len, written);
	  return crimson::ct_error::input_output_error::make();
	}
        TRACE("{} poffset={}~{} dma_write done",
              device_id_printer_t{device_id}, off, len);
	return write_ertr::now();
      });
    });
  });
}

read_ertr::future<> do_read(
  device_id_t device_id,
  seastar::file &device,
  uint64_t offset,
  size_t len,
  bufferptr &bptr)
{
  LOG_PREFIX(block_do_read);
  TRACE("{} poffset={}~{} ...", device_id_printer_t{device_id}, offset, len);
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
    ERROR("{} poffset={}~{} got error -- {}",
          device_id_printer_t{device_id}, offset, len, e);
    return crimson::ct_error::input_output_error::make();
  }).then([FNAME, device_id, offset, len](auto result) -> read_ertr::future<> {
    if (result != len) {
      ERROR("{} poffset={}~{} read len={} inconsistent",
            device_id_printer_t{device_id}, offset, len, result);
      return crimson::ct_error::input_output_error::make();
    }
    TRACE("{} poffset={}~{} done", device_id_printer_t{device_id}, offset, len);
    return read_ertr::now();
  });
}

using check_create_device_ret = access_ertr::future<>;
check_create_device_ret check_create_device(
  const std::string &path,
  size_t size)
{
  LOG_PREFIX(block_check_create_device);
  INFO("path={}, size={}", path, size);
  return seastar::open_file_dma(
    path,
    seastar::open_flags::exclusive |
    seastar::open_flags::rw |
    seastar::open_flags::create
  ).then([size, FNAME, &path](auto file) {
    return seastar::do_with(
      file,
      [size, FNAME, &path](auto &f) -> seastar::future<>
    {
      DEBUG("path={} created, truncating to {}", path, size);
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
    if (f.failed()) {
      try {
	f.get();
	return seastar::now();
      } catch (const std::system_error &e) {
	if (e.code().value() == EEXIST) {
          ERROR("path={} exists", path);
	  return seastar::now();
	} else {
          ERROR("path={} creation error -- {}", path, e);
	  return crimson::ct_error::input_output_error::make();
	}
      } catch (...) {
        ERROR("path={} creation error", path);
	return crimson::ct_error::input_output_error::make();
      }
    }

    DEBUG("path={} complete", path);
    std::ignore = f.discard_result();
    return seastar::now();
  });
}

open_device_ret open_device(
  const std::string &path)
{
  LOG_PREFIX(block_open_device);
  return seastar::file_stat(path, seastar::follow_symlink::yes
  ).then([&path, FNAME](auto stat) mutable {
    return seastar::open_file_dma(
      path,
      seastar::open_flags::rw | seastar::open_flags::dsync
    ).then([=, &path](auto file) {
      INFO("path={} successful, size={}", path, stat.size);
      return std::make_pair(file, stat);
    });
  }).handle_exception([FNAME, &path](auto e) -> open_device_ret {
    ERROR("path={} got error -- {}", path, e);
    return crimson::ct_error::input_output_error::make();
  });
}
}

}
