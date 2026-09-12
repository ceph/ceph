//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "spdk_nvme_io_driver.h"

#include <cstring>

#include <seastar/core/do_with.hh>

#include <spdk/nvme.h>
#include <spdk/nvme_spec.h>
#include <spdk/version.h>

#include "include/buffer.h"

#include "crimson/common/errorator-utils.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/spdk_dma_buffer.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore::random_block_device::nvme {

uint32_t SPDKNVMeIODriver::pi_flags() const
{
  return pi_enabled
    ? (SPDK_NVME_IO_FLAGS_PRACT | SPDK_NVME_IO_FLAGS_PRCHK_GUARD)
    : 0;
}

void SPDKNVMeIODriver::set_protection_info(bool enabled, uint32_t block_size)
{
  pi_enabled = enabled;
  nvme_block_size = block_size;
}

SPDKNVMeIODriver::open_ertr::future<seastar::stat_data>
SPDKNVMeIODriver::open(const std::string &, seastar::open_flags)
{
  LOG_PREFIX(SPDKNVMeIODriver::open);
  return nvme.probe(transport_id
  ).then([this, FNAME] {
    nvme.start_shard();
    namespace_id = nvme.get_nsid();

    // Capabilities from the SPDK identify data (same NVMe wire layout as the
    // kernel identify path), expressed as raw caps_t values.
    nvme_identify_controller_data_t idc;
    std::memcpy(idc.raw, spdk_nvme_ctrlr_get_data(nvme.get_ctrlr()),
                sizeof(idc.raw));
    caps.ctrl_identified = true;
    caps.awupf = idc.awupf + 1;

    nvme_identify_namespace_data_t idn;
    std::memcpy(idn.raw, spdk_nvme_ns_get_data(nvme.get_ns()),
                sizeof(idn.raw));
    caps.ns_identified = true;
    if (idn.nsfeat.opterf == 1) {
      caps.opterf = true;
      caps.npwg = idn.npwg;
      caps.npwa = idn.npwa;
    }

    seastar::stat_data stat = {};
    stat.size = nvme.size_bytes();
    stat.block_size = nvme.block_size();
    INFO("transport={} size=0x{:x} block_size=0x{:x} awupf={}",
         transport_id, stat.size, stat.block_size, caps.awupf);
    return stat;
  }).handle_exception([FNAME, this](auto e)
      -> open_ertr::future<seastar::stat_data> {
    ERROR("transport={} probe/open failed -- {}", transport_id, e);
    return crimson::ct_error::input_output_error::make();
  });
}

SPDKNVMeIODriver::close_ertr::future<> SPDKNVMeIODriver::close()
{
  nvme.stop_shard();
  return nvme.detach().then([] {
    return close_ertr::now();
  });
}

SPDKNVMeIODriver::read_ertr::future<> SPDKNVMeIODriver::read(
  device_id_t, uint64_t offset, size_t len, bufferptr &out)
{
  if (len == 0) {
    return read_ertr::now();
  }
  return nvme.do_io(/*is_write=*/false, offset, out.c_str(), len, pi_flags());
}

SPDKNVMeIODriver::read_ertr::future<> SPDKNVMeIODriver::readv(
  device_id_t device_id, uint64_t offset, std::vector<bufferptr> ptrs)
{
  return seastar::do_with(
    std::move(ptrs), (uint64_t)offset,
    [this, device_id](std::vector<bufferptr> &ptrs, uint64_t &off)
  {
    return read_ertr::parallel_for_each(
      ptrs,
      [this, &off](bufferptr &p) {
      auto at = off;
      off += p.length();
      return nvme.do_io(false, at, p.c_str(), p.length(), pi_flags());
    });
  });
}

SPDKNVMeIODriver::write_ertr::future<> SPDKNVMeIODriver::write(
  device_id_t, uint64_t offset, bufferptr &bptr, uint16_t /*stream*/)
{
  // Streams are advisory; SPDK maps them to qpairs, of which we have one per
  // shard, so the stream hint is currently ignored.
  return nvme.do_io(/*is_write=*/true, offset, bptr.c_str(), bptr.length(),
                    pi_flags());
}

SPDKNVMeIODriver::write_ertr::future<> SPDKNVMeIODriver::writev(
  device_id_t /*device_id*/, uint64_t offset,
  bufferlist &&bl, size_t /*block_size*/, uint16_t /*stream*/)
{
  // One write per record: zero-copy when the record is already a single DMA
  // buffer (the common RBM case), otherwise a single gathered DMA write.
  // Replaces the rebuild_aligned + per-fragment-bounce path, which rebuilt every
  // buffer into non-DMA memory and issued a separate command per fragment.
  return nvme.do_writev(offset, std::move(bl), pi_flags());
}

seastar::future<std::optional<nvme_identify_controller_data_t>>
SPDKNVMeIODriver::identify_controller()
{
  nvme_identify_controller_data_t d;
  std::memcpy(d.raw, spdk_nvme_ctrlr_get_data(nvme.get_ctrlr()),
              sizeof(d.raw));
  return seastar::make_ready_future<
    std::optional<nvme_identify_controller_data_t>>(std::move(d));
}

seastar::future<std::optional<nvme_identify_namespace_data_t>>
SPDKNVMeIODriver::identify_namespace()
{
  nvme_identify_namespace_data_t d;
  std::memcpy(d.raw, spdk_nvme_ns_get_data(nvme.get_ns()), sizeof(d.raw));
  return seastar::make_ready_future<
    std::optional<nvme_identify_namespace_data_t>>(std::move(d));
}

namespace {
// Map a crimson passthrough command (linux nvme_passthru_cmd layout) to an
// spdk_nvme_cmd.
void to_spdk_cmd(const nvme_passthru_cmd &in, struct spdk_nvme_cmd &out)
{
  out = {};
  out.opc = in.opcode;
  out.nsid = in.nsid;
  // Command dwords 2-3 (CDW2/CDW3). SPDK renamed these struct fields from
  // rsvd2/rsvd3 to cdw2/cdw3 in v26.05 (commit 1f7ad2abd); both name the same
  // SQE position, so forward them under whichever name the SPDK version uses.
#if SPDK_VERSION >= SPDK_VERSION_NUM(26, 5, 0)
  out.cdw2 = in.cdw2;
  out.cdw3 = in.cdw3;
#else
  out.rsvd2 = in.cdw2;
  out.rsvd3 = in.cdw3;
#endif
  out.cdw10 = in.cdw10;
  out.cdw11 = in.cdw11;
  out.cdw12 = in.cdw12;
  out.cdw13 = in.cdw13;
  out.cdw14 = in.cdw14;
  out.cdw15 = in.cdw15;
  // The data buffer is passed separately to admin_raw()/io_raw(); the metadata
  // pointer (in.metadata) is not forwarded -- the spdk_nvme_ctrlr_cmd_*_raw API
  // used here takes no separate metadata buffer. No current caller needs it.
}
} // anonymous namespace

nvme_command_ertr::future<int> SPDKNVMeIODriver::pass_admin(
  nvme_admin_command_t &admin_cmd)
{
  struct spdk_nvme_cmd c;
  to_spdk_cmd(admin_cmd.common, c);
  return nvme.admin_raw(
    c, (void*)(uintptr_t)admin_cmd.common.addr, admin_cmd.common.data_len
  ).then([](int rc) {
    return nvme_command_ertr::make_ready_future<int>(rc);
  });
}

nvme_command_ertr::future<int> SPDKNVMeIODriver::pass_through_io(
  nvme_io_command_t &io_cmd)
{
  struct spdk_nvme_cmd c;
  to_spdk_cmd(io_cmd.common, c);
  return nvme.io_raw(
    c, (void*)(uintptr_t)io_cmd.common.addr, io_cmd.common.data_len
  ).then([](int rc) {
    return nvme_command_ertr::make_ready_future<int>(rc);
  });
}

discard_ertr::future<> SPDKNVMeIODriver::discard(uint64_t, uint64_t)
{
  // TRIM is advisory; SPDK dataset-management deallocate is not implemented in
  // this first version, so discard is a no-op (no correctness impact).
  return discard_ertr::now();
}

ceph::unique_leakable_ptr<ceph::buffer::raw>
SPDKNVMeIODriver::alloc_io_buffer(size_t len)
{
  return crimson::os::seastore::create_spdk_dma(len);
}

}
