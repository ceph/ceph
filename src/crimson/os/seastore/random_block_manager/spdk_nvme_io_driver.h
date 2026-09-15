//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <string>
#include <vector>

#include "crimson/os/seastore/spdk_nvme_io.h"
#include "nvme_io_driver.h"

namespace crimson::os::seastore::random_block_device::nvme {

/**
 * SPDKNVMeIODriver
 *
 * Tier-2 SPDK transport for the random-block (RBM) backend: an NVMeIODriver
 * whose data path runs over an SPDK NVMe queue pair (composing the same
 * spdk_nvme_io helper as SPDKBlockIODriver), plus the NVMe control plane
 * implemented against the SPDK controller:
 *
 *   - capabilities and identify from spdk_nvme_ctrlr_get_data() /
 *     spdk_nvme_ns_get_data() (the SPDK analogue of the NVME_IOCTL identify),
 *   - end-to-end protection applied inline via the PRACT|PRCHK_GUARD io flags,
 *   - admin / IO passthrough via spdk_nvme_ctrlr_cmd_{admin,io}_raw().
 *
 * One instance per shard. The transport id is supplied at construction; open()'s
 * path argument is ignored.
 */
class SPDKNVMeIODriver final : public NVMeIODriver {
public:
  explicit SPDKNVMeIODriver(std::string transport_id)
    : transport_id(std::move(transport_id)) {}

  // BlockIODriver
  open_ertr::future<seastar::stat_data> open(
    const std::string &path, seastar::open_flags flags) final;
  close_ertr::future<> close() final;
  read_ertr::future<> read(
    device_id_t device_id, uint64_t offset, size_t len, bufferptr &out) final;
  read_ertr::future<> readv(
    device_id_t device_id, uint64_t offset, std::vector<bufferptr> ptrs) final;
  ceph::unique_leakable_ptr<ceph::buffer::raw> alloc_io_buffer(size_t len) final;
  bool dma_passthrough() const final { return true; }

  // NVMeIODriver
  const caps_t &get_caps() const final { return caps; }
  int get_namespace_id() const final { return namespace_id; }
  void set_protection_info(bool enabled, uint32_t block_size) final;

  write_ertr::future<> write(
    device_id_t device_id, uint64_t offset,
    bufferptr &bptr, uint16_t stream) final;
  write_ertr::future<> writev(
    device_id_t device_id, uint64_t offset,
    bufferlist &&bl, size_t block_size, uint16_t stream) final;

  seastar::future<std::optional<nvme_identify_controller_data_t>>
    identify_controller() final;
  seastar::future<std::optional<nvme_identify_namespace_data_t>>
    identify_namespace() final;

  nvme_command_ertr::future<int> pass_admin(
    nvme_admin_command_t &admin_cmd) final;
  nvme_command_ertr::future<int> pass_through_io(
    nvme_io_command_t &io_cmd) final;

  discard_ertr::future<> discard(uint64_t offset, uint64_t len) final;

private:
  uint32_t pi_flags() const;

  std::string transport_id;
  crimson::os::seastore::spdk_nvme_io nvme;
  caps_t caps;
  bool pi_enabled = false;
  uint32_t nvme_block_size = 0;
  int namespace_id = 0;
};

}
