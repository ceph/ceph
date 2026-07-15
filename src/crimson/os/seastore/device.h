// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

#include "include/buffer_fwd.h"

#include "common/fmt_common.h"
#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/common/smp_helpers.h"

namespace crimson::os::seastore {

using magic_t = uint64_t;

struct device_spec_t {
  magic_t magic = 0;
  device_type_t dtype = device_type_t::NONE;
  device_id_t id = DEVICE_ID_NULL;
  DENC(device_spec_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.magic, p);
    denc(v.dtype, p);
    denc(v.id, p);
    DENC_FINISH(p);
  }
};

std::ostream& operator<<(std::ostream&, const device_spec_t&);

using secondary_device_set_t =
  std::map<device_id_t, device_spec_t>;

struct device_config_t {
  bool major_dev = false;
  device_spec_t spec;
  seastore_meta_t meta;
  secondary_device_set_t secondary_devices;
  DENC(device_config_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.major_dev, p);
    denc(v.spec, p);
    denc(v.meta, p);
    denc(v.secondary_devices, p);
    DENC_FINISH(p);
  }
  static device_config_t create_primary(
    uuid_d new_osd_fsid,
    device_id_t id,
    device_type_t d_type,
    secondary_device_set_t sds) {
    return device_config_t{
             true,
             device_spec_t{
               (magic_t)std::rand(),
		d_type,
		id},
             seastore_meta_t{new_osd_fsid},
             sds};
   }
  static device_config_t create_secondary(
    uuid_d new_osd_fsid,
    device_id_t id,
    device_type_t d_type,
    magic_t magic) {
    return device_config_t{
             false,
             device_spec_t{
               magic,
               d_type,
               id},
             seastore_meta_t{new_osd_fsid},
             secondary_device_set_t()};
  }
};

std::ostream& operator<<(std::ostream&, const device_config_t&);

/* -----------------------------------------------------------------------
 * Unified superblock written at offset 0 on every Crimson device type:
 *   HDD (block-segment), ZBD (zoned-block), RBM (random-block / NVMe)
 * -----------------------------------------------------------------------
 */

/// On-disk superblock magic type (23 bytes, matching classic BlueStore prefix size).
/// Written outside the DENC envelope at device offset 0.
using superblock_magic_t = std::array<std::byte, 23>;

/// Magic identifying all Crimson device superblocks: "CRIMSON_DEVICE\0..."
constexpr superblock_magic_t CRIMSON_DEVICE_SUPERBLOCK_MAGIC = {{
    std::byte{'C'}, std::byte{'R'}, std::byte{'I'}, std::byte{'M'},
    std::byte{'S'}, std::byte{'O'}, std::byte{'N'}, std::byte{'_'},
    std::byte{'D'}, std::byte{'E'}, std::byte{'V'}, std::byte{'I'},
    std::byte{'C'}, std::byte{'E'}, std::byte{0},   std::byte{0},
    std::byte{0},   std::byte{0},   std::byte{0},   std::byte{0},
    std::byte{0},   std::byte{0},   std::byte{0}
}};

/// Size constants for the on-disk prefix (magic + null padding) that
/// precedes the DENC-encoded superblock, matching the classic BlueStore
/// 60-byte label prefix layout.
constexpr size_t SUPERBLOCK_MAGIC_SIZE = sizeof(CRIMSON_DEVICE_SUPERBLOCK_MAGIC); // 23
constexpr size_t SUPERBLOCK_MAGIC_PAD  = 37; // matching the UID block in BlueStore's
constexpr size_t SUPERBLOCK_HEADER_PREFIX = SUPERBLOCK_MAGIC_SIZE + SUPERBLOCK_MAGIC_PAD;
static_assert(SUPERBLOCK_HEADER_PREFIX == 60);

/// Current superblock format version
constexpr uint8_t CRIMSON_DEVICE_SUPERBLOCK_VERSION = 1;

/// Feature bits stored in device_superblock_t::feature
enum class device_feature_t : uint64_t {
  NVME_END_TO_END_PROTECTION = 1,
};

/// Unified per-shard layout info for all device types.
/// Fields unused by a given device type are left at their zero default.
struct device_shard_info_t {
  size_t size = 0;                    ///< usable shard size in bytes (all)
  size_t segments = 0;                ///< number of segments (HDD/ZBD; 0 for RBM)
  uint64_t first_segment_offset = 0;  ///< byte offset of first segment (HDD/ZBD)
  uint64_t tracker_offset = 0;        ///< byte offset of segment-state tracker (HDD)
  uint64_t start_offset = 0;          ///< byte offset of shard start (RBM)

  DENC(device_shard_info_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.size, p);
    denc(v.segments, p);
    denc(v.first_segment_offset, p);
    denc(v.tracker_offset, p);
    denc(v.start_offset, p);
    DENC_FINISH(p);
  }

  auto fmt_print_ctx(auto& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(),
      "device_shard_info(size={:#x}, segments={}, "
      "first_segment_offset={:#x}, tracker_offset={:#x}, start_offset={:#x})",
      size, segments, first_segment_offset, tracker_offset, start_offset);
  }
};

std::ostream& operator<<(std::ostream&, const device_shard_info_t&);

/// Unified on-disk superblock for all Crimson device types.
/// Fields specific to a device type are zero for other types.
struct device_superblock_t {
  // --- Fixed header (all device types) ---
  // NOTE: the magic string is written/read separately at device offset 0,
  // outside this DENC-encoded structure (see SUPERBLOCK_HEADER_PREFIX).
  uint16_t version = CRIMSON_DEVICE_SUPERBLOCK_VERSION;
  uint16_t shard_num = 0;
  size_t segment_size = 0;   ///< logical segment size in bytes (HDD/ZBD; 0 for RBM)
  size_t block_size = 0;
  device_config_t config;

  // --- Device-type-specific size information (union concept) ---
  size_t total_size = 0;          ///< total device capacity in bytes (RBM)
  uint64_t journal_size = 0;      ///< journal area size in bytes (RBM)
  size_t segment_capacity = 0;    ///< usable bytes/segment = zone_capacity*zones_per_segment (ZBD)
  size_t zones_per_segment = 0;   ///< zones per segment (ZBD)
  size_t zone_size = 0;           ///< physical zone size in bytes (ZBD)
  size_t zone_capacity = 0;       ///< usable zone capacity in bytes (ZBD)

  // --- Per-shard information ---
  std::vector<device_shard_info_t> shard_infos;

  // --- RBM-specific remaining fields ---
  checksum_t crc = 0;
  uint64_t feature = 0;          ///< device_feature_t bits
  uint32_t nvme_block_size = 0;  ///< NVMe logical block size (E2E protection)

  DENC(device_superblock_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.version, p);
    denc(v.shard_num, p);
    denc(v.segment_size, p);
    denc(v.block_size, p);
    denc(v.config, p);
    denc(v.total_size, p);
    denc(v.journal_size, p);
    denc(v.segment_capacity, p);
    denc(v.zones_per_segment, p);
    denc(v.zone_size, p);
    denc(v.zone_capacity, p);
    denc(v.shard_infos, p);
    denc(v.crc, p);
    denc(v.feature, p);
    denc(v.nvme_block_size, p);
    DENC_FINISH(p);
  }

  void validate() const;

  /// Create a superblock for a segmented (HDD/SSD) device.
  static device_superblock_t make_segmented(
    uint16_t shard_num,
    size_t segment_size,
    size_t block_size,
    device_config_t config,
    std::vector<device_shard_info_t> shard_infos)
  {
    device_superblock_t sb;
    sb.shard_num = shard_num;
    sb.segment_size = segment_size;
    sb.block_size = block_size;
    sb.config = std::move(config);
    sb.shard_infos = std::move(shard_infos);
    return sb;
  }

  /// Create a superblock for a ZBD (zone-based) segmented device.
  static device_superblock_t make_zbd(
    uint16_t shard_num,
    size_t segment_size,
    size_t block_size,
    device_config_t config,
    size_t zone_size,
    size_t zone_capacity,
    size_t zones_per_segment,
    std::vector<device_shard_info_t> shard_infos)
  {
    device_superblock_t sb;
    sb.shard_num = shard_num;
    sb.segment_size = segment_size;
    sb.block_size = block_size;
    sb.config = std::move(config);
    sb.segment_capacity = zone_capacity * zones_per_segment;
    sb.zones_per_segment = zones_per_segment;
    sb.zone_size = zone_size;
    sb.zone_capacity = zone_capacity;
    sb.shard_infos = std::move(shard_infos);
    return sb;
  }

  /// Create a superblock for an RBM (random-block) device.
  static device_superblock_t make_rbm(
    uint16_t shard_num,
    size_t block_size,
    size_t total_size,
    uint64_t journal_size,
    device_config_t config,
    std::vector<device_shard_info_t> shard_infos)
  {
    device_superblock_t sb;
    sb.shard_num = shard_num;
    sb.block_size = block_size;
    sb.total_size = total_size;
    sb.journal_size = journal_size;
    sb.config = std::move(config);
    sb.shard_infos = std::move(shard_infos);
    return sb;
  }

  bool is_end_to_end_data_protection() const {
    return feature & (uint64_t)device_feature_t::NVME_END_TO_END_PROTECTION;
  }
  void set_end_to_end_data_protection() {
    feature |= (uint64_t)device_feature_t::NVME_END_TO_END_PROTECTION;
  }

  auto fmt_print_ctx(auto& ctx) const -> decltype(ctx.out()) {
    fmt::format_to(ctx.out(),
      "device_superblock(version={}, shard_num={}, "
      "segment_size={:#x}, block_size={:#x}, config={}, "
      "total_size={:#x}, journal_size={:#x}, "
      "segment_capacity={:#x}, zones_per_segment={}, "
      "zone_size={:#x}, zone_capacity={:#x}, "
      "crc={}, feature={:#x}, nvme_block_size={}, shards:[",
      (unsigned)version, shard_num,
      segment_size, block_size, config,
      total_size, journal_size,
      segment_capacity, zones_per_segment,
      zone_size, zone_capacity,
      crc, feature, nvme_block_size);
    for (const auto& si : shard_infos) {
      fmt::format_to(ctx.out(), "{},", si);
    }
    return fmt::format_to(ctx.out(), "])");
  }
};

std::ostream& operator<<(std::ostream&, const device_superblock_t&);

class Device;
using DeviceRef = std::unique_ptr<Device>;

/**
 * Device
 *
 * Represents a general device regardless of the underlying medium.
 */
class Device {
// interfaces used by device
public:
  virtual ~Device() {}

  virtual seastar::future<> start(uint32_t shard_nums) {
    return seastar::now();
  }

  virtual seastar::future<> stop() {
    return seastar::now();
  }

  // called on the shard to get this shard device;
  virtual Device& get_sharded_device(store_index_t store_index = 0) {
    return *this;
  }

  using access_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::permission_denied,
    crimson::ct_error::enoent>;

  using mkfs_ertr = access_ertr;
  using mkfs_ret = mkfs_ertr::future<>;
  virtual mkfs_ret mkfs(device_config_t) = 0;

  using mount_ertr = access_ertr;
  using mount_ret = access_ertr::future<>;
  virtual mount_ret mount() = 0;

  static seastar::future<DeviceRef> make_device(
    const std::string &device,
    device_type_t dtype);

// interfaces used by each device shard
public:
  virtual device_id_t get_device_id() const = 0;

  virtual magic_t get_magic() const = 0;

  virtual device_type_t get_device_type() const = 0;

  virtual backend_type_t get_backend_type() const = 0;

  virtual const seastore_meta_t &get_meta() const = 0;

  virtual extent_len_t get_block_size() const = 0;

  virtual std::size_t get_available_size() const = 0;

  virtual secondary_device_set_t& get_secondary_devices() = 0;

  virtual bool is_end_to_end_data_protection() const {
    return false;
  }

  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  virtual close_ertr::future<> close() = 0;

  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  virtual read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) = 0;

  virtual read_ertr::future<> readv(paddr_t addr, std::vector<bufferptr> vecs) = 0;

  read_ertr::future<ceph::bufferptr> read(
    paddr_t addr,
    size_t len
  ) {
    auto ptrref = std::make_unique<ceph::bufferptr>(
      buffer::create_page_aligned(len));
    return read(addr, len, *ptrref
    ).safe_then([ptrref=std::move(ptrref)]() mutable {
      return read_ertr::make_ready_future<bufferptr>(std::move(*ptrref));
    });
  }
  virtual read_ertr::future<uint32_t> get_shard_nums() {
    return read_ertr::make_ready_future<uint32_t>(seastar::smp::count);
  }
};

using check_create_device_ertr = Device::access_ertr;
using check_create_device_ret = check_create_device_ertr::future<>;
check_create_device_ret check_create_device(
  const std::string path,
  size_t size);
}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::device_spec_t)
WRITE_CLASS_DENC(crimson::os::seastore::device_config_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::device_shard_info_t)
WRITE_CLASS_DENC(crimson::os::seastore::device_superblock_t)

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::device_config_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::device_spec_t> : fmt::ostream_formatter {};
#endif
