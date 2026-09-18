//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>

#include <linux/nvme_ioctl.h>

namespace crimson::os::seastore::random_block_device::nvme {
/*
 * NVMe protocol structures (nvme_XX, identify_XX)
 *
 * All structures relative to NVMe protocol are following NVMe protocol v1.4
 * (latest). NVMe is protocol for fast interfacing between user and SSD device.
 * We selectively adopted features among various NVMe features to ease
 * implementation. And also, NVMeBlockDevice provides generic command submission
 * APIs for IO and Admin commands. Please use pass_through_io() and pass_admin()
 * to do it.
 *
 * For more information about NVMe protocol, refer https://nvmexpress.org/
 */
struct nvme_identify_command_t {
  uint32_t common_dw[10];

  uint32_t cns : 8;
  uint32_t reserved : 8;
  uint32_t cnt_id : 16;

  static const uint8_t CNS_NAMESPACE = 0x00;
  static const uint8_t CNS_CONTROLLER = 0x01;
};

struct nvme_format_nvm_command_t {
  uint32_t common_dw[10];

  uint8_t lbaf : 4;
  uint8_t mset : 1;
  uint8_t pi : 3;
  uint8_t pil : 1;

  static const uint8_t PROTECT_INFORMATION_TYPE_2 = 2;
};

struct nvme_admin_command_t {
  union {
    nvme_passthru_cmd common;
    nvme_identify_command_t identify;
    nvme_format_nvm_command_t format;
  };

  static const uint8_t OPCODE_IDENTIFY = 0x06;
  static const uint8_t OPCODE_FORMAT_NVM = 0x80;
};

// Optional Admin Command Support (OACS)
// Indicates optional commands are supported by SSD or not
struct oacs_t {
  uint16_t unused : 5;
  uint16_t support_directives : 1; // Support multi-stream
  uint16_t unused2 : 10;
};

struct nvme_identify_controller_data_t {
  union {
    struct {
      uint8_t unused[256];  // [255:0]
      oacs_t oacs;          // [257:256]
      uint8_t unused2[270]; // [527:258]
      uint16_t awupf;       // [529:528]
    };
    uint8_t raw[4096];
  };
};

// End-to-end Data Protection Capabilities (DPC)
// Indicates type of E2E data protection supported by SSD
struct dpc_t {
  uint8_t support_type1 : 1;
  uint8_t support_type2 : 1;
  uint8_t support_type3 : 1;
  uint8_t support_first_meta : 1;
  uint8_t support_last_meta : 1;
  uint8_t reserved : 3;
};

// End-to-end Data Protection Type Settings (DPS)
// Indicates enabled type of E2E data protection
struct dps_t {
  uint8_t protection_type : 3;
  uint8_t protection_info : 1;
  uint8_t reserved : 4;
};

// Namespace Features (NSFEAT)
// Indicates features of namespace
struct nsfeat_t {
  uint8_t thinp : 1;
  uint8_t nsabp : 1;
  uint8_t dae : 1;
  uint8_t uid_reuse : 1;
  uint8_t opterf : 1; // Support NPWG, NPWA
  uint8_t reserved : 3;
};

// LBA Format (LBAF)
// Indicates LBA format (metadata size, data size, performance)
struct lbaf_t {
  uint32_t ms : 16;
  uint32_t lbads : 8;
  uint32_t rp : 2;
  uint32_t reserved : 6;
};

struct flbas_t {
  uint8_t lba_index : 4;
  uint8_t ms_transferred :1;
  uint8_t reserved : 3;
};

struct nvme_identify_namespace_data_t {
  union {
    struct {
      uint8_t unused[24];   // [23:0]
      nsfeat_t nsfeat;      // [24]
      uint8_t nlbaf;      // [25]
      flbas_t flbas;      // [26]
      uint8_t unused2;   // [27]
      dpc_t dpc;            // [28]
      dps_t dps;            // [29]
      uint8_t unused3[34];  // [63:30]
      uint16_t npwg;        // [65:64]
      uint16_t npwa;        // [67:66]
      uint8_t unused4[60];  // [127:68]
      lbaf_t lbaf[64];         // [383:128]
    };
    uint8_t raw[4096];
  };
  // meta size value to use device-level checksum
  static const uint8_t METASIZE_FOR_CHECKSUM_OFFLOAD = 8;
};

struct nvme_rw_command_t {
  uint32_t common_dw[10];

  uint64_t s_lba;

  uint32_t nlb : 16; // 0's based value
  uint32_t reserved : 4;
  uint32_t d_type : 4;
  uint32_t reserved2 : 2;
  uint32_t prinfo_prchk : 3;
  uint32_t prinfo_pract : 1;
  uint32_t fua : 1;
  uint32_t lr : 1;

  uint32_t reserved3 : 16;
  uint32_t dspec : 16;

  static const uint32_t DTYPE_STREAM = 1;

  static const uint8_t PROTECT_INFORMATION_ACTION_ENABLE = 1;
  static const uint8_t PROTECT_INFORMATION_CHECK_GUARD = 4;
  static const uint8_t PROTECT_INFORMATION_CHECK_APPLICATION_TAG = 2;
  static const uint8_t PROTECT_INFORMATION_CHECK_LOGICAL_REFERENCE_TAG = 1;
};

struct nvme_io_command_t {
  union {
    nvme_passthru_cmd common;
    nvme_rw_command_t rw;
  };
  static const uint8_t OPCODE_WRITE = 0x01;
  static const uint8_t OPCODE_READ = 0x02;
};

}
