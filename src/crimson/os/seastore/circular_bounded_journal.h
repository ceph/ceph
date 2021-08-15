// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/log.h"

#include <boost/intrusive_ptr.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/denc.h"

#include "crimson/osd/exceptions.h"
#include "crimson/os/seastore/journal.h"
#include "include/uuid.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/nvmedevice.h"
#include <list>


namespace crimson::os::seastore {

constexpr rbm_abs_addr CBJOURNAL_START_ADDRESS = 0;
constexpr uint64_t CBJOURNAL_MAGIC = 0xCCCC;
using NVMeBlockDevice = nvme_device::NVMeBlockDevice;

/**
 * CircularBoundedJournal
 *
 * TODO: move record from CBJournal to RandomBlockManager
 *
 */

class CBJournal : public Journal {
public:
  struct mkfs_config_t {
    std::string path;
    paddr_t start;
    paddr_t end;
    size_t block_size = 0;
    size_t total_size = 0;
    device_id_t device_id = 0;
    seastore_meta_t meta;
  };

  CBJournal(NVMeBlockDevice* device, const std::string path);
  ~CBJournal() {}

  open_for_write_ret open_for_write() final;
  open_for_write_ret open_for_write(rbm_abs_addr start);
  close_ertr::future<> close() final;

  /*
   * submit_record
   *
   * treat all bufferlists as a single write stream
   * , and issue those bufferlists to the CBJournal.
   *
   * @param bufferlists to write
   * @return <the start offset of entry submitted,
   * 	      journal_seq_t for the last entry submitted>
   *
   */
  submit_record_ret submit_record(
    record_t &&record,
    OrderingHandle &handle
  ) final;

  open_for_write_ertr::future<> _open_device(const std::string path);

  struct cbj_header_t;
  using write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::erange>;
  using write_ret = write_ertr::future<
    std::pair<paddr_t, uint64_t>
    >;
  /*
   * append_record
   *
   * append data to current write position of CBJournal
   *
   * Will write bl from offset to length
   *
   * @param bufferlist to write
   * @param laddr_t where data is written
   * @return <relative address to write,
   * 	      total length written to CBJournal (bytes)>
   *
   */
  write_ertr::future<> append_record(ceph::bufferlist bl, rbm_abs_addr addr);
  /*
   * device_write_bl
   *
   * @param device address to write
   * @param bufferlist to write
   *
   * @return <address to write,
   * 	      total length written to CBJournal (bytes)>
   */
  write_ertr::future<> device_write_bl(rbm_abs_addr offset, ceph::bufferlist &bl);

  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  using read_record_ertr = read_ertr;
  using read_record_ret = read_record_ertr::future<
	std::optional<std::pair<record_header_t, bufferlist>>
	>;
  using read_super_ertr = read_ertr;
  using read_super_ret = read_super_ertr::future<
	std::optional<std::pair<cbj_header_t, bufferlist>>
	>;
  /*
   * read_record
   *
   * read record from given address
   *
   * @param address to read
   *
   */
  read_record_ret read_record(paddr_t offset);
  /*
   * read_super
   *
   * read super block from given absolute address
   *
   * @param absolute address
   *
   */
  read_super_ret read_super(rbm_abs_addr start);

  ceph::bufferlist encode_super();

  using mkfs_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg
  >;
  using mkfs_ret = mkfs_ertr::future<>;
  mkfs_ret mkfs(mkfs_config_t& config);


  /**
   * CBJournal structure
   *
   * +-------------------------------------------------------+
   * |   header    | record | record | record | record | ... |
   * +-------------------------------------------------------+
   *               ^-----------block aligned-----------------^
   * <----fixed---->
   */


  /**
   *
   * CBJournal write
   *
   * NVMe supports a large block write (< 512KB) with atomic write unit command.
   * With this command, we expect that the most of incoming data can be stored
   * as a single write call, which has lower overhead than existing
   * way that uses a combination of system calls such as write() and sync().
   *
   */

  struct cbj_header_t {
    uint64_t magic;
    uuid_d uuid;
    uint64_t block_size; // aligned with block_size
    uint64_t size;   // max length of journal
    uint64_t used_size;  // current used_size of journal
    uint32_t error;      // reserved

    rbm_abs_addr start_offset;      // start offset of CBJournal
    rbm_abs_addr committed_to;
    rbm_abs_addr written_to;
    rbm_abs_addr applied_to;

    uint64_t flag;       // represent features (reserved)
    uint8_t csum_type;   // type of checksum algoritghm used in cbj_header_t
    uint64_t csum;       // checksum of entire cbj_header_t
    uint32_t cur_segment_seq;

    rbm_abs_addr start; // start address of the device
    rbm_abs_addr end;   // start address of the device
    device_id_t device_id;

    DENC(cbj_header_t, v, p) {
      DENC_START(1, 1, p);
      denc(v.magic, p);
      denc(v.uuid, p);
      denc(v.block_size, p);
      denc(v.size, p);
      denc(v.used_size, p);
      denc(v.error, p);

      denc(v.start_offset, p);

      denc(v.committed_to, p);
      denc(v.written_to, p);
      denc(v.applied_to, p);

      denc(v.flag, p);
      denc(v.csum_type, p);
      denc(v.csum, p);
      denc(v.cur_segment_seq, p);
      denc(v.start, p);
      denc(v.end, p);
      denc(v.device_id, p);

      DENC_FINISH(p);
    }
  } header;

  /**
   *
   * Write position for CBJournal
   *
   * |  written to the rbm  | written length to CBJournal | new write |
   * ----------------------->----------------------------->------------>
   * ^      	            ^                             ^
   * applied_to             committed_to                  written_to
   *
   */

  size_t get_used_size() const {
    return used_size;
  }
  size_t get_total_size() const {
    return size;
  }
  rbm_abs_addr get_start_addr() const {
    return header.start_offset;
  }
  size_t get_available_size() const {
    return size - used_size;
  }

  void update_applied_to(rbm_abs_addr addr, uint32_t len) {
    rbm_abs_addr new_applied_to = addr;
    used_size = committed_to >= new_applied_to ?
		written_to - (new_applied_to + len) :
		written_to + size - (new_applied_to + len);
    applied_to = new_applied_to;
  }

  write_ertr::future<> sync_super();

  read_record_ret return_record(record_header_t& header, bufferlist bl);
  bool validate_metadata(record_header_t& h, bufferlist bl);

  void set_write_pipeline(WritePipeline *_write_pipeline) final {
    write_pipeline = _write_pipeline;
  }

  rbm_abs_addr get_written_to() {
    return written_to;
  }
  rbm_abs_addr get_committed_to() {
    return committed_to;
  }
  rbm_abs_addr get_applied_to() {
    return applied_to;
  }
  device_id_t get_device_id() const {
    return header.device_id;
  }
  size_t get_block_size() {
    return block_size;
  }
private:
  size_t used_size = 0;
  size_t size = 0;
  uint64_t block_size = 0;
  uint64_t max_entry_length = 0;
  rbm_abs_addr written_to = 0;
  rbm_abs_addr committed_to = 0;
  rbm_abs_addr applied_to = 0;
  uint32_t cur_segment_seq;
  rbm_abs_addr start;
  rbm_abs_addr end;
  NVMeBlockDevice* device;
  std::string path;
  WritePipeline *write_pipeline = nullptr;
};

std::ostream &operator<<(std::ostream &out, const CBJournal::cbj_header_t &header);
}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::CBJournal::cbj_header_t)
