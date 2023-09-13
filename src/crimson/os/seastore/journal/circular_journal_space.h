// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <optional>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/shared_future.hh>

#include "include/buffer.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"
#include "crimson/os/seastore/journal/record_submitter.h"
#include "crimson/os/seastore/async_cleaner.h"

namespace crimson::os::seastore {
  class SegmentProvider;
  class JournalTrimmer;
}

namespace crimson::os::seastore::journal {

class CircularBoundedJournal;
class CircularJournalSpace : public JournalAllocator {

 public:
  const std::string& get_name() const final {
    return print_name;
  }

  extent_len_t get_block_size() const final;

  bool can_write() const final {
    return (device != nullptr);
  }

  segment_nonce_t get_nonce() const final {
    return header.magic;
  }

  bool needs_roll(std::size_t length) const final;

  roll_ertr::future<> roll() final;

  write_ret write(ceph::bufferlist&& to_write) final;

  void update_modify_time(record_t& record) final {}

  close_ertr::future<> close() final {
    return write_header(
    ).safe_then([this]() -> close_ertr::future<> {
      initialized = false;
      return close_ertr::now();
    }).handle_error(
      Journal::open_for_mount_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error write_header"
      }
    );
  }

  open_ret open(bool is_mkfs) final;

 public:
  CircularJournalSpace(RBMDevice * device);

  struct cbj_header_t;
  using write_ertr = Journal::submit_record_ertr;
  /*
   * device_write_bl
   *
   * @param device address to write
   * @param bufferlist to write
   *
   */
  write_ertr::future<> device_write_bl(rbm_abs_addr offset, ceph::bufferlist &bl);

  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  using read_header_ertr = read_ertr;
  using read_header_ret = read_header_ertr::future<
	std::optional<std::pair<cbj_header_t, bufferlist>>
	>;
  /*
   * read_header
   *
   * read header block from given absolute address
   *
   * @param absolute address
   *
   */
  read_header_ret read_header();

  ceph::bufferlist encode_header();

  write_ertr::future<> write_header();


  /**
   * CircularBoundedJournal structure
   *
   * +-------------------------------------------------------+
   * |   header    | record | record | record | record | ... |
   * +-------------------------------------------------------+
   *               ^-----------block aligned-----------------^
   * <----fixed---->
   */

  struct cbj_header_t {
    // start offset of CircularBoundedJournal in the device
    journal_seq_t dirty_tail;
    journal_seq_t alloc_tail;
    segment_nonce_t magic;

    DENC(cbj_header_t, v, p) {
      DENC_START(1, 1, p);
      denc(v.dirty_tail, p);
      denc(v.alloc_tail, p);
      denc(v.magic, p);
      DENC_FINISH(p);
    }
  };

  /**
   *
   * Write position for CircularBoundedJournal
   *
   * | written to rbm |    written length to CircularBoundedJournal    | new write |
   * ----------------->------------------------------------------------>
   *                  ^      	                                       ^
   *            applied_to                                        written_to
   *
   */

  journal_seq_t get_written_to() const {
    return written_to;
  }
  rbm_abs_addr get_rbm_addr(journal_seq_t seq) const {
    return convert_paddr_to_abs_addr(seq.offset);
  }
  void set_written_to(journal_seq_t seq) {
    rbm_abs_addr addr = convert_paddr_to_abs_addr(seq.offset);
    assert(addr >= get_records_start());
    assert(addr < get_journal_end());
    written_to = seq;
  }
  device_id_t get_device_id() const {
    return device->get_device_id();
  }

  journal_seq_t get_dirty_tail() const {
    return header.dirty_tail;
  }
  journal_seq_t get_alloc_tail() const {
    return header.alloc_tail;
  }

  /* 
    Size-related interfaces
     +---------------------------------------------------------+
     |   header      | record | record | record | record | ... | 
     +---------------------------------------------------------+
     ^               ^                                         ^
     |               |                                         |
   get_journal_start |                                     get_journal_end
              get_records_start
                     <-- get_records_total_size + block_size -->
     <--------------- get_journal_size ------------------------>
  */

  size_t get_records_used_size() const {
    auto rbm_written_to = get_rbm_addr(get_written_to());
    auto rbm_tail = get_rbm_addr(get_dirty_tail());
    return rbm_written_to >= rbm_tail ?
      rbm_written_to - rbm_tail :
      rbm_written_to + get_records_total_size() + get_block_size()
      - rbm_tail;
  }
  size_t get_records_total_size() const {
    assert(device);
    // a block is for header and a block is reserved to denote the end
    return device->get_journal_size() - (2 * get_block_size());
  }
  rbm_abs_addr get_records_start() const {
    assert(device);
    return device->get_shard_journal_start() + get_block_size();
  }
  size_t get_records_available_size() const {
    return get_records_total_size() - get_records_used_size();
  }
  bool is_available_size(uint64_t size) {
    auto rbm_written_to = get_rbm_addr(get_written_to());
    auto rbm_tail = get_rbm_addr(get_dirty_tail());
    if (rbm_written_to > rbm_tail && 
	(get_journal_end() - rbm_written_to) < size &&
	size > (get_records_used_size() - 
	(get_journal_end() - rbm_written_to))) {
      return false;
    } 
    return get_records_available_size() >= size;
  }
  rbm_abs_addr get_journal_end() const {
    assert(device);
    return device->get_shard_journal_start() + device->get_journal_size();
  }

  read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) {
    assert(device);
    return device->read(offset, bptr);
  }

  seastar::future<> update_journal_tail(
    journal_seq_t dirty,
    journal_seq_t alloc) {
    header.dirty_tail = dirty;
    header.alloc_tail = alloc;
    return write_header(
    ).handle_error(
      crimson::ct_error::assert_all{
      "encountered invalid error in update_journal_tail"
    });
  }

  void set_initialized(bool init) {
    initialized = init;
  }

  void set_cbj_header(cbj_header_t& head) {
    header = head;
  }

  cbj_header_t get_cbj_header() {
    return header;
  }

 private:
  std::string print_name;
  cbj_header_t header;
  RBMDevice* device;
  journal_seq_t written_to;
  bool initialized = false;
};

std::ostream &operator<<(std::ostream &out, const CircularJournalSpace::cbj_header_t &header);

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::journal::CircularJournalSpace::cbj_header_t)

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::journal::CircularJournalSpace::cbj_header_t> : fmt::ostream_formatter {};
#endif
