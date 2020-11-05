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
#include "crimson/os/poseidonstore/poseidonstore_types.h"
#include "crimson/os/poseidonstore/device_manager.h"
#include <list>


namespace crimson::os::poseidonstore {

constexpr paddr_t WAL_START_ADDRESS = 0;
constexpr uint32_t WAL_MAGIC = 0xCCCC;
constexpr uint32_t WAL_DATA_HEADER_MAGIC = 0xBBBB;
constexpr uint32_t WAL_COMMIT_HEADER_MAGIC = 0xFFFF;

/**
 * WAL in poseidonstore manages stream of small both data and metadata
 */

class WAL {
public:
  WAL(DeviceManager &device_manager);

  using open_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using open_ret = open_ertr::future<>; 
  open_ret open();
  //void close(); //TODO

  using submit_entry_ertr = crimson::errorator<
    crimson::ct_error::erange,
    crimson::ct_error::input_output_error,
    crimson::ct_error::enospc
    >;
  using submit_entry_ret = submit_entry_ertr::future<
    std::optional<std::pair<paddr_t, wal_seq_t>>
    >;
  /*
   * submit_entry
   *
   * treat all bufferlists as a single write stream
   * , and issue those bufferlists to the WAL.
   *
   * @param bufferlists to write
   * @return <the start offset of entry submitted, 
   * 	      wal_seq_t for the last entry submitted>
   *
   */
  submit_entry_ret submit_entry(RecordRef rec, bool need_sync = true);

  struct wal_data_block_header_t;
  struct wal_header_t;
  using write_block_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::erange>;
  using write_block_ret = write_block_ertr::future<
    std::pair<paddr_t, uint64_t>
    >;
  /*
   * append_block
   *
   * append data to current write position of WAL
   *
   * Will write bl from offset to length
   * 
   * @param bufferlist to write
   * @param laddr_t where data is written
   * @return <relative address to write,
   * 	      total length written to WAL (bytes)>
   *
   */
  write_block_ret append_block(ceph::bufferlist bl, laddr_t addr);

  /*
   * overwrite_block
   *
   * overwrite data to any postion
   *
   * @param absolute address to write
   * @param bufferlist to write
   * 
   * @return <absolute address to write,
   * 	      total length written to WAL (bytes)>
   */
  write_block_ret overwrite_block(paddr_t offset, ceph::bufferlist &bl);

  using read_entry_ertr = DeviceManager::read_ertr;
  using read_entry_ret = read_entry_ertr::future<
	std::optional<std::pair<wal_data_block_header_t, bufferlist>>
	>;
  using read_super_ertr = DeviceManager::read_ertr;
  using read_super_ret = read_super_ertr::future<
	std::optional<std::pair<wal_header_t, bufferlist>>
	>;
  /*
   * read_entry
   *
   * read wal entry from given relative address
   *
   * @param relative address to read
   *
   */
  read_entry_ret read_entry(laddr_t start);
  /*
   * read_super
   *
   * read super block from given absolute address
   *
   * @param absolute address
   *
   */
  read_super_ret read_super(paddr_t start);

  /**
   * encode entry
   *
   * With multiple bufferlists -- each bufferlist match a transaction, 
   * this create a write stream.
   * 
   * Will return a bufferlist and last_seq encoded 
   *
   * @param bufferlists to encode
   * @param last_seq of encoded entries
   * @return a bufferlist encoded from input bufferlists
   *
   */
  ceph::bufferlist encode_entry(std::vector<write_item> &items, ///< [in] bufferlists to encode
				wal_seq_t &last_seq); ///< [out] last encoded wal_seq_t 
  ceph::bufferlist encode_super();
  ceph::bufferlist encode_test_super(); // for test

  
  using create_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using create_ret = create_ertr::future<>; 
  create_ret create();


  /**
   * WAL structure
   *
   * +----------------------------------------------------------------------------------------+
   * | wal header  | data block header | data | wal commit block + padding | wal header | ... |
   * +----------------------------------------------------------------------------------------+
   *               ^-------------------block aligned-----------------------^
   * <----fixed---->
   */

  enum class block_type_t : uint8_t {
    WAL_HEADER = 0x01,
    WAL_DATA_BLOCK = 0x02,
    WAL_COMMIT_BLOCK = 0x04,
    NONE = 0x00
  };

  /**
   *
   * WAL write
   *
   * We make use of three data structures -- wal_data_block_t, data, wal_commit_block_t -- to record the data to WAL. 
   * The more simple way which uses two data structures -- wal_data_block_t, data -- can be used instead of above.
   * But, in the case of very large data entry, the underlying storage can not gurauntee atomic write even if the NVMe
   * provide a larger block than before. Consequently, we don't know whether submitting data is persistent or not. 
   * There is a way to overcome this by using checksum. With checksum, it causes a overhead whenever a write happens.
   *
   * To minimize overhead and provide validity, we exploit the data structures as above. Generally, NVMe can support
   * a large block (< 512KB) with atomic write unit command. Even though the size of data is larger then the block size
   * NVMe supports, we can check the validity of entry by checking the commit block (if failure occurs, there is no
   * commit block) by simply checking magic number or something. Also, checksum can be used to detect disk corruption (optional).
   *
   * We expect that the most of incoming data can be stored as a single write stream, which has lower overhead than existing
   * way that uses a combination of system calls such as write() and sync().
   *
   */

  struct header_t {
    // to indicate this is the WAL of poseidonstore,
    // and can ealsily recognize corruption
    uint32_t magic; 
    block_type_t block_type = block_type_t::NONE;
    wal_trans_id_t tid; // transaction id

    DENC(header_t, v, p) {
      DENC_START(1, 1, p);
      denc(v.magic, p);
      denc(v.block_type, p);
      denc(v.tid, p);
      DENC_FINISH(p);
    }
  };

  struct wal_header_t {
    struct header_t header;     // header for wal
    uint64_t fsid;       // TODO
    uint64_t block_size; // aligned with block_size
    uint64_t max_size;   // max length of wal
    uint64_t used_size;       // current used_size of wal
    uint32_t error;

    uint64_t start;      // start offset of WAL
    wal_trans_id_t start_tid;
    wal_seq_t applied;   // last applied
    wal_seq_t committed; // last committed
    wal_seq_t cur_pos;
    uint64_t flag;       // represent features (reserved)
    uint8_t csum_type;   // type of checksum algoritghm used in wal_header_t
    uint64_t csum;       // checksum of entire wal_header_t

    DENC(wal_header_t, v, p) {
      DENC_START(1, 1, p);
      denc(v.header, p);
      denc(v.fsid, p);
      denc(v.block_size, p);
      denc(v.max_size, p);
      denc(v.used_size, p);
      denc(v.error, p);

      denc(v.start, p);
      denc(v.start_tid, p);
      denc(v.applied, p);
      denc(v.committed, p);
      denc(v.cur_pos, p);
      denc(v.flag, p);
      denc(v.csum_type, p);
      denc(v.csum, p);
      DENC_FINISH(p);
    }
  } wal_header;

  // data block
  struct wal_data_block_header_t {
    struct header_t header;
    uint16_t  header_length;   // length of wal_data_block_header_t
    uint64_t  data_length;   // length of data
    uint64_t total_length;    // total length (wal_data_block_header_t + data + wal_commit_block_t)
    uint64_t flag;            // reserved
    uint64_t addr_to_partition; // address where the data block will be written

    DENC(wal_data_block_header_t, v, p) {
      DENC_START(1, 1, p);
      denc(v.header, p);
      denc(v.header_length, p);
      denc(v.data_length, p);
      denc(v.total_length, p);
      denc(v.flag, p);
      denc(v.addr_to_partition, p);
      DENC_FINISH(p);
    }
  };

  // commit block
  struct wal_commit_block_t {
    struct header_t header;
    uint32_t length;      // length of wal_commit_block_t + padding
    uint8_t csum_type;    // checksum if necessary
    uint64_t csum;        // checksum (data + commit_block_t)

    DENC(wal_commit_block_t, v, p) {
      DENC_START(1, 1, p);
      denc(v.header, p);
      denc(v.length, p);
      denc(v.csum_type, p);
      denc(v.csum, p);
      DENC_FINISH(p);
    }
  };

  /**
   * 
   * Write position for WAL
   *
   * 1) before a write on wal
   * ----------->
   *            ^
   *       cur_pos
   *
   * 2) after a write on wal
   *
   *      |  written to the data partition | written length to WAL |
   * -------------------------------------->----------------------->
   *      ^      	                   ^                       ^
   * last_applied                     last_committed             cur_pos
   *
   */

  using update_w_info_ertr = crimson::errorator<
    crimson::ct_error::erange,
    crimson::ct_error::enospc,
    crimson::ct_error::input_output_error
    >;

  /*
   * update_last_committed
   *
   * update information regarding WAL position after a write is completed.
   *
   * @param wal_seq_t the last entry
   */
  update_w_info_ertr::future<> update_last_committed(wal_seq_t &last) {
    if (get_wal_size() + last.length > max_size) {
      return crimson::ct_error::erange::make();
    }
    last_committed = last;
    used_size += last.length;
    return update_w_info_ertr::now();
  }

  update_w_info_ertr::future<> update_cur_pos(
      wal_seq_t &cur_pos,
      wal_seq_t last) {
    if (last.addr + last.length >= max_size) {
      cur_pos.addr = last.addr + last.length - max_size;
    } else {
      cur_pos.addr = last.addr + last.length;
    }
    cur_pos.id = cur_pos.id + last.id + 1;
    return update_w_info_ertr::now();
  }

  size_t get_wal_size() const {
    return used_size;
  }

  size_t get_total_wal_size() const {
    return max_size;
  }

  wal_trans_id_t get_next_tx_id() {
    return cur_pos.id + 1;
  }

  wal_seq_t get_cur_pos() const {
    return cur_pos;
  }

  wal_seq_t get_last_committed() const {
    return last_committed;
  }

  wal_seq_t get_last_applied() const {
    return last_applied;
  }

  paddr_t get_block_start_abs_addr() const {
    return wal_header.start;
  }

  paddr_t get_abs_addr(laddr_t addr) const {
    /* TODO: support list based commit */
    return addr + get_block_start_abs_addr();
  }

  void reset_cur_pos(wal_seq_t &cur_pos) {
    cur_pos.addr = 0;
    cur_pos.length = 0;
  }

  size_t get_aligned_entry_size(uint64_t len) const; 

  size_t get_available_size() const {
    return max_size - used_size;
  }

  /*
   * update_last_applied
   *
   * After finishing flush to the persistent device, last committed should be updated.
   * This will update last_applied up until the position which is the last entry stored to the storage.
   *
   * @param wal_seq_t the last entry written to the persisten storage
   *
   */
  bool update_last_applied(wal_seq_t last_seq) {
    if (used_size == 0) {
      // empty
      return false;
    }
    if (last_applied == last_seq) {
      return false;
    }
    // Do not allow continuous write if the length of entry hits the end of log
    if (last_seq.addr + last_seq.length > max_size) {
      return false;
    }
    // larger than cur_pos
    if (cur_pos.addr > last_seq.addr && 
	cur_pos.addr < last_seq.addr + last_seq.length) {
      return false;
    }

    last_applied = last_seq;
    return true;
  }

  void update_used_size(wal_seq_t cur_pos, wal_seq_t last_applied) {
    if (cur_pos.addr < last_applied.addr) {
      used_size = max_size - (last_applied.addr - cur_pos.addr);
      return;
    }
    used_size = cur_pos.addr - (last_applied.addr + last_applied.length);
    return;
  }

  size_t get_aligned_multi_entries_size(uint64_t len, int entries) const;
  size_t get_unaligned_entry_size(uint64_t len) const;

  template <class T>
  bool check_valid_entry(T &header) const;

  create_ret sync_super();

  void do_defered_works();
  void do_after_committed();
  void do_after_applied();

private:
  wal_seq_t cur_pos;      // current wal position where next entry is going to be written
  wal_seq_t last_committed;   // store last committed entry recored in the wal
  wal_seq_t last_applied;     // applied entry stored in the data partition
  size_t used_size = 0;
  size_t max_size = 0;
  uint64_t block_size = 0;
  uint64_t max_entry_length = 0;
  DeviceManager &device_manager;
  std::list<RecordRef> trans_for_commit; // TODO: will be lock-free list 
  std::list<RecordRef> trans_for_apply;
  bool need_trigger_flush = false;
};

std::ostream &operator<<(std::ostream &out, const WAL::block_type_t &t);
std::ostream &operator<<(std::ostream &out, const WAL::header_t &header);
std::ostream &operator<<(std::ostream &out, const WAL::wal_header_t &header);
std::ostream &operator<<(std::ostream &out, const WAL::wal_data_block_header_t &header);

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::poseidonstore::WAL::header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::poseidonstore::WAL::wal_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::poseidonstore::WAL::wal_data_block_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::poseidonstore::WAL::wal_commit_block_t)
