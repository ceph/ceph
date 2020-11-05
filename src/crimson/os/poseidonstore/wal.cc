// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>

#include "include/intarith.h"
#include "crimson/os/poseidonstore/device_manager.h"
#include "crimson/os/poseidonstore/wal.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::poseidonstore {

std::ostream &operator<<(std::ostream &out, const WAL::header_t &header)
{
  return out << "header_t(magic="
             << header.magic << ", block_type="
             << header.block_type
	     << ", tid="
	     << header.tid
             << ")";
}

std::ostream &operator<<(std::ostream &out, const WAL::wal_header_t &header)
{
  return out << "wal_header_t(header=" << header.header 
	     << ", fsid=" << header.fsid 
	     << ", block_size=" << header.block_size 
	     << ", max_size=" << header.max_size 
	     << ", used_size=" << header.used_size 
	     << ", error=" << header.error 
	     << ", start=" << header.start 
	     << ", stat_tid=" << header.start_tid 
	     << ", applied="<< header.applied
	     << ", committed="<< header.committed 
	     << ", cur_pos=" << header.cur_pos 
	     << ", flsg=" << header.flag 
	     << ", csum_type=" << header.csum_type 
	     << ", csum=" << header.csum
             << ")";
}

std::ostream &operator<<(std::ostream &out, const WAL::wal_data_block_header_t &header)
{
  return out << "wal_data_block_header_t(header="
             << header.header << ", header_length="
             << header.header_length
	     << ", data_length="
	     << header.data_length
	     << ", total_length="
	     << header.total_length
	     <<", flag="
	     << header.flag
             << ")";
}

std::ostream &operator<<(std::ostream &out, const WAL::block_type_t &t)
{
  switch (t) {
    case WAL::block_type_t::WAL_HEADER:
      return out << "WAL_HEADER";
    case WAL::block_type_t::WAL_DATA_BLOCK:
      return out << "WAL_DATA_BLOCK";
    case WAL::block_type_t::WAL_COMMIT_BLOCK:
      return out << "WAL_COMMIT_BLOCK";
    default:
      return out << "UNKNOWN";
  }
}

WAL::WAL(DeviceManager &device_manager)
  : max_size(device_manager.get_size()),
    block_size(device_manager.get_block_size()),
    max_entry_length(device_manager.get_block_size() * 1024),
    device_manager(device_manager) {}

/* For test */
WAL::create_ret
WAL::create()
{
  ceph::bufferlist bl;
  try {
    bl = encode_test_super();
  } catch (ceph::buffer::error &e) {
    logger().debug("unable to encode super block to underlying deivce");
    return crimson::ct_error::input_output_error::make();
  }
  logger().debug(
    "initialize superblock in WAL, length {}",
    bl.length());
  return overwrite_block(WAL_START_ADDRESS, bl
      ).safe_then([this](auto p){},
      create_ertr::pass_further{},
      crimson::ct_error::all_same_way([](auto e) {
	logger().debug("overwrite error");
	ceph_assert(0 == "TODO");
	//return crimson::ct_error::input_output_error::make();
      })
      );
}

/* For test */
ceph::bufferlist WAL::encode_test_super()
{
  bufferlist bl;
  WAL::header_t head{
    WAL_MAGIC, // TODO: magic number
    block_type_t::WAL_HEADER,   // wal_header_t
    0,
  }; 
  WAL::wal_header_t wal_head{
    head,
    0xFFFF, // TODO
    block_size, // start offset
    device_manager.get_size() - block_size, // max size
    0, // used_size
    0, // errno
    block_size, // start offset
    0, // start_tid
    wal_seq_t {0, 0}, // applied
    wal_seq_t {0, 0}, // committed
    wal_seq_t {0, 0}, // cur_pos
    0, // feature (reserved)
    0, // csum_type
    0, // csum
  };
  ::encode(wal_head, bl);
  return bl;
}

ceph::bufferlist WAL::encode_super()
{
  bufferlist bl;
  ::encode(wal_header, bl);
  return bl;
}

size_t WAL::get_aligned_entry_size(uint64_t len) const {
  return p2roundup(
	  ceph::encoded_sizeof_bounded<wal_data_block_header_t>() +
	  len + 
	  ceph::encoded_sizeof_bounded<wal_commit_block_t>(),
	  block_size);
}

size_t WAL::get_unaligned_entry_size(uint64_t len) const {
  return  ceph::encoded_sizeof_bounded<wal_data_block_header_t>() +
	  len + 
	  ceph::encoded_sizeof_bounded<wal_commit_block_t>();
}

size_t WAL::get_aligned_multi_entries_size(uint64_t len, int entries) const {
  return p2roundup(
	  (ceph::encoded_sizeof_bounded<wal_data_block_header_t>() * entries) +
	  len + 
	  (ceph::encoded_sizeof_bounded<wal_commit_block_t>() * entries),
	  block_size);
}

/**
 * encode_entry
 * 
 * return bufferlist which contains whole transaction entry including header, data and commit with 
 * padding aligned block size
 *
 */
ceph::bufferlist WAL::encode_entry(
  std::vector<write_item> &items,
  wal_seq_t &last_seq)
{
  bufferlist databl;
  size_t total_size = 0;
  for (auto &item : items) {
    wal_trans_id_t next_txid = get_next_tx_id();
    wal_data_block_header_t header {
      // header
      header_t { WAL_DATA_HEADER_MAGIC, block_type_t::WAL_DATA_BLOCK, next_txid },
      ceph::encoded_sizeof_bounded<wal_data_block_header_t>(),
      item.bl.length(),
      get_unaligned_entry_size(item.bl.length()),
      0,
      item.laddr
    };
    if (&item == &items.back()) {
      size_t entry_size = get_unaligned_entry_size(item.bl.length());
      header.total_length = 
	entry_size + (block_size - ((databl.length() + entry_size) % block_size));
      last_seq.addr = databl.length() + cur_pos.addr;
      last_seq.length = header.total_length;
    }
    wal_commit_block_t commit_block {
      // header
      header_t { WAL_COMMIT_HEADER_MAGIC, block_type_t::WAL_COMMIT_BLOCK, next_txid },
      header.total_length - header.header_length - item.bl.length(),
      0, // TODO
      0
    };

    ::encode(header, databl);
    databl.claim_append(item.bl);
    ::encode(commit_block, databl);
    total_size += header.total_length;

  }
  if (databl.length() % block_size != 0) {
    databl.append(
      ceph::bufferptr(
	block_size - (databl.length() % block_size)));
  }
  ceph_assert(databl.length() == total_size);
  return databl;
}

WAL::open_ret WAL::open()
{
  return read_super(WAL_START_ADDRESS
      ).safe_then([this](auto p) {
	auto &[header, bl] = *p;
	last_applied = header.applied;
	last_committed = header.committed;
	cur_pos = header.cur_pos;
	used_size = header.used_size;
	max_size = header.max_size ;
	wal_header = header;
      }).handle_error(
	read_entry_ertr::all_same_way([](auto e) {
	  logger().debug("read_super error");
	  abort();
	  })
	);
}

void WAL::do_after_committed()
{
  // TODO: need flush policy here
  auto p = trans_for_commit.begin();
  RecordRef last;
  while (p != trans_for_commit.end()) {
    if ((*p)->state == record_state_t::COMMITTED) {
      trans_for_apply.push_back(*p); 
      last = *p;
      // TODO: need actual flush here (async work)
      trans_for_commit.erase(p++);
    } else {
      break;
    } 
  }
  if (last) {
    // update if necessary (need wal lock)
    update_last_committed(last->cur_pos);
    // delayed update
    update_cur_pos(cur_pos, last->cur_pos);
  }
}

void WAL::do_after_applied() 
{
  // TODO
  if (need_trigger_flush) {
    RecordRef last;
    auto p_apply  = trans_for_apply.begin();
    while (p_apply != trans_for_apply.end()) {
      if ((*p_apply)->state == record_state_t::COMMITTED) {
	last = *p_apply;
	trans_for_apply.erase(p_apply++);
      } else {
	break;
      } 
    }
    if (last) {
      // update if necessary (need wal lock)
      update_last_applied((*p_apply)->cur_pos);
      update_used_size(cur_pos, last_applied);
    }
  }
}

void WAL::do_defered_works() 
{
  do_after_committed();
  do_after_applied();
}

/*
 * wal lock will protect all variable in WAL except for trans_for_commit and trans_for_apply
 *
 */
WAL::submit_entry_ret WAL::submit_entry(RecordRef rec, bool need_sync)
{
  size_t size = rec->get_write_items_length();
  int entries = rec->to_write.size();
  size_t aligned_size = get_aligned_multi_entries_size(size, entries);

  // enter lock-free, we'll use existing mutex instead of lock-free list at initaial stage of development.
  RecordRef last_record = trans_for_commit.empty() ? nullptr : trans_for_commit.back();
  wal_seq_t &pos = rec->cur_pos;
  if (last_record) {
    // calc current pos
    update_cur_pos(pos, last_record->cur_pos);
    pos.id = last_record->cur_pos.id + 1; // TODO: generate tx id
  } else {
    // need wal lock
    pos = cur_pos;
    pos.id = cur_pos.id + 1;
  }

  /* 
   * need wal lock even if lock-free is used
   * the following code is only critical region to be protected
   */
  update_used_size(rec->cur_pos, last_applied);
  if (aligned_size > get_available_size()) {
    return crimson::ct_error::enospc::make();
  }
  // wal unlock

  if (pos.addr + block_size > max_size) {
    // if remain bytes are smaller than wal_data_block_header_t, move cur_pos to the start point
    reset_cur_pos(pos);
  }

  // add record_t to the list, which is a list for now 
  // lock-free
  trans_for_commit.push_back(rec);

  wal_seq_t last_encoded;
  bufferlist databl = encode_entry(rec->to_write, last_encoded);
  ceph_assert(databl.length());
  ceph_assert(!(databl.length() % block_size));
  pos.length = databl.length();

  auto laddr_to_write = pos.addr;

  logger().debug("submit_entry: laddr {} vector size {} length {} pos {}", 
		  laddr_to_write, rec->to_write.size(), databl.length(), pos);

  return append_block(
      databl,
      laddr_to_write
      ).safe_then([this, rec, last_encoded, laddr_to_write, need_sync](auto addr) {
	  rec->state = record_state_t::COMMITTED;
	  // TODO: selective checkpointing
	  if (need_sync) {
	    do_after_committed();
	    return sync_super().safe_then([last_encoded, laddr_to_write]() {
		return submit_entry_ret(
			    submit_entry_ertr::ready_future_marker{},
			    std::make_pair(laddr_to_write, last_encoded)
			    );
		});
	  } else {
	    return submit_entry_ret(
			submit_entry_ertr::ready_future_marker{},
			std::make_pair(laddr_to_write, last_encoded)
			);
	  }
	  });
}

WAL::write_block_ret WAL::overwrite_block(paddr_t offset, bufferlist &bl)
{
  auto length = bl.length();
  if (offset + length > max_size) {
    return crimson::ct_error::erange::make();
  }
  logger().debug(
    "overwrite in WAL, offset {}, length {}",
    offset,
    length);
  return device_manager.write(offset, bl
    ).handle_error(
      write_block_ertr::pass_further{},
      crimson::ct_error::assert_all{ "TODO" }
    ).safe_then([this, offset, length] {
      return write_block_ret(
	write_block_ertr::ready_future_marker{},
	std::make_pair(offset, length));
      });
}

WAL::write_block_ret WAL::append_block(
  ceph::bufferlist bl,
  laddr_t laddr)
{
  bufferlist to_write;
  if (laddr + bl.length() <= max_size) {
    to_write = bl;
  } else {
    to_write.substr_of(bl, 0, max_size - laddr);
  } 
  auto paddr = get_abs_addr(laddr);
  logger().debug(
    "append_block, offset {}, length {}",
    paddr,
    to_write.length());
  return device_manager.write(paddr, to_write
    ).handle_error(
      write_block_ertr::pass_further{},
      crimson::ct_error::assert_all{ "TODO" }
    ).safe_then([this, laddr, bl=std::move(bl), length=to_write.length()] {
      if (bl.length() == length) {
	// complete
	return write_block_ret(
	  write_block_ertr::ready_future_marker{},
	    std::make_pair(
	      laddr, 
	      length));
      } else {
	auto next_write_addr = get_abs_addr(0);
	bufferlist next_write;
	next_write.substr_of(bl, length, bl.length() - length);
	return device_manager.write(next_write_addr, next_write
	  ).handle_error(
	    write_block_ertr::pass_further{},
	    crimson::ct_error::assert_all{ "TODO" }
	  ).safe_then([this, laddr, total_length = bl.length()] {
	    return write_block_ret(
	      write_block_ertr::ready_future_marker{},
		std::make_pair(
		  laddr, 
		  total_length));
	    }
	    );
      }

      });
}

WAL::read_super_ret WAL::read_super(paddr_t start)
{
  return device_manager.read(start, block_size
      ).safe_then(
	[this, start](bufferptr bptr) mutable
	-> read_super_ret {
	  logger().debug("read_super: reading {}", start);
	  bufferlist bl;
	  bl.append(bptr);
	  auto bp = bl.cbegin();
	  wal_header_t header;
	  try {
	    ::decode(header, bp);
	  } catch (ceph::buffer::error &e) {
	    return read_super_ret(
		read_entry_ertr::ready_future_marker{},
		std::nullopt);
	  }
	  cur_pos = header.cur_pos;
	  last_applied = header.applied;
	  last_committed = header.committed;
	  return read_super_ret(
	    read_entry_ertr::ready_future_marker{},
	    std::make_pair(header, bl)
	  );
      });
}

WAL::read_entry_ret WAL::read_entry(laddr_t start)
{
  paddr_t read_addr = get_abs_addr(start);
  if (start + block_size > max_size) {
    read_addr = get_abs_addr(0);
  }
  return device_manager.read(read_addr, block_size
      ).safe_then(
	[this, start, read_addr](bufferptr bptr) mutable
	-> read_entry_ret {
	  logger().debug("read_entry: reading absolute addr {} rel addr {}", read_addr, start);
	  bufferlist bl;
	  bl.append(bptr);
	  auto bp = bl.cbegin();
	  wal_data_block_header_t header;
	  try {
	    ::decode(header, bp);
	  } catch (ceph::buffer::error &e) {
	    return read_entry_ret(
		read_entry_ertr::ready_future_marker{},
		std::nullopt);
	  }
	  ceph_assert(check_valid_entry(header));
	  logger().debug("read_entry: header {}", header);
	  if (header.total_length > block_size) { 
	    paddr_t next_read_addr = get_abs_addr(start + block_size);
	    auto next_read = header.total_length - block_size;
	    logger().debug(" here start {}, next_read {} ", start, next_read);
	    if (max_size < start + block_size + next_read) {
	      // In this case, need two more reads.
	      // The first is to read remain bytes to the end of wal
	      // The second is to read the data at the begining of wal
	      next_read = max_size - (start + block_size);
	    }
	    logger().debug("read_entry: additional reading absolute addr {} length {}", 
			    next_read_addr, 
			    next_read);
	    return device_manager.read(
		next_read_addr,
		next_read
	      ).safe_then(
		[this, header=header, bl=std::move(bl)](
		  auto &&bptail) mutable {
		  bl.push_back(bptail);
		  if (header.total_length == bl.length()) {
		    logger().debug("read_entry: complte size {}", bl.length());
		    return read_entry_ret(
		      read_entry_ertr::ready_future_marker{},
		      std::make_pair(header, std::move(bl)));
		  } 
		  // need one more read
		  auto next_read_addr = get_abs_addr(0);
		  return device_manager.read(
		      next_read_addr,
		      header.total_length - bl.length() 
		    ).safe_then(
		      [header=header, bl=std::move(bl)](
			auto &&bptail) mutable {
			bl.push_back(bptail);
			logger().debug("read_entry: complte size {}", bl.length());
			return read_entry_ret(
			  read_entry_ertr::ready_future_marker{},
			  std::make_pair(header, std::move(bl)));
			});

		});
	  } else {
	    return read_entry_ret(
	      read_entry_ertr::ready_future_marker{},
	      std::make_pair(header, std::move(bl)));
	  }
      });
}

template <class T>
bool WAL::check_valid_entry(T &header) const
{
  if (std::is_same<T, wal_data_block_header_t>::value) {
    // TODO 
    return header.header.block_type == block_type_t::WAL_DATA_BLOCK;
  } else if (std::is_same<T, wal_header_t>::value) {
    // TODO
    return header.header.block_type == block_type_t::WAL_HEADER;
  }
}

WAL::create_ret
WAL::sync_super()
{
  wal_header.used_size = used_size;
  wal_header.max_size = max_size;
  wal_header.cur_pos = cur_pos;
  wal_header.applied = last_applied;
  wal_header.committed = last_committed;
  ceph::bufferlist bl;
  try {
    bl = encode_super();
  } catch (ceph::buffer::error &e) {
    logger().debug("unable to encode super block to underlying deivce");
    return crimson::ct_error::input_output_error::make();
  }
  logger().debug(
    "sync headerblock of WAL, length {}",
    bl.length());
  return overwrite_block(WAL_START_ADDRESS, bl
      ).safe_then([this](auto p){},
      create_ertr::pass_further{},
      crimson::ct_error::all_same_way([](auto e) {
	logger().debug("overwrite error");
	ceph_assert(0 == "TODO");
      })
      );
}

}

