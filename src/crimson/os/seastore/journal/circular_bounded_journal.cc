// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/errorator-loop.h"
#include "include/intarith.h"
#include "crimson/os/seastore/journal/circular_bounded_journal.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore::journal {

std::ostream &operator<<(std::ostream &out,
    const CircularBoundedJournal::cbj_header_t &header)
{
  return out << "cbj_header_t(magin=" << header.magic
	     << ", uuid=" << header.uuid
	     << ", block_size=" << header.block_size
	     << ", size=" << header.size
	     << ", journal_tail=" << header.journal_tail
	     << ", applied_to="<< header.applied_to
	     << ", "<< device_id_printer_t{header.device_id}
             << ")";
}


CircularBoundedJournal::CircularBoundedJournal(NVMeBlockDevice* device,
    const std::string path)
  : device(device), path(path) {}

CircularBoundedJournal::mkfs_ret
CircularBoundedJournal::mkfs(const mkfs_config_t& config)
{
  LOG_PREFIX(CircularBoundedJournal::mkfs);
  return _open_device(path
  ).safe_then([this, config, FNAME]() mutable -> mkfs_ret {
    assert(static_cast<seastore_off_t>(config.block_size) ==
      device->get_block_size());
    ceph::bufferlist bl;
    CircularBoundedJournal::cbj_header_t head;
    head.block_size = config.block_size;
    head.size = config.total_size - device->get_block_size();
    head.journal_tail = device->get_block_size();
    head.applied_to = head.journal_tail;
    head.device_id = config.device_id;
    encode(head, bl);
    header = head;
    written_to = head.journal_tail;
    DEBUG(
      "initialize header block in CircularBoundedJournal, length {}",
      bl.length());
    return write_header(
    ).handle_error(
      mkfs_ertr::pass_further{},
      crimson::ct_error::assert_all{
      "Invalid error device_write during CircularBoundedJournal::mkfs"
    }).safe_then([]() {
      return mkfs_ertr::now();
    });
  }).safe_then([this]() {
    if (device) {
      return device->close(
      ).safe_then([]() {
	return mkfs_ertr::now();
      });
    }
    return mkfs_ertr::now();
  });
}

CircularBoundedJournal::open_for_write_ertr::future<>
CircularBoundedJournal::_open_device(const std::string path)
{
  ceph_assert(device);
  return device->open(path, seastar::open_flags::rw
  ).handle_error(
    open_for_write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error device->open"
    }
  );
}

ceph::bufferlist CircularBoundedJournal::encode_header()
{
  bufferlist bl;
  encode(header, bl);
  auto header_crc_filler = bl.append_hole(sizeof(checksum_t));
  auto bliter = bl.cbegin();
  auto header_crc = bliter.crc32c(
    ceph::encoded_sizeof_bounded<cbj_header_t>(),
    -1);
  ceph_le32 header_crc_le;
  header_crc_le = header_crc;
  header_crc_filler.copy_in(
    sizeof(checksum_t),
    reinterpret_cast<const char *>(&header_crc_le));
  return bl;
}

CircularBoundedJournal::open_for_write_ret CircularBoundedJournal::open_for_write()
{
  ceph_assert(initialized);
  paddr_t paddr = convert_abs_addr_to_paddr(
    get_written_to(),
    header.device_id);
  return open_for_write_ret(
    open_for_write_ertr::ready_future_marker{},
    journal_seq_t{
      cur_segment_seq,
      paddr
  });
}

CircularBoundedJournal::close_ertr::future<> CircularBoundedJournal::close()
{
  return write_header(
  ).safe_then([this]() -> close_ertr::future<> {
    initialized = false;
    return device->close();
  }).handle_error(
    open_for_write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error write_header"
    }
  );
}

CircularBoundedJournal::open_for_write_ret
CircularBoundedJournal::open_device_read_header()
{
  LOG_PREFIX(CircularBoundedJournal::open_for_write);
  ceph_assert(!initialized);
  return _open_device(path
  ).safe_then([this, FNAME]() {
    return read_header(
    ).handle_error(
      open_for_write_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error read_header"
    }).safe_then([this, FNAME](auto p) mutable {
      auto &[head, bl] = *p;
      header = head;
      DEBUG("header : {}", header);
      paddr_t paddr = convert_abs_addr_to_paddr(
	get_written_to(),
	header.device_id);
      initialized = true;
      return open_for_write_ret(
	open_for_write_ertr::ready_future_marker{},
	journal_seq_t{
	  cur_segment_seq,
	  paddr
	});
    });
  }).handle_error(
    open_for_write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error _open_device"
  });
}

CircularBoundedJournal::write_ertr::future<> CircularBoundedJournal::append_record(
  ceph::bufferlist bl,
  rbm_abs_addr addr)
{
  LOG_PREFIX(CircularBoundedJournal::append_record);
  std::vector<std::pair<rbm_abs_addr, bufferlist>> writes;
  if (addr + bl.length() <= get_journal_end()) {
    writes.push_back(std::make_pair(addr, bl));
  } else {
    // write remaining data---in this case,
    // data is splited into two parts before due to the end of CircularBoundedJournal.
    // the following code is to write the second part
    bufferlist first_write, next_write;
    first_write.substr_of(bl, 0, get_journal_end() - addr);
    writes.push_back(std::make_pair(addr, first_write));
    next_write.substr_of(
      bl, first_write.length(), bl.length() - first_write.length());
    writes.push_back(std::make_pair(get_start_addr(), next_write));
  }

  return seastar::do_with(
    std::move(bl),
    [this, writes=move(writes), FNAME](auto& bl) mutable
  {
    DEBUG("original bl length {}", bl.length());
    return write_ertr::parallel_for_each(
      writes,
      [this, FNAME](auto& p) mutable
    {
      DEBUG(
	"append_record: offset {}, length {}",
	p.first,
	p.second.length());
      return device_write_bl(p.first, p.second 
      ).handle_error(
	write_ertr::pass_further{},
	crimson::ct_error::assert_all{ "Invalid error device->write" }
      ).safe_then([]() {
	return write_ertr::now();
      });
    });
  });
}

CircularBoundedJournal::submit_record_ret CircularBoundedJournal::submit_record(
  record_t &&record,
  OrderingHandle &handle)
{
  LOG_PREFIX(CircularBoundedJournal::submit_record);
  assert(write_pipeline);
  auto r_size = record_group_size_t(record.size, get_block_size());
  auto encoded_size = r_size.get_encoded_length();
  if (get_written_to() +
      ceph::encoded_sizeof_bounded<record_group_header_t>() > get_journal_end()) {
    // not enough space between written_to and the end of journal,
    // so that set written_to to the beginning of cbjournal to append 
    // the record at the start address of cbjournal
    // |        cbjournal      |
    // 	          v            v
    //      written_to <-> the end of journal
    set_written_to(get_start_addr());
  }
  if (encoded_size > get_available_size()) {
    ERROR(
      "CircularBoundedJournal::submit_record: record size {}, but available size {}",
      encoded_size,
      get_available_size()
      );
    return crimson::ct_error::erange::make();
  }

  journal_seq_t j_seq {
    cur_segment_seq++,
    convert_abs_addr_to_paddr(
      get_written_to(),
      header.device_id)};
  ceph::bufferlist to_write = encode_record(
    std::move(record), device->get_block_size(),
    j_seq, 0);
  auto target = get_written_to();
  if (get_written_to() + to_write.length() >= get_journal_end()) {
    set_written_to(get_start_addr() +
      (to_write.length() - (get_journal_end() - get_written_to())));
  } else {
    set_written_to(get_written_to() + to_write.length());
  }
  DEBUG(
    "submit_record: mdlength {}, dlength {}, target {}",
    r_size.get_mdlength(),
    r_size.dlength,
    target);

  auto write_result = write_result_t{
    j_seq,
    (seastore_off_t)to_write.length()
  };
  auto write_fut = append_record(to_write, target);
  return handle.enter(write_pipeline->device_submission
  ).then([write_fut = std::move(write_fut)]() mutable {
    return std::move(write_fut
    ).handle_error(
      write_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error in CircularBoundedJournal::append_record"
      }
    );
  }).safe_then([this, &handle] {
    return handle.enter(write_pipeline->finalize);
  }).safe_then([this, target,
    length=to_write.length(),
    write_result,
    r_size,
    FNAME] {
    DEBUG(
      "append_record: commit target {} used_size {} written length {}",
      target, get_used_size(), length);

    paddr_t paddr = convert_abs_addr_to_paddr(
      target + r_size.get_mdlength(),
      header.device_id);
    auto submit_result = record_locator_t{
      paddr,
      write_result
    };
    return submit_result;
  });
}

CircularBoundedJournal::write_ertr::future<> CircularBoundedJournal::device_write_bl(
    rbm_abs_addr offset, bufferlist &bl)
{
  LOG_PREFIX(CircularBoundedJournal::device_write_bl);
  auto length = bl.length();
  if (offset + length > get_journal_end()) {
    return crimson::ct_error::erange::make();
  }
  DEBUG(
    "overwrite in CircularBoundedJournal, offset {}, length {}",
    offset,
    length);
  return device->writev(offset, bl
  ).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all{ "Invalid error device->write" }
  ).safe_then([] {
    return write_ertr::now();
  });
}

CircularBoundedJournal::read_header_ret
CircularBoundedJournal::read_header()
{
  LOG_PREFIX(CircularBoundedJournal::read_header);
  auto bptr = bufferptr(ceph::buffer::create_page_aligned(
			device->get_block_size()));
  DEBUG("reading {}", CBJOURNAL_START_ADDRESS);
  return device->read(CBJOURNAL_START_ADDRESS, bptr
  ).safe_then([bptr, FNAME]() mutable
    -> read_header_ret {
    bufferlist bl;
    bl.append(bptr);
    auto bp = bl.cbegin();
    cbj_header_t cbj_header;
    try {
      decode(cbj_header, bp);
    } catch (ceph::buffer::error &e) {
      ERROR("read_header: unable to read header block");
      return crimson::ct_error::enoent::make();
    }
    auto bliter = bl.cbegin();
    auto test_crc = bliter.crc32c(
      ceph::encoded_sizeof_bounded<cbj_header_t>(),
      -1);
    ceph_le32 recorded_crc_le;
    decode(recorded_crc_le, bliter);
    uint32_t recorded_crc = recorded_crc_le;
    if (test_crc != recorded_crc) {
      ERROR("read_header: error, header crc mismatch.");
      return read_header_ret(
	read_header_ertr::ready_future_marker{},
	std::nullopt);
    }
    return read_header_ret(
      read_header_ertr::ready_future_marker{},
      std::make_pair(cbj_header, bl)
    );
  });
}

Journal::replay_ret CircularBoundedJournal::replay(
    delta_handler_t &&delta_handler)
{
  /*
   * read records from last applied record prior to written_to, and replay
   */
  LOG_PREFIX(CircularBoundedJournal::replay);
  auto fut = open_device_read_header();
  return fut.safe_then([this, FNAME, delta_handler=std::move(delta_handler)] (auto addr) {
    return seastar::do_with(
      rbm_abs_addr(get_journal_tail()),
      std::move(delta_handler),
      segment_seq_t(),
      [this, FNAME](auto &cursor_addr, auto &d_handler, auto &last_seq) {
      return crimson::repeat(
	[this, &cursor_addr, &d_handler, &last_seq, FNAME]() mutable
	-> replay_ertr::future<seastar::stop_iteration> {
	paddr_t cursor_paddr = convert_abs_addr_to_paddr(
	  cursor_addr,
	  header.device_id);
	return read_record(cursor_paddr
	).safe_then([this, &cursor_addr, &d_handler, &last_seq, FNAME](auto ret) {
	  if (!ret.has_value()) {
	    DEBUG("no more records");
	    return replay_ertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::yes);
	  }
	  auto [r_header, bl] = *ret;
	  if (last_seq > r_header.committed_to.segment_seq) {
	    DEBUG("found invalide record. stop replaying");
	    return replay_ertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::yes);
	  }
	  bufferlist mdbuf;
	  mdbuf.substr_of(bl, 0, r_header.mdlength);
	  paddr_t record_block_base = paddr_t::make_blk_paddr(
	    header.device_id, cursor_addr + r_header.mdlength);
	  auto maybe_record_deltas_list = try_decode_deltas(
	    r_header, mdbuf, record_block_base);
	  if (!maybe_record_deltas_list) {
	    DEBUG("unable to decode deltas for record {} at {}",
	      r_header, record_block_base);
	    return replay_ertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::yes);
	  }
	  DEBUG(" record_group_header_t: {}, cursor_addr: {} ",
	    r_header, cursor_addr);
	  auto write_result = write_result_t{
	    r_header.committed_to,
	    (seastore_off_t)bl.length()
	  };
	  cur_segment_seq = r_header.committed_to.segment_seq + 1;
	  cursor_addr += bl.length();
	  set_written_to(cursor_addr);
	  last_seq = r_header.committed_to.segment_seq;
	  return seastar::do_with(
	    std::move(*maybe_record_deltas_list),
	    [write_result,
	    this,
	    &d_handler,
	    &cursor_addr,
	    FNAME](auto& record_deltas_list) {
	    return crimson::do_for_each(
	      record_deltas_list,
	      [write_result,
	      &d_handler, FNAME](record_deltas_t& record_deltas) {
	      auto locator = record_locator_t{
		record_deltas.record_block_base,
		write_result
	      };
	      DEBUG("processing {} deltas at block_base {}",
		  record_deltas.deltas.size(),
		  locator);
	      return crimson::do_for_each(
		record_deltas.deltas,
		[locator,
		&d_handler](auto& p) {
		auto& modify_time = p.first;
		auto& delta = p.second;
		return d_handler(locator,
		  delta,
		  locator.write_result.start_seq,
		  modify_time);
	      });
	    }).safe_then([this, &cursor_addr]() {
	      if (cursor_addr >= get_journal_end()) {
		cursor_addr = (cursor_addr - get_journal_end()) + get_start_addr();
	      }
	      if (get_written_to() +
		  ceph::encoded_sizeof_bounded<record_group_header_t>() > get_journal_end()) {
		cursor_addr = get_start_addr();
	      }
	      return replay_ertr::make_ready_future<
		seastar::stop_iteration>(seastar::stop_iteration::no);
	    });
	  });
	});
      });
    });
  });
}

CircularBoundedJournal::read_record_ret
CircularBoundedJournal::return_record(record_group_header_t& header, bufferlist bl)
{
  LOG_PREFIX(CircularBoundedJournal::return_record);
  bufferlist md_bl, data_bl;
  md_bl.substr_of(bl, 0, get_block_size());
  data_bl.substr_of(bl, header.mdlength, header.dlength);
  if (validate_records_metadata(md_bl) &&
      validate_records_data(header, data_bl)) {
    return read_record_ret(
      read_record_ertr::ready_future_marker{},
      std::make_pair(header, std::move(bl)));
  } else {
    DEBUG("invalid matadata");
    return read_record_ret(
      read_record_ertr::ready_future_marker{},
      std::nullopt);
  }
}

CircularBoundedJournal::read_record_ret CircularBoundedJournal::read_record(paddr_t off)
{
  LOG_PREFIX(CircularBoundedJournal::read_record);
  rbm_abs_addr offset = convert_paddr_to_abs_addr(
    off);
  rbm_abs_addr addr = offset;
  auto read_length = get_block_size();
  if (addr + get_block_size() > get_journal_end()) {
    addr = get_start_addr();
    read_length = get_journal_end() - offset;
  }
  DEBUG("read_record: reading record from abs addr {} read length {}",
      addr, read_length);
  auto bptr = bufferptr(ceph::buffer::create_page_aligned(read_length));
  bptr.zero();
  return device->read(addr, bptr
  ).safe_then([this, addr, read_length, bptr, FNAME]() mutable
    -> read_record_ret {
    record_group_header_t h;
    bufferlist bl;
    bl.append(bptr);
    auto bp = bl.cbegin();
    try {
      decode(h, bp);
    } catch (ceph::buffer::error &e) {
      return read_record_ret(
	read_record_ertr::ready_future_marker{},
	std::nullopt);
    }
    /*
     * |          journal          |
     *        | record 1 header |  | record 1 data
     *  record 1 data  (remaining) |
     *
     *        <---- 1 block ----><--
     * -- 2 block --->
     *
     *  If record has logner than read_length and its data is located across
     *  the end of journal and the begining of journal, we need three reads
     *  ---reads of header, other remaining data before the end, and
     *  the other remaining data from the begining.
     *
     */
    if (h.mdlength + h.dlength > read_length) {
      rbm_abs_addr next_read_addr = addr + read_length;
      auto next_read = h.mdlength + h.dlength - read_length;
      DEBUG(" next_read_addr {}, next_read_length {} ",
	  next_read_addr, next_read);
      if (get_journal_end() < next_read_addr + next_read) {
	// In this case, need two more reads.
	// The first is to read remain bytes to the end of cbjournal
	// The second is to read the data at the begining of cbjournal
	next_read = get_journal_end() - (addr + read_length);
      }
      DEBUG("read_entry: additional reading addr {} length {}",
	    next_read_addr,
	    next_read);
      auto next_bptr = bufferptr(ceph::buffer::create_page_aligned(next_read));
      next_bptr.zero();
      return device->read(
	next_read_addr,
	next_bptr
      ).safe_then([this, h=h, next_bptr=std::move(next_bptr), bl=std::move(bl),
	 FNAME]() mutable {
	bl.append(next_bptr);
	if (h.mdlength + h.dlength == bl.length()) {
	  DEBUG("read_record: record length {} done", bl.length());
	  return return_record(h, bl);
	}
	// need one more read
	auto next_read_addr = get_start_addr();
	auto last_bptr = bufferptr(ceph::buffer::create_page_aligned(
	      h.mdlength + h.dlength - bl.length()));
	DEBUG("read_record: last additional reading addr {} length {}",
	      next_read_addr,
	      h.mdlength + h.dlength - bl.length());
	return device->read(
	  next_read_addr,
	  last_bptr
	).safe_then([this, h=h, last_bptr=std::move(last_bptr),
	  bl=std::move(bl), FNAME]() mutable {
	  bl.append(last_bptr);
	  DEBUG("read_record: complte size {}", bl.length());
	  return return_record(h, bl);
	});
      });
    } else {
      DEBUG("read_record: complte size {}", bl.length());
      return return_record(h, bl);
    }
  });
}

CircularBoundedJournal::write_ertr::future<>
CircularBoundedJournal::write_header()
{
  LOG_PREFIX(CircularBoundedJournal::write_header);
  ceph::bufferlist bl;
  try {
    bl = encode_header();
  } catch (ceph::buffer::error &e) {
    DEBUG("unable to encode header block from underlying deivce");
    return crimson::ct_error::input_output_error::make();
  }
  ceph_assert(bl.length() <= get_block_size());
  DEBUG(
    "sync header of CircularBoundedJournal, length {}",
    bl.length());
  return device_write_bl(CBJOURNAL_START_ADDRESS, bl);
}

}
