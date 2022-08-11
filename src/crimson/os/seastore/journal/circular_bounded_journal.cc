// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/errorator-loop.h"
#include "include/intarith.h"
#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/journal/circular_bounded_journal.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore::journal {

std::ostream &operator<<(std::ostream &out,
    const CircularBoundedJournal::cbj_header_t &header)
{
  return out << "cbj_header_t(magic=" << header.magic
	     << ", uuid=" << header.uuid
	     << ", block_size=" << header.block_size
	     << ", size=" << header.size
	     << ", journal_tail=" << header.journal_tail
	     << ", "<< device_id_printer_t{header.device_id}
             << ")";
}

CircularBoundedJournal::CircularBoundedJournal(
    JournalTrimmer &trimmer,
    RBMDevice* device,
    const std::string &path)
  : trimmer(trimmer), device(device), path(path) {}

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
    head.device_id = config.device_id;
    encode(head, bl);
    header = head;
    set_written_to(head.journal_tail);
    DEBUG(
      "initialize header block in CircularBoundedJournal, length {}",
      bl.length());
    return write_header(
    ).handle_error(
      mkfs_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error in CircularBoundedJournal::mkfs"
      }
    );
  }).safe_then([this]() -> mkfs_ret {
    if (device) {
      return device->close();
    }
    return mkfs_ertr::now();
  });
}

CircularBoundedJournal::open_for_mount_ertr::future<>
CircularBoundedJournal::_open_device(const std::string &path)
{
  ceph_assert(device);
  return device->open(path, seastar::open_flags::rw
  ).handle_error(
    open_for_mount_ertr::pass_further{},
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

CircularBoundedJournal::open_for_mkfs_ret
CircularBoundedJournal::open_for_mkfs()
{
  return open_for_mount();
}

CircularBoundedJournal::open_for_mount_ret
CircularBoundedJournal::open_for_mount()
{
  ceph_assert(initialized);
  paddr_t paddr = convert_abs_addr_to_paddr(
    get_written_to(),
    header.device_id);
  if (circulation_seq == NULL_SEG_SEQ) {
    circulation_seq = 0;
  }
  return open_for_mount_ret(
    open_for_mount_ertr::ready_future_marker{},
    journal_seq_t{
      circulation_seq,
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
    open_for_mount_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error write_header"
    }
  );
}

CircularBoundedJournal::open_for_mount_ret
CircularBoundedJournal::open_device_read_header()
{
  LOG_PREFIX(CircularBoundedJournal::open_device_read_header);
  ceph_assert(!initialized);
  return _open_device(path
  ).safe_then([this, FNAME]() {
    return read_header(
    ).handle_error(
      open_for_mount_ertr::pass_further{},
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
      return open_for_mount_ret(
	open_for_mount_ertr::ready_future_marker{},
	journal_seq_t{
	  circulation_seq,
	  paddr
	});
    });
  }).handle_error(
    open_for_mount_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error _open_device"
  });
}

CircularBoundedJournal::submit_record_ret CircularBoundedJournal::submit_record(
  record_t &&record,
  OrderingHandle &handle)
{
  LOG_PREFIX(CircularBoundedJournal::submit_record);
  assert(write_pipeline);
  assert(circulation_seq != NULL_SEG_SEQ);
  auto r_size = record_group_size_t(record.size, get_block_size());
  auto encoded_size = r_size.get_encoded_length();
  if (encoded_size > get_available_size()) {
    ERROR("record size {}, but available size {}",
          encoded_size, get_available_size());
    return crimson::ct_error::erange::make();
  }
  if (encoded_size + get_written_to() > get_journal_end()) {
    DEBUG("roll");
    set_written_to(get_start_addr());
    ++circulation_seq;
    if (encoded_size > get_available_size()) {
      ERROR("rolled, record size {}, but available size {}",
            encoded_size, get_available_size());
      return crimson::ct_error::erange::make();
    }
  }

  journal_seq_t j_seq {
    circulation_seq,
    convert_abs_addr_to_paddr(
      get_written_to(),
      header.device_id)};
  ceph::bufferlist to_write = encode_record(
    std::move(record), device->get_block_size(),
    j_seq, 0);
  assert(to_write.length() == encoded_size);
  auto target = get_written_to();
  auto new_written_to = target + encoded_size;
  if (new_written_to >= get_journal_end()) {
    assert(new_written_to == get_journal_end());
    DEBUG("roll");
    set_written_to(get_start_addr());
    ++circulation_seq;
  } else {
    set_written_to(new_written_to);
  }
  DEBUG("{}, target {}", r_size, target);

  auto write_result = write_result_t{
    j_seq,
    (seastore_off_t)encoded_size
  };
  auto write_fut = device_write_bl(target, to_write);
  return handle.enter(write_pipeline->device_submission
  ).then([write_fut = std::move(write_fut)]() mutable {
    return std::move(write_fut);
  }).safe_then([this, &handle] {
    return handle.enter(write_pipeline->finalize);
  }).safe_then([this, target,
    length=encoded_size,
    write_result,
    r_size,
    FNAME] {
    DEBUG("commit target {} used_size {} written length {}",
          target, get_used_size(), length);

    paddr_t paddr = convert_abs_addr_to_paddr(
      target + r_size.get_mdlength(),
      header.device_id);
    auto submit_result = record_locator_t{
      paddr,
      write_result
    };
    trimmer.set_journal_head(write_result.start_seq);
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
  );
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
      ERROR("unable to read header block");
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
      ERROR("error, header crc mismatch.");
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
  return open_device_read_header(
  ).safe_then([this, FNAME, delta_handler=std::move(delta_handler)] (auto) {
    circulation_seq = NULL_SEG_SEQ;
    set_written_to(get_journal_tail());
    return seastar::do_with(
      bool(false),
      rbm_abs_addr(get_journal_tail()),
      std::move(delta_handler),
      segment_seq_t(NULL_SEG_SEQ),
      [this, FNAME](auto &is_rolled, auto &cursor_addr, auto &d_handler, auto &expected_seq) {
      return crimson::repeat(
	[this, &is_rolled, &cursor_addr, &d_handler, &expected_seq, FNAME]() mutable
	-> replay_ertr::future<seastar::stop_iteration> {
	paddr_t record_paddr = convert_abs_addr_to_paddr(
	  cursor_addr,
	  header.device_id);
	return read_record(record_paddr, expected_seq
	).safe_then([this, &is_rolled, &cursor_addr, &d_handler, &expected_seq, FNAME](auto ret)
	    -> replay_ertr::future<seastar::stop_iteration> {
	  if (!ret.has_value()) {
	    if (expected_seq == NULL_SEG_SEQ || is_rolled) {
	      DEBUG("no more records, stop replaying");
	      return replay_ertr::make_ready_future<
	        seastar::stop_iteration>(seastar::stop_iteration::yes);
	    } else {
	      cursor_addr = get_start_addr();
	      ++expected_seq;
	      is_rolled = true;
	      return replay_ertr::make_ready_future<
	        seastar::stop_iteration>(seastar::stop_iteration::no);
	    }
	  }
	  auto [r_header, bl] = *ret;
	  bufferlist mdbuf;
	  mdbuf.substr_of(bl, 0, r_header.mdlength);
	  paddr_t record_block_base = paddr_t::make_blk_paddr(
	    header.device_id, cursor_addr + r_header.mdlength);
	  auto maybe_record_deltas_list = try_decode_deltas(
	    r_header, mdbuf, record_block_base);
	  if (!maybe_record_deltas_list) {
	    // This should be impossible, we did check the crc on the mdbuf
	    ERROR("unable to decode deltas for record {} at {}",
	          r_header, record_block_base);
	    return crimson::ct_error::input_output_error::make();
	  }
	  DEBUG("{} at {}", r_header, cursor_addr);
	  auto write_result = write_result_t{
	    r_header.committed_to,
	    (seastore_off_t)bl.length()
	  };
	  if (expected_seq == NULL_SEG_SEQ) {
	    expected_seq = r_header.committed_to.segment_seq;
	  } else {
	    assert(expected_seq == r_header.committed_to.segment_seq);
	  }
	  cursor_addr += bl.length();
	  if (cursor_addr >= get_journal_end()) {
	    assert(cursor_addr == get_journal_end());
	    cursor_addr = get_start_addr();
	    ++expected_seq;
	    is_rolled = true;
	  }
	  set_written_to(cursor_addr);
	  circulation_seq = expected_seq;
	  return seastar::do_with(
	    std::move(*maybe_record_deltas_list),
	    [write_result,
	    &d_handler,
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
		return d_handler(
		  locator,
		  delta,
		  locator.write_result.start_seq,
		  locator.write_result.start_seq,
		  modify_time).discard_result();
	      });
	    }).safe_then([]() {
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
  DEBUG("record size {}", bl.length());
  assert(bl.length() == header.mdlength + header.dlength);
  bufferlist md_bl, data_bl;
  md_bl.substr_of(bl, 0, header.mdlength);
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

CircularBoundedJournal::read_record_ret
CircularBoundedJournal::read_record(paddr_t off, segment_seq_t expected_seq)
{
  LOG_PREFIX(CircularBoundedJournal::read_record);
  rbm_abs_addr addr = convert_paddr_to_abs_addr(off);
  auto read_length = get_block_size();
  assert(addr + read_length <= get_journal_end());
  DEBUG("reading record from abs addr {} read length {}", addr, read_length);
  auto bptr = bufferptr(ceph::buffer::create_page_aligned(read_length));
  return device->read(addr, bptr
  ).safe_then([this, addr, bptr, expected_seq, FNAME]() mutable
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
    if (h.mdlength < get_block_size() ||
        h.mdlength % get_block_size() != 0 ||
        h.dlength % get_block_size() != 0 ||
        addr + h.mdlength + h.dlength > get_journal_end() ||
        h.committed_to.segment_seq == NULL_SEG_SEQ ||
        (expected_seq != NULL_SEG_SEQ &&
         h.committed_to.segment_seq != expected_seq)) {
      return read_record_ret(
        read_record_ertr::ready_future_marker{},
        std::nullopt);
    }
    auto record_size = h.mdlength + h.dlength;
    if (record_size > get_block_size()) {
      auto next_addr = addr + get_block_size();
      auto next_length = record_size - get_block_size();
      auto next_bptr = bufferptr(ceph::buffer::create_page_aligned(next_length));
      DEBUG("reading record part 2 from abs addr {} read length {}",
            next_addr, next_length);
      return device->read(next_addr, next_bptr
      ).safe_then([this, h, next_bptr=std::move(next_bptr), bl=std::move(bl)]() mutable {
        bl.append(next_bptr);
        return return_record(h, bl);
      });
    } else {
      assert(record_size == get_block_size());
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
