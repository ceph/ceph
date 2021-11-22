// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/extent_reader.h"
#include "crimson/common/log.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore {

ExtentReader::read_segment_header_ret
ExtentReader::read_segment_header(segment_id_t segment)
{
  auto& segment_manager = *segment_managers[segment.device_id()];
  return segment_manager.read(
    paddr_t::make_seg_paddr(segment, 0),
    segment_manager.get_block_size()
  ).handle_error(
    read_segment_header_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in ExtentReader::read_segment_header"
    }
  ).safe_then([=, &segment_manager](bufferptr bptr) -> read_segment_header_ret {
    logger().debug("segment {} bptr size {}", segment, bptr.length());

    segment_header_t header;
    bufferlist bl;
    bl.push_back(bptr);

    logger().debug(
      "ExtentReader::read_segment_header: segment {} block crc {}",
      segment,
      bl.begin().crc32c(segment_manager.get_block_size(), 0));

    auto bp = bl.cbegin();
    try {
      decode(header, bp);
    } catch (ceph::buffer::error &e) {
      logger().debug(
	"ExtentReader::read_segment_header: segment {} unable to decode "
	"header, skipping",
	segment);
      return crimson::ct_error::enodata::make();
    }
    logger().debug(
      "ExtentReader::read_segment_header: segment {} header {}",
      segment,
      header);
    return read_segment_header_ret(
      read_segment_header_ertr::ready_future_marker{},
      header);
  });
}
ExtentReader::scan_extents_ret ExtentReader::scan_extents(
  scan_extents_cursor &cursor,
  extent_len_t bytes_to_read)
{
  auto ret = std::make_unique<scan_extents_ret_bare>();
  auto* extents = ret.get();
  return read_segment_header(cursor.get_segment_id()
  ).handle_error(
    scan_extents_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in ExtentReader::scan_extents"
    }
  ).safe_then([bytes_to_read, extents, &cursor, this](auto segment_header) {
    auto segment_nonce = segment_header.segment_nonce;
    return seastar::do_with(
      found_record_handler_t(
	[extents, this](
	  paddr_t base,
	  const record_header_t &header,
	  const bufferlist &mdbuf) mutable {

	  auto infos = try_decode_extent_infos(
	    header,
	    mdbuf);
	  if (!infos) {
	    // This should be impossible, we did check the crc on the mdbuf
	    logger().error(
	      "ExtentReader::scan_extents unable to decode extents for record {}",
	      base);
	    assert(infos);
	  }

	  paddr_t extent_offset = base.add_offset(header.mdlength);
	  for (const auto &i : *infos) {
	    extents->emplace_back(extent_offset, i);
	    auto& seg_addr = extent_offset.as_seg_paddr();
	    seg_addr.set_segment_off(
	      seg_addr.get_segment_off() + i.len);
	  }
	  return scan_extents_ertr::now();
	}),
      [=, &cursor](auto &dhandler) {
	return scan_valid_records(
	  cursor,
	  segment_nonce,
	  bytes_to_read,
	  dhandler).discard_result();
      });
  }).safe_then([ret=std::move(ret)] {
    return std::move(*ret);
  });
}

ExtentReader::scan_valid_records_ret ExtentReader::scan_valid_records(
  scan_valid_records_cursor &cursor,
  segment_nonce_t nonce,
  size_t budget,
  found_record_handler_t &handler)
{
  auto& segment_manager =
    *segment_managers[cursor.get_segment_id().device_id()];
  if (cursor.get_segment_offset() == 0) {
    cursor.increment(segment_manager.get_block_size());
  }
  auto retref = std::make_unique<size_t>(0);
  auto &budget_used = *retref;
  return crimson::repeat(
    [=, &cursor, &budget_used, &handler]() mutable
    -> scan_valid_records_ertr::future<seastar::stop_iteration> {
      return [=, &handler, &cursor, &budget_used] {
	if (!cursor.last_valid_header_found) {
	  return read_validate_record_metadata(cursor.seq.offset, nonce
	  ).safe_then([=, &cursor](auto md) {
	    logger().debug(
	      "ExtentReader::scan_valid_records: read complete {}",
	      cursor.seq);
	    if (!md) {
	      logger().debug(
		"ExtentReader::scan_valid_records: found invalid header at {}, presumably at end",
		cursor.seq);
	      cursor.last_valid_header_found = true;
	      return scan_valid_records_ertr::now();
	    } else {
	      auto new_committed_to = md->first.committed_to;
	      logger().debug(
		"ExtentReader::scan_valid_records: valid record read at {}, now committed at {}",
		cursor.seq,
		new_committed_to);
	      ceph_assert(cursor.last_committed == journal_seq_t() ||
		          cursor.last_committed <= new_committed_to);
	      cursor.last_committed = new_committed_to;
	      cursor.pending_records.emplace_back(
		cursor.seq.offset,
		md->first,
		md->second);
	      cursor.increment(md->first.dlength + md->first.mdlength);
	      ceph_assert(new_committed_to == journal_seq_t() ||
	                  new_committed_to < cursor.seq);
	      return scan_valid_records_ertr::now();
	    }
	  }).safe_then([=, &cursor, &budget_used, &handler] {
	    return crimson::repeat(
	      [=, &budget_used, &cursor, &handler] {
		logger().debug(
		  "ExtentReader::scan_valid_records: valid record read, processing queue");
		if (cursor.pending_records.empty()) {
		  /* This is only possible if the segment is empty.
		   * A record's last_commited must be prior to its own
		   * location since it itself cannot yet have been committed
		   * at its own time of submission.  Thus, the most recently
		   * read record must always fall after cursor.last_committed */
		  return scan_valid_records_ertr::make_ready_future<
		    seastar::stop_iteration>(seastar::stop_iteration::yes);
		}
		auto &next = cursor.pending_records.front();
		journal_seq_t next_seq = {cursor.seq.segment_seq, next.offset};
		if (cursor.last_committed == journal_seq_t() ||
		    next_seq > cursor.last_committed) {
		  return scan_valid_records_ertr::make_ready_future<
		    seastar::stop_iteration>(seastar::stop_iteration::yes);
		}
		budget_used +=
		  next.header.dlength + next.header.mdlength;
		return handler(
		  next.offset,
		  next.header,
		  next.mdbuffer
		).safe_then([&cursor] {
		  cursor.pending_records.pop_front();
		  return scan_valid_records_ertr::make_ready_future<
		    seastar::stop_iteration>(seastar::stop_iteration::no);
		});
	      });
	  });
	} else {
	  assert(!cursor.pending_records.empty());
	  auto &next = cursor.pending_records.front();
	  return read_validate_data(next.offset, next.header
	  ).safe_then([=, &budget_used, &next, &cursor, &handler](auto valid) {
	    if (!valid) {
	      cursor.pending_records.clear();
	      return scan_valid_records_ertr::now();
	    }
	    budget_used +=
	      next.header.dlength + next.header.mdlength;
	    return handler(
	      next.offset,
	      next.header,
	      next.mdbuffer
	    ).safe_then([&cursor] {
	      cursor.pending_records.pop_front();
	      return scan_valid_records_ertr::now();
	    });
	  });
	}
      }().safe_then([=, &budget_used, &cursor] {
	if (cursor.is_complete() || budget_used >= budget) {
	  return seastar::stop_iteration::yes;
	} else {
	  return seastar::stop_iteration::no;
	}
      });
    }).safe_then([retref=std::move(retref)]() mutable -> scan_valid_records_ret {
      return scan_valid_records_ret(
	scan_valid_records_ertr::ready_future_marker{},
	std::move(*retref));
    });
}

ExtentReader::read_validate_record_metadata_ret
ExtentReader::read_validate_record_metadata(
  paddr_t start,
  segment_nonce_t nonce)
{
  auto& seg_addr = start.as_seg_paddr();
  auto& segment_manager = *segment_managers[seg_addr.get_segment_id().device_id()];
  auto block_size = segment_manager.get_block_size();
  if (seg_addr.get_segment_off() + block_size > 
      (int64_t)segment_manager.get_segment_size()) {
    return read_validate_record_metadata_ret(
      read_validate_record_metadata_ertr::ready_future_marker{},
      std::nullopt);
  }
  return segment_manager.read(start, block_size
  ).safe_then(
    [=, &segment_manager](bufferptr bptr) mutable
    -> read_validate_record_metadata_ret {
      logger().debug("read_validate_record_metadata: reading {}", start);
      auto block_size = segment_manager.get_block_size();
      bufferlist bl;
      bl.append(bptr);
      auto bp = bl.cbegin();
      record_header_t header;
      try {
	decode(header, bp);
      } catch (ceph::buffer::error &e) {
	return read_validate_record_metadata_ret(
	  read_validate_record_metadata_ertr::ready_future_marker{},
	  std::nullopt);
      }
      if (header.segment_nonce != nonce) {
	return read_validate_record_metadata_ret(
	  read_validate_record_metadata_ertr::ready_future_marker{},
	  std::nullopt);
      }
      auto& seg_addr = start.as_seg_paddr();
      if (header.mdlength > (extent_len_t)block_size) {
	if (seg_addr.get_segment_off() + header.mdlength >
	    (int64_t)segment_manager.get_segment_size()) {
	  return crimson::ct_error::input_output_error::make();
	}
	return segment_manager.read(
	  paddr_t::make_seg_paddr(seg_addr.get_segment_id(),
	   seg_addr.get_segment_off() + (segment_off_t)block_size),
	  header.mdlength - block_size).safe_then(
	    [header=std::move(header), bl=std::move(bl)](
	      auto &&bptail) mutable {
	      bl.push_back(bptail);
	      return read_validate_record_metadata_ret(
		read_validate_record_metadata_ertr::ready_future_marker{},
		std::make_pair(std::move(header), std::move(bl)));
	    });
      } else {
	return read_validate_record_metadata_ret(
	  read_validate_record_metadata_ertr::ready_future_marker{},
	  std::make_pair(std::move(header), std::move(bl))
	);
      }
    }).safe_then([=](auto p) {
      if (p && validate_metadata(p->second)) {
	return read_validate_record_metadata_ret(
	  read_validate_record_metadata_ertr::ready_future_marker{},
	  std::move(*p)
	);
      } else {
	return read_validate_record_metadata_ret(
	  read_validate_record_metadata_ertr::ready_future_marker{},
	  std::nullopt);
      }
    });
}

std::optional<std::vector<extent_info_t>>
ExtentReader::try_decode_extent_infos(
  record_header_t header,
  const bufferlist &bl)
{
  auto bliter = bl.cbegin();
  bliter += ceph::encoded_sizeof_bounded<record_header_t>();
  bliter += sizeof(checksum_t) /* crc */;
  logger().debug("{}: decoding {} extents", __func__, header.extents);
  std::vector<extent_info_t> extent_infos(header.extents);
  for (auto &&i : extent_infos) {
    try {
      decode(i, bliter);
    } catch (ceph::buffer::error &e) {
      return std::nullopt;
    }
  }
  return extent_infos;
}

ExtentReader::read_validate_data_ret
ExtentReader::read_validate_data(
  paddr_t record_base,
  const record_header_t &header)
{
  auto& segment_manager = *segment_managers[record_base.get_device_id()];
  return segment_manager.read(
    record_base.add_offset(header.mdlength),
    header.dlength
  ).safe_then([=, &header](auto bptr) {
    bufferlist bl;
    bl.append(bptr);
    return bl.crc32c(-1) == header.data_crc;
  });
}

bool ExtentReader::validate_metadata(const bufferlist &bl)
{
  auto bliter = bl.cbegin();
  auto test_crc = bliter.crc32c(
    ceph::encoded_sizeof_bounded<record_header_t>(),
    -1);
  ceph_le32 recorded_crc_le;
  decode(recorded_crc_le, bliter);
  uint32_t recorded_crc = recorded_crc_le;
  test_crc = bliter.crc32c(
    bliter.get_remaining(),
    test_crc);
  return test_crc == recorded_crc;
}

} // namespace crimson::os::seastore
