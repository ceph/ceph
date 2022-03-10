// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/extent_reader.h"

#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore {

ExtentReader::read_segment_tail_ret
ExtentReader::read_segment_tail(segment_id_t segment)
{
  auto& segment_manager = *segment_managers[segment.device_id()];
  return segment_manager.read(
    paddr_t::make_seg_paddr(
      segment,
      segment_manager.get_segment_size() -
        segment_manager.get_rounded_tail_length()),
    segment_manager.get_rounded_tail_length()
  ).handle_error(
    read_segment_header_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in ExtentReader::read_segment_tail"
    }
  ).safe_then([=, &segment_manager](bufferptr bptr) -> read_segment_tail_ret {
    LOG_PREFIX(ExtentReader::read_segment_tail);
    DEBUG("segment {} bptr size {}", segment, bptr.length());

    segment_tail_t tail;
    bufferlist bl;
    bl.push_back(bptr);

    DEBUG("segment {} block crc {}",
          segment,
          bl.begin().crc32c(segment_manager.get_block_size(), 0));

    auto bp = bl.cbegin();
    try {
      decode(tail, bp);
    } catch (ceph::buffer::error &e) {
      DEBUG("segment {} unable to decode tail, skipping -- {}",
            segment, e);
      return crimson::ct_error::enodata::make();
    }
    DEBUG("segment {} tail {}", segment, tail);
    return read_segment_tail_ret(
      read_segment_tail_ertr::ready_future_marker{},
      tail);
  });
}

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
    LOG_PREFIX(ExtentReader::read_segment_header);
    DEBUG("segment {} bptr size {}", segment, bptr.length());

    segment_header_t header;
    bufferlist bl;
    bl.push_back(bptr);

    DEBUG("segment {} block crc {}",
          segment,
          bl.begin().crc32c(segment_manager.get_block_size(), 0));

    auto bp = bl.cbegin();
    try {
      decode(header, bp);
    } catch (ceph::buffer::error &e) {
      DEBUG("segment {} unable to decode header, skipping -- {}",
            segment, e);
      return crimson::ct_error::enodata::make();
    }
    DEBUG("segment {} header {}", segment, header);
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
      found_record_handler_t([extents](
        record_locator_t locator,
        const record_group_header_t& header,
        const bufferlist& mdbuf) mutable -> scan_valid_records_ertr::future<>
      {
        LOG_PREFIX(ExtentReader::scan_extents);
        DEBUG("decoding {} records", header.records);
        auto maybe_record_extent_infos = try_decode_extent_infos(header, mdbuf);
        if (!maybe_record_extent_infos) {
          // This should be impossible, we did check the crc on the mdbuf
          ERROR("unable to decode extents for record {}",
                locator.record_block_base);
          return crimson::ct_error::input_output_error::make();
        }

        paddr_t extent_offset = locator.record_block_base;
        for (auto& r: *maybe_record_extent_infos) {
          DEBUG("decoded {} extents", r.extent_infos.size());
          for (const auto &i : r.extent_infos) {
            extents->emplace_back(
              extent_offset,
              std::pair<commit_info_t, extent_info_t>(
                {r.header.commit_time,
                r.header.commit_type},
                i));
            auto& seg_addr = extent_offset.as_seg_paddr();
            seg_addr.set_segment_off(
              seg_addr.get_segment_off() + i.len);
          }
        }
        return scan_extents_ertr::now();
      }),
      [bytes_to_read, segment_nonce, &cursor, this](auto &dhandler) {
        return scan_valid_records(
          cursor,
          segment_nonce,
          bytes_to_read,
          dhandler
        ).discard_result();
      }
    );
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
  LOG_PREFIX(ExtentReader::scan_valid_records);
  auto& segment_manager =
    *segment_managers[cursor.get_segment_id().device_id()];
  if (cursor.get_segment_offset() == 0) {
    INFO("start to scan segment {}", cursor.get_segment_id());
    cursor.increment_seq(segment_manager.get_block_size());
  }
  DEBUG("starting at {}, budget={}", cursor, budget);
  auto retref = std::make_unique<size_t>(0);
  auto &budget_used = *retref;
  return crimson::repeat(
    [=, &cursor, &budget_used, &handler]() mutable
    -> scan_valid_records_ertr::future<seastar::stop_iteration> {
      return [=, &handler, &cursor, &budget_used] {
	if (!cursor.last_valid_header_found) {
	  return read_validate_record_metadata(cursor.seq.offset, nonce
	  ).safe_then([=, &cursor](auto md) {
	    if (!md) {
	      cursor.last_valid_header_found = true;
	      if (cursor.is_complete()) {
	        INFO("complete at {}, invalid record group metadata",
                     cursor);
	      } else {
	        DEBUG("found invalid record group metadata at {}, "
	              "processing {} pending record groups",
	              cursor.seq,
	              cursor.pending_record_groups.size());
	      }
	      return scan_valid_records_ertr::now();
	    } else {
	      auto& [header, md_bl] = *md;
	      DEBUG("found valid {} at {}", header, cursor.seq);
	      cursor.emplace_record_group(header, std::move(md_bl));
	      return scan_valid_records_ertr::now();
	    }
	  }).safe_then([=, &cursor, &budget_used, &handler] {
	    DEBUG("processing committed record groups until {}, {} pending",
		  cursor.last_committed,
		  cursor.pending_record_groups.size());
	    return crimson::repeat(
	      [=, &budget_used, &cursor, &handler] {
		if (cursor.pending_record_groups.empty()) {
		  /* This is only possible if the segment is empty.
		   * A record's last_commited must be prior to its own
		   * location since it itself cannot yet have been committed
		   * at its own time of submission.  Thus, the most recently
		   * read record must always fall after cursor.last_committed */
		  return scan_valid_records_ertr::make_ready_future<
		    seastar::stop_iteration>(seastar::stop_iteration::yes);
		}
		auto &next = cursor.pending_record_groups.front();
		journal_seq_t next_seq = {cursor.seq.segment_seq, next.offset};
		if (cursor.last_committed == JOURNAL_SEQ_NULL ||
		    next_seq > cursor.last_committed) {
		  return scan_valid_records_ertr::make_ready_future<
		    seastar::stop_iteration>(seastar::stop_iteration::yes);
		}
		return consume_next_records(cursor, handler, budget_used
		).safe_then([] {
		  return scan_valid_records_ertr::make_ready_future<
		    seastar::stop_iteration>(seastar::stop_iteration::no);
		});
	      });
	  });
	} else {
	  assert(!cursor.pending_record_groups.empty());
	  auto &next = cursor.pending_record_groups.front();
	  return read_validate_data(next.offset, next.header
	  ).safe_then([this, FNAME, &budget_used, &cursor, &handler, &next](auto valid) {
	    if (!valid) {
	      INFO("complete at {}, invalid record group data at {}, {}",
		   cursor, next.offset, next.header);
	      cursor.pending_record_groups.clear();
	      return scan_valid_records_ertr::now();
	    }
            return consume_next_records(cursor, handler, budget_used);
	  });
	}
      }().safe_then([=, &budget_used, &cursor] {
	if (cursor.is_complete() || budget_used >= budget) {
	  DEBUG("finish at {}, budget_used={}, budget={}",
                cursor, budget_used, budget);
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
  LOG_PREFIX(ExtentReader::read_validate_record_metadata);
  auto& seg_addr = start.as_seg_paddr();
  auto& segment_manager = *segment_managers[seg_addr.get_segment_id().device_id()];
  auto block_size = segment_manager.get_block_size();
  auto segment_size = static_cast<int64_t>(segment_manager.get_segment_size());
  if (seg_addr.get_segment_off() + block_size > segment_size) {
    DEBUG("failed -- record group header block {}~4096 > segment_size {}", start, segment_size);
    return read_validate_record_metadata_ret(
      read_validate_record_metadata_ertr::ready_future_marker{},
      std::nullopt);
  }
  TRACE("reading record group header block {}~4096", start);
  return segment_manager.read(start, block_size
  ).safe_then([=, &segment_manager](bufferptr bptr) mutable
              -> read_validate_record_metadata_ret {
    auto block_size = static_cast<extent_len_t>(
        segment_manager.get_block_size());
    bufferlist bl;
    bl.append(bptr);
    auto maybe_header = try_decode_records_header(bl, nonce);
    if (!maybe_header.has_value()) {
      return read_validate_record_metadata_ret(
        read_validate_record_metadata_ertr::ready_future_marker{},
        std::nullopt);
    }
    auto& seg_addr = start.as_seg_paddr();
    auto& header = *maybe_header;
    if (header.mdlength < block_size ||
        header.mdlength % block_size != 0 ||
        header.dlength % block_size != 0 ||
        (header.committed_to != JOURNAL_SEQ_NULL &&
         header.committed_to.offset.as_seg_paddr().get_segment_off() % block_size != 0) ||
        (seg_addr.get_segment_off() + header.mdlength + header.dlength > segment_size)) {
      ERROR("failed, invalid record group header {}", start);
      return crimson::ct_error::input_output_error::make();
    }
    if (header.mdlength == block_size) {
      return read_validate_record_metadata_ret(
        read_validate_record_metadata_ertr::ready_future_marker{},
        std::make_pair(std::move(header), std::move(bl))
      );
    }

    auto rest_start = paddr_t::make_seg_paddr(
        seg_addr.get_segment_id(),
        seg_addr.get_segment_off() + (seastore_off_t)block_size
    );
    auto rest_len = header.mdlength - block_size;
    TRACE("reading record group header rest {}~{}", rest_start, rest_len);
    return segment_manager.read(rest_start, rest_len
    ).safe_then([header=std::move(header), bl=std::move(bl)
                ](auto&& bptail) mutable {
      bl.push_back(bptail);
      return read_validate_record_metadata_ret(
        read_validate_record_metadata_ertr::ready_future_marker{},
        std::make_pair(std::move(header), std::move(bl)));
    });
  }).safe_then([](auto p) {
    if (p && validate_records_metadata(p->second)) {
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

ExtentReader::read_validate_data_ret
ExtentReader::read_validate_data(
  paddr_t record_base,
  const record_group_header_t &header)
{
  LOG_PREFIX(ExtentReader::read_validate_data);
  auto& segment_manager = *segment_managers[record_base.get_device_id()];
  auto data_addr = record_base.add_offset(header.mdlength);
  TRACE("reading record group data blocks {}~{}", data_addr, header.dlength);
  return segment_manager.read(
    data_addr,
    header.dlength
  ).safe_then([=, &header](auto bptr) {
    bufferlist bl;
    bl.append(bptr);
    return validate_records_data(header, bl);
  });
}

ExtentReader::consume_record_group_ertr::future<>
ExtentReader::consume_next_records(
  scan_valid_records_cursor& cursor,
  found_record_handler_t& handler,
  std::size_t& budget_used)
{
  LOG_PREFIX(ExtentReader::consume_next_records);
  auto& next = cursor.pending_record_groups.front();
  auto total_length = next.header.dlength + next.header.mdlength;
  budget_used += total_length;
  auto locator = record_locator_t{
    next.offset.add_offset(next.header.mdlength),
    write_result_t{
      journal_seq_t{
        cursor.seq.segment_seq,
        next.offset
      },
      static_cast<seastore_off_t>(total_length)
    }
  };
  DEBUG("processing {} at {}, budget_used={}",
        next.header, locator, budget_used);
  return handler(
    locator,
    next.header,
    next.mdbuffer
  ).safe_then([FNAME, &cursor] {
    cursor.pop_record_group();
    if (cursor.is_complete()) {
      INFO("complete at {}, no more record group", cursor);
    }
  });
}

} // namespace crimson::os::seastore
