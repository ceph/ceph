// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/record_scanner.h"

#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore {

RecordScanner::scan_valid_records_ret
RecordScanner::scan_valid_records(
  scan_valid_records_cursor &cursor,
  segment_nonce_t nonce,
  size_t budget,
  found_record_handler_t &handler)
{
  LOG_PREFIX(RecordScanner::scan_valid_records);
  initialize_cursor(cursor);
  DEBUG("starting at {}, budget={}", cursor, budget);
  auto retref = std::make_unique<size_t>(0);
  auto &budget_used = *retref;
  return crimson::repeat(
    [=, &cursor, &budget_used, &handler, this]() mutable
    -> scan_valid_records_ertr::future<seastar::stop_iteration> {
      return [=, &handler, &cursor, &budget_used, this] {
	if (!cursor.last_valid_header_found) {
	  return read_validate_record_metadata(cursor, nonce
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
	  }).safe_then([=, &cursor, &budget_used, &handler, this] {
	    DEBUG("processing committed record groups until {}, {} pending",
		  cursor.last_committed,
		  cursor.pending_record_groups.size());
	    return crimson::repeat(
	      [=, &budget_used, &cursor, &handler, this] {
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
    }).safe_then([retref=std::move(retref)] {
      return scan_valid_records_ertr::make_ready_future();
    });
}

RecordScanner::read_validate_record_metadata_ret
RecordScanner::read_validate_record_metadata(
  scan_valid_records_cursor &cursor,
  segment_nonce_t nonce)
{
  LOG_PREFIX(RecordScanner::read_validate_record_metadata);
  paddr_t start = cursor.seq.offset;
  auto block_size = cursor.get_block_size();
  if (get_segment_off(cursor.seq.offset) + block_size > get_segment_end_offset(cursor.seq.offset)) {
    DEBUG("failed -- record group header block {}~4096 > segment_size {}",
      start, get_segment_end_offset(cursor.seq.offset));
    return read_validate_record_metadata_ret(
      read_validate_record_metadata_ertr::ready_future_marker{},
      std::nullopt);
  }
  TRACE("reading record group header block {}~4096", start);
  return read(start, block_size
  ).safe_then([=](bufferptr bptr) mutable
              -> read_validate_record_metadata_ret {
    bufferlist bl;
    bl.append(bptr);
    auto maybe_header = try_decode_records_header(bl, nonce);
    if (!maybe_header.has_value()) {
      return read_validate_record_metadata_ret(
        read_validate_record_metadata_ertr::ready_future_marker{},
        std::nullopt);
    }

    auto& header = *maybe_header;
    if (header.mdlength < block_size ||
        header.mdlength % block_size != 0 ||
        header.dlength % block_size != 0 ||
	(header.committed_to != JOURNAL_SEQ_NULL &&
	get_segment_off(header.committed_to.offset) %
	cursor.get_block_size() != 0) ||
	(get_segment_off(cursor.seq.offset) + header.mdlength + header.dlength >
	get_segment_end_offset(cursor.seq.offset))) {
      ERROR("failed, invalid record group header {}", header);
      return crimson::ct_error::input_output_error::make();
    }

    if (is_record_segment_seq_invalid(cursor, header)) {
      return read_validate_record_metadata_ret(
        read_validate_record_metadata_ertr::ready_future_marker{},
        std::nullopt);
    }

    if (header.mdlength == block_size) {
      return read_validate_record_metadata_ret(
        read_validate_record_metadata_ertr::ready_future_marker{},
        std::make_pair(std::move(header), std::move(bl))
      );
    }

    paddr_t rest_start = cursor.seq.offset.add_offset(block_size);
    auto rest_len = header.mdlength - block_size;
    TRACE("reading record group header rest {}~{}", rest_start, rest_len);
    return read(rest_start, rest_len
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

RecordScanner::read_validate_data_ret RecordScanner::read_validate_data(
  paddr_t record_base,
  const record_group_header_t &header)
{
  LOG_PREFIX(RecordScanner::read_validate_data);
  auto data_addr = record_base.add_offset(header.mdlength);
  TRACE("reading record group data blocks {}~{}", data_addr, header.dlength);
  return read(
    data_addr,
    header.dlength
  ).safe_then([=, &header](auto bptr) {
    bufferlist bl;
    bl.append(bptr);
    return validate_records_data(header, bl);
  });
}

RecordScanner::consume_record_group_ertr::future<>
RecordScanner::consume_next_records(
  scan_valid_records_cursor& cursor,
  found_record_handler_t& handler,
  std::size_t& budget_used)
{
  LOG_PREFIX(RecordScanner::consume_next_records);
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
      total_length
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

}
