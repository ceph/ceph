// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <boost/iterator/counting_iterator.hpp>

#include "crimson/os/seastore/journal.h"

#include "include/intarith.h"
#include "crimson/os/seastore/segment_cleaner.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const segment_header_t &header)
{
  return out << "segment_header_t("
	     << "segment_seq=" << header.journal_segment_seq
	     << ", physical_segment_id=" << header.physical_segment_id
	     << ", journal_tail=" << header.journal_tail
	     << ", segment_nonce=" << header.segment_nonce
	     << ", out-of-line=" << header.out_of_line
	     << ")";
}


std::ostream &operator<<(std::ostream &out, const extent_info_t &info)
{
  return out << "extent_info_t("
	     << " type: " << info.type
	     << " addr: " << info.addr
	     << " len: " << info.len
	     << ")";
}

segment_nonce_t generate_nonce(
  segment_seq_t seq,
  const seastore_meta_t &meta)
{
  return ceph_crc32c(
    seq,
    reinterpret_cast<const unsigned char *>(meta.seastore_id.bytes()),
    sizeof(meta.seastore_id.uuid));
}

Journal::Journal(
  SegmentManager& segment_manager,
  ExtentReader& scanner)
  : journal_segment_manager(segment_manager),
    scanner(scanner)
{}

Journal::write_record_ret Journal::write_record(
  record_size_t rsize,
  record_t &&record,
  OrderingHandle &handle)
{
  ceph::bufferlist to_write = encode_record(
    rsize,
    std::move(record),
    journal_segment_manager.get_block_size(),
    journal_segment_manager.get_committed_to(),
    journal_segment_manager.get_nonce());
  // Start write under the current exclusive stage, but wait for it
  // in the device_submission concurrent stage to permit multiple
  // overlapping writes.
  auto write_fut = journal_segment_manager.write(to_write);
  return handle.enter(write_pipeline->device_submission
  ).then([write_fut = std::move(write_fut)]() mutable {
    return std::move(write_fut);
  }).safe_then([this, &handle, rsize](journal_seq_t write_start) {
    return handle.enter(write_pipeline->finalize
    ).then([this, write_start, rsize] {
      auto committed_to = write_start;
      committed_to.offset.offset += (rsize.mdlength + rsize.dlength);
      journal_segment_manager.mark_committed(committed_to);
      return write_start.offset;
    });
  });
}

Journal::prep_replay_segments_fut
Journal::prep_replay_segments(
  std::vector<std::pair<segment_id_t, segment_header_t>> segments)
{
  logger().debug(
    "Journal::prep_replay_segments: have {} segments",
    segments.size());
  if (segments.empty()) {
    return crimson::ct_error::input_output_error::make();
  }
  std::sort(
    segments.begin(),
    segments.end(),
    [](const auto &lt, const auto &rt) {
      return lt.second.journal_segment_seq <
	rt.second.journal_segment_seq;
    });

  journal_segment_manager.set_segment_seq(
    segments.rbegin()->second.journal_segment_seq);
  std::for_each(
    segments.begin(),
    segments.end(),
    [this](auto &seg) {
      segment_provider->init_mark_segment_closed(
	seg.first,
	seg.second.journal_segment_seq,
	false);
    });

  auto journal_tail = segments.rbegin()->second.journal_tail;
  segment_provider->update_journal_tail_committed(journal_tail);
  auto replay_from = journal_tail.offset;
  logger().debug(
    "Journal::prep_replay_segments: journal_tail={}",
    journal_tail);
  auto from = segments.begin();
  if (replay_from != P_ADDR_NULL) {
    from = std::find_if(
      segments.begin(),
      segments.end(),
      [&replay_from](const auto &seg) -> bool {
	return seg.first == replay_from.segment;
      });
    if (from->second.journal_segment_seq != journal_tail.segment_seq) {
      logger().error(
	"Journal::prep_replay_segments: journal_tail {} does not match {}",
	journal_tail,
	from->second);
      assert(0 == "invalid");
    }
  } else {
    replay_from = paddr_t{
      from->first,
      (segment_off_t)journal_segment_manager.get_block_size()};
  }
  auto ret = replay_segments_t(segments.end() - from);
  std::transform(
    from, segments.end(), ret.begin(),
    [this](const auto &p) {
      auto ret = journal_seq_t{
	p.second.journal_segment_seq,
	paddr_t{
	  p.first,
	  (segment_off_t)journal_segment_manager.get_block_size()}};
      logger().debug(
	"Journal::prep_replay_segments: replaying from  {}",
	ret);
      return std::make_pair(ret, p.second);
    });
  ret[0].first.offset = replay_from;
  return prep_replay_segments_fut(
    prep_replay_segments_ertr::ready_future_marker{},
    std::move(ret));
}

std::optional<std::vector<delta_info_t>> Journal::try_decode_deltas(
  record_header_t header,
  const bufferlist &bl)
{
  auto bliter = bl.cbegin();
  bliter += ceph::encoded_sizeof_bounded<record_header_t>();
  bliter += sizeof(checksum_t) /* crc */;
  bliter += header.extents  * ceph::encoded_sizeof_bounded<extent_info_t>();
  logger().debug("Journal::try_decode_deltas: decoding {} deltas", header.deltas);
  std::vector<delta_info_t> deltas(header.deltas);
  for (auto &&i : deltas) {
    try {
      decode(i, bliter);
    } catch (ceph::buffer::error &e) {
      return std::nullopt;
    }
  }
  return deltas;
}

Journal::replay_ertr::future<>
Journal::replay_segment(
  journal_seq_t seq,
  segment_header_t header,
  delta_handler_t &handler)
{
  logger().debug("Journal::replay_segment: starting at {}", seq);
  return seastar::do_with(
    scan_valid_records_cursor(seq.offset),
    ExtentReader::found_record_handler_t(
      [=, &handler](paddr_t base,
		    const record_header_t &header,
		    const bufferlist &mdbuf) {
	auto deltas = try_decode_deltas(
	  header,
	  mdbuf);
	if (!deltas) {
	  // This should be impossible, we did check the crc on the mdbuf
	  logger().error(
	    "Journal::replay_segment: unable to decode deltas for record {}",
	    base);
	  assert(deltas);
	}

	return seastar::do_with(
	  std::move(*deltas),
	  [=](auto &deltas) {
	    return crimson::do_for_each(
	      deltas,
	      [=](auto &delta) {
		/* The journal may validly contain deltas for extents in
		 * since released segments.  We can detect those cases by
		 * checking whether the segment in question currently has a
		 * sequence number > the current journal segment seq. We can
		 * safetly skip these deltas because the extent must already
		 * have been rewritten.
		 *
		 * Note, this comparison exploits the fact that
		 * SEGMENT_SEQ_NULL is a large number.
		 */
		if (delta.paddr != P_ADDR_NULL &&
		    (segment_provider->get_seq(delta.paddr.segment) >
		     seq.segment_seq)) {
		  return replay_ertr::now();
		} else {
		  return handler(
		    journal_seq_t{seq.segment_seq, base},
		    base.add_offset(header.mdlength),
		    delta);
		}
	      });
	  });
      }),
    [=](auto &cursor, auto &dhandler) {
      return scanner.scan_valid_records(
	cursor,
	header.segment_nonce,
	std::numeric_limits<size_t>::max(),
	dhandler).safe_then([](auto){}
      ).handle_error(
	replay_ertr::pass_further{},
	crimson::ct_error::assert_all{
	  "shouldn't meet with any other error other replay_ertr"
	}
      );;
    });
}

Journal::replay_ret Journal::replay(
  std::vector<std::pair<segment_id_t, segment_header_t>>&& segment_headers,
  delta_handler_t &&delta_handler)
{
  return seastar::do_with(
    std::move(delta_handler), replay_segments_t(),
    [this, segment_headers=std::move(segment_headers)]
    (auto &handler, auto &segments) mutable -> replay_ret {
      return prep_replay_segments(std::move(segment_headers)).safe_then(
        [this, &handler, &segments](auto replay_segs) mutable {
          logger().debug("Journal::replay: found {} segments", replay_segs.size());
          segments = std::move(replay_segs);
          return crimson::do_for_each(segments, [this, &handler](auto i) mutable {
            return replay_segment(i.first, i.second, handler);
          });
        });
    });
}

Journal::JournalSegmentManager::JournalSegmentManager(
  SegmentManager& segment_manager)
  : segment_manager{segment_manager}
{
  reset();
}

Journal::JournalSegmentManager::close_ertr::future<>
Journal::JournalSegmentManager::close()
{
  return (
    current_journal_segment ?
    current_journal_segment->close() :
    Segment::close_ertr::now()
  ).handle_error(
    close_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in JournalSegmentManager::close()"
    }
  ).finally([this] {
    reset();
  });
}

Journal::JournalSegmentManager::roll_ertr::future<>
Journal::JournalSegmentManager::roll()
{
  auto old_segment_id = current_journal_segment ?
    current_journal_segment->get_segment_id() :
    NULL_SEG_ID;

  return (
    current_journal_segment ?
    current_journal_segment->close() :
    Segment::close_ertr::now()
  ).safe_then([this] {
    return segment_provider->get_segment(segment_manager.get_device_id());
  }).safe_then([this](auto segment) {
    return segment_manager.open(segment);
  }).safe_then([this](auto sref) {
    current_journal_segment = sref;
    return initialize_segment(*current_journal_segment);
  }).safe_then([this, old_segment_id] {
    if (old_segment_id != NULL_SEG_ID) {
      segment_provider->close_segment(old_segment_id);
    }
    segment_provider->set_journal_segment(
      current_journal_segment->get_segment_id(),
      get_segment_seq());
  }).handle_error(
    roll_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in JournalSegmentManager::roll"
    }
  );
}

Journal::JournalSegmentManager::write_ret
Journal::JournalSegmentManager::write(ceph::bufferlist to_write)
{
  auto write_length = to_write.length();
  auto write_start_seq = get_current_write_seq();
  logger().debug(
    "JournalSegmentManager::write: write_start {} => {}, length={}",
    write_start_seq,
    write_start_seq.offset.offset + write_length,
    write_length);
  assert((write_length % segment_manager.get_block_size()) == 0);
  assert(!needs_roll(write_length));

  auto write_start_offset = written_to;
  written_to += write_length;
  return current_journal_segment->write(
    write_start_offset, to_write
  ).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in JournalSegmentManager::write"
    }
  ).safe_then([write_start_seq] {
    return write_start_seq;
  });
}

void Journal::JournalSegmentManager::mark_committed(
  const journal_seq_t& new_committed_to)
{
  logger().debug(
    "JournalSegmentManager::mark_committed: committed_to {} => {}",
    committed_to, new_committed_to);
  assert(new_committed_to.segment_seq <=
         get_segment_seq());
  if (new_committed_to.segment_seq ==
      get_segment_seq()) {
    assert(committed_to.offset.offset < new_committed_to.offset.offset);
    committed_to = new_committed_to;
  }
}

Journal::JournalSegmentManager::initialize_segment_ertr::future<>
Journal::JournalSegmentManager::initialize_segment(Segment& segment)
{
  auto new_tail = segment_provider->get_journal_tail_target();
  // write out header
  ceph_assert(segment.get_write_ptr() == 0);
  bufferlist bl;

  segment_seq_t seq = next_journal_segment_seq++;
  current_segment_nonce = generate_nonce(
    seq, segment_manager.get_meta());
  auto header = segment_header_t{
    seq,
    segment.get_segment_id(),
    segment_provider->get_journal_tail_target(),
    current_segment_nonce,
    false};
  logger().debug(
    "JournalSegmentManager::initialize_segment: segment_id {} journal_tail_target {}, header {}",
    segment.get_segment_id(),
    new_tail,
    header);
  encode(header, bl);

  bufferptr bp(
    ceph::buffer::create_page_aligned(
      segment_manager.get_block_size()));
  bp.zero();
  auto iter = bl.cbegin();
  iter.copy(bl.length(), bp.c_str());
  bl.clear();
  bl.append(bp);

  written_to = 0;
  // FIXME: improve committed_to to point to another segment
  committed_to = get_current_write_seq();
  return write(bl
  ).safe_then([this, new_tail, write_size=bl.length()
              ](journal_seq_t write_start_seq) {
    auto committed_to = write_start_seq;
    committed_to.offset.offset += write_size;
    mark_committed(committed_to);
    segment_provider->update_journal_tail_committed(new_tail);
  });
}

}
