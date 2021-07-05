// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <boost/iterator/counting_iterator.hpp>

#include "crimson/os/seastore/journal.h"

#include "include/intarith.h"
#include "crimson/os/seastore/segment_manager.h"

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

Journal::Journal(SegmentManager &segment_manager)
  : segment_manager(segment_manager) {}


Journal::initialize_segment_ertr::future<segment_seq_t>
Journal::initialize_segment(Segment &segment)
{
  auto new_tail = segment_provider->get_journal_tail_target();
  logger().debug(
    "initialize_segment {} journal_tail_target {}",
    segment.get_segment_id(),
    new_tail);
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
    current_segment_nonce};
  encode(header, bl);

  bufferptr bp(
    ceph::buffer::create_page_aligned(
      segment_manager.get_block_size()));
  bp.zero();
  auto iter = bl.cbegin();
  iter.copy(bl.length(), bp.c_str());
  bl.clear();
  bl.append(bp);

  written_to = segment_manager.get_block_size();
  committed_to = 0;
  return segment.write(0, bl).safe_then(
    [=] {
      segment_provider->update_journal_tail_committed(new_tail);
      return seq;
    },
    initialize_segment_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in Journal::initialize_segment"
    });
}

ceph::bufferlist Journal::encode_record(
  record_size_t rsize,
  record_t &&record)
{
  bufferlist data_bl;
  for (auto &i: record.extents) {
    data_bl.append(i.bl);
  }

  bufferlist bl;
  record_header_t header{
    rsize.mdlength,
    rsize.dlength,
    (uint32_t)record.deltas.size(),
    (uint32_t)record.extents.size(),
    current_segment_nonce,
    committed_to,
    data_bl.crc32c(-1)
  };
  encode(header, bl);

  auto metadata_crc_filler = bl.append_hole(sizeof(uint32_t));

  for (const auto &i: record.extents) {
    encode(extent_info_t(i), bl);
  }
  for (const auto &i: record.deltas) {
    encode(i, bl);
  }
  auto block_size = segment_manager.get_block_size();
  if (bl.length() % block_size != 0) {
    bl.append_zero(
      block_size - (bl.length() % block_size));
  }
  ceph_assert(bl.length() == rsize.mdlength);


  auto bliter = bl.cbegin();
  auto metadata_crc = bliter.crc32c(
    ceph::encoded_sizeof_bounded<record_header_t>(),
    -1);
  bliter += sizeof(checksum_t); /* crc hole again */
  metadata_crc = bliter.crc32c(
    bliter.get_remaining(),
    metadata_crc);
  ceph_le32 metadata_crc_le;
  metadata_crc_le = metadata_crc;
  metadata_crc_filler.copy_in(
    sizeof(checksum_t),
    reinterpret_cast<const char *>(&metadata_crc_le));

  bl.claim_append(data_bl);
  ceph_assert(bl.length() == (rsize.dlength + rsize.mdlength));

  return bl;
}

bool Journal::validate_metadata(const bufferlist &bl)
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

Journal::read_validate_data_ret Journal::read_validate_data(
  paddr_t record_base,
  const record_header_t &header)
{
  return segment_manager.read(
    record_base.add_offset(header.mdlength),
    header.dlength
  ).safe_then([=, &header](auto bptr) {
    bufferlist bl;
    bl.append(bptr);
    return bl.crc32c(-1) == header.data_crc;
  });
}

Journal::write_record_ret Journal::write_record(
  record_size_t rsize,
  record_t &&record,
  OrderingHandle &handle)
{
  ceph::bufferlist to_write = encode_record(
    rsize, std::move(record));
  auto target = written_to;
  assert((to_write.length() % segment_manager.get_block_size()) == 0);
  written_to += to_write.length();
  logger().debug(
    "write_record, mdlength {}, dlength {}, target {}",
    rsize.mdlength,
    rsize.dlength,
    target);

  auto segment_id = current_journal_segment->get_segment_id();

  // Start write under the current exclusive stage, but wait for it
  // in the device_submission concurrent stage to permit multiple
  // overlapping writes.
  auto write_fut = current_journal_segment->write(target, to_write);
  return handle.enter(write_pipeline->device_submission
  ).then([write_fut = std::move(write_fut)]() mutable {
    return std::move(write_fut
    ).handle_error(
      write_record_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in Journal::write_record"
      }
    );
  }).safe_then([this, &handle] {
    return handle.enter(write_pipeline->finalize);
  }).safe_then([this, target, segment_id] {
    logger().debug(
      "write_record: commit target {}",
      target);
    if (segment_id == current_journal_segment->get_segment_id()) {
      assert(committed_to < target);
      committed_to = target;
    }
    return write_record_ret(
      write_record_ertr::ready_future_marker{},
      paddr_t{
	segment_id,
	target});
  });
}

Journal::record_size_t Journal::get_encoded_record_length(
  const record_t &record) const {
  extent_len_t metadata =
    (extent_len_t)ceph::encoded_sizeof_bounded<record_header_t>();
  metadata += sizeof(checksum_t) /* crc */;
  metadata += record.extents.size() *
    ceph::encoded_sizeof_bounded<extent_info_t>();
  extent_len_t data = 0;
  for (const auto &i: record.deltas) {
    metadata += ceph::encoded_sizeof(i);
  }
  for (const auto &i: record.extents) {
    data += i.bl.length();
  }
  metadata = p2roundup(metadata, (extent_len_t)segment_manager.get_block_size());
  return record_size_t{metadata, data};
}

bool Journal::needs_roll(segment_off_t length) const
{
  return length + written_to >
    current_journal_segment->get_write_capacity();
}

Journal::roll_journal_segment_ertr::future<segment_seq_t>
Journal::roll_journal_segment()
{
  auto old_segment_id = current_journal_segment ?
    current_journal_segment->get_segment_id() :
    NULL_SEG_ID;

  return (current_journal_segment ?
	  current_journal_segment->close() :
	  Segment::close_ertr::now()).safe_then([this] {
      return segment_provider->get_segment();
    }).safe_then([this](auto segment) {
      return segment_manager.open(segment);
    }).safe_then([this](auto sref) {
      current_journal_segment = sref;
      written_to = 0;
      return initialize_segment(*current_journal_segment);
    }).safe_then([=](auto seq) {
      if (old_segment_id != NULL_SEG_ID) {
	segment_provider->close_segment(old_segment_id);
      }
      segment_provider->set_journal_segment(
	current_journal_segment->get_segment_id(),
	seq);
      return seq;
    }).handle_error(
      roll_journal_segment_ertr::pass_further{},
      crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); })
    );
}

Journal::read_segment_header_ret
Journal::read_segment_header(segment_id_t segment)
{
  return segment_manager.read(
    paddr_t{segment, 0},
    segment_manager.get_block_size()
  ).handle_error(
    read_segment_header_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in Journal::read_segment_header"
    }
  ).safe_then([=](bufferptr bptr) -> read_segment_header_ret {
    logger().debug("segment {} bptr size {}", segment, bptr.length());

    segment_header_t header;
    bufferlist bl;
    bl.push_back(bptr);

    logger().debug(
      "Journal::read_segment_header: segment {} block crc {}",
      segment,
      bl.begin().crc32c(segment_manager.get_block_size(), 0));

    auto bp = bl.cbegin();
    try {
      decode(header, bp);
    } catch (ceph::buffer::error &e) {
      logger().debug(
	"Journal::read_segment_header: segment {} unable to decode "
	"header, skipping",
	segment);
      return crimson::ct_error::enodata::make();
    }
    logger().debug(
      "Journal::read_segment_header: segment {} header {}",
      segment,
      header);
    return read_segment_header_ret(
      read_segment_header_ertr::ready_future_marker{},
      header);
  });
}

Journal::open_for_write_ret Journal::open_for_write()
{
  return roll_journal_segment().safe_then([this](auto seq) {
    return open_for_write_ret(
      open_for_write_ertr::ready_future_marker{},
      journal_seq_t{
	seq,
	paddr_t{
	  current_journal_segment->get_segment_id(),
	    static_cast<segment_off_t>(segment_manager.get_block_size())}
      });
  });
}

Journal::find_replay_segments_fut Journal::find_replay_segments()
{
  return seastar::do_with(
    std::vector<std::pair<segment_id_t, segment_header_t>>(),
    [this](auto &&segments) mutable {
      return crimson::do_for_each(
	boost::make_counting_iterator(segment_id_t{0}),
	boost::make_counting_iterator(segment_manager.get_num_segments()),
	[this, &segments](auto i) {
	  return read_segment_header(i
	  ).safe_then([this, &segments, i](auto header) mutable {
	    if (generate_nonce(
		  header.journal_segment_seq,
		  segment_manager.get_meta()) != header.segment_nonce) {
	      logger().debug(
		"find_replay_segments: nonce mismatch segment {} header {}",
		i,
		header);
	      assert(0 == "impossible");
	      return find_replay_segments_ertr::now();
	    }

	    segments.emplace_back(i, std::move(header));
	    return find_replay_segments_ertr::now();
	  }).handle_error(
	    crimson::ct_error::enoent::handle([i](auto) {
	      logger().debug(
		"find_replay_segments: segment {} not available for read",
		i);
	      return find_replay_segments_ertr::now();
	    }),
	    crimson::ct_error::enodata::handle([i](auto) {
	      logger().debug(
		"find_replay_segments: segment {} header undecodable",
		i);
	      return find_replay_segments_ertr::now();
	    }),
	    find_replay_segments_ertr::pass_further{},
	    crimson::ct_error::assert_all{
	      "Invalid error in Journal::find_replay_segments"
            }
	  );
	}).safe_then([this, &segments]() mutable -> find_replay_segments_fut {
	  logger().debug(
	    "find_replay_segments: have {} segments",
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

	  next_journal_segment_seq =
	    segments.rbegin()->second.journal_segment_seq + 1;
	  std::for_each(
	    segments.begin(),
	    segments.end(),
	    [this](auto &seg) {
	      segment_provider->init_mark_segment_closed(
		seg.first,
		seg.second.journal_segment_seq);
	    });

	  auto journal_tail = segments.rbegin()->second.journal_tail;
	  segment_provider->update_journal_tail_committed(journal_tail);
	  auto replay_from = journal_tail.offset;
	  logger().debug(
	    "Journal::find_replay_segments: journal_tail={}",
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
		"find_replay_segments: journal_tail {} does not match {}",
		journal_tail,
		from->second);
	      assert(0 == "invalid");
	    }
	  } else {
	    replay_from = paddr_t{
	      from->first,
	      (segment_off_t)segment_manager.get_block_size()};
	  }
	  auto ret = replay_segments_t(segments.end() - from);
	  std::transform(
	    from, segments.end(), ret.begin(),
	    [this](const auto &p) {
	      auto ret = journal_seq_t{
		p.second.journal_segment_seq,
		paddr_t{
		  p.first,
		  (segment_off_t)segment_manager.get_block_size()}};
	      logger().debug(
		"Journal::find_replay_segments: replaying from  {}",
		ret);
	      return std::make_pair(ret, p.second);
	    });
	  ret[0].first.offset = replay_from;
	  return find_replay_segments_fut(
	    find_replay_segments_ertr::ready_future_marker{},
	    std::move(ret));
	});
    });
}

Journal::read_validate_record_metadata_ret Journal::read_validate_record_metadata(
  paddr_t start,
  segment_nonce_t nonce)
{
  auto block_size = segment_manager.get_block_size();
  if (start.offset + block_size > (int64_t)segment_manager.get_segment_size()) {
    return read_validate_record_metadata_ret(
      read_validate_record_metadata_ertr::ready_future_marker{},
      std::nullopt);
  }
  return segment_manager.read(start, block_size
  ).safe_then(
    [=](bufferptr bptr) mutable
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
      if (header.mdlength > (extent_len_t)block_size) {
	if (start.offset + header.mdlength >
	    (int64_t)segment_manager.get_segment_size()) {
	  return crimson::ct_error::input_output_error::make();
	}
	return segment_manager.read(
	  {start.segment, start.offset + (segment_off_t)block_size},
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

std::optional<std::vector<delta_info_t>> Journal::try_decode_deltas(
  record_header_t header,
  const bufferlist &bl)
{
  auto bliter = bl.cbegin();
  bliter += ceph::encoded_sizeof_bounded<record_header_t>();
  bliter += sizeof(checksum_t) /* crc */;
  bliter += header.extents  * ceph::encoded_sizeof_bounded<extent_info_t>();
  logger().debug("{}: decoding {} deltas", __func__, header.deltas);
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

std::optional<std::vector<extent_info_t>> Journal::try_decode_extent_infos(
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

Journal::replay_ertr::future<>
Journal::replay_segment(
  journal_seq_t seq,
  segment_header_t header,
  delta_handler_t &handler)
{
  logger().debug("replay_segment: starting at {}", seq);
  return seastar::do_with(
    scan_valid_records_cursor(seq.offset),
    found_record_handler_t(
      [=, &handler](paddr_t base,
		    const record_header_t &header,
		    const bufferlist &mdbuf) {
	auto deltas = try_decode_deltas(
	  header,
	  mdbuf);
	if (!deltas) {
	  // This should be impossible, we did check the crc on the mdbuf
	  logger().error(
	    "Journal::replay_segment unable to decode deltas for record {}",
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
      return scan_valid_records(
	cursor,
	header.segment_nonce,
	std::numeric_limits<size_t>::max(),
	dhandler).safe_then([](auto){});
    });
}

Journal::replay_ret Journal::replay(delta_handler_t &&delta_handler)
{
  return seastar::do_with(
    std::move(delta_handler), replay_segments_t(),
    [this](auto &handler, auto &segments) mutable -> replay_ret {
      return find_replay_segments().safe_then(
        [this, &handler, &segments](auto replay_segs) mutable {
          logger().debug("replay: found {} segments", replay_segs.size());
          segments = std::move(replay_segs);
          return crimson::do_for_each(segments, [this, &handler](auto i) mutable {
            return replay_segment(i.first, i.second, handler);
          });
        });
    });
}

Journal::scan_extents_ret Journal::scan_extents(
  scan_extents_cursor &cursor,
  extent_len_t bytes_to_read)
{
  auto ret = std::make_unique<scan_extents_ret_bare>();
  auto* extents = ret.get();
  return read_segment_header(cursor.get_offset().segment
  ).handle_error(
    scan_extents_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in Journal::scan_extents"
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
	      "Journal::scan_extents unable to decode extents for record {}",
	      base);
	    assert(infos);
	  }

	  paddr_t extent_offset = base.add_offset(header.mdlength);
	  for (const auto &i : *infos) {
	    extents->emplace_back(extent_offset, i);
	    extent_offset.offset += i.len;
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

Journal::scan_valid_records_ret Journal::scan_valid_records(
    scan_valid_records_cursor &cursor,
    segment_nonce_t nonce,
    size_t budget,
    found_record_handler_t &handler)
{
  if (cursor.offset.offset == 0) {
    cursor.offset.offset = segment_manager.get_block_size();
  }
  auto retref = std::make_unique<size_t>(0);
  auto budget_used = *retref;
  return crimson::repeat(
    [=, &cursor, &budget_used, &handler]() mutable
    -> scan_valid_records_ertr::future<seastar::stop_iteration> {
      return [=, &handler, &cursor, &budget_used] {
	if (!cursor.last_valid_header_found) {
	  return read_validate_record_metadata(cursor.offset, nonce
	  ).safe_then([=, &cursor](auto md) {
	    logger().debug(
	      "Journal::scan_valid_records: read complete {}",
	      cursor.offset);
	    if (!md) {
	      logger().debug(
		"Journal::scan_valid_records: found invalid header at {}, presumably at end",
		cursor.offset);
	      cursor.last_valid_header_found = true;
	      return scan_valid_records_ertr::now();
	    } else {
	      logger().debug(
		"Journal::scan_valid_records: valid record read at {}",
		cursor.offset);
	      cursor.last_committed = paddr_t{
		cursor.offset.segment,
		md->first.committed_to};
	      cursor.pending_records.emplace_back(
		cursor.offset,
		md->first,
		md->second);
	      cursor.offset.offset +=
		md->first.dlength + md->first.mdlength;
	      return scan_valid_records_ertr::now();
	    }
	  }).safe_then([=, &cursor, &budget_used, &handler] {
	    return crimson::repeat(
	      [=, &budget_used, &cursor, &handler] {
		logger().debug(
		  "Journal::scan_valid_records: valid record read, processing queue");
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
		if (next.offset > cursor.last_committed) {
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


}
