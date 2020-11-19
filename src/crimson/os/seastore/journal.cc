// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <boost/iterator/counting_iterator.hpp>

#include "crimson/os/seastore/journal.h"

#include "include/intarith.h"
#include "crimson/os/seastore/segment_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
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
  : block_size(segment_manager.get_block_size()),
    max_record_length(
      segment_manager.get_segment_size() -
      p2align(ceph::encoded_sizeof_bounded<segment_header_t>(),
	      size_t(block_size))),
    segment_manager(segment_manager) {}


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
  return segment.write(0, bl).safe_then(
    [=] {
      segment_provider->update_journal_tail_committed(new_tail);
      return seq;
    },
    initialize_segment_ertr::pass_further{},
    crimson::ct_error::assert_all{ "TODO" });
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
  ::decode(recorded_crc_le, bliter);
  uint32_t recorded_crc = recorded_crc_le;
  test_crc = bliter.crc32c(
    bliter.get_remaining(),
    test_crc);
  return test_crc == recorded_crc;
}

Journal::write_record_ret Journal::write_record(
  record_size_t rsize,
  record_t &&record)
{
  ceph::bufferlist to_write = encode_record(
    rsize, std::move(record));
  auto target = written_to;
  assert((to_write.length() % block_size) == 0);
  written_to += to_write.length();
  logger().debug(
    "write_record, mdlength {}, dlength {}, target {}",
    rsize.mdlength,
    rsize.dlength,
    target);
  return current_journal_segment->write(target, to_write).handle_error(
    write_record_ertr::pass_further{},
    crimson::ct_error::assert_all{ "TODO" }).safe_then([this, target] {
      return write_record_ret(
	write_record_ertr::ready_future_marker{},
	paddr_t{
	  current_journal_segment->get_segment_id(),
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
  metadata = p2roundup(metadata, block_size);
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
  return segment_manager.read(paddr_t{segment, 0}, block_size
  ).handle_error(
    read_segment_header_ertr::pass_further{},
    crimson::ct_error::assert_all{}
  ).safe_then([=](bufferptr bptr) -> read_segment_header_ret {
    logger().debug("segment {} bptr size {}", segment, bptr.length());

    segment_header_t header;
    bufferlist bl;
    bl.push_back(bptr);

    logger().debug(
      "find_replay_segments: segment {} block crc {}",
      segment,
      bl.begin().crc32c(block_size, 0));

    auto bp = bl.cbegin();
    try {
      decode(header, bp);
    } catch (ceph::buffer::error &e) {
      logger().debug(
	"find_replay_segments: segment {} unable to decode "
	"header, skipping",
	segment);
      return crimson::ct_error::enodata::make();
    }
    logger().debug(
      "find_replay_segments: segment {} header {}",
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
	  static_cast<segment_off_t>(block_size)}
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
	    crimson::ct_error::assert_all{}
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
	    replay_from = paddr_t{from->first, (segment_off_t)block_size};
	  }
	  auto ret = replay_segments_t(segments.end() - from);
	  std::transform(
	    from, segments.end(), ret.begin(),
	    [this](const auto &p) {
	      auto ret = journal_seq_t{
		p.second.journal_segment_seq,
		paddr_t{p.first, (segment_off_t)block_size}};
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

Journal::read_record_metadata_ret Journal::read_record_metadata(
  paddr_t start)
{
  if (start.offset + block_size > (int64_t)segment_manager.get_segment_size()) {
    return read_record_metadata_ret(
      read_record_metadata_ertr::ready_future_marker{},
      std::nullopt);
  }
  return segment_manager.read(start, block_size
  ).safe_then(
    [this, start](bufferptr bptr) mutable
    -> read_record_metadata_ret {
      logger().debug("read_record_metadata: reading {}", start);
      bufferlist bl;
      bl.append(bptr);
      auto bp = bl.cbegin();
      record_header_t header;
      try {
	decode(header, bp);
      } catch (ceph::buffer::error &e) {
	return read_record_metadata_ret(
	  read_record_metadata_ertr::ready_future_marker{},
	  std::nullopt);
      }
      if (header.mdlength > block_size) {
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
	      return read_record_metadata_ret(
		read_record_metadata_ertr::ready_future_marker{},
		std::make_pair(std::move(header), std::move(bl)));
	    });
      } else {
	  return read_record_metadata_ret(
	    read_record_metadata_ertr::ready_future_marker{},
	    std::make_pair(std::move(header), std::move(bl))
	  );
      }
    });
}

std::optional<std::vector<delta_info_t>> Journal::try_decode_deltas(
  record_header_t header,
  bufferlist &bl)
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
  bufferlist &bl)
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
    delta_scan_handler_t(
      [=, &handler](auto addr, auto base, const auto &delta) {
	/* The journal may validly contain deltas for extents in since released
	 * segments.  We can detect those cases by whether the segment in question
	 * currently has a sequence number > the current journal segment seq.
	 * We can safely skip these deltas because the extent must already have
	 * been rewritten.
	 *
	 * Note, this comparison exploits the fact that SEGMENT_SEQ_NULL is
	 * a large number.
	 */
	if (delta.paddr != P_ADDR_NULL &&
	    segment_provider->get_seq(delta.paddr.segment) > seq.segment_seq) {
	  return replay_ertr::now();
	} else {
	  return handler(
	    journal_seq_t{seq.segment_seq, addr},
	    base,
	    delta);
	}
      }),
    [=](auto &dhandler) {
      return scan_segment(
	seq.offset,
	segment_manager.get_segment_size(),
	header.segment_nonce,
	&dhandler,
	nullptr).safe_then([](auto){});
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
  paddr_t addr,
  extent_len_t bytes_to_read)
{
  // Caller doesn't know addr of first record, so addr.offset == 0 is special
  if (addr.offset == 0) addr.offset = block_size;

  return read_segment_header(addr.segment
  ).handle_error(
    scan_extents_ertr::pass_further{},
    crimson::ct_error::assert_all{}
  ).safe_then([=](auto header) {
    return seastar::do_with(
      scan_extents_ret_bare(),
      [=](auto &ret) {
	return seastar::do_with(
	  extent_handler_t([&ret](auto addr, const auto &info) mutable {
	    ret.second.push_back(std::make_pair(addr, info));
	    return scan_extents_ertr::now();
	  }),
	  [=, &ret](auto &handler) mutable {
	    return scan_segment(
	      addr,
	      bytes_to_read,
	      header.segment_nonce,
	      nullptr,
	      &handler).safe_then([&ret](auto next) mutable {
		ret.first = next;
		return std::move(ret);
	      });
	  });
      });
  });
}

Journal::scan_segment_ret Journal::scan_segment(
  paddr_t addr,
  extent_len_t bytes_to_read,
  segment_nonce_t nonce,
  delta_scan_handler_t *delta_handler,
  extent_handler_t *extent_info_handler)
{
  logger().debug("Journal::scan_segment: starting at {}", addr);
  return seastar::do_with(
    addr,
    [=](paddr_t &current) {
      return crimson::do_until(
	[=, &current]() -> scan_segment_ertr::future<bool> {
	  return read_record_metadata(current).safe_then
	    ([=, &current](auto p)
	     -> scan_segment_ertr::future<bool> {
	      if (!p.has_value()) {
		current = P_ADDR_NULL;
		return scan_segment_ertr::make_ready_future<bool>(true);
	      }

	      auto &[header, bl] = *p;

	      if (header.segment_nonce != nonce) {
		logger().debug(
		  "Journal::scan_segment: record offset {} nonce mismatch {} vs {}",
		  current,
		  nonce,
		  header.segment_nonce);
		return scan_segment_ertr::make_ready_future<bool>(true);
	      }

	      logger().debug(
		"Journal::scan_segment: next record offset {} mdlength {} dlength {}",
		current,
		header.mdlength,
		header.dlength);

	      auto record_start = current;
	      current.offset += header.mdlength + header.dlength;

	      return seastar::do_with(
		header,
		bl,
		[=, &current](auto &header, auto &bl) {
		  return scan_segment_ertr::now(
		  ).safe_then(
		    [=, &header, &bl]()
		    -> scan_segment_ertr::future<> {
		    if (!delta_handler) {
		      return scan_segment_ertr::now();
		    }

		    auto deltas = try_decode_deltas(
		      header,
		      bl);
		    if (!deltas) {
		      logger().error(
			"Journal::scan_segment unable to decode deltas for record {}",
			addr);
		      return crimson::ct_error::input_output_error::make();
		    }

		    return seastar::do_with(
		      std::move(*deltas),
		      [=](auto &deltas) {
			return crimson::do_for_each(
			  deltas,
			  [=](auto &info) {
			    return (*delta_handler)(
			      record_start,
			      record_start.add_offset(header.mdlength),
			      info);
			  });
		      });
		  }).safe_then(
		    [=, &header, &bl]() -> scan_segment_ertr::future<> {
		    if (!extent_info_handler) {
		      return scan_segment_ertr::now();
		    }

		    auto infos = try_decode_extent_infos(
		      header,
		      bl);
		    if (!infos) {
		      logger().error(
			"Journal::scan_segment unable to decode extent infos for record {}",
			addr);
		      return crimson::ct_error::input_output_error::make();
		    }

		    return seastar::do_with(
		      segment_off_t(0),
		      std::move(*infos),
		      [=](auto &pos, auto &deltas) {
			return crimson::do_for_each(
			  deltas,
			  [=, &pos](auto &info) {
			    auto addr = record_start
			      .add_offset(header.mdlength)
			      .add_offset(pos);
			    pos += info.len;
			    return (*extent_info_handler)(
			      addr,
			      info);
			  });
		      });
		    return scan_segment_ertr::now();
		  });
		}).safe_then([=, &current] {
		  if ((segment_off_t)(addr.offset + bytes_to_read)
		      <= current.offset) {
		    return scan_segment_ertr::make_ready_future<bool>(true);
		  } else {
		    return scan_segment_ertr::make_ready_future<bool>(false);
		  }
		});
	    });
	}).safe_then([&current] {
	  return scan_segment_ertr::make_ready_future<paddr_t>(current);
	});
    });
}

}
