// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

Journal::Journal(SegmentManager &segment_manager)
  : block_size(segment_manager.get_block_size()),
    max_record_length(
      segment_manager.get_segment_size() -
      p2align(ceph::encoded_sizeof_bounded<segment_header_t>(),
	      size_t(block_size))),
    segment_manager(segment_manager) {}


Journal::initialize_segment_ertr::future<> Journal::initialize_segment(
  Segment &segment)
{
  logger().debug("initialize_segment {}", segment.get_segment_id());
  // write out header
  ceph_assert(segment.get_write_ptr() == 0);
  bufferlist bl;
  auto header = segment_header_t{
    current_journal_segment_seq++,
    segment.get_segment_id(),
    current_replay_point};
  ::encode(header, bl);

  written_to = segment_manager.get_block_size();
  return segment.write(0, bl).handle_error(
    init_ertr::pass_further{},
    crimson::ct_error::assert_all{ "TODO" });
}

ceph::bufferlist Journal::encode_record(
  record_size_t rsize,
  record_t &&record)
{
  bufferlist metadatabl;
  record_header_t header{
    rsize.mdlength,
    rsize.dlength,
    current_journal_seq,
    0 /* checksum, TODO */,
    record.deltas.size(),
    record.extents.size()
  };
  ::encode(header, metadatabl);
  for (const auto &i: record.deltas) {
    ::encode(i, metadatabl);
  }
  bufferlist databl;
  for (auto &i: record.extents) {
    databl.claim_append(i.bl);
  }
  if (metadatabl.length() % block_size != 0) {
    metadatabl.append(
      ceph::bufferptr(
	block_size - (metadatabl.length() % block_size)));
  }

  ceph_assert(metadatabl.length() == rsize.mdlength);
  ceph_assert(databl.length() == rsize.dlength);
  metadatabl.claim_append(databl);
  ceph_assert(metadatabl.length() == (rsize.mdlength + rsize.dlength));
  return metadatabl;
}

Journal::write_record_ertr::future<> Journal::write_record(
  record_size_t rsize,
  record_t &&record)
{
  ceph::bufferlist to_write = encode_record(
    rsize, std::move(record));
  auto target = written_to;
  written_to += p2roundup(to_write.length(), (unsigned)block_size);
  logger().debug(
    "write_record, mdlength {}, dlength {}",
    rsize.mdlength,
    rsize.dlength);
  return current_journal_segment->write(target, to_write).handle_error(
    write_record_ertr::pass_further{},
    crimson::ct_error::assert_all{ "TODO" });
}

Journal::record_size_t Journal::get_encoded_record_length(
  const record_t &record) const {
  extent_len_t metadata =
    (extent_len_t)ceph::encoded_sizeof_bounded<record_header_t>();
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

paddr_t Journal::next_record_addr() const
{
  paddr_t ret{current_journal_segment->get_segment_id(), written_to};
  logger().debug("next_record_addr: {}", ret);
  return ret;
}

Journal::roll_journal_segment_ertr::future<>
Journal::roll_journal_segment()
{
  auto old_segment_id = current_journal_segment ?
    current_journal_segment->get_segment_id() :
    NULL_SEG_ID;

  return (current_journal_segment ?
	  current_journal_segment->close() :
	  Segment::close_ertr::now()).safe_then(
    [this, old_segment_id] {
      // TODO: pretty sure this needs to be atomic in some sense with
      // making use of the new segment, maybe this bit needs to take
      // the first transaction of the new segment?  Or the segment
      // header should include deltas?
      if (old_segment_id != NULL_SEG_ID) {
	segment_provider->put_segment(old_segment_id);
      }
      return segment_provider->get_segment();
    }).safe_then([this](auto segment) {
      return segment_manager.open(segment);
    }).safe_then([this](auto sref) {
      current_journal_segment = sref;
      written_to = 0;
      return initialize_segment(*current_journal_segment);
    }).handle_error(
      roll_journal_segment_ertr::pass_further{},
      crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); })
    );
}

Journal::init_ertr::future<> Journal::open_for_write()
{
  return roll_journal_segment();
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
	  return segment_manager.read(paddr_t{i, 0}, block_size
	  ).safe_then([this, &segments, i](bufferptr bptr) mutable {
	    logger().debug("segment {} bptr size {}", i, bptr.length());
	    segment_header_t header;
	    bufferlist bl;
	    bl.push_back(bptr);

	    logger().debug(
	      "find_replay_segments: segment {} block crc {}",
	      i,
	      bl.begin().crc32c(block_size, 0));

	    auto bp = bl.cbegin();
	    try {
	      ::decode(header, bp);
	    } catch (ceph::buffer::error &e) {
	      logger().debug(
		"find_replay_segments: segment {} unable to decode "
		"header, skipping",
		i);
	      return find_replay_segments_ertr::now();
	    }
	    segments.emplace_back(i, std::move(header));
	    return find_replay_segments_ertr::now();
	  }).handle_error(
	    find_replay_segments_ertr::pass_further{},
	    crimson::ct_error::discard_all{}
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

	  auto replay_from = segments.rbegin()->second.journal_replay_lb;
	  auto from = segments.begin();
	  if (replay_from != P_ADDR_NULL) {
	    from = std::find_if(
	      segments.begin(),
	      segments.end(),
	      [&replay_from](const auto &seg) -> bool {
		return seg.first == replay_from.segment;
	      });
	  } else {
	    replay_from = paddr_t{from->first, (segment_off_t)block_size};
	  }
	  auto ret = std::vector<paddr_t>(segments.end() - from);
	  std::transform(
	    from, segments.end(), ret.begin(),
	    [this](const auto &p) {
	      return paddr_t{p.first, (segment_off_t)block_size};
	    });
	  ret[0] = replay_from;
	  return find_replay_segments_fut(
	    find_replay_segments_ertr::ready_future_marker{},
	    std::move(ret));
	});
    });
}

Journal::read_record_metadata_ret Journal::read_record_metadata(
  paddr_t start)
{
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
	::decode(header, bp);
      } catch (ceph::buffer::error &e) {
	return read_record_metadata_ret(
	  read_record_metadata_ertr::ready_future_marker{},
	  std::nullopt);
      }
      if (header.mdlength > block_size) {
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
  std::vector<delta_info_t> deltas(header.deltas);
  for (auto &&i : deltas) {
    try {
      ::decode(i, bliter);
    } catch (ceph::buffer::error &e) {
      return std::nullopt;
    }
  }
  return deltas;
}

Journal::replay_ertr::future<>
Journal::replay_segment(
  paddr_t start,
  delta_handler_t &delta_handler)
{
  logger().debug("replay_segment: starting at {}", start);
  return seastar::do_with(
    std::move(start),
    [this, &delta_handler](auto &current) {
      return crimson::do_until(
	[this, &current, &delta_handler]() -> replay_ertr::future<bool> {
	  return read_record_metadata(current).safe_then
	    ([this, &current, &delta_handler](auto p)
	     -> replay_ertr::future<bool> {
	      if (!p.has_value()) {
		return replay_ertr::make_ready_future<bool>(true);
	      }

	      auto &[header, bl] = *p;

	      logger().debug(
		"replay_segment: next record offset {} mdlength {} dlength {}",
		current,
		header.mdlength,
		header.dlength);

	      auto record_start = current;
	      current.offset += header.mdlength + header.dlength;

	      auto deltas = try_decode_deltas(
		header,
		bl);
	      if (!deltas) {
		return replay_ertr::make_ready_future<bool>(true);
	      }

	      return seastar::do_with(
		std::move(*deltas),
		[this, &delta_handler, record_start](auto &deltas) {
		  return crimson::do_for_each(
		    deltas,
		    [this, &delta_handler, record_start](auto &info) {
		      return delta_handler(
			record_start.add_offset(block_size),
			info);
		    });
		}).safe_then([] {
		  return replay_ertr::make_ready_future<bool>(false);
		});
	    });
	});
    });
}

Journal::replay_ret Journal::replay(delta_handler_t &&delta_handler)
{
  return seastar::do_with(
    std::move(delta_handler), std::vector<paddr_t>(),
    [this](auto&& handler, auto&& segments) mutable -> replay_ret {
      return find_replay_segments().safe_then(
        [this, &handler, &segments](auto replay_segs) {
          logger().debug("replay: found {} segments", replay_segs.size());
          segments = std::move(replay_segs);
          return crimson::do_for_each(segments, [this, &handler](auto i) {
            return replay_segment(i, handler);
          });
        });
    });
}

}
