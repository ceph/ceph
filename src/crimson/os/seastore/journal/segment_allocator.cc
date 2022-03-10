// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "segment_allocator.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/segment_cleaner.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore::journal {

static segment_nonce_t generate_nonce(
  segment_seq_t seq,
  const seastore_meta_t &meta)
{
  return ceph_crc32c(
    seq,
    reinterpret_cast<const unsigned char *>(meta.seastore_id.bytes()),
    sizeof(meta.seastore_id.uuid));
}

SegmentAllocator::SegmentAllocator(
  segment_type_t type,
  SegmentProvider &sp,
  SegmentManager &sm)
  : type{type},
    segment_provider{sp},
    segment_manager{sm}
{
  ceph_assert(type != segment_type_t::NULL_SEG);
  reset();
}

void SegmentAllocator::set_next_segment_seq(segment_seq_t seq)
{
  LOG_PREFIX(SegmentAllocator::set_next_segment_seq);
  INFO("{} {} next_segment_seq={}",
       type, get_device_id(), segment_seq_printer_t{seq});
  assert(type == segment_seq_to_type(seq));
  next_segment_seq = seq;
}

SegmentAllocator::open_ertr::future<journal_seq_t>
SegmentAllocator::open()
{
  LOG_PREFIX(SegmentAllocator::open);
  ceph_assert(!current_segment);
  segment_seq_t new_segment_seq;
  if (type == segment_type_t::JOURNAL) {
    new_segment_seq = next_segment_seq++;
  } else { // OOL
    new_segment_seq = next_segment_seq;
  }
  assert(new_segment_seq == get_current_segment_seq());
  ceph_assert(segment_seq_to_type(new_segment_seq) == type);
  auto new_segment_id = segment_provider.get_segment(
      get_device_id(), new_segment_seq);
  return segment_manager.open(new_segment_id
  ).handle_error(
    open_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SegmentAllocator::open open"
    }
  ).safe_then([this, FNAME, new_segment_seq](auto sref) {
    // initialize new segment
    journal_seq_t new_journal_tail;
    if (type == segment_type_t::JOURNAL) {
      new_journal_tail = segment_provider.get_journal_tail_target();
      current_segment_nonce = generate_nonce(
          new_segment_seq, segment_manager.get_meta());
    } else { // OOL
      new_journal_tail = NO_DELTAS;
      assert(current_segment_nonce == 0);
    }
    segment_id_t segment_id = sref->get_segment_id();
    auto header = segment_header_t{
      new_segment_seq,
      segment_id,
      new_journal_tail,
      current_segment_nonce};
    INFO("{} {} writing header to new segment ... -- {}",
         type, get_device_id(), header);

    auto header_length = segment_manager.get_block_size();
    bufferlist bl;
    encode(header, bl);
    bufferptr bp(ceph::buffer::create_page_aligned(header_length));
    bp.zero();
    auto iter = bl.cbegin();
    iter.copy(bl.length(), bp.c_str());
    bl.clear();
    bl.append(bp);

    ceph_assert(sref->get_write_ptr() == 0);
    assert((unsigned)header_length == bl.length());
    written_to = header_length;
    auto new_journal_seq = journal_seq_t{
      new_segment_seq,
      paddr_t::make_seg_paddr(segment_id, written_to)};
    if (type == segment_type_t::OOL) {
      // FIXME: improve the special handling for OOL
      segment_provider.update_segment_avail_bytes(
          new_journal_seq.offset);
    }
    return sref->write(0, bl
    ).handle_error(
      open_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error in SegmentAllocator::open write"
      }
    ).safe_then([this,
                 FNAME,
                 new_journal_seq,
                 new_journal_tail,
                 sref=std::move(sref)]() mutable {
      ceph_assert(!current_segment);
      current_segment = std::move(sref);
      if (type == segment_type_t::JOURNAL) {
        segment_provider.update_journal_tail_committed(new_journal_tail);
      }
      DEBUG("{} {} rolled new segment id={}",
            type, get_device_id(), current_segment->get_segment_id());
      ceph_assert(new_journal_seq.segment_seq == get_current_segment_seq());
      return new_journal_seq;
    });
  });
}

SegmentAllocator::roll_ertr::future<>
SegmentAllocator::roll()
{
  ceph_assert(can_write());
  return close_segment(true).safe_then([this] {
    return open().discard_result();
  });
}

SegmentAllocator::write_ret
SegmentAllocator::write(ceph::bufferlist to_write)
{
  LOG_PREFIX(SegmentAllocator::write);
  assert(can_write());
  auto write_length = to_write.length();
  auto write_start_offset = written_to;
  auto write_start_seq = journal_seq_t{
    get_current_segment_seq(),
    paddr_t::make_seg_paddr(
      current_segment->get_segment_id(), write_start_offset)
  };
  TRACE("{} {} {}~{}", type, get_device_id(), write_start_seq, write_length);
  assert(write_length > 0);
  assert((write_length % segment_manager.get_block_size()) == 0);
  assert(!needs_roll(write_length));

  auto write_result = write_result_t{
    write_start_seq,
    static_cast<seastore_off_t>(write_length)
  };
  written_to += write_length;
  if (type == segment_type_t::OOL) {
    // FIXME: improve the special handling for OOL
    segment_provider.update_segment_avail_bytes(
      paddr_t::make_seg_paddr(
        current_segment->get_segment_id(), written_to)
    );
  }
  return current_segment->write(
    write_start_offset, to_write
  ).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SegmentAllocator::write"
    }
  ).safe_then([write_result, cs=current_segment] {
    return write_result;
  });
}

SegmentAllocator::close_ertr::future<>
SegmentAllocator::close()
{
  return [this] {
    LOG_PREFIX(SegmentAllocator::close);
    if (current_segment) {
      return close_segment(false);
    } else {
      INFO("{} {} no current segment", type, get_device_id());
      return close_segment_ertr::now();
    }
  }().finally([this] {
    reset();
  });
}

SegmentAllocator::close_segment_ertr::future<>
SegmentAllocator::close_segment(bool is_rolling)
{
  LOG_PREFIX(SegmentAllocator::close_segment);
  assert(can_write());
  auto close_segment_id = current_segment->get_segment_id();
  INFO("{} {} close segment id={}, seq={}, written_to={}, nonce={}",
       type, get_device_id(),
       close_segment_id,
       segment_seq_printer_t{get_current_segment_seq()},
       written_to,
       current_segment_nonce);
  if (is_rolling) {
    segment_provider.close_segment(close_segment_id);
  }
  segment_seq_t cur_segment_seq;
  if (type == segment_type_t::JOURNAL) {
    cur_segment_seq = next_segment_seq - 1;
  } else { // OOL
    cur_segment_seq = next_segment_seq;
  }
  journal_seq_t cur_journal_tail;
  if (type == segment_type_t::JOURNAL) {
    cur_journal_tail = segment_provider.get_journal_tail_target();
  } else { // OOL
    cur_journal_tail = NO_DELTAS;
  }
  auto tail = segment_tail_t{
    cur_segment_seq,
    close_segment_id,
    cur_journal_tail,
    current_segment_nonce,
    segment_provider.get_last_modified(
      close_segment_id).time_since_epoch().count(),
    segment_provider.get_last_rewritten(
      close_segment_id).time_since_epoch().count()};
  ceph::bufferlist bl;
  encode(tail, bl);

  bufferptr bp(
    ceph::buffer::create_page_aligned(
      segment_manager.get_block_size()));
  bp.zero();
  auto iter = bl.cbegin();
  iter.copy(bl.length(), bp.c_str());
  bl.clear();
  bl.append(bp);

  assert(bl.length() ==
    (size_t)segment_manager.get_rounded_tail_length());
  return current_segment->write(
    segment_manager.get_segment_size()
      - segment_manager.get_rounded_tail_length(),
    bl).safe_then([this] {
    return current_segment->close();
  }).safe_then([this] {
    current_segment.reset();
  }).handle_error(
    close_segment_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SegmentAllocator::close_segment"
    }
  );
}

}
