// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "segment_allocator.h"

#include <fmt/format.h>
#include <fmt/os.h>

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/async_cleaner.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore::journal {

SegmentAllocator::SegmentAllocator(
  JournalTrimmer *trimmer,
  data_category_t category,
  rewrite_gen_t gen,
  SegmentProvider &sp,
  SegmentSeqAllocator &ssa)
  : print_name{fmt::format("{}_G{}", category, gen)},
    type{trimmer == nullptr ?
         segment_type_t::OOL :
         segment_type_t::JOURNAL},
    category{category},
    gen{gen},
    segment_provider{sp},
    sm_group{*sp.get_segment_manager_group()},
    segment_seq_allocator(ssa),
    trimmer{trimmer}
{
  reset();
}

segment_nonce_t calc_new_nonce(
  segment_type_t type,
  uint32_t crc,
  unsigned char const *data,
  unsigned length)
{
  crc &= std::numeric_limits<uint32_t>::max() >> 1;
  crc |= static_cast<uint32_t>(type) << 31;
  return ceph_crc32c(crc, data, length);
}

SegmentAllocator::open_ret
SegmentAllocator::do_open(bool is_mkfs)
{
  LOG_PREFIX(SegmentAllocator::do_open);
  ceph_assert(!current_segment);
  segment_seq_t new_segment_seq =
    segment_seq_allocator.get_and_inc_next_segment_seq();
  auto meta = sm_group.get_meta();
  current_segment_nonce = calc_new_nonce(
    type,
    new_segment_seq,
    reinterpret_cast<const unsigned char *>(meta.seastore_id.bytes()),
    sizeof(meta.seastore_id.uuid));
  auto new_segment_id = segment_provider.allocate_segment(
      new_segment_seq, type, category, gen);
  ceph_assert(new_segment_id != NULL_SEG_ID);
  return sm_group.open(new_segment_id
  ).handle_error(
    open_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SegmentAllocator::do_open open"
    }
  ).safe_then([this, is_mkfs, FNAME, new_segment_seq](auto sref) {
    // initialize new segment
    segment_id_t segment_id = sref->get_segment_id();
    journal_seq_t dirty_tail;
    journal_seq_t alloc_tail;
    if (type == segment_type_t::JOURNAL) {
      dirty_tail = trimmer->get_dirty_tail();
      alloc_tail = trimmer->get_alloc_tail();
      if (is_mkfs) {
        ceph_assert(dirty_tail == JOURNAL_SEQ_NULL);
        ceph_assert(alloc_tail == JOURNAL_SEQ_NULL);
        auto mkfs_seq = journal_seq_t{
          new_segment_seq,
          paddr_t::make_seg_paddr(segment_id, 0)
        };
        dirty_tail = mkfs_seq;
        alloc_tail = mkfs_seq;
      } else {
        ceph_assert(dirty_tail != JOURNAL_SEQ_NULL);
        ceph_assert(alloc_tail != JOURNAL_SEQ_NULL);
      }
    } else { // OOL
      ceph_assert(!is_mkfs);
      dirty_tail = JOURNAL_SEQ_NULL;
      alloc_tail = JOURNAL_SEQ_NULL;
    }
    auto header = segment_header_t{
      new_segment_seq,
      segment_id,
      dirty_tail,
      alloc_tail,
      current_segment_nonce,
      type,
      category,
      gen};
    INFO("{} writing header {}", print_name, header);

    auto header_length = get_block_size();
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
    segment_provider.update_segment_avail_bytes(
        type, new_journal_seq.offset);
    return sref->write(0, std::move(bl)
    ).handle_error(
      open_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error in SegmentAllocator::do_open write"
      }
    ).safe_then([this,
                 FNAME,
                 new_journal_seq,
                 sref=std::move(sref)]() mutable {
      ceph_assert(!current_segment);
      current_segment = std::move(sref);
      DEBUG("{} rolled new segment id={}",
            print_name, current_segment->get_segment_id());
      ceph_assert(new_journal_seq.segment_seq ==
        segment_provider.get_seg_info(current_segment->get_segment_id()).seq);
      return new_journal_seq;
    });
  });
}

SegmentAllocator::open_ret
SegmentAllocator::open(bool is_mkfs)
{
  LOG_PREFIX(SegmentAllocator::open);
  auto& device_ids = sm_group.get_device_ids();
  ceph_assert(device_ids.size());
  std::ostringstream oss;
  for (auto& device_id : device_ids) {
    oss << device_id_printer_t{device_id} << "_";
  }
  oss << fmt::format("{}_G{}", category, gen);
  print_name = oss.str();

  DEBUG("{}", print_name);
  return do_open(is_mkfs);
}

SegmentAllocator::roll_ertr::future<>
SegmentAllocator::roll()
{
  ceph_assert(can_write());
  return close_segment().safe_then([this] {
    return do_open(false).discard_result();
  });
}

SegmentAllocator::write_ret
SegmentAllocator::write(ceph::bufferlist&& to_write)
{
  LOG_PREFIX(SegmentAllocator::write);
  assert(can_write());
  auto write_length = to_write.length();
  auto write_start_offset = written_to;
  auto write_start_seq = journal_seq_t{
    segment_provider.get_seg_info(current_segment->get_segment_id()).seq,
    paddr_t::make_seg_paddr(
      current_segment->get_segment_id(), write_start_offset)
  };
  TRACE("{} {}~{}", print_name, write_start_seq, write_length);
  assert(write_length > 0);
  assert((write_length % get_block_size()) == 0);
  assert(!needs_roll(write_length));

  auto write_result = write_result_t{
    write_start_seq,
    write_length
  };
  written_to += write_length;
  segment_provider.update_segment_avail_bytes(
    type,
    paddr_t::make_seg_paddr(
      current_segment->get_segment_id(), written_to)
  );
  return current_segment->write(
    write_start_offset, std::move(to_write)
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
      DEBUG("{} close current segment", print_name);
      return close_segment();
    } else {
      INFO("{} no current segment", print_name);
      return close_segment_ertr::now();
    }
  }().finally([this] {
    reset();
  });
}

SegmentAllocator::close_segment_ertr::future<>
SegmentAllocator::close_segment()
{
  LOG_PREFIX(SegmentAllocator::close_segment);
  assert(can_write());
  // Note: make sure no one can access the current segment once closing
  auto seg_to_close = std::move(current_segment);
  auto close_segment_id = seg_to_close->get_segment_id();
  auto close_seg_info = segment_provider.get_seg_info(close_segment_id);
  ceph_assert((close_seg_info.modify_time == NULL_TIME &&
               close_seg_info.num_extents == 0) ||
              (close_seg_info.modify_time != NULL_TIME &&
               close_seg_info.num_extents != 0));
  auto tail = segment_tail_t{
    close_seg_info.seq,
    close_segment_id,
    current_segment_nonce,
    type,
    timepoint_to_mod(close_seg_info.modify_time),
    close_seg_info.num_extents};
  ceph::bufferlist bl;
  encode(tail, bl);
  INFO("{} close segment {}, written_to={}",
       print_name,
       tail,
       written_to);

  bufferptr bp(ceph::buffer::create_page_aligned(get_block_size()));
  bp.zero();
  auto iter = bl.cbegin();
  iter.copy(bl.length(), bp.c_str());
  bl.clear();
  bl.append(bp);

  assert(bl.length() == sm_group.get_rounded_tail_length());

  auto p_seg_to_close = seg_to_close.get();
  return p_seg_to_close->advance_wp(
    sm_group.get_segment_size() - sm_group.get_rounded_tail_length()
  ).safe_then([this, FNAME, bl=std::move(bl), p_seg_to_close]() mutable {
    DEBUG("Writing tail info to segment {}", p_seg_to_close->get_segment_id());
    return p_seg_to_close->write(
      sm_group.get_segment_size() - sm_group.get_rounded_tail_length(),
      std::move(bl));
  }).safe_then([p_seg_to_close] {
    return p_seg_to_close->close();
  }).safe_then([this, seg_to_close=std::move(seg_to_close)] {
    segment_provider.close_segment(seg_to_close->get_segment_id());
  }).handle_error(
    close_segment_ertr::pass_further{},
    crimson::ct_error::assert_all {
    "Invalid error in SegmentAllocator::close_segment"
  });

}

}
