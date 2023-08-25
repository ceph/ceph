// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/segment_manager_group.h"

#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore {

SegmentManagerGroup::read_segment_tail_ret
SegmentManagerGroup::read_segment_tail(segment_id_t segment)
{
  assert(has_device(segment.device_id()));
  auto& segment_manager = *segment_managers[segment.device_id()];
  return segment_manager.read(
    paddr_t::make_seg_paddr(
      segment,
      segment_manager.get_segment_size() - get_rounded_tail_length()),
    get_rounded_tail_length()
  ).handle_error(
    read_segment_header_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SegmentManagerGroup::read_segment_tail"
    }
  ).safe_then([=, &segment_manager](bufferptr bptr) -> read_segment_tail_ret {
    LOG_PREFIX(SegmentManagerGroup::read_segment_tail);
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
            segment, e.what());
      return crimson::ct_error::enodata::make();
    }
    DEBUG("segment {} tail {}", segment, tail);
    return read_segment_tail_ret(
      read_segment_tail_ertr::ready_future_marker{},
      tail);
  });
}

SegmentManagerGroup::read_segment_header_ret
SegmentManagerGroup::read_segment_header(segment_id_t segment)
{
  assert(has_device(segment.device_id()));
  auto& segment_manager = *segment_managers[segment.device_id()];
  return segment_manager.read(
    paddr_t::make_seg_paddr(segment, 0),
    get_rounded_header_length()
  ).handle_error(
    read_segment_header_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SegmentManagerGroup::read_segment_header"
    }
  ).safe_then([=, &segment_manager](bufferptr bptr) -> read_segment_header_ret {
    LOG_PREFIX(SegmentManagerGroup::read_segment_header);
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
            segment, e.what());
      return crimson::ct_error::enodata::make();
    }
    DEBUG("segment {} header {}", segment, header);
    return read_segment_header_ret(
      read_segment_header_ertr::ready_future_marker{},
      header);
  });
}

void SegmentManagerGroup::initialize_cursor(
  scan_valid_records_cursor &cursor)
{
  LOG_PREFIX(SegmentManagerGroup::initialize_cursor);
  assert(has_device(cursor.get_segment_id().device_id()));
  auto& segment_manager =
    *segment_managers[cursor.get_segment_id().device_id()];
  if (cursor.get_segment_offset() == 0) {
    INFO("start to scan segment {}", cursor.get_segment_id());
    cursor.increment_seq(segment_manager.get_block_size());
  }
  cursor.block_size = segment_manager.get_block_size();
}

SegmentManagerGroup::read_ret
SegmentManagerGroup::read(paddr_t start, size_t len) 
{
  LOG_PREFIX(SegmentManagerGroup::read);
  assert(has_device(start.get_device_id()));
  auto& segment_manager = *segment_managers[start.get_device_id()];
  TRACE("reading data {}~{}", start, len);
  return segment_manager.read(
    start,
    len 
  ).safe_then([](auto bptr) {
    return read_ret(
      read_ertr::ready_future_marker{},
      std::move(bptr)
    );
  });
}

SegmentManagerGroup::find_journal_segment_headers_ret
SegmentManagerGroup::find_journal_segment_headers()
{
  return seastar::do_with(
    get_segment_managers(),
    find_journal_segment_headers_ret_bare{},
    [this](auto &sms, auto& ret) -> find_journal_segment_headers_ret
  {
    return crimson::do_for_each(sms,
      [this, &ret](SegmentManager *sm)
    {
      LOG_PREFIX(SegmentManagerGroup::find_journal_segment_headers);
      auto device_id = sm->get_device_id();
      auto num_segments = sm->get_num_segments();
      DEBUG("processing {} with {} segments",
            device_id_printer_t{device_id}, num_segments);
      return crimson::do_for_each(
        boost::counting_iterator<device_segment_id_t>(0),
        boost::counting_iterator<device_segment_id_t>(num_segments),
        [this, &ret, device_id](device_segment_id_t d_segment_id)
      {
        segment_id_t segment_id{device_id, d_segment_id};
        return read_segment_header(segment_id
        ).safe_then([segment_id, &ret](auto &&header) {
          if (header.get_type() == segment_type_t::JOURNAL) {
            ret.emplace_back(std::make_pair(segment_id, std::move(header)));
          }
        }).handle_error(
          crimson::ct_error::enoent::handle([](auto) {
            return find_journal_segment_headers_ertr::now();
          }),
          crimson::ct_error::enodata::handle([](auto) {
            return find_journal_segment_headers_ertr::now();
          }),
          crimson::ct_error::input_output_error::pass_further{}
        );
      });
    }).safe_then([&ret]() mutable {
      return find_journal_segment_headers_ret{
        find_journal_segment_headers_ertr::ready_future_marker{},
        std::move(ret)};
    });
  });
}

} // namespace crimson::os::seastore
