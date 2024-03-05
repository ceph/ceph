// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/chrono.h>
#include <seastar/core/metrics.hh>

#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/transaction_manager.h"

SET_SUBSYS(seastore_cleaner);

namespace {

enum class gc_formula_t {
  GREEDY,
  BENEFIT,
  COST_BENEFIT,
};
constexpr auto gc_formula = gc_formula_t::COST_BENEFIT;

}

namespace crimson::os::seastore {

void segment_info_t::set_open(
    segment_seq_t _seq, segment_type_t _type,
    data_category_t _category, rewrite_gen_t _generation)
{
  ceph_assert(_seq != NULL_SEG_SEQ);
  ceph_assert(_type != segment_type_t::NULL_SEG);
  ceph_assert(_category != data_category_t::NUM);
  ceph_assert(is_rewrite_generation(_generation));
  state = Segment::segment_state_t::OPEN;
  seq = _seq;
  type = _type;
  category = _category;
  generation = _generation;
  written_to = 0;
}

void segment_info_t::set_empty()
{
  state = Segment::segment_state_t::EMPTY;
  seq = NULL_SEG_SEQ;
  type = segment_type_t::NULL_SEG;
  category = data_category_t::NUM;
  generation = NULL_GENERATION;
  modify_time = NULL_TIME;
  num_extents = 0;
  written_to = 0;
}

void segment_info_t::set_closed()
{
  state = Segment::segment_state_t::CLOSED;
  // the rest of information is unchanged
}

void segment_info_t::init_closed(
    segment_seq_t _seq, segment_type_t _type,
    data_category_t _category, rewrite_gen_t _generation,
    segment_off_t seg_size)
{
  ceph_assert(_seq != NULL_SEG_SEQ);
  ceph_assert(_type != segment_type_t::NULL_SEG);
  ceph_assert(_category != data_category_t::NUM);
  ceph_assert(is_rewrite_generation(_generation));
  state = Segment::segment_state_t::CLOSED;
  seq = _seq;
  type = _type;
  category = _category;
  generation = _generation;
  written_to = seg_size;
}

std::ostream& operator<<(std::ostream &out, const segment_info_t &info)
{
  out << "seg_info_t("
      << "state=" << info.state
      << ", " << info.id;
  if (info.is_empty()) {
    // pass
  } else { // open or closed
    out << " " << info.type
        << " " << segment_seq_printer_t{info.seq}
        << " " << info.category
        << " " << rewrite_gen_printer_t{info.generation}
        << ", modify_time=" << sea_time_point_printer_t{info.modify_time}
        << ", num_extents=" << info.num_extents
        << ", written_to=" << info.written_to;
  }
  return out << ")";
}

void segments_info_t::reset()
{
  segments.clear();

  segment_size = 0;

  journal_segment_id = NULL_SEG_ID;
  num_in_journal_open = 0;
  num_type_journal = 0;
  num_type_ool = 0;

  num_open = 0;
  num_empty = 0;
  num_closed = 0;

  count_open_journal = 0;
  count_open_ool = 0;
  count_release_journal = 0;
  count_release_ool = 0;
  count_close_journal = 0;
  count_close_ool = 0;

  total_bytes = 0;
  avail_bytes_in_open = 0;

  modify_times.clear();
}

void segments_info_t::add_segment_manager(
    SegmentManager &segment_manager)
{
  LOG_PREFIX(segments_info_t::add_segment_manager);
  device_id_t d_id = segment_manager.get_device_id();
  auto ssize = segment_manager.get_segment_size();
  auto nsegments = segment_manager.get_num_segments();
  auto sm_size = segment_manager.get_available_size();
  INFO("adding segment manager {}, size={}, ssize={}, segments={}",
       device_id_printer_t{d_id}, sm_size, ssize, nsegments);
  ceph_assert(ssize > 0);
  ceph_assert(nsegments > 0);
  ceph_assert(sm_size > 0);

  // also validate if the device is duplicated
  segments.add_device(d_id, nsegments, segment_info_t{});

  // assume all the segment managers share the same settings as follows.
  if (segment_size == 0) {
    ceph_assert(ssize > 0);
    segment_size = ssize;
  } else {
    ceph_assert(segment_size == ssize);
  }

  // NOTE: by default the segments are empty
  num_empty += nsegments;

  total_bytes += sm_size;
}

void segments_info_t::init_closed(
    segment_id_t segment, segment_seq_t seq, segment_type_t type,
    data_category_t category, rewrite_gen_t generation)
{
  LOG_PREFIX(segments_info_t::init_closed);
  auto& segment_info = segments[segment];
  DEBUG("initiating {} {} {} {} {}, {}, "
        "num_segments(empty={}, opened={}, closed={})",
        segment, type, segment_seq_printer_t{seq},
        category, rewrite_gen_printer_t{generation},
        segment_info, num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_empty());
  ceph_assert(num_empty > 0);
  --num_empty;
  ++num_closed;
  if (type == segment_type_t::JOURNAL) {
    // init_closed won't initialize journal_segment_id
    ceph_assert(get_submitted_journal_head() == JOURNAL_SEQ_NULL);
    ++num_type_journal;
  } else {
    ++num_type_ool;
  }
  // do not increment count_close_*;

  if (segment_info.modify_time != NULL_TIME) {
    modify_times.insert(segment_info.modify_time);
  } else {
    ceph_assert(segment_info.num_extents == 0);
  }

  segment_info.init_closed(
      seq, type, category, generation, get_segment_size());
}

void segments_info_t::mark_open(
    segment_id_t segment, segment_seq_t seq, segment_type_t type,
    data_category_t category, rewrite_gen_t generation)
{
  LOG_PREFIX(segments_info_t::mark_open);
  auto& segment_info = segments[segment];
  INFO("opening {} {} {} {} {}, {}, "
       "num_segments(empty={}, opened={}, closed={})",
       segment, type, segment_seq_printer_t{seq},
       category, rewrite_gen_printer_t{generation},
       segment_info, num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_empty());
  ceph_assert(num_empty > 0);
  --num_empty;
  ++num_open;
  if (type == segment_type_t::JOURNAL) {
    if (journal_segment_id != NULL_SEG_ID) {
      auto& last_journal_segment = segments[journal_segment_id];
      ceph_assert(last_journal_segment.is_closed());
      ceph_assert(last_journal_segment.type == segment_type_t::JOURNAL);
      ceph_assert(last_journal_segment.seq + 1 == seq);
    }
    journal_segment_id = segment;

    ++num_in_journal_open;
    ++num_type_journal;
    ++count_open_journal;
  } else {
    ++num_type_ool;
    ++count_open_ool;
  }
  avail_bytes_in_open += get_segment_size();

  segment_info.set_open(seq, type, category, generation);
}

void segments_info_t::mark_empty(
    segment_id_t segment)
{
  LOG_PREFIX(segments_info_t::mark_empty);
  auto& segment_info = segments[segment];
  INFO("releasing {}, {}, num_segments(empty={}, opened={}, closed={})",
       segment, segment_info,
       num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_closed());
  auto type = segment_info.type;
  assert(type != segment_type_t::NULL_SEG);
  ceph_assert(num_closed > 0);
  --num_closed;
  ++num_empty;
  if (type == segment_type_t::JOURNAL) {
    ceph_assert(num_type_journal > 0);
    --num_type_journal;
    ++count_release_journal;
  } else {
    ceph_assert(num_type_ool > 0);
    --num_type_ool;
    ++count_release_ool;
  }

  if (segment_info.modify_time != NULL_TIME) {
    auto to_erase = modify_times.find(segment_info.modify_time);
    ceph_assert(to_erase != modify_times.end());
    modify_times.erase(to_erase);
  } else {
    ceph_assert(segment_info.num_extents == 0);
  }

  segment_info.set_empty();
}

void segments_info_t::mark_closed(
    segment_id_t segment)
{
  LOG_PREFIX(segments_info_t::mark_closed);
  auto& segment_info = segments[segment];
  INFO("closing {}, {}, num_segments(empty={}, opened={}, closed={})",
       segment, segment_info,
       num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_open());
  ceph_assert(num_open > 0);
  --num_open;
  ++num_closed;
  if (segment_info.type == segment_type_t::JOURNAL) {
    ceph_assert(num_in_journal_open > 0);
    --num_in_journal_open;
    ++count_close_journal;
  } else {
    ++count_close_ool;
  }
  ceph_assert(get_segment_size() >= segment_info.written_to);
  auto seg_avail_bytes = get_segment_size() - segment_info.written_to;
  ceph_assert(avail_bytes_in_open >= (std::size_t)seg_avail_bytes);
  avail_bytes_in_open -= seg_avail_bytes;

  if (segment_info.modify_time != NULL_TIME) {
    modify_times.insert(segment_info.modify_time);
  } else {
    ceph_assert(segment_info.num_extents == 0);
  }

  segment_info.set_closed();
}

void segments_info_t::update_written_to(
    segment_type_t type,
    paddr_t offset)
{
  LOG_PREFIX(segments_info_t::update_written_to);
  auto& saddr = offset.as_seg_paddr();
  auto& segment_info = segments[saddr.get_segment_id()];
  if (!segment_info.is_open()) {
    ERROR("segment is not open, not updating, type={}, offset={}, {}",
          type, offset, segment_info);
    ceph_abort();
  }

  auto new_written_to = saddr.get_segment_off();
  ceph_assert(new_written_to <= get_segment_size());
  if (segment_info.written_to > new_written_to) {
    ERROR("written_to should not decrease! type={}, offset={}, {}",
          type, offset, segment_info);
    ceph_abort();
  }

  DEBUG("type={}, offset={}, {}", type, offset, segment_info);
  ceph_assert(type == segment_info.type);
  auto avail_deduction = new_written_to - segment_info.written_to;
  ceph_assert(avail_bytes_in_open >= (std::size_t)avail_deduction);
  avail_bytes_in_open -= avail_deduction;
  segment_info.written_to = new_written_to;
}

std::ostream &operator<<(std::ostream &os, const segments_info_t &infos)
{
  return os << "segments("
            << "empty=" << infos.get_num_empty()
            << ", open=" << infos.get_num_open()
            << ", closed=" << infos.get_num_closed()
            << ", type_journal=" << infos.get_num_type_journal()
            << ", type_ool=" << infos.get_num_type_ool()
            << ", total=" << infos.get_total_bytes() << "B"
            << ", available=" << infos.get_available_bytes() << "B"
            << ", unavailable=" << infos.get_unavailable_bytes() << "B"
            << ", available_ratio=" << infos.get_available_ratio()
            << ", submitted_head=" << infos.get_submitted_journal_head()
            << ", time_bound=" << sea_time_point_printer_t{infos.get_time_bound()}
            << ")";
}

void JournalTrimmerImpl::config_t::validate() const
{
  ceph_assert(max_journal_bytes <= DEVICE_OFF_MAX);
  ceph_assert(max_journal_bytes > target_journal_dirty_bytes);
  ceph_assert(max_journal_bytes > target_journal_alloc_bytes);
  ceph_assert(rewrite_dirty_bytes_per_cycle > 0);
  ceph_assert(rewrite_backref_bytes_per_cycle > 0);
}

JournalTrimmerImpl::config_t
JournalTrimmerImpl::config_t::get_default(
  std::size_t roll_size, journal_type_t type)
{
  assert(roll_size);
  std::size_t target_dirty_bytes = 0;
  std::size_t target_alloc_bytes = 0;
  std::size_t max_journal_bytes = 0;
  if (type == journal_type_t::SEGMENTED) {
    target_dirty_bytes = 12 * roll_size;
    target_alloc_bytes = 2 * roll_size;
    max_journal_bytes = 16 * roll_size;
  } else {
    assert(type == journal_type_t::RANDOM_BLOCK);
    target_dirty_bytes = roll_size / 4;
    target_alloc_bytes = roll_size / 4;
    max_journal_bytes = roll_size / 2;
  }
  return config_t{
    target_dirty_bytes,
    target_alloc_bytes,
    max_journal_bytes,
    1<<17,// rewrite_dirty_bytes_per_cycle
    1<<24 // rewrite_backref_bytes_per_cycle
  };
}

JournalTrimmerImpl::config_t
JournalTrimmerImpl::config_t::get_test(
  std::size_t roll_size, journal_type_t type)
{
  assert(roll_size);
  std::size_t target_dirty_bytes = 0;
  std::size_t target_alloc_bytes = 0;
  std::size_t max_journal_bytes = 0;
  if (type == journal_type_t::SEGMENTED) {
    target_dirty_bytes = 2 * roll_size;
    target_alloc_bytes = 2 * roll_size;
    max_journal_bytes = 4 * roll_size;
  } else {
    assert(type == journal_type_t::RANDOM_BLOCK);
    target_dirty_bytes = roll_size / 36;
    target_alloc_bytes = roll_size / 4;
    max_journal_bytes = roll_size / 2;
  }
  return config_t{
    target_dirty_bytes,
    target_alloc_bytes,
    max_journal_bytes,
    1<<17,// rewrite_dirty_bytes_per_cycle
    1<<24 // rewrite_backref_bytes_per_cycle
  };
}

JournalTrimmerImpl::JournalTrimmerImpl(
  BackrefManager &backref_manager,
  config_t config,
  journal_type_t type,
  device_off_t roll_start,
  device_off_t roll_size)
  : backref_manager(backref_manager),
    config(config),
    journal_type(type),
    roll_start(roll_start),
    roll_size(roll_size),
    reserved_usage(0)
{
  config.validate();
  ceph_assert(roll_start >= 0);
  ceph_assert(roll_size > 0);
  register_metrics();
}

void JournalTrimmerImpl::set_journal_head(journal_seq_t head)
{
  LOG_PREFIX(JournalTrimmerImpl::set_journal_head);

  ceph_assert(head != JOURNAL_SEQ_NULL);
  ceph_assert(journal_head == JOURNAL_SEQ_NULL ||
              head >= journal_head);
  ceph_assert(journal_alloc_tail == JOURNAL_SEQ_NULL ||
              head >= journal_alloc_tail);
  ceph_assert(journal_dirty_tail == JOURNAL_SEQ_NULL ||
              head >= journal_dirty_tail);

  std::swap(journal_head, head);
  if (journal_head.segment_seq == head.segment_seq) {
    DEBUG("journal_head {} => {}, {}",
          head, journal_head, stat_printer_t{*this, false});
  } else {
    INFO("journal_head {} => {}, {}",
          head, journal_head, stat_printer_t{*this, false});
  }
  background_callback->maybe_wake_background();
}

void JournalTrimmerImpl::update_journal_tails(
  journal_seq_t dirty_tail,
  journal_seq_t alloc_tail)
{
  LOG_PREFIX(JournalTrimmerImpl::update_journal_tails);

  if (dirty_tail != JOURNAL_SEQ_NULL) {
    ceph_assert(journal_head == JOURNAL_SEQ_NULL ||
                journal_head >= dirty_tail);
    if (journal_dirty_tail != JOURNAL_SEQ_NULL &&
        journal_dirty_tail > dirty_tail) {
      ERROR("journal_dirty_tail {} => {} is backwards!",
            journal_dirty_tail, dirty_tail);
      ceph_abort();
    }
    std::swap(journal_dirty_tail, dirty_tail);
    if (journal_dirty_tail.segment_seq == dirty_tail.segment_seq) {
      DEBUG("journal_dirty_tail {} => {}, {}",
            dirty_tail, journal_dirty_tail, stat_printer_t{*this, false});
    } else {
      INFO("journal_dirty_tail {} => {}, {}",
            dirty_tail, journal_dirty_tail, stat_printer_t{*this, false});
    }
  }

  if (alloc_tail != JOURNAL_SEQ_NULL) {
    ceph_assert(journal_head == JOURNAL_SEQ_NULL ||
                journal_head >= alloc_tail);
    if (journal_alloc_tail != JOURNAL_SEQ_NULL &&
        journal_alloc_tail > alloc_tail) {
      ERROR("journal_alloc_tail {} => {} is backwards!",
            journal_alloc_tail, alloc_tail);
      ceph_abort();
    }
    std::swap(journal_alloc_tail, alloc_tail);
    if (journal_alloc_tail.segment_seq == alloc_tail.segment_seq) {
      DEBUG("journal_alloc_tail {} => {}, {}",
            alloc_tail, journal_alloc_tail, stat_printer_t{*this, false});
    } else {
      INFO("journal_alloc_tail {} => {}, {}",
            alloc_tail, journal_alloc_tail, stat_printer_t{*this, false});
    }
  }

  background_callback->maybe_wake_background();
  background_callback->maybe_wake_blocked_io();
}

journal_seq_t JournalTrimmerImpl::get_tail_limit() const
{
  assert(background_callback->is_ready());
  auto ret = journal_head.add_offset(
      journal_type,
      -static_cast<device_off_t>(config.max_journal_bytes),
      roll_start,
      roll_size);
  return ret;
}

journal_seq_t JournalTrimmerImpl::get_dirty_tail_target() const
{
  assert(background_callback->is_ready());
  auto ret = journal_head.add_offset(
      journal_type,
      -static_cast<device_off_t>(config.target_journal_dirty_bytes),
      roll_start,
      roll_size);
  return ret;
}

journal_seq_t JournalTrimmerImpl::get_alloc_tail_target() const
{
  assert(background_callback->is_ready());
  auto ret = journal_head.add_offset(
      journal_type,
      -static_cast<device_off_t>(config.target_journal_alloc_bytes),
      roll_start,
      roll_size);
  return ret;
}

std::size_t JournalTrimmerImpl::get_dirty_journal_size() const
{
  if (!background_callback->is_ready()) {
    return 0;
  }
  auto ret = journal_head.relative_to(
      journal_type,
      journal_dirty_tail,
      roll_start,
      roll_size);
  ceph_assert(ret >= 0);
  return static_cast<std::size_t>(ret);
}

std::size_t JournalTrimmerImpl::get_alloc_journal_size() const
{
  if (!background_callback->is_ready()) {
    return 0;
  }
  auto ret = journal_head.relative_to(
      journal_type,
      journal_alloc_tail,
      roll_start,
      roll_size);
  ceph_assert(ret >= 0);
  return static_cast<std::size_t>(ret);
}

seastar::future<> JournalTrimmerImpl::trim() {
  return seastar::when_all(
    [this] {
      if (should_trim_alloc()) {
        return trim_alloc(
        ).handle_error(
          crimson::ct_error::assert_all{
            "encountered invalid error in trim_alloc"
          }
        );
      } else {
        return seastar::now();
      }
    },
    [this] {
      if (should_trim_dirty()) {
        return trim_dirty(
        ).handle_error(
          crimson::ct_error::assert_all{
            "encountered invalid error in trim_dirty"
          }
        );
      } else {
        return seastar::now();
      }
    }
  ).discard_result();
}

JournalTrimmerImpl::trim_ertr::future<>
JournalTrimmerImpl::trim_alloc()
{
  LOG_PREFIX(JournalTrimmerImpl::trim_alloc);
  assert(background_callback->is_ready());
  return repeat_eagain([this, FNAME] {
    return extent_callback->with_transaction_intr(
      Transaction::src_t::TRIM_ALLOC,
      "trim_alloc",
      [this, FNAME](auto &t)
    {
      auto target = get_alloc_tail_target();
      DEBUGT("start, alloc_tail={}, target={}",
             t, journal_alloc_tail, target);
      return backref_manager.merge_cached_backrefs(
        t,
        target,
        config.rewrite_backref_bytes_per_cycle
      ).si_then([this, FNAME, &t](auto trim_alloc_to)
        -> ExtentCallbackInterface::submit_transaction_direct_iertr::future<>
      {
        DEBUGT("trim_alloc_to={}", t, trim_alloc_to);
        if (trim_alloc_to != JOURNAL_SEQ_NULL) {
          return extent_callback->submit_transaction_direct(
            t, std::make_optional<journal_seq_t>(trim_alloc_to));
        }
        return seastar::now();
      });
    });
  }).safe_then([this, FNAME] {
    DEBUG("finish, alloc_tail={}", journal_alloc_tail);
  });
}

JournalTrimmerImpl::trim_ertr::future<>
JournalTrimmerImpl::trim_dirty()
{
  LOG_PREFIX(JournalTrimmerImpl::trim_dirty);
  assert(background_callback->is_ready());
  return repeat_eagain([this, FNAME] {
    return extent_callback->with_transaction_intr(
      Transaction::src_t::TRIM_DIRTY,
      "trim_dirty",
      [this, FNAME](auto &t)
    {
      auto target = get_dirty_tail_target();
      DEBUGT("start, dirty_tail={}, target={}",
             t, journal_dirty_tail, target);
      return extent_callback->get_next_dirty_extents(
        t,
        target,
        config.rewrite_dirty_bytes_per_cycle
      ).si_then([this, FNAME, &t](auto dirty_list) {
        DEBUGT("rewrite {} dirty extents", t, dirty_list.size());
        return seastar::do_with(
          std::move(dirty_list),
          [this, &t](auto &dirty_list)
        {
          return trans_intr::do_for_each(
            dirty_list,
            [this, &t](auto &e) {
            return extent_callback->rewrite_extent(
                t, e, INIT_GENERATION, NULL_TIME);
          });
        });
      }).si_then([this, &t] {
        return extent_callback->submit_transaction_direct(t);
      });
    });
  }).safe_then([this, FNAME] {
    DEBUG("finish, dirty_tail={}", journal_dirty_tail);
  });
}

void JournalTrimmerImpl::register_metrics()
{
  namespace sm = seastar::metrics;
  metrics.add_group("journal_trimmer", {
    sm::make_counter("dirty_journal_bytes",
                     [this] { return get_dirty_journal_size(); },
                     sm::description("the size of the journal for dirty extents")),
    sm::make_counter("alloc_journal_bytes",
                     [this] { return get_alloc_journal_size(); },
                     sm::description("the size of the journal for alloc info"))
  });
}

std::ostream &operator<<(
    std::ostream &os, const JournalTrimmerImpl::stat_printer_t &stats)
{
  os << "JournalTrimmer(";
  if (stats.trimmer.background_callback->is_ready()) {
    os << "should_block_io_on_trim=" << stats.trimmer.should_block_io_on_trim()
       << ", should_(trim_dirty=" << stats.trimmer.should_trim_dirty()
       << ", trim_alloc=" << stats.trimmer.should_trim_alloc() << ")";
  } else {
    os << "not-ready";
  }
  if (stats.detailed) {
    os << ", journal_head=" << stats.trimmer.get_journal_head()
       << ", alloc_tail=" << stats.trimmer.get_alloc_tail()
       << ", dirty_tail=" << stats.trimmer.get_dirty_tail();
    if (stats.trimmer.background_callback->is_ready()) {
      os << ", alloc_tail_target=" << stats.trimmer.get_alloc_tail_target()
         << ", dirty_tail_target=" << stats.trimmer.get_dirty_tail_target()
         << ", tail_limit=" << stats.trimmer.get_tail_limit();
    }
  }
  os << ")";
  return os;
}

bool SpaceTrackerSimple::equals(const SpaceTrackerI &_other) const
{
  LOG_PREFIX(SpaceTrackerSimple::equals);
  const auto &other = static_cast<const SpaceTrackerSimple&>(_other);

  if (other.live_bytes_by_segment.size() != live_bytes_by_segment.size()) {
    ERROR("different segment counts, bug in test");
    assert(0 == "segment counts should match");
    return false;
  }

  bool all_match = true;
  for (auto i = live_bytes_by_segment.begin(), j = other.live_bytes_by_segment.begin();
       i != live_bytes_by_segment.end(); ++i, ++j) {
    if (i->second.live_bytes != j->second.live_bytes) {
      all_match = false;
      DEBUG("segment_id {} live bytes mismatch *this: {}, other: {}",
            i->first, i->second.live_bytes, j->second.live_bytes);
    }
  }
  return all_match;
}

int64_t SpaceTrackerDetailed::SegmentMap::allocate(
  device_segment_id_t segment,
  segment_off_t offset,
  extent_len_t len,
  const extent_len_t block_size)
{
  LOG_PREFIX(SegmentMap::allocate);
  assert(offset % block_size == 0);
  assert(len % block_size == 0);

  const auto b = (offset / block_size);
  const auto e = (offset + len) / block_size;

  bool error = false;
  for (auto i = b; i < e; ++i) {
    if (bitmap[i]) {
      if (!error) {
        ERROR("found allocated in {}, {} ~ {}", segment, offset, len);
	error = true;
      }
      DEBUG("block {} allocated", i * block_size);
    }
    bitmap[i] = true;
  }
  return update_usage(len);
}

int64_t SpaceTrackerDetailed::SegmentMap::release(
  device_segment_id_t segment,
  segment_off_t offset,
  extent_len_t len,
  const extent_len_t block_size)
{
  LOG_PREFIX(SegmentMap::release);
  assert(offset % block_size == 0);
  assert(len % block_size == 0);

  const auto b = (offset / block_size);
  const auto e = (offset + len) / block_size;

  bool error = false;
  for (auto i = b; i < e; ++i) {
    if (!bitmap[i]) {
      if (!error) {
	ERROR("found unallocated in {}, {} ~ {}", segment, offset, len);
	error = true;
      }
      DEBUG("block {} unallocated", i * block_size);
    }
    bitmap[i] = false;
  }
  return update_usage(-(int64_t)len);
}

bool SpaceTrackerDetailed::equals(const SpaceTrackerI &_other) const
{
  LOG_PREFIX(SpaceTrackerDetailed::equals);
  const auto &other = static_cast<const SpaceTrackerDetailed&>(_other);

  if (other.segment_usage.size() != segment_usage.size()) {
    ERROR("different segment counts, bug in test");
    assert(0 == "segment counts should match");
    return false;
  }

  bool all_match = true;
  for (auto i = segment_usage.begin(), j = other.segment_usage.begin();
       i != segment_usage.end(); ++i, ++j) {
    if (i->second.get_usage() != j->second.get_usage()) {
      all_match = false;
      ERROR("segment_id {} live bytes mismatch *this: {}, other: {}",
            i->first, i->second.get_usage(), j->second.get_usage());
    }
  }
  return all_match;
}

void SpaceTrackerDetailed::SegmentMap::dump_usage(extent_len_t block_size) const
{
  LOG_PREFIX(SegmentMap::dump_usage);
  INFO("dump start");
  for (unsigned i = 0; i < bitmap.size(); ++i) {
    if (bitmap[i]) {
      LOCAL_LOGGER.info("    {} still live", i * block_size);
    }
  }
}

void SpaceTrackerDetailed::dump_usage(segment_id_t id) const
{
  LOG_PREFIX(SpaceTrackerDetailed::dump_usage);
  INFO("{}", id);
  segment_usage[id].dump_usage(
    block_size_by_segment_manager[id.device_id()]);
}

void SpaceTrackerSimple::dump_usage(segment_id_t id) const
{
  LOG_PREFIX(SpaceTrackerSimple::dump_usage);
  INFO("id: {}, live_bytes: {}",
       id, live_bytes_by_segment[id].live_bytes);
}

std::ostream &operator<<(
    std::ostream &os, const AsyncCleaner::stat_printer_t &stats)
{
  stats.cleaner.print(os, stats.detailed);
  return os;
}

SegmentCleaner::SegmentCleaner(
  config_t config,
  SegmentManagerGroupRef&& sm_group,
  BackrefManager &backref_manager,
  SegmentSeqAllocator &segment_seq_allocator,
  bool detailed,
  bool is_cold)
  : detailed(detailed),
    is_cold(is_cold),
    config(config),
    sm_group(std::move(sm_group)),
    backref_manager(backref_manager),
    ool_segment_seq_allocator(segment_seq_allocator)
{
  config.validate();
}

void SegmentCleaner::register_metrics()
{
  namespace sm = seastar::metrics;
  stats.segment_util.buckets.resize(UTIL_BUCKETS);
  std::size_t i;
  for (i = 0; i < UTIL_BUCKETS; ++i) {
    stats.segment_util.buckets[i].upper_bound = ((double)(i + 1)) / 10;
    stats.segment_util.buckets[i].count = 0;
  }
  // NOTE: by default the segments are empty
  i = get_bucket_index(UTIL_STATE_EMPTY);
  stats.segment_util.buckets[i].count = segments.get_num_segments();

  std::string prefix;
  if (is_cold) {
    prefix.append("cold_");
  }
  prefix.append("segment_cleaner");

  metrics.add_group(prefix, {
    sm::make_counter("segments_number",
		     [this] { return segments.get_num_segments(); },
		     sm::description("the number of segments")),
    sm::make_counter("segment_size",
		     [this] { return segments.get_segment_size(); },
		     sm::description("the bytes of a segment")),
    sm::make_counter("segments_in_journal",
		     [this] { return get_segments_in_journal(); },
		     sm::description("the number of segments in journal")),
    sm::make_counter("segments_type_journal",
		     [this] { return segments.get_num_type_journal(); },
		     sm::description("the number of segments typed journal")),
    sm::make_counter("segments_type_ool",
		     [this] { return segments.get_num_type_ool(); },
		     sm::description("the number of segments typed out-of-line")),
    sm::make_counter("segments_open",
		     [this] { return segments.get_num_open(); },
		     sm::description("the number of open segments")),
    sm::make_counter("segments_empty",
		     [this] { return segments.get_num_empty(); },
		     sm::description("the number of empty segments")),
    sm::make_counter("segments_closed",
		     [this] { return segments.get_num_closed(); },
		     sm::description("the number of closed segments")),

    sm::make_counter("segments_count_open_journal",
		     [this] { return segments.get_count_open_journal(); },
		     sm::description("the count of open journal segment operations")),
    sm::make_counter("segments_count_open_ool",
		     [this] { return segments.get_count_open_ool(); },
		     sm::description("the count of open ool segment operations")),
    sm::make_counter("segments_count_release_journal",
		     [this] { return segments.get_count_release_journal(); },
		     sm::description("the count of release journal segment operations")),
    sm::make_counter("segments_count_release_ool",
		     [this] { return segments.get_count_release_ool(); },
		     sm::description("the count of release ool segment operations")),
    sm::make_counter("segments_count_close_journal",
		     [this] { return segments.get_count_close_journal(); },
		     sm::description("the count of close journal segment operations")),
    sm::make_counter("segments_count_close_ool",
		     [this] { return segments.get_count_close_ool(); },
		     sm::description("the count of close ool segment operations")),

    sm::make_counter("total_bytes",
		     [this] { return segments.get_total_bytes(); },
		     sm::description("the size of the space")),
    sm::make_counter("available_bytes",
		     [this] { return segments.get_available_bytes(); },
		     sm::description("the size of the space is available")),
    sm::make_counter("unavailable_unreclaimable_bytes",
		     [this] { return get_unavailable_unreclaimable_bytes(); },
		     sm::description("the size of the space is unavailable and unreclaimable")),
    sm::make_counter("unavailable_reclaimable_bytes",
		     [this] { return get_unavailable_reclaimable_bytes(); },
		     sm::description("the size of the space is unavailable and reclaimable")),
    sm::make_counter("used_bytes", stats.used_bytes,
		     sm::description("the size of the space occupied by live extents")),
    sm::make_counter("unavailable_unused_bytes",
		     [this] { return get_unavailable_unused_bytes(); },
		     sm::description("the size of the space is unavailable and not alive")),

    sm::make_counter("projected_count", stats.projected_count,
		    sm::description("the number of projected usage reservations")),
    sm::make_counter("projected_used_bytes_sum", stats.projected_used_bytes_sum,
		    sm::description("the sum of the projected usage in bytes")),

    sm::make_counter("reclaimed_bytes", stats.reclaimed_bytes,
		     sm::description("rewritten bytes due to reclaim")),
    sm::make_counter("reclaimed_segment_bytes", stats.reclaimed_segment_bytes,
		     sm::description("rewritten bytes due to reclaim")),
    sm::make_counter("closed_journal_used_bytes", stats.closed_journal_used_bytes,
		     sm::description("used bytes when close a journal segment")),
    sm::make_counter("closed_journal_total_bytes", stats.closed_journal_total_bytes,
		     sm::description("total bytes of closed journal segments")),
    sm::make_counter("closed_ool_used_bytes", stats.closed_ool_used_bytes,
		     sm::description("used bytes when close a ool segment")),
    sm::make_counter("closed_ool_total_bytes", stats.closed_ool_total_bytes,
		     sm::description("total bytes of closed ool segments")),

    sm::make_gauge("available_ratio",
                   [this] { return segments.get_available_ratio(); },
                   sm::description("ratio of available space to total space")),
    sm::make_gauge("reclaim_ratio",
                   [this] { return get_reclaim_ratio(); },
                   sm::description("ratio of reclaimable space to unavailable space")),

    sm::make_histogram("segment_utilization_distribution",
		       [this]() -> seastar::metrics::histogram& {
		         return stats.segment_util;
		       },
		       sm::description("utilization distribution of all segments"))
  });
}

segment_id_t SegmentCleaner::allocate_segment(
    segment_seq_t seq,
    segment_type_t type,
    data_category_t category,
    rewrite_gen_t generation)
{
  LOG_PREFIX(SegmentCleaner::allocate_segment);
  assert(seq != NULL_SEG_SEQ);
  ceph_assert(type == segment_type_t::OOL ||
              trimmer != nullptr); // segment_type_t::JOURNAL
  for (auto it = segments.begin();
       it != segments.end();
       ++it) {
    auto seg_id = it->first;
    auto& segment_info = it->second;
    if (segment_info.is_empty()) {
      auto old_usage = calc_utilization(seg_id);
      segments.mark_open(seg_id, seq, type, category, generation);
      background_callback->maybe_wake_background();
      auto new_usage = calc_utilization(seg_id);
      adjust_segment_util(old_usage, new_usage);
      INFO("opened, {}", stat_printer_t{*this, false});
      return seg_id;
    }
  }
  ERROR("out of space with {} {} {} {}",
        type, segment_seq_printer_t{seq}, category,
        rewrite_gen_printer_t{generation});
  ceph_abort("seastore device size setting is too small");
  return NULL_SEG_ID;
}

void SegmentCleaner::close_segment(segment_id_t segment)
{
  LOG_PREFIX(SegmentCleaner::close_segment);
  auto old_usage = calc_utilization(segment);
  segments.mark_closed(segment);
  auto &seg_info = segments[segment];
  if (seg_info.type == segment_type_t::JOURNAL) {
    stats.closed_journal_used_bytes += space_tracker->get_usage(segment);
    stats.closed_journal_total_bytes += segments.get_segment_size();
  } else {
    stats.closed_ool_used_bytes += space_tracker->get_usage(segment);
    stats.closed_ool_total_bytes += segments.get_segment_size();
  }
  auto new_usage = calc_utilization(segment);
  adjust_segment_util(old_usage, new_usage);
  INFO("closed, {} -- {}", stat_printer_t{*this, false}, seg_info);
}

double SegmentCleaner::calc_gc_benefit_cost(
  segment_id_t id,
  const sea_time_point &now_time,
  const sea_time_point &bound_time) const
{
  double util = calc_utilization(id);
  ceph_assert(util >= 0 && util < 1);
  if constexpr (gc_formula == gc_formula_t::GREEDY) {
    return 1 - util;
  }

  if constexpr (gc_formula == gc_formula_t::COST_BENEFIT) {
    if (util == 0) {
      return std::numeric_limits<double>::max();
    }
    auto modify_time = segments[id].modify_time;
    double age_segment = modify_time.time_since_epoch().count();
    double age_now = now_time.time_since_epoch().count();
    if (likely(age_now > age_segment)) {
      return (1 - util) * (age_now - age_segment) / (2 * util);
    } else {
      // time is wrong
      return (1 - util) / (2 * util);
    }
  }

  assert(gc_formula == gc_formula_t::BENEFIT);
  auto modify_time = segments[id].modify_time;
  double age_factor = 0.5; // middle value if age is invalid
  if (likely(bound_time != NULL_TIME &&
             modify_time != NULL_TIME &&
             now_time > modify_time)) {
    assert(modify_time >= bound_time);
    double age_bound = bound_time.time_since_epoch().count();
    double age_now = now_time.time_since_epoch().count();
    double age_segment = modify_time.time_since_epoch().count();
    age_factor = (age_now - age_segment) / (age_now - age_bound);
  }
  return ((1 - 2 * age_factor) * util * util +
          (2 * age_factor - 2) * util + 1);
}

SegmentCleaner::do_reclaim_space_ret
SegmentCleaner::do_reclaim_space(
    const std::vector<CachedExtentRef> &backref_extents,
    const backref_pin_list_t &pin_list,
    std::size_t &reclaimed,
    std::size_t &runs)
{
  return repeat_eagain([this, &backref_extents,
                        &pin_list, &reclaimed, &runs] {
    reclaimed = 0;
    runs++;
    auto src = Transaction::src_t::CLEANER_MAIN;
    if (is_cold) {
      src = Transaction::src_t::CLEANER_COLD;
    }
    return extent_callback->with_transaction_intr(
      src,
      "clean_reclaim_space",
      [this, &backref_extents, &pin_list, &reclaimed](auto &t)
    {
      return seastar::do_with(
        std::vector<CachedExtentRef>(backref_extents),
        [this, &t, &reclaimed, &pin_list](auto &extents)
      {
        LOG_PREFIX(SegmentCleaner::do_reclaim_space);
        // calculate live extents
        auto cached_backref_entries =
          backref_manager.get_cached_backref_entries_in_range(
            reclaim_state->start_pos, reclaim_state->end_pos);
        backref_entry_query_set_t backref_entries;
        for (auto &pin : pin_list) {
          backref_entries.emplace(
            pin->get_key(),
            pin->get_val(),
            pin->get_length(),
            pin->get_type(),
            JOURNAL_SEQ_NULL);
        }
        for (auto &cached_backref : cached_backref_entries) {
          if (cached_backref.laddr == L_ADDR_NULL) {
            auto it = backref_entries.find(cached_backref.paddr);
            assert(it->len == cached_backref.len);
            backref_entries.erase(it);
          } else {
            backref_entries.emplace(cached_backref);
          }
        }
        // retrieve live extents
        DEBUGT("start, backref_entries={}, backref_extents={}",
               t, backref_entries.size(), extents.size());
	return seastar::do_with(
	  std::move(backref_entries),
	  [this, &extents, &t](auto &backref_entries) {
	  return trans_intr::parallel_for_each(
	    backref_entries,
	    [this, &extents, &t](auto &ent)
	  {
	    LOG_PREFIX(SegmentCleaner::do_reclaim_space);
	    TRACET("getting extent of type {} at {}~{}",
	      t,
	      ent.type,
	      ent.paddr,
	      ent.len);
	    return extent_callback->get_extents_if_live(
	      t, ent.type, ent.paddr, ent.laddr, ent.len
	    ).si_then([FNAME, &extents, &ent, &t](auto list) {
	      if (list.empty()) {
		TRACET("addr {} dead, skipping", t, ent.paddr);
	      } else {
		for (auto &e : list) {
		  extents.emplace_back(std::move(e));
		}
	      }
	    });
	  });
	}).si_then([FNAME, &extents, this, &reclaimed, &t] {
          DEBUGT("reclaim {} extents", t, extents.size());
          // rewrite live extents
          auto modify_time = segments[reclaim_state->get_segment_id()].modify_time;
          return trans_intr::do_for_each(
            extents,
            [this, modify_time, &t, &reclaimed](auto ext)
          {
            reclaimed += ext->get_length();
            return extent_callback->rewrite_extent(
                t, ext, reclaim_state->target_generation, modify_time);
          });
        });
      }).si_then([this, &t] {
        return extent_callback->submit_transaction_direct(t);
      });
    });
  });
}

SegmentCleaner::clean_space_ret SegmentCleaner::clean_space()
{
  LOG_PREFIX(SegmentCleaner::clean_space);
  assert(background_callback->is_ready());
  ceph_assert(can_clean_space());
  if (!reclaim_state) {
    segment_id_t seg_id = get_next_reclaim_segment();
    auto &segment_info = segments[seg_id];
    INFO("reclaim {} {} start, usage={}, time_bound={}",
         seg_id, segment_info,
         space_tracker->calc_utilization(seg_id),
         sea_time_point_printer_t{segments.get_time_bound()});
    ceph_assert(segment_info.is_closed());
    reclaim_state = reclaim_state_t::create(
        seg_id, segment_info.generation, segments.get_segment_size());
  }
  reclaim_state->advance(config.reclaim_bytes_per_cycle);

  DEBUG("reclaiming {} {}~{}",
        rewrite_gen_printer_t{reclaim_state->generation},
        reclaim_state->start_pos,
        reclaim_state->end_pos);
  double pavail_ratio = get_projected_available_ratio();
  sea_time_point start = seastar::lowres_system_clock::now();

  // Backref-tree doesn't support tree-read during tree-updates with parallel
  // transactions.  So, concurrent transactions between trim and reclaim are
  // not allowed right now.
  return seastar::do_with(
    std::pair<std::vector<CachedExtentRef>, backref_pin_list_t>(),
    [this](auto &weak_read_ret) {
    return repeat_eagain([this, &weak_read_ret] {
      return extent_callback->with_transaction_intr(
	  Transaction::src_t::READ,
	  "retrieve_from_backref_tree",
	  [this, &weak_read_ret](auto &t) {
	return backref_manager.get_mappings(
	  t,
	  reclaim_state->start_pos,
	  reclaim_state->end_pos
	).si_then([this, &t, &weak_read_ret](auto pin_list) {
	  if (!pin_list.empty()) {
	    auto it = pin_list.begin();
	    auto &first_pin = *it;
	    if (first_pin->get_key() < reclaim_state->start_pos) {
	      // BackrefManager::get_mappings may include a entry before
	      // reclaim_state->start_pos, which is semantically inconsistent
	      // with the requirements of the cleaner
	      pin_list.erase(it);
	    }
	  }
	  return backref_manager.retrieve_backref_extents_in_range(
	    t,
	    reclaim_state->start_pos,
	    reclaim_state->end_pos
	  ).si_then([pin_list=std::move(pin_list),
		    &weak_read_ret](auto extents) mutable {
	    weak_read_ret = std::make_pair(std::move(extents), std::move(pin_list));
	  });
	});
      });
    }).safe_then([&weak_read_ret] {
      return std::move(weak_read_ret);
    });
  }).safe_then([this, FNAME, pavail_ratio, start](auto weak_read_ret) {
    return seastar::do_with(
      std::move(weak_read_ret.first),
      std::move(weak_read_ret.second),
      (size_t)0,
      (size_t)0,
      [this, FNAME, pavail_ratio, start](
        auto &backref_extents, auto &pin_list, auto &reclaimed, auto &runs)
    {
      return do_reclaim_space(
          backref_extents,
          pin_list,
          reclaimed,
          runs
      ).safe_then([this, FNAME, pavail_ratio, start, &reclaimed, &runs] {
        stats.reclaiming_bytes += reclaimed;
        auto d = seastar::lowres_system_clock::now() - start;
        DEBUG("duration: {}, pavail_ratio before: {}, repeats: {}",
              d, pavail_ratio, runs);
        if (reclaim_state->is_complete()) {
          auto segment_to_release = reclaim_state->get_segment_id();
          INFO("reclaim {} finish, reclaimed alive/total={}",
               segment_to_release,
               stats.reclaiming_bytes/(double)segments.get_segment_size());
          stats.reclaimed_bytes += stats.reclaiming_bytes;
          stats.reclaimed_segment_bytes += segments.get_segment_size();
          stats.reclaiming_bytes = 0;
          reclaim_state.reset();
          return sm_group->release_segment(segment_to_release
          ).handle_error(
            clean_space_ertr::pass_further{},
            crimson::ct_error::assert_all{
              "SegmentCleaner::clean_space encountered invalid error in release_segment"
            }
          ).safe_then([this, FNAME, segment_to_release] {
            auto old_usage = calc_utilization(segment_to_release);
            if(unlikely(old_usage != 0)) {
              space_tracker->dump_usage(segment_to_release);
              ERROR("segment {} old_usage {} != 0",
                     segment_to_release, old_usage);
              ceph_abort();
            }
            segments.mark_empty(segment_to_release);
            auto new_usage = calc_utilization(segment_to_release);
            adjust_segment_util(old_usage, new_usage);
            INFO("released {}, {}",
                 segment_to_release, stat_printer_t{*this, false});
            background_callback->maybe_wake_blocked_io();
          });
        } else {
          return clean_space_ertr::now();
        }
      });
    });
  });
}

SegmentCleaner::mount_ret SegmentCleaner::mount()
{
  LOG_PREFIX(SegmentCleaner::mount);
  const auto& sms = sm_group->get_segment_managers();
  INFO("{} segment managers", sms.size());

  assert(background_callback->get_state() == state_t::MOUNT);
  
  space_tracker.reset(
    detailed ?
    (SpaceTrackerI*)new SpaceTrackerDetailed(
      sms) :
    (SpaceTrackerI*)new SpaceTrackerSimple(
      sms));
  
  segments.reset();
  for (auto sm : sms) {
    segments.add_segment_manager(*sm);
  }
  segments.assign_ids();

  stats = {};
  metrics.clear();
  register_metrics();

  INFO("{} segments", segments.get_num_segments());
  return crimson::do_for_each(
    segments.begin(),
    segments.end(),
    [this, FNAME](auto& it)
  {
    auto segment_id = it.first;
    return sm_group->read_segment_header(
      segment_id
    ).safe_then([segment_id, this, FNAME](auto header) {
      DEBUG("segment_id={} -- {}", segment_id, header);
      auto s_type = header.get_type();
      if (s_type == segment_type_t::NULL_SEG) {
        ERROR("got null segment, segment_id={} -- {}", segment_id, header);
        ceph_abort();
      }
      return sm_group->read_segment_tail(
        segment_id
      ).safe_then([this, FNAME, segment_id, header](auto tail)
        -> scan_extents_ertr::future<> {
        if (tail.segment_nonce != header.segment_nonce) {
          return scan_no_tail_segment(header, segment_id);
        }
        ceph_assert(header.get_type() == tail.get_type());

        sea_time_point modify_time = mod_to_timepoint(tail.modify_time);
        std::size_t num_extents = tail.num_extents;
        if ((modify_time == NULL_TIME && num_extents == 0) ||
            (modify_time != NULL_TIME && num_extents != 0)) {
          segments.update_modify_time(segment_id, modify_time, num_extents);
        } else {
          ERROR("illegal modify time {}", tail);
          return crimson::ct_error::input_output_error::make();
        }

        init_mark_segment_closed(
          segment_id,
          header.segment_seq,
          header.type,
          header.category,
          header.generation);
        return seastar::now();
      }).handle_error(
        crimson::ct_error::enodata::handle(
          [this, header, segment_id](auto) {
          return scan_no_tail_segment(header, segment_id);
        }),
        crimson::ct_error::pass_further_all{}
      );
    }).handle_error(
      crimson::ct_error::enoent::handle([](auto) {
        return mount_ertr::now();
      }),
      crimson::ct_error::enodata::handle([](auto) {
        return mount_ertr::now();
      }),
      crimson::ct_error::input_output_error::pass_further{},
      crimson::ct_error::assert_all{"unexpected error"}
    );
  }).safe_then([this, FNAME] {
    INFO("done, {}", segments);
  });
}

SegmentCleaner::scan_extents_ret SegmentCleaner::scan_no_tail_segment(
  const segment_header_t &segment_header,
  segment_id_t segment_id)
{
  LOG_PREFIX(SegmentCleaner::scan_no_tail_segment);
  INFO("scan {} {}", segment_id, segment_header);
  return seastar::do_with(
    scan_valid_records_cursor({
      segments[segment_id].seq,
      paddr_t::make_seg_paddr(segment_id, 0)
    }),
    SegmentManagerGroup::found_record_handler_t(
      [this, segment_id, segment_header, FNAME](
        record_locator_t locator,
        const record_group_header_t &record_group_header,
        const bufferlist& mdbuf
      ) mutable -> SegmentManagerGroup::scan_valid_records_ertr::future<>
    {
      DEBUG("{} {}, decoding {} records",
            segment_id, segment_header.get_type(), record_group_header.records);

      auto maybe_headers = try_decode_record_headers(
          record_group_header, mdbuf);
      if (!maybe_headers) {
        // This should be impossible, we did check the crc on the mdbuf
        ERROR("unable to decode record headers for record group {}",
          locator.record_block_base);
        return crimson::ct_error::input_output_error::make();
      }

      for (auto &record_header : *maybe_headers) {
        auto modify_time = mod_to_timepoint(record_header.modify_time);
        if (record_header.extents == 0 || modify_time != NULL_TIME) {
          segments.update_modify_time(
              segment_id, modify_time, record_header.extents);
        } else {
          ERROR("illegal modify time {}", record_header);
          return crimson::ct_error::input_output_error::make();
        }
      }
      return seastar::now();
    }),
    [this, segment_header](auto &cursor, auto &handler)
  {
    return sm_group->scan_valid_records(
      cursor,
      segment_header.segment_nonce,
      segments.get_segment_size(),
      handler).discard_result();
  }).safe_then([this, segment_id, segment_header] {
    init_mark_segment_closed(
      segment_id,
      segment_header.segment_seq,
      segment_header.type,
      segment_header.category,
      segment_header.generation);
  });
}

bool SegmentCleaner::check_usage()
{
  SpaceTrackerIRef tracker(space_tracker->make_empty());
  extent_callback->with_transaction_weak(
      "check_usage",
      [this, &tracker](auto &t) {
    return backref_manager.scan_mapped_space(
      t,
      [&tracker](
        paddr_t paddr,
	paddr_t backref_key,
        extent_len_t len,
        extent_types_t type,
        laddr_t laddr)
    {
      if (paddr.get_addr_type() == paddr_types_t::SEGMENT) {
        if (is_backref_node(type)) {
	  assert(laddr == L_ADDR_NULL);
	  assert(backref_key != P_ADDR_NULL);
          tracker->allocate(
            paddr.as_seg_paddr().get_segment_id(),
            paddr.as_seg_paddr().get_segment_off(),
            len);
        } else if (laddr == L_ADDR_NULL) {
	  assert(backref_key == P_ADDR_NULL);
          tracker->release(
            paddr.as_seg_paddr().get_segment_id(),
            paddr.as_seg_paddr().get_segment_off(),
            len);
        } else {
	  assert(backref_key == P_ADDR_NULL);
          tracker->allocate(
            paddr.as_seg_paddr().get_segment_id(),
            paddr.as_seg_paddr().get_segment_off(),
            len);
        }
      }
    });
  }).unsafe_get0();
  return space_tracker->equals(*tracker);
}

void SegmentCleaner::mark_space_used(
  paddr_t addr,
  extent_len_t len)
{
  LOG_PREFIX(SegmentCleaner::mark_space_used);
  assert(background_callback->get_state() >= state_t::SCAN_SPACE);
  assert(len);
  // TODO: drop
  if (addr.get_addr_type() != paddr_types_t::SEGMENT) {
    return;
  }

  auto& seg_addr = addr.as_seg_paddr();
  stats.used_bytes += len;
  auto old_usage = calc_utilization(seg_addr.get_segment_id());
  [[maybe_unused]] auto ret = space_tracker->allocate(
    seg_addr.get_segment_id(),
    seg_addr.get_segment_off(),
    len);
  auto new_usage = calc_utilization(seg_addr.get_segment_id());
  adjust_segment_util(old_usage, new_usage);

  background_callback->maybe_wake_background();
  assert(ret > 0);
  DEBUG("segment {} new len: {}~{}, live_bytes: {}",
        seg_addr.get_segment_id(),
        addr,
        len,
        space_tracker->get_usage(seg_addr.get_segment_id()));
}

void SegmentCleaner::mark_space_free(
  paddr_t addr,
  extent_len_t len)
{
  LOG_PREFIX(SegmentCleaner::mark_space_free);
  assert(background_callback->get_state() >= state_t::SCAN_SPACE);
  assert(len);
  // TODO: drop
  if (addr.get_addr_type() != paddr_types_t::SEGMENT) {
    return;
  }

  ceph_assert(stats.used_bytes >= len);
  stats.used_bytes -= len;
  auto& seg_addr = addr.as_seg_paddr();

  DEBUG("segment {} free len: {}~{}",
        seg_addr.get_segment_id(), addr, len);
  auto old_usage = calc_utilization(seg_addr.get_segment_id());
  [[maybe_unused]] auto ret = space_tracker->release(
    seg_addr.get_segment_id(),
    seg_addr.get_segment_off(),
    len);
  auto new_usage = calc_utilization(seg_addr.get_segment_id());
  adjust_segment_util(old_usage, new_usage);
  background_callback->maybe_wake_blocked_io();
  assert(ret >= 0);
  DEBUG("segment {} free len: {}~{}, live_bytes: {}",
        seg_addr.get_segment_id(),
        addr,
        len,
        space_tracker->get_usage(seg_addr.get_segment_id()));
}

segment_id_t SegmentCleaner::get_next_reclaim_segment() const
{
  LOG_PREFIX(SegmentCleaner::get_next_reclaim_segment);
  segment_id_t id = NULL_SEG_ID;
  double max_benefit_cost = 0;
  sea_time_point now_time;
  if constexpr (gc_formula != gc_formula_t::GREEDY) {
    now_time = seastar::lowres_system_clock::now();
  } else {
    now_time = NULL_TIME;
  }
  sea_time_point bound_time;
  if constexpr (gc_formula == gc_formula_t::BENEFIT) {
    bound_time = segments.get_time_bound();
    if (bound_time == NULL_TIME) {
      WARN("BENEFIT -- bound_time is NULL_TIME");
    }
  } else {
    bound_time = NULL_TIME;
  }
  for (auto& [_id, segment_info] : segments) {
    if (segment_info.is_closed() &&
        (trimmer == nullptr ||
         !segment_info.is_in_journal(trimmer->get_journal_tail()))) {
      double benefit_cost = calc_gc_benefit_cost(_id, now_time, bound_time);
      if (benefit_cost > max_benefit_cost) {
        id = _id;
        max_benefit_cost = benefit_cost;
      }
    }
  }
  if (id != NULL_SEG_ID) {
    DEBUG("segment {}, benefit_cost {}",
          id, max_benefit_cost);
    return id;
  } else {
    ceph_assert(get_segments_reclaimable() == 0);
    // see should_clean_space()
    ceph_abort("impossible!");
    return NULL_SEG_ID;
  }
}

bool SegmentCleaner::try_reserve_projected_usage(std::size_t projected_usage)
{
  assert(background_callback->is_ready());
  stats.projected_used_bytes += projected_usage;
  if (should_block_io_on_clean()) {
    stats.projected_used_bytes -= projected_usage;
    return false;
  } else {
    ++stats.projected_count;
    stats.projected_used_bytes_sum += stats.projected_used_bytes;
    return true;
  }
}

void SegmentCleaner::release_projected_usage(std::size_t projected_usage)
{
  assert(background_callback->is_ready());
  ceph_assert(stats.projected_used_bytes >= projected_usage);
  stats.projected_used_bytes -= projected_usage;
  background_callback->maybe_wake_blocked_io();
}

void SegmentCleaner::print(std::ostream &os, bool is_detailed) const
{
  os << "SegmentCleaner(";
  if (background_callback->is_ready()) {
    os << "should_block_io_on_clean=" << should_block_io_on_clean()
       << ", should_clean=" << should_clean_space();
  } else {
    os << "not-ready";
  }
  os << ", projected_avail_ratio=" << get_projected_available_ratio()
     << ", reclaim_ratio=" << get_reclaim_ratio()
     << ", alive_ratio=" << get_alive_ratio();
  if (is_detailed) {
    os << ", unavailable_unreclaimable="
       << get_unavailable_unreclaimable_bytes() << "B"
       << ", unavailable_reclaimble="
       << get_unavailable_reclaimable_bytes() << "B"
       << ", alive=" << stats.used_bytes << "B"
       << ", " << segments;
  }
  os << ")";
}

RBMCleaner::RBMCleaner(
  RBMDeviceGroupRef&& rb_group,
  BackrefManager &backref_manager,
  bool detailed)
  : detailed(detailed),
    rb_group(std::move(rb_group)),
    backref_manager(backref_manager)
{}

void RBMCleaner::print(std::ostream &os, bool is_detailed) const
{
  // TODO
  return;
}

void RBMCleaner::mark_space_used(
  paddr_t addr,
  extent_len_t len)
{
  LOG_PREFIX(RBMCleaner::mark_space_used);
  assert(addr.get_addr_type() == paddr_types_t::RANDOM_BLOCK);
  auto rbms = rb_group->get_rb_managers();
  for (auto rbm : rbms) {
    if (addr.get_device_id() == rbm->get_device_id()) {
      if (rbm->get_start() <= addr) {
	DEBUG("allocate addr: {} len: {}", addr, len);
	stats.used_bytes += len;
	rbm->mark_space_used(addr, len);
      }
      return;
    }
  }
}

void RBMCleaner::mark_space_free(
  paddr_t addr,
  extent_len_t len)
{
  LOG_PREFIX(RBMCleaner::mark_space_free);
  assert(addr.get_addr_type() == paddr_types_t::RANDOM_BLOCK);
  auto rbms = rb_group->get_rb_managers();
  for (auto rbm : rbms) {
    if (addr.get_device_id() == rbm->get_device_id()) {
      if (rbm->get_start() <= addr) {
	DEBUG("free addr: {} len: {}", addr, len);
	ceph_assert(stats.used_bytes >= len);
	stats.used_bytes -= len;
	rbm->mark_space_free(addr, len);
      }
      return;
    }
  }
}

void RBMCleaner::commit_space_used(paddr_t addr, extent_len_t len) 
{
  auto rbms = rb_group->get_rb_managers();
  for (auto rbm : rbms) {
    if (addr.get_device_id() == rbm->get_device_id()) {
      if (rbm->get_start() <= addr) {
	rbm->complete_allocation(addr, len);
      }
      return;
    }
  }
}

bool RBMCleaner::try_reserve_projected_usage(std::size_t projected_usage)
{
  assert(background_callback->is_ready());
  stats.projected_used_bytes += projected_usage;
  return true;
}

void RBMCleaner::release_projected_usage(std::size_t projected_usage)
{
  assert(background_callback->is_ready());
  ceph_assert(stats.projected_used_bytes >= projected_usage);
  stats.projected_used_bytes -= projected_usage;
  background_callback->maybe_wake_blocked_io();
}

RBMCleaner::clean_space_ret RBMCleaner::clean_space()
{
  // TODO 
  return clean_space_ertr::now();
}

RBMCleaner::mount_ret RBMCleaner::mount()
{
  stats = {};
  register_metrics();
  return seastar::do_with(
    rb_group->get_rb_managers(),
    [](auto &rbs) {
    return crimson::do_for_each(
      rbs.begin(),
      rbs.end(),
      [](auto& it) {
      return it->open(
      ).handle_error(
	crimson::ct_error::input_output_error::pass_further(),
	crimson::ct_error::assert_all{
	"Invalid error when opening RBM"}
      );
    });
  });
}

bool RBMCleaner::check_usage()
{
  assert(detailed);
  const auto& rbms = rb_group->get_rb_managers();
  RBMSpaceTracker tracker(rbms);
  extent_callback->with_transaction_weak(
      "check_usage",
      [this, &tracker, &rbms](auto &t) {
    return backref_manager.scan_mapped_space(
      t,
      [&tracker, &rbms](
        paddr_t paddr,
	paddr_t backref_key,
        extent_len_t len,
        extent_types_t type,
        laddr_t laddr)
    {
      for (auto rbm : rbms) {
	if (rbm->get_device_id() == paddr.get_device_id()) {
	  if (is_backref_node(type)) {
	    assert(laddr == L_ADDR_NULL);
	    assert(backref_key != P_ADDR_NULL);
	    tracker.allocate(
	      paddr,
	      len);
	  } else if (laddr == L_ADDR_NULL) {
	    assert(backref_key == P_ADDR_NULL);
	    tracker.release(
	      paddr,
	      len);
	  } else {
	    assert(backref_key == P_ADDR_NULL);
	    tracker.allocate(
	      paddr,
	      len);
	  }
	}
      }
    });
  }).unsafe_get0();
  return equals(tracker);
}

bool RBMCleaner::equals(const RBMSpaceTracker &_other) const
{
  LOG_PREFIX(RBMSpaceTracker::equals);
  const auto &other = static_cast<const RBMSpaceTracker&>(_other);
  auto rbs = rb_group->get_rb_managers();
  //TODO: multiple rbm allocator
  auto rbm = rbs[0];
  assert(rbm);

  if (rbm->get_device()->get_available_size() / rbm->get_block_size()
      != other.block_usage.size()) {
    assert(0 == "block counts should match");
    return false;
  }
  bool all_match = true;
  for (auto i = other.block_usage.begin();
       i != other.block_usage.end(); ++i) {
    if (i->first < rbm->get_start().as_blk_paddr().get_device_off()) {
      continue;
    }
    auto addr = i->first;
    auto state = rbm->get_extent_state(
      convert_abs_addr_to_paddr(addr, rbm->get_device_id()),
      rbm->get_block_size());
    if ((i->second.used && state == rbm_extent_state_t::ALLOCATED) ||
	(!i->second.used && (state == rbm_extent_state_t::FREE ||
                         state == rbm_extent_state_t::RESERVED))) {
      // pass
    } else {
      all_match = false;
      ERROR("block addr {} mismatch other used: {}",
	    addr, i->second.used);
    }
  }
  return all_match;
}

void RBMCleaner::register_metrics()
{
  namespace sm = seastar::metrics;

  metrics.add_group("rbm_cleaner", {
    sm::make_counter("total_bytes",
		     [this] { return get_total_bytes(); },
		     sm::description("the size of the space")),
    sm::make_counter("available_bytes",
		     [this] { return get_total_bytes() - get_journal_bytes() - stats.used_bytes; },
		     sm::description("the size of the space is available")),
    sm::make_counter("used_bytes", stats.used_bytes,
		     sm::description("the size of the space occupied by live extents")),
  });
}

}
