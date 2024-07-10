// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/extent_placement_manager.h"

#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_epm);

namespace crimson::os::seastore {

SegmentedOolWriter::SegmentedOolWriter(
  data_category_t category,
  rewrite_gen_t gen,
  SegmentProvider& sp,
  SegmentSeqAllocator &ssa)
  : segment_allocator(nullptr, category, gen, sp, ssa),
    record_submitter(crimson::common::get_conf<uint64_t>(
                       "seastore_journal_iodepth_limit"),
                     crimson::common::get_conf<uint64_t>(
                       "seastore_journal_batch_capacity"),
                     crimson::common::get_conf<Option::size_t>(
                       "seastore_journal_batch_flush_size"),
                     crimson::common::get_conf<double>(
                       "seastore_journal_batch_preferred_fullness"),
                     segment_allocator)
{
}

SegmentedOolWriter::alloc_write_ertr::future<>
SegmentedOolWriter::write_record(
  Transaction& t,
  record_t&& record,
  std::list<LogicalCachedExtentRef>&& extents,
  bool with_atomic_roll_segment)
{
  LOG_PREFIX(SegmentedOolWriter::write_record);
  assert(extents.size());
  assert(extents.size() == record.extents.size());
  assert(!record.deltas.size());

  // account transactional ool writes before write()
  auto& stats = t.get_ool_write_stats();
  stats.extents.num += extents.size();
  stats.extents.bytes += record.size.dlength;
  stats.md_bytes += record.size.get_raw_mdlength();
  stats.num_records += 1;

  return record_submitter.submit(
    std::move(record),
    with_atomic_roll_segment
  ).safe_then([this, FNAME, &t, extents=std::move(extents)
              ](record_locator_t ret) mutable {
    DEBUGT("{} finish with {} and {} extents",
           t, segment_allocator.get_name(),
           ret, extents.size());
    paddr_t extent_addr = ret.record_block_base;
    for (auto& extent : extents) {
      TRACET("{} ool extent written at {} -- {}",
             t, segment_allocator.get_name(),
             extent_addr, *extent);
      t.update_delayed_ool_extent_addr(extent, extent_addr);
      extent_addr = extent_addr.as_seg_paddr().add_offset(
          extent->get_length());
    }
  });
}

SegmentedOolWriter::alloc_write_iertr::future<>
SegmentedOolWriter::do_write(
  Transaction& t,
  std::list<CachedExtentRef>& extents)
{
  LOG_PREFIX(SegmentedOolWriter::do_write);
  assert(!extents.empty());
  if (!record_submitter.is_available()) {
    DEBUGT("{} extents={} wait ...",
           t, segment_allocator.get_name(),
           extents.size());
    return trans_intr::make_interruptible(
      record_submitter.wait_available()
    ).si_then([this, &t, &extents] {
      return do_write(t, extents);
    });
  }
  record_t record(t.get_src());
  std::list<LogicalCachedExtentRef> pending_extents;
  auto commit_time = seastar::lowres_system_clock::now();

  for (auto it = extents.begin(); it != extents.end();) {
    auto& ext = *it;
    assert(ext->is_logical());
    auto extent = ext->template cast<LogicalCachedExtent>();
    record_size_t wouldbe_rsize = record.size;
    wouldbe_rsize.account_extent(extent->get_bptr().length());
    using action_t = journal::RecordSubmitter::action_t;
    action_t action = record_submitter.check_action(wouldbe_rsize);
    if (action == action_t::ROLL) {
      auto num_extents = pending_extents.size();
      DEBUGT("{} extents={} submit {} extents and roll, unavailable ...",
             t, segment_allocator.get_name(),
             extents.size(), num_extents);
      auto fut_write = alloc_write_ertr::now();
      if (num_extents > 0) {
        assert(record_submitter.check_action(record.size) !=
               action_t::ROLL);
        fut_write = write_record(
            t, std::move(record), std::move(pending_extents),
            true/* with_atomic_roll_segment */);
      }
      return trans_intr::make_interruptible(
        record_submitter.roll_segment(
        ).safe_then([fut_write=std::move(fut_write)]() mutable {
          return std::move(fut_write);
        })
      ).si_then([this, &t, &extents] {
        return do_write(t, extents);
      });
    }

    TRACET("{} extents={} add extent to record -- {}",
           t, segment_allocator.get_name(),
           extents.size(), *extent);
    ceph::bufferlist bl;
    extent->prepare_write();
    bl.append(extent->get_bptr());
    assert(bl.length() == extent->get_length());
    auto modify_time = extent->get_modify_time();
    if (modify_time == NULL_TIME) {
      modify_time = commit_time;
    }
    record.push_back(
      extent_t{
        extent->get_type(),
        extent->get_laddr(),
        std::move(bl)},
      modify_time);
    pending_extents.push_back(extent);
    it = extents.erase(it);

    assert(record_submitter.check_action(record.size) == action);
    if (action == action_t::SUBMIT_FULL) {
      DEBUGT("{} extents={} submit {} extents ...",
             t, segment_allocator.get_name(),
             extents.size(), pending_extents.size());
      return trans_intr::make_interruptible(
        write_record(t, std::move(record), std::move(pending_extents))
      ).si_then([this, &t, &extents] {
        if (!extents.empty()) {
          return do_write(t, extents);
        } else {
          return alloc_write_iertr::now();
        }
      });
    }
    // SUBMIT_NOT_FULL: evaluate the next extent
  }

  auto num_extents = pending_extents.size();
  DEBUGT("{} submit the rest {} extents ...",
         t, segment_allocator.get_name(),
         num_extents);
  assert(num_extents > 0);
  return trans_intr::make_interruptible(
    write_record(t, std::move(record), std::move(pending_extents)));
}

SegmentedOolWriter::alloc_write_iertr::future<>
SegmentedOolWriter::alloc_write_ool_extents(
  Transaction& t,
  std::list<CachedExtentRef>& extents)
{
  if (extents.empty()) {
    return alloc_write_iertr::now();
  }
  return seastar::with_gate(write_guard, [this, &t, &extents] {
    return do_write(t, extents);
  });
}

void ExtentPlacementManager::init(
    JournalTrimmerImplRef &&trimmer,
    AsyncCleanerRef &&cleaner,
    AsyncCleanerRef &&cold_cleaner)
{
  writer_refs.clear();
  auto cold_segment_cleaner = dynamic_cast<SegmentCleaner*>(cold_cleaner.get());
  dynamic_max_rewrite_generation = MIN_COLD_GENERATION - 1;
  if (cold_segment_cleaner) {
    dynamic_max_rewrite_generation = MAX_REWRITE_GENERATION;
  }

  if (trimmer->get_backend_type() == backend_type_t::SEGMENTED) {
    auto segment_cleaner = dynamic_cast<SegmentCleaner*>(cleaner.get());
    ceph_assert(segment_cleaner != nullptr);
    auto num_writers = generation_to_writer(dynamic_max_rewrite_generation + 1);

    data_writers_by_gen.resize(num_writers, nullptr);
    for (rewrite_gen_t gen = OOL_GENERATION; gen < MIN_COLD_GENERATION; ++gen) {
      writer_refs.emplace_back(std::make_unique<SegmentedOolWriter>(
	    data_category_t::DATA, gen, *segment_cleaner,
            *ool_segment_seq_allocator));
      data_writers_by_gen[generation_to_writer(gen)] = writer_refs.back().get();
    }

    md_writers_by_gen.resize(num_writers, {});
    for (rewrite_gen_t gen = OOL_GENERATION; gen < MIN_COLD_GENERATION; ++gen) {
      writer_refs.emplace_back(std::make_unique<SegmentedOolWriter>(
	    data_category_t::METADATA, gen, *segment_cleaner,
            *ool_segment_seq_allocator));
      md_writers_by_gen[generation_to_writer(gen)] = writer_refs.back().get();
    }

    for (auto *device : segment_cleaner->get_segment_manager_group()
				       ->get_segment_managers()) {
      add_device(device);
    }
  } else {
    assert(trimmer->get_backend_type() == backend_type_t::RANDOM_BLOCK);
    auto rb_cleaner = dynamic_cast<RBMCleaner*>(cleaner.get());
    ceph_assert(rb_cleaner != nullptr);
    auto num_writers = generation_to_writer(dynamic_max_rewrite_generation + 1);
    data_writers_by_gen.resize(num_writers, nullptr);
    md_writers_by_gen.resize(num_writers, {});
    writer_refs.emplace_back(std::make_unique<RandomBlockOolWriter>(
	    rb_cleaner));
    // TODO: implement eviction in RBCleaner and introduce further writers
    data_writers_by_gen[generation_to_writer(OOL_GENERATION)] = writer_refs.back().get();
    md_writers_by_gen[generation_to_writer(OOL_GENERATION)] = writer_refs.back().get();
    for (auto *rb : rb_cleaner->get_rb_group()->get_rb_managers()) {
      add_device(rb->get_device());
    }
  }

  if (cold_segment_cleaner) {
    for (rewrite_gen_t gen = MIN_COLD_GENERATION; gen < REWRITE_GENERATIONS; ++gen) {
      writer_refs.emplace_back(std::make_unique<SegmentedOolWriter>(
            data_category_t::DATA, gen, *cold_segment_cleaner,
            *ool_segment_seq_allocator));
      data_writers_by_gen[generation_to_writer(gen)] = writer_refs.back().get();
    }
    for (rewrite_gen_t gen = MIN_COLD_GENERATION; gen < REWRITE_GENERATIONS; ++gen) {
      writer_refs.emplace_back(std::make_unique<SegmentedOolWriter>(
            data_category_t::METADATA, gen, *cold_segment_cleaner,
            *ool_segment_seq_allocator));
      md_writers_by_gen[generation_to_writer(gen)] = writer_refs.back().get();
    }
    for (auto *device : cold_segment_cleaner->get_segment_manager_group()
                                            ->get_segment_managers()) {
      add_device(device);
    }
  }

  background_process.init(std::move(trimmer),
                          std::move(cleaner),
                          std::move(cold_cleaner));
  if (cold_segment_cleaner) {
    ceph_assert(get_main_backend_type() == backend_type_t::SEGMENTED);
    ceph_assert(background_process.has_cold_tier());
  } else {
    ceph_assert(!background_process.has_cold_tier());
  }
}

void ExtentPlacementManager::set_primary_device(Device *device)
{
  ceph_assert(primary_device == nullptr);
  primary_device = device;
  ceph_assert(devices_by_id[device->get_device_id()] == device);
}

device_stats_t
ExtentPlacementManager::get_device_stats(
  const writer_stats_t &journal_stats,
  bool report_detail) const
{
  LOG_PREFIX(ExtentPlacementManager::get_device_stats);

  /*
   * RecordSubmitter::get_stats() isn't reentrant.
   * And refer to EPM::init() for the writers.
   */

  writer_stats_t main_stats = journal_stats;
  std::vector<writer_stats_t> main_writer_stats;
  using enum data_category_t;
  if (get_main_backend_type() == backend_type_t::SEGMENTED) {
    // 0. oolmdat
    main_writer_stats.emplace_back(
        get_writer(METADATA, OOL_GENERATION)->get_stats());
    main_stats.add(main_writer_stats.back());
    // 1. ooldata
    main_writer_stats.emplace_back(
        get_writer(DATA, OOL_GENERATION)->get_stats());
    main_stats.add(main_writer_stats.back());
    // 2. mainmdat
    main_writer_stats.emplace_back();
    for (rewrite_gen_t gen = MIN_REWRITE_GENERATION; gen < MIN_COLD_GENERATION; ++gen) {
      const auto &writer = get_writer(METADATA, gen);
      ceph_assert(writer->get_type() == backend_type_t::SEGMENTED);
      main_writer_stats.back().add(writer->get_stats());
    }
    main_stats.add(main_writer_stats.back());
    // 3. maindata
    main_writer_stats.emplace_back();
    for (rewrite_gen_t gen = MIN_REWRITE_GENERATION; gen < MIN_COLD_GENERATION; ++gen) {
      const auto &writer = get_writer(DATA, gen);
      ceph_assert(writer->get_type() == backend_type_t::SEGMENTED);
      main_writer_stats.back().add(writer->get_stats());
    }
    main_stats.add(main_writer_stats.back());
  } else { // RBM
    ceph_assert(get_main_backend_type() == backend_type_t::RANDOM_BLOCK);
    // In RBM, md_writer and data_wrtier share a single writer, so we only register
    // md_writer's writer here.
    main_writer_stats.emplace_back(
        get_writer(METADATA, OOL_GENERATION)->get_stats());
    main_stats.add(main_writer_stats.back());
  }

  writer_stats_t cold_stats = {};
  std::vector<writer_stats_t> cold_writer_stats;
  bool has_cold_tier = background_process.has_cold_tier();
  if (has_cold_tier) {
    // 0. coldmdat
    cold_writer_stats.emplace_back();
    for (rewrite_gen_t gen = MIN_COLD_GENERATION; gen < REWRITE_GENERATIONS; ++gen) {
      const auto &writer = get_writer(METADATA, gen);
      ceph_assert(writer->get_type() == backend_type_t::SEGMENTED);
      cold_writer_stats.back().add(writer->get_stats());
    }
    cold_stats.add(cold_writer_stats.back());
    // 1. colddata
    cold_writer_stats.emplace_back();
    for (rewrite_gen_t gen = MIN_COLD_GENERATION; gen < REWRITE_GENERATIONS; ++gen) {
      const auto &writer = get_writer(DATA, gen);
      ceph_assert(writer->get_type() == backend_type_t::SEGMENTED);
      cold_writer_stats.back().add(writer->get_stats());
    }
    cold_stats.add(cold_writer_stats.back());
  }

  auto now = seastar::lowres_clock::now();
  if (last_tp == seastar::lowres_clock::time_point::min()) {
    last_tp = now;
    return {};
  }
  std::chrono::duration<double> duration_d = now - last_tp;
  double seconds = duration_d.count();
  last_tp = now;

  if (report_detail) {
    std::ostringstream oss;
    auto report_writer_stats = [seconds, &oss](
        const char* name,
        const writer_stats_t& stats) {
      oss << "\n" << name << ": " << writer_stats_printer_t{seconds, stats};
    };
    report_writer_stats("tier-main", main_stats);
    report_writer_stats("  inline", journal_stats);
    if (get_main_backend_type() == backend_type_t::SEGMENTED) {
      report_writer_stats("  oolmdat", main_writer_stats[0]);
      report_writer_stats("  ooldata", main_writer_stats[1]);
      report_writer_stats("  mainmdat", main_writer_stats[2]);
      report_writer_stats("  maindata", main_writer_stats[3]);
    } else { // RBM
      report_writer_stats("  ool", main_writer_stats[0]);
    }
    if (has_cold_tier) {
      report_writer_stats("tier-cold", cold_stats);
      report_writer_stats("  coldmdat", cold_writer_stats[0]);
      report_writer_stats("  colddata", cold_writer_stats[1]);
    }

    auto report_by_src = [seconds, has_cold_tier, &oss,
                          &journal_stats,
                          &main_writer_stats,
                          &cold_writer_stats](transaction_type_t src) {
      auto t_stats = get_by_src(journal_stats.stats_by_src, src);
      for (const auto &writer_stats : main_writer_stats) {
        t_stats += get_by_src(writer_stats.stats_by_src, src);
      }
      for (const auto &writer_stats : cold_writer_stats) {
        t_stats += get_by_src(writer_stats.stats_by_src, src);
      }
      if (src == transaction_type_t::READ) {
        ceph_assert(t_stats.is_empty());
        return;
      }
      oss << "\n" << src << ": "
          << tw_stats_printer_t{seconds, t_stats};

      auto report_tw_stats = [seconds, src, &oss](
          const char* name,
          const writer_stats_t& stats) {
        const auto& tw_stats = get_by_src(stats.stats_by_src, src);
        if (tw_stats.is_empty()) {
          return;
        }
        oss << "\n  " << name << ": "
            << tw_stats_printer_t{seconds, tw_stats};
      };
      report_tw_stats("inline", journal_stats);
      report_tw_stats("oolmdat", main_writer_stats[0]);
      report_tw_stats("ooldata", main_writer_stats[1]);
      report_tw_stats("mainmdat", main_writer_stats[2]);
      report_tw_stats("maindata", main_writer_stats[3]);
      if (has_cold_tier) {
        report_tw_stats("coldmdat", cold_writer_stats[0]);
        report_tw_stats("colddata", cold_writer_stats[1]);
      }
    };
    for (uint8_t _src=0; _src<TRANSACTION_TYPE_MAX; ++_src) {
      auto src = static_cast<transaction_type_t>(_src);
      report_by_src(src);
    }

    INFO("{}", oss.str());
  }

  main_stats.add(cold_stats);
  return {main_stats.io_depth_stats.num_io,
          main_stats.io_depth_stats.num_io_grouped,
          main_stats.get_total_bytes()};
}

ExtentPlacementManager::open_ertr::future<>
ExtentPlacementManager::open_for_write()
{
  LOG_PREFIX(ExtentPlacementManager::open_for_write);
  INFO("started with {} devices", num_devices);
  ceph_assert(primary_device != nullptr);
  return crimson::do_for_each(data_writers_by_gen, [](auto &writer) {
    if (writer) {
      return writer->open();
    }
    return open_ertr::now();
  }).safe_then([this] {
    return crimson::do_for_each(md_writers_by_gen, [](auto &writer) {
      if (writer) {
	return writer->open();
      }
      return open_ertr::now();
    });
  });
}

ExtentPlacementManager::dispatch_result_t
ExtentPlacementManager::dispatch_delayed_extents(Transaction &t)
{
  dispatch_result_t res;
  res.delayed_extents = t.get_delayed_alloc_list();

  // init projected usage
  for (auto &extent : t.get_inline_block_list()) {
    if (extent->is_valid()) {
      res.usage.inline_usage += extent->get_length();
      res.usage.cleaner_usage.main_usage += extent->get_length();
    }
  }

  for (auto &extent : res.delayed_extents) {
    if (dispatch_delayed_extent(extent)) {
      res.usage.inline_usage += extent->get_length();
      res.usage.cleaner_usage.main_usage += extent->get_length();
      t.mark_delayed_extent_inline(extent);
    } else {
      if (extent->get_rewrite_generation() < MIN_COLD_GENERATION) {
        res.usage.cleaner_usage.main_usage += extent->get_length();
      } else {
        assert(background_process.has_cold_tier());
        res.usage.cleaner_usage.cold_ool_usage += extent->get_length();
      }
      t.mark_delayed_extent_ool(extent);
      auto writer_ptr = get_writer(
          extent->get_user_hint(),
          get_extent_category(extent->get_type()),
          extent->get_rewrite_generation());
      res.alloc_map[writer_ptr].emplace_back(extent);
    }
  }
  return res;
}

ExtentPlacementManager::alloc_paddr_iertr::future<>
ExtentPlacementManager::write_delayed_ool_extents(
    Transaction& t,
    extents_by_writer_t& alloc_map) {
  return trans_intr::do_for_each(alloc_map, [&t](auto& p) {
    auto writer = p.first;
    auto& extents = p.second;
#ifndef NDEBUG
    std::for_each(
      extents.begin(),
      extents.end(),
      [](auto &extent) {
      assert(extent->is_valid());
    });
#endif
    return writer->alloc_write_ool_extents(t, extents);
  });
}

ExtentPlacementManager::alloc_paddr_iertr::future<>
ExtentPlacementManager::write_preallocated_ool_extents(
    Transaction &t,
    std::list<CachedExtentRef> extents)
{
  LOG_PREFIX(ExtentPlacementManager::write_preallocated_ool_extents);
  DEBUGT("start with {} allocated extents",
         t, extents.size());
  assert(writer_refs.size());
  return seastar::do_with(
      std::map<ExtentOolWriter*, std::list<CachedExtentRef>>(),
      [this, &t, extents=std::move(extents)](auto& alloc_map) {
    for (auto& extent : extents) {
      auto writer_ptr = get_writer(
          extent->get_user_hint(),
          get_extent_category(extent->get_type()),
          extent->get_rewrite_generation());
      alloc_map[writer_ptr].emplace_back(extent);
    }
    return trans_intr::do_for_each(alloc_map, [&t](auto& p) {
      auto writer = p.first;
      auto& extents = p.second;
      return writer->alloc_write_ool_extents(t, extents);
    });
  });
}

ExtentPlacementManager::close_ertr::future<>
ExtentPlacementManager::close()
{
  LOG_PREFIX(ExtentPlacementManager::close);
  INFO("started");
  return crimson::do_for_each(data_writers_by_gen, [](auto &writer) {
    if (writer) {
      return writer->close();
    }
    return close_ertr::now();
  }).safe_then([this] {
    return crimson::do_for_each(md_writers_by_gen, [](auto &writer) {
      if (writer) {
	return writer->close();
      }
      return close_ertr::now();
    });
  });
}

void ExtentPlacementManager::BackgroundProcess::log_state(const char *caller) const
{
  LOG_PREFIX(BackgroundProcess::log_state);
  DEBUG("caller {}, {}, {}",
        caller,
        JournalTrimmerImpl::stat_printer_t{*trimmer, true},
        AsyncCleaner::stat_printer_t{*main_cleaner, true});
  if (has_cold_tier()) {
    DEBUG("caller {}, cold_cleaner: {}",
          caller,
          AsyncCleaner::stat_printer_t{*cold_cleaner, true});
  }
}

void ExtentPlacementManager::BackgroundProcess::start_background()
{
  LOG_PREFIX(BackgroundProcess::start_background);
  INFO("{}, {}",
       JournalTrimmerImpl::stat_printer_t{*trimmer, true},
       AsyncCleaner::stat_printer_t{*main_cleaner, true});
  if (has_cold_tier()) {
    INFO("cold_cleaner: {}",
         AsyncCleaner::stat_printer_t{*cold_cleaner, true});
  }
  ceph_assert(trimmer->check_is_ready());
  ceph_assert(state == state_t::SCAN_SPACE);
  assert(!is_running());
  process_join = seastar::now();
  state = state_t::RUNNING;
  assert(is_running());
  process_join = run();
}

seastar::future<>
ExtentPlacementManager::BackgroundProcess::stop_background()
{
  LOG_PREFIX(BackgroundProcess::stop_background);
  return seastar::futurize_invoke([this, FNAME] {
    if (!is_running()) {
      if (state != state_t::HALT) {
        INFO("isn't RUNNING or HALT, STOP");
        state = state_t::STOP;
      } else {
        INFO("isn't RUNNING, already HALT");
      }
      return seastar::now();
    }
    INFO("is RUNNING, going to HALT...");
    auto ret = std::move(*process_join);
    process_join.reset();
    state = state_t::HALT;
    assert(!is_running());
    do_wake_background();
    return ret;
  }).then([this, FNAME] {
    INFO("done, {}, {}",
         JournalTrimmerImpl::stat_printer_t{*trimmer, true},
         AsyncCleaner::stat_printer_t{*main_cleaner, true});
    if (has_cold_tier()) {
      INFO("done, cold_cleaner: {}",
           AsyncCleaner::stat_printer_t{*cold_cleaner, true});
    }
  });
}

seastar::future<>
ExtentPlacementManager::BackgroundProcess::run_until_halt()
{
  // unit test only
  LOG_PREFIX(BackgroundProcess::run_until_halt);
  ceph_assert(state == state_t::HALT);
  assert(!is_running());
  if (is_running_until_halt) {
    WARN("already running");
    return seastar::now();
  }
  INFO("started...");
  is_running_until_halt = true;
  return seastar::do_until(
    [this] {
      log_state("run_until_halt");
      assert(is_running_until_halt);
      if (background_should_run()) {
        return false;
      } else {
        is_running_until_halt = false;
        return true;
      }
    },
    [this] {
      return do_background_cycle();
    }
  ).finally([FNAME] {
    INFO("finished");
  });
}

seastar::future<>
ExtentPlacementManager::BackgroundProcess::reserve_projected_usage(
    io_usage_t usage)
{
  if (!is_ready()) {
    return seastar::now();
  }
  ceph_assert(!blocking_io);
  // The pipeline configuration prevents another IO from entering
  // prepare until the prior one exits and clears this.
  ++stats.io_count;

  auto res = try_reserve_io(usage);
  if (res.is_successful()) {
    return seastar::now();
  } else {
    LOG_PREFIX(BackgroundProcess::reserve_projected_usage);
    DEBUG("blocked: inline={}, main={}, cold={}, usage={}",
          res.reserve_inline_success,
          res.cleaner_result.reserve_main_success,
          res.cleaner_result.reserve_cold_success,
          usage);
    abort_io_usage(usage, res);
    if (!res.reserve_inline_success) {
      ++stats.io_blocked_count_trim;
    }
    if (!res.cleaner_result.is_successful()) {
      ++stats.io_blocked_count_clean;
    }
    ++stats.io_blocking_num;
    ++stats.io_blocked_count;
    stats.io_blocked_sum += stats.io_blocking_num;

    blocking_io = seastar::promise<>();
    return blocking_io->get_future(
    ).then([this, usage, FNAME] {
      return seastar::repeat([this, usage, FNAME] {
        ceph_assert(!blocking_io);
        auto res = try_reserve_io(usage);
        if (res.is_successful()) {
          DEBUG("unblocked");
          assert(stats.io_blocking_num == 1);
          --stats.io_blocking_num;
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        } else {
          DEBUG("blocked again: inline={}, main={}, cold={}, usage={}",
                res.reserve_inline_success,
                res.cleaner_result.reserve_main_success,
                res.cleaner_result.reserve_cold_success,
                usage);
          abort_io_usage(usage, res);
          blocking_io = seastar::promise<>();
          return blocking_io->get_future(
          ).then([] {
            return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::no);
          });
        }
      });
    });
  }
}

void
ExtentPlacementManager::BackgroundProcess::maybe_wake_blocked_io()
{
  if (!is_ready()) {
    return;
  }
  LOG_PREFIX(ExtentPlacementManager::maybe_wake_blocked_io);
  if (!should_block_io() && blocking_io) {
    DEBUG("");
    blocking_io->set_value();
    blocking_io = std::nullopt;
  }
}

seastar::future<>
ExtentPlacementManager::BackgroundProcess::run()
{
  assert(is_running());
  return seastar::repeat([this] {
    if (!is_running()) {
      log_state("run(exit)");
      return seastar::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::yes);
    }
    return seastar::futurize_invoke([this] {
      if (background_should_run()) {
        log_state("run(background)");
        return do_background_cycle();
      } else {
        log_state("run(block)");
        ceph_assert(!blocking_background);
        blocking_background = seastar::promise<>();
        return blocking_background->get_future();
      }
    }).then([] {
      return seastar::stop_iteration::no;
    });
  });
}

/**
 * Reservation Process
 *
 * Most of transctions need to reserve its space usage before performing the
 * ool writes and committing transactions. If the space reservation is
 * unsuccessful, the current transaction is blocked, and waits for new
 * background transactions to finish.
 *
 * The following are the reservation requirements for each transaction type:
 * 1. MUTATE transaction:
 *      (1) inline usage on the trimmer,
 *      (2) inline usage with OOL usage on the main cleaner,
 *      (3) cold OOL usage to the cold cleaner(if it exists).
 * 2. TRIM_DIRTY/TRIM_ALLOC transaction:
 *      (1) all extents usage on the main cleaner,
 *      (2) usage on the cold cleaner(if it exists)
 * 3. CLEANER_MAIN:
 *      (1) cleaned extents size on the cold cleaner(if it exists).
 * 4. CLEANER_COLD transction does not require space reservation.
 *
 * The reserve implementation should satisfy the following conditions:
 * 1. The reservation should be atomic. If a reservation involves several reservations,
 *    such as the MUTATE transaction that needs to reserve space on both the trimmer
 *    and cleaner at the same time, the successful condition is that all of its
 *    sub-reservations succeed. If one or more operations fail, the entire reservation
 *    fails, and the successful operation should be reverted.
 * 2. The reserve/block relationship should form a DAG to avoid deadlock. For example,
 *    TRIM_ALLOC transaction might be blocked by cleaner due to the failure of reserving
 *    on the cleaner. In such cases, the cleaner must not reserve space on the trimmer
 *    since the trimmer is already blocked by itself.
 *
 * Finally the reserve relationship can be represented as follows:
 *
 *    +-------------------------+----------------+
 *    |                         |                |
 *    |                         v                v
 * MUTATE ---> TRIM_* ---> CLEANER_MAIN ---> CLEANER_COLD
 *              |                                ^
 *              |                                |
 *              +--------------------------------+
 */
bool ExtentPlacementManager::BackgroundProcess::try_reserve_cold(std::size_t usage)
{
  if (has_cold_tier()) {
    return cold_cleaner->try_reserve_projected_usage(usage);
  } else {
    assert(usage == 0);
    return true;
  }
}
void ExtentPlacementManager::BackgroundProcess::abort_cold_usage(
  std::size_t usage, bool success)
{
  if (has_cold_tier() && success) {
    cold_cleaner->release_projected_usage(usage);
  }
}

reserve_cleaner_result_t
ExtentPlacementManager::BackgroundProcess::try_reserve_cleaner(
  const cleaner_usage_t &usage)
{
  return {
    main_cleaner->try_reserve_projected_usage(usage.main_usage),
    try_reserve_cold(usage.cold_ool_usage)
  };
}

void ExtentPlacementManager::BackgroundProcess::abort_cleaner_usage(
  const cleaner_usage_t &usage,
  const reserve_cleaner_result_t &result)
{
  if (result.reserve_main_success) {
    main_cleaner->release_projected_usage(usage.main_usage);
  }
  abort_cold_usage(usage.cold_ool_usage, result.reserve_cold_success);
}

reserve_io_result_t
ExtentPlacementManager::BackgroundProcess::try_reserve_io(
  const io_usage_t &usage)
{
  return {
    trimmer->try_reserve_inline_usage(usage.inline_usage),
    try_reserve_cleaner(usage.cleaner_usage)
  };
}

void ExtentPlacementManager::BackgroundProcess::abort_io_usage(
  const io_usage_t &usage,
  const reserve_io_result_t &result)
{
  if (result.reserve_inline_success) {
    trimmer->release_inline_usage(usage.inline_usage);
  }
  abort_cleaner_usage(usage.cleaner_usage, result.cleaner_result);
}

seastar::future<>
ExtentPlacementManager::BackgroundProcess::do_background_cycle()
{
  LOG_PREFIX(BackgroundProcess::do_background_cycle);
  assert(is_ready());
  bool should_trim = trimmer->should_trim();
  bool proceed_trim = false;
  auto trim_size = trimmer->get_trim_size_per_cycle();
  cleaner_usage_t trim_usage{
    trim_size,
    // We take a cautious policy here that the trimmer also reserves
    // the max value on cold cleaner even if no extents will be rewritten
    // to the cold tier. Cleaner also takes the same policy.
    // The reason is that we don't know the exact value of reservation until
    // the construction of trimmer transaction completes after which the reservation
    // might fail then the trimmer is possible to be invalidated by cleaner.
    // Reserving the max size at first could help us avoid these trouble.
    has_cold_tier() ? trim_size : 0
  };

  reserve_cleaner_result_t trim_reserve_res;
  if (should_trim) {
    trim_reserve_res = try_reserve_cleaner(trim_usage);
    if (trim_reserve_res.is_successful()) {
      proceed_trim = true;
    } else {
      abort_cleaner_usage(trim_usage, trim_reserve_res);
    }
  }

  if (proceed_trim) {
    DEBUG("started trimming...");
    return trimmer->trim(
    ).finally([this, trim_usage, FNAME] {
      DEBUG("finished trimming");
      abort_cleaner_usage(trim_usage, {true, true});
    });
  } else {
    assert(!proceed_trim);
    bool should_clean_main_for_trim =
      should_trim && !trim_reserve_res.reserve_main_success;
    bool should_clean_main =
      main_cleaner_should_run() || should_clean_main_for_trim;
    bool proceed_clean_main = false;
    auto main_cold_usage = main_cleaner->get_reclaim_size_per_cycle();
    if (should_clean_main) {
      if (has_cold_tier()) {
        proceed_clean_main = try_reserve_cold(main_cold_usage);
      } else {
        proceed_clean_main = true;
      }
    }

    bool should_clean_cold_for_trim =
      should_trim && !trim_reserve_res.reserve_cold_success;
    bool should_clean_cold_for_main =
      should_clean_main && !proceed_clean_main;
    bool proceed_clean_cold = false;
    if (has_cold_tier() &&
        (cold_cleaner->should_clean_space() ||
         should_clean_cold_for_trim ||
         should_clean_cold_for_main)) {
      proceed_clean_cold = true;
    }

    if (!proceed_clean_main && !proceed_clean_cold) {
      ceph_abort("no background process will start");
    }
    return seastar::when_all(
      [this, FNAME, proceed_clean_main,
       should_clean_main_for_trim, main_cold_usage] {
        if (!proceed_clean_main) {
          return seastar::now();
        }
        DEBUG("started clean main... "
              "should_clean={}, for_trim={}, for_fast_evict={}",
              main_cleaner->should_clean_space(),
              should_clean_main_for_trim,
              main_cleaner_should_fast_evict());
        return main_cleaner->clean_space(
        ).handle_error(
          crimson::ct_error::assert_all{
            "do_background_cycle encountered invalid error in main clean_space"
          }
        ).finally([this, main_cold_usage, FNAME] {
          DEBUG("finished clean main");
          abort_cold_usage(main_cold_usage, true);
        });
      },
      [this, FNAME, proceed_clean_cold,
       should_clean_cold_for_trim, should_clean_cold_for_main] {
        if (!proceed_clean_cold) {
          return seastar::now();
        }
        DEBUG("started clean cold... "
              "should_clean={}, for_trim={}, for_main={}",
              cold_cleaner->should_clean_space(),
              should_clean_cold_for_trim,
              should_clean_cold_for_main);
        return cold_cleaner->clean_space(
        ).handle_error(
          crimson::ct_error::assert_all{
            "do_background_cycle encountered invalid error in cold clean_space"
          }
        ).finally([FNAME] {
          DEBUG("finished clean cold");
        });
      }
    ).discard_result();
  }
}

void ExtentPlacementManager::BackgroundProcess::register_metrics()
{
  namespace sm = seastar::metrics;
  metrics.add_group("background_process", {
    sm::make_counter("io_count", stats.io_count,
                     sm::description("the sum of IOs")),
    sm::make_counter("io_blocked_count", stats.io_blocked_count,
                     sm::description("IOs that are blocked by gc")),
    sm::make_counter("io_blocked_count_trim", stats.io_blocked_count_trim,
                     sm::description("IOs that are blocked by trimming")),
    sm::make_counter("io_blocked_count_clean", stats.io_blocked_count_clean,
                     sm::description("IOs that are blocked by cleaning")),
    sm::make_counter("io_blocked_sum", stats.io_blocked_sum,
                     sm::description("the sum of blocking IOs"))
  });
}

RandomBlockOolWriter::alloc_write_iertr::future<>
RandomBlockOolWriter::alloc_write_ool_extents(
  Transaction& t,
  std::list<CachedExtentRef>& extents)
{
  if (extents.empty()) {
    return alloc_write_iertr::now();
  }
  return seastar::with_gate(write_guard, [this, &t, &extents] {
    return do_write(t, extents);
  });
}

RandomBlockOolWriter::alloc_write_iertr::future<>
RandomBlockOolWriter::do_write(
  Transaction& t,
  std::list<CachedExtentRef>& extents)
{
  LOG_PREFIX(RandomBlockOolWriter::do_write);
  assert(!extents.empty());
  DEBUGT("start with {} allocated extents",
         t, extents.size());
  return trans_intr::do_for_each(extents,
    [this, &t, FNAME](auto& ex) {
    auto paddr = ex->get_paddr();
    assert(paddr.is_absolute());
    RandomBlockManager * rbm = rb_cleaner->get_rbm(paddr); 
    assert(rbm);
    TRACE("extent {}, allocated addr {}", fmt::ptr(ex.get()), paddr);
    auto& stats = t.get_ool_write_stats();
    stats.extents.num += 1;
    stats.extents.bytes += ex->get_length();
    stats.num_records += 1;

    ex->prepare_write();
    extent_len_t offset = 0;
    bufferptr bp;
    if (can_inplace_rewrite(t, ex)) {
      assert(ex->is_logical());
      auto r = ex->template cast<LogicalCachedExtent>()->get_modified_region();
      ceph_assert(r.has_value());
      offset = p2align(r->offset, rbm->get_block_size());
      extent_len_t len =
	p2roundup(r->offset + r->len, rbm->get_block_size()) - offset;
      bp = ceph::bufferptr(ex->get_bptr(), offset, len);
    } else {
      bp = ex->get_bptr();
      auto& trans_stats = get_by_src(w_stats.stats_by_src, t.get_src());
      ++(trans_stats.num_records);
      trans_stats.data_bytes += ex->get_length();
      w_stats.data_bytes += ex->get_length();
    }
    return trans_intr::make_interruptible(
      rbm->write(paddr + offset,
	bp
      ).handle_error(
	alloc_write_iertr::pass_further{},
	crimson::ct_error::assert_all{
	  "Invalid error when writing record"}
      )
    ).si_then([this, &t, &ex, paddr, FNAME] {
      TRACET("ool extent written at {} -- {}",
	     t, paddr, *ex);
      if (ex->is_initial_pending()) {
	t.mark_allocated_extent_ool(ex);
      } else if (can_inplace_rewrite(t, ex)) {
        assert(ex->is_logical());
	t.mark_inplace_rewrite_extent_ool(
          ex->template cast<LogicalCachedExtent>());
      } else {
	ceph_assert("impossible");
      }
      return alloc_write_iertr::now();
    });
  });
}

}
