// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <random>

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"
#include "crimson/os/seastore/segment_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

class TMTestState {
protected:
  std::unique_ptr<segment_manager::EphemeralSegmentManager> segment_manager;
  std::unique_ptr<SegmentCleaner> segment_cleaner;
  std::unique_ptr<Journal> journal;
  std::unique_ptr<Cache> cache;
  LBAManagerRef lba_manager;
  std::unique_ptr<TransactionManager> tm;

  TMTestState()
    : segment_manager(segment_manager::create_test_ephemeral()) {
    init();
  }

  void init() {
    segment_cleaner = std::make_unique<SegmentCleaner>(
      SegmentCleaner::config_t::default_from_segment_manager(
	*segment_manager),
      true);
    journal = std::make_unique<Journal>(*segment_manager);
    cache = std::make_unique<Cache>(*segment_manager);
    lba_manager = lba_manager::create_lba_manager(*segment_manager, *cache);
    tm = std::make_unique<TransactionManager>(
      *segment_manager, *segment_cleaner, *journal, *cache, *lba_manager);

    journal->set_segment_provider(&*segment_cleaner);
    segment_cleaner->set_extent_callback(&*tm);
  }

  void destroy() {
    tm.reset();
    lba_manager.reset();
    cache.reset();
    journal.reset();
    segment_cleaner.reset();
  }

  seastar::future<> tm_setup() {
    return segment_manager->init(
    ).safe_then([this] {
      return tm->mkfs();
    }).safe_then([this] {
      return tm->close();
    }).safe_then([this] {
      destroy();
      static_cast<segment_manager::EphemeralSegmentManager*>(
	&*segment_manager)->remount();
      init();
      return tm->mount();
    }).handle_error(crimson::ct_error::assert_all{});
  }

  seastar::future<> tm_teardown() {
    return tm->close(
    ).handle_error(crimson::ct_error::assert_all{});
  }
};
