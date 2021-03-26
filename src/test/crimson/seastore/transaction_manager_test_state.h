// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <random>

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"
#include "crimson/os/seastore/seastore.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/collection_manager/flat_collection_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

class EphemeralTestState {
protected:
  std::unique_ptr<segment_manager::EphemeralSegmentManager> segment_manager;

  EphemeralTestState()
    : segment_manager(segment_manager::create_test_ephemeral()) {}

  virtual void _init() = 0;
  void init() {
    _init();
  }

  virtual void _destroy() = 0;
  void destroy() {
    _destroy();
  }

  virtual seastar::future<> _teardown() = 0;
  virtual seastar::future<> _mkfs() = 0;
  virtual seastar::future<> _mount() = 0;

  void restart() {
    _teardown().get0();
    destroy();
    static_cast<segment_manager::EphemeralSegmentManager*>(&*segment_manager)->remount();
    init();
    _mount().get0();
  }

  seastar::future<> tm_setup() {
    init();
    return segment_manager->init(
    ).safe_then([this] {
      return _mkfs();
    }).safe_then([this] {
      return _teardown();
    }).safe_then([this] {
      destroy();
      static_cast<segment_manager::EphemeralSegmentManager*>(
	&*segment_manager)->remount();
      init();
      return _mount();
    }).handle_error(crimson::ct_error::assert_all{});
  }

  seastar::future<> tm_teardown() {
    return _teardown();
  }
};

auto get_transaction_manager(
  SegmentManager &segment_manager
) {
  auto segment_cleaner = std::make_unique<SegmentCleaner>(
    SegmentCleaner::config_t::default_from_segment_manager(
      segment_manager),
    true);
  auto journal = std::make_unique<Journal>(segment_manager);
  auto cache = std::make_unique<Cache>(segment_manager);
  auto lba_manager = lba_manager::create_lba_manager(segment_manager, *cache);

  journal->set_segment_provider(&*segment_cleaner);

  auto ret = std::make_unique<TransactionManager>(
    segment_manager,
    std::move(segment_cleaner),
    std::move(journal),
    std::move(cache),
    std::move(lba_manager));
  return ret;
}

auto get_seastore(
  SegmentManager &segment_manager
) {
  auto segment_cleaner = std::make_unique<SegmentCleaner>(
    SegmentCleaner::config_t::default_from_segment_manager(
      segment_manager),
    true);
  auto journal = std::make_unique<Journal>(segment_manager);
  auto cache = std::make_unique<Cache>(segment_manager);
  auto lba_manager = lba_manager::create_lba_manager(segment_manager, *cache);

  journal->set_segment_provider(&*segment_cleaner);

  auto tm = get_transaction_manager(segment_manager);
  auto cm = std::make_unique<collection_manager::FlatCollectionManager>(*tm);
  return std::make_unique<SeaStore>(
    std::move(tm),
    std::move(cm),
    std::make_unique<crimson::os::seastore::onode::FLTreeOnodeManager>(*tm));
}


class TMTestState : public EphemeralTestState {
protected:
  std::unique_ptr<TransactionManager> tm;
  LBAManager *lba_manager;
  SegmentCleaner *segment_cleaner;

  TMTestState() : EphemeralTestState() {}

  virtual void _init() {
    tm = get_transaction_manager(*segment_manager);
    segment_cleaner = tm->get_segment_cleaner();
    lba_manager = tm->get_lba_manager();
  }

  virtual void _destroy() {
    segment_cleaner = nullptr;
    lba_manager = nullptr;
    tm.reset();
  }

  virtual seastar::future<> _teardown() {
    return tm->close(
    ).handle_error(
      crimson::ct_error::assert_all{"Error in teardown"}
    );
  }

  virtual seastar::future<> _mount() {
    return tm->mount(
    ).handle_error(
      crimson::ct_error::assert_all{"Error in mount"}
    ).then([this] {
      return segment_cleaner->stop();
    }).then([this] {
      return segment_cleaner->run_until_halt();
    });
  }

  virtual seastar::future<> _mkfs() {
    return tm->mkfs(
    ).handle_error(
      crimson::ct_error::assert_all{"Error in teardown"}
    );
  }
};

class SeaStoreTestState : public EphemeralTestState {
protected:
  std::unique_ptr<SeaStore> seastore;

  SeaStoreTestState() : EphemeralTestState() {}

  virtual void _init() {
    seastore = get_seastore(*segment_manager);
  }

  virtual void _destroy() {
    seastore.reset();
  }

  virtual seastar::future<> _teardown() {
    return seastore->stop();
  }

  virtual seastar::future<> _mount() {
    return seastore->mount();
  }

  virtual seastar::future<> _mkfs() {
    return seastore->mkfs(uuid_d{});
  }
};
