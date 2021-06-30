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
  SegmentManager &segment_manager) {
  auto segment_cleaner = std::make_unique<SegmentCleaner>(
    SegmentCleaner::config_t::get_default(),
    true);
  auto journal = std::make_unique<Journal>(segment_manager);
  auto cache = std::make_unique<Cache>(segment_manager);
  auto lba_manager = lba_manager::create_lba_manager(segment_manager, *cache);

  journal->set_segment_provider(&*segment_cleaner);

  return std::make_unique<TransactionManager>(
    segment_manager,
    std::move(segment_cleaner),
    std::move(journal),
    std::move(cache),
    std::move(lba_manager));
}

auto get_seastore(SegmentManagerRef sm) {
  auto tm = get_transaction_manager(*sm);
  auto cm = std::make_unique<collection_manager::FlatCollectionManager>(*tm);
  return std::make_unique<SeaStore>(
    std::move(sm),
    std::move(tm),
    std::move(cm),
    std::make_unique<crimson::os::seastore::onode::FLTreeOnodeManager>(*tm));
}


class TMTestState : public EphemeralTestState {
protected:
  TransactionManagerRef tm;
  InterruptedTransactionManager itm;
  LBAManager *lba_manager;
  SegmentCleaner *segment_cleaner;

  TMTestState() : EphemeralTestState() {}

  virtual void _init() {
    tm = get_transaction_manager(*segment_manager);
    itm = InterruptedTransactionManager(*tm);
    segment_cleaner = tm->get_segment_cleaner();
    lba_manager = tm->get_lba_manager();
  }

  virtual void _destroy() {
    segment_cleaner = nullptr;
    lba_manager = nullptr;
    itm.reset();
    tm.reset();
  }

  virtual seastar::future<> _teardown() {
    return tm->close().safe_then([this] {
      _destroy();
      return seastar::now();
    }).handle_error(
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

  auto submit_transaction_fut(Transaction &t) {
    return with_trans_intr(
      t,
      [this](auto &t) {
	return tm->submit_transaction(t);
      });
  }

  void submit_transaction(TransactionRef t) {
    submit_transaction_fut(*t).unsafe_get0();
    segment_cleaner->run_until_halt().get0();
  }
};

class TestSegmentManagerWrapper final : public SegmentManager {
  SegmentManager &sm;
public:
  TestSegmentManagerWrapper(SegmentManager &sm) : sm(sm) {}

  mount_ret mount() final {
    return mount_ertr::now(); // we handle this above
  }

  mkfs_ret mkfs(seastore_meta_t c) final {
    return mkfs_ertr::now(); // we handle this above
  }


  open_ertr::future<SegmentRef> open(segment_id_t id) final {
    return sm.open(id);
  }

  release_ertr::future<> release(segment_id_t id) final {
    return sm.release(id);
  }

  read_ertr::future<> read(
    paddr_t addr, size_t len, ceph::bufferptr &out) final {
    return sm.read(addr, len, out);
  }

  size_t get_size() const final { return sm.get_size(); }
  segment_off_t get_block_size() const final { return sm.get_block_size(); }
  segment_off_t get_segment_size() const final {
    return sm.get_segment_size();
  }
  const seastore_meta_t &get_meta() const final {
    return sm.get_meta();
  }
  ~TestSegmentManagerWrapper() final {}
};

class SeaStoreTestState : public EphemeralTestState {
protected:
  std::unique_ptr<SeaStore> seastore;

  SeaStoreTestState() : EphemeralTestState() {}

  virtual void _init() {
    seastore = get_seastore(
      std::make_unique<TestSegmentManagerWrapper>(*segment_manager));
  }

  virtual void _destroy() {
    seastore.reset();
  }

  virtual seastar::future<> _teardown() {
    return seastore->umount().then([this] {
      seastore.reset();
    });
  }

  virtual seastar::future<> _mount() {
    return seastore->mount();
  }

  virtual seastar::future<> _mkfs() {
    return seastore->mkfs(uuid_d{});
  }
};
