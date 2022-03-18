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
  segment_manager::EphemeralSegmentManagerRef segment_manager;

  EphemeralTestState() = default;

  virtual void _init() = 0;
  void init() {
    _init();
  }

  virtual void _destroy() = 0;
  void destroy() {
    _destroy();
  }

  virtual seastar::future<> _teardown() = 0;
  virtual FuturizedStore::mkfs_ertr::future<> _mkfs() = 0;
  virtual FuturizedStore::mount_ertr::future<> _mount() = 0;

  void restart() {
    _teardown().get0();
    destroy();
    static_cast<segment_manager::EphemeralSegmentManager*>(&*segment_manager)->remount();
    init();
    _mount().handle_error(crimson::ct_error::assert_all{}).get0();
  }

  seastar::future<> tm_setup() {
    segment_manager = segment_manager::create_test_ephemeral();
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
    return _teardown().then([this] {
      segment_manager.reset();
    });
  }
};

auto get_seastore(SeaStore::MDStoreRef mdstore, SegmentManagerRef sm) {
  auto tm = make_transaction_manager(*sm, true);
  auto cm = std::make_unique<collection_manager::FlatCollectionManager>(*tm);
  return std::make_unique<SeaStore>(
    "",
    std::move(mdstore),
    std::move(sm),
    std::move(tm),
    std::move(cm),
    std::make_unique<crimson::os::seastore::onode::FLTreeOnodeManager>(*tm));
}


class TMTestState : public EphemeralTestState {
protected:
  TransactionManagerRef tm;
  LBAManager *lba_manager;
  SegmentCleaner *segment_cleaner;

  TMTestState() : EphemeralTestState() {}

  virtual void _init() override {
    tm = make_transaction_manager(*segment_manager, true);
    tm->add_segment_manager(segment_manager.get());
    segment_cleaner = tm->get_segment_cleaner();
    lba_manager = tm->get_lba_manager();
  }

  virtual void _destroy() override {
    segment_cleaner = nullptr;
    lba_manager = nullptr;
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

  virtual FuturizedStore::mount_ertr::future<> _mount() {
    return tm->mount(
    ).handle_error(
      crimson::ct_error::assert_all{"Error in mount"}
    ).then([this] {
      return segment_cleaner->stop();
    }).then([this] {
      return segment_cleaner->run_until_halt();
    });
  }

  virtual FuturizedStore::mkfs_ertr::future<> _mkfs() {
    return tm->mkfs(
    ).handle_error(
      crimson::ct_error::assert_all{"Error in teardown"}
    );
  }

  auto create_mutate_transaction() {
    return tm->create_transaction(
        Transaction::src_t::MUTATE, "test_mutate");
  }

  auto create_read_transaction() {
    return tm->create_transaction(
        Transaction::src_t::READ, "test_read");
  }

  auto create_weak_transaction() {
    return tm->create_weak_transaction(
        Transaction::src_t::READ, "test_read_weak");
  }

  auto submit_transaction_fut2(Transaction& t) {
    return tm->submit_transaction(t);
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
  device_id_t device_id = 0;
  secondary_device_set_t set;
public:
  TestSegmentManagerWrapper(
    SegmentManager &sm,
    device_id_t device_id = 0)
    : sm(sm), device_id(device_id) {}

  device_id_t get_device_id() const {
    return device_id;
  }

  mount_ret mount() final {
    return mount_ertr::now(); // we handle this above
  }

  mkfs_ret mkfs(segment_manager_config_t c) final {
    return mkfs_ertr::now(); // we handle this above
  }

  close_ertr::future<> close() final {
    return sm.close();
  }

  secondary_device_set_t& get_secondary_devices() final {
    return sm.get_secondary_devices();
  }

  device_spec_t get_device_spec() const final {
    return sm.get_device_spec();
  }

  magic_t get_magic() const final {
    return sm.get_magic();
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
  seastore_off_t get_block_size() const final { return sm.get_block_size(); }
  seastore_off_t get_segment_size() const final {
    return sm.get_segment_size();
  }
  const seastore_meta_t &get_meta() const final {
    return sm.get_meta();
  }
  ~TestSegmentManagerWrapper() final {}
};

class SeaStoreTestState : public EphemeralTestState {
  class TestMDStoreState {
    std::map<std::string, std::string> md;
    public:
    class Store final : public SeaStore::MDStore {
      TestMDStoreState &parent;
    public:
      Store(TestMDStoreState &parent) : parent(parent) {}

      write_meta_ret write_meta(
	const std::string& key, const std::string& value) final {
	parent.md[key] = value;
	return seastar::now();
      }

      read_meta_ret read_meta(const std::string& key) final {
	auto iter = parent.md.find(key);
	if (iter != parent.md.end()) {
	  return read_meta_ret(
	    read_meta_ertr::ready_future_marker{},
	    iter->second);
	} else {
	  return read_meta_ret(
	    read_meta_ertr::ready_future_marker{},
	    std::nullopt);
	}
      }
    };
    Store get_mdstore() {
      return Store(*this);
    }
  } mdstore_state;

protected:
  std::unique_ptr<SeaStore> seastore;

  SeaStoreTestState() : EphemeralTestState() {}

  virtual void _init() final {
    seastore = get_seastore(
      std::make_unique<TestMDStoreState::Store>(mdstore_state.get_mdstore()),
      std::make_unique<TestSegmentManagerWrapper>(*segment_manager));
  }

  virtual void _destroy() final {
    seastore.reset();
  }

  virtual seastar::future<> _teardown() final {
    return seastore->umount().then([this] {
      seastore.reset();
    });
  }

  virtual FuturizedStore::mount_ertr::future<> _mount() final {
    return seastore->mount();
  }

  virtual FuturizedStore::mkfs_ertr::future<> _mkfs() final {
    return seastore->mkfs(uuid_d{});
  }
};
