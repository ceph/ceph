// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <random>
#include <boost/iterator/counting_iterator.hpp>

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"
#include "crimson/os/seastore/seastore.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/collection_manager/flat_collection_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"
#include "crimson/os/seastore/journal/circular_bounded_journal.h"
#include "crimson/os/seastore/random_block_manager/block_rb_manager.h"
#ifdef UNIT_TESTS_BUILT
#include "test/crimson/gtest_seastar.h"
#endif

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

enum class integrity_check_t : uint8_t {
  FULL_CHECK,
  NONFULL_CHECK
};

class EphemeralDevices {
public:
  virtual seastar::future<> setup() = 0;
  virtual void remount() = 0;
  virtual std::size_t get_num_devices() const = 0;
  virtual void reset() = 0;
  virtual std::vector<Device*> get_secondary_devices() = 0;
  virtual ~EphemeralDevices() {}
  virtual Device* get_primary_device() = 0;
  virtual DeviceRef get_primary_device_ref() = 0;
  virtual void set_primary_device_ref(DeviceRef) = 0;
};
using EphemeralDevicesRef = std::unique_ptr<EphemeralDevices>;

class EphemeralSegmentedDevices : public EphemeralDevices {
  segment_manager::EphemeralSegmentManagerRef segment_manager;
  std::list<segment_manager::EphemeralSegmentManagerRef> secondary_segment_managers;
  std::size_t num_main_device_managers;
  std::size_t num_cold_device_managers;

public:
  EphemeralSegmentedDevices(std::size_t num_main_devices,
			    std::size_t num_cold_devices)
    : num_main_device_managers(num_main_devices),
      num_cold_device_managers(num_cold_devices)
  {
    auto num_device_managers = num_main_device_managers + num_cold_device_managers;
    assert(num_device_managers > 0);
    secondary_segment_managers.resize(num_device_managers - 1);
  }

  seastar::future<> setup() final {
    segment_manager = segment_manager::create_test_ephemeral();
    for (auto &sec_sm : secondary_segment_managers) {
      sec_sm = segment_manager::create_test_ephemeral();
    }
    return segment_manager->init(
    ).safe_then([this] {
      return crimson::do_for_each(
        secondary_segment_managers.begin(),
        secondary_segment_managers.end(),
        [](auto &sec_sm)
      {
        return sec_sm->init();
      });
    }).safe_then([this] {
      return segment_manager->mkfs(
        segment_manager::get_ephemeral_device_config(
          0, num_main_device_managers, num_cold_device_managers));
    }).safe_then([this] {
      return seastar::do_with(std::size_t(0), [this](auto &cnt) {
        return crimson::do_for_each(
          secondary_segment_managers.begin(),
          secondary_segment_managers.end(),
          [this, &cnt](auto &sec_sm)
        {
          ++cnt;
          return sec_sm->mkfs(
            segment_manager::get_ephemeral_device_config(
              cnt, num_main_device_managers, num_cold_device_managers));
        });
      });
    }).handle_error(
      crimson::ct_error::assert_all{}
    );
  }

  void remount() final {
    segment_manager->remount();
    for (auto &sec_sm : secondary_segment_managers) {
      sec_sm->remount();
    }
  }

  std::size_t get_num_devices() const final {
    return secondary_segment_managers.size() + 1;
  }

  void reset() final {
    segment_manager.reset();
    for (auto &sec_sm : secondary_segment_managers) {
      sec_sm.reset();
    }
  }

  std::vector<Device*> get_secondary_devices() final {
    std::vector<Device*> sec_devices;
    for (auto &sec_sm : secondary_segment_managers) {
      sec_devices.emplace_back(sec_sm.get());
    }
    return sec_devices;
  }

  Device* get_primary_device() final {
    return segment_manager.get();
  }
  DeviceRef get_primary_device_ref() final;
  void set_primary_device_ref(DeviceRef) final;
};

class EphemeralRandomBlockDevices : public EphemeralDevices {
  random_block_device::RBMDeviceRef rb_device;
  std::list<random_block_device::RBMDeviceRef> secondary_rb_devices;

public:
  EphemeralRandomBlockDevices(std::size_t num_device_managers) {
    assert(num_device_managers > 0);
    secondary_rb_devices.resize(num_device_managers - 1);
  }
  
  seastar::future<> setup() final {
    rb_device = random_block_device::create_test_ephemeral();
    device_config_t config = get_rbm_ephemeral_device_config(0, 1);
    return rb_device->mkfs(config).handle_error(crimson::ct_error::assert_all{});
  }

  void remount() final {}

  std::size_t get_num_devices() const final {
    return secondary_rb_devices.size() + 1;
  }

  void reset() final {
    rb_device.reset();
    for (auto &sec_rb : secondary_rb_devices) {
      sec_rb.reset();
    }
  }

  std::vector<Device*> get_secondary_devices() final {
    std::vector<Device*> sec_devices;
    for (auto &sec_rb : secondary_rb_devices) {
      sec_devices.emplace_back(sec_rb.get());
    }
    return sec_devices;
  }

  Device* get_primary_device() final {
    return rb_device.get();
  }
  DeviceRef get_primary_device_ref() final;
  void set_primary_device_ref(DeviceRef) final;
};

class EphemeralTestState 
#ifdef UNIT_TESTS_BUILT
  : public ::testing::WithParamInterface<
	      std::tuple<const char*, integrity_check_t>> {
#else 
  {
#endif
protected:
  size_t num_main_device_managers = 0;
  size_t num_cold_device_managers = 0;
  EphemeralDevicesRef devices;
  bool secondary_is_cold;
  EphemeralTestState(std::size_t num_main_device_managers,
                     std::size_t num_cold_device_managers) :
    num_main_device_managers(num_main_device_managers),
    num_cold_device_managers(num_cold_device_managers) {}

  virtual seastar::future<> _init() = 0;

  virtual seastar::future<> _destroy() = 0;
  virtual seastar::future<> _teardown() = 0;
  seastar::future<> teardown() {
    return _teardown().then([this] {
      return _destroy();
    });
  }

  virtual FuturizedStore::mkfs_ertr::future<> _mkfs() = 0;
  virtual FuturizedStore::mount_ertr::future<> _mount() = 0;

  seastar::future<> restart_fut() {
    LOG_PREFIX(EphemeralTestState::restart_fut);
    SUBINFO(test, "begin ...");
    return teardown().then([this] {
      devices->remount();
      return _init().then([this] {
        return _mount().handle_error(crimson::ct_error::assert_all{});
      });
    }).then([FNAME] {
      SUBINFO(test, "finish");
    });
  }

  void restart() {
    restart_fut().get0();
  }

  seastar::future<> tm_setup() {
    LOG_PREFIX(EphemeralTestState::tm_setup);
#ifdef UNIT_TESTS_BUILT
    std::string j_type = std::get<0>(GetParam());
#else
    std::string j_type = "segmented";
#endif
    if (j_type == "circularbounded") {
      //TODO: multiple devices
      ceph_assert(num_main_device_managers == 1);
      ceph_assert(num_cold_device_managers == 0);
      devices.reset(new EphemeralRandomBlockDevices(1));
    } else {
      // segmented by default
      devices.reset(new
        EphemeralSegmentedDevices(
          num_main_device_managers, num_cold_device_managers));
    }
    SUBINFO(test, "begin with {} devices ...", devices->get_num_devices());
    return devices->setup(
    ).then([this] {
      return _init();
    }).then([this, FNAME] {
        return _mkfs(
      ).safe_then([this] {
	return restart_fut();
      }).handle_error(
	crimson::ct_error::assert_all{}
      ).then([FNAME] {
	SUBINFO(test, "finish");
      });
    });   
  }

  seastar::future<> tm_teardown() {
    LOG_PREFIX(EphemeralTestState::tm_teardown);
    SUBINFO(test, "begin");
    return teardown().then([this, FNAME] {
      devices->reset();
      SUBINFO(test, "finish");
    });
  }
};

class TMTestState : public EphemeralTestState {
protected:
  TransactionManagerRef tm;
  LBAManager *lba_manager;
  Cache* cache;
  ExtentPlacementManager *epm;
  uint64_t seq = 0;

  TMTestState() : EphemeralTestState(1, 0) {}

  TMTestState(std::size_t num_main_devices, std::size_t num_cold_devices)
    : EphemeralTestState(num_main_devices, num_cold_devices) {}

  virtual seastar::future<> _init() override {
    auto sec_devices = devices->get_secondary_devices();
    auto p_dev = devices->get_primary_device();
    auto fut = seastar::now();
#ifdef UNIT_TESTS_BUILT
    if (std::get<1>(GetParam()) == integrity_check_t::FULL_CHECK) {
      fut = crimson::common::local_conf().set_val(
	"seastore_full_integrity_check", "true");
    } else {
      fut = crimson::common::local_conf().set_val(
	"seastore_full_integrity_check", "false");
    }
#endif
    tm = make_transaction_manager(p_dev, sec_devices, true);
    epm = tm->get_epm();
    lba_manager = tm->get_lba_manager();
    cache = tm->get_cache();
    return fut;
  }

  virtual seastar::future<> _destroy() override {
    epm = nullptr;
    lba_manager = nullptr;
    cache = nullptr;
    tm.reset();
    return seastar::now();
  }

  virtual seastar::future<> _teardown() {
    return tm->close().handle_error(
      crimson::ct_error::assert_all{"Error in teardown"}
    );
  }

  virtual FuturizedStore::mount_ertr::future<> _mount() {
    return tm->mount(
    ).handle_error(
      crimson::ct_error::assert_all{"Error in mount"}
    ).then([this] {
      return epm->stop_background();
    }).then([this] {
      return epm->run_background_work_until_halt();
    });
  }

  virtual FuturizedStore::mkfs_ertr::future<> _mkfs() {
    return tm->mkfs(
    ).handle_error(
      crimson::ct_error::assert_all{"Error in mkfs"}
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
    return tm->create_transaction(
        Transaction::src_t::READ, "test_read_weak", true);
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
  auto submit_transaction_fut_with_seq(Transaction &t) {
    using ertr = TransactionManager::base_iertr;
    return with_trans_intr(
      t,
      [this](auto &t) {
	return tm->submit_transaction(t
	).si_then([this] {
	  return ertr::make_ready_future<uint64_t>(seq++);
	});
      });
  }

  void submit_transaction(TransactionRef t) {
    submit_transaction_fut(*t).unsafe_get0();
    epm->run_background_work_until_halt().get0();
  }
};


DeviceRef EphemeralSegmentedDevices::get_primary_device_ref() {
  return std::move(segment_manager);
}

DeviceRef EphemeralRandomBlockDevices::get_primary_device_ref() {
  return std::move(rb_device);
}

void EphemeralSegmentedDevices::set_primary_device_ref(DeviceRef dev) {
  segment_manager = 
    segment_manager::EphemeralSegmentManagerRef(
      static_cast<segment_manager::EphemeralSegmentManager*>(dev.release()));
}

void EphemeralRandomBlockDevices::set_primary_device_ref(DeviceRef dev) {
  rb_device = 
    random_block_device::RBMDeviceRef(
      static_cast<random_block_device::RBMDevice*>(dev.release()));
}

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
  FuturizedStore::Shard *sharded_seastore;

  SeaStoreTestState() : EphemeralTestState(1, 0) {}

  virtual seastar::future<> _init() final {
    auto fut = seastar::now();
#ifdef UNIT_TESTS_BUILT
    if (std::get<1>(GetParam()) == integrity_check_t::FULL_CHECK) {
      fut = crimson::common::local_conf().set_val(
	"seastore_full_integrity_check", "true");
    } else {
      fut = crimson::common::local_conf().set_val(
	"seastore_full_integrity_check", "false");
    }
#endif
    seastore = make_test_seastore(
      std::make_unique<TestMDStoreState::Store>(mdstore_state.get_mdstore()));
    return fut.then([this] {
      return seastore->test_start(devices->get_primary_device_ref());
    }).then([this] {
      sharded_seastore = &(seastore->get_sharded_store());
    });
  }

  virtual seastar::future<> _destroy() final {
    devices->set_primary_device_ref(seastore->get_primary_device_ref());
    return seastore->stop().then([this] {
      seastore.reset();
    });
  }

  virtual seastar::future<> _teardown() final {
    return seastore->umount();
  }

  virtual FuturizedStore::mount_ertr::future<> _mount() final {
    return seastore->test_mount();
  }

  virtual FuturizedStore::mkfs_ertr::future<> _mkfs() final {
    return seastore->test_mkfs(uuid_d{});
  }
};
