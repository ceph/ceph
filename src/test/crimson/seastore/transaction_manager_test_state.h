// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
  virtual std::vector<Device*> get_cache_devices() = 0;
  virtual std::vector<DeviceRef> take_cache_devices() = 0;
  virtual std::vector<Device*> get_data_devices() = 0;
  virtual std::vector<DeviceRef> take_data_devices() = 0;
  virtual ~EphemeralDevices() {}
  virtual Device* get_primary_device() = 0;
  virtual void set_primary_device(Device*) = 0;
  virtual void set_cache_devices(std::vector<DeviceRef>&&) = 0;
  virtual void set_data_devices(std::vector<DeviceRef>&&) = 0;
};
using EphemeralDevicesRef = std::unique_ptr<EphemeralDevices>;

class EphemeralSegmentedDevices : public EphemeralDevices {
  segment_manager::EphemeralSegmentManager* segment_manager;
  std::list<segment_manager::EphemeralSegmentManagerRef> cache_segment_managers;
  std::list<segment_manager::EphemeralSegmentManagerRef> data_segment_managers;
  std::size_t num_cache_device_managers;
  std::size_t num_data_device_managers;

public:
  EphemeralSegmentedDevices(std::size_t num_cache_devices,
			    std::size_t num_data_devices)
    : num_cache_device_managers(num_cache_devices),
      num_data_device_managers(num_data_devices)
  {
    assert(num_cache_devices + num_data_devices > 0);
    cache_segment_managers.resize(num_cache_device_managers);
    data_segment_managers.resize(num_data_device_managers);
  }

  seastar::future<> setup() final {
    device_id_t id = 0;
    for (auto &cache_sm : cache_segment_managers) {
      cache_sm = segment_manager::create_test_ephemeral(
        id, device_type_t::EPHEMERAL_MAIN);
      if (id == 0) {
        segment_manager = cache_sm.get();
      }
      id++;
    }
    for (auto &data_sm : data_segment_managers) {
      data_sm = segment_manager::create_test_ephemeral(
        id,
        cache_segment_managers.empty()
          ? device_type_t::EPHEMERAL_MAIN
          : device_type_t::EPHEMERAL_COLD);
      if (id == 0) {
        segment_manager = data_sm.get();
      }
      id++;
    }
    for (auto &cache_sm : cache_segment_managers) {
      co_await cache_sm->init().handle_error(
        crimson::ct_error::assert_all("unexpected error"));
    }
    for (auto &data_sm : data_segment_managers) {
      co_await data_sm->init().handle_error(
        crimson::ct_error::assert_all("unexpected error"));
    }
    cache_device_set_t cache_devices;
    for (auto &cache_sm : cache_segment_managers) {
      auto cache_dev = segment_manager::get_ephemeral_device_config(
        cache_sm->get_device_id(),
        cache_device_set_t{},
        false);
      co_await cache_sm->mkfs(cache_dev).handle_error(
        crimson::ct_error::assert_all("unexpected error"));
      cache_devices.emplace(cache_sm->get_device_id(), cache_dev.spec);
    }
    for (auto &data_sm : data_segment_managers) {
      auto data_dev = segment_manager::get_ephemeral_device_config(
        data_sm->get_device_id(),
        cache_devices,
        true);
      co_await data_sm->mkfs(data_dev).handle_error(
        crimson::ct_error::assert_all("unexpected error"));
    }
  }

  void remount() final {
    for (auto &cache_sm : cache_segment_managers) {
      cache_sm->remount();
    }
    for (auto &data_sm : data_segment_managers) {
      data_sm->remount();
    }
  }

  std::size_t get_num_devices() const final {
    return cache_segment_managers.size() + data_segment_managers.size();
  }

  void reset() final {
    for (auto &cache_sm : cache_segment_managers) {
      cache_sm.reset();
    }
    for (auto &data_sm : data_segment_managers) {
      data_sm.reset();
    }
  }

  std::vector<Device*> get_cache_devices() final {
    std::vector<Device*> cache_devices;
    for (auto &cache_sm : cache_segment_managers) {
      cache_devices.emplace_back(cache_sm.get());
    }
    return cache_devices;
  }

  std::vector<DeviceRef> take_cache_devices() final {
    std::vector<DeviceRef> cache_devices;
    for (auto &cache_sm : cache_segment_managers) {
      cache_devices.emplace_back(std::move(cache_sm));
    }
    cache_segment_managers.clear();
    return cache_devices;
  }

  std::vector<Device*> get_data_devices() final {
    std::vector<Device*> data_devices;
    for (auto &data_sm : data_segment_managers) {
      data_devices.emplace_back(data_sm.get());
    }
    return data_devices;
  }

  std::vector<DeviceRef> take_data_devices() final {
    std::vector<DeviceRef> data_devices;
    for (auto &data_sm : data_segment_managers) {
      data_devices.emplace_back(std::move(data_sm));
    }
    data_segment_managers.clear();
    return data_devices;
  }

  Device* get_primary_device() final {
    return segment_manager;
  }
  void set_primary_device(Device*) final;
  void set_cache_devices(std::vector<DeviceRef> &&cache_devs) {
    assert(cache_segment_managers.empty());
    for (auto &cache_dev : cache_devs) {
      cache_segment_managers.emplace_back(
        segment_manager::EphemeralSegmentManagerRef(
          static_cast<segment_manager::EphemeralSegmentManager*>(
            cache_dev.release())));
    }
  }
  void set_data_devices(std::vector<DeviceRef> &&data_devs) {
    assert(data_segment_managers.empty());
    for (auto &data_dev : data_devs) {
      data_segment_managers.emplace_back(
        segment_manager::EphemeralSegmentManagerRef(
          static_cast<segment_manager::EphemeralSegmentManager*>(
            data_dev.release())));
    }
  }
};

class EphemeralRandomBlockDevices : public EphemeralDevices {
  random_block_device::RBMDevice* rb_device;
  std::list<random_block_device::RBMDeviceRef> cache_rb_devices;
  std::list<random_block_device::RBMDeviceRef> data_rb_devices;

public:
  EphemeralRandomBlockDevices(
    std::size_t num_cache_device_managers,
    std::size_t num_data_device_managers) {
    if (num_cache_device_managers > 0) {
      assert(num_data_device_managers > 0);
    }
    cache_rb_devices.resize(num_cache_device_managers);
    data_rb_devices.resize(num_data_device_managers);
  }
  
  seastar::future<> setup() final {
    device_id_t id = 0;
    for (auto &cache_rb : cache_rb_devices) {
      cache_rb = random_block_device::create_test_ephemeral(id);
      if (id == 0) {
        rb_device = cache_rb.get();
      }
      id++;
    }
    for (auto &data_rb : data_rb_devices) {
      data_rb = random_block_device::create_test_ephemeral(id);
      if (id == 0) {
        rb_device = data_rb.get();
      }
      id++;
    }
    cache_device_set_t cache_devices;
    for (auto &cache_rb : cache_rb_devices) {
      auto cache_dev = get_rbm_ephemeral_device_config(
        cache_rb->get_device_id(),
        cache_device_set_t{},
        false);
      co_await cache_rb->mkfs(cache_dev).handle_error(
        crimson::ct_error::assert_all("unexpected error"));
      cache_devices.emplace(cache_rb->get_device_id(), cache_dev.spec);
    }
    for (auto &data_rb : data_rb_devices) {
      auto data_dev = get_rbm_ephemeral_device_config(
        data_rb->get_device_id(),
        cache_devices,
        true);
      co_await data_rb->mkfs(data_dev).handle_error(
        crimson::ct_error::assert_all("unexpected error"));
    }
  }

  void remount() final {}

  std::size_t get_num_devices() const final {
    return cache_rb_devices.size() + data_rb_devices.size();
  }

  void reset() final {
    for (auto &cache_rb : cache_rb_devices) {
      cache_rb.reset();
    }
    for (auto &cache_rb : cache_rb_devices) {
      cache_rb.reset();
    }
  }

  std::vector<Device*> get_cache_devices() final {
    std::vector<Device*> cache_devices;
    for (auto &cache_rb : cache_rb_devices) {
      cache_devices.emplace_back(cache_rb.get());
    }
    return cache_devices;
  }

  std::vector<DeviceRef> take_cache_devices() final {
    std::vector<DeviceRef> cache_devices;
    for (auto &cache_rb : cache_rb_devices) {
      cache_devices.emplace_back(std::move(cache_rb));
    }
    return cache_devices;
  }

  std::vector<Device*> get_data_devices() final {
    std::vector<Device*> data_devices;
    for (auto &data_rb : data_rb_devices) {
      data_devices.emplace_back(data_rb.get());
    }
    return data_devices;
  }

  std::vector<DeviceRef> take_data_devices() final {
    std::vector<DeviceRef> data_devices;
    for (auto &data_rb : data_rb_devices) {
      data_devices.emplace_back(std::move(data_rb));
    }
    data_rb_devices.clear();
    return data_devices;
  }

  Device* get_primary_device() final {
    return rb_device;
  }
  void set_primary_device(Device*) final;
  void set_cache_devices(std::vector<DeviceRef> &&cache_devs) {
    assert(cache_rb_devices.empty());
    for (auto &cache_dev : cache_devs) {
      cache_rb_devices.emplace_back(
        random_block_device::RBMDeviceRef(
          static_cast<random_block_device::RBMDevice*>(
            cache_dev.release())));
    }
  }
  void set_data_devices(std::vector<DeviceRef> &&data_devs) {
    assert(data_rb_devices.empty());
    for (auto &data_dev : data_devs) {
      data_rb_devices.emplace_back(
        random_block_device::RBMDeviceRef(
          static_cast<random_block_device::RBMDevice*>(
            data_dev.release())));
    }
  }
};

class EphemeralTestState 
#ifdef UNIT_TESTS_BUILT
  : public ::testing::WithParamInterface<
	      std::tuple<const char*, integrity_check_t>> {
#else 
  {
#endif
protected:
  size_t num_cache_device_managers = 0;
  size_t num_data_device_managers = 0;
  EphemeralDevicesRef devices;
  bool secondary_is_cold;
  EphemeralTestState(std::size_t num_cache_device_managers,
                     std::size_t num_data_device_managers) :
    num_cache_device_managers(num_cache_device_managers),
    num_data_device_managers(num_data_device_managers) {
    if (num_cache_device_managers > 0) {
      assert(num_data_device_managers > 0);
    }
  }

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
        return _mount().handle_error(crimson::ct_error::assert_all("unexpected error"));
      });
    }).then([FNAME] {
      SUBINFO(test, "finish");
    });
  }

  void restart() {
    restart_fut().get();
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
      ceph_assert(num_cache_device_managers == 0);
      ceph_assert(num_data_device_managers == 1);
      devices.reset(new EphemeralRandomBlockDevices(0, 1));
    } else {
      // segmented by default
      devices.reset(new
        EphemeralSegmentedDevices(
          num_cache_device_managers, num_data_device_managers));
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
	crimson::ct_error::assert_all("unexpected error")
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
  shard_stats_t shard_stats;

  TMTestState() : EphemeralTestState(0, 1) {}

  TMTestState(std::size_t num_cache_devices, std::size_t num_data_devices)
    : EphemeralTestState(num_cache_devices, num_data_devices) {}

  virtual seastar::future<> _init() override {
    auto cache_devices = devices->get_cache_devices();
    auto data_devices = devices->get_data_devices();
    auto primary_dev = devices->get_primary_device();
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
    shard_stats = {};
    tm = make_transaction_manager(
      primary_dev,
      cache_devices,
      data_devices,
      shard_stats,
      0,
      true);
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
      crimson::ct_error::assert_all("Error in teardown")
    );
  }

  virtual FuturizedStore::mount_ertr::future<> _mount() {
    return tm->mount(
    ).handle_error(
      crimson::ct_error::assert_all("Error in mount")
    ).then([this] {
      return epm->stop_background();
    }).then([this] {
      return epm->run_background_work_until_halt();
    });
  }

  virtual FuturizedStore::mkfs_ertr::future<> _mkfs() {
    return tm->mkfs(
    ).handle_error(
      crimson::ct_error::assert_all("Error in mkfs")
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
    return with_trans_intr(
      t,
      [this](auto &t) {
	return tm->submit_transaction(t
	).si_then([this] {
	  return base_iertr::make_ready_future<uint64_t>(seq++);
	});
      });
  }

  void submit_transaction(TransactionRef t) {
    submit_transaction_fut(*t).unsafe_get();
    epm->run_background_work_until_halt().get();
  }
};

void EphemeralSegmentedDevices::set_primary_device(Device* dev) {
  segment_manager = static_cast<segment_manager::EphemeralSegmentManager*>(dev);
}

void EphemeralRandomBlockDevices::set_primary_device(Device* dev) {
  rb_device = static_cast<random_block_device::RBMDevice*>(dev);
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

  SeaStoreTestState() : EphemeralTestState(0, 1) {}

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
      return seastore->test_start(
        devices->get_primary_device(),
        devices->take_cache_devices(),
        devices->take_data_devices());
    }).then([this] {
      sharded_seastore = &(seastore->get_sharded_store());
    });
  }

  virtual seastar::future<> _destroy() final {
    devices->set_primary_device(seastore->get_primary_device());
    devices->set_cache_devices(seastore->take_cache_devices());
    devices->set_data_devices(seastore->take_data_devices());
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
