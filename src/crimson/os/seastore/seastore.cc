// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastore.h"

#include <algorithm>

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/shared_mutex.hh>

#include "common/safe_io.h"
#include "include/stringify.h"
#include "os/Transaction.h"
#include "osd/osd_types_fmt.h"

#include "crimson/common/buffer_io.h"

#include "crimson/os/futurized_collection.h"

#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/collection_manager/flat_collection_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"
#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/object_data_handler.h"

using crimson::common::local_conf;

template <> struct fmt::formatter<crimson::os::seastore::op_type_t>
  : fmt::formatter<std::string_view> {
  using op_type_t =  crimson::os::seastore::op_type_t;
  // parse is inherited from formatter<string_view>.
  template <typename FormatContext>
  auto format(op_type_t op, FormatContext& ctx) const {
    std::string_view name = "unknown";
    switch (op) {
      case op_type_t::DO_TRANSACTION:
      name = "do_transaction";
      break;
    case op_type_t::READ:
      name = "read";
      break;
    case op_type_t::WRITE:
      name = "write";
      break;
    case op_type_t::GET_ATTR:
      name = "get_attr";
      break;
    case op_type_t::GET_ATTRS:
      name = "get_attrs";
      break;
    case op_type_t::STAT:
      name = "stat";
      break;
    case op_type_t::OMAP_GET_VALUES:
      name = "omap_get_values";
      break;
    case op_type_t::OMAP_GET_VALUES2:
      name = "omap_get_values2";
      break;
    case op_type_t::MAX:
      name = "unknown";
      break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};

SET_SUBSYS(seastore);

namespace crimson::os::seastore {

class FileMDStore final : public SeaStore::MDStore {
  std::string root;
public:
  FileMDStore(const std::string& root) : root(root) {}

  write_meta_ret write_meta(
    const std::string& key, const std::string& value) final {
    std::string path = fmt::format("{}/{}", root, key);
    ceph::bufferlist bl;
    bl.append(value + "\n");
    return crimson::write_file(std::move(bl), path);
  }

  read_meta_ret read_meta(const std::string& key) final {
    std::string path = fmt::format("{}/{}", root, key);
    return seastar::file_exists(
      path
    ).then([path] (bool exist) {
      if (exist) {
	return crimson::read_file(path)
	  .then([] (auto tmp_buf) {
	    std::string v = {tmp_buf.get(), tmp_buf.size()};
	    std::size_t pos = v.find("\n");
	    std::string str = v.substr(0, pos);
	    return seastar::make_ready_future<std::optional<std::string>>(str);
	  });
      } else {
	return seastar::make_ready_future<std::optional<std::string>>(std::nullopt);
      }
    });
  }
};

using crimson::common::get_conf;

SeaStore::Shard::Shard(
  std::string root,
  Device* dev,
  bool is_test)
  :root(root),
   max_object_size(
     get_conf<uint64_t>("seastore_default_max_object_size")),
   is_test(is_test),
   throttler(
      get_conf<uint64_t>("seastore_max_concurrent_transactions"))
{
  device = &(dev->get_sharded_device());
  register_metrics();
}

SeaStore::SeaStore(
  const std::string& root,
  MDStoreRef mdstore)
  : root(root),
    mdstore(std::move(mdstore))
{
}

SeaStore::~SeaStore() = default;

void SeaStore::Shard::register_metrics()
{
  namespace sm = seastar::metrics;
  using op_type_t = crimson::os::seastore::op_type_t;
  std::pair<op_type_t, sm::label_instance> labels_by_op_type[] = {
    {op_type_t::DO_TRANSACTION,   sm::label_instance("latency", "DO_TRANSACTION")},
    {op_type_t::READ,             sm::label_instance("latency", "READ")},
    {op_type_t::WRITE,            sm::label_instance("latency", "WRITE")},
    {op_type_t::GET_ATTR,         sm::label_instance("latency", "GET_ATTR")},
    {op_type_t::GET_ATTRS,        sm::label_instance("latency", "GET_ATTRS")},
    {op_type_t::STAT,             sm::label_instance("latency", "STAT")},
    {op_type_t::OMAP_GET_VALUES,  sm::label_instance("latency", "OMAP_GET_VALUES")},
    {op_type_t::OMAP_GET_VALUES2, sm::label_instance("latency", "OMAP_GET_VALUES2")},
  };

  for (auto& [op_type, label] : labels_by_op_type) {
    auto desc = fmt::format("latency of seastore operation (optype={})",
                            op_type);
    metrics.add_group(
      "seastore",
      {
        sm::make_histogram(
          "op_lat", [this, op_type=op_type] {
            return get_latency(op_type);
          },
          sm::description(desc),
          {label}
        ),
      }
    );
  }

  metrics.add_group(
    "seastore",
    {
      sm::make_gauge(
	"concurrent_transactions",
	[this] {
	  return throttler.get_current();
	},
	sm::description("transactions that are running inside seastore")
      ),
      sm::make_gauge(
	"pending_transactions",
	[this] {
	  return throttler.get_pending();
	},
	sm::description("transactions waiting to get "
		        "through seastore's throttler")
      )
    }
  );
}

seastar::future<> SeaStore::start()
{
  LOG_PREFIX(SeaStore::start);
  INFO("...");

  ceph_assert(seastar::this_shard_id() == primary_core);
#ifndef NDEBUG
  bool is_test = true;
#else
  bool is_test = false;
#endif
  using crimson::common::get_conf;
  std::string type = get_conf<std::string>("seastore_main_device_type");
  device_type_t d_type = string_to_device_type(type);
  assert(d_type == device_type_t::SSD ||
         d_type == device_type_t::RANDOM_BLOCK_SSD);

  ceph_assert(root != "");
  return Device::make_device(root, d_type
  ).then([this](DeviceRef device_obj) {
    device = std::move(device_obj);
    return device->start();
  }).then([this, is_test] {
    ceph_assert(device);
    return shard_stores.start(root, device.get(), is_test);
  }).then([FNAME] {
    INFO("done");
  });
}

seastar::future<> SeaStore::test_start(DeviceRef device_obj)
{
  LOG_PREFIX(SeaStore::test_start);
  INFO("...");

  ceph_assert(device_obj);
  ceph_assert(root == "");
  device = std::move(device_obj);
  return shard_stores.start_single(root, device.get(), true
  ).then([FNAME] {
    INFO("done");
  });
}

seastar::future<> SeaStore::stop()
{
  LOG_PREFIX(SeaStore::stop);
  INFO("...");

  ceph_assert(seastar::this_shard_id() == primary_core);
  return seastar::do_for_each(secondaries, [](auto& sec_dev) {
    return sec_dev->stop();
  }).then([this] {
    secondaries.clear();
    if (device) {
      return device->stop();
    } else {
      return seastar::now();
    }
  }).then([this] {
    return shard_stores.stop();
  }).then([FNAME] {
    INFO("done");
  });
}

SeaStore::mount_ertr::future<> SeaStore::test_mount()
{
  LOG_PREFIX(SeaStore::test_mount);
  INFO("...");

  ceph_assert(seastar::this_shard_id() == primary_core);
  return shard_stores.local().mount_managers(
  ).then([FNAME] {
    INFO("done");
  });
}

SeaStore::mount_ertr::future<> SeaStore::mount()
{
  LOG_PREFIX(SeaStore::mount);
  INFO("...");

  ceph_assert(seastar::this_shard_id() == primary_core);
  return device->mount(
  ).safe_then([this] {
    ceph_assert(device->get_sharded_device().get_block_size()
		>= laddr_t::UNIT_SIZE);
    auto &sec_devices = device->get_sharded_device().get_secondary_devices();
    return crimson::do_for_each(sec_devices, [this](auto& device_entry) {
      device_id_t id = device_entry.first;
      magic_t magic = device_entry.second.magic;
      device_type_t dtype = device_entry.second.dtype;
      std::string path =
        fmt::format("{}/block.{}.{}", root, dtype, std::to_string(id));
      return Device::make_device(path, dtype
      ).then([this, path, magic](DeviceRef sec_dev) {
        return sec_dev->start(
        ).then([this, magic, sec_dev = std::move(sec_dev)]() mutable {
          return sec_dev->mount(
          ).safe_then([this, sec_dev=std::move(sec_dev), magic]() mutable {
	    ceph_assert(sec_dev->get_sharded_device().get_block_size()
			>= laddr_t::UNIT_SIZE);
            boost::ignore_unused(magic);  // avoid clang warning;
            assert(sec_dev->get_sharded_device().get_magic() == magic);
            secondaries.emplace_back(std::move(sec_dev));
          });
        }).safe_then([this] {
          return set_secondaries();
        });
      });
    });
  }).safe_then([this] {
    return shard_stores.invoke_on_all([](auto &local_store) {
      return local_store.mount_managers();
    });
  }).safe_then([FNAME] {
    INFO("done");
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::mount"
    }
  );
}

seastar::future<> SeaStore::Shard::mount_managers()
{
  init_managers();
  return transaction_manager->mount(
  ).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in mount_managers"
  });
}

seastar::future<> SeaStore::umount()
{
  LOG_PREFIX(SeaStore::umount);
  INFO("...");

  ceph_assert(seastar::this_shard_id() == primary_core);
  return shard_stores.invoke_on_all([](auto &local_store) {
    return local_store.umount();
  }).then([FNAME] {
    INFO("done");
  });
}

seastar::future<> SeaStore::Shard::umount()
{
  return [this] {
    if (transaction_manager) {
      return transaction_manager->close();
    } else {
      return TransactionManager::close_ertr::now();
    }
  }().safe_then([this] {
    return crimson::do_for_each(
      secondaries,
      [](auto& sec_dev) -> SegmentManager::close_ertr::future<>
    {
      return sec_dev->close();
    });
  }).safe_then([this] {
    return device->close();
  }).safe_then([this] {
    secondaries.clear();
    transaction_manager.reset();
    collection_manager.reset();
    onode_manager.reset();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStoreS::umount"
    }
  );
}

seastar::future<> SeaStore::write_fsid(uuid_d new_osd_fsid)
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  LOG_PREFIX(SeaStore::write_fsid);
  return read_meta("fsid").then([this, FNAME, new_osd_fsid] (auto tuple) {
    auto [ret, fsid] = tuple;
    std::string str_fsid = stringify(new_osd_fsid);
    if (ret == -1) {
      return write_meta("fsid", stringify(new_osd_fsid));
    } else if (ret == 0 && fsid != str_fsid) {
      ERROR("on-disk fsid {} != provided {}",
            fsid, stringify(new_osd_fsid));
      throw std::runtime_error("store fsid error");
    } else {
      return seastar::now();
    }
  });
}

seastar::future<>
SeaStore::Shard::mkfs_managers()
{
  init_managers();
  return transaction_manager->mkfs(
  ).safe_then([this] {
    init_managers();
    return transaction_manager->mount();
  }).safe_then([this] {

    ++(shard_stats.io_num);
    ++(shard_stats.pending_io_num);
    // For TM::submit_transaction()
    ++(shard_stats.processing_inlock_io_num);

    return repeat_eagain([this] {
      ++(shard_stats.repeat_io_num);

      return transaction_manager->with_transaction_intr(
	Transaction::src_t::MUTATE,
	"mkfs_seastore",
	CACHE_HINT_TOUCH,
	[this](auto& t)
      {
        LOG_PREFIX(SeaStoreS::mkfs_managers);
        DEBUGT("...", t);
	return onode_manager->mkfs(t
	).si_then([this, &t] {
	  return collection_manager->mkfs(t);
	}).si_then([this, &t](auto coll_root) {
	  transaction_manager->write_collection_root(
	    t, coll_root);
	  return transaction_manager->submit_transaction(t);
	});
      });
    });
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in Shard::mkfs_managers"
    }
  ).finally([this] {
    assert(shard_stats.pending_io_num);
    --(shard_stats.pending_io_num);
    // XXX: it's wrong to assume no failure
    --(shard_stats.processing_postlock_io_num);
  });
}

seastar::future<> SeaStore::set_secondaries()
{
  auto sec_dev_ite = secondaries.rbegin();
  Device* sec_dev = sec_dev_ite->get();
  return shard_stores.invoke_on_all([sec_dev](auto &local_store) {
    local_store.set_secondaries(sec_dev->get_sharded_device());
  });
}

SeaStore::mkfs_ertr::future<> SeaStore::test_mkfs(uuid_d new_osd_fsid)
{
  LOG_PREFIX(SeaStore::test_mkfs);
  INFO("uuid={} ...", new_osd_fsid);

  ceph_assert(seastar::this_shard_id() == primary_core);
  return read_meta("mkfs_done"
  ).then([this, new_osd_fsid, FNAME](auto tuple) {
    auto [done, value] = tuple;
    if (done == 0) {
      ERROR("failed");
      return seastar::now();
    } 
    return shard_stores.local().mkfs_managers(
    ).then([this, new_osd_fsid] {
      return prepare_meta(new_osd_fsid);
    }).then([FNAME] {
      INFO("done");
    });
  });
}

seastar::future<> SeaStore::prepare_meta(uuid_d new_osd_fsid)
{
  ceph_assert(seastar::this_shard_id() == primary_core);
  return write_fsid(new_osd_fsid).then([this] {
    return read_meta("type").then([this] (auto tuple) {
      auto [ret, type] = tuple;
      if (ret == 0 && type == "seastore") {
	return seastar::now();
      } else if (ret == 0 && type != "seastore") {
	LOG_PREFIX(SeaStore::prepare_meta);
	ERROR("expected seastore, but type is {}", type);
	throw std::runtime_error("store type error");
      } else {
	return write_meta("type", "seastore");
      }
    });
  }).then([this] {
    return write_meta("mkfs_done", "yes");
  });
}

SeaStore::mkfs_ertr::future<> SeaStore::mkfs(uuid_d new_osd_fsid)
{
  LOG_PREFIX(SeaStore::mkfs);
  INFO("uuid={}, root={} ...", new_osd_fsid, root);

  ceph_assert(seastar::this_shard_id() == primary_core);
  return read_meta("mkfs_done"
  ).then([this, new_osd_fsid, FNAME](auto tuple) {
    auto [done, value] = tuple;
    if (done == 0) {
      ERROR("failed");
      return seastar::now();
    } else {
      return seastar::do_with(
        secondary_device_set_t(),
        [this, new_osd_fsid, FNAME](auto& sds) {
        auto fut = seastar::now();
        if (!root.empty()) {
          fut = seastar::open_directory(root
          ).then([this, &sds, new_osd_fsid, FNAME](seastar::file rdir) mutable {
            std::unique_ptr<seastar::file> root_f =
              std::make_unique<seastar::file>(std::move(rdir));
            auto sub = root_f->list_directory(
              [this, &sds, new_osd_fsid, FNAME](auto de) mutable -> seastar::future<>
            {
              DEBUG("found file: {}", de.name);
              if (de.name.find("block.") == 0
                  && de.name.length() > 6 /* 6 for "block." */) {
                std::string entry_name = de.name;
                auto dtype_end = entry_name.find_first_of('.', 6);
                device_type_t dtype =
                  string_to_device_type(
                    entry_name.substr(6, dtype_end - 6));
                if (dtype == device_type_t::NONE) {
                  // invalid device type
                  return seastar::now();
                }
                auto id = std::stoi(entry_name.substr(dtype_end + 1));
                std::string path = fmt::format("{}/{}", root, entry_name);
                return Device::make_device(path, dtype
                ).then([this, &sds, id, dtype, new_osd_fsid](DeviceRef sec_dev) {
                  auto p_sec_dev = sec_dev.get();
                  secondaries.emplace_back(std::move(sec_dev));
                  return p_sec_dev->start(
                  ).then([&sds, id, dtype, new_osd_fsid, p_sec_dev]() {
                    magic_t magic = (magic_t)std::rand();
                    sds.emplace(
                      (device_id_t)id,
                      device_spec_t{magic, dtype, (device_id_t)id});
                    return p_sec_dev->mkfs(device_config_t::create_secondary(
                      new_osd_fsid, id, dtype, magic)
                    ).handle_error(crimson::ct_error::assert_all{"not possible"});
                  });
                }).then([this] {
                   return set_secondaries();
                });
              }
              return seastar::now();
            });
            return sub.done().then([root_f=std::move(root_f)] {});
          });
        }
        return fut.then([this, &sds, new_osd_fsid] {
          device_id_t id = 0;
          device_type_t d_type = device->get_device_type();
          assert(d_type == device_type_t::SSD ||
            d_type == device_type_t::RANDOM_BLOCK_SSD);
          if (d_type == device_type_t::RANDOM_BLOCK_SSD) {
            id = static_cast<device_id_t>(DEVICE_ID_RANDOM_BLOCK_MIN);
          }

          return device->mkfs(
            device_config_t::create_primary(new_osd_fsid, id, d_type, sds)
          );
        }).safe_then([this] {
          return crimson::do_for_each(secondaries, [](auto& sec_dev) {
            return sec_dev->mount();
          });
        });
      }).safe_then([this] {
        return device->mount();
      }).safe_then([this] {
        return shard_stores.invoke_on_all([] (auto &local_store) {
          return local_store.mkfs_managers();
        });
      }).safe_then([this, new_osd_fsid] {
        return prepare_meta(new_osd_fsid);
      }).safe_then([this] {
	return umount();
      }).safe_then([FNAME] {
        INFO("done");
      }).handle_error(
        crimson::ct_error::assert_all{
          "Invalid error in SeaStore::mkfs"
        }
      );
    }
  });
}

using coll_core_t = SeaStore::coll_core_t;
seastar::future<std::vector<coll_core_t>>
SeaStore::list_collections()
{
  LOG_PREFIX(SeaStore::list_collections);
  DEBUG("...");

  ceph_assert(seastar::this_shard_id() == primary_core);
  return shard_stores.map([](auto &local_store) {
    return local_store.list_collections();
  }).then([FNAME](std::vector<std::vector<coll_core_t>> results) {
    std::vector<coll_core_t> collections;
    for (auto& colls : results) {
      collections.insert(collections.end(), colls.begin(), colls.end());
    }
    DEBUG("got {} collections", collections.size());
    return seastar::make_ready_future<std::vector<coll_core_t>>(
      std::move(collections));
  });
}

store_statfs_t SeaStore::Shard::stat() const
{
  LOG_PREFIX(SeaStoreS::stat);
  auto ss = transaction_manager->store_stat();
  DEBUG("stat={}", ss);
  return ss;
}

seastar::future<store_statfs_t> SeaStore::stat() const
{
  LOG_PREFIX(SeaStore::stat);
  DEBUG("...");

  ceph_assert(seastar::this_shard_id() == primary_core);
  return shard_stores.map_reduce0(
    [](const SeaStore::Shard &local_store) {
      return local_store.stat();
    },
    store_statfs_t(),
    [](auto &&ss, auto &&ret) {
      ss.add(ret);
      return std::move(ss);
    }
  ).then([FNAME](store_statfs_t ss) {
    DEBUG("done, stat={}", ss);
    return seastar::make_ready_future<store_statfs_t>(std::move(ss));
  });
}

seastar::future<store_statfs_t> SeaStore::pool_statfs(int64_t pool_id) const
{
  LOG_PREFIX(SeaStore::pool_statfs);
  DEBUG("pool_id={} ...", pool_id);
  ceph_assert(seastar::this_shard_id() == primary_core);
  //TODO
  return SeaStore::stat(
  ).then([FNAME, pool_id](store_statfs_t ss) {
    DEBUG("done, pool_id={}, ret={}", pool_id, ss);
    return seastar::make_ready_future<store_statfs_t>(std::move(ss));
  });
}

seastar::future<> SeaStore::report_stats()
{
  LOG_PREFIX(SeaStore::report_stats);
  DEBUG("...");

  ceph_assert(seastar::this_shard_id() == primary_core);
  shard_device_stats.resize(seastar::smp::count);
  shard_io_stats.resize(seastar::smp::count);
  shard_cache_stats.resize(seastar::smp::count);
  return shard_stores.invoke_on_all([this](const Shard &local_store) {
    bool report_detail = false;
    double seconds = 0;
    if (seastar::this_shard_id() == 0) {
      // avoid too verbose logs, only report detail in a particular shard
      report_detail = true;
      seconds = local_store.reset_report_interval();
    }
    shard_device_stats[seastar::this_shard_id()] =
      local_store.get_device_stats(report_detail, seconds);
    shard_io_stats[seastar::this_shard_id()] =
      local_store.get_io_stats(report_detail, seconds);
    shard_cache_stats[seastar::this_shard_id()] =
      local_store.get_cache_stats(report_detail, seconds);
  }).then([this, FNAME] {
    auto now = seastar::lowres_clock::now();
    if (last_tp == seastar::lowres_clock::time_point::min()) {
      last_tp = now;
      return seastar::now();
    }
    std::chrono::duration<double> duration_d = now - last_tp;
    double seconds = duration_d.count();
    last_tp = now;

    device_stats_t device_total = {};
    for (const auto &s : shard_device_stats) {
      device_total.add(s);
    }
    constexpr const char* dfmt = "{:.2f}";
    auto device_total_num_io = static_cast<double>(device_total.num_io);

    std::ostringstream oss_iops;
    auto iops = device_total.num_io/seconds;
    oss_iops << "device IOPS: "
             << fmt::format(dfmt, iops)
             << " "
             << fmt::format(dfmt, iops/seastar::smp::count)
             << "(";

    std::ostringstream oss_bd;
    auto bd_mb = device_total.total_bytes/seconds/(1<<20);
    oss_bd << "device bandwidth(MiB): "
           << fmt::format(dfmt, bd_mb)
           << " "
           << fmt::format(dfmt, bd_mb/seastar::smp::count)
           << "(";

    for (const auto &s : shard_device_stats) {
      oss_iops << fmt::format(dfmt, s.num_io/seconds) << ",";
      oss_bd << fmt::format(dfmt, s.total_bytes/seconds/(1<<20)) << ",";
    }
    oss_iops << ")";
    oss_bd << ")";

    INFO("{}", oss_iops.str());
    INFO("{}", oss_bd.str());
    INFO("device IO depth per writer: {:.2f}",
         device_total.total_depth/device_total_num_io);
    INFO("device bytes per write: {:.2f}",
         device_total.total_bytes/device_total_num_io);

    shard_stats_t io_total = {};
    for (const auto &s : shard_io_stats) {
      io_total.add(s);
    }
    INFO("trans IOPS: {:.2f},{:.2f},{:.2f},{:.2f} per-shard: {:.2f},{:.2f},{:.2f},{:.2f}",
         io_total.io_num/seconds,
         io_total.read_num/seconds,
         io_total.get_bg_num()/seconds,
         io_total.flush_num/seconds,
         io_total.io_num/seconds/seastar::smp::count,
         io_total.read_num/seconds/seastar::smp::count,
         io_total.get_bg_num()/seconds/seastar::smp::count,
         io_total.flush_num/seconds/seastar::smp::count);
    auto calc_conflicts = [](uint64_t ios, uint64_t repeats) {
      return (double)(repeats-ios)/ios;
    };
    INFO("trans conflicts: {:.2f},{:.2f},{:.2f}",
         calc_conflicts(io_total.io_num, io_total.repeat_io_num),
         calc_conflicts(io_total.read_num, io_total.repeat_read_num),
         calc_conflicts(io_total.get_bg_num(), io_total.get_repeat_bg_num()));
    INFO("trans outstanding: {},{},{},{} "
         "per-shard: {:.2f}({:.2f},{:.2f},{:.2f},{:.2f},{:.2f}),{:.2f},{:.2f},{:.2f}",
         io_total.pending_io_num,
         io_total.pending_read_num,
         io_total.pending_bg_num,
         io_total.pending_flush_num,
         (double)io_total.pending_io_num/seastar::smp::count,
         (double)io_total.starting_io_num/seastar::smp::count,
         (double)io_total.waiting_collock_io_num/seastar::smp::count,
         (double)io_total.waiting_throttler_io_num/seastar::smp::count,
         (double)io_total.processing_inlock_io_num/seastar::smp::count,
         (double)io_total.processing_postlock_io_num/seastar::smp::count,
         (double)io_total.pending_read_num/seastar::smp::count,
         (double)io_total.pending_bg_num/seastar::smp::count,
         (double)io_total.pending_flush_num/seastar::smp::count);

    std::ostringstream oss_pending;
    for (const auto &s : shard_io_stats) {
      oss_pending << s.pending_io_num
                 << "(" << s.starting_io_num
                 << "," << s.waiting_collock_io_num
                 << "," << s.waiting_throttler_io_num
                 << "," << s.processing_inlock_io_num
                 << "," << s.processing_postlock_io_num
                 << ") ";
    }
    INFO("details: {}", oss_pending.str());

    cache_stats_t cache_total = {};
    for (const auto& s : shard_cache_stats) {
      cache_total.add(s);
    }

    cache_size_stats_t lru_sizes_ps = cache_total.lru_sizes;
    lru_sizes_ps.divide_by(seastar::smp::count);
    cache_io_stats_t lru_io_ps = cache_total.lru_io;
    lru_io_ps.divide_by(seastar::smp::count);
    INFO("cache lru: total{} {}; per-shard: total{} {}",
         cache_total.lru_sizes,
         cache_io_stats_printer_t{seconds, cache_total.lru_io},
         lru_sizes_ps,
         cache_io_stats_printer_t{seconds, lru_io_ps});

    cache_size_stats_t dirty_sizes_ps = cache_total.dirty_sizes;
    dirty_sizes_ps.divide_by(seastar::smp::count);
    dirty_io_stats_t dirty_io_ps = cache_total.dirty_io;
    dirty_io_ps.divide_by(seastar::smp::count);
    INFO("cache dirty: total{} {}; per-shard: total{} {}",
         cache_total.dirty_sizes,
         dirty_io_stats_printer_t{seconds, cache_total.dirty_io},
         dirty_sizes_ps,
         dirty_io_stats_printer_t{seconds, dirty_io_ps});

    cache_access_stats_t access_ps = cache_total.access;
    access_ps.divide_by(seastar::smp::count);
    INFO("cache_access: total{}; per-shard{}",
         cache_access_stats_printer_t{seconds, cache_total.access},
         cache_access_stats_printer_t{seconds, access_ps});

    return seastar::now();
  });
}

TransactionManager::read_extent_iertr::future<std::optional<unsigned>>
SeaStore::Shard::get_coll_bits(CollectionRef ch, Transaction &t) const
{
  return transaction_manager->read_collection_root(t)
    .si_then([this, ch, &t](auto coll_root) {
      return collection_manager->list(coll_root, t);
    }).si_then([ch](auto colls) {
      auto it = std::find_if(colls.begin(), colls.end(),
        [ch](const std::pair<coll_t, coll_info_t>& element) {
          return element.first == ch->get_cid();
      });
      if (it != colls.end()) {
        return TransactionManager::read_extent_iertr::make_ready_future<
          std::optional<unsigned>>(it->second.split_bits);
      } else {
        return TransactionManager::read_extent_iertr::make_ready_future<
	  std::optional<unsigned>>(std::nullopt);
      }
    });
}

col_obj_ranges_t
SeaStore::get_objs_range(CollectionRef ch, unsigned bits)
{
  col_obj_ranges_t obj_ranges;
  spg_t pgid;
  constexpr uint32_t MAX_HASH = std::numeric_limits<uint32_t>::max();
  const std::string_view MAX_NSPACE = "\xff";
  if (ch->get_cid().is_pg(&pgid)) {
    obj_ranges.obj_begin.shard_id = pgid.shard;
    obj_ranges.temp_begin = obj_ranges.obj_begin;

    obj_ranges.obj_begin.hobj.pool = pgid.pool();
    obj_ranges.temp_begin.hobj.pool = -2ll - pgid.pool();

    obj_ranges.obj_end = obj_ranges.obj_begin;
    obj_ranges.temp_end = obj_ranges.temp_begin;

    uint32_t reverse_hash = hobject_t::_reverse_bits(pgid.ps());
    obj_ranges.obj_begin.hobj.set_bitwise_key_u32(reverse_hash);
    obj_ranges.temp_begin.hobj.set_bitwise_key_u32(reverse_hash);

    uint64_t end_hash = reverse_hash  + (1ull << (32 - bits));
    if (end_hash > MAX_HASH) {
      // make sure end hobj is even greater than the maximum possible hobj
      obj_ranges.obj_end.hobj.set_bitwise_key_u32(MAX_HASH);
      obj_ranges.temp_end.hobj.set_bitwise_key_u32(MAX_HASH);
      obj_ranges.obj_end.hobj.nspace = MAX_NSPACE;
    } else {
      obj_ranges.obj_end.hobj.set_bitwise_key_u32(end_hash);
      obj_ranges.temp_end.hobj.set_bitwise_key_u32(end_hash);
    }
  } else {
    obj_ranges.obj_begin.shard_id = shard_id_t::NO_SHARD;
    obj_ranges.obj_begin.hobj.pool = -1ull;

    obj_ranges.obj_end = obj_ranges.obj_begin;
    obj_ranges.obj_begin.hobj.set_bitwise_key_u32(0);
    obj_ranges.obj_end.hobj.set_bitwise_key_u32(MAX_HASH);
    obj_ranges.obj_end.hobj.nspace = MAX_NSPACE;
    // no separate temp section
    obj_ranges.temp_begin = obj_ranges.obj_end;
    obj_ranges.temp_end = obj_ranges.obj_end;
  }

  obj_ranges.obj_begin.generation = 0;
  obj_ranges.obj_end.generation = 0;
  obj_ranges.temp_begin.generation = 0;
  obj_ranges.temp_end.generation = 0;
  return obj_ranges;
}

static std::list<std::pair<ghobject_t, ghobject_t>>
get_ranges(CollectionRef ch,
           ghobject_t start,
           ghobject_t end,
           col_obj_ranges_t obj_ranges)
{
  ceph_assert(start <= end);
  std::list<std::pair<ghobject_t, ghobject_t>> ranges;
  if (start < obj_ranges.temp_end) {
    ranges.emplace_back(
      std::max(obj_ranges.temp_begin, start),
      std::min(obj_ranges.temp_end, end));
  }
  if (end > obj_ranges.obj_begin) {
    ranges.emplace_back(
      std::max(obj_ranges.obj_begin, start),
      std::min(obj_ranges.obj_end, end));
  }
  return ranges;
}

seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
SeaStore::Shard::list_objects(CollectionRef ch,
			      const ghobject_t& start,
			      const ghobject_t& end,
			      uint64_t limit,
			      uint32_t op_flags) const
{
  ++(shard_stats.read_num);
  ++(shard_stats.pending_read_num);

  ceph_assert(start <= end);
  using list_iertr = OnodeManager::list_onodes_iertr;
  using RetType = typename OnodeManager::list_onodes_bare_ret;
  return seastar::do_with(
    RetType(std::vector<ghobject_t>(), start),
    std::move(limit),
    [this, ch, start, end, op_flags](auto& ret, auto& limit) {
    return repeat_eagain([this, ch, start, end, &limit, &ret, op_flags] {
      ++(shard_stats.repeat_read_num);

      return transaction_manager->with_transaction_intr(
        Transaction::src_t::READ,
        "list_objects",
	op_flags,
        [this, ch, start, end, &limit, &ret](auto &t)
      {
        LOG_PREFIX(SeaStoreS::list_objects);
        DEBUGT("cid={} start={} end={} limit={} ...",
               t, ch->get_cid(), start, end, limit);
        return get_coll_bits(
          ch, t
	).si_then([FNAME, this, ch, &t, start, end, &limit, &ret](auto bits) {
          if (!bits) {
            DEBUGT("no bits, return none", t);
            return list_iertr::make_ready_future<
              OnodeManager::list_onodes_bare_ret
	      >(std::make_tuple(
		  std::vector<ghobject_t>(),
		  ghobject_t::get_max()));
          } else {
	    DEBUGT("bits={} ...", t, *bits);
            auto filter = SeaStore::get_objs_range(ch, *bits);
	    using list_iertr = OnodeManager::list_onodes_iertr;
	    using repeat_ret = list_iertr::future<seastar::stop_iteration>;
            return trans_intr::repeat(
              [this, FNAME, &t, &ret, &limit, end,
	       filter, ranges = get_ranges(ch, start, end, filter)
	      ]() mutable -> repeat_ret {
		if (limit == 0 || ranges.empty()) {
		  return list_iertr::make_ready_future<
		    seastar::stop_iteration
		    >(seastar::stop_iteration::yes);
		}
		auto ite = ranges.begin();
		auto pstart = ite->first;
		auto pend = ite->second;
		ranges.pop_front();
		DEBUGT("pstart {}, pend {}, limit {} ...", t, pstart, pend, limit);
		return onode_manager->list_onodes(
		  t, pstart, pend, limit
		).si_then([&limit, &ret, pend, &t, last=ranges.empty(), end, FNAME]
			  (auto &&_ret) mutable {
		  auto &next_objects = std::get<0>(_ret);
		  auto &ret_objects = std::get<0>(ret);
		  ret_objects.insert(
		    ret_objects.end(),
		    next_objects.begin(),
		    next_objects.end());
		  std::get<1>(ret) = std::get<1>(_ret);
		  assert(limit >= next_objects.size());
		  limit -= next_objects.size();
		  DEBUGT("got {} objects, left limit {}",
		    t, next_objects.size(), limit);
		  assert(limit == 0 ||
			 std::get<1>(ret) == pend ||
			 std::get<1>(ret) == ghobject_t::get_max());
		  if (last && std::get<1>(ret) == pend) {
		    std::get<1>(ret) = end;
		  }
		  return list_iertr::make_ready_future<
		    seastar::stop_iteration
		    >(seastar::stop_iteration::no);
		});
	      }
            ).si_then([&ret, FNAME] {
              DEBUG("got {} objects, next={}",
                    std::get<0>(ret).size(), std::get<1>(ret));
              return list_iertr::make_ready_future<
                OnodeManager::list_onodes_bare_ret>(std::move(ret));
            });
          }
        });
      }).safe_then([&ret](auto&& _ret) {
        ret = std::move(_ret);
      });
    }).safe_then([&ret] {
      return std::move(ret);
    }).handle_error(
      crimson::ct_error::assert_all{
        "Invalid error in SeaStoreS::list_objects"
      }
    );
  }).finally([this] {
    assert(shard_stats.pending_read_num);
    --(shard_stats.pending_read_num);
  });
}

seastar::future<CollectionRef>
SeaStore::Shard::create_new_collection(const coll_t& cid)
{
  LOG_PREFIX(SeaStoreS::create_new_collection);
  DEBUG("cid={}", cid);
  return seastar::make_ready_future<CollectionRef>(_get_collection(cid));
}

seastar::future<CollectionRef>
SeaStore::Shard::open_collection(const coll_t& cid)
{
  LOG_PREFIX(SeaStoreS::open_collection);
  DEBUG("cid={} ...", cid);
  return list_collections(
  ).then([cid, this, FNAME] (auto colls_cores) {
    if (auto found = std::find(colls_cores.begin(),
                               colls_cores.end(),
                               std::make_pair(cid, seastar::this_shard_id()));
      found != colls_cores.end()) {
      DEBUG("cid={} exists", cid);
      return seastar::make_ready_future<CollectionRef>(_get_collection(cid));
    } else {
      DEBUG("cid={} not exists", cid);
      return seastar::make_ready_future<CollectionRef>();
    }
  });
}

seastar::future<>
SeaStore::Shard::set_collection_opts(CollectionRef c,
                                        const pool_opts_t& opts)
{
  LOG_PREFIX(SeaStoreS::set_collection_opts);
  DEBUG("cid={}, opts={} not implemented", c->get_cid(), opts);
  //TODO
  return seastar::now();
}

seastar::future<std::vector<coll_core_t>>
SeaStore::Shard::list_collections()
{
  ++(shard_stats.read_num);
  ++(shard_stats.pending_read_num);

  return seastar::do_with(
    std::vector<coll_core_t>(),
    [this](auto &ret) {
      return repeat_eagain([this, &ret] {
        ++(shard_stats.repeat_read_num);

        return transaction_manager->with_transaction_intr(
          Transaction::src_t::READ,
          "list_collections",
	  CACHE_HINT_TOUCH,
          [this, &ret](auto& t)
        {
          LOG_PREFIX(SeaStoreS::list_collections);
          DEBUGT("...", t);
          return transaction_manager->read_collection_root(t
          ).si_then([this, &t](auto coll_root) {
            return collection_manager->list(coll_root, t);
          }).si_then([&ret](auto colls) {
            ret.resize(colls.size());
            std::transform(
              colls.begin(), colls.end(), ret.begin(),
              [](auto p) {
              return std::make_pair(p.first, seastar::this_shard_id());
            });
          });
        });
      }).safe_then([&ret] {
        return seastar::make_ready_future<std::vector<coll_core_t>>(ret);
      });
    }
  ).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStoreS::list_collections"
    }
  ).finally([this] {
    assert(shard_stats.pending_read_num);
    --(shard_stats.pending_read_num);
  });
}

SeaStore::base_iertr::future<ceph::bufferlist>
SeaStore::Shard::_read(
  Transaction& t,
  Onode& onode,
  uint64_t offset,
  std::size_t len,
  uint32_t op_flags)
{
  LOG_PREFIX(SeaStoreS::_read);
  size_t size = onode.get_layout().size;
  if (offset >= size) {
    DEBUGT("0x{:x}~0x{:x} onode-size=0x{:x} flags=0x{:x}, got none",
           t, offset, len, size, op_flags);
    return seastar::make_ready_future<ceph::bufferlist>();
  }

  DEBUGT("0x{:x}~0x{:x} onode-size=0x{:x} flags=0x{:x} ...",
         t, offset, len, size, op_flags);
  size_t corrected_len = (len == 0) ?
    size - offset :
    std::min(size - offset, len);

  return ObjectDataHandler(max_object_size).read(
    ObjectDataHandler::context_t{
      *transaction_manager,
      t,
      onode,
    },
    offset,
    corrected_len
  ).si_then([FNAME, &t](auto bl) {
    DEBUGT("got bl length=0x{:x}", t, bl.length());
    return bl;
  });
}

SeaStore::Shard::read_errorator::future<ceph::bufferlist>
SeaStore::Shard::read(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  uint32_t op_flags)
{
  ++(shard_stats.read_num);
  ++(shard_stats.pending_read_num);

  return repeat_with_onode<ceph::bufferlist>(
    ch,
    oid,
    Transaction::src_t::READ,
    "read",
    op_type_t::READ,
    op_flags,
    [this, offset, len, op_flags](auto &t, auto &onode) {
    return _read(t, onode, offset, len, op_flags);
  }).finally([this] {
    assert(shard_stats.pending_read_num);
    --(shard_stats.pending_read_num);
  });
}

SeaStore::Shard::base_errorator::future<bool>
SeaStore::Shard::exists(
  CollectionRef c,
  const ghobject_t& oid,
  uint32_t op_flags)
{
  LOG_PREFIX(SeaStoreS::exists);
  ++(shard_stats.read_num);
  ++(shard_stats.pending_read_num);

  return repeat_with_onode<bool>(
    c,
    oid,
    Transaction::src_t::READ,
    "exists",
    op_type_t::READ,
    op_flags,
    [FNAME](auto& t, auto&) {
    DEBUGT("exists", t);
    return seastar::make_ready_future<bool>(true);
  }).handle_error(
    crimson::ct_error::enoent::handle([FNAME] {
      DEBUG("not exists");
      return seastar::make_ready_future<bool>(false);
    }),
    crimson::ct_error::assert_all{"unexpected error"}
  ).finally([this] {
    assert(shard_stats.pending_read_num);
    --(shard_stats.pending_read_num);
  });
}

SeaStore::Shard::read_errorator::future<ceph::bufferlist>
SeaStore::Shard::readv(
  CollectionRef ch,
  const ghobject_t& _oid,
  interval_set<uint64_t>& m,
  uint32_t op_flags)
{
  LOG_PREFIX(SeaStoreS::readv);
  DEBUG("cid={} oid={} op_flags=0x{:x} {} intervals",
        ch->get_cid(), _oid, op_flags, m.num_intervals());

  return seastar::do_with(
    _oid,
    ceph::bufferlist{},
    [ch, op_flags, this, FNAME, &m](auto &oid, auto &ret) {
    return crimson::do_for_each(
      m,
      [ch, op_flags, this, &oid, &ret](auto &p) {
      return read(
	ch, oid, p.first, p.second, op_flags
	).safe_then([&ret](auto bl) {
        ret.claim_append(bl);
      });
    }).safe_then([&ret, FNAME] {
      DEBUG("got bl length=0x{:x}", ret.length());
      return read_errorator::make_ready_future<ceph::bufferlist>
        (std::move(ret));
    });
  });
}

using crimson::os::seastore::omap_manager::BtreeOMapManager;

SeaStore::Shard::_omap_get_value_ret
SeaStore::Shard::_get_attr(
  Transaction& t,
  Onode& onode,
  std::string_view name) const
{
  LOG_PREFIX(SeaStoreS::_get_attr);
  auto& layout = onode.get_layout();
  if (name == OI_ATTR && layout.oi_size) {
    ceph::bufferlist bl;
    bl.append(ceph::bufferptr(&layout.oi[0], layout.oi_size));
    DEBUGT("got OI_ATTR, value length=0x{:x}", t, bl.length());
    return seastar::make_ready_future<ceph::bufferlist>(std::move(bl));
  }
  if (name == SS_ATTR && layout.ss_size) {
    ceph::bufferlist bl;
    bl.append(ceph::bufferptr(&layout.ss[0], layout.ss_size));
    DEBUGT("got SS_ATTR, value length=0x{:x}", t, bl.length());
    return seastar::make_ready_future<ceph::bufferlist>(std::move(bl));
  }
  DEBUGT("name={} ...", t, name);
  return _omap_get_value(
    t,
    layout.xattr_root.get(
      onode.get_metadata_hint(device->get_block_size())),
    name);
}

SeaStore::Shard::get_attr_errorator::future<ceph::bufferlist>
SeaStore::Shard::get_attr(
  CollectionRef ch,
  const ghobject_t& oid,
  std::string_view name,
  uint32_t op_flags) const
{
  ++(shard_stats.read_num);
  ++(shard_stats.pending_read_num);

  return repeat_with_onode<ceph::bufferlist>(
    ch,
    oid,
    Transaction::src_t::READ,
    "get_attr",
    op_type_t::GET_ATTR,
    op_flags,
    [this, name](auto &t, auto& onode) {
    return _get_attr(t, onode, name);
  }).handle_error(
    crimson::ct_error::input_output_error::assert_failure{
      "EIO when getting attrs"},
    crimson::ct_error::pass_further_all{}
  ).finally([this] {
    assert(shard_stats.pending_read_num);
    --(shard_stats.pending_read_num);
  });
}

SeaStore::base_iertr::future<SeaStore::Shard::attrs_t>
SeaStore::Shard::_get_attrs(
  Transaction& t,
  Onode& onode)
{
  LOG_PREFIX(SeaStoreS::_get_attrs);
  DEBUGT("...", t);
  auto& layout = onode.get_layout();
  return do_omap_get_values(t, onode, std::nullopt,
    onode.get_layout().xattr_root
  ).si_then([&layout, &t, FNAME](auto p) {
    auto& attrs = std::get<1>(p);
    DEBUGT("got {} attrs, OI length=0x{:x}, SS length=0x{:x}",
           t, attrs.size(), (uint32_t)layout.oi_size, (uint32_t)layout.ss_size);
    ceph::bufferlist bl;
    if (layout.oi_size) {
      bl.append(ceph::bufferptr(&layout.oi[0], layout.oi_size));
      attrs.emplace(OI_ATTR, std::move(bl));
    }
    if (layout.ss_size) {
      bl.clear();
      bl.append(ceph::bufferptr(&layout.ss[0], layout.ss_size));
      attrs.emplace(SS_ATTR, std::move(bl));
    }
    return seastar::make_ready_future<attrs_t>(std::move(attrs));
  });
}

SeaStore::Shard::get_attrs_ertr::future<SeaStore::Shard::attrs_t>
SeaStore::Shard::get_attrs(
  CollectionRef ch,
  const ghobject_t& oid,
  uint32_t op_flags)
{
  ++(shard_stats.read_num);
  ++(shard_stats.pending_read_num);

  return repeat_with_onode<attrs_t>(
    ch,
    oid,
    Transaction::src_t::READ,
    "get_attrs",
    op_type_t::GET_ATTRS,
    op_flags,
    [this](auto &t, auto& onode) {
    return _get_attrs(t, onode);
  }).handle_error(
    crimson::ct_error::input_output_error::assert_failure{
      "EIO when getting attrs"},
    crimson::ct_error::pass_further_all{}
  ).finally([this] {
    assert(shard_stats.pending_read_num);
    --(shard_stats.pending_read_num);
  });
}

seastar::future<struct stat> SeaStore::Shard::_stat(
  Transaction& t,
  Onode& onode,
  const ghobject_t& oid)
{
  LOG_PREFIX(SeaStoreS::_stat);
  struct stat st;
  auto &olayout = onode.get_layout();
  st.st_size = olayout.size;
  st.st_blksize = device->get_block_size();
  st.st_blocks = (st.st_size + st.st_blksize - 1) / st.st_blksize;
  st.st_nlink = 1;
  DEBUGT("oid={}, size=0x{:x}, blksize=0x{:x}",
         t, oid, st.st_size, st.st_blksize);
  return seastar::make_ready_future<struct stat>(st);
}

seastar::future<struct stat> SeaStore::Shard::stat(
  CollectionRef c,
  const ghobject_t& oid,
  uint32_t op_flags)
{
  ++(shard_stats.read_num);
  ++(shard_stats.pending_read_num);

  return repeat_with_onode<struct stat>(
    c,
    oid,
    Transaction::src_t::READ,
    "stat",
    op_type_t::STAT,
    op_flags,
    [this, oid](auto &t, auto &onode) {
    return _stat(t, onode, oid);
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStoreS::stat"
    }
  ).finally([this] {
    assert(shard_stats.pending_read_num);
    --(shard_stats.pending_read_num);
  });
}

SeaStore::Shard::get_attr_errorator::future<ceph::bufferlist>
SeaStore::Shard::omap_get_header(
  CollectionRef ch,
  const ghobject_t& oid,
  uint32_t op_flags)
{
  return get_attr(ch, oid, OMAP_HEADER_XATTR_KEY, op_flags);
}

const omap_root_le_t& SeaStore::Shard::select_log_omap_root(Onode& onode)
{
  const auto &log_root_le = onode.get_layout().log_root;
  laddr_t hint = onode.get_metadata_hint(device->get_block_size());
  if (log_root_le.get(hint).is_null()) {
    return onode.get_layout().omap_root;
  } else {
    ceph_assert(onode.get_layout().omap_root.get(hint).is_null());
    return log_root_le;
  }
}

SeaStore::base_iertr::future<SeaStore::Shard::omap_values_t>
SeaStore::Shard::do_omap_get_values(
  Transaction& t,
  Onode& onode,
  const omap_keys_t& keys)
{
  LOG_PREFIX(SeaStoreS::do_omap_get_values);
  DEBUGT("{} keys ...", t, keys.size());
  laddr_t hint = onode.get_metadata_hint(device->get_block_size());
  auto r = select_log_omap_root(onode);
  omap_root_t root = r.get(hint);
  return _omap_get_values(
    t,
    std::move(root),
    keys);
}

SeaStore::Shard::read_errorator::future<SeaStore::Shard::omap_values_t>
SeaStore::Shard::omap_get_values(
  CollectionRef ch,
  const ghobject_t &oid,
  const omap_keys_t &keys,
  uint32_t op_flags)
{
  ++(shard_stats.read_num);
  ++(shard_stats.pending_read_num);

  return repeat_with_onode<omap_values_t>(
    ch,
    oid,
    Transaction::src_t::READ,
    "omap_get_values",
    op_type_t::OMAP_GET_VALUES,
    op_flags,
    [this, keys](auto &t, auto &onode) {
    return do_omap_get_values(t, onode, keys);
  }).finally([this] {
    assert(shard_stats.pending_read_num);
    --(shard_stats.pending_read_num);
  });
}

SeaStore::Shard::_omap_get_value_ret
SeaStore::Shard::_omap_get_value(
  Transaction &t,
  omap_root_t &&root,
  std::string_view key) const
{
  return seastar::do_with(
    BtreeOMapManager(*transaction_manager),
    std::move(root),
    std::string(key),
    [&t](auto &manager, auto& root, auto& key) -> _omap_get_value_ret {
    LOG_PREFIX(SeaStoreS::_omap_get_value);
    if (root.is_null()) {
      DEBUGT("key={} is absent because of null root", t, key);
      return crimson::ct_error::enodata::make();
    }
    return manager.omap_get_value(root, t, key
    ).si_then([&key, &t, FNAME](auto opt) -> _omap_get_value_ret {
      if (!opt) {
        DEBUGT("key={} is absent", t, key);
        return crimson::ct_error::enodata::make();
      }
      DEBUGT("key={}, value length=0x{:x}", t, key, opt->length());
      return seastar::make_ready_future<ceph::bufferlist>(std::move(*opt));
    });
  });
}

SeaStore::base_iertr::future<SeaStore::Shard::omap_values_t>
SeaStore::Shard::_omap_get_values(
  Transaction &t,
  omap_root_t &&omap_root,
  const omap_keys_t &keys) const
{
  LOG_PREFIX(SeaStoreS::_omap_get_values);
  if (omap_root.is_null()) {
    DEBUGT("{} keys are absent because of null root", t, keys.size());
    return seastar::make_ready_future<omap_values_t>();
  }
  return seastar::do_with(
    BtreeOMapManager(*transaction_manager),
    std::move(omap_root),
    omap_values_t(),
    [&t, &keys, FNAME](auto &manager, auto &root, auto &ret) {
    return trans_intr::do_for_each(
      keys.begin(),
      keys.end(),
      [&t, &manager, &root, &ret](auto &key) {
      return manager.omap_get_value(
        root,
        t,
        key
      ).si_then([&ret, &key](auto &&p) {
        if (p) {
          bufferlist bl;
          bl.append(*p);
          ret.emplace(
            std::move(key),
            std::move(bl));
        }
        return seastar::now();
      });
    }).si_then([&t, &ret, &keys, FNAME] {
      DEBUGT("{} keys got {} values", t, keys.size(), ret.size());
      return std::move(ret);
    });
  });
}

SeaStore::Shard::omap_list_ret
SeaStore::Shard::omap_list(
  Onode &onode,
  const omap_root_le_t& omap_root,
  Transaction& t,
  const std::optional<std::string>& start,
  OMapManager::omap_list_config_t config) const
{
  auto root = omap_root.get(
    onode.get_metadata_hint(device->get_block_size()));
  if (root.is_null()) {
    return seastar::make_ready_future<omap_list_bare_ret>(
      true, omap_values_t{}
    );
  }
  return seastar::do_with(
    BtreeOMapManager(*transaction_manager),
    root,
    start,
    std::optional<std::string>(std::nullopt),
    [&t, config](auto &manager, auto &root, auto &start, auto &end) {
      return manager.omap_list(root, t, start, end, config);
  });
}

SeaStore::base_iertr::future<SeaStore::Shard::omap_values_paged_t>
SeaStore::Shard::do_omap_get_values(
  Transaction& t,
  Onode& onode,
  const std::optional<std::string>& start,
  const omap_root_le_t& omap_root)
{
  LOG_PREFIX(SeaStoreS::do_omap_get_values);
  DEBUGT("start={} type={} ...", t, start.has_value() ? *start : "",
    omap_root.get_type());
  return omap_list(
    onode,
    omap_root,
    t,
    start,
    OMapManager::omap_list_config_t()
      .with_inclusive(false, false)
      .without_max()
  ).si_then([FNAME, &t](omap_values_paged_t ret) {
    DEBUGT("got {} values, complete={}",
           t, std::get<1>(ret).size(), std::get<0>(ret));
    return ret;
  });
}

SeaStore::Shard::read_errorator::future<SeaStore::Shard::omap_values_paged_t>
SeaStore::Shard::omap_get_values(
  CollectionRef ch,
  const ghobject_t &oid,
  const std::optional<std::string> &start,
  uint32_t op_flags)
{
  ++(shard_stats.read_num);
  ++(shard_stats.pending_read_num);

  return repeat_with_onode<omap_values_paged_t>(
    ch,
    oid,
    Transaction::src_t::READ,
    "omap_get_values2",
    op_type_t::OMAP_GET_VALUES2,
    op_flags,
    [this, start](auto &t, auto &onode) {
    const auto &root = select_log_omap_root(onode);
    return do_omap_get_values(t, onode, start, root);
  }).finally([this] {
    assert(shard_stats.pending_read_num);
    --(shard_stats.pending_read_num);
  });
}

SeaStore::base_iertr::future<SeaStore::Shard::fiemap_ret_t>
SeaStore::Shard::_fiemap(
  Transaction &t,
  Onode &onode,
  uint64_t off,
  uint64_t len) const
{
  LOG_PREFIX(SeaStoreS::_fiemap);
  size_t size = onode.get_layout().size;
  if (off >= size) {
    DEBUGT("0x{:x}~0x{:x} onode-size=0x{:x}, got none",
           t, off, len, size);
    return seastar::make_ready_future<std::map<uint64_t, uint64_t>>();
  }
  DEBUGT("0x{:x}~0x{:x} onode-size=0x{:x} ...",
         t, off, len, size);
  size_t adjust_len = (len == 0) ?
    size - off:
    std::min(size - off, len);
  return seastar::do_with(
    ObjectDataHandler(max_object_size),
    [this, off, adjust_len, &t, &onode](auto &objhandler) {
    return objhandler.fiemap(
      ObjectDataHandler::context_t{
        *transaction_manager,
        t,
        onode,
      },
      off,
      adjust_len);
  }).si_then([FNAME, &t](auto ret) {
    DEBUGT("got {} intervals", t, ret.size());
    return ret;
  });
}

SeaStore::Shard::read_errorator::future<SeaStore::Shard::fiemap_ret_t>
SeaStore::Shard::fiemap(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t off,
  uint64_t len,
  uint32_t op_flags)
{
  ++(shard_stats.read_num);
  ++(shard_stats.pending_read_num);

  return repeat_with_onode<fiemap_ret_t>(
    ch,
    oid,
    Transaction::src_t::READ,
    "fiemap",
    op_type_t::READ,
    op_flags,
    [this, off, len](auto &t, auto &onode) {
    return _fiemap(t, onode, off, len);
  }).finally([this] {
    assert(shard_stats.pending_read_num);
    --(shard_stats.pending_read_num);
  });
}

void SeaStore::Shard::on_error(ceph::os::Transaction &t) {
  LOG_PREFIX(SeaStoreS::on_error);
  ERROR(" transaction dump:\n");
  JSONFormatter f(true);
  f.open_object_section("transaction");
  t.dump(&f);
  f.close_section();
  std::stringstream str;
  f.flush(str);
  ERROR("{}", str.str());
  abort();
}

seastar::future<> SeaStore::Shard::do_transaction_no_callbacks(
  CollectionRef _ch,
  ceph::os::Transaction&& _t)
{
  ++(shard_stats.io_num);
  ++(shard_stats.pending_io_num);
  ++(shard_stats.starting_io_num);

  // repeat_with_internal_context ensures ordering via collection lock
  auto num_bytes = _t.get_num_bytes();
  return repeat_with_internal_context(
    _ch,
    std::move(_t),
    Transaction::src_t::MUTATE,
    "do_transaction",
    op_type_t::DO_TRANSACTION,
    [this, num_bytes](auto &ctx) {
      LOG_PREFIX(SeaStoreS::do_transaction_no_callbacks);
      return with_trans_intr(*ctx.transaction, [&ctx, this, FNAME, num_bytes](auto &t) {
        DEBUGT("cid={}, {} operations, 0x{:x} bytes, {} colls, {} objects ...",
               t, ctx.ch->get_cid(),
               ctx.ext_transaction.get_num_ops(),
               num_bytes,
               ctx.iter.colls.size(),
               ctx.iter.objects.size());
#ifndef NDEBUG
	TRACET(" transaction dump:\n", t);
	JSONFormatter f(true);
	f.open_object_section("transaction");
	ctx.ext_transaction.dump(&f);
	f.close_section();
	std::stringstream str;
	f.flush(str);
	TRACET("{}", t, str.str());
#endif
        return seastar::do_with(
	  std::vector<OnodeRef>(ctx.iter.objects.size()),
          [this, &ctx](auto& onodes)
        {
          return trans_intr::repeat(
            [this, &ctx, &onodes]()
            -> tm_iertr::future<seastar::stop_iteration>
          {
            if (ctx.iter.have_op()) {
              return _do_transaction_step(
                ctx, ctx.ch, onodes, ctx.iter
              ).si_then([] {
                return seastar::make_ready_future<seastar::stop_iteration>(
                  seastar::stop_iteration::no);
              });
            } else {
              return seastar::make_ready_future<seastar::stop_iteration>(
                seastar::stop_iteration::yes);
            };
          });
        }).si_then([this, &ctx] {
          return transaction_manager->submit_transaction(*ctx.transaction);
        });
      }).safe_then([FNAME, &ctx] {
        DEBUGT("done", *ctx.transaction);
      });
    }
  ).finally([this] {
    assert(shard_stats.pending_io_num);
    --(shard_stats.pending_io_num);
    // XXX: it's wrong to assume no failure
    --(shard_stats.processing_postlock_io_num);
  });
}


seastar::future<> SeaStore::Shard::flush(CollectionRef ch)
{
  ++(shard_stats.flush_num);
  ++(shard_stats.pending_flush_num);

  return seastar::do_with(
    get_dummy_ordering_handle(),
    [this, ch](auto &handle) {
      return handle.take_collection_lock(
	static_cast<SeastoreCollection&>(*ch).ordering_lock
      ).then([this, &handle] {
	return transaction_manager->flush(handle);
      });
    }
  ).finally([this] {
    assert(shard_stats.pending_flush_num);
    --(shard_stats.pending_flush_num);
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_do_transaction_step(
  internal_context_t &ctx,
  CollectionRef &col,
  std::vector<OnodeRef> &onodes,
  ceph::os::Transaction::iterator &i)
{
  LOG_PREFIX(SeaStoreS::_do_transaction_step);
  auto op = i.decode_op();

  using ceph::os::Transaction;
  if (op->op == Transaction::OP_NOP) {
    DEBUGT("op NOP", *ctx.transaction);
    return tm_iertr::now();
  }

  switch (op->op) {
    case Transaction::OP_RMCOLL:
    {
      coll_t cid = i.get_cid(op->cid);
      DEBUGT("op RMCOLL, cid={} ...", *ctx.transaction, cid);
      return _remove_collection(ctx, cid);
    }
    case Transaction::OP_MKCOLL:
    {
      coll_t cid = i.get_cid(op->cid);
      DEBUGT("op MKCOLL, cid={} ...", *ctx.transaction, cid);
      return _create_collection(ctx, cid, op->split_bits);
    }
    case Transaction::OP_COLL_HINT:
    {
      DEBUGT("op COLL_HINT", *ctx.transaction);
      ceph::bufferlist hint;
      i.decode_bl(hint);
      return tm_iertr::now();
    }
  }

  using onode_iertr = OnodeManager::get_onode_iertr::extend<
    crimson::ct_error::value_too_large>;
  auto fut = onode_iertr::make_ready_future<OnodeRef>(OnodeRef());
  bool create = false;
  if (op->op == Transaction::OP_TOUCH ||
      op->op == Transaction::OP_CREATE ||
      op->op == Transaction::OP_WRITE ||
      op->op == Transaction::OP_ZERO) {
    create = true;
  }
  if (!onodes[op->oid]) {
    const ghobject_t& oid = i.get_oid(op->oid);
    if (!create) {
      DEBUGT("op {}, get oid={} ...",
             *ctx.transaction, (uint32_t)op->op, oid);
      fut = onode_manager->get_onode(*ctx.transaction, oid);
    } else {
      DEBUGT("op {}, get_or_create oid={} ...",
             *ctx.transaction, (uint32_t)op->op, oid);
      fut = onode_manager->get_or_create_onode(*ctx.transaction, oid);
    }
  }
  return fut.si_then([&, op, this, FNAME](auto get_onode) {
    OnodeRef& onode = onodes[op->oid];
    if (!onode) {
      assert(get_onode);
      onode = get_onode;
    }
    OnodeRef& d_onode = onodes[op->dest_oid];
    if ((op->op == Transaction::OP_CLONE
	  || op->op == Transaction::OP_COLL_MOVE_RENAME)
	&& !d_onode) {
      const ghobject_t& dest_oid = i.get_oid(op->dest_oid);
      DEBUGT("op {}, get_or_create dest oid={} ...",
             *ctx.transaction, (uint32_t)op->op, dest_oid);
      //TODO: use when_all_succeed after making onode tree
      //      support parallel extents loading
      return onode_manager->get_or_create_onode(*ctx.transaction, dest_oid
      ).si_then([&d_onode](auto dest_onode) {
	assert(dest_onode);
	assert(!d_onode);
	d_onode = dest_onode;
	return seastar::now();
      });
    } else {
      return OnodeManager::get_or_create_onode_iertr::now();
    }
  }).si_then([&ctx, &i, &onodes, op, this, FNAME]() -> tm_ret {
    const ghobject_t& oid = i.get_oid(op->oid);
    OnodeRef& onode = onodes[op->oid];
    assert(onode);
    try {
      switch (op->op) {
      case Transaction::OP_REMOVE:
      {
        DEBUGT("op REMOVE, oid={} ...", *ctx.transaction, oid);
        return _remove(ctx, onode
	).si_then([&onode] {
	  onode.reset();
	});
      }
      case Transaction::OP_CREATE:
      case Transaction::OP_TOUCH:
      {
        DEBUGT("op CREATE/TOUCH, oid={} ...", *ctx.transaction, oid);
        return _touch(ctx, onode);
      }
      case Transaction::OP_WRITE:
      {
        uint64_t off = op->off;
        uint64_t len = op->len;
        uint32_t fadvise_flags = i.get_fadvise_flags();
        ceph::bufferlist bl;
        i.decode_bl(bl);
        DEBUGT("op WRITE, oid={}, 0x{:x}~0x{:x}, flags=0x{:x} ...",
               *ctx.transaction, oid, off, len, fadvise_flags);
        return _write(
	  ctx, onode, off, len, std::move(bl),
	  fadvise_flags);
      }
      case Transaction::OP_TRUNCATE:
      {
        uint64_t off = op->off;
        DEBUGT("op TRUNCATE, oid={}, 0x{:x} ...", *ctx.transaction, oid, off);
        return _truncate(ctx, onode, off);
      }
      case Transaction::OP_SETATTR:
      {
        std::string name = i.decode_string();
        std::map<std::string, bufferlist> to_set;
        ceph::bufferlist& bl = to_set[name];
        i.decode_bl(bl);
        DEBUGT("op SETATTR, oid={}, attr name={}, value length=0x{:x} ...",
               *ctx.transaction, oid, name, bl.length());
        return _setattrs(ctx, onode, std::move(to_set));
      }
      case Transaction::OP_SETATTRS:
      {
        std::map<std::string, bufferlist> to_set;
        i.decode_attrset(to_set);
        DEBUGT("op SETATTRS, oid={}, attrs size={} ...",
               *ctx.transaction, oid, to_set.size());
        return _setattrs(ctx, onode, std::move(to_set));
      }
      case Transaction::OP_RMATTR:
      {
        std::string name = i.decode_string();
        DEBUGT("op RMATTR, oid={}, attr name={} ...",
               *ctx.transaction, oid, name);
        return _rmattr(ctx, onode, name);
      }
      case Transaction::OP_RMATTRS:
      {
        DEBUGT("op RMATTRS, oid={} ...", *ctx.transaction, oid);
        return _rmattrs(ctx, onode);
      }
      case Transaction::OP_OMAP_SETKEYS:
      {
        std::map<std::string, ceph::bufferlist> aset;
        i.decode_attrset(aset);
	const omap_root_le_t &root = select_log_omap_root(*onode);
        DEBUGT("op OMAP_SETKEYS, oid={}, omap size={}, type={} ...",
               *ctx.transaction, oid, aset.size(), root.get_type());
        return _omap_set_values(
          ctx,
          onode,
          std::move(aset),
	  root);
      }
      case Transaction::OP_OMAP_SETHEADER:
      {
        ceph::bufferlist bl;
        i.decode_bl(bl);
        DEBUGT("op OMAP_SETHEADER, oid={}, length=0x{:x} ...",
               *ctx.transaction, oid, bl.length());
        return _omap_set_header(ctx, onode, std::move(bl));
      }
      case Transaction::OP_OMAP_RMKEYS:
      {
        omap_keys_t keys;
        i.decode_keyset(keys);
	const omap_root_le_t &root = select_log_omap_root(*onode);
        DEBUGT("op OMAP_RMKEYS, oid={}, omap size={}, type={} ...",
               *ctx.transaction, oid, keys.size(), root.get_type());
        return _omap_rmkeys(
          ctx,
          onode,
          std::move(keys),
	  root);
      }
      case Transaction::OP_OMAP_RMKEYRANGE:
      {
        std::string first, last;
        first = i.decode_string();
        last = i.decode_string();
	const omap_root_le_t &root = select_log_omap_root(*onode);
        DEBUGT("op OMAP_RMKEYRANGE, oid={}, first={}, last={}, type={}...",
               *ctx.transaction, oid, first, last, root.get_type());
        return _omap_rmkeyrange(
          ctx,
          onode,
          std::move(first), std::move(last),
	  root);
      }
      case Transaction::OP_OMAP_CLEAR:
      {
        DEBUGT("op OMAP_CLEAR, oid={} ...", *ctx.transaction, oid);
        return _omap_clear(ctx, onode);
      }
      case Transaction::OP_ZERO:
      {
        objaddr_t off = op->off;
        extent_len_t len = op->len;
        DEBUGT("op ZERO, oid={}, 0x{:x}~0x{:x} ...",
               *ctx.transaction, oid, off, len);
        return _zero(ctx, onode, off, len);
      }
      case Transaction::OP_SETALLOCHINT:
      {
        DEBUGT("op SETALLOCHINT, oid={}, not implemented",
               *ctx.transaction, oid);
        // TODO
	if (op->hint & CEPH_OSD_ALLOC_HINT_FLAG_LOG) {
	  OnodeRef &onode = onodes[op->oid];
	  const auto &log_root = onode->get_layout().log_root.get(
	    onode->get_metadata_hint(device->get_block_size()));
	  const auto &omap_root = onode->get_layout().omap_root.get(
	    onode->get_metadata_hint(device->get_block_size()));
	  ceph_assert(log_root.is_null() && omap_root.is_null());
	  return BtreeOMapManager(*transaction_manager).initialize_omap(
	    *ctx.transaction, onode->get_metadata_hint(device->get_block_size()),
	    log_root.get_type()
	  ).si_then([&onode, &ctx](auto new_root) {
	    if (new_root.must_update()) {
	      onode->update_log_root(*ctx.transaction, new_root);
	    }
	  });
	}
        return tm_iertr::now();
      }
      case Transaction::OP_CLONE:
      {
        DEBUGT("op CLONE, oid={}, dest oid={} ...",
               *ctx.transaction, oid, i.get_oid(op->dest_oid));
	return _clone(ctx, onode, onodes[op->dest_oid]);
      }
      case Transaction::OP_COLL_MOVE_RENAME:
      {
        DEBUGT("op COLL_MOVE_RENAME, oid={}, dest oid={} ...",
               *ctx.transaction, oid, i.get_oid(op->dest_oid));
	ceph_assert(op->cid == op->dest_cid);
	return _rename(
	  ctx, onode, onodes[op->dest_oid]
	).si_then([&onode] {
	  onode.reset();
	});
      }
      default:
        ERROR("bad op {}", static_cast<unsigned>(op->op));
        return crimson::ct_error::input_output_error::make();
      }
    } catch (std::exception &e) {
      ERROR("got exception {}", e);
      return crimson::ct_error::input_output_error::make();
    }
  }).handle_error_interruptible(
    tm_iertr::pass_further{},
    crimson::ct_error::enoent::handle([op] {
      //OMAP_CLEAR, TRUNCATE, REMOVE etc ops will tolerate absent onode.
      if (op->op == Transaction::OP_CLONERANGE ||
          op->op == Transaction::OP_CLONE ||
          op->op == Transaction::OP_CLONERANGE2 ||
          op->op == Transaction::OP_COLL_ADD ||
          op->op == Transaction::OP_SETATTR ||
          op->op == Transaction::OP_SETATTRS ||
          op->op == Transaction::OP_RMATTR ||
          op->op == Transaction::OP_OMAP_SETKEYS ||
          op->op == Transaction::OP_OMAP_RMKEYS ||
          op->op == Transaction::OP_OMAP_RMKEYRANGE ||
          op->op == Transaction::OP_OMAP_SETHEADER) {
        ceph_abort_msg("unexpected enoent error");
      }
      return seastar::now();
    }),
    crimson::ct_error::assert_all{
      "Invalid error in SeaStoreS::do_transaction_step"
    }
  );
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_rename(
  internal_context_t &ctx,
  OnodeRef &onode,
  OnodeRef &d_onode)
{
  auto olayout = onode->get_layout();
  uint32_t size = olayout.size;
  auto omap_root = olayout.omap_root.get(
    d_onode->get_metadata_hint(device->get_block_size()));
  auto xattr_root = olayout.xattr_root.get(
    d_onode->get_metadata_hint(device->get_block_size()));
  auto log_root = olayout.log_root.get(
    d_onode->get_metadata_hint(device->get_block_size()));
  auto object_data = olayout.object_data.get();
  auto oi_bl = ceph::bufferlist::static_from_mem(
    &olayout.oi[0],
    (uint32_t)olayout.oi_size);
  auto ss_bl = ceph::bufferlist::static_from_mem(
    &olayout.ss[0],
    (uint32_t)olayout.ss_size);

  d_onode->update_onode_size(*ctx.transaction, size);
  d_onode->update_omap_root(*ctx.transaction, omap_root);
  d_onode->update_xattr_root(*ctx.transaction, xattr_root);
  d_onode->update_log_root(*ctx.transaction, log_root);
  d_onode->update_object_data(*ctx.transaction, object_data);
  d_onode->update_object_info(*ctx.transaction, oi_bl);
  d_onode->update_snapset(*ctx.transaction, ss_bl);
  return onode_manager->erase_onode(
    *ctx.transaction, onode
  ).handle_error_interruptible(
    crimson::ct_error::input_output_error::pass_further(),
    crimson::ct_error::assert_all{
      "Invalid error in SeaStoreS::_rename"}
  );
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_remove_omaps(
  internal_context_t &ctx,
  OnodeRef &onode,
  omap_root_t &&omap_root)
{
  if (omap_root.get_location() != L_ADDR_NULL) {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      std::move(omap_root),
      [&ctx, onode](auto &omap_manager, auto &omap_root) {
      return omap_manager.omap_clear(
	omap_root,
	*ctx.transaction
      ).handle_error_interruptible(
	crimson::ct_error::input_output_error::pass_further(),
	crimson::ct_error::assert_all{
	  "Invalid error in SeaStoreS::_remove_omaps"
	}
      );
    });
  }
  return tm_iertr::now();
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_remove(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  return _remove_omaps(
    ctx,
    onode,
    onode->get_layout().omap_root.get(
      onode->get_metadata_hint(device->get_block_size()))
  ).si_then([this, &ctx, onode]() mutable {
    return _remove_omaps(
      ctx,
      onode,
      onode->get_layout().xattr_root.get(
	onode->get_metadata_hint(device->get_block_size())));
  }).si_then([this, &ctx, onode]() mutable {
    return _remove_omaps(
      ctx,
      onode,
      onode->get_layout().log_root.get(
	onode->get_metadata_hint(device->get_block_size())));
  }).si_then([this, &ctx, onode] {
    return seastar::do_with(
      ObjectDataHandler(max_object_size),
      [=, this, &ctx](auto &objhandler) {
	return objhandler.clear(
	  ObjectDataHandler::context_t{
	    *transaction_manager,
	    *ctx.transaction,
	    *onode,
	  });
    });
  }).si_then([this, &ctx, onode]() mutable {
    return onode_manager->erase_onode(*ctx.transaction, onode);
  }).handle_error_interruptible(
    crimson::ct_error::input_output_error::pass_further(),
    crimson::ct_error::assert_all(
      "Invalid error in SeaStoreS::_remove"
    )
  );
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_touch(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  return tm_iertr::now();
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_write(
  internal_context_t &ctx,
  OnodeRef &onode,
  uint64_t offset, size_t len,
  ceph::bufferlist &&_bl,
  uint32_t fadvise_flags)
{
  const auto &object_size = onode->get_layout().size;
  if (offset + len > object_size) {
    onode->update_onode_size(
      *ctx.transaction,
      std::max<uint64_t>(offset + len, object_size));
  }
  return seastar::do_with(
    std::move(_bl),
    ObjectDataHandler(max_object_size),
    [=, this, &ctx, &onode](auto &bl, auto &objhandler) {
      return objhandler.write(
        ObjectDataHandler::context_t{
          *transaction_manager,
          *ctx.transaction,
          *onode,
        },
        offset,
        bl);
    });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_clone_omaps(
  internal_context_t &ctx,
  OnodeRef &onode,
  OnodeRef &d_onode,
  const omap_type_t otype)
{
  return trans_intr::repeat([&ctx, &onode, &d_onode, this, otype] {
    return seastar::do_with(
      std::optional<std::string>(std::nullopt),
      [&ctx, &onode, &d_onode, this, otype](auto &start) {
      auto& layout = onode->get_layout();
      return omap_list(
	*onode,
	layout.get_root(otype),
	*ctx.transaction,
	start,
	OMapManager::omap_list_config_t().with_inclusive(false, false)
      ).si_then([&ctx, &d_onode, this, otype, &start](auto p) mutable {
	auto complete = std::get<0>(p);
	auto &attrs = std::get<1>(p);
	if (attrs.empty()) {
	  assert(complete);
	  return tm_iertr::make_ready_future<
	    seastar::stop_iteration>(
	      seastar::stop_iteration::yes);
	}
	std::string nstart = attrs.rbegin()->first;
	return _omap_set_values(
	  ctx,
	  d_onode,
	  std::map<std::string, ceph::bufferlist>(attrs.begin(), attrs.end()),
	  d_onode->get_layout().get_root(otype)
	).si_then([complete, nstart=std::move(nstart),
		  &start]() mutable {
	  if (complete) {
	    return seastar::make_ready_future<
	      seastar::stop_iteration>(
		seastar::stop_iteration::yes);
	  } else {
	    start = std::move(nstart);
	    return seastar::make_ready_future<
	      seastar::stop_iteration>(
		seastar::stop_iteration::no);
	  }
	});
      });
    });
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_clone(
  internal_context_t &ctx,
  OnodeRef &onode,
  OnodeRef &d_onode)
{
  return seastar::do_with(
    ObjectDataHandler(max_object_size),
    [this, &ctx, &onode, &d_onode](auto &objHandler) {
    auto &object_size = onode->get_layout().size;
    d_onode->update_onode_size(*ctx.transaction, object_size);
    return objHandler.clone(
      ObjectDataHandler::context_t{
	*transaction_manager,
	*ctx.transaction,
	*onode,
	d_onode.get()});
  }).si_then([&ctx, &onode, &d_onode, this] {
    return _clone_omaps(ctx, onode, d_onode, omap_type_t::XATTR);
  }).si_then([&ctx, &onode, &d_onode, this] {
    return _clone_omaps(ctx, onode, d_onode, omap_type_t::OMAP);
  }).si_then([&ctx, &onode, &d_onode, this] {
    return _clone_omaps(ctx, onode, d_onode, omap_type_t::LOG);
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_zero(
  internal_context_t &ctx,
  OnodeRef &onode,
  objaddr_t offset,
  extent_len_t len)
{
  if (offset + len >= max_object_size) {
    LOG_PREFIX(SeaStoreS::_zero);
    ERRORT("0x{:x}~0x{:x} >= 0x{:x}",
           *ctx.transaction, offset, len, max_object_size);
    return crimson::ct_error::input_output_error::make();
  }
  const auto &object_size = onode->get_layout().size;
  onode->update_onode_size(
    *ctx.transaction,
    std::max<uint64_t>(offset + len, object_size));
  return seastar::do_with(
    ObjectDataHandler(max_object_size),
    [=, this, &ctx, &onode](auto &objhandler) {
      return objhandler.zero(
        ObjectDataHandler::context_t{
          *transaction_manager,
          *ctx.transaction,
          *onode,
        },
        offset,
        len);
  });
}

SeaStore::Shard::omap_set_kvs_ret
SeaStore::Shard::_omap_set_kvs(
  const OnodeRef &onode,
  const omap_root_le_t& omap_root,
  Transaction& t,
  std::map<std::string, ceph::bufferlist>&& kvs)
{
  return seastar::do_with(
    BtreeOMapManager(*transaction_manager),
    omap_root.get(onode->get_metadata_hint(device->get_block_size())),
    [&, keys=std::move(kvs)](auto &omap_manager, auto &root) {
      tm_iertr::future<> maybe_create_root =
        !root.is_null() ?
        tm_iertr::now() :
        omap_manager.initialize_omap(
          t, onode->get_metadata_hint(device->get_block_size()),
	  root.get_type()
        ).si_then([&root](auto new_root) {
          root = new_root;
        });
      return maybe_create_root.si_then(
        [&, keys=std::move(keys)]() mutable {
          return omap_manager.omap_set_keys(root, t, std::move(keys));
      }).si_then([&] {
        return tm_iertr::make_ready_future<omap_root_t>(std::move(root));
      });
    }
  );
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_omap_set_values(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::map<std::string, ceph::bufferlist> &&aset,
  const omap_root_le_t &omap_root)
{
  return _omap_set_kvs(
    onode,
    omap_root,
    *ctx.transaction,
    std::move(aset)
  ).si_then([onode, &ctx](auto root) {
    if (root.must_update()) {
      if (root.get_type() == omap_type_t::OMAP) {
	onode->update_omap_root(*ctx.transaction, root);
      } else if (root.get_type() == omap_type_t::XATTR) {
	onode->update_xattr_root(*ctx.transaction, root);
      } else {
	ceph_assert(root.get_type() == omap_type_t::LOG);
	onode->update_log_root(*ctx.transaction, root);
      }
    }
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_omap_set_header(
  internal_context_t &ctx,
  OnodeRef &onode,
  ceph::bufferlist &&header)
{
  std::map<std::string, bufferlist> to_set;
  to_set[OMAP_HEADER_XATTR_KEY] = header;
  return _setattrs(ctx, onode,std::move(to_set));
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_omap_clear(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  return _xattr_rmattr(ctx, onode, std::string(OMAP_HEADER_XATTR_KEY)
  ).si_then([this, &ctx, &onode]() -> tm_ret {
    if (auto omap_root = onode->get_layout().omap_root.get(
      onode->get_metadata_hint(device->get_block_size()));
      omap_root.is_null()) {
      return seastar::now();
    } else {
      return seastar::do_with(
        BtreeOMapManager(*transaction_manager),
        onode->get_layout().omap_root.get(
          onode->get_metadata_hint(device->get_block_size())),
        [&ctx, &onode](
        auto &omap_manager,
        auto &omap_root) {
        return omap_manager.omap_clear(
          omap_root,
          *ctx.transaction
        ).si_then([&] {
          if (omap_root.must_update()) {
	    onode->update_omap_root(*ctx.transaction, omap_root);
          }
        });
      });
    }
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_omap_rmkeys(
  internal_context_t &ctx,
  OnodeRef &onode,
  omap_keys_t &&keys,
  const omap_root_le_t &_omap_root)
{
  auto omap_root = _omap_root.get(
    onode->get_metadata_hint(device->get_block_size()));
  if (omap_root.is_null()) {
    return seastar::now();
  } else {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      _omap_root.get(
        onode->get_metadata_hint(device->get_block_size())),
      std::move(keys),
      [&ctx, &onode](
	auto &omap_manager,
	auto &omap_root,
	auto &keys) {
          return trans_intr::do_for_each(
            keys.begin(),
            keys.end(),
            [&](auto &p) {
              return omap_manager.omap_rm_key(
                omap_root,
                *ctx.transaction,
                p);
            }
          ).si_then([&] {
            if (omap_root.must_update()) {
	      if (omap_root.get_type() == omap_type_t::OMAP) {
		onode->update_omap_root(*ctx.transaction, omap_root);
	      } else {
		ceph_assert(omap_root.get_type() == omap_type_t::LOG);
		onode->update_log_root(*ctx.transaction, omap_root);
	      }
            }
          });
      }
    );
  }
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_omap_rmkeyrange(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::string first,
  std::string last,
  const omap_root_le_t &_omap_root)
{
  if (first > last) {
    LOG_PREFIX(SeaStoreS::_omap_rmkeyrange);
    ERRORT("range error, first:{} > last:{}", *ctx.transaction, first, last);
    ceph_abort();
  }
  auto omap_root = _omap_root.get(
    onode->get_metadata_hint(device->get_block_size()));
  if (omap_root.is_null()) {
    return seastar::now();
  } else {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      _omap_root.get(
        onode->get_metadata_hint(device->get_block_size())),
      std::move(first),
      std::move(last),
      [&ctx, &onode](
	auto &omap_manager,
	auto &omap_root,
	auto &first,
	auto &last) {
      auto config = OMapManager::omap_list_config_t()
	.with_inclusive(true, false)
	.without_max();
      return omap_manager.omap_rm_key_range(
	omap_root,
	*ctx.transaction,
	first,
	last,
	config
      ).si_then([&] {
        if (omap_root.must_update()) {
	  if (omap_root.get_type() == omap_type_t::OMAP) {
	    onode->update_omap_root(*ctx.transaction, omap_root);
	  } else { 
	    ceph_assert(omap_root.get_type() == omap_type_t::LOG);
	    onode->update_log_root(*ctx.transaction, omap_root);
	  }
        }
      });
    });
  }
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_truncate(
  internal_context_t &ctx,
  OnodeRef &onode,
  uint64_t size)
{
  onode->update_onode_size(*ctx.transaction, size);
  return seastar::do_with(
    ObjectDataHandler(max_object_size),
    [=, this, &ctx, &onode](auto &objhandler) {
    return objhandler.truncate(
      ObjectDataHandler::context_t{
        *transaction_manager,
        *ctx.transaction,
        *onode
      },
      size);
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_setattrs(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::map<std::string, bufferlist>&& aset)
{
  LOG_PREFIX(SeaStoreS::_setattrs);
  auto fut = tm_iertr::now();
  auto& layout = onode->get_layout();
  if (auto it = aset.find(OI_ATTR); it != aset.end()) {
    auto& val = it->second;
    if (likely(val.length() <= onode_layout_t::MAX_OI_LENGTH)) {

      if (!layout.oi_size) {
	// if oi was not in the layout, it probably exists in the omap,
	// need to remove it first
	fut = _xattr_rmattr(ctx, onode, OI_ATTR);
      }
      onode->update_object_info(*ctx.transaction, val);
      aset.erase(it);
      DEBUGT("set oi in onode layout", *ctx.transaction);
    } else {
      onode->clear_object_info(*ctx.transaction);
    }
  }

  if (auto it = aset.find(SS_ATTR); it != aset.end()) {
    auto& val = it->second;
    if (likely(val.length() <= onode_layout_t::MAX_SS_LENGTH)) {

      if (!layout.ss_size) {
	fut = _xattr_rmattr(ctx, onode, SS_ATTR);
      }
      onode->update_snapset(*ctx.transaction, val);
      aset.erase(it);
      DEBUGT("set ss in onode layout", *ctx.transaction);
    } else {
      onode->clear_snapset(*ctx.transaction);
    }
  }

  if (aset.empty()) {
    DEBUGT("all attrs set in onode layout", *ctx.transaction);
    return fut;
  }

  DEBUGT("set attrs in omap", *ctx.transaction);
  return fut.si_then(
    [this, onode, &ctx, aset=std::move(aset)]() mutable {
    return _omap_set_values(
      ctx,
      onode,
      std::move(aset),
      onode->get_layout().xattr_root);
  });
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_rmattr(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::string name)
{
  auto& layout = onode->get_layout();
  if ((name == OI_ATTR) && (layout.oi_size > 0)) {
    onode->clear_object_info(*ctx.transaction);
    return tm_iertr::now();
  } else if ((name == SS_ATTR) && (layout.ss_size > 0)) {
    onode->clear_snapset(*ctx.transaction);
    return tm_iertr::now();
  } else {
    return _xattr_rmattr(
      ctx,
      onode,
      std::move(name));
  }
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_xattr_rmattr(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::string &&name)
{
  LOG_PREFIX(SeaStoreS::_xattr_rmattr);
  DEBUGT("onode={}", *ctx.transaction, *onode);
  auto xattr_root = onode->get_layout().xattr_root.get(
    onode->get_metadata_hint(device->get_block_size()));
  if (xattr_root.is_null()) {
    return seastar::now();
  } else {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      onode->get_layout().xattr_root.get(
        onode->get_metadata_hint(device->get_block_size())),
      std::move(name),
      [&ctx, &onode](auto &omap_manager, auto &xattr_root, auto &name) {
        return omap_manager.omap_rm_key(xattr_root, *ctx.transaction, name)
          .si_then([&] {
          if (xattr_root.must_update()) {
	    onode->update_xattr_root(*ctx.transaction, xattr_root);
          }
        });
    });
  }
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_rmattrs(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  onode->clear_object_info(*ctx.transaction);
  onode->clear_snapset(*ctx.transaction);
  return _xattr_clear(ctx, onode);
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_xattr_clear(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  LOG_PREFIX(SeaStoreS::_xattr_clear);
  DEBUGT("onode={}", *ctx.transaction, *onode);
  auto xattr_root = onode->get_layout().xattr_root.get(
    onode->get_metadata_hint(device->get_block_size()));
  if (xattr_root.is_null()) {
    return seastar::now();
  } else {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      onode->get_layout().xattr_root.get(
	onode->get_metadata_hint(device->get_block_size())),
      [&ctx, &onode](auto &omap_manager, auto &xattr_root) {
        return omap_manager.omap_clear(xattr_root, *ctx.transaction)
	  .si_then([&] {
	  if (xattr_root.must_update()) {
	    onode->update_xattr_root(*ctx.transaction, xattr_root);
          }
        });
    });
  }
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_create_collection(
  internal_context_t &ctx,
  const coll_t& cid, int bits)
{
  return transaction_manager->read_collection_root(
    *ctx.transaction
  ).si_then([=, this, &ctx](auto _cmroot) {
    return seastar::do_with(
      _cmroot,
      [=, this, &ctx](auto &cmroot) {
        return collection_manager->create(
          cmroot,
          *ctx.transaction,
          cid,
          bits
        ).si_then([this, &ctx, &cmroot] {
          if (cmroot.must_update()) {
            transaction_manager->write_collection_root(
              *ctx.transaction,
              cmroot);
          }
        });
      }
    );
  }).handle_error_interruptible(
    tm_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SeaStoreS::_create_collection"
    }
  );
}

SeaStore::Shard::tm_ret
SeaStore::Shard::_remove_collection(
  internal_context_t &ctx,
  const coll_t& cid)
{
  return transaction_manager->read_collection_root(
    *ctx.transaction
  ).si_then([=, this, &ctx](auto _cmroot) {
    return seastar::do_with(
      _cmroot,
      [=, this, &ctx](auto &cmroot) {
        return collection_manager->remove(
          cmroot,
          *ctx.transaction,
          cid
        ).si_then([this, &ctx, &cmroot] {
          // param here denotes whether it already existed, probably error
          if (cmroot.must_update()) {
            transaction_manager->write_collection_root(
              *ctx.transaction,
              cmroot);
          }
        });
      });
  }).handle_error_interruptible(
    tm_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SeaStoreS::_create_collection"
    }
  );
}

boost::intrusive_ptr<SeastoreCollection>
SeaStore::Shard::_get_collection(const coll_t& cid)
{
  return new SeastoreCollection{cid};
}

seastar::future<> SeaStore::write_meta(
  const std::string& key,
  const std::string& value) {
  LOG_PREFIX(SeaStore::write_meta);
  DEBUG("key={} value={} ...", key, value);

  ceph_assert(seastar::this_shard_id() == primary_core);
  return seastar::do_with(key, value,
    [this, FNAME](auto& key, auto& value) {
    return shard_stores.local().write_meta(key, value
    ).then([this, &key, &value] {
      return mdstore->write_meta(key, value);
    }).safe_then([FNAME, &key, &value] {
      DEBUG("key={} value={} done", key, value);
    }).handle_error(
      crimson::ct_error::assert_all{"Invalid error in SeaStore::write_meta"}
    );
  });
}

seastar::future<> SeaStore::Shard::write_meta(
  const std::string& key,
  const std::string& value)
{
  ++(shard_stats.io_num);
  ++(shard_stats.pending_io_num);
  // For TM::submit_transaction()
  ++(shard_stats.processing_inlock_io_num);

  return repeat_eagain([this, &key, &value] {
    ++(shard_stats.repeat_io_num);

    return transaction_manager->with_transaction_intr(
      Transaction::src_t::MUTATE,
      "write_meta",
      CACHE_HINT_NOCACHE,
      [this, &key, &value](auto& t)
    {
      LOG_PREFIX(SeaStoreS::write_meta);
      DEBUGT("key={} value={} ...", t, key, value);
      return transaction_manager->update_root_meta(
        t, key, value
      ).si_then([this, &t] {
        return transaction_manager->submit_transaction(t);
      });
    });
  }).handle_error(
    crimson::ct_error::assert_all{"Invalid error in SeaStoreS::write_meta"}
  ).finally([this] {
    assert(shard_stats.pending_io_num);
    --(shard_stats.pending_io_num);
    // XXX: it's wrong to assume no failure,
    // but failure leads to fatal error
    --(shard_stats.processing_postlock_io_num);
  });
}

seastar::future<std::tuple<int, std::string>>
SeaStore::read_meta(const std::string& key)
{
  LOG_PREFIX(SeaStore::read_meta);
  DEBUG("key={} ...", key);

  ceph_assert(seastar::this_shard_id() == primary_core);
  return mdstore->read_meta(key
  ).safe_then([key, FNAME](auto v) {
    if (v) {
      DEBUG("key={}, value={}", key, *v);
      return std::make_tuple(0, std::move(*v));
    } else {
      ERROR("key={} failed", key);
      return std::make_tuple(-1, std::string(""));
    }
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::read_meta"
    }
  );
}

seastar::future<std::string> SeaStore::get_default_device_class()
{
  using crimson::common::get_conf;
  std::string type = get_conf<std::string>("seastore_main_device_type");
  return seastar::make_ready_future<std::string>(type);
}

uuid_d SeaStore::Shard::get_fsid() const
{
  return device->get_meta().seastore_id;
}

void SeaStore::Shard::init_managers()
{
  transaction_manager.reset();
  collection_manager.reset();
  onode_manager.reset();
  shard_stats = {};

  transaction_manager = make_transaction_manager(
      device, secondaries, shard_stats, is_test);
  collection_manager = std::make_unique<collection_manager::FlatCollectionManager>(
      *transaction_manager);
  onode_manager = std::make_unique<crimson::os::seastore::onode::FLTreeOnodeManager>(
      *transaction_manager);
}

double SeaStore::Shard::reset_report_interval() const
{
  double seconds;
  auto now = seastar::lowres_clock::now();
  if (last_tp == seastar::lowres_clock::time_point::min()) {
    seconds = 0;
  } else {
    std::chrono::duration<double> duration_d = now - last_tp;
    seconds = duration_d.count();
  }
  last_tp = now;
  return seconds;
}

device_stats_t SeaStore::Shard::get_device_stats(
    bool report_detail, double seconds) const
{
  return transaction_manager->get_device_stats(report_detail, seconds);
}

shard_stats_t SeaStore::Shard::get_io_stats(
    bool report_detail, double seconds) const
{
  shard_stats_t ret = shard_stats;
  ret.minus(last_shard_stats);

  if (report_detail && seconds != 0) {
    LOG_PREFIX(SeaStoreS::get_io_stats);
    auto calc_conflicts = [](uint64_t ios, uint64_t repeats) {
      return (double)(repeats-ios)/ios;
    };
    INFO("iops={:.2f},{:.2f},{:.2f}({:.2f},{:.2f},{:.2f},{:.2f}),{:.2f} "
         "conflicts={:.2f},{:.2f},{:.2f}({:.2f},{:.2f},{:.2f},{:.2f}) "
         "outstanding={}({},{},{},{},{}),{},{},{}",
         // iops
         ret.io_num/seconds,
         ret.read_num/seconds,
         ret.get_bg_num()/seconds,
         ret.trim_alloc_num/seconds,
         ret.trim_dirty_num/seconds,
         ret.cleaner_main_num/seconds,
         ret.cleaner_cold_num/seconds,
         ret.flush_num/seconds,
         // conflicts
         calc_conflicts(ret.io_num, ret.repeat_io_num),
         calc_conflicts(ret.read_num, ret.repeat_read_num),
         calc_conflicts(ret.get_bg_num(), ret.get_repeat_bg_num()),
         calc_conflicts(ret.trim_alloc_num, ret.repeat_trim_alloc_num),
         calc_conflicts(ret.trim_dirty_num, ret.repeat_trim_dirty_num),
         calc_conflicts(ret.cleaner_main_num, ret.repeat_cleaner_main_num),
         calc_conflicts(ret.cleaner_cold_num, ret.repeat_cleaner_cold_num),
         // outstanding
         ret.pending_io_num,
         ret.starting_io_num,
         ret.waiting_collock_io_num,
         ret.waiting_throttler_io_num,
         ret.processing_inlock_io_num,
         ret.processing_postlock_io_num,
         ret.pending_read_num,
         ret.pending_bg_num,
         ret.pending_flush_num);
  }

  last_shard_stats = shard_stats;
  return ret;
}

cache_stats_t SeaStore::Shard::get_cache_stats(
    bool report_detail, double seconds) const
{
  return transaction_manager->get_cache_stats(
      report_detail, seconds);
}

std::unique_ptr<SeaStore> make_seastore(
  const std::string &device)
{
  auto mdstore = std::make_unique<FileMDStore>(device);
  return std::make_unique<SeaStore>(
    device,
    std::move(mdstore));
}

std::unique_ptr<SeaStore> make_test_seastore(
  SeaStore::MDStoreRef mdstore)
{
  return std::make_unique<SeaStore>(
    "",
    std::move(mdstore));
}

}
