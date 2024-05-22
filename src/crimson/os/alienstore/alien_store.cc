// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "alien_collection.h"
#include "alien_store.h"
#include "alien_log.h"

#include <algorithm>
#include <iterator>
#include <map>
#include <string_view>
#include <boost/algorithm/string/trim.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <seastar/core/alien.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/resource.hh>

#include "common/ceph_context.h"
#include "global/global_context.h"
#include "include/Context.h"
#include "os/ObjectStore.h"
#include "os/Transaction.h"

#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"
#include "crimson/os/futurized_store.h"

using std::map;
using std::set;
using std::string;

namespace {

seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_alienstore);
}

class OnCommit final: public Context
{
  const int cpuid;
  seastar::alien::instance &alien;
  seastar::promise<> &alien_done;
public:
  OnCommit(
    int id,
    seastar::promise<> &done,
    seastar::alien::instance &alien,
    ceph::os::Transaction& txn)
    : cpuid(id),
      alien(alien),
      alien_done(done) {
  }

  void finish(int) final {
    std::ignore = seastar::alien::submit_to(alien, cpuid,
        [&_alien_done=this->alien_done] {
      _alien_done.set_value();
      return seastar::make_ready_future<>();
    });
  }
};
}

namespace crimson::os {

using crimson::common::get_conf;

AlienStore::AlienStore(const std::string& type,
                       const std::string& path,
                       const ConfigValues& values)
  : type(type),
    path{path},
    values(values)
{
}

AlienStore::~AlienStore()
{
}

seastar::future<> AlienStore::start()
{
  cct = std::make_unique<CephContext>(
    CEPH_ENTITY_TYPE_OSD,
    CephContext::create_options { CODE_ENVIRONMENT_UTILITY, 0,
      [](const ceph::logging::SubsystemMap* subsys_map) {
	return new ceph::logging::CnLog(subsys_map, seastar::engine().alien(), seastar::this_shard_id());
      }
    }
  );
  g_ceph_context = cct.get();
  cct->_conf.set_config_values(values);
  cct->_log->start();

  store = ObjectStore::create(cct.get(), type, path);
  if (!store) {
    ceph_abort_msgf("unsupported objectstore type: %s", type.c_str());
  }
  auto cpu_cores = seastar::resource::parse_cpuset(
    get_conf<std::string>("crimson_alien_thread_cpu_cores"));
  //  crimson_alien_thread_cpu_cores are assigned to alien threads.
  if (!cpu_cores.has_value()) {
    // no core isolation by default, seastar_cpu_cores will be
    // shared between both alien and seastar reactor threads.
    cpu_cores = seastar::resource::parse_cpuset(
      get_conf<std::string>("crimson_seastar_cpu_cores"));
    ceph_assert(cpu_cores.has_value());
  }
  const auto num_threads =
    get_conf<uint64_t>("crimson_alien_op_num_threads");
  tp = std::make_unique<crimson::os::ThreadPool>(num_threads, 128, cpu_cores);
  return tp->start();
}

seastar::future<> AlienStore::stop()
{
  if (!tp) {
    // not really started yet
    return seastar::now();
  }
  return tp->submit([this] {
    for (auto [cid, ch]: coll_map) {
      static_cast<AlienCollection*>(ch.get())->collection.reset();
    }
    store.reset();
    cct.reset();
    g_ceph_context = nullptr;

  }).then([this] {
    return tp->stop();
  });
}

AlienStore::base_errorator::future<bool>
AlienStore::exists(
  CollectionRef ch,
  const ghobject_t& oid)
{
  return seastar::with_gate(op_gate, [=, this] {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, this] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->exists(c->collection, oid);
    });
  });
}

AlienStore::mount_ertr::future<> AlienStore::mount()
{
  logger().debug("{}", __func__);
  assert(tp);
  return tp->submit([this] {
    return store->mount();
  }).then([] (const int r) -> mount_ertr::future<> {
    if (r != 0) {
      return crimson::stateful_ec{
        std::error_code(-r, std::generic_category()) };
    } else {
      return mount_ertr::now();
    }
  });
}

seastar::future<> AlienStore::umount()
{
  logger().info("{}", __func__);
  if (!tp) {
    // not really started yet
    return seastar::now();
  }
  return op_gate.close().then([this] {
    return tp->submit([this] {
      return store->umount();
    });
  }).then([] (int r) {
    assert(r == 0);
    return seastar::now();
  });
}

AlienStore::mkfs_ertr::future<> AlienStore::mkfs(uuid_d osd_fsid)
{
  logger().debug("{}", __func__);
  store->set_fsid(osd_fsid);
  assert(tp);
  return tp->submit([this] {
    return store->mkfs();
  }).then([] (int r) -> mkfs_ertr::future<> {
    if (r != 0) {
      return crimson::stateful_ec{
        std::error_code(-r, std::generic_category()) };
    } else {
      return mkfs_ertr::now();
    }
  });
}

seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
AlienStore::list_objects(CollectionRef ch,
                        const ghobject_t& start,
                        const ghobject_t& end,
                        uint64_t limit) const
{
  logger().debug("{}", __func__);
  assert(tp);
  return do_with_op_gate(std::vector<ghobject_t>(), ghobject_t(),
                         [=, this] (auto &objects, auto &next) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()),
      [=, this, &objects, &next] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->collection_list(c->collection, start, end,
                                    store->get_ideal_list_max(),
                                    &objects, &next);
    }).then([&objects, &next] (int r) {
      assert(r == 0);
      return seastar::make_ready_future<
	std::tuple<std::vector<ghobject_t>, ghobject_t>>(
	  std::move(objects), std::move(next));
    });
  });
}

seastar::future<CollectionRef> AlienStore::create_new_collection(const coll_t& cid)
{
  logger().debug("{}", __func__);
  assert(tp);
  return tp->submit([this, cid] {
    return store->create_new_collection(cid);
  }).then([this, cid] (ObjectStore::CollectionHandle c) {
    CollectionRef ch;
    auto cp = coll_map.find(c->cid);
    if (cp == coll_map.end()) {
      ch = new AlienCollection(c);
      coll_map[c->cid] = ch;
    } else {
      ch = cp->second;
      auto ach = static_cast<AlienCollection*>(ch.get());
      if (ach->collection != c) {
        ach->collection = c;
      }
    }
    return seastar::make_ready_future<CollectionRef>(ch);
  });

}

seastar::future<CollectionRef> AlienStore::open_collection(const coll_t& cid)
{
  logger().debug("{}", __func__);
  assert(tp);
  return tp->submit([this, cid] {
    return store->open_collection(cid);
  }).then([this] (ObjectStore::CollectionHandle c) {
    if (!c) {
      return seastar::make_ready_future<CollectionRef>();
    }
    CollectionRef ch;
    auto cp = coll_map.find(c->cid);
    if (cp == coll_map.end()){
      ch = new AlienCollection(c);
      coll_map[c->cid] = ch;
    } else {
      ch = cp->second;
      auto ach = static_cast<AlienCollection*>(ch.get());
      if (ach->collection != c){
        ach->collection = c;
      }
    }
    return seastar::make_ready_future<CollectionRef>(ch);
  });
}

seastar::future<std::vector<coll_core_t>> AlienStore::list_collections()
{
  logger().debug("{}", __func__);
  assert(tp);

  return do_with_op_gate(std::vector<coll_t>{}, [this] (auto &ls) {
    return tp->submit([this, &ls] {
      return store->list_collections(ls);
    }).then([&ls] (int r) -> seastar::future<std::vector<coll_core_t>> {
      assert(r == 0);
      std::vector<coll_core_t> ret;
      ret.resize(ls.size());
      std::transform(
        ls.begin(), ls.end(), ret.begin(),
        [](auto p) { return std::make_pair(p, NULL_CORE); });
      return seastar::make_ready_future<std::vector<coll_core_t>>(std::move(ret));
    });
  });
}

seastar::future<> AlienStore::set_collection_opts(CollectionRef ch,
                                      const pool_opts_t& opts)
{
  logger().debug("{}", __func__);
  assert(tp);

  return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, this] {
    auto c = static_cast<AlienCollection*>(ch.get());
    return store->set_collection_opts(c->collection, opts);
  }).then([] (int r) {
    assert(r==0);
    return seastar::now();
  });
}

AlienStore::read_errorator::future<ceph::bufferlist>
AlienStore::read(CollectionRef ch,
                 const ghobject_t& oid,
                 uint64_t offset,
                 size_t len,
                 uint32_t op_flags)
{
  logger().debug("{}", __func__);
  assert(tp);
  return do_with_op_gate(ceph::bufferlist{}, [=, this] (auto &bl) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, this, &bl] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->read(c->collection, oid, offset, len, bl, op_flags);
    }).then([&bl] (int r) -> read_errorator::future<ceph::bufferlist> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else if (r == -EIO) {
        return crimson::ct_error::input_output_error::make();
      } else {
        return read_errorator::make_ready_future<ceph::bufferlist>(
          std::move(bl));
      }
    });
  });
}

AlienStore::read_errorator::future<ceph::bufferlist>
AlienStore::readv(CollectionRef ch,
		  const ghobject_t& oid,
		  interval_set<uint64_t>& m,
		  uint32_t op_flags)
{
  logger().debug("{}", __func__);
  assert(tp);
  return do_with_op_gate(ceph::bufferlist{},
    [this, ch, oid, &m, op_flags](auto& bl) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()),
      [this, ch, oid, &m, op_flags, &bl] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->readv(c->collection, oid, m, bl, op_flags);
    }).then([&bl](int r) -> read_errorator::future<ceph::bufferlist> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else if (r == -EIO) {
        return crimson::ct_error::input_output_error::make();
      } else {
        return read_errorator::make_ready_future<ceph::bufferlist>(
	  std::move(bl));
      }
    });
  });
}

AlienStore::get_attr_errorator::future<ceph::bufferlist>
AlienStore::get_attr(CollectionRef ch,
                     const ghobject_t& oid,
                     std::string_view name) const
{
  logger().debug("{}", __func__);
  assert(tp);
  return do_with_op_gate(ceph::bufferlist{}, std::string{name},
                         [=, this] (auto &value, const auto& name) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, this, &value, &name] {
      // XXX: `name` isn't a `std::string_view` anymore! it had to be converted
      // to `std::string` for the sake of extending life-time not only of
      // a _ptr-to-data_ but _data_ as well. Otherwise we would run into a use-
      // after-free issue.
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->getattr(c->collection, oid, name.c_str(), value);
    }).then([oid, &value](int r) -> get_attr_errorator::future<ceph::bufferlist> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else if (r == -ENODATA) {
        return crimson::ct_error::enodata::make();
      } else {
        return get_attr_errorator::make_ready_future<ceph::bufferlist>(
          std::move(value));
      }
    });
  });
}

AlienStore::get_attrs_ertr::future<AlienStore::attrs_t>
AlienStore::get_attrs(CollectionRef ch,
                      const ghobject_t& oid)
{
  logger().debug("{}", __func__);
  assert(tp);
  return do_with_op_gate(attrs_t{}, [=, this] (auto &aset) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, this, &aset] {
      auto c = static_cast<AlienCollection*>(ch.get());
      const auto r = store->getattrs(c->collection, oid, aset);
      return r;
    }).then([&aset] (int r) -> get_attrs_ertr::future<attrs_t> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else {
        return get_attrs_ertr::make_ready_future<attrs_t>(std::move(aset));
      }
    });
  });
}

auto AlienStore::omap_get_values(CollectionRef ch,
                                 const ghobject_t& oid,
                                 const set<string>& keys)
  -> read_errorator::future<omap_values_t>
{
  logger().debug("{}", __func__);
  assert(tp);
  return do_with_op_gate(omap_values_t{}, [=, this] (auto &values) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, this, &values] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->omap_get_values(c->collection, oid, keys,
		                    reinterpret_cast<map<string, bufferlist>*>(&values));
    }).then([&values] (int r) -> read_errorator::future<omap_values_t> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else {
        assert(r == 0);
        return read_errorator::make_ready_future<omap_values_t>(
	  std::move(values));
      }
    });
  });
}

auto AlienStore::omap_get_values(CollectionRef ch,
                                 const ghobject_t &oid,
                                 const std::optional<string> &start)
  -> read_errorator::future<std::tuple<bool, omap_values_t>>
{
  logger().debug("{} with_start", __func__);
  assert(tp);
  return do_with_op_gate(omap_values_t{}, [=, this] (auto &values) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, this, &values] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->omap_get_values(c->collection, oid, start,
		                    reinterpret_cast<map<string, bufferlist>*>(&values));
    }).then([&values] (int r)
      -> read_errorator::future<std::tuple<bool, omap_values_t>> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else if (r < 0){
        logger().error("omap_get_values(start): {}", r);
        return crimson::ct_error::input_output_error::make();
      } else {
        return read_errorator::make_ready_future<std::tuple<bool, omap_values_t>>(
          true, std::move(values));
      }
    });
  });
}

seastar::future<> AlienStore::do_transaction_no_callbacks(
  CollectionRef ch,
  ceph::os::Transaction&& txn)
{
  logger().debug("{}", __func__);
  auto id = seastar::this_shard_id();
  auto done = seastar::promise<>();
  return do_with_op_gate(
    std::move(txn),
    std::move(done),
    [this, ch, id] (auto &txn, auto &done) {
	AlienCollection* alien_coll = static_cast<AlienCollection*>(ch.get());
        // moving the `ch` is crucial for buildability on newer S* versions.
	return alien_coll->with_lock([this, ch=std::move(ch), id, &txn, &done] {
	  assert(tp);
	  return tp->submit(ch->get_cid().hash_to_shard(tp->size()),
	    [this, ch, id, &txn, &done, &alien=seastar::engine().alien()] {
	    txn.register_on_commit(new OnCommit(id, done, alien, txn));
	    auto c = static_cast<AlienCollection*>(ch.get());
	    return store->queue_transaction(c->collection, std::move(txn));
	  });
	}).then([&done] (int r) {
	  assert(r == 0);
	  return done.get_future();
	});
    });
}

seastar::future<> AlienStore::inject_data_error(const ghobject_t& o)
{
  logger().debug("{}", __func__);
  assert(tp);
  return seastar::with_gate(op_gate, [=, this] {
    return tp->submit([o, this] {
      return store->inject_data_error(o);
    });
  });
}

seastar::future<> AlienStore::inject_mdata_error(const ghobject_t& o)
{
  logger().debug("{}", __func__);
  assert(tp);
  return seastar::with_gate(op_gate, [=, this] {
    return tp->submit([=, this] {
      return store->inject_mdata_error(o);
    });
  });
}

seastar::future<> AlienStore::write_meta(const std::string& key,
                                         const std::string& value)
{
  logger().debug("{}", __func__);
  assert(tp);
  return seastar::with_gate(op_gate, [=, this] {
    return tp->submit([=, this] {
      return store->write_meta(key, value);
    }).then([] (int r) {
      assert(r == 0);
      return seastar::make_ready_future<>();
    });
  });
}

seastar::future<std::tuple<int, std::string>>
AlienStore::read_meta(const std::string& key)
{
  logger().debug("{}", __func__);
  assert(tp);
  return seastar::with_gate(op_gate, [this, key] {
    return tp->submit([this, key] {
      std::string value;
      int r = store->read_meta(key, &value);
      if (r > 0) {
        value.resize(r);
        boost::algorithm::trim_right_if(value,
          [] (unsigned char c) {return isspace(c);});
      } else {
        value.clear();
      }
      return std::make_pair(r, value);
    }).then([] (auto entry) {
      return seastar::make_ready_future<std::tuple<int, std::string>>(
        std::move(entry));
    });
  });
}

uuid_d AlienStore::get_fsid() const
{
  logger().debug("{}", __func__);
  return store->get_fsid();
}

seastar::future<store_statfs_t> AlienStore::stat() const
{
  logger().info("{}", __func__);
  assert(tp);
  return do_with_op_gate(store_statfs_t{}, [this] (store_statfs_t &st) {
    return tp->submit([this, &st] {
      return store->statfs(&st, nullptr);
    }).then([&st] (int r) {
      assert(r == 0);
      return seastar::make_ready_future<store_statfs_t>(std::move(st));
    });
  });
}

seastar::future<store_statfs_t> AlienStore::pool_statfs(int64_t pool_id) const
{
  logger().info("{}", __func__);
  assert(tp);
  return do_with_op_gate(store_statfs_t{}, [this, pool_id] (store_statfs_t &st) {
    return tp->submit([this, pool_id, &st]{
      bool per_pool_omap_stats = false;
      return store->pool_statfs(pool_id, &st, &per_pool_omap_stats);
    }).then([&st] (int r) {
      assert(r==0);
      return seastar::make_ready_future<store_statfs_t>(std::move(st));
    });
  });
}

unsigned AlienStore::get_max_attr_name_length() const
{
  logger().info("{}", __func__);
  return 256;
}

seastar::future<struct stat> AlienStore::stat(
  CollectionRef ch,
  const ghobject_t& oid)
{
  assert(tp);
  return do_with_op_gate((struct stat){}, [this, ch, oid](auto& st) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [this, ch, oid, &st] {
      auto c = static_cast<AlienCollection*>(ch.get());
      store->stat(c->collection, oid, &st);
      return st;
    });
  });
}

auto AlienStore::omap_get_header(CollectionRef ch,
                                 const ghobject_t& oid)
  -> get_attr_errorator::future<ceph::bufferlist>
{
  assert(tp);
  return do_with_op_gate(ceph::bufferlist(), [=, this](auto& bl) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, this, &bl] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->omap_get_header(c->collection, oid, &bl);
    }).then([&bl](int r) -> get_attr_errorator::future<ceph::bufferlist> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else if (r < 0) {
        logger().error("omap_get_header: {}", r);
        ceph_assert(0 == "impossible");
      } else {
        return get_attr_errorator::make_ready_future<ceph::bufferlist>(
	  std::move(bl));
      }
    });
  });
}

AlienStore::read_errorator::future<std::map<uint64_t, uint64_t>> AlienStore::fiemap(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t off,
  uint64_t len)
{
  assert(tp);
  return do_with_op_gate(std::map<uint64_t, uint64_t>(), [=, this](auto& destmap) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, this, &destmap] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->fiemap(c->collection, oid, off, len, destmap);
    }).then([&destmap](int r)
      -> read_errorator::future<std::map<uint64_t, uint64_t>> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else {
        return read_errorator::make_ready_future<std::map<uint64_t, uint64_t>>(
          std::move(destmap));
      }
    });
  });
}

}
