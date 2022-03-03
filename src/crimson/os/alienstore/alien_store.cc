// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "alien_collection.h"
#include "alien_store.h"

#include <map>
#include <string_view>
#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <seastar/core/alien.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>

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
  Context *oncommit;
  seastar::alien::instance &alien;
  seastar::promise<> &alien_done;
public:
  OnCommit(
    int id,
    seastar::promise<> &done,
    Context *oncommit,
    seastar::alien::instance &alien,
    ceph::os::Transaction& txn)
    : cpuid(id),
      oncommit(oncommit),
      alien(alien),
      alien_done(done) {
  }

  void finish(int) final {
    return seastar::alien::submit_to(alien, cpuid, [this] {
      if (oncommit) {
        oncommit->complete(0);
      }
      alien_done.set_value();
      return seastar::make_ready_future<>();
    }).wait();
  }
};
}

namespace crimson::os {

using crimson::common::get_conf;

AlienStore::AlienStore(const std::string& type,
                       const std::string& path,
                       const ConfigValues& values)
  : type(type),
    path{path}
{
  cct = std::make_unique<CephContext>(CEPH_ENTITY_TYPE_OSD);
  g_ceph_context = cct.get();
  cct->_conf.set_config_values(values);
}

seastar::future<> AlienStore::start()
{
  store = ObjectStore::create(cct.get(), type, path);
  if (!store) {
    ceph_abort_msgf("unsupported objectstore type: %s", type.c_str());
  }
  std::vector<uint64_t> cpu_cores = _parse_cpu_cores();
  // cores except the first "N_CORES_FOR_SEASTAR" ones will
  // be used for alien threads scheduling:
  // 	[0, N_CORES_FOR_SEASTAR) are reserved for seastar reactors
  // 	[N_CORES_FOR_SEASTAR, ..] are assigned to alien threads.
  if (cpu_cores.empty()) {
    if (long nr_cpus = sysconf(_SC_NPROCESSORS_ONLN);
	nr_cpus > N_CORES_FOR_SEASTAR ) {
      for (int i = N_CORES_FOR_SEASTAR; i < nr_cpus; i++) {
        cpu_cores.push_back(i);
      }
    } else {
      logger().error("{}: unable to get nproc: {}", __func__, errno);
    }
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
  }).then([this] {
    return tp->stop();
  });
}

AlienStore::~AlienStore() = default;

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
                         [=] (auto &objects, auto &next) {
    objects.reserve(limit);
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()),
      [=, &objects, &next] {
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

seastar::future<std::vector<coll_t>> AlienStore::list_collections()
{
  logger().debug("{}", __func__);
  assert(tp);

  return do_with_op_gate(std::vector<coll_t>{}, [=] (auto &ls) {
    return tp->submit([this, &ls] {
      return store->list_collections(ls);
    }).then([&ls] (int r) {
      assert(r == 0);
      return seastar::make_ready_future<std::vector<coll_t>>(std::move(ls));
    });
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
  return do_with_op_gate(ceph::bufferlist{}, [=] (auto &bl) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, &bl] {
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
                         [=] (auto &value, const auto& name) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, &value, &name] {
      // XXX: `name` isn't a `std::string_view` anymore! it had to be converted
      // to `std::string` for the sake of extending life-time not only of
      // a _ptr-to-data_ but _data_ as well. Otherwise we would run into a use-
      // after-free issue.
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->getattr(c->collection, oid, name.c_str(), value);
    }).then([oid, &value] (int r) -> get_attr_errorator::future<ceph::bufferlist> {
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
  return do_with_op_gate(attrs_t{}, [=] (auto &aset) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, &aset] {
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
  return do_with_op_gate(omap_values_t{}, [=] (auto &values) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, &values] {
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
  return do_with_op_gate(omap_values_t{}, [=] (auto &values) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, &values] {
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

seastar::future<> AlienStore::do_transaction(CollectionRef ch,
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
	  Context *crimson_wrapper =
	    ceph::os::Transaction::collect_all_contexts(txn);
	  assert(tp);
	  return tp->submit(ch->get_cid().hash_to_shard(tp->size()),
	    [this, ch, id, crimson_wrapper, &txn, &done, &alien=seastar::engine().alien()] {
	    txn.register_on_commit(new OnCommit(id, done, crimson_wrapper,
						alien, txn));
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
  return seastar::with_gate(op_gate, [=] {
    return tp->submit([=] {
      return store->inject_data_error(o);
    });
  });
}

seastar::future<> AlienStore::inject_mdata_error(const ghobject_t& o)
{
  logger().debug("{}", __func__);
  assert(tp);
  return seastar::with_gate(op_gate, [=] {
    return tp->submit([=] {
      return store->inject_mdata_error(o);
    });
  });
}

seastar::future<> AlienStore::write_meta(const std::string& key,
                                         const std::string& value)
{
  logger().debug("{}", __func__);
  assert(tp);
  return seastar::with_gate(op_gate, [=] {
    return tp->submit([=] {
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
  -> read_errorator::future<ceph::bufferlist>
{
  assert(tp);
  return do_with_op_gate(ceph::bufferlist(), [=](auto& bl) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, &bl] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->omap_get_header(c->collection, oid, &bl);
    }).then([&bl] (int r) -> read_errorator::future<ceph::bufferlist> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else if (r < 0) {
        logger().error("omap_get_header: {}", r);
        return crimson::ct_error::input_output_error::make();
      } else {
        return read_errorator::make_ready_future<ceph::bufferlist>(
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
  return do_with_op_gate(std::map<uint64_t, uint64_t>(), [=](auto& destmap) {
    return tp->submit(ch->get_cid().hash_to_shard(tp->size()), [=, &destmap] {
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

seastar::future<FuturizedStore::OmapIteratorRef> AlienStore::get_omap_iterator(
  CollectionRef ch,
  const ghobject_t& oid)
{
  assert(tp);
  return tp->submit(ch->get_cid().hash_to_shard(tp->size()),
    [this, ch, oid] {
    auto c = static_cast<AlienCollection*>(ch.get());
    auto iter = store->get_omap_iterator(c->collection, oid);
    return FuturizedStore::OmapIteratorRef(
	      new AlienStore::AlienOmapIterator(iter,
						this,
						ch));
  });
}

//TODO: each iterator op needs one submit, this is not efficient,
//      needs further optimization.
seastar::future<> AlienStore::AlienOmapIterator::seek_to_first()
{
  assert(store->tp);
  return store->tp->submit(ch->get_cid().hash_to_shard(store->tp->size()),
    [this] {
    return iter->seek_to_first();
  }).then([] (int r) {
    assert(r == 0);
    return seastar::now();
  });
}

seastar::future<> AlienStore::AlienOmapIterator::upper_bound(
  const std::string& after)
{
  assert(store->tp);
  return store->tp->submit(ch->get_cid().hash_to_shard(store->tp->size()),
    [this, after] {
    return iter->upper_bound(after);
  }).then([] (int r) {
    assert(r == 0);
    return seastar::now();
  });
}

seastar::future<> AlienStore::AlienOmapIterator::lower_bound(
  const std::string& to)
{
  assert(store->tp);
  return store->tp->submit(ch->get_cid().hash_to_shard(store->tp->size()),
    [this, to] {
    return iter->lower_bound(to);
  }).then([] (int r) {
    assert(r == 0);
    return seastar::now();
  });
}

seastar::future<> AlienStore::AlienOmapIterator::next()
{
  assert(store->tp);
  return store->tp->submit(ch->get_cid().hash_to_shard(store->tp->size()),
    [this] {
    return iter->next();
  }).then([] (int r) {
    assert(r == 0);
    return seastar::now();
  });
}

bool AlienStore::AlienOmapIterator::valid() const
{
  return iter->valid();
}

std::string AlienStore::AlienOmapIterator::key()
{
  return iter->key();
}

ceph::buffer::list AlienStore::AlienOmapIterator::value()
{
  return iter->value();
}

int AlienStore::AlienOmapIterator::status() const
{
  return iter->status();
}

std::vector<uint64_t> AlienStore::_parse_cpu_cores()
{
  std::vector<uint64_t> cpu_cores;
  auto cpu_string =
    get_conf<std::string>("crimson_alien_thread_cpu_cores");

  std::string token;
  std::istringstream token_stream(cpu_string);
  while (std::getline(token_stream, token, ',')) {
    std::istringstream cpu_stream(token);
    std::string cpu;
    std::getline(cpu_stream, cpu, '-');
    uint64_t start_cpu = std::stoull(cpu);
    std::getline(cpu_stream, cpu, '-');
    uint64_t end_cpu = std::stoull(cpu);
    for (uint64_t i = start_cpu; i < end_cpu; i++) {
      cpu_cores.push_back(i);
    }
  }
  return cpu_cores;
}

}
