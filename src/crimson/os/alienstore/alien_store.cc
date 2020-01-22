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
#include "os/bluestore/BlueStore.h"
#include "os/ObjectStore.h"
#include "os/Transaction.h"

#include "crimson/common/log.h"
#include "crimson/os/futurized_store.h"

namespace {
  seastar::logger& logger()
  {
    return crimson::get_logger(ceph_subsys_filestore);
  }

class OnCommit final: public Context
{
  int cpuid;
  Context* on_commit = nullptr;
public:
  seastar::promise<> alien_done;
  OnCommit(int id, ceph::os::Transaction& txn): cpuid(id) {
    if (txn.has_contexts()) {
      on_commit = txn.get_on_commit();
    }
  }

  void finish(int) final {
    auto fut = seastar::alien::submit_to(cpuid, [this] {
      if (on_commit) {
        on_commit->complete(0);
      }
      alien_done.set_value();
      return seastar::make_ready_future<>();
    });
    fut.wait();
  }
};
}

namespace crimson::os {

AlienStore::AlienStore(const std::string& path, const ConfigValues& values)
  : path{path}
{
  cct = std::make_unique<CephContext>(CEPH_ENTITY_TYPE_OSD);
  g_ceph_context = cct.get();
  cct->_conf.set_config_values(values);
  store = std::make_unique<BlueStore>(cct.get(), path);
  tp = std::make_unique<crimson::thread::ThreadPool>(1, 128, seastar::engine().cpu_id() + 10);
}

seastar::future<> AlienStore::start()
{
  return tp->start();
}

seastar::future<> AlienStore::stop()
{
  return tp->submit([this] {
    for (auto [cid, ch]: coll_map)
      static_cast<AlienCollection*>(ch.get())->collection.reset();
    store.reset();
  }).then([this] {
    return tp->stop();
  });
}

AlienStore::~AlienStore() = default;

seastar::future<> AlienStore::mount()
{
  logger().debug("{}", __func__);
  return tp->submit([this] {
    return store->mount();
  }).then([] (int) {
    return seastar::now();
  });
}

seastar::future<> AlienStore::umount()
{
  logger().debug("{}", __func__);
  return transaction_gate.close().then([this] {
    return tp->submit([this] {
      return store->umount();
    });
  }).then([] (int) {
    return seastar::now();
  });
}

seastar::future<> AlienStore::mkfs(uuid_d new_osd_fsid)
{
  logger().debug("{}", __func__);
  osd_fsid = new_osd_fsid;
  return tp->submit([this] {
    return store->mkfs();
  }).then([] (int) {
    return seastar::now();
  });
}

seastar::future<std::vector<ghobject_t>, ghobject_t>
AlienStore::list_objects(CollectionRef ch,
                        const ghobject_t& start,
                        const ghobject_t& end,
                        uint64_t limit) const
{
  logger().debug("{}", __func__);
  return seastar::do_with(std::vector<ghobject_t>(), ghobject_t(),
                          [=] (auto &objects, auto &next) {
    objects.reserve(limit);
    return tp->submit([=, &objects, &next] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->collection_list(c->collection, start, end,
                                    store->get_ideal_list_max(),
                                    &objects, &next);
    }).then([&objects, &next] (int) {
      return seastar::make_ready_future<std::vector<ghobject_t>, ghobject_t>(
                                         std::move(objects), std::move(next));
    });
  });
}

seastar::future<CollectionRef> AlienStore::create_new_collection(const coll_t& cid)
{
  logger().debug("{}", __func__);
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
    return tp->submit([this, cid] {
    return store->open_collection(cid);
  }).then([this] (ObjectStore::CollectionHandle c) {
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

  return seastar::do_with(std::vector<coll_t>{}, [=] (auto &ls) {
    return tp->submit([this, &ls] {
      return store->list_collections(ls);
    }).then([&ls] (int) {
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
  return seastar::do_with(ceph::bufferlist{}, [=] (auto &bl) {
    return tp->submit([=, &bl] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->read(c->collection, oid, offset, len, bl, op_flags);
    }).then([&bl] (int r) -> read_errorator::future<ceph::bufferlist> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else if (r == -EIO) {
        return crimson::ct_error::input_output_error::make();
      } else {
        return read_errorator::make_ready_future<ceph::bufferlist>(std::move(bl));
      }
    });
  });
}

AlienStore::get_attr_errorator::future<ceph::bufferptr>
AlienStore::get_attr(CollectionRef ch,
                     const ghobject_t& oid,
                     std::string_view name) const
{
  logger().debug("{}", __func__);
  return seastar::do_with(ceph::bufferptr{}, [=] (auto &value) {
    return tp->submit([=, &value] {
      auto c =static_cast<AlienCollection*>(ch.get());
      return store->getattr(c->collection, oid,
		            static_cast<std::string>(name).c_str(), value);
    }).then([oid, name, &value] (int r) -> get_attr_errorator::future<ceph::bufferptr> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();
      } else if (r == -ENODATA) {
        return crimson::ct_error::enodata::make();
      } else {
        return get_attr_errorator::make_ready_future<ceph::bufferptr>(
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
  return seastar::do_with(attrs_t{}, [=] (auto &aset) {
    return tp->submit([=, &aset] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->getattrs(c->collection, oid,
		             reinterpret_cast<map<string,bufferptr>&>(aset));
    }).then([&aset] (int r) -> get_attrs_ertr::future<attrs_t> {
      if (r == -ENOENT) {
        return crimson::ct_error::enoent::make();;
      } else {
        return get_attrs_ertr::make_ready_future<attrs_t>(std::move(aset));
      }
    });
  });
}

seastar::future<AlienStore::omap_values_t>
AlienStore::omap_get_values(CollectionRef ch,
                       const ghobject_t& oid,
                       const set<string>& keys)
{
  logger().debug("{}", __func__);
  return seastar::do_with(omap_values_t{}, [=] (auto &values) {
    return tp->submit([=, &values] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->omap_get_values(c->collection, oid, keys,
		                    reinterpret_cast<map<string, bufferlist>*>(&values));
    }).then([&values] (int) {
      return seastar::make_ready_future<omap_values_t>(std::move(values));
    });
  });
}

seastar::future<bool, AlienStore::omap_values_t>
AlienStore::omap_get_values(CollectionRef ch,
                            const ghobject_t &oid,
                            const std::optional<string> &start)
{
  logger().debug("{} with_start", __func__);
  return seastar::do_with(omap_values_t{}, [=] (auto &values) {
    return tp->submit([=, &values] {
      auto c = static_cast<AlienCollection*>(ch.get());
      return store->omap_get_values(c->collection, oid, start,
		                    reinterpret_cast<map<string, bufferlist>*>(&values));
    }).then([&values] (int r) {
      return seastar::make_ready_future<bool, omap_values_t>(true, std::move(values));
    });
  });
}

seastar::future<> AlienStore::do_transaction(CollectionRef ch,
                                             ceph::os::Transaction&& txn)
{
  logger().debug("{}", __func__);
  auto callback =
    std::make_unique<OnCommit>(seastar::engine().cpu_id(), txn);
  return seastar::do_with(std::move(txn), std::move(callback),
      [this, ch] (ceph::os::Transaction &txn, auto &callback) {
    return seastar::with_gate(transaction_gate, [this, ch, &txn, &callback] {
      return tp_mutex.lock().then ([this, ch, &txn, &callback] {
        return tp->submit([=, &txn, &callback] {
          txn.register_on_commit(callback.get());
          auto c = static_cast<AlienCollection*>(ch.get());
          return store->queue_transaction(c->collection, std::move(txn));
        });
      }).then([this, &callback] (int) {
	tp_mutex.unlock();
        return callback->alien_done.get_future();
      });
    });
  });
}

seastar::future<> AlienStore::write_meta(const std::string& key,
                                         const std::string& value)
{
  logger().debug("{}", __func__);
  return tp->submit([=] {
    return store->write_meta(key, value);
  }).then([] (int) {
    return seastar::make_ready_future<>();
  });
}

seastar::future<int, std::string> AlienStore::read_meta(const std::string& key)
{
  logger().debug("{}", __func__);
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
    return seastar::make_ready_future<int, std::string>(entry.first, entry.second);
  });
}

uuid_d AlienStore::get_fsid() const
{
  logger().debug("{}", __func__);
  return osd_fsid;
}

seastar::future<store_statfs_t> AlienStore::stat() const
{
  logger().info("{}", __func__);
  return seastar::do_with(store_statfs_t{}, [this] (store_statfs_t &st) {
    return tp->submit([this, &st] {
      return store->statfs(&st, nullptr);
    }).then([&st] (int) {
      return seastar::make_ready_future<store_statfs_t>(std::move(st));
    });
  });
}

unsigned AlienStore::get_max_attr_name_length() const
{
  logger().info("{}", __func__);
  return 256;
}

}
