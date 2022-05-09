// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "xstore-crimson.h"

#include "common/ceph_argparse.h"
#include "messages/MPing.h"
#include "messages/MCommand.h"
#include "crimson/auth/DummyAuth.h"
#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Messenger.h"

#include <map>
#include <random>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/core/alien.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/thread.hh>
#include <os/ObjectStore.h>
#include "crimson/os/futurized_collection.h"
#include <crimson/os/futurized_store.h>
#include <crimson/os/alienstore/semaphore.h>

using namespace std::chrono_literals;
namespace bpo = boost::program_options;
using crimson::common::local_conf;

using crimson::os::FuturizedStore;

namespace ceph::common {
class CephContext;
}

class XStore : public ObjectStore
{
  struct FinalizeThread : public Thread {
    XStore *store;
    static constexpr unsigned QUEUE_SIZE = 128;
    crimson::counting_semaphore<QUEUE_SIZE> sem{0};
    boost::lockfree::queue<Context*> pending{QUEUE_SIZE};

    explicit FinalizeThread(XStore *s) : store(s) {}
    void *entry() override {
      // When semaphore is signalled but no element means stop processing.
      bool popped = true;
      do {
	sem.acquire();
	Context* c = nullptr;
	popped = pending.pop(c);
	if (popped) {
	  c->complete(0);
	}
      } while (popped);
      return nullptr;
    }
    void stop_signal() {
      sem.release();
    }
    void stop_wait() {
      join();
    }
  };

  class OnCommit final: public Context
  {
    XStore* store;
    Context *oncommit;
  public:
    OnCommit(
      XStore* store,
      Context *oncommit)
      : store(store)
      , oncommit(oncommit) {
    }
    void finish(int) override {
      ceph_assert(oncommit);
      store->finalize_thread.pending.push(oncommit);
      store->finalize_thread.sem.release();
    }
  };

  struct ReactorThread : public Thread {
    XStore *xstore;
    std::mutex lock;              // this is compiled with seastar, ceph::mutex
    std::condition_variable cond; // and ceph::cond are dummies
    seastar::semaphore stop;
    explicit ReactorThread(XStore *s)
      : xstore(s)
      , stop(0) {}
    void *entry() override {
      std::vector<char*> xargs;
      xargs.push_back(strdup("/me"));
      xargs.push_back(strdup("-c"));
      xargs.push_back(strdup("2"));
      std::string cluster_name{"ceph"};
      std::string conf_file_list;
      auto ceph_args = argv_to_vec(xargs.size(), xargs.data());
      auto init_params =
	ceph_argparse_early_args(args, CEPH_ENTITY_TYPE_CLIENT,
				 &cluster_name, &conf_file_list);
      using crimson::common::sharded_conf;
      xstore->app.add_options()
	("verbose,v", bpo::value<bool>()->default_value(false),
	 "chatty if true");
      xstore->app.run(xargs.size(), xargs.data(), [&] {
	return seastar::async([&] {
	  sharded_conf().start(init_params.name, cluster_name).get();
	  local_conf().parse_config_files(conf_file_list).get();
	  local_conf().parse_env().get();
	  local_conf().parse_argv(ceph_args).get();
	  return;
	}).then([&] {
	  {
	    // signal started
	    std::unique_lock l{lock};
	    cond.notify_all();
	  }
	  return stop.wait(1).then([] {
	    return sharded_conf().stop();
	  });
	});
      });
      for (auto x : xargs) {
	free(x);
      }
      return nullptr;
    }

    bool wait_start() {
      std::unique_lock l{lock};
      cond.wait(l);
      return true;
    }

    void stop_signal() {
      using crimson::common::sharded_conf;
      seastar::alien::submit_to(xstore->app.alien(), 0, [this] {
	stop.signal(1);
	return seastar::make_ready_future<>();
      }).wait();
    }
    void stop_wait() {
      join();
    }
  };

  seastar::app_template app;
  FinalizeThread finalize_thread;
  ReactorThread reactor;
  std::unique_ptr<FuturizedStore> fstore;
  std::string type; //FuturizedStore does not have type, save it for us
  uuid_d osd_fsid;
public:
  static inline std::vector<const char*> args;

  XStore(ceph::common::CephContext *cct,
	 const std::string& type,
	 const std::string& path)
    : ObjectStore(cct, path)
    , finalize_thread(this)
    , reactor(this)
    , type(type) {
    finalize_thread.create("xstore-fin");
    reactor.create("xstore-reactor");
    reactor.wait_start();
  }

  // Actual creation moved away from constructor to give
  // FuturizedStore::create a chance to fail.
  bool init(const ConfigValues& cv) {
    seastar::alien::submit_to(app.alien(), 0, [&] {
      return crimson::os::FuturizedStore::create(type, path, cv).
	then([&] (auto&& fstore) {
	  this->fstore = std::move(fstore);
	  return seastar::make_ready_future<>();
	});
    }).wait();
    return bool(fstore);
  }

  ~XStore() {
    finalize_thread.stop_signal();
    reactor.stop_signal();
    finalize_thread.stop_wait();
    reactor.stop_wait();
  }


  objectstore_perf_stat_t get_cur_stats() override {
    ceph_assert(0 && "not implemented");
    return objectstore_perf_stat_t{};
  }

  const PerfCounters* get_perf_counters() const override {
    ceph_assert(0 && "not implemented");
    return nullptr;
  }

  std::string get_type() override {
    //todo improve
    return "xstore";
  }

  bool test_mount_in_use() override {
    return true;
  }

  int mount() override {
    seastar::alien::submit_to(app.alien(), 0, [this]
    {
      return fstore->start().then([this] {
	return fstore->mount().handle_error(
	  crimson::stateful_ec::handle([] (const auto& ec) {
	    std::exit(EXIT_FAILURE);
	  }));
      });
    }).wait();
    return 0;
  }

  int umount() override {
    seastar::alien::submit_to(app.alien(), 0, [this] {
      return fstore->umount().then([this] {
	return fstore->stop();
      });
    }).wait();
    return 0;
  }

  int validate_hobject_key(const hobject_t &obj) const override {
    ceph_assert(0 && "not implemented");
    return 0;
  }

  unsigned get_max_attr_name_length() override {
    ceph_assert(0 && "not implemented");
    return 256;
  }

  int mkfs() override {
    seastar::alien::submit_to(app.alien(), 0, [this] {
      return fstore->start().then([this] {
	return fstore->mkfs(osd_fsid).handle_error(
	  crimson::stateful_ec::handle([] (const auto& ec) {
	    std::exit(EXIT_FAILURE);
	  })).then([this] {
	    return fstore->stop();
	  });
      });
    }).wait();
    return 0;
  }

  int mkjournal() override {
    return 0;
  }

  bool needs_journal() override {
    return false;
  }

  bool wants_journal() override {
    return false;
  }

  bool allows_journal() override {
    return false;
  }

  int statfs(struct store_statfs_t *buf,
	     osd_alert_list_t* alerts) override {
    ceph_assert(0 && "not implemented");
    return 0;
  }

  int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf,
		  bool *per_pool_omap) override {
    ceph_assert(0 && "not implemented");
    return 0;
  }

  struct Collection : public ObjectStore::CollectionImpl {
    XStore* xstore;
    crimson::os::FuturizedStore::CollectionRef col;
    Collection(XStore* xstore,
	       const coll_t &cid,
	       crimson::os::FuturizedStore::CollectionRef col)
      :CollectionImpl(xstore->cct, cid)
      ,xstore(xstore)
      ,col(col) {
    }
    ~Collection() override {
    }
    void flush() override {
      seastar::alien::submit_to(xstore->app.alien(), 0, [this] {
	  return xstore->fstore->flush(col);
	}).wait();
    }
    bool flush_commit(Context *c) override {
      seastar::alien::submit_to(xstore->app.alien(), 0, [this] {
	  return xstore->fstore->flush(col);
	}).wait();
      return true;
    }
  };

  inline const crimson::os::FuturizedStore::CollectionRef get_coll(const CollectionHandle& h) const {
    Collection* c = static_cast<Collection*>(h.get());
    return c->col;
  }

  ObjectStore::CollectionHandle open_collection(const coll_t &cid) override {
    auto fcoll = seastar::alien::submit_to(app.alien(), 0, [this, cid]
    {
      return fstore->open_collection(cid);
    });
    fcoll.wait();
    auto coll = fcoll.get();
    if (coll) {
      return ceph::make_ref<Collection>(this, cid, coll);
    } else {
      return CollectionHandle{};
    }
  }

  ObjectStore::CollectionHandle create_new_collection(const coll_t &cid) override {
    auto fcoll = seastar::alien::submit_to(app.alien(), 0, [this, cid]
    {
      return fstore->create_new_collection(cid);
    });
    fcoll.wait();
    return ceph::make_ref<Collection>(this, cid, fcoll.get());
  }

  int queue_transactions(CollectionHandle& c, std::vector<Transaction>& tls,
			 TrackedOpRef op, ThreadPool::TPHandle *handle) override {
    auto res = seastar::alien::submit_to(app.alien(), 0, [this, &tls, &c] {
      return seastar::do_with(tls.begin(), [this, &tls, &c] (auto&& p) {
	return seastar::do_until(
	  [&tls, &p] { return p == tls.end(); },
	  [this, &p, &c ] {
	    auto& txn = *p;
	    p++;
	    Context *crimson_wrapper = ceph::os::Transaction::collect_all_contexts(txn);
	    if (crimson_wrapper)
	      txn.register_on_commit(new OnCommit(this, crimson_wrapper));
	    return fstore->do_transaction(get_coll(c), std::move(txn));
	  });
      });
    });
    res.wait();
    return 0;
  }

  void set_collection_commit_queue(const coll_t &cid, ContextQueue *commit_queue) override {
    ceph_assert(0 && "not implemented");
  }

  bool exists(CollectionHandle& c, const ghobject_t& oid) override {
    // FuturizeStore does not have exists call.
    // We use get_attr instead.
    auto res = seastar::alien::submit_to(app.alien(), 0, [this, c, &oid] {
      return fstore->get_attr(get_coll(c), oid, "=====").
	safe_then([] (ceph::buffer::list&& attr_val) {
	  // strangely, object does have '=====' attribute
	  return seastar::make_ready_future<bool>(true);
	}).
	handle_error(
	  crimson::ct_error::enoent::handle([] {
	    // ENOENT means that object does not exist
	    return seastar::make_ready_future<bool>(false);
	  }),
	  crimson::ct_error::enodata::handle([] {
	    // ENODATA means that object existed only did not have attribute
	    // It is expected result, we select attribute so it most likely did not exist
	    return seastar::make_ready_future<bool>(true);
	  })
	);
    });
    res.wait();
    return res.get();
  }

  int set_collection_opts(
    CollectionHandle& c, const pool_opts_t& opts) override {
    ceph_assert(0 && "not implemented");
    return 0;
  }

  int stat(CollectionHandle &c, const ghobject_t& oid,
	   struct stat *st, bool allow_eio) override {
    auto res = seastar::alien::submit_to(app.alien(), 0, [this, c, &oid] {
      return fstore->stat(get_coll(c), oid);
    });
    res.wait();
    *st = res.get();
    // TODO: either modify this function to check for oid existence
    // or modify FuturizedStore::stat call
    return 0;
  }

  int read(CollectionHandle &c, const ghobject_t& oid,
	   uint64_t offset, size_t len,
	   ceph::buffer::list& result_bl, uint32_t op_flags) override {
    auto res = seastar::alien::submit_to(app.alien(), 0,
      [=, &result_bl, &oid] () -> seastar::future<int> {
	return fstore->read(get_coll(c), oid, offset, len, op_flags).
	  safe_then([&result_bl] (ceph::buffer::list&& b) {
	  result_bl = std::move(b);
	  return seastar::make_ready_future<int>(result_bl.length());
	}).
	handle_error(
	  crimson::ct_error::input_output_error::handle([] {
	    return seastar::make_ready_future<int>(-EIO);
	  }),
	  crimson::ct_error::enoent::handle([] {
	    return seastar::make_ready_future<int>(-ENOENT);
	  })
	);
      });
    res.wait();
    return res.get();
  }

  int fiemap(CollectionHandle& c, const ghobject_t& oid,
	     uint64_t offset, size_t len, ceph::buffer::list& bl) override {
    std::map<uint64_t, uint64_t> destmap;
    int r = fiemap(c, oid, offset, len, destmap);
    encode(destmap, bl);
    return r;
  }

  int fiemap(CollectionHandle& c, const ghobject_t& oid,
	     uint64_t offset, size_t len, std::map<uint64_t, uint64_t>& destmap) override {
  using crimson::ct_error::input_output_error;
    auto res = seastar::alien::submit_to(app.alien(), 0, [=, &oid, &destmap] {
      return fstore->fiemap(get_coll(c), oid, offset, len).
	safe_then([&destmap] (std::map<uint64_t, uint64_t>&& fmap) {
	  destmap = std::move(fmap);
	  return seastar::make_ready_future<int>(0);
	}).
	handle_error(
	  input_output_error::handle([] {
	    return seastar::make_ready_future<int>(-EIO);
	  }),
	  crimson::ct_error::enoent::handle([] {
	    return seastar::make_ready_future<int>(-ENOENT);
	  })
	);
    });
    res.wait();
    return res.get();
  }

  int readv(CollectionHandle &c, const ghobject_t& oid,
	    interval_set<uint64_t>& m, ceph::buffer::list& bl,
	    uint32_t op_flags) override {
    ceph_assert(0 && "not implemented");
    return 0;
  }

  int dump_onode(CollectionHandle &c, const ghobject_t& oid,
		 const std::string& section_name, ceph::Formatter *f) override {
    ceph_assert(0 && "not implemented");
    return 0;
  }

  int getattr(CollectionHandle &c, const ghobject_t& oid,
	      const char *name, ceph::buffer::ptr& value) override {
    auto res = seastar::alien::submit_to(app.alien(), 0, [=, &oid, &value] {
      return fstore->get_attr(get_coll(c), oid, name).
	safe_then([&value] (ceph::bufferlist&& v) {
	  // convert buffer::list to buffer::ptr
	  // TODO maybe just extract first buffer::ptr from the buffer::list ?
	  ceph::buffer::ptr value1(v.c_str(), v.length());
	  value = std::move(value1);
	  return seastar::make_ready_future<int>(0);
	}).
	handle_error(
	  crimson::ct_error::enodata::handle([] {
	    return seastar::make_ready_future<int>(-ENODATA);
	  }),
	  crimson::ct_error::enoent::handle([] {
	    return seastar::make_ready_future<int>(-ENOENT);
	  })
	);
    });
    res.wait();
    return res.get();
  }

  int getattrs(CollectionHandle &c, const ghobject_t& oid,
	       std::map<std::string,ceph::buffer::ptr, std::less<>>& aset) override {
    auto res = seastar::alien::submit_to(app.alien(), 0, [=, &oid, &aset] {
      return fstore->get_attrs(get_coll(c), oid).
	safe_then([&aset] (std::map<std::string, ceph::bufferlist, std::less<>>&& fset) {
	  aset.clear();
	  auto hint = aset.begin();
	  for (auto& [n, v] : fset) {
	    hint = aset.emplace_hint(hint, n, ceph::buffer::ptr(v.c_str(), v.length()));
	  }
	  return seastar::make_ready_future<int>(0);
	}).
	handle_error(
	  crimson::ct_error::enoent::handle([] {
	    return seastar::make_ready_future<int>(-ENOENT);
	  })
	);
    });
    res.wait();
    return res.get();
  }

  int list_collections(std::vector<coll_t>& ls) override {
    ceph_assert(0 && "not implemented");
    return 0;
  }

  bool collection_exists(const coll_t& cid) override {
    auto res = seastar::alien::submit_to(app.alien(), 0, [this, cid]
    {
      return fstore->open_collection(cid);
    });
    res.wait();
    return bool(res.get());
  }

  int collection_empty(CollectionHandle& c, bool *empty) override {
    ghobject_t next;
    std::vector<ghobject_t> ls;
    int r = collection_list(c, ghobject_t(), ghobject_t::get_max(), 1, &ls, &next);
    *empty = ls.size() == 0;
    return r;
  }

  int collection_bits(CollectionHandle& c) override {
    ceph_assert(0 && "not implemented");
    return 0;
  }

  int collection_list(CollectionHandle &c, const ghobject_t& start,
		      const ghobject_t& end, int max,
		      std::vector<ghobject_t> *ls, ghobject_t *next) override {
    auto res = seastar::alien::submit_to(app.alien(), 0, [=, &start, &end] {
      return fstore->list_objects(get_coll(c), start, end, max).
	then([ls, next] (std::tuple<std::vector<ghobject_t>, ghobject_t>&& list_next) {
	  *ls = std::get<0>(list_next);
	  if (next) {
	    *next = std::get<1>(list_next);
	  }
	  return seastar::make_ready_future<>();
	});
    });
    res.wait();
    return 0;
  }

  int omap_get(CollectionHandle &c, const ghobject_t &oid, ceph::buffer::list *header,
	       std::map<std::string, ceph::buffer::list> *k_v) override {
    auto res = seastar::alien::submit_to(app.alien(), 0, [=, &oid] {
      std::optional<std::string> start; //uninitialized, list from begin
      std::function<FuturizedStore::read_errorator::future<>(const std::optional<std::string>&& start)> more_omaps;
      // Strangely, defining and setting of more_omaps must be in separate statements.
      // Assignment in definition "std::function<...> more_omaps = [] ...;" compiles, but gives faulty code.
      more_omaps = [=, &oid] (const std::optional<std::string>&& start) -> FuturizedStore::read_errorator::future<> {
	return fstore->omap_get_values(get_coll(c), oid, start).
	safe_then([k_v, more_omaps] (std::tuple<bool, FuturizedStore::omap_values_t>&& done_omaps) {
	  bool done = std::get<0>(done_omaps);
	  auto& kvs = std::get<1>(done_omaps);
	  auto hint = k_v->end();
	  for (auto i = kvs.begin(); i != kvs.end(); ++i) {
	    hint = k_v->emplace_hint(hint, i->first, i->second);
	  }
	  if (!done) {
	    return more_omaps(kvs.rbegin()->first);
	  }
	  return FuturizedStore::read_errorator::now();
	});
      };
      return more_omaps(std::optional<std::string>()). /*string not present - start from beginning*/
	safe_then([=, &oid] () {
	  return fstore->omap_get_header(get_coll(c), oid).
	    safe_then([header] (ceph::bufferlist&& v) {
	      *header = std::move(v);
	      return seastar::make_ready_future<int>(0);
	    });
	}).
	handle_error(
	  crimson::ct_error::input_output_error::handle([] {
	    return seastar::make_ready_future<int>(-EIO);
	  }),
	  crimson::ct_error::enodata::handle([] {
	    return seastar::make_ready_future<int>(-ENODATA);
	  }),
	  crimson::ct_error::enoent::handle([] {
	    return seastar::make_ready_future<int>(-ENOENT);
	  })
	);
    });
    res.wait();
    return res.get();
  }

  int omap_get_header(CollectionHandle &c, const ghobject_t &oid,
		      ceph::buffer::list *header, bool allow_eio) override {
    auto res = seastar::alien::submit_to(app.alien(), 0, [=, &oid] {
      return fstore->omap_get_header(get_coll(c), oid).
	safe_then([header] (ceph::bufferlist&& v) {
	  *header = v;
	  return seastar::make_ready_future<int>(0);
	}).
	handle_error(
	  crimson::ct_error::enodata::handle([] {
	    return seastar::make_ready_future<int>(-ENODATA);
	  }),
	  crimson::ct_error::enoent::handle([] {
	    return seastar::make_ready_future<int>(-ENOENT);
	  })
	);
    });
    res.wait();
    return res.get();
  }

  int omap_get_keys(CollectionHandle &c, const ghobject_t &oid,
		    std::set<std::string> *keys) override {
    keys->clear();
    auto res = seastar::alien::submit_to(app.alien(), 0, [=, &oid] {
      std::function<FuturizedStore::read_errorator::future<>(const std::optional<std::string>&& start)> more_omaps;
      more_omaps = [=, &oid] (const std::optional<std::string>&& start) -> FuturizedStore::read_errorator::future<> {
	return fstore->omap_get_values(get_coll(c), oid, start).
	safe_then([keys, more_omaps] (std::tuple<bool, FuturizedStore::omap_values_t>&& done_omaps) {
	  bool done = std::get<0>(done_omaps);
	  auto k_v = std::get<1>(done_omaps);
	  auto hint = keys->end();
	  for (auto& [k, v] : k_v) {
	    hint = keys->emplace_hint(hint, k);
	  }
	  if (!done) {
	    return more_omaps(k_v.rbegin()->first);
	  }
	  return FuturizedStore::read_errorator::now();
	});
      };

      return
	more_omaps(std::optional<std::string>()).
	safe_then([] {
	  return seastar::make_ready_future<int>(0);
	}).
	handle_error(
	  crimson::ct_error::input_output_error::handle([] {
	    return seastar::make_ready_future<int>(-EIO);
	  }),
	  crimson::ct_error::enoent::handle([] {
	    return seastar::make_ready_future<int>(-ENOENT);
	  })
	);
    });
    res.wait();
    return res.get();
  }

  int omap_get_values(CollectionHandle &c, const ghobject_t &oid,
		      const std::set<std::string> &keys,
		      std::map<std::string, ceph::buffer::list> *out) override {
    auto res = seastar::alien::submit_to(app.alien(), 0,
      [=, &oid, &keys]() -> seastar::future<int> {
	return fstore->omap_get_values(get_coll(c), oid, keys).
	  safe_then([=] (FuturizedStore::omap_values_t&& values) {
	    for (auto& i : values) {
	      out->emplace(i);
	    }
	    return seastar::make_ready_future<int>(0);
	  }).
	  handle_error(
	    crimson::ct_error::input_output_error::handle([] {
	      return seastar::make_ready_future<int>(-EIO);
	    }),
	    crimson::ct_error::enoent::handle([] {
	      return seastar::make_ready_future<int>(-ENOENT);
	    })
	  );
      });
    res.wait();
    return res.get();
  }

  int omap_get_values(CollectionHandle &c, const ghobject_t &oid,
		      const std::optional<std::string> &start_after,
		      std::map<std::string, ceph::buffer::list> *out) override {
    return -ENOTSUP;
  }

  int omap_check_keys(CollectionHandle &c, const ghobject_t &oid,
		      const std::set<std::string> &keys, std::set<std::string> *out) override {
    ceph_assert(0 && "not implemented");
    return 0;
  }

  class OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    crimson::os::FuturizedStore::OmapIteratorRef oit;
    XStore* xstore;
  public:
    OmapIteratorImpl(XStore* xstore,
		     crimson::os::FuturizedStore::CollectionRef col,
		     const ghobject_t &oid)
      :xstore(xstore)
    {
      seastar::alien::submit_to(xstore->app.alien(), 0, [&]
      {
	return xstore->fstore->get_omap_iterator(col, oid).
	  then([&] (auto oit) {
	    this->oit = std::move(oit);
	    return seastar::make_ready_future<>();
	  });
      }).wait();
    }
    ~OmapIteratorImpl() override {
    }
    int seek_to_first() override {
      auto res = seastar::alien::submit_to(xstore->app.alien(), 0, [this]
      {
	return oit->seek_to_first();
      });
      res.wait();
      return 0;
    }
    int upper_bound(const std::string &after) override {
      seastar::alien::submit_to(xstore->app.alien(), 0, [this, &after]
      {
	return oit->upper_bound(after);
      }).wait();
      return 0;
    }
    int lower_bound(const std::string &to) override {
      seastar::alien::submit_to(xstore->app.alien(), 0, [this, &to]
      {
	return oit->lower_bound(to);
      }).wait();
      return 0;
    }
    bool valid() override {
      return oit->valid();
    }
    int next() override {
      seastar::alien::submit_to(xstore->app.alien(), 0, [this]
      {
	return oit->next();
      }).wait();
      return 0;
    }
    std::string key() override {
      return oit->key();
    }
    ceph::buffer::list value() override {
      return oit->value();
    }
    std::string tail_key() override {
      ceph_assert(0 && "unable to calculate tail_key");
      return "";
    }
    int status() override {
      return oit->status();
    }
  };

  ObjectMap::ObjectMapIterator
  get_omap_iterator(CollectionHandle &c, const ghobject_t &oid) override {
    return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(this, get_coll(c), oid));
  }

  void set_fsid(uuid_d u) override {
    osd_fsid = u;
  }

  uuid_d get_fsid() override {
    return osd_fsid;
  }

  uint64_t estimate_objects_overhead(uint64_t num_objects) override {
    ceph_assert(0 && "not implemented");
    return 0;
  }
};

std::unique_ptr<ObjectStore> XStore__create(
  ceph::common::CephContext *cct,
  const ConfigValues& cv,
  const std::string& type,
  const std::string& data)
{
  XStore* xstore = new XStore(cct, type, data);
  if (!xstore->init(cv)) {
    delete xstore;
    xstore = nullptr;
  }
  return std::unique_ptr<ObjectStore>(xstore);
}

void XStore__save_args(int argc, char** argv) {
  for (int i = 0; i < argc; i++) {
    XStore::args.push_back(argv[i]);
  }
}

