// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

/**
 * crimson-store-bench
 *
 * This tool measures various IO patterns against the crimson FuturizedStore
 * interface.
 *
 * Usage should be:
 *
 * crimson-store-bench --store-path <path>
 *
 * where <path> is a directory containing a file block.  block should either
 * be a symlink to an actual block device or a file truncated to an appropriate
 * size if performance isn't relevant (testing or developement of this utility,
 * for instance).
 *
 * One might want to add something like the following to one's .bashrc to quickly
 * run this utility during development from build/:
 *
 * function run_store_bench {
 *   rm -rf store_bench_dir
 *   mkdir store_bench_dir
 *   touch store_bench_dir/block
 *   truncate -s 10G store_bench_dir/block
 *   ./bin/crimson-store-bench --store-path store_bench_dir $@
 * }
 */

#include <random>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <seastar/apps/lib/stop_signal.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include "crimson/common/coroutine.h"
#include "crimson/common/log.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

namespace po = boost::program_options;

using namespace ceph;

SET_SUBSYS(osd);

/**
 * example_io
 *
 * Performs some simple operations against store.
 * The FuturizedStore interface can be found at
 * crimson/os/futurized_store.h
 */
seastar::future<> example_io(crimson::os::FuturizedStore &global_store)
{
  LOG_PREFIX(example_io);
  /* crimson-osd's architecture partitions most resources per seastar
   * reactor.  This allows us to (mostly) avoid locking and other forms
   * of contention.  This call gets us the FuturizedStore::Shard
   * local to the reactor we are executing on. */
  auto &local_store = global_store.get_sharded_store();

  /* Objects in FuturizedStore instances are stored in collections.
   * crimson-osd (and ceph-osd) use one collection per pg, so
   * collections are designated by spg_t via coll_t
   * (see osd/osd_types.h).
   */
  coll_t cid(
    spg_t(
      // Map shard to pool for this test
      pg_t(seastar::this_shard_id(), 0)
    )
  );
  auto coll_ref = co_await local_store.create_new_collection(cid);

  /* Objects in FuturizedStore are named by ghobject_t --
   * see common/hobject.h.  Let's create a convenience function
   * to generate instances which differ by one paramater.
   *
   * Note that objects must have globally unique names(ghobject_t), even if
   * in different collections.
   */
  auto create_hobj = [](unsigned id) {
    return ghobject_t(
      shard_id_t::NO_SHARD,
      seastar::this_shard_id(),
      id, // hash, normally rjenkins of name, but let's just set it to id
      "", // namespace, empty here
      "", // name, empty here
      0,  // snapshot
      ghobject_t::NO_GEN);
  };

  /* ceph buffers are represented using bufferlist.
   * See include/buffer.h
   */
  auto make_bl = [](std::string_view sv) {
    bufferlist bl;
    bl.append(bufferptr(sv.data(), sv.size()));
    return bl;
  };
  auto bl_to_str = [](bufferlist bl) {
    return std::string(bl.c_str(), bl.length());
  };

  /* Writes to FuturizedStore instances are performed via
   * the ceph::os::Transaction class.  All operations within
   * the transaction will be applied atomically -- after a
   * failure either the whole transaction will have happened
   * or none of it will.
   *
   * ceph::os::Transaction is shared with classic because it
   * is part of the primary->replica replication protocol.
   * See os/Transaction.h for details.
   */
  auto obj0 = create_hobj(0);
  std::string key("foo");
  std::string val("bar");
  {
    ceph::os::Transaction t;
    // create object
    t.create(cid, obj0);
    // set omap key "foo" to "bar"
    std::map<std::string, bufferlist> attrs;
    attrs[key] = make_bl(val);
    t.omap_setkeys(
      cid,
      obj0,
      attrs);
    // actually submit the transaction and await commit
    co_await local_store.do_transaction(
      coll_ref,
      std::move(t));
    ERROR("created object {} in collection {} with omap {}->{}",
          obj0, cid, key, val);
  }

  {
    std::set<std::string> keys;
    keys.insert(key);
    auto result = co_await local_store.omap_get_values(
      coll_ref,
      obj0,
      keys
    ).handle_error(
      crimson::ct_error::assert_all("error reading object")
    );
    auto iter = result.find(key);
    if (iter == result.end()) {
      ERROR("Failed to find key {} on obj {}", key, obj0);
    } else if (bl_to_str(iter->second) != val) {
      ERROR("key {} on obj {} does not match", key, obj0);
    } else {
      ERROR("read key {} on obj {}, matches", key, obj0);
    }
  }
}

int main(int argc, char** argv)
{
  LOG_PREFIX(main);
  po::options_description desc{"Allowed options"};
  bool debug = false;
  std::string store_type;
  std::string store_path;
  std::string io_pattern;

  desc.add_options()
    ("help,h", "show help message")
    ("store-type",
     po::value<std::string>(&store_type)->default_value("seastore"),
     "set store type")
    /* store-path is a path to a directory containing a file 'block'
     * block should be a symlink to a real device for actual performance
     * testing, but may be a file for testing this utility.
     * See build/dev/osd* after starting a vstart cluster for an example
     * of what that looks like.
     */
    ("store-path", po::value<std::string>(&store_path),
     "path to store, <store-path>/block should "
     "be a symlink to the target device for bluestore or seastore")
    ("debug", po::value<bool>(&debug)->default_value(false),
     "enable debugging");

  po::variables_map vm;
  std::vector<std::string> unrecognized_options;
  try {
    auto parsed = po::command_line_parser(argc, argv)
      .options(desc)
      .allow_unregistered()
      .run();
    po::store(parsed, vm);
    if (vm.count("help")) {
      std::cout << desc << std::endl;
      return 0;
    }

    po::notify(vm);
    unrecognized_options =
      po::collect_unrecognized(parsed.options, po::include_positional);
  } catch(const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  seastar::app_template::config app_cfg;
  app_cfg.name = "crimson-store-bench";
  app_cfg.auto_handle_sigint_sigterm = false;
  seastar::app_template app(std::move(app_cfg));

  std::vector<char*> av{argv[0]};
  std::transform(
    std::begin(unrecognized_options),
    std::end(unrecognized_options),
    std::back_inserter(av),
    [](auto& s) {
      return const_cast<char*>(s.c_str());
    });
  return app.run(
    av.size(), av.data(),
    /* crimson-osd uses seastar as its scheduler.  We use
     * sesastar::app_template::run to start the base task for the
     * application -- this lambda.  The -> seastar::future<int> here
     * explicitely states the return type of the lambda, a future
     * which resolves to an int.  We need to do this because the
     * co_return at the end is insufficient to express the type.
     *
     * The lambda internally uses co_await/co_return and is therefore
     * a coroutine.  co_await <future> suspends execution until <future>
     * resolves.  The whole co_await expression then evaluates to the
     * contents of the future -- int for seastar::future<int>.
     *
     * What's a bit confusing is that a coroutine generally *returns*
     * at the first suspension point yielding it's return type, a
     * seastar::future<int> in this case.  This is tricky for
     * lambda-coroutines because it means that the lambda could go out
     * of scope before the coroutine actually completes, resulting in
     * captured variables (references to everything in the parent frame
     * in this case -- [&]) being free'd.  Resuming the coroutine would
     * then hit a use-after-free as soon as it tries to access any
     * of those variables.  seastar::coroutine::lambda avoids this.
     * I suggest having a look at src/seastar/include/seastar/core/coroutine.hh
     * for the implementation.
     * Note, the language guarrantees that *arguments* (whether to
     * a lambda or not) have their lifetimes extended for the duration
     * of the coroutine, so this isn't a problem for non-lambda
     * coroutines.
     */
    seastar::coroutine::lambda([&]() -> seastar::future<int> {
    if (debug) {
      seastar::global_logger_registry().set_all_loggers_level(
        seastar::log_level::debug
      );
    } else {
      seastar::global_logger_registry().set_all_loggers_level(
        seastar::log_level::error
      );
    }

    co_await crimson::common::sharded_conf().start(
      EntityName{}, std::string_view{"ceph"});
    co_await crimson::common::local_conf().start();

    {
      std::vector<const char*> cav;
      std::transform(
        std::begin(unrecognized_options),
        std::end(unrecognized_options),
        std::back_inserter(cav),
        [](auto& s) {
          return s.c_str();
        });
      co_await crimson::common::local_conf().parse_argv(
        cav);
    }

    auto store = crimson::os::FuturizedStore::create(
      store_type,
      store_path,
      crimson::common::local_conf().get_config_values()
    );

    uuid_d uuid;
    uuid.generate_random();

    co_await store->start();
    /* FuturizedStore interfaces use errorated-futures rather than bare
     * seastar futures in order to encode possible errors in the type.
     * However, this utility doesn't really need to do anything clever
     * with a failure to execute mkfs other than tell the user what
     * happened, so we simply respond uniformly to all error cases
     * using the handle_error handler.  See FuturizedStore::mkfs for
     * the actual return type and crimson/common/errorator.h for the
     * implementation of errorators.
     */
    co_await store->mkfs(uuid).handle_error(
      crimson::stateful_ec::assert_failure(
        std::format(
          "error creating empty object store type {} in {}",
          store_type,
          store_path).c_str()));
    co_await store->stop();

    co_await store->start();
    co_await store->mount().handle_error(
      crimson::stateful_ec::assert_failure(
        std::format(
          "error mounting object store type {} in {}",
          store_type,
          store_path).c_str()));

    for (unsigned i = 0; i < seastar::smp::count; ++i) {
      co_await seastar::smp::submit_to(
        i,
        seastar::coroutine::lambda(
          [FNAME, &store_ref=*store]() -> seastar::future<> {
            ERROR("running example_io on reactor {}", seastar::this_shard_id());
            co_await example_io(store_ref);
          })
      );
    }

    co_await store->umount();
    co_await store->stop();
    co_await crimson::common::sharded_conf().stop();
    co_return 0;
  }));
}
