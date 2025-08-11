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
 * One might want to add something like the following to one's .bashrc to
 * quickly run this utility during development from build/:
 * rm -rf store_bench_dir
   mkdir store_bench_dir
   touch store_bench_dir/block
   truncate -s 10G store_bench_dir/block
   ./build/bin/crimson-store-bench --store-path store_bench_dir --smp 4 "$@"
 */

#include <random>
#include <vector>

#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <seastar/apps/lib/stop_signal.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include "crimson/common/config_proxy.h"
#include "crimson/common/coroutine.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

namespace po = boost::program_options;

using namespace ceph;

/**
 * These are functions that are used in both types of work_loads
 */

/**
 * Creates ghobject_t object identifier from integer identifier.
 */
ghobject_t create_hobj(unsigned id) {
  return ghobject_t(
      shard_id_t::NO_SHARD, seastar::this_shard_id(),
      id, // hash, normally rjenkins of name, but let's just set it to id
      "", // namespace, empty here
      "", // name, empty here
      0,  // snapshot
      ghobject_t::NO_GEN);
};

/**
 * The function is called whenever we create an object
 * This function creates a collection identified by pg_id and shard_id
 * we calculate pg_id as integer division of obj_id/num_objects_per_collection
 * This ensures that the number of objects in each collection is essentially
 * dependent on objects_per_collection in the case of pg_workload we set this to
 * 1 , so every object will fall into a different pg in the case of rgw_workload
 * we set it to x so all objects with obj id be 0 and x-1 will fall into 1
 * collection
 */
coll_t make_cid(int obj_id, int num_objects_per_collection) {
  int pg_id = obj_id / num_objects_per_collection;
  return coll_t(spg_t(pg_t(pg_id, 0)));
}

/**
 * The struct stores the number of operations and the total latency for all
 * these operations For the pg workload type write+delete increases the number
 * of operations by 1 For the rgw index workload, each write increases the
 * num_operations by 1 Each delete increases the number of operations by 1
 */
struct results_t {
  int num_operations = 0;
  std::chrono::duration<double> tot_latency_sec =
      std::chrono::duration<double>(0.0);
  int duration = 0;

  results_t &operator += (const results_t &other_result) {
    num_operations += other_result.num_operations;
    tot_latency_sec += other_result.tot_latency_sec;
    return *this;
  }

  void dump(ceph::Formatter *f) const {
    f->open_object_section("results_t");
    f->dump_int("number of operations done", num_operations);
    f->dump_float(
        "the total latency aka time for these operations",
        tot_latency_sec.count());
    f->dump_int("the time the workload took to run is ",
                                   duration);
    f->close_section();
  }
};

/**
 * This function is used to run a workload num_con_io times concurrently
 * the  parallel_for_each loop launches k co_routines
 * Each coroutine is the actual work_load function which returns a
 * future<results_t> Since its a co_await once that future is resolved it is
 * added to the vector so all_io_res containes resolved futures
 */
seastar::future<results_t>
run_concurrent_ios(int duration, int num_concurrent_io,
                   std::function<seastar::future<results_t>()> work_load) {
  std::vector<int> container_io;
  std::vector<results_t> all_io_res;
  for (int i = 0; i < num_concurrent_io; ++i) {
    container_io.push_back(i);
  }
  co_await seastar::parallel_for_each(
      container_io, (seastar::coroutine::lambda([&](int) -> seastar::future<> {
        auto res = co_await work_load();
        all_io_res.push_back(std::move(res));
        co_return;
      })));

  results_t total_result_all_io = {0, std::chrono::duration<double>(0.0),
                                   duration};
  for (const auto res : all_io_res) {
    total_result_all_io += res;
  }
  ceph::JSONFormatter jf;
  total_result_all_io.dump(&jf);
  jf.flush(std::cout);
  std::cout << std::endl;
  co_return total_result_all_io;
};

/**
 * This function adds and removes log entries to a log object
 * It returns throughput(number of operations/nano sec)
 * Also returns average latency (time/operation)
 * This function is split into 3 steps:
 * (a)pre filling the logs to steady state
 * (b) writing and removing logs ,this is considered 1 I/O
 * (c) doing N I/o's concurrently on 1 thread
 */
seastar::future<> pg_log_workload(crimson::os::FuturizedStore &global_store,
                                  int num_logs, int num_concurrent_io,
                                  int duration, int log_size, int log_length) {
  auto &local_store = global_store.get_sharded_store();

  std::map<int, coll_t> collection_id;
  std::map<int, crimson::os::CollectionRef> coll_ref_map;

  /**
   * This method returns a future with pre filled logs
   * It creates num_log number of log objects
   * Each log object has log_length number of entries
   * Each entry is a key value pair, with the value having size log_size
   */

  auto pre_fill_logs = [&]() -> seastar::future<> {
    for (int i = 0; i < num_logs; ++i) {
      auto obj_i = create_hobj(i);
      auto coll_id = make_cid(i, 1);
      collection_id[i] = coll_id;
      auto coll_ref = co_await local_store.create_new_collection(coll_id);
      coll_ref_map[i] = coll_ref;
      std::map<std::string, bufferlist> data;
      for (int j = 0; j < log_length; ++j) {
        std::string key = std::to_string(j);
        bufferlist bl_value;
        bl_value.append_zero(log_size);
        data[key] = bl_value;
      }
      ceph::os::Transaction txn;
      txn.create(coll_id, obj_i);
      txn.omap_setkeys(coll_id, obj_i, data);
      co_await local_store.do_transaction(coll_ref, std::move(txn));
    }
    co_return;
  };

  std::vector<int> first_key_per_log(num_logs,
                                     0); // first key in each log object
  std::vector<int> last_key_per_log(num_logs,
                                    log_length); // last key in each log object

  /**
   * This method returns a future of type struct results_t
   * In this function we choose a random log object to write to and remove keys
   * from We add and remove keys sequentially, add keys to the end, remove from
   * the front Total latency per io is calculated as the sum of the time it
   * takes to add and remove each key
   */
  auto add_remove_entry = [&]() -> seastar::future<results_t> {
    int num_ops = 0;
    std::chrono::duration<double> tot_latency =
        std::chrono::duration<double>(0.0);
    auto start = ceph::mono_clock::now();

    while (ceph::mono_clock::now() - start <=
           (std::chrono::seconds(duration))) { // duration is an int
      int obj_num = std::rand() % num_logs;

      auto object = create_hobj(obj_num);
      auto coll_id = collection_id[obj_num];
      auto coll_ref = coll_ref_map[obj_num];

      std::string key_to_write = std::to_string(last_key_per_log[obj_num]);
      last_key_per_log[obj_num] += 1;

      std::string key_to_remove = std::to_string(first_key_per_log[obj_num]);
      first_key_per_log[obj_num] += 1;

      bufferlist val;
      val.append_zero(log_size);
      std::map<std::string, bufferlist> key_val;
      key_val[key_to_write] = val;

      ceph::os::Transaction one_write_delete;
      one_write_delete.omap_setkeys(coll_id, object, key_val);
      one_write_delete.omap_rmkey(coll_id, object, key_to_remove);

      auto latency_start = ceph::mono_clock::now();
      co_await local_store.do_transaction(coll_ref,
                                          std::move(one_write_delete));
      auto latency_end = ceph::mono_clock::now();

      auto time_nanosec = (latency_end - latency_start);
      std::chrono::duration<double> time_sec =
          std::chrono::duration<double>(time_nanosec);
      tot_latency += time_sec;
      num_ops++;
    }
    co_return results_t{num_ops, tot_latency, duration};
  };
  co_await pre_fill_logs();
  co_await run_concurrent_ios(duration, num_concurrent_io, add_remove_entry);
  co_return;
}

// rgw start

/**
 * This function is a helper function specfically for the rgw workload
 * Since this workload involves insertion and deletion of random keys we have a
 * function to generate a random key of size key_size
 */
std::string generate_random_string(int key_size) {
  std::string res = "";
  for (int i = 0; i < key_size; ++i) {
    char letter = char(std::rand() % 26 + 97);
    res += letter;
  }
  return res;
}

// helper functions for rgw_workload specifically

/**
 * This function is writing a key to a specific bucket
 * given a bucket, a set of keys that aready exist in that bucket
 * the current size of that bucket, this function writes a randomly generated
 * unique key to  that bucket The size of the bucket and the actual insertion
 * into the keyset of the bucket is completed after the transaction to write the
 * key is submitted this method returns a future of type result_t, with the
 * number of operations as 1, and the time as the time it took for the write
 * every time this method is called we will incremnet the number of operations
 * by 1 and the latency by the time it took for this operation
 */

seastar::future<results_t>
write_unique_key(crimson::os::FuturizedStore::Shard &shard_ref, coll_t coll_id,
                 crimson::os::CollectionRef coll_ref, ghobject_t bucket,
                 std::set<std::string> &existing_keys, int key_size,
                 int value_size) {
  std::string new_key = generate_random_string(key_size);
  while (existing_keys.count(new_key) > 0) {
    new_key = generate_random_string(key_size);
  }
  bufferlist value;
  value.append_zero(value_size);

  std::map<std::string, bufferlist> data_entry;
  data_entry[new_key] = value;

  ceph::os::Transaction one_write;
  one_write.omap_setkeys(coll_id, bucket, data_entry);

  auto latency_start = ceph::mono_clock::now();
  co_await shard_ref.do_transaction(coll_ref, std::move(one_write));
  auto latency_end = ceph::mono_clock::now();
  auto time_per_write = (latency_end - latency_start);
  std::chrono::duration<double> time_per_write_sec = time_per_write;

  existing_keys.insert(new_key);
  co_return results_t{1, time_per_write_sec};
}

/**
 * This function is deleting a key from a specific bucket
 * given a bucket, a set of keys that aready exist in that bucket
 * the current size of that bucket, this function deletes a randomly selected
 * key from that bucket The size of the bucket and the actual deletion from the
 * keyset of the bucket is completed after the transaction to write the key is
 * submitted this method returns a future of type result_t, with the number of
 * operations as 1, and the time as the time it took for the write every time
 * this method is called we will incremnet the number of operations by 1 and the
 * latency by the time it took for this operation
 */

seastar::future<results_t>
delete_random_key(crimson::os::FuturizedStore::Shard &shard_ref,
                  coll_t &coll_id, crimson::os::CollectionRef coll_ref,
                  ghobject_t &bucket, std::set<std::string> &existing_keys) {
  int index = std::rand() % existing_keys.size();
  auto it = existing_keys.begin();
  std::advance(it, index);
  std::string key_to_delete = *it;
  existing_keys.erase(it);

  ceph::os::Transaction one_delete;
  one_delete.omap_rmkey(coll_id, bucket, key_to_delete);
  auto latency_start = ceph::mono_clock::now();
  co_await shard_ref.do_transaction(coll_ref, std::move(one_delete));
  auto latency_end = ceph::mono_clock::now();
  auto time_per_delete = (latency_end - latency_start);
  std::chrono::duration<double> time_per_delete_sec = time_per_delete;
  co_return results_t{1, time_per_delete_sec};
}

/**
 * This function randomly adds and removes keys to a randomly chosen rgw bucket
 * It returns throughput(number of operations/nano sec)
 * Also returns average latency (time/operation)
 * This function is split into 3 steps:
 * (a)pre filling the buckets to steady state aka to a set required number of
 * keys in each set of each bucket (b)Randomly choosing to write or remove a key
 * based on the chosen bucets size,aka if its within acceptable range (c) doing
 * N I/o's concurrently on 1 thread
 */
seastar::future<> rgw_index_workload(crimson::os::FuturizedStore &global_store,
                                     int num_indices, int num_concurrent_io,
                                     int duration, int key_size, int value_size,
                                     int target_keys_per_bucket,
                                     int tolerance_range,
                                     int num_buckets_per_collection) {

  auto &local_store = global_store.get_sharded_store();
  std::map<int, coll_t> collection_id_for_rgw;
  std::map<int, crimson::os::CollectionRef>
      coll_ref_map_rgw; // map of bucket number and coll_ref
  std::vector<std::set<std::string>> keys_per_bucket(
      num_indices); // vector where each index is the bucket number and the
                    // value at that index is a set of existing keys in that//
                    // bucket

  /**
   * This method returns a future with pre filled buckets
   * It creates num_indices number of buckets
   * Each bucket is pre-filled to target_keys_per_bucket
   * Each entry is a key value pair, with each key being unique of size key_size
   * and each value with size value_size
   */

  auto pre_fill_buckets = [&]() -> seastar::future<> {
    for (int i = 0; i < num_indices; ++i) {
      auto bucket_i = create_hobj(i);
      auto coll_id = make_cid(i, num_buckets_per_collection);
      collection_id_for_rgw[i] = coll_id;
      auto coll_ref = co_await local_store.create_new_collection(coll_id);
      coll_ref_map_rgw[i] = coll_ref;

      std::map<std::string, bufferlist> omap_for_this_bucket;
      std::set<std::string> keys_in_this_bucket;

      for (int j = 0; j < target_keys_per_bucket; ++j) {
        std::string possible_key = generate_random_string(key_size);
        while (keys_in_this_bucket.count(possible_key) > 0) {
          possible_key = generate_random_string(key_size);
        }
        keys_in_this_bucket.insert(possible_key);
        bufferlist val_for_poss_key;
        val_for_poss_key.append_zero(value_size);
        omap_for_this_bucket[possible_key] = val_for_poss_key;
      }
      keys_per_bucket[i] = keys_in_this_bucket;
      ceph::os::Transaction txn_write_omap_for_bucket;
      txn_write_omap_for_bucket.create(coll_id, bucket_i);
      txn_write_omap_for_bucket.omap_setkeys(coll_id, bucket_i,
                                             omap_for_this_bucket);
      co_await local_store.do_transaction(coll_ref,
                                          std::move(txn_write_omap_for_bucket));
    }
    co_return;
  };

  // size of each bucket, initially each bucket has size
  // target_keys_per_bucket,because all buckets are pre filled
  std::vector<int> size_per_bucket(num_indices, target_keys_per_bucket);

  // min and max size is the range of allowable bucket size
  int min_size =
      std::floor(target_keys_per_bucket * (1 - tolerance_range / 100.0));
  int max_size =
      std::ceil(target_keys_per_bucket * (1 + tolerance_range / 100.0));

  /**
   * This method returns a future of type struct results_t
   * In this function we choose a random bucket to write and remove keys from
   * Based on the size of the bucket we chose, we either force write,force
   * delete or randomly pick to write or delete a key
   */
  auto rgw_actual_test = [&]() -> seastar::future<results_t> {
    int num_ops = 0;
    std::chrono::duration<double> tot_latency =
        std::chrono::duration<double>(0.0);
    auto start = ceph::mono_clock::now();

    while (ceph::mono_clock::now() - start <= std::chrono::seconds(duration)) {

      int bucket_num_we_choose = std::rand() % num_indices;
      auto bucket = create_hobj(bucket_num_we_choose);
      auto coll_id = collection_id_for_rgw[bucket_num_we_choose];
      auto coll_ref = coll_ref_map_rgw[bucket_num_we_choose];

      int size_bucket_we_choose = size_per_bucket[bucket_num_we_choose];
      auto &keys_in_that_bucket = keys_per_bucket[bucket_num_we_choose];

      // this case happens when the size of the bucket is min size and we choose
      // to delete
      if (size_bucket_we_choose <= min_size) {
        results_t result = co_await write_unique_key(
            local_store, coll_id, coll_ref, bucket, keys_in_that_bucket,
            key_size, value_size);
        size_per_bucket[bucket_num_we_choose] += 1;
        tot_latency += result.tot_latency_sec;
        num_ops += result.num_operations;

      } else if (size_bucket_we_choose >= max_size) {
        results_t result = co_await delete_random_key(
            local_store, coll_id, coll_ref, bucket, keys_in_that_bucket);
        size_per_bucket[bucket_num_we_choose] -= 1;
        tot_latency += result.tot_latency_sec;
        num_ops += result.num_operations;
      } else {
        int choice = std::rand() % 2;
        // choice 0 is write, choice 1 is delete
        if (choice == 0) {
          results_t result = co_await write_unique_key(
              local_store, coll_id, coll_ref, bucket, keys_in_that_bucket,
              key_size, value_size);
          size_per_bucket[bucket_num_we_choose] += 1;
          tot_latency += result.tot_latency_sec;
          num_ops += result.num_operations;
        } else {
          results_t result = co_await delete_random_key(
              local_store, coll_id, coll_ref, bucket, keys_in_that_bucket);
          size_per_bucket[bucket_num_we_choose] -= 1;
          tot_latency += result.tot_latency_sec;
          num_ops += result.num_operations;
        }
      };
    }
    co_return results_t{num_ops, tot_latency, duration};
  };

  co_await pre_fill_buckets();
  co_await run_concurrent_ios(duration, num_concurrent_io, rgw_actual_test);
  co_return;
};

int main(int argc, char **argv) {
  po::options_description desc{"Allowed options"};
  bool debug = false;
  std::string store_type;
  std::string store_path;

  desc.add_options()
    ("help,h", "show help message")
    ("store-type", po::value<std::string>(&store_type)->default_value("seastore"),
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
  } catch (const po::error &e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  seastar::app_template::seastar_options app_cfg;
  app_cfg.name = "crimson-store-bench";
  app_cfg.auto_handle_sigint_sigterm = true;
  // Only show "Reactor stalled for" above 200ms
  app_cfg.reactor_opts.blocked_reactor_notify_ms.set_default_value(200);
  seastar::app_template app(std::move(app_cfg));

  std::string work_load_type = "pg_log";
  int num_logs = 0;
  int log_length = 0;
  int log_size = 0;
  int num_concurrent_io = 0;
  int duration = 0;
  int key_size = 0;
  int value_size = 0;
  int num_indices = 0;
  int target_keys_per_bucket = 0;
  int tolerance_range = 0;
  int num_buckets_per_collection = 0;

  std::vector<char *> av{argv[0]};
  std::transform(std::begin(unrecognized_options),
                 std::end(unrecognized_options), std::back_inserter(av),
                 [](auto &s) { return const_cast<char *>(s.c_str()); });
  app.add_options()(
      "work_load_type",
      po::value<std::string>(&work_load_type)->default_value("pg_log"),
      "work load type: pg_log or rgw_index")

      ("num_logs", po::value<int>(&num_logs),
       "how many different logs we create; aka, we create a log for every "
       "object,so how many objects we create")

      ("log_length", po::value<int>(&log_length),
        "number of entries per log")

      ("log_size", po::value<int>(&log_size), "size of each log entry")

      ("num_concurrent_io", po::value<int>(&num_concurrent_io),
        "number of IOs happening simultaneously")

      ("duration", po::value<int>(&duration),
        "how long in seconds the actual testing loop runs "
        "for")
      // for rgw
      ("num_indices", po::value<int>(&num_indices),
       "number of RGW indices/buckets")

      ("key_size", po::value<int>(&key_size), "size of keys in bytes")

      ("value_size", po::value<int>(&value_size),
        "size of values in bytes")

      ("target_keys_per_bucket",
        po::value<int>(&target_keys_per_bucket),
        "target number of keys per bucket")

      ("tolerance_range", po::value<int>(&tolerance_range),
        "tolerance range percentage")

      ("num_buckets_per_collection",
        po::value<int>(&num_buckets_per_collection),
        "the number of objects in each collection ");

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
       * I suggest having a look at
       * src/seastar/include/seastar/core/coroutine.hh for the implementation.
       * Note, the language guarrantees that *arguments* (whether to
       * a lambda or not) have their lifetimes extended for the duration
       * of the coroutine, so this isn't a problem for non-lambda
       * coroutines.
       */
      seastar::coroutine::lambda([&]() -> seastar::future<int> {
        if (debug) {
          seastar::global_logger_registry().set_all_loggers_level(
              seastar::log_level::debug);
        } else {
          seastar::global_logger_registry().set_all_loggers_level(
              seastar::log_level::error);
        }

        co_await crimson::common::sharded_conf().start(
            EntityName{}, std::string_view{"ceph"});
        co_await crimson::common::local_conf().start();

        {
          std::vector<const char *> cav;
          std::transform(
              std::begin(unrecognized_options), std::end(unrecognized_options),
              std::back_inserter(cav), [](auto &s) { return s.c_str(); });
          co_await crimson::common::local_conf().parse_argv(cav);
        }

        auto store = crimson::os::FuturizedStore::create(
            store_type, store_path,
            crimson::common::local_conf().get_config_values());

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
                std::format("error creating empty object store type {} in {}",
                            store_type, store_path)
                    .c_str()));
        co_await store->stop();

        co_await store->start();
        co_await store->mount().handle_error(
            crimson::stateful_ec::assert_failure(
                std::format("error mounting object store type {} in {}",
                            store_type, store_path)
                    .c_str()));
        std::vector<seastar::future<>> per_shard_futures;
        for (unsigned i = 0; i < seastar::smp::count; ++i) {
          auto named_lambda = [=, &store_ref = *store]() -> seastar::future<> {
            fmt::println(std::cout, "running example_io on reactor {}",
                         seastar::this_shard_id());
            if (work_load_type == "pg_log") {
              co_await pg_log_workload(store_ref, num_logs, num_concurrent_io,
                                       duration, log_size, log_length);
            } else if (work_load_type == "rgw_index") {
              co_await rgw_index_workload(
                  store_ref, num_indices, num_concurrent_io, duration, key_size,
                  value_size, target_keys_per_bucket, tolerance_range,
                  num_buckets_per_collection);
            }
            co_return;
          };
          per_shard_futures.push_back(
              seastar::smp::submit_to(i, std::move(named_lambda)));
        }

        co_await seastar::when_all(per_shard_futures.begin(),
                                   per_shard_futures.end());
        co_await store->umount();
        co_await store->stop();
        co_await crimson::common::sharded_conf().stop();
        co_return 0;
      }));
}
