// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

/**
 * crimson-store-bench
 *
 * This tool measures various IO patterns against the crimson FuturizedStore
 * interface.
 *
 * Usage should be:
 *
 * crimson-store-bench --store-path <path> --duration <seconds> --work-load-type <type>
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
   ./build/bin/crimson-store-bench --store-path store_bench_dir --smp 4 --duration 10 --work-load-type pg_log --seastore_device_size 10G
 */

#include <fstream>
#include <random>
#include <vector>
#include <unordered_map>
#include <experimental/random>

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

#include "common/JSONFormatter.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/coroutine.h"
#include "crimson/common/log.h"
#include "crimson/common/metrics_helpers.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

namespace po = boost::program_options;

using namespace std::literals;
using namespace ceph;

SET_SUBSYS(osd);

/**
 * Per-shard results: the number of operations and the total latency for all
 * operations run on one shard, merged across that shard's num_concurrent_io
 * coroutines via operator+=. For the pg workload type write+delete increases
 * the number of operations by 1 For the rgw index workload, each write
 * increases the num_operations by 1 Each delete increases the number of
 * operations by 1
 */
struct results_t {
  uint64_t ios_completed = 0;
  std::chrono::duration<double> total_latency = 0s;
  std::chrono::duration<double> duration = 0s;
  // per-bucket breakdown for this shard, one entry per time bucket
  std::vector<results_t> buckets_io_vector;
  //tracked metric name->value snapshots, one map per bucket index;
  std::vector<std::map<std::string, double>> tracked_metrics_buckets;
  // single aggregate tracked-metric snapshot: populated instead of
  // tracked_metrics_buckets when track-metrics is requested without
  // --show-bucket-output/--csv-output, so a plain --track-metrics run gets
  // one summary value per metric instead of a per-bucket breakdown.
  std::map<std::string, double> tracked_metrics;

  results_t &operator += (const results_t &other_result) {
    ios_completed += other_result.ios_completed;
    total_latency += other_result.total_latency;
    if (buckets_io_vector.empty()) {
      buckets_io_vector = other_result.buckets_io_vector;
    } else {
      // bucket counts are always in lockstep here: every coroutine merged
      // via this operator computed its bucket vector from the same
      // common.bucket_sample_period / common.get_duration(), so both sides are
      // guaranteed the same size.
      for (size_t i = 0; i < buckets_io_vector.size(); ++i) {
        buckets_io_vector[i] += other_result.buckets_io_vector[i];
      }
    }
    // tracked metrics come from a shard-wide registry, not per-coroutine
    // state: every coroutine on this shard reads the same value for a given
    // bucket, so only the first one to report it should populate the
    // bucket -- otherwise we'd add the same value in multiple times.
    if (tracked_metrics_buckets.empty()) {
      tracked_metrics_buckets = other_result.tracked_metrics_buckets;
    } else {
      for (size_t i = 0; i < tracked_metrics_buckets.size(); ++i) {
        if (tracked_metrics_buckets[i].empty()) {
          tracked_metrics_buckets[i] = other_result.tracked_metrics_buckets[i];
        }
      }
    }
    return *this;
  }

  void dump(ceph::Formatter *f,bool show_bucket_output) const {
    f->dump_int("ios_completed", ios_completed);
    f->dump_float("total_latency_s", total_latency.count());
    f->dump_float("total_duration_s", duration.count());
    if (!tracked_metrics.empty()) {
      f->open_object_section("track_metrics");
      for (const auto &[name, val] : tracked_metrics) {
        f->dump_float(name, val);
      }
      f->close_section();
    }
    if (!buckets_io_vector.empty() && show_bucket_output) {
      f->open_object_section("buckets");
      for (size_t i = 0; i < buckets_io_vector.size(); ++i) {
        const auto &b = buckets_io_vector[i];
        f->open_object_section("bucket_" + std::to_string(i));
        f->dump_unsigned("ios_completed", b.ios_completed);
        f->dump_float("avg_latency_s",
          b.ios_completed > 0 ? b.total_latency.count() / b.ios_completed : 0.0);
        if (i < tracked_metrics_buckets.size() &&
            !tracked_metrics_buckets[i].empty()) {
          f->open_object_section("track_metrics");
          for (const auto &[name, val] : tracked_metrics_buckets[i]) {
            f->dump_float(name, val);
          }
          f->close_section();
        }
        f->close_section();
      }
      f->close_section();
    }
  }
};

struct common_options_t {
private:
  unsigned duration = 0;
public:
  unsigned num_concurrent_io = 16;
  bool dump_metrics = false;
  bool show_bucket_output = false;
  std::string track_metrics="";
  unsigned bucket_sample_period = 0;
  std::string csv_output = "";
  std::string raw_elapsed_time_io = "";
  std::chrono::duration<uint64_t> get_duration() const {
    return std::chrono::seconds(duration);
  }

  std::set<std::string> get_requested_metrics() const {
    std::set<std::string> result;
    if (!track_metrics.empty()) {
      std::stringstream ss(track_metrics);
      std::string name;
      while (std::getline(ss, name, ',')) {
        result.insert(name);
      }
    }
    return result;
  }

  po::options_description get_options() {
    po::options_description ret{"Common Options"};
    ret.add_options()
      ("num-concurrent-io", po::value<unsigned>(&num_concurrent_io),
       "number of IOs happening simultaneously")
      ("duration", po::value<unsigned>(&duration)->required(),
       "how long in seconds the actual testing loop runs "
       "for")
      ("dump-metrics", po::bool_switch(&dump_metrics),
       "Dump JSON formatted metrics to stdout")
      ("track-metrics",po:: value<std::string>(&track_metrics),
        "Metrics we want to include in result list,filtered from dump-metrics")
      ("show-bucket-output", po::bool_switch(&show_bucket_output),
       "Show the per-bucket IO/latency (and tracked metrics) breakdown in "
       "the results dump")
      ("bucket-sample-period", po::value<unsigned>(&bucket_sample_period),
       "collect metrics per bucket of this duration in seconds (0 = disabled)")
      ("csv-output", po::value<std::string>(&csv_output),
       "write per-bucket metrics to a CSV file at this path (requires "
       "--bucket-sample-period); one row per bucket, one column per shard per metric")
      ("raw-elapsed-time-io", po::value<std::string>(&raw_elapsed_time_io),
       "write raw (elapsed_s, latency_s) samples to <path>.shard<N>, one "
       "file per shard, buffered in memory and flushed periodically so "
       "long runs don't grow memory unbounded")
      ;
    return ret;
  }
};

class StoreBenchWorkload {
public:
  virtual po::options_description get_options() = 0;
  virtual seastar::future<results_t> run(
    const common_options_t &common,
    crimson::os::FuturizedStore &global_store) = 0;
  virtual ~StoreBenchWorkload() {}
};


class PGLogWorkload final : public StoreBenchWorkload {
  unsigned num_logs = 4;
  unsigned log_size = 1024;
  unsigned log_length = 256;

public:
  po::options_description get_options() final {
    po::options_description ret{"PGLogWorkload"};
    ret.add_options()
      ("num-logs", po::value<unsigned>(&num_logs),
       "how many different logs we create; aka, we create a log for every "
       "object,so how many objects we create")
      ("log-length", po::value<unsigned>(&log_length),
       "number of entries per log")
      ("log-size", po::value<unsigned>(&log_size),
       "size of each log entry")
      ;
    return ret;
  }
  seastar::future<results_t> run(
    const common_options_t &common,
    crimson::os::FuturizedStore &global_store) final;
  ~PGLogWorkload() final {}
};

class RGWIndexWorkload final : public StoreBenchWorkload {
  unsigned num_indices = 16;
  uint64_t key_size = 1024;
  uint64_t value_size = 1024;
  unsigned target_keys_per_bucket = 256;
  unsigned tolerance_range = 10;
  unsigned num_buckets_per_collection = 16;

public:
  po::options_description options{"RGWIndexWorkload"};

  po::options_description get_options() final {
    po::options_description ret{"PGLogWorkload"};
    ret.add_options()
      ("num_indices", po::value<unsigned>(&num_indices),
       "number of RGW indices/buckets")
      ("key_size", po::value<uint64_t>(&key_size),
       "size of keys in bytes")
      ("value_size", po::value<uint64_t>(&value_size),
        "size of values in bytes")
      ("target_keys_per_bucket",
       po::value<unsigned>(&target_keys_per_bucket),
       "target number of keys per bucket")
      ("tolerance_range",
       po::value<unsigned>(&tolerance_range),
       "tolerance range percentage")
      ("num_buckets_per_collection",
       po::value<unsigned>(&num_buckets_per_collection),
       "the number of objects in each collection ")
    ;
    return ret;
  }
  seastar::future<results_t> run(
    const common_options_t &common,
    crimson::os::FuturizedStore &global_store) final;
  ~RGWIndexWorkload() final {}
};

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
 * This function is used to run a workload num_con_io times concurrently
 * the  parallel_for_each loop launches k co_routines
 * Each coroutine is the actual work_load function which returns a
 * future<results_t> Since its a co_await once that future is resolved it is
 * added to the vector so all_io_res containes resolved futures
 */
seastar::future<results_t>
run_concurrent_ios(
  std::chrono::duration<double> duration,
  int num_concurrent_io,
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

  results_t total_result_all_io = {
    0,
    std::chrono::duration<double>(0.0),
    duration
  };
  for (const auto res : all_io_res) {
    total_result_all_io += res;
  }
  co_return total_result_all_io;
};

/**
 * Reads the current values of the requested metrics out of the seastar
 * metrics registry. An empty `requested` set means "no metrics requested"
 * (not "all metrics") -- callers should only invoke this when track-metrics
 * was actually set.
 */
std::map<std::string, double> snapshot_metric_values(
  const std::set<std::string> &requested) {
  LOG_PREFIX(snapshot_metric_values);
  std::map<std::string, double> read_map_metrics;
  for (const auto &[full_name, metric_family] :
       seastar::scollectd::get_value_map()) {
    if (requested.count(full_name) == 0) {
      continue;
    }
    for (const auto &[labels, metric] : metric_family) {
      if (!metric || !metric->is_enabled()) {
        continue;
      }
      switch (auto v = (*metric)(); v.type()) {
      case seastar::metrics::impl::data_type::GAUGE:
      case seastar::metrics::impl::data_type::REAL_COUNTER:
        read_map_metrics[full_name] += v.d();
        break;
      case seastar::metrics::impl::data_type::COUNTER: {
        double val;
        try {
          val = v.ui();
        } catch (std::range_error&) {
          // seastar's cpu steal time may be negative
          val = 0;
        }
        read_map_metrics[full_name] += val;
        break;
      }
      case seastar::metrics::impl::data_type::HISTOGRAM:
        WARN("skipping histogram metric {}, no scalar value to track", full_name);
        break;
      default:
        std::abort();
        break;
      }
    }
  }
  return read_map_metrics;
}

/**
 * Writes the collected per-shard bucket results to a CSV file, one row per
 * bucket. Each shard's buckets are written out in order (shard 0's buckets,
 * then shard 1's, etc.), tagged with a shard column so rows stay
 * identifiable once concatenated.
 */
void write_bucket_csv(
  const std::string &path,
  const std::vector<results_t> &per_shard_results,
  const std::set<std::string> &requested_metrics) {
  std::ofstream csv(path);

  csv << "shard,bucket_index,ios_completed,avg_latency_s";
  for (const auto &name : requested_metrics) {
    csv << "," << name;
  }
  csv << "\n";

  for (size_t s = 0; s < per_shard_results.size(); ++s) {
    const auto &r = per_shard_results[s];
    for (size_t b = 0; b < r.buckets_io_vector.size(); ++b) {
      const auto &bucket = r.buckets_io_vector[b];
      double avg_latency = bucket.ios_completed > 0
        ? bucket.total_latency.count() / bucket.ios_completed
        : 0.0;
      csv << s << "," << b << "," << bucket.ios_completed << "," << avg_latency;
      for (const auto &name : requested_metrics) {
        csv << ",";
        if (b < r.tracked_metrics_buckets.size()) {
          auto it = r.tracked_metrics_buckets[b].find(name);
          if (it != r.tracked_metrics_buckets[b].end()) {
            csv << it->second;
          }
        }
      }
      csv << "\n";
    }
  }
}

/**
 * This function adds and removes log entries to a log object
 * It returns throughput(number of operations/nano sec)
 * Also returns average latency (time/operation)
 * This function is split into 3 steps:
 * (a)pre filling the logs to steady state
 * (b) writing and removing logs ,this is considered 1 I/O
 * (c) doing N I/o's concurrently on 1 thread
 */
seastar::future<results_t> PGLogWorkload::run(
  const common_options_t &common,
  crimson::os::FuturizedStore &global_store)
{
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
    for (unsigned i = 0; i < num_logs; ++i) {
      auto obj_i = create_hobj(i);
      auto coll_id = make_cid(i, 1);
      collection_id[i] = coll_id;
      auto coll_ref = co_await local_store.create_new_collection(coll_id);
      coll_ref_map[i] = coll_ref;
      std::map<std::string, bufferlist> data;
      for (unsigned j = 0; j < log_length; ++j) {
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

  // Buffered, per-shard raw (elapsed_s, latency_s) sample writer, shared by
  // reference across this shard's num_concurrent_io coroutines below.
  constexpr size_t raw_elapsed_time_io_buffer_capacity = 8192;
  std::vector<std::pair<double, double>> raw_elapsed_time_io_buffer;
  std::ofstream raw_elapsed_time_io_file;
  if (!common.raw_elapsed_time_io.empty()) {
    raw_elapsed_time_io_buffer.reserve(raw_elapsed_time_io_buffer_capacity);
    raw_elapsed_time_io_file.open(
      common.raw_elapsed_time_io + ".shard" +
      std::to_string(seastar::this_shard_id()));
    raw_elapsed_time_io_file << "elapsed_s,latency_s\n";
  }
  auto flush_raw_elapsed_time_io = [&]() {
    for (const auto &[elapsed_s, latency_s] : raw_elapsed_time_io_buffer) {
      raw_elapsed_time_io_file << elapsed_s << "," << latency_s << "\n";
    }
    raw_elapsed_time_io_buffer.clear();
  };

  /**
   * This method returns a future of type struct results_t
   * In this function we choose a random log object to write to and remove keys
   * from We add and remove keys sequentially, add keys to the end, remove from
   * the front Total latency per io is calculated as the sum of the time it
   * takes to add and remove each key
   */
  auto add_remove_entry = [&]() -> seastar::future<results_t> {
    uint64_t num_ops = 0;
    std::chrono::duration<double> tot_latency =
        std::chrono::duration<double>(0.0);
    auto start = ceph::mono_clock::now();

    unsigned num_buckets = 0;
    if (common.bucket_sample_period > 0) {
      num_buckets = common.get_duration().count() / common.bucket_sample_period;
      if (num_buckets == 0) {
        num_buckets = 1;
      }
    }
    std::vector<results_t> local_buckets(num_buckets);
    std::set<std::string> requested_metrics = common.get_requested_metrics();
    std::vector<std::map<std::string, double>> local_track_metrics_buckets(num_buckets);

    while (ceph::mono_clock::now() - start <= common.get_duration()) {
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

      if (num_buckets > 0 || !common.raw_elapsed_time_io.empty()) {
        std::chrono::duration<double> elapsed = latency_end - start;

        if (!common.raw_elapsed_time_io.empty()) {
          raw_elapsed_time_io_buffer.push_back({elapsed.count(), time_sec.count()});
          if (raw_elapsed_time_io_buffer.size() >= raw_elapsed_time_io_buffer_capacity) {
            flush_raw_elapsed_time_io();
          }
        }

        if (num_buckets > 0) {
          unsigned bucket_index = elapsed.count() / common.bucket_sample_period;
          if (bucket_index >= num_buckets) {
            bucket_index = num_buckets - 1;
          }
          local_buckets[bucket_index].ios_completed++;
          local_buckets[bucket_index].total_latency += time_sec;
          if (!common.track_metrics.empty() &&
              local_track_metrics_buckets[bucket_index].empty()) {
            local_track_metrics_buckets[bucket_index] =
              snapshot_metric_values(requested_metrics);
          }
        }
      }
    }
    if (!common.raw_elapsed_time_io.empty()) {
      flush_raw_elapsed_time_io();
    }
    co_return results_t{num_ops, tot_latency, common.get_duration(),
      std::move(local_buckets), std::move(local_track_metrics_buckets)};
  };
  co_await pre_fill_logs();
  auto result = co_await run_concurrent_ios(
    common.get_duration(), common.num_concurrent_io, add_remove_entry);
  // --track-metrics without --show-bucket-output means "one summary value
  // per metric", not a per-bucket breakdown
  if (!common.show_bucket_output && !common.track_metrics.empty()) {
    result.tracked_metrics =
      snapshot_metric_values(common.get_requested_metrics());
  }
  co_return result;
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
seastar::future<results_t> RGWIndexWorkload::run(
  const common_options_t &common,
  crimson::os::FuturizedStore &global_store)
{
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
    for (unsigned i = 0; i < num_indices; ++i) {
      auto bucket_i = create_hobj(i);
      auto coll_id = make_cid(i, num_buckets_per_collection);
      collection_id_for_rgw[i] = coll_id;
      auto coll_ref = co_await local_store.create_new_collection(coll_id);
      coll_ref_map_rgw[i] = coll_ref;

      std::map<std::string, bufferlist> omap_for_this_bucket;
      std::set<std::string> keys_in_this_bucket;

      for (unsigned j = 0; j < target_keys_per_bucket; ++j) {
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
    auto start = ceph::mono_clock::now();
    results_t results;

    while (ceph::mono_clock::now() - start <= common.get_duration()) {

      int bucket_num_we_choose = std::rand() % num_indices;
      auto bucket = create_hobj(bucket_num_we_choose);
      auto coll_id = collection_id_for_rgw[bucket_num_we_choose];
      auto coll_ref = coll_ref_map_rgw[bucket_num_we_choose];

      int size_bucket_we_choose = size_per_bucket[bucket_num_we_choose];
      auto &keys_in_that_bucket = keys_per_bucket[bucket_num_we_choose];

      // this case happens when the size of the bucket is min size and we choose
      // to delete
      if (size_bucket_we_choose <= min_size) {
        results += co_await write_unique_key(
            local_store, coll_id, coll_ref, bucket, keys_in_that_bucket,
            key_size, value_size);
        size_per_bucket[bucket_num_we_choose] += 1;

      } else if (size_bucket_we_choose >= max_size) {
        results += co_await delete_random_key(
            local_store, coll_id, coll_ref, bucket, keys_in_that_bucket);
        size_per_bucket[bucket_num_we_choose] -= 1;
      } else {
        int choice = std::rand() % 2;
        // choice 0 is write, choice 1 is delete
        if (choice == 0) {
          results += co_await write_unique_key(
              local_store, coll_id, coll_ref, bucket, keys_in_that_bucket,
              key_size, value_size);
          size_per_bucket[bucket_num_we_choose] += 1;
        } else {
          results += co_await delete_random_key(
              local_store, coll_id, coll_ref, bucket, keys_in_that_bucket);
          size_per_bucket[bucket_num_we_choose] -= 1;
        }
      };
    }
    results.duration = ceph::mono_clock::now() - start;
    co_return results;
  };

  co_await pre_fill_buckets();
  co_return co_await run_concurrent_ios(
    common.get_duration(), common.num_concurrent_io, rgw_actual_test);
};


/**
 * RandomWriteWorkload 
 *
 * Performs a simple random write workload.
 */
class RandomWriteWorkload final : public StoreBenchWorkload {
  uint64_t prefill_size = 128<<10;
  uint64_t io_size = 4<<10;
  uint64_t size_per_shard = 64<<20;
  uint64_t size_per_obj = 4<<20;
  uint64_t colls_per_shard = 16;
  uint64_t io_concurrency_per_shard = 16;
  uint64_t get_obj_per_shard() const {
    return size_per_shard / size_per_obj;
  }
public:
  po::options_description get_options() final {
    po::options_description ret{"RandomWriteWorkload"};
    ret.add_options()
      ("prefill-size", po::value<uint64_t>(&prefill_size),
       "IO size to use when prefilling objets")
      ("io-size", po::value<uint64_t>(&io_size),
       "IO size")
      ("size-per-shard", po::value<uint64_t>(&size_per_shard),
       "Total size per shard")
      ("size-per-obj", po::value<uint64_t>(&size_per_obj),
       "Object Size")
      ("colls-per-shard", po::value<uint64_t>(&colls_per_shard),
       "Collections per shard")
      ("io-concurrency-per-shard", po::value<uint64_t>(&io_concurrency_per_shard),
       "IO Concurrency Per Shard")
      ;
    return ret;
  }
  seastar::future<results_t> run(
    const common_options_t &common,
    crimson::os::FuturizedStore &global_store) final;
  ~RandomWriteWorkload() final {}
};

seastar::future<bufferptr> generate_random_bp(uint64_t size)
{
  bufferptr bp(ceph::buffer::create_page_aligned(size));
  auto f = co_await seastar::open_file_dma(
    "/dev/urandom", seastar::open_flags::ro);
  static constexpr uint64_t STRIDE = 256<<10;
  for (uint64_t off = 0; off < size; off += STRIDE) {
    co_await f.dma_read(off, bp.c_str() + off, STRIDE);
  }
  co_return bp;
}


seastar::future<results_t> RandomWriteWorkload::run(
  const common_options_t &common,
  crimson::os::FuturizedStore &global_store)
{
  LOG_PREFIX(random_write);
  auto &local_store = global_store.get_sharded_store();

  auto random_buffer = co_await generate_random_bp(16<<20);
  auto get_random_buffer = [&random_buffer](uint64_t size) {
    assert((size % CEPH_PAGE_SIZE) == 0);
    bufferptr bp(
      random_buffer,
      std::experimental::randint<uint64_t>(
        0,
        (random_buffer.length() - size) / CEPH_PAGE_SIZE) *
      CEPH_PAGE_SIZE,
      size);
    assert(bp.is_page_aligned());
    bufferlist bl;
    bl.append(bp);
    return bl;
  };

  auto create_hobj = [](uint64_t obj_id) {
    return ghobject_t(
      shard_id_t::NO_SHARD,
      0,     // pool id
      obj_id, // hash, normally rjenkins of name, but let's just set it to id
      "",    // namespace, empty here
      "",    // name, empty here
      0,     // snapshot
      ghobject_t::NO_GEN);
  };

  std::unordered_map<
    uint64_t,
    std::pair<coll_t, crimson::os::CollectionRef>
    > coll_refs;
  for (uint64_t collidx = 0; collidx < colls_per_shard; ++collidx) {
   coll_t cid(
     spg_t(pg_t(0, (seastar::this_shard_id() * colls_per_shard) + collidx))
   );
   auto ref = co_await local_store.create_new_collection(
     cid);
   coll_refs.emplace(collidx, std::make_pair(cid, std::move(ref)));
  }
  auto get_coll_id = [&](uint64_t obj_id) {
    assert(coll_refs.contains(obj_id % colls_per_shard));
    return coll_refs.at(obj_id % colls_per_shard).first;
  };
  auto get_coll_ref = [&](uint64_t obj_id) {
    assert(coll_refs.contains(obj_id % colls_per_shard));
    return coll_refs.at(obj_id % colls_per_shard).second;
  };

  unsigned running = 0;
  std::optional<seastar::promise<>> complete;

  static constexpr unsigned io_concurrency_per_shard = 16;
  seastar::semaphore sem{io_concurrency_per_shard};
  results_t results;
  auto submit_transaction = [&](
    crimson::os::CollectionRef &col_ref,
    ceph::os::Transaction &&t) -> seastar::future<> {
    ++running;
    co_await sem.wait(1);
    std::ignore = local_store.do_transaction(
      col_ref,
      std::move(t)
    ).finally([&, start = ceph::mono_clock::now()] {
      --running;
      if (running == 0 && complete) {
        complete->set_value();
      }
      sem.signal(1);
      results.ios_completed++;
      results.total_latency += ceph::mono_clock::now() - start;
    });
  };

  for (uint64_t obj_id = 0; obj_id < get_obj_per_shard(); ++obj_id) {
    auto hobj = create_hobj(obj_id);
    auto coll_id = get_coll_id(obj_id);
    auto coll_ref = get_coll_ref(obj_id);
    
    {
      ceph::os::Transaction t;
      t.create(coll_id, hobj);
      co_await submit_transaction(coll_ref, std::move(t));
    }
    for (uint64_t off = 0; off < size_per_obj; off += prefill_size) {
      ceph::os::Transaction t;
      t.write(coll_id, hobj, off, prefill_size, get_random_buffer(prefill_size));
      co_await submit_transaction(coll_ref, std::move(t));
    }
    INFO("wrote obj {} of {}", obj_id, get_obj_per_shard());
  }

  INFO("finished populating");

  auto start = ceph::mono_clock::now();
  uint64_t writes_started = 0;
  while (ceph::mono_clock::now() - start < common.get_duration()) {
    auto obj_id = std::experimental::randint<uint64_t>(0, get_obj_per_shard() - 1);
    auto hobj = create_hobj(obj_id);
    auto coll_id = get_coll_id(obj_id);
    auto coll_ref = get_coll_ref(obj_id);

    auto offset = std::experimental::randint<uint64_t>(
      0,
      (size_per_obj / io_size) - 1) * io_size;
    
    ceph::os::Transaction t;
    t.write(
      coll_id,
      hobj,
      offset,
      io_size,
      get_random_buffer(io_size));
    co_await submit_transaction(coll_ref, std::move(t));
    ++writes_started;
  }

  INFO("writes_started {}", writes_started);
  for (auto &[_, entry]: coll_refs) {
    auto &[id, ref] = entry;
    INFO("flushing {}", id);
    co_await local_store.flush(ref);
  }

  if (running > 0) {
    complete = seastar::promise<>();
    co_await complete->get_future();
  }

  results.duration = ceph::mono_clock::now() - start;
  co_return results;
}

int main(int argc, char **argv) {
  LOG_PREFIX(main);
  po::options_description desc{"Allowed options"};
  bool debug = false;
  std::string store_type;
  std::string store_path;
  std::string work_load_type;
  unsigned smp = 4;

  desc.add_options()("help,h", "show help message")
    ("store-type",
     po::value<std::string>(&store_type)->default_value("seastore"),
     "set store type")
      /* store-path is a path to a directory containing a file 'block'
       * block should be a symlink to a real device for actual performance
       * testing, but may be a file for testing this utility.
       * See build/dev/osd* after starting a vstart cluster for an example
       * of what that looks like.
       */
    ("store-path", po::value<std::string>(&store_path)->required(),
     "path to store, <store-path>/block should "
     "be a symlink to the target device for bluestore or seastore")
    ("debug", po::bool_switch(&debug), "enable debugging")
    ("work-load-type",
     po::value<std::string>(&work_load_type)->required(),
     "work load type: pg_log, rgw_index or random_write")
    ("smp", po::value<unsigned>(&smp),
     "number of reactors");
  
  common_options_t common_options;
  std::map<std::string, std::unique_ptr<StoreBenchWorkload>> workloads;
    
  workloads.emplace("pg_log", std::make_unique<PGLogWorkload>());
  workloads.emplace("rgw_index", std::make_unique<RGWIndexWorkload>());
  workloads.emplace("random_write", std::make_unique<RandomWriteWorkload>());
  
  desc.add(common_options.get_options());
  for (auto &[name, workload] : workloads) {
    desc.add(workload->get_options());
  }

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

  auto smp_str = std::to_string(smp);
  const char *av[] = { argv[0], "--smp", smp_str.c_str() };
  return app.run(
      sizeof(av) / sizeof(av[0]), const_cast<char **>(av),
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
        std::vector<seastar::future<results_t>> per_shard_futures;

        auto named_lambda = [&, &store_ref = *store]()
          -> seastar::future<results_t> {
          DEBUG("running example_io on reactor {}", seastar::this_shard_id());
          auto iter = workloads.find(work_load_type);
          if (iter != workloads.end()) {
            co_return co_await iter->second->run(common_options, store_ref);
          } else {
            co_return results_t{};
          }
        };
        for (unsigned i = 0; i < seastar::smp::count; ++i) {
          per_shard_futures.push_back(
              seastar::smp::submit_to(i, std::move(named_lambda)));
        }

        JSONFormatter f(true /* pretty */);
        f.open_object_section("store-bench");
        std::vector<results_t> per_shard_results;
        auto requested_metrics = common_options.get_requested_metrics();
        {
          f.dump_float("duration_s", common_options.get_duration().count());
          f.open_array_section("results");
          for (unsigned i = 0; i < per_shard_futures.size(); ++i) {
            auto results = co_await std::move(per_shard_futures[i]);
            f.open_object_section("result");
            // when writing buckets to CSV, keep the (potentially large)
            // per-bucket breakdown out of the stdout JSON entirely.
            results.dump(&f, common_options.show_bucket_output &&
              common_options.csv_output.empty());
            f.dump_string("shard", std::to_string(i));
            if (common_options.dump_metrics) {
              // scollectd::get_value_map() only sees the calling shard's
              // local metrics registry, so this has to run on shard i
              // itself rather than on the shard driving this loop.
              co_await seastar::smp::submit_to(i, [&f, &requested_metrics]() {
                f.open_array_section("metrics_values");
                crimson::metrics::dump_metric_value_map(
                  seastar::scollectd::get_value_map(),
                  &f,
                  [&requested_metrics](const std::string& metric_name) {
                    return requested_metrics.empty() ||
                      requested_metrics.count(metric_name) > 0;
                  });
                f.close_section();
              });
            }
            f.close_section();
            per_shard_results.push_back(std::move(results));
          }
          f.close_section();
        }
        f.close_section();
        f.flush(std::cout);

        if (!common_options.csv_output.empty()) {
          write_bucket_csv(
            common_options.csv_output, per_shard_results, requested_metrics);
        }

        co_await store->umount();
        co_await store->stop();
        co_await crimson::common::sharded_conf().stop();
        co_return 0;
      }));
}
