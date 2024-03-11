#include "common.h"

ceph::mutex glock = ceph::make_mutex("glock");

po::options_description make_usage() {
  po::options_description desc("Usage");
  desc.add_options()
    ("help,h", ": produce help message")
    ("--pool <POOL> --chunk-pool <POOL>",
     ": perform deduplication on the target pool")
    ;
  po::options_description op_desc("Opational arguments");
  op_desc.add_options()
    ("chunk-size", po::value<int>(), ": chunk size (byte)")
    ("chunk-algorithm", po::value<std::string>(), ": <fixed|fastcdc>, set chunk-algorithm")
    ("fingerprint-algorithm", po::value<std::string>(), ": <sha1|sha256|sha512>, set fingerprint-algorithm")
    ("chunk-pool", po::value<std::string>(), ": set chunk pool name")
    ("max-thread", po::value<int>(), ": set max thread")
    ("report-period", po::value<int>(), ": set report-period")
    ("max-seconds", po::value<int>(), ": set max runtime")
    ("max-read-size", po::value<int>(), ": set max read size")
    ("pool", po::value<std::string>(), ": set pool name")
    ("min-chunk-size", po::value<int>(), ": min chunk size (byte)")
    ("max-chunk-size", po::value<int>(), ": max chunk size (byte)")
    ("dedup-cdc-chunk-size", po::value<unsigned int>(), ": set dedup chunk size for cdc")
    ("snap", ": deduplciate snapshotted object")
    ("debug", ": enable debug")
    ("pgid", ": set pgid")
    ("chunk-dedup-threshold", po::value<int>(), ": set the threshold for chunk dedup (number of duplication) ")
    ("sampling-ratio", po::value<int>(), ": set the sampling ratio (percentile)")
    ("wakeup-period", po::value<int>(), ": set the wakeup period of crawler thread (sec)")
    ("fpstore-threshold", po::value<size_t>()->default_value(100_M), ": set max size of in-memory fingerprint store (bytes)")
  ;
  desc.add(op_desc);
  return desc;
}

using AioCompRef = unique_ptr<AioCompletion>;

class SampleDedupWorkerThread : public Thread
{
public:
  struct chunk_t {
    string oid = "";
    size_t start = 0;
    size_t size = 0;
    string fingerprint = "";
    bufferlist data;
  };

  using dup_count_t = size_t;

  template <typename K, typename V>
  class FpMap {
    using map_t = std::unordered_map<K, V>;
  public:
    /// Represents a nullable reference into logical container
    class entry_t {
      /// Entry may be into one of two maps or NONE, indicates which
      enum entry_into_t {
	UNDER, OVER, NONE
      } entry_into = NONE;

      /// Valid iterator into map for UNDER|OVER, default for NONE
      typename map_t::iterator iter;

      entry_t(entry_into_t entry_into, typename map_t::iterator iter) :
	entry_into(entry_into), iter(iter) {
	ceph_assert(entry_into != NONE);
      }

    public:
      entry_t() = default;

      auto &operator*() {
	ceph_assert(entry_into != NONE);
	return *iter;
      }
      auto operator->() {
	ceph_assert(entry_into != NONE);
	return iter.operator->();
      }
      bool is_valid() const {
	return entry_into != NONE;
      }
      bool is_above_threshold() const {
	return entry_into == entry_t::OVER;
      }
      friend class FpMap;
    };

    /// inserts str, count into container, must not already be present
    entry_t insert(const K &str, V count) {
      std::pair<typename map_t::iterator, bool> r;
      typename entry_t::entry_into_t s;
      if (count < dedup_threshold) {
       r = under_threshold_fp_map.insert({str, count});
       s = entry_t::UNDER;
      } else {
       r = over_threshold_fp_map.insert({str, count});
       s = entry_t::OVER;
      }
      ceph_assert(r.second);
      return entry_t{s, r.first};
    }

    /// increments refcount for entry, promotes as necessary, entry must be valid
    entry_t increment_reference(entry_t entry) {
      ceph_assert(entry.is_valid());
      entry.iter->second++;
      if (entry.entry_into == entry_t::OVER ||
	  entry.iter->second < dedup_threshold) {
	return entry;
      } else {
	auto [over_iter, inserted] = over_threshold_fp_map.insert(
	  *entry);
	ceph_assert(inserted);
	under_threshold_fp_map.erase(entry.iter);
	return entry_t{entry_t::OVER, over_iter};
      }
    }

    /// returns entry for fp, return will be !is_valid() if not present
    auto find(const K &fp) {
      if (auto iter = under_threshold_fp_map.find(fp);
	  iter != under_threshold_fp_map.end()) {
	return entry_t{entry_t::UNDER, iter};
      } else if (auto iter = over_threshold_fp_map.find(fp);
		 iter != over_threshold_fp_map.end()) {
	return entry_t{entry_t::OVER, iter};
      }  else {
	return entry_t{};
      }
    }

    /// true if container contains fp
    bool contains(const K &fp) {
      return find(fp).is_valid();
    }

    /// returns number of items
    size_t get_num_items() const {
      return under_threshold_fp_map.size() + over_threshold_fp_map.size();
    }

    /// returns estimate of total in-memory size (bytes)
    size_t estimate_total_size() const {
      size_t total = 0;
      if (!under_threshold_fp_map.empty()) {
	total += under_threshold_fp_map.size() *
	  (under_threshold_fp_map.begin()->first.size() + sizeof(V));
      }
      if (!over_threshold_fp_map.empty()) {
	total += over_threshold_fp_map.size() *
	  (over_threshold_fp_map.begin()->first.size() + sizeof(V));
      }
      return total;
    }

    /// true if empty
    bool empty() const {
      return under_threshold_fp_map.empty() && over_threshold_fp_map.empty();
    }

    /// instructs container to drop entries with refcounts below threshold
    void drop_entries_below_threshold() {
      under_threshold_fp_map.clear();
    }

    FpMap(size_t dedup_threshold) : dedup_threshold(dedup_threshold) {}
    FpMap() = delete;
  private:
    map_t under_threshold_fp_map;
    map_t over_threshold_fp_map;
    const size_t dedup_threshold;
  };

  class FpStore {
  public:
    void maybe_print_status() {
      utime_t now = ceph_clock_now();
      if (next_report != utime_t() && now > next_report) {
	cerr << (int)(now - start) << "s : read "
	     << total_bytes << " bytes so far..."
	     << std::endl;
	next_report = now;
	next_report += report_period;
      }
    }

    bool contains(string& fp) {
      std::shared_lock lock(fingerprint_lock);
      return fp_map.contains(fp);
    }

    // return true if the chunk is duplicate
    bool add(chunk_t& chunk) {
      std::unique_lock lock(fingerprint_lock);
      auto entry = fp_map.find(chunk.fingerprint);
      total_bytes += chunk.size;
      if (!entry.is_valid()) {
	if (is_fpmap_full()) {
	  fp_map.drop_entries_below_threshold();
	  if (is_fpmap_full()) {
	    return false;
	  }
	}
	entry = fp_map.insert(chunk.fingerprint, 1);
      } else {
	entry = fp_map.increment_reference(entry);
      }
      return entry.is_above_threshold();
    }

    bool is_fpmap_full() const {
      return fp_map.estimate_total_size() >= memory_threshold;
    }

    FpStore(size_t chunk_threshold,
      uint32_t report_period,	
      size_t memory_threshold) :
      report_period(report_period),
      memory_threshold(memory_threshold),
      fp_map(chunk_threshold) { }
    FpStore() = delete;

  private:
    std::shared_mutex fingerprint_lock;
    const utime_t start = ceph_clock_now();
    utime_t next_report;
    const uint32_t report_period;
    size_t total_bytes = 0; // Accessed in the worker threads under fingerprint_lock
    const size_t memory_threshold;
    FpMap<std::string, dup_count_t> fp_map; // Accessed in the worker threads under fingerprint_lock
  };

  struct SampleDedupGlobal {
    FpStore fp_store;
    const double sampling_ratio = -1;
    SampleDedupGlobal(
      size_t chunk_threshold,
      int sampling_ratio,
      uint32_t report_period,
      size_t fpstore_threshold) :
      fp_store(chunk_threshold, report_period, fpstore_threshold),
      sampling_ratio(static_cast<double>(sampling_ratio) / 100) { }
  };

  SampleDedupWorkerThread(
    IoCtx &io_ctx,
    IoCtx &chunk_io_ctx,
    ObjectCursor begin,
    ObjectCursor end,
    size_t chunk_size,
    std::string &fp_algo,
    std::string &chunk_algo,
    SampleDedupGlobal &sample_dedup_global,
    bool snap) :
    chunk_io_ctx(chunk_io_ctx),
    chunk_size(chunk_size),
    fp_type(pg_pool_t::get_fingerprint_from_str(fp_algo)),
    chunk_algo(chunk_algo),
    sample_dedup_global(sample_dedup_global),
    begin(begin),
    end(end),
    snap(snap) {
      this->io_ctx.dup(io_ctx);
    }

  ~SampleDedupWorkerThread() { };

  size_t get_total_duplicated_size() const {
    return total_duplicated_size;
  }

  size_t get_total_object_size() const {
    return total_object_size;
  }

protected:
  void* entry() override {
    crawl();
    return nullptr;
  }

private:
  void crawl();
  std::tuple<std::vector<ObjectItem>, ObjectCursor> get_objects(
    ObjectCursor current,
    ObjectCursor end,
    size_t max_object_count);
  std::vector<size_t> sample_object(size_t count);
  void try_dedup_and_accumulate_result(ObjectItem &object, snap_t snap = 0);
  bool ok_to_dedup_all();
  int do_chunk_dedup(chunk_t &chunk, snap_t snap);
  bufferlist read_object(ObjectItem &object);
  std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> do_cdc(
    ObjectItem &object,
    bufferlist &data);
  std::string generate_fingerprint(bufferlist chunk_data);
  AioCompRef do_async_evict(string oid);

  IoCtx io_ctx;
  IoCtx chunk_io_ctx;
  size_t total_duplicated_size = 0;
  size_t total_object_size = 0;

  std::set<std::pair<std::string, snap_t>> oid_for_evict;
  const size_t chunk_size = 0;
  pg_pool_t::fingerprint_t fp_type = pg_pool_t::TYPE_FINGERPRINT_NONE;
  std::string chunk_algo;
  SampleDedupGlobal &sample_dedup_global;
  ObjectCursor begin;
  ObjectCursor end;
  bool snap;
};

void SampleDedupWorkerThread::crawl()
{
  cout << "new iteration" << std::endl;

  ObjectCursor current_object = begin;
  while (current_object < end) {
    std::vector<ObjectItem> objects;
    // Get the list of object IDs to deduplicate
    std::tie(objects, current_object) = get_objects(current_object, end, 100);

    // Pick few objects to be processed. Sampling ratio decides how many
    // objects to pick. Lower sampling ratio makes crawler have lower crawling
    // overhead but find less duplication.
    auto sampled_indexes = sample_object(objects.size());
    for (size_t index : sampled_indexes) {
      ObjectItem target = objects[index];
      if (snap) {
	io_ctx.snap_set_read(librados::SNAP_DIR);
	snap_set_t snap_set;
	int snap_ret;
	ObjectReadOperation op;
	op.list_snaps(&snap_set, &snap_ret);
	io_ctx.operate(target.oid, &op, NULL);

	for (vector<librados::clone_info_t>::const_iterator r = snap_set.clones.begin();
	  r != snap_set.clones.end();
	  ++r) {
	  io_ctx.snap_set_read(r->cloneid);
	  try_dedup_and_accumulate_result(target, r->cloneid);
	}
      } else {
	try_dedup_and_accumulate_result(target);
      }
    }
  }

  vector<AioCompRef> evict_completions(oid_for_evict.size());
  int i = 0;
  for (auto &oid : oid_for_evict) {
    if (snap) {
      io_ctx.snap_set_read(oid.second);
    }
    evict_completions[i] = do_async_evict(oid.first);
    i++;
  }
  for (auto &completion : evict_completions) {
    completion->wait_for_complete();
  }
  cout << "done iteration" << std::endl;
}

AioCompRef SampleDedupWorkerThread::do_async_evict(string oid)
{
  Rados rados;
  ObjectReadOperation op_tier;
  AioCompRef completion(rados.aio_create_completion());
  op_tier.tier_evict();
  io_ctx.aio_operate(
      oid,
      completion.get(),
      &op_tier,
      NULL);
  return completion;
}

std::tuple<std::vector<ObjectItem>, ObjectCursor> SampleDedupWorkerThread::get_objects(
  ObjectCursor current, ObjectCursor end, size_t max_object_count)
{
  std::vector<ObjectItem> objects;
  ObjectCursor next;
  int ret = io_ctx.object_list(
    current,
    end,
    max_object_count,
    {},
    &objects,
    &next);
  if (ret < 0 ) {
    cerr << "error object_list" << std::endl;
    objects.clear();
  }

  return std::make_tuple(objects, next);
}

std::vector<size_t> SampleDedupWorkerThread::sample_object(size_t count)
{
  std::vector<size_t> indexes(count);
  for (size_t i = 0 ; i < count ; i++) {
    indexes[i] = i;
  }
  default_random_engine generator;
  shuffle(indexes.begin(), indexes.end(), generator);
  size_t sampling_count = static_cast<double>(count) *
    sample_dedup_global.sampling_ratio;
  indexes.resize(sampling_count);

  return indexes;
}

void SampleDedupWorkerThread::try_dedup_and_accumulate_result(
  ObjectItem &object, snap_t snap)
{
  bufferlist data = read_object(object);
  if (data.length() == 0) {
    cerr << __func__ << " skip object " << object.oid
	 << " read returned size 0" << std::endl;
    return;
  }
  auto chunks = do_cdc(object, data);
  size_t chunk_total_amount = 0;

  // First, check total size of created chunks
  for (auto &chunk : chunks) {
    auto &chunk_data = std::get<0>(chunk);
    chunk_total_amount += chunk_data.length();
  }
  if (chunk_total_amount != data.length()) {
    cerr << __func__ << " sum of chunked length(" << chunk_total_amount
	 << ") is different from object data length(" << data.length() << ")"
	 << std::endl;
    return;
  }

  size_t duplicated_size = 0;
  list<chunk_t> redundant_chunks;
  for (auto &chunk : chunks) {
    auto &chunk_data = std::get<0>(chunk);
    std::string fingerprint = generate_fingerprint(chunk_data);
    std::pair<uint64_t, uint64_t> chunk_boundary = std::get<1>(chunk);
    chunk_t chunk_info = {
      .oid = object.oid,
      .start = chunk_boundary.first,
      .size = chunk_boundary.second,
      .fingerprint = fingerprint,
      .data = chunk_data
      };

    if (sample_dedup_global.fp_store.contains(fingerprint)) {
      duplicated_size += chunk_data.length();
    }
    if (sample_dedup_global.fp_store.add(chunk_info)) {
      redundant_chunks.push_back(chunk_info);
    }
  }

  size_t object_size = data.length();

  // perform chunk-dedup
  for (auto &p : redundant_chunks) {
    do_chunk_dedup(p, snap);
  }
  total_duplicated_size += duplicated_size;
  total_object_size += object_size;
}

bufferlist SampleDedupWorkerThread::read_object(ObjectItem &object)
{
  bufferlist whole_data;
  size_t offset = 0;
  int ret = -1;
  while (ret != 0) {
    bufferlist partial_data;
    ret = io_ctx.read(object.oid, partial_data, default_op_size, offset);
    if (ret < 0) {
      cerr << "read object error " << object.oid << " offset " << offset
        << " size " << default_op_size << " error(" << cpp_strerror(ret)
        << std::endl;
      bufferlist empty_buf;
      return empty_buf;
    }
    offset += ret;
    whole_data.claim_append(partial_data);
  }
  return whole_data;
}

std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> SampleDedupWorkerThread::do_cdc(
  ObjectItem &object,
  bufferlist &data)
{
  std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> ret;

  unique_ptr<CDC> cdc = CDC::create(chunk_algo, cbits(chunk_size) - 1);
  vector<pair<uint64_t, uint64_t>> chunks;
  cdc->calc_chunks(data, &chunks);
  for (auto &p : chunks) {
    bufferlist chunk;
    chunk.substr_of(data, p.first, p.second);
    ret.push_back(make_tuple(chunk, p));
  }

  return ret;
}

std::string SampleDedupWorkerThread::generate_fingerprint(bufferlist chunk_data)
{
  string ret;

  switch (fp_type) {
    case pg_pool_t::TYPE_FINGERPRINT_SHA1:
      ret = crypto::digest<crypto::SHA1>(chunk_data).to_str();
      break;

    case pg_pool_t::TYPE_FINGERPRINT_SHA256:
      ret = crypto::digest<crypto::SHA256>(chunk_data).to_str();
      break;

    case pg_pool_t::TYPE_FINGERPRINT_SHA512:
      ret = crypto::digest<crypto::SHA512>(chunk_data).to_str();
      break;
    default:
      ceph_assert(0 == "Invalid fp type");
      break;
  }
  return ret;
}

int SampleDedupWorkerThread::do_chunk_dedup(chunk_t &chunk, snap_t snap)
{
  uint64_t size;
  time_t mtime;

  int ret = chunk_io_ctx.stat(chunk.fingerprint, &size, &mtime);

  if (ret == -ENOENT) {
    bufferlist bl;
    bl.append(chunk.data);
    ObjectWriteOperation wop;
    wop.write_full(bl);
    chunk_io_ctx.operate(chunk.fingerprint, &wop);
  } else {
    ceph_assert(ret == 0);
  }

  ObjectReadOperation op;
  op.set_chunk(
      chunk.start,
      chunk.size,
      chunk_io_ctx,
      chunk.fingerprint,
      0,
      CEPH_OSD_OP_FLAG_WITH_REFERENCE);
  ret = io_ctx.operate(chunk.oid, &op, nullptr);
  oid_for_evict.insert(make_pair(chunk.oid, snap));
  return ret;
}

int make_crawling_daemon(const po::variables_map &opts)
{
  string base_pool_name = get_opts_pool_name(opts);
  string chunk_pool_name = get_opts_chunk_pool(opts);
  unsigned max_thread = get_opts_max_thread(opts);
  uint32_t report_period = get_opts_report_period(opts);

  int sampling_ratio = -1;
  if (opts.count("sampling-ratio")) {
    sampling_ratio = opts["sampling-ratio"].as<int>();
  }
  size_t chunk_size = 8192;
  if (opts.count("chunk-size")) {
    chunk_size = opts["chunk-size"].as<int>();
  } else {
    cout << "8192 is set as chunk size by default" << std::endl;
  }
  bool snap = false;
  if (opts.count("snap")) {
    snap = true;
  }

  uint32_t chunk_dedup_threshold = -1;
  if (opts.count("chunk-dedup-threshold")) {
    chunk_dedup_threshold = opts["chunk-dedup-threshold"].as<int>();
  }

  std::string chunk_algo = get_opts_chunk_algo(opts);

  Rados rados;
  int ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
    cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  ret = rados.connect();
  if (ret) {
    cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  int wakeup_period = 100;
  if (opts.count("wakeup-period")) {
    wakeup_period = opts["wakeup-period"].as<int>();
  } else {
    cout << "100 second is set as wakeup period by default" << std::endl;
  }

  const size_t fp_threshold = opts["fpstore-threshold"].as<size_t>();

  std::string fp_algo = get_opts_fp_algo(opts);

  list<string> pool_names;
  IoCtx io_ctx, chunk_io_ctx;
  pool_names.push_back(base_pool_name);
  ret = rados.ioctx_create(base_pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening base pool "
      << base_pool_name << ": "
      << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }

  ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_io_ctx);
  if (ret < 0) {
    cerr << "error opening chunk pool "
      << chunk_pool_name << ": "
      << cpp_strerror(ret) << std::endl;
    return -EINVAL;
  }
  bufferlist inbl;
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "fingerprint_algorithm", fp_algo),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "dedup_chunk_algorithm", "fastcdc"),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "dedup_cdc_chunk_size", chunk_size),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = rados.mon_command(
      make_pool_str(base_pool_name, "dedup_tier", chunk_pool_name),
      inbl, NULL, NULL);
  if (ret < 0) {
    cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    return ret;
  }

  cout << "SampleRatio : " << sampling_ratio << std::endl
    << "Chunk Dedup Threshold : " << chunk_dedup_threshold << std::endl
    << "Chunk Size : " << chunk_size << std::endl
    << std::endl;

  while (true) {
    lock_guard lock(glock);
    ObjectCursor begin = io_ctx.object_list_begin();
    ObjectCursor end = io_ctx.object_list_end();

    SampleDedupWorkerThread::SampleDedupGlobal sample_dedup_global(
      chunk_dedup_threshold, sampling_ratio, report_period, fp_threshold);

    std::list<SampleDedupWorkerThread> threads;
    size_t total_size = 0;
    size_t total_duplicate_size = 0;
    for (unsigned i = 0; i < max_thread; i++) {
      cout << " add thread.. " << std::endl;
      ObjectCursor shard_start;
      ObjectCursor shard_end;
      io_ctx.object_list_slice(
        begin,
        end,
        i,
        max_thread,
        &shard_start,
        &shard_end);

      threads.emplace_back(
	io_ctx,
	chunk_io_ctx,
	shard_start,
	shard_end,
	chunk_size,
	fp_algo,
	chunk_algo,
	sample_dedup_global,
	snap);
      threads.back().create("sample_dedup");
    }

    for (auto &p : threads) {
      p.join();
      total_size += p.get_total_object_size();
      total_duplicate_size += p.get_total_duplicated_size();
    }

    cerr << "Summary: read "
	 << total_size << " bytes so far and found saveable space ("
	 << total_duplicate_size << " bytes)."
	 << std::endl;

    sleep(wakeup_period);

    map<string, librados::pool_stat_t> stats;
    ret = rados.get_pool_stats(pool_names, stats);
    if (ret < 0) {
      cerr << "error fetching pool stats: " << cpp_strerror(ret) << std::endl;
      return -EINVAL;
    }
    if (stats.find(base_pool_name) == stats.end()) {
      cerr << "stats can not find pool name: " << base_pool_name << std::endl;
      return -EINVAL;
    }
  }

  return 0;
}

static void handle_signal(int signum) 
{
}

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }

  po::variables_map opts;
  po::positional_options_description p;
  p.add("command", 1);
  po::options_description desc = make_usage();
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(desc).positional(p).allow_unregistered().run();
    po::store(parsed, opts);
    po::notify(opts);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
  if (opts.count("help") || opts.count("h")) {
    cout<< desc << std::endl;
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			CODE_ENVIRONMENT_DAEMON,
			CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  Preforker forker;
  if (global_init_prefork(g_ceph_context) >= 0) {
    std::string err;
    int r = forker.prefork(err);
    if (r < 0) {
      cerr << err << std::endl;
      return r;
    }
    if (forker.is_parent()) {
      g_ceph_context->_log->start();
      if (forker.parent_wait(err) != 0) {
        return -ENXIO;
      }
      return 0;
    }
    global_init_postfork_start(g_ceph_context);
  }
  common_init_finish(g_ceph_context);
  global_init_postfork_finish(g_ceph_context);
  forker.daemonize();

  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  int ret = make_crawling_daemon(opts);

  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  
  return forker.signal_exit(ret);
}
