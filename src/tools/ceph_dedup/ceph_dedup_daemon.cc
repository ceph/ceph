#include "common.h"

#undef dout_prefix
#define dout_prefix *_dout << "ceph_dedup_daemon: " \
                           << __func__ << ": "

ceph::mutex glock = ceph::make_mutex("glock");
class SampleDedupWorkerThread;
std::list<SampleDedupWorkerThread> threads;
bool all_stop = false;

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
    ("pool", po::value<std::string>(), ": set pool name")
    ("no-snap", ": do not deduplciate snapshotted object")
    ("chunk-dedup-threshold", po::value<int>(), ": set the threshold for chunk dedup (number of duplication) ")
    ("sampling-ratio", po::value<int>(), ": set the sampling ratio (percentile)")
    ("wakeup-period", po::value<int>(), ": set the wakeup period of crawler thread (sec)")
    ("fpstore-threshold", po::value<size_t>()->default_value(100_M), ": set max size of in-memory fingerprint store (bytes)")
    ("conf,c", po::value<std::string>(), "read configuration from the given configuration file")
    ("id,i", po::value<std::string>(), "set ID portion of my name")
    ("name,n", po::value<std::string>(), "set name")
    ("cluster", po::value<std::string>(), "set cluster name (default: ceph)")
    ("setuser", po::value<std::string>(), "set uid to user or uid (and gid to user's gid)")
    ("setgroup", po::value<std::string>(), "set gid to group or gid")
    ("version", "show version and quit")
    (",d", "run in foreground, log to stderr")
    (",f", "run in foreground, log to usual location")
    ("debug_ms", po::value<std::string>(), "set message debug level (e.g. 1)")
    ("run-once", ": do a single iteration for debug")
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
    snap_t snap;
    uint64_t ver;
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
	dout(5) << (int)(now - start) << "s : read "
	     << total_bytes << " bytes so far..."
	     << dendl;
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
    size_t total_bytes = 0;
    const size_t memory_threshold;
    FpMap<std::string, dup_count_t> fp_map;
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
    SampleDedupGlobal &sample_dedup_global,
    bool snap, 
    string checkpoint_oid,
    bool primary,
    ceph_dedup_options &d_opts) :
    chunk_io_ctx(chunk_io_ctx),
    sample_dedup_global(sample_dedup_global),
    d_opts(d_opts),
    begin(begin),
    end(end),
    snap(snap),
    checkpoint_oid(checkpoint_oid),
    primary(primary) {
      this->io_ctx.dup(io_ctx);
    }

  ~SampleDedupWorkerThread() { };

  size_t get_total_duplicated_size() const {
    return total_duplicated_size;
  }

  size_t get_total_deduped_size() const {
    return total_deduped_size;
  }

  void signal(int signum) {
    std::lock_guard l{m_lock};

    switch (signum) {
    case SIGINT:
    case SIGTERM:
      dout(0) << "got a signal(" << signum << "), daemon wil be terminiated" << dendl;
      stop = true;
      break;

    default:
      ceph_abort_msgf("unexpected signal %d", signum);
    }
  }

  void store_checkpoint_info(std::list<SampleDedupWorkerThread> &threads);
  

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
  list<SampleDedupWorkerThread::chunk_t> scan_duplicate_chunks(ObjectItem &object, uint64_t ver, snap_t snap = 0);
  bufferlist read_object(ObjectItem &object);
  std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> do_cdc(
    ObjectItem &object,
    bufferlist &data);
  std::string generate_fingerprint(bufferlist chunk_data);
  void do_dedup(list<SampleDedupWorkerThread::chunk_t> &redundant_chunks);

  IoCtx io_ctx;
  IoCtx chunk_io_ctx;
  size_t total_duplicated_size = 0;
  size_t total_deduped_size = 0;

  SampleDedupGlobal &sample_dedup_global;
  struct ceph_dedup_options &d_opts;
  ObjectCursor begin;
  ObjectCursor end;
  bool snap;
  bool stop = false;
  ceph::mutex m_lock = ceph::make_mutex("SampleDedupWorkerThread");
  string checkpoint_oid;
  bool primary = false;
};

void SampleDedupWorkerThread::store_checkpoint_info(
  std::list<SampleDedupWorkerThread> &threads) {
  std::lock_guard l{glock};
  int id = 0;
  for (auto &p : threads) {
    std::unique_lock l{p.m_lock};
    d_opts.set_checkpoint_info(id, p.checkpoint_oid);
    id++;
  }
  d_opts.store_dedup_conf(io_ctx);
}

void SampleDedupWorkerThread::crawl()
{
  ObjectCursor current_object = begin;
  // find a checkpoint object
  if (checkpoint_oid != string()) {
    librados::NObjectIterator it = io_ctx.nobjects_begin();
    for (;it != io_ctx.nobjects_end(); ++it) {
      librados::ObjectCursor cursor = it.get_cursor();
      if (it->get_oid() == checkpoint_oid) {
       current_object = cursor;
       break;
      }
    }
    if (it != io_ctx.nobjects_end() && ++it == io_ctx.nobjects_end()) {
      current_object = io_ctx.nobjects_begin().get_cursor();
    }
  }

  while (!stop && current_object < end) {
    std::vector<ObjectItem> objects;
    // Get the list of object IDs to deduplicate
    std::tie(objects, current_object) = get_objects(current_object, end, 100);
    // Pick few objects to be processed. Sampling ratio decides how many
    // objects to pick. Lower sampling ratio makes crawler have lower crawling
    // overhead but find less duplication.
    auto sampled_indexes = sample_object(objects.size());
    list<chunk_t> redundant_chunks;
    for (size_t index : sampled_indexes) {
      ObjectItem target = objects[index];
      auto get_ver_with_operation = 
	[this](ObjectReadOperation &op, string &oid) -> std::optional<uint64_t> {
	AioCompRef comp(librados::Rados::aio_create_completion());
	int r = io_ctx.aio_operate(
	  oid,
	  comp.get(),
	  &op,
	  NULL);
	if (r < 0) {
	  return std::nullopt;
	}
	comp->wait_for_complete();
	return comp->get_version64();
      };
      if (snap) {
	io_ctx.snap_set_read(librados::SNAP_DIR);
	snap_set_t snap_set;
	int snap_ret;
	ObjectReadOperation op;
	op.list_snaps(&snap_set, &snap_ret);
	auto v = get_ver_with_operation(op, target.oid);
	if (!v) {
	  derr << "list snap error" << dendl;
	  continue;
	}

	for (vector<librados::clone_info_t>::const_iterator r = snap_set.clones.begin();
	  r != snap_set.clones.end();
	  ++r) {
	  io_ctx.snap_set_read(r->cloneid);
	  auto ret = scan_duplicate_chunks(target, *v, r->cloneid);
	  redundant_chunks.splice(redundant_chunks.end(), ret);
	}
      } else {
	ObjectReadOperation op;
	op.stat(nullptr, nullptr, nullptr);
	auto v = get_ver_with_operation(op, target.oid);
	if (!v) {
	  derr << "stat error" << dendl;
	  continue;
	}
	auto ret = scan_duplicate_chunks(target, *v);
	redundant_chunks.splice(redundant_chunks.end(), ret);
      }
      std::unique_lock l{m_lock};
      if (stop) {
	redundant_chunks.clear();
	break;
      }
    }
    do_dedup(redundant_chunks);
    if (primary) {
      store_checkpoint_info(threads);
    }
    dout(20) << "total redundant chunk size found so far: " << total_duplicated_size
      << ", total size deduplicated: " <<  total_deduped_size << dendl;
  }
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
    derr << "error object_list" << dendl;
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

list<SampleDedupWorkerThread::chunk_t> 
SampleDedupWorkerThread::scan_duplicate_chunks(ObjectItem &object,
  uint64_t ver, snap_t snap)
{
  bufferlist data = read_object(object);
  list<chunk_t> redundant_chunks;
  if (data.length() == 0) {
    derr << __func__ << " skip object " << object.oid
	 << " read returned size 0" << dendl;
    return redundant_chunks;
  }
  auto chunks = do_cdc(object, data);
  size_t chunk_total_amount = 0;

  // First, check total size of created chunks
  for (auto &chunk : chunks) {
    auto &chunk_data = std::get<0>(chunk);
    chunk_total_amount += chunk_data.length();
  }
  if (chunk_total_amount != data.length()) {
    derr << __func__ << " sum of chunked length(" << chunk_total_amount
	 << ") is different from object data length(" << data.length() << ")"
	 << dendl;
    return redundant_chunks;
  }

  for (auto &chunk : chunks) {
    auto &chunk_data = std::get<0>(chunk);
    std::string fingerprint = generate_fingerprint(chunk_data);
    std::pair<uint64_t, uint64_t> chunk_boundary = std::get<1>(chunk);
    chunk_t chunk_info = {
      .oid = object.oid,
      .start = chunk_boundary.first,
      .size = chunk_boundary.second,
      .fingerprint = fingerprint,
      .data = chunk_data,
      .snap = snap,
      .ver = ver
      };

    dout(20) << "generate a chunk (chunk oid: " << chunk_info.oid << ", offset: " 
      << chunk_info.start << ", length: " << chunk_info.size << ", fingerprint: "
      << chunk_info.fingerprint << dendl;

    if (sample_dedup_global.fp_store.add(chunk_info)) {
      redundant_chunks.push_back(chunk_info);
      total_duplicated_size += chunk_info.data.length();
      dout(20) << chunk_info.fingerprint << "is duplicated, try to perform dedup" << dendl;
    }
  }
  return redundant_chunks;
}

void SampleDedupWorkerThread::do_dedup(list<chunk_t> &redundant_chunks)
{
  vector<std::pair<chunk_t, AioCompRef>> wait_evict_comp;
  
  auto do_set_chunk = [this](chunk_t &chunk) {
    uint64_t size;
    time_t mtime;
    bool remove_if_fail = false;
    int ret = chunk_io_ctx.stat(chunk.fingerprint, &size, &mtime);
    if (ret == -ENOENT) {
      bufferlist bl;
      bl.append(chunk.data);
      ObjectWriteOperation wop;
      wop.write_full(bl);
      ret = chunk_io_ctx.operate(chunk.fingerprint, &wop);
      remove_if_fail = true;
    } 

    if (ret < 0) {
      return ret;
    }

    ObjectReadOperation op;
    op.assert_version(chunk.ver);
    op.set_chunk(
      chunk.start,
      chunk.size,
      chunk_io_ctx,
      chunk.fingerprint,
      0,
      CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    ret = io_ctx.operate(chunk.oid, &op, nullptr);
    if (ret < 0 && remove_if_fail) {
      ObjectWriteOperation wop;
      wop.remove();
      ret = chunk_io_ctx.operate(chunk.fingerprint, &wop);
      if (ret < 0) {
	derr << " error occurs. " << chunk.fingerprint << " in " 
	  << d_opts.get_chunk_pool_name() << " must be deleted." << dendl;
      }
    }
    return ret;
  };

  auto do_async_evict = [this](string oid) {
    Rados rados;
    ObjectReadOperation op;
    AioCompRef completion(rados.aio_create_completion());
    op.tier_evict();
    int ret = io_ctx.aio_operate(
      oid,
      completion.get(),
      &op,
      NULL);
    if (ret < 0) {
      return AioCompRef();
    }
    return completion;
  };

  for (auto &p : redundant_chunks) {
    // As to each of the duplicate chunks, set_chunk should be done synchronously first.
    // Then, send tier_evict asynchronously 
    if (snap) {
      io_ctx.snap_set_read(p.snap);
    }
    int ret = do_set_chunk(p);
    if (ret >= 0) {
      auto comp = do_async_evict(p.oid);
      if (comp) {
	wait_evict_comp.push_back(std::make_pair(p, std::move(comp)));
      }
    } else {
      dout(5) << " set_chunk returns: " << ret << dendl;
    }
    std::unique_lock l{m_lock};
    if (stop) {
      wait_evict_comp.clear();
      break;
    }
  }

  for (auto &comp : wait_evict_comp) {
    assert(comp.second);
    comp.second->wait_for_complete();
    total_deduped_size += comp.first.size;
    std::unique_lock l{m_lock};
    checkpoint_oid = comp.first.oid;
  }
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
      derr << "read object error " << object.oid << " offset " << offset
        << " size " << default_op_size << " error(" << cpp_strerror(ret)
        << dendl;
      bufferlist empty_buf;
      return empty_buf;
    }
    offset += ret;
    whole_data.claim_append(partial_data);
  }
  dout(20) << " got object: " << object.oid << " size: " << whole_data.length() << dendl;
  return whole_data;
}

std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> SampleDedupWorkerThread::do_cdc(
  ObjectItem &object,
  bufferlist &data)
{
  std::vector<std::tuple<bufferlist, pair<uint64_t, uint64_t>>> ret;

  unique_ptr<CDC> cdc = CDC::create(d_opts.get_chunk_algo(),
    cbits(d_opts.get_chunk_size()) - 1);
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

  switch (pg_pool_t::get_fingerprint_from_str(d_opts.get_fp_algo())) {
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

int make_crawling_daemon(const po::variables_map &opts)
{
  CephContext* _cct = g_ceph_context;
  struct ceph_dedup_options d_opts;
  bool run_once = false; // for debug

  d_opts.set_conf(POOL, get_opts_pool_name(opts));

  Rados rados;
  int ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
    derr << "couldn't initialize rados: " << cpp_strerror(ret) << dendl;
    return -EINVAL;
  }
  ret = rados.connect();
  if (ret) {
    derr << "couldn't connect to cluster: " << cpp_strerror(ret) << dendl;
    return -EINVAL;
  }

  list<string> pool_names;
  IoCtx io_ctx, chunk_io_ctx;
  pool_names.push_back(d_opts.get_base_pool_name());
  ret = rados.ioctx_create(d_opts.get_base_pool_name().c_str(), io_ctx);
  if (ret < 0) {
    derr << "error opening base pool "
      << d_opts.get_base_pool_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -EINVAL;
  }

  bool config_done = d_opts.load_dedup_conf_from_pool(io_ctx);
  
  if (!config_done) {
    d_opts.load_dedup_conf_by_default(_cct);
    d_opts.store_dedup_conf(io_ctx);
  }
  d_opts.load_dedup_conf_from_argument(opts);

  bool snap = true;
  if (opts.count("no-snap")) {
    snap = false;
  }

  ret = rados.ioctx_create(d_opts.get_chunk_pool_name().c_str(), chunk_io_ctx);
  if (ret < 0) {
    derr << "error opening chunk pool "
      << d_opts.get_chunk_pool_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -EINVAL;
  }

  if (opts.count("run-once")) {
    run_once = true;
  }

  dout(0) << d_opts << dendl;

  while (!all_stop) {
    ObjectCursor begin = io_ctx.object_list_begin();
    ObjectCursor end = io_ctx.object_list_end();

    SampleDedupWorkerThread::SampleDedupGlobal sample_dedup_global(
      d_opts.get_chunk_dedup_threshold(), d_opts.get_sampling_ratio(),
      d_opts.get_report_period(), d_opts.get_fp_threshold());
    size_t total_deduped_size = 0;
    size_t total_duplicate_size = 0;
    {
      lock_guard lock(glock);
      for (int i = 0; i < d_opts.get_max_thread(); i++) {
	dout(15) << " spawn thread " << i << dendl;
	ObjectCursor shard_start;
	ObjectCursor shard_end;
	io_ctx.object_list_slice(
	  begin,
	  end,
	  i,
	  d_opts.get_max_thread(),
	  &shard_start,
	  &shard_end);

	threads.emplace_back(
	  io_ctx,
	  chunk_io_ctx,
	  shard_start,
	  shard_end,
	  sample_dedup_global,
	  snap,
	  run_once ? string() : d_opts.load_checkpoint_info(i),
	  i == 0 ? true : false,
	  d_opts);
	threads.back().create("sample_dedup");
      }
    }

    for (auto &p : threads) {
      p.join();
      total_deduped_size += p.get_total_deduped_size();
      total_duplicate_size += p.get_total_duplicated_size();
    }

    sample_dedup_global.fp_store.maybe_print_status();
    dout(5) << "Summary: deduped "
	 << total_deduped_size << " bytes so far and found saveable space ("
	 << total_duplicate_size << " bytes)."
	 << dendl;

    {
      lock_guard lock(glock);
      threads.clear();
      if (run_once) {
	all_stop = true;
	break;
      }
    }
    sleep(d_opts.get_wakeup_period());

    map<string, librados::pool_stat_t> stats;
    ret = rados.get_pool_stats(pool_names, stats);
    if (ret < 0) {
      derr << "error fetching pool stats: " << cpp_strerror(ret) << dendl;
      return -EINVAL;
    }
    if (stats.find(d_opts.get_base_pool_name()) == stats.end()) {
      derr << "stats can not find pool name: " << d_opts.get_base_pool_name() << dendl;
      return -EINVAL;
    }
  }

  dout(0) << "done" << dendl;
  return 0;
}

static void handle_signal(int signum) 
{
  std::lock_guard l{glock};
  all_stop = true;
  for (auto &p : threads) {
    p.signal(signum);
  }
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
  if (g_conf()->daemonize) {
    global_init_postfork_finish(g_ceph_context);
    forker.daemonize();
  }

  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  int ret = make_crawling_daemon(opts);

  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  
  return forker.signal_exit(ret);
}
