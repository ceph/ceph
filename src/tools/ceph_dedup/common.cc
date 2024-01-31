#include "common.h"

string get_opts_pool_name(const po::variables_map &opts) {
  if (opts.count("pool")) {
    return opts["pool"].as<string>();
  } 
  return string();
}

string get_opts_op_name(const po::variables_map &opts) {
  if (opts.count("op")) {
    return opts["op"].as<string>();
  } else {
    cerr << "must specify op" << std::endl;
    exit(1);
  }
}

string get_opts_object_name(const po::variables_map &opts) {
  if (opts.count("object")) {
    return opts["object"].as<string>();
  } else {
    cerr << "must specify object" << std::endl;
    exit(1);
  }
}

string make_pool_str(string pool, string var, string val)
{
  return string("{\"prefix\": \"osd pool set\",\"pool\":\"") + pool
    + string("\",\"var\": \"") + var + string("\",\"val\": \"")
    + val + string("\"}");
}

string make_pool_str(string pool, string var, int val)
{
  return make_pool_str(pool, var, stringify(val));
}

void ceph_dedup_options::store_dedup_conf(IoCtx &base_pool)
{
  bufferlist bl;
  ::encode(f, bl);
  assert(bl.length() <= CEPH_DEDUP_DAEMON_OBJ_SIZE);
  int ret = base_pool.write(CEPH_DEDUP_DAEMON_OBJ, bl, bl.length(), 0);
  assert(ret >= 0);
}

bool ceph_dedup_options::load_dedup_conf_from_pool(IoCtx &base_pool) 
{
  bufferlist bl;
  int ret = base_pool.read(CEPH_DEDUP_DAEMON_OBJ, bl, CEPH_DEDUP_DAEMON_OBJ_SIZE, 0);
  if (ret < 0) {
    return false;
  }

  auto iter = bl.cbegin();
  try {
    ::decode(f, iter);
  } catch (buffer::error& err) {
    assert(0 == "Failed to decode object");
  }

  return true;
}

void ceph_dedup_options::load_dedup_conf_from_argument(const po::variables_map &opts)
{
  if (opts.count("max-thread")) {
    int max_thread = opts["max-thread"].as<int>();
    set_conf(MAX_THREAD, to_string(max_thread));
  }
  if (opts.count("chunk-pool")) {
    string chunk_pool_name = opts["chunk-pool"].as<string>();
    set_conf(CHUNK_POOL, chunk_pool_name);
  }
  if (opts.count("sampling-ratio")) {
    int sampling_ratio = opts["sampling-ratio"].as<int>();
    set_conf(SAMPLE_RATIO, to_string(sampling_ratio));
  }
  if (opts.count("chunk-size")) {
    int chunk_size = opts["chunk-size"].as<int>();
    set_conf(CHUNK_SIZE, to_string(chunk_size));
  }
  if (opts.count("chunk-dedup-threshold")) {
    int chunk_dedup_threshold = opts["chunk-dedup-threshold"].as<int>();
    set_conf(CHUNK_DEDUP_THRESHOLD, to_string(chunk_dedup_threshold));
  }
  if (opts.count("chunk-algorithm")) {
    string chunk_algo = opts["chunk-algorithm"].as<string>();
    set_conf(CHUNK_ALGO, chunk_algo);
    if (!CDC::create(chunk_algo, 12)) {
      cerr << "unrecognized chunk-algorithm " << chunk_algo << std::endl;
      exit(1);
    }
  }
  if (opts.count("fingerprint-algorithm")) {
    string fp_algo = opts["fingerprint-algorithm"].as<string>();
    set_conf(FINGERPRINT_ALGO, fp_algo);
    if (fp_algo != "sha1"
       && fp_algo != "sha256" && fp_algo != "sha512") {
      cerr << "unrecognized fingerprint-algorithm " << fp_algo << std::endl;
      exit(1);
    }
  }
  if (opts.count("wakeup-period")) {
    int wakeup_period = opts["wakeup-period"].as<int>();
    set_conf(WAKEUP_PERIOD, to_string(wakeup_period));
  }
  if (opts.count("report-period")) {
    int report_period = opts["report-period"].as<int>();
    set_conf(REPORT_PERIOD, to_string(report_period));
  }
  if (opts.count("fpstore-threshold")) {
    size_t fp_threshold = opts["fpstore-threshold"].as<size_t>();
    set_conf(FP_MEM_THRESHOLD, to_string(fp_threshold));
  }
}

void ceph_dedup_options::load_dedup_conf_by_default(CephContext *_cct)
{
  assert(_cct);
  set_conf(MAX_THREAD, to_string(
    _cct->_conf.get_val<int64_t>(MAX_THREAD)));
  set_conf(SAMPLE_RATIO, to_string(
    _cct->_conf.get_val<int64_t>(SAMPLE_RATIO)));
  set_conf(CHUNK_SIZE, to_string(
    _cct->_conf.get_val<int64_t>(CHUNK_SIZE)));
  set_conf(CHUNK_DEDUP_THRESHOLD, to_string(
    _cct->_conf.get_val<int64_t>(CHUNK_DEDUP_THRESHOLD)));
  set_conf(CHUNK_ALGO,
    _cct->_conf.get_val<string>(CHUNK_ALGO));
  set_conf(FINGERPRINT_ALGO,
    _cct->_conf.get_val<string>(FINGERPRINT_ALGO));
  set_conf(WAKEUP_PERIOD, to_string(
    _cct->_conf.get_val<int64_t>(WAKEUP_PERIOD)));
  set_conf(REPORT_PERIOD, to_string(
    _cct->_conf.get_val<int64_t>(REPORT_PERIOD)));
  set_conf(FP_MEM_THRESHOLD, to_string(
    _cct->_conf.get_val<int64_t>(FP_MEM_THRESHOLD)));
}

string ceph_dedup_options::load_checkpoint_info(int id)
{
  return (string)f[THREAD_SECTION][string(
	  THREAD_CHECKPOINT_PREFIX + to_string(id))];
}

void ceph_dedup_options::set_checkpoint_info(int id, string oid) {
  set_dedup_conf(THREAD_SECTION, string(
    THREAD_CHECKPOINT_PREFIX + to_string(id)), oid);
}

std::ostream &operator<<(std::ostream &out, struct ceph_dedup_options &d_opts)
{
  JSONFormatter formatter;
  encode_json("", d_opts.f, &formatter);
  formatter.flush(out);
  return out;
}
