#pragma once

#include "include/types.h"

#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.hpp"

#include "acconfig.h"

#include "common/Cond.h"
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/obj_bencher.h"
#include "global/global_init.h"

#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <errno.h>
#include <dirent.h>
#include <stdexcept>
#include <climits>
#include <locale>
#include <memory>
#include <math.h>

#include "tools/RadosDump.h"
#include "cls/cas/cls_cas_client.h"
#include "cls/cas/cls_cas_internal.h"
#include "include/stringify.h"
#include "global/signal_handler.h"
#include "common/CDC.h"
#include "common/Preforker.h"
#include "common/ceph_json.h"

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

using namespace std;
namespace po = boost::program_options;
using namespace librados;

constexpr unsigned default_op_size = 1 << 26;

// dedup configuration
#define SAMPLE_RATIO "sampling_ratio"
#define CHUNK_SIZE "chunk_size"
#define CHUNK_DEDUP_THRESHOLD "chunk_dedup_threshold"
#define POOL "pool"
#define CHUNK_POOL "chunk_pool"
#define CHUNK_ALGO "chunk_algorithm"
#define FINGERPRINT_ALGO "fingerprint_algorithm"
#define MAX_THREAD "max_thread"
#define WAKEUP_PERIOD "wakeup_period"
#define REPORT_PERIOD "report_period"
#define FP_MEM_THRESHOLD "fpstore_threshold"

// dedup object
#define CEPH_DEDUP_DAEMON_OBJ "ceph_dedup_daemon_obj"
#define CEPH_DEDUP_DAEMON_OBJ_SIZE 4096

struct ceph_dedup_options {
public:

  // load/store conf
  void store_dedup_conf(IoCtx &base_pool);
  bool load_dedup_conf_from_pool(IoCtx &base_pool);
  void load_dedup_conf_by_default(CephContext *_cct);
  void load_dedup_conf_from_argument(const po::variables_map &opts);

  // get/set conf
  void set_conf(string key, string value) {
    set_dedup_conf("", key, value);
  }
  void set_dedup_conf(string section, string key, string val) {
    f.set(section, "{ \""+ key + "\" : \"" + val + "\"}");
  }

#define GET_OPERATION(name, type, key) \
  type get_##name() const { \
    return (type)f[key]; \
  } 
  GET_OPERATION(sampling_ratio, int, SAMPLE_RATIO);
  GET_OPERATION(chunk_size, int, CHUNK_SIZE);
  GET_OPERATION(chunk_dedup_threshold, int, CHUNK_DEDUP_THRESHOLD);
  GET_OPERATION(base_pool_name, string, POOL);
  GET_OPERATION(chunk_pool_name, string, CHUNK_POOL);
  GET_OPERATION(fp_algo, string, FINGERPRINT_ALGO);
  GET_OPERATION(chunk_algo, string, CHUNK_ALGO);
  GET_OPERATION(max_thread, int, MAX_THREAD);
  GET_OPERATION(wakeup_period, int, WAKEUP_PERIOD);
  GET_OPERATION(report_period, int, REPORT_PERIOD);
  GET_OPERATION(fp_threshold, int, FP_MEM_THRESHOLD);

  friend std::ostream &operator<<(std::ostream &out, struct ceph_dedup_options &d_opts);
private:
  JSONFormattable f;
};

std::ostream &operator<<(std::ostream &out, struct ceph_dedup_options &opt);

string get_opts_pool_name(const po::variables_map &opts);
string get_opts_op_name(const po::variables_map &opts);
string get_opts_object_name(const po::variables_map &opts);
string make_pool_str(string pool, string var, string val);
string make_pool_str(string pool, string var, int val);
