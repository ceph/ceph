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

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

using namespace std;
namespace po = boost::program_options;
using namespace librados;

constexpr unsigned default_op_size = 1 << 26;

string get_opts_pool_name(const po::variables_map &opts);
string get_opts_chunk_algo(const po::variables_map &opts);
string get_opts_fp_algo(const po::variables_map &opts);
string get_opts_op_name(const po::variables_map &opts);
string get_opts_chunk_pool(const po::variables_map &opts);
string get_opts_object_name(const po::variables_map &opts);
int get_opts_max_thread(const po::variables_map &opts);
int get_opts_report_period(const po::variables_map &opts);
string make_pool_str(string pool, string var, string val);
string make_pool_str(string pool, string var, int val);
