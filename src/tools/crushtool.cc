// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <fstream>

#include "common/debug.h"
#include "common/errno.h"
#include "common/config.h"

#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "osd/OSDMap.h"
#include "crush/CrushWrapper.h"
#include "crush/CrushCompiler.h"
#include "crush/CrushTester.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_crush

using namespace std;

const char *infn = "stdin";

static int get_fd_data(int fd, bufferlist &bl)
{

  uint64_t total = 0;
  do {
    ssize_t bytes = bl.read_fd(fd, 1024*1024);
    if (bytes < 0) {
      cerr << "read_fd error " << cpp_strerror(-bytes) << "\n";
      return -1;
    }

    if (bytes == 0)
      break;

    total += bytes;
  } while(true);

  assert(bl.length() == total);
  return 0;
}

////////////////////////////////////////////////////////////////////////////

void data_analysis_usage()
{
cout << "data output from testing routine ...\n";
cout << "          absolute_weights\n";
cout << "                the decimal weight of each OSD\n";
cout << "                data layout: ROW MAJOR\n";
cout << "                             OSD id (int), weight (int)\n";
cout << "           batch_device_expected_utilization_all\n";
cout << "                 the expected number of objects each OSD should receive per placement batch\n";
cout << "                 which may be a decimal value\n";
cout << "                 data layout: COLUMN MAJOR\n";
cout << "                              round (int), objects expected on OSD 0...OSD n (float)\n";
cout << "           batch_device_utilization_all\n";
cout << "                 the number of objects stored on each OSD during each placement round\n";
cout << "                 data layout: COLUMN MAJOR\n";
cout << "                              round (int), objects stored on OSD 0...OSD n (int)\n";
cout << "           device_utilization_all\n";
cout << "                  the number of objects stored on each OSD at the end of placements\n";
cout << "                  data_layout: ROW MAJOR\n";
cout << "                               OSD id (int), objects stored (int), objects expected (float)\n";
cout << "           device_utilization\n";
cout << "                  the number of objects stored on each OSD marked 'up' at the end of placements\n";
cout << "                  data_layout: ROW MAJOR\n";
cout << "                               OSD id (int), objects stored (int), objects expected (float)\n";
cout << "           placement_information\n";
cout << "                  the map of input -> OSD\n";
cout << "                  data_layout: ROW MAJOR\n";
cout << "                               input (int), OSD's mapped (int)\n";
cout << "           proportional_weights_all\n";
cout << "                  the proportional weight of each OSD specified in the CRUSH map\n";
cout << "                  data_layout: ROW MAJOR\n";
cout << "                               OSD id (int), proportional weight (float)\n";
cout << "           proportional_weights\n";
cout << "                  the proportional weight of each 'up' OSD specified in the CRUSH map\n";
cout << "                  data_layout: ROW MAJOR\n";
cout << "                               OSD id (int), proportional weight (float)\n";
}

void usage()
{
  cout << "usage: crushtool ...\n";
  cout << "   --decompile|-d map    decompile a crush map to source\n";
  cout << "   --tree                print map summary as a tree\n";
  cout << "   --compile|-c map.txt  compile a map from source\n";
  cout << "   [-o outfile [--clobber]]\n";
  cout << "                         specify output for for (de)compilation\n";
  cout << "   --build --num_osds N layer1 ...\n";
  cout << "                         build a new map, where each 'layer' is\n";
  cout << "                           'name (uniform|straw|list|tree) size'\n";
  cout << "   -i mapfn --test       test a range of inputs on the map\n";
  cout << "      [--min-x x] [--max-x x] [--x x]\n";
  cout << "      [--min-rule r] [--max-rule r] [--rule r]\n";
  cout << "      [--num-rep n]\n";
  cout << "      [--batches b]      split the CRUSH mapping into b > 1 rounds\n";
  cout << "      [--weight|-w devno weight]\n";
  cout << "                         where weight is 0 to 1.0\n";
  cout << "      [--simulate]       simulate placements using a random\n";
  cout << "                         number generator in place of the CRUSH\n";
  cout << "                         algorithm\n";
  cout << "   -i mapfn --add-item id weight name [--loc type name ...]\n";
  cout << "                         insert an item into the hierarchy at the\n";
  cout << "                         given location\n";
  cout << "   -i mapfn --update-item id weight name [--loc type name ...]\n";
  cout << "                         insert or move an item into the hierarchy at the\n";
  cout << "                         given location\n";
  cout << "   -i mapfn --remove-item name\n"
       << "                         remove the given item\n";
  cout << "   -i mapfn --reweight-item name weight\n";
  cout << "                         reweight a given item (and adjust ancestor\n"
       << "                         weights as needed)\n";
  cout << "   -i mapfn --reweight   recalculate all bucket weights\n";
  cout << "\n";
  cout << "Options for the display/test stage\n";
  cout << "\n";
  cout << "   --check-names max_id\n";
  cout << "                         check if any item is referencing an unknown name/type\n";
  cout << "   -i mapfn --show-location id\n";
  cout << "                         show location for given device id\n";
  cout << "   --show-utilization    show OSD usage\n";
  cout << "   --show utilization-all\n";
  cout << "                         include zero weight items\n";
  cout << "   --show-statistics     show chi squared statistics\n";
  cout << "   --show-mappings       show mappings\n";
  cout << "   --show-bad-mappings   show bad mappings\n";
  cout << "   --show-choose-tries   show choose tries histogram\n";
  cout << "   --set-choose-local-tries N\n";
  cout << "                         set choose local retries before re-descent\n";
  cout << "   --set-choose-local-fallback-tries N\n";
  cout << "                         set choose local retries using fallback\n";
  cout << "                         permutation before re-descent\n";
  cout << "   --set-choose-total-tries N\n";
  cout << "                         set choose total descent attempts\n";
  cout << "   --set-chooseleaf-descend-once <0|1>\n";
  cout << "                         set chooseleaf to (not) retry the recursive descent\n";
  cout << "   --set-chooseleaf-vary-r <0|1>\n";
  cout << "                         set chooseleaf to (not) vary r based on parent\n";
  cout << "   --output-name name\n";
  cout << "                         prepend the data file(s) generated during the\n";
  cout << "                         testing routine with name\n";
  cout << "   --output-csv\n";
  cout << "                         export select data generated during testing routine\n";
  cout << "                         to CSV files for off-line post-processing\n";
  cout << "                         use --help-output for more information\n";
}

struct bucket_types_t {
  const char *name;
  int type;
} bucket_types[] = {
  { "uniform", CRUSH_BUCKET_UNIFORM },
  { "list", CRUSH_BUCKET_LIST },
  { "straw", CRUSH_BUCKET_STRAW },
  { "straw2", CRUSH_BUCKET_STRAW2 },
  { "tree", CRUSH_BUCKET_TREE },
  { 0, 0 },
};

struct layer_t {
  const char *name;
  const char *buckettype;
  int size;
};

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  const char *me = argv[0];
  std::string infn, srcfn, outfn, add_name, remove_name, reweight_name;
  bool compile = false;
  bool decompile = false;
  bool check_names = false;
  int max_id = -1;
  bool test = false;
  bool display = false;
  bool tree = false;
  int full_location = -1;
  bool write_to_file = false;
  int verbose = 0;
  bool unsafe_tunables = false;

  bool reweight = false;
  int add_item = -1;
  bool update_item = false;
  float add_weight = 0;
  map<string,string> add_loc;
  float reweight_weight = 0;

  bool adjust = false;

  int build = 0;
  int num_osds =0;
  vector<layer_t> layers;

  int choose_local_tries = -1;
  int choose_local_fallback_tries = -1;
  int choose_total_tries = -1;
  int chooseleaf_descend_once = -1;
  int chooseleaf_vary_r = -1;
  int straw_calc_version = -1;
  int allowed_bucket_algs = -1;

  CrushWrapper crush;

  CrushTester tester(crush, cout);

  // we use -c, don't confuse the generic arg parsing
  // only parse arguments from CEPH_ARGS, if in the environment
  vector<const char *> env_args;
  env_to_vec(env_args);
  global_init(NULL, env_args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  int x;
  float y;

  std::string val;
  std::ostringstream err;
  int tmp;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      exit(0);
    } else if (ceph_argparse_witharg(args, i, &val, "-d", "--decompile", (char*)NULL)) {
      infn = val;
      decompile = true;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--infn", (char*)NULL)) {
      infn = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-o", "--outfn", (char*)NULL)) {
      outfn = val;
    } else if (ceph_argparse_flag(args, i, "-v", "--verbose", (char*)NULL)) {
      verbose += 1;
    } else if (ceph_argparse_flag(args, i, "--tree", (char*)NULL)) {
      tree = true;
    } else if (ceph_argparse_flag(args, i, "--show_utilization", (char*)NULL)) {
      display = true;
      tester.set_output_utilization(true);
    } else if (ceph_argparse_flag(args, i, "--show_utilization_all", (char*)NULL)) {
      display = true;
      tester.set_output_utilization_all(true);
    } else if (ceph_argparse_flag(args, i, "--show_statistics", (char*)NULL)) {
      display = true;
      tester.set_output_statistics(true);
    } else if (ceph_argparse_flag(args, i, "--show_mappings", (char*)NULL)) {
      display = true;
      tester.set_output_mappings(true);
    } else if (ceph_argparse_flag(args, i, "--show_bad_mappings", (char*)NULL)) {
      display = true;
      tester.set_output_bad_mappings(true);
    } else if (ceph_argparse_flag(args, i, "--show_choose_tries", (char*)NULL)) {
      display = true;
      tester.set_output_choose_tries(true);
    } else if (ceph_argparse_witharg(args, i, &val, "-c", "--compile", (char*)NULL)) {
      srcfn = val;
      compile = true;
    } else if (ceph_argparse_withint(args, i, &max_id, &err, "--check-names", (char*)NULL)) {
      check_names = true;
    } else if (ceph_argparse_flag(args, i, "-t", "--test", (char*)NULL)) {
      test = true;
    } else if (ceph_argparse_withint(args, i, &full_location, &err, "--show-location", (char*)NULL)) {
    } else if (ceph_argparse_flag(args, i, "-s", "--simulate", (char*)NULL)) {
      tester.set_random_placement();
    } else if (ceph_argparse_flag(args, i, "--enable-unsafe-tunables", (char*)NULL)) {
      unsafe_tunables = true;
    } else if (ceph_argparse_withint(args, i, &choose_local_tries, &err,
				     "--set_choose_local_tries", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_withint(args, i, &choose_local_fallback_tries, &err,
				     "--set_choose_local_fallback_tries", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_withint(args, i, &choose_total_tries, &err,
				     "--set_choose_total_tries", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_withint(args, i, &chooseleaf_descend_once, &err,
				     "--set_chooseleaf_descend_once", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_withint(args, i, &chooseleaf_vary_r, &err,
				     "--set_chooseleaf_vary_r", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_withint(args, i, &straw_calc_version, &err,
				     "--set_straw_calc_version", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_withint(args, i, &allowed_bucket_algs, &err,
				     "--set_allowed_bucket_algs", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_flag(args, i, "--reweight", (char*)NULL)) {
      reweight = true;
    } else if (ceph_argparse_withint(args, i, &add_item, &err, "--add_item", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      if (i == args.end()) {
	cerr << "expecting additional argument to --add-item" << std::endl;
	exit(EXIT_FAILURE);
      }
      add_weight = atof(*i);
      i = args.erase(i);
      if (i == args.end()) {
	cerr << "expecting additional argument to --add-item" << std::endl;
	exit(EXIT_FAILURE);
      }
      add_name.assign(*i);
      i = args.erase(i);
    } else if (ceph_argparse_withint(args, i, &add_item, &err, "--update_item", (char*)NULL)) {
      update_item = true;
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      if (i == args.end()) {
	cerr << "expecting additional argument to --update-item" << std::endl;
	exit(EXIT_FAILURE);
      }
      add_weight = atof(*i);
      i = args.erase(i);
      if (i == args.end()) {
	cerr << "expecting additional argument to --update-item" << std::endl;
	exit(EXIT_FAILURE);
      }
      add_name.assign(*i);
      i = args.erase(i);
    } else if (ceph_argparse_witharg(args, i, &val, "--loc", (char*)NULL)) {
      std::string type(val);
      if (i == args.end()) {
	cerr << "expecting additional argument to --loc" << std::endl;
	exit(EXIT_FAILURE);
      }
      std::string name(*i);
      i = args.erase(i);
      add_loc[type] = name;
    } else if (ceph_argparse_flag(args, i, "--output-csv", (char*)NULL)) {
      write_to_file = true;
      tester.set_output_data_file(true);
      tester.set_output_csv(true);
    } else if (ceph_argparse_flag(args, i, "--help-output", (char*)NULL)) {
      data_analysis_usage();
      exit(0);
    } else if (ceph_argparse_witharg(args, i, &val, "--output-name", (char*)NULL)) {
      std::string name(val);
      if (i == args.end()) {
	cerr << "expecting additional argument to --output-name" << std::endl;
	exit(EXIT_FAILURE);
      }
      else {
        tester.set_output_data_file_name(name + "-");
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--remove_item", (char*)NULL)) {
      remove_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--reweight_item", (char*)NULL)) {
      reweight_name = val;
      if (i == args.end()) {
	cerr << "expecting additional argument to --reweight-item" << std::endl;
	exit(EXIT_FAILURE);
      }
      reweight_weight = atof(*i);
      i = args.erase(i);
    } else if (ceph_argparse_flag(args, i, "--build", (char*)NULL)) {
      build = true;
    } else if (ceph_argparse_withint(args, i, &num_osds, &err, "--num_osds", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
    } else if (ceph_argparse_withint(args, i, &x, &err, "--num_rep", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      tester.set_num_rep(x);
    } else if (ceph_argparse_withint(args, i, &x, &err, "--max_x", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      tester.set_max_x(x);
    } else if (ceph_argparse_withint(args, i, &x, &err, "--min_x", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      tester.set_min_x(x);
    } else if (ceph_argparse_withint(args, i, &x, &err, "--x", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      tester.set_x(x);
    } else if (ceph_argparse_withint(args, i, &x, &err, "--max_rule", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      tester.set_max_rule(x);
    } else if (ceph_argparse_withint(args, i, &x, &err, "--min_rule", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      tester.set_min_rule(x);
    } else if (ceph_argparse_withint(args, i, &x, &err, "--rule", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      tester.set_rule(x);
    } else if (ceph_argparse_withint(args, i, &x, &err, "--batches", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      tester.set_batches(x);
    } else if (ceph_argparse_withfloat(args, i, &y, &err, "--mark-down-ratio", (char*)NULL)) {
      if (!err.str().empty()) {
        cerr << err.str() << std::endl;
        exit(EXIT_FAILURE);
      }
      tester.set_device_down_ratio(y);
    } else if (ceph_argparse_withfloat(args, i, &y, &err, "--mark-down-bucket-ratio", (char*)NULL)) {
      if (!err.str().empty()) {
        cerr << err.str() << std::endl;
        exit(EXIT_FAILURE);
      }
      tester.set_bucket_down_ratio(y);
    } else if (ceph_argparse_withint(args, i, &tmp, &err, "--weight", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      int dev = tmp;
      if (i == args.end()) {
	cerr << "expecting additional argument to --weight" << std::endl;
	exit(EXIT_FAILURE);
      }
      float f = atof(*i);
      i = args.erase(i);
      tester.set_device_weight(dev, f);
    }
    else {
      ++i;
    }
  }

  if (test && !check_name && !display && !write_to_file) {
    cerr << "WARNING: no output selected; use --output-csv or --show-X" << std::endl;
  }

  if (decompile + compile + build > 1) {
    cerr << "cannot specify more than one of compile, decompile, and build" << std::endl;
    exit(EXIT_FAILURE);
  }
  if (!check_names && !compile && !decompile && !build && !test && !reweight && !adjust && !tree &&
      add_item < 0 && full_location < 0 &&
      remove_name.empty() && reweight_name.empty()) {
    cerr << "no action specified; -h for help" << std::endl;
    exit(EXIT_FAILURE);
  }
  if ((!build) && (!args.empty())) {
    cerr << "unrecognized arguments: " << args << std::endl;
    exit(EXIT_FAILURE);
  }
  else {
    if ((args.size() % 3) != 0U) {
      cerr << "remaining args: " << args << std::endl;
      cerr << "layers must be specified with 3-tuples of (name, buckettype, size)"
    	   << std::endl;
      exit(EXIT_FAILURE);
    }
    for (size_t j = 0; j < args.size(); j += 3) {
      layer_t l;
      l.name = args[j];
      l.buckettype = args[j+1];
      l.size = atoi(args[j+2]);
      layers.push_back(l);
    }
  }

  /*
  if (outfn) cout << "outfn " << outfn << std::endl;
  if (cinfn) cout << "cinfn " << cinfn << std::endl;
  if (dinfn) cout << "dinfn " << dinfn << std::endl;
  */

  bool modified = false;

  if (!infn.empty()) {
    bufferlist bl;
    std::string error;

    int r = 0;
    if (infn == "-") {
      if (isatty(STDIN_FILENO)) {
        cerr << "stdin must not be from a tty" << std::endl;
        exit(EXIT_FAILURE);
      }
      r = get_fd_data(STDIN_FILENO, bl);
      if (r < 0) {
        cerr << "error reading data from STDIN" << std::endl;
        exit(EXIT_FAILURE);
      }
    } else {
      r = bl.read_file(infn.c_str(), &error);
      if (r < 0) {
        cerr << me << ": error reading '" << infn << "': " 
             << error << std::endl;
        exit(1);
      }
    }
    bufferlist::iterator p = bl.begin();
    crush.decode(p);
  }

  if (full_location >= 0) {
    map<string, string> loc = crush.get_full_location(full_location);
    for (map<string,string>::iterator p = loc.begin();
	 p != loc.end();
	 ++p) {
      cout << p->first << "\t" << p->second << std::endl;
    }
    exit(0);
  }
  if (decompile) {
    CrushCompiler cc(crush, cerr, verbose);
    if (!outfn.empty()) {
      ofstream o;
      o.open(outfn.c_str(), ios::out | ios::binary | ios::trunc);
      if (!o.is_open()) {
	cerr << me << ": error writing '" << outfn << "'" << std::endl;
	exit(1);
      }
      cc.decompile(o);
      o.close();
    } else {
      cc.decompile(cout);
    }
  }
  if (tree) {
    ostringstream oss;
    crush.dump_tree(&oss, NULL);
    dout(1) << "\n" << oss.str() << dendl;
  }

  if (compile) {
    crush.create();

    // read the file
    ifstream in(srcfn.c_str());
    if (!in.is_open()) {
      cerr << "input file " << srcfn << " not found" << std::endl;
      return -ENOENT;
    }

    CrushCompiler cc(crush, cerr, verbose);
    if (unsafe_tunables)
      cc.enable_unsafe_tunables();
    int r = cc.compile(in, srcfn.c_str());
    if (r < 0) 
      exit(1);

    modified = true;
  }

  if (build) {
    if (layers.empty()) {
      cerr << me << ": must specify at least one layer" << std::endl;
      exit(1);
    }

    crush.create();

    vector<int> lower_items;
    vector<int> lower_weights;

    for (int i=0; i<num_osds; i++) {
      lower_items.push_back(i);
      lower_weights.push_back(0x10000);
    }
    crush.set_max_devices(num_osds);

    int type = 1;
    for (vector<layer_t>::iterator p = layers.begin(); p != layers.end(); ++p, type++) {
      layer_t &l = *p;

      dout(2) << "layer " << type
	      << "  " << l.name
	      << "  bucket type " << l.buckettype
	      << "  " << l.size 
	      << dendl;

      crush.set_type_name(type, l.name);

      int buckettype = -1;
      for (int i = 0; bucket_types[i].name; i++)
	if (l.buckettype && strcmp(l.buckettype, bucket_types[i].name) == 0) {
	  buckettype = bucket_types[i].type;
	  break;
	}
      if (buckettype < 0) {
	cerr << "unknown bucket type '" << l.buckettype << "'" << std::endl << std::endl;
	exit(EXIT_FAILURE);
      }

      // build items
      vector<int> cur_items;
      vector<int> cur_weights;
      unsigned lower_pos = 0;  // lower pos

      dout(2) << "lower_items " << lower_items << dendl;
      dout(2) << "lower_weights " << lower_weights << dendl;

      int i = 0;
      while (1) {
	if (lower_pos == lower_items.size())
	  break;

	int items[num_osds];
	int weights[num_osds];

	int weight = 0;
	int j;
	for (j=0; j<l.size || l.size==0; j++) {
	  if (lower_pos == lower_items.size())
	    break;
	  items[j] = lower_items[lower_pos];
	  weights[j] = lower_weights[lower_pos];
	  weight += weights[j];
	  lower_pos++;
	  dout(2) << "  item " << items[j] << " weight " << weights[j] << dendl;
	}

	int id;
	int r = crush.add_bucket(0, buckettype, CRUSH_HASH_DEFAULT, type, j, items, weights, &id);
	if (r < 0) {
	  dout(2) << "Couldn't add bucket: " << cpp_strerror(r) << dendl;
	}

	char format[20];
	format[sizeof(format)-1] = '\0';
	if (l.size)
	  snprintf(format, sizeof(format)-1, "%s%%d", l.name);
	else
	  strncpy(format, l.name, sizeof(format)-1);
	char name[20];
	snprintf(name, sizeof(name), format, i);
	crush.set_item_name(id, name);

	dout(2) << " in bucket " << id << " '" << name << "' size " << j << " weight " << weight << dendl;

	cur_items.push_back(id);
	cur_weights.push_back(weight);
	i++;
      }

      lower_items.swap(cur_items);
      lower_weights.swap(cur_weights);
    }

    {
      ostringstream oss;
      crush.dump_tree(&oss, NULL);
      dout(1) << "\n" << oss.str() << dendl;
    }

    string root = layers.back().size == 0 ? layers.back().name :
      string(layers.back().name) + "0";

    {
      set<int> roots;
      crush.find_roots(roots);
      if (roots.size() > 1)
	dout(1)	<< "The crush rulesets will use the root " << root << "\n"
		<< "and ignore the others.\n"
		<< "There are " << roots.size() << " roots, they can be\n"
		<< "grouped into a single root by appending something like:\n"
		<< "  root straw 0\n"
		<< dendl;
    }
    
    if (OSDMap::build_simple_crush_rulesets(g_ceph_context, crush, root, &cerr))
      exit(EXIT_FAILURE);

    modified = true;
  }

  if (!reweight_name.empty()) {
    cout << me << " reweighting item " << reweight_name << " to " << reweight_weight << std::endl;
    int r;
    if (!crush.name_exists(reweight_name)) {
      cerr << " name " << reweight_name << " dne" << std::endl;
      r = -ENOENT;
    } else {
      int item = crush.get_item_id(reweight_name);
      r = crush.adjust_item_weightf(g_ceph_context, item, reweight_weight);
    }
    if (r >= 0)
      modified = true;
    else {
      cerr << me << " " << cpp_strerror(r) << std::endl;
      return r;
    }
        
  }
  if (!remove_name.empty()) {
    cout << me << " removing item " << remove_name << std::endl;
    int r;
    if (!crush.name_exists(remove_name)) {
      cerr << " name " << remove_name << " dne" << std::endl;
      r = -ENOENT;
    } else {
      int remove_item = crush.get_item_id(remove_name);
      r = crush.remove_item(g_ceph_context, remove_item, false);
    }
    if (r == 0)
      modified = true;
    else {
      cerr << me << " " << cpp_strerror(r) << std::endl;
      return r;
    }
  }
  if (add_item >= 0) {
    int r;
    if (update_item) {
      r = crush.update_item(g_ceph_context, add_item, add_weight, add_name.c_str(), add_loc);
    } else {
      r = crush.insert_item(g_ceph_context, add_item, add_weight, add_name.c_str(), add_loc);
    }
    if (r >= 0) {
      modified = true;
    } else {
      cerr << me << " " << cpp_strerror(r) << std::endl;
      return r;
    }
  }
  if (reweight) {
    crush.reweight(g_ceph_context);
    modified = true;
  }

  if (choose_local_tries >= 0) {
    crush.set_choose_local_tries(choose_local_tries);
    modified = true;
  }
  if (choose_local_fallback_tries >= 0) {
    crush.set_choose_local_fallback_tries(choose_local_fallback_tries);
    modified = true;
  }
  if (choose_total_tries >= 0) {
    crush.set_choose_total_tries(choose_total_tries);
    modified = true;
  }
  if (chooseleaf_descend_once >= 0) {
    crush.set_chooseleaf_descend_once(chooseleaf_descend_once);
    modified = true;
  }
  if (chooseleaf_vary_r >= 0) {
    crush.set_chooseleaf_vary_r(chooseleaf_vary_r);
    modified = true;
  }
  if (straw_calc_version >= 0) {
    crush.set_straw_calc_version(straw_calc_version);
    modified = true;
  }
  if (allowed_bucket_algs >= 0) {
    crush.set_allowed_bucket_algs(allowed_bucket_algs);
    modified = true;
  }

 if (modified) {
   crush.finalize();

    if (outfn.empty()) {
      cout << me << " successfully built or modified map.  Use '-o <file>' to write it out." << std::endl;
    } else {
      bufferlist bl;
      crush.encode(bl);
      int r = bl.write_file(outfn.c_str());
      if (r < 0) {
	cerr << me << ": error writing '" << outfn << "': " << cpp_strerror(r) << std::endl;
	exit(1);
      }
      if (verbose)
	cout << "wrote crush map to " << outfn << std::endl;
    }
  }

  if (check_names) {
    if (!tester.check_name_maps(max_id)) {
      exit(1);
    }
  }

  if (test) {
    if (tester.get_output_utilization_all() ||
	tester.get_output_utilization())
      tester.set_output_statistics(true);

    int r = tester.test();
    if (r < 0)
      exit(1);
  }

  return 0;
}
/*
 * Local Variables:
 * compile-command: "cd .. ; make crushtool && test/run-cli-tests"
 * End:
 */
