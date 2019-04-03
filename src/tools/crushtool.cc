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
#include <type_traits>

#include "common/debug.h"
#include "common/errno.h"
#include "common/config.h"
#include "common/Formatter.h"

#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "osd/OSDMap.h"
#include "crush/CrushWrapper.h"
#include "crush/CrushCompiler.h"
#include "crush/CrushTester.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_crush


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

  ceph_assert(bl.length() == total);
  return 0;
}

////////////////////////////////////////////////////////////////////////////

void data_analysis_usage()
{
cout << "data output from testing routine ...\n";
cout << "           absolute_weights\n";
cout << "                  the decimal weight of each OSD\n";
cout << "                  data layout: ROW MAJOR\n";
cout << "                               OSD id (int), weight (int)\n";
cout << "           batch_device_expected_utilization_all\n";
cout << "                  the expected number of objects each OSD should receive per placement batch\n";
cout << "                  which may be a decimal value\n";
cout << "                  data layout: COLUMN MAJOR\n";
cout << "                               round (int), objects expected on OSD 0...OSD n (float)\n";
cout << "           batch_device_utilization_all\n";
cout << "                  the number of objects stored on each OSD during each placement round\n";
cout << "                  data layout: COLUMN MAJOR\n";
cout << "                               round (int), objects stored on OSD 0...OSD n (int)\n";
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
  cout << "\n";
  cout << "Display, modify and test a crush map\n";
  cout << "\n";
  cout << "There are five stages, running one after the other:\n";
  cout << "\n";
  cout << " - input/build\n";
  cout << " - tunables adjustments\n";
  cout << " - modifications\n";
  cout << " - display/test\n";
  cout << " - output\n";
  cout << "\n";
  cout << "Options that are not specific to a stage.\n";
  cout << "\n";
  cout << "   [--infn|-i infile]\n";
  cout << "                         read the crush map from infile\n";
  cout << "\n";
  cout << "Options for the input/build stage\n";
  cout << "\n";
  cout << "   --decompile|-d map    decompile a crush map to source\n";
  cout << "   [--outfn|-o outfile]\n";
  cout << "                         specify output for for (de)compilation\n";
  cout << "   --compile|-c map.txt  compile a map from source\n";
  cout << "   --enable-unsafe-tunables\n";
  cout << "                         compile with unsafe tunables\n";
  cout << "   --build --num_osds N layer1 ...\n";
  cout << "                         build a new map, where each 'layer' is\n";
  cout << "                         'name (uniform|straw2|straw|list|tree) size'\n";
  cout << "\n";
  cout << "Options for the tunables adjustments stage\n";
  cout << "\n";
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
  cout << "   --set-chooseleaf-stable <0|1>\n";
  cout << "                         set chooseleaf firstn to (not) return stable results\n";
  cout << "\n";
  cout << "Options for the modifications stage\n";
  cout << "\n";
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
  cout << "   -i mapfn --add-bucket name type [--loc type name ...]\n"
       << "                         insert a bucket into the hierarchy at the given\n"
       << "                         location\n";
  cout << "   -i mapfn --move       name --loc type name ...\n"
       << "                         move the given item to specified location\n";
  cout << "   -i mapfn --reweight   recalculate all bucket weights\n";
  cout << "   -i mapfn --rebuild-class-roots\n";
  cout << "                         rebuild the per-class shadow trees (normally a no-op)\n";
  cout << "   -i mapfn --create-simple-rule name root type mode\n"
       << "                         create crush rule <name> to start from <root>,\n"
       << "                         replicate across buckets of type <type>, using\n"
       << "                         a choose mode of <firstn|indep>\n";
  cout << "   -i mapfn --create-replicated-rule name root type\n"
       << "                         create crush rule <name> to start from <root>,\n"
       << "                         replicate across buckets of type <type>\n";
  cout << "   --device-class <class>\n";
  cout << "                         use device class <class> for new rule\n";
  cout << "   -i mapfn --remove-rule name\n"
       << "                         remove the specified crush rule\n";
  cout << "\n";
  cout << "Options for the display/test stage\n";
  cout << "\n";
  cout << "   -f --format           the format of --dump, defaults to json-pretty\n";
  cout << "                         can be one of json, json-pretty, xml, xml-pretty,\n";
  cout << "                         table, table-kv, html, html-pretty\n";
  cout << "   --dump                dump the crush map\n";
  cout << "   --tree                print map summary as a tree\n";
  cout << "   --check [max_id]      check if any item is referencing an unknown name/type\n";
  cout << "   -i mapfn --show-location id\n";
  cout << "                         show location for given device id\n";
  cout << "   -i mapfn --test       test a range of inputs on the map\n";
  cout << "      [--min-x x] [--max-x x] [--x x]\n";
  cout << "      [--min-rule r] [--max-rule r] [--rule r] [--ruleset rs]\n";
  cout << "      [--num-rep n]\n";
  cout << "      [--pool-id n]      specifies pool id\n";
  cout << "      [--batches b]      split the CRUSH mapping into b > 1 rounds\n";
  cout << "      [--weight|-w devno weight]\n";
  cout << "                         where weight is 0 to 1.0\n";
  cout << "      [--simulate]       simulate placements using a random\n";
  cout << "                         number generator in place of the CRUSH\n";
  cout << "                         algorithm\n";
  cout << "   --show-utilization    show OSD usage\n";
  cout << "   --show-utilization-all\n";
  cout << "                         include zero weight items\n";
  cout << "   --show-statistics     show chi squared statistics\n";
  cout << "   --show-mappings       show mappings\n";
  cout << "   --show-bad-mappings   show bad mappings\n";
  cout << "   --show-choose-tries   show choose tries histogram\n";
  cout << "   --output-name name\n";
  cout << "                         prepend the data file(s) generated during the\n";
  cout << "                         testing routine with name\n";
  cout << "   --output-csv\n";
  cout << "                         export select data generated during testing routine\n";
  cout << "                         to CSV files for off-line post-processing\n";
  cout << "                         use --help-output for more information\n";
  cout << "   --reclassify          transform legacy CRUSH map buckets and rules\n";
  cout << "                         by adding classes\n";
  cout << "      --reclassify-bucket <bucket-match> <class> <default-parent>\n";
  cout << "      --reclassify-root <bucket-name> <class>\n";
  cout << "   --set-subtree-class <bucket-name> <class>\n";
  cout << "                         set class for all items beneath bucket-name\n";
  cout << "   --compare <otherfile> compare two maps using --test parameters\n";
  cout << "\n";
  cout << "Options for the output stage\n";
  cout << "\n";
  cout << "   [--outfn|-o outfile]\n";
  cout << "                         specify output for modified crush map\n";
  cout << "\n";
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

template<typename... Args>
bool argparse_withargs(std::vector<const char*> &args,
		       std::vector<const char*>::iterator& i,
		       std::ostream& oss,
		       const char* opt,
		       Args*... opts)
{
  if (!ceph_argparse_flag(args, i, opt, nullptr)) {
    return false;
  }
  auto parse = [&](auto& opt) {
    if (i == args.end()) {
      oss << "expecting additional argument to " << opt;
      return false;
    }
    using opt_t = std::remove_pointer_t<decay_t<decltype(opt)>>;
    string err;
    if constexpr (std::is_same_v<opt_t, string>) {
      opt->assign(*i);
    } else if constexpr (is_same_v<opt_t, int>) {
      *opt = strict_strtol(*i, 10, &err);
    } else if constexpr (is_same_v<opt_t, float>) {
      *opt = strict_strtof(*i, &err);
    }
    i = args.erase(i);
    if (err.empty())
      return true;
    else {
      oss << err;
      return false;
    }
  };
  (... && parse(opts));
  return true;
}

int do_add_bucket(CephContext* cct,
		  const char* me,
		  CrushWrapper& crush,
		  const string& add_name,
		  const string& add_type,
		  const map<string,string>& add_loc) {
  int bucketno;
  if (crush.name_exists(add_name)) {
    cerr << me << " bucket '" << add_name << "' already exists" << std::endl;
    return -EEXIST;
  }
  int type = crush.get_type_id(add_type);
  if (type <= 0) {
    cerr << me << " bad bucket type: " << add_type << std::endl;
    return -EINVAL;
  }
  if (int r = crush.add_bucket(0, 0, CRUSH_HASH_DEFAULT, type, 0, nullptr, nullptr, &bucketno);
      r < 0) {
    cerr << me << " unable to add bucket: " << cpp_strerror(r) << std::endl;
    return r;
  }
  if (int r = crush.set_item_name(bucketno, add_name); r < 0) {
    cerr << me << " bad bucket name: " << add_name << std::endl;
    return r;
  }
  if (!add_loc.empty()) {
    if (!crush.check_item_loc(cct, bucketno, add_loc, (int*)nullptr)) {
      if (int r = crush.move_bucket(cct, bucketno, add_loc); r < 0) {
	cerr << me << " error moving bucket '" << add_name << "' to " << add_loc << std::endl;
	return r;
      }
    }
  }
  return 0;
}

// return 1 for no change, 0 for successful change, negative on error
int do_move_item(CephContext* cct,
		 const char *me,
		 CrushWrapper& crush,
		 const string& name,
		 const map<string,string>& loc)
{
  if (!crush.name_exists(name)) {
    cerr << me << " item '" << name << "' does not exist" << std::endl;
    return -ENOENT;
  }
  int id = crush.get_item_id(name);
  if (loc.empty()) {
    cerr << me << " expecting additional --loc argument to --move" << std::endl;
    return -EINVAL;
  }
  if (crush.check_item_loc(cct, id, loc, (int*)nullptr)) {
    // it's already there
    cerr << me << " item '" << name << "' already at " << loc << std::endl;
    return 1;
  }
  if (id >= 0) {
    switch (int r = crush.create_or_move_item(cct, id, 0, name, loc)) {
    case 0:
      return 1;
    case 1:
      return 0;
    default:
      return r;
    }
  } else {
    return crush.move_bucket(cct, id, loc);
  }
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  const char *me = argv[0];
  std::string infn, srcfn, outfn, add_name, add_type, remove_name, reweight_name;
  std::string move_name;
  bool compile = false;
  bool decompile = false;
  bool check = false;
  int max_id = -1;
  bool test = false;
  bool display = false;
  bool tree = false;
  string dump_format = "json-pretty";
  bool dump = false;
  int full_location = -1;
  bool write_to_file = false;
  int verbose = 0;
  bool unsafe_tunables = false;

  bool rebuild_class_roots = false;

  bool reweight = false;
  int add_item = -1;
  bool add_bucket = false;
  bool update_item = false;
  bool move_item = false;
  bool add_rule = false;
  std::string rule_name, rule_root, rule_type, rule_mode, rule_device_class;
  bool del_rule = false;
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
  int chooseleaf_stable = -1;
  int straw_calc_version = -1;
  int allowed_bucket_algs = -1;

  bool reclassify = false;
  map<string,pair<string,string>> reclassify_bucket; // %suffix or prefix% -> class, default_root
  map<string,string> reclassify_root;        // bucket -> class
  map<string,string> set_subtree_class;     // bucket -> class

  string compare;

  CrushWrapper crush;

  CrushTester tester(crush, cout);

  // we use -c, don't confuse the generic arg parsing
  // only parse arguments from CEPH_ARGS, if in the environment
  vector<const char *> empty_args;
  auto cct = global_init(NULL, empty_args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  // crushtool times out occasionally when quits. so do not
  // release the g_ceph_context.
  cct->get();
  common_init_finish(g_ceph_context);

  int x;
  float y;
  long long z;

  std::string val;
  std::ostringstream err;
  int tmp;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "-d", "--decompile", (char*)NULL)) {
      infn = val;
      decompile = true;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--infn", (char*)NULL)) {
      infn = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-o", "--outfn", (char*)NULL)) {
      outfn = val;
    } else if (ceph_argparse_flag(args, i, "-v", "--verbose", (char*)NULL)) {
      verbose += 1;
    } else if (ceph_argparse_witharg(args, i, &val, "--compare", (char*)NULL)) {
      compare = val;
    } else if (ceph_argparse_flag(args, i, "--reclassify", (char*)NULL)) {
      reclassify = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--reclassify-bucket",
				     (char*)NULL)) {
      if (i == args.end()) {
	cerr << "expecting additional argument" << std::endl;
	return EXIT_FAILURE;
      }
      string c = *i;
      i = args.erase(i);
      if (i == args.end()) {
	cerr << "expecting additional argument" << std::endl;
	return EXIT_FAILURE;
      }
      reclassify_bucket[val] = make_pair(c, *i);
      i = args.erase(i);
    } else if (ceph_argparse_witharg(args, i, &val, "--reclassify-root",
				     (char*)NULL)) {
      if (i == args.end()) {
	cerr << "expecting additional argument" << std::endl;
	return EXIT_FAILURE;
      }
      reclassify_root[val] = *i;
      i = args.erase(i);
    } else if (ceph_argparse_witharg(args, i, &val, "--set-subtree-class",
				     (char*)NULL)) {
      if (i == args.end()) {
	cerr << "expecting additional argument" << std::endl;
	return EXIT_FAILURE;
      }
      set_subtree_class[val] = *i;
      i = args.erase(i);
    } else if (ceph_argparse_flag(args, i, "--tree", (char*)NULL)) {
      tree = true;
    } else if (ceph_argparse_witharg(args, i, &val, "-f", "--format", (char*)NULL)) {
      dump_format = val;
    } else if (ceph_argparse_flag(args, i, "--dump", (char*)NULL)) {
      dump = true;
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
    } else if (ceph_argparse_witharg(args, i, &max_id, err, "--check", (char*)NULL)) {
      check = true;
    } else if (ceph_argparse_flag(args, i, "-t", "--test", (char*)NULL)) {
      test = true;
    } else if (ceph_argparse_witharg(args, i, &full_location, err, "--show-location", (char*)NULL)) {
    } else if (ceph_argparse_flag(args, i, "-s", "--simulate", (char*)NULL)) {
      tester.set_random_placement();
    } else if (ceph_argparse_flag(args, i, "--enable-unsafe-tunables", (char*)NULL)) {
      unsafe_tunables = true;
    } else if (ceph_argparse_witharg(args, i, &choose_local_tries, err,
				     "--set_choose_local_tries", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_witharg(args, i, &choose_local_fallback_tries, err,
				     "--set_choose_local_fallback_tries", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_witharg(args, i, &choose_total_tries, err,
				     "--set_choose_total_tries", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_witharg(args, i, &chooseleaf_descend_once, err,
				     "--set_chooseleaf_descend_once", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_witharg(args, i, &chooseleaf_vary_r, err,
				     "--set_chooseleaf_vary_r", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_witharg(args, i, &chooseleaf_stable, err,
				     "--set_chooseleaf_stable", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_witharg(args, i, &straw_calc_version, err,
				     "--set_straw_calc_version", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_witharg(args, i, &allowed_bucket_algs, err,
				     "--set_allowed_bucket_algs", (char*)NULL)) {
      adjust = true;
    } else if (ceph_argparse_flag(args, i, "--reweight", (char*)NULL)) {
      reweight = true;
    } else if (ceph_argparse_flag(args, i, "--rebuild-class-roots", (char*)NULL)) {
      rebuild_class_roots = true;
    } else if (ceph_argparse_witharg(args, i, &add_item, err, "--add_item", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      if (i == args.end()) {
	cerr << "expecting additional argument to --add-item" << std::endl;
	return EXIT_FAILURE;
      }
      add_weight = atof(*i);
      i = args.erase(i);
      if (i == args.end()) {
	cerr << "expecting additional argument to --add-item" << std::endl;
	return EXIT_FAILURE;
      }
      add_name.assign(*i);
      i = args.erase(i);
    } else if (ceph_argparse_witharg(args, i, &add_item, err, "--update_item", (char*)NULL)) {
      update_item = true;
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      if (i == args.end()) {
	cerr << "expecting additional argument to --update-item" << std::endl;
	return EXIT_FAILURE;
      }
      add_weight = atof(*i);
      i = args.erase(i);
      if (i == args.end()) {
	cerr << "expecting additional argument to --update-item" << std::endl;
	return EXIT_FAILURE;
      }
      add_name.assign(*i);
      i = args.erase(i);
    } else if (argparse_withargs(args, i, err, "--add-bucket",
				 &add_name, &add_type)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      add_bucket = true;
    } else if (argparse_withargs(args, i, err, "--move",
				 &move_name)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      move_item = true;
    } else if (ceph_argparse_witharg(args, i, &val, err, "--create-simple-rule", (char*)NULL)) {
      rule_name.assign(val);
      if (!err.str().empty()) {
        cerr << err.str() << std::endl;
        return EXIT_FAILURE;
      }
      if (i == args.end()) {
        cerr << "expecting additional argument to --create-simple-rule" << std::endl;
        return EXIT_FAILURE;
      }

      rule_root.assign(*i);
      i = args.erase(i);
      if (i == args.end()) {
        cerr << "expecting additional argument to --create-simple-rule" << std::endl;
        return EXIT_FAILURE;
      }

      rule_type.assign(*i);
      i = args.erase(i);
      if (i == args.end()) {
        cerr << "expecting additional argument to --create-simple-rule" << std::endl;
        return EXIT_FAILURE;
      }

      rule_mode.assign(*i);
      i = args.erase(i);

      cout << "--create-simple-rule:"
           << " name=" << rule_name
           << " root=" << rule_root
           << " type=" << rule_type
           << " mode=" << rule_mode
           << std::endl;
      add_rule = true;
    } else if (ceph_argparse_witharg(args, i, &val, err, "--create-replicated-rule", (char*)NULL)) {
      rule_name.assign(val);
      if (!err.str().empty()) {
        cerr << err.str() << std::endl;
        return EXIT_FAILURE;
      }
      if (i == args.end()) {
        cerr << "expecting additional argument to --create-replicated-rule" << std::endl;
        return EXIT_FAILURE;
      }

      rule_root.assign(*i);
      i = args.erase(i);
      if (i == args.end()) {
        cerr << "expecting additional argument to --create-replicated-rule" << std::endl;
        return EXIT_FAILURE;
      }

      rule_type.assign(*i);
      i = args.erase(i);
      rule_mode = "firstn";

      cout << "--create-replicated-rule:"
           << " name=" << rule_name
           << " root=" << rule_root
           << " type=" << rule_type
           << std::endl;
      add_rule = true;

    } else if (ceph_argparse_witharg(args, i, &val, "--device-class", (char*)NULL)) {
      rule_device_class.assign(val);
      if (!err.str().empty()) {
        cerr << err.str() << std::endl;
        return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--remove-rule", (char*)NULL)) {
      rule_name.assign(val);
      if (!err.str().empty()) {
        cerr << err.str() << std::endl;
        return EXIT_FAILURE;
      }
      del_rule = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--loc", (char*)NULL)) {
      std::string type(val);
      if (i == args.end()) {
	cerr << "expecting additional argument to --loc" << std::endl;
	return EXIT_FAILURE;
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
      return EXIT_SUCCESS;
    } else if (ceph_argparse_witharg(args, i, &val, "--output-name", (char*)NULL)) {
      std::string name(val);
      if (i == args.end()) {
	cerr << "expecting additional argument to --output-name" << std::endl;
	return EXIT_FAILURE;
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
	return EXIT_FAILURE;
      }
      reweight_weight = atof(*i);
      i = args.erase(i);
    } else if (ceph_argparse_flag(args, i, "--build", (char*)NULL)) {
      build = true;
    } else if (ceph_argparse_witharg(args, i, &num_osds, err, "--num_osds", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
    } else if (ceph_argparse_witharg(args, i, &x, err, "--num_rep", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      tester.set_num_rep(x);
    } else if (ceph_argparse_witharg(args, i, &x, err, "--max_x", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      tester.set_max_x(x);
    } else if (ceph_argparse_witharg(args, i, &x, err, "--min_x", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      tester.set_min_x(x);
    } else if (ceph_argparse_witharg(args, i, &z, err, "--pool_id", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      tester.set_pool_id(z);
    } else if (ceph_argparse_witharg(args, i, &x, err, "--x", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      tester.set_x(x);
    } else if (ceph_argparse_witharg(args, i, &x, err, "--max_rule", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      tester.set_max_rule(x);
    } else if (ceph_argparse_witharg(args, i, &x, err, "--min_rule", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      tester.set_min_rule(x);
    } else if (ceph_argparse_witharg(args, i, &x, err, "--rule", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      tester.set_rule(x);
    } else if (ceph_argparse_witharg(args, i, &x, err, "--ruleset", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      tester.set_ruleset(x);
    } else if (ceph_argparse_witharg(args, i, &x, err, "--batches", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      tester.set_batches(x);
    } else if (ceph_argparse_witharg(args, i, &y, err, "--mark-down-ratio", (char*)NULL)) {
      if (!err.str().empty()) {
        cerr << err.str() << std::endl;
        return EXIT_FAILURE;
      }
      tester.set_device_down_ratio(y);
    } else if (ceph_argparse_witharg(args, i, &y, err, "--mark-down-bucket-ratio", (char*)NULL)) {
      if (!err.str().empty()) {
        cerr << err.str() << std::endl;
        return EXIT_FAILURE;
      }
      tester.set_bucket_down_ratio(y);
    } else if (ceph_argparse_witharg(args, i, &tmp, err, "--weight", (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return EXIT_FAILURE;
      }
      int dev = tmp;
      if (i == args.end()) {
	cerr << "expecting additional argument to --weight" << std::endl;
	return EXIT_FAILURE;
      }
      float f = atof(*i);
      i = args.erase(i);
      tester.set_device_weight(dev, f);
    }
    else {
      ++i;
    }
  }

  if (test && !check && !display && !write_to_file && compare.empty()) {
    cerr << "WARNING: no output selected; use --output-csv or --show-X" << std::endl;
  }

  if (decompile + compile + build > 1) {
    cerr << "cannot specify more than one of compile, decompile, and build" << std::endl;
    return EXIT_FAILURE;
  }
  if (!check && !compile && !decompile && !build && !test && !reweight && !adjust && !tree && !dump &&
      add_item < 0 && !add_bucket && !move_item && !add_rule && !del_rule && full_location < 0 &&
      !reclassify && !rebuild_class_roots &&
      compare.empty() &&
      remove_name.empty() && reweight_name.empty()) {
    cerr << "no action specified; -h for help" << std::endl;
    return EXIT_FAILURE;
  }
  if ((!build) && (!args.empty())) {
    cerr << "unrecognized arguments: " << args << std::endl;
    return EXIT_FAILURE;
  }
  else {
    if ((args.size() % 3) != 0U) {
      cerr << "remaining args: " << args << std::endl;
      cerr << "layers must be specified with 3-tuples of (name, buckettype, size)"
    	   << std::endl;
      return EXIT_FAILURE;
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

  // input ----

  if (!infn.empty()) {
    bufferlist bl;
    std::string error;

    int r = 0;
    if (infn == "-") {
      if (isatty(STDIN_FILENO)) {
        cerr << "stdin must not be from a tty" << std::endl;
        return EXIT_FAILURE;
      }
      r = get_fd_data(STDIN_FILENO, bl);
      if (r < 0) {
        cerr << "error reading data from STDIN" << std::endl;
        return EXIT_FAILURE;
      }
    } else {
      r = bl.read_file(infn.c_str(), &error);
      if (r < 0) {
        cerr << me << ": error reading '" << infn << "': " 
             << error << std::endl;
        return EXIT_FAILURE;
      }
    }
    auto p = bl.cbegin();
    try {
      crush.decode(p);
    } catch(...) {
      cerr << me << ": unable to decode " << infn << std::endl;
      return EXIT_FAILURE;
    }
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
      return EXIT_FAILURE;

    modified = true;
  }

  if (build) {
    if (layers.empty()) {
      cerr << me << ": must specify at least one layer" << std::endl;
      return EXIT_FAILURE;
    }

    crush.create();

    vector<int> lower_items;
    vector<int> lower_weights;

    crush.set_max_devices(num_osds);
    for (int i=0; i<num_osds; i++) {
      lower_items.push_back(i);
      lower_weights.push_back(0x10000);
      crush.set_item_name(i, "osd." + stringify(i));
    }

    crush.set_type_name(0, "osd");
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
	cerr << "unknown bucket type '" << l.buckettype << "'" << std::endl;
	return EXIT_FAILURE;
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
          cerr << " Couldn't add bucket: " << cpp_strerror(r) << std::endl;
          return r;
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

    string root = layers.back().size == 0 ? layers.back().name :
      string(layers.back().name) + "0";

    {
      set<int> roots;
      crush.find_roots(&roots);
      if (roots.size() > 1) {
	cerr << "The crush rulesets will use the root " << root << "\n"
	     << "and ignore the others.\n"
	     << "There are " << roots.size() << " roots, they can be\n"
	     << "grouped into a single root by appending something like:\n"
	     << "  root straw 0\n"
	     << std::endl;
      }
    }
    
    if (OSDMap::build_simple_crush_rules(g_ceph_context, crush, root, &cerr))
      return EXIT_FAILURE;

    modified = true;
  }

  // mutate ----

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
  if (chooseleaf_stable >= 0) {
    crush.set_chooseleaf_stable(chooseleaf_stable);
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

  if (add_bucket) {
    if (int r = do_add_bucket(cct.get(), me, crush, add_name, add_type, add_loc); !r) {
      modified = true;
    } else {
      return r;
    }
  }

  if (move_item) {
    if (int r = do_move_item(cct.get(), me, crush, move_name, add_loc); !r) {
      modified = true;
    } else {
      return r;
    }
  }
  if (add_rule) {
    if (crush.rule_exists(rule_name)) {
      cerr << "rule " << rule_name << " already exists" << std::endl;
      return EXIT_FAILURE;
    }
    int r = crush.add_simple_rule(rule_name, rule_root, rule_type,
				  rule_device_class,
				  rule_mode, pg_pool_t::TYPE_REPLICATED, &err);
    if (r < 0) {
      cerr << err.str() << std::endl;
      return EXIT_FAILURE;
    }
    modified = true;
  }

  if (del_rule) {
    if (!crush.rule_exists(rule_name)) {
      cerr << "rule " << rule_name << " does not exist" << std::endl;
      return 0;
    }
    int ruleno = crush.get_rule_id(rule_name);
    ceph_assert(ruleno >= 0);
    int r = crush.remove_rule(ruleno);
    if (r < 0) {
      cerr << "fail to remove rule " << rule_name << std::endl;
      return EXIT_FAILURE;
    }
    modified = true;
  }

  if (reweight) {
    crush.reweight(g_ceph_context);
    modified = true;
  }
  if (rebuild_class_roots) {
    int r = crush.rebuild_roots_with_classes(g_ceph_context);
    if (r < 0) {
      cerr << "failed to rebuidl roots with classes" << std::endl;
      return EXIT_FAILURE;
    }
    modified = true;
  }

  for (auto& i : set_subtree_class) {
    crush.set_subtree_class(i.first, i.second);
    modified = true;
  }
  if (reclassify) {
    int r = crush.reclassify(
      g_ceph_context,
      cout,
      reclassify_root,
      reclassify_bucket);
    if (r < 0) {
      cerr << "failed to reclassify map" << std::endl;
      return EXIT_FAILURE;
    }
    modified = true;
  }

  // display ---
  if (full_location >= 0) {
    map<string, string> loc = crush.get_full_location(full_location);
    for (map<string,string>::iterator p = loc.begin();
	 p != loc.end();
	 ++p) {
      cout << p->first << "\t" << p->second << std::endl;
    }
  }

  if (tree) {
    crush.dump_tree(&cout, NULL, {}, true);
  }

  if (dump) {
    boost::scoped_ptr<Formatter> f(Formatter::create(dump_format, "json-pretty", "json-pretty"));
    f->open_object_section("crush_map");
    crush.dump(f.get());
    f->close_section();
    f->flush(cout);
    cout << "\n";
  }

  if (decompile) {
    CrushCompiler cc(crush, cerr, verbose);
    if (!outfn.empty()) {
      ofstream o;
      o.open(outfn.c_str(), ios::out | ios::binary | ios::trunc);
      if (!o.is_open()) {
	cerr << me << ": error writing '" << outfn << "'" << std::endl;
	return EXIT_FAILURE;
      }
      cc.decompile(o);
      o.close();
    } else {
      cc.decompile(cout);
    }
  }

  if (check) {
    tester.check_overlapped_rules();
    if (max_id >= 0) {
      if (!tester.check_name_maps(max_id)) {
	return EXIT_FAILURE;
      }
    }
  }

  if (test) {
    if (tester.get_output_utilization_all() ||
	tester.get_output_utilization())
      tester.set_output_statistics(true);

    int r = tester.test();
    if (r < 0)
      return EXIT_FAILURE;
  }

  if (compare.size()) {
    CrushWrapper crush2;
    bufferlist in;
    string error;
    int r = in.read_file(compare.c_str(), &error);
    if (r < 0) {
      cerr << me << ": error reading '" << compare << "': "
	   << error << std::endl;
      return EXIT_FAILURE;
    }
    auto p = in.cbegin();
    try {
      crush2.decode(p);
    } catch(...) {
      cerr << me << ": unable to decode " << compare << std::endl;
      return EXIT_FAILURE;
    }
    r = tester.compare(crush2);
    if (r < 0)
      return EXIT_FAILURE;
  }

  // output ---
  if (modified) {
    crush.finalize();

    if (outfn.empty()) {
      cout << me << " successfully built or modified map.  Use '-o <file>' to write it out." << std::endl;
    } else {
      bufferlist bl;
      crush.encode(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);
      int r = bl.write_file(outfn.c_str());
      if (r < 0) {
	cerr << me << ": error writing '" << outfn << "': " << cpp_strerror(r) << std::endl;
	return EXIT_FAILURE;
      }
      if (verbose)
	cout << "wrote crush map to " << outfn << std::endl;
    }
  }

  return 0;
}
/*
 * Local Variables:
 * compile-command: "cd .. ; make crushtool && test/run-cli-tests"
 * End:
 */
