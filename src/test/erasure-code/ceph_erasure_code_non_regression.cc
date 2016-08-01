// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Red Hat (C) 2014, 2015 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <errno.h>
#include <stdlib.h>
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/algorithm/string.hpp>

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "erasure-code/ErasureCodePlugin.h"

namespace po = boost::program_options;
using namespace std;

class ErasureCodeNonRegression {
  unsigned stripe_width;
  string plugin;
  bool create;
  bool check;
  bool show_path;
  string base;
  string directory;
  ErasureCodeProfile profile;
public:
  int setup(int argc, char** argv);
  int run();
  int run_show_path();
  int run_create();
  int run_check();
  int decode_erasures(ErasureCodeInterfaceRef erasure_code,
		      set<int> erasures,
		      map<int,bufferlist> chunks);
  string content_path();
  string chunk_path(unsigned int chunk);
};

int ErasureCodeNonRegression::setup(int argc, char** argv) {

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("stripe-width,s", po::value<int>()->default_value(4 * 1024),
     "stripe_width, i.e. the size of the buffer to be encoded")
    ("plugin,p", po::value<string>()->default_value("jerasure"),
     "erasure code plugin name")
    ("base", po::value<string>()->default_value("."),
     "prefix all paths with base")
    ("parameter,P", po::value<vector<string> >(),
     "add a parameter to the erasure code profile")
    ("path", po::value<string>(), "content path instead of inferring it from parameters")
    ("show-path", "display the content path and exit")
    ("create", "create the erasure coded content in the directory")
    ("check", "check the content in the directory matches the chunks and vice versa")
    ;

  po::variables_map vm;
  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
  po::store(
    parsed,
    vm);
  po::notify(vm);

  vector<const char *> ceph_options, def_args;
  vector<string> ceph_option_strings = po::collect_unrecognized(
    parsed.options, po::include_positional);
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_CLIENT,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);
  const char* env = getenv("CEPH_LIB");
  std::string libs_dir(env ? env : ".libs");
  g_conf->set_val("erasure_code_dir", libs_dir, false, false);

  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  stripe_width = vm["stripe-width"].as<int>();
  plugin = vm["plugin"].as<string>();
  base = vm["base"].as<string>();
  check = vm.count("check") > 0;
  create = vm.count("create") > 0;
  show_path = vm.count("show-path") > 0;

  if (!check && !create && !show_path) {
    cerr << "must specifify either --check, --create or --show-path" << endl;
    return 1;
  }

  {
    stringstream path;
    path << base << "/" << "plugin=" << plugin << " stripe-width=" << stripe_width;
    directory = path.str();
  }

  if (vm.count("parameter")) {
    const vector<string> &p = vm["parameter"].as< vector<string> >();
    for (vector<string>::const_iterator i = p.begin();
	 i != p.end();
	 ++i) {
      std::vector<std::string> strs;
      boost::split(strs, *i, boost::is_any_of("="));
      if (strs.size() != 2) {
	cerr << "--parameter " << *i << " ignored because it does not contain exactly one =" << endl;
      } else {
	profile[strs[0]] = strs[1];
      }
      directory += " " + *i;
    }
  }

  if (vm.count("path"))
    directory = vm["path"].as<string>();

  return 0;
}

int ErasureCodeNonRegression::run()
  {
  int ret = 0;
  if(create && (ret = run_create()))
    return ret;
  if(check && (ret = run_check()))
    return ret;
  if(show_path && (ret = run_show_path()))
    return ret;
  return ret;
}

int ErasureCodeNonRegression::run_show_path()
{
  cout << directory << endl;
  return 0;
}

int ErasureCodeNonRegression::run_create()
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeInterfaceRef erasure_code;
  stringstream messages;
  int code = instance.factory(plugin,
			      g_conf->erasure_code_dir,
			      profile, &erasure_code, &messages);
  if (code) {
    cerr << messages.str() << endl;
    return code;
  }

  if (::mkdir(directory.c_str(), 0755)) {
    cerr << "mkdir(" << directory << "): " << cpp_strerror(errno) << endl;
    return 1;
  }
  unsigned payload_chunk_size = 37;
  string payload;
  for (unsigned j = 0; j < payload_chunk_size; ++j)
    payload.push_back('a' + (rand() % 26));
  bufferlist in;
  for (unsigned j = 0; j < stripe_width; j += payload_chunk_size)
    in.append(payload);
  if (stripe_width < in.length())
    in.splice(stripe_width, in.length() - stripe_width);
  if (in.write_file(content_path().c_str()))
    return 1;
  set<int> want_to_encode;
  for (unsigned int i = 0; i < erasure_code->get_chunk_count(); i++) {
    want_to_encode.insert(i);
  }
  map<int,bufferlist> encoded;
  code = erasure_code->encode(want_to_encode, in, &encoded);
  if (code)
    return code;
  for (map<int,bufferlist>::iterator chunk = encoded.begin();
       chunk != encoded.end();
       ++chunk) {
    if (chunk->second.write_file(chunk_path(chunk->first).c_str()))
      return 1;
  }
  return 0;
}

int ErasureCodeNonRegression::decode_erasures(ErasureCodeInterfaceRef erasure_code,
					      set<int> erasures,
					      map<int,bufferlist> chunks)
{
  map<int,bufferlist> available;
  for (map<int,bufferlist>::iterator chunk = chunks.begin();
       chunk != chunks.end();
       ++chunk) {
    if (erasures.count(chunk->first) == 0)
      available[chunk->first] = chunk->second;
      
  }
  map<int,bufferlist> decoded;
  int code = erasure_code->decode(erasures, available, &decoded);
  if (code)
    return code;
  for (set<int>::iterator erasure = erasures.begin();
       erasure != erasures.end();
       ++erasure) {
    if (!chunks[*erasure].contents_equal(decoded[*erasure])) {
      cerr << "chunk " << *erasure << " incorrectly recovered" << endl;
      return 1;
    }
  }
  return 0;
}

int ErasureCodeNonRegression::run_check()
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeInterfaceRef erasure_code;
  stringstream messages;
  int code = instance.factory(plugin,
			      g_conf->erasure_code_dir,
			      profile, &erasure_code, &messages);
  if (code) {
    cerr << messages.str() << endl;
    return code;
  }
  string errors;
  bufferlist in;
  if (in.read_file(content_path().c_str(), &errors)) {
    cerr << errors << endl;
    return 1;
  }
  set<int> want_to_encode;
  for (unsigned int i = 0; i < erasure_code->get_chunk_count(); i++) {
    want_to_encode.insert(i);
  }

  map<int,bufferlist> encoded;
  code = erasure_code->encode(want_to_encode, in, &encoded);
  if (code)
    return code;

  for (map<int,bufferlist>::iterator chunk = encoded.begin();
       chunk != encoded.end();
       ++chunk) {
    bufferlist existing;
    if (existing.read_file(chunk_path(chunk->first).c_str(), &errors)) {
      cerr << errors << endl;
      return 1;
    }
    bufferlist &old = chunk->second;
    if (existing.length() != old.length() ||
	memcmp(existing.c_str(), old.c_str(), old.length())) {
      cerr << "chunk " << chunk->first << " encodes differently" << endl;
      return 1;
    }
  }

  // erasing a single chunk is likely to use a specific code path in every plugin
  set<int> erasures;
  erasures.clear();
  erasures.insert(0);
  code = decode_erasures(erasure_code, erasures, encoded);
  if (code)
    return code;

  if (erasure_code->get_chunk_count() - erasure_code->get_data_chunk_count() > 1) {
    // erasing two chunks is likely to be the general case
    erasures.clear();
    erasures.insert(0);
    erasures.insert(erasure_code->get_chunk_count() - 1);
    code = decode_erasures(erasure_code, erasures, encoded);
    if (code)
      return code;
  }
  
  return 0;
}

string ErasureCodeNonRegression::content_path()
{
  stringstream path;
  path << directory << "/content";
  return path.str();
}

string ErasureCodeNonRegression::chunk_path(unsigned int chunk)
{
  stringstream path;
  path << directory << "/" << chunk;
  return path.str();
}

int main(int argc, char** argv) {
  ErasureCodeNonRegression non_regression;
  int err = non_regression.setup(argc, argv);
  if (err)
    return err;
  return non_regression.run();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make ceph_erasure_code_non_regression &&
 *   libtool --mode=execute valgrind --tool=memcheck --leak-check=full \
 *      ./ceph_erasure_code_non_regression \
 *      --plugin jerasure \
 *      --parameter technique=reed_sol_van \
 *      --parameter k=2 \
 *      --parameter m=2 \
 *      --directory /tmp/ceph_erasure_code_non_regression \
 *      --stripe-width 3181 \
 *      --create \
 *      --check
 * "
 * End:
 */
