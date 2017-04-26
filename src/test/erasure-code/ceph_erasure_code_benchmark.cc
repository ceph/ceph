// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

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
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/Clock.h"
#include "include/utime.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "erasure-code/ErasureCode.h"
#include "ceph_erasure_code_benchmark.h"

namespace po = boost::program_options;

int ErasureCodeBench::setup(int argc, char** argv) {

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("verbose,v", "explain what happens")
    ("size,s", po::value<int>()->default_value(1024 * 1024),
     "size of the buffer to be encoded")
    ("iterations,i", po::value<int>()->default_value(1),
     "number of encode/decode runs")
    ("plugin,p", po::value<string>()->default_value("jerasure"),
     "erasure code plugin name")
    ("workload,w", po::value<string>()->default_value("encode"),
     "run either encode or decode")
    ("erasures,e", po::value<int>()->default_value(1),
     "number of erasures when decoding")
    ("erased", po::value<vector<int> >(),
     "erased chunk (repeat if more than one chunk is erased)")
    ("erasures-generation,E", po::value<string>()->default_value("random"),
     "If set to 'random', pick the number of chunks to recover (as specified by "
     " --erasures) at random. If set to 'exhaustive' try all combinations of erasures "
     " (i.e. k=4,m=3 with one erasure will try to recover from the erasure of "
     " the first chunk, then the second etc.)")
    ("parameter,P", po::value<vector<string> >(),
     "add a parameter to the erasure code profile")
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

  cct = global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_CLIENT,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
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
    }
  }

  in_size = vm["size"].as<int>();
  max_iterations = vm["iterations"].as<int>();
  plugin = vm["plugin"].as<string>();
  workload = vm["workload"].as<string>();
  erasures = vm["erasures"].as<int>();
  if (vm.count("erasures-generation") > 0 &&
      vm["erasures-generation"].as<string>() == "exhaustive")
    exhaustive_erasures = true;
  else
    exhaustive_erasures = false;
  if (vm.count("erased") > 0)
    erased = vm["erased"].as<vector<int> >();

  k = atoi(profile["k"].c_str());
  m = atoi(profile["m"].c_str());
  
  if (k <= 0) {
    cout << "parameter k is " << k << ". But k needs to be > 0." << endl;
    return -EINVAL;
  } else if ( m < 0 ) {
    cout << "parameter m is " << m << ". But m needs to be >= 0." << endl;
    return -EINVAL;
  } 

  verbose = vm.count("verbose") > 0 ? true : false;

  return 0;
}

int ErasureCodeBench::run() {
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  instance.disable_dlclose = true;

  if (workload == "encode")
    return encode();
  else
    return decode();
}

int ErasureCodeBench::encode()
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

  if (erasure_code->get_data_chunk_count() != (unsigned int)k ||
      (erasure_code->get_chunk_count() - erasure_code->get_data_chunk_count()
       != (unsigned int)m)) {
    cout << "parameter k is " << k << "/m is " << m << ". But data chunk count is "
      << erasure_code->get_data_chunk_count() <<"/parity chunk count is "
      << erasure_code->get_chunk_count() - erasure_code->get_data_chunk_count() << endl;
    return -EINVAL;
  }

  bufferlist in;
  in.append(string(in_size, 'X'));
  in.rebuild_aligned(ErasureCode::SIMD_ALIGN);
  set<int> want_to_encode;
  for (int i = 0; i < k + m; i++) {
    want_to_encode.insert(i);
  }
  utime_t begin_time = ceph_clock_now(g_ceph_context);
  for (int i = 0; i < max_iterations; i++) {
    map<int,bufferlist> encoded;
    code = erasure_code->encode(want_to_encode, in, &encoded);
    if (code)
      return code;
  }
  utime_t end_time = ceph_clock_now(g_ceph_context);
  cout << (end_time - begin_time) << "\t" << (max_iterations * (in_size / 1024)) << endl;
  return 0;
}

static void display_chunks(const map<int,bufferlist> &chunks,
			   unsigned int chunk_count) {
  cout << "chunks ";
  for (unsigned int chunk = 0; chunk < chunk_count; chunk++) {
    if (chunks.count(chunk) == 0) {
      cout << "(" << chunk << ")";
    } else {
      cout << " " << chunk << " ";
    }
    cout << " ";
  }
  cout << "(X) is an erased chunk" << endl;
}

int ErasureCodeBench::decode_erasures(const map<int,bufferlist> &all_chunks,
				      const map<int,bufferlist> &chunks,
				      unsigned i,
				      unsigned want_erasures,
				      ErasureCodeInterfaceRef erasure_code)
{
  int code = 0;

  if (want_erasures == 0) {
    if (verbose)
      display_chunks(chunks, erasure_code->get_chunk_count());
    set<int> want_to_read;
    for (unsigned int chunk = 0; chunk < erasure_code->get_chunk_count(); chunk++)
      if (chunks.count(chunk) == 0)
	want_to_read.insert(chunk);

    map<int,bufferlist> decoded;
    code = erasure_code->decode(want_to_read, chunks, &decoded);
    if (code)
      return code;
    for (set<int>::iterator chunk = want_to_read.begin();
	 chunk != want_to_read.end();
	 ++chunk) {
      if (all_chunks.find(*chunk)->second.length() != decoded[*chunk].length()) {
	cerr << "chunk " << *chunk << " length=" << all_chunks.find(*chunk)->second.length()
	     << " decoded with length=" << decoded[*chunk].length() << endl;
	return -1;
      }
      bufferlist tmp = all_chunks.find(*chunk)->second;
      if (!tmp.contents_equal(decoded[*chunk])) {
	cerr << "chunk " << *chunk
	     << " content and recovered content are different" << endl;
	return -1;
      }
    }
    return 0;
  }

  for (; i < erasure_code->get_chunk_count(); i++) {
    map<int,bufferlist> one_less = chunks;
    one_less.erase(i);
    code = decode_erasures(all_chunks, one_less, i + 1, want_erasures - 1, erasure_code);
    if (code)
      return code;
  }

  return 0;
}

int ErasureCodeBench::decode()
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
  if (erasure_code->get_data_chunk_count() != (unsigned int)k ||
      (erasure_code->get_chunk_count() - erasure_code->get_data_chunk_count()
       != (unsigned int)m)) {
    cout << "parameter k is " << k << "/m is " << m << ". But data chunk count is "
      << erasure_code->get_data_chunk_count() <<"/parity chunk count is "
      << erasure_code->get_chunk_count() - erasure_code->get_data_chunk_count() << endl;
    return -EINVAL;
  }
  bufferlist in;
  in.append(string(in_size, 'X'));
  in.rebuild_aligned(ErasureCode::SIMD_ALIGN);

  set<int> want_to_encode;
  for (int i = 0; i < k + m; i++) {
    want_to_encode.insert(i);
  }

  map<int,bufferlist> encoded;
  code = erasure_code->encode(want_to_encode, in, &encoded);
  if (code)
    return code;

  set<int> want_to_read = want_to_encode;

  if (erased.size() > 0) {
    for (vector<int>::const_iterator i = erased.begin();
	 i != erased.end();
	 ++i)
      encoded.erase(*i);
    display_chunks(encoded, erasure_code->get_chunk_count());
  }

  utime_t begin_time = ceph_clock_now(g_ceph_context);
  for (int i = 0; i < max_iterations; i++) {
    if (exhaustive_erasures) {
      code = decode_erasures(encoded, encoded, 0, erasures, erasure_code);
      if (code)
	return code;
    } else if (erased.size() > 0) {
      map<int,bufferlist> decoded;
      code = erasure_code->decode(want_to_read, encoded, &decoded);
      if (code)
	return code;
    } else {
      map<int,bufferlist> chunks = encoded;
      for (int j = 0; j < erasures; j++) {
	int erasure;
	do {
	  erasure = rand() % ( k + m );
	} while(chunks.count(erasure) == 0);
	chunks.erase(erasure);
      }
      map<int,bufferlist> decoded;
      code = erasure_code->decode(want_to_read, chunks, &decoded);
      if (code)
	return code;
    }
  }
  utime_t end_time = ceph_clock_now(g_ceph_context);
  cout << (end_time - begin_time) << "\t" << (max_iterations * (in_size / 1024)) << endl;
  return 0;
}

int main(int argc, char** argv) {
  ErasureCodeBench ecbench;
  try {
    int err = ecbench.setup(argc, argv);
    if (err)
      return err;
    return ecbench.run();
  } catch(po::error &e) {
    cerr << e.what() << endl; 
    return 1;
  }
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 ceph_erasure_code_benchmark &&
 *   valgrind --tool=memcheck --leak-check=full \
 *      ./ceph_erasure_code_benchmark \
 *      --plugin jerasure \
 *      --parameter directory=.libs \
 *      --parameter technique=reed_sol_van \
 *      --parameter k=2 \
 *      --parameter m=2 \
 *      --iterations 1
 * "
 * End:
 */
