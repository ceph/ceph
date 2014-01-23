// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <iostream>
#include <set>
#include <sstream>
#include <stdlib.h>
#include <fstream>

#include "common/Formatter.h"

#include "bencher.h"
#include "rados_backend.h"
#include "detailed_stat_collector.h"
#include "distribution.h"
#include "global/global_init.h"
#include "os/FileStore.h"
#include "testfilestore_backend.h"
#include "common/perf_counters.h"

namespace po = boost::program_options;
using namespace std;

struct MorePrinting : public DetailedStatCollector::AdditionalPrinting {
  CephContext *cct;
  MorePrinting(CephContext *cct) : cct(cct) {}
  void operator()(std::ostream *out) {
    bufferlist bl;
    Formatter *f = new_formatter("json-pretty");
    cct->get_perfcounters_collection()->dump_formatted(f, 0);
    f->flush(bl);
    delete f;
    bl.append('\0');
    *out << bl.c_str() << std::endl;
  }
};

int main(int argc, char **argv)
{
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("num-concurrent-ops", po::value<unsigned>()->default_value(10),
     "set number of concurrent ops")
    ("num-objects", po::value<unsigned>()->default_value(500),
     "set number of objects to use")
    ("object-size", po::value<unsigned>()->default_value(4<<20),
     "set object size")
    ("io-size", po::value<unsigned>()->default_value(4<<10),
     "set io size")
    ("write-ratio", po::value<double>()->default_value(0.75),
     "set ratio of read to write")
    ("duration", po::value<unsigned>()->default_value(0),
     "set max duration, 0 for unlimited")
    ("max-ops", po::value<unsigned>()->default_value(0),
     "set max ops, 0 for unlimited")
    ("seed", po::value<unsigned>(),
     "seed")
    ("num-colls", po::value<unsigned>()->default_value(20),
     "number of collections")
    ("op-dump-file", po::value<string>()->default_value(""),
     "set file for dumping op details, omit for stderr")
    ("filestore-path", po::value<string>(),
     "path to filestore directory, mandatory")
    ("journal-path", po::value<string>(),
     "path to journal, mandatory")
    ("offset-align", po::value<unsigned>()->default_value(4096),
     "align offset by")
    ("write-infos", po::value<bool>()->default_value(false),
      "write info objects with main writes")
    ("sequential", po::value<bool>()->default_value(false),
     "do sequential writes like rbd")
    ("disable-detailed-ops", po::value<bool>()->default_value(false),
     "don't dump per op stats")
    ("num-writers", po::value<unsigned>()->default_value(1),
     "num write threads")
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

  if (!vm.count("filestore-path") || !vm.count("journal-path")) {
    cout << "Must provide filestore-path and journal-path" << std::endl
	 << desc << std::endl;
    return 1;
  }

  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  rngen_t rng;
  if (vm.count("seed"))
    rng = rngen_t(vm["seed"].as<unsigned>());

  set<pair<double, Bencher::OpType> > ops;
  ops.insert(make_pair(vm["write-ratio"].as<double>(), Bencher::WRITE));
  ops.insert(make_pair(1-vm["write-ratio"].as<double>(), Bencher::READ));

  FileStore fs(vm["filestore-path"].as<string>(),
	       vm["journal-path"].as<string>());

  if (fs.mkfs() < 0) {
    cout << "mkfs failed" << std::endl;
    return 1;
  }

  if (fs.mount() < 0) {
    cout << "mount failed" << std::endl;
    return 1;
  }

  ostream *detailed_ops = 0;
  ofstream myfile;
  if (vm["disable-detailed-ops"].as<bool>()) {
    detailed_ops = 0;
  } else if (vm["op-dump-file"].as<string>().size()) {
    myfile.open(vm["op-dump-file"].as<string>().c_str());
    detailed_ops = &myfile;
  } else {
    detailed_ops = &cerr;
  }

  ceph::shared_ptr<StatCollector> col(
    new DetailedStatCollector(
      1, new JSONFormatter, detailed_ops, &cout,
      new MorePrinting(g_ceph_context)));

  cout << "Creating objects.." << std::endl;
  bufferlist bl;
  for (uint64_t i = 0; i < vm["object-size"].as<unsigned>(); ++i) {
    bl.append(0);
  }

  for (uint64_t num = 0; num < vm["num-colls"].as<unsigned>(); ++num) {
    stringstream coll;
    coll << "collection_" << num;
    std::cout << "collection " << coll.str() << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(coll_t(coll.str()));
    fs.apply_transaction(t);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(coll_t(string("meta")));
    fs.apply_transaction(t);
  }

  vector<ceph::shared_ptr<Bencher> > benchers(
    vm["num-writers"].as<unsigned>());
  for (vector<ceph::shared_ptr<Bencher> >::iterator i = benchers.begin();
       i != benchers.end();
       ++i) {
    set<string> objects;
    for (uint64_t num = 0; num < vm["num-objects"].as<unsigned>(); ++num) {
      unsigned col_num = num % vm["num-colls"].as<unsigned>();
      stringstream coll, obj;
      coll << "collection_" << col_num;
      obj << "obj_" << num << "_bencher_" << (i - benchers.begin());
      objects.insert(coll.str() + string("/") + obj.str());
    }
    Distribution<
      boost::tuple<string, uint64_t, uint64_t, Bencher::OpType> > *gen = 0;
    if (vm["sequential"].as<bool>()) {
      std::cout << "Using Sequential generator" << std::endl;
      gen = new SequentialLoad(
	objects,
	vm["object-size"].as<unsigned>(),
	vm["io-size"].as<unsigned>(),
	new WeightedDist<Bencher::OpType>(rng, ops)
	);
    } else {
      std::cout << "Using random generator" << std::endl;
      gen = new FourTupleDist<string, uint64_t, uint64_t, Bencher::OpType>(
	new RandomDist<string>(rng, objects),
	new Align(
	  new UniformRandom(
	    rng,
	    0,
	    vm["object-size"].as<unsigned>() - vm["io-size"].as<unsigned>()),
	  vm["offset-align"].as<unsigned>()
	  ),
	new Uniform(vm["io-size"].as<unsigned>()),
	new WeightedDist<Bencher::OpType>(rng, ops)
	);
    }

    Bencher *bencher = new Bencher(
      gen,
      col,
      new TestFileStoreBackend(&fs, vm["write-infos"].as<bool>()),
      vm["num-concurrent-ops"].as<unsigned>(),
      vm["duration"].as<unsigned>(),
      vm["max-ops"].as<unsigned>());

    bencher->init(objects, vm["object-size"].as<unsigned>(), &std::cout);
    cout << "Created objects..." << std::endl;
    (*i).reset(bencher);
  }

  for (vector<ceph::shared_ptr<Bencher> >::iterator i = benchers.begin();
       i != benchers.end();
       ++i) {
    (*i)->create();
  }
  for (vector<ceph::shared_ptr<Bencher> >::iterator i = benchers.begin();
       i != benchers.end();
       ++i) {
    (*i)->join();
  }

  fs.umount();
  if (vm["op-dump-file"].as<string>().size()) {
    myfile.close();
  }
  return 0;
}
