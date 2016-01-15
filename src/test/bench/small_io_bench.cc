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

namespace po = boost::program_options;
using namespace std;

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
    ("ceph-client-id", po::value<string>()->default_value("admin"),
     "set ceph client id")
    ("pool-name", po::value<string>()->default_value("data"),
     "set pool")
    ("op-dump-file", po::value<string>()->default_value(""),
     "set file for dumping op details, omit for stderr")
    ("init-only", po::value<bool>()->default_value(false),
     "populate object set")
    ("do-not-init", po::value<bool>()->default_value(false),
     "use existing object set")
    ("use-prefix", po::value<string>()->default_value(""),
     "use previously populated prefix")
    ("offset-align", po::value<unsigned>()->default_value(4096),
     "align offset by")
    ("sequential", po::value<bool>()->default_value(false),
     "use sequential access pattern")
    ("disable-detailed-ops", po::value<bool>()->default_value(false),
     "don't dump per op stats")
    ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  if (vm["do-not-init"].as<bool>() && !vm["use-prefix"].as<string>().size()) {
    cout << "Must supply prefix if do-not-init is specified" << std::endl;
    cout << desc << std::endl;
    return 1;
  }

  if (vm["init-only"].as<bool>() && !vm["use-prefix"].as<string>().size()) {
    cout << "Must supply prefix for init-only" << std::endl;
    cout << desc << std::endl;
    return 1;
  }

  string prefix;
  if (vm["use-prefix"].as<string>().size()) {
    prefix = vm["use-prefix"].as<string>();
  } else {
    char hostname_cstr[100];
    gethostname(hostname_cstr, 100);
    stringstream hostpid;
    hostpid << hostname_cstr << getpid() << "-";
    prefix = hostpid.str();
  }

  set<string> objects;
  for (unsigned i = 0; i < vm["num-objects"].as<unsigned>();
       ++i) {
    stringstream name;
    name << prefix << "-object_" << i;
    objects.insert(name.str());
  }

  rngen_t rng;
  if (vm.count("seed"))
    rng = rngen_t(vm["seed"].as<unsigned>());

  set<pair<double, Bencher::OpType> > ops;
  ops.insert(make_pair(vm["write-ratio"].as<double>(), Bencher::WRITE));
  ops.insert(make_pair(1-vm["write-ratio"].as<double>(), Bencher::READ));

  librados::Rados rados;
  librados::IoCtx ioctx;
  int r = rados.init(vm["ceph-client-id"].as<string>().c_str());
  if (r < 0) {
    cerr << "error in init r=" << r << std::endl;
    return -r;
  }
  r = rados.conf_read_file(NULL);
  if (r < 0) {
    cerr << "error in conf_read_file r=" << r << std::endl;
    return -r;
  }
  r = rados.conf_parse_env(NULL);
  if (r < 0) {
    cerr << "error in conf_parse_env r=" << r << std::endl;
    return -r;
  }
  r = rados.connect();
  if (r < 0) {
    cerr << "error in connect r=" << r << std::endl;
    return -r;
  }
  r = rados.ioctx_create(vm["pool-name"].as<string>().c_str(), ioctx);
  if (r < 0) {
    cerr << "error in ioctx_create r=" << r << std::endl;
    return -r;
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

  Bencher bencher(
    gen,
    new DetailedStatCollector(1, new JSONFormatter, detailed_ops, &cout),
    new RadosBackend(&ioctx),
    vm["num-concurrent-ops"].as<unsigned>(),
    vm["duration"].as<unsigned>(),
    vm["max-ops"].as<unsigned>());

  if (!vm["do-not-init"].as<bool>()) {
    bencher.init(objects, vm["object-size"].as<unsigned>(), &std::cout);
    cout << "Created objects..." << std::endl;
  } else {
    cout << "Not initing objects..." << std::endl;
  }

  if (!vm["init-only"].as<bool>()) {
    bencher.run_bench();
  } else {
    cout << "init-only" << std::endl;
  }

  rados.shutdown();
  if (vm["op-dump-file"].as<string>().size()) {
    myfile.close();
  }
  return 0;
}
