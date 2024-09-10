// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/program_options.hpp>

#include "librados/librados_asio.h"
#include "common/ceph_argparse.h"
#include "include/interval_set.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "common/Thread.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/split.h"

#include "common/io_exercising/DataGenerator.h"
#include "common/io_exercising/Model.h"
#include "common/io_exercising/IoOp.h"
#include "common/io_exercising/IoSequence.h"

#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

namespace po = boost::program_options;

const std::string usage[] = {
  "Basic usage:",
  "",
  "ceph_test_rados_io_sequence",
  "\t Test I/O to a single object using default settings. Good for",
  "\t testing boundary conditions",
  "",
  "ceph_test_rados_io_sequence --parallel <n>",
  "\t Run parallel test to multiple objects. First object is tested with",
  "\t default settings, other objects are tested with random settings",
  "",
  "Advanced usage:",
  "",
  "ceph_test_rados_io_sequence --blocksize <b> --km <k,m> --plugin <p>",
  "                            --objectsize <min,max> --threads <t>",
  "ceph_test_rados_io_sequence --blocksize <b> --pool <p> --object <oid>",
  "                            --objectsize <min,max> --threads <t>",
  "\tCustomize the test, if a pool is specified then it defines the",
  "\tReplica/EC configuration",
  "",
  "ceph_test_rados_io_sequence --listsequence",
  "\t Display list of supported I/O sequences",
  "",
  "ceph_test_rados_io_sequence --dryrun --sequence <n>",
  "\t Show I/O that will be generated for a sequence, validate",
  "\t seqeunce has correct I/O barriers to restrict concurrency",
  "",
  "ceph_test_rados_io_sequence --seed <seed>",
  "\t Repeat a previous test with the same random numbers (seed is",
  "\t displayed at start of test), if threads = 1 then this will produce",
  "\t the exact same sequence of I/O, if threads > 1 then I/Os are issued",
  "\t in parallel so ordering might be slightly different",
  "",
  "ceph_test_rados_io_sequence --sequence <n> --seqseed <n>",
  "\t Repeat a sequence from a previous test with the same random",
  "\t numbers (seqseed is displayed at start of sequence)",
  "",
  "ceph_test_rados_io_sequence --pool <p> --object <oid> --interactive",
  "\t Execute sequence of I/O commands from stdin. Offset and length",
  "\t are specified with unit of blocksize. Supported commands:",
  "\t\t create <len>",
  "\t\t remove",
  "\t\t read|write <off> <len>",
  "\t\t read2|write2 <off> <len> <off> <len>",
  "\t\t read3|write3 <off> <len> <off> <len> <off> <len>",
  "\t\t done"
};

/* Overview
 *
 * class SelectObjectSize
 *   Selects min and max object sizes for a test
 *
 * class SelectECProfile
 *   Selects an EC profile (plugin,k and m) for a test
 *
 * class SelectBlockSize
 *   Selects a block size for a test
 *
 * class SelectNumThreads
 *   Selects number of threads for a test
 *
 * class SelectSeqRange
 *   Selects a sequence or range of sequences for a test
 *
 * class TestObject
 *   Runs a test against an object, generating IOSequence
 *   and applying them to an IoExerciser
 *
 * main
 *   Run sequences of I/O with data integrity checking to
 *   one or more objects in parallel
 */

class SelectObjectSize
{
protected:
  ceph::util::random_number_generator<int>& rng;
  bool forced;
  std::pair<int,int> force_value;
  bool first;

  static constexpr int size = 10;

  // Choices for min and max object size
  static constexpr std::pair<uint64_t,uint64_t> choices[size] = {
    {1,32},  // Default - best for boundary checking
    {12,14},
    {28,30},
    {36,38},
    {42,44},
    {52,54},
    {66,68},
    {72,74},
    {83,83},
    {97,97}
  };

public:
  SelectObjectSize(ceph::util::random_number_generator<int>& rng,
		   po::variables_map vm) : rng(rng) {
    first = true;
    forced = vm.count("objectsize");
    if (forced) {
      force_value = vm["objectsize"].as<std::pair<int,int>>();
    }
  }

  const std::pair<uint64_t,uint64_t> choose() {
    if (forced) {
      return force_value;
    }else if (first) {
      first = false;
      return choices[0];
    } else {
      return choices[rng(size-1)];
    }
  }
};

class SelectECProfile
{
protected:
  ceph::util::random_number_generator<int>& rng;
  bool forced;
  std::pair<int,int> force_value;
  bool forced_pool;
  bool first;
  std::string pool_name;
  bool forced_plugin;
  std::string plugin;
  bool forced_stripe_unit;
  int stripe_unit;

  static constexpr int kmsize = 6;

  // Choices for EC k+m profile
  static constexpr std::pair<int,int> kmchoices[kmsize] = {
    {2,2}, // Default - reasonable coverage
    {2,1},
    {2,3},
    {3,2},
    {4,2},
    {5,1}
  };

  static constexpr int pluginsize = 2;

  // Choices for plugin
  static constexpr std::string_view pluginchoices[pluginsize] = {
    "jerasure",
    "isa"
  };

  static constexpr int stripeunitsize = 3;

  // Choices for stripe unit
  static constexpr int stripeunitchoices[stripeunitsize] = {
    4096,
    64*1024,
    256*1024
  };
  
public:
  SelectECProfile(ceph::util::random_number_generator<int>& rng,
		  po::variables_map vm) : rng(rng) {
    first = true;
    forced = vm.count("km");
    if (forced) {
      force_value = vm["km"].as<std::pair<int,int>>();
    } else {
      forced_pool = vm.count("pool");
      if (forced_pool) {
        pool_name = vm["pool"].as<std::string>();
      }
    }
    forced_plugin = vm.count("plugin");
    if (forced_plugin) {
      plugin = vm["plugin"].as<std::string>();
    }
    forced_stripe_unit = vm.count("stripe_unit");
    if (forced_stripe_unit) {
      stripe_unit = vm["stripe_unit"].as<int>();
    }
  }
    
  void create_pool(librados::Rados& rados, const std::string& pool_name,
                   const std::string& plugin, int k, int m, int stripe_unit)
  {
    int rc;
    bufferlist inbl, outbl;
    std::string profile_create =
      "{\"prefix\": \"osd erasure-code-profile set\", \
      \"name\": \"testprofile-" + pool_name + "\", \
      \"profile\": [ \"plugin=" + plugin + "\", \"k=" + std::to_string(k) +
      "\", \"m=" + std::to_string(m) + "\", \"crush-failure-domain=osd\", \
      \"stripe_unit=" + std::to_string(stripe_unit) + "\"]}";
    rc = rados.mon_command(profile_create, inbl, &outbl, nullptr);
    ceph_assert(rc == 0);
    std::string cmdstr =
      "{\"prefix\": \"osd pool create\", \
      \"pool\": \"" + pool_name + "\", \
      \"pool_type\": \"erasure\", \
      \"pg_num\": 8, \
      \"pgp_num\": 8, \
      \"erasure_code_profile\": \"testprofile-" + pool_name + "\"}";
    rc = rados.mon_command(cmdstr, inbl, &outbl, nullptr);
    ceph_assert(rc == 0);
  }

  const std::string choose(librados::Rados& rados) {
    std::pair<int,int> value;
    if (forced) {
      value = force_value;
    }else if (forced_pool) {
      return pool_name;
    }else if (first) {
      first = false;
      value = kmchoices[0];
    } else {
      value = kmchoices[rng(kmsize-1)];
    }
    int k = value.first;
    int m = value.second;
    if (!forced_plugin) {
      plugin = pluginchoices[rng(pluginsize-1)];
    }
    if (!forced_stripe_unit) {
      stripe_unit = stripeunitchoices[rng(stripeunitsize-1)];
    }
    pool_name = "ec_" + plugin + "_k" + std::to_string(k) +
      "_m" + std::to_string(m) + "_s" + std::to_string(stripe_unit);
    create_pool(rados, pool_name, plugin, k, m, stripe_unit);
    return pool_name;
  }
};

class SelectBlockSize
{
protected:
  ceph::util::random_number_generator<int>& rng;
  bool forced;
  uint64_t force_value;
  bool first;

  static constexpr int size = 5;

  // Choices for block size
  static constexpr uint64_t choices[size] = {
    2048, // Default - test boundaries for EC 4K chunk size
    512,
    3767,
    4096,
    32768
  };

public:
  SelectBlockSize(ceph::util::random_number_generator<int>& rng,
		  po::variables_map vm) : rng(rng) {
    first = true;
    forced = vm.count("blocksize");
    if (forced) {
      force_value = vm["blocksize"].as<uint64_t>();
    }
  }
  
  const uint64_t choose() {
    if (forced) {
      return force_value;
    }else if (first) {
      first = false;
      return choices[0];
    } else {
      return choices[rng(size-1)];
    }
  }
};

class SelectNumThreads
{
protected:
  ceph::util::random_number_generator<int>& rng;
  bool forced;
  int force_value;
  bool first;

  static constexpr int size = 4;

  // Choices for number of threads
  static constexpr int choices[size] = {
    1, // Default
    2,
    4,
    8
  };

public:
  SelectNumThreads(ceph::util::random_number_generator<int>& rng,
		   po::variables_map vm) : rng(rng) {
    first = true;
    forced = vm.count("threads");
    if (forced) {
      force_value = vm["threads"].as<int>();
    }
  }
  
  const int choose() {
    if (forced) {
      return force_value;
    }else if (first) {
      first = false;
      return choices[0];
    } else {
      return choices[rng(size-1)];
    }
  }
};

class SelectSeqRange
{
protected:
  ceph::util::random_number_generator<int>& rng;
  bool forced;
  std::pair<Sequence,Sequence> force_value;

public:
  SelectSeqRange(ceph::util::random_number_generator<int>& rng,
		 po::variables_map vm) : rng(rng) {
    forced = vm.count("sequence");
    if (forced) {
      Sequence s = static_cast<Sequence>(vm["sequence"].as<int>());
      if (s < SEQUENCE_BEGIN || s >= SEQUENCE_END) {
        std::cout << "Sequence argument out of range" << std::endl;
        throw po::validation_error(po::validation_error::invalid_option_value);
      }
      Sequence e = s;
      force_value = std::make_pair(s, ++e);
    }
  }

  const std::pair<Sequence,Sequence> choose() {
    if (forced) {
      return force_value;
    } else {
      return std::make_pair(SEQUENCE_BEGIN,SEQUENCE_END);
    }
  }
};

class TestObject
{
protected:
  std::unique_ptr<IoExerciser> ie;
  std::pair<int,int> obj_size_range;
  std::pair<Sequence,Sequence> seq_range;
  Sequence curseq;
  std::unique_ptr<IoSequence> seq;
  std::unique_ptr<IoOp> op;
  bool done;
  ceph::util::random_number_generator<int>& rng;
  bool verbose;
  bool has_seqseed;
  int seqseed;

public:
  TestObject( const std::string oid,
	      librados::Rados& rados,
	      boost::asio::io_context& asio,
	      ceph::mutex& lock,
	      ceph::condition_variable& cond,
	      SelectBlockSize& sbs,
	      SelectECProfile& sep,
	      SelectObjectSize& sos,
	      SelectNumThreads& snt,
	      SelectSeqRange & ssr,
	      ceph::util::random_number_generator<int>& rng,
	      bool dryrun,
	      bool _verbose,
	      bool has_seqseed,
	      int  seqseed) :
    rng(rng), verbose(_verbose), has_seqseed(has_seqseed), seqseed(seqseed)
  {
    if (dryrun) {
      verbose = true;
      ie = std::make_unique<ObjectModel>(oid,
					 sbs.choose(),
					 rng());
    } else {
      const std::string pool = sep.choose(rados);
      int threads = snt.choose();
      ie = std::make_unique<RadosIo>(rados,
				     asio,
				     pool,
				     oid,
				     sbs.choose(),
				     rng(),
				     threads,
				     lock,
				     cond);
      dout(0) << "= " << oid << " pool=" << pool << " threads=" <<
	threads << " blocksize=" << ie->get_block_size() << " =" << dendl;
    }
    obj_size_range = sos.choose();
    seq_range = ssr.choose();
    curseq = seq_range.first;
    seq = IoSequence::generate_sequence(curseq, obj_size_range,
					has_seqseed ? seqseed : rng());
    op = seq->next();
    done = false;
    dout(0) << "== " << ie->get_oid() << " " << curseq <<
      " " << seq->get_name() << " ==" <<dendl;
  }

  bool readyForIo()
  {
    return ie->readyForIoOp(*op);
  }

  bool next()
  {
    if (!done) {
      if (verbose) {
	dout(0) << ie->get_oid() << " Step " << seq->get_step() << ": " <<
	  op->to_string(ie->get_block_size()) << dendl;
      } else {
	// By default dout(5) goes just to in memory log, so log of recent
	// I/Os is shown only if an assert occurs
	dout(5) << ie->get_oid() << " Step " << seq->get_step() << ": " <<
	  op->to_string(ie->get_block_size()) << dendl;
      }
      ie->applyIoOp(*op);
      if (op->done()) {
	++curseq;
	if (curseq == seq_range.second) {
	  done = true;
          dout(0) << ie->get_oid() << " Number of IOs = " <<
	    ie->get_num_io() << dendl;
	} else {
	  seq = IoSequence::generate_sequence(curseq, obj_size_range,
					      has_seqseed ? seqseed : rng());
          dout(0) << "== " << ie->get_oid() << " " << curseq <<
	    " " << seq->get_name() << " ==" <<dendl;
          op = seq->next();
	}
      } else {
	op = seq->next();
      }
    }
    return done;
  }

  bool finished()
  {
    return done;
  }

  int get_num_io()
  {
    return ie->get_num_io();
  }
};


std::string get_token() {
  static std::string line;
  static ceph::split split = ceph::split("");
  static ceph::spliterator tokens;
  while (line.empty() || tokens == split.end()) {
    if (!std::getline(std::cin, line)) {
      throw std::runtime_error("End of input");
    }
    split = ceph::split(line);
    tokens = split.begin();
  }
  return std::string(*tokens++);
}

uint64_t get_num_token() {
  std::string parse_error;
  std::string token = get_token();
  uint64_t num = strict_iecstrtoll(token, &parse_error);
  if (!parse_error.empty()) {
    throw std::runtime_error("Invalid number "+token);
  }
  return num;
}

void do_interactive( const std::string oid,
		     librados::Rados& rados,
		     boost::asio::io_context& asio,
		     ceph::mutex& lock,
		     ceph::condition_variable& cond,
		     SelectBlockSize& sbs,
		     SelectECProfile& sep,
		     ceph::util::random_number_generator<int>& rng,
		     bool dryrun )
{
  bool done = false;
  std::unique_ptr<IoOp> ioop;
  std::unique_ptr<IoExerciser> ie;

  if (dryrun) {
    ie = std::make_unique<ObjectModel>(oid,
				       sbs.choose(),
				       rng());
  } else {
    const std::string pool = sep.choose(rados);
    ie = std::make_unique<RadosIo>(rados,
				   asio,
				   pool,
				   oid,
				   sbs.choose(),
				   rng(),
				   1, // 1 thread
				   lock,
				   cond);
  }

  while (!done) {
    const std::string op = get_token();
    if (!op.compare("done")  || !op.compare("q") || !op.compare("quit")) {
      ioop = IoOp::generate_done();
    } else if (!op.compare("create")) {
      ioop = IoOp::generate_create(get_num_token());
    } else if (!op.compare("remove") || !op.compare("delete")) {
      ioop = IoOp::generate_remove();
    } else if (!op.compare("read")) {
      uint64_t offset = get_num_token();
      uint64_t length = get_num_token();
      ioop = IoOp::generate_read(offset, length);
    } else if (!op.compare("read2")) {
      uint64_t offset1 = get_num_token();
      uint64_t length1 = get_num_token();
      uint64_t offset2 = get_num_token();
      uint64_t length2 = get_num_token();
      ioop = IoOp::generate_read2(offset1, length1, offset2, length2);
    } else if (!op.compare("read3")) {
      uint64_t offset1 = get_num_token();
      uint64_t length1 = get_num_token();
      uint64_t offset2 = get_num_token();
      uint64_t length2 = get_num_token();
      uint64_t offset3 = get_num_token();
      uint64_t length3 = get_num_token();
      ioop = IoOp::generate_read3(offset1, length1, offset2, length2,
				  offset3, length3);
    } else if (!op.compare("write")) {
      uint64_t offset = get_num_token();
      uint64_t length = get_num_token();
      ioop = IoOp::generate_write(offset, length);
    } else if (!op.compare("write2")) {
      uint64_t offset1 = get_num_token();
      uint64_t length1 = get_num_token();
      uint64_t offset2 = get_num_token();
      uint64_t length2 = get_num_token();
      ioop = IoOp::generate_write2(offset1, length1, offset2, length2);
    } else if (!op.compare("write3")) {
      uint64_t offset1 = get_num_token();
      uint64_t length1 = get_num_token();
      uint64_t offset2 = get_num_token();
      uint64_t length2 = get_num_token();
      uint64_t offset3 = get_num_token();
      uint64_t length3 = get_num_token();
      ioop = IoOp::generate_write3(offset1, length1, offset2, length2,
				   offset3, length3);
    } else {
      throw std::runtime_error("Invalid operation "+op);
    }
    dout(0) << ioop->to_string(ie->get_block_size()) << dendl;
    ie->applyIoOp(*ioop);
    done = ioop->done();
    if (!done) {
      ioop = IoOp::generate_barrier();
      ie->applyIoOp(*ioop);
    }
  }
}
    
struct Size {};

void validate(boost::any& v, const std::vector<std::string>& values,
              Size *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);

  std::string parse_error;
  uint64_t size = strict_iecstrtoll(s, &parse_error);
  if (!parse_error.empty()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
  v = boost::any(size);
}

struct Pair {};
void validate(boost::any& v, const std::vector<std::string>& values,
              Pair *target_type, int) {
  po::validators::check_first_occurrence(v);
  const std::string &s = po::validators::get_single_string(values);
  auto part = ceph::split(s).begin();
  std::string parse_error;
  int first = strict_iecstrtoll(*part++, &parse_error);
  int second = strict_iecstrtoll(*part, &parse_error);
  if (!parse_error.empty()) {
    throw po::validation_error(po::validation_error::invalid_option_value);
  }
  v = boost::any(std::pair<int,int>{first,second});
}

int main(int argc, char **argv)
{
  po::options_description desc("ceph_test_rados_io options");

  desc.add_options()
    ("help,h", "show help message")
    ("listsequence,l", "show list of sequences")
    ("dryrun,d", "test sequence, do not issue any I/O")
    ("verbose", "more verbose output during test")
    ("sequence,s", po::value<int>(), "test specified sequence")
    ("seed", po::value<int>(), "seed for whole test")
    ("seqseed", po::value<int>(), "seed for sequence")
    ("blocksize,b", po::value<Size>(), "block size (default 2048)")
    ("pool,p", po::value<std::string>(), "pool name")
    ("object,o", po::value<std::string>()->default_value("test"), "object name")
    ("km", po::value<Pair>(), "k,m EC pool profile (default 2,2)")
    ("stripeunit,u", po::value<int>(), "stripe_unit aka chunk size for EC pool profile (e.g. 4096)")
    ("plugin", po::value<std::string>(), "EC plugin (isa or jerasure)")
    ("objectsize", po::value<Pair>(), "min,max object size in blocks (default 1,32)")
    ("threads,t", po::value<int>(), "number of threads of I/O per object (default 1)")
    ("parallel", po::value<int>()->default_value(1), "number of objects to exercise in parallel")
    ("interactive", "interactive mode, execute IO commands from stdin");

  po::variables_map vm;
  std::vector<std::string> unrecognized_options;
  try {
    auto parsed = po::command_line_parser(argc, argv)
      .options(desc)
      .allow_unregistered()
      .run();
    po::store(parsed, vm);
    if (vm.count("help")) {
      std::cout << desc << std::endl;
      for (auto line : usage ) {
	std::cout << line << std::endl;
      }
      return 0;
    }
    po::notify(vm);
    unrecognized_options = po::collect_unrecognized(parsed.options,
						    po::include_positional);
  } catch(const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }

  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  // Seed
  int seed = time(nullptr);
  if (vm.count("seed")) {
    seed = vm["seed"].as<int>();
  }
  std::cout << "Test using seed " << seed << std::endl;
  auto rng = ceph::util::random_number_generator<int>(seed);
  
  // Select block size
  std::unique_ptr<SelectBlockSize> sbs =
    std::make_unique<SelectBlockSize>(rng, vm);

  // Select pool
  std::unique_ptr<SelectECProfile> sep =
    std::make_unique<SelectECProfile>(rng, vm);

  // Select object size range
  std::unique_ptr<SelectObjectSize> sos =
    std::make_unique<SelectObjectSize>(rng, vm);

  // Select number of threads
  std::unique_ptr<SelectNumThreads> snt =
    std::make_unique<SelectNumThreads>(rng, vm);

  // Select range of sequences
  std::unique_ptr<SelectSeqRange> ssr;
  try {
    ssr = std::make_unique<SelectSeqRange>(rng, vm);
  } catch(const po::error& e) {
    return 1;
  }

  bool dryrun = vm.count("dryrun");
  bool verbose = vm.count("verbose");
  bool has_seqseed = vm.count("seqseed");
  int seqseed = 0;
  if (has_seqseed) {
    seqseed = vm["seqseed"].as<int>();
  }
  int num_objects = vm["parallel"].as<int>();
  const std::string object_name = vm["object"].as<std::string>();
  bool interactive = vm.count("interactive");
  
  // List seqeunces
  if (vm.count("listsequence")) {
    std::pair<int,int> obj_size_range = sos->choose();
    for (Sequence s = SEQUENCE_BEGIN; s < SEQUENCE_END; ++s) {
      std::unique_ptr<IoSequence> seq =
	IoSequence::generate_sequence(s, obj_size_range,
				      has_seqseed ? seqseed : rng());
      std::cout << s << " " << seq->get_name() << std::endl;
    }
    return 0;
  }

  librados::Rados rados;
  boost::asio::io_context asio;
  std::thread thread;
  std::optional<boost::asio::executor_work_guard<
                  boost::asio::io_context::executor_type>> guard;
  ceph::mutex lock = ceph::make_mutex("RadosIo::lock");
  ceph::condition_variable cond;

  if (!dryrun) {
    int rc;
    rc = rados.init_with_context(g_ceph_context);
    ceph_assert(rc == 0);
    rc = rados.connect();
    ceph_assert(rc == 0);

    guard.emplace(boost::asio::make_work_guard(asio));
    thread = make_named_thread("io_thread",[&asio] { asio.run(); });
  }

  if (interactive) {
    do_interactive(object_name, rados, asio, lock, cond,
		   *sbs, *sep, rng, dryrun);
  } else {
    // Create a test for each object
    std::vector<std::shared_ptr<TestObject>> test_objects;
    for (int obj = 0; obj < num_objects; obj++) {
      std::string name;
      if (obj == 0) {
	name = object_name;
      } else {
	name = object_name + std::to_string(obj);
      }
      test_objects.push_back(std::make_shared<TestObject>(
	name, rados, asio, lock, cond,
	*sbs, *sep, *sos, *snt, *ssr,
	rng, dryrun, verbose, has_seqseed, seqseed));
    }
    if (!dryrun) {
      rados.wait_for_latest_osdmap();
    }

    // Main loop of test - while not all test objects have finished
    // check to see if any are able to start a new I/O. If all test
    // objects are waiting for I/O to complete then wait on a cond
    // that is signalled each time an I/O completes
    bool started_io = true;
    bool need_wait = true;
    while (started_io || need_wait) {
      started_io = false;
      need_wait = false;
      for (auto obj = test_objects.begin(); obj != test_objects.end(); ++obj) {
	std::shared_ptr<TestObject> to = *obj;
	if (!to->finished()) {
	  lock.lock();
	  bool ready = to->readyForIo();
	  lock.unlock();
	  if (ready)
	    {
	      to->next();
	      started_io = true;
	    } else {
	    need_wait = true;
	  }
	}
      }
      if (!started_io && need_wait) {
	std::unique_lock l(lock);
	cond.wait(l);
      }
    }    

    int total_io = 0;
    for (auto obj = test_objects.begin(); obj != test_objects.end(); ++obj) {
      std::shared_ptr<TestObject> to = *obj;
      total_io += to->get_num_io();
    }
    dout(0) << "Total number of IOs = " << total_io << dendl;
  }

  if (!dryrun) {
    guard = std::nullopt;
    asio.stop();
    thread.join();
    rados.shutdown();
  }
  return 0;
}
