#include <boost/asio/io_context.hpp>
#include <boost/program_options.hpp>

#include "global/global_init.h"
#include "global/global_context.h"
#include "librados/librados_asio.h"
#include "common/ceph_argparse.h"

#include "ConsistencyChecker.h"

#define dout_context g_ceph_context

namespace po = boost::program_options;
using bufferlist = ceph::bufferlist;

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(cct.get());

  librados::Rados rados;
  boost::asio::io_context asio;
  std::thread thread;
  std::optional<boost::asio::executor_work_guard<
                  boost::asio::io_context::executor_type>> guard;

  po::options_description desc("ceph_ec_consistency_checker options");

  desc.add_options()
    ("help,h", "show help message")
    ("pool,p", po::value<std::string>(), "pool name")
    ("oid,i", po::value<std::string>(), "object io")
    ("blocksize,b", po::value<int>(), "block size")
    ("offset,o", po::value<int>(), "offset")
    ("length,l", po::value<int>(), "length");

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
      return 0;
      }
      po::notify(vm);
      unrecognized_options = po::collect_unrecognized(parsed.options, po::include_positional);
  } catch(const po::error& e) {
      std::cerr << "error: " << e.what() << std::endl;
      return 1;
  }

  auto pool = vm["pool"].as<std::string>();
  auto oid = vm["oid"].as<std::string>();
  auto blocksize = vm["blocksize"].as<int>();
  auto offset = vm["offset"].as<int>();
  auto length = vm["length"].as<int>();

  int rc;
  rc = rados.init_with_context(g_ceph_context);
  ceph_assert(rc == 0);
  rc = rados.connect();
  ceph_assert(rc == 0);

  guard.emplace(boost::asio::make_work_guard(asio));
  thread = make_named_thread("io_thread",[&asio] { asio.run(); });

  auto checker = ceph::consistency::ConsistencyChecker(rados, asio, pool);
  checker.single_read_and_check_consistency(oid, blocksize, offset, length);
  checker.print_results(std::cout);

  exit(0);
}