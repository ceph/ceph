#include <algorithm>
#include <boost/program_options/value_semantic.hpp>
#include <cassert>
#include <cctype>
#include <cstdlib>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <fstream>
#include <filesystem>
#include <mutex>
#include "include/rados/buffer_fwd.h"
#include "include/rados/librados.hpp"
#include <atomic>
#include <fmt/format.h>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <iostream>
#include <vector>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include "ops_parser.h"
namespace po = boost::program_options;

using namespace std;
using namespace ceph;

static set<shared_ptr<string>, StringPtrCompare> string_cache;
static std::atomic<uint64_t> in_flight_ops(0);
static std::condition_variable cv;
static std::mutex in_flight_mutex;


void gen_buffer(bufferlist& bl, uint64_t size) {
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(size);
    std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char> e;
    std::generate(buffer.get(), buffer.get()+size, std::ref(e));
    bl.append(buffer.get(), size);
}

void completion_cb(librados::completion_t cb, void *arg) {
  Op *op = static_cast<Op*>(arg);
  // Process the completed operation here
  // std::cout << fmt::format("Completed op {} object={} range={}~{}", op->type, *op->object, op->offset, op->length) << std::endl;

  delete op->completion;
  op->completion = nullptr;
  if (op->type == Read) {
   op->read_bl.clear();
  }

  {
    std::lock_guard<std::mutex> lock(in_flight_mutex);
    in_flight_ops--;
  }
  cv.notify_one();
}

void worker_thread_entry(uint64_t id, uint64_t nworker_threads, vector<Op> &ops, uint64_t max_buffer_size, uint64_t io_depth, librados::IoCtx* io) {
  // Create a buffer big enough for every operation. We will take enoguh bytes from it for every operation
  bufferlist bl;
  gen_buffer(bl, max_buffer_size);
  hash<string> hasher;

  cout << fmt::format("Starting thread {} with io_depth={} max_buffer_size={}", id, io_depth, max_buffer_size) << endl;
  for (auto &op : ops) {
    {
      std::unique_lock<std::mutex> lock(in_flight_mutex);
      cv.wait(lock, [&io_depth] { return in_flight_ops < io_depth; });
    }
    size_t key = hasher(*op.who) % nworker_threads;
    if (key != id) {
        continue;
    }
    // cout << fmt::format("Running op {} object={} range={}~{}", op.type, *op.object, op.offset, op.length) << endl;
    op.completion = librados::Rados::aio_create_completion(static_cast<void*>(&op), completion_cb);
    switch (op.type) {
      case Write: {
        bufferlist trimmed;
        trimmed.substr_of(bl, 0, op.length);
        int ret = io->aio_write(*op.object, op.completion, trimmed, op.length, op.offset);
        if (ret != 0) {
          cout << fmt::format("Error writing ecode={}", ret) << endl;;
        }
        break;
      }
      case WriteFull: {
        bufferlist trimmed;
        trimmed.substr_of(bl, 0, op.length);
        int ret = io->aio_write_full(*op.object, op.completion, trimmed);
        if (ret != 0) {
          cout << fmt::format("Error writing full ecode={}", ret) << endl;;
        }
        break;
      }
      case Read: {
        bufferlist read;
        int ret = io->aio_read(*op.object, op.completion, &op.read_bl, op.length, op.offset);
        if (ret != 0) {
          cout << fmt::format("Error reading ecode={}", ret) << endl;;
        }
        break;
      }
      case Truncate: {
          librados::ObjectWriteOperation write_operation;
          write_operation.truncate(op.offset);
          int ret = io->aio_operate(*op.object, op.completion, &write_operation);
          if (ret != 0) {
            cout << fmt::format("Error truncating ecode={}", ret) << endl;;
          }
          break;
      }
      case Zero: {
          librados::ObjectWriteOperation write_operation;
          write_operation.zero(op.offset, op.length);
          int ret = io->aio_operate(*op.object, op.completion, &write_operation);
          if (ret != 0) {
            cout << fmt::format("Error zeroing ecode={}", ret) << endl;;
          }
          break;
      }
    }
    in_flight_ops++;
  }
}


int op_comparison_by_date(const Op& lhs, const Op& rhs) {
  return lhs.at < rhs.at;
}

void usage(po::options_description &desc) {
  cout << desc << std::endl;
}

int main(int argc, char** argv) {
  vector<Op> ops;
  librados::Rados cluster;
  librados::IoCtx io;
  uint64_t max_buffer_size = 0; // We can use a single buffer for writes and trim it at will. The buffer will be the size of the maximum length op.

  // options
  uint64_t io_depth = 8;
  uint64_t nparser_threads = 16;
  uint64_t nworker_threads = 16;
  string file("input.txt");
  string ceph_conf_path("./ceph.conf");
  string pool("test_pool");
  bool skip_do_ops = false;
  po::options_description po_options("Options");
  po_options.add_options()
    ("help,h", "produce help message")
    ("input-files,i", po::value<vector<string>>()->multitoken(), "List of input files (output of op_scraper.py). Multiple files will be merged and sorted by time order")
    ("ceph-conf", po::value<string>(&ceph_conf_path)->default_value("ceph.conf"), "Path to ceph conf")
    ("io-depth", po::value<uint64_t>(&io_depth)->default_value(64), "I/O depth")
    ("parser-threads", po::value<uint64_t>(&nparser_threads)->default_value(16), "Number of parser threads")
    ("worker-threads", po::value<uint64_t>(&nworker_threads)->default_value(16), "Number of I/O worker threads")
    ("pool", po::value<string>(&pool)->default_value("test_pool"), "Pool to use for I/O")
    ("skip-do-ops", po::bool_switch(&skip_do_ops)->default_value(false), "Skip doing operations")
    ;

  po::options_description po_all("All options");
  po_all.add(po_options);

  po::variables_map vm;
  po::parsed_options parsed = po::command_line_parser(argc, argv).options(po_all).allow_unregistered().run();
  po::store( parsed, vm);
  po::notify(vm);
  
  if (vm.count("help")) {
    usage(po_all);
    exit(EXIT_SUCCESS);
  }
  
  assert(vm.count("input-files") > 0);
  
  vector<string> input_files = vm["input-files"].as<vector<string>>();

  parse_files(input_files, nparser_threads, ops, max_buffer_size);

  cout << "Sorting ops by date..." << endl;
  sort(ops.begin(), ops.end(), op_comparison_by_date);
  cout << "Sorting ops by date done" << endl;
  
  if (skip_do_ops) {
    return EXIT_SUCCESS;
  }
  
  int ret = cluster.init2("client.admin", "ceph", 0);
  if (ret < 0) {
    std::cerr << "Couldn't init ceph! error " << ret << std::endl;
    return EXIT_FAILURE;
  }
  std::cout << "cluster init ready" << std::endl;

  ret = cluster.conf_read_file(ceph_conf_path.c_str());
  if (ret < 0) {
    std::cerr << "Couldn't read the Ceph configuration file! error " << ret << std::endl;
    return EXIT_FAILURE;
  }
  std::cout << "cluster config ready" << std::endl;
  ret = cluster.connect();
  if (ret < 0) {
    std::cerr << "Couldn't connect to cluster! error " << ret << std::endl;
    return EXIT_FAILURE;
  }
  std::cout << "cluster connect ready" << std::endl;
  cluster.ioctx_create(pool.c_str(), io);
  if (ret < 0) {
    std::cerr << "Couldn't set up ioctx! error " << ret << std::endl;
    exit(EXIT_FAILURE);
  }
  std::cout << fmt::format("pool {} ready", pool) << std::endl;


  // process ops
  vector<thread> worker_threads;
  for (int i = 0; i < nworker_threads; i++) {
      worker_threads.push_back(thread(worker_thread_entry, i, nworker_threads, std::ref(ops), max_buffer_size, io_depth, &io));
  }
  for (auto& worker : worker_threads) {
      worker.join();
  }
  while (in_flight_ops > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  cout << ops.size() << endl;
  return EXIT_SUCCESS;
}
