#include <algorithm>
#include <functional>
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
#include <atomic>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <iostream>
#include <vector>

#include <fmt/format.h>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include "include/rados/buffer_fwd.h"
#include "include/rados/librados.hpp"

namespace po = boost::program_options;


using namespace std;
using namespace ceph;

namespace settings {

// Returns a function which restricts a value to a specified range by throwing if it is not in range:
// (Note: std::clamp() does not throw.)
auto clamp_or_throw(auto min, auto max)
{
 return [=](auto& x) { 
		if(std::less<>{}(x, min) or std::greater<>{}(x, max)) {
		 throw std::out_of_range(fmt::format("value expected between {} and {}, but got {}", min, max, x));
		}

		return x;	
 	};
}

} // namespace settings

// compare shared_ptr<string>
struct StringPtrCompare
{
  int operator()(const shared_ptr<string>& lhs, const shared_ptr<string>& rhs) const {
    if (lhs && rhs) {
        // Compare the content of the strings
        return *lhs < *rhs;
    }
    return lhs < rhs;
  }
};


static set<shared_ptr<string>, StringPtrCompare> string_cache;
static std::atomic<uint64_t> in_flight_ops(0);
static std::condition_variable cv;
static std::mutex in_flight_mutex;

enum op_type {
  Write,
  WriteFull,
  Read,
  Truncate,
  Zero
};

struct Op {
  time_t at;
  op_type type;
  uint64_t offset;
  uint64_t length;
  shared_ptr<string> object;
  shared_ptr<string> collection;
  shared_ptr<string> who;
  librados::AioCompletion *completion;
  bufferlist read_bl;

  Op(
    time_t at,
    op_type type,
    uint64_t offset,
    uint64_t length,
    shared_ptr<string> object,
    shared_ptr<string> collection,
    shared_ptr<string> who
  ) : at(at), type(type), offset(offset), length(length), object(object), collection(collection), who(who), completion(nullptr) {}

};

struct ParserContext {
    set<shared_ptr<string>, StringPtrCompare> collection_cache;
    set<shared_ptr<string>, StringPtrCompare> object_cache;
    set<shared_ptr<string>, StringPtrCompare> who_cache;
    vector<Op> ops;
    char *start; // starts and ends in new line or eof
    char *end;
    uint64_t max_buffer_size;
};

class MemoryStreamBuf : public std::streambuf {
public:
    MemoryStreamBuf(const char* start, const char* end) {
        this->setg(const_cast<char*>(start), const_cast<char*>(start), const_cast<char*>(end));
    }
};

class MemoryInputStream : public std::istream {
    MemoryStreamBuf _buffer;
public:
    MemoryInputStream(const char* start, const char* end)
        : std::istream(&_buffer), _buffer(start, end) {
        rdbuf(&_buffer);
    }
};

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


uint64_t timestamp_parser(std::string& date) {
  uint64_t timestamp = 0;
  uint64_t year, month, day, hour, minute, second;
  // expeted format
  // 2024-05-10 12:06:24.792232+00:00
  // 0123456789012345678------------
  year = std::stoull(date.substr(0, 4));
  month = std::stoull(date.substr(5, 2));
  day = std::stoull(date.substr(8, 2));
  hour = std::stoull(date.substr(11, 2));
  minute = std::stoull(date.substr(14, 2));
  second = std::stoull(date.substr(17, 2));
  //  SECONDS SINCE JAN 01 1970. (UTC), we don't care about timestamp timezone accuracy
  timestamp += (year - 1970) * 365 * 24 * 60 * 60;
  timestamp += (month * 30 * 24 * 60 * 60); // Yes, 30 day month is the best format ever and you cannot complain
  timestamp += (day * 24 * 60 * 60);
  timestamp += (hour * 60 * 60);
  timestamp += (minute * 60);
  timestamp += second;
  return timestamp;
}

void parse_entry_point(shared_ptr<ParserContext> context) {
  cout << fmt::format("Starting parser thread start={:p} end={:p}", context->start, context->end) << endl;

  string date, time, who, type, range, object, collection;
  MemoryInputStream fstream(context->start, context->end);
  // we expect this input:
  // 2024-05-10 12:06:24.990831+00:00 client.607247697.0:5632274 write 4096~4096 2:d03a455a:::08b0f2fd5f20f504e76c2dd3d24683a1:head 2.1c0b
  while (fstream >> date){
    // cout << date << endl;
    if (!(date.size() > 4 && isdigit(date[0]) && isdigit(date[1]) && isdigit(date[2]) && isdigit(date[3]) && date[4] == '-')) {
      fstream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
      continue;

    }
    fstream >> time >> who >> type >> range >> object >> collection;

    date += " " + time;
    // cout << date << endl;
    // FIXME: this is wrong  but it returns a reasonable bad timestamp :P
    time_t at = timestamp_parser(date);

    // cout << fmt::format("{} {} {} {} {} {} {}", date, at, who, type, range, object, collection) << endl;

    shared_ptr<string> who_ptr = make_shared<string>(who);
    auto who_it = context->who_cache.find(who_ptr);
    if (who_it == context->who_cache.end()) {
      context->who_cache.insert(who_ptr);
    } else {
      who_ptr = *who_it;
    }

    shared_ptr<string> object_ptr = make_shared<string>(object);
    auto object_it = context->object_cache.find(object_ptr);
    if (object_it == context->object_cache.end()) {
      context->object_cache.insert(object_ptr);
    } else {
      object_ptr = *object_it;
    }

    op_type ot;
    switch (type[0]) {
      case 'r': {
        ot = Read;
        break;
      }
      case 's': {
        ot = Read;
        break;
      }
      case 'z': {
        ot = Zero;
        break;
      }
      case 't': {
        ot = Truncate;
        break;
      }
      case 'w': {
        if (type.size() > 6) {
          ot = WriteFull;
        } else {
          ot = Write;
        }
        break;
      }
      default: {
        cout << "invalid type " << type << endl;
        exit(1);
      }
    }

    shared_ptr<string> collection_ptr = make_shared<string>(collection);
    auto collection_it = context->collection_cache.find(collection_ptr);
    if (collection_it == context->collection_cache.end()) {
      context->collection_cache.insert(collection_ptr);
    } else {
      collection_ptr = *collection_it;
    }

    uint64_t offset = 0, length = 0;
    stringstream range_stream(range);
    string offset_str, length_str;
    getline(range_stream, offset_str, '~');
    offset = stoll(offset_str);

    if (ot != Truncate) {
        // Truncate only has one number
        getline(range_stream, length_str, '~');
        length = stoll(length_str);
    }

    context->max_buffer_size = max(length, context->max_buffer_size);

    context->ops.push_back(Op(at, ot, offset, length, object_ptr, collection_ptr, who_ptr));
  }
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
  int nparser_threads = 16;
  int nworker_threads = 16;
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
    ("parser-threads", po::value<int>(&nparser_threads)->default_value(16)->notifier(settings::clamp_or_throw(1, 256)), "Number of parser threads")
    ("worker-threads", po::value<int>(&nworker_threads)->default_value(16)->notifier(settings::clamp_or_throw(1, 256)), "Number of I/O worker threads")
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

  vector<shared_ptr<ParserContext>> complete_parser_contexts; // list of ALL parser contexts so that shared_ptrs do not die.
  for (auto &file : input_files) {
    // Parse input file
    vector<std::thread> parser_threads;
    vector<shared_ptr<ParserContext>> parser_contexts;
    cout << fmt::format("parsing file {}", file) << endl;
    int fd = open(file.c_str(), O_RDONLY);
    if (fd == -1) {
        cout << "Error opening file" << endl;
        exit(EXIT_FAILURE);
    }
    struct stat file_stat;
    fstat(fd, &file_stat);
    char* mapped_buffer = (char*)mmap(NULL, file_stat.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (mapped_buffer == nullptr) {
        cout << "error mapping buffer" << endl;
        exit(EXIT_FAILURE);
    }
    uint64_t start_offset = 0;
    uint64_t step_size = file_stat.st_size / nparser_threads;
    for (int i = 0; i < nparser_threads; i++) {
      char* end = mapped_buffer + start_offset + step_size;
      while(*end != '\n') {
          end--;
      }
      if (i == nparser_threads - 1) {
          end = mapped_buffer + file_stat.st_size;
      }
      shared_ptr<ParserContext> context = make_shared<ParserContext>();
      context->start = mapped_buffer + start_offset;
      context->end = end;
      context->max_buffer_size = 0;
      parser_contexts.push_back(context);
      parser_threads.push_back(std::thread(parse_entry_point, context));
      start_offset += (end - mapped_buffer - start_offset);
    }
    for (auto& t : parser_threads) {
        t.join();
    }
    // reduce
    for (auto context : parser_contexts) {
        ops.insert(ops.end(), context->ops.begin(), context->ops.end());
        max_buffer_size = max(context->max_buffer_size, max_buffer_size);
        // context->ops.clear();
    }
    munmap(mapped_buffer, file_stat.st_size);
    complete_parser_contexts.insert(complete_parser_contexts.end(), parser_contexts.begin(), parser_contexts.end());
  }
  
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
