#include <algorithm>
#include <cassert>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <mutex>
#include <rados/buffer_fwd.h>
#include <rados/librados.hpp>
#include <atomic>
#include <fmt/format.h>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <iostream>
#include <vector>


using namespace std;
using namespace ceph;


static map<string, shared_ptr<string>> string_cache;
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
    map<string, shared_ptr<string>> string_cache;
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

void parse_entry_point(shared_ptr<ParserContext> context) {
    string date, time, who, type, range, object, collection;
    MemoryInputStream fstream(context->start, context->end);
    const char* date_format_first_column = "%Y-%m-%d";
    // we expect this input:
    // 2024-05-10 12:06:24.990831+00:00 client.607247697.0:5632274 write 4096~4096 2:d03a455a:::08b0f2fd5f20f504e76c2dd3d24683a1:head 2.1c0b
    while (fstream >> date){
      // cout << date << endl;
      tm t;
      char* res = strptime(date.c_str(), date_format_first_column, &t);
      if (res == nullptr) {
        fstream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        continue;
      }
      fstream >> time >> who >> type >> range >> object >> collection;

      date += " " + time;
      // cout << date << endl;
      // FIXME: this is wrong  but it returns a reasonable bad timestamp :P
      const char* date_format_full = "%Y-%m-%d %H:%M:%S.%f%z";
      res = strptime(date.c_str(), date_format_full, &t);
      time_t at = mktime(&t);

      // cout << fmt::format("{} {} {} {} {} {} {}", date, at, who, type, range, object, collection) << endl;

      shared_ptr<string> who_ptr = make_shared<string>(who);
      auto who_it = string_cache.find(who);
      if (who_it == string_cache.end()) {
        string_cache.insert({ who, who_ptr });
      } else {
        who_ptr = who_it->second;
      }

      shared_ptr<string> object_ptr = make_shared<string>(object);
      auto object_it = string_cache.find(object);
      if (object_it == string_cache.end()) {
        string_cache.insert({ object, object_ptr });
      } else {
        object_ptr = object_it->second;
      }

      op_type ot;
      if (type == "write") {
          ot = Write;
      } else if (type == "writefull") {
          ot = WriteFull;
      } else if (type == "read") {
          ot = Read;
      } else if (type == "sparse-read") {
          ot = Read;
      } else if (type == "truncate") {
          ot = Truncate;
      } else if (type == "zero") {
          ot = Zero;
      } else {
          cout << "invalid type " << type << endl;
          exit(1);
      }

      shared_ptr<string> collection_ptr = make_shared<string>(collection);
      auto collection_it = string_cache.find(collection);
      if (collection_it == string_cache.end()) {
        string_cache.insert({ collection, collection_ptr });
      } else {
        collection_ptr = collection_it->second;
      }

      uint64_t offset = 0, length = 0;
      stringstream range_stream(range);
      string offset_str, length_str;
      getline(range_stream, offset_str, '~');
      offset = stoll(offset_str);

      if (ot != Truncate) {
          // Truncate doesn't only has one number
          getline(range_stream, length_str, '~');
          length = stoll(length_str);
      }

      context->max_buffer_size = max(length, context->max_buffer_size);

      context->ops.push_back(Op(at, ot, offset, length, object_ptr, collection_ptr, who_ptr));
    }
}

void worker_thread_entry(uint64_t id, uint64_t nworker_threads, vector<Op> &ops, uint64_t max_buffer_size, uint64_t io_depth, librados::IoCtx* io) {

    bufferlist bl;
    gen_buffer(bl, max_buffer_size);
    hash<string> hasher;

    cout << "starting thread " << io_depth << endl;
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

int main(int argc, char** argv) {
  vector<Op> ops;
  librados::Rados cluster;
  librados::IoCtx io;
  uint64_t max_buffer_size = 0;
  uint64_t io_depth = 8;
  string file;
  std::filesystem::path ceph_conf_path;

  if (argc < 3) {
    cout << fmt::format("usage: ops_replayer file ceph.conf") << endl;
  }
  file = argv[1];
  ceph_conf_path = argv[2];
  cout << file << endl;

  uint64_t nthreads = 16;
  vector<std::thread> parser_threads;
  vector<shared_ptr<ParserContext>> parser_contexts;
  int fd = open(file.c_str(), O_RDONLY);
  if (fd == -1) {
      cout << "Error opening file" << endl;
  }
  struct stat file_stat;
  fstat(fd, &file_stat);
  char* mapped_buffer = (char*)mmap(NULL, file_stat.st_size, PROT_READ, MAP_SHARED, fd, 0);
  if (mapped_buffer == nullptr) {
      cout << "error mapping buffer" << endl;
  }
  uint64_t start_offset = 0;
  uint64_t step_size = file_stat.st_size / nthreads;
  for (int i = 0; i < nthreads; i++) {
    char* end = mapped_buffer + start_offset + step_size;
    while(*end != '\n') {
        end--;
    }
    if (i == nthreads-1) {
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
  for (auto context : parser_contexts) {
      string_cache.insert(context->string_cache.begin(), context->string_cache.end());
      ops.insert(ops.end(), context->ops.begin(), context->ops.end());
      max_buffer_size = max(context->max_buffer_size, max_buffer_size);
      context->string_cache.clear();
      context->ops.clear();
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
  cluster.ioctx_create("test_pool", io);
  if (ret < 0) {
    std::cerr << "Couldn't set up ioctx! error " << ret << std::endl;
    exit(EXIT_FAILURE);
  }
  std::cout << "test-pool ready" << std::endl;


  // process ops
  // Create a buffer big enough for every operation. We will take enoguh bytes from it for every operation
  vector<thread> worker_threads;
  uint64_t nworker_threads = 16;
  for (int i = 0; i < nworker_threads; i++) {
      worker_threads.push_back(thread(worker_thread_entry, i, nworker_threads, std::ref(ops), max_buffer_size, io_depth, &io));
  }
  for (auto& worker : worker_threads) {
      worker.join();
  }
  while (in_flight_ops > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  // io.write(const std::string &oid, bufferlist &bl, size_t len, uint64_t off)

  cout << ops.size() << endl;
  return 0;
}
