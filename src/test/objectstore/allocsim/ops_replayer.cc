#include <algorithm>
#include <cassert>
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
#include <fstream>
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
  Read
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

void gen_buffer(bufferlist& bl, uint64_t size) {
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(size);
    std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char> e;
    std::generate(buffer.get(), buffer.get()+size, std::ref(e));
    bl.append(buffer.get(), size);
}

void completion_cb(librados::completion_t cb, void *arg) {
  Op *op = static_cast<Op*>(arg);
  // Process the completed operation here
  std::cout << fmt::format("Completed op {} object={} range={}~{}", op->type, *op->object, op->offset, op->length) << std::endl;
  
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

int main(int argc, char** argv) {
  vector<Op> ops;
  librados::Rados cluster;
  librados::IoCtx io;
  uint64_t max_buffer_size = 0;
  uint64_t io_depth = 64;
  string file;
  std::filesystem::path ceph_conf_path;
  
  if (argc < 3) {
    cout << fmt::format("usage: ops_replayer file ceph.conf") << endl;
  }
  file = argv[1];
  ceph_conf_path = argv[2];
  cout << file << endl;
  
  
  
  string date, time, who, type, range, object, collection;
  ifstream fstream(file, ifstream::in);
  const char* date_format_first_column = "%Y-%m-%d";
  // we expect this input:
  // 2024-05-10 12:06:24.990831+00:00 client.607247697.0:5632274 write 4096~4096 2:d03a455a:::08b0f2fd5f20f504e76c2dd3d24683a1:head 2.1c0b
  while (fstream >> date){
    cout << date << endl;
    tm t;
    char* res = strptime(date.c_str(), date_format_first_column, &t);
    if (res == nullptr) {
      fstream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
      continue;
    }
    fstream >> time >> who >> type >> range >> object >> collection;
    
    date += " " + time;
    cout << date << endl;
    // FIXME: this is wrong  but it returns a reasonable bad timestamp :P
    const char* date_format_full = "%Y-%m-%d %H:%M:%S.%f%z";
    res = strptime(date.c_str(), date_format_full, &t);
    time_t at = mktime(&t);
    
    cout << fmt::format("{} {} {} {} {} {} {}", date, at, who, type, range, object, collection) << endl;
    
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
    getline(range_stream, length_str, '~');
    offset = stoll(offset_str);
    length = stoll(length_str);
    
    max_buffer_size = max(length, max_buffer_size);
    
    op_type ot = type == "write" ? Write : Read;
    ops.push_back(Op(at, ot, offset, length, object_ptr, collection_ptr, who_ptr));
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
  bufferlist bl;
  gen_buffer(bl, max_buffer_size);
  
  for (auto &op : ops) {
    {
      std::unique_lock<std::mutex> lock(in_flight_mutex);
      cv.wait(lock, [&io_depth] { return in_flight_ops < io_depth; });
      
    }
    cout << fmt::format("Running op {} object={} range={}~{}", op.type, *op.object, op.offset, op.length) << endl;
    op.completion = librados::Rados::aio_create_completion(static_cast<void*>(&op), completion_cb);
    switch (op.type) {
      case Write: {
        int ret = io.aio_write(*op.object, op.completion, bl, op.length, op.offset);
        if (ret != 0) {
          cout << fmt::format("Error writing ecode={}", ret) << endl;;
        }
        break;
      }
      case Read: {
        bufferlist read;
        int ret = io.aio_read(*op.object, op.completion, &op.read_bl, op.length, op.offset);
        if (ret != 0) {
          cout << fmt::format("Error reading ecode={}", ret) << endl;;
        }
        break;
      }
    }
    in_flight_ops++;
  }
  while (in_flight_ops > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  // io.write(const std::string &oid, bufferlist &bl, size_t len, uint64_t off)
  
  cout << ops.size() << endl;
  return 0;
}
