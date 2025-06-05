// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <chrono>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <random>
#include <thread>
#include <stack>
#include <gtest/gtest.h>
#include "common/dout.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "include/scope_guard.h"
#include "common/errno.h"

#include "os/bluestore/Allocator.h"
#include "os/bluestore/bluefs_types.h"
#include "os/bluestore/bluestore_common.h"
#include "os/bluestore/BlueFS.h"

using namespace std;

// some test should not be executed on jenkins make check
#define SKIP_JENKINS() \
  if (getenv("JENKINS_HOME") != nullptr) GTEST_SKIP_("test disabled on jenkins");


std::unique_ptr<char[]> gen_buffer(uint64_t size)
{
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(size);
    std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char> e;
    std::generate(buffer.get(), buffer.get()+size, std::ref(e));
    return buffer;
}

class TempBdev {
public:
  TempBdev(uint64_t size)
    : path{get_temp_bdev(size)}
  {}
  TempBdev()
  {}
  void Init(uint64_t size) {
    path = get_temp_bdev(size);
  }
  ~TempBdev() {
    if (!path.empty()) {
      rm_temp_bdev(path);
    }
  }
  std::string path;
private:
  static string get_temp_bdev(uint64_t size)
  {
    static int n = 0;
    string fn = "ceph_test_bluefs.tmp.block." + stringify(getpid())
      + "." + stringify(++n);
    int fd = ::open(fn.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0644);
    ceph_assert(fd >= 0);
    int r = ::ftruncate(fd, size);
    ceph_assert(r >= 0);
    ::close(fd);
    return fn;
  }
  static void rm_temp_bdev(string f)
  {
    ::unlink(f.c_str());
  }
};

class ConfSaver {
  std::stack<std::pair<std::string, std::string>> saved_settings;
  ConfigProxy& conf;
public:
  ConfSaver(ConfigProxy& conf) : conf(conf) {
    conf._clear_safe_to_start_threads();
  };
  ~ConfSaver() {
    conf._clear_safe_to_start_threads();
    while(saved_settings.size() > 0) {
      auto& e = saved_settings.top();
      conf.set_val_or_die(e.first, e.second);
      saved_settings.pop();
    }
    conf.set_safe_to_start_threads();
    conf.apply_changes(nullptr);
  }
  void SetVal(const char* key, const char* val) {
    std::string skey(key);
    std::string prev_val;
    conf.get_val(skey, &prev_val);
    conf.set_val_or_die(skey, val);
    saved_settings.emplace(skey, prev_val);
  }
  void ApplyChanges() {
    conf.set_safe_to_start_threads();
    conf.apply_changes(nullptr);
  }
};

TEST(BlueFS, mkfs) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  uuid_d fsid;
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
}

TEST(BlueFS, mkfs_mount) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(fs.get_block_device_size(BlueFS::BDEV_DB), size);
  ASSERT_LT(fs.get_free(BlueFS::BDEV_DB), size);
  fs.umount();
}

TEST(BlueFS, write_read) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write("dir", "file", &h, false));
    h->append("foo", 3);
    h->append("bar", 3);
    h->append("baz", 3);
    fs.fsync(h);
    fs.close_writer(h);
  }
  {
    BlueFS::FileReader *h;
    ASSERT_EQ(0, fs.open_for_read("dir", "file", &h));
    bufferlist bl;
    ASSERT_EQ(9, fs.read(h, 0, 1024, &bl, NULL));
    ASSERT_EQ(0, strncmp("foobarbaz", bl.c_str(), 9));
    delete h;
  }
  fs.umount();
}

TEST(BlueFS, small_appends) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write("dir", "file", &h, false));
    for (unsigned i = 0; i < 10000; ++i) {
      h->append("abcdeabcdeabcdeabcdeabcdeabc", 23);
    }
    fs.fsync(h);
    fs.close_writer(h);
  }
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.open_for_write("dir", "file_sync", &h, false));
    for (unsigned i = 0; i < 1000; ++i) {
      h->append("abcdeabcdeabcdeabcdeabcdeabc", 23);
      ASSERT_EQ(0, fs.fsync(h));
    }
    fs.close_writer(h);
  }
  fs.umount();
}

TEST(BlueFS, very_large_write) {
  SKIP_JENKINS();
  // we'll write a ~5G file, so allocate more than that for the whole fs
  uint64_t size = 1048576 * 1024 * 6ull;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);

  bool old = g_ceph_context->_conf.get_val<bool>("bluefs_buffered_io");
  g_ceph_context->_conf.set_val("bluefs_buffered_io", "false");
  uint64_t total_written = 0;

  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  char buf[1048571]; // this is biggish, but intentionally not evenly aligned
  for (unsigned i = 0; i < sizeof(buf); ++i) {
    buf[i] = i;
  }
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write("dir", "bigfile", &h, false));
    for (unsigned i = 0; i < 3*1024*1048576ull / sizeof(buf); ++i) {
      h->append(buf, sizeof(buf));
      total_written += sizeof(buf);
    }
    fs.fsync(h);
    for (unsigned i = 0; i < 2*1024*1048576ull / sizeof(buf); ++i) {
      h->append(buf, sizeof(buf));
      total_written += sizeof(buf);
    }
    fs.fsync(h);
    fs.close_writer(h);
  }
  {
    BlueFS::FileReader *h;
    ASSERT_EQ(0, fs.open_for_read("dir", "bigfile", &h));
    bufferlist bl;
    ASSERT_EQ(h->file->fnode.size, total_written);
    for (unsigned i = 0; i < 3*1024*1048576ull / sizeof(buf); ++i) {
      bl.clear();
      fs.read(h, i * sizeof(buf), sizeof(buf), &bl, NULL);
      int r = memcmp(buf, bl.c_str(), sizeof(buf));
      if (r) {
	cerr << "read got mismatch at offset " << i*sizeof(buf) << " r " << r
	     << std::endl;
      }
      ASSERT_EQ(0, r);
    }
    for (unsigned i = 0; i < 2*1024*1048576ull / sizeof(buf); ++i) {
      bl.clear();
      fs.read(h, i * sizeof(buf), sizeof(buf), &bl, NULL);
      int r = memcmp(buf, bl.c_str(), sizeof(buf));
      if (r) {
	cerr << "read got mismatch at offset " << i*sizeof(buf) << " r " << r
	     << std::endl;
      }
      ASSERT_EQ(0, r);
    }
    delete h;
    ASSERT_EQ(0, fs.open_for_read("dir", "bigfile", &h));
    ASSERT_EQ(h->file->fnode.size, total_written);
    auto huge_buf = std::make_unique<char[]>(h->file->fnode.size);
    auto l = h->file->fnode.size;
    int64_t r = fs.read(h, 0, l, NULL, huge_buf.get());
    ASSERT_EQ(r, l);
    delete h;
  }
  fs.umount();

  g_ceph_context->_conf.set_val("bluefs_buffered_io", stringify((int)old));
}

TEST(BlueFS, very_large_write2) {
  SKIP_JENKINS();
  // we'll write a ~5G file, so allocate more than that for the whole fs
  uint64_t size_full = 1048576 * 1024 * 6ull;
  uint64_t size = 1048576 * 1024 * 5ull;
  TempBdev bdev{ size_full };
  BlueFS fs(g_ceph_context);

  bool old = g_ceph_context->_conf.get_val<bool>("bluefs_buffered_io");
  g_ceph_context->_conf.set_val("bluefs_buffered_io", "false");
  uint64_t total_written = 0;

  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));

  char fill_arr[1 << 20]; // 1M
  for (size_t i = 0; i < sizeof(fill_arr); ++i) {
    fill_arr[i] = (char)i;
  }
  std::unique_ptr<char[]> buf;
  buf.reset(new char[size]);
  for (size_t i = 0; i < size; i += sizeof(fill_arr)) {
    memcpy(buf.get() + i, fill_arr, sizeof(fill_arr));
  }
  {
    BlueFS::FileWriter* h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write("dir", "bigfile", &h, false));
    fs.append_try_flush(h, buf.get(), size);
    total_written = size;
    fs.fsync(h);
    fs.close_writer(h);
  }
  memset(buf.get(), 0, size);
  {
    BlueFS::FileReader* h;
    ASSERT_EQ(0, fs.open_for_read("dir", "bigfile", &h));
    ASSERT_EQ(h->file->fnode.size, total_written);
    auto l = h->file->fnode.size;
    int64_t r = fs.read(h, 0, l, NULL, buf.get());
    ASSERT_EQ(r, l);
    for (size_t i = 0; i < size; i += sizeof(fill_arr)) {
      ceph_assert(memcmp(buf.get() + i, fill_arr, sizeof(fill_arr)) == 0);
    }
    delete h;
  }
  fs.umount();

  g_ceph_context->_conf.set_val("bluefs_buffered_io", stringify((int)old));
}

#define ALLOC_SIZE 4096

void write_data(BlueFS &fs, uint64_t rationed_bytes)
{
    int j=0, r=0;
    uint64_t written_bytes = 0;
    rationed_bytes -= ALLOC_SIZE;
    stringstream ss;
    string dir = "dir.";
    ss << std::this_thread::get_id();
    dir.append(ss.str());
    dir.append(".");
    dir.append(to_string(j));
    ASSERT_EQ(0, fs.mkdir(dir));
    while (1) {
      string file = "file.";
      file.append(to_string(j));
      BlueFS::FileWriter *h;
      ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
      ASSERT_NE(nullptr, h);
      auto sg = make_scope_guard([&fs, h] { fs.close_writer(h); });
      bufferlist bl;
      std::unique_ptr<char[]> buf = gen_buffer(ALLOC_SIZE);
      bufferptr bp = buffer::claim_char(ALLOC_SIZE, buf.get());
      bl.push_back(bp);
      h->append(bl.c_str(), bl.length());
      r = fs.fsync(h);
      if (r < 0) {
         break;
      }
      written_bytes += g_conf()->bluefs_alloc_size;
      j++;
      if ((rationed_bytes - written_bytes) <= g_conf()->bluefs_alloc_size) {
        break;
      }
    }
}

void create_single_file(BlueFS &fs)
{
    BlueFS::FileWriter *h;
    stringstream ss;
    string dir = "dir.test";
    ASSERT_EQ(0, fs.mkdir(dir));
    string file = "testfile";
    ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
    bufferlist bl;
    std::unique_ptr<char[]> buf = gen_buffer(ALLOC_SIZE);
    bufferptr bp = buffer::claim_char(ALLOC_SIZE, buf.get());
    bl.push_back(bp);
    h->append(bl.c_str(), bl.length());
    fs.fsync(h);
    fs.close_writer(h);
}

void write_single_file(BlueFS &fs, uint64_t rationed_bytes)
{
    stringstream ss;
    const string dir = "dir.test";
    const string file = "testfile";
    uint64_t written_bytes = 0;
    rationed_bytes -= ALLOC_SIZE;
    while (1) {
      BlueFS::FileWriter *h;
      ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
      ASSERT_NE(nullptr, h);
      auto sg = make_scope_guard([&fs, h] { fs.close_writer(h); });
      bufferlist bl;
      std::unique_ptr<char[]> buf = gen_buffer(ALLOC_SIZE);
      bufferptr bp = buffer::claim_char(ALLOC_SIZE, buf.get());
      bl.push_back(bp);
      h->append(bl.c_str(), bl.length());
      int r = fs.fsync(h);
      if (r < 0) {
         break;
      }
      written_bytes += g_conf()->bluefs_alloc_size;
      if ((rationed_bytes - written_bytes) <= g_conf()->bluefs_alloc_size) {
        break;
      }
    }
}

bool writes_done = false;

void sync_fs(BlueFS &fs)
{
    while (1) {
      if (writes_done == true)
        break;
      fs.sync_metadata(false);
      sleep(1);
    }
}


void do_join(std::thread& t)
{
    t.join();
}

void join_all(std::vector<std::thread>& v)
{
    std::for_each(v.begin(),v.end(),do_join);
}

#define NUM_WRITERS 3
#define NUM_SYNC_THREADS 1

#define NUM_SINGLE_FILE_WRITERS 1
#define NUM_MULTIPLE_FILE_WRITERS 2

TEST(BlueFS, test_flush_1) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  g_ceph_context->_conf.set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf.apply_changes(nullptr);

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    std::vector<std::thread> write_thread_multiple;
    uint64_t effective_size = size - (32 * 1048576); // leaving the last 32 MB for log compaction
    uint64_t per_thread_bytes = (effective_size/(NUM_MULTIPLE_FILE_WRITERS + NUM_SINGLE_FILE_WRITERS));
    for (int i=0; i<NUM_MULTIPLE_FILE_WRITERS ; i++) {
      write_thread_multiple.push_back(std::thread(write_data, std::ref(fs), per_thread_bytes));
    }

    create_single_file(fs);
    std::vector<std::thread> write_thread_single;
    for (int i=0; i<NUM_SINGLE_FILE_WRITERS; i++) {
      write_thread_single.push_back(std::thread(write_single_file, std::ref(fs), per_thread_bytes));
    }

    join_all(write_thread_single);
    join_all(write_thread_multiple);
  }
  fs.umount();
}

TEST(BlueFS, test_flush_2) {
  uint64_t size = 1048576 * 256;
  TempBdev bdev{size};
  g_ceph_context->_conf.set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf.apply_changes(nullptr);

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    uint64_t effective_size = size - (128 * 1048576); // leaving the last 32 MB for log compaction
    uint64_t per_thread_bytes = (effective_size/(NUM_WRITERS));
    std::vector<std::thread> write_thread_multiple;
    for (int i=0; i<NUM_WRITERS; i++) {
      write_thread_multiple.push_back(std::thread(write_data, std::ref(fs), per_thread_bytes));
    }

    join_all(write_thread_multiple);
  }
  fs.umount();
}

TEST(BlueFS, test_flush_3) {
  uint64_t size = 1048576 * 256;
  TempBdev bdev{size};
  g_ceph_context->_conf.set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf.apply_changes(nullptr);

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    std::vector<std::thread> write_threads;
    uint64_t effective_size = size - (64 * 1048576); // leaving the last 11 MB for log compaction
    uint64_t per_thread_bytes = (effective_size/(NUM_WRITERS));
    for (int i=0; i<NUM_WRITERS; i++) {
      write_threads.push_back(std::thread(write_data, std::ref(fs), per_thread_bytes));
    }

    std::vector<std::thread> sync_threads;
    for (int i=0; i<NUM_SYNC_THREADS; i++) {
      sync_threads.push_back(std::thread(sync_fs, std::ref(fs)));
    }

    join_all(write_threads);
    writes_done = true;
    join_all(sync_threads);
  }
  fs.umount();
}

TEST(BlueFS, test_simple_compaction_sync) {
  g_ceph_context->_conf.set_val(
    "bluefs_compact_log_sync",
    "true");
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    for (int i=0; i<10; i++) {
       string dir = "dir.";
       dir.append(to_string(i));
       ASSERT_EQ(0, fs.mkdir(dir));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          BlueFS::FileWriter *h;
          ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
          ASSERT_NE(nullptr, h);
          auto sg = make_scope_guard([&fs, h] { fs.close_writer(h); });
          bufferlist bl;
          std::unique_ptr<char[]> buf = gen_buffer(4096);
	  bufferptr bp = buffer::claim_char(4096, buf.get());
	  bl.push_back(bp);
          h->append(bl.c_str(), bl.length());
          fs.fsync(h);
       }
    }
  }
  {
    for (int i=0; i<10; i+=2) {
       string dir = "dir.";
       dir.append(to_string(i));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          fs.unlink(dir, file);
	  fs.sync_metadata(false);
       }
       ASSERT_EQ(0, fs.rmdir(dir));
       fs.sync_metadata(false);
    }
  }
  fs.compact_log();
  fs.umount();
}

TEST(BlueFS, test_simple_compaction_async) {
  g_ceph_context->_conf.set_val(
    "bluefs_compact_log_sync",
    "false");
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    for (int i=0; i<10; i++) {
       string dir = "dir.";
       dir.append(to_string(i));
       ASSERT_EQ(0, fs.mkdir(dir));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          BlueFS::FileWriter *h;
          ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
          ASSERT_NE(nullptr, h);
          auto sg = make_scope_guard([&fs, h] { fs.close_writer(h); });
          bufferlist bl;
          std::unique_ptr<char[]> buf = gen_buffer(4096);
	  bufferptr bp = buffer::claim_char(4096, buf.get());
	  bl.push_back(bp);
          h->append(bl.c_str(), bl.length());
          fs.fsync(h);
       }
    }
  }
  {
    for (int i=0; i<10; i+=2) {
       string dir = "dir.";
       dir.append(to_string(i));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          fs.unlink(dir, file);
	  fs.sync_metadata(false);
       }
       ASSERT_EQ(0, fs.rmdir(dir));
       fs.sync_metadata(false);
    }
  }
  fs.compact_log();
  fs.umount();
}

TEST(BlueFS, test_compaction_sync) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  g_ceph_context->_conf.set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf.set_val(
    "bluefs_compact_log_sync",
    "true");
  const char* canary_dir = "dir.after_compact_test";
  const char* canary_file = "file.after_compact_test";
  const char* canary_data = "some random data";

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    std::vector<std::thread> write_threads;
    uint64_t effective_size = size - (32 * 1048576); // leaving the last 32 MB for log compaction
    uint64_t per_thread_bytes = (effective_size/(NUM_WRITERS));
    for (int i=0; i<NUM_WRITERS; i++) {
      write_threads.push_back(std::thread(write_data, std::ref(fs), per_thread_bytes));
    }

    std::vector<std::thread> sync_threads;
    for (int i=0; i<NUM_SYNC_THREADS; i++) {
      sync_threads.push_back(std::thread(sync_fs, std::ref(fs)));
    }

    join_all(write_threads);
    writes_done = true;
    join_all(sync_threads);
    fs.compact_log();

    {
      ASSERT_EQ(0, fs.mkdir(canary_dir));
      BlueFS::FileWriter *h;
      ASSERT_EQ(0, fs.open_for_write(canary_dir, canary_file, &h, false));
      ASSERT_NE(nullptr, h);
      auto sg = make_scope_guard([&fs, h] { fs.close_writer(h); });
      h->append(canary_data, strlen(canary_data));
      int r = fs.fsync(h);
      ASSERT_EQ(r, 0);
    }
  }
  fs.umount();

  fs.mount();
  {
    BlueFS::FileReader *h;
    ASSERT_EQ(0, fs.open_for_read(canary_dir, canary_file, &h));
    ASSERT_NE(nullptr, h);
    bufferlist bl;
    ASSERT_EQ(strlen(canary_data), fs.read(h, 0, 1024, &bl, NULL));
    std::cout << bl.c_str() << std::endl;
    ASSERT_EQ(0, strncmp(canary_data, bl.c_str(), strlen(canary_data)));
    delete h;
  }
  fs.umount();
}

TEST(BlueFS, test_compaction_async) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  g_ceph_context->_conf.set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf.set_val(
    "bluefs_compact_log_sync",
    "false");
  const char* canary_dir = "dir.after_compact_test";
  const char* canary_file = "file.after_compact_test";
  const char* canary_data = "some random data";

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    std::vector<std::thread> write_threads;
    uint64_t effective_size = size - (32 * 1048576); // leaving the last 32 MB for log compaction
    uint64_t per_thread_bytes = (effective_size/(NUM_WRITERS));
    for (int i=0; i<NUM_WRITERS; i++) {
      write_threads.push_back(std::thread(write_data, std::ref(fs), per_thread_bytes));
    }

    std::vector<std::thread> sync_threads;
    for (int i=0; i<NUM_SYNC_THREADS; i++) {
      sync_threads.push_back(std::thread(sync_fs, std::ref(fs)));
    }

    join_all(write_threads);
    writes_done = true;
    join_all(sync_threads);
    fs.compact_log();

    {
      ASSERT_EQ(0, fs.mkdir(canary_dir));
      BlueFS::FileWriter *h;
      ASSERT_EQ(0, fs.open_for_write(canary_dir, canary_file, &h, false));
      ASSERT_NE(nullptr, h);
      auto sg = make_scope_guard([&fs, h] { fs.close_writer(h); });
      h->append(canary_data, strlen(canary_data));
      int r = fs.fsync(h);
      ASSERT_EQ(r, 0);
    }
  }
  fs.umount();

  fs.mount();
  {
    BlueFS::FileReader *h;
    ASSERT_EQ(0, fs.open_for_read(canary_dir, canary_file, &h));
    ASSERT_NE(nullptr, h);
    bufferlist bl;
    ASSERT_EQ(strlen(canary_data), fs.read(h, 0, 1024, &bl, NULL));
    std::cout << bl.c_str() << std::endl;
    ASSERT_EQ(0, strncmp(canary_data, bl.c_str(), strlen(canary_data)));
    delete h;
  }
  fs.umount();
}

TEST(BlueFS, test_replay) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  g_ceph_context->_conf.set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf.set_val(
    "bluefs_compact_log_sync",
    "false");

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    std::vector<std::thread> write_threads;
    uint64_t effective_size = size - (32 * 1048576); // leaving the last 32 MB for log compaction
    uint64_t per_thread_bytes = (effective_size/(NUM_WRITERS));
    for (int i=0; i<NUM_WRITERS; i++) {
      write_threads.push_back(std::thread(write_data, std::ref(fs), per_thread_bytes));
    }

    std::vector<std::thread> sync_threads;
    for (int i=0; i<NUM_SYNC_THREADS; i++) {
      sync_threads.push_back(std::thread(sync_fs, std::ref(fs)));
    }

    join_all(write_threads);
    writes_done = true;
    join_all(sync_threads);
    fs.compact_log();
  }
  fs.umount();
  // remount and check log can replay safe?
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  fs.umount();
}

TEST(BlueFS, test_replay_growth) {
  uint64_t size = 1048576LL * (2 * 1024 + 128);
  TempBdev bdev{size};

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_alloc_size", "4096");
  conf.SetVal("bluefs_shared_alloc_size", "4096");
  conf.SetVal("bluefs_compact_log_sync", "false");
  conf.SetVal("bluefs_min_log_runway", "32768");
  conf.SetVal("bluefs_max_log_runway", "65536");
  conf.SetVal("bluefs_allocator", "stupid");
  conf.SetVal("bluefs_sync_write", "true");
  conf.ApplyChanges();

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mkdir("dir"));

  char data[2000] = {'x'};
  BlueFS::FileWriter *h;
  ASSERT_EQ(0, fs.open_for_write("dir", "file", &h, false));
  for (size_t i = 0; i < 10000; i++) {
    h->append(data, 2000);
    fs.fsync(h);
  }
  fs.close_writer(h);
  fs.umount(true); //do not compact on exit!

  // remount and check log can replay safe?
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  fs.umount();
}

TEST(BlueFS, test_tracker_50965) {
  uint64_t size_wal = 1048576 * 64;
  TempBdev bdev_wal{size_wal};
  uint64_t size_db = 1048576 * 128;
  TempBdev bdev_db{size_db};
  uint64_t size_slow = 1048576 * 256;
  TempBdev bdev_slow{size_slow};

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.ApplyChanges();

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_WAL,  bdev_wal.path,  false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB,   bdev_db.path,   false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_SLOW, bdev_slow.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, true, true }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, true, true }));

  string dir_slow = "dir.slow";
  ASSERT_EQ(0, fs.mkdir(dir_slow));
  string dir_db = "dir_db";
  ASSERT_EQ(0, fs.mkdir(dir_db));

  string file_slow = "file";
  BlueFS::FileWriter *h_slow;
  ASSERT_EQ(0, fs.open_for_write(dir_slow, file_slow, &h_slow, false));
  ASSERT_NE(nullptr, h_slow);

  string file_db = "file";
  BlueFS::FileWriter *h_db;
  ASSERT_EQ(0, fs.open_for_write(dir_db, file_db, &h_db, false));
  ASSERT_NE(nullptr, h_db);

  bufferlist bl1;
  std::unique_ptr<char[]> buf1 = gen_buffer(70000);
  bufferptr bp1 = buffer::claim_char(70000, buf1.get());
  bl1.push_back(bp1);
  h_slow->append(bl1.c_str(), bl1.length());
  fs.flush(h_slow);

  uint64_t h_slow_dirty_seq_1 = fs.debug_get_dirty_seq(h_slow);

  bufferlist bl2;
  std::unique_ptr<char[]> buf2 = gen_buffer(1000);
  bufferptr bp2 = buffer::claim_char(1000, buf2.get());
  bl2.push_back(bp2);
  h_db->append(bl2.c_str(), bl2.length());
  fs.fsync(h_db);

  uint64_t h_slow_dirty_seq_2 = fs.debug_get_dirty_seq(h_slow);
  bool h_slow_dev_dirty = fs.debug_get_is_dev_dirty(h_slow, BlueFS::BDEV_SLOW);

  //problem if allocations are stable in log but slow device is not flushed yet
  ASSERT_FALSE(h_slow_dirty_seq_1 != 0 &&
	       h_slow_dirty_seq_2 == 0 &&
	       h_slow_dev_dirty == true);

  fs.close_writer(h_slow);
  fs.close_writer(h_db);

  fs.umount();
}

// borrowed from store_test
static bool bl_eq(bufferlist& expected, bufferlist& actual) {
  if (expected.contents_equal(actual))
    return true;

  unsigned first = 0;
  if(expected.length() != actual.length()) {
    cout << "--- buffer lengths mismatch " << std::hex
         << "expected 0x" << expected.length() << " != actual 0x"
         << actual.length() << std::dec << std::endl;
  }
  auto len = std::min(expected.length(), actual.length());
  while ( first<len && expected[first] == actual[first])
    ++first;
  unsigned last = len;
  while (last > 0 && expected[last-1] == actual[last-1])
    --last;
  if(len > 0) {
    cout << "--- buffer mismatch between offset 0x" << std::hex << first
         << " and 0x" << last << ", total 0x" << len << std::dec
         << std::endl;
    cout << "--- expected:\n";
    expected.hexdump(cout);
    cout << "--- actual:\n";
    actual.hexdump(cout);
  }
  return false;
}

TEST(BlueFS, test_wal_write) {
  uint64_t size_wal = 1048576 * 64;
  TempBdev bdev_wal{size_wal};
  uint64_t size_db = 1048576 * 128;
  TempBdev bdev_db{size_db};
  uint64_t size_slow = 1048576 * 256;
  TempBdev bdev_slow{size_slow};

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.ApplyChanges();

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_WAL,  bdev_wal.path,  false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB,   bdev_db.path,   false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_SLOW, bdev_slow.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, true, true }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, true, true }));

  string dir_db = "db.wal";
  ASSERT_EQ(0, fs.mkdir(dir_db));

  string wal_file = "wal1.log";
  BlueFS::FileWriter *writer;
  ASSERT_EQ(0, fs.open_for_write(dir_db, wal_file, &writer, false));
  ASSERT_NE(nullptr, writer);

  bufferlist bl1;
  auto gen_debugable = [](size_t amount, bufferlist& bl) {
    for (size_t i = 0; i < amount; i++) {
      bl.append('a');
    }
  };
  gen_debugable(70000, bl1);
  fs.append_try_flush(writer, bl1.c_str(), bl1.length());
  fs.fsync(writer);
  fs.close_writer(writer);

  // WAL files don't update internal extents while writing to save memory, only on _replay
  fs.umount();
  fs.mount();

  BlueFS::FileReader *reader;
  ASSERT_EQ(0, fs.open_for_read(dir_db, wal_file, &reader));
  bufferlist read_bl;
  fs.read(reader, 0, 70000, &read_bl, NULL);
  ASSERT_TRUE(bl_eq(bl1, read_bl));
  delete reader;
  fs.umount();

}


class BlueFS_wal : virtual public ::testing::Test {
public:
  BlueFS fs;
  explicit BlueFS_wal() :
    fs(g_ceph_context)
  {}
  void SetUp() override {};
  void TearDown() override {};
  void Create(uint64_t slow, uint64_t db, uint64_t wal) {
    if (slow > 0) {
      bdev_slow.Init(slow);
      ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_SLOW, bdev_slow.path, false));
    }
    if (db > 0) {
      bdev_db.Init(db);
      ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev_db.path, false));
    }
    if (wal > 0) {
      bdev_wal.Init(wal);
      ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_WAL, bdev_wal.path,  false));
    }
    ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, true, true }));
  }
  TempBdev bdev_wal;
  TempBdev bdev_db;
  TempBdev bdev_slow;
  uuid_d fsid;
  void many_small_writes(
    const std::string& dir,
    const std::string& file,
    bufferlist& content,
    uint32_t min_size = 10000,
    uint32_t chunk_from = 1,
    uint32_t chunk_to = 10,
    uint32_t chunk_step = 1)
  {
    int r = fs.mkdir(dir);
    ASSERT_TRUE(r == 0 || r == -EEXIST);
    BlueFS::FileWriter *writer;
    ASSERT_EQ(0, fs.open_for_write(dir, file, &writer, false));
    ASSERT_NE(nullptr, writer);
    many_small_writes(writer, content, min_size,
      chunk_from, chunk_to, chunk_step);
    fs.close_writer(writer);
    //delete writer;
  }

  void many_small_writes(
    BlueFS::FileWriter *writer,
    bufferlist& content,
    uint32_t min_size = 10000,
    uint32_t chunk_from = 1,
    uint32_t chunk_to = 10,
    uint32_t chunk_step = 1)
  {
    uint32_t c = chunk_from;
    uint8_t v = 0;
    ASSERT_NE(nullptr, writer);
    do {
      std::string data(c, v);
      fs.append_try_flush(writer, data.c_str(), data.length());
      fs.fsync(writer);
      content.append(data);
      v++;
      c += chunk_step;
      if (c > chunk_to) {
        c = chunk_from;
      }
    } while (content.length() < min_size);
    //delete writer;
  }

  void many_small_reads(
    const std::string& dir,
    const std::string& file,
    bufferlist& content,
    uint32_t min_size = 10000,
    uint32_t chunk_from = 1,
    uint32_t chunk_to = 10,
    uint32_t chunk_step = 1)
  {
    content.clear();
    BlueFS::FileReader *reader;
    ASSERT_EQ(0, fs.open_for_read(dir, file, &reader));
    many_small_reads(reader, content, min_size,
      chunk_from, chunk_to, chunk_step);
    delete reader;
  }

  void many_small_reads(
    BlueFS::FileReader *reader,
    bufferlist& content,
    uint32_t min_size = 10000,
    uint32_t chunk_from = 1,
    uint32_t chunk_to = 10,
    uint32_t chunk_step = 1)
  {
    uint32_t c = chunk_from;
    uint32_t pos = 0;
    do {
      bufferlist read_bl;
      int r = fs.read(reader, pos, c, &read_bl, nullptr);
      if (r <= 0) break;
      content.append(read_bl);
      pos = pos + c;
      c += chunk_step;
      if (c > chunk_to) {
        c = chunk_from;
      }
    } while (content.length() < min_size);
  }
};

TEST(BlueFS, test_wal_migrate) {
  uint64_t size_wal = 1048576 * 64;
  TempBdev bdev_wal{size_wal};
  uint64_t size_db = 1048576 * 128;
  TempBdev bdev_db{size_db};
  uint64_t size_slow = 1048576 * 256;
  TempBdev bdev_slow{size_slow};

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.ApplyChanges();

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_WAL,  bdev_wal.path,  false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB,   bdev_db.path,   false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_SLOW, bdev_slow.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, true, true }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, true, true }));

  string dir_db = "db.wal";
  ASSERT_EQ(0, fs.mkdir(dir_db));

  string wal_file = "wal1.log";
  BlueFS::FileWriter *writer;
  ASSERT_EQ(0, fs.open_for_write(dir_db, wal_file, &writer, false));
  ASSERT_NE(nullptr, writer);

  bufferlist bl1;
  auto gen_debugable = [](size_t amount, bufferlist& bl) {
    for (size_t i = 0; i < amount; i++) {
      bl.append('a');
    }
  };
  gen_debugable(70000, bl1);
  fs.append_try_flush(writer, bl1.c_str(), bl1.length());
  fs.fsync(writer);
  fs.close_writer(writer);
  // WAL files don't update internal extents while writing to save memory, only on _replay
  fs.umount();
  fs.mount();
  fs.revert_wal_to_plain();

  BlueFS::FileReader *reader;
  ASSERT_EQ(0, fs.open_for_read(dir_db, wal_file, &reader));
  ASSERT_EQ(reader->file->fnode.encoding, bluefs_node_encoding::PLAIN);
  ASSERT_EQ(reader->file->envelope_mode(), false);

  bufferlist read_bl;
  fs.read(reader, 0, 70000, &read_bl, NULL);
  ASSERT_TRUE(bl_eq(bl1, read_bl));
  delete reader;
  fs.umount();
}


TEST_F(BlueFS_wal, wal_v2_check)
{
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.SetVal("bluefs_wal_envelope_mode", "true");
  conf.ApplyChanges();

  Create(1048576 * 256, 1048576 * 128, 1048576 * 64);
  ASSERT_EQ(0, fs.mount());

  bufferlist content;
  many_small_writes("db.wal", "wal1.log", content, 10);
  fs.umount();
  fs.mount();
  bufferlist read_content;
  many_small_reads("db.wal", "wal1.log", read_content, 10);
  ASSERT_EQ(content, read_content);
  fs.umount();
}

TEST_F(BlueFS_wal, wal_v2_check_feature)
{
  SKIP_JENKINS();
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.SetVal("bluefs_wal_envelope_mode", "true");
  conf.ApplyChanges();

  Create(1048576 * 256, 1048576 * 128, 1048576 * 64);
  ASSERT_EQ(0, fs.mount());
  mono_clock::time_point t0, t1;
  bufferlist content;
  uint32_t cnt = 125;
  t1 = mono_clock::now();
  double d_w1, d_w2;
  // calibrate cnt to run more than 1s
  do
  {
    content.clear();
    cnt = cnt * 2;
    fs.unlink("db.wal", "wal.log");
    t0 = mono_clock::now();
    many_small_writes("db.wal", "wal.log", content, cnt, 1, 1, 1);
    t1 = mono_clock::now();
    d_w2 = std::chrono::duration<double>(t1 - t0).count();
  } while (d_w2 < 1.0);
  cout << "time wal envelope_mode=" << d_w2 << std::endl;

  t0 = mono_clock::now();
  content.clear();
  many_small_writes("db.wal", "not-wal.xxx", content, cnt, 1, 1, 1);
  t1 = mono_clock::now();
  d_w1 = std::chrono::duration<double>(t1 - t0).count();
  cout << "time wal basic=" << d_w1 << std::endl;
  fs.umount();
  //if not 20% faster, its not working
  EXPECT_LT(d_w2 * 1.2, d_w1);
}

TEST_F(BlueFS_wal, wal_v2_truncate)
{
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.SetVal("bluefs_wal_envelope_mode", "true");
  conf.ApplyChanges();

  Create(1048576 * 256, 1048576 * 128, 1048576 * 64);
  ASSERT_EQ(0, fs.mount());
  std::string dir = "dir";
  std::string file = "wal.log";
  int r = fs.mkdir(dir);
  ASSERT_TRUE(r == 0 || r == -EEXIST);
  BlueFS::FileWriter *writer;
  ASSERT_EQ(0, fs.open_for_write(dir, file, &writer, false));
  ASSERT_NE(nullptr, writer);
  bufferlist content;
  many_small_writes(writer, content, 10);
  uint64_t size;
  fs.stat(dir, file, &size, nullptr);
  EXPECT_EQ(size, content.length());
  fs.truncate(writer, content.length());
  fs.close_writer(writer);
  fs.umount();
  fs.mount();
  fs.stat(dir, file, &size, nullptr);
  EXPECT_EQ(size, content.length());
  bufferlist read_content;
  many_small_reads(dir, file, read_content, 10);
  ASSERT_EQ(content, read_content);
  fs.umount();
}

TEST_F(BlueFS_wal, wal_v2_simulate_crash)
{
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.SetVal("bluefs_wal_envelope_mode", "true");
  conf.ApplyChanges();

  Create(1048576 * 256, 1048576 * 128, 1048576 * 64);
  ASSERT_EQ(0, fs.mount());
  std::string dir = "dir";
  std::string file = "wal.log";
  int r = fs.mkdir(dir);
  ASSERT_TRUE(r == 0 || r == -EEXIST);
  BlueFS::FileWriter *writer;
  ASSERT_EQ(0, fs.open_for_write(dir, file, &writer, false));
  ASSERT_NE(nullptr, writer);
  bufferlist content;
  many_small_writes(writer, content, 100);
  delete writer; //close without orderly shutdown, simulate failure
  fs.umount();
  fs.mount();
  bufferlist read_content;
  many_small_reads(dir, file, read_content, 100);
  ASSERT_EQ(content, read_content);
  fs.umount();
}

TEST_F(BlueFS_wal, wal_v2_recovery_from_dirty_allocated)
{
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_alloc_size", "4096");
  conf.SetVal("bluefs_shared_alloc_size", "4096");
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.SetVal("bluefs_wal_envelope_mode", "true");
  conf.ApplyChanges();

  Create(1048576 * 256, 1048576 * 128, 1048576 * 64);
  ASSERT_EQ(0, fs.mount());
  std::string dir = "dir";
  std::string file = "wal.log";
  int r = fs.mkdir(dir);
  ASSERT_TRUE(r == 0 || r == -EEXIST);
  BlueFS::FileWriter *writer;
  ASSERT_EQ(0, fs.open_for_write(dir, file, &writer, false));
  ASSERT_NE(nullptr, writer);
  bufferlist content;

  //preallocate and write "0xae" so ENVELOPE_MODE recovery will see length=0xaeaeaeaeaeaeaeae
  fs.preallocate(writer->file, 0, 4096 * 2);
  bluefs_extent_t ext = writer->file->fnode.extents[0];
  BlockDevice* x = fs.get_block_device(ext.bdev);
  ASSERT_EQ(ext.length, 4096 * 2);
  bufferlist filler;
  filler.append(string(4096 * 2, 0xae));
  x->write(ext.offset, filler, false);
  x->flush();

  many_small_writes(writer, content, 4096 / (48 + 8 + 8) * 48, 48, 48, 0);
  delete writer; //close without orderly shutdown, simulate failure
  fs.umount();
  fs.mount();
  bufferlist read_content;
  BlueFS::FileReader *h;
  ASSERT_EQ(0, fs.open_for_read(dir, file, &h));
  bufferlist bl;
  ASSERT_EQ(0, fs.read(h, 0xea01020304050607, 1, &bl, NULL)); // no read but no failure
  delete h;
  fs.umount();
}

TEST_F(BlueFS_wal, support_wal_v2_and_v1)
{
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.SetVal("bluefs_wal_envelope_mode", "true");
  conf.ApplyChanges();

  std::string dir = "dir";
  std::string file_v2 = "wal-v2.log";
  std::string file_v1 = "wal-v1.log";
  bufferlist content_v2;
  bufferlist content_v1;
  int r;
  Create(1048576 * 256, 1048576 * 128, 1048576 * 64);
  ASSERT_EQ(0, fs.mount());
  r = fs.mkdir(dir);
  ASSERT_TRUE(r == 0 || r == -EEXIST);
  many_small_writes(dir, file_v2, content_v2, 100);
  fs.umount();
  // switch to v1
  g_ceph_context->_conf.set_val("bluefs_wal_envelope_mode", "false");
  ASSERT_EQ(0, fs.mount());
  r = fs.mkdir(dir);
  ASSERT_TRUE(r == 0 || r == -EEXIST);
  many_small_writes(dir, file_v1, content_v1, 100);
  fs.umount();

  fs.mount();
  bufferlist read_content;
  BlueFS::FileReader *reader;
  many_small_reads(dir, file_v2, read_content, 100);
  EXPECT_EQ(content_v2, read_content);
  ASSERT_EQ(0, fs.open_for_read(dir, file_v2, &reader));
  EXPECT_EQ(reader->file->envelope_mode(), true);
  delete reader;
  reader = nullptr;

  many_small_reads(dir, file_v1, read_content, 100);
  EXPECT_EQ(content_v1, read_content);
  ASSERT_EQ(0, fs.open_for_read(dir, file_v1, &reader));
  EXPECT_EQ(reader->file->envelope_mode(), false);
  delete reader;

  fs.umount();
}

TEST_F(BlueFS_wal, wal_v2_read_after_write)
{
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.SetVal("bluefs_wal_envelope_mode", "true");
  conf.ApplyChanges();

  Create(1048576 * 256, 1048576 * 128, 1048576 * 64);
  ASSERT_EQ(0, fs.mount());
  std::string dir = "dir";
  std::string file = "wal.log";
  int r = fs.mkdir(dir);
  ASSERT_TRUE(r == 0 || r == -EEXIST);
  BlueFS::FileWriter *writer;
  ASSERT_EQ(0, fs.open_for_write(dir, file, &writer, false));
  ASSERT_NE(nullptr, writer);
  bufferlist content;
  many_small_writes(writer, content, 10);
  uint64_t size;
  fs.stat(dir, file, &size, nullptr);
  EXPECT_EQ(size, content.length());
  fs.close_writer(writer);
  fs.stat(dir, file, &size, nullptr);
  EXPECT_EQ(size, content.length());
  bufferlist read_content;
  many_small_reads(dir, file, read_content, 10);
  ASSERT_EQ(content, read_content);
  fs.umount();
}

TEST(BlueFS, test_wal_read_after_rollback_to_v1) {
  // test whether we still read with v2 version even though new files will be v1
  uint64_t size_wal = 1048576 * 64;
  TempBdev bdev_wal{size_wal};
  uint64_t size_db = 1048576 * 128;
  TempBdev bdev_db{size_db};
  uint64_t size_slow = 1048576 * 256;
  TempBdev bdev_slow{size_slow};

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.SetVal("bluefs_wal_envelope_mode", "true");
  conf.ApplyChanges();

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_WAL,  bdev_wal.path,  false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB,   bdev_db.path,   false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_SLOW, bdev_slow.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, true, true }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, true, true }));

  string dir_db = "db.wal";
  ASSERT_EQ(0, fs.mkdir(dir_db));

  string wal_file = "wal1.log";
  BlueFS::FileWriter *writer;
  ASSERT_EQ(0, fs.open_for_write(dir_db, wal_file, &writer, false));
  ASSERT_EQ(writer->file->fnode.encoding, bluefs_node_encoding::ENVELOPE);
  ASSERT_NE(nullptr, writer);

  bufferlist bl1;
  auto gen_debugable = [](size_t amount, bufferlist& bl) {
    for (size_t i = 0; i < amount; i++) {
      bl.append('a');
    }
  };
  gen_debugable(70000, bl1);
  fs.append_try_flush(writer, bl1.c_str(), bl1.length());
  fs.fsync(writer);
  fs.close_writer(writer);

  g_ceph_context->_conf.set_val("bluefs_wal_envelope_mode", "false");
  fs.umount();
  fs.mount();

  BlueFS::FileReader *reader;
  ASSERT_EQ(0, fs.open_for_read(dir_db, wal_file, &reader));
  bufferlist read_bl;
  fs.read(reader, 0, 70000, &read_bl, NULL);
  ASSERT_TRUE(bl_eq(bl1, read_bl));
  delete reader;

  {
    // open another file to ensure v1 is set correctly
    string wal_file = "wal2.log";
    BlueFS::FileWriter *writer;
    ASSERT_EQ(0, fs.open_for_write(dir_db, wal_file, &writer, false));
    ASSERT_NE(nullptr, writer);
    ASSERT_EQ(writer->file->fnode.encoding, bluefs_node_encoding::PLAIN);
    fs.close_writer(writer);
  }
  fs.umount();
}


TEST(BlueFS, test_truncate_stable_53129) {

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_min_flush_size", "65536");
  conf.ApplyChanges();

  uint64_t size_wal = 1048576 * 64;
  TempBdev bdev_wal{size_wal};
  uint64_t size_db = 1048576 * 128;
  TempBdev bdev_db{size_db};
  uint64_t size_slow = 1048576 * 256;
  TempBdev bdev_slow{size_slow};

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_WAL,  bdev_wal.path,  false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB,   bdev_db.path,   false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_SLOW, bdev_slow.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, true, true }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, true, true }));

  string dir_slow = "dir.slow";
  ASSERT_EQ(0, fs.mkdir(dir_slow));
  string dir_db = "dir_db";
  ASSERT_EQ(0, fs.mkdir(dir_db));

  string file_slow = "file";
  BlueFS::FileWriter *h_slow;
  ASSERT_EQ(0, fs.open_for_write(dir_slow, file_slow, &h_slow, false));
  ASSERT_NE(nullptr, h_slow);

  string file_db = "file";
  BlueFS::FileWriter *h_db;
  ASSERT_EQ(0, fs.open_for_write(dir_db, file_db, &h_db, false));
  ASSERT_NE(nullptr, h_db);

  bufferlist bl1;
  std::unique_ptr<char[]> buf1 = gen_buffer(70000);
  bufferptr bp1 = buffer::claim_char(70000, buf1.get());
  bl1.push_back(bp1);
  // add 70000 bytes
  h_slow->append(bl1.c_str(), bl1.length());
  fs.flush(h_slow);
  // and truncate to 60000 bytes
  fs.truncate(h_slow, 60000);

  // write something to file on DB device
  bufferlist bl2;
  std::unique_ptr<char[]> buf2 = gen_buffer(1000);
  bufferptr bp2 = buffer::claim_char(1000, buf2.get());
  bl2.push_back(bp2);
  h_db->append(bl2.c_str(), bl2.length());
  // and force bluefs log to flush
  fs.fsync(h_db);

  // This is the actual test point.
  // We completed truncate, and we expect
  // - size to be 60000
  // - data to be stable on slow device
  // OR
  // - size = 0 or file does not exist
  // - dev_dirty is irrelevant
  bool h_slow_dev_dirty = fs.debug_get_is_dev_dirty(h_slow, BlueFS::BDEV_SLOW);
  // Imagine power goes down here.

  fs.close_writer(h_slow);
  fs.close_writer(h_db);

  fs.umount();

  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, true, true }));

  uint64_t size;
  utime_t mtime;
  ASSERT_EQ(0, fs.stat("dir.slow", "file", &size, &mtime));
  // check file size 60000
  ASSERT_EQ(size, 60000);
  // check that dev_dirty was false (data stable on media)
  ASSERT_EQ(h_slow_dev_dirty, false);

  fs.umount();
}

TEST(BlueFS, test_update_ino1_delta_after_replay) {
  uint64_t size = 1048576LL * (2 * 1024 + 128);
  TempBdev bdev{size};

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_alloc_size", "4096");
  conf.SetVal("bluefs_shared_alloc_size", "4096");
  conf.SetVal("bluefs_compact_log_sync", "false");
  conf.SetVal("bluefs_min_log_runway", "32768");
  conf.SetVal("bluefs_max_log_runway", "65536");
  conf.SetVal("bluefs_allocator", "stupid");
  conf.ApplyChanges();

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mkdir("dir"));

  char data[2000] = {'a'};
  BlueFS::FileWriter *h;
  ASSERT_EQ(0, fs.open_for_write("dir", "file", &h, false));
  for (size_t i = 0; i < 100; i++) {
    h->append(data, 2000);
    fs.fsync(h);
  }
  fs.close_writer(h);
  fs.umount(true); //do not compact on exit!

  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.open_for_write("dir", "file2", &h, false));
  for (size_t i = 0; i < 100; i++) {
    h->append(data, 2000);
    fs.fsync(h);
  }
  fs.close_writer(h);
  fs.umount();

  // remount and check log can replay safe?
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  fs.umount();
}

TEST(BlueFS, broken_unlink_fsync_seq) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    /*
    * This reproduces a weird file op sequence (unlink+fsync) that Octopus
    * RocksDB might issue to BlueFS when recycle_log_file_num setting is 0
    * See https://tracker.ceph.com/issues/55636 for more details
    *
    */
    char buf[1048571]; // this is biggish, but intentionally not evenly aligned
    for (unsigned i = 0; i < sizeof(buf); ++i) {
      buf[i] = i;
    }
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write("dir", "file", &h, false));

    h->append(buf, sizeof(buf));
    fs.flush(h);
    h->append(buf, sizeof(buf));
    fs.unlink("dir", "file");
    fs.fsync(h);
    fs.close_writer(h);
  }
  fs.umount();

  // remount and check log can replay safe?
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  fs.umount();
}

TEST(BlueFS, truncate_fsync) {
  uint64_t bdev_size = 128 * 1048576;
  uint64_t block_size = 4096;
  TempBdev bdev{bdev_size};
  uuid_d fsid;
  const char* DIR_NAME="dir";
  const char* FILE_NAME="file1";

  size_t sizes[] = {3, 1024, 4096, 1024 * 4096};
  for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++) {
    const size_t content_size= sizes[i];
    const size_t read_size = p2roundup(content_size, size_t(block_size));
    const std::string content(content_size, 'x');
    {
      BlueFS fs(g_ceph_context);
      ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
      ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
      ASSERT_EQ(0, fs.mount());
      ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
      {
        BlueFS::FileWriter *h;
        ASSERT_EQ(0, fs.mkdir("dir"));
        ASSERT_EQ(0, fs.open_for_write(DIR_NAME, FILE_NAME, &h, false));
        h->append(content.c_str(), content.length());
        fs.fsync(h);
        fs.close_writer(h);
      }
      {
        BlueFS::FileReader *h;
        ASSERT_EQ(0, fs.open_for_read(DIR_NAME, FILE_NAME, &h));
        bufferlist bl;
        ASSERT_EQ(content.length(), fs.read(h, 0, read_size, &bl, NULL));
        ASSERT_EQ(0, strncmp(content.c_str(), bl.c_str(), content.length()));
        delete h;
      }
      {
        BlueFS::FileWriter *h;
        ASSERT_EQ(0, fs.open_for_write(DIR_NAME, FILE_NAME, &h, true));
        fs.truncate(h, 0);
        fs.fsync(h);
        fs.close_writer(h);
      }
      fs.umount();
    }
    {
      //this was broken due to https://tracker.ceph.com/issues/55307
      BlueFS fs(g_ceph_context);
      ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
      ASSERT_EQ(0, fs.mount());
      BlueFS::FileReader *h;
      ASSERT_EQ(0, fs.open_for_read(DIR_NAME, FILE_NAME, &h));
      bufferlist bl;
      ASSERT_EQ(0, fs.read(h, 0, read_size, &bl, NULL));
      delete h;
      fs.umount();
    }
  }
}

TEST(BlueFS, test_shared_alloc) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev_slow{size};
  uint64_t size_db = 1048576 * 8;
  TempBdev bdev_db{size_db};

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_shared_alloc_size", "1048576");

  bluefs_shared_alloc_context_t shared_alloc;
  uint64_t shared_alloc_unit = 4096;
  shared_alloc.set(
    Allocator::create(g_ceph_context, g_ceph_context->_conf->bluefs_allocator,
                      size, shared_alloc_unit, "test shared allocator"),
    shared_alloc_unit);
  shared_alloc.a->init_add_free(0, size);
  auto sg = make_scope_guard([&shared_alloc] { delete shared_alloc.a; });
  BlueFS fs(g_ceph_context);
  // DB device is fully utilized
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev_db.path, false));
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_SLOW, bdev_slow.path, false,
                                   &shared_alloc));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    for (int i=0; i<10; i++) {
       string dir = "dir.";
       dir.append(to_string(i));
       ASSERT_EQ(0, fs.mkdir(dir));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          BlueFS::FileWriter *h;
          ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
          ASSERT_NE(nullptr, h);
          auto sg = make_scope_guard([&fs, h] { fs.close_writer(h); });
          bufferlist bl;
          std::unique_ptr<char[]> buf = gen_buffer(4096);
	  bufferptr bp = buffer::claim_char(4096, buf.get());
	  bl.push_back(bp);
          h->append(bl.c_str(), bl.length());
          fs.fsync(h);
       }
    }
  }
  {
    for (int i=0; i<10; i+=2) {
       string dir = "dir.";
       dir.append(to_string(i));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          fs.unlink(dir, file);
	  fs.sync_metadata(false);
       }
       ASSERT_EQ(0, fs.rmdir(dir));
       fs.sync_metadata(false);
    }
  }
  fs.compact_log();
  uint64_t num_files = 0;
  {
    auto *logger = fs.get_perf_counters();
    ASSERT_NE(logger->get(l_bluefs_alloc_shared_dev_fallbacks), 0);
    num_files = logger->get(l_bluefs_num_files);
    fs.umount();
  }
  {
    fs.mount();
    auto *logger = fs.get_perf_counters();
    ASSERT_EQ(num_files, logger->get(l_bluefs_num_files));
    fs.umount();
  }
}

TEST(BlueFS, test_shared_alloc_sparse) {
  uint64_t size = 1048576 * 128 * 2;
  uint64_t main_unit = 4096;
  uint64_t bluefs_alloc_unit = 1048576;
  TempBdev bdev_slow{size};

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_shared_alloc_size",
    stringify(bluefs_alloc_unit).c_str());

  bluefs_shared_alloc_context_t shared_alloc;
  shared_alloc.set(
    Allocator::create(g_ceph_context, g_ceph_context->_conf->bluefs_allocator,
                      size, main_unit, "test shared allocator"),
    main_unit);
  // prepare sparse free space but let's have a continuous chunk at
  // the beginning to fit initial log's fnode into superblock,
  // we don't have any tricks to deal with sparse allocations
  // (and hence long fnode) at mkfs
  shared_alloc.a->init_add_free(bluefs_alloc_unit, 4 * bluefs_alloc_unit);
  for(uint64_t i = 5 * bluefs_alloc_unit; i < size; i += 2 * main_unit) {
    shared_alloc.a->init_add_free(i, main_unit);
  }
  auto sg = make_scope_guard([&shared_alloc] { delete shared_alloc.a; });

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev_slow.path, false,
                                   &shared_alloc));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    for (int i=0; i<10; i++) {
       string dir = "dir.";
       dir.append(to_string(i));
       ASSERT_EQ(0, fs.mkdir(dir));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          BlueFS::FileWriter *h;
          ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
          ASSERT_NE(nullptr, h);
          auto sg = make_scope_guard([&fs, h] { fs.close_writer(h); });
          bufferlist bl;
          std::unique_ptr<char[]> buf = gen_buffer(4096);
	  bufferptr bp = buffer::claim_char(4096, buf.get());
	  bl.push_back(bp);
          h->append(bl.c_str(), bl.length());
          fs.fsync(h);
       }
    }
  }
  {
    for (int i=0; i<10; i+=2) {
       string dir = "dir.";
       dir.append(to_string(i));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          fs.unlink(dir, file);
	  fs.sync_metadata(false);
       }
       ASSERT_EQ(0, fs.rmdir(dir));
       fs.sync_metadata(false);
    }
  }
  fs.compact_log();

  uint64_t num_files = 0;
  {
    auto *logger = fs.get_perf_counters();
    ASSERT_NE(logger->get(l_bluefs_alloc_shared_size_fallbacks), 0);
    num_files = logger->get(l_bluefs_num_files);
    fs.umount();
  }
  {
    fs.mount();
    auto *logger = fs.get_perf_counters();
    ASSERT_EQ(num_files, logger->get(l_bluefs_num_files));
    fs.umount();
  }
}

TEST(BlueFS, test_4k_shared_alloc) {
  uint64_t size = 1048576 * 128 * 2;
  uint64_t main_unit = 4096;
  uint64_t bluefs_alloc_unit = main_unit;
  TempBdev bdev_slow{size};

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_shared_alloc_size",
    stringify(bluefs_alloc_unit).c_str());

  bluefs_shared_alloc_context_t shared_alloc;
  shared_alloc.set(
    Allocator::create(g_ceph_context, g_ceph_context->_conf->bluefs_allocator,
                      size, main_unit, "test shared allocator"),
    main_unit);
  shared_alloc.a->init_add_free(bluefs_alloc_unit, size - bluefs_alloc_unit);
  auto sg = make_scope_guard([&shared_alloc] { delete shared_alloc.a; });

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev_slow.path, false,
                                   &shared_alloc));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  {
    for (int i=0; i<10; i++) {
       string dir = "dir.";
       dir.append(to_string(i));
       ASSERT_EQ(0, fs.mkdir(dir));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          BlueFS::FileWriter *h;
          ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
          ASSERT_NE(nullptr, h);
          auto sg = make_scope_guard([&fs, h] { fs.close_writer(h); });
          bufferlist bl;
          std::unique_ptr<char[]> buf = gen_buffer(4096);
	  bufferptr bp = buffer::claim_char(4096, buf.get());
	  bl.push_back(bp);
          h->append(bl.c_str(), bl.length());
          fs.fsync(h);
       }
    }
  }
  {
    for (int i=0; i<10; i+=2) {
       string dir = "dir.";
       dir.append(to_string(i));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          fs.unlink(dir, file);
	  fs.sync_metadata(false);
       }
       ASSERT_EQ(0, fs.rmdir(dir));
       fs.sync_metadata(false);
    }
  }
  fs.compact_log();

  uint64_t num_files = 0;
  {
    auto *logger = fs.get_perf_counters();
    ASSERT_EQ(logger->get(l_bluefs_alloc_shared_dev_fallbacks), 0);
    ASSERT_EQ(logger->get(l_bluefs_alloc_shared_size_fallbacks), 0);
    num_files = logger->get(l_bluefs_num_files);
    fs.umount();
  }
  {
    fs.mount();
    auto *logger = fs.get_perf_counters();
    ASSERT_EQ(num_files, logger->get(l_bluefs_num_files));
    fs.umount();
  }
}

void create_files(BlueFS &fs,
		  atomic_bool& stop_creating,
		  atomic_bool& started_creating)
{
  uint32_t i = 0;
  stringstream ss;
  string dir = "dir.";
  ss << std::this_thread::get_id();
  dir.append(ss.str());
  dir.append(".");
  dir.append(to_string(i));
  ASSERT_EQ(0, fs.mkdir(dir));
  while (!stop_creating.load()) {
    string file = "file.";
    file.append(to_string(i));
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
    ASSERT_NE(nullptr, h);
    fs.close_writer(h);
    i++;
    started_creating = true;
  }
}


TEST(BlueFS, test_concurrent_dir_link_and_compact_log_56210) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  ConfSaver conf(g_ceph_context->_conf);

  conf.SetVal("bluefs_alloc_size", "65536");
  conf.SetVal("bluefs_compact_log_sync", "false");
  // make sure fsync always trigger log compact
  conf.SetVal("bluefs_log_compact_min_ratio", "0");
  conf.SetVal("bluefs_log_compact_min_size", "0");
  conf.ApplyChanges();

  for (int i=0; i<10; ++i) {
    BlueFS fs(g_ceph_context);
    ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
    uuid_d fsid;
    ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
    ASSERT_EQ(0, fs.mount());
    ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
    {
      atomic_bool stop_creating{false};
      atomic_bool started_creating{false};
      std::thread create_thread;
      create_thread = std::thread(create_files,
				  std::ref(fs),
				  std::ref(stop_creating),
				  std::ref(started_creating));
      while (!started_creating.load()) {
      }
      BlueFS::FileWriter *h;
      ASSERT_EQ(0, fs.mkdir("foo"));
      ASSERT_EQ(0, fs.open_for_write("foo", "bar", &h, false));
      fs.fsync(h);
      fs.close_writer(h);

      stop_creating = true;
      do_join(create_thread);

      fs.umount(true); //do not compact on exit!
      ASSERT_EQ(0, fs.mount());
      fs.umount();
    }
  }
}

TEST(BlueFS, truncate_drops_allocations) {
  constexpr uint64_t K = 1024;
  constexpr uint64_t M = 1024 * K;
  uuid_d fsid;
  const char* DIR_NAME="dir";
  const char* FILE_NAME="file1";
  struct {
    uint64_t preallocated_size;
    uint64_t write_size;
    uint64_t truncate_to;
    uint64_t allocated_after_truncate;
    uint64_t slow_size = 0;
    uint64_t slow_alloc_size = 64*K;
    uint64_t db_size = 128*M;
    uint64_t db_alloc_size = 1*M;
  } scenarios [] = {
    // on DB(which is SLOW) : 1 => 1, 64K remains
    { 1*M, 1, 1, 64*K },
    // on DB(which is SLOW), alloc 4K : 1 => 1, 4K remains
    { 1*M, 1, 1, 4*K, 0, 4*K },
    // on DB(which is SLOW), truncation on AU boundary : 128K => 128K, 128K remains
    { 1*M, 128*K, 128*K, 128*K },
    // on DB(which is SLOW), no prealloc, truncation to 0 : 1666K => 0, 0 remains
    { 0, 1666*K, 0, 0 },
    // on DB, truncate to 123K, expect 1M occupied
    { 1234*K, 123*K, 123*K, 1*M, 128*M, 64*K, 10*M, 1*M },
    // on DB, truncate to 0, expect 0 occupied
    { 1234*K, 345*K, 0, 0, 128*M, 64*K, 10*M, 1*M },
    // on DB, truncate to AU boundary, expect exactly 1M occupied
    { 1234*K, 1123*K, 1*M, 1*M, 128*M, 64*K, 10*M, 1*M },
    // on DB and SLOW, truncate only data on SLOW
    { 0, 10*M+1, 10*M+1, 10*M+64*K, 128*M, 64*K, 10*M, 1*M },
    // on DB and SLOW, preallocate and truncate only data on SLOW
    { 6*M, 12*M, 10*M+1, 10*M+64*K, 128*M, 64*K, 10*M, 1*M },
    // on DB and SLOW, preallocate and truncate all in SLOW and some on DB
    // note! prealloc 6M is important, one allocation for 12M will fallback to SLOW
    // in 6M + 6M we can be sure that 6M is on DB and 6M is on SLOW
    { 6*M, 12*M, 3*M+1, 4*M, 128*M, 64*K, 11*M, 1*M },
  };
  for (auto& s : scenarios) {
    ConfSaver conf(g_ceph_context->_conf);
    conf.SetVal("bluefs_shared_alloc_size", stringify(s.slow_alloc_size).c_str());
    conf.SetVal("bluefs_alloc_size", stringify(s.db_alloc_size).c_str());

    g_ceph_context->_conf.set_val("bluefs_shared_alloc_size", stringify(s.slow_alloc_size));
    g_ceph_context->_conf.set_val("bluefs_alloc_size", stringify(s.db_alloc_size));
    TempBdev bdev_db{s.db_size};
    TempBdev bdev_slow{s.slow_size};

    bluefs_shared_alloc_context_t shared_alloc;
    uint64_t shared_alloc_unit = s.slow_alloc_size;
    shared_alloc.set(
      Allocator::create(g_ceph_context, g_ceph_context->_conf->bluefs_allocator,
                        s.slow_size ? s.slow_size : s.db_size,
                        shared_alloc_unit, "test shared allocator"),
      shared_alloc_unit);
    shared_alloc.a->init_add_free(0, s.slow_size ? s.slow_size : s.db_size);

    BlueFS fs(g_ceph_context);
    if (s.db_size != 0) {
      ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev_db.path, false,
      s.slow_size > 0 ? nullptr : &shared_alloc));
    }
    if (s.slow_size != 0) {
      ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_SLOW, bdev_slow.path, false, &shared_alloc));
    }

    ASSERT_EQ(0, fs.mkfs(fsid, {BlueFS::BDEV_DB, false, false}));
    ASSERT_EQ(0, fs.mount());
    ASSERT_EQ(0, fs.maybe_verify_layout({BlueFS::BDEV_DB, false, false}));
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write(DIR_NAME, FILE_NAME, &h, false));
    uint64_t pre = fs.get_used();
    ASSERT_EQ(0, fs.preallocate(h->file, 0, s.preallocated_size));
    const std::string content(s.write_size, 'x');
    h->append(content.c_str(), content.length());
    fs.fsync(h);
    ASSERT_EQ(0, fs.truncate(h, s.truncate_to));
    fs.fsync(h);
    uint64_t post = fs.get_used();
    fs.close_writer(h);
    EXPECT_EQ(pre, post - s.allocated_after_truncate);

    fs.umount();
    delete shared_alloc.a;
  }
}




TEST(BlueFS, test_log_runway) {
  uint64_t max_log_runway = 65536;
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_compact_log_sync", "false");
  conf.SetVal("bluefs_min_log_runway", "32768");
  conf.SetVal("bluefs_max_log_runway", std::to_string(max_log_runway).c_str());
  conf.ApplyChanges();

  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  // longer transaction than current runway
  std::string longdir(max_log_runway, 'a');
  fs.mkdir(longdir);
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write("dir", "file", &h, false));
    h->append("foo", 3);
    h->append("bar", 3);
    h->append("baz", 3);
    fs.fsync(h);
    fs.close_writer(h);
  }
  fs.umount(true);
  fs.mount();

  std::vector<std::string> ls;
  fs.readdir("dir", &ls);
  ASSERT_EQ(ls.front(), "file"); 
  uint64_t file_size = 0;
  utime_t mtime;
  fs.stat("dir", "file", &file_size, &mtime);
  ASSERT_EQ(file_size, 9);
  fs.umount();
}

TEST(BlueFS, test_log_runway_2) {
  uint64_t max_log_runway = 65536;
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_compact_log_sync", "false");
  conf.SetVal("bluefs_min_log_runway", "32768");
  conf.SetVal("bluefs_max_log_runway", std::to_string(max_log_runway).c_str());
  conf.ApplyChanges();

  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  // longer transaction than current runway
  size_t name_length = max_log_runway * 2;
  std::string longdir(name_length, 'a');
  std::string longfile(name_length, 'b');
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir(longdir));
    ASSERT_EQ(0, fs.open_for_write(longdir, longfile, &h, false));
    h->append("canary", 6);
    fs.fsync(h);
    fs.close_writer(h);
    fs.sync_metadata(true);
  }
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write("dir", "file", &h, false));
    h->append("foo", 3);
    h->append("bar", 3);
    h->append("baz", 3);
    fs.fsync(h);
    fs.close_writer(h);
  }
  fs.umount(true);
  fs.mount();

  std::vector<std::string> ls;
  fs.readdir("dir", &ls);
  ASSERT_EQ(ls.front(), "file");
  uint64_t file_size = 0;
  utime_t mtime;
  fs.stat("dir", "file", &file_size, &mtime);
  ASSERT_EQ(file_size, 9);
  fs.stat(longdir, longfile, &file_size, &mtime);
  ASSERT_EQ(file_size, 6);

  std::vector<std::string> ls_longdir;
  fs.readdir(longdir, &ls_longdir);
  ASSERT_EQ(ls_longdir.front(), longfile);
  fs.umount();
}

TEST(BlueFS, test_log_runway_3) {
  uint64_t max_log_runway = 65536;
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_alloc_size", "4096");
  conf.SetVal("bluefs_shared_alloc_size", "4096");
  conf.SetVal("bluefs_compact_log_sync", "false");
  conf.SetVal("bluefs_min_log_runway", "32768");
  conf.SetVal("bluefs_max_log_runway", std::to_string(max_log_runway).c_str());
  conf.ApplyChanges();

  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));
  // longer transaction than current runway
  for (size_t m = 0; m < 40; m++) {
    std::string longdir(max_log_runway + m, 'A' + m);
    std::string longfile(max_log_runway + m, 'A' + m);
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir(longdir));
    ASSERT_EQ(0, fs.open_for_write(longdir, longfile, &h, false));
    h->append("canary", 6);
    fs.fsync(h);
    fs.close_writer(h);
    fs.sync_metadata(true);
  }
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write("dir", "file", &h, false));
    h->append("foo", 3);
    h->append("bar", 3);
    h->append("baz", 3);
    fs.fsync(h);
    fs.close_writer(h);
  }
  fs.umount(true);
  fs.mount();

  std::vector<std::string> ls;
  fs.readdir("dir", &ls);
  ASSERT_EQ(ls.front(), "file");
  uint64_t file_size = 0;
  utime_t mtime;
  fs.stat("dir", "file", &file_size, &mtime);
  ASSERT_EQ(file_size, 9);
  for (size_t m = 0; m < 40; m++) {
    uint64_t file_size = 0;
    utime_t mtime;
    std::string longdir(max_log_runway + m, 'A' + m);
    std::string longfile(max_log_runway + m, 'A' + m);
    fs.stat(longdir, longfile, &file_size, &mtime);
    ASSERT_EQ(file_size, 6);
  }
  fs.umount();
}

TEST(BlueFS, test_log_runway_advance_seq) {
  uint64_t max_log_runway = 65536;
  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_alloc_size", "4096");
  conf.SetVal("bluefs_shared_alloc_size", "4096");
  conf.SetVal("bluefs_compact_log_sync", "false");
  conf.SetVal("bluefs_min_log_runway", "32768");
  conf.SetVal("bluefs_max_log_runway", std::to_string(max_log_runway).c_str());
  conf.ApplyChanges();

  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));

  std::string longdir(max_log_runway*2, 'A');
  ASSERT_EQ(fs.mkdir(longdir), 0);
  fs.compact_log();
  fs.umount();
}

TEST(BlueFS, test_69481_truncate_corrupts_log) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));

  BlueFS::FileWriter *f = nullptr;
  BlueFS::FileWriter *a = nullptr;
  ASSERT_EQ(0, fs.mkdir("dir"));
  ASSERT_EQ(0, fs.open_for_write("dir", "test-file", &f, false));
  ASSERT_EQ(0, fs.open_for_write("dir", "just-allocate", &a, false));

  // create 4 distinct extents in file f
  // a is here only to prevent f from merging extents together
  fs.preallocate(f->file, 0, 0x10000);
  fs.preallocate(a->file, 0, 0x10000);
  fs.preallocate(f->file, 0, 0x20000);
  fs.preallocate(a->file, 0, 0x20000);
  fs.preallocate(f->file, 0, 0x30000);
  fs.preallocate(a->file, 0, 0x30000);
  fs.preallocate(f->file, 0, 0x40000);
  fs.preallocate(a->file, 0, 0x40000);
  fs.close_writer(a);

  fs.truncate(f, 0);
  fs.fsync(f);

  bufferlist bl;
  bl.append(std::string(0x15678, ' '));
  f->append(bl);
  fs.truncate(f, 0x15678);
  fs.fsync(f);
  fs.close_writer(f);

  fs.umount();
  // remount to verify
  ASSERT_EQ(0, fs.mount());
  fs.umount();
}

TEST(BlueFS, test_69481_truncate_asserts) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid, { BlueFS::BDEV_DB, false, false }));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({ BlueFS::BDEV_DB, false, false }));

  BlueFS::FileWriter *f = nullptr;
  BlueFS::FileWriter *a = nullptr;
  ASSERT_EQ(0, fs.mkdir("dir"));
  ASSERT_EQ(0, fs.open_for_write("dir", "test-file", &f, false));
  ASSERT_EQ(0, fs.open_for_write("dir", "just-allocate", &a, false));

  // create 4 distinct extents in file f
  // a is here only to prevent f from merging extents together
  fs.preallocate(f->file, 0, 0x10000);
  fs.preallocate(a->file, 0, 0x10000);
  fs.preallocate(f->file, 0, 0x20000);
  fs.preallocate(a->file, 0, 0x20000);
  fs.preallocate(f->file, 0, 0x30000);
  fs.preallocate(a->file, 0, 0x30000);
  fs.preallocate(f->file, 0, 0x40000);
  fs.preallocate(a->file, 0, 0x40000);
  fs.close_writer(a);

  fs.truncate(f, 0);
  fs.fsync(f);

  bufferlist bl;
  bl.append(std::string(0x35678, ' '));
  f->append(bl);
  fs.truncate(f, 0x35678);
  fs.fsync(f);
  fs.close_writer(f);

  fs.umount();
}

TEST(bluefs_locked_extents_t, basics) {
  const uint64_t M = 1 << 20;
  {
    uint64_t reserved = 0x2000;
    uint64_t au = 1*M;
    uint64_t fullsize = 128*M;
    bluefs_locked_extents_t lcke(reserved, fullsize, au);
    ASSERT_EQ(lcke.head_offset, reserved);
    ASSERT_EQ(lcke.head_length, au - reserved);
    ASSERT_EQ(lcke.gray_tail_offset, fullsize - au + reserved);
    ASSERT_EQ(lcke.gray_tail_length, au - reserved);
    ASSERT_EQ(lcke.tail_offset, 0);
    ASSERT_EQ(lcke.tail_length, 0);

    // no ops
    lcke.reset_intersected(bluefs_extent_t(0, 1*M, 1*M));
    lcke.reset_intersected(bluefs_extent_t(0, 10*M, 1*M));
    lcke.reset_intersected(bluefs_extent_t(0, 127*M, reserved));
    ASSERT_EQ(lcke.head_offset, reserved);
    ASSERT_EQ(lcke.head_length, au - reserved);
    ASSERT_EQ(lcke.gray_tail_offset, fullsize - au + reserved);
    ASSERT_EQ(lcke.gray_tail_length, au - reserved);
    ASSERT_EQ(lcke.tail_offset, 0);
    ASSERT_EQ(lcke.tail_length, 0);

    // get_merged verification
    auto e1 = lcke.get_merged();
    ASSERT_EQ(e1.head_offset, lcke.head_offset);
    ASSERT_EQ(e1.head_length, lcke.head_length);
    ASSERT_EQ(e1.gray_tail_offset, 0);
    ASSERT_EQ(e1.gray_tail_length, 0);
    ASSERT_EQ(e1.tail_offset, lcke.gray_tail_offset);
    ASSERT_EQ(e1.tail_length, lcke.gray_tail_length);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, lcke.head_offset);
    ASSERT_EQ(e1.head_length, lcke.head_length);
    ASSERT_EQ(e1.gray_tail_offset, 0);
    ASSERT_EQ(e1.gray_tail_length, 0);
    ASSERT_EQ(e1.tail_offset, lcke.tail_offset);
    ASSERT_EQ(e1.tail_length, lcke.tail_length);

    // head has intersection
    lcke.reset_intersected(bluefs_extent_t(0, reserved, au));
    ASSERT_EQ(lcke.head_offset, 0);
    ASSERT_EQ(lcke.head_length, 0);
    ASSERT_EQ(lcke.gray_tail_offset, fullsize - au + reserved);
    ASSERT_EQ(lcke.gray_tail_length, au - reserved);
    ASSERT_EQ(lcke.tail_offset, 0);
    ASSERT_EQ(lcke.tail_length, 0);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, 0);
    ASSERT_EQ(e1.head_length, 0);
    ASSERT_EQ(e1.tail_offset, lcke.gray_tail_offset);
    ASSERT_EQ(e1.tail_length, lcke.gray_tail_length);

    // gray_tail has intersections
    lcke.reset_intersected(bluefs_extent_t(0, 127*M + reserved, 0x1000));
    lcke.reset_intersected(bluefs_extent_t(0, 128*M - 0x1000, 0x1000));
    ASSERT_EQ(lcke.head_offset, 0);
    ASSERT_EQ(lcke.head_length, 0);
    ASSERT_EQ(lcke.gray_tail_offset, 0);
    ASSERT_EQ(lcke.gray_tail_length, 0);
    ASSERT_EQ(lcke.tail_offset, 0);
    ASSERT_EQ(lcke.tail_length, 0);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, 0);
    ASSERT_EQ(e1.head_length, 0);
    ASSERT_EQ(e1.tail_offset, 0);
    ASSERT_EQ(e1.tail_length, 0);
  }
  {
    uint64_t reserved = 0x1000;
    uint64_t au = 1*M;
    uint64_t extra_tail = 0x10000;
    uint64_t fullsize = 128*M + extra_tail;
    bluefs_locked_extents_t lcke(reserved, fullsize, au);
    ASSERT_EQ(lcke.head_offset, reserved);
    ASSERT_EQ(lcke.head_length, au - reserved);
    ASSERT_EQ(lcke.gray_tail_offset, fullsize - extra_tail + reserved);
    ASSERT_EQ(lcke.gray_tail_length, extra_tail - reserved);
    ASSERT_EQ(lcke.tail_offset, fullsize - extra_tail);
    ASSERT_EQ(lcke.tail_length, extra_tail);

    // no ops
    lcke.reset_intersected(bluefs_extent_t(0, 1*M, 1*M));
    lcke.reset_intersected(bluefs_extent_t(0, 10*M, 1*M));
    lcke.reset_intersected(bluefs_extent_t(0, 127*M, reserved));
    ASSERT_EQ(lcke.head_offset, reserved);
    ASSERT_EQ(lcke.head_length, au - reserved);
    ASSERT_EQ(lcke.gray_tail_offset, fullsize - extra_tail + reserved);
    ASSERT_EQ(lcke.gray_tail_length, extra_tail - reserved);
    ASSERT_EQ(lcke.tail_offset, fullsize - extra_tail);
    ASSERT_EQ(lcke.tail_length, extra_tail);

    // get_merged verification
    auto e1 = lcke.get_merged();
    ASSERT_EQ(e1.head_offset, lcke.head_offset);
    ASSERT_EQ(e1.head_length, lcke.head_length);
    ASSERT_EQ(e1.gray_tail_offset, 0);
    ASSERT_EQ(e1.gray_tail_length, 0);
    ASSERT_EQ(e1.tail_offset, std::min(lcke.gray_tail_offset, lcke.tail_offset));
    ASSERT_EQ(e1.tail_length, fullsize - e1.tail_offset);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, lcke.head_offset);
    ASSERT_EQ(e1.head_length, lcke.head_length);
    ASSERT_EQ(e1.tail_offset, lcke.tail_offset);
    ASSERT_EQ(e1.tail_length, lcke.tail_length);

    // head has intersection
    lcke.reset_intersected(bluefs_extent_t(0, reserved, au));
    ASSERT_EQ(lcke.head_offset, 0);
    ASSERT_EQ(lcke.head_length, 0);
    ASSERT_EQ(lcke.gray_tail_offset, fullsize - extra_tail + reserved);
    ASSERT_EQ(lcke.gray_tail_length, extra_tail - reserved);
    ASSERT_EQ(lcke.tail_offset, fullsize - extra_tail);
    ASSERT_EQ(lcke.tail_length, extra_tail);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, 0);
    ASSERT_EQ(e1.head_length, 0);
    ASSERT_EQ(e1.tail_offset, lcke.gray_tail_offset);
    ASSERT_EQ(e1.tail_length, lcke.gray_tail_length);

    // tail has intersections
    lcke.reset_intersected(bluefs_extent_t(0, 128*M, 0x1000));
    ASSERT_EQ(lcke.head_offset, 0);
    ASSERT_EQ(lcke.head_length, 0);
    ASSERT_EQ(lcke.gray_tail_offset, fullsize - extra_tail + reserved);
    ASSERT_EQ(lcke.gray_tail_length, extra_tail - reserved);
    ASSERT_EQ(lcke.tail_offset, 0);
    ASSERT_EQ(lcke.tail_length, 0);

    // gray_tail has intersections
    lcke.reset_intersected(bluefs_extent_t(0, 128*M + reserved, 0x1000));
    ASSERT_EQ(lcke.head_offset, 0);
    ASSERT_EQ(lcke.head_length, 0);
    ASSERT_EQ(lcke.gray_tail_offset, 0);
    ASSERT_EQ(lcke.gray_tail_length, 0);
    ASSERT_EQ(lcke.tail_offset, 0);
    ASSERT_EQ(lcke.tail_length, 0);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, 0);
    ASSERT_EQ(e1.head_length, 0);
    ASSERT_EQ(e1.tail_offset, 0);
    ASSERT_EQ(e1.tail_length, 0);
  }
  {
    uint64_t reserved = 0x2000;
    uint64_t au = 1*M;
    uint64_t extra_tail = 0x1000;
    uint64_t fullsize = 128*M + extra_tail;
    bluefs_locked_extents_t lcke(reserved, fullsize, au);
    ASSERT_EQ(lcke.head_offset, reserved);
    ASSERT_EQ(lcke.head_length, au - reserved);
    ASSERT_EQ(lcke.gray_tail_offset, fullsize - au + reserved - extra_tail);
    ASSERT_EQ(lcke.gray_tail_length, au - reserved + extra_tail);
    ASSERT_EQ(lcke.tail_offset, fullsize - extra_tail);
    ASSERT_EQ(lcke.tail_length, extra_tail);

    // no ops
    lcke.reset_intersected(bluefs_extent_t(0, 1*M, 1*M));
    lcke.reset_intersected(bluefs_extent_t(0, 10*M, 1*M));
    lcke.reset_intersected(bluefs_extent_t(0, 127*M, reserved));
    ASSERT_EQ(lcke.head_offset, reserved);
    ASSERT_EQ(lcke.head_length, au - reserved);
    ASSERT_EQ(lcke.gray_tail_offset, fullsize - au + reserved - extra_tail);
    ASSERT_EQ(lcke.gray_tail_length, au - reserved + extra_tail);
    ASSERT_EQ(lcke.tail_offset, fullsize - extra_tail);
    ASSERT_EQ(lcke.tail_length, extra_tail);

    // get_merged verification
    auto e1 = lcke.get_merged();
    ASSERT_EQ(e1.head_offset, lcke.head_offset);
    ASSERT_EQ(e1.head_length, lcke.head_length);
    ASSERT_EQ(e1.gray_tail_offset, 0);
    ASSERT_EQ(e1.gray_tail_length, 0);
    ASSERT_EQ(e1.tail_offset, std::min(lcke.gray_tail_offset, lcke.tail_offset));
    ASSERT_EQ(e1.tail_length, fullsize - e1.tail_offset);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, lcke.head_offset);
    ASSERT_EQ(e1.head_length, lcke.head_length);
    ASSERT_EQ(e1.tail_offset, lcke.tail_offset);
    ASSERT_EQ(e1.tail_length, lcke.tail_length);

    // head has intersection, hopefully partial
    lcke.reset_intersected(bluefs_extent_t(reserved - 0x1000, reserved, au));
    ASSERT_EQ(lcke.head_offset, 0);
    ASSERT_EQ(lcke.head_length, 0);
    ASSERT_EQ(lcke.gray_tail_offset, fullsize - au + reserved - extra_tail);
    ASSERT_EQ(lcke.gray_tail_length, au - reserved + extra_tail);
    ASSERT_EQ(lcke.tail_offset, fullsize - extra_tail);
    ASSERT_EQ(lcke.tail_length, extra_tail);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, 0);
    ASSERT_EQ(e1.head_length, 0);
    ASSERT_EQ(e1.tail_offset, lcke.gray_tail_offset);
    ASSERT_EQ(e1.tail_length, lcke.gray_tail_length);

    // tail&gray_tail have intersections
    lcke.reset_intersected(bluefs_extent_t(0, 128*M, 0x1000));
    ASSERT_EQ(lcke.head_offset, 0);
    ASSERT_EQ(lcke.head_length, 0);
    ASSERT_EQ(lcke.gray_tail_offset, 0);
    ASSERT_EQ(lcke.gray_tail_length, 0);
    ASSERT_EQ(lcke.tail_offset, 0);
    ASSERT_EQ(lcke.tail_length, 0);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, 0);
    ASSERT_EQ(e1.head_length, 0);
    ASSERT_EQ(e1.tail_offset, 0);
    ASSERT_EQ(e1.tail_length, 0);
  }
  {
    uint64_t reserved = 0x2000;
    uint64_t au = 1*M;
    uint64_t extra_tail = 0x2000;
    uint64_t fullsize = 128*M + extra_tail;
    bluefs_locked_extents_t lcke(reserved, fullsize, au);
    ASSERT_EQ(lcke.head_offset, reserved);
    ASSERT_EQ(lcke.head_length, au - reserved);
    ASSERT_EQ(lcke.gray_tail_offset, 0);
    ASSERT_EQ(lcke.gray_tail_length, 0);
    ASSERT_EQ(lcke.tail_offset, fullsize - extra_tail);
    ASSERT_EQ(lcke.tail_length, extra_tail);

    // no ops
    lcke.reset_intersected(bluefs_extent_t(0, 1*M, 1*M));
    lcke.reset_intersected(bluefs_extent_t(0, 10*M, 1*M));
    lcke.reset_intersected(bluefs_extent_t(0, 127*M, reserved));
    ASSERT_EQ(lcke.head_offset, reserved);
    ASSERT_EQ(lcke.head_length, au - reserved);
    ASSERT_EQ(lcke.gray_tail_offset, 0);
    ASSERT_EQ(lcke.gray_tail_length, 0);
    ASSERT_EQ(lcke.tail_offset, fullsize - extra_tail);
    ASSERT_EQ(lcke.tail_length, extra_tail);

    // get_merged verification
    auto e1 = lcke.get_merged();
    ASSERT_EQ(e1.head_offset, lcke.head_offset);
    ASSERT_EQ(e1.head_length, lcke.head_length);
    ASSERT_EQ(e1.gray_tail_offset, 0);
    ASSERT_EQ(e1.gray_tail_length, 0);
    ASSERT_EQ(e1.tail_offset, lcke.tail_offset);
    ASSERT_EQ(e1.tail_length, fullsize - e1.tail_offset);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, lcke.head_offset);
    ASSERT_EQ(e1.head_length, lcke.head_length);
    ASSERT_EQ(e1.tail_offset, lcke.tail_offset);
    ASSERT_EQ(e1.tail_length, lcke.tail_length);

    // head has intersection, hopefully partial
    lcke.reset_intersected(bluefs_extent_t(reserved - 0x1000, reserved, au));
    ASSERT_EQ(lcke.head_offset, 0);
    ASSERT_EQ(lcke.head_length, 0);
    ASSERT_EQ(lcke.gray_tail_offset, 0);
    ASSERT_EQ(lcke.gray_tail_length, 0);
    ASSERT_EQ(lcke.tail_offset, fullsize - extra_tail);
    ASSERT_EQ(lcke.tail_length, extra_tail);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, 0);
    ASSERT_EQ(e1.head_length, 0);
    ASSERT_EQ(e1.tail_offset, 0);
    ASSERT_EQ(e1.tail_length, 0);

    // tail have intersections
    lcke.reset_intersected(bluefs_extent_t(0, 128*M, 0x1000));
    ASSERT_EQ(lcke.head_offset, 0);
    ASSERT_EQ(lcke.head_length, 0);
    ASSERT_EQ(lcke.gray_tail_offset, 0);
    ASSERT_EQ(lcke.gray_tail_length, 0);
    ASSERT_EQ(lcke.tail_offset, 0);
    ASSERT_EQ(lcke.tail_length, 0);

    e1 = lcke.finalize();
    ASSERT_EQ(e1.head_offset, 0);
    ASSERT_EQ(e1.head_length, 0);
    ASSERT_EQ(e1.tail_offset, 0);
    ASSERT_EQ(e1.tail_length, 0);
  }
}

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
  map<string,string> defaults = {
    { "debug_bluefs", "1/20" },
    { "debug_bdev", "1/20" }
  };

  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "*");
  g_ceph_context->_conf.apply_changes(nullptr);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
