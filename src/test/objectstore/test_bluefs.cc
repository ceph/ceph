// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "include/scope_guard.h"
#include "common/errno.h"

#include "os/bluestore/Allocator.h"
#include "os/bluestore/bluestore_common.h"
#include "os/bluestore/BlueFS.h"

using namespace std;

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
  ~TempBdev() {
    rm_temp_bdev(path);
  }
  const std::string path;
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
  ASSERT_EQ(fs.get_total(BlueFS::BDEV_DB), size - DB_SUPER_RESERVED);
  ASSERT_LT(fs.get_free(BlueFS::BDEV_DB), size - DB_SUPER_RESERVED);
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
  auto *logger = fs.get_perf_counters();
  ASSERT_NE(logger->get(l_bluefs_alloc_shared_size_fallbacks), 0);
  auto num_files = logger->get(l_bluefs_num_files);
  fs.umount();

  fs.mount();
  ASSERT_EQ(num_files, logger->get(l_bluefs_num_files));
  fs.umount();
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
  auto *logger = fs.get_perf_counters();
  ASSERT_EQ(logger->get(l_bluefs_alloc_shared_dev_fallbacks), 0);
  ASSERT_EQ(logger->get(l_bluefs_alloc_shared_size_fallbacks), 0);
  auto num_files = logger->get(l_bluefs_num_files);
  fs.umount();

  fs.mount();
  ASSERT_EQ(num_files, logger->get(l_bluefs_num_files));
  fs.umount();
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
