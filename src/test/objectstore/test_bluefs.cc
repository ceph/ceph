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
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "include/scope_guard.h"
#include "common/errno.h"
#include <gtest/gtest.h>

#include "os/bluestore/BlueFS.h"

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

TEST(BlueFS, mkfs) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  uuid_d fsid;
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  ASSERT_EQ(0, fs.mkfs(fsid));
}

TEST(BlueFS, mkfs_mount) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(fs.get_total(BlueFS::BDEV_DB), size - 1048576);
  ASSERT_LT(fs.get_free(BlueFS::BDEV_DB), size - 1048576);
  fs.umount();
}

TEST(BlueFS, write_read) {
  uint64_t size = 1048576 * 128;
  TempBdev bdev{size};
  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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
    BlueFS::FileReaderBuffer buf(4096);
    ASSERT_EQ(9, fs.read(h, &buf, 0, 1024, &bl, NULL));
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
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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
    BlueFS::FileReaderBuffer readbuf(10485760);
    ASSERT_EQ(h->file->fnode.size, total_written);
    for (unsigned i = 0; i < 3*1024*1048576ull / sizeof(buf); ++i) {
      bl.clear();
      fs.read(h, &readbuf, i * sizeof(buf), sizeof(buf), &bl, NULL);
      int r = memcmp(buf, bl.c_str(), sizeof(buf));
      if (r) {
	cerr << "read got mismatch at offset " << i*sizeof(buf) << " r " << r
	     << std::endl;
      }
      ASSERT_EQ(0, r);
    }
    for (unsigned i = 0; i < 2*1024*1048576ull / sizeof(buf); ++i) {
      bl.clear();
      fs.read(h, &readbuf, i * sizeof(buf), sizeof(buf), &bl, NULL);
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
    unique_ptr<char> huge_buf(new char[h->file->fnode.size]);
    auto l = h->file->fnode.size;
    int64_t r = fs.read(h, &readbuf, 0, l, NULL, huge_buf.get());
    ASSERT_EQ(r, (int64_t)l);
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

  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false, 1048576));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size_full - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());

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
    BlueFS::FileReaderBuffer readbuf(10485760);
    int64_t r = fs.read(h, &readbuf, 0, l, NULL, buf.get());
    ASSERT_EQ(r, (int64_t)l);
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
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
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
  fs.umount();
}

TEST(BlueFS, test_tracker_50965) {
  uint64_t size_wal = 1048576 * 64;
  TempBdev bdev_wal{size_wal};
  uint64_t size_db = 1048576 * 128;
  TempBdev bdev_db{size_db};
  uint64_t size_slow = 1048576 * 256;
  TempBdev bdev_slow{size_slow};

  g_ceph_context->_conf.set_val(
    "bluefs_min_flush_size",
    "65536");

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_WAL,  bdev_wal.path,  false, 0));
  fs.add_block_extent(BlueFS::BDEV_WAL, 0, size_wal);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB,   bdev_db.path,   false, 0));
  fs.add_block_extent(BlueFS::BDEV_DB, 0, size_db);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_SLOW, bdev_slow.path, false, 0));
  fs.add_block_extent(BlueFS::BDEV_SLOW, 0, size_slow);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());

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

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

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
