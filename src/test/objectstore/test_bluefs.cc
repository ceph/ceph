// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"
#include <gtest/gtest.h>

#include "os/bluestore/BlueFS.h"

string get_temp_bdev(uint64_t size)
{
  static int n = 0;
  string fn = "ceph_test_bluefs.tmp.block." + stringify(getpid())
    + "." + stringify(++n);
  int fd = ::open(fn.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0644);
  assert(fd >= 0);
  int r = ::ftruncate(fd, size);
  assert(r >= 0);
  ::close(fd);
  return fn;
}

char* gen_buffer(uint64_t size)
{
    char *buffer = new char[size];
    boost::random::random_device rand;
    rand.generate(buffer, buffer + size);
    return buffer;
}


void rm_temp_bdev(string f)
{
  ::unlink(f.c_str());
}

TEST(BlueFS, mkfs) {
  uint64_t size = 1048576 * 128;
  string fn = get_temp_bdev(size);
  uuid_d fsid;
  BlueFS fs;
  fs.add_block_device(BlueFS::BDEV_DB, fn);
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  fs.mkfs(fsid);
  rm_temp_bdev(fn);
}

TEST(BlueFS, mkfs_mount) {
  uint64_t size = 1048576 * 128;
  string fn = get_temp_bdev(size);
  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(fs.get_total(BlueFS::BDEV_DB), size - 1048576);
  ASSERT_LT(fs.get_free(BlueFS::BDEV_DB), size - 1048576);
  fs.umount();
  rm_temp_bdev(fn);
}

TEST(BlueFS, write_read) {
  uint64_t size = 1048576 * 128;
  string fn = get_temp_bdev(size);
  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write("dir", "file", &h, false));
    bufferlist bl;
    bl.append("foo");
    h->append(bl);
    bl.append("bar");
    h->append(bl);
    bl.append("baz");
    h->append(bl);
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
  rm_temp_bdev(fn);
}

TEST(BlueFS, small_appends) {
  uint64_t size = 1048576 * 128;
  string fn = get_temp_bdev(size);
  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.mkdir("dir"));
    ASSERT_EQ(0, fs.open_for_write("dir", "file", &h, false));
    for (unsigned i = 0; i < 10000; ++i) {
      bufferlist bl;
      bl.append("fddjdjdjdjdjdjdjdjdjdjjddjoo");
      h->append(bl);
    }
    fs.fsync(h);
    fs.close_writer(h);
  }
  {
    BlueFS::FileWriter *h;
    ASSERT_EQ(0, fs.open_for_write("dir", "file_sync", &h, false));
    for (unsigned i = 0; i < 1000; ++i) {
      bufferlist bl;
      bl.append("fddjdjdjdjdjdjdjdjdjdjjddjoo");
      h->append(bl);
      fs.fsync(h);
    }
    fs.close_writer(h);
  }
  fs.umount();
  rm_temp_bdev(fn);
}

#define ALLOC_SIZE 4096

void write_data(BlueFS &fs, uint64_t rationed_bytes)
{
    BlueFS::FileWriter *h;
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
      ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
      bufferlist bl;
      char *buf = gen_buffer(ALLOC_SIZE);
      bufferptr bp = buffer::claim_char(ALLOC_SIZE, buf);
      bl.push_back(bp);
      h->append(bl);
      r = fs.fsync(h);
      if (r < 0) {
         fs.close_writer(h);
         break;
      }
      written_bytes += g_conf->bluefs_alloc_size;
      fs.close_writer(h);
      j++;
      if ((rationed_bytes - written_bytes) <= g_conf->bluefs_alloc_size) {
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
    char *buf = gen_buffer(ALLOC_SIZE);
    bufferptr bp = buffer::claim_char(ALLOC_SIZE, buf);
    bl.push_back(bp);
    h->append(bl);
    fs.fsync(h);
    fs.close_writer(h);
}

void write_single_file(BlueFS &fs, uint64_t rationed_bytes)
{
    BlueFS::FileWriter *h;
    stringstream ss;
    string dir = "dir.test";
    string file = "testfile";
    int r=0, j=0;
    uint64_t written_bytes = 0;
    rationed_bytes -= ALLOC_SIZE;
    while (1) {
      ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
      bufferlist bl;
      char *buf = gen_buffer(ALLOC_SIZE);
      bufferptr bp = buffer::claim_char(ALLOC_SIZE, buf);
      bl.push_back(bp);
      h->append(bl);
      r = fs.fsync(h);
      if (r < 0) {
         fs.close_writer(h);
         break;
      }
      written_bytes += g_conf->bluefs_alloc_size;
      fs.close_writer(h);
      j++;
      if ((rationed_bytes - written_bytes) <= g_conf->bluefs_alloc_size) {
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
      fs.sync_metadata();
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
  string fn = get_temp_bdev(size);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->apply_changes(NULL);

  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
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
  rm_temp_bdev(fn);
}

TEST(BlueFS, test_flush_2) {
  uint64_t size = 1048576 * 256;
  string fn = get_temp_bdev(size);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->apply_changes(NULL);

  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
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
  rm_temp_bdev(fn);
}

TEST(BlueFS, test_flush_3) {
  uint64_t size = 1048576 * 256;
  string fn = get_temp_bdev(size);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->apply_changes(NULL);

  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
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
  rm_temp_bdev(fn);
}

TEST(BlueFS, test_simple_compaction_sync) {
  g_ceph_context->_conf->set_val(
    "bluefs_compact_log_sync",
    "true");
  uint64_t size = 1048576 * 128;
  string fn = get_temp_bdev(size);

  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
  {
    BlueFS::FileWriter *h;
    for (int i=0; i<10; i++) {
       string dir = "dir.";
       dir.append(to_string(i));
       ASSERT_EQ(0, fs.mkdir(dir));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
          bufferlist bl;
          char *buf = gen_buffer(4096);
	  bufferptr bp = buffer::claim_char(4096, buf);
	  bl.push_back(bp);
          h->append(bl);
          fs.fsync(h);
          fs.close_writer(h);
       }
    }
  }
  // Don't remove all
  {
    for (int i=0; i<10; i+=2) {
       string dir = "dir.";
       dir.append(to_string(i));
       for (int j=0; j<10; j+=2) {
          string file = "file.";
	  file.append(to_string(j));
          fs.unlink(dir, file);
	  fs.flush_log();
       }
       fs.rmdir(dir);
       fs.flush_log();
    }
  }
  fs.compact_log();
  fs.umount();
  rm_temp_bdev(fn);
}

TEST(BlueFS, test_simple_compaction_async) {
  g_ceph_context->_conf->set_val(
    "bluefs_compact_log_sync",
    "false");
  uint64_t size = 1048576 * 128;
  string fn = get_temp_bdev(size);

  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
  {
    BlueFS::FileWriter *h;
    for (int i=0; i<10; i++) {
       string dir = "dir.";
       dir.append(to_string(i));
       ASSERT_EQ(0, fs.mkdir(dir));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
          bufferlist bl;
          char *buf = gen_buffer(4096);
	  bufferptr bp = buffer::claim_char(4096, buf);
	  bl.push_back(bp);
          h->append(bl);
          fs.fsync(h);
          fs.close_writer(h);
       }
    }
  }
  // Don't remove all
  {
    for (int i=0; i<10; i+=2) {
       string dir = "dir.";
       dir.append(to_string(i));
       for (int j=0; j<10; j+=2) {
          string file = "file.";
	  file.append(to_string(j));
          fs.unlink(dir, file);
	  fs.flush_log();
       }
       fs.rmdir(dir);
       fs.flush_log();
    }
  }
  fs.compact_log();
  fs.umount();
  rm_temp_bdev(fn);
}

TEST(BlueFS, test_compaction_sync) {
  uint64_t size = 1048576 * 128;
  string fn = get_temp_bdev(size);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->set_val(
    "bluefs_compact_log_sync",
    "true");

  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
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
  rm_temp_bdev(fn);
}

TEST(BlueFS, test_compaction_async) {
  uint64_t size = 1048576 * 128;
  string fn = get_temp_bdev(size);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->set_val(
    "bluefs_compact_log_sync",
    "false");

  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
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
  rm_temp_bdev(fn);
}

TEST(BlueFS, test_replay) {
  uint64_t size = 1048576 * 128;
  string fn = get_temp_bdev(size);
  g_ceph_context->_conf->set_val(
    "bluefs_alloc_size",
    "65536");
  g_ceph_context->_conf->set_val(
    "bluefs_compact_log_sync",
    "false");

  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
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
  fs.mount();
  fs.umount();
  rm_temp_bdev(fn);
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  vector<const char *> def_args;
  def_args.push_back("--debug-bluefs=1/20");
  def_args.push_back("--debug-bdev=1/20");

  global_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "*");
  g_ceph_context->_conf->apply_changes(NULL);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
