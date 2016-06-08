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
  uint64_t size = 1048476 * 128;
  string fn = get_temp_bdev(size);
  uuid_d fsid;
  BlueFS fs;
  fs.add_block_device(BlueFS::BDEV_DB, fn);
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  fs.mkfs(fsid);
  rm_temp_bdev(fn);
}

TEST(BlueFS, mkfs_mount) {
  uint64_t size = 1048476 * 128;
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
  uint64_t size = 1048476 * 128;
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
  uint64_t size = 1048476 * 128;
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

TEST(BlueFS, write_small_files) {
  uint64_t size = 1048476 * 128;
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
  {
    for (int i=0; i<10; i++) {
       string dir = "dir.";
       dir.append(to_string(i));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          fs.unlink(dir, file);
	  fs.flush_log();
       }
       fs.rmdir(dir);
       fs.flush_log();
    }
  }
  fs.umount();
  rm_temp_bdev(fn);
}

TEST(BlueFS, write_big_files) {
  uint64_t size = 1048476 * 128;
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
          char *buf = gen_buffer(409600);
	  bufferptr bp = buffer::claim_char(409600, buf);
	  bl.push_back(bp);
          h->append(bl);
          fs.fsync(h);
          fs.close_writer(h);
       }
    }
  }
  {
    for (int i=0; i<10; i++) {
       string dir = "dir.";
       dir.append(to_string(i));
       for (int j=0; j<10; j++) {
          string file = "file.";
	  file.append(to_string(j));
          fs.unlink(dir, file);
	  fs.flush_log();
       }
       fs.rmdir(dir);
       fs.flush_log();
    }
  }
  fs.umount();
  rm_temp_bdev(fn);
}

TEST(BlueFS, test_simple_compaction) {
  uint64_t size = 1048476 * 128;
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

bool pause_thread = false;
bool stop = false;

void write_data(BlueFS &fs)
{
    BlueFS::FileWriter *h;
    int j=0;
    stringstream ss;
    string dir = "dir.";
    ss << std::this_thread::get_id();
    dir.append(ss.str());
    dir.append(".");
    dir.append(to_string(j));
    ASSERT_EQ(0, fs.mkdir(dir));
    while(1) {
      if (stop == true)
        break;
      if (pause_thread == true)
        sleep(10);
      string file = "file.";
      file.append(to_string(j));
      ASSERT_EQ(0, fs.open_for_write(dir, file, &h, false));
      bufferlist bl;
      char *buf = gen_buffer(4096);
      bufferptr bp = buffer::claim_char(4096, buf);
      bl.push_back(bp);
      h->append(bl);
      fs.fsync(h);
      j++;
      if (j%4 == 0)
      {
          file = "file.";
	  file.append(to_string(j-1));
          fs.unlink(dir, file);
	  fs.flush_log();
      }
      fs.close_writer(h);
    }
}

void sync_fs(BlueFS &fs)
{
    while(1) {
      if(stop == true)
        break;
      if (pause_thread == true)
        sleep(10);
      fs.sync_metadata();
      sleep(2);
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

#define NUM_WRITERS 5
#define NUM_SYNC_THREADS 1

TEST(BlueFS, test_compaction) {
  uint64_t size = 1048476 * 512;
  string fn = get_temp_bdev(size);

  BlueFS fs;
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, fn));
  fs.add_block_extent(BlueFS::BDEV_DB, 1048576, size - 1048576);
  uuid_d fsid;
  ASSERT_EQ(0, fs.mkfs(fsid));
  ASSERT_EQ(0, fs.mount());
  {
    std::vector<std::thread> write_threads;
    for (int i=0; i<NUM_WRITERS; i++) {
      write_threads.push_back(std::thread(write_data, std::ref(fs)));
    }

    sleep(2);

    std::vector<std::thread> sync_threads;
    for (int i=0; i<NUM_SYNC_THREADS; i++) {
      sync_threads.push_back(std::thread(sync_fs, std::ref(fs)));
    }

    pause_thread = true;
    sleep(1);
    pause_thread = false;
    sleep(2);
    stop = true;
    sleep(1);
    join_all(write_threads);
    join_all(sync_threads);
    fs.compact_log();
  }
  fs.umount();
  rm_temp_bdev(fn);
}


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "*");
  g_ceph_context->_conf->set_val("debug_bdev", "0/20");
  g_ceph_context->_conf->set_val("debug_bluefs", "0/20");
  g_ceph_context->_conf->apply_changes(NULL);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
