// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdlib>
#include <initializer_list>
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

int argc;
char **argv;

std::unique_ptr<char[]> gen_buffer(uint64_t size)
{
    std::unique_ptr<char[]> buffer = std::make_unique<char[]>(size);
    std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char> e;
    std::generate(buffer.get(), buffer.get()+size, std::ref(e));
    return buffer;
}

class TempBdev {
public:
  TempBdev() {}
  ~TempBdev() {}
  void choose_name(pid_t pid = getpid()) {
    static int n = 0;
    path = "ceph_test_bluefs.tmp.block." + stringify(pid)
    + "." + stringify(++n);
  }
  void create_bdev(uint64_t size) {
    ceph_assert(!path.empty());
    int fd = ::open(path.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0644);
    ceph_assert(fd >= 0);
    int r = ::ftruncate(fd, size);
    ceph_assert(r >= 0);
    ::close(fd);
  }
  void rm_bdev() {
    ceph_assert(!path.empty());
    ::unlink(path.c_str());
  }
  std::string path;
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


class BlueFS_ex : virtual public ::testing::Test {

public:
  explicit BlueFS_ex()
  {
  }
  boost::intrusive_ptr<CephContext> init_ceph()
  {
    boost::intrusive_ptr<CephContext> cct;
    auto args = argv_to_vec(argc, argv);
    map<string, string> defaults = {
      {"debug_bluefs", "0/20"},
      {"debug_bdev", "0/20"},
      {"log_to_stderr", "false"}};
    cct = global_init(
      &defaults, args, CEPH_ENTITY_TYPE_CLIENT,
      CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
    common_init_finish(g_ceph_context);
    g_ceph_context->_conf.set_val(
        "enable_experimental_unrecoverable_data_corrupting_features", "*");
    g_ceph_context->_conf.apply_changes(nullptr);
    return cct;
  }

  void SetUp() override
  {
  }
  void TearDown() override
  {
  }

  void grow_log_interrupt_on_compact(pid_t parent_pid, uint32_t stop_point)
  {
    auto cct = init_ceph();
    ConfSaver conf(g_ceph_context->_conf);
    conf.SetVal("bluefs_alloc_size", "4096");
    conf.SetVal("bluefs_shared_alloc_size", "4096");
    conf.SetVal("bluefs_compact_log_sync", "false");
    conf.SetVal("bluefs_log_compact_min_size", "1048576");
    conf.ApplyChanges();

    auto stop_at_fixed_point = [&](uint32_t i) -> void {
      if (i == stop_point) _exit(107);
    };
    BlueFS fs(g_ceph_context);
    fs.tracepoint_async_compact = stop_at_fixed_point;
    ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
    uuid_d fsid;
    ASSERT_EQ(0, fs.mkfs(fsid, {BlueFS::BDEV_DB, false, false}));
    ASSERT_EQ(0, fs.mount());
    ASSERT_EQ(0, fs.maybe_verify_layout({BlueFS::BDEV_DB, false, false}));
    ASSERT_EQ(0, fs.mkdir("dir"));

    auto fill = [&](uint32_t filenum) {
      char data[2000] = {'x'};
      BlueFS::FileWriter *h;
      ASSERT_EQ(0, fs.open_for_write("dir", "file"+to_string(filenum), &h, false));
      for (size_t i = 0; i < 10000; i++) {
        h->append(data, 2000);
        fs.fsync(h);
      }
      fs.close_writer(h);
    };
    std::thread thr[10];
    for (int i=0; i< 10;i++) {
      thr[i] = std::thread(fill, i);
    }
    for (int i=0; i< 10;i++) {
      thr[i].join();
    }
    EXPECT_TRUE(false && "reaching this point means test was not executed");
    exit(111);
  }

  TempBdev bdev;
};

TEST_F(BlueFS_ex, test_interrupted_compaction)
{
  for (uint32_t stop_point = 1; stop_point <= 6; stop_point++)
  {
    pid_t fork_for_test = fork();
    if (fork_for_test != 0) {
      int stat;
      waitpid(fork_for_test, &stat, 0);
      ASSERT_TRUE(WIFEXITED(stat));
      ASSERT_TRUE(WEXITSTATUS(stat) == 0);
      continue;
    }
    pid_t parent_pid = getpid();
    uint64_t size = 1048576LL * (2 * 1024 + 128);
    bdev.choose_name();
    bdev.create_bdev(size);
    pid_t fork_pid = fork();
    if (fork_pid == 0) {
      std::cout << "growing BlueFS log for async compact, stop at #" << (int)stop_point << std::endl;
      grow_log_interrupt_on_compact(parent_pid, stop_point);
    } else {
      int stat;
      std::cout << "waiting for compaction to terminate" << std::endl;
      waitpid(fork_pid, &stat, 0);
      std::cout << "done code=" << WEXITSTATUS(stat) << std::endl;
      if(!WIFEXITED(stat) || WEXITSTATUS(stat) != 107) exit(107);
      auto cct = init_ceph();
      ConfSaver conf(g_ceph_context->_conf);
      conf.SetVal("bluefs_alloc_size", "4096");
      conf.SetVal("bluefs_shared_alloc_size", "4096");
      conf.SetVal("bluefs_compact_log_sync", "false");
      conf.SetVal("bluefs_log_compact_min_size", "1048576");
      conf.ApplyChanges();

      BlueFS fs(g_ceph_context);
      ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev.path, false));
      // fs.log_dump();
      ASSERT_EQ(0, fs.mount());
      fs.umount();
    }
    bdev.rm_bdev();
    exit(0); //this terminates one loop of 'fork_for_test'
  }

}

int main(int _argc, char **_argv) {
  argc = _argc;
  argv = _argv;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
