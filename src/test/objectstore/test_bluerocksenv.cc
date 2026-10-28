// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stack>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "include/intarith.h"
#include "include/stringify.h"

#include "os/bluestore/BlueFS.h"
#include "os/bluestore/BlueRocksEnv.h"

#include "rocksdb/db.h"
#include "rocksdb/options.h"

using namespace std;

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
    string fn = "ceph_test_bluerocksenv.tmp.block." + stringify(getpid())
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
};

// End-to-end regression test for https://tracker.ceph.com/issues/76256.
//
// RocksDB preallocates space for the files it writes via
// WritableFile::Allocate(), and expects the unused tail to be released when
// the file is closed. It doesn't call Truncate() for that.
TEST(BlueRocksEnv, close_releases_preallocated_tail) {
  constexpr uint64_t alloc_unit = 4 << 10;
  const char* DB_DIR = "db";

  ConfSaver conf(g_ceph_context->_conf);
  conf.SetVal("bluefs_shared_alloc_size", stringify(alloc_unit).c_str());
  conf.SetVal("bluefs_alloc_size", stringify(alloc_unit).c_str());

  uuid_d fsid;
  TempBdev bdev_db{(64 << 20)};

  BlueFS fs(g_ceph_context);
  ASSERT_EQ(0, fs.add_block_device(BlueFS::BDEV_DB, bdev_db.path, false));
  ASSERT_EQ(0, fs.mkfs(fsid, {BlueFS::BDEV_DB, false, false}));
  ASSERT_EQ(0, fs.mount());
  ASSERT_EQ(0, fs.maybe_verify_layout({BlueFS::BDEV_DB, false, false}));
  ASSERT_EQ(0, fs.mkdir(DB_DIR));

  auto make_options = [&](BlueRocksEnv* env) {
    rocksdb::Options options;
    options.env = env;
    options.create_if_missing = true;
    // use smallest possible sizes to speed up the test
    options.write_buffer_size = 64 << 10;
    options.manifest_preallocation_size = 64 << 10;
    return options;
  };

  // create a database
  {
    BlueRocksEnv env(&fs);
    rocksdb::DB* db = nullptr;
    ASSERT_TRUE(rocksdb::DB::Open(make_options(&env), DB_DIR, &db).ok());
    ASSERT_TRUE(db->Put(rocksdb::WriteOptions(), "key", "v").ok());
    // Close the db without flushing the memtable, so that the WAL and the MANIFEST are
    // closed but stay alive -- the state in which the preallocated tails were leaked.
    ASSERT_TRUE(db->Close().ok());
    delete db;
  }

  std::vector<std::string> ls;
  ASSERT_EQ(0, fs.readdir(DB_DIR, &ls));
  ASSERT_LT(3, ls.size());
  for (std::string& fname : ls) {
    if (fname == "." || fname == "..") {
      continue;
    }
    BlueFS::FileReader* r;
    ASSERT_EQ(0, fs.open_for_read(DB_DIR, fname, &r));
    auto& fnode = r->file->fnode;
    // This is what was fixed in the issue - the file held allocated more than necessary
    EXPECT_EQ(p2roundup(fnode.size, alloc_unit), fnode.get_allocated())
      << DB_DIR << "/" << fname << " holds " << fnode.get_allocated()
      << " bytes for " << fnode.size << " bytes of data";
    delete r;
  }

  fs.umount();
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

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
