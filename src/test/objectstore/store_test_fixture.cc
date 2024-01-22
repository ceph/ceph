#include <stdlib.h>
#include <string>
#include <iostream>
#include <assert.h>
#include <gtest/gtest.h>

#include "common/errno.h"
#include "common/config.h"
#include "os/ObjectStore.h"

#if defined(WITH_BLUESTORE)
#include "os/bluestore/BlueStore.h"
#endif
#include "store_test_fixture.h"

using namespace std;

static void rm_r(const string& path)
{
  string cmd = string("rm -r ") + path;
  cout << "==> " << cmd << std::endl;
  int r = ::system(cmd.c_str());
  if (r) {
    if (r == -1) {
      r = errno;
      cerr << "system() failed to fork() " << cpp_strerror(r)
           << ", continuing anyway" << std::endl;
    } else {
      cerr << "failed with exit code " << r
           << ", continuing anyway" << std::endl;
    }
  }
}

void StoreTestFixture::SetUp()
{

  int r = ::mkdir(data_dir.c_str(), 0777);
  if (r < 0) {
    r = -errno;
    cerr << __func__ << ": unable to create " << data_dir << ": " << cpp_strerror(r) << std::endl;
  }
  ASSERT_EQ(0, r);

  store = ObjectStore::create(g_ceph_context,
                              type,
                              data_dir,
                              "store_test_temp_journal");
  if (!store) {
    cerr << __func__ << ": objectstore type " << type << " doesn't exist yet!" << std::endl;
  }
  ASSERT_TRUE(store);
#if defined(WITH_BLUESTORE)
  if (type == "bluestore") {
    BlueStore *s = static_cast<BlueStore*>(store.get());
    // better test coverage!
    s->set_cache_shards(5);
  }
#endif
  ASSERT_EQ(0, store->mkfs());
  ASSERT_EQ(0, store->mount());

  // we keep this stuff 'unsafe' out of test case scope to be able to update ANY
  // config settings. Hence setting it to 'safe' here to proceed with the test
  // case
  g_conf().set_safe_to_start_threads();
}

void StoreTestFixture::TearDown()
{
  if (store) {
    int r = store->umount();
    EXPECT_EQ(0, r);
    rm_r(data_dir);
  }
  // we keep this stuff 'unsafe' out of test case scope to be able to update ANY
  // config settings. Hence setting it to 'unsafe' here as test case is closing.
  g_conf()._clear_safe_to_start_threads();
  PopSettings(0);
}

void StoreTestFixture::SetVal(ConfigProxy& _conf, const char* key, const char* val)
{
  ceph_assert(!conf || conf == &_conf);
  conf = &_conf;
  std::string skey(key);
  std::string prev_val;
  conf->get_val(skey, &prev_val);
  conf->set_val_or_die(key, val);
  saved_settings.emplace(skey, prev_val);
}

void StoreTestFixture::PopSettings(size_t pos)
{
  if (conf) {
    ceph_assert(pos == 0 || pos <= saved_settings.size()); // for sanity
    while(pos < saved_settings.size())
    {
      auto& e = saved_settings.top();
      conf->set_val_or_die(e.first, e.second);
      saved_settings.pop();
    }
    conf->apply_changes(NULL);
  }
}

void StoreTestFixture::CloseAndReopen() {
  ceph_assert(store != nullptr);
  g_conf()._clear_safe_to_start_threads();
  int r = store->umount();
  EXPECT_EQ(0, r);
  ch.reset(nullptr);
  store.reset(nullptr);
  store = ObjectStore::create(g_ceph_context,
                              type,
                              data_dir,
                              "store_test_temp_journal");
  if (!store) {
    cerr << __func__ << ": objectstore type " << type << " failed to reopen!" << std::endl;
  }
  ASSERT_TRUE(store);
#if defined(WITH_BLUESTORE)
  if (type == "bluestore") {
    BlueStore *s = static_cast<BlueStore*>(store.get());
    // better test coverage!
    s->set_cache_shards(5);
  }
#endif
  ASSERT_EQ(0, store->mount());
  g_conf().set_safe_to_start_threads();
}
