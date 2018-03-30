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

  store.reset(ObjectStore::create(g_ceph_context,
                                  type,
                                  data_dir,
                                  string("store_test_temp_journal")));
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
}

void StoreTestFixture::TearDown()
{
  PopSettings(0);
  if (store) {
    int r = store->umount();
    EXPECT_EQ(0, r);
    rm_r(data_dir);
  }
}

void StoreTestFixture::SetVal(md_config_t* _conf, const char* key, const char* val)
{
  assert(!conf || conf == _conf);
  conf = _conf;
  std::string skey(key);
  std::string prev_val;
  conf->get_val(skey, &prev_val);
  conf->set_val(key, val);
  saved_settings.emplace(skey, prev_val);
}

void StoreTestFixture::PopSettings(size_t pos)
{
  if (conf) {
    assert(pos == 0 || pos <= saved_settings.size()); // for sanity
    while(pos < saved_settings.size())
    {
      auto& e = saved_settings.top();
      conf->set_val(e.first, e.second);
      saved_settings.pop();
    }
    conf->apply_changes(NULL);
  }
}
