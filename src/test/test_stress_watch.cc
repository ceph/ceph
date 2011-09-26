#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/rados-api/test.h"

#include "gtest/gtest.h"
#include <semaphore.h>
#include <errno.h>
#include <map>
#include <sstream>
#include <iostream>
#include <string>

using namespace librados;
using ceph::buffer;
using std::map;
using std::ostringstream;
using std::string;

static sem_t sem;

class WatchNotifyTestCtx : public WatchCtx
{
public:
    void notify(uint8_t opcode, uint64_t ver, bufferlist& bl)
    {
      sem_post(&sem);
    }
};

TEST(WatchStress, Stress1) {
  ASSERT_EQ(0, sem_init(&sem, 0, 0));
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  WatchNotifyTestCtx ctx;
  
  ASSERT_EQ(0, ioctx.create("foo", false));

  for (int i = 0; i < 10000; ++i) {
    std::cerr << "Iteration " << i << std::endl;
    uint64_t handle;
    WatchNotifyTestCtx ctx;
    ASSERT_EQ(0, ioctx.watch("foo", 0, &handle, &ctx));
    bufferlist bl2;
    ASSERT_EQ(0, ioctx.notify("foo", 0, bl2));
    TestAlarm alarm;
    sem_wait(&sem);
    ioctx.unwatch("foo", handle);
  }

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
  sem_destroy(&sem);
}
