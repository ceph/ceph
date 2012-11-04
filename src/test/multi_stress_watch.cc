// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

#include <semaphore.h>
#include <errno.h>
#include <map>
#include <sstream>
#include <iostream>
#include <string>
#include <stdlib.h>
#include <unistd.h>

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

int main(int args, char **argv)
{
  if (args < 3) {
    std::cerr << "Error: " << argv[0] << " pool_name obj_name" << std::endl;
    return 1;
  }

  std::string pool_name(argv[1]);
  std::string obj_name(argv[2]);
  std::cerr << "pool_name, obj_name are " << pool_name << ", " << obj_name << std::endl;

  char *id = getenv("CEPH_CLIENT_ID");
  if (id) std::cerr << "Client id is: " << id << std::endl;
  Rados cluster;
  int ret;
  ret = cluster.init(id);
  if (ret) {
    std::cerr << "Error " << ret << " in cluster.init" << std::endl;
    return ret;
  }
  ret = cluster.conf_read_file(NULL);
  if (ret) {
    std::cerr << "Error " << ret << " in cluster.conf_read_file" << std::endl;
    return ret;
  }
  ret = cluster.conf_parse_env(NULL);
  if (ret) {
    std::cerr << "Error " << ret << " in cluster.conf_read_env" << std::endl;
    return ret;
  }
  cluster.connect();

  // May already exist
  cluster.pool_create(pool_name.c_str());

  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  ioctx.create(obj_name, false);

  
  for (int i = 0; i < 10000; ++i) {
    std::cerr << "Iteration " << i << std::endl;
    uint64_t handle;
    WatchNotifyTestCtx ctx;
    ret = ioctx.watch(obj_name, 0, &handle, &ctx);
    assert(!ret);
    bufferlist bl2;
    ret = ioctx.notify(obj_name, 0, bl2);
    assert(!ret);
    TestAlarm alarm;
    sem_wait(&sem);
    ioctx.unwatch(obj_name, handle);
  }

  ioctx.close();
  sem_destroy(&sem);
  return 0;
}
