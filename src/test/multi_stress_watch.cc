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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

void
test_loop(Rados &cluster, std::string pool_name, std::string obj_name)
{
  int ret;
  IoCtx ioctx;
  ret = cluster.ioctx_create(pool_name.c_str(), ioctx);
  if (ret < 0) {
    std::cerr << "ioctx_create " << pool_name << " failed with " << ret << std::endl;
    exit(1);
  }

  ret = ioctx.create(obj_name, false);
  if (ret < 0) {
    std::cerr << "create failed with " << ret << std::endl;
    exit(1);
  }

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
}

#pragma GCC diagnostic pop

void
test_replicated(Rados &cluster, std::string pool_name, std::string obj_name)
{
  // May already exist
  cluster.pool_create(pool_name.c_str());

  test_loop(cluster, pool_name, obj_name);
}

void
test_erasure(Rados &cluster, std::string pool_name, std::string obj_name)
{
  string outs;
  bufferlist inbl;
  int ret;
  ret = cluster.mon_command(
    "{\"prefix\": \"osd erasure-code-profile set\", \"name\": \"testprofile\", \"profile\": [ \"k=2\", \"m=1\", \"ruleset-failure-domain=osd\"]}",
    inbl, NULL, &outs);
  if (ret < 0) {
    std::cerr << "mon_command erasure-code-profile set failed with " << ret << std::endl;
    exit(1);
  }
  //std::cout << outs << std::endl;

  outs.clear();
  ret = cluster.mon_command(
    "{\"prefix\": \"osd pool create\", \"pool\": \"" + pool_name + "\", \"pool_type\":\"erasure\", \"pg_num\":12, \"pgp_num\":12, \"erasure_code_profile\":\"testprofile\"}",
    inbl, NULL, &outs);
  if (ret < 0) {
    std::cerr << outs << std::endl;
    std::cerr << "mon_command create pool failed with " << ret << std::endl;
    exit(1);
  }
  //std::cout << outs << std::endl;

  cluster.wait_for_latest_osdmap();
  test_loop(cluster, pool_name, obj_name);
  return;
}

int main(int args, char **argv)
{
  if (args != 3 && args != 4) {
    std::cerr << "Error: " << argv[0] << " [ec|rep] pool_name obj_name" << std::endl;
    return 1;
  }

  std::string pool_name, obj_name, type;
  // For backward compatibility with unmodified teuthology version
  if (args == 3) {
    type = "rep";
    pool_name = argv[1];
    obj_name = argv[2];
  } else {
    type = argv[1];
    pool_name = argv[2];
    obj_name = argv[3];
  }
  std::cerr << "Test type " << type << std::endl;
  std::cerr << "pool_name, obj_name are " << pool_name << ", " << obj_name << std::endl;

  if (type != "ec" && type != "rep") {
    std::cerr << "Error: " << argv[0] << " Invalid arg must be 'ec' or 'rep' saw " << type << std::endl;
    return 1;
  }

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

  if (type == "rep")
    test_replicated(cluster, pool_name, obj_name);
  else if (type == "ec")
    test_erasure(cluster, pool_name, obj_name);

  sem_destroy(&sem);
  return 0;
}
