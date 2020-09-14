// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test_cxx.h"

#include <errno.h>

#include <mutex>
#include <condition_variable>
#include <chrono>
#include <map>
#include <iostream>
#include <string>
#include <stdlib.h>
#include <unistd.h>

using namespace librados;
using std::map;
using std::string;

class WatchNotifyTestCtx : public WatchCtx
{
public:
  WatchNotifyTestCtx(std::mutex &lock)
    : lock{lock} {}
  void notify(uint8_t opcode, uint64_t ver, bufferlist &bl) override {
    std::unique_lock locker {lock};
    notified = true;
    cond.notify_one();
  }
  bool wait() {
    std::unique_lock locker {lock};
    return cond.wait_for(locker, std::chrono::seconds(1200),
			 [this] { return notified; });
  }

private:
  bool notified = false;
  std::mutex& lock;
  std::condition_variable cond;
};

#pragma GCC diagnostic ignored "-Wpragmas"
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
  ioctx.application_enable("rados", true);

  ret = ioctx.create(obj_name, false);
  if (ret < 0) {
    std::cerr << "create failed with " << ret << std::endl;
    exit(1);
  }

  std::mutex lock;
  constexpr int NR_ITERATIONS = 10000;
  for (int i = 0; i < NR_ITERATIONS; ++i) {
    std::cout << "Iteration " << i << std::endl;
    uint64_t handle;
    WatchNotifyTestCtx ctx{lock};
    ret = ioctx.watch(obj_name, 0, &handle, &ctx);
    ceph_assert(!ret);
    bufferlist bl2;
    ret = ioctx.notify(obj_name, 0, bl2);
    ceph_assert(!ret);
    ceph_assert_always(ctx.wait());
    ioctx.unwatch(obj_name, handle);
  }
  ioctx.close();
  ret = cluster.pool_delete(pool_name.c_str());
  if (ret < 0) {
    std::cerr << "pool_delete failed with " << ret << std::endl;
    exit(1);
  }
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

void
test_replicated(Rados &cluster, std::string pool_name, const std::string &obj_name)
{
  // May already exist
  cluster.pool_create(pool_name.c_str());

  test_loop(cluster, pool_name, obj_name);
}

void
test_erasure(Rados &cluster, const std::string &pool_name, const std::string &obj_name)
{
  string outs;
  bufferlist inbl;
  int ret;
  ret = cluster.mon_command(
    "{\"prefix\": \"osd erasure-code-profile set\", \"name\": \"testprofile\", \"profile\": [ \"k=2\", \"m=1\", \"crush-failure-domain=osd\"]}",
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
  std::cout << "Test type " << type << std::endl;
  std::cout << "pool_name, obj_name are " << pool_name << ", " << obj_name << std::endl;

  if (type != "ec" && type != "rep") {
    std::cerr << "Error: " << argv[0] << " Invalid arg must be 'ec' or 'rep' saw " << type << std::endl;
    return 1;
  }

  Rados cluster;
  std::string err = connect_cluster_pp(cluster);
  if (err.length()) {
      std::cerr << "Error " << err << std::endl;
      return 1;
  }

  if (type == "rep")
    test_replicated(cluster, pool_name, obj_name);
  else if (type == "ec")
    test_erasure(cluster, pool_name, obj_name);

  return 0;
}
