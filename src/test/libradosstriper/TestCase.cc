// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <thread>
#include <errno.h>
#include "test/librados/test.h"
#include "test/librados/test_cxx.h"
#include "test/libradosstriper/TestCase.h"

using std::thread;

using namespace libradosstriper;

std::string StriperTest::pool_name;
rados_t StriperTest::s_cluster = NULL;

void StriperTest::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &s_cluster));
}

void StriperTest::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool(pool_name, &s_cluster));
}

void StriperTest::SetUp()
{
  cluster = StriperTest::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  ASSERT_EQ(0, rados_striper_create(ioctx, &striper));
}

void StriperTest::TearDown()
{
  rados_striper_destroy(striper);
  rados_ioctx_destroy(ioctx);
}

std::string StriperTestPP::pool_name;
librados::Rados StriperTestPP::s_cluster;

void StriperTestPP::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void StriperTestPP::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void StriperTestPP::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  ASSERT_EQ(0, RadosStriper::striper_create(ioctx, &striper));
}

// this is pure copy and paste from previous class
// but for the inheritance from TestWithParam
// with gtest >= 1.6, we couldd avoid this by using
// inheritance from WithParamInterface
std::string StriperTestParam::pool_name;
librados::Rados StriperTestParam::s_cluster;

void StriperTestParam::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void StriperTestParam::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void StriperTestParam::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  ASSERT_EQ(0, RadosStriper::striper_create(ioctx, &striper));
}


std::string MultiStriperTest::pool_name;
librados::Rados MultiStriperTest::s_cluster;

void MultiStriperTest::init() {
  connect_cluster_pp(*cluster);
  cluster->ioctx_create(pool_name.c_str(), *ioctx);
  RadosStriper::striper_create(*ioctx, striper);
}

void MultiStriperTest::destory() {
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void ParallelMultiStriperTest::access(
  function<int(rados_striper_t&)> rw_access,
  const size_t n_threads) {
  refs.clear();

  auto do_access = [&] () {
    MultiStriperTest *multi_striper = new MultiStriperTest;
    multi_striper->init();
    rados_striper_t rsc;
    RadosStriper::to_rados_striper_t(*(multi_striper->striper), &rsc);
    multi_striper->ret = rw_access(rsc);
    std::lock_guard<mutex> guard(m);
    refs.push_back(multi_striper);
  };

  vector<thread *> threads;
  for(int i = 0; i < n_threads; i++) {
    auto t = new thread(do_access);
    threads.push_back(t);
  }

  for(auto t : threads) {
    t->join();
    delete t;
  }
}

void ParallelMultiStriperTest::async_read(char *buf, const size_t size, const size_t n_threads) {
  auto read_access = [&] (rados_striper_t& rsc) -> int {
    rados_completion_t comp;
    rados_aio_create_completion(nullptr, nullptr, nullptr, &comp);
    int ret = rados_striper_aio_read(rsc, MultiStriperTest::pool_name.c_str(), comp, buf, size, 0);

    if (ret >= 0) {
      rados_aio_wait_for_complete(comp);
    }

    rados_aio_release(comp);

    return ret;
  };

  access(read_access, n_threads);
}

void ParallelMultiStriperTest::async_write(char *buf, const size_t size, const size_t n_threads) {
  auto write_access = [&] (rados_striper_t& rsc) -> int {
    rados_completion_t comp;
    rados_aio_create_completion(nullptr, nullptr, nullptr, &comp);
    int ret = rados_striper_aio_write(rsc, MultiStriperTest::pool_name.c_str(), comp, buf, size, 0);

    if (ret >= 0) {
      rados_aio_wait_for_complete(comp);
    }

    rados_aio_release(comp);
    return ret;
  };

  access(write_access, n_threads);
}

void ParallelMultiStriperTest::sync_read(char *buf, const size_t size, const size_t n_threads) {
  auto read_access = [&] (rados_striper_t& rsc) -> int {
    return rados_striper_read(rsc,  MultiStriperTest::pool_name.c_str(), buf, size, 0);
  };

  access(read_access, n_threads);
}

void ParallelMultiStriperTest::sync_write(char *buf, const size_t size, const size_t n_threads) {
  auto write_access = [&] (rados_striper_t& rsc) -> int {
    return rados_striper_write(rsc, MultiStriperTest::pool_name.c_str(), buf, size, 0);
  };

  access(write_access, n_threads);
}
