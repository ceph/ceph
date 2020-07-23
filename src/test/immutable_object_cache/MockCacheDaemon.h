// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef IMMUTABLE_OBJECT_CACHE_MOCK_DAEMON
#define IMMUTABLE_OBJECT_CACHE_MOCK_DAEMON

#include <iostream>
#include <unistd.h>

#include "gmock/gmock.h"

#include "include/Context.h"
#include "tools/immutable_object_cache/CacheClient.h"

namespace ceph {
namespace immutable_obj_cache {

class MockCacheClient {
 public:
  MockCacheClient(const std::string& file, CephContext* ceph_ctx) {}
  MOCK_METHOD0(run, void());
  MOCK_METHOD0(is_session_work, bool());
  MOCK_METHOD0(close, void());
  MOCK_METHOD0(stop, void());
  MOCK_METHOD0(connect, int());
  MOCK_METHOD1(connect, void(Context*));
  MOCK_METHOD5(lookup_object, void(std::string, uint64_t, uint64_t, std::string,
                                   CacheGenContextURef));
  MOCK_METHOD1(register_client, int(Context*));
};

class MockCacheServer {
 public:
  MockCacheServer(CephContext* cct, const std::string& file,
                  ProcessMsg processmsg) {
  }
  MOCK_METHOD0(run, int());
  MOCK_METHOD0(start_accept, int()); 
  MOCK_METHOD0(stop, int());
};

}  // namespace immutable_obj_cach3
}  // namespace ceph

#endif  // IMMUTABLE_OBJECT_CACHE_MOCK_DAEMON
