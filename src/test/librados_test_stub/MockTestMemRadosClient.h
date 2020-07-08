// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRADOS_TEST_STUB_MOCK_TEST_MEM_RADOS_CLIENT_H
#define LIBRADOS_TEST_STUB_MOCK_TEST_MEM_RADOS_CLIENT_H

#include "test/librados_test_stub/TestMemRadosClient.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "gmock/gmock.h"

namespace librados {

class TestMemCluster;

class MockTestMemRadosClient : public TestMemRadosClient {
public:
  MockTestMemRadosClient(CephContext *cct, TestMemCluster *test_mem_cluster)
    : TestMemRadosClient(cct, test_mem_cluster) {
    default_to_dispatch();
  }

  MOCK_METHOD0(connect, int());
  int do_connect() {
    return TestMemRadosClient::connect();
  }

  MOCK_METHOD2(create_ioctx, TestIoCtxImpl *(int64_t pool_id,
                                             const std::string &pool_name));
  MockTestMemIoCtxImpl* do_create_ioctx(int64_t pool_id,
                                        const std::string &pool_name) {
    return new ::testing::NiceMock<MockTestMemIoCtxImpl>(
      this, this, pool_id, pool_name,
      get_mem_cluster()->get_pool(pool_name));
  }

  MOCK_METHOD2(blacklist_add, int(const std::string& client_address,
                                  uint32_t expire_seconds));
  int do_blacklist_add(const std::string& client_address,
                       uint32_t expire_seconds) {
    return TestMemRadosClient::blacklist_add(client_address, expire_seconds);
  }

  MOCK_METHOD1(get_min_compatible_osd, int(int8_t*));
  int do_get_min_compatible_osd(int8_t* require_osd_release) {
    return TestMemRadosClient::get_min_compatible_osd(require_osd_release);
  }

  MOCK_METHOD2(get_min_compatible_client, int(int8_t*, int8_t*));
  int do_get_min_compatible_client(int8_t* min_compat_client,
                                   int8_t* require_min_compat_client) {
    return TestMemRadosClient::get_min_compatible_client(
      min_compat_client, require_min_compat_client);
  }

  MOCK_METHOD3(service_daemon_register,
               int(const std::string&,
                   const std::string&,
                   const std::map<std::string,std::string>&));
  int do_service_daemon_register(const std::string& service,
                                 const std::string& name,
                                 const std::map<std::string,std::string>& metadata) {
    return TestMemRadosClient::service_daemon_register(service, name, metadata);
  }

  // workaround of https://github.com/google/googletest/issues/1155
  MOCK_METHOD1(service_daemon_update_status_r,
               int(const std::map<std::string,std::string>&));
  int do_service_daemon_update_status_r(const std::map<std::string,std::string>& status) {
    auto s = status;
    return TestMemRadosClient::service_daemon_update_status(std::move(s));
  }

  MOCK_METHOD4(mon_command, int(const std::vector<std::string>&,
                                const bufferlist&, bufferlist*, std::string*));
  int do_mon_command(const std::vector<std::string>& cmd,
                     const bufferlist &inbl, bufferlist *outbl,
                     std::string *outs) {
    return mon_command(cmd, inbl, outbl, outs);
  }

  MOCK_METHOD0(wait_for_latest_osd_map, int());
  int do_wait_for_latest_osd_map() {
    return wait_for_latest_osd_map();
  }

  void default_to_dispatch() {
    using namespace ::testing;

    ON_CALL(*this, connect()).WillByDefault(Invoke(this, &MockTestMemRadosClient::do_connect));
    ON_CALL(*this, create_ioctx(_, _)).WillByDefault(Invoke(this, &MockTestMemRadosClient::do_create_ioctx));
    ON_CALL(*this, blacklist_add(_, _)).WillByDefault(Invoke(this, &MockTestMemRadosClient::do_blacklist_add));
    ON_CALL(*this, get_min_compatible_osd(_)).WillByDefault(Invoke(this, &MockTestMemRadosClient::do_get_min_compatible_osd));
    ON_CALL(*this, get_min_compatible_client(_, _)).WillByDefault(Invoke(this, &MockTestMemRadosClient::do_get_min_compatible_client));
    ON_CALL(*this, service_daemon_register(_, _, _)).WillByDefault(Invoke(this, &MockTestMemRadosClient::do_service_daemon_register));
    ON_CALL(*this, service_daemon_update_status_r(_)).WillByDefault(Invoke(this, &MockTestMemRadosClient::do_service_daemon_update_status_r));
    ON_CALL(*this, mon_command(_, _, _, _)).WillByDefault(Invoke(this, &MockTestMemRadosClient::do_mon_command));
    ON_CALL(*this, wait_for_latest_osd_map()).WillByDefault(Invoke(this, &MockTestMemRadosClient::do_wait_for_latest_osd_map));
  }
};

} // namespace librados

#endif // LIBRADOS_TEST_STUB_MOCK_TEST_MEM_RADOS_CLIENT_H
