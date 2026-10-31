// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <cstdint>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "global/global_context.h"
#include "rgw_client_io.h"
#include "rgw_log.h"
#include "rgw_process_env.h"
#include "driver/rados/rgw_sal_rados.h"

namespace {
class TestAccounter : public rgw::io::Accounter, public rgw::io::BasicClient {
public:
  RGWEnv env;
  int init_env(CephContext* cct) override { env.init(cct); return 0; }
  void set_account(bool) override {}
  uint64_t get_bytes_sent() const override { return 100; }
  uint64_t get_bytes_received() const override { return 50; }
  RGWEnv& get_env() noexcept override { return env; }
  size_t complete_request() override { return 0; }
};

class CaptureSink : public OpsLogSink {
public:
  uint64_t sent = 0;
  uint64_t received = 0;
  unsigned calls = 0;
  std::string status;
  int log(req_state*, rgw_log_entry& entry) override {
    sent = entry.bytes_sent;
    received = entry.bytes_received;
    status = entry.http_status;
    ++calls;
    return 0;
  }
};

struct AccountingCase {
  const char* name;
  RGWOpType op;
  const char* method;
  uint64_t rdma;
  unsigned status;
  uint64_t sent;
  uint64_t received;
};

class CuObjLog : public ::testing::TestWithParam<AccountingCase> {};

TEST_P(CuObjLog, AddsRdmaBytesOnlyInTheTransferDirection)
{
  const auto& test = GetParam();
  RGWProcessEnv penv;
  TestAccounter client;
  ASSERT_EQ(0, client.init(g_ceph_context));
  client.env.set("REQUEST_METHOD", test.method);
  req_state state(g_ceph_context, penv, &client.env, 0);
  state.cio = &client;
  state.user = std::make_unique<rgw::sal::RadosUser>(nullptr, rgw_user("test"));
  state.enable_ops_log = true;
  state.enable_usage_log = false;
  state.op_type = test.op;
  state.rdma_bytes_transferred = test.rdma;
  state.err.http_ret = test.status;
  CaptureSink sink;
  ASSERT_EQ(0, rgw_log_op(nullptr, &state, nullptr, &sink));
  ASSERT_EQ(sink.calls, 1);
  EXPECT_EQ(sink.sent, test.sent);
  EXPECT_EQ(sink.received, test.received);
  EXPECT_EQ(sink.status, std::to_string(test.status));
}

const AccountingCase accounting_cases[] = {
  {"RdmaGet", RGW_OP_GET_OBJ, "GET", 4096, 200, 4196, 50},
  {"RdmaPut", RGW_OP_PUT_OBJ, "PUT", 4096, 200, 100, 4146},
  {"HttpGet", RGW_OP_GET_OBJ, "GET", 0, 200, 100, 50},
  {"HttpPut", RGW_OP_PUT_OBJ, "PUT", 0, 200, 100, 50},
  {"Head", RGW_OP_GET_OBJ, "HEAD", 0, 200, 100, 50},
  {"ShortGet", RGW_OP_GET_OBJ, "GET", 2, 500, 102, 50},
  {"FailedGet", RGW_OP_GET_OBJ, "GET", 0, 500, 100, 50},
  {"FailedPut", RGW_OP_PUT_OBJ, "PUT", 0, 500, 100, 50},
  {"LargeGet", RGW_OP_GET_OBJ, "GET", 1ULL << 32, 200, (1ULL << 32) + 100, 50},
  {"LargePut", RGW_OP_PUT_OBJ, "PUT", 1ULL << 32, 200, 100, (1ULL << 32) + 50},
  {"ListBucket", RGW_OP_LIST_BUCKET, "GET", 0, 200, 100, 50},
  {"OtherOp", RGW_OP_LIST_BUCKET, "GET", 4096, 200, 100, 50},
};

INSTANTIATE_TEST_SUITE_P(Transfers, CuObjLog,
                        ::testing::ValuesIn(accounting_cases),
                        [](const ::testing::TestParamInfo<AccountingCase>& info) {
                          return info.param.name;
                        });
} // anonymous namespace
