// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "common/ceph_context.h"
#include "rgw/rgw_common.h"
#include "rgw/rgw_kms.cc"

using ::testing::_;
using ::testing::Action;
using ::testing::ActionInterface;
using ::testing::MakeAction;


class MockTransitSecretEngine : public TransitSecretEngine {

public:
  MockTransitSecretEngine(CephContext *cct) : TransitSecretEngine(cct){}

  MOCK_METHOD(int, send_request, (std::string_view key_id, JSONParser* parser), (override));

};

class MockKvSecretEngine : public KvSecretEngine {

public:
  MockKvSecretEngine(CephContext *cct) : KvSecretEngine(cct){}

  MOCK_METHOD(int, send_request, (std::string_view key_id, JSONParser* parser), (override));

};

class TestSSEKMS : public ::testing::Test {

protected:
  CephContext *cct;
  MockTransitSecretEngine* transit_engine;
  MockKvSecretEngine* kv_engine;

  void SetUp() override {
    cct = (new CephContext(CEPH_ENTITY_TYPE_ANY))->get();
    transit_engine = new MockTransitSecretEngine(cct);
    kv_engine = new MockKvSecretEngine(cct);
  }

  void TearDown() {
    delete transit_engine;
    delete kv_engine;
  }

};


TEST_F(TestSSEKMS, vault_token_file_unset)
{
  cct->_conf.set_val("rgw_crypt_vault_auth", "token");
  TransitSecretEngine te(cct);
  KvSecretEngine kv(cct);

  std::string_view key_id("my_key");
  std::string actual_key;

  ASSERT_EQ(te.get_key(key_id, actual_key), -EINVAL);
  ASSERT_EQ(kv.get_key(key_id, actual_key), -EINVAL);
}


TEST_F(TestSSEKMS, non_existent_vault_token_file)
{
  cct->_conf.set_val("rgw_crypt_vault_auth", "token");
  cct->_conf.set_val("rgw_crypt_vault_token_file", "/nonexistent/file");
  TransitSecretEngine te(cct);
  KvSecretEngine kv(cct);

  std::string_view key_id("my_key/1");
  std::string actual_key;

  ASSERT_EQ(te.get_key(key_id, actual_key), -ENOENT);
  ASSERT_EQ(kv.get_key(key_id, actual_key), -ENOENT);
}


typedef int SendRequestMethod(std::string_view, JSONParser*);

class SetPointedValueAction : public ActionInterface<SendRequestMethod> {
 public:
  string json;

  SetPointedValueAction(std::string json){
    this->json = json;
  }

  int Perform(const ::std::tuple<std::string_view, JSONParser*>& args) override {
    JSONParser* p = ::std::get<1>(args);
    JSONParser* parser = new JSONParser();

    parser->parse(json.c_str(), json.length());
    *p = *parser;
    return 0;
  }
};

Action<SendRequestMethod> SetPointedValue(std::string json) {
  return MakeAction(new SetPointedValueAction(json));
}


TEST_F(TestSSEKMS, test_transit_key_version_extraction){
  string json = R"({"data": {"keys": {"6": "8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="}}})";
  EXPECT_CALL(*transit_engine, send_request(_, _)).WillOnce(SetPointedValue(json));

  std::string actual_key;
  std::string tests[11] {"/", "my_key/", "my_key", "", "my_key/a", "my_key/1a",
    "my_key/a1", "my_key/1a1", "my_key/1/a", "1", "my_key/1/"
  };

  int res;
  for (const auto &test: tests) {
    res = transit_engine->get_key(std::string_view(test), actual_key);
    ASSERT_EQ(res, -EINVAL);
  }

  res = transit_engine->get_key(std::string_view("1/2/3/4/5/6"), actual_key);
  ASSERT_EQ(res, 0);
  ASSERT_EQ(actual_key, from_base64("8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="));
}


TEST_F(TestSSEKMS, test_transit_backend){

  std::string_view my_key("my_key/1");
  std::string actual_key;

  // Mocks the expected return Value from Vault Server using custom Argument Action
  string json = R"({"data": {"keys": {"1": "8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="}}})";
  EXPECT_CALL(*transit_engine, send_request(_, _)).WillOnce(SetPointedValue(json));

  int res = transit_engine->get_key(my_key, actual_key);

  ASSERT_EQ(res, 0);
  ASSERT_EQ(actual_key, from_base64("8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="));
}


TEST_F(TestSSEKMS, test_kv_backend){

  std::string_view my_key("my_key");
  std::string actual_key;

  // Mocks the expected return value from Vault Server using custom Argument Action
  string json = R"({"data": {"data": {"key": "8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="}}})";
  EXPECT_CALL(*kv_engine, send_request(_, _)).WillOnce(SetPointedValue(json));

  int res = kv_engine->get_key(my_key, actual_key);

  ASSERT_EQ(res, 0);
  ASSERT_EQ(actual_key, from_base64("8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="));
}


TEST_F(TestSSEKMS, concat_url)
{
  // Each test has 3 strings:
  // * the base URL
  // * the path we want to concatenate
  // * the exepected final URL
  std::string tests[9][3] ={
    {"", "", ""},
    {"", "bar", "/bar"},
    {"", "/bar", "/bar"},
    {"foo", "", "foo"},
    {"foo", "bar", "foo/bar"},
    {"foo", "/bar", "foo/bar"},
    {"foo/", "", "foo/"},
    {"foo/", "bar", "foo/bar"},
    {"foo/", "/bar", "foo/bar"},
  };
  for (const auto &test: tests) {
    std::string url(test[0]), path(test[1]), expected(test[2]);
    concat_url(url, path);
    ASSERT_EQ(url, expected);
  }
}


TEST_F(TestSSEKMS, test_transit_backend_empty_response)
{
  std::string_view my_key("/key/nonexistent/1");
  std::string actual_key;

  // Mocks the expected return Value from Vault Server using custom Argument Action
  string json = R"({"errors": ["version does not exist or cannot be found"]})";
  EXPECT_CALL(*transit_engine, send_request(_, _)).WillOnce(SetPointedValue(json));

  int res = transit_engine->get_key(my_key, actual_key);

  ASSERT_EQ(res, -EINVAL);
  ASSERT_EQ(actual_key, from_base64(""));
}

