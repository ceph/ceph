// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "common/ceph_context.h"
#include "rgw_common.h"
#define FORTEST_VIRTUAL virtual
#include "rgw_kms.cc"

using ::testing::_;
using ::testing::Action;
using ::testing::ActionInterface;
using ::testing::MakeAction;
using ::testing::StrEq;


class MockTransitSecretEngine : public TransitSecretEngine {

public:
  MockTransitSecretEngine(CephContext *cct, SSEContext & kctx, EngineParmMap parms) : TransitSecretEngine(cct, kctx, parms){}

  MOCK_METHOD(int, send_request, (const DoutPrefixProvider *dpp, const char *method, std::string_view infix, std::string_view key_id, const std::string& postdata, optional_yield y, bufferlist &bl), (override));

};

class MockKvSecretEngine : public KvSecretEngine {

public:
  MockKvSecretEngine(CephContext *cct, SSEContext & kctx, EngineParmMap parms) : KvSecretEngine(cct, kctx, parms){}

  MOCK_METHOD(int, send_request, (const DoutPrefixProvider *dpp, const char *method, std::string_view infix, std::string_view key_id, const std::string& postdata, optional_yield y, bufferlist &bl), (override));

};

class TestSSEKMS : public ::testing::Test {

protected:
  CephContext *cct;
  MockTransitSecretEngine* old_engine;
  MockKvSecretEngine* kv_engine;
  MockTransitSecretEngine* transit_engine;

  void SetUp() override {
    EngineParmMap old_parms, kv_parms, new_parms;
    cct = (new CephContext(CEPH_ENTITY_TYPE_ANY))->get();
    KMSContext kctx { cct };
    old_parms["compat"] = "2";
    old_engine = new MockTransitSecretEngine(cct, kctx, std::move(old_parms));
    kv_engine = new MockKvSecretEngine(cct, kctx, std::move(kv_parms));
    new_parms["compat"] = "1";
    transit_engine = new MockTransitSecretEngine(cct, kctx, std::move(new_parms));
  }

  void TearDown() {
    delete old_engine;
    delete kv_engine;
    delete transit_engine;
  }

};


TEST_F(TestSSEKMS, vault_token_file_unset)
{
  cct->_conf.set_val("rgw_crypt_vault_auth", "token");
  EngineParmMap old_parms, kv_parms;
  KMSContext kctx { cct };
  TransitSecretEngine te(cct, kctx, std::move(old_parms));
  KvSecretEngine kv(cct, kctx, std::move(kv_parms));
  const NoDoutPrefix no_dpp(cct, 1);

  std::string_view key_id("my_key");
  std::string actual_key;

  ASSERT_EQ(te.get_key(&no_dpp, key_id, null_yield, actual_key), -EINVAL);
  ASSERT_EQ(kv.get_key(&no_dpp, key_id, null_yield, actual_key), -EINVAL);
}


TEST_F(TestSSEKMS, non_existent_vault_token_file)
{
  cct->_conf.set_val("rgw_crypt_vault_auth", "token");
  cct->_conf.set_val("rgw_crypt_vault_token_file", "/nonexistent/file");
  EngineParmMap old_parms, kv_parms;
  KMSContext kctx { cct };
  TransitSecretEngine te(cct, kctx, std::move(old_parms));
  KvSecretEngine kv(cct, kctx, std::move(kv_parms));
  const NoDoutPrefix no_dpp(cct, 1);

  std::string_view key_id("my_key/1");
  std::string actual_key;

  ASSERT_EQ(te.get_key(&no_dpp, key_id, null_yield, actual_key), -ENOENT);
  ASSERT_EQ(kv.get_key(&no_dpp, key_id, null_yield, actual_key), -ENOENT);
}


typedef int SendRequestMethod(const DoutPrefixProvider *dpp, const char *,
  std::string_view, std::string_view,
  const std::string &, optional_yield, bufferlist &);

class SetPointedValueAction : public ActionInterface<SendRequestMethod> {
 public:
  std::string json;

  SetPointedValueAction(std::string json){
    this->json = json;
  }

  int Perform(const ::std::tuple<const DoutPrefixProvider*, const char *, std::string_view, std::string_view, const std::string &, optional_yield, bufferlist &>& args) override {
//    const DoutPrefixProvider *dpp = ::std::get<0>(args);
//    const char *method = ::std::get<1>(args);
//    std::string_view infix = ::std::get<2>(args);
//    std::string_view key_id = ::std::get<3>(args);
//    const std::string& postdata = ::std::get<4>(args);
//    optional_yield y = ::std::get<5>(args);
    bufferlist& bl = ::std::get<6>(args);

// std::cout << "method = " << method << " infix = " << infix << " key_id = " << key_id
// << " postdata = " << postdata
// << " => json = " << json
// << std::endl;

    bl.append(json);
	// note: in the bufferlist, the string is not
	// necessarily 0 terminated at this point.  Logic in
	// rgw_kms.cc must handle this (by appending a 0.)
    return 0;
  }
};

Action<SendRequestMethod> SetPointedValue(std::string json) {
  return MakeAction(new SetPointedValueAction(json));
}


TEST_F(TestSSEKMS, test_transit_key_version_extraction){
  const NoDoutPrefix no_dpp(cct, 1);
  string json = R"({"data": {"keys": {"6": "8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="}}})";
  EXPECT_CALL(*old_engine, send_request(&no_dpp, StrEq("GET"), StrEq(""), StrEq("1/2/3/4/5/6"), StrEq(""), _, _)).WillOnce(SetPointedValue(json));

  std::string actual_key;
  std::string tests[11] {"/", "my_key/", "my_key", "", "my_key/a", "my_key/1a",
    "my_key/a1", "my_key/1a1", "my_key/1/a", "1", "my_key/1/"
  };

  int res;
  for (const auto &test: tests) {
    res = old_engine->get_key(&no_dpp, std::string_view(test), null_yield, actual_key);
    ASSERT_EQ(res, -EINVAL);
  }

  res = old_engine->get_key(&no_dpp, std::string_view("1/2/3/4/5/6"), null_yield, actual_key);
  ASSERT_EQ(res, 0);
  ASSERT_EQ(actual_key, from_base64("8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="));
}


TEST_F(TestSSEKMS, test_transit_backend){

  std::string_view my_key("my_key/1");
  std::string actual_key;

  // Mocks the expected return Value from Vault Server using custom Argument Action
  string json = R"({"data": {"keys": {"1": "8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="}}})";
  const NoDoutPrefix no_dpp(cct, 1);
  EXPECT_CALL(*old_engine, send_request(&no_dpp, StrEq("GET"), StrEq(""), StrEq("my_key/1"), StrEq(""), _, _)).WillOnce(SetPointedValue(json));

  int res = old_engine->get_key(&no_dpp, my_key, null_yield, actual_key);

  ASSERT_EQ(res, 0);
  ASSERT_EQ(actual_key, from_base64("8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="));
}


TEST_F(TestSSEKMS, test_transit_makekey){

  std::string_view my_key("my_key");
  std::string actual_key;
  map<string, bufferlist> attrs;
  const NoDoutPrefix no_dpp(cct, 1);

  // Mocks the expected return Value from Vault Server using custom Argument Action
  string post_json = R"({"data": {"ciphertext": "vault:v2:HbdxLnUztGVo+RseCIaYVn/4wEUiJNT6GQfw57KXQmhXVe7i1/kgLWegEPg1I6lexhIuXAM6Q2YvY0aZ","key_version": 1,"plaintext": "3xfTra/dsIf3TMa3mAT2IxPpM7YWm/NvUb4gDfSDX4g="}})";
  EXPECT_CALL(*transit_engine, send_request(&no_dpp, StrEq("POST"), StrEq("/datakey/plaintext/"), StrEq("my_key"), _, _, _))
		.WillOnce(SetPointedValue(post_json));

  set_attr(attrs, RGW_ATTR_CRYPT_CONTEXT, R"({"aws:s3:arn": "fred"})");
  set_attr(attrs, RGW_ATTR_CRYPT_KEYID, my_key);

  int res = transit_engine->make_actual_key(&no_dpp, attrs, null_yield, actual_key);
  std::string cipher_text { get_str_attribute(attrs,RGW_ATTR_CRYPT_DATAKEY) };

  ASSERT_EQ(res, 0);
  ASSERT_EQ(actual_key, from_base64("3xfTra/dsIf3TMa3mAT2IxPpM7YWm/NvUb4gDfSDX4g="));
  ASSERT_EQ(cipher_text, "vault:v2:HbdxLnUztGVo+RseCIaYVn/4wEUiJNT6GQfw57KXQmhXVe7i1/kgLWegEPg1I6lexhIuXAM6Q2YvY0aZ");
}

TEST_F(TestSSEKMS, test_transit_reconstitutekey){

  std::string_view my_key("my_key");
  std::string actual_key;
  map<string, bufferlist> attrs;
  const NoDoutPrefix no_dpp(cct, 1);

  // Mocks the expected return Value from Vault Server using custom Argument Action
  set_attr(attrs, RGW_ATTR_CRYPT_DATAKEY, "vault:v2:HbdxLnUztGVo+RseCIaYVn/4wEUiJNT6GQfw57KXQmhXVe7i1/kgLWegEPg1I6lexhIuXAM6Q2YvY0aZ");
  string post_json = R"({"data": {"key_version": 1,"plaintext": "3xfTra/dsIf3TMa3mAT2IxPpM7YWm/NvUb4gDfSDX4g="}})";
  EXPECT_CALL(*transit_engine, send_request(&no_dpp, StrEq("POST"), StrEq("/decrypt/"), StrEq("my_key"), _, _, _))
		.WillOnce(SetPointedValue(post_json));

  set_attr(attrs, RGW_ATTR_CRYPT_CONTEXT, R"({"aws:s3:arn": "fred"})");
  set_attr(attrs, RGW_ATTR_CRYPT_KEYID, my_key);

  int res = transit_engine->reconstitute_actual_key(&no_dpp, attrs, null_yield, actual_key);

  ASSERT_EQ(res, 0);
  ASSERT_EQ(actual_key, from_base64("3xfTra/dsIf3TMa3mAT2IxPpM7YWm/NvUb4gDfSDX4g="));
}

TEST_F(TestSSEKMS, test_kv_backend){

  std::string_view my_key("my_key");
  std::string actual_key;
  const NoDoutPrefix no_dpp(cct, 1);

  // Mocks the expected return value from Vault Server using custom Argument Action
  string json = R"({"data": {"data": {"key": "8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="}}})";
  EXPECT_CALL(*kv_engine, send_request(&no_dpp, StrEq("GET"), StrEq(""), StrEq("my_key"), StrEq(""), _, _))
		.WillOnce(SetPointedValue(json));

  int res = kv_engine->get_key(&no_dpp, my_key, null_yield, actual_key);

  ASSERT_EQ(res, 0);
  ASSERT_EQ(actual_key, from_base64("8qgPWvdtf6zrriS5+nkOzDJ14IGVR6Bgkub5dJn6qeg="));
}


TEST_F(TestSSEKMS, concat_url)
{
  // Each test has 3 strings:
  // * the base URL
  // * the path we want to concatenate
  // * the expected final URL
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


TEST_F(TestSSEKMS, string_ends_maybe_slash)
{
  struct { std::string hay, needle; bool expected; } tests[] ={
    {"jack here", "fred", false},
    {"here is a fred", "fred", true},
    {"and a fred/", "fred", true},
    {"no fred here", "fred", false},
    {"double fred//", "fred", true},
  };
  for (const auto &test: tests) {
    bool expected { string_ends_maybe_slash(test.hay, test.needle) };
    ASSERT_EQ(expected, test.expected);
  }
}


TEST_F(TestSSEKMS, test_transit_backend_empty_response)
{
  std::string_view my_key("/key/nonexistent/1");
  std::string actual_key;
  const NoDoutPrefix no_dpp(cct, 1);

  // Mocks the expected return Value from Vault Server using custom Argument Action
  string json = R"({"errors": ["version does not exist or cannot be found"]})";
  EXPECT_CALL(*old_engine, send_request(&no_dpp, StrEq("GET"), StrEq(""), StrEq("/key/nonexistent/1"), StrEq(""), _, _)).WillOnce(SetPointedValue(json));

  int res = old_engine->get_key(&no_dpp, my_key, null_yield, actual_key);

  ASSERT_EQ(res, -EINVAL);
  ASSERT_EQ(actual_key, from_base64(""));
}
