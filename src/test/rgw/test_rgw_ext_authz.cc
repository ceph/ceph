// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_ext_authz.h"

#include <string>

#include <gtest/gtest.h>

#include "common/ceph_json.h"

using namespace rgw::ext_authz;
using namespace rgw::ext_authz::ibm;

namespace {

const std::string good_crn =
    "crn:v1:bluemix:public:software-defined-storage:us-south:"
    "a/0123456789abcdef0123456789abcdef:11111111-2222-3333-4444-555555555555::";

RGWExtAuthzConfig make_config()
{
  RGWExtAuthzConfig c;
  c.enabled = true;
  c.provider = "ibm-iam";
  c.endpoint = "https://iam.cloud.ibm.com";
  c.service_credential = "secret-api-key";
  c.service_instance = good_crn;
  c.readonly_role_name = "fusion-reader";
  c.readwrite_role_name = "fusion-writer";
  return c;
}

Request make_request(const std::string& role_name)
{
  Request r;
  r.request_type = "AssumeRoleWithWebIdentity";
  r.subject_token = "hdr.payload.sig";
  r.subject = "IBMid-123";
  r.issuer = "iam.cloud.ibm.com/identity";
  r.account_id = "RGW11111111111111111";
  r.role_name = role_name;
  r.role_arn = "arn:aws:iam::RGW11111111111111111:role/" + role_name;
  r.correlation_id = "tx0000000000000000001-0001";
  return r;
}

} // namespace

// ---- CRN

TEST(ExtAuthzIbm, ParseCrn)
{
  Crn crn;
  ASSERT_TRUE(parse_crn(good_crn, crn));
  EXPECT_EQ("software-defined-storage", crn.service_name);
  EXPECT_EQ("0123456789abcdef0123456789abcdef", crn.account_id);
  EXPECT_EQ("11111111-2222-3333-4444-555555555555", crn.instance_id);
}

TEST(ExtAuthzIbm, ParseCrnRejectsMalformed)
{
  Crn crn;
  EXPECT_FALSE(parse_crn("", crn));
  EXPECT_FALSE(parse_crn("not-a-crn", crn));
  EXPECT_FALSE(parse_crn("crn:v2:bluemix:public:sds:us:a/acct:guid::", crn));
  // account scope missing the a/ prefix
  EXPECT_FALSE(parse_crn("crn:v1:bluemix:public:sds:us:acct:guid::", crn));
  // empty account, service or instance
  EXPECT_FALSE(parse_crn("crn:v1:bluemix:public:sds:us:a/:guid::", crn));
  EXPECT_FALSE(parse_crn("crn:v1:bluemix:public::us:a/acct:guid::", crn));
  EXPECT_FALSE(parse_crn("crn:v1:bluemix:public:sds:us:a/acct::::", crn));
  // too few fields
  EXPECT_FALSE(parse_crn("crn:v1:bluemix:public:sds:us:a/acct", crn));
}

// ---- action mapping

TEST(ExtAuthzIbm, ReadOnlyRoleMapsToReadOnly)
{
  EXPECT_EQ("software-defined-storage.s3.readonly",
            select_action(make_config(), make_request("fusion-reader")));
}

TEST(ExtAuthzIbm, ReadWriteRoleMapsToReadWrite)
{
  EXPECT_EQ("software-defined-storage.s3.readwrite",
            select_action(make_config(), make_request("fusion-writer")));
}

TEST(ExtAuthzIbm, CustomRoleRequiresAssumeRole)
{
  EXPECT_EQ("software-defined-storage.s3.assume-role",
            select_action(make_config(), make_request("my-custom-role")));
}

TEST(ExtAuthzIbm, StandardActionsAreNotUsedWhenNoPredefinedRolesConfigured)
{
  auto c = make_config();
  c.readonly_role_name.clear();
  c.readwrite_role_name.clear();
  // an empty role name must not match the empty configured names
  EXPECT_EQ("software-defined-storage.s3.assume-role",
            select_action(c, make_request("")));
  EXPECT_EQ("software-defined-storage.s3.assume-role",
            select_action(c, make_request("fusion-reader")));
}

TEST(ExtAuthzIbm, RoleNameMatchIsExact)
{
  EXPECT_EQ("software-defined-storage.s3.assume-role",
            select_action(make_config(), make_request("fusion-reader2")));
  EXPECT_EQ("software-defined-storage.s3.assume-role",
            select_action(make_config(), make_request("Fusion-Reader")));
}

TEST(ExtAuthzIbm, ConfiguredActionsAreUsed)
{
  auto c = make_config();
  c.action_readonly = "sds.read";
  c.action_readwrite = "sds.write";
  c.action_assume_role = "sds.assume";
  EXPECT_EQ("sds.read", select_action(c, make_request("fusion-reader")));
  EXPECT_EQ("sds.write", select_action(c, make_request("fusion-writer")));
  EXPECT_EQ("sds.assume", select_action(c, make_request("my-custom-role")));
}

TEST(ExtAuthzIbm, PredefinedRoleDetection)
{
  const auto c = make_config();
  EXPECT_TRUE(is_predefined_role(c, make_request("fusion-reader")));
  EXPECT_TRUE(is_predefined_role(c, make_request("fusion-writer")));
  EXPECT_FALSE(is_predefined_role(c, make_request("my-custom-role")));
  EXPECT_FALSE(is_predefined_role(c, make_request("")));
}

TEST(ExtAuthzIbm, CustomActionNameStillScopesCustomRoleToArn)
{
  // the role ARN is added by role kind, not by comparing action names
  auto c = make_config();
  c.action_assume_role = "sds.assume";
  Crn crn;
  ASSERT_TRUE(parse_crn(good_crn, crn));
  const auto req = make_request("my-custom-role");
  const auto body = build_pep_request(
      crn, select_action(c, req), req, !is_predefined_role(c, req));
  JSONParser p;
  ASSERT_TRUE(p.parse(body.c_str(), body.size()));
  JSONObj* r = *p.find_first();
  EXPECT_EQ("sds.assume", r->find_obj("action")->get_data());
  EXPECT_NE(nullptr,
            r->find_obj("resource")->find_obj("attributes")->find_obj("objectRole"));
}

// ---- PEP request

TEST(ExtAuthzIbm, PepRequestForCustomRoleCarriesRoleArn)
{
  Crn crn;
  ASSERT_TRUE(parse_crn(good_crn, crn));
  auto req = make_request("my-custom-role");
  const auto body = build_pep_request(
      crn, RGWExtAuthzConfig::default_action_assume_role, req, true);

  JSONParser p;
  ASSERT_TRUE(p.parse(body.c_str(), body.size()));
  ASSERT_TRUE(p.is_array());
  auto it = p.find_first();
  ASSERT_FALSE(it.end());
  JSONObj* r = *it;
  EXPECT_EQ("software-defined-storage.s3.assume-role",
            r->find_obj("action")->get_data());
  JSONObj* attrs = r->find_obj("resource")->find_obj("attributes");
  ASSERT_NE(nullptr, attrs);
  EXPECT_EQ("software-defined-storage", attrs->find_obj("serviceName")->get_data());
  EXPECT_EQ(crn.instance_id, attrs->find_obj("serviceInstance")->get_data());
  EXPECT_EQ(crn.account_id, attrs->find_obj("accountId")->get_data());
  EXPECT_EQ(req.account_id, attrs->find_obj("objectAccount")->get_data());
  EXPECT_EQ(req.role_arn, attrs->find_obj("objectRole")->get_data());
  // only the payload segment is sent, never the signed token
  EXPECT_EQ("payload",
            r->find_obj("subject")->find_obj("accessTokenBody")->get_data());
}

TEST(ExtAuthzIbm, JwtPayload)
{
  EXPECT_EQ("payload", jwt_payload("hdr.payload.sig"));
  EXPECT_EQ("", jwt_payload(""));
  EXPECT_EQ("", jwt_payload("nodots"));
  EXPECT_EQ("", jwt_payload("one.two"));
  EXPECT_EQ("", jwt_payload(".payload.sig"));
  EXPECT_EQ("", jwt_payload("hdr..sig"));
  EXPECT_EQ("", jwt_payload("hdr.payload."));
  EXPECT_EQ("", jwt_payload("a.b.c.d"));
}

TEST(ExtAuthzIbm, PepRequestForStandardActionHasNoRoleAttribute)
{
  Crn crn;
  ASSERT_TRUE(parse_crn(good_crn, crn));
  const auto body = build_pep_request(
      crn, RGWExtAuthzConfig::default_action_readonly,
      make_request("fusion-reader"), false);
  JSONParser p;
  ASSERT_TRUE(p.parse(body.c_str(), body.size()));
  JSONObj* attrs = (*p.find_first())->find_obj("resource")->find_obj("attributes");
  ASSERT_NE(nullptr, attrs);
  EXPECT_EQ(nullptr, attrs->find_obj("objectRole"));
  EXPECT_NE(nullptr, attrs->find_obj("objectAccount"));
}

// ---- role tag that binds a role to an authorizer

TEST(ExtAuthzRoleTag, UntaggedRoleHasNoRequirement)
{
  Request r;
  apply_role_tags(r, {{"team", "storage"}});
  EXPECT_TRUE(r.required_provider.empty());
  EXPECT_FALSE(r.authorizer_tag_invalid);
}

TEST(ExtAuthzRoleTag, TagValueNamesTheProvider)
{
  Request r;
  apply_role_tags(r, {{"team", "storage"}, {"rgw:ext-authz", "ibm-iam"}});
  EXPECT_EQ("ibm-iam", r.required_provider);
  EXPECT_FALSE(r.authorizer_tag_invalid);
}

TEST(ExtAuthzRoleTag, EmptyValueIsInvalid)
{
  Request r;
  apply_role_tags(r, {{"rgw:ext-authz", ""}});
  EXPECT_TRUE(r.authorizer_tag_invalid);
}

TEST(ExtAuthzRoleTag, ConflictingValuesAreInvalid)
{
  Request r;
  apply_role_tags(r, {{"rgw:ext-authz", "ibm-iam"}, {"rgw:ext-authz", "other"}});
  EXPECT_TRUE(r.authorizer_tag_invalid);
}

TEST(ExtAuthzRoleTag, RepeatedSameValueIsValid)
{
  Request r;
  apply_role_tags(r, {{"rgw:ext-authz", "ibm-iam"}, {"rgw:ext-authz", "ibm-iam"}});
  EXPECT_EQ("ibm-iam", r.required_provider);
  EXPECT_FALSE(r.authorizer_tag_invalid);
}

TEST(ExtAuthzRoleTag, ApplyResetsEarlierState)
{
  Request r;
  apply_role_tags(r, {{"rgw:ext-authz", ""}});
  ASSERT_TRUE(r.authorizer_tag_invalid);
  apply_role_tags(r, {});
  EXPECT_TRUE(r.required_provider.empty());
  EXPECT_FALSE(r.authorizer_tag_invalid);
}

TEST(ExtAuthzRoleTag, KeyMatchIsExact)
{
  // a near miss of the key does not bind the role, and is not an error
  Request r;
  apply_role_tags(r, {{"rgw:ext-authz ", "ibm-iam"}, {"RGW:EXT-AUTHZ", "ibm-iam"}});
  EXPECT_TRUE(r.required_provider.empty());
  EXPECT_FALSE(r.authorizer_tag_invalid);
}

// ---- PEP response: only an explicit boolean true is Allow

TEST(ExtAuthzIbm, PepResponseAllow)
{
  bool permitted = false;
  ASSERT_EQ(0, parse_pep_response(
      R"({"responses":[{"status":"200","authorizationDecision":{"permitted":true}}]})",
      permitted));
  EXPECT_TRUE(permitted);
}

TEST(ExtAuthzIbm, PepResponseDeny)
{
  bool permitted = true;
  ASSERT_EQ(0, parse_pep_response(
      R"({"responses":[{"authorizationDecision":{"permitted":false,"reason":"x"}}]})",
      permitted));
  EXPECT_FALSE(permitted);
}

TEST(ExtAuthzIbm, PepResponseMalformedIsAnError)
{
  bool permitted = true;
  for (const char* body : {
      "", "not json", "[]", "{}",
      R"({"responses":[]})",
      R"({"responses":{}})",
      R"({"responses":[{}]})",
      R"({"responses":[{"authorizationDecision":{}}]})",
      R"({"responses":[{"authorizationDecision":{"permitted":"true"}}]})",
      R"({"responses":[{"authorizationDecision":{"permitted":1}}]})",
      R"({"responses":[{"authorizationDecision":{"permitted":null}}]})",
      // an error status without a decision
      R"({"responses":[{"status":"403","error":{"errors":[]}}]})",
      // more than one decision for a single request
      R"({"responses":[{"authorizationDecision":{"permitted":true}},
                       {"authorizationDecision":{"permitted":true}}]})"}) {
    permitted = true;
    EXPECT_EQ(-EBADMSG, parse_pep_response(body, permitted)) << body;
  }
}

// ---- token response

TEST(ExtAuthzIbm, TokenResponse)
{
  std::string token;
  int64_t expires = 0;
  ASSERT_EQ(0, parse_token_response(
      R"({"access_token":"abc.def","token_type":"Bearer","expires_in":3600})",
      token, expires));
  EXPECT_EQ("abc.def", token);
  EXPECT_EQ(3600, expires);
}

TEST(ExtAuthzIbm, TokenResponseMalformed)
{
  std::string token;
  int64_t expires = 0;
  for (const char* body : {
      "", "x", "{}", R"({"access_token":"a"})", R"({"expires_in":10})",
      R"({"access_token":"","expires_in":10})",
      R"({"access_token":"a","expires_in":0})",
      R"({"access_token":"a","expires_in":-5})",
      R"({"access_token":"a","expires_in":"soon"})",
      R"({"access_token":5,"expires_in":10})"}) {
    EXPECT_EQ(-EBADMSG, parse_token_response(body, token, expires)) << body;
  }
}

// ---- config

TEST(ExtAuthzConfig, ValidateAccepts)
{
  std::string err;
  EXPECT_EQ(0, make_config().validate(err)) << err;
}

TEST(ExtAuthzConfig, ValidateRejects)
{
  std::string err;
  {
    auto c = make_config(); c.provider.clear();
    EXPECT_EQ(-EINVAL, c.validate(err));
  }
  {
    auto c = make_config(); c.endpoint = "http://iam.cloud.ibm.com";
    EXPECT_EQ(-EINVAL, c.validate(err)); // plain http is refused
  }
  {
    auto c = make_config(); c.endpoint.clear();
    EXPECT_EQ(-EINVAL, c.validate(err));
  }
  {
    auto c = make_config(); c.pep_endpoint = "http://pep";
    EXPECT_EQ(-EINVAL, c.validate(err));
  }
  {
    auto c = make_config();
    c.service_credential.clear(); c.service_credential_file.clear();
    EXPECT_EQ(-EINVAL, c.validate(err));
  }
  {
    auto c = make_config(); c.request_timeout_secs = 0;
    EXPECT_EQ(-EINVAL, c.validate(err)); // a zero timeout would never expire
  }
  {
    auto c = make_config(); c.connect_timeout_secs = 0;
    EXPECT_EQ(-EINVAL, c.validate(err));
  }
  {
    auto c = make_config();
    c.request_timeout_secs = RGWExtAuthzConfig::max_timeout_secs + 1;
    EXPECT_EQ(-EINVAL, c.validate(err));
  }
  {
    auto c = make_config(); c.readwrite_role_name = c.readonly_role_name;
    EXPECT_EQ(-EINVAL, c.validate(err));
  }
}

TEST(ExtAuthzConfig, ProviderValidatesCrn)
{
  auto* p = get_provider("ibm-iam");
  ASSERT_NE(nullptr, p);
  std::string err;
  EXPECT_EQ(0, p->validate_config(make_config(), err)) << err;

  auto c = make_config(); c.service_instance = "nope";
  EXPECT_EQ(-EINVAL, p->validate_config(c, err));
  c.service_instance =
      "crn:v1:bluemix:public:cloud-object-storage:global:a/acct:guid::";
  EXPECT_EQ(-EINVAL, p->validate_config(c, err)); // wrong service
}

TEST(ExtAuthzConfig, UnknownProvider)
{
  EXPECT_EQ(nullptr, get_provider("nope"));
  EXPECT_EQ(nullptr, get_provider(""));
}

TEST(ExtAuthzConfig, EncodeDecodeRoundTrip)
{
  auto c = make_config();
  c.pep_endpoint = "https://pep.example";
  c.tls_ca_bundle = "-----BEGIN CERTIFICATE-----\n...";
  c.service_credential_file = "/etc/ceph/key";
  c.connect_timeout_secs = 3;
  c.request_timeout_secs = 7;

  bufferlist bl;
  encode(c, bl);
  RGWExtAuthzConfig d;
  auto it = bl.cbegin();
  decode(d, it);
  EXPECT_EQ(c.enabled, d.enabled);
  EXPECT_EQ(c.provider, d.provider);
  EXPECT_EQ(c.endpoint, d.endpoint);
  EXPECT_EQ(c.pep_endpoint, d.pep_endpoint);
  EXPECT_EQ(c.service_credential, d.service_credential);
  EXPECT_EQ(c.service_credential_file, d.service_credential_file);
  EXPECT_EQ(c.service_instance, d.service_instance);
  EXPECT_EQ(c.tls_ca_bundle, d.tls_ca_bundle);
  EXPECT_EQ(c.connect_timeout_secs, d.connect_timeout_secs);
  EXPECT_EQ(c.request_timeout_secs, d.request_timeout_secs);
  EXPECT_EQ(c.readonly_role_name, d.readonly_role_name);
  EXPECT_EQ(c.readwrite_role_name, d.readwrite_role_name);
}

TEST(ExtAuthzConfig, ActionsEncodeDecodeAndDefaults)
{
  auto c = make_config();
  c.action_readonly = "a.ro";
  c.action_readwrite = "a.rw";
  c.action_assume_role = "a.ar";
  bufferlist bl;
  encode(c, bl);
  RGWExtAuthzConfig d;
  auto it = bl.cbegin();
  decode(d, it);
  EXPECT_EQ("a.ro", d.action_readonly);
  EXPECT_EQ("a.rw", d.action_readwrite);
  EXPECT_EQ("a.ar", d.action_assume_role);

  // json without actions keeps the defaults
  const std::string j = R"({"enabled":true,"provider":"ibm-iam"})";
  JSONParser p;
  ASSERT_TRUE(p.parse(j.c_str(), j.size()));
  RGWExtAuthzConfig e;
  e.decode_json(&p);
  EXPECT_EQ(RGWExtAuthzConfig::default_action_readonly, e.action_readonly);
  EXPECT_EQ(RGWExtAuthzConfig::default_action_readwrite, e.action_readwrite);
  EXPECT_EQ(RGWExtAuthzConfig::default_action_assume_role, e.action_assume_role);
}

TEST(ExtAuthzConfig, JsonWithoutTimeoutsKeepsDefaults)
{
  const std::string j = R"({"enabled":true,"provider":"ibm-iam"})";
  JSONParser p;
  ASSERT_TRUE(p.parse(j.c_str(), j.size()));
  RGWExtAuthzConfig e;
  e.decode_json(&p);
  EXPECT_EQ(RGWExtAuthzConfig::default_connect_timeout_secs,
            e.connect_timeout_secs);
  EXPECT_EQ(RGWExtAuthzConfig::default_request_timeout_secs,
            e.request_timeout_secs);
}

TEST(ExtAuthzConfig, ValidateRejectsBadActions)
{
  std::string err;
  auto c = make_config();
  c.action_readonly.clear();
  EXPECT_EQ(-EINVAL, c.validate(err));

  c = make_config();
  c.action_assume_role.clear();
  EXPECT_EQ(-EINVAL, c.validate(err));

  // equal actions would let a read-only grant assume other roles
  c = make_config();
  c.action_readwrite = c.action_readonly;
  EXPECT_EQ(-EINVAL, c.validate(err));
  c = make_config();
  c.action_assume_role = c.action_readonly;
  EXPECT_EQ(-EINVAL, c.validate(err));
  c = make_config();
  c.action_assume_role = c.action_readwrite;
  EXPECT_EQ(-EINVAL, c.validate(err));
}

TEST(ExtAuthzConfig, DefaultsAreDisabled)
{
  // a config that was never set must not change STS behaviour
  RGWExtAuthzConfig c;
  EXPECT_FALSE(c.enabled);
}

TEST(ExtAuthzConfig, JsonRoundTripAndRedaction)
{
  auto c = make_config();

  JSONFormatter plain;
  plain.open_object_section("");
  c.dump(&plain, false);
  plain.close_section();
  std::stringstream ps; plain.flush(ps);
  EXPECT_NE(std::string::npos, ps.str().find("secret-api-key"));

  JSONFormatter red;
  red.open_object_section("");
  c.dump(&red, true);
  red.close_section();
  std::stringstream rs; red.flush(rs);
  EXPECT_EQ(std::string::npos, rs.str().find("secret-api-key"));
  EXPECT_NE(std::string::npos, rs.str().find("<redacted>"));

  JSONParser p;
  ASSERT_TRUE(p.parse(ps.str().c_str(), ps.str().size()));
  RGWExtAuthzConfig d;
  d.decode_json(&p);
  EXPECT_EQ(c.service_credential, d.service_credential);
  EXPECT_EQ(c.service_instance, d.service_instance);
  EXPECT_TRUE(d.enabled);
}
