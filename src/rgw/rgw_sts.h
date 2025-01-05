// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_role.h"
#include "rgw_auth.h"
#include "rgw_web_idp.h"

namespace STS {

class AssumeRoleRequestBase {
protected:
  static constexpr uint64_t MIN_POLICY_SIZE = 1;
  static constexpr uint64_t MAX_POLICY_SIZE = 2048;
  static constexpr uint64_t DEFAULT_DURATION_IN_SECS = 3600;
  static constexpr uint64_t MIN_ROLE_ARN_SIZE = 2;
  static constexpr uint64_t MAX_ROLE_ARN_SIZE = 2048;
  static constexpr uint64_t MIN_ROLE_SESSION_SIZE = 2;
  static constexpr uint64_t MAX_ROLE_SESSION_SIZE = 64;
  uint64_t MIN_DURATION_IN_SECS;
  uint64_t MAX_DURATION_IN_SECS;
  CephContext* cct;
  uint64_t duration;
  std::string err_msg;
  std::string iamPolicy;
  std::string roleArn;
  std::string roleSessionName;
public:
  AssumeRoleRequestBase(CephContext* cct,
                        const std::string& duration,
                        const std::string& iamPolicy,
                        const std::string& roleArn,
                        const std::string& roleSessionName);
  const std::string& getRoleARN() const { return roleArn; }
  const std::string& getRoleSessionName() const { return roleSessionName; }
  const std::string& getPolicy() const {return iamPolicy; }
  static const uint64_t& getMaxPolicySize() { return MAX_POLICY_SIZE; }
  void setMaxDuration(const uint64_t& maxDuration) { MAX_DURATION_IN_SECS = maxDuration; }
  const uint64_t& getDuration() const { return duration; }
  int validate_input(const DoutPrefixProvider *dpp) const;
};

class AssumeRoleWithWebIdentityRequest : public AssumeRoleRequestBase {
  static constexpr uint64_t MIN_PROVIDER_ID_LEN = 4;
  static constexpr uint64_t MAX_PROVIDER_ID_LEN = 2048;
  std::string providerId;
  std::string iamPolicy;
  std::string iss;
  std::string sub;
  std::string aud;
  std::vector<std::pair<std::string,std::string>> session_princ_tags;
public:
  AssumeRoleWithWebIdentityRequest( CephContext* cct,
                      const std::string& duration,
                      const std::string& providerId,
                      const std::string& iamPolicy,
                      const std::string& roleArn,
                      const std::string& roleSessionName,
                      const std::string& iss,
                      const std::string& sub,
                      const std::string& aud,
                      std::vector<std::pair<std::string,std::string>> session_princ_tags)
    : AssumeRoleRequestBase(cct, duration, iamPolicy, roleArn, roleSessionName),
      providerId(providerId), iss(iss), sub(sub), aud(aud), session_princ_tags(session_princ_tags) {}
  const std::string& getProviderId() const { return providerId; }
  const std::string& getIss() const { return iss; }
  const std::string& getAud() const { return aud; }
  const std::string& getSub() const { return sub; }
  const std::vector<std::pair<std::string,std::string>>& getPrincipalTags() const { return session_princ_tags; }
  int validate_input(const DoutPrefixProvider *dpp) const;
};

class AssumeRoleRequest : public AssumeRoleRequestBase {
  static constexpr uint64_t MIN_EXTERNAL_ID_LEN = 2;
  static constexpr uint64_t MAX_EXTERNAL_ID_LEN = 1224;
  static constexpr uint64_t MIN_SERIAL_NUMBER_SIZE = 9;
  static constexpr uint64_t MAX_SERIAL_NUMBER_SIZE = 256;
  static constexpr uint64_t TOKEN_CODE_SIZE = 6;
  std::string externalId;
  std::string serialNumber;
  std::string tokenCode;
public:
  AssumeRoleRequest(CephContext* cct,
                    const std::string& duration,
                    const std::string& externalId,
                    const std::string& iamPolicy,
                    const std::string& roleArn,
                    const std::string& roleSessionName,
                    const std::string& serialNumber,
                    const std::string& tokenCode)
    : AssumeRoleRequestBase(cct, duration, iamPolicy, roleArn, roleSessionName),
      externalId(externalId), serialNumber(serialNumber), tokenCode(tokenCode){}
  int validate_input(const DoutPrefixProvider *dpp) const;
};

class GetSessionTokenRequest {
protected:
  static constexpr uint64_t MIN_DURATION_IN_SECS = 900;
  static constexpr uint64_t DEFAULT_DURATION_IN_SECS = 3600;
  uint64_t duration;
  std::string serialNumber;
  std::string tokenCode;

public:
  GetSessionTokenRequest(const std::string& duration, const std::string& serialNumber, const std::string& tokenCode);

  const uint64_t& getDuration() const { return duration; }
  static const uint64_t& getMinDuration() { return MIN_DURATION_IN_SECS; }
};

class AssumedRoleUser {
  std::string arn;
  std::string assumeRoleId;
public:
  int generateAssumedRoleUser( CephContext* cct,
                                rgw::sal::Driver* driver,
                                const std::string& roleId,
                                const rgw::ARN& roleArn,
                                const std::string& roleSessionName);
  const std::string& getARN() const { return arn; }
  const std::string& getAssumeRoleId() const { return assumeRoleId; }
  void dump(Formatter *f) const;
};

struct SessionToken {
  std::string access_key_id;
  std::string secret_access_key;
  std::string expiration;
  std::string policy;
  std::string roleId;
  rgw_user user;
  std::string acct_name;
  uint32_t perm_mask;
  bool is_admin;
  uint32_t acct_type;
  std::string role_session;
  std::vector<std::string> token_claims;
  std::string issued_at;
  std::vector<std::pair<std::string,std::string>> principal_tags;

  SessionToken() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 1, bl);
    encode(access_key_id, bl);
    encode(secret_access_key, bl);
    encode(expiration, bl);
    encode(policy, bl);
    encode(roleId, bl);
    encode(user, bl);
    encode(acct_name, bl);
    encode(perm_mask, bl);
    encode(is_admin, bl);
    encode(acct_type, bl);
    encode(role_session, bl);
    encode(token_claims, bl);
    encode(issued_at, bl);
    encode(principal_tags, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(5, bl);
    decode(access_key_id, bl);
    decode(secret_access_key, bl);
    decode(expiration, bl);
    decode(policy, bl);
    decode(roleId, bl);
    decode(user, bl);
    decode(acct_name, bl);
    decode(perm_mask, bl);
    decode(is_admin, bl);
    decode(acct_type, bl);
    if (struct_v >= 2) {
      decode(role_session, bl);
    }
    if (struct_v >= 3) {
      decode(token_claims, bl);
    }
    if (struct_v >= 4) {
      decode(issued_at, bl);
    }
    if (struct_v >= 5) {
      decode(principal_tags, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(SessionToken)

class Credentials {
  static constexpr int MAX_ACCESS_KEY_LEN = 20;
  static constexpr int MAX_SECRET_KEY_LEN = 40;
  std::string accessKeyId;
  std::string expiration;
  std::string secretAccessKey;
  std::string sessionToken;
public:
  int generateCredentials(const DoutPrefixProvider *dpp,
                          CephContext* cct,
                          const uint64_t& duration,
                          const boost::optional<std::string>& policy,
                          const boost::optional<std::string>& roleId,
                          const boost::optional<std::string>& role_session,
                          const boost::optional<std::vector<std::string>>& token_claims,
                          const boost::optional<std::vector<std::pair<std::string,std::string>>>& session_princ_tags,
                          boost::optional<rgw_user> user,
                          rgw::auth::Identity* identity);
  const std::string& getAccessKeyId() const { return accessKeyId; }
  const std::string& getExpiration() const { return expiration; }
  const std::string& getSecretAccessKey() const { return secretAccessKey; }
  const std::string& getSessionToken() const { return sessionToken; }
  void dump(Formatter *f) const;
};

struct AssumeRoleResponse {
  int retCode;
  AssumedRoleUser user;
  Credentials creds;
  uint64_t packedPolicySize;
};

struct AssumeRoleWithWebIdentityResponse {
  AssumeRoleResponse assumeRoleResp;
  std::string aud;
  std::string providerId;
  std::string sub;
};

using AssumeRoleResponse = struct AssumeRoleResponse ;
using GetSessionTokenResponse = std::tuple<int, Credentials>;
using AssumeRoleWithWebIdentityResponse = struct AssumeRoleWithWebIdentityResponse;

class STSService {
  CephContext* cct;
  rgw::sal::Driver* driver;
  rgw_user user_id;
  std::unique_ptr<rgw::sal::RGWRole> role;
  rgw::auth::Identity* identity;
public:
  STSService() = default;
  STSService(CephContext* cct, rgw::sal::Driver* driver, rgw_user user_id,
	     rgw::auth::Identity* identity)
    : cct(cct), driver(driver), user_id(user_id), identity(identity) {}
  std::tuple<int, rgw::sal::RGWRole*> getRoleInfo(const DoutPrefixProvider *dpp, const std::string& arn, optional_yield y);
  AssumeRoleResponse assumeRole(const DoutPrefixProvider *dpp, AssumeRoleRequest& req, optional_yield y);
  GetSessionTokenResponse getSessionToken(const DoutPrefixProvider *dpp, GetSessionTokenRequest& req);
  AssumeRoleWithWebIdentityResponse assumeRoleWithWebIdentity(const DoutPrefixProvider *dpp, AssumeRoleWithWebIdentityRequest& req);
};
}
