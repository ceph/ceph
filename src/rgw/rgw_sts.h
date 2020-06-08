// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_STS_H
#define CEPH_RGW_STS_H

#include "rgw_role.h"
#include "rgw_auth.h"
#include "rgw_web_idp.h"

namespace STS {

class AssumeRoleRequestBase {
protected:
  static constexpr uint64_t MIN_POLICY_SIZE = 1;
  static constexpr uint64_t MAX_POLICY_SIZE = 2048;
  static constexpr uint64_t DEFAULT_DURATION_IN_SECS = 3600;
  static constexpr uint64_t MIN_DURATION_IN_SECS = 900;
  static constexpr uint64_t MIN_ROLE_ARN_SIZE = 2;
  static constexpr uint64_t MAX_ROLE_ARN_SIZE = 2048;
  static constexpr uint64_t MIN_ROLE_SESSION_SIZE = 2;
  static constexpr uint64_t MAX_ROLE_SESSION_SIZE = 64;
  uint64_t MAX_DURATION_IN_SECS;
  uint64_t duration;
  string err_msg;
  string iamPolicy;
  string roleArn;
  string roleSessionName;
public:
  AssumeRoleRequestBase(const string& duration,
                        const string& iamPolicy,
                        const string& roleArn,
                        const string& roleSessionName);
  const string& getRoleARN() const { return roleArn; }
  const string& getRoleSessionName() const { return roleSessionName; }
  const string& getPolicy() const {return iamPolicy; }
  static const uint64_t& getMaxPolicySize() { return MAX_POLICY_SIZE; }
  void setMaxDuration(const uint64_t& maxDuration) { MAX_DURATION_IN_SECS = maxDuration; }
  const uint64_t& getDuration() const { return duration; }
  int validate_input() const;
};

class AssumeRoleWithWebIdentityRequest : public AssumeRoleRequestBase {
  static constexpr uint64_t MIN_PROVIDER_ID_LEN = 4;
  static constexpr uint64_t MAX_PROVIDER_ID_LEN = 2048;
  string providerId;
  string iamPolicy;
  string iss;
  string sub;
  string aud;
public:
  AssumeRoleWithWebIdentityRequest( const string& duration,
                      const string& providerId,
                      const string& iamPolicy,
                      const string& roleArn,
                      const string& roleSessionName,
                      const string& iss,
                      const string& sub,
                      const string& aud)
    : AssumeRoleRequestBase(duration, iamPolicy, roleArn, roleSessionName),
      providerId(providerId), iss(iss), sub(sub), aud(aud) {}
  const string& getProviderId() const { return providerId; }
  const string& getIss() const { return iss; }
  const string& getAud() const { return aud; }
  const string& getSub() const { return sub; }
  int validate_input() const;
};

class AssumeRoleRequest : public AssumeRoleRequestBase {
  static constexpr uint64_t MIN_EXTERNAL_ID_LEN = 2;
  static constexpr uint64_t MAX_EXTERNAL_ID_LEN = 1224;
  static constexpr uint64_t MIN_SERIAL_NUMBER_SIZE = 9;
  static constexpr uint64_t MAX_SERIAL_NUMBER_SIZE = 256;
  static constexpr uint64_t TOKEN_CODE_SIZE = 6;
  string externalId;
  string serialNumber;
  string tokenCode;
public:
  AssumeRoleRequest(const string& duration,
                    const string& externalId,
                    const string& iamPolicy,
                    const string& roleArn,
                    const string& roleSessionName,
                    const string& serialNumber,
                    const string& tokenCode)
    : AssumeRoleRequestBase(duration, iamPolicy, roleArn, roleSessionName),
      externalId(externalId), serialNumber(serialNumber), tokenCode(tokenCode){}
  int validate_input() const;
};

class GetSessionTokenRequest {
protected:
  static constexpr uint64_t MIN_DURATION_IN_SECS = 900;
  static constexpr uint64_t DEFAULT_DURATION_IN_SECS = 3600;
  uint64_t duration;
  string serialNumber;
  string tokenCode;

public:
  GetSessionTokenRequest(const string& duration, const string& serialNumber, const string& tokenCode);

  const uint64_t& getDuration() const { return duration; }
  static const uint64_t& getMinDuration() { return MIN_DURATION_IN_SECS; }
};

class AssumedRoleUser {
  string arn;
  string assumeRoleId;
public:
  int generateAssumedRoleUser( CephContext* cct,
                                rgw::sal::RGWRadosStore *store,
                                const string& roleId,
                                const rgw::ARN& roleArn,
                                const string& roleSessionName);
  const string& getARN() const { return arn; }
  const string& getAssumeRoleId() const { return assumeRoleId; }
  void dump(Formatter *f) const;
};

struct SessionToken {
  string access_key_id;
  string secret_access_key;
  string expiration;
  string policy;
  string roleId;
  rgw_user user;
  string acct_name;
  uint32_t perm_mask;
  bool is_admin;
  uint32_t acct_type;
  string role_session;

  SessionToken() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
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
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(2, bl);
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
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(SessionToken)

class Credentials {
  static constexpr int MAX_ACCESS_KEY_LEN = 20;
  static constexpr int MAX_SECRET_KEY_LEN = 40;
  string accessKeyId;
  string expiration;
  string secretAccessKey;
  string sessionToken;
public:
  int generateCredentials(CephContext* cct,
                          const uint64_t& duration,
                          const boost::optional<string>& policy,
                          const boost::optional<string>& roleId,
                          const boost::optional<string>& role_session,
                          boost::optional<rgw_user> user,
                          rgw::auth::Identity* identity);
  const string& getAccessKeyId() const { return accessKeyId; }
  const string& getExpiration() const { return expiration; }
  const string& getSecretAccessKey() const { return secretAccessKey; }
  const string& getSessionToken() const { return sessionToken; }
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
  string aud;
  string providerId;
  string sub;
};

using AssumeRoleResponse = struct AssumeRoleResponse ;
using GetSessionTokenResponse = std::tuple<int, Credentials>;
using AssumeRoleWithWebIdentityResponse = struct AssumeRoleWithWebIdentityResponse;

class STSService {
  CephContext* cct;
  rgw::sal::RGWRadosStore *store;
  rgw_user user_id;
  RGWRole role;
  rgw::auth::Identity* identity;
  int storeARN(string& arn);
public:
  STSService() = default;
  STSService(CephContext* cct, rgw::sal::RGWRadosStore *store, rgw_user user_id, rgw::auth::Identity* identity) : cct(cct), store(store), user_id(user_id), identity(identity) {}
  std::tuple<int, RGWRole> getRoleInfo(const string& arn);
  AssumeRoleResponse assumeRole(AssumeRoleRequest& req);
  GetSessionTokenResponse getSessionToken(GetSessionTokenRequest& req);
  AssumeRoleWithWebIdentityResponse assumeRoleWithWebIdentity(AssumeRoleWithWebIdentityRequest& req);
};
}
#endif /* CEPH_RGW_STS_H */

