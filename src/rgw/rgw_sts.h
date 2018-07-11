#ifndef CEPH_RGW_STS_H
#define CEPH_RGW_STS_H

#include "rgw_role.h"
#include "rgw_auth.h"

namespace STS {

class AssumeRoleRequest {
  static constexpr uint64_t MIN_POLICY_SIZE = 1;
  static constexpr uint64_t MAX_POLICY_SIZE = 2048;
  static constexpr uint64_t DEFAULT_DURATION_IN_SECS = 3600;
  static constexpr uint64_t MIN_DURATION_IN_SECS = 900;
  static constexpr uint64_t MIN_EXTERNAL_ID_LEN = 2;
  static constexpr uint64_t MAX_EXTERNAL_ID_LEN = 1224;
  static constexpr uint64_t MIN_ROLE_ARN_SIZE = 2;
  static constexpr uint64_t MAX_ROLE_ARN_SIZE = 2048;
  static constexpr uint64_t MIN_ROLE_SESSION_SIZE = 2;
  static constexpr uint64_t MAX_ROLE_SESSION_SIZE = 64;
  static constexpr uint64_t MIN_SERIAL_NUMBER_SIZE = 9;
  static constexpr uint64_t MAX_SERIAL_NUMBER_SIZE = 256;
  static constexpr uint64_t TOKEN_CODE_SIZE = 6;
  uint64_t MAX_DURATION_IN_SECS;
  uint64_t duration;
  string externalId;
  string iamPolicy;
  string roleArn;
  string roleSessionName;
  string serialNumber;
  string tokenCode;
public:
  AssumeRoleRequest( string& _duration,
                      string& _externalId,
                      string& _iamPolicy,
                      string& _roleArn,
                      string& _roleSessionName,
                      string& _serialNumber,
                      string& _tokenCode);
  const string& getRoleARN() const { return roleArn; }
  const string& getRoleSessionName() const { return roleSessionName; }
  const string& getPolicy() const {return iamPolicy; }
  static const uint64_t& getMaxPolicySize() { return MAX_POLICY_SIZE; }
  void setMaxDuration(const uint64_t& maxDuration) { MAX_DURATION_IN_SECS = maxDuration; }
  uint64_t& getDuration() { return duration; }
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
  GetSessionTokenRequest(string& duration, string& serialNumber, string& tokenCode);

  const uint64_t& getDuration() const { return duration; }
  static const uint64_t& getMinDuration() { return MIN_DURATION_IN_SECS; }
};

class AssumedRoleUser {
  string arn;
  string assumeRoleId;
public:
  int generateAssumedRoleUser( CephContext* cct,
                                RGWRados *store,
                                const string& roleId,
                                const rgw::IAM::ARN& roleArn,
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

  SessionToken() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
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
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
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
                          boost::optional<rgw_user> user,
                          rgw::auth::Identity* identity);
  const string& getAccessKeyId() const { return accessKeyId; }
  const string& getExpiration() const { return expiration; }
  const string& getSecretAccessKey() const { return secretAccessKey; }
  const string& getSessionToken() const { return sessionToken; }
  void dump(Formatter *f) const;
};

//AssumedRoleUser, Credentials, PackedpolicySize
using AssumeRoleResponse = std::tuple<int, AssumedRoleUser, Credentials, uint64_t> ;
using GetSessionTokenResponse = std::tuple<int, Credentials>;

class STSService {
  CephContext* cct;
  RGWRados *store;
  rgw_user user_id;
  RGWRole role;
  rgw::auth::Identity* identity;
  int storeARN(string& arn);
public:
  STSService() = default;
  STSService(CephContext* cct, RGWRados *store, rgw_user user_id, rgw::auth::Identity* identity) : cct(cct), store(store), user_id(user_id), identity(identity) {}
  std::tuple<int, RGWRole> getRoleInfo(const string& arn);
  AssumeRoleResponse assumeRole(AssumeRoleRequest& req);
  GetSessionTokenResponse getSessionToken(GetSessionTokenRequest& req);
};
}
#endif /* CEPH_RGW_STS_H */

