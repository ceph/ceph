#ifndef CEPH_STS_ASSUME_ROLE_H
#define CEPH_STS_ASSUME_ROLE_H

#include "rgw_role.h"

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

class Credentials {
  static constexpr int MAX_ACCESS_KEY_LEN = 64;
  string accessKeyId;
  string expiration;
  string secretAccessKey;
  string sessionToken;
public:
  int generateCredentials(CephContext* cct, const uint64_t& duration);
  const string& getAccessKeyId() const { return accessKeyId; }
  const string& getExpiration() const { return expiration; }
  const string& getSecretAccessKey() const { return secretAccessKey; }
  const string& getSessionToken() const { return sessionToken; }
  void dump(Formatter *f) const;
};

//AssumedRoleUser, Credentials, PackedpolicySize
using AssumeRoleResponse = std::tuple<int, AssumedRoleUser, Credentials, uint64_t> ;

class STSService {
  CephContext* cct;
  RGWRados *store;
  RGWRole role;
public:
  STSService() = default;
  STSService(CephContext* _cct, RGWRados *_store) : cct(_cct), store(_store) {}
  std::tuple<int, RGWRole> getRoleInfo(const string& arn);
  AssumeRoleResponse assumeRole(AssumeRoleRequest& req);
};
}
#endif /* CEPH_STS_ASSUME_ROLE_H */

