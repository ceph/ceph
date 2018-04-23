#ifndef CEPH_STS_ASSUME_ROLE_H
#define CEPH_STS_ASSUME_ROLE_H

namespace STS {

class AssumeRoleRequest {
  static constexpr int MAX_POLICY_SIZE = 2048;
  uint64_t duration;
  string externalId;
  string iamPolicy;
  string roleArn;
  string roleSessionName;
  string serialNumber;
  string tokenCode;
public:
  AssumeRoleRequest(uint64_t _duration, string _externalId, string _iamPolicy,
                    string _roleArn, string _roleSessionName, string _serialNumber,
                    string _tokenCode)
                    : duration(_duration), externalId(_externalId), iamPolicy(_iamPolicy),
                      roleArn(_roleArn), roleSessionName(_roleSessionName),
                      serialNumber(_serialNumber), tokenCode(_tokenCode) {}
  string getRoleARN() const { return roleArn; }
  string getRoleSessionName() const { return roleSessionName; }
  string getPolicy() const {return iamPolicy; }
  int getMaxPolicySize() const { return MAX_POLICY_SIZE; }
};


class AssumedRoleUser {
  string arn;
  string assumeRoleId;
public:
  int generateAssumedRoleUser( CephContext* cct,
                                RGWRados *store,
                                const string& roleArn,
                                const string& roleSessionName);
  string getARN() const { return arn; }
  string getAssumeRoleId() const { return assumeRoleId; }
};

class Credentials {
  static constexpr int MAX_ACCESS_KEY_LEN = 64;
  static constexpr int EXPIRATION_TIME_IN_SECS = 86400; // 1 day
  string accessKeyId;
  string expiration;
  string secretAccessKey;
  string sessionToken;
public:
  int generateCredentials(CephContext* cct);
  string getAccessKeyId() const { return accessKeyId; }
  string getExpiration() const { return expiration; }
  string getSecretAccessKey() const { return secretAccessKey; }
  string getSessionToken() const { return sessionToken; }
};

using AssumeRoleResponse = std::tuple<AssumedRoleUser, Credentials, uint64_t> ;

class STSService {
  CephContext* cct;
  RGWRados *store;
public:
  STSService(CephContext* _cct, RGWRados *_store) : cct(_cct), store(_store) {}
  AssumeRoleResponse assumeRole(const AssumeRoleRequest& req);
};
}
#endif /* CEPH_STS_ASSUME_ROLE_H */

