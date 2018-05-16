#include <errno.h>
#include <ctime>
#include <regex>
#include <boost/format.hpp>
#include <boost/algorithm/string/replace.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_rados.h"
#include "auth/Crypto.h"
#include "include/ceph_fs.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_role.h"
#include "rgw_user.h"
#include "rgw_iam_policy.h"
#include "sts-assume-role.h"

#define dout_subsys ceph_subsys_rgw

namespace STS {

void Credentials::dump(Formatter *f) const
{
  encode_json("AccessKeyId", accessKeyId , f);
  encode_json("Expiration", expiration , f);
  encode_json("SecretAccessKey", secretAccessKey , f);
  encode_json("SessionToken", sessionToken , f);
}

int Credentials::generateCredentials(CephContext* cct, const uint64_t& duration)
{
  uuid_d accessKey, secretKey;
  char accessKeyId_str[MAX_ACCESS_KEY_LEN], secretAccessKey_str[MAX_ACCESS_KEY_LEN];

  //AccessKeyId
  accessKey.generate_random();
  accessKey.print(accessKeyId_str);
  accessKeyId = accessKeyId_str;

  //SecretAccessKey
  secretKey.generate_random();
  secretKey.print(secretAccessKey_str);
  secretAccessKey = secretAccessKey_str;

  //Expiration
  real_clock::time_point t = real_clock::now();

  struct timeval tv;
  real_clock::to_timeval(t, tv);
  tv.tv_sec += duration;

  struct tm result;
  gmtime_r(&tv.tv_sec, &result);
  int usec = (int)tv.tv_usec/1000;
  expiration = boost::str(boost::format("%s-%s-%sT%s:%s:%s.%sZ")
                                         % (result.tm_year + 1900)
                                         % (result.tm_mon + 1)
                                         % result.tm_mday
                                         % result.tm_hour
                                         % result.tm_min
                                         % result.tm_sec
                                         % usec);

  //Session Token - Encrypt using AES & base64 encode the result
  auto* cryptohandler = cct->get_crypto_handler(CEPH_CRYPTO_AES);
  if (! cryptohandler) {
    return -EINVAL;
  }
  char secret_s[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  };
  bufferptr secret(secret_s, sizeof(secret_s));
  int ret = 0;
  if (ret = cryptohandler->validate_secret(secret); ret < 0) {
    ldout(cct, 0) << "ERROR: Invalid secret key" << dendl;
    return ret;
  }
  string error;
  auto* keyhandler = cryptohandler->get_key_handler(secret, error);
  if (! keyhandler) {
    return -EINVAL;
  }
  error.clear();
  string encrypted_str, input_str = "acess_key_id=" + accessKeyId + "&" +
                     "secret_access_key=" + secretAccessKey + "&" +
                     "expiration=" + expiration;
  bufferlist input, enc_output;
  input.append(input_str);
  if (ret = keyhandler->encrypt(input, enc_output, &error); ret < 0) {
    return ret;
  }

  enc_output.append('\0');
  encrypted_str = enc_output.c_str();

  bufferlist enc_bp, encoded_op;
  enc_bp.append(encrypted_str);
  enc_bp.encode_base64(encoded_op);
  encoded_op.append('\0');
  sessionToken = encoded_op.c_str();

  return ret;
}

void AssumedRoleUser::dump(Formatter *f) const
{
  encode_json("Arn", arn , f);
  encode_json("AssumeRoleId", assumeRoleId , f);
}

int AssumedRoleUser::generateAssumedRoleUser(CephContext* cct,
                                              RGWRados *store,
                                              const string& roleId,
                                              const rgw::IAM::ARN& roleArn,
                                              const string& roleSessionName)
{
  string resource = std::move(roleArn.resource);
  boost::replace_first(resource, "role", "assumed-role");
  resource.append("/");
  resource.append(roleSessionName);
  
  rgw::IAM::ARN assumed_role_arn(rgw::IAM::Partition::aws,
                                  rgw::IAM::Service::sts,
                                  "", roleArn.account, resource);
  arn = assumed_role_arn.to_string();

  //Assumeroleid = roleid:rolesessionname
  assumeRoleId = roleId + ":" + roleSessionName;

  return 0;
}

AssumeRoleRequest::AssumeRoleRequest(string& _duration, string& _externalId, string& _iamPolicy,
                    string& _roleArn, string& _roleSessionName, string& _serialNumber,
                    string& _tokenCode)
    : externalId(_externalId), iamPolicy(_iamPolicy),
      roleArn(_roleArn), roleSessionName(_roleSessionName),
      serialNumber(_serialNumber), tokenCode(_tokenCode)
{
  if (_duration.empty()) {
    duration = DEFAULT_DURATION_IN_SECS;
  } else {
    duration = std::stoull(_duration);
  }
}

int AssumeRoleRequest::validate_input() const
{
  if (duration < MIN_DURATION_IN_SECS ||
          duration > MAX_DURATION_IN_SECS) {
    return -EINVAL;
  }

  if (! externalId.empty()) {
    if (externalId.length() < MIN_EXTERNAL_ID_LEN ||
          externalId.length() > MAX_EXTERNAL_ID_LEN) {
      return -EINVAL;
    }

    std::regex regex_externalId("[A-Za-z0-9_=,.@:/-]+");
    if (! std::regex_match(externalId, regex_externalId)) {
      return -EINVAL;
    }
  }
  if (! iamPolicy.empty() &&
          (iamPolicy.size() < MIN_POLICY_SIZE || iamPolicy.size() > MAX_POLICY_SIZE)) {
    return -ERR_PACKED_POLICY_TOO_LARGE;
  }

  if (! roleArn.empty() &&
          (roleArn.size() < MIN_ROLE_ARN_SIZE || roleArn.size() > MAX_ROLE_ARN_SIZE)) {
    return -EINVAL;
  }

  if (! roleSessionName.empty()) {
    if (roleSessionName.size() < MIN_ROLE_SESSION_SIZE || roleSessionName.size() > MAX_ROLE_SESSION_SIZE) {
      return -EINVAL;
    }

    std::regex regex_roleSession("[A-Za-z0-9_=,.@-]+");
    if (! std::regex_match(roleSessionName, regex_roleSession)) {
      return -EINVAL;
    }
  }
  if (! serialNumber.empty()){
    if (serialNumber.size() < MIN_SERIAL_NUMBER_SIZE || serialNumber.size() > MAX_SERIAL_NUMBER_SIZE) {
      return -EINVAL;
    }

    std::regex regex_serialNumber("[A-Za-z0-9_=/:,.@-]+");
    if (! std::regex_match(serialNumber, regex_serialNumber)) {
      return -EINVAL;
    }
  }
  if (! tokenCode.empty() && tokenCode.size() == TOKEN_CODE_SIZE) {
    return -EINVAL;
  }

  return 0;
}

std::tuple<int, RGWRole> STSService::getRoleInfo(const string& arn)
{
  if (auto r_arn = rgw::IAM::ARN::parse(arn); r_arn) {
    auto pos = r_arn->resource.find_last_of('/');
    string roleName = r_arn->resource.substr(pos + 1);
    RGWRole role(cct, store, roleName, r_arn->account);
    if (int ret = role.get(); ret < 0) {
      if (ret == -ENOENT) {
        ret = -ERR_NO_ROLE_FOUND;
      }
      return make_tuple(ret, this->role);
    } else {
      this->role = std::move(role);
      return make_tuple(0, this->role);
    }
  } else {
    return make_tuple(-EINVAL, this->role);
  }
}

int STSService::_storeARNandPolicy(string& policy, string& arn)
{
  int ret = 0;
  RGWUserInfo info;
  if (ret = rgw_get_user_info_by_uid(store, user_id, info); ret < 0) {
    return -ERR_NO_SUCH_ENTITY;
  }

  info.assumed_role_arn = arn;

  map<string, bufferlist> uattrs;
  if (ret = rgw_get_user_attrs_by_uid(store, user_id, uattrs); ret == -ENOENT) {
    return -ERR_NO_SUCH_ENTITY;
  }
  if (! policy.empty()) {
    bufferlist bl = bufferlist::static_from_string(policy);
    ldout(cct, 20) << "bufferlist policy: " << bl.c_str() << dendl;
    try {
      const rgw::IAM::Policy p(cct, user_id.tenant, bl);
      map<string, string> policies;
      if (auto it = uattrs.find(RGW_ATTR_USER_POLICY); it != uattrs.end()) {
        bufferlist out_bl = uattrs[RGW_ATTR_USER_POLICY];
        decode(policies, out_bl);
      }
      bufferlist in_bl;
      policies["assumerolepolicy"] = policy;
      encode(policies, in_bl);
      uattrs[RGW_ATTR_USER_POLICY] = in_bl;
    } catch (rgw::IAM::PolicyParseException& e) {
      ldout(cct, 20) << "failed to parse policy: " << e.what() << dendl;
      return -ERR_MALFORMED_DOC;
    }
  }
  RGWObjVersionTracker objv_tracker;
  if (rgw_store_user_info(store, info, &info, &objv_tracker, real_time(),
          false, &uattrs); ret < 0) {
    return -ERR_INTERNAL_ERROR;
  }
  return ret;
}

AssumeRoleResponse STSService::assumeRole(AssumeRoleRequest& req)
{
  uint64_t packedPolicySize = 0, roleMaxSessionDuration = 0;
  AssumedRoleUser user;
  Credentials cred;
  string roleId;

  //Get the role info which is being assumed
  boost::optional<rgw::IAM::ARN> r_arn;
  if (r_arn = rgw::IAM::ARN::parse(req.getRoleARN()); r_arn == boost::none) {
    return make_tuple(-EINVAL, user, cred, packedPolicySize);
  }

  roleId = role.get_id();
  roleMaxSessionDuration = role.get_max_session_duration();
  req.setMaxDuration(roleMaxSessionDuration);

  //Validate input
  int ret = 0;
  if (ret = req.validate_input(); ret < 0) {
    return make_tuple(ret, user, cred, packedPolicySize);
  }

  //Calculate PackedPolicySize
  string policy = req.getPolicy();
  packedPolicySize = (policy.size() / req.getMaxPolicySize()) * 100;

  //Generate Assumed Role User
  if (ret = user.generateAssumedRoleUser(cct, store, roleId, r_arn.get(), req.getRoleSessionName()); ret < 0) {
    return make_tuple(ret, user, cred, packedPolicySize);
  }

  //Generate Credentials
  if (ret = cred.generateCredentials(cct, req.getDuration()); ret < 0) {
    return make_tuple(ret, user, cred, packedPolicySize);
  }

  //Save ARN and Policy with the user
  string arn = user.getARN();
  if (ret = _storeARNandPolicy(policy, arn); ret < 0) {
    return make_tuple(ret, user, cred, packedPolicySize);
  }

  return make_tuple(0, user, cred, packedPolicySize);
}

}