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
#include "common/iso_8601.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_b64.h"
#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_role.h"
#include "rgw_user.h"
#include "rgw_iam_policy.h"
#include "rgw_sts.h"

#define dout_subsys ceph_subsys_rgw

namespace STS {

void Credentials::dump(Formatter *f) const
{
  encode_json("AccessKeyId", accessKeyId , f);
  encode_json("Expiration", expiration , f);
  encode_json("SecretAccessKey", secretAccessKey , f);
  encode_json("SessionToken", sessionToken , f);
}

int Credentials::generateCredentials(CephContext* cct,
                          const uint64_t& duration,
                          const boost::optional<string>& policy,
                          const boost::optional<string>& roleId,
                          boost::optional<rgw_user> user,
                          rgw::auth::Identity* identity)
{
  uuid_d accessKey, secretKey;
  char accessKeyId_str[MAX_ACCESS_KEY_LEN], secretAccessKey_str[MAX_SECRET_KEY_LEN];

  //AccessKeyId
  gen_rand_alphanumeric_plain(cct, accessKeyId_str, sizeof(accessKeyId_str));
  accessKeyId = accessKeyId_str;

  //SecretAccessKey
  gen_rand_alphanumeric_upper(cct, secretAccessKey_str, sizeof(secretAccessKey_str));
  secretAccessKey = secretAccessKey_str;

  //Expiration
  real_clock::time_point t = real_clock::now();
  real_clock::time_point exp = t + std::chrono::seconds(duration);
  expiration = ceph::to_iso_8601(exp);

  //Session Token - Encrypt using AES
  auto* cryptohandler = cct->get_crypto_handler(CEPH_CRYPTO_AES);
  if (! cryptohandler) {
    return -EINVAL;
  }
  string secret_s = cct->_conf->rgw_sts_key;
  buffer::ptr secret(secret_s.c_str(), secret_s.length());
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
  //Storing policy and roleId as part of token, so that they can be extracted
  // from the token itself for policy evaluation.
  SessionToken token;
  //authentication info
  token.access_key_id = accessKeyId;
  token.secret_access_key = secretAccessKey;
  token.expiration = expiration;

  //Authorization info
  if (policy)
    token.policy = *policy;
  else
    token.policy = {};

  if (roleId)
    token.roleId = *roleId;
  else
    token.roleId = {};

  if (user)
    token.user = *user;
  else {
    rgw_user u({}, {});
    token.user = u;
  }

  if (identity) {
    token.acct_name = identity->get_acct_name();
    token.perm_mask = identity->get_perm_mask();
    token.is_admin = identity->is_admin_of(token.user);
    token.acct_type = identity->get_identity_type();
  } else {
    token.acct_name = {};
    token.perm_mask = 0;
    token.is_admin = 0;
    token.acct_type = TYPE_NONE;
  }

  buffer::list input, enc_output;
  encode(token, input);

  if (ret = keyhandler->encrypt(input, enc_output, &error); ret < 0) {
    return ret;
  }

  bufferlist encoded_op;
  enc_output.encode_base64(encoded_op);
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

int STSService::storeARN(string& arn)
{
  int ret = 0;
  RGWUserInfo info;
  if (ret = rgw_get_user_info_by_uid(store, user_id, info); ret < 0) {
    return -ERR_NO_SUCH_ENTITY;
  }

  info.assumed_role_arn = arn;

  RGWObjVersionTracker objv_tracker;
  if (rgw_store_user_info(store, info, &info, &objv_tracker, real_time(),
          false); ret < 0) {
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
  //Role and Policy provide the authorization info, user id and applier info are not needed
  if (ret = cred.generateCredentials(cct, req.getDuration(),
                                      req.getPolicy(), roleId,
                                      boost::none, nullptr); ret < 0) {
    return make_tuple(ret, user, cred, packedPolicySize);
  }

  //Save ARN with the user
  string arn = user.getARN();
  if (ret = storeARN(arn); ret < 0) {
    return make_tuple(ret, user, cred, packedPolicySize);
  }

  return make_tuple(0, user, cred, packedPolicySize);
}

GetSessionTokenRequest::GetSessionTokenRequest(string& duration, string& serialNumber, string& tokenCode)
{
  if (duration.empty()) {
    this->duration = DEFAULT_DURATION_IN_SECS;
  } else {
    this->duration = stoull(duration);
  }
  this->serialNumber = serialNumber;
  this->tokenCode = tokenCode;
}

GetSessionTokenResponse STSService::getSessionToken(GetSessionTokenRequest& req)
{
  int ret;
  Credentials cred;

  //Generate Credentials
  if (ret = cred.generateCredentials(cct,
                                      req.getDuration(),
                                      boost::none,
                                      boost::none); ret < 0) {
    return make_tuple(ret, cred);
  }

  return make_tuple(0, cred);
}

}