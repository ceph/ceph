// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <ctime>
#include <regex>
#include <boost/format.hpp>
#include <boost/algorithm/string/replace.hpp>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
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
#include "rgw_sal.h"
#include "rgw_sal_rados.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace STS {

void Credentials::dump(Formatter *f) const
{
  encode_json("AccessKeyId", accessKeyId , f);
  encode_json("Expiration", expiration , f);
  encode_json("SecretAccessKey", secretAccessKey , f);
  encode_json("SessionToken", sessionToken , f);
}

int Credentials::generateCredentials(const DoutPrefixProvider *dpp,
                          CephContext* cct,
                          const uint64_t& duration,
                          const boost::optional<std::string>& policy,
                          const boost::optional<std::string>& roleId,
                          const boost::optional<std::string>& role_session,
                          const boost::optional<std::vector<std::string>>& token_claims,
                          const boost::optional<std::vector<std::pair<std::string,std::string>>>& session_princ_tags,
                          boost::optional<rgw_user> user,
                          rgw::auth::Identity* identity)
{
  uuid_d accessKey, secretKey;
  char accessKeyId_str[MAX_ACCESS_KEY_LEN + 1], secretAccessKey_str[MAX_SECRET_KEY_LEN + 1];

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
    ldpp_dout(dpp, 0) << "ERROR: No AES crypto handler found !" << dendl;
    return -EINVAL;
  }
  string secret_s = cct->_conf->rgw_sts_key;
  buffer::ptr secret(secret_s.c_str(), secret_s.length());
  int ret = 0;
  if (ret = cryptohandler->validate_secret(secret); ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid rgw sts key, please ensure its length is 16" << dendl;
    return ret;
  }
  string error;
  std::unique_ptr<CryptoKeyHandler> keyhandler(cryptohandler->get_key_handler(secret, error));
  if (! keyhandler) {
    ldpp_dout(dpp, 0) << "ERROR: No Key handler found !" << dendl;
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
  token.issued_at = ceph::to_iso_8601(t);

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
    rgw_user u({}, {}, {});
    token.user = u;
  }

  if (token_claims) {
    token.token_claims = std::move(*token_claims);
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
    token.acct_type = TYPE_ROLE;
    token.role_session = role_session.get();
  }

  if (session_princ_tags) {
    token.principal_tags = std::move(*session_princ_tags);
  }
  buffer::list input, enc_output;
  encode(token, input);

  if (ret = keyhandler->encrypt(input, enc_output, &error); ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: Encrypting session token returned an error !" << dendl;
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
                                              rgw::sal::Driver* driver,
                                              const string& roleId,
                                              const rgw::ARN& roleArn,
                                              const string& roleSessionName)
{
  string resource = std::move(roleArn.resource);
  boost::replace_first(resource, "role", "assumed-role");
  resource.append("/");
  resource.append(roleSessionName);
  
  rgw::ARN assumed_role_arn(rgw::Partition::aws,
                                  rgw::Service::sts,
                                  "", roleArn.account, resource);
  arn = assumed_role_arn.to_string();

  //Assumeroleid = roleid:rolesessionname
  assumeRoleId = roleId + ":" + roleSessionName;

  return 0;
}

AssumeRoleRequestBase::AssumeRoleRequestBase( CephContext* cct,
                                              const string& duration,
                                              const string& iamPolicy,
                                              const string& roleArn,
                                              const string& roleSessionName)
  : cct(cct), iamPolicy(iamPolicy), roleArn(roleArn), roleSessionName(roleSessionName)
{
  MIN_DURATION_IN_SECS = cct->_conf->rgw_sts_min_session_duration;
  if (duration.empty()) {
    this->duration = DEFAULT_DURATION_IN_SECS;
  } else {
    this->duration = strict_strtoll(duration.c_str(), 10, &this->err_msg);
  }
}

int AssumeRoleRequestBase::validate_input(const DoutPrefixProvider *dpp) const
{
  if (!err_msg.empty()) {
    ldpp_dout(dpp, 0) << "ERROR: error message is empty !" << dendl;
    return -EINVAL;
  }

  if (duration < MIN_DURATION_IN_SECS ||
          duration > MAX_DURATION_IN_SECS) {
    ldpp_dout(dpp, 0) << "ERROR: Incorrect value of duration: " << duration << dendl;
    return -EINVAL;
  }

  if (! iamPolicy.empty() &&
          (iamPolicy.size() < MIN_POLICY_SIZE || iamPolicy.size() > MAX_POLICY_SIZE)) {
    ldpp_dout(dpp, 0) << "ERROR: Incorrect size of iamPolicy: " << iamPolicy.size() << dendl;
    return -ERR_PACKED_POLICY_TOO_LARGE;
  }

  if (! roleArn.empty() &&
          (roleArn.size() < MIN_ROLE_ARN_SIZE || roleArn.size() > MAX_ROLE_ARN_SIZE)) {
    ldpp_dout(dpp, 0) << "ERROR: Incorrect size of roleArn: " << roleArn.size() << dendl;
    return -EINVAL;
  }

  if (! roleSessionName.empty()) {
    if (roleSessionName.size() < MIN_ROLE_SESSION_SIZE || roleSessionName.size() > MAX_ROLE_SESSION_SIZE) {
      ldpp_dout(dpp, 0) << "ERROR: Either role session name is empty or role session size is incorrect: " << roleSessionName.size() << dendl;
      return -EINVAL;
    }

    std::regex regex_roleSession("[A-Za-z0-9_=,.@-]+");
    if (! std::regex_match(roleSessionName, regex_roleSession)) {
      ldpp_dout(dpp, 0) << "ERROR: Role session name is incorrect: " << roleSessionName << dendl;
      return -EINVAL;
    }
  }

  return 0;
}

int AssumeRoleWithWebIdentityRequest::validate_input(const DoutPrefixProvider *dpp) const
{
  if (! providerId.empty()) {
    if (providerId.length() < MIN_PROVIDER_ID_LEN ||
          providerId.length() > MAX_PROVIDER_ID_LEN) {
      ldpp_dout(dpp, 0) << "ERROR: Either provider id is empty or provider id length is incorrect: " << providerId.length() << dendl;
      return -EINVAL;
    }
  }
  return AssumeRoleRequestBase::validate_input(dpp);
}

int AssumeRoleRequest::validate_input(const DoutPrefixProvider *dpp) const
{
  if (! externalId.empty()) {
    if (externalId.length() < MIN_EXTERNAL_ID_LEN ||
          externalId.length() > MAX_EXTERNAL_ID_LEN) {
      ldpp_dout(dpp, 0) << "ERROR: Either external id is empty or external id length is incorrect: " << externalId.length() << dendl;
      return -EINVAL;
    }

    std::regex regex_externalId("[A-Za-z0-9_=,.@:/-]+");
    if (! std::regex_match(externalId, regex_externalId)) {
      ldpp_dout(dpp, 0) << "ERROR: Invalid external Id: " << externalId << dendl;
      return -EINVAL;
    }
  }
  if (! serialNumber.empty()){
    if (serialNumber.size() < MIN_SERIAL_NUMBER_SIZE || serialNumber.size() > MAX_SERIAL_NUMBER_SIZE) {
      ldpp_dout(dpp, 0) << "Either serial number is empty or serial number length is incorrect: " << serialNumber.size() << dendl;
      return -EINVAL;
    }

    std::regex regex_serialNumber("[A-Za-z0-9_=/:,.@-]+");
    if (! std::regex_match(serialNumber, regex_serialNumber)) {
      ldpp_dout(dpp, 0) << "Incorrect serial number: " << serialNumber << dendl;
      return -EINVAL;
    }
  }
  if (! tokenCode.empty() && tokenCode.size() == TOKEN_CODE_SIZE) {
    ldpp_dout(dpp, 0) << "Either token code is empty or token code size is invalid: " << tokenCode.size() << dendl;
    return -EINVAL;
  }

  return AssumeRoleRequestBase::validate_input(dpp);
}

std::tuple<int, rgw::sal::RGWRole*> STSService::getRoleInfo(const DoutPrefixProvider *dpp,
                                                 const string& arn,
						 optional_yield y)
{
  if (auto r_arn = rgw::ARN::parse(arn); r_arn) {
    auto pos = r_arn->resource.find_last_of('/');
    string roleName = r_arn->resource.substr(pos + 1);
    std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(roleName, r_arn->account);
    if (int ret = role->get(dpp, y); ret < 0) {
      if (ret == -ENOENT) {
        ldpp_dout(dpp, 0) << "Role doesn't exist: " << roleName << dendl;
        ret = -ERR_NO_ROLE_FOUND;
      }
      return make_tuple(ret, nullptr);
    } else {
      auto path_pos = r_arn->resource.find('/');
      string path;
      if (path_pos == pos) {
        path = "/";
      } else {
        path = r_arn->resource.substr(path_pos, ((pos - path_pos) + 1));
      }
      string r_path = role->get_path();
      if (path != r_path) {
        ldpp_dout(dpp, 0) << "Invalid Role ARN: Path in ARN does not match with the role path: " << path << " " << r_path << dendl;
        return make_tuple(-EACCES, nullptr);
      }
      this->role = std::move(role);
      return make_tuple(0, this->role.get());
    }
  } else {
    ldpp_dout(dpp, 0) << "Invalid role arn: " << arn << dendl;
    return make_tuple(-EINVAL, nullptr);
  }
}

AssumeRoleWithWebIdentityResponse STSService::assumeRoleWithWebIdentity(const DoutPrefixProvider *dpp, AssumeRoleWithWebIdentityRequest& req)
{
  AssumeRoleWithWebIdentityResponse response;
  response.assumeRoleResp.packedPolicySize = 0;
  std::vector<string> token_claims;

  if (req.getProviderId().empty()) {
    response.providerId = req.getIss();
  }
  response.aud = req.getAud();
  response.sub = req.getSub();

  token_claims.emplace_back(string("iss") + ":" + req.getIss());
  token_claims.emplace_back(string("aud") + ":" + req.getAud());
  token_claims.emplace_back(string("sub") + ":" + req.getSub());

  //Get the role info which is being assumed
  boost::optional<rgw::ARN> r_arn = rgw::ARN::parse(req.getRoleARN());
  if (r_arn == boost::none) {
    ldpp_dout(dpp, 0) << "Error in parsing role arn: " << req.getRoleARN() << dendl;
    response.assumeRoleResp.retCode = -EINVAL;
    return response;
  }

  string roleId = role->get_id();
  uint64_t roleMaxSessionDuration = role->get_max_session_duration();
  req.setMaxDuration(roleMaxSessionDuration);

  //Validate input
  response.assumeRoleResp.retCode = req.validate_input(dpp);
  if (response.assumeRoleResp.retCode < 0) {
    return response;
  }

  //Calculate PackedPolicySize
  string policy = req.getPolicy();
  response.assumeRoleResp.packedPolicySize = (policy.size() / req.getMaxPolicySize()) * 100;

  //Generate Assumed Role User
  response.assumeRoleResp.retCode = response.assumeRoleResp.user.generateAssumedRoleUser(cct,
                                                                                          driver,
                                                                                          roleId,
                                                                                          r_arn.get(),
                                                                                          req.getRoleSessionName());
  if (response.assumeRoleResp.retCode < 0) {
    return response;
  }

  //Generate Credentials
  //Role and Policy provide the authorization info, user id and applier info are not needed
  response.assumeRoleResp.retCode = response.assumeRoleResp.creds.generateCredentials(dpp, cct, req.getDuration(),
                                                                                      req.getPolicy(), roleId,
                                                                                      req.getRoleSessionName(),
                                                                                      token_claims,
                                                                                      req.getPrincipalTags(),
                                                                                      user_id, nullptr);
  if (response.assumeRoleResp.retCode < 0) {
    return response;
  }

  response.assumeRoleResp.retCode = 0;
  return response;
}

AssumeRoleResponse STSService::assumeRole(const DoutPrefixProvider *dpp, 
                                          AssumeRoleRequest& req,
					  optional_yield y)
{
  AssumeRoleResponse response;
  response.packedPolicySize = 0;

  //Get the role info which is being assumed
  boost::optional<rgw::ARN> r_arn = rgw::ARN::parse(req.getRoleARN());
  if (r_arn == boost::none) {
    ldpp_dout(dpp, 0) << "Error in parsing role arn: " << req.getRoleARN() << dendl;
    response.retCode = -EINVAL;
    return response;
  }

  string roleId = role->get_id();
  uint64_t roleMaxSessionDuration = role->get_max_session_duration();
  req.setMaxDuration(roleMaxSessionDuration);

  //Validate input
  response.retCode = req.validate_input(dpp);
  if (response.retCode < 0) {
    return response;
  }

  //Calculate PackedPolicySize
  string policy = req.getPolicy();
  response.packedPolicySize = (policy.size() / req.getMaxPolicySize()) * 100;

  //Generate Assumed Role User
  response.retCode = response.user.generateAssumedRoleUser(cct, driver, roleId, r_arn.get(), req.getRoleSessionName());
  if (response.retCode < 0) {
    return response;
  }

  //Generate Credentials
  //Role and Policy provide the authorization info, user id and applier info are not needed
  response.retCode = response.creds.generateCredentials(dpp, cct, req.getDuration(),
                                              req.getPolicy(), roleId,
                                              req.getRoleSessionName(),
                                              boost::none,
                                              boost::none,
                                              user_id, nullptr);
  if (response.retCode < 0) {
    return response;
  }

  response.retCode = 0;
  return response;
}

GetSessionTokenRequest::GetSessionTokenRequest(const string& duration, const string& serialNumber, const string& tokenCode)
{
  if (duration.empty()) {
    this->duration = DEFAULT_DURATION_IN_SECS;
  } else {
    this->duration = stoull(duration);
  }
  this->serialNumber = serialNumber;
  this->tokenCode = tokenCode;
}

GetSessionTokenResponse STSService::getSessionToken(const DoutPrefixProvider *dpp, GetSessionTokenRequest& req)
{
  int ret;
  Credentials cred;

  //Generate Credentials
  if (ret = cred.generateCredentials(dpp, cct,
                                      req.getDuration(),
                                      boost::none,
                                      boost::none,
                                      boost::none,
                                      boost::none,
                                      boost::none,
                                      user_id,
                                      identity); ret < 0) {
    return make_tuple(ret, cred);
  }

  return make_tuple(0, cred);
}

}
