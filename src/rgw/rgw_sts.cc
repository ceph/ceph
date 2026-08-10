// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <errno.h>
#include <ctime>
#include <algorithm>
#include <memory>
#include <regex>
#include <string_view>
#include <boost/format.hpp>
#include <boost/algorithm/string/replace.hpp>

#include <openssl/evp.h>

#include "common/errno.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "common/ceph_crypto.h"
#include "auth/Crypto.h"
#include "include/ceph_fs.h"
#include "common/iso_8601.h"

#include "include/types.h"
#include "rgw_string.h"

#include "rgw_account.h"
#include "rgw_b64.h"
#include "rgw_common.h"
#include "rgw_role.h"
#include "driver/rados/rgw_user.h"
#include "rgw_iam_policy.h"
#include "rgw_sts.h"
#include "rgw_sts_keyring.h"
#include "rgw_sts_keyring_cache.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {

// AEAD-sealed tokens carry this prefix; legacy CBC tokens have none.
constexpr std::string_view STS_AEAD_PREFIX = "v2.";
constexpr size_t STS_AEAD_IV_SIZE = 12;
constexpr size_t STS_AEAD_TAG_SIZE = 16;
constexpr size_t STS_AEAD_SALT_SIZE = 32; // per-token key-derivation salt

constexpr unsigned char STS_AEAD_ZERO_IV[STS_AEAD_IV_SIZE] = {0};

using rgw::sts::STS_AEAD_KEY_ID_SIZE;
using rgw::sts::sts_aead_key;

// the sealed envelope prepends the key id and salt to the ciphertext
constexpr size_t STS_AEAD_HEADER_SIZE = STS_AEAD_KEY_ID_SIZE + STS_AEAD_SALT_SIZE;

bool sts_gcm_seal(const unsigned char* key,
                  const unsigned char* in, int in_len,
                  unsigned char* out, unsigned char* tag)
{
  std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)>
    ctx{EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free};
  int len = 0;
  if (! ctx ||
      1 != EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr) ||
      1 != EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN, STS_AEAD_IV_SIZE, nullptr) ||
      1 != EVP_EncryptInit_ex(ctx.get(), nullptr, nullptr, key, STS_AEAD_ZERO_IV) ||
      1 != EVP_EncryptUpdate(ctx.get(), out, &len, in, in_len)) {
    return false;
  }
  int final_len = 0;
  if (1 != EVP_EncryptFinal_ex(ctx.get(), out + len, &final_len) ||
      1 != EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG, STS_AEAD_TAG_SIZE, tag)) {
    return false;
  }
  return true;
}

bool sts_gcm_open(const unsigned char* key,
                  const unsigned char* in, int in_len,
                  const unsigned char* tag, unsigned char* out)
{
  std::unique_ptr<EVP_CIPHER_CTX, decltype(&EVP_CIPHER_CTX_free)>
    ctx{EVP_CIPHER_CTX_new(), EVP_CIPHER_CTX_free};
  int len = 0;
  if (! ctx ||
      1 != EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr) ||
      1 != EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_IVLEN, STS_AEAD_IV_SIZE, nullptr) ||
      1 != EVP_DecryptInit_ex(ctx.get(), nullptr, nullptr, key, STS_AEAD_ZERO_IV) ||
      1 != EVP_DecryptUpdate(ctx.get(), out, &len, in, in_len) ||
      1 != EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG, STS_AEAD_TAG_SIZE,
                               const_cast<unsigned char*>(tag))) {
    return false;
  }
  int final_len = 0;
  return 1 == EVP_DecryptFinal_ex(ctx.get(), out + len, &final_len);
}

/*
 * Derive a per-token key from the salt so the GCM nonce can stay zero and
 * still never repeat a (key, nonce) pair. Returns false, and wipes out, if the
 * HMAC fails.
 */
bool sts_derive_token_key(const std::string& master,
                          const unsigned char* salt,
                          unsigned char (&out)[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE])
{
  try {
    ceph::crypto::HMACSHA256 hmac(
      reinterpret_cast<const unsigned char*>(master.data()), master.size());
    hmac.Update(salt, STS_AEAD_SALT_SIZE);
    hmac.Final(out);
  } catch (const ceph::crypto::DigestException&) {
    ::ceph::crypto::zeroize_for_security(out, sizeof(out));
    return false;
  }
  return true;
}

int get_sts_cbc_key_handler(const DoutPrefixProvider* dpp, CephContext* cct,
                            STS::KeyringCache* keyring_cache,
                            std::unique_ptr<CryptoKeyHandler>& out)
{
  auto* cryptohandler = cct->get_crypto_handler(CEPH_CRYPTO_AES);
  if (! cryptohandler) {
    ldpp_dout(dpp, 0) << "ERROR: No AES crypto handler found !" << dendl;
    return -EINVAL;
  }
  // rgw_sts_key always takes precedence over the stored legacy key
  std::string secret_s = cct->_conf.get_val<std::string>("rgw_sts_key");
  if (secret_s.empty() && keyring_cache) {
    if (const auto legacy = keyring_cache->get_legacy(); legacy) {
      secret_s = *legacy;
    }
  }
  if (secret_s.empty()) {
    ldpp_dout(dpp, 1) << "ERROR: rgw sts key not set" << dendl;
    return -EINVAL;
  }
  bufferptr secret(secret_s.c_str(), secret_s.length());
  if (int ret = cryptohandler->validate_secret(secret); ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: Invalid rgw sts key, please ensure it is an alphanumeric key of length 16" << dendl;
    return ret;
  }
  std::string error;
  out.reset(cryptohandler->get_key_handler(secret, error));
  if (! out) {
    ldpp_dout(dpp, 0) << "ERROR: No Key handler found !" << dendl;
    return -EINVAL;
  }
  return 0;
}

int get_sts_keyring(const DoutPrefixProvider* dpp,
                    STS::KeyringCache* keyring_cache,
                    STS::KeyringSnapshot& out)
{
  out = keyring_cache ? keyring_cache->get() : nullptr;
  if (! out) {
    ldpp_dout(dpp, 0) << "ERROR: the STS keyring is not available" << dendl;
    return -EINVAL;
  }
  return 0;
}

bool sts_aead_token_format(CephContext* cct)
{
  return cct->_conf.get_val<std::string>("rgw_sts_token_format") == "aead";
}

} // anonymous namespace

namespace STS {

int seal_session_token(const DoutPrefixProvider* dpp, CephContext* cct,
                       const sts_aead_key& active,
                       bufferlist plaintext, std::string& out)
{
  unsigned char salt[STS_AEAD_SALT_SIZE];
  cct->random()->get_bytes(reinterpret_cast<char*>(salt), sizeof(salt));
  unsigned char key[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];
  if (! sts_derive_token_key(active.key, salt, key)) {
    ldpp_dout(dpp, 0) << "ERROR: session token key derivation failed" << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  const int in_len = static_cast<int>(plaintext.length());
  std::string envelope;
  envelope.reserve(STS_AEAD_HEADER_SIZE + in_len + STS_AEAD_TAG_SIZE);
  envelope.append(active.id);
  envelope.append(reinterpret_cast<const char*>(salt), STS_AEAD_SALT_SIZE);
  const size_t cipher_off = envelope.size();
  envelope.resize(cipher_off + in_len + STS_AEAD_TAG_SIZE);
  auto* cipher = reinterpret_cast<unsigned char*>(envelope.data()) + cipher_off;

  const bool ok = sts_gcm_seal(key,
                               reinterpret_cast<const unsigned char*>(plaintext.c_str()),
                               in_len, cipher, cipher + in_len);
  ::ceph::crypto::zeroize_for_security(key, sizeof(key));
  if (! ok) {
    ldpp_dout(dpp, 0) << "ERROR: AES-256-GCM sealing of session token failed" << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  out.assign(STS_AEAD_PREFIX);
  out += rgw::to_base64(envelope);
  return 0;
}

int unseal_session_token(const DoutPrefixProvider* dpp,
                         const rgw::sts::StsKeyring& keyring,
                         std::string_view token, bufferlist& plaintext)
{
  std::string envelope;
  try {
    envelope = rgw::from_base64(token.substr(STS_AEAD_PREFIX.size()));
  } catch (...) {
    ldpp_dout(dpp, 0) << "ERROR: session token is not valid base64" << dendl;
    return -EINVAL;
  }
  if (envelope.size() <= STS_AEAD_HEADER_SIZE + STS_AEAD_TAG_SIZE) {
    ldpp_dout(dpp, 0) << "ERROR: session token is truncated" << dendl;
    return -EINVAL;
  }

  const std::string_view key_id{envelope.data(), STS_AEAD_KEY_ID_SIZE};
  const auto* buf = reinterpret_cast<const unsigned char*>(envelope.data());
  const unsigned char* salt = buf + STS_AEAD_KEY_ID_SIZE;
  const unsigned char* cipher = buf + STS_AEAD_HEADER_SIZE;
  const int cipher_len = static_cast<int>(
    envelope.size() - STS_AEAD_HEADER_SIZE - STS_AEAD_TAG_SIZE);
  const unsigned char* tag = cipher + cipher_len;

  const sts_aead_key* active = keyring.find(key_id);
  if (! active) {
    ldpp_dout(dpp, 0) << "ERROR: session token sealed with an unknown key id" << dendl;
    return -EPERM;
  }

  unsigned char key[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];
  if (! sts_derive_token_key(active->key, salt, key)) {
    ldpp_dout(dpp, 0) << "ERROR: session token key derivation failed" << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  bufferptr opened(cipher_len);
  const bool ok = sts_gcm_open(key, cipher, cipher_len, tag,
                               reinterpret_cast<unsigned char*>(opened.c_str()));
  ::ceph::crypto::zeroize_for_security(key, sizeof(key));
  if (! ok) {
    // this plaintext isn't authenticated, so wipe it
    ::ceph::crypto::zeroize_for_security(opened.c_str(), opened.length());
    ldpp_dout(dpp, 0) << "ERROR: session token failed authentication" << dendl;
    return -EPERM;
  }

  plaintext.clear();
  plaintext.push_back(std::move(opened));
  return 0;
}

int decode_session_token(const DoutPrefixProvider* dpp, CephContext* cct,
                         STS::KeyringCache* keyring_cache,
                         std::string_view session_token, SessionToken& token)
{
  bufferlist plaintext;
  if (session_token.starts_with(STS_AEAD_PREFIX)) {
    STS::KeyringSnapshot keyring;
    if (int ret = get_sts_keyring(dpp, keyring_cache, keyring); ret < 0) {
      return ret;
    }
    if (int ret = unseal_session_token(dpp, *keyring, session_token, plaintext); ret < 0) {
      return ret;
    }
  } else {
    if (sts_aead_token_format(cct)) {
      ldpp_dout(dpp, 0) << "ERROR: legacy session token rejected because rgw_sts_token_format is aead" << dendl;
      return -EPERM;
    }
    std::string decoded;
    try {
      decoded = rgw::from_base64(session_token);
    } catch (...) {
      ldpp_dout(dpp, 0) << "ERROR: Invalid session token, not base64 encoded." << dendl;
      return -EINVAL;
    }
    std::unique_ptr<CryptoKeyHandler> keyhandler;
    if (int ret = get_sts_cbc_key_handler(dpp, cct, keyring_cache, keyhandler); ret < 0) {
      return ret;
    }
    std::string error;
    bufferlist encrypted = bufferlist::static_from_string(decoded);
    if (int ret = keyhandler->decrypt(encrypted, plaintext, &error); ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: Decryption failed: " << error << dendl;
      return -EPERM;
    }
    plaintext.append('\0');
  }
  try {
    auto iter = plaintext.cbegin();
    decode(token, iter);
  } catch (const buffer::error& e) {
    ldpp_dout(dpp, 0) << "ERROR: decode SessionToken failed" << dendl;
    return -EINVAL;
  }
  return 0;
}

void Credentials::dump(Formatter *f) const
{
  encode_json("AccessKeyId", accessKeyId , f);
  encode_json("Expiration", expiration , f);
  encode_json("SecretAccessKey", secretAccessKey , f);
  encode_json("SessionToken", sessionToken , f);
}

int Credentials::generateCredentials(const DoutPrefixProvider *dpp,
                          CephContext* cct,
                          STS::KeyringCache* keyring_cache,
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

  const bool aead = sts_aead_token_format(cct);
  STS::KeyringSnapshot keyring;
  std::unique_ptr<CryptoKeyHandler> keyhandler;
  if (aead) {
    if (int ret = get_sts_keyring(dpp, keyring_cache, keyring); ret < 0) {
      return ret;
    }
  } else if (int ret = get_sts_cbc_key_handler(dpp, cct, keyring_cache,
                                               keyhandler); ret < 0) {
    return ret;
  }

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
    token.is_admin = identity->is_admin();
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

  buffer::list input;
  encode(token, input);

  if (aead) {
    return seal_session_token(dpp, cct, keyring->sealing_key(), std::move(input), sessionToken);
  }

  string error;
  buffer::list enc_output;
  if (int ret = keyhandler->encrypt(input, enc_output, &error); ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: Encrypting session token returned an error !" << dendl;
    return ret;
  }

  bufferlist encoded_op;
  enc_output.encode_base64(encoded_op);
  encoded_op.append('\0');
  sessionToken = encoded_op.c_str();

  return 0;
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
  if (! tokenCode.empty() && tokenCode.size() != TOKEN_CODE_SIZE) {
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
    string tenant = r_arn->account;

    rgw_account_id account;
    if (rgw::account::validate_id(tenant)) {
      account = std::move(tenant);
      tenant.clear();
    }

    std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(roleName, tenant, account);
    if (int ret = role->load_by_name(dpp, y); ret < 0) {
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
  response.assumeRoleResp.retCode = response.assumeRoleResp.creds.generateCredentials(dpp, cct, keyring_cache, req.getDuration(),
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

  auto [ret, r] = getRoleInfo(dpp, req.getRoleARN(), y);
  if (ret < 0) {
    response.retCode = ret;
    return response;
  }

  boost::optional<rgw::ARN> r_arn = rgw::ARN::parse(req.getRoleARN());

  string roleId = r->get_id();
  uint64_t roleMaxSessionDuration = r->get_max_session_duration();
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
  response.retCode = response.creds.generateCredentials(dpp, cct, keyring_cache, req.getDuration(),
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
  if (ret = cred.generateCredentials(dpp, cct, keyring_cache,
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
