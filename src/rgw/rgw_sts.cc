#include <iostream>
#include <map>
#include <string>

#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"

#include "rgw_common.h"
#include "rgw_b64.h"
#include "rgw_sts.h"

using namespace std;

const string RGWSts::key = "LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE";

int RGWSts::generate_key(char* buf, int size)
{
  std::string id;
  do {
    int ret = gen_rand_alphanumeric_upper(g_ceph_context, buf, size);

    if (ret < 0) {
      return ret;
    }
    id = buf;
  } while (! validate_access_key(id));

  return 0;
}

void RGWSts::credentials::dump(Formatter *f) const
{
  encode_json("AccessKeyId", access_key_id , f);
  encode_json("SecretAccessKey", secret_access_key , f);
  encode_json("SessionToken", session_token, f);
  encode_json("Expiration", std::to_string(expiration), f);
}

void RGWSts::assumedroleuser::dump(Formatter *f) const
{
  encode_json("AssumedRoleId", assumedroleid , f);
}

void RGWSts::sts::dump(Formatter *f) const
{
  encode_json("Credentials", cred , f);
  encode_json("AssumedRoleUser", role , f);
  encode_json("PackedPolicySize", packedpolicysize, f);
}

int RGWSts::validate_input()
{
  JSONParser p;
  if (!p.parse(policy.c_str(), policy.length())) {
    return -EINVAL;
  }

  if (policy.size() > RGWSts::POLICY_PLAIN_TEXT_SIZE) {
    return -EINVAL;
  }

  return 0;
}

int RGWSts::build_output(struct sts& sts_output) {
  int ret;
  sts_output.role.assumedroleid = std::move(role_id);

  char access_key[ACCESS_KEY_LEN];
  char secret_key[SECRET_KEY_LEN];

  ret = generate_key(access_key, ACCESS_KEY_LEN);
  if (ret < 0) {
    return ret;
  }
  ret = generate_key(secret_key, SECRET_KEY_LEN);
  if (ret < 0) {
    return ret;
  }

  sts_output.cred.access_key_id = access_key;
  sts_output.cred.secret_access_key = secret_key;

  real_time now = real_clock::now();
  real_time expiration = now + ceph::make_timespan(duration);
  sts_output.cred.expiration = real_clock::to_time_t(expiration);
  std::string time_str = std::to_string(sts_output.cred.expiration);

  sts_output.packedpolicysize = (policy.size() * 100 / POLICY_PLAIN_TEXT_SIZE);

  std::string string_to_sign = "access_key_id=" + sts_output.cred.access_key_id +
                                "&" + "secret_access_key=" + sts_output.cred.secret_access_key +
                                "&" + "role_id=" + sts_output.role.assumedroleid +
                                "&" + "policy=" + policy + "&" + "expiration=" + time_str;
  for (auto it : saml_keys) {
    string_to_sign = string_to_sign + "&" + it.first + "=" + it.second;
  }
  //cout << "string to sign: " << string_to_sign << std::endl;
  sts_output.cred.session_token = rgw::to_base64(string_to_sign);

  return 0;
}

