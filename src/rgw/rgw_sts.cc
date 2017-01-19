#include <iostream>
#include <map>
#include <sstream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/ceph_time.h"
#include "global/global_init.h"
#include "include/assert.h"
#include "include/str_list.h"
#include "rgw_common.h"
#include "rgw_b64.h"
#include "rgw_sts.h"

using namespace std;

const string RGWSTS::key = "LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE";

std::string timeval_to_string(struct timeval& tv)
{
  char buf[30];
  struct tm result;
  gmtime_r(&tv.tv_sec, &result);
  strftime(buf,30,"%Y-%m-%dT%H:%M:%S", &result);
  sprintf(buf + strlen(buf),".%dZ",(int)tv.tv_usec/1000);

  return std::string(buf);
}

void RGWSTS::build_output() {
  boost::uuids::random_generator generator;

  boost::uuids::uuid uuid1 = generator();
  boost::uuids::uuid uuid2 = generator();

  access_key_id = to_string(uuid1);
  secret_access_key = to_string(uuid2);

  real_time now = real_clock::now();
  real_time expiration = now + ceph::make_timespan(duration);
  real_clock::to_timeval(expiration, this->expiration);
  std::string time_str = timeval_to_string(this->expiration);

  policy_size = (policy.size() / policy_plain_text_size);

  std::string string_to_sign = "access_key_id=" + access_key_id + "&" + "secret_access_key=" + secret_access_key + "&" + "role_id=" + role_id + "&" + "policy=" + policy + "&" + "expiration=" + time_str;
  for (auto it : saml_keys) {
    string_to_sign = string_to_sign + "&" + it.first + "=" + it.second;
  }
  cout << "string to sign: " << string_to_sign << std::endl;

  char signature[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];
  calc_hmac_sha256(key.c_str(), key.size(), string_to_sign.c_str(), string_to_sign.size(), signature);

  char aux[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE * 2 + 1];
  buf_to_hex((unsigned char *) signature, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, aux);
  string signature_str = string(aux);
  session_token = rgw::to_base64(signature_str);
}

void help()
{
  cout<<"option:  "<<"--help  help"<< std::endl;
  cout<<"         "<<"--role-id  role id"<< std::endl;
  cout<<"         "<<"--policy  policy"<< std::endl;
  cout<<"         "<<"--saml-keys  saml keys"<< std::endl;
  cout<<"         "<<"--duration  duration in seconds"<< std::endl;
}

void string_to_map(const std::string& input, std::map<std::string, std::string>& saml_map)
{
  std::string token, key, value;
  std::istringstream iss(input);

  while (std::getline(iss, token, ',')) {
  token.erase(std::remove_if( token.begin(), token.end(),
    [](char c){ return (c =='\r' || c =='\t' || c == ' ' || c == '\n' || c == '{' || c == '}' || c == '"');}), token.end() );
    auto pos = token.find(':');
    key = token.substr(0, pos);
    value = token.substr(pos + 1);
    saml_map[key] = value;
  }
}

int main(int argc,char *argv[]) {

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);

  common_init_finish(g_ceph_context);

  std::string role_id, policy, val, err;
  int duration = 0;
  std::map<std::string, std::string> saml_map;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, &val, "--help", (char*)NULL)) {
      help();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &val, "--role-id", (char*)NULL)) {
      role_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--policy", (char*)NULL)) {
      policy = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--duration", (char*)NULL)) {
      duration = (int)strict_strtol(val.c_str(), 10, &err);
    } else if (ceph_argparse_witharg(args, i, &val, "--saml-keys", (char*)NULL)) {
      string_to_map(val, saml_map);
    }
  }
  RGWSTS sts(duration, role_id, policy, saml_map);
  sts.build_output();
  struct timeval tv = sts.get_expiration_time();
  std::string time_str = timeval_to_string(tv);
  cout << "Credentials: Access Key Id: " << sts.get_access_key_id() << std::endl;
  cout << "Credentials: Secret Access Key: " << sts.get_secret_access_key() << std::endl;
  cout << "Credentials: Expiration: " << sts.get_expiration_time() << std::endl;
  cout << "Credentials: Session Token: " << sts.get_session_token() << std::endl;
  cout << "AssumedRoleUser: Assumed Role Id: " << sts.get_role_id() << std::endl;
  cout << "PackedPolicySize: "  <<  sts.get_packed_policy_size() << std::endl;

  return 0;
}

