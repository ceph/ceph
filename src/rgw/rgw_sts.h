#ifndef CEPH_RGW_STS_H
#define CEPH_RGW_STS_H

class RGWSTS
{
  static const string key;
  static constexpr int policy_plain_text_size = 2048;

  int duration;
  int policy_size;
  std::string role_id;
  std::string policy;
  std::string access_key_id;
  std::string secret_access_key;
  std::string session_token;
  std::map<std::string, std::string> saml_keys;
  struct timeval expiration;

public:
  RGWSTS (int duration, std::string role_id, std::string policy, std::map<std::string, std::string> saml_keys) :
    duration(std::move(duration)),
    role_id(std::move(role_id)),
    policy(std::move(policy)),
    saml_keys(std::move(saml_keys)) {}

    void build_output();

    std::string get_access_key_id() {
      return access_key_id;
    }
    std::string get_secret_access_key() {
      return secret_access_key;
    }
    std::string get_session_token() {
      return session_token;
    }
    std::string get_role_id() {
      return role_id;
    }
    int get_packed_policy_size() {
      return policy_size;
    }
    struct timeval get_expiration_time() {
      return expiration;
    }
};

#endif /* CEPH_RGW_STS_H */

