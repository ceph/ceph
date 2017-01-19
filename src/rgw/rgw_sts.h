#ifndef CEPH_RGW_STS_H
#define CEPH_RGW_STS_H

class RGWSts
{
  static const string key;
  static constexpr int POLICY_PLAIN_TEXT_SIZE = 2048;
  static constexpr int ACCESS_KEY_LEN = 20;
  static constexpr int SECRET_KEY_LEN = 40;

  int duration;
  std::string role_id;
  std::string policy;
  std::map<std::string, std::string> saml_keys;

  int generate_key(char* buf, int size);

public:
  struct credentials {
    std::string access_key_id;
    std::string secret_access_key;
    std::string session_token;
    time_t      expiration;

    void dump(Formatter *f) const;
  };

  struct assumedroleuser {
    std::string assumedroleid;

    void dump(Formatter *f) const;
  };

  struct sts{
    struct credentials cred;
    struct assumedroleuser role;
    int packedpolicysize;

    void dump(Formatter *f) const;
  };

  RGWSts (int duration, std::string role_id, std::string policy, std::map<std::string, std::string> saml_keys) :
    duration(std::move(duration)),
    role_id(std::move(role_id)),
    policy(std::move(policy)),
    saml_keys(std::move(saml_keys)) {}

  int validate_input();
  int build_output(struct sts& sts_output);
};

#endif /* CEPH_RGW_STS_H */

