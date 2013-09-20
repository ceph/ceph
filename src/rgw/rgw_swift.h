
#ifndef CEPH_RGW_SWIFT_H
#define CEPH_RGW_SWIFT_H

#include "rgw_common.h"
#include "common/Cond.h"

class RGWRados;

struct rgw_swift_auth_info {
  int status;
  string auth_groups;
  string user;
  string display_name;
  long long ttl;

  rgw_swift_auth_info() : status(0), ttl(0) {}
};

class KeystoneToken {
public:
  string tenant_name;
  string tenant_id;
  string user_name;
  time_t expiration;

  map<string, bool> roles;

  KeystoneToken() : expiration(0) {}

  int parse(CephContext *cct, bufferlist& bl);

  bool expired() {
    uint64_t now = ceph_clock_now(NULL).sec();
    return (now >= (uint64_t)expiration);
  }
};

class RGWSwift {
  CephContext *cct;
  atomic_t down_flag;

  int validate_token(const char *token, struct rgw_swift_auth_info *info);
  int validate_keystone_token(RGWRados *store, const string& token, struct rgw_swift_auth_info *info,
			      RGWUserInfo& rgw_user);

  int parse_keystone_token_response(const string& token, bufferlist& bl, struct rgw_swift_auth_info *info,
		                    KeystoneToken& t);
  int update_user_info(RGWRados *store, struct rgw_swift_auth_info *info, RGWUserInfo& user_info);

  class KeystoneRevokeThread : public Thread {
    CephContext *cct;
    RGWSwift *swift;
    Mutex lock;
    Cond cond;

  public:
    KeystoneRevokeThread(CephContext *_cct, RGWSwift *_swift) : cct(_cct), swift(_swift), lock("KeystoneRevokeThread") {}
    void *entry();
    void stop();
  };

  KeystoneRevokeThread *keystone_revoke_thread;

  void init();
  void finalize();
  void init_keystone();
  void finalize_keystone();
  bool supports_keystone() {
    return !cct->_conf->rgw_keystone_url.empty();
  }
protected:
  int check_revoked();
public:

  RGWSwift(CephContext *_cct) : cct(_cct), keystone_revoke_thread(NULL) {
    init();
  }
  ~RGWSwift() {
    finalize();
  }

  bool verify_swift_token(RGWRados *store, req_state *s);
  bool going_down();
};

extern RGWSwift *rgw_swift;
void swift_init(CephContext *cct);
void swift_finalize();

#endif

