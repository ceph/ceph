// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_RGW_SWIFT_H
#define CEPH_RGW_SWIFT_H

#include "rgw_common.h"
#include "common/Cond.h"

class RGWRados;
class KeystoneToken;

struct rgw_swift_auth_info {
  int status;
  string auth_groups;
  string user;
  string display_name;
  long long ttl;

  rgw_swift_auth_info() : status(0), ttl(0) {}
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
  int get_keystone_url(std::string& url);
  int get_keystone_admin_token(std::string& token);

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
  bool do_verify_swift_token(RGWRados *store, req_state *s);
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

  /* Static methods shared between Swift API and S3. */
  static int get_keystone_url(CephContext *cct, std::string& url);
  static int get_keystone_admin_token(CephContext *cct, std::string& token);
};

extern RGWSwift *rgw_swift;
void swift_init(CephContext *cct);
void swift_finalize();

#endif

