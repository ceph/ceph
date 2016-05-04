// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_json.h"

#include "rgw_op.h"
#include "rgw_user.h"
#include "rgw_rest_user.h"

#include "include/str_list.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

class RGWOp_User_Info : public RGWRESTOp {

public:
  RGWOp_User_Info() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_READ);
  }

  void execute();

  virtual const string name() { return "get_user_info"; }
};

void RGWOp_User_Info::execute()
{
  RGWUserAdminOpState op_state;

  std::string uid_str;
  bool fetch_stats;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_bool(s, "stats", false, &fetch_stats);

  op_state.set_user_id(uid);
  op_state.set_fetch_stats(fetch_stats);

  http_ret = RGWUserAdminOp_User::info(store, op_state, flusher);
}

class RGWOp_User_Create : public RGWRESTOp {

public:
  RGWOp_User_Create() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "create_user"; }
};

void RGWOp_User_Create::execute()
{
  std::string uid_str;
  std::string display_name;
  std::string email;
  std::string access_key;
  std::string secret_key;
  std::string key_type_str;
  std::string caps;

  bool gen_key;
  bool suspended;
  bool system;
  bool exclusive;

  uint32_t max_buckets;
  uint32_t default_max_buckets = s->cct->_conf->rgw_user_max_buckets;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "display-name", display_name, &display_name);
  RESTArgs::get_string(s, "email", email, &email);
  RESTArgs::get_string(s, "access-key", access_key, &access_key);
  RESTArgs::get_string(s, "secret-key", secret_key, &secret_key);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);
  RESTArgs::get_string(s, "user-caps", caps, &caps);
  RESTArgs::get_bool(s, "generate-key", true, &gen_key);
  RESTArgs::get_bool(s, "suspended", false, &suspended);
  RESTArgs::get_uint32(s, "max-buckets", default_max_buckets, &max_buckets);
  RESTArgs::get_bool(s, "system", false, &system);
  RESTArgs::get_bool(s, "exclusive", false, &exclusive);

  if (!s->user->system && system) {
    ldout(s->cct, 0) << "cannot set system flag by non-system user" << dendl;
    http_ret = -EINVAL;
    return;
  }

  // FIXME: don't do double argument checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!display_name.empty())
    op_state.set_display_name(display_name);

  if (!email.empty())
    op_state.set_user_email(email);

  if (!caps.empty())
    op_state.set_caps(caps);

  if (!access_key.empty())
    op_state.set_access_key(access_key);

  if (!secret_key.empty())
    op_state.set_secret_key(secret_key);

  if (!key_type_str.empty()) {
    int32_t key_type = KEY_TYPE_UNDEFINED;
    if (key_type_str.compare("swift") == 0)
      key_type = KEY_TYPE_SWIFT;
    else if (key_type_str.compare("s3") == 0)
      key_type = KEY_TYPE_S3;

    op_state.set_key_type(key_type);
  }

  if (max_buckets != default_max_buckets)
    op_state.set_max_buckets(max_buckets);

  if (s->info.args.exists("suspended"))
    op_state.set_suspension(suspended);

  if (s->info.args.exists("system"))
    op_state.set_system(system);

  if (s->info.args.exists("exclusive"))
    op_state.set_exclusive(exclusive);

  if (gen_key)
    op_state.set_generate_key();

  http_ret = RGWUserAdminOp_User::create(store, op_state, flusher);
}

class RGWOp_User_Modify : public RGWRESTOp {

public:
  RGWOp_User_Modify() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "modify_user"; }
};

void RGWOp_User_Modify::execute()
{
  std::string uid_str;
  std::string display_name;
  std::string email;
  std::string access_key;
  std::string secret_key;
  std::string key_type_str;
  std::string caps;

  bool gen_key;
  bool suspended;
  bool system;

  uint32_t max_buckets;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "display-name", display_name, &display_name);
  RESTArgs::get_string(s, "email", email, &email);
  RESTArgs::get_string(s, "access-key", access_key, &access_key);
  RESTArgs::get_string(s, "secret-key", secret_key, &secret_key);
  RESTArgs::get_string(s, "user-caps", caps, &caps);
  RESTArgs::get_bool(s, "generate-key", false, &gen_key);
  RESTArgs::get_bool(s, "suspended", false, &suspended);
  RESTArgs::get_uint32(s, "max-buckets", RGW_DEFAULT_MAX_BUCKETS, &max_buckets);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);

  RESTArgs::get_bool(s, "system", false, &system);

  if (!s->user->system && system) {
    ldout(s->cct, 0) << "cannot set system flag by non-system user" << dendl;
    http_ret = -EINVAL;
    return;
  }

  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!display_name.empty())
    op_state.set_display_name(display_name);

  if (!email.empty())
    op_state.set_user_email(email);

  if (!caps.empty())
    op_state.set_caps(caps);

  if (!access_key.empty())
    op_state.set_access_key(access_key);

  if (!secret_key.empty())
    op_state.set_secret_key(secret_key);

  if (max_buckets != RGW_DEFAULT_MAX_BUCKETS)
    op_state.set_max_buckets(max_buckets);

  if (gen_key)
    op_state.set_generate_key();

  if (!key_type_str.empty()) {
    int32_t key_type = KEY_TYPE_UNDEFINED;
    if (key_type_str.compare("swift") == 0)
      key_type = KEY_TYPE_SWIFT;
    else if (key_type_str.compare("s3") == 0)
      key_type = KEY_TYPE_S3;

    op_state.set_key_type(key_type);
  }

  if (s->info.args.exists("suspended"))
    op_state.set_suspension(suspended);

  if (s->info.args.exists("system"))
    op_state.set_system(system);

  http_ret = RGWUserAdminOp_User::modify(store, op_state, flusher);
}

class RGWOp_User_Remove : public RGWRESTOp {

public:
  RGWOp_User_Remove() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "remove_user"; }
};

void RGWOp_User_Remove::execute()
{
  std::string uid_str;
  bool purge_data;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_bool(s, "purge-data", false, &purge_data);

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  op_state.set_purge_data(purge_data);

  http_ret = RGWUserAdminOp_User::remove(store, op_state, flusher);
}

class RGWOp_Subuser_Create : public RGWRESTOp {

public:
  RGWOp_Subuser_Create() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "create_subuser"; }
};

void RGWOp_Subuser_Create::execute()
{
  std::string uid_str;
  std::string subuser;
  std::string secret_key;
  std::string perm_str;
  std::string key_type_str;

  bool gen_subuser = false; // FIXME placeholder
  bool gen_secret;

  uint32_t perm_mask = 0;
  int32_t key_type = KEY_TYPE_SWIFT;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "subuser", subuser, &subuser);
  RESTArgs::get_string(s, "secret-key", secret_key, &secret_key);
  RESTArgs::get_string(s, "access", perm_str, &perm_str);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);
  //RESTArgs::get_bool(s, "generate-subuser", false, &gen_subuser);
  RESTArgs::get_bool(s, "generate-secret", false, &gen_secret);

  perm_mask = rgw_str_to_perm(perm_str.c_str());
  op_state.set_perm(perm_mask);

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!subuser.empty())
    op_state.set_subuser(subuser);

  if (!secret_key.empty())
    op_state.set_secret_key(secret_key);

  op_state.set_generate_subuser(gen_subuser);

  if (gen_secret)
    op_state.set_gen_secret();

  if (!key_type_str.empty()) {
    if (key_type_str.compare("swift") == 0)
      key_type = KEY_TYPE_SWIFT;
    else if (key_type_str.compare("s3") == 0)
      key_type = KEY_TYPE_S3;
  }
  op_state.set_key_type(key_type);

  http_ret = RGWUserAdminOp_Subuser::create(store, op_state, flusher);
}

class RGWOp_Subuser_Modify : public RGWRESTOp {

public:
  RGWOp_Subuser_Modify() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "modify_subuser"; }
};

void RGWOp_Subuser_Modify::execute()
{
  std::string uid_str;
  std::string subuser;
  std::string secret_key;
  std::string key_type_str;
  std::string perm_str;

  RGWUserAdminOpState op_state;

  uint32_t perm_mask;
  int32_t key_type = KEY_TYPE_SWIFT;

  bool gen_secret;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "subuser", subuser, &subuser);
  RESTArgs::get_string(s, "secret-key", secret_key, &secret_key);
  RESTArgs::get_string(s, "access", perm_str, &perm_str);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);
  RESTArgs::get_bool(s, "generate-secret", false, &gen_secret);

  perm_mask = rgw_str_to_perm(perm_str.c_str());
  op_state.set_perm(perm_mask);

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!subuser.empty())
    op_state.set_subuser(subuser);

  if (!secret_key.empty())
    op_state.set_secret_key(secret_key);

  if (gen_secret)
    op_state.set_gen_secret();

  if (!key_type_str.empty()) {
    if (key_type_str.compare("swift") == 0)
      key_type = KEY_TYPE_SWIFT;
    else if (key_type_str.compare("s3") == 0)
      key_type = KEY_TYPE_S3;
  }
  op_state.set_key_type(key_type);

  http_ret = RGWUserAdminOp_Subuser::modify(store, op_state, flusher);
}

class RGWOp_Subuser_Remove : public RGWRESTOp {

public:
  RGWOp_Subuser_Remove() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "remove_subuser"; }
};

void RGWOp_Subuser_Remove::execute()
{
  std::string uid_str;
  std::string subuser;
  bool purge_keys;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "subuser", subuser, &subuser);
  RESTArgs::get_bool(s, "purge-keys", true, &purge_keys);

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!subuser.empty())
    op_state.set_subuser(subuser);

  if (purge_keys)
    op_state.set_purge_keys();

  http_ret = RGWUserAdminOp_Subuser::remove(store, op_state, flusher);
}

class RGWOp_Key_Create : public RGWRESTOp {

public:
  RGWOp_Key_Create() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "create_access_key"; }
};

void RGWOp_Key_Create::execute()
{
  std::string uid_str;
  std::string subuser;
  std::string access_key;
  std::string secret_key;
  std::string key_type_str;

  bool gen_key;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "subuser", subuser, &subuser);
  RESTArgs::get_string(s, "access-key", access_key, &access_key);
  RESTArgs::get_string(s, "secret-key", secret_key, &secret_key);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);
  RESTArgs::get_bool(s, "generate-key", true, &gen_key);

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!subuser.empty())
    op_state.set_subuser(subuser);

  if (!access_key.empty())
    op_state.set_access_key(access_key);

  if (!secret_key.empty())
    op_state.set_secret_key(secret_key);

  if (gen_key)
    op_state.set_generate_key();

  if (!key_type_str.empty()) {
    int32_t key_type = KEY_TYPE_UNDEFINED;
    if (key_type_str.compare("swift") == 0)
      key_type = KEY_TYPE_SWIFT;
    else if (key_type_str.compare("s3") == 0)
      key_type = KEY_TYPE_S3;

    op_state.set_key_type(key_type);
  }

  http_ret = RGWUserAdminOp_Key::create(store, op_state, flusher);
}

class RGWOp_Key_Remove : public RGWRESTOp {

public:
  RGWOp_Key_Remove() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "remove_access_key"; }
};

void RGWOp_Key_Remove::execute()
{
  std::string uid_str;
  std::string subuser;
  std::string access_key;
  std::string key_type_str;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "subuser", subuser, &subuser);
  RESTArgs::get_string(s, "access-key", access_key, &access_key);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!subuser.empty())
    op_state.set_subuser(subuser);

  if (!access_key.empty())
    op_state.set_access_key(access_key);

  if (!key_type_str.empty()) {
    int32_t key_type = KEY_TYPE_UNDEFINED;
    if (key_type_str.compare("swift") == 0)
      key_type = KEY_TYPE_SWIFT;
    else if (key_type_str.compare("s3") == 0)
      key_type = KEY_TYPE_S3;

    op_state.set_key_type(key_type);
  }

  http_ret = RGWUserAdminOp_Key::remove(store, op_state, flusher);
}

class RGWOp_Caps_Add : public RGWRESTOp {

public:
  RGWOp_Caps_Add() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "add_user_caps"; }
};

void RGWOp_Caps_Add::execute()
{
  std::string uid_str;
  std::string caps;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "user-caps", caps, &caps);

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!caps.empty())
    op_state.set_caps(caps);

  http_ret = RGWUserAdminOp_Caps::add(store, op_state, flusher);
}

class RGWOp_Caps_Remove : public RGWRESTOp {

public:
  RGWOp_Caps_Remove() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "remove_user_caps"; }
};

void RGWOp_Caps_Remove::execute()
{
  std::string uid_str;
  std::string caps;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  rgw_user uid(uid_str);

  RESTArgs::get_string(s, "user-caps", caps, &caps);

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!caps.empty())
    op_state.set_caps(caps);

  http_ret = RGWUserAdminOp_Caps::remove(store, op_state, flusher);
}

struct UserQuotas {
  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  UserQuotas() {}

  explicit UserQuotas(RGWUserInfo& info) : bucket_quota(info.bucket_quota), 
				  user_quota(info.user_quota) {}

  void dump(Formatter *f) const {
    encode_json("bucket_quota", bucket_quota, f);
    encode_json("user_quota", user_quota, f);
  }
  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("bucket_quota", bucket_quota, obj);
    JSONDecoder::decode_json("user_quota", user_quota, obj);
  }
};

class RGWOp_Quota_Info : public RGWRESTOp {

public:
  RGWOp_Quota_Info() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_READ);
  }

  void execute();

  virtual const string name() { return "get_quota_info"; }
};


void RGWOp_Quota_Info::execute()
{
  RGWUserAdminOpState op_state;

  std::string uid_str;
  std::string quota_type;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "quota-type", quota_type, &quota_type);

  if (uid_str.empty()) {
    http_ret = -EINVAL;
    return;
  }

  rgw_user uid(uid_str);

  bool show_all = quota_type.empty();
  bool show_bucket = show_all || (quota_type == "bucket");
  bool show_user = show_all || (quota_type == "user");

  if (!(show_all || show_bucket || show_user)) {
    http_ret = -EINVAL;
    return;
  }

  op_state.set_user_id(uid);

  RGWUser user;
  http_ret = user.init(store, op_state);
  if (http_ret < 0)
    return;

  RGWUserInfo info;
  string err_msg;
  http_ret = user.info(info, &err_msg);
  if (http_ret < 0)
    return;

  flusher.start(0);
  if (show_all) {
    UserQuotas quotas(info);
    encode_json("quota", quotas, s->formatter);
  } else if (show_user) {
    encode_json("user_quota", info.user_quota, s->formatter);
  } else {
    encode_json("bucket_quota", info.bucket_quota, s->formatter);
  }

  flusher.flush();
}

class RGWOp_Quota_Set : public RGWRESTOp {

public:
  RGWOp_Quota_Set() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const string name() { return "set_quota_info"; }
};

/**
 * set quota
 *
 * two different ways to set the quota info: as json struct in the message body or via http params.
 *
 * as json:
 *
 * PUT /admin/user?uid=<uid>[&quota-type=<type>]
 *
 * whereas quota-type is optional and is either user, or bucket
 *
 * if quota-type is not specified then we expect to get a structure that contains both quotas,
 * otherwise we'll only get the relevant configuration.
 *
 * E.g., if quota type not specified:
 * {
 *    "user_quota" : {
 *      "max_size_kb" : 4096,
 *      "max_objects" : -1,
 *      "enabled" : false
 *    },
 *    "bucket_quota" : {
 *      "max_size_kb" : 1024,
 *      "max_objects" : -1,
 *      "enabled" : true
 *    }
 * }
 *
 *
 * or if quota type is specified:
 * {
 *   "max_size_kb" : 4096,
 *   "max_objects" : -1,
 *   "enabled" : false
 * }
 *
 * Another option is not to pass any body and set the following http params:
 *
 *
 * max-size-kb=<size>
 * max-objects=<max objects>
 * enabled[={true,false}]
 *
 * all params are optionals and default to the current settings. With this type of configuration the
 * quota-type param is mandatory.
 *
 */

void RGWOp_Quota_Set::execute()
{
  RGWUserAdminOpState op_state;

  std::string uid_str;
  std::string quota_type;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "quota-type", quota_type, &quota_type);

  if (uid_str.empty()) {
    http_ret = -EINVAL;
    return;
  }

  rgw_user uid(uid_str);

  bool set_all = quota_type.empty();
  bool set_bucket = set_all || (quota_type == "bucket");
  bool set_user = set_all || (quota_type == "user");

  if (!(set_all || set_bucket || set_user)) {
    ldout(store->ctx(), 20) << "invalid quota type" << dendl;
    http_ret = -EINVAL;
    return;
  }

  bool use_http_params;

  if (s->content_length > 0) {
    use_http_params = false;
  } else {
    const char *encoding = s->info.env->get("HTTP_TRANSFER_ENCODING");
    use_http_params = (!encoding || strcmp(encoding, "chunked") != 0);
  }

  if (use_http_params && set_all) {
    ldout(store->ctx(), 20) << "quota type was not specified, can't set all quotas via http headers" << dendl;
    http_ret = -EINVAL;
    return;
  }

  op_state.set_user_id(uid);

  RGWUser user;
  http_ret = user.init(store, op_state);
  if (http_ret < 0) {
    ldout(store->ctx(), 20) << "failed initializing user info: " << http_ret << dendl;
    return;
  }

#define QUOTA_INPUT_MAX_LEN 1024
  if (set_all) {
    UserQuotas quotas;

    if ((http_ret = rgw_rest_get_json_input(store->ctx(), s, quotas, QUOTA_INPUT_MAX_LEN, NULL)) < 0) {
      ldout(store->ctx(), 20) << "failed to retrieve input" << dendl;
      return;
    }

    op_state.set_user_quota(quotas.user_quota);
    op_state.set_bucket_quota(quotas.bucket_quota);
  } else {
    RGWQuotaInfo quota;

    if (!use_http_params) {
      bool empty;
      http_ret = rgw_rest_get_json_input(store->ctx(), s, quota, QUOTA_INPUT_MAX_LEN, &empty);
      if (http_ret < 0) {
        ldout(store->ctx(), 20) << "failed to retrieve input" << dendl;
        if (!empty)
          return;

        /* was probably chunked input, but no content provided, configure via http params */
        use_http_params = true;
      }
    }

    if (use_http_params) {
      RGWUserInfo info;
      string err_msg;
      http_ret = user.info(info, &err_msg);
      if (http_ret < 0) {
        ldout(store->ctx(), 20) << "failed to get user info: " << http_ret << dendl;
        return;
      }
      RGWQuotaInfo *old_quota;
      if (set_user) {
        old_quota = &info.user_quota;
      } else {
        old_quota = &info.bucket_quota;
      }

      int64_t old_max_size_kb = rgw_rounded_kb(old_quota->max_size);
      int64_t max_size_kb;
      RESTArgs::get_int64(s, "max-objects", old_quota->max_objects, &quota.max_objects);
      RESTArgs::get_int64(s, "max-size-kb", old_max_size_kb, &max_size_kb);
      quota.max_size = max_size_kb * 1024;
      RESTArgs::get_bool(s, "enabled", old_quota->enabled, &quota.enabled);
    }

    if (set_user) {
      op_state.set_user_quota(quota);
    } else {
      op_state.set_bucket_quota(quota);
    }
  }

  string err;
  http_ret = user.modify(op_state, &err);
  if (http_ret < 0) {
    ldout(store->ctx(), 20) << "failed updating user info: " << http_ret << ": " << err << dendl;
    return;
  }
}

RGWOp *RGWHandler_User::op_get()
{
  if (s->info.args.sub_resource_exists("quota"))
    return new RGWOp_Quota_Info;

  return new RGWOp_User_Info;
}

RGWOp *RGWHandler_User::op_put()
{
  if (s->info.args.sub_resource_exists("subuser"))
    return new RGWOp_Subuser_Create;

  if (s->info.args.sub_resource_exists("key"))
    return new RGWOp_Key_Create;

  if (s->info.args.sub_resource_exists("caps"))
    return new RGWOp_Caps_Add;

  if (s->info.args.sub_resource_exists("quota"))
    return new RGWOp_Quota_Set;

  return new RGWOp_User_Create;
}

RGWOp *RGWHandler_User::op_post()
{
  if (s->info.args.sub_resource_exists("subuser"))
    return new RGWOp_Subuser_Modify;

  return new RGWOp_User_Modify;
}

RGWOp *RGWHandler_User::op_delete()
{
  if (s->info.args.sub_resource_exists("subuser"))
    return new RGWOp_Subuser_Remove;

  if (s->info.args.sub_resource_exists("key"))
    return new RGWOp_Key_Remove;

  if (s->info.args.sub_resource_exists("caps"))
    return new RGWOp_Caps_Remove;

  return new RGWOp_User_Remove;
}

