#include "rgw_op.h"
#include "rgw_user.h"
#include "rgw_rest_user.h"

#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

class RGWOp_User_Info : public RGWRESTOp {

public:
  RGWOp_User_Info() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_READ);
  }

  void execute();

  virtual const char *name() { return "get_user_info"; }
};

void RGWOp_User_Info::execute()
{
  RGWUserAdminOpState op_state;

  std::string uid;

  RESTArgs::get_string(s, "uid", uid, &uid);

  op_state.set_user_id(uid);

  http_ret = RGWUserAdminOp_User::info(store, op_state, flusher);
}

class RGWOp_User_Create : public RGWRESTOp {

public:
  RGWOp_User_Create() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "create_user"; }
};

void RGWOp_User_Create::execute()
{
  std::string uid;
  std::string display_name;
  std::string email;
  std::string access_key;
  std::string secret_key;
  std::string key_type_str;
  std::string caps;

  bool gen_key;
  bool suspended;

  uint32_t max_buckets;
  int32_t key_type = KEY_TYPE_UNDEFINED;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
  RESTArgs::get_string(s, "display-name", display_name, &display_name);
  RESTArgs::get_string(s, "email", email, &email);
  RESTArgs::get_string(s, "access-key", access_key, &access_key);
  RESTArgs::get_string(s, "secret-key", secret_key, &secret_key);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);
  RESTArgs::get_string(s, "user-caps", caps, &caps);
  RESTArgs::get_bool(s, "generate-key", true, &gen_key);
  RESTArgs::get_bool(s, "suspended", false, &suspended);
  RESTArgs::get_uint32(s, "max-buckets", RGW_DEFAULT_MAX_BUCKETS, &max_buckets);

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
    if (key_type_str.compare("swift") == 0)
      key_type = KEY_TYPE_SWIFT;
    else if (key_type_str.compare("s3") == 0)
      key_type = KEY_TYPE_S3;

    op_state.set_key_type(key_type);
  }

  if (max_buckets != RGW_DEFAULT_MAX_BUCKETS)
    op_state.set_max_buckets(max_buckets);

  if (s->args.exists("suspended"))
    op_state.set_suspension(suspended);

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

  virtual const char *name() { return "modify_user"; }
};

void RGWOp_User_Modify::execute()
{
  std::string uid;
  std::string display_name;
  std::string email;
  std::string access_key;
  std::string secret_key;
  std::string key_type_str;
  std::string caps;

  bool gen_key;
  bool suspended;

  uint32_t max_buckets;
  int32_t key_type = KEY_TYPE_UNDEFINED;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
  RESTArgs::get_string(s, "display-name", display_name, &display_name);
  RESTArgs::get_string(s, "email", email, &email);
  RESTArgs::get_string(s, "access-key", access_key, &access_key);
  RESTArgs::get_string(s, "secret-key", secret_key, &secret_key);
  RESTArgs::get_string(s, "user-caps", caps, &caps);
  RESTArgs::get_bool(s, "generate-key", false, &gen_key);
  RESTArgs::get_bool(s, "suspended", false, &suspended);
  RESTArgs::get_uint32(s, "max-buckets", RGW_DEFAULT_MAX_BUCKETS, &max_buckets);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);

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
    if (key_type_str.compare("swift") == 0)
      key_type = KEY_TYPE_SWIFT;
    else if (key_type_str.compare("s3") == 0)
      key_type = KEY_TYPE_S3;

    op_state.set_key_type(key_type);
  }

  if (s->args.exists("suspended"))
    op_state.set_suspension(suspended);

  http_ret = RGWUserAdminOp_User::modify(store, op_state, flusher);
}

class RGWOp_User_Remove : public RGWRESTOp {

public:
  RGWOp_User_Remove() {}

  int check_caps(RGWUserCaps& caps) {
    return caps.check_cap("users", RGW_CAP_WRITE);
  }

  void execute();

  virtual const char *name() { return "remove_user"; }
};

void RGWOp_User_Remove::execute()
{
  std::string uid;
  bool purge_data;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
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

  virtual const char *name() { return "create_subuser"; }
};

void RGWOp_Subuser_Create::execute()
{
  std::string uid;
  std::string subuser;
  std::string secret_key;
  std::string perm_str;
  std::string key_type_str;

  bool gen_subuser = false; // FIXME placeholder
  bool gen_secret;

  uint32_t perm_mask = 0;
  int32_t key_type = KEY_TYPE_SWIFT;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
  RESTArgs::get_string(s, "subuser", subuser, &subuser);
  RESTArgs::get_string(s, "secret-key", secret_key, &secret_key);
  RESTArgs::get_string(s, "access", perm_str, &perm_str);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);
  //RESTArgs::get_bool(s, "generate-subuser", false, &gen_subuser);
  RESTArgs::get_bool(s, "generate-secret", false, &gen_secret);

  perm_mask = rgw_str_to_perm(perm_str.c_str());

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!subuser.empty())
    op_state.set_subuser(subuser);

  if (!secret_key.empty())
    op_state.set_secret_key(secret_key);

  if (perm_mask != 0)
    op_state.set_perm(perm_mask);

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

  virtual const char *name() { return "modify_subuser"; }
};

void RGWOp_Subuser_Modify::execute()
{
  std::string uid;
  std::string subuser;
  std::string secret_key;
  std::string key_type_str;
  std::string perm_str;

  RGWUserAdminOpState op_state;

  uint32_t perm_mask;
  int32_t key_type = KEY_TYPE_SWIFT;

  bool gen_secret;

  RESTArgs::get_string(s, "uid", uid, &uid);
  RESTArgs::get_string(s, "subuser", subuser, &subuser);
  RESTArgs::get_string(s, "secret-key", secret_key, &secret_key);
  RESTArgs::get_string(s, "access", perm_str, &perm_str);
  RESTArgs::get_string(s, "key-type", key_type_str, &key_type_str);
  RESTArgs::get_bool(s, "generate-secret", false, &gen_secret);

  perm_mask = rgw_str_to_perm(perm_str.c_str());

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!subuser.empty())
    op_state.set_subuser(subuser);

  if (!secret_key.empty())
    op_state.set_secret_key(secret_key);

  if (gen_secret)
    op_state.set_gen_secret();

  if (perm_mask != 0)
    op_state.set_perm(perm_mask);

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

  virtual const char *name() { return "remove_subuser"; }
};

void RGWOp_Subuser_Remove::execute()
{
  std::string uid;
  std::string subuser;
  bool purge_keys;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
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

  virtual const char *name() { return "create_access_key"; }
};

void RGWOp_Key_Create::execute()
{
  std::string uid;
  std::string subuser;
  std::string access_key;
  std::string secret_key;
  std::string key_type_str;

  int32_t key_type = KEY_TYPE_UNDEFINED;
  bool gen_key;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
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

  virtual const char *name() { return "remove_access_key"; }
};

void RGWOp_Key_Remove::execute()
{
  std::string uid;
  std::string subuser;
  std::string access_key;
  std::string key_type_str;

  int32_t key_type = KEY_TYPE_UNDEFINED;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
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

  virtual const char *name() { return "add_user_caps"; }
};

void RGWOp_Caps_Add::execute()
{
  std::string uid;
  std::string caps;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
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

  virtual const char *name() { return "remove_user_caps"; }
};

void RGWOp_Caps_Remove::execute()
{
  std::string uid;
  std::string caps;

  RGWUserAdminOpState op_state;

  RESTArgs::get_string(s, "uid", uid, &uid);
  RESTArgs::get_string(s, "user-caps", caps, &caps);

  // FIXME: no double checking
  if (!uid.empty())
    op_state.set_user_id(uid);

  if (!caps.empty())
    op_state.set_caps(caps);

  http_ret = RGWUserAdminOp_Caps::remove(store, op_state, flusher);
}

RGWOp *RGWHandler_User::op_get()
{
  return new RGWOp_User_Info;
};

RGWOp *RGWHandler_User::op_put()
{
  if (s->args.sub_resource_exists("subuser"))
    return new RGWOp_Subuser_Create;

  if (s->args.sub_resource_exists("key"))
    return new RGWOp_Key_Create;

  if (s->args.sub_resource_exists("caps"))
    return new RGWOp_Caps_Add;

  return new RGWOp_User_Create;
};

RGWOp *RGWHandler_User::op_post()
{
  if (s->args.sub_resource_exists("subuser"))
    return new RGWOp_Subuser_Modify;

  return new RGWOp_User_Modify;
};

RGWOp *RGWHandler_User::op_delete()
{
  if (s->args.sub_resource_exists("subuser"))
    return new RGWOp_Subuser_Remove;

  if (s->args.sub_resource_exists("key"))
    return new RGWOp_Key_Remove;

  if (s->args.sub_resource_exists("caps"))
    return new RGWOp_Caps_Remove;

  return new RGWOp_User_Remove;
};

