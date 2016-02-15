// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_USER_H
#define CEPH_RGW_USER_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"

#include "rgw_rados.h"

#include "rgw_string.h"

#include "common/Formatter.h"
#include "rgw_formats.h"

using namespace std;

#define RGW_USER_ANON_ID "anonymous"

#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20
#define RAND_SUBUSER_LEN 5

/**
 * A string wrapper that includes encode/decode functions
 * for easily accessing a UID in all forms
 */
struct RGWUID
{
  rgw_user user_id;
  void encode(bufferlist& bl) const {
    string s;
    user_id.to_str(s);
    ::encode(s, bl);
  }
  void decode(bufferlist::iterator& bl) {
    string s;
    ::decode(s, bl);
    user_id.from_str(s);
  }
};
WRITE_CLASS_ENCODER(RGWUID)

extern int rgw_user_sync_all_stats(RGWRados *store, const rgw_user& user_id);
/**
 * Get the anonymous (ie, unauthenticated) user info.
 */
extern void rgw_get_anon_user(RGWUserInfo& info,
                              rgw_user& auth_user,
                              RGWRados * store = NULL,
                              const std::string& account_name = std::string());

/**
 * verify that user is an actual user, and not the anonymous user
 */
extern bool rgw_user_is_authenticated(const rgw_user& auth_user);
/**
 * Save the given user information to storage.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_store_user_info(RGWRados *store,
                               RGWUserInfo& info,
                               RGWUserInfo *old_info,
                               RGWObjVersionTracker *objv_tracker,
                               real_time mtime,
                               bool exclusive,
                               map<string, bufferlist> *pattrs = NULL);

/**
 * Given an user_id, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_uid(RGWRados *store,
                                    const rgw_user& user_id,
                                    RGWUserInfo& info,
                                    RGWObjVersionTracker *objv_tracker = NULL,
                                    real_time *pmtime                     = NULL,
                                    rgw_cache_entry_info *cache_info   = NULL,
                                    map<string, bufferlist> *pattrs    = NULL);
/**
 * Given an email, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_email(RGWRados *store, string& email, RGWUserInfo& info,
                                      RGWObjVersionTracker *objv_tracker = NULL, real_time *pmtime = NULL);
/**
 * Given an swift username, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_swift(RGWRados *store, string& swift_name, RGWUserInfo& info,
                                      RGWObjVersionTracker *objv_tracker = NULL, real_time *pmtime = NULL);
/**
 * Given an access key, finds the user info associated with it.
 * returns: 0 on success, -ERR# on failure (including nonexistence)
 */
extern int rgw_get_user_info_by_access_key(RGWRados *store, string& access_key, RGWUserInfo& info,
                                           RGWObjVersionTracker *objv_tracker = NULL, real_time *pmtime = NULL);
/**
 * Get all the custom metadata stored for user specified in @user_id
 * and put it into @attrs.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_get_user_attrs_by_uid(RGWRados *store,
                                     const rgw_user& user_id,
                                     map<string, bufferlist>& attrs,
                                     RGWObjVersionTracker *objv_tracker = NULL);
/**
 * Given an RGWUserInfo, deletes the user and its bucket ACLs.
 */
extern int rgw_delete_user(RGWRados *store, RGWUserInfo& user, RGWObjVersionTracker& objv_tracker);
/**
 * Store a list of the user's buckets, with associated functinos.
 */

/*
 * remove the different indexes
 */
extern int rgw_remove_key_index(RGWRados *store, RGWAccessKey& access_key);
extern int rgw_remove_uid_index(RGWRados *store, rgw_user& uid);
extern int rgw_remove_email_index(RGWRados *store, string& email);
extern int rgw_remove_swift_name_index(RGWRados *store, string& swift_name);

/*
 * An RGWUser class along with supporting classes created
 * to support the creation of an RESTful administrative API
 */

extern void rgw_perm_to_str(uint32_t mask, char *buf, int len);
extern uint32_t rgw_str_to_perm(const char *str);

enum ObjectKeyType {
  KEY_TYPE_SWIFT,
  KEY_TYPE_S3,
  KEY_TYPE_UNDEFINED
};

enum RGWKeyPoolOp {
  GENERATE_KEY,
  MODIFY_KEY
};

enum RGWUserId {
  RGW_USER_ID,
  RGW_SWIFT_USERNAME,
  RGW_USER_EMAIL,
  RGW_ACCESS_KEY,
};

struct RGWUserAdminOpState {
  // user attributes
  RGWUserInfo info;
  rgw_user user_id;
  std::string user_email;
  std::string display_name;
  uint32_t max_buckets;
  __u8 suspended;
  __u8 admin;
  __u8 system;
  __u8 exclusive;
  __u8 fetch_stats;
  std::string caps;
  RGWObjVersionTracker objv;
  uint32_t op_mask;
  map<int, string> temp_url_keys;

  // subuser attributes
  std::string subuser;
  uint32_t perm_mask;

  // key_attributes
  std::string id; // access key
  std::string key; // secret key
  int32_t key_type;

  // operation attributes
  bool existing_user;
  bool existing_key;
  bool existing_subuser;
  bool existing_email;
  bool subuser_specified;
  bool gen_secret;
  bool gen_access;
  bool gen_subuser;
  bool id_specified;
  bool key_specified;
  bool type_specified;
  bool key_type_setbycontext;   // key type set by user or subuser context
  bool purge_data;
  bool purge_keys;
  bool display_name_specified;
  bool user_email_specified;
  bool max_buckets_specified;
  bool perm_specified;
  bool op_mask_specified;
  bool caps_specified;
  bool suspension_op;
  bool admin_specified;
  bool system_specified;
  bool key_op;
  bool temp_url_key_specified;
  bool found_by_uid; 
  bool found_by_email;  
  bool found_by_key;
 
  // req parameters
  bool populated;
  bool initialized;
  bool key_params_checked;
  bool subuser_params_checked;
  bool user_params_checked;

  bool bucket_quota_specified;
  bool user_quota_specified;

  RGWQuotaInfo bucket_quota;
  RGWQuotaInfo user_quota;

  void set_access_key(std::string& access_key) {
    if (access_key.empty())
      return;

    id = access_key;
    id_specified = true;
    gen_access = false;
    key_op = true;
  }
  void set_secret_key(std::string& secret_key) {
    if (secret_key.empty())
      return;

    key = secret_key;
    key_specified = true;
    gen_secret = false;
    key_op = true;
  }
  void set_user_id(rgw_user& id) {
    if (id.empty())
      return;

    user_id = id;
  }
  void set_user_email(std::string& email) {
    if (email.empty())
      return;

    user_email = email;
    user_email_specified = true;
  }
  void set_display_name(std::string& name) {
    if (name.empty())
      return;

    display_name = name;
    display_name_specified = true;
  }
  void set_subuser(std::string& _subuser) {
    if (_subuser.empty())
      return;

    size_t pos = _subuser.find(":");
    if (pos != string::npos) {
      rgw_user tmp_id;
      tmp_id.from_str(_subuser.substr(0, pos));
      if (tmp_id.tenant.empty()) {
        user_id.id = tmp_id.id;
      } else {
        user_id = tmp_id;
      }
      subuser = _subuser.substr(pos+1);
    } else {
      subuser = _subuser;
    }

    subuser_specified = true;
  }
  void set_caps(std::string& _caps) {
    if (_caps.empty())
      return;

    caps = _caps;
    caps_specified = true;
  }
  void set_perm(uint32_t perm) {
    perm_mask = perm;
    perm_specified = true;
  }
  void set_op_mask(uint32_t mask) {
    op_mask = mask;
    op_mask_specified = true;
  }
  void set_temp_url_key(const string& key, int index) {
    temp_url_keys[index] = key;
    temp_url_key_specified = true;
  }
  void set_key_type(int32_t type) {
    key_type = type;
    type_specified = true;
  }
  void set_suspension(__u8 is_suspended) {
    suspended = is_suspended;
    suspension_op = true;
  }
  void set_admin(__u8 is_admin) {
    admin = is_admin;
    admin_specified = true;
  }
  void set_system(__u8 is_system) {
    system = is_system;
    system_specified = true;
  }
  void set_exclusive(__u8 is_exclusive) {
    exclusive = is_exclusive;
  }
  void set_fetch_stats(__u8 is_fetch_stats) {
    fetch_stats = is_fetch_stats;
  }
  void set_user_info(RGWUserInfo& user_info) {
    user_id = user_info.user_id;
    info = user_info;
  }
  void set_max_buckets(uint32_t mb) {
    max_buckets = mb;
    max_buckets_specified = true;
  }
  void set_gen_access() {
    gen_access = true;
    key_op = true;
  }
  void set_gen_secret() {
    gen_secret = true;
    key_op = true;
  }
  void set_generate_key() {
    if (id.empty())
      gen_access = true;
    if (key.empty())
      gen_secret = true;
    key_op = true;
  }
  void clear_generate_key() {
    gen_access = false;
    gen_secret = false;
  }
  void set_purge_keys() {
    purge_keys = true;
    key_op = true;
  }

  void set_bucket_quota(RGWQuotaInfo& quota) {
    bucket_quota = quota;
    bucket_quota_specified = true;
  }

  void set_user_quota(RGWQuotaInfo& quota) {
    user_quota = quota;
    user_quota_specified = true;
  }

  bool is_populated() { return populated; }
  bool is_initialized() { return initialized; }
  bool has_existing_user() { return existing_user; }
  bool has_existing_key() { return existing_key; }
  bool has_existing_subuser() { return existing_subuser; }
  bool has_existing_email() { return existing_email; }
  bool has_subuser() { return subuser_specified; }
  bool has_key_op() { return key_op; }
  bool has_caps_op() { return caps_specified; }
  bool has_suspension_op() { return suspension_op; }
  bool has_subuser_perm() { return perm_specified; }
  bool has_op_mask() { return op_mask_specified; }
  bool will_gen_access() { return gen_access; }
  bool will_gen_secret() { return gen_secret; }
  bool will_gen_subuser() { return gen_subuser; }
  bool will_purge_keys() { return purge_keys; }
  bool will_purge_data() { return purge_data; }
  bool will_generate_subuser() { return gen_subuser; }
  bool has_bucket_quota() { return bucket_quota_specified; }
  bool has_user_quota() { return user_quota_specified; }
  void set_populated() { populated = true; }
  void clear_populated() { populated = false; }
  void set_initialized() { initialized = true; }
  void set_existing_user(bool flag) { existing_user = flag; }
  void set_existing_key(bool flag) { existing_key = flag; }
  void set_existing_subuser(bool flag) { existing_subuser = flag; }
  void set_existing_email(bool flag) { existing_email = flag; }
  void set_purge_data(bool flag) { purge_data = flag; }
  void set_generate_subuser(bool flag) { gen_subuser = flag; }
  __u8 get_suspension_status() { return suspended; }
  int32_t get_key_type() {return key_type; }
  uint32_t get_subuser_perm() { return perm_mask; }
  uint32_t get_max_buckets() { return max_buckets; }
  uint32_t get_op_mask() { return op_mask; }
  RGWQuotaInfo& get_bucket_quota() { return bucket_quota; }
  RGWQuotaInfo& get_user_quota() { return user_quota; }

  rgw_user& get_user_id() { return user_id; }
  std::string get_subuser() { return subuser; }
  std::string get_access_key() { return id; }
  std::string get_secret_key() { return key; }
  std::string get_caps() { return caps; }
  std::string get_user_email() { return user_email; }
  std::string get_display_name() { return display_name; }
  map<int, std::string>& get_temp_url_keys() { return temp_url_keys; }

  RGWUserInfo&  get_user_info() { return info; }

  map<std::string, RGWAccessKey> *get_swift_keys() { return &info.swift_keys; }
  map<std::string, RGWAccessKey> *get_access_keys() { return &info.access_keys; }
  map<std::string, RGWSubUser> *get_subusers() { return &info.subusers; }

  RGWUserCaps *get_caps_obj() { return &info.caps; }

  std::string build_default_swift_kid() {
    if (user_id.empty() || subuser.empty())
      return "";

    std::string kid;
    user_id.to_str(kid);
    kid.append(":");
    kid.append(subuser);

    return kid;
  }

  std::string generate_subuser() {
    if (user_id.empty())
      return "";

    std::string generated_subuser;
    user_id.to_str(generated_subuser);
    std::string rand_suffix;

    int sub_buf_size = RAND_SUBUSER_LEN + 1;
    char sub_buf[RAND_SUBUSER_LEN + 1];

    if (gen_rand_alphanumeric_upper(g_ceph_context, sub_buf, sub_buf_size) < 0)
      return "";

    rand_suffix = sub_buf;
    if (rand_suffix.empty())
      return "";

    generated_subuser.append(rand_suffix);
    subuser = generated_subuser;

    return generated_subuser;
  }

  RGWUserAdminOpState() : user_id(RGW_USER_ANON_ID)
  {
    max_buckets = RGW_DEFAULT_MAX_BUCKETS;
    key_type = -1;
    perm_mask = RGW_PERM_NONE;
    suspended = 0;
    admin = 0;
    system = 0;
    exclusive = 0;
    fetch_stats = 0;
    op_mask = 0;

    existing_user = false;
    existing_key = false;
    existing_subuser = false;
    existing_email = false;
    subuser_specified = false;
    caps_specified = false;
    purge_keys = false;
    gen_secret = false;
    gen_access = false;
    gen_subuser = false;
    id_specified = false;
    key_specified = false;
    type_specified = false;
    key_type_setbycontext = false;
    purge_data = false;
    display_name_specified = false;
    user_email_specified = false;
    max_buckets_specified = false;
    perm_specified = false;
    op_mask_specified = false;
    suspension_op = false;
    system_specified = false;
    key_op = false;
    populated = false;
    initialized = false;
    key_params_checked = false;
    subuser_params_checked = false;
    user_params_checked = false;
    bucket_quota_specified = false;
    temp_url_key_specified = false;
    user_quota_specified = false;
    found_by_uid = false;
    found_by_email = false;
    found_by_key = false;
  }
};

class RGWUser;

class RGWAccessKeyPool
{
  RGWUser *user;

  std::map<std::string, int, ltstr_nocase> key_type_map;
  rgw_user user_id;
  RGWRados *store;

  map<std::string, RGWAccessKey> *swift_keys;
  map<std::string, RGWAccessKey> *access_keys;

  // we don't want to allow keys for the anonymous user or a null user
  bool keys_allowed;

private:
  int create_key(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);
  int generate_key(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);
  int modify_key(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);

  int check_key_owner(RGWUserAdminOpState& op_state);
  bool check_existing_key(RGWUserAdminOpState& op_state);
  int check_op(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);

  /* API Contract Fulfilment */
  int execute_add(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);
  int execute_remove(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);
  int remove_subuser_keys(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);

  int add(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);
  int remove(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);
public:
  explicit RGWAccessKeyPool(RGWUser* usr);
  ~RGWAccessKeyPool();

  int init(RGWUserAdminOpState& op_state);

  /* API Contracted Methods */
  int add(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);
  int remove(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);

  friend class RGWUser;
  friend class RGWSubUserPool;
};

class RGWSubUserPool
{
  RGWUser *user;

  rgw_user user_id;
  RGWRados *store;
  bool subusers_allowed;

  map<string, RGWSubUser> *subuser_map;

private:
  int check_op(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);

  /* API Contract Fulfillment */
  int execute_add(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);
  int execute_remove(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);
  int execute_modify(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);

  int add(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);
  int remove(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);
  int modify(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);
public:
  explicit RGWSubUserPool(RGWUser *user);
  ~RGWSubUserPool();

  bool exists(std::string subuser);
  int init(RGWUserAdminOpState& op_state);

  /* API contracted methods */
  int add(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);
  int remove(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);
  int modify(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);

  friend class RGWUser;
};

class RGWUserCapPool
{
  RGWUserCaps *caps;
  bool caps_allowed;
  RGWUser *user;

private:
  int add(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);
  int remove(RGWUserAdminOpState& op_state, std::string *err_msg, bool defer_save);

public:
  explicit RGWUserCapPool(RGWUser *user);
  ~RGWUserCapPool();

  int init(RGWUserAdminOpState& op_state);

  /* API contracted methods */
  int add(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);
  int remove(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);

  friend class RGWUser;
};

class RGWUser
{

private:
  RGWUserInfo old_info;
  RGWRados *store;

  rgw_user user_id;
  bool info_stored;

  void set_populated() { info_stored = true; }
  void clear_populated() { info_stored = false; }
  bool is_populated() { return info_stored; }

  int check_op(RGWUserAdminOpState&  req, std::string *err_msg);
  int update(RGWUserAdminOpState& op_state, std::string *err_msg);

  void clear_members();
  void init_default();

  /* API Contract Fulfillment */
  int execute_add(RGWUserAdminOpState& op_state, std::string *err_msg);
  int execute_remove(RGWUserAdminOpState& op_state, std::string *err_msg);
  int execute_modify(RGWUserAdminOpState& op_state, std::string *err_msg);

public:
  RGWUser();
  ~RGWUser();

  int init(RGWRados *storage, RGWUserAdminOpState& op_state);

  int init_storage(RGWRados *storage);
  int init(RGWUserAdminOpState& op_state);
  int init_members(RGWUserAdminOpState& op_state);

  RGWRados *get_store() { return store; }

  /* API Contracted Members */
  RGWUserCapPool caps;
  RGWAccessKeyPool keys;
  RGWSubUserPool subusers;

  /* API Contracted Methods */
  int add(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);
  int remove(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);

  /* remove an already populated RGWUser */
  int remove(std::string *err_msg = NULL);

  int modify(RGWUserAdminOpState& op_state, std::string *err_msg = NULL);

  /* retrieve info from an existing user in the RGW system */
  int info(RGWUserAdminOpState& op_state, RGWUserInfo& fetched_info, std::string *err_msg = NULL);

  /* info from an already populated RGWUser */
  int info (RGWUserInfo& fetched_info, std::string *err_msg = NULL);

  friend class RGWAccessKeyPool;
  friend class RGWSubUserPool;
  friend class RGWUserCapPool;
};

/* Wrapers for admin API functionality */

class RGWUserAdminOp_User
{
public:
  static int info(RGWRados *store,
                  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);

  static int create(RGWRados *store,
                  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);

  static int modify(RGWRados *store,
                  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);

  static int remove(RGWRados *store,
                  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);
};

class RGWUserAdminOp_Subuser
{
public:
  static int create(RGWRados *store,
                  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);

  static int modify(RGWRados *store,
                  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);

  static int remove(RGWRados *store,
                  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);
};

class RGWUserAdminOp_Key
{
public:
  static int create(RGWRados *store,
                  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);

  static int remove(RGWRados *store,
                  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);
};

class RGWUserAdminOp_Caps
{
public:
  static int add(RGWRados *store,
		  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);

  static int remove(RGWRados *store,
		  RGWUserAdminOpState& op_state, RGWFormatterFlusher& flusher);
};

class RGWMetadataManager;

extern void rgw_user_init(RGWRados *store);

#endif
