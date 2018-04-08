#include "rgw_admin_opt_user.h"

#include "rgw_bucket.h"
#include "rgw_admin_argument_parsing.h"

int handle_opt_user_create(const std::string& subuser, RGWUserAdminOpState& user_op, RGWUser& user)
{
  if (!user_op.has_existing_user()) {
    user_op.set_generate_key(); // generate a new key by default
  }
  std::string err_msg;
  int ret = user.add(user_op, &err_msg);
  if (ret < 0) {
    cerr << "could not create user: " << err_msg << std::endl;
    if (ret == -ERR_INVALID_TENANT_NAME)
      ret = -EINVAL;

    return -ret;
  }
  if (!subuser.empty()) {
    ret = user.subusers.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create subuser: " << err_msg << std::endl;
      return -ret;
    }
  }
  return 0;
}

int handle_opt_user_stats(bool sync_stats, const std::string& bucket_name, const std::string& tenant,
                          rgw_user& user_id, RGWRados *store, Formatter *formatter)
{
  if (user_id.empty()) {
    cerr << "ERROR: uid not specified" << std::endl;
    return EINVAL;
  }
  if (sync_stats) {
    if (!bucket_name.empty()) {
      int ret = rgw_bucket_sync_user_stats(store, tenant, bucket_name);
      if (ret < 0) {
        cerr << "ERROR: could not sync bucket stats: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      int ret = rgw_user_sync_all_stats(store, user_id);
      if (ret < 0) {
        cerr << "ERROR: failed to sync user stats: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  cls_user_header header;
  string user_str = user_id.to_str();
  int ret = store->cls_user_get_header(user_str, &header);
  if (ret < 0) {
    if (ret == -ENOENT) { /* in case of ENOENT */
      cerr << "User has not been initialized or user does not exist" << std::endl;
    } else {
      cerr << "ERROR: can't read user: " << cpp_strerror(ret) << std::endl;
    }
    return -ret;
  }

  encode_json("header", header, formatter);
  formatter->flush(cout);
  return 0;
}

int RgwAdminUserCommandsHandler::parse_command_and_parameters() {
  const rgw_admin_params::commandline_parameter ACCESS = {"access",
                                                          "Set access permissions for sub-user, should be one of read, write, readwrite, full"};
  const rgw_admin_params::commandline_parameter ADMIN = {"admin", "Set the admin flag on the user"};
  const rgw_admin_params::commandline_parameter CAPS = {"caps",
                                                        "List of caps (e.g., \"usage=read, write; user=read\")"};
  const rgw_admin_params::commandline_parameter DISPLAY_NAME = {"display-name", "User's display name"};
  const rgw_admin_params::commandline_parameter KEY_TYPE = {"key-type", "Key type, options are: swift, s3"};
  const rgw_admin_params::commandline_parameter MAX_BUCKETS = {"max-buckets", "Max number of buckets for a user"};
  const rgw_admin_params::commandline_parameter OP_MASK = {"op-mask", ""};
  const rgw_admin_params::commandline_parameter PURGE_KEYS = {"purge-keys",
                                                              "When specified, subuser removal will also purge all the subuser keys"};
  const rgw_admin_params::commandline_parameter SUBUSER = {"subuser", "Subuser name"};
  const rgw_admin_params::commandline_parameter SYNC_STATS = {"sync-stats",
                                                              "Update user stats with current stats reported by user's buckets indexes"};
  const rgw_admin_params::commandline_parameter SYSTEM = {"system", "Set the system flag on the user"};
  const rgw_admin_params::commandline_parameter TEMP_URL_KEY = {"temp-url-key", ""};
  const rgw_admin_params::commandline_parameter TEMP_URL_KEY_2 = {"temp-url-key-2", ""};
  const rgw_admin_params::commandline_parameter USER_EMAIL = {"email", "User's email address"};
  std::string access, access_key, secret_key, caps, display_name, key_type_str, op_mask_str, user_id_str, user_email;
  std::vector<std::string> tmp_url_keys(2);
  bool gen_access_key = false;
  bool gen_secret_key = false;
  bool purge_data = false;
  bool purge_keys = false;
  bool set_admin, set_system;
  int max_buckets;
  boost::program_options::options_description desc{"User options"};
  desc.add_options()
      (ACCESS.name, boost::program_options::value(&access), ACCESS.description)
      (rgw_admin_params::ACCESS_KEY.name, boost::program_options::value(&access_key),
       rgw_admin_params::ACCESS_KEY.description)
      (rgw_admin_params::GEN_ACCESS_KEY.name, boost::program_options::value(&gen_access_key),
       rgw_admin_params::GEN_ACCESS_KEY.description)
      (rgw_admin_params::SECRET_KEY.name, boost::program_options::value(&secret_key),
       rgw_admin_params::SECRET_KEY.description)
      (rgw_admin_params::GEN_SECRET_KEY.name, boost::program_options::value(&gen_secret_key),
       rgw_admin_params::GEN_SECRET_KEY.description)
      (ADMIN.name, boost::program_options::value(&set_admin), ADMIN.description)
      (rgw_admin_params::BUCKET_NAME.name, boost::program_options::value(&bucket_name),
       rgw_admin_params::BUCKET_NAME.description)
      (CAPS.name, boost::program_options::value(&caps), CAPS.description)
      (DISPLAY_NAME.name, boost::program_options::value(&display_name), DISPLAY_NAME.description)
      (KEY_TYPE.name, boost::program_options::value(&key_type_str), KEY_TYPE.description)
      (MAX_BUCKETS.name, boost::program_options::value(&max_buckets), MAX_BUCKETS.description)
      (OP_MASK.name, boost::program_options::value(&op_mask_str), OP_MASK.description)
      (rgw_admin_params::PURGE_DATA.name, boost::program_options::value(&purge_data),
       rgw_admin_params::PURGE_DATA.description)
      (PURGE_KEYS.name, boost::program_options::value(&purge_keys), PURGE_KEYS.description)
      (SUBUSER.name, boost::program_options::value(&subuser), SUBUSER.description)
      (SYNC_STATS.name, boost::program_options::value(&sync_stats), SYNC_STATS.description)
      (SYSTEM.name, boost::program_options::value(&set_system), SYSTEM.description)
      (TEMP_URL_KEY.name, boost::program_options::value(&tmp_url_keys[0]), TEMP_URL_KEY.description)
      (TEMP_URL_KEY_2.name, boost::program_options::value(&tmp_url_keys[0]), TEMP_URL_KEY_2.description)
      (rgw_admin_params::TENANT.name, boost::program_options::value(&tenant), rgw_admin_params::TENANT.description)
      (rgw_admin_params::USER_ID.name, boost::program_options::value(&user_id_str),
       rgw_admin_params::USER_ID.description)
      (USER_EMAIL.name, boost::program_options::value(&user_email), USER_EMAIL.description);
  boost::program_options::variables_map var_map;

  int ret = parse_command(desc, var_map);
  if (ret > 0) {
    return ret;
  }

  user_op.set_purge_data(purge_data);
  if (gen_access_key) {
    // not generating the key as user_op will take care of it
    user_op.set_generate_key();
  }
  if (gen_secret_key) {
    user_op.set_gen_secret();
  }
  if (purge_keys) {
    user_op.set_purge_keys();
  }
  if (var_map.count(ADMIN.name)) {
    user_op.set_admin(set_admin);
  }
  if (var_map.count(SYSTEM.name)) {
    user_op.set_system(set_system);
  }

  if (var_map.count(ACCESS.name)) {
    user_op.set_perm(rgw_str_to_perm(access.c_str()));
  }
  if (var_map.count(rgw_admin_params::ACCESS_KEY.name)) {
    user_op.set_access_key(access_key);
  }
  if (var_map.count(rgw_admin_params::SECRET_KEY.name)) {
    user_op.set_secret_key(secret_key);
  }
  if (var_map.count(CAPS.name)) {
    user_op.set_caps(caps);
  }
  if (var_map.count(DISPLAY_NAME.name)) {
    user_op.set_display_name(display_name);
  }
  if (var_map.count(KEY_TYPE.name)) {
    if (key_type_str == "swift") {
      user_op.set_key_type(KEY_TYPE_SWIFT);
    } else if (key_type_str == "s3") {
      user_op.set_key_type(KEY_TYPE_S3);
    } else {
      cerr << "bad key type: " << key_type_str << std::endl;
      return EINVAL;
    }
  }
  if (var_map.count(MAX_BUCKETS.name)) {
    user_op.set_max_buckets(max_buckets);
  }
  if (var_map.count(OP_MASK.name)) {
    uint32_t op_mask;
    int ret = rgw_parse_op_type_list(op_mask_str, &op_mask);
    if (ret < 0) {
      cerr << "failed to parse op_mask: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    user_op.set_op_mask(op_mask);
  }
  if (var_map.count(SUBUSER.name)) {
    user_op.set_subuser(subuser);
  }
  if (var_map.count(TEMP_URL_KEY.name)) {
    user_op.set_temp_url_key(tmp_url_keys[0], 0);
  }
  if (var_map.count(TEMP_URL_KEY_2.name)) {
    user_op.set_temp_url_key(tmp_url_keys[1], 1);
  }
  if (var_map.count(rgw_admin_params::USER_ID.name)) {
    user_id.from_str(user_id_str);
    user_op.set_user_id(user_id);
  }
  if (var_map.count(USER_EMAIL.name)) {
    user_op.set_user_email(user_email);
  }
  return 0;
}

void RgwAdminUserCommandsHandler::show_user_info() {
  encode_json("user_info", info, formatter);
  formatter->flush(cout);
  cout << std::endl;
}