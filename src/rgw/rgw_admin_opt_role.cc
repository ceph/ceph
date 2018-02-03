#include "rgw_admin_opt_role.h"

#include "rgw_rest_conn.h"
#include "rgw_realm_watcher.h"

#include "common/ceph_json.h"
#include "rgw_role.h"
#include "rgw_admin_common.h"

static void show_perm_policy(const std::string& perm_policy, Formatter* formatter)
{
  formatter->open_object_section("role");
  formatter->dump_string("Permission policy", perm_policy);
  formatter->close_section();
  formatter->flush(cout);
}

static void show_policy_names(const std::vector<std::string>& policy_names, Formatter* formatter)
{
  formatter->open_array_section("PolicyNames");
  for (const auto& it : policy_names) {
    formatter->dump_string("policyname", it);
  }
  formatter->close_section();
  formatter->flush(cout);
}

static void show_role_info(RGWRole& role, Formatter* formatter)
{
  formatter->open_object_section("role");
  role.dump(formatter);
  formatter->close_section();
  formatter->flush(cout);
}

static void show_roles_info(const vector<RGWRole>& roles, Formatter* formatter)
{
  formatter->open_array_section("Roles");
  for (const auto& it : roles) {
    formatter->open_object_section("role");
    it.dump(formatter);
    formatter->close_section();
  }
  formatter->close_section();
  formatter->flush(cout);
}

int handle_opt_role_create(const std::string& role_name, const std::string& assume_role_doc, const std::string& path,
                           const std::string& tenant, CephContext *context, RGWRados *store, Formatter *formatter)
{
  if (role_name.empty()) {
    cerr << "ERROR: role name is empty" << std::endl;
    return -EINVAL;
  }

  if (assume_role_doc.empty()) {
    cerr << "ERROR: assume role policy document is empty" << std::endl;
    return -EINVAL;
  }
  /* The following two calls will be replaced by read_decode_json or something
     similar when the code for AWS Policies is in places */
  bufferlist bl;
  int ret = read_input(assume_role_doc, bl);
  if (ret < 0) {
    cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    cout << "ERROR: failed to parse JSON: " << assume_role_doc << std::endl;
    return -EINVAL;
  }
  std::string trust_policy = bl.to_str();
  RGWRole role(context, store, role_name, path, trust_policy, tenant);
  ret = role.create(true);
  if (ret < 0) {
    return -ret;
  }
  show_role_info(role, formatter);
  return 0;
}

int handle_opt_role_delete(const std::string& role_name, const std::string& tenant, CephContext *context, RGWRados *store)
{
  if (role_name.empty()) {
    cerr << "ERROR: empty role name" << std::endl;
    return -EINVAL;
  }
  RGWRole role(context, store, role_name, tenant);
  int ret = role.delete_obj();
  if (ret < 0) {
    return -ret;
  }
  cout << "role: " << role_name << " successfully deleted" << std::endl;
  return 0;
}

int handle_opt_role_get(const std::string& role_name, const std::string& tenant, CephContext *context,
                        RGWRados *store, Formatter *formatter)
{
  if (role_name.empty()) {
    cerr << "ERROR: empty role name" << std::endl;
    return -EINVAL;
  }
  RGWRole role(context, store, role_name, tenant);
  int ret = role.get();
  if (ret < 0) {
    return -ret;
  }
  show_role_info(role, formatter);
  return 0;
}

int handle_opt_role_modify(const std::string& role_name, const std::string& assume_role_doc, const std::string& tenant,
                           CephContext *context, RGWRados *store)
{
  if (role_name.empty()) {
    cerr << "ERROR: role name is empty" << std::endl;
    return -EINVAL;
  }

  if (assume_role_doc.empty()) {
    cerr << "ERROR: assume role policy document is empty" << std::endl;
    return -EINVAL;
  }

  /* The following two calls will be replaced by read_decode_json or something
     similar when the code for AWS Policies is in place */
  bufferlist bl;
  int ret = read_input(assume_role_doc, bl);
  if (ret < 0) {
    cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    cout << "ERROR: failed to parse JSON: " << assume_role_doc << std::endl;
    return -EINVAL;
  }
  std::string trust_policy = bl.to_str();
  RGWRole role(context, store, role_name, tenant);
  ret = role.get();
  if (ret < 0) {
    return -ret;
  }
  role.update_trust_policy(trust_policy);
  ret = role.update();
  if (ret < 0) {
    return -ret;
  }
  cout << "Assume role policy document updated successfully for role: " << role_name << std::endl;
  return 0;
}

int handle_opt_role_list(const std::string& path_prefix, const std::string& tenant, CephContext *context,
                         RGWRados *store, Formatter *formatter)
{
  vector<RGWRole> result;
  int ret = RGWRole::get_roles_by_path_prefix(store, context, path_prefix, tenant, result);
  if (ret < 0) {
    return -ret;
  }
  show_roles_info(result, formatter);
  return 0;
}

int handle_opt_role_policy_put(const std::string& role_name, const std::string& policy_name, const std::string& perm_policy_doc,
                               const std::string& tenant, CephContext *context, RGWRados *store)
{
  if (role_name.empty()) {
    cerr << "role name is empty" << std::endl;
    return -EINVAL;
  }

  if (policy_name.empty()) {
    cerr << "policy name is empty" << std::endl;
    return -EINVAL;
  }

  if (perm_policy_doc.empty()) {
    cerr << "permission policy document is empty" << std::endl;
    return -EINVAL;
  }

  /* The following two calls will be replaced by read_decode_json or something
     similar, when code for AWS Policies is in place.*/
  bufferlist bl;
  int ret = read_input(perm_policy_doc, bl);
  if (ret < 0) {
    cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    cout << "ERROR: failed to parse JSON: " << std::endl;
    return -EINVAL;
  }
  std::string perm_policy;
  perm_policy = bl.c_str();

  RGWRole role(context, store, role_name, tenant);
  ret = role.get();
  if (ret < 0) {
    return -ret;
  }
  role.set_perm_policy(policy_name, perm_policy);
  ret = role.update();
  if (ret < 0) {
    return -ret;
  }
  cout << "Permission policy attached successfully" << std::endl;
  return 0;
}

int handle_opt_role_policy_list(const std::string& role_name, const std::string& tenant, CephContext *context,
                                RGWRados *store, Formatter *formatter)
{
  if (role_name.empty()) {
    cerr << "ERROR: Role name is empty" << std::endl;
    return -EINVAL;
  }
  RGWRole role(context, store, role_name, tenant);
  int ret = role.get();
  if (ret < 0) {
    return -ret;
  }
  std::vector<std::string> policy_names = role.get_role_policy_names();
  show_policy_names(policy_names, formatter);
  return 0;
}

int handle_opt_role_policy_get(const std::string& role_name, const std::string& policy_name, const std::string& tenant,
                               CephContext *context, RGWRados *store, Formatter *formatter)
{
  if (role_name.empty()) {
    cerr << "ERROR: role name is empty" << std::endl;
    return -EINVAL;
  }

  if (policy_name.empty()) {
    cerr << "ERROR: policy name is empty" << std::endl;
    return -EINVAL;
  }
  RGWRole role(context, store, role_name, tenant);
  int ret = role.get();
  if (ret < 0) {
    return -ret;
  }
  std::string perm_policy;
  ret = role.get_role_policy(policy_name, perm_policy);
  if (ret < 0) {
    return -ret;
  }
  show_perm_policy(perm_policy, formatter);
  return 0;
}

int handle_opt_role_policy_delete(const std::string& role_name, const std::string& policy_name, const std::string& tenant,
                                  CephContext *context, RGWRados *store)
{
  if (role_name.empty()) {
    cerr << "ERROR: role name is empty" << std::endl;
    return -EINVAL;
  }

  if (policy_name.empty()) {
    cerr << "ERROR: policy name is empty" << std::endl;
    return -EINVAL;
  }
  RGWRole role(context, store, role_name, tenant);
  int ret = role.get();
  if (ret < 0) {
    return -ret;
  }
  ret = role.delete_policy(policy_name);
  if (ret < 0) {
    return -ret;
  }
  ret = role.update();
  if (ret < 0) {
    return -ret;
  }
  cout << "Policy: " << policy_name << " successfully deleted for role: "
       << role_name << std::endl;
  return 0;
}