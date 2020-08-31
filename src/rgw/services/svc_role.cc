#include "svc_role.h"

const std::string role_name_oid_prefix = "role_names.";
const std::string role_oid_prefix = "roles.";
const std::string role_path_oid_prefix = "role_paths.";
const std::string role_arn_prefix = "arn:aws:iam::";

std::string RGWSI_Role::get_role_meta_key(const std::string& role_id)
{
  return role_oid_prefix + role_id;
}

std::string RGWSI_Role::get_role_name_meta_key(const std::string& role_name, const std::string& tenant)
{
  return tenant + role_name_oid_prefix + role_name;
}

std::string RGWSI_Role::get_role_path_meta_key(const std::string& path, const std::string& role_id, const std::string& tenant)
{
  return tenant + role_path_oid_prefix + path + role_oid_prefix + role_id;
}
