#include "svc_role.h"

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
