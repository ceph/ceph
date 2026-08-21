// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_keystone_scope.h"
#include "rgw_keystone.h"
#include "common/ceph_context.h"
#include "common/Formatter.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::keystone {

void ScopeInfo::dump(ceph::Formatter *f) const
{
  f->open_object_section("keystone_scope");

  // Project
  f->open_object_section("project");
  f->dump_string("id", project.id);
  f->dump_string("name", project.name);
  f->open_object_section("domain");
  f->dump_string("id", project.domain.id);
  f->dump_string("name", project.domain.name);
  f->close_section(); // domain
  f->close_section(); // project

  // User (optional based on config)
  if (user.has_value()) {
    f->open_object_section("user");
    f->dump_string("id", user->id);
    f->dump_string("name", user->name);
    f->open_object_section("domain");
    f->dump_string("id", user->domain.id);
    f->dump_string("name", user->domain.name);
    f->close_section(); // domain
    f->close_section(); // user
  }

  // Roles (may be empty based on config)
  if (!roles.empty()) {
    f->open_array_section("roles");
    for (const auto& role : roles) {
      f->dump_string("role", role);
    }
    f->close_section(); // roles
  }

  // Application credential (optional, present only for app cred auth)
  if (app_cred.has_value()) {
    f->open_object_section("application_credential");
    f->dump_string("id", app_cred->id);
    f->dump_string("name", app_cred->name);
    f->dump_bool("restricted", app_cred->restricted);
    f->close_section(); // application_credential
  }

  f->close_section(); // keystone_scope
}

std::optional<ScopeInfo> build_scope_info(
    CephContext* cct,
    const TokenEnvelope& token)
{
  // Check if scope logging is enabled
  if (!cct->_conf->rgw_keystone_scope_enabled) {
    return std::nullopt;
  }

  ScopeInfo scope;
  bool include_names = cct->_conf->rgw_keystone_scope_include_user;

  // Project/tenant scope - IDs always included, names only if include_user=true
  scope.project.id = token.get_project_id();
  scope.project.domain.id = token.project.domain.id;
  if (include_names) {
    scope.project.name = token.get_project_name();
    scope.project.domain.name = token.project.domain.name;
  }

  // User identity (controlled by include_user flag - both field and names)
  if (include_names) {
    ScopeInfo::user_t user;
    user.id = token.get_user_id();
    user.name = token.get_user_name();
    user.domain.id = token.user.domain.id;
    user.domain.name = token.user.domain.name;
    scope.user = std::move(user);
  }

  // Roles (controlled by include_roles flag)
  if (cct->_conf->rgw_keystone_scope_include_roles) {
    for (const auto& role : token.roles) {
      scope.roles.push_back(role.name);
    }
  }

  // Application credential (if present in token) - ID always, name only if include_user=true
  if (token.app_cred.has_value()) {
    ScopeInfo::app_cred_t app_cred;
    app_cred.id = token.app_cred->id;
    if (include_names) {
      app_cred.name = token.app_cred->name;
    }
    app_cred.restricted = token.app_cred->restricted;
    scope.app_cred = std::move(app_cred);
  }

  return scope;
}

} // namespace rgw::keystone
