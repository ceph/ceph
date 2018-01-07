//
// Created by cache-nez on 08.01.18.
//

#include "rgw_admin_opt_policy.h"

void show_perm_policy(const string& perm_policy, Formatter* formatter)
{
  formatter->open_object_section("role");
  formatter->dump_string("Permission policy", perm_policy);
  formatter->close_section();
  formatter->flush(cout);
}

void show_policy_names(const std::vector<string>& policy_names, Formatter* formatter)
{
  formatter->open_array_section("PolicyNames");
  for (const auto& it : policy_names) {
    formatter->dump_string("policyname", it);
  }
  formatter->close_section();
  formatter->flush(cout);
}

void show_role_info(RGWRole& role, Formatter* formatter)
{
  formatter->open_object_section("role");
  role.dump(formatter);
  formatter->close_section();
  formatter->flush(cout);
}

void show_roles_info(const vector<RGWRole>& roles, Formatter* formatter)
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