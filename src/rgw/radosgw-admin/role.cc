// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/role.h"

#include <fcntl.h>
#include <iostream>
#include <limits>
#include <string>
#include <unistd.h>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "include/buffer.h"
#include "rgw_iam_policy.h"
#include "rgw_role.h"
#include "rgw_sal.h"
#include "rgw_user_types.h"

using namespace rgw_admin;
using namespace std;

namespace {


static int read_input(const string& infile, bufferlist& bl)
{
  int fd = 0;
  if (infile.size()) {
    fd = open(infile.c_str(), O_RDONLY);
    if (fd < 0) {
      int err = -errno;
      cerr << "error reading input file " << infile << std::endl;
      return err;
    }
  }

  constexpr auto READ_CHUNK = 8196;
  int r;
  int err;

  do {
    char buf[READ_CHUNK];
    r = safe_read(fd, buf, READ_CHUNK);
    if (r < 0) {
      err = -errno;
      cerr << "error while reading input" << std::endl;
      goto out;
    }
    bl.append(buf, r);
  } while (r > 0);
  err = 0;

 out:
  if (infile.size()) {
    close(fd);
  }
  return err;
}


static void show_perm_policy(string perm_policy, ceph::Formatter* formatter)
{
  formatter->open_object_section("role");
  formatter->dump_string("Permission policy", perm_policy);
  formatter->close_section();
  formatter->flush(cout);
}

static void show_policy_names(const std::vector<string>& policy_names, ceph::Formatter* formatter)
{
  formatter->open_array_section("PolicyNames");
  for (const auto& it : policy_names) {
    formatter->dump_string("policyname", it);
  }
  formatter->close_section();
  formatter->flush(cout);
}


static void show_policy_arns(const boost::container::flat_set<std::string>& arns,
                             ceph::Formatter* formatter)
{
  formatter->open_array_section("AttachedPolicies");
  for (const auto& arn : arns) {
    formatter->dump_string("PolicyArn", arn);
  }
  formatter->close_section();
}


} // anonymous namespace

int rgw_admin_role(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   ceph::Formatter* formatter,
                   const rgw_admin_role_options& opts)
{
  auto& command = opts.command;
  auto& role_name = *opts.role_name;
  auto& tenant = *opts.tenant;
  auto& account_id = *opts.account_id;
  auto& path = *opts.path;
  auto& assume_role_doc = *opts.assume_role_doc;
  auto& perm_policy_doc = *opts.perm_policy_doc;
  auto& policy_name = *opts.policy_name;
  auto& policy_arn = *opts.policy_arn;
  auto& description = *opts.description;
  auto& path_prefix = *opts.path_prefix;
  auto& max_session_duration = *opts.max_session_duration;
  auto& marker = *opts.marker;
  auto& infile = *opts.infile;
  int max_entries = opts.max_entries;
  bool max_entries_specified = opts.max_entries_specified;
  int ret = 0;

  switch (command) {
  case OPT::ROLE_CREATE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (assume_role_doc.empty()) {
        cerr << "ERROR: assume role policy document is empty" << std::endl;
        return -EINVAL;
      }
      try {
        const rgw::IAM::Policy p(
	  driver->ctx(), nullptr, assume_role_doc,
	  driver->ctx()->_conf.get_val<bool>(
	    "rgw_policy_reject_invalid_principals"));
      } catch (rgw::IAM::PolicyParseException& e) {
        cerr << "failed to parse policy: " << e.what() << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id, path,
                                                                 assume_role_doc, description, max_session_duration);
      ret = role->create(dpp, "", null_yield);
      if (ret < 0) {
        return -ret;
      }
      encode_json("role", role->get_info(), formatter);
      formatter->flush(cout);
      return 0;
    }
  case OPT::ROLE_DELETE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: empty role name" << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      ret = role->delete_obj(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "role: " << role_name << " successfully deleted" << std::endl;
      return 0;
    }
  case OPT::ROLE_GET:
    {
      if (role_name.empty()) {
        cerr << "ERROR: empty role name" << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      ret = role->load_by_name(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      encode_json("role", role->get_info(), formatter);
      formatter->flush(cout);
      return 0;
    }
  case OPT::ROLE_TRUST_POLICY_MODIFY:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (assume_role_doc.empty()) {
        cerr << "ERROR: assume role policy document is empty" << std::endl;
        return -EINVAL;
      }

      try {
        const rgw::IAM::Policy p(driver->ctx(), nullptr, assume_role_doc,
				 driver->ctx()->_conf.get_val<bool>(
				   "rgw_policy_reject_invalid_principals"));
      } catch (rgw::IAM::PolicyParseException& e) {
        cerr << "failed to parse policy: " << e.what() << std::endl;
        return -EINVAL;
      }

      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      ret = role->load_by_name(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      role->update_trust_policy(assume_role_doc);
      constexpr bool exclusive = false;
      ret = role->store_info(dpp, exclusive, null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "Assume role policy document updated successfully for role: " << role_name << std::endl;
      return 0;
    }
  case OPT::ROLE_LIST:
    {
      rgw::sal::RoleList listing;
      listing.next_marker = marker;

      int32_t remaining = std::numeric_limits<int32_t>::max();
      if (max_entries_specified) {
        remaining = max_entries;
        formatter->open_object_section("result");
      }
      formatter->open_array_section("Roles");

      do {
        constexpr int32_t max_chunk = 100;
        int32_t count = std::min(max_chunk, remaining);

        // Copy the marker to a separate local variable to break the reference alias
        std::string current_marker = listing.next_marker;
        // Clear the roles list to prevent appending duplicates across loop iterations
        listing.roles.clear();
        listing.next_marker.clear();

        if (!account_id.empty()) {
          // list roles in the account
          ret = driver->list_account_roles(dpp, null_yield, account_id,
                                           path_prefix, current_marker,
                                           count, listing);
        } else {
          // list roles in the tenant
          ret = driver->list_roles(dpp, null_yield, tenant, path_prefix,
                                   current_marker, count, listing);
        }
        if (ret < 0) {
          return -ret;
        }
        for (const auto& info : listing.roles) {
          encode_json("member", info, formatter);
        }
        formatter->flush(cout);
        remaining -= listing.roles.size();
      } while (!listing.next_marker.empty() && remaining > 0);

      formatter->close_section(); // Roles

      if (max_entries_specified) {
        if (!listing.next_marker.empty()) {
          encode_json("next-marker", listing.next_marker, formatter);
        }
        formatter->close_section(); // result
      }
      formatter->flush(cout);
      return 0;
    }
  case OPT::ROLE_POLICY_PUT:
    {
      if (role_name.empty()) {
        cerr << "role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "policy name is empty" << std::endl;
        return -EINVAL;
      }

      if (perm_policy_doc.empty() && infile.empty()) {
        cerr << "permission policy document is empty" << std::endl;
        return -EINVAL;
      }

      if (!infile.empty()) {
        bufferlist bl;
        int ret = read_input(infile, bl);
        if (ret < 0) {
          cerr << "ERROR: failed to read input policy document: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        perm_policy_doc = bl.to_str();
      }
      try {
        const rgw::IAM::Policy p(driver->ctx(), nullptr, perm_policy_doc,
				 driver->ctx()->_conf.get_val<bool>(
				   "rgw_policy_reject_invalid_principals"));
      } catch (rgw::IAM::PolicyParseException& e) {
        cerr << "failed to parse perm policy: " << e.what() << std::endl;
        return -EINVAL;
      }

      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      ret = role->load_by_name(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      role->set_perm_policy(policy_name, perm_policy_doc);
      constexpr bool exclusive = false;
      ret = role->store_info(dpp, exclusive, null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "Permission policy attached successfully" << std::endl;
      return 0;
    }
  case OPT::ROLE_POLICY_LIST:
    {
      if (role_name.empty()) {
        cerr << "ERROR: Role name is empty" << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      ret = role->load_by_name(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      std::vector<string> policy_names = role->get_role_policy_names();
      show_policy_names(policy_names, formatter);
      return 0;
    }
  case OPT::ROLE_POLICY_GET:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "ERROR: policy name is empty" << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      int ret = role->load_by_name(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      string perm_policy;
      ret = role->get_role_policy(dpp, policy_name, perm_policy);
      if (ret < 0) {
        return -ret;
      }
      show_perm_policy(perm_policy, formatter);
      return 0;
    }
  case OPT::ROLE_POLICY_DELETE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "ERROR: policy name is empty" << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      ret = role->load_by_name(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      ret = role->delete_policy(dpp, policy_name);
      if (ret < 0) {
        return -ret;
      }
      constexpr bool exclusive = false;
      ret = role->store_info(dpp, exclusive, null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "Policy: " << policy_name << " successfully deleted for role: "
           << role_name << std::endl;
      return 0;
    }
  case OPT::ROLE_POLICY_ATTACH:
    {
      if (role_name.empty()) {
        cerr << "role name is empty" << std::endl;
        return EINVAL;
      }
      if (policy_arn.empty()) {
        cerr << "policy arn is empty" << std::endl;
        return EINVAL;
      }
      try {
        if (!rgw::IAM::get_managed_policy(driver->ctx(), policy_arn)) {
          cerr << "unrecognized policy arn " << policy_arn << std::endl;
          return ENOENT;
        }
      } catch (rgw::IAM::PolicyParseException& e) {
        cerr << "failed to parse managed policy: " << e.what() << std::endl;
        return EINVAL;
      }

      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      ret = role->load_by_id(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      if (role->get_info().account_id.empty()) {
        std::cerr << "Managed policies are only supported for account roles" << std::endl;
        return EINVAL;
      }

      auto &policies = role->get_info().managed_policies;
      const bool inserted = policies.arns.insert(policy_arn).second;
      if (!inserted) {
        cout << "That managed policy is already attached." << std::endl;
        return EEXIST;
      }
      constexpr bool exclusive = false;
      ret = role->store_info(dpp, exclusive, null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "Managed policy attached successfully" << std::endl;
      return 0;
    }
  case OPT::ROLE_POLICY_DETACH:
    {
      if (role_name.empty()) {
        cerr << "role name is empty" << std::endl;
        return EINVAL;
      }
      if (policy_arn.empty()) {
        cerr << "policy arn is empty" << std::endl;
        return EINVAL;
      }

      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      ret = role->load_by_id(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      // insert the policy arn. if it's already there, just return success
      auto &policies = role->get_info().managed_policies;
      auto i = policies.arns.find(policy_arn);
      if (i == policies.arns.end()) {
        cout << "That managed policy is not attached." << std::endl;
        return ENOENT;
      }
      policies.arns.erase(i);

      constexpr bool exclusive = false;
      ret = role->store_info(dpp, exclusive, null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "Managed policy detached successfully" << std::endl;
      return 0;
    }
  case OPT::ROLE_POLICY_LIST_ATTACHED:
    {
      if (role_name.empty()) {
        cerr << "ERROR: Role name is empty" << std::endl;
        return EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      ret = role->load_by_id(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      show_policy_arns(role->get_info().managed_policies.arns, formatter);
      formatter->flush(cout);
      return 0;
    }
  case OPT::ROLE_UPDATE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, account_id);
      ret = role->load_by_name(dpp, null_yield);
      if (ret < 0) {
        return -ret;
      }
      role->update_max_session_duration(max_session_duration);
      if (!role->validate_max_session_duration(dpp)) {
        ret = -EINVAL;
        return ret;
      }
      constexpr bool exclusive = false;
      ret = role->store_info(dpp, exclusive, null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "Max session duration updated successfully for role: " << role_name << std::endl;
      return 0;
    }

  default:
    return EINVAL;
  }
}
