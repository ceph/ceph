// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <exception>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "include/buffer.h"

#include "common/ceph_argparse.h"
#include "common/common_init.h"

#include "global/global_init.h"

#include "rgw/rgw_auth.h"
#include "rgw/rgw_iam_policy.h"

using namespace std::literals;

namespace buffer = ceph::buffer;

using rgw::auth::Principal;
using rgw::auth::Identity;

class FakeIdentity : public Identity {
  const Principal id;
public:

  explicit FakeIdentity(Principal&& id) : id(std::move(id)) {}

  ACLOwner get_aclowner() const override {
    ceph_abort();
    return {};
  }

  uint32_t get_perms_from_aclspec(const DoutPrefixProvider* dpp, const aclspec_t& aclspec) const override {
    ceph_abort();
    return 0;
  };

  bool is_admin() const override {
    ceph_abort();
    return false;
  }

  bool is_owner_of(const rgw_owner& owner) const override {
    ceph_abort();
    return false;
  }

  bool is_root() const override {
    ceph_abort();
    return false;
  }

  virtual uint32_t get_perm_mask() const override {
    ceph_abort();
    return 0;
  }

  std::string get_acct_name() const override {
    ceph_abort();
    return {};
  }

  std::string get_subuser() const override {
    ceph_abort();
    return {};
  }

  const std::string& get_tenant() const override {
    ceph_abort();
    static std::string empty;
    return empty;
  }

  const std::optional<RGWAccountInfo>& get_account() const override {
    ceph_abort();
    static std::optional<RGWAccountInfo> empty;
    return empty;
  }

  void to_str(std::ostream& out) const override {
    out << id;
  }

  bool is_identity(const Principal& p) const override {
    return id.is_wildcard() || p.is_wildcard() || p == id;
  }

  uint32_t get_identity_type() const override {
    return TYPE_RGW;
  }

  std::optional<rgw::ARN> get_caller_identity() const override {
    return std::nullopt;
  }
};

void
evaluate(CephContext* cct, const std::string* tenant,
         const std::string& fname, std::istream& in,
         const std::unordered_multimap<std::string, std::string>& environment,
         boost::optional<const rgw::auth::Identity&> ida, uint64_t action,
         boost::optional<const rgw::ARN&> resource)
{
  buffer::list bl;
  bl.append(in);
  try {
    auto p = rgw::IAM::Policy(
      cct, tenant, bl.to_str(),
      cct->_conf.get_val<bool>("rgw_policy_reject_invalid_principals"));
    auto effect = p.eval(std::cout, environment, ida, action, resource);
    std::cout << effect << std::endl;
  } catch (const rgw::IAM::PolicyParseException& e) {
    std::cerr << fname << ": " << e.what() << std::endl;
    throw;
  } catch (const std::exception& e) {
    std::cerr << fname << ": caught exception: " << e.what() << std::endl;
    throw;
  }
}

int helpful_exit(std::string_view cmdname)
{
  std::cerr << cmdname << " -h for usage" << std::endl;
  return 1;
}

void usage(std::string_view cmdname)
{
  std::cout << "usage: " << cmdname << " [options...] ACTION POLICY" << std::endl;
  std::cout << "options:" << std::endl;
  std::cout <<
    "  -t | --tenant=TENANT\ttenant owning the resource the policy governs\n";
  std::cout <<
    "  -e | --environment=KEY=VALUE\tPair to set in the environment\n";
  std::cout << "  -I | --identity=IDENTITY\tIdentity to test against\n";
  std::cout << "  -R | --resource=ARN\tResource to test access to\n\n";
  std::cout << "  ACTION\tAction to test against, e.g. s3:GetObject\n";
  std::cout << "  POLICY\tFilename of the policy or - for standard input\n";
  std::cout.flush();
}

// This has an uncaught exception. Even if the exception is caught, the program
// would need to be terminated, so the warning is simply suppressed.
// coverity[root_function:SUPPRESS]
int main(int argc, const char* argv[])
{
  std::string_view cmdname = argv[0];
  std::optional<std::string> tenant;
  std::unordered_multimap<std::string, std::string> environment;
  boost::optional<FakeIdentity> identity_;
  boost::optional<const rgw::auth::Identity&> identity;
  uint64_t action = 0;
  boost::optional<rgw::ARN> resource_;
  boost::optional<const rgw::ARN&> resource;

  auto args = argv_to_vec(argc, argv);
  if (ceph_argparse_need_usage(args)) {
    usage(cmdname);
    return 0;
  }

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DAEMON_ACTIONS |
			 CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(cct.get());
  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--tenant", "-t",
				     (char*)nullptr)) {
      tenant = std::move(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--environment", "-e",
				     (char*)nullptr)) {
      auto equal = val.find('=');
      if (equal == val.npos || equal == 0) {
        return helpful_exit(cmdname);
      }
      std::string k{val.data(), equal};
      std::string v{val.data() + equal + 1};
      environment.insert({std::move(k), std::move(v)});
    } else if (ceph_argparse_witharg(args, i, &val, "--resource", "-R",
				     (char*)nullptr)) {
      resource_ = rgw::ARN::parse(val);
      if (!resource_) {
        return helpful_exit(cmdname);
      }
      resource = *resource_;
    } else if (ceph_argparse_witharg(args, i, &val, "--identity", "-I",
				     (char*)nullptr)) {
      std::string errmsg;
      auto principal = rgw::IAM::parse_principal(std::move(val), &errmsg);
      if (!principal) {
        std::cerr << errmsg << std::endl;
        return helpful_exit(cmdname);
      }
      identity_.emplace(std::move(*principal));
      identity = *identity_;
    } else {
      ++i;
    }
  }

  if (args.size() != 2) {
    return helpful_exit(cmdname);
  }

  if (auto act = rgw::IAM::parse_action(args[0]); act) {
    action = *act;
  } else {
    std::cerr << args[0] << " is not a valid action." << std::endl;
    return helpful_exit(cmdname);
  }

  try {
    if (args[1] == "-"s) {
      evaluate(cct.get(), tenant ? &(*tenant) : nullptr, "(stdin)", std::cin,
               environment, identity, action, resource);
    } else {
      std::ifstream in;
      in.open(args[1], std::ifstream::in);
      if (!in.is_open()) {
        std::cerr << "Can't read " << args[1] << std::endl;
        return 1;
      }
      evaluate(cct.get(), tenant ? &(*tenant) : nullptr, args[1], in, environment,
               identity, action, resource);
    }
  } catch (const std::exception&) {
    return 1;
  }

  return 0;
}
