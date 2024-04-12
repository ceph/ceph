// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <array>
#include <string>
#include <variant>

#include "common/errno.h"
#include "rgw_common.h"
#include "rgw_auth.h"
#include "rgw_quota.h"
#include "rgw_user.h"
#include "rgw_http_client.h"
#include "rgw_iam_managed_policy.h"
#include "rgw_keystone.h"
#include "rgw_sal.h"
#include "rgw_log.h"

#include "include/function2.hpp"
#include "include/str_list.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw {
namespace auth {

// match a principal by path/name[:subuser]
static bool match_principal(std::string_view path,
                            std::string_view name,
                            std::string_view subuser,
                            std::string_view expected)
{
  // leading / was already matched by ":user/" in parse_principal()
  if (!path.empty()) {
    path.remove_prefix(1);
  }

  // match user path
  if (!expected.starts_with(path)) {
    return false;
  }
  expected.remove_prefix(path.size());

  // match user by id
  if (!expected.starts_with(name)) {
    return false;
  }
  expected.remove_prefix(name.size());
  if (expected.empty()) { // exact match
    return true;
  }

  // try to match name:subuser
  if (!expected.starts_with(":")) {
    return false;
  }
  expected.remove_prefix(1);
  if (expected.empty()) {
    return false;
  }
  return (expected == "*" || expected == subuser);
}

static bool match_owner(const rgw_owner& owner, const rgw_user& uid,
                        const std::optional<RGWAccountInfo>& account)
{
  return std::visit(fu2::overload(
      [&uid] (const rgw_user& u) { return u == uid; },
      [&account] (const rgw_account_id& a) {
        return account && a == account->id;
      }), owner);
}

static bool match_account_or_tenant(const std::optional<RGWAccountInfo>& account,
                                    std::string_view tenant,
                                    std::string_view expected)
{
  return (account && account->id == expected)
      || (tenant == expected);
}

static void load_inline_policy(CephContext* cct, const bufferlist& bl,
                               const string* tenant,
                               std::vector<rgw::IAM::Policy>& policies)
{
  map<string, string> policy_map;
  using ceph::decode;
  decode(policy_map, bl);
  for (const auto& [name, policy] : policy_map) {
    policies.emplace_back(cct, tenant, policy, false);
  }
}

static void load_managed_policy(CephContext* cct, const bufferlist& bl,
                                std::vector<rgw::IAM::Policy>& policies)
{
  rgw::IAM::ManagedPolicies policy_set;
  using ceph::decode;
  decode(policy_set, bl);
  for (const auto& arn : policy_set.arns) {
    if (auto p = rgw::IAM::get_managed_policy(cct, arn); p) {
      policies.push_back(std::move(*p));
    }
  }
}

static int load_group_policies(const DoutPrefixProvider* dpp,
                               optional_yield y,
                               rgw::sal::Driver* driver,
                               const std::string* tenant,
                               std::string_view group_id,
                               std::vector<rgw::IAM::Policy>& policies)
{
  RGWGroupInfo info;
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;
  int r = driver->load_group_by_id(dpp, y, group_id, info, attrs, objv);
  if (r < 0) {
    return r;
  }

  CephContext* cct = dpp->get_cct();
  if (auto i = attrs.find(RGW_ATTR_IAM_POLICY); i != attrs.end()) {
    load_inline_policy(cct, i->second, tenant, policies);
  }
  if (auto i = attrs.find(RGW_ATTR_MANAGED_POLICY); i != attrs.end()) {
    load_managed_policy(cct, i->second, policies);
  }
  return 0;
}

int load_account_and_policies(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              sal::Driver* driver,
                              const RGWUserInfo& info,
                              const sal::Attrs& attrs,
                              std::optional<RGWAccountInfo>& account,
                              std::vector<IAM::Policy>& policies)
{
  if (!info.account_id.empty()) {
    account.emplace();
    rgw::sal::Attrs attrs; // ignored
    RGWObjVersionTracker objv; // ignored
    int r = driver->load_account_by_id(dpp, y, info.account_id,
                                       *account, attrs, objv);
    if (r < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to load account "
          << info.account_id << " for user " << info.user_id
          << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  // non-account identity policy is restricted to the current tenant
  const std::string* policy_tenant = info.account_id.empty()
      ? &info.user_id.tenant : nullptr;

  // load user policies from user attrs
  CephContext* cct = dpp->get_cct();
  if (auto bl = attrs.find(RGW_ATTR_USER_POLICY); bl != attrs.end()) {
    load_inline_policy(cct, bl->second, policy_tenant, policies);
  }
  if (auto bl = attrs.find(RGW_ATTR_MANAGED_POLICY); bl != attrs.end()) {
    load_managed_policy(cct, bl->second, policies);
  }

  // load each group and its policies
  for (const auto& id : info.group_ids) {
    int r = load_group_policies(dpp, y, driver, policy_tenant, id, policies);
    if (r == -ENOENT) {
      // in multisite, metadata sync may race to replicate the user before its
      // group. ignore ENOENT here so we don't reject all the user's requests
      // in the meantime
      ldpp_dout(dpp, 1) << "WARNING: skipping nonexistent group id " << id
          << " for user " << info.user_id << ": " << cpp_strerror(r) << dendl;
    } else if (r < 0) {
      ldpp_dout(dpp, 1) << "ERROR: failed to load group id " << id
          << " for user " << info.user_id << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  return 0;
}

static auto transform_old_authinfo(const RGWUserInfo& user,
                                   std::optional<RGWAccountInfo> account,
                                   std::vector<IAM::Policy> policies)
  -> std::unique_ptr<rgw::auth::Identity>
{
  /* This class is not intended for public use. Should be removed altogether
   * with this function after moving all our APIs to the new authentication
   * infrastructure. */
  class DummyIdentityApplier : public rgw::auth::IdentityApplier {
    /* For this particular case it's OK to use rgw_user structure to convey
     * the identity info as this was the policy for doing that before the
     * new auth. */
    const rgw_user id;
    const std::string display_name;
    const std::string path;
    const bool is_admin;
    const uint32_t type;
    const std::optional<RGWAccountInfo> account;
    const std::vector<IAM::Policy> policies;
  public:
    DummyIdentityApplier(const RGWUserInfo& user,
                         std::optional<RGWAccountInfo> account,
                         std::vector<IAM::Policy> policies)
      : id(user.user_id),
        display_name(user.display_name),
        path(user.path),
        is_admin(user.admin),
        type(user.type),
        account(std::move(account)),
        policies(std::move(policies))
    {}

    ACLOwner get_aclowner() const {
      ACLOwner owner;
      if (account) {
        owner.id = account->id;
        owner.display_name = account->name;
      } else {
        owner.id = id;
        owner.display_name = display_name;
      }
      return owner;
    }

    uint32_t get_perms_from_aclspec(const DoutPrefixProvider* dpp, const aclspec_t& aclspec) const override {
      return rgw_perms_from_aclspec_default_strategy(id.to_str(), aclspec, dpp);
    }

    bool is_admin_of(const rgw_owner& o) const override {
      return is_admin;
    }

    bool is_owner_of(const rgw_owner& o) const override {
      return match_owner(o, id, account);
    }

    bool is_identity(const Principal& p) const override {
      if (p.is_wildcard()) {
        return true;
      } else if (p.is_account()) {
        return match_account_or_tenant(account, id.tenant,
                                       p.get_account());
      } else if (p.is_user()) {
        std::string_view no_subuser;
        // account users can match both account- and tenant-based arns
        if (account && p.get_account() == account->id) {
          return match_principal(path, display_name, no_subuser, p.get_id());
        } else {
          return p.get_account() == id.tenant
              && match_principal(path, id.id, no_subuser, p.get_id());
        }
      }
      return false;
    }

    uint32_t get_perm_mask() const override {
      return RGW_PERM_FULL_CONTROL;
    }

    uint32_t get_identity_type() const override {
      return type;
    }

    string get_acct_name() const override {
      return {};
    }

    string get_subuser() const override {
      return {};
    }
    const std::string& get_tenant() const override {
      return id.tenant;
    }
    const std::optional<RGWAccountInfo>& get_account() const override {
      return account;
    }

    void write_ops_log_entry(rgw_log_entry& entry) const override {
      if (account) {
        entry.account_id = account->id;
      }
    }

    void to_str(std::ostream& out) const override {
      out << "RGWDummyIdentityApplier(auth_id=" << id
          << ", is_admin=" << is_admin << ")";
    }

    void load_acct_info(const DoutPrefixProvider* dpp,
                        RGWUserInfo& user_info) const override {
      // noop, this user info was passed in on construction
    }

    void modify_request_state(const DoutPrefixProvider* dpp, req_state* s) const {
      // copy our identity policies into req_state
      s->iam_identity_policies.insert(s->iam_identity_policies.end(),
                                      policies.begin(), policies.end());
    }
  };

  return std::make_unique<DummyIdentityApplier>(
      user, std::move(account), std::move(policies));
}

auto transform_old_authinfo(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            sal::Driver* driver,
                            sal::User* user)
  -> tl::expected<std::unique_ptr<Identity>, int>
{
  const RGWUserInfo& info = user->get_info();
  const sal::Attrs& attrs = user->get_attrs();

  std::optional<RGWAccountInfo> account;
  std::vector<IAM::Policy> policies;

  int r = load_account_and_policies(dpp, y, driver, info, attrs,
                                    account, policies);
  if (r < 0) {
    return tl::unexpected(r);
  }

  return transform_old_authinfo(info, std::move(account), std::move(policies));
}

} /* namespace auth */
} /* namespace rgw */


uint32_t rgw_perms_from_aclspec_default_strategy(
  const std::string& uid,
  const rgw::auth::Identity::aclspec_t& aclspec,
  const DoutPrefixProvider *dpp)
{
  ldpp_dout(dpp, 5) << "Searching permissions for uid=" << uid <<  dendl;

  const auto iter = aclspec.find(uid);
  if (std::end(aclspec) != iter) {
    ldpp_dout(dpp, 5) << "Found permission: " << iter->second << dendl;
    return iter->second;
  }

  ldpp_dout(dpp, 5) << "Permissions for user not found" << dendl;
  return 0;
}


static inline const std::string make_spec_item(const std::string& tenant,
                                               const std::string& id)
{
  return tenant + ":" + id;
}


static inline std::pair<bool, rgw::auth::Engine::result_t>
strategy_handle_rejected(rgw::auth::Engine::result_t&& engine_result,
                         const rgw::auth::Strategy::Control policy,
                         rgw::auth::Engine::result_t&& strategy_result)
{
  using Control = rgw::auth::Strategy::Control;
  switch (policy) {
    case Control::REQUISITE:
      /* Don't try next. */
      return std::make_pair(false, std::move(engine_result));

    case Control::SUFFICIENT:
      /* Don't try next. */
      return std::make_pair(false, std::move(engine_result));

    case Control::FALLBACK:
      /* Don't try next. */
      return std::make_pair(false, std::move(engine_result));

    default:
      /* Huh, memory corruption? */
      ceph_abort();
  }
}

static inline std::pair<bool, rgw::auth::Engine::result_t>
strategy_handle_denied(rgw::auth::Engine::result_t&& engine_result,
                       const rgw::auth::Strategy::Control policy,
                       rgw::auth::Engine::result_t&& strategy_result)
{
  using Control = rgw::auth::Strategy::Control;
  switch (policy) {
    case Control::REQUISITE:
      /* Don't try next. */
      return std::make_pair(false, std::move(engine_result));

    case Control::SUFFICIENT:
      /* Just try next. */
      return std::make_pair(true, std::move(engine_result));

    case Control::FALLBACK:
      return std::make_pair(true, std::move(strategy_result));

    default:
      /* Huh, memory corruption? */
      ceph_abort();
  }
}

static inline std::pair<bool, rgw::auth::Engine::result_t>
strategy_handle_granted(rgw::auth::Engine::result_t&& engine_result,
                        const rgw::auth::Strategy::Control policy,
                        rgw::auth::Engine::result_t&& strategy_result)
{
  using Control = rgw::auth::Strategy::Control;
  switch (policy) {
    case Control::REQUISITE:
      /* Try next. */
      return std::make_pair(true, std::move(engine_result));

    case Control::SUFFICIENT:
      /* Don't try next. */
      return std::make_pair(false, std::move(engine_result));

    case Control::FALLBACK:
      /* Don't try next. */
      return std::make_pair(false, std::move(engine_result));

    default:
      /* Huh, memory corruption? */
      ceph_abort();
  }
}

rgw::auth::Engine::result_t
rgw::auth::Strategy::authenticate(const DoutPrefixProvider* dpp, const req_state* const s, optional_yield y) const
{
  result_t strategy_result = result_t::deny();

  for (const stack_item_t& kv : auth_stack) {
    const rgw::auth::Engine& engine = kv.first;
    const auto& policy = kv.second;

    ldpp_dout(dpp, 20) << get_name() << ": trying " << engine.get_name() << dendl;

    result_t engine_result = result_t::deny();
    try {
      engine_result = engine.authenticate(dpp, s, y);
    } catch (const int err) {
      engine_result = result_t::deny(err);
    }

    bool try_next = true;
    switch (engine_result.get_status()) {
      case result_t::Status::REJECTED: {
        ldpp_dout(dpp, 20) << engine.get_name() << " rejected with reason="
                 << engine_result.get_reason() << dendl;

        std::tie(try_next, strategy_result) = \
          strategy_handle_rejected(std::move(engine_result), policy,
                                   std::move(strategy_result));
        break;
      }
      case result_t::Status::DENIED: {
        ldpp_dout(dpp, 20) << engine.get_name() << " denied with reason="
                 << engine_result.get_reason() << dendl;

        std::tie(try_next, strategy_result) = \
          strategy_handle_denied(std::move(engine_result), policy,
                                 std::move(strategy_result));
        break;
      }
      case result_t::Status::GRANTED: {
        ldpp_dout(dpp, 20) << engine.get_name() << " granted access" << dendl;

        std::tie(try_next, strategy_result) = \
          strategy_handle_granted(std::move(engine_result), policy,
                                  std::move(strategy_result));
        break;
      }
      default: {
        ceph_abort();
      }
    }

    if (! try_next) {
      break;
    }
  }

  return strategy_result;
}

int
rgw::auth::Strategy::apply(const DoutPrefixProvider *dpp, const rgw::auth::Strategy& auth_strategy,
                           req_state* const s, optional_yield y) noexcept
{
  try {
    auto result = auth_strategy.authenticate(dpp, s, y);
    if (result.get_status() != decltype(result)::Status::GRANTED) {
      /* Access denied is acknowledged by returning a std::unique_ptr with
       * nullptr inside. */
      ldpp_dout(dpp, 5) << "Failed the auth strategy, reason="
                       << result.get_reason() << dendl;
      // Special handling for expired pre-signed URL
      if (result.get_reason() == ERR_PRESIGNED_URL_EXPIRED) {
        result = result_t::deny(-EPERM);
        set_req_state_err(s, -EPERM, "The pre-signed URL has expired");
      }
      // Special handling for disabled presigned URL
      if (result.get_reason() == ERR_PRESIGNED_URL_DISABLED) {
        result = result_t::deny(-EPERM);
        set_req_state_err(s, -EPERM, "Presigned URLs are disabled by admin");
      }
      return result.get_reason();
    }

    try {
      rgw::auth::IdentityApplier::aplptr_t applier = result.get_applier();
      rgw::auth::Completer::cmplptr_t completer = result.get_completer();

      /* Account used by a given RGWOp is decoupled from identity employed
       * in the authorization phase (RGWOp::verify_permissions). */
      applier->load_acct_info(dpp, s->user->get_info());
      s->perm_mask = applier->get_perm_mask();

      /* This is the single place where we pass req_state as a pointer
       * to non-const and thus its modification is allowed. In the time
       * of writing only RGWTempURLEngine needed that feature. */
      applier->modify_request_state(dpp, s);
      if (completer) {
        completer->modify_request_state(dpp, s);
      }

      s->auth.identity = std::move(applier);
      s->auth.completer = std::move(completer);

      /* Populate the owner info. */
      s->owner = s->auth.identity->get_aclowner();

      return 0;
    } catch (const int err) {
      ldpp_dout(dpp, 5) << "applier throwed err=" << err << dendl;
      return err;
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 5) << "applier throwed unexpected err: " << e.what()
                        << dendl;
      return -EPERM;
    }
  } catch (const int err) {
    ldpp_dout(dpp, 5) << "auth engine throwed err=" << err << dendl;
    return err;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 5) << "auth engine throwed unexpected err: " << e.what()
                      << dendl;
  }

  /* We never should be here. */
  return -EPERM;
}

void
rgw::auth::Strategy::add_engine(const Control ctrl_flag,
                                const Engine& engine) noexcept
{
  auth_stack.push_back(std::make_pair(std::cref(engine), ctrl_flag));
}

ACLOwner rgw::auth::WebIdentityApplier::get_aclowner() const
{
  ACLOwner owner;
  if (account) {
    owner.id = account->id;
    owner.display_name = account->name;
  } else {
    owner.id = rgw_user{role_tenant, sub, "oidc"};
    owner.display_name = user_name;
  }
  return owner;
}

bool rgw::auth::WebIdentityApplier::is_owner_of(const rgw_owner& o) const
{
  return match_owner(o, rgw_user{role_tenant, sub, "oidc"}, account);
}

void rgw::auth::WebIdentityApplier::to_str(std::ostream& out) const
{
  out << "rgw::auth::WebIdentityApplier(sub =" << sub
      << ", user_name=" << user_name
      << ", role_id=" << role_id
      << ", provider_id =" << iss << ")";
}

void rgw::auth::WebIdentityApplier::write_ops_log_entry(rgw_log_entry& entry) const
{
  if (account) {
    entry.account_id = account->id;
  }
  entry.role_id = role_id;
}

string rgw::auth::WebIdentityApplier::get_idp_url() const
{
  string idp_url = this->iss;
  idp_url = url_remove_prefix(idp_url);
  return idp_url;
}

void rgw::auth::WebIdentityApplier::create_account(const DoutPrefixProvider* dpp,
                                              const rgw_user& acct_user,
                                              const string& display_name,
                                              RGWUserInfo& user_info) const      /* out */
{
  std::unique_ptr<rgw::sal::User> user = driver->get_user(acct_user);
  user->get_info().display_name = display_name;
  user->get_info().type = TYPE_WEB;
  user->get_info().max_buckets =
    cct->_conf.get_val<int64_t>("rgw_user_max_buckets");
  rgw_apply_default_bucket_quota(user->get_info().quota.bucket_quota, cct->_conf);
  rgw_apply_default_user_quota(user->get_info().quota.user_quota, cct->_conf);

  int ret = user->store_user(dpp, null_yield, true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to store new user info: user="
                  << user << " ret=" << ret << dendl;
    throw ret;
  }
  user_info = user->get_info();
}

void rgw::auth::WebIdentityApplier::load_acct_info(const DoutPrefixProvider* dpp, RGWUserInfo& user_info) const {
  rgw_user federated_user;
  federated_user.id = this->sub;
  federated_user.tenant = role_tenant;
  federated_user.ns = "oidc";

  if (account) {
    // we don't need shadow users for account roles because bucket ownership,
    // quota, and stats are tracked by the account instead of the user
    user_info.user_id = std::move(federated_user);
    user_info.display_name = user_name;
    user_info.type = TYPE_WEB;
    return;
  }

  std::unique_ptr<rgw::sal::User> user = driver->get_user(federated_user);

  //Check in oidc namespace
  if (user->load_user(dpp, null_yield) >= 0) {
    /* Succeeded. */
    user_info = user->get_info();
    return;
  }

  user->clear_ns();
  //Check for old users which wouldn't have been created in oidc namespace
  if (user->load_user(dpp, null_yield) >= 0) {
    /* Succeeded. */
    user_info = user->get_info();
    return;
  }

  //Check if user_id.buckets already exists, may have been from the time, when shadow users didnt exist
  RGWStorageStats stats;
  ceph::real_time last_synced;
  ceph::real_time last_updated;
  int ret = driver->load_stats(dpp, null_yield, federated_user, stats,
                               last_synced, last_updated);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "ERROR: reading stats for the user returned error " << ret << dendl;
    return;
  }
  if (ret == -ENOENT) { /* in case of ENOENT, which means user doesnt have buckets */
    //In this case user will be created in oidc namespace
    ldpp_dout(dpp, 5) << "NOTICE: incoming user has no buckets " << federated_user << dendl;
    federated_user.ns = "oidc";
  } else {
    //User already has buckets associated, hence wont be created in oidc namespace.
    ldpp_dout(dpp, 5) << "NOTICE: incoming user already has buckets associated " << federated_user << ", won't be created in oidc namespace"<< dendl;
    federated_user.ns = "";
  }

  ldpp_dout(dpp, 0) << "NOTICE: couldn't map oidc federated user " << federated_user << dendl;
  create_account(dpp, federated_user, this->user_name, user_info);
}

void rgw::auth::WebIdentityApplier::modify_request_state(const DoutPrefixProvider *dpp, req_state* s) const
{
  s->info.args.append("sub", this->sub);
  s->info.args.append("aud", this->aud);
  s->info.args.append("provider_id", this->iss);
  s->info.args.append("client_id", this->client_id);

  string condition;
  string idp_url = get_idp_url();
  for (auto& claim : token_claims) {
    if (claim.first == "aud") {
      condition.clear();
      condition = idp_url + ":app_id";
      s->env.emplace(condition, claim.second);
    }
    condition.clear();
    condition = idp_url + ":" + claim.first;
    s->env.emplace(condition, claim.second);
  }

  if (principal_tags) {
    constexpr size_t KEY_SIZE = 128, VAL_SIZE = 256;
    std::set<std::pair<string, string>> p_tags = principal_tags.get();
    for (auto& it : p_tags) {
      string key = it.first;
      string val = it.second;
      if (key.find("aws:") == 0 || val.find("aws:") == 0) {
        ldpp_dout(dpp, 0) << "ERROR: Tag/Value can't start with aws:, hence skipping it" << dendl;
        continue;
      }
      if (key.size() > KEY_SIZE || val.size() > VAL_SIZE)  {
        ldpp_dout(dpp, 0) << "ERROR: Invalid tag/value size, hence skipping it" << dendl;
        continue;
      }
      std::string p_key = "aws:PrincipalTag/";
      p_key.append(key);
      s->principal_tags.emplace_back(std::make_pair(p_key, val));
      ldpp_dout(dpp, 10) << "Principal Tag Key: " << p_key << " Value: " << val << dendl;

      std::string e_key = "aws:RequestTag/";
      e_key.append(key);
      s->env.emplace(e_key, val);
      ldpp_dout(dpp, 10) << "RGW Env Tag Key: " << e_key << " Value: " << val << dendl;

      s->env.emplace("aws:TagKeys", key);
        ldpp_dout(dpp, 10) << "aws:TagKeys: " << key << dendl;

      if (s->principal_tags.size() == 50) {
        ldpp_dout(dpp, 0) << "ERROR: Number of tag/value pairs exceeding 50, hence skipping the rest" << dendl;
        break;
      }
    }
  }

  if (role_tags) {
    for (auto& it : role_tags.get()) {
      std::string p_key = "aws:PrincipalTag/";
      p_key.append(it.first);
      s->principal_tags.emplace_back(std::make_pair(p_key, it.second));
      ldpp_dout(dpp, 10) << "Principal Tag Key: " << p_key << " Value: " << it.second << dendl;

      std::string e_key = "iam:ResourceTag/";
      e_key.append(it.first);
      s->env.emplace(e_key, it.second);
      ldpp_dout(dpp, 10) << "RGW Env Tag Key: " << e_key << " Value: " << it.second << dendl;
    }
  }
}

bool rgw::auth::WebIdentityApplier::is_identity(const Principal& p) const
{
  return p.is_oidc_provider()
      && p.get_idp_url() == get_idp_url();
}

const std::string rgw::auth::RemoteApplier::AuthInfo::NO_SUBUSER;
const std::string rgw::auth::RemoteApplier::AuthInfo::NO_ACCESS_KEY;

/* rgw::auth::RemoteAuthApplier */
ACLOwner rgw::auth::RemoteApplier::get_aclowner() const
{
  ACLOwner owner;
  if (account) {
    owner.id = account->id;
    owner.display_name = account->name;
  } else {
    owner.id = info.acct_user;
    owner.display_name = info.acct_name;
  }
  return owner;
}

uint32_t rgw::auth::RemoteApplier::get_perms_from_aclspec(const DoutPrefixProvider* dpp, const aclspec_t& aclspec) const
{
  uint32_t perm = 0;

  /* For backward compatibility with ACLOwner. */
  perm |= rgw_perms_from_aclspec_default_strategy(info.acct_user.to_str(),
                                                  aclspec, dpp);

  /* We also need to cover cases where rgw_keystone_implicit_tenants
   * was enabled. */
  if (info.acct_user.tenant.empty()) {
    const rgw_user tenanted_acct_user(info.acct_user.id, info.acct_user.id);

    perm |= rgw_perms_from_aclspec_default_strategy(tenanted_acct_user.to_str(),
                                                    aclspec, dpp);
  }

  /* Now it's a time for invoking additional strategy that was supplied by
   * a specific auth engine. */
  if (extra_acl_strategy) {
    perm |= extra_acl_strategy(aclspec);
  }

  ldpp_dout(dpp, 20) << "from ACL got perm=" << perm << dendl;
  return perm;
}

bool rgw::auth::RemoteApplier::is_admin_of(const rgw_owner& o) const
{
  return info.is_admin;
}

bool rgw::auth::RemoteApplier::is_owner_of(const rgw_owner& o) const
{
  auto* uid = std::get_if<rgw_user>(&o);
  if (!uid) {
    return false;
  }

  if (info.acct_user.tenant.empty()) {
    const rgw_user tenanted_acct_user(info.acct_user.id, info.acct_user.id);

    if (tenanted_acct_user == *uid) {
      return true;
    }
  }

  return info.acct_user == *uid;
}

bool rgw::auth::RemoteApplier::is_identity(const Principal& p) const {
  // We also need to cover cases where rgw_keystone_implicit_tenants
  // was enabled.
  std::string_view tenant = info.acct_user.tenant.empty() ?
                            info.acct_user.id :
                            info.acct_user.tenant;
  if (p.is_wildcard()) {
    return true;
  } else if (p.is_account()) {
    return p.get_account() == tenant;
  } else if (p.is_user()) {
    return p.get_id() == info.acct_user.id
        && p.get_account() == tenant;
  }
  return false;
}

void rgw::auth::RemoteApplier::to_str(std::ostream& out) const
{
  out << "rgw::auth::RemoteApplier(acct_user=" << info.acct_user
      << ", acct_name=" << info.acct_name
      << ", perm_mask=" << info.perm_mask
      << ", is_admin=" << info.is_admin << ")";
}

void rgw::auth::ImplicitTenants::recompute_value(const ConfigProxy& c)
{
  std::string s = c.get_val<std::string>("rgw_keystone_implicit_tenants");
  int v = 0;
  if (boost::iequals(s, "both")
    || boost::iequals(s, "true")
    || boost::iequals(s, "1")) {
    v = IMPLICIT_TENANTS_S3|IMPLICIT_TENANTS_SWIFT;
  } else if (boost::iequals(s, "0")
    || boost::iequals(s, "none")
    || boost::iequals(s, "false")) {
    v = 0;
  } else if (boost::iequals(s, "s3")) {
    v = IMPLICIT_TENANTS_S3;
  } else if (boost::iequals(s, "swift")) {
    v = IMPLICIT_TENANTS_SWIFT;
  } else {  /* "" (and anything else) */
    v = IMPLICIT_TENANTS_BAD;
    // assert(0);
  }
  saved = v;
}

const char **rgw::auth::ImplicitTenants::get_tracked_conf_keys() const
{
  static const char *keys[] = {
    "rgw_keystone_implicit_tenants",
  nullptr };
  return keys;
}

void rgw::auth::ImplicitTenants::handle_conf_change(const ConfigProxy& c,
	const std::set <std::string> &changed)
{
  if (changed.count("rgw_keystone_implicit_tenants")) {
    recompute_value(c);
  }
}

void rgw::auth::RemoteApplier::create_account(const DoutPrefixProvider* dpp,
                                              const rgw_user& acct_user,
                                              bool implicit_tenant,
                                              RGWUserInfo& user_info) const      /* out */
{
  rgw_user new_acct_user = acct_user;

  /* An upper layer may enforce creating new accounts within their own
   * tenants. */
  if (new_acct_user.tenant.empty() && implicit_tenant) {
    new_acct_user.tenant = new_acct_user.id;
  }

  std::unique_ptr<rgw::sal::User> user = driver->get_user(new_acct_user);
  user->get_info().display_name = info.acct_name;
  if (info.acct_type) {
    //ldap/keystone for s3 users
    user->get_info().type = info.acct_type;
  }
  user->get_info().max_buckets =
    cct->_conf.get_val<int64_t>("rgw_user_max_buckets");
  rgw_apply_default_bucket_quota(user->get_info().quota.bucket_quota, cct->_conf);
  rgw_apply_default_user_quota(user->get_info().quota.user_quota, cct->_conf);
  user_info = user->get_info();

  int ret = user->store_user(dpp, null_yield, true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to store new user info: user="
                  << user << " ret=" << ret << dendl;
    throw ret;
  }
}

void rgw::auth::RemoteApplier::write_ops_log_entry(rgw_log_entry& entry) const
{
  entry.access_key_id = info.access_key_id;
  entry.subuser = info.subuser;
  if (account) {
    entry.account_id = account->id;
  }
}

/* TODO(rzarzynski): we need to handle display_name changes. */
void rgw::auth::RemoteApplier::load_acct_info(const DoutPrefixProvider* dpp, RGWUserInfo& user_info) const      /* out */
{
  /* It's supposed that RGWRemoteAuthApplier tries to load account info
   * that belongs to the authenticated identity. Another policy may be
   * applied by using a RGWThirdPartyAccountAuthApplier decorator. */
  const rgw_user& acct_user = info.acct_user;
  auto implicit_value = implicit_tenant_context.get_value();
  bool implicit_tenant = implicit_value.implicit_tenants_for_(implicit_tenant_bit);
  bool split_mode = implicit_value.is_split_mode();
  std::unique_ptr<rgw::sal::User> user;

  /* Normally, empty "tenant" field of acct_user means the authenticated
   * identity has the legacy, global tenant. However, due to inclusion
   * of multi-tenancy, we got some special compatibility kludge for remote
   * backends like Keystone.
   * If the global tenant is the requested one, we try the same tenant as
   * the user name first. If that RGWUserInfo exists, we use it. This way,
   * migrated OpenStack users can get their namespaced containers and nobody's
   * the wiser.
   * If that fails, we look up in the requested (possibly empty) tenant.
   * If that fails too, we create the account within the global or separated
   * namespace depending on rgw_keystone_implicit_tenants.
   * For compatibility with previous versions of ceph, it is possible
   * to enable implicit_tenants for only s3 or only swift.
   * in this mode ("split_mode"), we must constrain the id lookups to
   * only use the identifier space that would be used if the id were
   * to be created. */

  if (split_mode && !implicit_tenant)
	;	/* suppress lookup for id used by "other" protocol */
  else if (acct_user.tenant.empty()) {
    const rgw_user tenanted_uid(acct_user.id, acct_user.id);
    user = driver->get_user(tenanted_uid);

    if (user->load_user(dpp, null_yield) >= 0) {
      /* Succeeded. */
      (void) load_account_and_policies(dpp, null_yield, driver, user->get_info(),
                                       user->get_attrs(), account, policies);

      user_info = std::move(user->get_info());
      return;
    }
  }

  user = driver->get_user(acct_user);

  if (split_mode && implicit_tenant)
	;	/* suppress lookup for id used by "other" protocol */
  else if (user->load_user(dpp, null_yield) >= 0) {
    /* Succeeded. */
    (void) load_account_and_policies(dpp, null_yield, driver, user->get_info(),
                                     user->get_attrs(), account, policies);

    user_info = std::move(user->get_info());
    return;
  }

  ldpp_dout(dpp, 0) << "NOTICE: couldn't map swift user " << acct_user << dendl;
  create_account(dpp, acct_user, implicit_tenant, user_info);

  /* Succeeded if we are here (create_account() hasn't throwed). */
}

void rgw::auth::RemoteApplier::modify_request_state(const DoutPrefixProvider* dpp, req_state* s) const
{
  // copy our identity policies into req_state
  s->iam_identity_policies.insert(s->iam_identity_policies.end(),
                                  policies.begin(), policies.end());
}

/* rgw::auth::LocalApplier */
/* static declaration */
const std::string rgw::auth::LocalApplier::NO_SUBUSER;
const std::string rgw::auth::LocalApplier::NO_ACCESS_KEY;

ACLOwner rgw::auth::LocalApplier::get_aclowner() const
{
  ACLOwner owner;
  if (account) {
    owner.id = account->id;
    owner.display_name = account->name;
  } else {
    owner.id = user_info.user_id;
    owner.display_name = user_info.display_name;
  }
  return owner;
}

uint32_t rgw::auth::LocalApplier::get_perms_from_aclspec(const DoutPrefixProvider* dpp, const aclspec_t& aclspec) const
{
  // match acl grants to the specific user id
  uint32_t mask = rgw_perms_from_aclspec_default_strategy(
      user_info.user_id.to_str(), aclspec, dpp);

  if (account) {
    // account users also match acl grants to the account id. in aws, grantees
    // ONLY refer to accounts. but we continue to match user grants to preserve
    // access when moving legacy users into new accounts
    mask |= rgw_perms_from_aclspec_default_strategy(account->id, aclspec, dpp);
  }

  return mask;
}

bool rgw::auth::LocalApplier::is_admin_of(const rgw_owner& o) const
{
  return user_info.admin || user_info.system;
}

bool rgw::auth::LocalApplier::is_owner_of(const rgw_owner& o) const
{
  return match_owner(o, user_info.user_id, account);
}

bool rgw::auth::LocalApplier::is_identity(const Principal& p) const {
  if (p.is_wildcard()) {
    return true;
  } else if (p.is_account()) {
    return match_account_or_tenant(account, user_info.user_id.tenant,
                                   p.get_account());
  } else if (p.is_user()) {
    // account users can match both account- and tenant-based arns
    if (account && p.get_account() == account->id) {
      return match_principal(user_info.path, user_info.display_name,
                             subuser, p.get_id());
    } else {
      return p.get_account() == user_info.user_id.tenant
          && match_principal(user_info.path, user_info.user_id.id,
                             subuser, p.get_id());
    }
  }
  return false;
}

void rgw::auth::LocalApplier::to_str(std::ostream& out) const {
  out << "rgw::auth::LocalApplier(acct_user=" << user_info.user_id
      << ", acct_name=" << user_info.display_name
      << ", subuser=" << subuser
      << ", perm_mask=" << get_perm_mask()
      << ", is_admin=" << static_cast<bool>(user_info.admin) << ")";
}

uint32_t rgw::auth::LocalApplier::get_perm_mask(const std::string& subuser_name,
                                                const RGWUserInfo &uinfo) const
{
  if (! subuser_name.empty() && subuser_name != NO_SUBUSER) {
    const auto iter = uinfo.subusers.find(subuser_name);

    if (iter != std::end(uinfo.subusers)) {
      return iter->second.perm_mask;
    } else {
      /* Subuser specified but not found. */
      return RGW_PERM_NONE;
    }
  } else {
    /* Due to backward compatibility. */
    return RGW_PERM_FULL_CONTROL;
  }
}

void rgw::auth::LocalApplier::load_acct_info(const DoutPrefixProvider* dpp, RGWUserInfo& user_info) const /* out */
{
  /* Load the account that belongs to the authenticated identity. An extra call
   * to RADOS may be safely skipped in this case. */
  user_info = this->user_info;
}

void rgw::auth::LocalApplier::modify_request_state(const DoutPrefixProvider* dpp, req_state* s) const
{
  // copy our identity policies into req_state
  s->iam_identity_policies.insert(s->iam_identity_policies.end(),
                                  policies.begin(), policies.end());
}

void rgw::auth::LocalApplier::write_ops_log_entry(rgw_log_entry& entry) const
{
  entry.access_key_id = access_key_id;
  entry.subuser = subuser;
  if (account) {
    entry.account_id = account->id;
  }
}

ACLOwner rgw::auth::RoleApplier::get_aclowner() const
{
  ACLOwner owner;
  if (role.account) {
    owner.id = role.account->id;
    owner.display_name = role.account->name;
  } else {
    owner.id = token_attrs.user_id;
    owner.display_name = role.name;
  }
  return owner;
}

bool rgw::auth::RoleApplier::is_owner_of(const rgw_owner& o) const
{
  return match_owner(o, token_attrs.user_id, role.account);
}

void rgw::auth::RoleApplier::to_str(std::ostream& out) const {
  out << "rgw::auth::RoleApplier(role name =" << role.name;
  for (auto& policy: role.inline_policies) {
    out << ", role policy =" << policy;
  }
  for (std::string_view arn : role.managed_policies) {
    if (auto p = arn.find('/'); p != arn.npos) {
      out << ", managed policy =" << arn.substr(p + 1);
    } else {
      out << ", managed policy =" << arn;
    }
  }
  out << ", token policy =" << token_attrs.token_policy;
  out << ")";
}

bool rgw::auth::RoleApplier::is_identity(const Principal& p) const {
  if (p.is_wildcard()) {
    return true;
  } else if (p.is_account()) {
    return match_account_or_tenant(role.account, role.tenant,
                                   p.get_account());
  } else if (p.is_role()) {
    std::string_view no_subuser;
    // account roles can match both account- and tenant-based arns
    return match_account_or_tenant(role.account, role.tenant, p.get_account())
        && match_principal(role.path, role.name, no_subuser, p.get_id());
  } else if (p.is_assumed_role()) {
    string role_session = role.name + "/" + token_attrs.role_session_name; //role/role-session
    return p.get_account() == role.tenant
        && p.get_role_session() == role_session;
  } else {
    string oidc_id;
    if (token_attrs.user_id.ns.empty()) {
      oidc_id = token_attrs.user_id.id;
    } else {
      oidc_id = token_attrs.user_id.ns + "$" + token_attrs.user_id.id;
    }
    return p.get_id() == oidc_id
        && p.get_account() == token_attrs.user_id.tenant;
  }
  return false;
}

void rgw::auth::RoleApplier::load_acct_info(const DoutPrefixProvider* dpp, RGWUserInfo& user_info) const /* out */
{
  /* Load the user id */
  user_info.user_id = this->token_attrs.user_id;
}

void rgw::auth::RoleApplier::write_ops_log_entry(rgw_log_entry& entry) const
{
  if (role.account) {
    entry.account_id = role.account->id;
  }
  entry.role_id = role.id;
}

void rgw::auth::RoleApplier::modify_request_state(const DoutPrefixProvider *dpp, req_state* s) const
{
  // non-account identity policy is restricted to the current tenant
  const std::string* policy_tenant = role.account ? nullptr : &role.tenant;

  for (const auto& policy : role.inline_policies) {
    try {
      const rgw::IAM::Policy p(s->cct, policy_tenant, policy, false);
      s->iam_identity_policies.push_back(std::move(p));
    } catch (rgw::IAM::PolicyParseException& e) {
      //Control shouldn't reach here as the policy has already been
      //verified earlier
      ldpp_dout(dpp, 20) << "failed to parse role policy: " << e.what() << dendl;
    }
  }
  for (const auto& arn : role.managed_policies) {
    try {
      if (auto p = rgw::IAM::get_managed_policy(s->cct, arn); p) {
        s->iam_identity_policies.push_back(std::move(*p));
      }
    } catch (rgw::IAM::PolicyParseException& e) {
      //Control shouldn't reach here as the policy has already been
      //verified earlier
      ldpp_dout(dpp, 20) << "failed to parse role policy: " << e.what() << dendl;
    }
  }

  if (!this->token_attrs.token_policy.empty()) {
    try {
      string policy = this->token_attrs.token_policy;
      const rgw::IAM::Policy p(s->cct, policy_tenant, policy, false);
      s->session_policies.push_back(std::move(p));
    } catch (rgw::IAM::PolicyParseException& e) {
      //Control shouldn't reach here as the policy has already been
      //verified earlier
      ldpp_dout(dpp, 20) << "failed to parse token policy: " << e.what() << dendl;
    }
  }

  string condition = "aws:userid";
  string value = role.id + ":" + token_attrs.role_session_name;
  s->env.emplace(condition, value);

  s->env.emplace("aws:TokenIssueTime", token_attrs.token_issued_at);

  for (auto& m : token_attrs.principal_tags) {
    s->env.emplace(m.first, m.second);
    ldpp_dout(dpp, 10) << "Principal Tag Key: " << m.first << " Value: " << m.second << dendl;
    std::size_t pos = m.first.find('/');
    string key = m.first.substr(pos + 1);
    s->env.emplace("aws:TagKeys", key);
    ldpp_dout(dpp, 10) << "aws:TagKeys: " << key << dendl;
  }

  s->token_claims.emplace_back("sts");
  s->token_claims.emplace_back("role_name:" + role.tenant + "$" + role.name);
  s->token_claims.emplace_back("role_session:" + token_attrs.role_session_name);
  for (auto& it : token_attrs.token_claims) {
    s->token_claims.emplace_back(it);
  }
}

rgw::auth::Engine::result_t
rgw::auth::AnonymousEngine::authenticate(const DoutPrefixProvider* dpp, const req_state* const s, optional_yield y) const
{
  if (! is_applicable(s)) {
    return result_t::deny(-EPERM);
  } else {
    RGWUserInfo user_info;
    rgw_get_anon_user(user_info);

    auto apl = \
      apl_factory->create_apl_local(cct, s, user_info, std::nullopt, {},
                                    rgw::auth::LocalApplier::NO_SUBUSER,
                                    std::nullopt, rgw::auth::LocalApplier::NO_ACCESS_KEY);
    return result_t::grant(std::move(apl));
  }
}
