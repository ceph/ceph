// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <array>

#include "rgw_common.h"
#include "rgw_auth.h"
#include "rgw_user.h"
#include "rgw_http_client.h"
#include "rgw_keystone.h"

#include "include/str_list.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw


namespace rgw {
namespace auth {

std::unique_ptr<rgw::auth::Identity>
transform_old_authinfo(const req_state* const s)
{
  /* This class is not intended for public use. Should be removed altogether
   * with this function after moving all our APIs to the new authentication
   * infrastructure. */
  class DummyIdentityApplier : public rgw::auth::Identity {
    CephContext* const cct;

    /* For this particular case it's OK to use rgw_user structure to convey
     * the identity info as this was the policy for doing that before the
     * new auth. */
    const rgw_user id;
    const int perm_mask;
    const bool is_admin;
  public:
    DummyIdentityApplier(CephContext* const cct,
                         const rgw_user& auth_id,
                         const int perm_mask,
                         const bool is_admin)
      : cct(cct),
        id(auth_id),
        perm_mask(perm_mask),
        is_admin(is_admin) {
    }

    uint32_t get_perms_from_aclspec(const aclspec_t& aclspec) const override {
      return rgw_perms_from_aclspec_default_strategy(id, aclspec);
    }

    bool is_admin_of(const rgw_user& acct_id) const override {
      return is_admin;
    }

    bool is_owner_of(const rgw_user& acct_id) const override {
      return id == acct_id;
    }

    uint32_t get_perm_mask() const override {
      return perm_mask;
    }

    void to_str(std::ostream& out) const override {
      out << "RGWDummyIdentityApplier(auth_id=" << id
          << ", perm_mask=" << perm_mask
          << ", is_admin=" << is_admin << ")";
    }
  };

  return std::unique_ptr<rgw::auth::Identity>(
        new DummyIdentityApplier(s->cct,
                                 s->user->user_id,
                                 s->perm_mask,
  /* System user has admin permissions by default - it's supposed to pass
   * through any security check. */
                                 s->system_request));
}

} /* namespace auth */
} /* namespace rgw */


uint32_t rgw_perms_from_aclspec_default_strategy(
  const rgw_user& uid,
  const rgw::auth::Identity::aclspec_t& aclspec)
{
  dout(5) << "Searching permissions for uid=" << uid <<  dendl;

  const auto iter = aclspec.find(uid.to_str());
  if (std::end(aclspec) != iter) {
    dout(5) << "Found permission: " << iter->second << dendl;
    return iter->second;
  }

  dout(5) << "Permissions for user not found" << dendl;
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
      return std::make_pair(false, std::move(strategy_result));

    default:
      /* Huh, memory corruption? */
      abort();
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
      abort();
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
      abort();
  }
}

rgw::auth::Engine::result_t
rgw::auth::Strategy::authenticate(const req_state* const s) const
{
  result_t strategy_result = result_t::deny();

  for (const stack_item_t& kv : auth_stack) {
    const rgw::auth::Engine& engine = kv.first;
    const auto& policy = kv.second;

    dout(20) << get_name() << ": trying " << engine.get_name() << dendl;

    result_t engine_result = result_t::deny();
    try {
      engine_result = engine.authenticate(s);
    } catch (const int err) {
      engine_result = result_t::deny(err);
    }

    bool try_next = true;
    switch (engine_result.get_status()) {
      case result_t::Status::REJECTED: {
        dout(20) << engine.get_name() << " rejected with reason="
                 << engine_result.get_reason() << dendl;

        std::tie(try_next, strategy_result) = \
          strategy_handle_rejected(std::move(engine_result), policy,
                                   std::move(strategy_result));
        break;
      }
      case result_t::Status::DENIED: {
        dout(20) << engine.get_name() << " denied with reason="
                 << engine_result.get_reason() << dendl;

        std::tie(try_next, strategy_result) = \
          strategy_handle_denied(std::move(engine_result), policy,
                                 std::move(strategy_result));
        break;
      }
      case result_t::Status::GRANTED: {
        dout(20) << engine.get_name() << " granted access" << dendl;

        std::tie(try_next, strategy_result) = \
          strategy_handle_granted(std::move(engine_result), policy,
                                  std::move(strategy_result));
        break;
      }
      default: {
        abort();
      }
    }

    if (! try_next) {
      break;
    }
  }

  return strategy_result;
}

void
rgw::auth::Strategy::add_engine(const Control ctrl_flag,
                                const Engine& engine) noexcept
{
  auth_stack.push_back(std::make_pair(std::cref(engine), ctrl_flag));
}


/* rgw::auth::RemoteAuthApplier */
uint32_t rgw::auth::RemoteApplier::get_perms_from_aclspec(const aclspec_t& aclspec) const
{
  uint32_t perm = 0;

  /* For backward compatibility with ACLOwner. */
  perm |= rgw_perms_from_aclspec_default_strategy(info.acct_user,
                                                  aclspec);

  /* We also need to cover cases where rgw_keystone_implicit_tenants
   * was enabled. */
  if (info.acct_user.tenant.empty()) {
    const rgw_user tenanted_acct_user(info.acct_user.id, info.acct_user.id);

    perm |= rgw_perms_from_aclspec_default_strategy(tenanted_acct_user,
                                                    aclspec);
  }

  /* Now it's a time for invoking additional strategy that was supplied by
   * a specific auth engine. */
  if (extra_acl_strategy) {
    perm |= extra_acl_strategy(aclspec);
  }

  ldout(cct, 20) << "from ACL got perm=" << perm << dendl;
  return perm;
}

bool rgw::auth::RemoteApplier::is_admin_of(const rgw_user& uid) const
{
  return info.is_admin;
}

bool rgw::auth::RemoteApplier::is_owner_of(const rgw_user& uid) const
{
  if (info.acct_user.tenant.empty()) {
    const rgw_user tenanted_acct_user(info.acct_user.id, info.acct_user.id);

    if (tenanted_acct_user == uid) {
      return true;
    }
  }

  return info.acct_user == uid;
}

void rgw::auth::RemoteApplier::to_str(std::ostream& out) const
{
  out << "rgw::auth::RemoteApplier(acct_user=" << info.acct_user
      << ", acct_name=" << info.acct_name
      << ", perm_mask=" << info.perm_mask
      << ", is_admin=" << info.is_admin << ")";
}

void rgw::auth::RemoteApplier::create_account(const rgw_user& acct_user,
                                              RGWUserInfo& user_info) const      /* out */
{
  rgw_user new_acct_user = acct_user;

  if (info.acct_type) {
    //ldap/keystone for s3 users
    user_info.type = info.acct_type;
  }

  /* An upper layer may enforce creating new accounts within their own
   * tenants. */
  if (new_acct_user.tenant.empty() && implicit_tenants) {
    new_acct_user.tenant = new_acct_user.id;
  }

  user_info.user_id = new_acct_user;
  user_info.display_name = info.acct_name;

  int ret = rgw_store_user_info(store, user_info, nullptr, nullptr,
                                real_time(), true);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to store new user info: user="
                  << user_info.user_id << " ret=" << ret << dendl;
    throw ret;
  }
}

/* TODO(rzarzynski): we need to handle display_name changes. */
void rgw::auth::RemoteApplier::load_acct_info(RGWUserInfo& user_info) const      /* out */
{
  /* It's supposed that RGWRemoteAuthApplier tries to load account info
   * that belongs to the authenticated identity. Another policy may be
   * applied by using a RGWThirdPartyAccountAuthApplier decorator. */
  const rgw_user& acct_user = info.acct_user;

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
   * namespace depending on rgw_keystone_implicit_tenants. */
  if (acct_user.tenant.empty()) {
    const rgw_user tenanted_uid(acct_user.id, acct_user.id);

    if (rgw_get_user_info_by_uid(store, tenanted_uid, user_info) >= 0) {
      /* Succeeded. */
      return;
    }
  }

  if (rgw_get_user_info_by_uid(store, acct_user, user_info) < 0) {
    ldout(cct, 0) << "NOTICE: couldn't map swift user " << acct_user << dendl;
    create_account(acct_user, user_info);
  }

  /* Succeeded if we are here (create_account() hasn't throwed). */
}


/* rgw::auth::LocalApplier */
/* static declaration */
const std::string rgw::auth::LocalApplier::NO_SUBUSER;

uint32_t rgw::auth::LocalApplier::get_perms_from_aclspec(const aclspec_t& aclspec) const
{
  return rgw_perms_from_aclspec_default_strategy(user_info.user_id, aclspec);
}

bool rgw::auth::LocalApplier::is_admin_of(const rgw_user& uid) const
{
  return user_info.admin || user_info.system;
}

bool rgw::auth::LocalApplier::is_owner_of(const rgw_user& uid) const
{
  return uid == user_info.user_id;
}

void rgw::auth::LocalApplier::to_str(std::ostream& out) const
{
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

void rgw::auth::LocalApplier::load_acct_info(RGWUserInfo& user_info) const /* out */
{
  /* Load the account that belongs to the authenticated identity. An extra call
   * to RADOS may be safely skipped in this case. */
  user_info = this->user_info;
}


rgw::auth::Engine::result_t
rgw::auth::AnonymousEngine::authenticate(const req_state* const s) const
{
  if (! is_applicable(s)) {
    return result_t::deny();
  } else {
    RGWUserInfo user_info;
    rgw_get_anon_user(user_info);

    // FIXME: over 80 columns
    auto apl = apl_factory->create_apl_local(cct, s, user_info,
                                             rgw::auth::LocalApplier::NO_SUBUSER);
    return result_t::grant(std::move(apl));
  }
}
