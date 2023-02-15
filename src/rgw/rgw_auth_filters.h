// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <type_traits>

#include <boost/logic/tribool.hpp>
#include <boost/optional.hpp>

#include "rgw_service.h"
#include "rgw_common.h"
#include "rgw_auth.h"
#include "rgw_user.h"

namespace rgw {
namespace auth {

/* Abstract decorator over any implementation of rgw::auth::IdentityApplier
 * which could be provided both as a pointer-to-object or the object itself. */
template <typename DecorateeT>
class DecoratedApplier : public rgw::auth::IdentityApplier {
  typedef typename std::remove_pointer<DecorateeT>::type DerefedDecorateeT;

  static_assert(std::is_base_of<rgw::auth::IdentityApplier,
                                DerefedDecorateeT>::value,
                "DecorateeT must be a subclass of rgw::auth::IdentityApplier");

  DecorateeT decoratee;

  /* There is an indirection layer over accessing decoratee to share the same
   * code base between dynamic and static decorators. The difference is about
   * what we store internally: pointer to a decorated object versus the whole
   * object itself. Googling for "SFINAE" can help to understand the code. */
  template <typename T = void,
            typename std::enable_if<
    std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  DerefedDecorateeT& get_decoratee() {
    return *decoratee;
  }

  template <typename T = void,
            typename std::enable_if<
    ! std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  DerefedDecorateeT& get_decoratee() {
    return decoratee;
  }

  template <typename T = void,
            typename std::enable_if<
    std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  const DerefedDecorateeT& get_decoratee() const {
    return *decoratee;
  }

  template <typename T = void,
            typename std::enable_if<
    ! std::is_pointer<DecorateeT>::value, T>::type* = nullptr>
  const DerefedDecorateeT& get_decoratee() const {
    return decoratee;
  }

public:
  explicit DecoratedApplier(DecorateeT&& decoratee)
    : decoratee(std::forward<DecorateeT>(decoratee)) {
  }

  uint32_t get_perms_from_aclspec(const DoutPrefixProvider* dpp, const aclspec_t& aclspec) const override {
    return get_decoratee().get_perms_from_aclspec(dpp, aclspec);
  }

  bool is_admin_of(const rgw_user& uid) const override {
    return get_decoratee().is_admin_of(uid);
  }

  bool is_owner_of(const rgw_user& uid) const override {
    return get_decoratee().is_owner_of(uid);
  }

  bool is_anonymous() const override {
    return get_decoratee().is_anonymous();
  }

  uint32_t get_perm_mask() const override {
    return get_decoratee().get_perm_mask();
  }

  uint32_t get_identity_type() const override {
    return get_decoratee().get_identity_type();
  }

  std::string get_acct_name() const override {
    return get_decoratee().get_acct_name();
  }

  std::string get_subuser() const override {
    return get_decoratee().get_subuser();
  }

  bool is_identity(
    const boost::container::flat_set<Principal>& ids) const override {
    return get_decoratee().is_identity(ids);
  }

  void to_str(std::ostream& out) const override {
    get_decoratee().to_str(out);
  }

  std::string get_role_tenant() const override {     /* in/out */
    return get_decoratee().get_role_tenant();
  }

  void load_acct_info(const DoutPrefixProvider* dpp, RGWUserInfo& user_info) const override {  /* out */
    return get_decoratee().load_acct_info(dpp, user_info);
  }

  void modify_request_state(const DoutPrefixProvider* dpp, req_state * s) const override {     /* in/out */
    return get_decoratee().modify_request_state(dpp, s);
  }

  void write_ops_log_entry(rgw_log_entry& entry) const override {
    return get_decoratee().write_ops_log_entry(entry);
  }
};


template <typename T>
class ThirdPartyAccountApplier : public DecoratedApplier<T> {
  rgw::sal::Driver* driver;
  const rgw_user acct_user_override;

public:
  /* A value representing situations where there is no requested account
   * override. In other words, acct_user_override will be equal to this
   * constant where the request isn't a cross-tenant one. */
  static const rgw_user UNKNOWN_ACCT;

  template <typename U>
  ThirdPartyAccountApplier(rgw::sal::Driver* driver,
                           const rgw_user &acct_user_override,
                           U&& decoratee)
    : DecoratedApplier<T>(std::move(decoratee)),
      driver(driver),
      acct_user_override(acct_user_override) {
  }

  void to_str(std::ostream& out) const override;
  void load_acct_info(const DoutPrefixProvider* dpp, RGWUserInfo& user_info) const override;   /* out */
};

/* static declaration: UNKNOWN_ACCT will be an empty rgw_user that is a result
 * of the default construction. */
template <typename T>
const rgw_user ThirdPartyAccountApplier<T>::UNKNOWN_ACCT;

template <typename T>
void ThirdPartyAccountApplier<T>::to_str(std::ostream& out) const
{
  out << "rgw::auth::ThirdPartyAccountApplier(" + acct_user_override.to_str() + ")"
      <<   " -> ";
  DecoratedApplier<T>::to_str(out);
}

template <typename T>
void ThirdPartyAccountApplier<T>::load_acct_info(const DoutPrefixProvider* dpp, RGWUserInfo& user_info) const
{
  if (UNKNOWN_ACCT == acct_user_override) {
    /* There is no override specified by the upper layer. This means that we'll
     * load the account owned by the authenticated identity (aka auth_user). */
    DecoratedApplier<T>::load_acct_info(dpp, user_info);
  } else if (DecoratedApplier<T>::is_owner_of(acct_user_override)) {
    /* The override has been specified but the account belongs to the authenticated
     * identity. We may safely forward the call to a next stage. */
    DecoratedApplier<T>::load_acct_info(dpp, user_info);
  } else if (this->is_anonymous()) {
    /* If the user was authed by the anonymous engine then scope the ANON user
     * to the correct tenant */
    if (acct_user_override.tenant.empty())
      user_info.user_id = rgw_user(acct_user_override.id, RGW_USER_ANON_ID);
    else
      user_info.user_id = rgw_user(acct_user_override.tenant, RGW_USER_ANON_ID);
  } else {
    /* Compatibility mechanism for multi-tenancy. For more details refer to
     * load_acct_info method of rgw::auth::RemoteApplier. */
    std::unique_ptr<rgw::sal::User> user;

    if (acct_user_override.tenant.empty()) {
      const rgw_user tenanted_uid(acct_user_override.id, acct_user_override.id);
      user = driver->get_user(tenanted_uid);

      if (user->load_user(dpp, null_yield) >= 0) {
	user_info = user->get_info();
        /* Succeeded. */
        return;
      }
    }

    user = driver->get_user(acct_user_override);
    const int ret = user->load_user(dpp, null_yield);
    if (ret < 0) {
      /* We aren't trying to recover from ENOENT here. It's supposed that creating
       * someone else's account isn't a thing we want to support in this filter. */
      if (ret == -ENOENT) {
        throw -EACCES;
      } else {
        throw ret;
      }
    }
    user_info = user->get_info();
  }
}

template <typename T> static inline
ThirdPartyAccountApplier<T> add_3rdparty(rgw::sal::Driver* driver,
                                         const rgw_user &acct_user_override,
                                         T&& t) {
  return ThirdPartyAccountApplier<T>(driver, acct_user_override,
                                     std::forward<T>(t));
}


template <typename T>
class SysReqApplier : public DecoratedApplier<T> {
  CephContext* const cct;
  rgw::sal::Driver* driver;
  const RGWHTTPArgs& args;
  mutable boost::tribool is_system;

public:
  template <typename U>
  SysReqApplier(CephContext* const cct,
		rgw::sal::Driver* driver,
                const req_state* const s,
                U&& decoratee)
    : DecoratedApplier<T>(std::forward<T>(decoratee)),
      cct(cct),
      driver(driver),
      args(s->info.args),
      is_system(boost::logic::indeterminate) {
  }

  void to_str(std::ostream& out) const override;
  void load_acct_info(const DoutPrefixProvider* dpp, RGWUserInfo& user_info) const override;   /* out */
  void modify_request_state(const DoutPrefixProvider* dpp, req_state* s) const override;       /* in/out */
};

template <typename T>
void SysReqApplier<T>::to_str(std::ostream& out) const
{
  out << "rgw::auth::SysReqApplier" << " -> ";
  DecoratedApplier<T>::to_str(out);
}

template <typename T>
void SysReqApplier<T>::load_acct_info(const DoutPrefixProvider* dpp, RGWUserInfo& user_info) const
{
  DecoratedApplier<T>::load_acct_info(dpp, user_info);
  is_system = user_info.system;

  if (is_system) {
    //ldpp_dout(dpp, 20) << "system request" << dendl;

    rgw_user effective_uid(args.sys_get(RGW_SYS_PARAM_PREFIX "uid"));
    if (! effective_uid.empty()) {
      /* We aren't writing directly to user_info for consistency and security
       * reasons. rgw_get_user_info_by_uid doesn't trigger the operator=() but
       * calls ::decode instead. */
      std::unique_ptr<rgw::sal::User> user = driver->get_user(effective_uid);
      if (user->load_user(dpp, null_yield) < 0) {
        //ldpp_dout(dpp, 0) << "User lookup failed!" << dendl;
        throw -EACCES;
      }
      user_info = user->get_info();
    }
  }
}

template <typename T>
void SysReqApplier<T>::modify_request_state(const DoutPrefixProvider* dpp, req_state* const s) const
{
  if (boost::logic::indeterminate(is_system)) {
    RGWUserInfo unused_info;
    load_acct_info(dpp, unused_info);
  }

  if (is_system) {
    s->info.args.set_system();
    s->system_request = true;
  }
  DecoratedApplier<T>::modify_request_state(dpp, s);
}

template <typename T> static inline
SysReqApplier<T> add_sysreq(CephContext* const cct,
			    rgw::sal::Driver* driver,
                            const req_state* const s,
                            T&& t) {
  return SysReqApplier<T>(cct, driver, s, std::forward<T>(t));
}

} /* namespace auth */
} /* namespace rgw */
