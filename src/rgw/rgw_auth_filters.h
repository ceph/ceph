// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_AUTH_FILTERS_H
#define CEPH_RGW_AUTH_FILTERS_H

#include <type_traits>

#include <boost/optional.hpp>

#include "rgw_common.h"
#include "rgw_auth.h"

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
  DecoratedApplier(DecorateeT&& decoratee)
    : decoratee(std::forward<DecorateeT>(decoratee)) {
  }

  uint32_t get_perms_from_aclspec(const aclspec_t& aclspec) const override {
    return get_decoratee().get_perms_from_aclspec(aclspec);
  }

  bool is_admin_of(const rgw_user& uid) const override {
    return get_decoratee().is_admin_of(uid);
  }

  bool is_owner_of(const rgw_user& uid) const override {
    return get_decoratee().is_owner_of(uid);
  }

  uint32_t get_perm_mask() const override {
    return get_decoratee().get_perm_mask();
  }

  void to_str(std::ostream& out) const override {
    get_decoratee().to_str(out);
  }

  void load_acct_info(RGWUserInfo& user_info) const override {  /* out */
    return get_decoratee().load_acct_info(user_info);
  }

  void modify_request_state(req_state * s) const override {     /* in/out */
    return get_decoratee().modify_request_state(s);
  }
};


} /* namespace auth */
} /* namespace rgw */

#endif /* CEPH_RGW_AUTH_FILTERS_H */
