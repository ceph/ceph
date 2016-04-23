// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#ifndef CEPH_RGW_AUTH_DECOIMPL_H
#define CEPH_RGW_AUTH_DECOIMPL_H

#include "rgw_auth.h"

/* static declaration */
template <typename T>
const rgw_user RGWThirdPartyAccountAuthApplier<T>::UNKNOWN_ACCT;

template <typename T>
void RGWThirdPartyAccountAuthApplier<T>::load_acct_info(RGWUserInfo& user_info) const
{
  rgw_user auth_user;
  uint32_t perm_mask;
  bool is_admin;

  RGWDecoratingAuthApplier<T>::load_user_info(auth_user, perm_mask, is_admin);

  if (UNKNOWN_ACCT == acct_user_override) {
    /* There is no override specified by the upper layer. This means that we'll
     * load the account owned by the authenticated identity (aka auth_user). */
    RGWDecoratingAuthApplier<T>::load_acct_info(user_info);
  } else if (acct_user_override == auth_user) {
    /* The override has been specified but the account belongs to the authenticated
     * identity. We may safely forward the call to a next stage. */
    RGWDecoratingAuthApplier<T>::load_acct_info(user_info);
  } else {
    int ret = rgw_get_user_info_by_uid(store, acct_user_override, user_info);
    if (ret < 0) {
      /* We aren't trying to recover from ENOENT here. It's supposed that creating
       * someone else's account isn't a thing we want to support. */
      throw ret;
    }
  }
}

#endif /* CEPH_RGW_AUTH_DECOIMPL_H */
