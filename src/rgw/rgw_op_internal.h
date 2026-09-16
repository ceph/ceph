// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <string_view>

#include "rgw_iam_policy.h"

class DoutPrefixProvider;
struct req_state;

void rgw_iam_add_objtags_for_policy(const DoutPrefixProvider *dpp,
                                    req_state *s);

int rgw_verify_object_permission_for_policy(const DoutPrefixProvider *dpp,
                                            req_state *s,
                                            const rgw::IAM::action_t action);

bool rgw_acl_targets_object(const req_state *s);

int rgw_verify_get_acl_permission(const DoutPrefixProvider *dpp,
                                  req_state *s);

int rgw_verify_put_acl_permission(const DoutPrefixProvider *dpp,
                                  req_state *s,
                                  int& op_ret);

constexpr rgw::IAM::action_t rgw_object_action_for_instance(
  const std::string_view instance,
  const rgw::IAM::action_t unversioned_action,
  const rgw::IAM::action_t versioned_action)
{
  return instance.empty() ?
    unversioned_action :
    versioned_action;
}
