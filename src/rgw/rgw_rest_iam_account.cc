// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_process_env.h"
#include "rgw_rest_iam_account.h"

int RGWGetAccountSummary::verify_permission(optional_yield y)
{
  std::string account_id;
  if (const auto& account = s->auth.identity->get_account(); account) {
    account_id = account->id;
  } else {
    return -ERR_METHOD_NOT_ALLOWED;
  }
  const rgw::ARN arn{"", "root", account_id, true};
  if (verify_user_permission(this, s, arn, rgw::IAM::iamGetAccountSummary)) {
    return 0;
  }
  return -EACCES;
}

void RGWGetAccountSummary::add_entry(const std::string& type, int64_t value)
{
  s->formatter->open_object_section("entry");
  s->formatter->dump_string("key", type);
  s->formatter->dump_int("value", value);
  s->formatter->close_section();
}

void RGWGetAccountSummary::execute(optional_yield y)
{
  const auto& info = s->user->get_info();
  const auto& account = s->auth.identity->get_account();
  uint32_t users_count = 0;
  uint32_t groups_count = 0;

  if (account->max_users >= 0) { 
    op_ret = driver->count_account_users(this, y, info.account_id, users_count);
    if (op_ret < 0) {
      ldpp_dout(this, 4) << "failed to count users for iam account "
          << info.account_id << ": " << op_ret << dendl;
      return;
    }
  }

  if (account->max_groups >= 0) {
    op_ret = driver->count_account_groups(this, y, info.account_id, groups_count);
    if (op_ret < 0) {
      ldpp_dout(this, 4) << "failed to count groups for iam account "
          << info.account_id << ": " << op_ret << dendl;
      return;
    }
  }

  s->formatter->open_object_section("GetAccountSummaryResponse");
  s->formatter->open_object_section("ResponseMetadata");
  s->formatter->dump_string("RequestId", s->trans_id);
  s->formatter->close_section();
  s->formatter->open_object_section("GetAccountSummaryResult");
  s->formatter->open_object_section("SummaryMap");
  add_entry("Users", users_count);
  add_entry("Groups", groups_count);
  add_entry("UsersQuota", account->max_users);
  add_entry("GroupsQuota", account->max_groups);
  add_entry("AccessKeysPerUserQuota", account->max_access_keys);
  s->formatter->close_section();
  s->formatter->close_section();
  s->formatter->close_section();
}

