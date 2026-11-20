// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_sal_store.h"
#include <gtest/gtest.h>

namespace {

class TestUser : public rgw::sal::StoreUser {
public:
  std::unique_ptr<rgw::sal::User> clone() override {
    return std::make_unique<TestUser>(*this);
  }
  int read_attrs(const DoutPrefixProvider*, optional_yield) override { return 0; }
  int read_usage(const DoutPrefixProvider*, uint64_t, uint64_t, uint32_t,
                 bool*, RGWUsageIter&,
                 std::map<rgw_user_bucket, rgw_usage_log_entry>&) override { return 0; }
  int trim_usage(const DoutPrefixProvider*, uint64_t, uint64_t,
                 optional_yield) override { return 0; }
  int load_user(const DoutPrefixProvider*, optional_yield) override { return 0; }
  int store_user(const DoutPrefixProvider*, optional_yield, bool,
                 RGWUserInfo*) override { return 0; }
  int remove_user(const DoutPrefixProvider*, optional_yield) override { return 0; }
  int merge_and_store_attrs(const DoutPrefixProvider*, rgw::sal::Attrs&,
                            optional_yield) override { return 0; }
  int verify_mfa(const std::string&, bool*, const DoutPrefixProvider*,
                 optional_yield) override { return 0; }
  int list_groups(const DoutPrefixProvider*, optional_yield,
                  std::string_view, uint32_t,
                  rgw::sal::GroupList&) override { return 0; }
};

} // namespace

TEST(StoreUserCOW, get_info_mut_does_not_mutate_shared)
{
  RGWUserInfo orig;
  orig.display_name = "shared";
  auto shared = std::make_shared<const RGWUserInfo>(orig);

  TestUser user;
  EXPECT_EQ(user.get_info().display_name, "");

  user.get_info_mut().display_name = "owned1";
  EXPECT_EQ(user.get_info().display_name, "owned1");

  // Verify that get_info_mut() after set_info_shared() performs a
  // copy-on-write: the shared object must not be mutated, and the
  // user's own view must reflect the change.
  user.set_info_shared(shared);
  auto info = user.get_info();
  EXPECT_EQ(user.get_info().display_name, "shared");

  user.get_info_mut().display_name = "owned2";
  EXPECT_EQ(user.get_info().display_name, "owned2");

  EXPECT_EQ(shared->display_name, "shared");

  // get_info_shared() returns the user info as shared_ptr,
  // regardless of current mode
  auto shared2 = user.get_info_shared();
  EXPECT_EQ(shared2->display_name, "owned2");

  // Info saved while shared stays valid.
  shared.reset();
  EXPECT_EQ(info.display_name, "shared");
}
