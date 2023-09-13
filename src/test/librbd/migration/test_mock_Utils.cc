// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/migration/Utils.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"

namespace librbd {
namespace migration {
namespace util {

class TestMockMigrationUtils : public TestMockFixture {
public:
};

TEST_F(TestMockMigrationUtils, ParseUrl) {
   UrlSpec url_spec;
   ASSERT_EQ(-EINVAL, parse_url(g_ceph_context, "", &url_spec));
   ASSERT_EQ(-EINVAL, parse_url(g_ceph_context, "jttp://google.com/path",
                                &url_spec));
   ASSERT_EQ(-EINVAL, parse_url(g_ceph_context, "http://google.com:absd/path",
                                &url_spec));

   ASSERT_EQ(0, parse_url(g_ceph_context, "ceph.io/path", &url_spec));
   ASSERT_EQ(UrlSpec(URL_SCHEME_HTTP, "ceph.io", "80", "/path"), url_spec);

   ASSERT_EQ(0, parse_url(g_ceph_context, "http://google.com/path", &url_spec));
   ASSERT_EQ(UrlSpec(URL_SCHEME_HTTP, "google.com", "80", "/path"), url_spec);

   ASSERT_EQ(0, parse_url(g_ceph_context, "https://ceph.io/", &url_spec));
   ASSERT_EQ(UrlSpec(URL_SCHEME_HTTPS, "ceph.io", "443", "/"), url_spec);

   ASSERT_EQ(0, parse_url(g_ceph_context,
                          "http://google.com:1234/some/other/path", &url_spec));
   ASSERT_EQ(UrlSpec(URL_SCHEME_HTTP, "google.com", "1234", "/some/other/path"),
             url_spec);

   ASSERT_EQ(0, parse_url(g_ceph_context,
                          "http://1.2.3.4/", &url_spec));
   ASSERT_EQ(UrlSpec(URL_SCHEME_HTTP, "1.2.3.4", "80", "/"), url_spec);
}

} // namespace util
} // namespace migration
} // namespace librbd
