#include <common/keyring.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace ceph {

class LinuxKeyringTest : public  ::testing::Test {
 protected:
  void SetUp() override {
    if (!LinuxKeyringSecret::supported()) {
      GTEST_SKIP();
    }
  }
};

TEST(LinuxKeyringTest, Basics) {
  std::string secret("secret");
  auto maybe_keyring_secret = LinuxKeyringSecret::add("testkey", secret);
  
  ASSERT_TRUE(maybe_keyring_secret.has_value());
  auto keyring_secret = std::move(maybe_keyring_secret.value());

  std::string out;
  ASSERT_FALSE(keyring_secret.read(out));
  ASSERT_EQ(secret, out);

  ASSERT_FALSE(keyring_secret.remove());
  ASSERT_TRUE(keyring_secret.read(out));
}

}  // namespace ceph
