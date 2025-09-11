#include <common/keyring.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
extern "C" {
#include <keyutils.h>
}

namespace ceph {

class LinuxKeyringTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::error_code ec;
    if (!LinuxKeyringSecret::supported(&ec)) {
      GTEST_SKIP() << "Linux Keyring is unsupported. " << ec
                   << ". Skipping test";
    }
  }
};

TEST_F(LinuxKeyringTest, Basics) {
  std::string secret("secret");
  auto maybe_keyring_secret = LinuxKeyringSecret::add("testkey", secret);

  ASSERT_TRUE(maybe_keyring_secret.has_value()) << maybe_keyring_secret.error();
  auto keyring_secret = std::move(maybe_keyring_secret.value());

  std::string out;
  ASSERT_FALSE(keyring_secret.read(out));
  ASSERT_EQ(secret, out);

  ASSERT_FALSE(keyring_secret.remove());
  ASSERT_TRUE(keyring_secret.read(out));
}

TEST_F(LinuxKeyringTest, Lifecycle) {
  std::string secret("secret");
  auto uut = LinuxKeyringSecret::add("testkey", secret);
  ASSERT_TRUE(uut.has_value());
  ASSERT_TRUE(uut->initialized());
  auto next = std::move(uut);
  ASSERT_FALSE(uut->initialized());
  ASSERT_TRUE(next->initialized());
}

TEST_F(LinuxKeyringTest, LifecycleMoveAssignResetsDestination) {
  std::string secret("secret");
  auto dest = LinuxKeyringSecret::add("testkey", secret);
  auto source = LinuxKeyringSecret::add("testkey2", secret);
  ASSERT_TRUE(dest.has_value());
  ASSERT_TRUE(source.has_value());

  auto dest_serial = dest->_serial;
  ASSERT_NE(-1, dest_serial);

  dest = std::move(source);

  std::array<char, 1024> buf = {0};
  auto ret = keyctl_describe(dest_serial, buf.data(), buf.size());

  EXPECT_EQ(-1, ret) << buf.data();
  ASSERT_EQ(ENOKEY, errno);
}

}  // namespace ceph
