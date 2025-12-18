#include <common/keyring.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
extern "C" {
#include <keyutils.h>
}

namespace ceph {

class LinuxKeyringTest : public ::testing::Test {
 protected:
  std::unique_ptr<Keyring> keyring;

  LinuxKeyringTest() : keyring(new LinuxKeyring()) {}

  void SetUp() override {
    std::error_code ec;
    if (!keyring->supported(&ec)) {
      GTEST_SKIP() << "Linux Keyring is unsupported. " << ec
                   << ". Skipping test";
    }
  }
};

TEST_F(LinuxKeyringTest, Basics) {
  std::string secret("secret");
  auto maybe_keyring_secret = keyring->add("testkey", secret);

  ASSERT_TRUE(maybe_keyring_secret.has_value()) << maybe_keyring_secret.error();
  auto keyring_secret = std::move(maybe_keyring_secret.value());

  std::string out;
  ASSERT_FALSE(keyring_secret->read(out));
  ASSERT_EQ(secret, out);

  ASSERT_FALSE(keyring_secret->remove());
  ASSERT_TRUE(keyring_secret->read(out));
}

TEST_F(LinuxKeyringTest, Lifecycle) {
  std::string secret("secret");
  auto maybe = keyring->add("testkey", secret);
  ASSERT_TRUE(maybe.has_value());
  auto* ptr = dynamic_cast<LinuxKeyringSecret*>(maybe.value().get());
  ASSERT_TRUE(ptr->initialized());
  auto next = std::move(*ptr);
  ASSERT_TRUE(next.initialized());
  ASSERT_FALSE(ptr->initialized());
}

TEST_F(LinuxKeyringTest, LifecycleMoveAssignResetsDestination) {
  std::string secret("secret");
  auto dest = keyring->add("testkey", secret);
  auto source = keyring->add("testkey2", secret);
  ASSERT_TRUE(dest.has_value());
  ASSERT_TRUE(source.has_value());

  const auto* lks = dynamic_cast<LinuxKeyringSecret*>(dest.value().get());
  auto dest_serial = lks->_serial;
  ASSERT_NE(-1, dest_serial);

  dest = std::move(source);

  std::array<char, 1024> buf = {0};
  auto ret = keyctl_describe(dest_serial, buf.data(), buf.size());

  EXPECT_EQ(-1, ret) << buf.data();
  ASSERT_EQ(ENOKEY, errno);
}

}  // namespace ceph
