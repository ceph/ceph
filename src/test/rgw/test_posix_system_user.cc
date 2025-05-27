#include <filesystem>
#include "rgw_sal_posix.h"
#include "posix_system_user.h"
#include "gtest/gtest.h"

#define dout_subsys ceph_subsys_rgw

namespace fs = std::filesystem;
namespace net = boost::asio;

class Environment* env;
fs::path test_path{std::filesystem::current_path().string() + "/posix_system_test"};

std::string get_test_name()
{
  return testing::UnitTest::GetInstance()->current_test_info()->name();
}

class Environment : public ::testing::Environment {
  public:
    Environment() {}

    virtual ~Environment() {}

    void SetUp() override {
      fs::remove_all(test_path);
      fs::create_directory(test_path.c_str());
      std::vector<const char*> args;
      cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                        CODE_ENVIRONMENT_UTILITY,
                        CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

      common_init_finish(g_ceph_context);
      dpp = new DoutPrefix(cct->get(), dout_subsys, "POSIX System User Test: ");
      cct->_conf->rgw_posix_user_mapping_type = "direct";
      root = std::make_unique<rgw::sal::Directory>(test_path, nullptr, cct.get());
      ASSERT_EQ(root->open(dpp), 0);
    }

    virtual void TearDown() {
      delete dpp;
      fs::remove_all(test_path);
    }

    std::unique_ptr<rgw::sal::Directory> root;
    boost::intrusive_ptr<ceph::common::CephContext> cct;
    DoutPrefixProvider* dpp;
};

class POSIXSystemFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      mapping_path = g_conf().get_val<std::string>("rgw_posix_base_path");
      fs::remove_all(mapping_path);
      fs::create_directory(mapping_path.c_str());
      mapping_path += "/user_mapping";

      ASSERT_EQ(sys_mgr.init(env->dpp), 0);
      
      ruser.id = "test";
      posix_user.set_uid(0);
      posix_user.set_gid(0);

      /* Populate bl in expected mapped format */
      JSONFormatter formatter;
      formatter.open_object_section("user");
      formatter.dump_string("rgw_user", ruser.id.c_str());
      formatter.open_object_section("posix_user");
      formatter.dump_int("uid", posix_user.get_uid());
      formatter.dump_int("gid", posix_user.get_gid());
      formatter.close_section();
      formatter.close_section();
      formatter.flush(bl);
    }

    virtual void TearDown() {
      fs::remove(mapping_path);
    }

    POSIXSystemManager sys_mgr;
    std::string mapping_path;
    rgw_user ruser;
    POSIXSystemUser posix_user;
    bufferlist bl;
};

TEST_F(POSIXSystemFixture, CreateEntry) {
  EXPECT_EQ(sys_mgr.update_posix_user(env->dpp, ruser, posix_user), 0);

  POSIXSystemUser out_user;
  EXPECT_EQ(sys_mgr.find_posix_user(env->dpp, ruser, out_user), 0);
  EXPECT_EQ(posix_user.get_uid(), out_user.get_uid());
  EXPECT_EQ(posix_user.get_gid(), out_user.get_gid());

  int fd = open(mapping_path.c_str(), (O_APPEND) | O_RDONLY | O_NOFOLLOW, S_IRWXU);
  ASSERT_GE(fd, 0);

  char read_buf[bl.length()];
  memset(read_buf, 0, bl.length());
  ASSERT_EQ(lseek(fd, (size_t)0, SEEK_SET), 0);
  ASSERT_EQ(::read(fd, read_buf, bl.length()), bl.length());
  std::string read_str = read_buf;
  EXPECT_EQ(read_str.substr(0, bl.length()), bl.to_str());

  close(fd);
}

TEST_F(POSIXSystemFixture, RemoveEntry) {
  EXPECT_EQ(sys_mgr.update_posix_user(env->dpp, ruser, posix_user), 0);
  EXPECT_EQ(sys_mgr.remove_posix_user(env->dpp, ruser), 0);

  int fd = open(mapping_path.c_str(), (O_APPEND) | O_RDONLY | O_NOFOLLOW, S_IRWXU);
  ASSERT_GE(fd, 0);

  char read_buf[bl.length()];
  memset(read_buf, 0, bl.length());
  ASSERT_EQ(lseek(fd, (size_t)0, SEEK_SET), 0);
  ASSERT_EQ(::read(fd, read_buf, bl.length()), 0); // empty file

  close(fd);
}

TEST_F(POSIXSystemFixture, DirectWrite) {
  struct stat statbuf;
  EXPECT_EQ(sys_mgr.update_posix_user(env->dpp, ruser, posix_user), 0);

  bool existed;
  rgw::sal::File testFile{test_path.c_str() + get_test_name(), env->root.get(), env->cct.get()};
  ASSERT_EQ(testFile.create(env->dpp, &existed), 0);
  std::unique_ptr<rgw::sal::FSEnt> ent;
  ASSERT_EQ(env->root->get_ent(env->dpp, null_yield, test_path.c_str() + get_test_name(), std::string(), ent), 0);

  int ret, call_ret;
  bufferlist write_buf;
  write_buf.append("hello world");
  ent->open(env->dpp);
  EXPECT_EQ(sys_mgr.check_permissions(env->dpp, ruser, ent.get(), statbuf), 0);
  RUN_AS(env->dpp, posix_user.get_uid(), posix_user.get_gid(), statbuf.st_uid, statbuf.st_gid, ent->write(0, write_buf, env->dpp, null_yield), ret, call_ret);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(call_ret, 0);
  ent->close();

  std::string file_path = test_path.c_str() + get_test_name();
  int fd = open(file_path.c_str(), (O_APPEND) | O_RDONLY | O_NOFOLLOW, S_IRUSR);
  ASSERT_GE(fd, 0);
  char read_buf[write_buf.length()];
  memset(read_buf, 0, write_buf.length());
  ASSERT_EQ(lseek(fd, (size_t)0, SEEK_SET), 0);
  EXPECT_EQ(::read(fd, read_buf, write_buf.length()), write_buf.length());
  std::string read_str = read_buf;
  EXPECT_EQ(read_str.substr(0, write_buf.length()), write_buf.to_str());
  close(fd);

out:
  ASSERT_EQ(ret, 0); // fails
}

TEST_F(POSIXSystemFixture, DirectRead) {
  struct stat statbuf;
  EXPECT_EQ(sys_mgr.update_posix_user(env->dpp, ruser, posix_user), 0);

  bool existed;
  rgw::sal::File testFile{test_path.c_str() + get_test_name(), env->root.get(), env->cct.get()};
  ASSERT_EQ(testFile.create(env->dpp, &existed), 0);
  std::unique_ptr<rgw::sal::FSEnt> ent;
  ASSERT_EQ(env->root->get_ent(env->dpp, null_yield, test_path.c_str() + get_test_name(), std::string(), ent), 0);

  std::string file_path = test_path.c_str() + get_test_name();
  int fd = open(file_path.c_str(), (O_APPEND) | O_RDWR | O_NOFOLLOW, S_IRUSR);
  ASSERT_GE(fd, 0);
  bufferlist write_buf;
  write_buf.append("hello world");
  int count = write(fd, write_buf.c_str(), write_buf.length());
  ASSERT_EQ(count, write_buf.length());
  close(fd);

  int ret, len;
  bufferlist read_buf;
  ent->open(env->dpp);
  EXPECT_EQ(sys_mgr.check_permissions(env->dpp, ruser, ent.get(), statbuf), 0);
  RUN_AS(env->dpp, posix_user.get_uid(), posix_user.get_gid(), statbuf.st_uid, statbuf.st_gid, ent->read(0, write_buf.length(), read_buf, env->dpp, null_yield), ret, len);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(len, write_buf.length());
  ent->close();

out:
  ASSERT_EQ(ret, 0); // fails
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
