#include <gtest/gtest.h>
#include "common/ceph_context.h"
#include "rgw/rgw_common.h"
#include "rgw/rgw_auth.h"
#include "rgw/rgw_process.h"
#include "rgw/rgw_sal_rados.h"
#include "rgw/rgw_lua_request.h"
#include "rgw/rgw_lua_background.h"

using namespace std;
using namespace rgw;
using boost::container::flat_set;
using rgw::auth::Identity;
using rgw::auth::Principal;

class CctCleaner {
  CephContext* cct;
public:
  CctCleaner(CephContext* _cct) : cct(_cct) {}
  ~CctCleaner() { 
#ifdef WITH_SEASTAR
    delete cct; 
#else
    cct->put(); 
#endif
  }
};

class FakeIdentity : public Identity {
public:
  FakeIdentity() = default;

  uint32_t get_perms_from_aclspec(const DoutPrefixProvider* dpp, const aclspec_t& aclspec) const override {
    return 0;
  };

  bool is_admin_of(const rgw_user& uid) const override {
    return false;
  }

  bool is_owner_of(const rgw_user& uid) const override {
    return false;
  }

  virtual uint32_t get_perm_mask() const override {
    return 0;
  }

  uint32_t get_identity_type() const override {
    return TYPE_RGW;
  }

  string get_acct_name() const override {
    return "";
  }

  string get_subuser() const override {
    return "";
  }

  void to_str(std::ostream& out) const override {
    return;
  }

  bool is_identity(const flat_set<Principal>& ids) const override {
    return false;
  }
};

class TestUser : public sal::User {
public:
  virtual std::unique_ptr<User> clone() override {
    return std::unique_ptr<User>(new TestUser(*this));
  }

  virtual int list_buckets(const DoutPrefixProvider *dpp, const string&, const string&, uint64_t, bool, sal::BucketList&, optional_yield y) override {
    return 0;
  }

  virtual int create_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b, const std::string& zonegroup_id, rgw_placement_rule& placement_rule, std::string& swift_ver_location, const RGWQuotaInfo* pquota_info, const RGWAccessControlPolicy& policy, sal::Attrs& attrs, RGWBucketInfo& info, obj_version& ep_objv, bool exclusive, bool obj_lock_enabled, bool* existed, req_info& req_info, std::unique_ptr<sal::Bucket>* bucket, optional_yield y) override {
    return 0;
  }

  virtual int read_attrs(const DoutPrefixProvider *dpp, optional_yield y) override {
    return 0;
  }

  virtual int read_stats(const DoutPrefixProvider *dpp, optional_yield y, RGWStorageStats* stats, ceph::real_time *last_stats_sync, ceph::real_time *last_stats_update) override {
    return 0;
  }

  virtual int read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB *cb) override {
    return 0;
  }

  virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) override {
    return 0;
  }

  virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries, bool *is_truncated, RGWUsageIter& usage_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage) override {
    return 0;
  }

  virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override {
    return 0;
  }

  virtual int load_user(const DoutPrefixProvider *dpp, optional_yield y) override {
    return 0;
  }

  virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info) override {
    return 0;
  }

  virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override {
    return 0;
  }
  virtual int merge_and_store_attrs(const DoutPrefixProvider *dpp, rgw::sal::Attrs& attrs, optional_yield y) override {
    return 0;
  }
  virtual ~TestUser() = default;
};

class TestAccounter : public io::Accounter, public io::BasicClient {
  RGWEnv env;

protected:
  virtual int init_env(CephContext *cct) override {
    return 0;
  }

public:
  ~TestAccounter() = default;

  virtual void set_account(bool enabled) override {
  }

  virtual uint64_t get_bytes_sent() const override {
    return 0;
  }

  virtual uint64_t get_bytes_received() const override {
    return 0;
  }
  
  virtual RGWEnv& get_env() noexcept override {
    return env;
  }
  
  virtual size_t complete_request() override {
    return 0;
  }
};

auto g_cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);

CctCleaner cleaner(g_cct);

#define DEFINE_REQ_STATE RGWEnv e; req_state s(g_cct, &e, 0);

TEST(TestRGWLua, EmptyScript)
{
  const std::string script;

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, SyntaxError)
{
  const std::string script = R"(
    if 3 < 5 then
      RGWDebugLog("missing 'end'")
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "", script);
  ASSERT_EQ(rc, -1);
}

TEST(TestRGWLua, Hello)
{
  const std::string script = R"(
    RGWDebugLog("hello from lua")
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, RGWDebugLogNumber)
{
  const std::string script = R"(
    RGWDebugLog(1234567890)
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, RGWDebugNil)
{
  const std::string script = R"(
    RGWDebugLog(nil)
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "", script);
  ASSERT_EQ(rc, -1);
}

TEST(TestRGWLua, URI)
{
  const std::string script = R"(
    RGWDebugLog(Request.DecodedURI)
    assert(Request.DecodedURI == "http://hello.world/")
  )";

  DEFINE_REQ_STATE;
  s.decoded_uri = "http://hello.world/";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, Response)
{
  const std::string script = R"(
    assert(Request.Response.Message == "This is a bad request")
    assert(Request.Response.HTTPStatus == "Bad Request")
    assert(Request.Response.RGWCode == 4000)
    assert(Request.Response.HTTPStatusCode == 400)
  )";

  DEFINE_REQ_STATE;
  s.err.http_ret = 400;
  s.err.ret = 4000;
  s.err.err_code = "Bad Request";
  s.err.message = "This is a bad request";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, SetResponse)
{
  const std::string script = R"(
    assert(Request.Response.Message == "this is a bad request")
    Request.Response.Message = "this is a good request"
    assert(Request.Response.Message == "this is a good request")
  )";

  DEFINE_REQ_STATE;
  s.err.message = "this is a bad request";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, RGWIdNotWriteable)
{
  const std::string script = R"(
    assert(Request.RGWId == "foo")
    Request.RGWId = "bar"
  )";

  DEFINE_REQ_STATE;
  s.host_id = "foo";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_NE(rc, 0);
}

TEST(TestRGWLua, InvalidField)
{
  const std::string script = R"(
    RGWDebugLog(Request.Kaboom)
  )";

  DEFINE_REQ_STATE;
  s.host_id = "foo";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "kaboom", script);
  ASSERT_EQ(rc, -1);
}

TEST(TestRGWLua, InvalidSubField)
{
  const std::string script = R"(
    RGWDebugLog(Request.Error.Kaboom)
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "kaboom", script);
  ASSERT_EQ(rc, -1);
}

TEST(TestRGWLua, Bucket)
{
  const std::string script = R"(
    assert(Request.Bucket)
    RGWDebugLog("Bucket Id: " .. Request.Bucket.Id)
    assert(Request.Bucket.Marker == "mymarker")
    assert(Request.Bucket.Name == "myname")
    assert(Request.Bucket.Tenant == "mytenant")
    assert(Request.Bucket.Count == 0)
    assert(Request.Bucket.Size == 0)
    assert(Request.Bucket.ZoneGroupId)
    assert(Request.Bucket.CreationTime)
    assert(Request.Bucket.MTime)
    assert(Request.Bucket.Quota.MaxSize == -1)
    assert(Request.Bucket.Quota.MaxObjects == -1)
    assert(tostring(Request.Bucket.Quota.Enabled))
    assert(tostring(Request.Bucket.Quota.Rounded))
    assert(Request.Bucket.User.Id)
    assert(Request.Bucket.User.Tenant)
  )";

  DEFINE_REQ_STATE;

  rgw_bucket b;
  b.tenant = "mytenant";
  b.name = "myname";
  b.marker = "mymarker";
  b.bucket_id = "myid"; 
  s.bucket.reset(new sal::RadosBucket(nullptr, b));

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, GenericAttributes)
{
  const std::string script = R"(
    assert(Request.GenericAttributes["hello"] == "world")
    assert(Request.GenericAttributes["foo"] == "bar")
    assert(Request.GenericAttributes["kaboom"] == nil)
    assert(#Request.GenericAttributes == 4)
    for k, v in pairs(Request.GenericAttributes) do
      assert(k)
      assert(v)
    end
  )";

  DEFINE_REQ_STATE;
  s.generic_attrs["hello"] = "world";
  s.generic_attrs["foo"] = "bar";
  s.generic_attrs["goodbye"] = "cruel world";
  s.generic_attrs["ka"] = "boom";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, Environment)
{
  const std::string script = R"(
  assert(Request.Environment[""] == "bar")
  assert(Request.Environment["goodbye"] == "cruel world")
  assert(Request.Environment["ka"] == "boom")
  assert(#Request.Environment == 3, #Request.Environment)
  for k, v in pairs(Request.Environment) do
    assert(k)
    assert(v)
  end
  )";

  DEFINE_REQ_STATE;
  s.env.emplace("", "bar");
  s.env.emplace("goodbye", "cruel world");
  s.env.emplace("ka", "boom");

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, Tags)
{
  const std::string script = R"(
    assert(#Request.Tags == 4)
    assert(Request.Tags["foo"] == "bar")
    for k, v in pairs(Request.Tags) do
      assert(k)
      assert(v)
    end
  )";

  DEFINE_REQ_STATE;
  s.tagset.add_tag("hello", "world");
  s.tagset.add_tag("foo", "bar");
  s.tagset.add_tag("goodbye", "cruel world");
  s.tagset.add_tag("ka", "boom");

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, TagsNotWriteable)
{
  const std::string script = R"(
    Request.Tags["hello"] = "goodbye"
  )";

  DEFINE_REQ_STATE;
  s.tagset.add_tag("hello", "world");

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_NE(rc, 0);
}

TEST(TestRGWLua, Metadata)
{
  const std::string script = R"(
    assert(#Request.HTTP.Metadata == 3)
    for k, v in pairs(Request.HTTP.Metadata) do
      assert(k)
      assert(v)
    end
    assert(Request.HTTP.Metadata["hello"] == "world")
    assert(Request.HTTP.Metadata["kaboom"] == nil)
    Request.HTTP.Metadata["hello"] = "goodbye"
    Request.HTTP.Metadata["kaboom"] = "boom"
    assert(#Request.HTTP.Metadata == 4)
    assert(Request.HTTP.Metadata["hello"] == "goodbye")
    assert(Request.HTTP.Metadata["kaboom"] == "boom")
  )";

  DEFINE_REQ_STATE;
  s.info.x_meta_map["hello"] = "world";
  s.info.x_meta_map["foo"] = "bar";
  s.info.x_meta_map["ka"] = "boom";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, Acl)
{
  const std::string script = R"(
    function print_grant(g)
      print("Grant Type: " .. g.Type)
      print("Grant Group Type: " .. g.GroupType)
      print("Grant Referer: " .. g.Referer)
      if (g.User) then
        print("Grant User.Tenant: " .. g.User.Tenant)
        print("Grant User.Id: " .. g.User.Id)
      end
    end

    assert(Request.UserAcl.Owner.DisplayName == "jack black", Request.UserAcl.Owner.DisplayName)
    assert(Request.UserAcl.Owner.User.Id == "black", Request.UserAcl.Owner.User.Id)
    assert(Request.UserAcl.Owner.User.Tenant == "jack", Request.UserAcl.Owner.User.Tenant)
    assert(#Request.UserAcl.Grants == 5)
    print_grant(Request.UserAcl.Grants[""])
    for k, v in pairs(Request.UserAcl.Grants) do
      print_grant(v)
      if k == "john$doe" then
        assert(v.Permission == 4)
      elseif k == "jane$doe" then
        assert(v.Permission == 1)
      else
        assert(false)
      end
    end
  )";

  DEFINE_REQ_STATE;
  ACLOwner owner;
  owner.set_id(rgw_user("jack", "black"));
  owner.set_name("jack black");
  s.user_acl.reset(new RGWAccessControlPolicy(g_cct));
  s.user_acl->set_owner(owner);
  ACLGrant grant1, grant2, grant3, grant4, grant5;
  grant1.set_canon(rgw_user("jane", "doe"), "her grant", 1);
  grant2.set_group(ACL_GROUP_ALL_USERS ,2);
  grant3.set_referer("http://localhost/ref2", 3);
  grant4.set_canon(rgw_user("john", "doe"), "his grant", 4);
  grant5.set_group(ACL_GROUP_AUTHENTICATED_USERS, 5);
  s.user_acl->get_acl().add_grant(&grant1);
  s.user_acl->get_acl().add_grant(&grant2);
  s.user_acl->get_acl().add_grant(&grant3);
  s.user_acl->get_acl().add_grant(&grant4);
  s.user_acl->get_acl().add_grant(&grant5);
  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, User)
{
  const std::string script = R"(
    assert(Request.User)
    assert(Request.User.Id == "myid")
    assert(Request.User.Tenant == "mytenant")
  )";

  DEFINE_REQ_STATE;

  rgw_user u;
  u.tenant = "mytenant";
  u.id = "myid";
  s.user.reset(new sal::RadosUser(nullptr, u));

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}


TEST(TestRGWLua, UseFunction)
{
	const std::string script = R"(
		function print_owner(owner)
  		print("Owner Dispaly Name: " .. owner.DisplayName)
  		print("Owner Id: " .. owner.User.Id)
  		print("Owner Tenanet: " .. owner.User.Tenant)
		end

		print_owner(Request.ObjectOwner)
    
    function print_acl(acl_type)
      index = acl_type .. "ACL"
      acl = Request[index]
      if acl then
        print(acl_type .. "ACL Owner")
        print_owner(acl.Owner)
      else
        print("no " .. acl_type .. " ACL in request: " .. Request.Id)
      end 
    end

    print_acl("User")
    print_acl("Bucket")
    print_acl("Object")
	)";

  DEFINE_REQ_STATE;
  s.owner.set_name("user two");
  s.owner.set_id(rgw_user("tenant2", "user2"));
  s.user_acl.reset(new RGWAccessControlPolicy());
  s.user_acl->get_owner().set_name("user three");
  s.user_acl->get_owner().set_id(rgw_user("tenant3", "user3"));
  s.bucket_acl.reset(new RGWAccessControlPolicy());
  s.bucket_acl->get_owner().set_name("user four");
  s.bucket_acl->get_owner().set_id(rgw_user("tenant4", "user4"));
  s.object_acl.reset(new RGWAccessControlPolicy());
  s.object_acl->get_owner().set_name("user five");
  s.object_acl->get_owner().set_id(rgw_user("tenant5", "user5"));

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, WithLib)
{
  const std::string script = R"(
    expected_result = {"my", "bucket", "name", "is", "fish"}
    i = 1
    for p in string.gmatch(Request.Bucket.Name, "%a+") do
      assert(p == expected_result[i])
      i = i + 1
    end
  )";

  DEFINE_REQ_STATE;

  rgw_bucket b;
  b.name = "my-bucket-name-is-fish";
  s.bucket.reset(new sal::RadosBucket(nullptr, b));

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, NotAllowedInLib)
{
  const std::string script = R"(
    os.clock() -- this should be ok
    os.exit()  -- this should fail (os.exit() is removed)
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "put_obj", script);
  ASSERT_NE(rc, 0);
}

TEST(TestRGWLua, OpsLog)
{
  const std::string script = R"(
		if Request.Response.HTTPStatusCode == 200 then
			assert(Request.Response.Message == "Life is great")
		else 
      assert(Request.Bucket)
    	assert(Request.Log() == 0)
		end
  )";

  auto store = std::unique_ptr<sal::RadosStore>(new sal::RadosStore);
  store->setRados(new RGWRados);

  struct MockOpsLogSink : OpsLogSink {
    bool logged = false;
    int log(req_state*, rgw_log_entry&) override { logged = true; return 0; }
  };
  MockOpsLogSink olog;

  DEFINE_REQ_STATE;
  s.err.http_ret = 200;
  s.err.ret = 0;
  s.err.err_code = "200OK";
  s.err.message = "Life is great";
  rgw_bucket b;
  b.tenant = "tenant";
  b.name = "name";
  b.marker = "marker";
  b.bucket_id = "id"; 
  s.bucket.reset(new sal::RadosBucket(nullptr, b));
  s.bucket_name = "name";
	s.enable_ops_log = true;
	s.enable_usage_log = false;
	s.user.reset(new TestUser());
  TestAccounter ac;
  s.cio = &ac; 
	s.cct->_conf->rgw_ops_log_rados	= false;

  s.auth.identity = std::unique_ptr<rgw::auth::Identity>(
                        new FakeIdentity());

  auto rc = lua::request::execute(store.get(), nullptr, &olog, &s, "put_obj", script);
  EXPECT_EQ(rc, 0);
  EXPECT_FALSE(olog.logged); // don't log http_ret=200
 
	s.err.http_ret = 400;
  rc = lua::request::execute(store.get(), nullptr, &olog, &s, "put_obj", script);
  EXPECT_EQ(rc, 0);
  EXPECT_TRUE(olog.logged);
}

class TestBackground : public rgw::lua::Background {
  const unsigned read_time;
protected:
  int read_script() override {
    // don't read the object from the store
    std::this_thread::sleep_for(std::chrono::seconds(read_time));
    return 0;
  }

public:
  TestBackground(const std::string& script, unsigned read_time = 0) : 
    rgw::lua::Background(nullptr, g_cct, "", 1 /*run every second*/),
    read_time(read_time) {
      // the script is passed in the constructor
      rgw_script = script;
    }

  ~TestBackground() override {
    shutdown();
  }
};

TEST(TestRGWLuaBackground, Start)
{
  {
    // ctr and dtor without running
    TestBackground lua_background("");
  }
  {
    // ctr and dtor with running
    TestBackground lua_background("");
    lua_background.start();
  }
}


constexpr auto wait_time = std::chrono::seconds(2);

TEST(TestRGWLuaBackground, Script)
{
  const std::string script = R"(
    local key = "hello"
    local value = "world"
    RGW[key] = value
  )";

  TestBackground lua_background(script);
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  EXPECT_EQ(lua_background.get_table_value("hello"), "world");
}

TEST(TestRGWLuaBackground, RequestScript)
{
  const std::string background_script = R"(
    local key = "hello"
    local value = "from background"
    RGW[key] = value
  )";

  TestBackground lua_background(background_script);
  lua_background.start();
  std::this_thread::sleep_for(wait_time);

  const std::string request_script = R"(
    local key = "hello"
    assert(RGW[key] == "from background") 
    local value = "from request"
    RGW[key] = value
  )";

  DEFINE_REQ_STATE;

  // to make sure test is consistent we have to puase the background
  lua_background.pause();
  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "", request_script, &lua_background);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(lua_background.get_table_value("hello"), "from request");
  // now we resume and let the background set the value
  lua_background.resume(nullptr);
  std::this_thread::sleep_for(wait_time);
  EXPECT_EQ(lua_background.get_table_value("hello"), "from background");
}

TEST(TestRGWLuaBackground, Pause)
{
  const std::string script = R"(
    local key = "hello"
    local value = "1"
    if RGW[key] then
      RGW[key] = value..RGW[key]
    else
      RGW[key] = value
    end
  )";

  TestBackground lua_background(script);
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  const auto value_len = lua_background.get_table_value("hello").size();
  EXPECT_GT(value_len, 0);
  lua_background.pause();
  std::this_thread::sleep_for(wait_time);
  // no change in len
  EXPECT_EQ(value_len, lua_background.get_table_value("hello").size());
}

TEST(TestRGWLuaBackground, PauseWhileReading)
{
  const std::string script = R"(
    local key = "hello"
    local value = "world"
    RGW[key] = value
    if RGW[key] then
      RGW[key] = value..RGW[key]
    else
      RGW[key] = value
    end
  )";

  constexpr auto long_wait_time = std::chrono::seconds(6);
  TestBackground lua_background(script, 2);
  lua_background.start();
  std::this_thread::sleep_for(long_wait_time);
  const auto value_len = lua_background.get_table_value("hello").size();
  EXPECT_GT(value_len, 0);
  lua_background.pause();
  std::this_thread::sleep_for(long_wait_time);
  // one execution might occur after pause
  EXPECT_TRUE(value_len + 1 >= lua_background.get_table_value("hello").size());
}

TEST(TestRGWLuaBackground, ReadWhilePaused)
{
  const std::string script = R"(
    local key = "hello"
    local value = "world"
    RGW[key] = value
  )";

  TestBackground lua_background(script);
  lua_background.pause();
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  EXPECT_EQ(lua_background.get_table_value("hello"), "");
  lua_background.resume(nullptr);
  std::this_thread::sleep_for(wait_time);
  EXPECT_EQ(lua_background.get_table_value("hello"), "world");
}

TEST(TestRGWLuaBackground, PauseResume)
{
  const std::string script = R"(
    local key = "hello"
    local value = "1"
    if RGW[key] then
      RGW[key] = value..RGW[key]
    else
      RGW[key] = value
    end
  )";

  TestBackground lua_background(script);
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  const auto value_len = lua_background.get_table_value("hello").size();
  EXPECT_GT(value_len, 0);
  lua_background.pause();
  std::this_thread::sleep_for(wait_time);
  // no change in len
  EXPECT_EQ(value_len, lua_background.get_table_value("hello").size());
  lua_background.resume(nullptr);
  std::this_thread::sleep_for(wait_time);
  // should be a change in len
  EXPECT_GT(lua_background.get_table_value("hello").size(), value_len);
}

TEST(TestRGWLuaBackground, MultipleStarts)
{
  const std::string script = R"(
    local key = "hello"
    local value = "1"
    if RGW[key] then
      RGW[key] = value..RGW[key]
    else
      RGW[key] = value
    end
  )";

  TestBackground lua_background(script);
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  const auto value_len = lua_background.get_table_value("hello").size();
  EXPECT_GT(value_len, 0);
  lua_background.start();
  lua_background.shutdown();
  lua_background.shutdown();
  std::this_thread::sleep_for(wait_time);
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  // should be a change in len
  EXPECT_GT(lua_background.get_table_value("hello").size(), value_len);
}

