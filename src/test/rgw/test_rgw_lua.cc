#include <gtest/gtest.h>
#include "common/async/context_pool.h"
#include "common/ceph_context.h"
#include "rgw_common.h"
#include "rgw_auth_registry.h"
#include "rgw_process_env.h"
#include "rgw_sal_rados.h"
#include "rgw_lua_request.h"
#include "rgw_lua_background.h"
#include "rgw_lua_data_filter.h"
#include "rgw_sal_config.h"

using namespace std;
using namespace rgw;
using boost::container::flat_set;
using rgw::auth::Identity;
using rgw::auth::Principal;

DoutPrefix init_dp{g_ceph_context, ceph_subsys_rgw, "Initializing test_rgw_lua"};

static auto init_configstore() {
  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  std::unique_ptr<rgw::sal::ConfigStore> cfgstore
    = DriverManager::create_config_store(&init_dp, config_store_type);
  if (!cfgstore) {
    std::cerr << "Unable to initialize config store" << std::endl;
    abort();
  }
  assert(cfgstore);
  return cfgstore;
}

static auto get_configstore() {
  static auto configstore = init_configstore();
  return configstore.get();
}

static auto init_site() {
  rgw::SiteConfig site;
  auto r = site.load(&init_dp, null_yield, get_configstore());
  if (r < 0) {
    std::cerr << "Unable to initialize SiteConfig: r=" << r << std::endl;
    abort();
  }
  return site;
}

static auto& get_site() {
  static auto site = init_site();
  return site;
}

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

class TestUser : public sal::StoreUser {
public:
  virtual std::unique_ptr<User> clone() override {
    return std::unique_ptr<User>(new TestUser(*this));
  }

  virtual int list_buckets(const DoutPrefixProvider *dpp, const string&, const string&, uint64_t, bool, sal::BucketList& results, optional_yield y) {
    return 0;
  }

  virtual int read_attrs(const DoutPrefixProvider *dpp, optional_yield y) override {
    return 0;
  }

  virtual int read_stats(const DoutPrefixProvider *dpp, optional_yield y, RGWStorageStats* stats, ceph::real_time *last_stats_sync, ceph::real_time *last_stats_update) override {
    return 0;
  }

  virtual int read_stats_async(const DoutPrefixProvider *dpp, boost::intrusive_ptr<sal::ReadStatsCB> cb) override {
    return 0;
  }

  virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) override {
    return 0;
  }

  virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries, bool *is_truncated, RGWUsageIter& usage_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage) override {
    return 0;
  }

  virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y) override {
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
  virtual int verify_mfa(const std::string& mfa_str, bool* verified, const DoutPrefixProvider* dpp, optional_yield y) override {
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

tracing::Tracer tracer;

inline std::unique_ptr<sal::RadosStore> make_store() {
  auto context_pool = std::make_unique<ceph::async::io_context_pool>(
    g_cct->_conf->rgw_thread_pool_size);

  struct StoreBundle : public sal::RadosStore {
    std::unique_ptr<ceph::async::io_context_pool> context_pool;
    StoreBundle(std::unique_ptr<ceph::async::io_context_pool> context_pool_)
      : sal::RadosStore(*context_pool_.get()),
        context_pool(std::move(context_pool_)) {
      setRados(new RGWRados);
    }
    virtual ~StoreBundle() = default;
  };
  return std::make_unique<StoreBundle>(std::move(context_pool));
};

#define DEFINE_REQ_STATE RGWProcessEnv pe; \
  auto store = make_store();                   \
  pe.lua.manager = store->get_lua_manager(""); \
  RGWEnv e; \
  req_state s(g_cct, pe, &e, 0);

#define INIT_TRACE tracer.init(g_cct, "test"); \
                   s.trace = tracer.start_trace("test", true);

TEST(TestRGWLua, EmptyScript)
{
  const std::string script;

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, SyntaxError)
{
  const std::string script = R"(
    if 3 < 5 then
      RGWDebugLog("missing 'end'")
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, -1);
}

TEST(TestRGWLua, Hello)
{
  const std::string script = R"(
    RGWDebugLog("hello from lua")
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, RGWDebugLogNumber)
{
  const std::string script = R"(
    RGWDebugLog(1234567890)
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, RGWDebugNil)
{
  const std::string script = R"(
    RGWDebugLog(nil)
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_NE(rc, 0);
}

TEST(TestRGWLua, InvalidField)
{
  const std::string script = R"(
    RGWDebugLog(Request.Kaboom)
  )";

  DEFINE_REQ_STATE;
  s.host_id = "foo";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, -1);
}

TEST(TestRGWLua, InvalidSubField)
{
  const std::string script = R"(
    RGWDebugLog(Request.Error.Kaboom)
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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
    assert(Request.Bucket.ZoneGroupId)
    assert(Request.Bucket.CreationTime)
    assert(Request.Bucket.MTime)
    assert(Request.Bucket.Quota.MaxSize == -1)
    assert(Request.Bucket.Quota.MaxObjects == -1)
    assert(tostring(Request.Bucket.Quota.Enabled))
    assert(tostring(Request.Bucket.Quota.Rounded))
    assert(Request.Bucket.User.Id == "myuser")
    assert(Request.Bucket.User.Tenant == "mytenant")
  )";

  DEFINE_REQ_STATE;

  RGWBucketInfo info;
  info.bucket.tenant = "mytenant";
  info.bucket.name = "myname";
  info.bucket.marker = "mymarker";
  info.bucket.bucket_id = "myid";
  info.owner.id = "myuser";
  info.owner.tenant = "mytenant";
  s.bucket.reset(new sal::RadosBucket(nullptr, info));

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, WriteBucket)
{
  const std::string script = R"(
    assert(Request.Bucket)
    assert(Request.Bucket.Name == "myname")
    Request.Bucket.Name = "othername"
  )";

  DEFINE_REQ_STATE;
  s.init_state.url_bucket = "myname";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
  ASSERT_EQ(s.init_state.url_bucket, "othername");
}

TEST(TestRGWLua, WriteBucketFail)
{
  const std::string script = R"(
    assert(Request.Bucket)
    assert(Request.Bucket.Name == "myname")
    Request.Bucket.Name = "othername"
  )";

  DEFINE_REQ_STATE;
  rgw_bucket b;
  b.name = "myname";
  s.bucket.reset(new sal::RadosBucket(nullptr, b));

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_NE(rc, 0);
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

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, TagsNotWriteable)
{
  const std::string script = R"(
    Request.Tags["hello"] = "goodbye"
  )";

  DEFINE_REQ_STATE;
  s.tagset.add_tag("hello", "world");

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, WriteMetadata)
{
  const std::string script = R"(
    -- change existing entry
    Request.HTTP.Metadata["hello"] = "earth"
    -- add new entry
    Request.HTTP.Metadata["goodbye"] = "mars"
    -- delete existing entry
    Request.HTTP.Metadata["foo"] = nil
    -- delete missing entry
    Request.HTTP.Metadata["venus"] = nil

    assert(Request.HTTP.Metadata["hello"] == "earth")
    assert(Request.HTTP.Metadata["goodbye"] == "mars")
    assert(Request.HTTP.Metadata["foo"] == nil)
    assert(Request.HTTP.Metadata["venus"] == nil)
  )";

  DEFINE_REQ_STATE;
  s.info.x_meta_map["hello"] = "world";
  s.info.x_meta_map["foo"] = "bar";
  s.info.x_meta_map["ka"] = "boom";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, MetadataIterateWrite)
{
  const std::string script = R"(
    assert(#Request.HTTP.Metadata == 7)
    counter = 0
    for k,v in pairs(Request.HTTP.Metadata) do
      counter = counter + 1
      if tostring(k) == "c" then
        Request.HTTP.Metadata["c"] = nil
        print("'c' is deleted and iterator moves")
        assert(tostring(k) ~= "c")
      end
    end
    assert(counter == 6)
    counter = 0
    for k,v in pairs(Request.HTTP.Metadata) do
      counter = counter + 1
      if tostring(k) == "d" then
        Request.HTTP.Metadata["e"] = nil
        print("'e' is deleted")
      end
      assert(tostring(k) ~= "e")
    end
    assert(counter == 5)
  )";

  DEFINE_REQ_STATE;
  s.info.x_meta_map["a"] = "1";
  s.info.x_meta_map["b"] = "2";
  s.info.x_meta_map["c"] = "3";
  s.info.x_meta_map["d"] = "4";
  s.info.x_meta_map["e"] = "5";
  s.info.x_meta_map["f"] = "6";
  s.info.x_meta_map["g"] = "7";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
  ASSERT_EQ(s.info.x_meta_map.count("c"), 0);
}

TEST(TestRGWLua, MetadataIterator)
{

  DEFINE_REQ_STATE;
  s.info.x_meta_map["a"] = "1";
  s.info.x_meta_map["b"] = "2";
  s.info.x_meta_map["c"] = "3";
  s.info.x_meta_map["d"] = "4";
  s.info.x_meta_map["e"] = "5";
  s.info.x_meta_map["f"] = "6";
  s.info.x_meta_map["g"] = "7";
  
  std::string script = R"(
    -- nested loop
    counter = 0
    for k1,v1 in pairs(Request.HTTP.Metadata) do
      assert(k1)
      assert(v2)
      for k2,v2 in pairs(Request.HTTP.Metadata) do
        print("we should not reach this line")
        print(k2, v2)
      end
      counter = counter + 1
    end
  )";

  auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_NE(rc, 0);
  
  script = R"(
    -- break loop
    counter = 0
    for k,v in pairs(Request.HTTP.Metadata) do
      counter = counter + 1
      assert(k)
      assert(v)
      if counter == 3 then
        break
      end
      counter = counter + 1
    end
    -- full loop
    for k,v in pairs(Request.HTTP.Metadata) do
        print("we should not reach this line")
      print(k,v)
    end
  )";

  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_NE(rc, 0);
  
  script = R"(
    -- 2 loops
    counter = 0
    for k,v in pairs(Request.HTTP.Metadata) do
     assert(k)
     assert(v)
     counter = counter + 1
    end
    assert(counter == #Request.HTTP.Metadata)
    counter = 0
    for k,v in pairs(Request.HTTP.Metadata) do
      assert(k)
      assert(v)
      counter = counter + 1
    end
    assert(counter == #Request.HTTP.Metadata)
  )";

  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, Acl)
{
  const std::string script = R"(
    function print_grant(k, g)
      print("Grant Key: " .. tostring(k))
      print("Grant Type: " .. g.Type)
      if (g.GroupType) then
        print("Grant Group Type: " .. g.GroupType)
      end
      if (g.Referer) then
        print("Grant Referer: " .. g.Referer)
      end
      if (g.User) then
        print("Grant User.Tenant: " .. g.User.Tenant)
        print("Grant User.Id: " .. g.User.Id)
      end
    end

    assert(Request.UserAcl.Owner.DisplayName == "jack black", Request.UserAcl.Owner.DisplayName)
    assert(Request.UserAcl.Owner.User.Id == "black", Request.UserAcl.Owner.User.Id)
    assert(Request.UserAcl.Owner.User.Tenant == "jack", Request.UserAcl.Owner.User.Tenant)
    assert(#Request.UserAcl.Grants == 7)
    print_grant("", Request.UserAcl.Grants[""])
    for k, v in pairs(Request.UserAcl.Grants) do
      if tostring(k) == "john$doe" then
        assert(v.Permission == 4)
      elseif tostring(k) == "jane$doe" then
        assert(v.Permission == 1)
      elseif tostring(k) == "kill$bill" then
        assert(v.Permission == 6 or v.Permission == 7)
      elseif tostring(k) ~= "" then
        assert(false)
      end
    end
  )";

  DEFINE_REQ_STATE;
  const ACLOwner owner{
    .id = rgw_user("jack", "black"),
    .display_name = "jack black"
  };
  s.user_acl.set_owner(owner);
  ACLGrant grant1, grant2, grant3, grant4, grant5, grant6_1, grant6_2;
  grant1.set_canon(rgw_user("jane", "doe"), "her grant", 1);
  grant2.set_group(ACL_GROUP_ALL_USERS ,2);
  grant3.set_referer("http://localhost/ref2", 3);
  grant4.set_canon(rgw_user("john", "doe"), "his grant", 4);
  grant5.set_group(ACL_GROUP_AUTHENTICATED_USERS, 5);
  grant6_1.set_canon(rgw_user("kill", "bill"), "his grant", 6);
  grant6_2.set_canon(rgw_user("kill", "bill"), "her grant", 7);
  s.user_acl.get_acl().add_grant(grant1);
  s.user_acl.get_acl().add_grant(grant2);
  s.user_acl.get_acl().add_grant(grant3);
  s.user_acl.get_acl().add_grant(grant4);
  s.user_acl.get_acl().add_grant(grant5);
  s.user_acl.get_acl().add_grant(grant6_1);
  s.user_acl.get_acl().add_grant(grant6_2);
  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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

  s.user.reset(new sal::RadosUser(nullptr, rgw_user("mytenant", "myid")));

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, UseFunction)
{
	const std::string script = R"(
		function print_owner(owner)
  		print("Owner Display Name: " .. owner.DisplayName)
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
  s.owner.display_name = "user two";
  s.owner.id = rgw_user("tenant2", "user2");
  s.user_acl.get_owner().display_name = "user three";
  s.user_acl.get_owner().id = rgw_user("tenant3", "user3");
  s.bucket_acl.get_owner().display_name = "user four";
  s.bucket_acl.get_owner().id = rgw_user("tenant4", "user4");
  s.object_acl.get_owner().display_name = "user five";
  s.object_acl.get_owner().id = rgw_user("tenant5", "user5");

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, NotAllowedInLib)
{
  const std::string script = R"(
    os.clock() -- this should be ok
    os.exit()  -- this should fail (os.exit() is removed)
  )";

  DEFINE_REQ_STATE;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
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

  auto rc = lua::request::execute(store.get(), nullptr, &olog, &s, nullptr, script);
  EXPECT_EQ(rc, 0);
  EXPECT_FALSE(olog.logged); // don't log http_ret=200
 
	s.err.http_ret = 400;
  rc = lua::request::execute(store.get(), nullptr, &olog, &s, nullptr, script);
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
  TestBackground(sal::RadosStore* store, const std::string& script, rgw::sal::LuaManager* manager, unsigned read_time = 0) : 
    rgw::lua::Background(store, 
        g_cct, 
        manager,
        1 /* run every second */),
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
  auto store = make_store();
  auto manager = store->get_lua_manager("");
  {
    // ctr and dtor without running
    TestBackground lua_background(store.get(), "", manager.get());
  }
  {
    // ctr and dtor with running
    TestBackground lua_background(store.get(), "", manager.get());
    lua_background.start();
  }
}


constexpr auto wait_time = std::chrono::seconds(3);

template<typename T>
const T& get_table_value(const TestBackground& b, const std::string& index) {
  try {
    return std::get<T>(b.get_table_value(index));
  } catch (std::bad_variant_access const& ex) {
    std::cout << "expected RGW[" << index << "] to be: " << typeid(T).name() << std::endl;
    throw(ex);
  }
}

TEST(TestRGWLuaBackground, Script)
{
  const std::string script = R"(
    local key = "hello"
    local value = "world"
    RGW[key] = value
  )";

  auto store = make_store();
  auto manager = store->get_lua_manager("");
  TestBackground lua_background(store.get(), script, manager.get());
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "hello"), "world");
}

TEST(TestRGWLuaBackground, RequestScript)
{
  const std::string background_script = R"(
    local key = "hello"
    local value = "from background"
    RGW[key] = value
  )";

  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), background_script, pe.lua.manager.get());
  lua_background.start();
  std::this_thread::sleep_for(wait_time);

  const std::string request_script = R"(
    local key = "hello"
    assert(RGW[key] == "from background") 
    local value = "from request"
    RGW[key] = value
  )";

  pe.lua.background = &lua_background;

  // to make sure test is consistent we have to pause the background
  lua_background.pause();
  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "hello"), "from request");
  // now we resume and let the background set the value
  lua_background.resume(store.get());
  std::this_thread::sleep_for(wait_time);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "hello"), "from background");
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

  auto store = make_store();
  auto manager = store->get_lua_manager("");
  TestBackground lua_background(store.get(), script, manager.get());
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  const auto value_len = get_table_value<std::string>(lua_background, "hello").size();
  EXPECT_GT(value_len, 0);
  lua_background.pause();
  std::this_thread::sleep_for(wait_time);
  // no change in len
  EXPECT_EQ(value_len, get_table_value<std::string>(lua_background, "hello").size());
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

  auto store = make_store();
  auto manager = store->get_lua_manager("");
  TestBackground lua_background(store.get(), script, manager.get(), 2);
  lua_background.start();
  constexpr auto long_wait_time = std::chrono::seconds(6);
  std::this_thread::sleep_for(long_wait_time);
  const auto value_len = get_table_value<std::string>(lua_background, "hello").size();
  EXPECT_GT(value_len, 0);
  lua_background.pause();
  std::this_thread::sleep_for(long_wait_time);
  // one execution might occur after pause
  EXPECT_TRUE(value_len + 1 >= get_table_value<std::string>(lua_background, "hello").size());
}

TEST(TestRGWLuaBackground, ReadWhilePaused)
{
  const std::string script = R"(
    local key = "hello"
    local value = "world"
    RGW[key] = value
  )";

  auto store = make_store();
  auto manager = store->get_lua_manager("");
  TestBackground lua_background(store.get(), script, manager.get());
  lua_background.pause();
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "hello"), "");
  lua_background.resume(store.get());
  std::this_thread::sleep_for(wait_time);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "hello"), "world");
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

  auto store = make_store();
  auto manager = store->get_lua_manager("");
  TestBackground lua_background(store.get(), script, manager.get());
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  const auto value_len = get_table_value<std::string>(lua_background, "hello").size();
  EXPECT_GT(value_len, 0);
  lua_background.pause();
  std::this_thread::sleep_for(wait_time);
  // no change in len
  EXPECT_EQ(value_len, get_table_value<std::string>(lua_background, "hello").size());
  lua_background.resume(store.get());
  std::this_thread::sleep_for(wait_time);
  // should be a change in len
  EXPECT_GT(get_table_value<std::string>(lua_background, "hello").size(), value_len);
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

  auto store = make_store();
  auto manager = store->get_lua_manager("");
  TestBackground lua_background(store.get(), script, manager.get());
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  const auto value_len = get_table_value<std::string>(lua_background, "hello").size();
  EXPECT_GT(value_len, 0);
  lua_background.start();
  lua_background.shutdown();
  lua_background.shutdown();
  std::this_thread::sleep_for(wait_time);
  lua_background.start();
  std::this_thread::sleep_for(wait_time);
  // should be a change in len
  EXPECT_GT(get_table_value<std::string>(lua_background, "hello").size(), value_len);
}

TEST(TestRGWLuaBackground, TableValues)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  const std::string request_script = R"(
    RGW["key1"] = "string value"
    RGW["key2"] = 42
    RGW["key3"] = 42.2
    RGW["key4"] = true
  )";

  pe.lua.background = &lua_background;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key1"), "string value");
  EXPECT_EQ(get_table_value<long long int>(lua_background, "key2"), 42);
  EXPECT_EQ(get_table_value<double>(lua_background, "key3"), 42.2);
  EXPECT_TRUE(get_table_value<bool>(lua_background, "key4"));
}

TEST(TestRGWLuaBackground, TablePersist)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  std::string request_script = R"(
    RGW["key1"] = "string value"
    RGW["key2"] = 42
  )";

  pe.lua.background = &lua_background;

  auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key1"), "string value");
  EXPECT_EQ(get_table_value<long long int>(lua_background, "key2"), 42);
  
  request_script = R"(
    RGW["key3"] = RGW["key1"]
    RGW["key4"] = RGW["key2"]
  )";
  
  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key1"), "string value");
  EXPECT_EQ(get_table_value<long long int>(lua_background, "key2"), 42);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key3"), "string value");
  EXPECT_EQ(get_table_value<long long int>(lua_background, "key4"), 42);
}

TEST(TestRGWLuaBackground, TableValuesFromRequest)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());
  lua_background.start();

  const std::string request_script = R"(
    RGW["key1"] = Request.Response.RGWCode
    RGW["key2"] = Request.Response.Message
    RGW["key3"] = Request.Response.RGWCode*0.1
    RGW["key4"] = Request.Tags["key1"] == Request.Tags["key2"] 
  )";

  pe.lua.background = &lua_background;

  s.tagset.add_tag("key1", "val1");
  s.tagset.add_tag("key2", "val1");
  s.err.ret = -99;
  s.err.message = "hi";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(get_table_value<long long int>(lua_background, "key1"), -99);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key2"), "hi");
  EXPECT_EQ(get_table_value<double>(lua_background, "key3"), -9.9);
  EXPECT_EQ(get_table_value<bool>(lua_background, "key4"), true);
}

TEST(TestRGWLuaBackground, TableInvalidValue)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());
  lua_background.start();

  const std::string request_script = R"(
    RGW["key1"] = "val1"
    RGW["key2"] = 42
    RGW["key3"] = 42.2
    RGW["key4"] = true
    RGW["key5"] = Request.Tags
  )";

  pe.lua.background = &lua_background;
  s.tagset.add_tag("key1", "val1");
  s.tagset.add_tag("key2", "val2");

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_NE(rc, 0);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key1"), "val1");
  EXPECT_EQ(get_table_value<long long int>(lua_background, "key2"), 42);
  EXPECT_EQ(get_table_value<double>(lua_background, "key3"), 42.2);
  EXPECT_EQ(get_table_value<bool>(lua_background, "key4"), true);
}

TEST(TestRGWLuaBackground, TableErase)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  std::string request_script = R"(
    RGW["size"] = 0
    RGW["key1"] = "string value"
    RGW["key2"] = 42
    RGW["key3"] = "another string value"
    RGW["size"] = #RGW
  )";

  pe.lua.background = &lua_background;

  auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key1"), "string value");
  EXPECT_EQ(get_table_value<long long int>(lua_background, "key2"), 42);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key3"), "another string value");
  EXPECT_EQ(get_table_value<long long int>(lua_background, "size"), 4);
  
  request_script = R"(
    -- erase key1
    RGW["key1"] = nil
    -- following should be a no op
    RGW["key4"] = nil
    RGW["size"] = #RGW
  )";
  
  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key1"), "");
  EXPECT_EQ(get_table_value<long long int>(lua_background, "key2"), 42);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key3"), "another string value");
  EXPECT_EQ(get_table_value<long long int>(lua_background, "size"), 3);
}

TEST(TestRGWLuaBackground, TableIterate)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  const std::string request_script = R"(
    RGW["key1"] = "string value"
    RGW["key2"] = 42
    RGW["key3"] = 42.2
    RGW["key4"] = true
    RGW["size"] = 0
    for k, v in pairs(RGW) do
      RGW["size"] = RGW["size"] + 1
    end
  )";

  pe.lua.background = &lua_background;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(get_table_value<std::string>(lua_background, "key1"), "string value");
  EXPECT_EQ(get_table_value<long long int>(lua_background, "key2"), 42);
  EXPECT_EQ(get_table_value<double>(lua_background, "key3"), 42.2);
  EXPECT_TRUE(get_table_value<bool>(lua_background, "key4"));
  EXPECT_EQ(get_table_value<long long int>(lua_background, "size"), 5);
}

TEST(TestRGWLuaBackground, TableIterateWrite)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  const std::string request_script = R"(
    RGW["a"] = 1
    RGW["b"] = 2
    RGW["c"] = 3
    RGW["d"] = 4
    RGW["e"] = 5
    counter = 0 
    for k, v in pairs(RGW) do
      counter = counter + 1
      if tostring(k) == "c" then
        RGW["c"] = nil
        print("'c' is deleted and iterator moves")
        assert(tostring(k) ~= "c")
      end
    end
    assert(counter == 4)
  )";

  pe.lua.background = &lua_background;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
  EXPECT_EQ(lua_background.get_table_value("c"), TestBackground::empty_table_value);
}

TEST(TestRGWLuaBackground, TableIncrement)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  const std::string request_script = R"(
    RGW["key1"] = 42
    RGW["key2"] = 42.2
    RGW.increment("key1")
    assert(RGW["key1"] == 43)
    RGW.increment("key2")
    assert(RGW["key2"] == 43.2)
  )";

  pe.lua.background = &lua_background;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLuaBackground, TableIncrementBy)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  const std::string request_script = R"(
    RGW["key1"] = 42
    RGW["key2"] = 42.2
    RGW.increment("key1", 10)
    assert(RGW["key1"] == 52)
    RGW.increment("key2", 10)
    assert(RGW["key2"] == 52.2)
    RGW.increment("key1", 0.2)
    assert(RGW["key1"] == 52.2)
  )";

  pe.lua.background = &lua_background;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLuaBackground, TableDecrement)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  const std::string request_script = R"(
    RGW["key1"] = 42
    RGW["key2"] = 42.2
    RGW.decrement("key1")
    assert(RGW["key1"] == 41)
    RGW.decrement("key2")
    assert(RGW["key2"] == 41.2)
  )";

  pe.lua.background = &lua_background;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLuaBackground, TableDecrementBy)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  const std::string request_script = R"(
    RGW["key1"] = 42
    RGW["key2"] = 42.2
    RGW.decrement("key1", 10)
    assert(RGW["key1"] == 32)
    RGW.decrement("key2", 10)
    assert(RGW["key2"] == 32.2)
    RGW.decrement("key1", 0.8)
    assert(RGW["key1"] == 31.2)
  )";

  pe.lua.background = &lua_background;

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLuaBackground, TableIncrementValueError)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  std::string request_script = R"(
    -- cannot increment string values
    RGW["key1"] = "hello"
    RGW.increment("key1")
  )";

  pe.lua.background = &lua_background;

  auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_NE(rc, 0);
  
  request_script = R"(
    -- cannot increment bool values
    RGW["key1"] = true
    RGW.increment("key1")
  )";

  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_NE(rc, 0);
  
  request_script = R"(
    -- cannot increment by string values
    RGW["key1"] = 99
    RGW.increment("key1", "kaboom")
  )";

  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_NE(rc, 0);
}

TEST(TestRGWLuaBackground, TableIncrementError)
{
  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());

  std::string request_script = R"(
    -- missing argument
    RGW["key1"] = 11
    RGW.increment()
  )";

  pe.lua.background = &lua_background;

  auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_NE(rc, 0);
  
  request_script = R"(
    -- used as settable field
    RGW.increment = 11
  )";

  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, request_script);
  ASSERT_NE(rc, 0);
}

TEST(TestRGWLua, TracingSetAttribute)
{
  const std::string script = R"(
    Request.Trace.SetAttribute("str-attr", "value")
    Request.Trace.SetAttribute("int-attr", 42)
    Request.Trace.SetAttribute("double-attr", 42.5)
  )";

  DEFINE_REQ_STATE;
  INIT_TRACE;
  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, TracingSetBadAttribute)
{
  const std::string script = R"(
    Request.Trace.SetAttribute("attr", nil)
  )";

  DEFINE_REQ_STATE;
  INIT_TRACE;
  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  #ifdef HAVE_JAEGER
   ASSERT_NE(rc, 0);
  #else
   ASSERT_EQ(rc, 0);
  #endif
}

TEST(TestRGWLua, TracingAddEvent)
{
  const std::string script = R"(
    event_attrs = {}
    event_attrs["x"] = "value-x"
    event_attrs[42] = 42
    event_attrs[42.5] = 42.5
    event_attrs["y"] = "value-y"

    Request.Trace.AddEvent("my_event", event_attrs)
  )";

  DEFINE_REQ_STATE;
  INIT_TRACE;
  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, Data)
{
  const std::string script = R"(
    local expected = "The quick brown fox jumps over the lazy dog"
    local actual = ""
    RGW["key1"] = 0
    
    for i, c in pairs(Data) do
      actual = actual .. c
      RGW.increment("key1")
    end 
    assert(expected == actual)
    assert(#Data == #expected);
    assert(RGW["key1"] == #Data)
    assert(Request.RGWId == "foo")
    assert(Offset == 12345678)
  )";

  DEFINE_REQ_STATE;
  TestBackground lua_background(store.get(), "", pe.lua.manager.get());
  s.host_id = "foo";
  pe.lua.background = &lua_background;
  lua::RGWObjFilter filter(&s, script);
  bufferlist bl;
  bl.append("The quick brown fox jumps over the lazy dog");
  off_t offset = 12345678;
  const auto rc = filter.execute(bl, offset, "put_obj");
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, WriteDataFail)
{
  const std::string script = R"(
    Data[1] = "h"
    Data[2] = "e"
    Data[3] = "l"
    Data[4] = "l"
    Data[5] = "o"
  )";

  DEFINE_REQ_STATE;
  lua::RGWObjFilter filter(&s, script);
  bufferlist bl;
  bl.append("The quick brown fox jumps over the lazy dog");
  const auto rc = filter.execute(bl, 0, "put_obj");
  ASSERT_NE(rc, 0);
}

TEST(TestRGWLua, MemoryLimit)
{
  std::string script = "print(\"hello world\")";

  DEFINE_REQ_STATE;
  
  // memory should be sufficient
  s.cct->_conf->rgw_lua_max_memory_per_state = 1024*32;
  auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
  
  // no memory limit
  s.cct->_conf->rgw_lua_max_memory_per_state = 0;
  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
  
  // not enough memory to start lua
  s.cct->_conf->rgw_lua_max_memory_per_state = 2048;
  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_NE(rc, 0);

  // not enough memory for initial setup
  s.cct->_conf->rgw_lua_max_memory_per_state = 1024*16;
  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_NE(rc, 0);
  
  // not enough memory for the script
  script = R"(
    t = {}
    for i = 1,1000 do
      table.insert(t, i)
    end
  )";
  s.cct->_conf->rgw_lua_max_memory_per_state = 1024*32;
  rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_NE(rc, 0);
}

TEST(TestRGWLua, DifferentContextUser)
{
  const std::string script = R"(
    assert(Request.User.Id == "user1")
    assert(Request.User.Tenant == "tenant1")
    assert(Request.Bucket.User.Id == "user2")
    assert(Request.Bucket.User.Tenant == "tenant2")
    local u1 = Request.User
    local u2 = Request.Bucket.User
    assert(u1.Id == "user1")
    assert(u2.Id == "user2")
    assert(u1.Tenant == "tenant1")
    assert(u2.Tenant == "tenant2")
  )";

  DEFINE_REQ_STATE;

  s.user.reset(new sal::RadosUser(nullptr, rgw_user("tenant1", "user1")));
  RGWBucketInfo info;
  info.bucket.name = "bucket1";
  info.owner.id = "user2";
  info.owner.tenant = "tenant2";
  s.bucket.reset(new sal::RadosBucket(nullptr, info));

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

TEST(TestRGWLua, NestedLoop)
{
  const std::string script = R"(
  for k1, v1 in pairs(Request.Environment) do
    assert(k1)
    assert(v1)
    for k2, v2 in pairs(Request.HTTP.Metadata) do
      assert(k2)
      assert(v2)
    end
  end
  )";

  DEFINE_REQ_STATE;
  s.env.emplace("1", "a");
  s.env.emplace("2", "b");
  s.env.emplace("3", "c");
  s.info.x_meta_map["11"] = "aa";
  s.info.x_meta_map["22"] = "bb";
  s.info.x_meta_map["33"] = "cc";
  s.info.x_meta_map["44"] = "dd";

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, nullptr, script);
  ASSERT_EQ(rc, 0);
}

