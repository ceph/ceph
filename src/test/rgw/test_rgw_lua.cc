#include <gtest/gtest.h>
#include "common/ceph_context.h"
#include "rgw/rgw_common.h"
#include "rgw/rgw_auth.h"
#include "rgw/rgw_process.h"
#include "rgw/rgw_sal_rados.h"
#include "rgw/rgw_lua_request.h"

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

class TestRGWUser : public sal::RGWUser {
public:
  virtual int list_buckets(const DoutPrefixProvider *dpp, const string&, const string&, uint64_t, bool, sal::RGWBucketList&, optional_yield y) override {
    return 0;
  }

  virtual sal::RGWBucket* create_bucket(rgw_bucket& bucket, ceph::real_time creation_time) override {
    return nullptr;
  }

  virtual int load_by_id(const DoutPrefixProvider *dpp, optional_yield y) override {
    return 0;
  }

  virtual ~TestRGWUser() = default;
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

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);

CctCleaner cleaner(cct);

TEST(TestRGWLua, EmptyScript)
{
  const std::string script;

  RGWEnv e;
  uint64_t id = 0;
  req_state s(cct, &e, id); 

  const auto rc = lua::request::execute(nullptr, nullptr, nullptr, &s, "", script);
  ASSERT_EQ(rc, 0);
}

#define DEFINE_REQ_STATE RGWEnv e; req_state s(cct, &e, 0);

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

TEST(TestRGWLua, SetRGWId)
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
  s.bucket.reset(new sal::RGWRadosBucket(nullptr, b));

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
  s.user_acl.reset(new RGWAccessControlPolicy(cct));
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
  s.bucket.reset(new sal::RGWRadosBucket(nullptr, b));

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
#include <sys/socket.h>
#include <stdlib.h>

bool unix_socket_client_ended_ok = false;

void unix_socket_client(const std::string& path) {
  int fd;
  // create the socket
  if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    std::cout << "unix socket error: " << errno << std::endl;
    return;
  }
  // set the path
  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path)-1);

	// let the socket be created by the "rgw" side
	std::this_thread::sleep_for(std::chrono::seconds(2));
	if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
		std::cout << "unix socket connect error: " << errno << std::endl;
		return;
 	}

  char buff[256];
	int rc;
 	while((rc=read(fd, buff, sizeof(buff))) > 0) {
		std::cout << std::string(buff, rc);
    unix_socket_client_ended_ok = true;
  }
}

TEST(TestRGWLua, OpsLog)
{
	const std::string unix_socket_path = "./aSocket.sock";
	unlink(unix_socket_path.c_str());

	std::thread unix_socket_thread(unix_socket_client, unix_socket_path);

  const std::string script = R"(
		if Request.Response.HTTPStatusCode == 200 then
			assert(Request.Response.Message == "Life is great")
		else 
      assert(Request.Bucket)
    	assert(Request.Log() == 0)
		end
  )";

  auto store = std::unique_ptr<sal::RGWRadosStore>(new sal::RGWRadosStore);
  store->setRados(new RGWRados);
  auto olog = std::unique_ptr<OpsLogSocket>(new OpsLogSocket(cct, 1024));
  ASSERT_TRUE(olog->init(unix_socket_path));

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
  s.bucket.reset(new sal::RGWRadosBucket(nullptr, b));
  s.bucket_name = "name";
	s.enable_ops_log = true;
	s.enable_usage_log = false;
	s.user.reset(new TestRGWUser());
  TestAccounter ac;
  s.cio = &ac; 
	s.cct->_conf->rgw_ops_log_rados	= false;

  s.auth.identity = std::unique_ptr<rgw::auth::Identity>(
                        new FakeIdentity());

  auto rc = lua::request::execute(store.get(), nullptr, olog.get(), &s, "put_obj", script);
  EXPECT_EQ(rc, 0);
 
	s.err.http_ret = 400;
  rc = lua::request::execute(store.get(), nullptr, olog.get(), &s, "put_obj", script);
  EXPECT_EQ(rc, 0);

	// give the socket client time to read
	std::this_thread::sleep_for(std::chrono::seconds(5));
	unix_socket_thread.detach(); // read is stuck there, so we cannot join
  EXPECT_TRUE(unix_socket_client_ended_ok);
}

