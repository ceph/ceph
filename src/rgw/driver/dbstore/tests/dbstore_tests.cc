#include "gtest/gtest.h"
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dbstore.h>
#include <sqliteDB.h>
#include "rgw_common.h"

using namespace std;
using DB = rgw::store::DB;

vector<const char*> args;

namespace gtest {
  class Environment* env;

  class Environment : public ::testing::Environment {
    public:
      Environment(): tenant("default_ns"), db(nullptr),
      db_type("SQLite"), ret(-1) {}

      Environment(string tenantname, string db_typename): 
        tenant(tenantname), db(nullptr),
        db_type(db_typename), ret(-1) {}

      virtual ~Environment() {}

      void SetUp() override {
        cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
            CODE_ENVIRONMENT_DAEMON,
            CINIT_FLAG_NO_DEFAULT_CONFIG_FILE | CINIT_FLAG_NO_MON_CONFIG | CINIT_FLAG_NO_DAEMON_ACTIONS);
        if (!db_type.compare("SQLite")) {
          db = new SQLiteDB(tenant, cct.get());
          ASSERT_TRUE(db != nullptr);
          ret = db->Initialize(logfile, loglevel);
          ASSERT_GE(ret, 0);
        }
      }

      void TearDown() override {
        if (!db)
          return;
        db->Destroy(db->get_def_dpp());
        delete db;
      }

      string tenant;
      DB *db;
      string db_type;
      int ret;
      string logfile = "rgw_dbstore_tests.log";
      int loglevel = 30;
      boost::intrusive_ptr<CephContext> cct;
  };
}

ceph::real_time bucket_mtime = real_clock::now();
string marker1;

class DBGetDataCB : public RGWGetDataCB {
  public:
    bufferlist data_bl;
    off_t data_ofs, data_len;

    int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
      data_bl = bl;
      data_ofs = bl_ofs;
      data_len = bl_len;
      return 0;
    }
};

namespace {

  class DBStoreTest : public ::testing::Test {
    protected:
      int ret;
      DB *db = nullptr;
      string user1 = "user1";
      string user_id1 = "user_id1";
      string bucket1 = "bucket1";
      string object1 = "object1";
      string data = "Hello World";
      DBOpParams GlobalParams = {};
      const DoutPrefixProvider *dpp;

      DBStoreTest() {}
      void SetUp() {
        db = gtest::env->db;
        ASSERT_TRUE(db != nullptr);
        dpp = db->get_def_dpp();
        ASSERT_TRUE(dpp != nullptr);

        GlobalParams.op.user.uinfo.display_name = user1;
        GlobalParams.op.user.uinfo.user_id.id = user_id1;
        GlobalParams.op.bucket.info.bucket.name = bucket1;
        GlobalParams.op.bucket.owner = user_id1;
        GlobalParams.op.obj.state.obj.bucket = GlobalParams.op.bucket.info.bucket;
        GlobalParams.op.obj.state.obj.key.name = object1;
        GlobalParams.op.obj.state.obj.key.instance = "inst1";
        GlobalParams.op.obj.obj_id = "obj_id1";
        GlobalParams.op.obj_data.part_num = 0;

        /* As of now InitializeParams doesnt do anything
         * special based on fop. Hence its okay to do
         * global initialization once.
         */
        ret = db->InitializeParams(dpp, &GlobalParams);
        ASSERT_EQ(ret, 0);
      }

      void TearDown() {
      }

      int write_object(const DoutPrefixProvider *dpp, DBOpParams params) {
        DB::Object op_target(db, params.op.bucket.info,
                             params.op.obj.state.obj);
        DB::Object::Write write_op(&op_target);
        map<string, bufferlist> setattrs;
        ret = write_op.prepare(dpp);
        if (ret)
          return ret;

        write_op.meta.mtime = &bucket_mtime;
        write_op.meta.category = RGWObjCategory::Main;
        write_op.meta.owner = params.op.user.uinfo.user_id;

        bufferlist b1 = params.op.obj.head_data;
        write_op.meta.data = &b1;

        bufferlist b2;
        encode("ACL", b2);
        setattrs[RGW_ATTR_ACL] = b2;

        ret = write_op.write_meta(0, params.op.obj.state.size, b1.length()+1, setattrs);
        return ret;
      }
  };
}

TEST_F(DBStoreTest, InsertUser) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  params.op.user.uinfo.user_id.tenant = "tenant";
  params.op.user.uinfo.user_email = "user1@dbstore.com";
  params.op.user.uinfo.suspended = 123;
  params.op.user.uinfo.max_buckets = 456;
  params.op.user.uinfo.placement_tags.push_back("tags");
  RGWAccessKey k1("id1", "key1");
  RGWAccessKey k2("id2", "key2");
  params.op.user.uinfo.access_keys["id1"] = k1;
  params.op.user.uinfo.access_keys["id2"] = k2;
  params.op.user.user_version.ver = 1;    
  params.op.user.user_version.tag = "UserTAG";    

  ret = db->ProcessOp(dpp, "InsertUser", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, GetUser) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "GetUser", &params);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(params.op.user.uinfo.user_id.tenant, "tenant");
  ASSERT_EQ(params.op.user.uinfo.user_email, "user1@dbstore.com");
  ASSERT_EQ(params.op.user.uinfo.user_id.id, "user_id1");
  ASSERT_EQ(params.op.user.uinfo.suspended, 123);
  ASSERT_EQ(params.op.user.uinfo.max_buckets, 456);
  ASSERT_EQ(params.op.user.uinfo.placement_tags.back(), "tags");
  RGWAccessKey k;
  map<string, RGWAccessKey>::iterator it2 = params.op.user.uinfo.access_keys.begin();
  k = it2->second;
  ASSERT_EQ(k.id, "id1");
  ASSERT_EQ(k.key, "key1");
  it2++;
  k = it2->second;
  ASSERT_EQ(k.id, "id2");
  ASSERT_EQ(k.key, "key2");

}

TEST_F(DBStoreTest, GetUserQuery) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  params.op.query_str = "email";
  params.op.user.uinfo.user_email = "user1@dbstore.com";

  ret = db->ProcessOp(dpp, "GetUser", &params);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(params.op.user.uinfo.user_id.tenant, "tenant");
  ASSERT_EQ(params.op.user.uinfo.user_email, "user1@dbstore.com");
  ASSERT_EQ(params.op.user.uinfo.user_id.id, "user_id1");
  ASSERT_EQ(params.op.user.uinfo.suspended, 123);
  ASSERT_EQ(params.op.user.uinfo.max_buckets, 456);
  ASSERT_EQ(params.op.user.uinfo.placement_tags.back(), "tags");
  RGWAccessKey k;
  map<string, RGWAccessKey>::iterator it2 = params.op.user.uinfo.access_keys.begin();
  k = it2->second;
  ASSERT_EQ(k.id, "id1");
  ASSERT_EQ(k.key, "key1");
  it2++;
  k = it2->second;
  ASSERT_EQ(k.id, "id2");
  ASSERT_EQ(k.key, "key2");

}

TEST_F(DBStoreTest, GetUserQueryByEmail) {
  int ret = -1;
  RGWUserInfo uinfo;
  string email = "user1@dbstore.com";
  map<std::string, bufferlist> attrs;
  RGWObjVersionTracker objv;

  ret = db->get_user(dpp, "email", email, uinfo, &attrs, &objv);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(uinfo.user_id.tenant, "tenant");
  ASSERT_EQ(uinfo.user_email, "user1@dbstore.com");
  ASSERT_EQ(uinfo.user_id.id, "user_id1");
  ASSERT_EQ(uinfo.suspended, 123);
  ASSERT_EQ(uinfo.max_buckets, 456);
  ASSERT_EQ(uinfo.placement_tags.back(), "tags");
  RGWAccessKey k;
  map<string, RGWAccessKey>::iterator it2 = uinfo.access_keys.begin();
  k = it2->second;
  ASSERT_EQ(k.id, "id1");
  ASSERT_EQ(k.key, "key1");
  it2++;
  k = it2->second;
  ASSERT_EQ(k.id, "id2");
  ASSERT_EQ(k.key, "key2");
  ASSERT_EQ(objv.read_version.ver, 1);
}

TEST_F(DBStoreTest, GetUserQueryByAccessKey) {
  int ret = -1;
  RGWUserInfo uinfo;
  string key = "id1";

  ret = db->get_user(dpp, "access_key", key, uinfo, nullptr, nullptr);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(uinfo.user_id.tenant, "tenant");
  ASSERT_EQ(uinfo.user_email, "user1@dbstore.com");
  ASSERT_EQ(uinfo.user_id.id, "user_id1");
  ASSERT_EQ(uinfo.suspended, 123);
  ASSERT_EQ(uinfo.max_buckets, 456);
  ASSERT_EQ(uinfo.placement_tags.back(), "tags");
  RGWAccessKey k;
  map<string, RGWAccessKey>::iterator it2 = uinfo.access_keys.begin();
  k = it2->second;
  ASSERT_EQ(k.id, "id1");
  ASSERT_EQ(k.key, "key1");
  it2++;
  k = it2->second;
  ASSERT_EQ(k.id, "id2");
  ASSERT_EQ(k.key, "key2");
}

TEST_F(DBStoreTest, StoreUser) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  RGWUserInfo uinfo, old_uinfo;
  map<std::string, bufferlist> attrs;
  RGWObjVersionTracker objv_tracker;

  bufferlist attr1, attr2;
  encode("attrs1", attr1);
  attrs["attr1"] = attr1;
  encode("attrs2", attr2);
  attrs["attr2"] = attr2;

  uinfo.user_id.id = "user_id2";
  uinfo.user_id.tenant = "tenant";
  uinfo.user_email = "user2@dbstore.com";
  uinfo.suspended = 123;
  uinfo.max_buckets = 456;
  uinfo.placement_tags.push_back("tags");
  RGWAccessKey k1("id1", "key1");
  RGWAccessKey k2("id2", "key2");
  uinfo.access_keys["id1"] = k1;
  uinfo.access_keys["id2"] = k2;

  /* non exclusive create..should create new one */
  ret = db->store_user(dpp, uinfo, false, &attrs, &objv_tracker, &old_uinfo);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(old_uinfo.user_email, "");
  ASSERT_EQ(objv_tracker.read_version.ver, 1);
  ASSERT_EQ(objv_tracker.read_version.tag, "UserTAG");

  /* invalid version number */
  objv_tracker.read_version.ver = 4;
  ret = db->store_user(dpp, uinfo, true, &attrs, &objv_tracker, &old_uinfo);
  ASSERT_EQ(ret, -125); /* returns ECANCELED */
  ASSERT_EQ(old_uinfo.user_id.id, uinfo.user_id.id);
  ASSERT_EQ(old_uinfo.user_email, uinfo.user_email);

  /* exclusive create..should not create new one */
  uinfo.user_email = "user2_new@dbstore.com";
  objv_tracker.read_version.ver = 1;
  ret = db->store_user(dpp, uinfo, true, &attrs, &objv_tracker, &old_uinfo);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(old_uinfo.user_email, "user2@dbstore.com");
  ASSERT_EQ(objv_tracker.read_version.ver, 1);

  ret = db->store_user(dpp, uinfo, false, &attrs, &objv_tracker, &old_uinfo);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(old_uinfo.user_email, "user2@dbstore.com");
  ASSERT_EQ(objv_tracker.read_version.ver, 2);
  ASSERT_EQ(objv_tracker.read_version.tag, "UserTAG");
}

TEST_F(DBStoreTest, GetUserQueryByUserID) {
  int ret = -1;
  RGWUserInfo uinfo;
  map<std::string, bufferlist> attrs;
  RGWObjVersionTracker objv;

  uinfo.user_id.tenant = "tenant";
  uinfo.user_id.id = "user_id2";

  ret = db->get_user(dpp, "user_id", "user_id2", uinfo, &attrs, &objv);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(uinfo.user_id.tenant, "tenant");
  ASSERT_EQ(uinfo.user_email, "user2_new@dbstore.com");
  ASSERT_EQ(uinfo.user_id.id, "user_id2");
  ASSERT_EQ(uinfo.suspended, 123);
  ASSERT_EQ(uinfo.max_buckets, 456);
  ASSERT_EQ(uinfo.placement_tags.back(), "tags");
  RGWAccessKey k;
  map<string, RGWAccessKey>::iterator it = uinfo.access_keys.begin();
  k = it->second;
  ASSERT_EQ(k.id, "id1");
  ASSERT_EQ(k.key, "key1");
  it++;
  k = it->second;
  ASSERT_EQ(k.id, "id2");
  ASSERT_EQ(k.key, "key2");

  ASSERT_EQ(objv.read_version.ver, 2);

  bufferlist k1, k2;
  string attr;
  map<std::string, bufferlist>::iterator it2 = attrs.begin();
  k1 = it2->second;
  decode(attr, k1);
  ASSERT_EQ(attr, "attrs1");
  it2++;
  k2 = it2->second;
  decode(attr, k2);
  ASSERT_EQ(attr, "attrs2");
}

TEST_F(DBStoreTest, ListAllUsers) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ListAllUsers(dpp, &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, InsertBucket) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  params.op.bucket.info.bucket.name = "bucket1";
  params.op.bucket.info.bucket.tenant = "tenant";
  params.op.bucket.info.bucket.marker = "marker1";

  params.op.bucket.ent.size = 1024;

  params.op.bucket.info.has_instance_obj = false;
  params.op.bucket.bucket_version.ver = 1;
  params.op.bucket.bucket_version.tag = "read_tag";

  params.op.bucket.mtime = bucket_mtime;

  ret = db->ProcessOp(dpp, "InsertBucket", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, UpdateBucketAttrs) {
  int ret = -1;
  RGWBucketInfo info;
  map<std::string, bufferlist> attrs;
  RGWObjVersionTracker objv;

  bufferlist aclbl, aclbl2;
  encode("attrs1", aclbl);
  attrs["attr1"] = aclbl;
  encode("attrs2", aclbl2);
  attrs["attr2"] = aclbl2;

  info.bucket.name = "bucket1";

  /* invalid version number */
  objv.read_version.ver = 4;
  ret = db->update_bucket(dpp, "attrs", info, false, nullptr, &attrs, &bucket_mtime, &objv);
  ASSERT_EQ(ret, -125); /* returns ECANCELED */

  /* right version number */
  objv.read_version.ver = 1;
  ret = db->update_bucket(dpp, "attrs", info, false, nullptr, &attrs, &bucket_mtime, &objv);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(objv.read_version.ver, 2);
}

TEST_F(DBStoreTest, UpdateBucketInfo) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  RGWBucketInfo info;

  params.op.bucket.info.bucket.name = "bucket1";

  ret = db->ProcessOp(dpp, "GetBucket", &params);
  ASSERT_EQ(ret, 0);

  info = params.op.bucket.info;

  info.bucket.marker = "marker2";
  ret = db->update_bucket(dpp, "info", info, false, nullptr, nullptr, &bucket_mtime, nullptr);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(info.objv_tracker.read_version.ver, 3);
}

TEST_F(DBStoreTest, GetBucket) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  params.op.bucket.info.bucket.name = "bucket1";
  ret = db->ProcessOp(dpp, "GetBucket", &params);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(params.op.bucket.info.bucket.name, "bucket1");
  ASSERT_EQ(params.op.bucket.info.bucket.tenant, "tenant");
  ASSERT_EQ(params.op.bucket.info.bucket.marker, "marker2");
  ASSERT_EQ(params.op.bucket.ent.size, 1024);
  ASSERT_EQ(params.op.bucket.ent.bucket.name, "bucket1");
  ASSERT_EQ(params.op.bucket.ent.bucket.tenant, "tenant");
  ASSERT_EQ(params.op.bucket.info.has_instance_obj, false);
  ASSERT_EQ(params.op.bucket.info.objv_tracker.read_version.ver, 3);
  ASSERT_EQ(params.op.bucket.info.objv_tracker.read_version.tag, "read_tag");
  ASSERT_EQ(params.op.bucket.mtime, bucket_mtime);
  ASSERT_EQ(to_string(params.op.bucket.info.owner), "user_id1");
  bufferlist k, k2;
  string acl;
  map<std::string, bufferlist>::iterator it2 = params.op.bucket.bucket_attrs.begin();
  k = it2->second;
  decode(acl, k);
  ASSERT_EQ(acl, "attrs1");
  it2++;
  k2 = it2->second;
  decode(acl, k2);
  ASSERT_EQ(acl, "attrs2");
}

TEST_F(DBStoreTest, CreateBucket) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  RGWBucketInfo info;
  rgw_user owner;
  rgw_bucket bucket;
  obj_version objv;
  rgw_placement_rule rule;
  map<std::string, bufferlist> attrs;

  owner.id = "user_id1";
  bucket.name = "bucket1";
  bucket.tenant = "tenant";

  rule.name = "rule1";
  rule.storage_class = "sc1";

  ret = db->create_bucket(dpp, owner, bucket, "zid", rule, attrs, "swift_ver",
      std::nullopt, bucket_mtime, nullptr, info, null_yield);
  ASSERT_EQ(ret, 0);
  bucket.name = "bucket2";
  ret = db->create_bucket(dpp, owner, bucket, "zid", rule, attrs, "swift_ver",
      std::nullopt, bucket_mtime, nullptr, info, null_yield);
  ASSERT_EQ(ret, 0);
  bucket.name = "bucket3";
  ret = db->create_bucket(dpp, owner, bucket, "zid", rule, attrs, "swift_ver",
      std::nullopt, bucket_mtime, nullptr, info, null_yield);
  ASSERT_EQ(ret, 0);
  bucket.name = "bucket4";
  ret = db->create_bucket(dpp, owner, bucket, "zid", rule, attrs, "swift_ver",
      std::nullopt, bucket_mtime, nullptr, info, null_yield);
  ASSERT_EQ(ret, 0);
  bucket.name = "bucket5";
  ret = db->create_bucket(dpp, owner, bucket, "zid", rule, attrs, "swift_ver",
      std::nullopt, bucket_mtime, nullptr, info, null_yield);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, GetBucketQueryByName) {
  int ret = -1;
  RGWBucketInfo binfo;
  binfo.bucket.name = "bucket2";
  rgw::sal::Attrs attrs;
  ceph::real_time mtime;
  obj_version objv;

  ret = db->get_bucket_info(dpp, "name", "", binfo, &attrs, &mtime, &objv);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(binfo.bucket.name, "bucket2");
  ASSERT_EQ(binfo.bucket.tenant, "tenant");
  ASSERT_EQ(to_string(binfo.owner), "user_id1");
  ASSERT_EQ(binfo.objv_tracker.read_version.ver, 1);
  ASSERT_FALSE(binfo.objv_tracker.read_version.tag.empty());
  ASSERT_EQ(binfo.zonegroup, "zid");
  ASSERT_EQ(binfo.creation_time, bucket_mtime);
  ASSERT_EQ(binfo.placement_rule.name, "rule1");
  ASSERT_EQ(binfo.placement_rule.storage_class, "sc1");
  ASSERT_EQ(objv.ver, 1);
  ASSERT_FALSE(objv.tag.empty());

  marker1 = binfo.bucket.marker;
}

TEST_F(DBStoreTest, ListUserBuckets) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  std::string owner = "user_id1";
  int max = 2;
  bool need_stats = true;
  bool is_truncated = false;
  RGWUserBuckets ulist;

  marker1 = "";
  do {
    is_truncated = false;
    ret = db->list_buckets(dpp, "", owner, marker1, "", max, need_stats, &ulist,
          &is_truncated);
    ASSERT_EQ(ret, 0);

    cout << "marker1 :" << marker1 << "\n";

    cout << "is_truncated :" << is_truncated << "\n";

    for (const auto& ent: ulist.get_buckets()) {
      RGWBucketEnt e = ent.second;
      cout << "###################### \n";
      cout << "ent.bucket.id : " << e.bucket.name << "\n";
      cout << "ent.bucket.marker : " << e.bucket.marker << "\n";
      cout << "ent.bucket.bucket_id : " << e.bucket.bucket_id << "\n";
      cout << "ent.size : " << e.size << "\n";
      cout << "ent.rule.name : " << e.placement_rule.name << "\n";

      marker1 = e.bucket.name;
    }
    ulist.clear();
  } while(is_truncated);
}

TEST_F(DBStoreTest, BucketChown) {
  int ret = -1;
  RGWBucketInfo info;
  rgw_owner user = rgw_user{"user_id2"};

  info.bucket.name = "bucket5";

  ret = db->update_bucket(dpp, "owner", info, false, &user, nullptr, &bucket_mtime, nullptr);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(info.objv_tracker.read_version.ver, 2);
}

TEST_F(DBStoreTest, ListAllBuckets) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ListAllBuckets(dpp, &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, ListAllBuckets2) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  std::string owner; // empty
  int max = 2;
  bool need_stats = true;
  bool is_truncated = false;
  RGWUserBuckets ulist;

  marker1 = "";
  do {
    is_truncated = false;
    ret = db->list_buckets(dpp, "all", owner, marker1, "", max, need_stats, &ulist,
          &is_truncated);
    ASSERT_EQ(ret, 0);

    cout << "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ \n";
    cout << "ownerID : " << owner << "\n";
    cout << "marker1 :" << marker1 << "\n";

    cout << "is_truncated :" << is_truncated << "\n";

    for (const auto& ent: ulist.get_buckets()) {
      RGWBucketEnt e = ent.second;
      cout << "###################### \n";
      cout << "ent.bucket.id : " << e.bucket.name << "\n";
      cout << "ent.bucket.marker : " << e.bucket.marker << "\n";
      cout << "ent.bucket.bucket_id : " << e.bucket.bucket_id << "\n";
      cout << "ent.size : " << e.size << "\n";
      cout << "ent.rule.name : " << e.placement_rule.name << "\n";

      marker1 = e.bucket.name;
    }
    ulist.clear();
  } while(is_truncated);
}

TEST_F(DBStoreTest, RemoveBucketAPI) {
  int ret = -1;
  RGWBucketInfo info;

  info.bucket.name = "bucket5";

  ret = db->remove_bucket(dpp, info);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, RemoveUserAPI) {
  int ret = -1;
  RGWUserInfo uinfo;
  RGWObjVersionTracker objv;

  uinfo.user_id.tenant = "tenant";
  uinfo.user_id.id = "user_id2";

  /* invalid version number...should fail */
  objv.read_version.ver = 4;
  ret = db->remove_user(dpp, uinfo, &objv);
  ASSERT_EQ(ret, -125);

  objv.read_version.ver = 2;
  ret = db->remove_user(dpp, uinfo, &objv);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, PutObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  params.op.obj.category = RGWObjCategory::Main;
  params.op.obj.storage_class = "STANDARD";
  bufferlist b1;
  encode("HELLO WORLD", b1);
  cout<<"XXXXXXXXX Insert b1.length " << b1.length() << "\n";
  params.op.obj.head_data = b1;
  params.op.obj.state.size = 12;
  params.op.obj.state.is_olh = false;
  ret = db->ProcessOp(dpp, "PutObject", &params);
  ASSERT_EQ(ret, 0);

  /* Insert another objects */
  params.op.obj.state.obj.key.name = "object2";
  params.op.obj.state.obj.key.instance = "inst2";
  ret = db->ProcessOp(dpp, "PutObject", &params);
  ASSERT_EQ(ret, 0);

  params.op.obj.state.obj.key.name = "object3";
  params.op.obj.state.obj.key.instance = "inst3";
  ret = db->ProcessOp(dpp, "PutObject", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, ListAllObjects) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ListAllObjects(dpp, &params);
  ASSERT_GE(ret, 0);
}

TEST_F(DBStoreTest, GetObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "GetObject", &params);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(params.op.obj.category, RGWObjCategory::Main);
  ASSERT_EQ(params.op.obj.storage_class, "STANDARD");
  string data;
  decode(data, params.op.obj.head_data);
  ASSERT_EQ(data, "HELLO WORLD");
  ASSERT_EQ(params.op.obj.state.size, 12);
  cout << "versionNum :" << params.op.obj.version_num << "\n";
}

TEST_F(DBStoreTest, GetObjectState) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  RGWObjState* s;

  params.op.obj.state.obj.key.name = "object2";
  params.op.obj.state.obj.key.instance = "inst2";
  DB::Object op_target(db, params.op.bucket.info,
      params.op.obj.state.obj);

  ret = op_target.get_obj_state(dpp, params.op.bucket.info, params.op.obj.state.obj,
      false, &s);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(s->size, 12);
  ASSERT_EQ(s->is_olh, false);
  cout << "versionNum :" << params.op.obj.version_num << "\n";

  /* Recheck with get_state API */
  ret = op_target.get_state(dpp, &s, false);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(s->size, 12);
  ASSERT_EQ(s->is_olh, false);
  cout << "versionNum :" << params.op.obj.version_num << "\n";
}

TEST_F(DBStoreTest, ObjAttrs) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  map<string, bufferlist> setattrs;
  map<string, bufferlist> rmattrs;
  map<string, bufferlist> readattrs;

  bufferlist b1, b2, b3;
  encode("ACL", b1);
  setattrs[RGW_ATTR_ACL] = b1;
  encode("LC", b2);
  setattrs[RGW_ATTR_LC] = b2;
  encode("ETAG", b3);
  setattrs[RGW_ATTR_ETAG] = b3;

  DB::Object op_target(db, params.op.bucket.info,
      params.op.obj.state.obj);

  /* Set some attrs */
  ret = op_target.set_attrs(dpp, setattrs, nullptr);
  ASSERT_EQ(ret, 0);

  /* read those attrs */
  DB::Object::Read read_op(&op_target);
  read_op.params.attrs = &readattrs;
  ret = read_op.prepare(dpp);
  ASSERT_EQ(ret, 0);

  string val;
  decode(val, readattrs[RGW_ATTR_ACL]);
  ASSERT_EQ(val, "ACL");
  decode(val, readattrs[RGW_ATTR_LC]);
  ASSERT_EQ(val, "LC");
  decode(val, readattrs[RGW_ATTR_ETAG]);
  ASSERT_EQ(val, "ETAG");

  /* Remove some attrs */
  rmattrs[RGW_ATTR_ACL] = b1;
  map<string, bufferlist> empty;
  ret = op_target.set_attrs(dpp, empty, &rmattrs);
  ASSERT_EQ(ret, 0);

  /* read those attrs */
  ret = read_op.prepare(dpp);
  ASSERT_EQ(ret, 0);

  ASSERT_EQ(readattrs.count(RGW_ATTR_ACL), 0);
  decode(val, readattrs[RGW_ATTR_LC]);
  ASSERT_EQ(val, "LC");
  decode(val, readattrs[RGW_ATTR_ETAG]);
  ASSERT_EQ(val, "ETAG");
}

TEST_F(DBStoreTest, WriteObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  params.op.obj.state.obj.key.name = "object3";
  params.op.obj.state.obj.key.instance = "inst3";
  DB::Object op_target(db, params.op.bucket.info,
      params.op.obj.state.obj);

  bufferlist b1;
  encode("HELLO WORLD - Object3", b1);
  params.op.obj.head_data = b1;
  params.op.obj.state.size = 22;

  ret = write_object(dpp, params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, ReadObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  map<string, bufferlist> readattrs;
  params.op.obj.state.obj.key.name = "object3";
  params.op.obj.state.obj.key.instance = "inst3";
  uint64_t obj_size;
  DB::Object op_target(db, params.op.bucket.info,
      params.op.obj.state.obj);
  DB::Object::Read read_op(&op_target);
  read_op.params.attrs = &readattrs;
  read_op.params.obj_size = &obj_size;
  ret = read_op.prepare(dpp);
  ASSERT_EQ(ret, 0);

  bufferlist bl;
  ret = read_op.read(0, 25, bl, dpp);
  cout<<"XXXXXXXXX Insert bl.length " << bl.length() << "\n";
  ASSERT_EQ(ret, 25);

  string data;
  decode(data, bl);
  ASSERT_EQ(data, "HELLO WORLD - Object3");
  ASSERT_EQ(obj_size, 22);
}

TEST_F(DBStoreTest, IterateObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  map<string, bufferlist> readattrs;
  uint64_t obj_size;
  DBGetDataCB cb;

  DB::Object op_target(db, params.op.bucket.info,
      params.op.obj.state.obj);
  DB::Object::Read read_op(&op_target);
  read_op.params.attrs = &readattrs;
  read_op.params.obj_size = &obj_size;
  ret = read_op.prepare(dpp);
  ASSERT_EQ(ret, 0);

  bufferlist bl;
  ret = read_op.iterate(dpp, 0, 15, &cb);
  ASSERT_EQ(ret, 0);
  string data;
  decode(data, cb.data_bl);
  cout << "XXXXXXXXXX iterate data is " << data << ", bl_ofs = " << cb.data_ofs << ", bl_len = " << cb.data_len << "\n";
  ASSERT_EQ(data, "HELLO WORLD");
  ASSERT_EQ(cb.data_ofs, 0);
  ASSERT_EQ(cb.data_len, 15);
}

TEST_F(DBStoreTest, ListBucketObjects) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  
  int max = 2;
  bool is_truncated = false;
  rgw_obj_key marker1;
  DB::Bucket target(db, params.op.bucket.info);
  DB::Bucket::List list_op(&target);

  vector<rgw_bucket_dir_entry> dir_list;

  marker1.name = "";
  do {
    is_truncated = false;
    list_op.params.marker = marker1;
    ret = list_op.list_objects(dpp, max, &dir_list, nullptr, &is_truncated);
    ASSERT_EQ(ret, 0);

    cout << "marker1 :" << marker1.name << "\n";

    cout << "is_truncated :" << is_truncated << "\n";

    for (const auto& ent: dir_list) {
      cls_rgw_obj_key key = ent.key;
      cout << "###################### \n";
      cout << "key.name : " << key.name << "\n";
      cout << "key.instance : " << key.instance << "\n";

      marker1 = list_op.get_next_marker();
    }
    dir_list.clear();
  } while(is_truncated);
}

TEST_F(DBStoreTest, DeleteObj) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  RGWObjState *s;

  /* delete object2 */
  params.op.obj.state.obj.key.name = "object2";
  params.op.obj.state.obj.key.instance = "inst2";
  DB::Object op_target(db, params.op.bucket.info,
      params.op.obj.state.obj);

  DB::Object::Delete delete_op(&op_target);
  ret = delete_op.delete_obj(dpp);
  ASSERT_EQ(ret, 0);

  /* Should return ENOENT */
  ret = op_target.get_state(dpp, &s, false);
  ASSERT_EQ(ret, -2);
}

TEST_F(DBStoreTest, WriteVersionedObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  std::string instances[] = {"inst1", "inst2", "inst3"};
  bufferlist b1;

  params.op.obj.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
  params.op.obj.state.obj.key.name = "object1";

  /* Write versioned objects */
  DB::Object op_target(db, params.op.bucket.info, params.op.obj.state.obj);
  DB::Object::Write write_op(&op_target);

  /* Version1 */
  params.op.obj.state.obj.key.instance = instances[0];
  encode("HELLO WORLD", b1);
  params.op.obj.head_data = b1;
  params.op.obj.state.size = 12;
  ret = write_object(dpp, params);
  ASSERT_EQ(ret, 0);

  /* Version2 */
  params.op.obj.state.obj.key.instance = instances[1];
  b1.clear();
  encode("HELLO WORLD ABC", b1);
  params.op.obj.head_data = b1;
  params.op.obj.state.size = 16;
  ret = write_object(dpp, params);
  ASSERT_EQ(ret, 0);

  /* Version3 */
  params.op.obj.state.obj.key.instance = instances[2];
  b1.clear();
  encode("HELLO WORLD A", b1);
  params.op.obj.head_data = b1;
  params.op.obj.state.size = 14;
  ret = write_object(dpp, params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, ListVersionedObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  std::string instances[] = {"inst1", "inst2", "inst3"};
  int i = 0;

  /* list versioned objects */
  params.op.obj.state.obj.key.instance.clear();
  params.op.list_max_count = MAX_VERSIONED_OBJECTS;
  ret = db->ProcessOp(dpp, "ListVersionedObjects", &params);
  ASSERT_EQ(ret, 0);

  i = 2;
  for (auto ent: params.op.obj.list_entries) {


    ASSERT_EQ(ent.key.instance, instances[i]);
    i--;
  }
}

TEST_F(DBStoreTest, ReadVersionedObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  std::string instances[] = {"inst1", "inst2", "inst3"};
  std::string data;

  /* read object.. should fetch latest version */
  RGWObjState* s;
  params = GlobalParams;
  params.op.obj.state.obj.key.instance.clear();
  DB::Object op_target2(db, params.op.bucket.info, params.op.obj.state.obj);
  ret = op_target2.get_obj_state(dpp, params.op.bucket.info, params.op.obj.state.obj,
                                 true, &s);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(s->obj.key.instance, instances[2]);
  decode(data, s->data);
  ASSERT_EQ(data, "HELLO WORLD A");
  ASSERT_EQ(s->size, 14);

  /* read a particular non-current version */
  params.op.obj.state.obj.key.instance = instances[1];
  DB::Object op_target3(db, params.op.bucket.info, params.op.obj.state.obj);
  ret = op_target3.get_obj_state(dpp, params.op.bucket.info, params.op.obj.state.obj,
                                 true, &s);
  ASSERT_EQ(ret, 0);
  decode(data, s->data);
  ASSERT_EQ(data, "HELLO WORLD ABC");
  ASSERT_EQ(s->size, 16);
}

TEST_F(DBStoreTest, DeleteVersionedObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  std::string instances[] = {"inst1", "inst2", "inst3"};
  std::string data;
  std::string dm_instance;
  int i = 0;

  /* Delete object..should create delete marker */
  params.op.obj.state.obj.key.instance.clear();
  DB::Object op_target(db, params.op.bucket.info, params.op.obj.state.obj);
  DB::Object::Delete delete_op(&op_target);
  delete_op.params.versioning_status |= BUCKET_VERSIONED;

  ret = delete_op.delete_obj(dpp);
  ASSERT_EQ(ret, 0);

  /* list versioned objects */
  params = GlobalParams;
  params.op.obj.state.obj.key.instance.clear();
  params.op.list_max_count = MAX_VERSIONED_OBJECTS;
  ret = db->ProcessOp(dpp, "ListVersionedObjects", &params);

  i = 3;
  for (auto ent: params.op.obj.list_entries) {
    string is_delete_marker = (ent.flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER)? "true" : "false";
    cout << "ent.name: " << ent.key.name << ". ent.instance: " << ent.key.instance << " is_delete_marker = " << is_delete_marker << "\n";

    if (i == 3) {
      ASSERT_EQ(is_delete_marker, "true");
      dm_instance = ent.key.instance;
    } else {
      ASSERT_EQ(is_delete_marker, "false");
      ASSERT_EQ(ent.key.instance, instances[i]);
    }

    i--;
  }

  /* read object.. should return -ENOENT */
  RGWObjState* s;
  params = GlobalParams;
  params.op.obj.state.obj.key.instance.clear();
  DB::Object op_target2(db, params.op.bucket.info, params.op.obj.state.obj);
  ret = op_target2.get_obj_state(dpp, params.op.bucket.info, params.op.obj.state.obj,
                                 true, &s);
  ASSERT_EQ(ret, -ENOENT);

  /* Delete delete marker..should be able to read object now */ 
  params.op.obj.state.obj.key.instance = dm_instance;
  DB::Object op_target3(db, params.op.bucket.info, params.op.obj.state.obj);
  DB::Object::Delete delete_op2(&op_target3);
  delete_op2.params.versioning_status |= BUCKET_VERSIONED;

  ret = delete_op2.delete_obj(dpp);
  ASSERT_EQ(ret, 0);

  /* read object.. should fetch latest version */
  params = GlobalParams;
  params.op.obj.state.obj.key.instance.clear();
  DB::Object op_target4(db, params.op.bucket.info, params.op.obj.state.obj);
  ret = op_target4.get_obj_state(dpp, params.op.bucket.info, params.op.obj.state.obj,
                                 true, &s);
  ASSERT_EQ(s->obj.key.instance, instances[2]);
  decode(data, s->data);
  ASSERT_EQ(data, "HELLO WORLD A");
  ASSERT_EQ(s->size, 14);

  /* delete latest version using version-id. Next version should get promoted */
  params.op.obj.state.obj.key.instance = instances[2];
  DB::Object op_target5(db, params.op.bucket.info, params.op.obj.state.obj);
  DB::Object::Delete delete_op3(&op_target5);
  delete_op3.params.versioning_status |= BUCKET_VERSIONED;

  ret = delete_op3.delete_obj(dpp);
  ASSERT_EQ(ret, 0);

  /* list versioned objects..only two versions should be present
   * with second version marked as CURRENT */
  params = GlobalParams;
  params.op.obj.state.obj.key.instance.clear();
  params.op.list_max_count = MAX_VERSIONED_OBJECTS;
  ret = db->ProcessOp(dpp, "ListVersionedObjects", &params);

  i = 1;
  for (auto ent: params.op.obj.list_entries) {

    if (i == 1) {
      dm_instance = ent.key.instance;
    } else {
      ASSERT_EQ(ent.key.instance, instances[i]);
    }

    i--;
  }

}

TEST_F(DBStoreTest, ObjectOmapSetVal) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  DB::Object op_target(db, params.op.bucket.info,
      params.op.obj.state.obj);

  string val = "part1_val";
  bufferlist bl;
  encode(val, bl);
  ret = op_target.obj_omap_set_val_by_key(dpp, "part1", bl, false);
  ASSERT_EQ(ret, 0);

  val = "part2_val";
  bl.clear();
  encode(val, bl);
  ret = op_target.obj_omap_set_val_by_key(dpp, "part2", bl, false);
  ASSERT_EQ(ret, 0);

  val = "part3_val";
  bl.clear();
  encode(val, bl);
  ret = op_target.obj_omap_set_val_by_key(dpp, "part3", bl, false);
  ASSERT_EQ(ret, 0);

  val = "part4_val";
  bl.clear();
  encode(val, bl);
  ret = op_target.obj_omap_set_val_by_key(dpp, "part4", bl, false);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, ObjectOmapGetValsByKeys) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  std::set<std::string> keys;
  std::map<std::string, bufferlist> vals;

  DB::Object op_target(db, params.op.bucket.info,
      params.op.obj.state.obj);

  keys.insert("part2");
  keys.insert("part4");

  ret = op_target.obj_omap_get_vals_by_keys(dpp, "", keys, &vals);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(vals.size(), 2);

  string val;
  decode(val, vals["part2"]);
  ASSERT_EQ(val, "part2_val");
  decode(val, vals["part4"]);
  ASSERT_EQ(val, "part4_val");
}

TEST_F(DBStoreTest, ObjectOmapGetAll) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  std::map<std::string, bufferlist> vals;

  DB::Object op_target(db, params.op.bucket.info,
      params.op.obj.state.obj);

  ret = op_target.obj_omap_get_all(dpp, &vals);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(vals.size(), 4);

  string val;
  decode(val, vals["part1"]);
  ASSERT_EQ(val, "part1_val");
  decode(val, vals["part2"]);
  ASSERT_EQ(val, "part2_val");
  decode(val, vals["part3"]);
  ASSERT_EQ(val, "part3_val");
  decode(val, vals["part4"]);
  ASSERT_EQ(val, "part4_val");
}

TEST_F(DBStoreTest, ObjectOmapGetVals) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  std::set<std::string> keys;
  std::map<std::string, bufferlist> vals;
  bool pmore;

  DB::Object op_target(db, params.op.bucket.info,
      params.op.obj.state.obj);

  ret = op_target.obj_omap_get_vals(dpp, "part3", 10, &vals, &pmore);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(vals.size(), 2);

  string val;
  decode(val, vals["part3"]);
  ASSERT_EQ(val, "part3_val");
  decode(val, vals["part4"]);
  ASSERT_EQ(val, "part4_val");
}

TEST_F(DBStoreTest, PutObjectData) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  params.op.obj_data.part_num = 1;
  params.op.obj_data.offset = 10;
  params.op.obj_data.multipart_part_str = "2";
  bufferlist b1;
  encode("HELLO WORLD", b1);
  params.op.obj_data.data = b1;
  params.op.obj_data.size = 12;
  params.op.obj.state.mtime = real_clock::now();
  ret = db->ProcessOp(dpp, "PutObjectData", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, UpdateObjectData) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  params.op.obj.state.mtime = bucket_mtime;
  ret = db->ProcessOp(dpp, "UpdateObjectData", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, GetObjectData) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  params.op.obj.state.obj.key.instance = "inst1";
  params.op.obj.state.obj.key.name = "object1";
  ret = db->ProcessOp(dpp, "GetObjectData", &params);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(params.op.obj_data.part_num, 1);
  ASSERT_EQ(params.op.obj_data.offset, 10);
  ASSERT_EQ(params.op.obj_data.multipart_part_str, "2");
  ASSERT_EQ(params.op.obj.state.obj.key.instance, "inst1");
  ASSERT_EQ(params.op.obj.state.obj.key.name, "object1");
  ASSERT_EQ(params.op.obj.state.mtime, bucket_mtime);
  string data;
  decode(data, params.op.obj_data.data);
  ASSERT_EQ(data, "HELLO WORLD");
}

TEST_F(DBStoreTest, DeleteObjectData) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "DeleteObjectData", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, DeleteObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "DeleteObject", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, LCTables) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->createLCTables(dpp);
  ASSERT_GE(ret, 0);
}

TEST_F(DBStoreTest, LCHead) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  std::string index1 = "bucket1";
  std::string index2 = "bucket2";
  time_t lc_time = ceph_clock_now();
  std::unique_ptr<rgw::sal::Lifecycle::LCHead> head;
  std::string ents[] = {"entry1", "entry2", "entry3"};
  rgw::sal::StoreLifecycle::StoreLCHead head1(lc_time, 0, ents[0]);
  rgw::sal::StoreLifecycle::StoreLCHead head2(lc_time, 0, ents[1]);
  rgw::sal::StoreLifecycle::StoreLCHead head3(lc_time, 0, ents[2]);

  ret = db->put_head(index1, head1);
  ASSERT_EQ(ret, 0);
  ret = db->put_head(index2, head2);
  ASSERT_EQ(ret, 0);

  ret = db->get_head(index1, &head);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(head->get_marker(), "entry1");

  ret = db->get_head(index2, &head);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(head->get_marker(), "entry2");

  // update index1
  ret = db->put_head(index1, head3);
  ASSERT_EQ(ret, 0);
  ret = db->get_head(index1, &head);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(head->get_marker(), "entry3");

}
TEST_F(DBStoreTest, LCEntry) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  uint64_t lc_time = ceph_clock_now();
  std::string index1 = "lcindex1";
  std::string index2 = "lcindex2";
  typedef enum {lc_uninitial = 1, lc_complete} status;
  std::string ents[] = {"bucket1", "bucket2", "bucket3", "bucket4"};
  std::unique_ptr<rgw::sal::Lifecycle::LCEntry> entry;
  rgw::sal::StoreLifecycle::StoreLCEntry entry1(ents[0], lc_time, lc_uninitial);
  rgw::sal::StoreLifecycle::StoreLCEntry entry2(ents[1], lc_time, lc_uninitial);
  rgw::sal::StoreLifecycle::StoreLCEntry entry3(ents[2], lc_time, lc_uninitial);
  rgw::sal::StoreLifecycle::StoreLCEntry entry4(ents[3], lc_time, lc_uninitial);

  vector<std::unique_ptr<rgw::sal::Lifecycle::LCEntry>> lc_entries;

  ret = db->set_entry(index1, entry1);
  ASSERT_EQ(ret, 0);
  ret = db->set_entry(index1, entry2);
  ASSERT_EQ(ret, 0);
  ret = db->set_entry(index1, entry3);
  ASSERT_EQ(ret, 0);
  ret = db->set_entry(index2, entry4);
  ASSERT_EQ(ret, 0);

  // get entry index1, entry1
  ret = db->get_entry(index1, ents[0], &entry); 
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(entry->get_status(), lc_uninitial);
  ASSERT_EQ(entry->get_start_time(), lc_time);

  // get next entry index1, entry2
  ret = db->get_next_entry(index1, ents[1], &entry); 
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(entry->get_bucket(), ents[2]);
  ASSERT_EQ(entry->get_status(), lc_uninitial);
  ASSERT_EQ(entry->get_start_time(), lc_time);

  // update entry4 to entry5
  entry4.status = lc_complete;
  ret = db->set_entry(index2, entry4);
  ASSERT_EQ(ret, 0);
  ret = db->get_entry(index2, ents[3], &entry); 
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(entry->get_status(), lc_complete);

  // list entries
  ret = db->list_entries(index1, "", 5, lc_entries);
  ASSERT_EQ(ret, 0);
  for (const auto& ent: lc_entries) {
    cout << "###################### \n";
    cout << "lc entry.bucket : " << ent->get_bucket() << "\n";
    cout << "lc entry.status : " << ent->get_status() << "\n";
  }

  // remove index1, entry3
  ret = db->rm_entry(index1, entry3); 
  ASSERT_EQ(ret, 0);

  // get next entry index1, entry2.. should be null
  entry.release();
  ret = db->get_next_entry(index1, ents[1], &entry); 
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(entry.get(), nullptr);
}

TEST_F(DBStoreTest, RemoveBucket) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "RemoveBucket", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, RemoveUser) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "RemoveUser", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, InsertTestIDUser) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  params.op.user.uinfo.user_id.id = "testid";
  params.op.user.uinfo.display_name = "M. Tester";
  params.op.user.uinfo.user_id.tenant = "tenant";
  params.op.user.uinfo.user_email = "tester@ceph.com";
  RGWAccessKey k1("0555b35654ad1656d804", "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==");
  params.op.user.uinfo.access_keys["0555b35654ad1656d804"] = k1;
  params.op.user.user_version.ver = 1;    
  params.op.user.user_version.tag = "UserTAG";    

  ret = db->ProcessOp(dpp, "InsertUser", &params);
  ASSERT_EQ(ret, 0);
}

int main(int argc, char **argv)
{
  int ret = -1;
  string c_logfile = "rgw_dbstore_tests.log";
  int c_loglevel = 20;

  // format: ./dbstore-tests logfile loglevel
  if (argc == 3) {
    c_logfile = argv[1];
    c_loglevel = (atoi)(argv[2]);
    cout << "logfile:" << c_logfile << ", loglevel set to " << c_loglevel << "\n";
  }

  ::testing::InitGoogleTest(&argc, argv);

  gtest::env = new gtest::Environment();
  gtest::env->logfile = c_logfile;
  gtest::env->loglevel = c_loglevel;
  ::testing::AddGlobalTestEnvironment(gtest::env);

  ret = RUN_ALL_TESTS();

  return ret;
}
