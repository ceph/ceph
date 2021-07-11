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
        tenant("tenantname"), db(nullptr),
        db_type("db_typename"), ret(-1) {}

      virtual ~Environment() {}

      void SetUp() override {
        cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
            CODE_ENVIRONMENT_DAEMON, CINIT_FLAG_NO_MON_CONFIG, 1)->get();
        if (!db_type.compare("SQLite")) {
          db = new SQLiteDB(tenant, cct);
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
      CephContext *cct;
  };
}

ceph::real_time bucket_mtime = real_clock::now();
string marker1;

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
        GlobalParams.object = object1;
        GlobalParams.offset = 0;
        GlobalParams.data = data;
        GlobalParams.datalen = data.length();

        /* As of now InitializeParams doesnt do anything
         * special based on fop. Hence its okay to do
         * global initialization once.
         */
        ret = db->InitializeParams(dpp, "", &GlobalParams);
        ASSERT_EQ(ret, 0);
      }

      void TearDown() {
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
  params.op.user.uinfo.assumed_role_arn = "role";
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
  ASSERT_EQ(params.op.user.uinfo.assumed_role_arn, "role");
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
  ASSERT_EQ(params.op.user.uinfo.assumed_role_arn, "role");
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
  ASSERT_EQ(uinfo.assumed_role_arn, "role");
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
  ASSERT_EQ(uinfo.assumed_role_arn, "role");
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
  uinfo.assumed_role_arn = "role";
  uinfo.placement_tags.push_back("tags");
  RGWAccessKey k1("id1", "key1");
  RGWAccessKey k2("id2", "key2");
  uinfo.access_keys["id1"] = k1;
  uinfo.access_keys["id2"] = k2;

  /* non exclusive create..should create new one */
  ret = db->store_user(dpp, uinfo, true, &attrs, &objv_tracker, &old_uinfo);
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

  ret = db->get_user(dpp, "user_id", "", uinfo, &attrs, &objv);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(uinfo.user_id.tenant, "tenant");
  ASSERT_EQ(uinfo.user_email, "user2_new@dbstore.com");
  ASSERT_EQ(uinfo.user_id.id, "user_id2");
  ASSERT_EQ(uinfo.suspended, 123);
  ASSERT_EQ(uinfo.max_buckets, 456);
  ASSERT_EQ(uinfo.assumed_role_arn, "role");
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

TEST_F(DBStoreTest, BucketChown) {
  int ret = -1;
  RGWBucketInfo info;
  rgw_user user;
  user.id = "user_id2";

  info.bucket.name = "bucket1";

  ret = db->update_bucket(dpp, "owner", info, false, &user, nullptr, &bucket_mtime, nullptr);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(info.objv_tracker.read_version.ver, 3);
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
  ASSERT_EQ(info.objv_tracker.read_version.ver, 4);
}

TEST_F(DBStoreTest, GetBucket) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "GetBucket", &params);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(params.op.bucket.info.bucket.name, "bucket1");
  ASSERT_EQ(params.op.bucket.info.bucket.tenant, "tenant");
  ASSERT_EQ(params.op.bucket.info.bucket.marker, "marker2");
  ASSERT_EQ(params.op.bucket.ent.size, 1024);
  ASSERT_EQ(params.op.bucket.ent.bucket.name, "bucket1");
  ASSERT_EQ(params.op.bucket.ent.bucket.tenant, "tenant");
  ASSERT_EQ(params.op.bucket.info.has_instance_obj, false);
  ASSERT_EQ(params.op.bucket.info.objv_tracker.read_version.ver, 4);
  ASSERT_EQ(params.op.bucket.info.objv_tracker.read_version.tag, "read_tag");
  ASSERT_EQ(params.op.bucket.mtime, bucket_mtime);
  ASSERT_EQ(params.op.bucket.info.owner.id, "user_id2");
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

TEST_F(DBStoreTest, RemoveBucketAPI) {
  int ret = -1;
  RGWBucketInfo info;

  info.bucket.name = "bucket1";

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

  /* invalid version number...should fail */
  objv.read_version.ver = 2;
  ret = db->remove_user(dpp, uinfo, &objv);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, CreateBucket) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  RGWBucketInfo info;
  RGWUserInfo owner;
  rgw_bucket bucket;
  obj_version objv;
  rgw_placement_rule rule;
  map<std::string, bufferlist> attrs;

  owner.user_id.id = "user_id1";
  bucket.name = "bucket1";
  bucket.tenant = "tenant";

  objv.ver = 2;
  objv.tag = "write_tag";

  rule.name = "rule1";
  rule.storage_class = "sc1";

  ret = db->create_bucket(dpp, owner, bucket, "zid", rule, "swift_ver", NULL,
      attrs, info, &objv, NULL, bucket_mtime, NULL, NULL,
      null_yield, false);
  ASSERT_EQ(ret, 0);
  bucket.name = "bucket2";
  ret = db->create_bucket(dpp, owner, bucket, "zid", rule, "swift_ver", NULL,
      attrs, info, &objv, NULL, bucket_mtime, NULL, NULL,
      null_yield, false);
  ASSERT_EQ(ret, 0);
  bucket.name = "bucket3";
  ret = db->create_bucket(dpp, owner, bucket, "zid", rule, "swift_ver", NULL,
      attrs, info, &objv, NULL, bucket_mtime, NULL, NULL,
      null_yield, false);
  ASSERT_EQ(ret, 0);
  bucket.name = "bucket4";
  ret = db->create_bucket(dpp, owner, bucket, "zid", rule, "swift_ver", NULL,
      attrs, info, &objv, NULL, bucket_mtime, NULL, NULL,
      null_yield, false);
  ASSERT_EQ(ret, 0);
  bucket.name = "bucket5";
  ret = db->create_bucket(dpp, owner, bucket, "zid", rule, "swift_ver", NULL,
      attrs, info, &objv, NULL, bucket_mtime, NULL, NULL,
      null_yield, false);
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
  ASSERT_EQ(binfo.owner.id, "user_id1");
  ASSERT_EQ(binfo.objv_tracker.read_version.ver, 2);
  ASSERT_EQ(binfo.objv_tracker.read_version.tag, "write_tag");
  ASSERT_EQ(binfo.zonegroup, "zid");
  ASSERT_EQ(binfo.creation_time, bucket_mtime);
  ASSERT_EQ(binfo.placement_rule.name, "rule1");
  ASSERT_EQ(binfo.placement_rule.storage_class, "sc1");
  ASSERT_EQ(objv.ver, 2);
  ASSERT_EQ(objv.tag, "write_tag");

  marker1 = binfo.bucket.marker;
}

TEST_F(DBStoreTest, ListUserBuckets) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;
  rgw_user owner;
  int max = 2;
  bool need_stats = true;
  bool is_truncated = false;
  RGWUserBuckets ulist;

  owner.id = "user_id1";

  marker1 = "";
  do {
    is_truncated = false;
    ret = db->list_buckets(dpp, owner, marker1, "", max, need_stats, &ulist, &is_truncated);
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

TEST_F(DBStoreTest, ListAllBuckets) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ListAllBuckets(dpp, &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, InsertObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "InsertObject", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, ListObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "ListObject", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, ListAllObjects) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ListAllObjects(dpp, &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, PutObjectData) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "PutObjectData", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, GetObjectData) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "GetObjectData", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, DeleteObjectData) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "DeleteObjectData", &params);
  ASSERT_EQ(ret, 0);
}

TEST_F(DBStoreTest, RemoveObject) {
  struct DBOpParams params = GlobalParams;
  int ret = -1;

  ret = db->ProcessOp(dpp, "RemoveObject", &params);
  ASSERT_EQ(ret, 0);
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
