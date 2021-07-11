// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef DB_STORE_H
#define DB_STORE_H

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>
// this seems safe to use, at least for now--arguably, we should
// prefer header-only fmt, in general
#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include "fmt/format.h"
#include <map>
#include "dbstore_log.h"
#include "rgw/rgw_sal.h"
#include "rgw/rgw_common.h"
#include "rgw/rgw_bucket.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_context.h"

using namespace std;

namespace rgw { namespace store {

class DB;

struct DBOpUserInfo {
  RGWUserInfo uinfo = {};
  obj_version user_version;
  rgw::sal::Attrs user_attrs;
};

struct DBOpBucketInfo {
  RGWBucketEnt ent; // maybe not needed. not used in create/get_bucket
  RGWBucketInfo info;
  RGWUser* owner = nullptr;
  rgw::sal::Attrs bucket_attrs;
  obj_version bucket_version;
  ceph::real_time mtime;
  // used for list query
  string min_marker;
  string max_marker;
  list<RGWBucketEnt> list_entries;
};

struct DBOpInfo {
  string name; // Op name
  /* Support only single access_key for now. So store
   * it separately as primary access_key_id & secret to
   * be able to query easily.
   *
   * XXX: Swift keys and subuser not supported for now */
  DBOpUserInfo user;
  string query_str;
  DBOpBucketInfo bucket;
  uint64_t list_max_count;
};

struct DBOpParams {
  CephContext *cct;

  /* Tables */
  string user_table;
  string bucket_table;
  string object_table;

  /* Ops*/
  DBOpInfo op;

  /* Below are subject to change */
  string objectdata_table;
  string quota_table;
  string object;
  size_t offset;
  string data;
  size_t datalen;
};

/* Used for prepared schemas.
 * Difference with above structure is that all 
 * the fields are strings here to accommodate any
 * style identifiers used by backend db. By default
 * initialized with sqlitedb style, can be overriden
 * using InitPrepareParams()
 *
 * These identifiers are used in prepare and bind statements
 * to get the right index of each param.
 */
struct DBOpUserPrepareInfo {
  string user_id = ":user_id";
  string tenant = ":tenant";
  string ns = ":ns";
  string display_name = ":display_name";
  string user_email = ":user_email";
  /* Support only single access_key for now. So store
   * it separately as primary access_key_id & secret to
   * be able to query easily.
   *
   * In future, when need to support & query from multiple
   * access keys, better to maintain them in a separate table.
   */
  string access_keys_id = ":access_keys_id";
  string access_keys_secret = ":access_keys_secret";
  string access_keys = ":access_keys";
  string swift_keys = ":swift_keys";
  string subusers = ":subusers";
  string suspended = ":suspended";
  string max_buckets = ":max_buckets";
  string op_mask = ":op_mask";
  string user_caps = ":user_caps";
  string admin = ":admin";
  string system = ":system";
  string placement_name = ":placement_name";
  string placement_storage_class = ":placement_storage_class";
  string placement_tags = ":placement_tags";
  string bucket_quota = ":bucket_quota";
  string temp_url_keys = ":temp_url_keys";
  string user_quota = ":user_quota";
  string type = ":type";
  string mfa_ids = ":mfa_ids";
  string assumed_role_arn = ":assumed_role_arn";
  string user_attrs = ":user_attrs";
  string user_ver = ":user_vers";
  string user_ver_tag = ":user_ver_tag";
};

struct DBOpBucketPrepareInfo {
  string bucket_name = ":bucket_name";
  string tenant = ":tenant";
  string marker = ":marker";
  string bucket_id = ":bucket_id";
  string size = ":size";
  string size_rounded = ":size_rounded";
  string creation_time = ":creation_time";
  string count = ":count";
  string placement_name = ":placement_name";
  string placement_storage_class = ":placement_storage_class";
  /* ownerid - maps to DBOpUserPrepareInfo */
  string flags = ":flags";
  string zonegroup = ":zonegroup";
  string has_instance_obj = ":has_instance_obj";
  string quota = ":quota";
  string requester_pays = ":requester_pays";
  string has_website = ":has_website";
  string website_conf = ":website_conf";
  string swift_versioning = ":swift_versioning";
  string swift_ver_location = ":swift_ver_location";
  string mdsearch_config = ":mdsearch_config";
  string new_bucket_instance_id = ":new_bucket_instance_id";
  string obj_lock = ":obj_lock";
  string sync_policy_info_groups = ":sync_policy_info_groups";
  string bucket_attrs = ":bucket_attrs";
  string bucket_ver = ":bucket_vers";
  string bucket_ver_tag = ":bucket_ver_tag";
  string mtime = ":mtime";
  string min_marker = ":min_marker";
  string max_marker = ":max_marker";
};

struct DBOpPrepareInfo {
  DBOpUserPrepareInfo user;
  string query_str = ":query_str";
  DBOpBucketPrepareInfo bucket;
  string list_max_count = ":list_max_count";
};

struct DBOpPrepareParams {
  /* Tables */
  string user_table = ":user_table";
  string bucket_table = ":bucket_table";
  string object_table = ":object_table";

  /* Ops */
  DBOpPrepareInfo op;


  /* below subject to change */
  string objectdata_table = ":objectdata_table";
  string quota_table = ":quota_table";
  string object = ":object";
  string offset = ":offset";
  string data = ":data";
  string datalen = ":datalen";
};

struct DBOps {
  class InsertUserOp *InsertUser;
  class RemoveUserOp *RemoveUser;
  class GetUserOp *GetUser;
  class InsertBucketOp *InsertBucket;
  class UpdateBucketOp *UpdateBucket;
  class RemoveBucketOp *RemoveBucket;
  class GetBucketOp *GetBucket;
  class ListUserBucketsOp *ListUserBuckets;
};

class ObjectOp {
  public:
    ObjectOp() {};

    virtual ~ObjectOp() {}

    class InsertObjectOp *InsertObject;
    class RemoveObjectOp *RemoveObject;
    class ListObjectOp *ListObject;
    class PutObjectDataOp *PutObjectData;
    class GetObjectDataOp *GetObjectData;
    class DeleteObjectDataOp *DeleteObjectData;

    virtual int InitializeObjectOps(const DoutPrefixProvider *dpp) { return 0; }
    virtual int FreeObjectOps(const DoutPrefixProvider *dpp) { return 0; }
};

class DBOp {
  private:
    const string CreateUserTableQ =
      /* Corresponds to RGWUser
       *
       * For now only UserID is made Primary key.
       * If multiple tenants are stored in single .db handle, should
       * make both (UserID, Tenant) as Primary Key.
       *
       * XXX:
       * - AccessKeys, SwiftKeys, Subusers (map<>) are stored as blob.
       *   To enable easy query, first accesskey is stored in separate fields
       *   AccessKeysID, AccessKeysSecret.
       *   In future, may be have separate table to store these keys and
       *   query on that table.
       * - Quota stored as blob .. should be linked to quota table.
       */
      "CREATE TABLE IF NOT EXISTS '{}' (	\
      UserID TEXT NOT NULL UNIQUE,		\
      Tenant TEXT ,		\
      NS TEXT ,		\
      DisplayName TEXT , \
      UserEmail TEXT ,	\
      AccessKeysID TEXT ,	\
      AccessKeysSecret TEXT ,	\
      AccessKeys BLOB ,	\
      SwiftKeys BLOB ,	\
      SubUsers BLOB ,		\
      Suspended INTEGER ,	\
      MaxBuckets INTEGER ,	\
      OpMask	INTEGER ,	\
      UserCaps BLOB ,		\
      Admin	INTEGER ,	\
      System INTEGER , 	\
      PlacementName TEXT , 	\
      PlacementStorageClass TEXT , 	\
      PlacementTags BLOB ,	\
      BucketQuota BLOB ,	\
      TempURLKeys BLOB ,	\
      UserQuota BLOB ,	\
      TYPE INTEGER ,		\
      MfaIDs BLOB ,	\
      AssumedRoleARN TEXT , \
      UserAttrs   BLOB,   \
      UserVersion   INTEGER,    \
      UserVersionTag TEXT,      \
      PRIMARY KEY (UserID) \n);";

    const string CreateBucketTableQ =
      /* Corresponds to RGWBucket
       *
       *  For now only BucketName is made Primary key.
       *  If multiple tenants are stored in single .db handle, should
       *  make both (BucketName, Tenant) as Primary Key. Also should
       *  reference (UserID, Tenant) as Foreign key.
       *
       * leaving below RADOS specific fields
       *   - rgw_data_placement_target explicit_placement (struct rgw_bucket)
       *   - rgw::BucketLayout layout (struct RGWBucketInfo)
       *   - const static uint32_t NUM_SHARDS_BLIND_BUCKET (struct RGWBucketInfo),
       *     should be '0' indicating no sharding.
       *   - cls_rgw_reshard_status reshard_status (struct RGWBucketInfo)
       *
       * XXX:
       *   - Quota stored as blob .. should be linked to quota table.
       *   - WebsiteConf stored as BLOB..if required, should be split
       *   - Storing bucket_version (struct RGWBucket), objv_tracker
       *     (struct RGWBucketInfo) separately. Are they same?
       *
       */
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      BucketName TEXT NOT NULL UNIQUE , \
      Tenant TEXT,        \
      Marker TEXT,        \
      BucketID TEXT,      \
      Size   INTEGER,     \
      SizeRounded INTEGER,\
      CreationTime BLOB,  \
      Count  INTEGER,     \
      PlacementName TEXT , 	\
      PlacementStorageClass TEXT , 	\
      OwnerID TEXT NOT NULL, \
      Flags   INTEGER,       \
      Zonegroup TEXT,         \
      HasInstanceObj BOOLEAN, \
      Quota   BLOB,       \
      RequesterPays BOOLEAN,  \
      HasWebsite  BOOLEAN,    \
      WebsiteConf BLOB,   \
      SwiftVersioning BOOLEAN, \
      SwiftVerLocation TEXT,  \
      MdsearchConfig  BLOB,   \
      NewBucketInstanceID TEXT,\
      ObjectLock BLOB, \
      SyncPolicyInfoGroups BLOB, \
      BucketAttrs   BLOB,   \
      BucketVersion   INTEGER,    \
      BucketVersionTag TEXT,      \
      Mtime   BLOB,   \
      PRIMARY KEY (BucketName) \
      FOREIGN KEY (OwnerID) \
      REFERENCES '{}' (UserID) ON DELETE CASCADE ON UPDATE CASCADE \n);";
    const string CreateObjectTableQ =
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      BucketName TEXT NOT NULL , \
      ObjectName TEXT NOT NULL , \
      PRIMARY KEY (BucketName, ObjectName), \
      FOREIGN KEY (BucketName) \
      REFERENCES '{}' (BucketName) ON DELETE CASCADE ON UPDATE CASCADE \n);";
    const string CreateObjectDataTableQ =
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      BucketName TEXT NOT NULL , \
      ObjectName TEXT NOT NULL , \
      Offset   INTEGER NOT NULL, \
      Data     BLOB,             \
      Size 	 INTEGER NOT NULL, \
      PRIMARY KEY (BucketName, ObjectName, Offset), \
      FOREIGN KEY (BucketName, ObjectName) \
      REFERENCES '{}' (BucketName, ObjectName) ON DELETE CASCADE ON UPDATE CASCADE \n);";

    const string CreateQuotaTableQ =
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      QuotaID INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE , \
      MaxSizeSoftThreshold INTEGER ,	\
      MaxObjsSoftThreshold INTEGER ,	\
      MaxSize	INTEGER ,		\
      MaxObjects INTEGER ,		\
      Enabled Boolean ,		\
      CheckOnRaw Boolean \n);";

    const string DropQ = "DROP TABLE IF EXISTS '{}'";
    const string ListAllQ = "SELECT  * from '{}'";

  public:
    DBOp() {};
    virtual ~DBOp() {};

    string CreateTableSchema(string type, DBOpParams *params) {
      if (!type.compare("User"))
        return fmt::format(CreateUserTableQ.c_str(),
            params->user_table.c_str());
      if (!type.compare("Bucket"))
        return fmt::format(CreateBucketTableQ.c_str(),
            params->bucket_table.c_str(),
            params->user_table.c_str());
      if (!type.compare("Object"))
        return fmt::format(CreateObjectTableQ.c_str(),
            params->object_table.c_str(),
            params->bucket_table.c_str());
      if (!type.compare("ObjectData"))
        return fmt::format(CreateObjectDataTableQ.c_str(),
            params->objectdata_table.c_str(),
            params->object_table.c_str());
      if (!type.compare("Quota"))
        return fmt::format(CreateQuotaTableQ.c_str(),
            params->quota_table.c_str());

      ldout(params->cct, 0) << "Incorrect table type("<<type<<") specified" << dendl;

      return NULL;
    }

    string DeleteTableSchema(string table) {
      return fmt::format(DropQ.c_str(), table.c_str());
    }
    string ListTableSchema(string table) {
      return fmt::format(ListAllQ.c_str(), table.c_str());
    }

    virtual int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; }
    virtual int Execute(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; }
};

class InsertUserOp : public DBOp {
  private:
    /* For existing entires, -
     * (1) INSERT or REPLACE - it will delete previous entry and then
     * inserts new one. Since it deletes previos enties, it will
     * trigger all foriegn key cascade deletes or other triggers.
     * (2) INSERT or UPDATE - this will set NULL values to unassigned
     * fields.
     * more info: https://code-examples.net/en/q/377728
     *
     * For now using INSERT or REPLACE. If required of updating existing
     * record, will use another query.
     */
    const string Query = "INSERT OR REPLACE INTO '{}'	\
                          (UserID, Tenant, NS, DisplayName, UserEmail, \
                           AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                           SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                           System, PlacementName, PlacementStorageClass, PlacementTags, \
                           BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                           UserAttrs, UserVersion, UserVersionTag) \
                          VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
                              {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});";

  public:
    virtual ~InsertUserOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.user_table.c_str(),
          params.op.user.user_id.c_str(), params.op.user.tenant, params.op.user.ns,
          params.op.user.display_name, params.op.user.user_email,
          params.op.user.access_keys_id, params.op.user.access_keys_secret,
          params.op.user.access_keys, params.op.user.swift_keys,
          params.op.user.subusers, params.op.user.suspended,
          params.op.user.max_buckets, params.op.user.op_mask,
          params.op.user.user_caps, params.op.user.admin, params.op.user.system,
          params.op.user.placement_name, params.op.user.placement_storage_class,
          params.op.user.placement_tags, params.op.user.bucket_quota,
          params.op.user.temp_url_keys, params.op.user.user_quota,
          params.op.user.type, params.op.user.mfa_ids,
          params.op.user.assumed_role_arn, params.op.user.user_attrs,
          params.op.user.user_ver, params.op.user.user_ver_tag);
    }

};

class RemoveUserOp: public DBOp {
  private:
    const string Query =
      "DELETE from '{}' where UserID = {}";

  public:
    virtual ~RemoveUserOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.user_table.c_str(),
          params.op.user.user_id.c_str());
    }
};

class GetUserOp: public DBOp {
  private:
    /* If below query columns are updated, make sure to update the indexes
     * in list_user() cbk in sqliteDB.cc */
    const string Query = "SELECT \
                          UserID, Tenant, NS, DisplayName, UserEmail, \
                          AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                          SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                          System, PlacementName, PlacementStorageClass, PlacementTags, \
                          BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                          UserAttrs, UserVersion, UserVersionTag from '{}' where UserID = {}";

    const string QueryByEmail = "SELECT \
                                 UserID, Tenant, NS, DisplayName, UserEmail, \
                                 AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                                 SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                                 System, PlacementName, PlacementStorageClass, PlacementTags, \
                                 BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                                 UserAttrs, UserVersion, UserVersionTag from '{}' where UserEmail = {}";

    const string QueryByAccessKeys = "SELECT \
                                      UserID, Tenant, NS, DisplayName, UserEmail, \
                                      AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                                      SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                                      System, PlacementName, PlacementStorageClass, PlacementTags, \
                                      BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                                      UserAttrs, UserVersion, UserVersionTag from '{}' where AccessKeysID = {}";

    const string QueryByUserID = "SELECT \
                                  UserID, Tenant, NS, DisplayName, UserEmail, \
                                  AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                                  SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                                  System, PlacementName, PlacementStorageClass, PlacementTags, \
                                  BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                                  UserAttrs, UserVersion, UserVersionTag \
                                  from '{}' where Tenant = {} and UserID = {} and NS = {}";

  public:
    virtual ~GetUserOp() {}

    string Schema(DBOpPrepareParams &params) {
      if (params.op.query_str == "email") {
        return fmt::format(QueryByEmail.c_str(), params.user_table.c_str(),
            params.op.user.user_email.c_str());
      } else if (params.op.query_str == "access_key") {
        return fmt::format(QueryByAccessKeys.c_str(),
            params.user_table.c_str(),
            params.op.user.access_keys_id.c_str());
      } else if (params.op.query_str == "user_id") {
        return fmt::format(QueryByUserID.c_str(),
            params.user_table.c_str(),
            params.op.user.tenant.c_str(),
            params.op.user.user_id.c_str(),
            params.op.user.ns.c_str());
      } else {
        return fmt::format(Query.c_str(), params.user_table.c_str(),
            params.op.user.user_id.c_str());
      }
    }
};

class InsertBucketOp: public DBOp {
  private:
    const string Query =
      "INSERT OR REPLACE INTO '{}' \
      (BucketName, Tenant, Marker, BucketID, Size, SizeRounded, CreationTime, \
       Count, PlacementName, PlacementStorageClass, OwnerID, Flags, Zonegroup, \
       HasInstanceObj, Quota, RequesterPays, HasWebsite, WebsiteConf, \
       SwiftVersioning, SwiftVerLocation, \
       MdsearchConfig, NewBucketInstanceID, ObjectLock, \
       SyncPolicyInfoGroups, BucketAttrs, BucketVersion, BucketVersionTag, Mtime) \
      VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, \
          {}, {}, {}, {}, {}, {}, {}, {}, {}, \
          {}, {}, {}, {}, {}, {}, {}, {}, {}, {})";

  public:
    virtual ~InsertBucketOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.bucket_table.c_str(),
          params.op.bucket.bucket_name, params.op.bucket.tenant,
          params.op.bucket.marker, params.op.bucket.bucket_id,
          params.op.bucket.size, params.op.bucket.size_rounded,
          params.op.bucket.creation_time, params.op.bucket.count,
          params.op.bucket.placement_name, params.op.bucket.placement_storage_class,
          params.op.user.user_id,
          params.op.bucket.flags, params.op.bucket.zonegroup, params.op.bucket.has_instance_obj,
          params.op.bucket.quota, params.op.bucket.requester_pays, params.op.bucket.has_website,
          params.op.bucket.website_conf, params.op.bucket.swift_versioning,
          params.op.bucket.swift_ver_location, params.op.bucket.mdsearch_config,
          params.op.bucket.new_bucket_instance_id, params.op.bucket.obj_lock,
          params.op.bucket.sync_policy_info_groups, params.op.bucket.bucket_attrs,
          params.op.bucket.bucket_ver, params.op.bucket.bucket_ver_tag,
          params.op.bucket.mtime);
    }
};

class UpdateBucketOp: public DBOp {
  private:
    // Updates Info, Mtime, Version
    const string InfoQuery =
      "UPDATE '{}' SET Tenant = {}, Marker = {}, BucketID = {}, CreationTime = {}, \
      Count = {}, PlacementName = {}, PlacementStorageClass = {}, OwnerID = {}, Flags = {}, \
      Zonegroup = {}, HasInstanceObj = {}, Quota = {}, RequesterPays = {}, HasWebsite = {}, \
      WebsiteConf = {}, SwiftVersioning = {}, SwiftVerLocation = {}, MdsearchConfig = {}, \
      NewBucketInstanceID = {}, ObjectLock = {}, SyncPolicyInfoGroups = {}, \
      BucketVersion = {}, Mtime = {} WHERE BucketName = {}";
    // Updates Attrs, OwnerID, Mtime, Version
    const string AttrsQuery =
      "UPDATE '{}' SET OwnerID = {}, BucketAttrs = {}, Mtime = {}, BucketVersion = {} \
      WHERE BucketName = {}";
    // Updates OwnerID, CreationTime, Mtime, Version
    const string OwnerQuery =
      "UPDATE '{}' SET OwnerID = {}, CreationTime = {}, Mtime = {}, BucketVersion = {} WHERE BucketName = {}";

  public:
    virtual ~UpdateBucketOp() {}

    string Schema(DBOpPrepareParams &params) {
      if (params.op.query_str == "info") {
        return fmt::format(InfoQuery.c_str(), params.bucket_table.c_str(),
            params.op.bucket.tenant, params.op.bucket.marker, params.op.bucket.bucket_id,
            params.op.bucket.creation_time, params.op.bucket.count,
            params.op.bucket.placement_name, params.op.bucket.placement_storage_class,
            params.op.user.user_id,
            params.op.bucket.flags, params.op.bucket.zonegroup, params.op.bucket.has_instance_obj,
            params.op.bucket.quota, params.op.bucket.requester_pays, params.op.bucket.has_website,
            params.op.bucket.website_conf, params.op.bucket.swift_versioning,
            params.op.bucket.swift_ver_location, params.op.bucket.mdsearch_config,
            params.op.bucket.new_bucket_instance_id, params.op.bucket.obj_lock,
            params.op.bucket.sync_policy_info_groups,
            params.op.bucket.bucket_ver, params.op.bucket.mtime,
            params.op.bucket.bucket_name);
      }
      if (params.op.query_str == "attrs") {
        return fmt::format(AttrsQuery.c_str(), params.bucket_table.c_str(),
            params.op.user.user_id, params.op.bucket.bucket_attrs,
            params.op.bucket.mtime,
            params.op.bucket.bucket_ver, params.op.bucket.bucket_name.c_str());
      }
      if (params.op.query_str == "owner") {
        return fmt::format(OwnerQuery.c_str(), params.bucket_table.c_str(),
            params.op.user.user_id, params.op.bucket.creation_time,
            params.op.bucket.mtime,
            params.op.bucket.bucket_ver, params.op.bucket.bucket_name.c_str());
      }
      return "";
    }
};

class RemoveBucketOp: public DBOp {
  private:
    const string Query =
      "DELETE from '{}' where BucketName = {}";

  public:
    virtual ~RemoveBucketOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.bucket_table.c_str(),
          params.op.bucket.bucket_name.c_str());
    }
};

class GetBucketOp: public DBOp {
  private:
    const string Query = "SELECT  \
                          BucketName, BucketTable.Tenant, Marker, BucketID, Size, SizeRounded, CreationTime, \
                          Count, BucketTable.PlacementName, BucketTable.PlacementStorageClass, OwnerID, Flags, Zonegroup, \
                          HasInstanceObj, Quota, RequesterPays, HasWebsite, WebsiteConf, \
                          SwiftVersioning, SwiftVerLocation, \
                          MdsearchConfig, NewBucketInstanceID, ObjectLock, \
                          SyncPolicyInfoGroups, BucketAttrs, BucketVersion, BucketVersionTag, Mtime, NS \
                          from '{}' as BucketTable INNER JOIN '{}' ON OwnerID = UserID where BucketName = {}";

  public:
    virtual ~GetBucketOp() {}

    string Schema(DBOpPrepareParams &params) {
      //return fmt::format(Query.c_str(), params.op.bucket.bucket_name.c_str(),
      //          params.bucket_table.c_str(), params.user_table.c_str());
      return fmt::format(Query.c_str(),
          params.bucket_table.c_str(), params.user_table.c_str(),
          params.op.bucket.bucket_name.c_str());
    }
};

class ListUserBucketsOp: public DBOp {
  private:
    // once we have stats also stored, may have to update this query to join
    // these two tables.
    const string Query = "SELECT  \
                          BucketName, Tenant, Marker, BucketID, Size, SizeRounded, CreationTime, \
                          Count, PlacementName, PlacementStorageClass, OwnerID, Flags, Zonegroup, \
                          HasInstanceObj, Quota, RequesterPays, HasWebsite, WebsiteConf, \
                          SwiftVersioning, SwiftVerLocation, \
                          MdsearchConfig, NewBucketInstanceID, ObjectLock, \
                          SyncPolicyInfoGroups, BucketAttrs, BucketVersion, BucketVersionTag, Mtime \
                          FROM '{}' WHERE OwnerID = {} AND BucketName > {} ORDER BY BucketName ASC LIMIT {}";

  public:
    virtual ~ListUserBucketsOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.bucket_table.c_str(),
          params.op.user.user_id.c_str(), params.op.bucket.min_marker.c_str(),
          params.op.list_max_count.c_str());
    }
};

class InsertObjectOp: public DBOp {
  private:
    const string Query =
      "INSERT OR REPLACE INTO '{}' (BucketName, ObjectName) VALUES ({}, {})";

  public:
    virtual ~InsertObjectOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(),
          params.object_table.c_str(), params.op.bucket.bucket_name.c_str(),
          params.object.c_str());
    }
};

class RemoveObjectOp: public DBOp {
  private:
    const string Query =
      "DELETE from '{}' where BucketName = {} and ObjectName = {}";

  public:
    virtual ~RemoveObjectOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.object_table.c_str(),
          params.op.bucket.bucket_name.c_str(), params.object.c_str());
    }
};

class ListObjectOp: public DBOp {
  private:
    const string Query =
      "SELECT  * from '{}' where BucketName = {} and ObjectName = {}";
    // XXX: Include queries for specific bucket and user too

  public:
    virtual ~ListObjectOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.object_table.c_str(),
          params.op.bucket.bucket_name.c_str(), params.object.c_str());
    }
};

class PutObjectDataOp: public DBOp {
  private:
    const string Query =
      "INSERT OR REPLACE INTO '{}' (BucketName, ObjectName, Offset, Data, Size) \
      VALUES ({}, {}, {}, {}, {})";

  public:
    virtual ~PutObjectDataOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(),
          params.objectdata_table.c_str(),
          params.op.bucket.bucket_name.c_str(), params.object.c_str(),
          params.offset.c_str(), params.data.c_str(),
          params.datalen.c_str());
    }
};

class GetObjectDataOp: public DBOp {
  private:
    const string Query =
      "SELECT * from '{}' where BucketName = {} and ObjectName = {}";

  public:
    virtual ~GetObjectDataOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(),
          params.objectdata_table.c_str(), params.op.bucket.bucket_name.c_str(),
          params.object.c_str());
    }
};

class DeleteObjectDataOp: public DBOp {
  private:
    const string Query =
      "DELETE from '{}' where BucketName = {} and ObjectName = {}";

  public:
    virtual ~DeleteObjectDataOp() {}

    string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(),
          params.objectdata_table.c_str(), params.op.bucket.bucket_name.c_str(),
          params.object.c_str());
    }
};

class DB {
  private:
    const string db_name;
    const string user_table;
    const string bucket_table;
    const string quota_table;
    static map<string, class ObjectOp*> objectmap;
    pthread_mutex_t mutex; // to protect objectmap and other shared
    // objects if any. This mutex is taken
    // before processing every fop (i.e, in
    // ProcessOp()). If required this can be
    // made further granular by taking separate
    // locks for objectmap and db operations etc.

  protected:
    void *db;
    CephContext *cct;
    const DoutPrefix dp;
    uint64_t max_bucket_id = 0;

  public:	
    DB(string db_name, CephContext *_cct) : db_name(db_name),
    user_table(db_name+".user.table"),
    bucket_table(db_name+".bucket.table"),
    quota_table(db_name+".quota.table"),
    cct(_cct),
    dp(_cct, dout_subsys, "rgw DBStore backend: ")
  {}
    /*	DB() {}*/

    DB(CephContext *_cct) : db_name("default_db"),
    user_table("user.table"),
    bucket_table("bucket.table"),
    quota_table("quota.table"),
    cct(_cct),
    dp(_cct, dout_subsys, "rgw DBStore backend: ")
  {}
    virtual	~DB() {}

    const string getDBname() { return db_name + ".db"; }
    const string getUserTable() { return user_table; }
    const string getBucketTable() { return bucket_table; }
    const string getQuotaTable() { return quota_table; }

    map<string, class ObjectOp*> getObjectMap();

    struct DBOps dbops; // DB operations, make it private?

    void set_context(CephContext *_cct) {
      cct = _cct;
    }

    CephContext *ctx() { return cct; }
    const DoutPrefixProvider *get_def_dpp() { return &dp; }

    int Initialize(string logfile, int loglevel);
    int Destroy(const DoutPrefixProvider *dpp);
    int LockInit(const DoutPrefixProvider *dpp);
    int LockDestroy(const DoutPrefixProvider *dpp);
    int Lock(const DoutPrefixProvider *dpp);
    int Unlock(const DoutPrefixProvider *dpp);

    int InitializeParams(const DoutPrefixProvider *dpp, string Op, DBOpParams *params);
    int ProcessOp(const DoutPrefixProvider *dpp, string Op, DBOpParams *params);
    DBOp* getDBOp(const DoutPrefixProvider *dpp, string Op, struct DBOpParams *params);
    int objectmapInsert(const DoutPrefixProvider *dpp, string bucket, void *ptr);
    int objectmapDelete(const DoutPrefixProvider *dpp, string bucket);

    virtual void *openDB(const DoutPrefixProvider *dpp) { return NULL; }
    virtual int closeDB(const DoutPrefixProvider *dpp) { return 0; }
    virtual int createTables(const DoutPrefixProvider *dpp) { return 0; }
    virtual int InitializeDBOps(const DoutPrefixProvider *dpp) { return 0; }
    virtual int FreeDBOps(const DoutPrefixProvider *dpp) { return 0; }
    virtual int InitPrepareParams(const DoutPrefixProvider *dpp, DBOpPrepareParams &params) = 0;

    virtual int ListAllBuckets(const DoutPrefixProvider *dpp, DBOpParams *params) = 0;
    virtual int ListAllUsers(const DoutPrefixProvider *dpp, DBOpParams *params) = 0;
    virtual int ListAllObjects(const DoutPrefixProvider *dpp, DBOpParams *params) = 0;

    int get_user(const DoutPrefixProvider *dpp,
        const std::string& query_str, const std::string& query_str_val,
        RGWUserInfo& uinfo, map<string, bufferlist> *pattrs,
        RGWObjVersionTracker *pobjv_tracker);
    int store_user(const DoutPrefixProvider *dpp,
        RGWUserInfo& uinfo, bool exclusive, map<string, bufferlist> *pattrs,
        RGWObjVersionTracker *pobjv_tracker, RGWUserInfo* pold_info);
    int remove_user(const DoutPrefixProvider *dpp,
        RGWUserInfo& uinfo, RGWObjVersionTracker *pobjv_tracker);
    int get_bucket_info(const DoutPrefixProvider *dpp, const std::string& query_str,
        const std::string& query_str_val,
        RGWBucketInfo& info, rgw::sal::Attrs* pattrs, ceph::real_time* pmtime,
        obj_version* pbucket_version);
    int create_bucket(const DoutPrefixProvider *dpp,
        const RGWUserInfo& owner, rgw_bucket& bucket,
        const string& zonegroup_id,
        const rgw_placement_rule& placement_rule,
        const string& swift_ver_location,
        const RGWQuotaInfo * pquota_info,
        map<std::string, bufferlist>& attrs,
        RGWBucketInfo& info,
        obj_version *pobjv,
        obj_version *pep_objv,
        real_time creation_time,
        rgw_bucket *pmaster_bucket,
        uint32_t *pmaster_num_shards,
        optional_yield y,
        bool exclusive);

    int next_bucket_id() { return ++max_bucket_id; };

    int remove_bucket(const DoutPrefixProvider *dpp, const RGWBucketInfo info);
    int list_buckets(const DoutPrefixProvider *dpp, const rgw_user& user,
        const string& marker,
        const string& end_marker,
        uint64_t max,
        bool need_stats,
        RGWUserBuckets *buckets,
        bool *is_truncated);
    int update_bucket(const DoutPrefixProvider *dpp, const std::string& query_str,
        RGWBucketInfo& info, bool exclusive,
        const rgw_user* powner_id, map<std::string, bufferlist>* pattrs,
        ceph::real_time* pmtime, RGWObjVersionTracker* pobjv);
};

} } // namespace rgw::store

#endif
