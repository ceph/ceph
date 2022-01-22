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
#include "rgw/rgw_sal.h"
#include "rgw/rgw_common.h"
#include "rgw/rgw_bucket.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_context.h"
#include "rgw/rgw_obj_manifest.h"
#include "rgw/rgw_multi.h"

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
  std::string min_marker;
  std::string max_marker;
  std::list<RGWBucketEnt> list_entries;
};

struct DBOpObjectInfo {
  RGWAccessControlPolicy acls;
  RGWObjState state;

  /* Below are taken from rgw_bucket_dir_entry */
  RGWObjCategory category;
  std::string etag;
  std::string owner;
  std::string owner_display_name;
  std::string content_type;
  std::string storage_class;
  bool appendable;
  uint64_t index_ver;
  std::string tag;
  uint16_t flags;
  uint64_t versioned_epoch;

  /* from state.manifest (RGWObjManifest) */
  std::map<uint64_t, RGWObjManifestPart> objs;
  uint64_t head_size{0};
  rgw_placement_rule head_placement_rule;
  uint64_t max_head_size{0};
  std::string prefix;
  rgw_bucket_placement tail_placement; /* might be different than the original bucket,
                                          as object might have been copied across pools */
  std::map<uint64_t, RGWObjManifestRule> rules;
  std::string tail_instance; /* tail object's instance */


  /* Obj's omap <key,value> store */
  std::map<std::string, bufferlist> omap;

  /* Extra fields */
  bool is_multipart;
  std::list<RGWUploadPartInfo> mp_parts;

  bufferlist head_data;
  std::string min_marker;
  std::string max_marker;
  std::list<rgw_bucket_dir_entry> list_entries;
  /* Below used to update mp_parts obj name
   * from meta object to src object on completion */
  rgw_obj_key new_obj_key;
};

struct DBOpObjectDataInfo {
  RGWObjState state;
  uint64_t part_num;
  std::string multipart_part_str;
  uint64_t offset;
  uint64_t size;
  bufferlist data{};
};

struct DBOpLCHeadInfo {
  std::string index;
  rgw::sal::Lifecycle::LCHead head;
};

struct DBOpLCEntryInfo {
  std::string index;
  rgw::sal::Lifecycle::LCEntry entry;
  // used for list query
  std::string min_marker;
  std::list<rgw::sal::Lifecycle::LCEntry> list_entries;
};

struct DBOpInfo {
  std::string name; // Op name
  /* Support only single access_key for now. So store
   * it separately as primary access_key_id & secret to
   * be able to query easily.
   *
   * XXX: Swift keys and subuser not supported for now */
  DBOpUserInfo user;
  std::string query_str;
  DBOpBucketInfo bucket;
  DBOpObjectInfo obj;
  DBOpObjectDataInfo obj_data;
  DBOpLCHeadInfo lc_head;
  DBOpLCEntryInfo lc_entry;
  uint64_t list_max_count;
};

struct DBOpParams {
  CephContext *cct;

  /* Tables */
  std::string user_table;
  std::string bucket_table;
  std::string object_table;

  /* Ops*/
  DBOpInfo op;

  /* Below are subject to change */
  std::string objectdata_table;
  std::string quota_table;
  std::string lc_head_table;
  std::string lc_entry_table;
  std::string obj;
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
  std::string user_id = ":user_id";
  std::string tenant = ":tenant";
  std::string ns = ":ns";
  std::string display_name = ":display_name";
  std::string user_email = ":user_email";
  /* Support only single access_key for now. So store
   * it separately as primary access_key_id & secret to
   * be able to query easily.
   *
   * In future, when need to support & query from multiple
   * access keys, better to maintain them in a separate table.
   */
  std::string access_keys_id = ":access_keys_id";
  std::string access_keys_secret = ":access_keys_secret";
  std::string access_keys = ":access_keys";
  std::string swift_keys = ":swift_keys";
  std::string subusers = ":subusers";
  std::string suspended = ":suspended";
  std::string max_buckets = ":max_buckets";
  std::string op_mask = ":op_mask";
  std::string user_caps = ":user_caps";
  std::string admin = ":admin";
  std::string system = ":system";
  std::string placement_name = ":placement_name";
  std::string placement_storage_class = ":placement_storage_class";
  std::string placement_tags = ":placement_tags";
  std::string bucket_quota = ":bucket_quota";
  std::string temp_url_keys = ":temp_url_keys";
  std::string user_quota = ":user_quota";
  std::string type = ":type";
  std::string mfa_ids = ":mfa_ids";
  std::string assumed_role_arn = ":assumed_role_arn";
  std::string user_attrs = ":user_attrs";
  std::string user_ver = ":user_vers";
  std::string user_ver_tag = ":user_ver_tag";
};

struct DBOpBucketPrepareInfo {
  std::string bucket_name = ":bucket_name";
  std::string tenant = ":tenant";
  std::string marker = ":marker";
  std::string bucket_id = ":bucket_id";
  std::string size = ":size";
  std::string size_rounded = ":size_rounded";
  std::string creation_time = ":creation_time";
  std::string count = ":count";
  std::string placement_name = ":placement_name";
  std::string placement_storage_class = ":placement_storage_class";
  /* ownerid - maps to DBOpUserPrepareInfo */
  std::string flags = ":flags";
  std::string zonegroup = ":zonegroup";
  std::string has_instance_obj = ":has_instance_obj";
  std::string quota = ":quota";
  std::string requester_pays = ":requester_pays";
  std::string has_website = ":has_website";
  std::string website_conf = ":website_conf";
  std::string swift_versioning = ":swift_versioning";
  std::string swift_ver_location = ":swift_ver_location";
  std::string mdsearch_config = ":mdsearch_config";
  std::string new_bucket_instance_id = ":new_bucket_instance_id";
  std::string obj_lock = ":obj_lock";
  std::string sync_policy_info_groups = ":sync_policy_info_groups";
  std::string bucket_attrs = ":bucket_attrs";
  std::string bucket_ver = ":bucket_vers";
  std::string bucket_ver_tag = ":bucket_ver_tag";
  std::string mtime = ":mtime";
  std::string min_marker = ":min_marker";
  std::string max_marker = ":max_marker";
};

struct DBOpObjectPrepareInfo {
  std::string obj_name = ":obj_name";
  std::string obj_instance = ":obj_instance";
  std::string obj_ns  = ":obj_ns";
  std::string acls = ":acls";
  std::string index_ver = ":index_ver";
  std::string tag = ":tag";
  std::string flags = ":flags";
  std::string versioned_epoch = ":versioned_epoch";
  std::string obj_category = ":obj_category";
  std::string etag = ":etag";
  std::string owner = ":owner";
  std::string owner_display_name = ":owner_display_name";
  std::string storage_class = ":storage_class";
  std::string appendable = ":appendable";
  std::string content_type = ":content_type";
  std::string index_hash_source = ":index_hash_source";
  std::string obj_size = ":obj_size";
  std::string accounted_size = ":accounted_size";
  std::string mtime = ":mtime";
  std::string epoch = ":epoch";
  std::string obj_tag = ":obj_tag";
  std::string tail_tag = ":tail_tag";
  std::string write_tag = ":write_tag";
  std::string fake_tag = ":fake_tag";
  std::string shadow_obj = ":shadow_obj";
  std::string has_data = ":has_data";
  std::string is_olh = ":is_ols";
  std::string olh_tag = ":olh_tag";
  std::string pg_ver = ":pg_ver";
  std::string zone_short_id = ":zone_short_id";
  std::string obj_version = ":obj_version";
  std::string obj_version_tag = ":obj_version_tag";
  std::string obj_attrs = ":obj_attrs";
  std::string head_size = ":head_size";
  std::string max_head_size = ":max_head_size";
  std::string prefix = ":prefix";
  std::string tail_instance = ":tail_instance";
  std::string head_placement_rule_name = ":head_placement_rule_name";
  std::string head_placement_storage_class  = ":head_placement_storage_class";
  std::string tail_placement_rule_name = ":tail_placement_rule_name";
  std::string tail_placement_storage_class  = ":tail_placement_storage_class";
  std::string manifest_part_objs = ":manifest_part_objs";
  std::string manifest_part_rules = ":manifest_part_rules";
  std::string omap = ":omap";
  std::string is_multipart = ":is_multipart";
  std::string mp_parts = ":mp_parts";
  std::string head_data = ":head_data";
  std::string min_marker = ":min_marker";
  std::string max_marker = ":max_marker";
  /* Below used to update mp_parts obj name
   * from meta object to src object on completion */
  std::string new_obj_name = ":new_obj_name";
  std::string new_obj_instance = ":new_obj_instance";
  std::string new_obj_ns  = ":new_obj_ns";
};

struct DBOpObjectDataPrepareInfo {
  std::string part_num = ":part_num";
  std::string offset = ":offset";
  std::string data = ":data";
  std::string size = ":size";
  std::string multipart_part_str = ":multipart_part_str";
};

struct DBOpLCEntryPrepareInfo {
  std::string index = ":index";
  std::string bucket_name = ":bucket_name";
  std::string start_time = ":start_time";
  std::string status = ":status";
  std::string min_marker = ":min_marker";
};

struct DBOpLCHeadPrepareInfo {
  std::string index = ":index";
  std::string start_date = ":start_date";
  std::string marker = ":marker";
};

struct DBOpPrepareInfo {
  DBOpUserPrepareInfo user;
  std::string query_str = ":query_str";
  DBOpBucketPrepareInfo bucket;
  DBOpObjectPrepareInfo obj;
  DBOpObjectDataPrepareInfo obj_data;
  DBOpLCHeadPrepareInfo lc_head;
  DBOpLCEntryPrepareInfo lc_entry;
  std::string list_max_count = ":list_max_count";
};

struct DBOpPrepareParams {
  /* Tables */
  std::string user_table;
  std::string bucket_table;
  std::string object_table;

  /* Ops */
  DBOpPrepareInfo op;


  /* below subject to change */
  std::string objectdata_table;
  std::string quota_table;
  std::string lc_head_table;
  std::string lc_entry_table;
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
  class InsertLCEntryOp *InsertLCEntry;
  class RemoveLCEntryOp *RemoveLCEntry;
  class GetLCEntryOp *GetLCEntry;
  class ListLCEntriesOp *ListLCEntries;
  class InsertLCHeadOp *InsertLCHead;
  class RemoveLCHeadOp *RemoveLCHead;
  class GetLCHeadOp *GetLCHead;
};

class ObjectOp {
  public:
    ObjectOp() {};

    virtual ~ObjectOp() {}

    class PutObjectOp *PutObject;
    class DeleteObjectOp *DeleteObject;
    class GetObjectOp *GetObject;
    class UpdateObjectOp *UpdateObject;
    class ListBucketObjectsOp *ListBucketObjects;
    class PutObjectDataOp *PutObjectData;
    class UpdateObjectDataOp *UpdateObjectData;
    class GetObjectDataOp *GetObjectData;
    class DeleteObjectDataOp *DeleteObjectData;

    virtual int InitializeObjectOps(std::string db_name, const DoutPrefixProvider *dpp) { return 0; }
    virtual int FreeObjectOps(const DoutPrefixProvider *dpp) { return 0; }
};

class DBOp {
  private:
    const std::string CreateUserTableQ =
      /* Corresponds to rgw::sal::User
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

    const std::string CreateBucketTableQ =
      /* Corresponds to rgw::sal::Bucket
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

    const std::string CreateObjectTableQ =
      /* Corresponds to rgw::sal::Object
       *
       *  For now only BucketName, ObjName is made Primary key.
       *  If multiple tenants are stored in single .db handle, should
       *  include Tenant too in the Primary Key. Also should
       *  reference (BucketID, Tenant) as Foreign key.
       * 
       * referring to 
       * - rgw_bucket_dir_entry - following are added for now
       *   flags,
       *   versioned_epoch
       *   tag
       *   index_ver
       *   meta.category
       *   meta.etag
       *   meta.storageclass
       *   meta.appendable
       *   meta.content_type
       *   meta.owner
       *   meta.owner_display_name
       *
       * - RGWObjState. Below are omitted from that struct
       *    as they seem in-memory variables
       *    * is_atomic, has_atts, exists, prefetch_data, keep_tail, 
       * - RGWObjManifest
       *
       * Extra field added "IsMultipart" to flag multipart uploads,
       * HeadData to store first chunk data.
       */
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      ObjName TEXT NOT NULL , \
      ObjInstance TEXT, \
      ObjNS TEXT, \
      BucketName TEXT NOT NULL , \
      ACLs    BLOB,   \
      IndexVer    INTEGER,    \
      Tag TEXT,   \
      Flags INTEGER, \
      VersionedEpoch INTEGER, \
      ObjCategory INTEGER,    \
      Etag   TEXT,    \
      Owner TEXT, \
      OwnerDisplayName TEXT,  \
      StorageClass    TEXT,   \
      Appendable  BOOL,   \
      ContentType TEXT,   \
      IndexHashSource TEXT, \
      ObjSize  INTEGER,   \
      AccountedSize INTEGER,  \
      Mtime   BLOB,   \
      Epoch  INTEGER, \
      ObjTag  BLOB,   \
      TailTag BLOB,   \
      WriteTag    TEXT,   \
      FakeTag BOOL,   \
      ShadowObj   TEXT,   \
      HasData  BOOL,  \
      IsOLH BOOL,  \
      OLHTag    BLOB, \
      PGVer   INTEGER, \
      ZoneShortID  INTEGER,  \
      ObjVersion   INTEGER,    \
      ObjVersionTag TEXT,      \
      ObjAttrs    BLOB,   \
      HeadSize    INTEGER,    \
      MaxHeadSize    INTEGER,    \
      Prefix      std::string, \
      TailInstance    std::string, \
      HeadPlacementRuleName   std::string, \
      HeadPlacementRuleStorageClass String, \
      TailPlacementRuleName   std::string, \
      TailPlacementStorageClass String, \
      ManifestPartObjs    BLOB,   \
      ManifestPartRules   BLOB,   \
      Omap    BLOB,   \
      IsMultipart     BOOL,   \
      MPPartsList    BLOB,   \
      HeadData  BLOB,   \
      PRIMARY KEY (ObjName, ObjInstance, BucketName), \
      FOREIGN KEY (BucketName) \
      REFERENCES '{}' (BucketName) ON DELETE CASCADE ON UPDATE CASCADE \n);";

    const std::string CreateObjectDataTableQ =
      /* Extra field 'MultipartPartStr' added which signifies multipart
       * <uploadid + partnum>. For regular object, it is '0.0'
       *
       *  - part: a collection of stripes that make a contiguous part of an
       object. A regular object will only have one part (although might have
       many stripes), a multipart object might have many parts. Each part
       has a fixed stripe size (ObjChunkSize), although the last stripe of a
       part might be smaller than that.
       */
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      ObjName TEXT NOT NULL , \
      ObjInstance TEXT, \
      ObjNS TEXT, \
      BucketName TEXT NOT NULL , \
      MultipartPartStr TEXT, \
      PartNum  INTEGER NOT NULL, \
      Offset   INTEGER, \
      Size 	 INTEGER, \
      Data     BLOB,             \
      PRIMARY KEY (ObjName, BucketName, ObjInstance, MultipartPartStr, PartNum), \
      FOREIGN KEY (BucketName, ObjName, ObjInstance) \
      REFERENCES '{}' (BucketName, ObjName, ObjInstance) ON DELETE CASCADE ON UPDATE CASCADE \n);";

    const std::string CreateQuotaTableQ =
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      QuotaID INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE , \
      MaxSizeSoftThreshold INTEGER ,	\
      MaxObjsSoftThreshold INTEGER ,	\
      MaxSize	INTEGER ,		\
      MaxObjects INTEGER ,		\
      Enabled Boolean ,		\
      CheckOnRaw Boolean \n);";

    const std::string CreateLCEntryTableQ =
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      LCIndex  TEXT NOT NULL , \
      BucketName TEXT NOT NULL , \
      StartTime  INTEGER , \
      Status     INTEGER , \
      PRIMARY KEY (LCIndex, BucketName), \
      FOREIGN KEY (BucketName) \
      REFERENCES '{}' (BucketName) ON DELETE CASCADE ON UPDATE CASCADE \n);";

    const std::string CreateLCHeadTableQ =
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      LCIndex  TEXT NOT NULL , \
      Marker TEXT , \
      StartDate  INTEGER , \
      PRIMARY KEY (LCIndex) \n);";

    const std::string DropQ = "DROP TABLE IF EXISTS '{}'";
    const std::string ListAllQ = "SELECT  * from '{}'";

  public:
    DBOp() {}
    virtual ~DBOp() {}
    std::mutex mtx; // to protect prepared stmt

    std::string CreateTableSchema(std::string type, DBOpParams *params) {
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
      if (!type.compare("LCHead"))
        return fmt::format(CreateLCHeadTableQ.c_str(),
            params->lc_head_table.c_str());
      if (!type.compare("LCEntry"))
        return fmt::format(CreateLCEntryTableQ.c_str(),
            params->lc_entry_table.c_str(),
            params->bucket_table.c_str());

      lsubdout(params->cct, rgw, 0) << "rgw dbstore: Incorrect table type("<<type<<") specified" << dendl;

      return NULL;
    }

    std::string DeleteTableSchema(std::string table) {
      return fmt::format(DropQ.c_str(), table.c_str());
    }
    std::string ListTableSchema(std::string table) {
      return fmt::format(ListAllQ.c_str(), table.c_str());
    }

    virtual int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; }
    virtual int Bind(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; }
    virtual int Execute(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; }
};

class InsertUserOp : virtual public DBOp {
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
    const std::string Query = "INSERT OR REPLACE INTO '{}'	\
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

    std::string Schema(DBOpPrepareParams &params) {
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

class RemoveUserOp: virtual public DBOp {
  private:
    const std::string Query =
      "DELETE from '{}' where UserID = {}";

  public:
    virtual ~RemoveUserOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.user_table.c_str(),
          params.op.user.user_id.c_str());
    }
};

class GetUserOp: virtual public DBOp {
  private:
    /* If below query columns are updated, make sure to update the indexes
     * in list_user() cbk in sqliteDB.cc */
    const std::string Query = "SELECT \
                          UserID, Tenant, NS, DisplayName, UserEmail, \
                          AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                          SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                          System, PlacementName, PlacementStorageClass, PlacementTags, \
                          BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                          UserAttrs, UserVersion, UserVersionTag from '{}' where UserID = {}";

    const std::string QueryByEmail = "SELECT \
                                 UserID, Tenant, NS, DisplayName, UserEmail, \
                                 AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                                 SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                                 System, PlacementName, PlacementStorageClass, PlacementTags, \
                                 BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                                 UserAttrs, UserVersion, UserVersionTag from '{}' where UserEmail = {}";

    const std::string QueryByAccessKeys = "SELECT \
                                      UserID, Tenant, NS, DisplayName, UserEmail, \
                                      AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                                      SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                                      System, PlacementName, PlacementStorageClass, PlacementTags, \
                                      BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                                      UserAttrs, UserVersion, UserVersionTag from '{}' where AccessKeysID = {}";

    const std::string QueryByUserID = "SELECT \
                                  UserID, Tenant, NS, DisplayName, UserEmail, \
                                  AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                                  SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                                  System, PlacementName, PlacementStorageClass, PlacementTags, \
                                  BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                                  UserAttrs, UserVersion, UserVersionTag \
                                  from '{}' where Tenant = {} and UserID = {} and NS = {}";

  public:
    virtual ~GetUserOp() {}

    std::string Schema(DBOpPrepareParams &params) {
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

class InsertBucketOp: virtual public DBOp {
  private:
    const std::string Query =
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

    std::string Schema(DBOpPrepareParams &params) {
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

class UpdateBucketOp: virtual public DBOp {
  private:
    // Updates Info, Mtime, Version
    const std::string InfoQuery =
      "UPDATE '{}' SET Tenant = {}, Marker = {}, BucketID = {}, CreationTime = {}, \
      Count = {}, PlacementName = {}, PlacementStorageClass = {}, OwnerID = {}, Flags = {}, \
      Zonegroup = {}, HasInstanceObj = {}, Quota = {}, RequesterPays = {}, HasWebsite = {}, \
      WebsiteConf = {}, SwiftVersioning = {}, SwiftVerLocation = {}, MdsearchConfig = {}, \
      NewBucketInstanceID = {}, ObjectLock = {}, SyncPolicyInfoGroups = {}, \
      BucketVersion = {}, Mtime = {} WHERE BucketName = {}";
    // Updates Attrs, OwnerID, Mtime, Version
    const std::string AttrsQuery =
      "UPDATE '{}' SET OwnerID = {}, BucketAttrs = {}, Mtime = {}, BucketVersion = {} \
      WHERE BucketName = {}";
    // Updates OwnerID, CreationTime, Mtime, Version
    const std::string OwnerQuery =
      "UPDATE '{}' SET OwnerID = {}, CreationTime = {}, Mtime = {}, BucketVersion = {} WHERE BucketName = {}";

  public:
    virtual ~UpdateBucketOp() {}

    std::string Schema(DBOpPrepareParams &params) {
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

class RemoveBucketOp: virtual public DBOp {
  private:
    const std::string Query =
      "DELETE from '{}' where BucketName = {}";

  public:
    virtual ~RemoveBucketOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.bucket_table.c_str(),
          params.op.bucket.bucket_name.c_str());
    }
};

class GetBucketOp: virtual public DBOp {
  private:
    const std::string Query = "SELECT  \
                          BucketName, BucketTable.Tenant, Marker, BucketID, Size, SizeRounded, CreationTime, \
                          Count, BucketTable.PlacementName, BucketTable.PlacementStorageClass, OwnerID, Flags, Zonegroup, \
                          HasInstanceObj, Quota, RequesterPays, HasWebsite, WebsiteConf, \
                          SwiftVersioning, SwiftVerLocation, \
                          MdsearchConfig, NewBucketInstanceID, ObjectLock, \
                          SyncPolicyInfoGroups, BucketAttrs, BucketVersion, BucketVersionTag, Mtime, NS \
                          from '{}' as BucketTable INNER JOIN '{}' ON OwnerID = UserID where BucketName = {}";

  public:
    virtual ~GetBucketOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      //return fmt::format(Query.c_str(), params.op.bucket.bucket_name.c_str(),
      //          params.bucket_table.c_str(), params.user_table.c_str());
      return fmt::format(Query.c_str(),
          params.bucket_table.c_str(), params.user_table.c_str(),
          params.op.bucket.bucket_name.c_str());
    }
};

class ListUserBucketsOp: virtual public DBOp {
  private:
    // once we have stats also stored, may have to update this query to join
    // these two tables.
    const std::string Query = "SELECT  \
                          BucketName, Tenant, Marker, BucketID, Size, SizeRounded, CreationTime, \
                          Count, PlacementName, PlacementStorageClass, OwnerID, Flags, Zonegroup, \
                          HasInstanceObj, Quota, RequesterPays, HasWebsite, WebsiteConf, \
                          SwiftVersioning, SwiftVerLocation, \
                          MdsearchConfig, NewBucketInstanceID, ObjectLock, \
                          SyncPolicyInfoGroups, BucketAttrs, BucketVersion, BucketVersionTag, Mtime \
                          FROM '{}' WHERE OwnerID = {} AND BucketName > {} ORDER BY BucketName ASC LIMIT {}";

  public:
    virtual ~ListUserBucketsOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.bucket_table.c_str(),
          params.op.user.user_id.c_str(), params.op.bucket.min_marker.c_str(),
          params.op.list_max_count.c_str());
    }
};

class PutObjectOp: virtual public DBOp {
  private:
    const std::string Query =
      "INSERT OR REPLACE INTO '{}' \
      (ObjName, ObjInstance, ObjNS, BucketName, ACLs, IndexVer, Tag, \
       Flags, VersionedEpoch, ObjCategory, Etag, Owner, OwnerDisplayName, \
       StorageClass, Appendable, ContentType, IndexHashSource, ObjSize, \
       AccountedSize, Mtime, Epoch, ObjTag, TailTag, WriteTag, FakeTag, \
       ShadowObj, HasData, IsOLH, OLHTag, PGVer, ZoneShortID, \
       ObjVersion, ObjVersionTag, ObjAttrs, HeadSize, MaxHeadSize, \
       Prefix, TailInstance, HeadPlacementRuleName, HeadPlacementRuleStorageClass, \
       TailPlacementRuleName, TailPlacementStorageClass, \
       ManifestPartObjs, ManifestPartRules, Omap, IsMultipart, MPPartsList, HeadData )     \
      VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
          {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
          {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})";

  public:
    virtual ~PutObjectOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(),
          params.object_table.c_str(), params.op.obj.obj_name,
          params.op.obj.obj_instance, params.op.obj.obj_ns,
          params.op.bucket.bucket_name, params.op.obj.acls, params.op.obj.index_ver,
          params.op.obj.tag, params.op.obj.flags, params.op.obj.versioned_epoch,
          params.op.obj.obj_category, params.op.obj.etag, params.op.obj.owner,
          params.op.obj.owner_display_name, params.op.obj.storage_class,
          params.op.obj.appendable, params.op.obj.content_type,
          params.op.obj.index_hash_source, params.op.obj.obj_size,
          params.op.obj.accounted_size, params.op.obj.mtime,
          params.op.obj.epoch, params.op.obj.obj_tag, params.op.obj.tail_tag,
          params.op.obj.write_tag, params.op.obj.fake_tag, params.op.obj.shadow_obj,
          params.op.obj.has_data, params.op.obj.is_olh, params.op.obj.olh_tag,
          params.op.obj.pg_ver, params.op.obj.zone_short_id,
          params.op.obj.obj_version, params.op.obj.obj_version_tag,
          params.op.obj.obj_attrs, params.op.obj.head_size,
          params.op.obj.max_head_size, params.op.obj.prefix,
          params.op.obj.tail_instance,
          params.op.obj.head_placement_rule_name,
          params.op.obj.head_placement_storage_class,
          params.op.obj.tail_placement_rule_name,
          params.op.obj.tail_placement_storage_class,
          params.op.obj.manifest_part_objs,
          params.op.obj.manifest_part_rules, params.op.obj.omap,
          params.op.obj.is_multipart, params.op.obj.mp_parts, params.op.obj.head_data);
    }
};

class DeleteObjectOp: virtual public DBOp {
  private:
    const std::string Query =
      "DELETE from '{}' where BucketName = {} and ObjName = {} and ObjInstance = {}";

  public:
    virtual ~DeleteObjectOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.object_table.c_str(),
          params.op.bucket.bucket_name.c_str(),
          params.op.obj.obj_name.c_str(),
          params.op.obj.obj_instance.c_str());
    }
};

class GetObjectOp: virtual public DBOp {
  private:
    const std::string Query =
      "SELECT  \
      ObjName, ObjInstance, ObjNS, BucketName, ACLs, IndexVer, Tag, \
      Flags, VersionedEpoch, ObjCategory, Etag, Owner, OwnerDisplayName, \
      StorageClass, Appendable, ContentType, IndexHashSource, ObjSize, \
      AccountedSize, Mtime, Epoch, ObjTag, TailTag, WriteTag, FakeTag, \
      ShadowObj, HasData, IsOLH, OLHTag, PGVer, ZoneShortID, \
      ObjVersion, ObjVersionTag, ObjAttrs, HeadSize, MaxHeadSize, \
      Prefix, TailInstance, HeadPlacementRuleName, HeadPlacementRuleStorageClass, \
      TailPlacementRuleName, TailPlacementStorageClass, \
      ManifestPartObjs, ManifestPartRules, Omap, IsMultipart, MPPartsList, HeadData from '{}' \
      where BucketName = {} and ObjName = {} and ObjInstance = {}";

  public:
    virtual ~GetObjectOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(),
          params.object_table.c_str(),
          params.op.bucket.bucket_name.c_str(),
          params.op.obj.obj_name.c_str(),
          params.op.obj.obj_instance.c_str());
    }
};

class ListBucketObjectsOp: virtual public DBOp {
  private:
    // once we have stats also stored, may have to update this query to join
    // these two tables.
    const std::string Query =
      "SELECT  \
      ObjName, ObjInstance, ObjNS, BucketName, ACLs, IndexVer, Tag, \
      Flags, VersionedEpoch, ObjCategory, Etag, Owner, OwnerDisplayName, \
      StorageClass, Appendable, ContentType, IndexHashSource, ObjSize, \
      AccountedSize, Mtime, Epoch, ObjTag, TailTag, WriteTag, FakeTag, \
      ShadowObj, HasData, IsOLH, OLHTag, PGVer, ZoneShortID, \
      ObjVersion, ObjVersionTag, ObjAttrs, HeadSize, MaxHeadSize, \
      Prefix, TailInstance, HeadPlacementRuleName, HeadPlacementRuleStorageClass, \
      TailPlacementRuleName, TailPlacementStorageClass, \
      ManifestPartObjs, ManifestPartRules, Omap, IsMultipart, MPPartsList, HeadData from '{}' \
      where BucketName = {} and ObjName > {} ORDER BY ObjName ASC LIMIT {}";
  public:
    virtual ~ListBucketObjectsOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      /* XXX: Include prefix, delim */
      return fmt::format(Query.c_str(),
          params.object_table.c_str(),
          params.op.bucket.bucket_name.c_str(),
          params.op.obj.min_marker.c_str(),
          params.op.list_max_count.c_str());
    }
};

class UpdateObjectOp: virtual public DBOp {
  private:
    // Updates Omap
    const std::string OmapQuery =
      "UPDATE '{}' SET Omap = {}, Mtime = {} \
      where BucketName = {} and ObjName = {} and ObjInstance = {}";
    const std::string AttrsQuery =
      "UPDATE '{}' SET ObjAttrs = {}, Mtime = {}  \
      where BucketName = {} and ObjName = {} and ObjInstance = {}";
    const std::string MPQuery =
      "UPDATE '{}' SET MPPartsList = {}, Mtime = {}  \
      where BucketName = {} and ObjName = {} and ObjInstance = {}";
    const std::string MetaQuery =
      "UPDATE '{}' SET \
       ObjNS = {}, ACLs = {}, IndexVer = {}, Tag = {}, Flags = {}, VersionedEpoch = {}, \
       ObjCategory = {}, Etag = {}, Owner = {}, OwnerDisplayName = {}, \
       StorageClass = {}, Appendable = {}, ContentType = {}, \
       IndexHashSource = {}, ObjSize = {}, AccountedSize = {}, Mtime = {}, \
       Epoch = {}, ObjTag = {}, TailTag = {}, WriteTag = {}, FakeTag = {}, \
       ShadowObj = {}, HasData = {}, IsOLH = {}, OLHTag = {}, PGVer = {}, \
       ZoneShortID = {}, ObjVersion = {}, ObjVersionTag = {}, ObjAttrs = {}, \
       HeadSize = {}, MaxHeadSize = {}, Prefix = {}, TailInstance = {}, \
       HeadPlacementRuleName = {}, HeadPlacementRuleStorageClass = {}, \
       TailPlacementRuleName = {}, TailPlacementStorageClass = {}, \
       ManifestPartObjs = {}, ManifestPartRules = {}, Omap = {}, \
       IsMultipart = {}, MPPartsList = {}, HeadData = {} \
       WHERE ObjName = {} and ObjInstance = {} and BucketName = {}";

  public:
    virtual ~UpdateObjectOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      if (params.op.query_str == "omap") {
        return fmt::format(OmapQuery.c_str(),
            params.object_table.c_str(), params.op.obj.omap.c_str(),
            params.op.obj.mtime.c_str(),
            params.op.bucket.bucket_name.c_str(),
            params.op.obj.obj_name.c_str(),
            params.op.obj.obj_instance.c_str());
      }
      if (params.op.query_str == "attrs") {
        return fmt::format(AttrsQuery.c_str(),
            params.object_table.c_str(), params.op.obj.obj_attrs.c_str(),
            params.op.obj.mtime.c_str(),
            params.op.bucket.bucket_name.c_str(),
            params.op.obj.obj_name.c_str(),
            params.op.obj.obj_instance.c_str());
      }
      if (params.op.query_str == "mp") {
        return fmt::format(MPQuery.c_str(),
            params.object_table.c_str(), params.op.obj.mp_parts.c_str(),
            params.op.obj.mtime.c_str(),
            params.op.bucket.bucket_name.c_str(),
            params.op.obj.obj_name.c_str(),
            params.op.obj.obj_instance.c_str());
      }
      if (params.op.query_str == "meta") {
        return fmt::format(MetaQuery.c_str(),
          params.object_table.c_str(),
          params.op.obj.obj_ns, params.op.obj.acls, params.op.obj.index_ver,
          params.op.obj.tag, params.op.obj.flags, params.op.obj.versioned_epoch,
          params.op.obj.obj_category, params.op.obj.etag, params.op.obj.owner,
          params.op.obj.owner_display_name, params.op.obj.storage_class,
          params.op.obj.appendable, params.op.obj.content_type,
          params.op.obj.index_hash_source, params.op.obj.obj_size,
          params.op.obj.accounted_size, params.op.obj.mtime,
          params.op.obj.epoch, params.op.obj.obj_tag, params.op.obj.tail_tag,
          params.op.obj.write_tag, params.op.obj.fake_tag, params.op.obj.shadow_obj,
          params.op.obj.has_data, params.op.obj.is_olh, params.op.obj.olh_tag,
          params.op.obj.pg_ver, params.op.obj.zone_short_id,
          params.op.obj.obj_version, params.op.obj.obj_version_tag,
          params.op.obj.obj_attrs, params.op.obj.head_size,
          params.op.obj.max_head_size, params.op.obj.prefix,
          params.op.obj.tail_instance,
          params.op.obj.head_placement_rule_name,
          params.op.obj.head_placement_storage_class,
          params.op.obj.tail_placement_rule_name,
          params.op.obj.tail_placement_storage_class,
          params.op.obj.manifest_part_objs,
          params.op.obj.manifest_part_rules, params.op.obj.omap,
          params.op.obj.is_multipart, params.op.obj.mp_parts, params.op.obj.head_data,
          params.op.obj.obj_name, params.op.obj.obj_instance,
          params.op.bucket.bucket_name);
      }
      return "";
    }
};

class PutObjectDataOp: virtual public DBOp {
  private:
    const std::string Query =
      "INSERT OR REPLACE INTO '{}' \
      (ObjName, ObjInstance, ObjNS, BucketName, MultipartPartStr, PartNum, Offset, Size, Data) \
      VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})";

  public:
    virtual ~PutObjectDataOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(),
          params.objectdata_table.c_str(),
          params.op.obj.obj_name, params.op.obj.obj_instance,
          params.op.obj.obj_ns,
          params.op.bucket.bucket_name.c_str(),
          params.op.obj_data.multipart_part_str.c_str(),
          params.op.obj_data.part_num,
          params.op.obj_data.offset.c_str(),
          params.op.obj_data.size,
          params.op.obj_data.data.c_str());
    }
};

class UpdateObjectDataOp: virtual public DBOp {
  private:
    const std::string Query =
      "UPDATE '{}' \
      SET ObjName = {}, ObjInstance = {}, ObjNS = {} \
      WHERE ObjName = {} and ObjInstance = {} and ObjNS = {} and \
      BucketName = {}";

  public:
    virtual ~UpdateObjectDataOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(),
          params.objectdata_table.c_str(),
          params.op.obj.new_obj_name, params.op.obj.new_obj_instance,
          params.op.obj.new_obj_ns,
          params.op.obj.obj_name, params.op.obj.obj_instance,
          params.op.obj.obj_ns,
          params.op.bucket.bucket_name.c_str());
    }
};
class GetObjectDataOp: virtual public DBOp {
  private:
    const std::string Query =
      "SELECT  \
      ObjName, ObjInstance, ObjNS, BucketName, MultipartPartStr, PartNum, Offset, Size, Data \
      from '{}' where BucketName = {} and ObjName = {} and ObjInstance = {} ORDER BY MultipartPartStr, PartNum";

  public:
    virtual ~GetObjectDataOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(),
          params.objectdata_table.c_str(),
          params.op.bucket.bucket_name.c_str(),
          params.op.obj.obj_name.c_str(),
          params.op.obj.obj_instance.c_str());
    }
};

class DeleteObjectDataOp: virtual public DBOp {
  private:
    const std::string Query =
      "DELETE from '{}' where BucketName = {} and ObjName = {} and ObjInstance = {}";

  public:
    virtual ~DeleteObjectDataOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(),
          params.objectdata_table.c_str(),
          params.op.bucket.bucket_name.c_str(),
          params.op.obj.obj_name.c_str(),
          params.op.obj.obj_instance.c_str());
    }
};

class InsertLCEntryOp: virtual public DBOp {
  private:
    const std::string Query =
      "INSERT OR REPLACE INTO '{}' \
      (LCIndex, BucketName, StartTime, Status) \
      VALUES ({}, {}, {}, {})";

  public:
    virtual ~InsertLCEntryOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.lc_entry_table.c_str(),
          params.op.lc_entry.index, params.op.lc_entry.bucket_name,
          params.op.lc_entry.start_time, params.op.lc_entry.status);
    }
};

class RemoveLCEntryOp: virtual public DBOp {
  private:
    const std::string Query =
      "DELETE from '{}' where LCIndex = {} and BucketName = {}";

  public:
    virtual ~RemoveLCEntryOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.lc_entry_table.c_str(),
          params.op.lc_entry.index, params.op.lc_entry.bucket_name);
    }
};

class GetLCEntryOp: virtual public DBOp {
  private:
    const std::string Query = "SELECT  \
                          LCIndex, BucketName, StartTime, Status \
                          from '{}' where LCIndex = {} and BucketName = {}";
    const std::string NextQuery = "SELECT  \
                          LCIndex, BucketName, StartTime, Status \
                          from '{}' where LCIndex = {} and BucketName > {} ORDER BY BucketName ASC";

  public:
    virtual ~GetLCEntryOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      if (params.op.query_str == "get_next_entry") {
        return fmt::format(NextQuery.c_str(), params.lc_entry_table.c_str(),
            params.op.lc_entry.index, params.op.lc_entry.bucket_name);
      }
      // default 
      return fmt::format(Query.c_str(), params.lc_entry_table.c_str(),
          params.op.lc_entry.index, params.op.lc_entry.bucket_name);
    }
};

class ListLCEntriesOp: virtual public DBOp {
  private:
    const std::string Query = "SELECT  \
                          LCIndex, BucketName, StartTime, Status \
                          FROM '{}' WHERE LCIndex = {} AND BucketName > {} ORDER BY BucketName ASC LIMIT {}";

  public:
    virtual ~ListLCEntriesOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.lc_entry_table.c_str(),
          params.op.lc_entry.index.c_str(), params.op.lc_entry.min_marker.c_str(),
          params.op.list_max_count.c_str());
    }
};

class InsertLCHeadOp: virtual public DBOp {
  private:
    const std::string Query =
      "INSERT OR REPLACE INTO '{}' \
      (LCIndex, Marker, StartDate) \
      VALUES ({}, {}, {})";

  public:
    virtual ~InsertLCHeadOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.lc_head_table.c_str(),
          params.op.lc_head.index, params.op.lc_head.marker,
          params.op.lc_head.start_date);
    }
};

class RemoveLCHeadOp: virtual public DBOp {
  private:
    const std::string Query =
      "DELETE from '{}' where LCIndex = {}";

  public:
    virtual ~RemoveLCHeadOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.lc_head_table.c_str(),
          params.op.lc_head.index);
    }
};

class GetLCHeadOp: virtual public DBOp {
  private:
    const std::string Query = "SELECT  \
                          LCIndex, Marker, StartDate \
                          from '{}' where LCIndex = {}";

  public:
    virtual ~GetLCHeadOp() {}

    std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query.c_str(), params.lc_head_table.c_str(),
          params.op.lc_head.index);
    }
};

/* taken from rgw_rados.h::RGWOLHInfo */
struct DBOLHInfo {
  rgw_obj target;
  bool removed;
  DBOLHInfo() : removed(false) {}
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(target, bl);
    encode(removed, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(target, bl);
    decode(removed, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(DBOLHInfo)

class DB {
  private:
    const std::string db_name;
    rgw::sal::Store* store;
    const std::string user_table;
    const std::string bucket_table;
    const std::string quota_table;
    const std::string lc_head_table;
    const std::string lc_entry_table;
    static std::map<std::string, class ObjectOp*> objectmap;

  protected:
    void *db;
    CephContext *cct;
    const DoutPrefix dp;
    uint64_t max_bucket_id = 0;
    // XXX: default ObjStripeSize or ObjChunk size - 4M, make them configurable?
    uint64_t ObjHeadSize = 1024; /* 1K - default head data size */
    uint64_t ObjChunkSize = (get_blob_limit() - 1000); /* 1000 to accommodate other fields */
    // Below mutex is to protect objectmap and other shared
    // objects if any.
    std::mutex mtx;

  public:
    DB(std::string db_name, CephContext *_cct) : db_name(db_name),
    user_table(db_name+".user.table"),
    bucket_table(db_name+".bucket.table"),
    quota_table(db_name+".quota.table"),
    lc_head_table(db_name+".lc_head.table"),
    lc_entry_table(db_name+".lc_entry.table"),
    cct(_cct),
    dp(_cct, ceph_subsys_rgw, "rgw DBStore backend: ")
  {}
    /*	DB() {}*/

    DB(CephContext *_cct) : db_name("default_db"),
    user_table(db_name+".user.table"),
    bucket_table(db_name+".bucket.table"),
    quota_table(db_name+".quota.table"),
    lc_head_table(db_name+".lc_head.table"),
    lc_entry_table(db_name+".lc_entry.table"),
    cct(_cct),
    dp(_cct, ceph_subsys_rgw, "rgw DBStore backend: ")
  {}
    virtual	~DB() {}

    const std::string getDBname() { return db_name; }
    const std::string getDBfile() { return db_name + ".db"; }
    const std::string getUserTable() { return user_table; }
    const std::string getBucketTable() { return bucket_table; }
    const std::string getQuotaTable() { return quota_table; }
    const std::string getLCHeadTable() { return lc_head_table; }
    const std::string getLCEntryTable() { return lc_entry_table; }
    const std::string getObjectTable(std::string bucket) {
      return db_name+"."+bucket+".object.table"; }
    const std::string getObjectDataTable(std::string bucket) {
      return db_name+"."+bucket+".objectdata.table"; }

    std::map<std::string, class ObjectOp*> getObjectMap();

    struct DBOps dbops; // DB operations, make it private?

    void set_store(rgw::sal::Store* _store) {
      store = _store;
    }

    void set_context(CephContext *_cct) {
      cct = _cct;
    }

    CephContext *ctx() { return cct; }
    const DoutPrefixProvider *get_def_dpp() { return &dp; }

    int Initialize(std::string logfile, int loglevel);
    int Destroy(const DoutPrefixProvider *dpp);
    int LockInit(const DoutPrefixProvider *dpp);
    int LockDestroy(const DoutPrefixProvider *dpp);
    int Lock(const DoutPrefixProvider *dpp);
    int Unlock(const DoutPrefixProvider *dpp);

    int InitializeParams(const DoutPrefixProvider *dpp, std::string Op, DBOpParams *params);
    int ProcessOp(const DoutPrefixProvider *dpp, std::string Op, DBOpParams *params);
    DBOp* getDBOp(const DoutPrefixProvider *dpp, std::string Op, struct DBOpParams *params);
    int objectmapInsert(const DoutPrefixProvider *dpp, std::string bucket, class ObjectOp* ptr);
    int objectmapDelete(const DoutPrefixProvider *dpp, std::string bucket);

    virtual uint64_t get_blob_limit() { return 0; };
    virtual void *openDB(const DoutPrefixProvider *dpp) { return NULL; }
    virtual int closeDB(const DoutPrefixProvider *dpp) { return 0; }
    virtual int createTables(const DoutPrefixProvider *dpp) { return 0; }
    virtual int InitializeDBOps(const DoutPrefixProvider *dpp) { return 0; }
    virtual int FreeDBOps(const DoutPrefixProvider *dpp) { return 0; }
    virtual int InitPrepareParams(const DoutPrefixProvider *dpp, DBOpPrepareParams &params) = 0;
    virtual int createLCTables(const DoutPrefixProvider *dpp) = 0;

    virtual int ListAllBuckets(const DoutPrefixProvider *dpp, DBOpParams *params) = 0;
    virtual int ListAllUsers(const DoutPrefixProvider *dpp, DBOpParams *params) = 0;
    virtual int ListAllObjects(const DoutPrefixProvider *dpp, DBOpParams *params) = 0;

    int get_user(const DoutPrefixProvider *dpp,
        const std::string& query_str, const std::string& query_str_val,
        RGWUserInfo& uinfo, std::map<std::string, bufferlist> *pattrs,
        RGWObjVersionTracker *pobjv_tracker);
    int store_user(const DoutPrefixProvider *dpp,
        RGWUserInfo& uinfo, bool exclusive, std::map<std::string, bufferlist> *pattrs,
        RGWObjVersionTracker *pobjv_tracker, RGWUserInfo* pold_info);
    int remove_user(const DoutPrefixProvider *dpp,
        RGWUserInfo& uinfo, RGWObjVersionTracker *pobjv_tracker);
    int get_bucket_info(const DoutPrefixProvider *dpp, const std::string& query_str,
        const std::string& query_str_val,
        RGWBucketInfo& info, rgw::sal::Attrs* pattrs, ceph::real_time* pmtime,
        obj_version* pbucket_version);
    int create_bucket(const DoutPrefixProvider *dpp,
        const RGWUserInfo& owner, rgw_bucket& bucket,
        const std::string& zonegroup_id,
        const rgw_placement_rule& placement_rule,
        const std::string& swift_ver_location,
        const RGWQuotaInfo * pquota_info,
        std::map<std::string, bufferlist>& attrs,
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
        const std::string& marker,
        const std::string& end_marker,
        uint64_t max,
        bool need_stats,
        RGWUserBuckets *buckets,
        bool *is_truncated);
    int update_bucket(const DoutPrefixProvider *dpp, const std::string& query_str,
        RGWBucketInfo& info, bool exclusive,
        const rgw_user* powner_id, std::map<std::string, bufferlist>* pattrs,
        ceph::real_time* pmtime, RGWObjVersionTracker* pobjv);

    uint64_t get_max_head_size() { return ObjHeadSize; }
    uint64_t get_max_chunk_size() { return ObjChunkSize; }
    void gen_rand_obj_instance_name(rgw_obj_key *target_key);

    // db raw obj string is of format -
    // "<bucketname>_<objname>_<objinstance>_<multipart-part-str>_<partnum>"
    const std::string raw_obj_oid = "{0}_{1}_{2}_{3}_{4}";

    inline std::string to_oid(const std::string& bucket, const std::string& obj_name, const std::string& obj_instance,
        std::string mp_str, uint64_t partnum) {
      std::string s = fmt::format(raw_obj_oid.c_str(), bucket, obj_name, obj_instance, mp_str, partnum);
      return s;
    }
    inline int from_oid(const std::string& oid, std::string& bucket, std::string& obj_name,
        std::string& obj_instance,
        std::string& mp_str, uint64_t& partnum) {
      std::vector<std::string> result;
      boost::split(result, oid, boost::is_any_of("_"));
      bucket = result[0];
      obj_name = result[1];
      obj_instance = result[2];
      mp_str = result[3];
      partnum = stoi(result[4]);

      return 0;
    }

    struct raw_obj {
      DB* db;

      std::string bucket_name;
      std::string obj_name;
      std::string obj_instance;
      std::string obj_ns;
      std::string multipart_part_str;
      uint64_t part_num;

      std::string obj_table;
      std::string obj_data_table;

      raw_obj(DB* _db) {
        db = _db;
      }

      raw_obj(DB* _db, std::string& _bname, std::string& _obj_name, std::string& _obj_instance,
          std::string& _obj_ns, std::string _mp_part_str, int _part_num) {
        db = _db;
        bucket_name = _bname;
        obj_name = _obj_name;
        obj_instance = _obj_instance;
        obj_ns = _obj_ns;
        multipart_part_str = _mp_part_str;
        part_num = _part_num;

        obj_table = bucket_name+".object.table";
        obj_data_table = bucket_name+".objectdata.table";
      }

      raw_obj(DB* _db, std::string& oid) {
        int r;

        db = _db;
        r = db->from_oid(oid, bucket_name, obj_name, obj_instance, multipart_part_str,
            part_num);
        if (r < 0) {
          multipart_part_str = "0.0";
          part_num = 0;
        }

        obj_table = db->getObjectTable(bucket_name);
        obj_data_table = db->getObjectDataTable(bucket_name);
      }

      int InitializeParamsfromRawObj (const DoutPrefixProvider *dpp, DBOpParams* params);

      int read(const DoutPrefixProvider *dpp, int64_t ofs, uint64_t end, bufferlist& bl);
      int write(const DoutPrefixProvider *dpp, int64_t ofs, int64_t write_ofs, uint64_t len, bufferlist& bl);
    };

    class Bucket {
      friend class DB;
      DB* store;

      RGWBucketInfo bucket_info;

      public:
        Bucket(DB *_store, const RGWBucketInfo& _binfo) : store(_store), bucket_info(_binfo) {}
        DB *get_store() { return store; }
        rgw_bucket& get_bucket() { return bucket_info.bucket; }
        RGWBucketInfo& get_bucket_info() { return bucket_info; }

      class List {
      protected:
        // absolute maximum number of objects that
        // list_objects_(un)ordered can return
        static constexpr int64_t bucket_list_objects_absolute_max = 25000;

        DB::Bucket *target;
        rgw_obj_key next_marker;

      public:

        struct Params {
          std::string prefix;
          std::string delim;
          rgw_obj_key marker;
          rgw_obj_key end_marker;
          std::string ns;
          bool enforce_ns;
          RGWAccessListFilter* access_list_filter;
          RGWBucketListNameFilter force_check_filter;
          bool list_versions;
	  bool allow_unordered;

          Params() :
	        enforce_ns(true),
	        access_list_filter(nullptr),
	        list_versions(false),
	        allow_unordered(false)
	        {}
        } params;

        explicit List(DB::Bucket *_target) : target(_target) {}

        /* XXX: Handle ordered and unordered separately.
         * For now returning only ordered entries */
        int list_objects(const DoutPrefixProvider *dpp, int64_t max,
			   std::vector<rgw_bucket_dir_entry> *result,
			   std::map<std::string, bool> *common_prefixes, bool *is_truncated);
        rgw_obj_key& get_next_marker() {
          return next_marker;
        }
      };
    };

    class Object {
      friend class DB;
      DB* store;

      RGWBucketInfo bucket_info;
      rgw_obj obj;

      RGWObjState *state;

      bool versioning_disabled;

      bool bs_initialized;

      public:
      Object(DB *_store, const RGWBucketInfo& _bucket_info, RGWObjectCtx& _ctx, const rgw_obj& _obj) : store(_store), bucket_info(_bucket_info),
      obj(_obj),
      state(NULL), versioning_disabled(false),
      bs_initialized(false) {}

      Object(DB *_store, const RGWBucketInfo& _bucket_info, const rgw_obj& _obj) : store(_store), bucket_info(_bucket_info), obj(_obj) {}

      struct Read {
        DB::Object *source;

        struct GetObjState {
          rgw_obj obj;
        } state;

        struct ConditionParams {
          const ceph::real_time *mod_ptr;
          const ceph::real_time *unmod_ptr;
          bool high_precision_time;
          uint32_t mod_zone_id;
          uint64_t mod_pg_ver;
          const char *if_match;
          const char *if_nomatch;

          ConditionParams() :
            mod_ptr(NULL), unmod_ptr(NULL), high_precision_time(false), mod_zone_id(0), mod_pg_ver(0),
            if_match(NULL), if_nomatch(NULL) {}
        } conds;

        struct Params {
          ceph::real_time *lastmod;
          uint64_t *obj_size;
	  std::map<std::string, bufferlist> *attrs;
          rgw_obj *target_obj;

          Params() : lastmod(nullptr), obj_size(nullptr), attrs(nullptr),
          target_obj(nullptr) {}
        } params;

        explicit Read(DB::Object *_source) : source(_source) {}

        int prepare(const DoutPrefixProvider *dpp);
        static int range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end);
        int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider *dpp);
        int iterate(const DoutPrefixProvider *dpp, int64_t ofs, int64_t end, RGWGetDataCB *cb);
        int get_attr(const DoutPrefixProvider *dpp, const char *name, bufferlist& dest);
      };

      struct Write {
        DB::Object *target;
        RGWObjState obj_state;
        std::string mp_part_str = "0.0"; // multipart num

        struct MetaParams {
          ceph::real_time *mtime;
	  std::map<std::string, bufferlist>* rmattrs;
          const bufferlist *data;
          RGWObjManifest *manifest;
          const std::string *ptag;
          std::list<rgw_obj_index_key> *remove_objs;
          ceph::real_time set_mtime;
          rgw_user owner;
          RGWObjCategory category;
          int flags;
          const char *if_match;
          const char *if_nomatch;
          std::optional<uint64_t> olh_epoch;
          ceph::real_time delete_at;
          bool canceled;
          const std::string *user_data;
          rgw_zone_set *zones_trace;
          bool modify_tail;
          bool completeMultipart;
          bool appendable;

          MetaParams() : mtime(NULL), rmattrs(NULL), data(NULL), manifest(NULL), ptag(NULL),
          remove_objs(NULL), category(RGWObjCategory::Main), flags(0),
          if_match(NULL), if_nomatch(NULL), canceled(false), user_data(nullptr), zones_trace(nullptr),
          modify_tail(false),  completeMultipart(false), appendable(false) {}
        } meta;

        explicit Write(DB::Object *_target) : target(_target) {}

        void set_mp_part_str(std::string _mp_part_str) { mp_part_str = _mp_part_str;}
        int prepare(const DoutPrefixProvider* dpp);
        int write_data(const DoutPrefixProvider* dpp,
                               bufferlist& data, uint64_t ofs);
        int _do_write_meta(const DoutPrefixProvider *dpp,
            uint64_t size, uint64_t accounted_size,
	    std::map<std::string, bufferlist>& attrs,
            bool assume_noent, bool modify_tail);
        int write_meta(const DoutPrefixProvider *dpp, uint64_t size,
	    uint64_t accounted_size, std::map<std::string, bufferlist>& attrs);
        /* Below are used to update mp data rows object name
         * from meta to src object name on multipart upload
         * completion
         */
        int update_mp_parts(const DoutPrefixProvider *dpp, rgw_obj_key new_obj_key);
      };

      struct Delete {
        DB::Object *target;

        struct DeleteParams {
          rgw_user bucket_owner;
          int versioning_status;
          ACLOwner obj_owner; /* needed for creation of deletion marker */
          uint64_t olh_epoch;
          std::string marker_version_id;
          uint32_t bilog_flags;
          std::list<rgw_obj_index_key> *remove_objs;
          ceph::real_time expiration_time;
          ceph::real_time unmod_since;
          ceph::real_time mtime; /* for setting delete marker mtime */
          bool high_precision_time;
          rgw_zone_set *zones_trace;
          bool abortmp;
          uint64_t parts_accounted_size;

          DeleteParams() : versioning_status(0), olh_epoch(0), bilog_flags(0), remove_objs(NULL), high_precision_time(false), zones_trace(nullptr), abortmp(false), parts_accounted_size(0) {}
        } params;

        struct DeleteResult {
          bool delete_marker;
          std::string version_id;

          DeleteResult() : delete_marker(false) {}
        } result;

        explicit Delete(DB::Object *_target) : target(_target) {}

        int delete_obj(const DoutPrefixProvider *dpp);
      };

      /* XXX: the parameters may be subject to change. All we need is bucket name
       * & obj name,instance - keys */
      int get_obj_state(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info,
                        const rgw_obj& obj,
                        bool follow_olh, RGWObjState **state);
      int get_olh_target_state(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj,
          RGWObjState* olh_state, RGWObjState** target);
      int follow_olh(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, RGWObjState *state,
          const rgw_obj& olh_obj, rgw_obj *target);

      int get_state(const DoutPrefixProvider *dpp, RGWObjState **pstate, bool follow_olh);
      int get_manifest(const DoutPrefixProvider *dpp, RGWObjManifest **pmanifest);

      DB *get_store() { return store; }
      rgw_obj& get_obj() { return obj; }
      RGWBucketInfo& get_bucket_info() { return bucket_info; }

      int InitializeParamsfromObject(const DoutPrefixProvider *dpp, DBOpParams* params);
      int set_attrs(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist>& setattrs,
          std::map<std::string, bufferlist>* rmattrs);
      int obj_omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val, bool must_exist);
      int obj_omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
          const std::set<std::string>& keys,
          std::map<std::string, bufferlist>* vals);
      int obj_omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist> *m);
      int obj_omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
          std::map<std::string, bufferlist> *m, bool* pmore);
      using iterate_obj_cb = int (*)(const DoutPrefixProvider*, const raw_obj&, off_t, off_t,
          bool, RGWObjState*, void*);
      int add_mp_part(const DoutPrefixProvider *dpp, RGWUploadPartInfo info);
      int get_mp_parts_list(const DoutPrefixProvider *dpp, std::list<RGWUploadPartInfo>& info);

      int iterate_obj(const DoutPrefixProvider *dpp,
          const RGWBucketInfo& bucket_info, const rgw_obj& obj,
          off_t ofs, off_t end, uint64_t max_chunk_size,
          iterate_obj_cb cb, void *arg);
    };
    int get_obj_iterate_cb(const DoutPrefixProvider *dpp,
        const raw_obj& read_obj, off_t obj_ofs,
        off_t len, bool is_head_obj,
        RGWObjState *astate, void *arg);

    int get_entry(const std::string& oid, const std::string& marker,
                  rgw::sal::Lifecycle::LCEntry& entry);
    int get_next_entry(const std::string& oid, std::string& marker,
                  rgw::sal::Lifecycle::LCEntry& entry);
    int set_entry(const std::string& oid, const rgw::sal::Lifecycle::LCEntry& entry);
    int list_entries(const std::string& oid, const std::string& marker,
			   uint32_t max_entries, std::vector<rgw::sal::Lifecycle::LCEntry>& entries);
    int rm_entry(const std::string& oid, const rgw::sal::Lifecycle::LCEntry& entry);
    int get_head(const std::string& oid, rgw::sal::Lifecycle::LCHead& head);
    int put_head(const std::string& oid, const rgw::sal::Lifecycle::LCHead& head);
};

struct db_get_obj_data {
  DB* store;
  RGWGetDataCB* client_cb = nullptr;
  uint64_t offset; // next offset to write to client

  db_get_obj_data(DB* db, RGWGetDataCB* cb, uint64_t offset) :
    store(db), client_cb(cb), offset(offset) {}
  ~db_get_obj_data() {}
};

} } // namespace rgw::store

#endif
