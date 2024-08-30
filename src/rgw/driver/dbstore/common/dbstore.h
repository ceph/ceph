// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include "fmt/format.h"
#include <map>
#include "rgw_sal_store.h"
#include "rgw_common.h"
#include "driver/rados/rgw_bucket.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_context.h"
#include "rgw_multi.h"

#include "driver/rados/rgw_obj_manifest.h" // FIXME: subclass dependency

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
  std::string owner;
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
  RGWObjState state = {};

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
  std::string obj_id;
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
  std::string prefix;
  std::list<rgw_bucket_dir_entry> list_entries;
  /* XXX: Maybe use std::vector instead of std::list */

  /* for versioned objects */
  bool is_versioned;
  uint64_t version_num = 0;
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
  rgw::sal::StoreLifecycle::StoreLCHead head;
};

struct DBOpLCEntryInfo {
  std::string index;
  rgw::sal::StoreLifecycle::StoreLCEntry entry;
  // used for list query
  std::string min_marker;
  std::list<rgw::sal::StoreLifecycle::StoreLCEntry> list_entries;
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

  std::string objectdata_table;
  std::string object_trigger;
  std::string object_view;
  std::string quota_table;
  std::string lc_head_table;
  std::string lc_entry_table;
  std::string obj;
};

/* Used for prepared schemas.
 * Difference with above structure is that all 
 * the fields are strings here to accommodate any
 * style identifiers used by backend db. By default
 * initialized with sqlitedb style, can be overridden
 * using InitPrepareParams()
 *
 * These identifiers are used in prepare and bind statements
 * to get the right index of each param.
 */
struct DBOpUserPrepareInfo {
  static constexpr const char* user_id = ":user_id";
  static constexpr const char* tenant = ":tenant";
  static constexpr const char* ns = ":ns";
  static constexpr const char* display_name = ":display_name";
  static constexpr const char* user_email = ":user_email";
  /* Support only single access_key for now. So store
   * it separately as primary access_key_id & secret to
   * be able to query easily.
   *
   * In future, when need to support & query from multiple
   * access keys, better to maintain them in a separate table.
   */
  static constexpr const char* access_keys_id = ":access_keys_id";
  static constexpr const char* access_keys_secret = ":access_keys_secret";
  static constexpr const char* access_keys = ":access_keys";
  static constexpr const char* swift_keys = ":swift_keys";
  static constexpr const char* subusers = ":subusers";
  static constexpr const char* suspended = ":suspended";
  static constexpr const char* max_buckets = ":max_buckets";
  static constexpr const char* op_mask = ":op_mask";
  static constexpr const char* user_caps = ":user_caps";
  static constexpr const char* admin = ":admin";
  static constexpr const char* system = ":system";
  static constexpr const char* placement_name = ":placement_name";
  static constexpr const char* placement_storage_class = ":placement_storage_class";
  static constexpr const char* placement_tags = ":placement_tags";
  static constexpr const char* bucket_quota = ":bucket_quota";
  static constexpr const char* temp_url_keys = ":temp_url_keys";
  static constexpr const char* user_quota = ":user_quota";
  static constexpr const char* type = ":type";
  static constexpr const char* mfa_ids = ":mfa_ids";
  static constexpr const char* user_attrs = ":user_attrs";
  static constexpr const char* user_ver = ":user_vers";
  static constexpr const char* user_ver_tag = ":user_ver_tag";
};

struct DBOpBucketPrepareInfo {
  static constexpr const char* bucket_name = ":bucket_name";
  static constexpr const char* tenant = ":tenant";
  static constexpr const char* marker = ":marker";
  static constexpr const char* bucket_id = ":bucket_id";
  static constexpr const char* size = ":size";
  static constexpr const char* size_rounded = ":size_rounded";
  static constexpr const char* creation_time = ":creation_time";
  static constexpr const char* count = ":count";
  static constexpr const char* placement_name = ":placement_name";
  static constexpr const char* placement_storage_class = ":placement_storage_class";
  /* ownerid - maps to DBOpUserPrepareInfo */
  static constexpr const char* flags = ":flags";
  static constexpr const char* zonegroup = ":zonegroup";
  static constexpr const char* has_instance_obj = ":has_instance_obj";
  static constexpr const char* quota = ":quota";
  static constexpr const char* requester_pays = ":requester_pays";
  static constexpr const char* has_website = ":has_website";
  static constexpr const char* website_conf = ":website_conf";
  static constexpr const char* swift_versioning = ":swift_versioning";
  static constexpr const char* swift_ver_location = ":swift_ver_location";
  static constexpr const char* mdsearch_config = ":mdsearch_config";
  static constexpr const char* new_bucket_instance_id = ":new_bucket_instance_id";
  static constexpr const char* obj_lock = ":obj_lock";
  static constexpr const char* sync_policy_info_groups = ":sync_policy_info_groups";
  static constexpr const char* bucket_attrs = ":bucket_attrs";
  static constexpr const char* bucket_ver = ":bucket_vers";
  static constexpr const char* bucket_ver_tag = ":bucket_ver_tag";
  static constexpr const char* mtime = ":mtime";
  static constexpr const char* min_marker = ":min_marker";
  static constexpr const char* max_marker = ":max_marker";
};

struct DBOpObjectPrepareInfo {
  static constexpr const char* obj_name = ":obj_name";
  static constexpr const char* obj_instance = ":obj_instance";
  static constexpr const char* obj_ns  = ":obj_ns";
  static constexpr const char* acls = ":acls";
  static constexpr const char* index_ver = ":index_ver";
  static constexpr const char* tag = ":tag";
  static constexpr const char* flags = ":flags";
  static constexpr const char* versioned_epoch = ":versioned_epoch";
  static constexpr const char* obj_category = ":obj_category";
  static constexpr const char* etag = ":etag";
  static constexpr const char* owner = ":owner";
  static constexpr const char* owner_display_name = ":owner_display_name";
  static constexpr const char* storage_class = ":storage_class";
  static constexpr const char* appendable = ":appendable";
  static constexpr const char* content_type = ":content_type";
  static constexpr const char* index_hash_source = ":index_hash_source";
  static constexpr const char* obj_size = ":obj_size";
  static constexpr const char* accounted_size = ":accounted_size";
  static constexpr const char* mtime = ":mtime";
  static constexpr const char* epoch = ":epoch";
  static constexpr const char* obj_tag = ":obj_tag";
  static constexpr const char* tail_tag = ":tail_tag";
  static constexpr const char* write_tag = ":write_tag";
  static constexpr const char* fake_tag = ":fake_tag";
  static constexpr const char* shadow_obj = ":shadow_obj";
  static constexpr const char* has_data = ":has_data";
  static constexpr const char* is_versioned = ":is_versioned";
  static constexpr const char* version_num = ":version_num";
  static constexpr const char* pg_ver = ":pg_ver";
  static constexpr const char* zone_short_id = ":zone_short_id";
  static constexpr const char* obj_version = ":obj_version";
  static constexpr const char* obj_version_tag = ":obj_version_tag";
  static constexpr const char* obj_attrs = ":obj_attrs";
  static constexpr const char* head_size = ":head_size";
  static constexpr const char* max_head_size = ":max_head_size";
  static constexpr const char* obj_id = ":obj_id";
  static constexpr const char* tail_instance = ":tail_instance";
  static constexpr const char* head_placement_rule_name = ":head_placement_rule_name";
  static constexpr const char* head_placement_storage_class  = ":head_placement_storage_class";
  static constexpr const char* tail_placement_rule_name = ":tail_placement_rule_name";
  static constexpr const char* tail_placement_storage_class  = ":tail_placement_storage_class";
  static constexpr const char* manifest_part_objs = ":manifest_part_objs";
  static constexpr const char* manifest_part_rules = ":manifest_part_rules";
  static constexpr const char* omap = ":omap";
  static constexpr const char* is_multipart = ":is_multipart";
  static constexpr const char* mp_parts = ":mp_parts";
  static constexpr const char* head_data = ":head_data";
  static constexpr const char* min_marker = ":min_marker";
  static constexpr const char* max_marker = ":max_marker";
  static constexpr const char* prefix = ":prefix";
  /* Below used to update mp_parts obj name
   * from meta object to src object on completion */
  static constexpr const char* new_obj_name = ":new_obj_name";
  static constexpr const char* new_obj_instance = ":new_obj_instance";
  static constexpr const char* new_obj_ns  = ":new_obj_ns";
};

struct DBOpObjectDataPrepareInfo {
  static constexpr const char* part_num = ":part_num";
  static constexpr const char* offset = ":offset";
  static constexpr const char* data = ":data";
  static constexpr const char* size = ":size";
  static constexpr const char* multipart_part_str = ":multipart_part_str";
};

struct DBOpLCEntryPrepareInfo {
  static constexpr const char* index = ":index";
  static constexpr const char* bucket_name = ":bucket_name";
  static constexpr const char* start_time = ":start_time";
  static constexpr const char* status = ":status";
  static constexpr const char* min_marker = ":min_marker";
};

struct DBOpLCHeadPrepareInfo {
  static constexpr const char* index = ":index";
  static constexpr const char* start_date = ":start_date";
  static constexpr const char* marker = ":marker";
};

struct DBOpPrepareInfo {
  DBOpUserPrepareInfo user;
  std::string_view query_str; // view into DBOpInfo::query_str
  DBOpBucketPrepareInfo bucket;
  DBOpObjectPrepareInfo obj;
  DBOpObjectDataPrepareInfo obj_data;
  DBOpLCHeadPrepareInfo lc_head;
  DBOpLCEntryPrepareInfo lc_entry;
  static constexpr const char* list_max_count = ":list_max_count";
};

struct DBOpPrepareParams {
  /* Tables */
  std::string user_table;
  std::string bucket_table;
  std::string object_table;

  /* Ops */
  DBOpPrepareInfo op;


  std::string objectdata_table;
  std::string object_trigger;
  std::string object_view;
  std::string quota_table;
  std::string lc_head_table;
  std::string lc_entry_table;
};

struct DBOps {
  std::shared_ptr<class InsertUserOp> InsertUser;
  std::shared_ptr<class RemoveUserOp> RemoveUser;
  std::shared_ptr<class GetUserOp> GetUser;
  std::shared_ptr<class InsertBucketOp> InsertBucket;
  std::shared_ptr<class UpdateBucketOp> UpdateBucket;
  std::shared_ptr<class RemoveBucketOp> RemoveBucket;
  std::shared_ptr<class GetBucketOp> GetBucket;
  std::shared_ptr<class ListUserBucketsOp> ListUserBuckets;
  std::shared_ptr<class InsertLCEntryOp> InsertLCEntry;
  std::shared_ptr<class RemoveLCEntryOp> RemoveLCEntry;
  std::shared_ptr<class GetLCEntryOp> GetLCEntry;
  std::shared_ptr<class ListLCEntriesOp> ListLCEntries;
  std::shared_ptr<class  InsertLCHeadOp> InsertLCHead;
  std::shared_ptr<class RemoveLCHeadOp> RemoveLCHead;
  std::shared_ptr<class GetLCHeadOp> GetLCHead;
};

class ObjectOp {
  public:
    ObjectOp() {};

    virtual ~ObjectOp() {}

    std::shared_ptr<class PutObjectOp> PutObject;
    std::shared_ptr<class DeleteObjectOp> DeleteObject;
    std::shared_ptr<class GetObjectOp> GetObject;
    std::shared_ptr<class UpdateObjectOp> UpdateObject;
    std::shared_ptr<class ListBucketObjectsOp> ListBucketObjects;
    std::shared_ptr<class ListVersionedObjectsOp> ListVersionedObjects;
    std::shared_ptr<class PutObjectDataOp> PutObjectData;
    std::shared_ptr<class UpdateObjectDataOp> UpdateObjectData;
    std::shared_ptr<class GetObjectDataOp> GetObjectData;
    std::shared_ptr<class DeleteObjectDataOp> DeleteObjectData;
    std::shared_ptr<class DeleteStaleObjectDataOp> DeleteStaleObjectData;

    virtual int InitializeObjectOps(std::string db_name, const DoutPrefixProvider *dpp) { return 0; }
};

class DBOp {
  private:
    static constexpr std::string_view CreateUserTableQ =
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

    static constexpr std::string_view CreateBucketTableQ =
      /* Corresponds to rgw::sal::Bucket
       *  
       *  For now only BucketName is made Primary key. Since buckets should
       *  be unique across users in rgw, OwnerID is not made part of primary key.
       *  However it is still referenced as foreign key
       *
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
      PRIMARY KEY (BucketName) \n);";

    static constexpr std::string_view CreateObjectTableTriggerQ =
      "CREATE TRIGGER IF NOT EXISTS '{}' \
          AFTER INSERT ON '{}' \
       BEGIN \
          UPDATE '{}' \
          SET VersionNum = (SELECT COALESCE(max(VersionNum), 0) from '{}' where ObjName = new.ObjName) + 1 \
          where ObjName = new.ObjName and ObjInstance = new.ObjInstance; \
       END;";

    static constexpr std::string_view CreateObjectTableQ =
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
      IsVersioned BOOL,  \
      VersionNum  INTEGER, \
      PGVer   INTEGER, \
      ZoneShortID  INTEGER,  \
      ObjVersion   INTEGER,    \
      ObjVersionTag TEXT,      \
      ObjAttrs    BLOB,   \
      HeadSize    INTEGER,    \
      MaxHeadSize    INTEGER,    \
      ObjID      TEXT NOT NULL, \
      TailInstance  TEXT, \
      HeadPlacementRuleName   TEXT, \
      HeadPlacementRuleStorageClass TEXT, \
      TailPlacementRuleName   TEXT, \
      TailPlacementStorageClass TEXT, \
      ManifestPartObjs    BLOB,   \
      ManifestPartRules   BLOB,   \
      Omap    BLOB,   \
      IsMultipart     BOOL,   \
      MPPartsList    BLOB,   \
      HeadData  BLOB,   \
      PRIMARY KEY (ObjName, ObjInstance, BucketName), \
      FOREIGN KEY (BucketName) \
      REFERENCES '{}' (BucketName) ON DELETE CASCADE ON UPDATE CASCADE \n);";

    static constexpr std::string_view CreateObjectDataTableQ =
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
      ObjID      TEXT NOT NULL , \
      MultipartPartStr TEXT, \
      PartNum  INTEGER NOT NULL, \
      Offset   INTEGER, \
      Size 	 INTEGER, \
      Mtime  BLOB,       \
      Data     BLOB,             \
      PRIMARY KEY (ObjName, BucketName, ObjInstance, ObjID, MultipartPartStr, PartNum), \
      FOREIGN KEY (BucketName) \
      REFERENCES '{}' (BucketName) ON DELETE CASCADE ON UPDATE CASCADE \n);";

    static constexpr std::string_view CreateObjectViewQ =
      /* This query creates temporary view with entries from ObjectData table which have
       * corresponding head object (i.e, with same ObjName, ObjInstance, ObjNS, ObjID)
       * in the Object table.
       *
       * GC thread can use this view to delete stale entries from the ObjectData table which
       * do not exist in this view.
       *
       * XXX: This view is throwing ForeignKey mismatch error, mostly may be because all the keys
       * of objectdata table are not referenced here. So this view is not used atm.
       */
      "CREATE TEMP VIEW IF NOT EXISTS '{}' AS \
      SELECT s.ObjName, s.ObjInstance, s.ObjID from '{}' as s INNER JOIN '{}' USING \
      (ObjName, BucketName, ObjInstance, ObjID);";


    static constexpr std::string_view CreateQuotaTableQ =
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      QuotaID INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE , \
      MaxSizeSoftThreshold INTEGER ,	\
      MaxObjsSoftThreshold INTEGER ,	\
      MaxSize	INTEGER ,		\
      MaxObjects INTEGER ,		\
      Enabled Boolean ,		\
      CheckOnRaw Boolean \n);";

    static constexpr std::string_view CreateLCEntryTableQ =
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      LCIndex  TEXT NOT NULL , \
      BucketName TEXT NOT NULL , \
      StartTime  INTEGER , \
      Status     INTEGER , \
      PRIMARY KEY (LCIndex, BucketName) \n);";

    static constexpr std::string_view CreateLCHeadTableQ =
      "CREATE TABLE IF NOT EXISTS '{}' ( \
      LCIndex  TEXT NOT NULL , \
      Marker TEXT , \
      StartDate  INTEGER , \
      PRIMARY KEY (LCIndex) \n);";

    static constexpr std::string_view DropQ = "DROP TABLE IF EXISTS '{}'";
    static constexpr std::string_view ListAllQ = "SELECT  * from '{}'";

  public:
    DBOp() {}
    virtual ~DBOp() {}
    std::mutex mtx; // to protect prepared stmt

    static std::string CreateTableSchema(std::string_view type,
                                         const DBOpParams *params) {
      if (!type.compare("User"))
        return fmt::format(CreateUserTableQ,
            params->user_table);
      if (!type.compare("Bucket"))
        return fmt::format(CreateBucketTableQ,
            params->bucket_table,
            params->user_table);
      if (!type.compare("Object"))
        return fmt::format(CreateObjectTableQ,
            params->object_table,
            params->bucket_table);
      if (!type.compare("ObjectTrigger"))
        return fmt::format(CreateObjectTableTriggerQ,
            params->object_trigger,
            params->object_table,
            params->object_table,
            params->object_table);
      if (!type.compare("ObjectData"))
        return fmt::format(CreateObjectDataTableQ,
            params->objectdata_table,
            params->bucket_table);
      if (!type.compare("ObjectView"))
        return fmt::format(CreateObjectTableQ,
            params->object_view,
            params->objectdata_table,
            params->object_table);
      if (!type.compare("Quota"))
        return fmt::format(CreateQuotaTableQ,
            params->quota_table);
      if (!type.compare("LCHead"))
        return fmt::format(CreateLCHeadTableQ,
            params->lc_head_table);
      if (!type.compare("LCEntry"))
        return fmt::format(CreateLCEntryTableQ,
            params->lc_entry_table,
            params->bucket_table);

      ceph_abort_msgf("incorrect table type %.*s", type.size(), type.data());
    }

    static std::string DeleteTableSchema(std::string_view table) {
      return fmt::format(DropQ, table);
    }
    static std::string ListTableSchema(std::string_view table) {
      return fmt::format(ListAllQ, table);
    }

    virtual int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; }
    virtual int Bind(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; }
    virtual int Execute(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; }
};

class InsertUserOp : virtual public DBOp {
  private:
    /* For existing entires, -
     * (1) INSERT or REPLACE - it will delete previous entry and then
     * inserts new one. Since it deletes previous entries, it will
     * trigger all foreign key cascade deletes or other triggers.
     * (2) INSERT or UPDATE - this will set NULL values to unassigned
     * fields.
     * more info: https://code-examples.net/en/q/377728
     *
     * For now using INSERT or REPLACE. If required of updating existing
     * record, will use another query.
     */
    static constexpr std::string_view Query = "INSERT OR REPLACE INTO '{}'	\
                          (UserID, Tenant, NS, DisplayName, UserEmail, \
                           AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                           SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                           System, PlacementName, PlacementStorageClass, PlacementTags, \
                           BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, \
                           UserAttrs, UserVersion, UserVersionTag) \
                          VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
                              {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});";

  public:
    virtual ~InsertUserOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.user_table,
          params.op.user.user_id, params.op.user.tenant, params.op.user.ns,
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
          params.op.user.user_attrs, params.op.user.user_ver,
          params.op.user.user_ver_tag);
    }

};

class RemoveUserOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "DELETE from '{}' where UserID = {}";

  public:
    virtual ~RemoveUserOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.user_table,
          params.op.user.user_id);
    }
};

class GetUserOp: virtual public DBOp {
  private:
    /* If below query columns are updated, make sure to update the indexes
     * in list_user() cbk in sqliteDB.cc */
    static constexpr std::string_view Query = "SELECT \
                          UserID, Tenant, NS, DisplayName, UserEmail, \
                          AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                          SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                          System, PlacementName, PlacementStorageClass, PlacementTags, \
                          BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                          UserAttrs, UserVersion, UserVersionTag from '{}' where UserID = {}";

    static constexpr std::string_view QueryByEmail = "SELECT \
                                 UserID, Tenant, NS, DisplayName, UserEmail, \
                                 AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                                 SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                                 System, PlacementName, PlacementStorageClass, PlacementTags, \
                                 BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                                 UserAttrs, UserVersion, UserVersionTag from '{}' where UserEmail = {}";

    static constexpr std::string_view QueryByAccessKeys = "SELECT \
                                      UserID, Tenant, NS, DisplayName, UserEmail, \
                                      AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                                      SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                                      System, PlacementName, PlacementStorageClass, PlacementTags, \
                                      BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                                      UserAttrs, UserVersion, UserVersionTag from '{}' where AccessKeysID = {}";

    static constexpr std::string_view QueryByUserID = "SELECT \
                                  UserID, Tenant, NS, DisplayName, UserEmail, \
                                  AccessKeysID, AccessKeysSecret, AccessKeys, SwiftKeys,\
                                  SubUsers, Suspended, MaxBuckets, OpMask, UserCaps, Admin, \
                                  System, PlacementName, PlacementStorageClass, PlacementTags, \
                                  BucketQuota, TempURLKeys, UserQuota, Type, MfaIDs, AssumedRoleARN, \
                                  UserAttrs, UserVersion, UserVersionTag \
                                  from '{}' where UserID = {}";

  public:
    virtual ~GetUserOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      if (params.op.query_str == "email") {
        return fmt::format(QueryByEmail, params.user_table,
            params.op.user.user_email);
      } else if (params.op.query_str == "access_key") {
        return fmt::format(QueryByAccessKeys,
            params.user_table,
            params.op.user.access_keys_id);
      } else if (params.op.query_str == "user_id") {
        return fmt::format(QueryByUserID,
            params.user_table,
            params.op.user.user_id);
      } else {
        return fmt::format(Query, params.user_table,
            params.op.user.user_id);
      }
    }
};

class InsertBucketOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
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

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.bucket_table,
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
    static constexpr std::string_view InfoQuery =
      "UPDATE '{}' SET Tenant = {}, Marker = {}, BucketID = {}, CreationTime = {}, \
      Count = {}, PlacementName = {}, PlacementStorageClass = {}, OwnerID = {}, Flags = {}, \
      Zonegroup = {}, HasInstanceObj = {}, Quota = {}, RequesterPays = {}, HasWebsite = {}, \
      WebsiteConf = {}, SwiftVersioning = {}, SwiftVerLocation = {}, MdsearchConfig = {}, \
      NewBucketInstanceID = {}, ObjectLock = {}, SyncPolicyInfoGroups = {}, \
      BucketVersion = {}, Mtime = {} WHERE BucketName = {}";
    // Updates Attrs, OwnerID, Mtime, Version
    static constexpr std::string_view AttrsQuery =
      "UPDATE '{}' SET OwnerID = {}, BucketAttrs = {}, Mtime = {}, BucketVersion = {} \
      WHERE BucketName = {}";
    // Updates OwnerID, CreationTime, Mtime, Version
    static constexpr std::string_view OwnerQuery =
      "UPDATE '{}' SET OwnerID = {}, CreationTime = {}, Mtime = {}, BucketVersion = {} WHERE BucketName = {}";

  public:
    virtual ~UpdateBucketOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      if (params.op.query_str == "info") {
        return fmt::format(InfoQuery, params.bucket_table,
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
        return fmt::format(AttrsQuery, params.bucket_table,
            params.op.user.user_id, params.op.bucket.bucket_attrs,
            params.op.bucket.mtime,
            params.op.bucket.bucket_ver, params.op.bucket.bucket_name);
      }
      if (params.op.query_str == "owner") {
        return fmt::format(OwnerQuery, params.bucket_table,
            params.op.user.user_id, params.op.bucket.creation_time,
            params.op.bucket.mtime,
            params.op.bucket.bucket_ver, params.op.bucket.bucket_name);
      }
      return "";
    }
};

class RemoveBucketOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "DELETE from '{}' where BucketName = {}";

  public:
    virtual ~RemoveBucketOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.bucket_table,
          params.op.bucket.bucket_name);
    }
};

class GetBucketOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query = "SELECT  \
                          BucketName, Tenant, Marker, BucketID, Size, SizeRounded, CreationTime, \
                          Count, PlacementName, PlacementStorageClass, OwnerID, Flags, Zonegroup, \
                          HasInstanceObj, Quota, RequesterPays, HasWebsite, WebsiteConf, \
                          SwiftVersioning, SwiftVerLocation, \
                          MdsearchConfig, NewBucketInstanceID, ObjectLock, \
                          SyncPolicyInfoGroups, BucketAttrs, BucketVersion, BucketVersionTag, Mtime \
                          from '{}' where BucketName = {}";

  public:
    virtual ~GetBucketOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query,
          params.bucket_table,
          params.op.bucket.bucket_name);
    }
};

class ListUserBucketsOp: virtual public DBOp {
  private:
    // once we have stats also stored, may have to update this query to join
    // these two tables.
    static constexpr std::string_view Query = "SELECT  \
                          BucketName, Tenant, Marker, BucketID, Size, SizeRounded, CreationTime, \
                          Count, PlacementName, PlacementStorageClass, OwnerID, Flags, Zonegroup, \
                          HasInstanceObj, Quota, RequesterPays, HasWebsite, WebsiteConf, \
                          SwiftVersioning, SwiftVerLocation, \
                          MdsearchConfig, NewBucketInstanceID, ObjectLock, \
                          SyncPolicyInfoGroups, BucketAttrs, BucketVersion, BucketVersionTag, Mtime \
                          FROM '{}' WHERE OwnerID = {} AND BucketName > {} ORDER BY BucketName ASC LIMIT {}";

    /* BucketNames are unique across users. Hence userid/OwnerID is not used as
     * marker or for ordering here in the below query 
     */
    static constexpr std::string_view AllQuery = "SELECT  \
                          BucketName, Tenant, Marker, BucketID, Size, SizeRounded, CreationTime, \
                          Count, PlacementName, PlacementStorageClass, OwnerID, Flags, Zonegroup, \
                          HasInstanceObj, Quota, RequesterPays, HasWebsite, WebsiteConf, \
                          SwiftVersioning, SwiftVerLocation, \
                          MdsearchConfig, NewBucketInstanceID, ObjectLock, \
                          SyncPolicyInfoGroups, BucketAttrs, BucketVersion, BucketVersionTag, Mtime \
                          FROM '{}' WHERE BucketName > {} ORDER BY BucketName ASC LIMIT {}";

  public:
    virtual ~ListUserBucketsOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      if (params.op.query_str == "all") {
        return fmt::format(AllQuery, params.bucket_table,
          params.op.bucket.min_marker,
          params.op.list_max_count);
      } else {
        return fmt::format(Query, params.bucket_table,
          params.op.user.user_id, params.op.bucket.min_marker,
          params.op.list_max_count);
      }
    }
};

class PutObjectOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "INSERT OR REPLACE INTO '{}' \
      (ObjName, ObjInstance, ObjNS, BucketName, ACLs, IndexVer, Tag, \
       Flags, VersionedEpoch, ObjCategory, Etag, Owner, OwnerDisplayName, \
       StorageClass, Appendable, ContentType, IndexHashSource, ObjSize, \
       AccountedSize, Mtime, Epoch, ObjTag, TailTag, WriteTag, FakeTag, \
       ShadowObj, HasData, IsVersioned, VersionNum, PGVer, ZoneShortID, \
       ObjVersion, ObjVersionTag, ObjAttrs, HeadSize, MaxHeadSize, \
       ObjID, TailInstance, HeadPlacementRuleName, HeadPlacementRuleStorageClass, \
       TailPlacementRuleName, TailPlacementStorageClass, \
       ManifestPartObjs, ManifestPartRules, Omap, IsMultipart, MPPartsList, \
       HeadData)     \
      VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
          {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
          {}, {}, {}, \
          {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})";

  public:
    virtual ~PutObjectOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query,
          params.object_table, params.op.obj.obj_name,
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
          params.op.obj.has_data, params.op.obj.is_versioned,
          params.op.obj.version_num,
          params.op.obj.pg_ver, params.op.obj.zone_short_id,
          params.op.obj.obj_version, params.op.obj.obj_version_tag,
          params.op.obj.obj_attrs, params.op.obj.head_size,
          params.op.obj.max_head_size, params.op.obj.obj_id,
          params.op.obj.tail_instance,
          params.op.obj.head_placement_rule_name,
          params.op.obj.head_placement_storage_class,
          params.op.obj.tail_placement_rule_name,
          params.op.obj.tail_placement_storage_class,
          params.op.obj.manifest_part_objs,
          params.op.obj.manifest_part_rules, params.op.obj.omap,
          params.op.obj.is_multipart, params.op.obj.mp_parts,
          params.op.obj.head_data);
    }
};

class DeleteObjectOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "DELETE from '{}' where BucketName = {} and ObjName = {} and ObjInstance = {}";

  public:
    virtual ~DeleteObjectOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.object_table,
          params.op.bucket.bucket_name,
          params.op.obj.obj_name,
          params.op.obj.obj_instance);
    }
};

class GetObjectOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "SELECT  \
      ObjName, ObjInstance, ObjNS, BucketName, ACLs, IndexVer, Tag, \
      Flags, VersionedEpoch, ObjCategory, Etag, Owner, OwnerDisplayName, \
      StorageClass, Appendable, ContentType, IndexHashSource, ObjSize, \
      AccountedSize, Mtime, Epoch, ObjTag, TailTag, WriteTag, FakeTag, \
      ShadowObj, HasData, IsVersioned, VersionNum, PGVer, ZoneShortID, \
      ObjVersion, ObjVersionTag, ObjAttrs, HeadSize, MaxHeadSize, \
      ObjID, TailInstance, HeadPlacementRuleName, HeadPlacementRuleStorageClass, \
      TailPlacementRuleName, TailPlacementStorageClass, \
      ManifestPartObjs, ManifestPartRules, Omap, IsMultipart, MPPartsList, \
      HeadData from '{}' \
      where BucketName = {} and ObjName = {} and ObjInstance = {}";

  public:
    virtual ~GetObjectOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query,
          params.object_table,
          params.op.bucket.bucket_name,
          params.op.obj.obj_name,
          params.op.obj.obj_instance);
    }
};

class ListBucketObjectsOp: virtual public DBOp {
  private:
    // once we have stats also stored, may have to update this query to join
    // these two tables.
    static constexpr std::string_view Query =
      "SELECT  \
      ObjName, ObjInstance, ObjNS, BucketName, ACLs, IndexVer, Tag, \
      Flags, VersionedEpoch, ObjCategory, Etag, Owner, OwnerDisplayName, \
      StorageClass, Appendable, ContentType, IndexHashSource, ObjSize, \
      AccountedSize, Mtime, Epoch, ObjTag, TailTag, WriteTag, FakeTag, \
      ShadowObj, HasData, IsVersioned, VersionNum, PGVer, ZoneShortID, \
      ObjVersion, ObjVersionTag, ObjAttrs, HeadSize, MaxHeadSize, \
      ObjID, TailInstance, HeadPlacementRuleName, HeadPlacementRuleStorageClass, \
      TailPlacementRuleName, TailPlacementStorageClass, \
      ManifestPartObjs, ManifestPartRules, Omap, IsMultipart, MPPartsList, HeadData from '{}' \
      where BucketName = {} and ObjName >= {} and ObjName LIKE {} ORDER BY ObjName ASC, VersionNum DESC LIMIT {}";
  public:
    virtual ~ListBucketObjectsOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      /* XXX: Include obj_id, delim */
      return fmt::format(Query,
          params.object_table,
          params.op.bucket.bucket_name,
          params.op.obj.min_marker,
          params.op.obj.prefix,
          params.op.list_max_count);
    }
};

#define MAX_VERSIONED_OBJECTS 20
class ListVersionedObjectsOp: virtual public DBOp {
  private:
    // once we have stats also stored, may have to update this query to join
    // these two tables.
    static constexpr std::string_view Query =
      "SELECT  \
      ObjName, ObjInstance, ObjNS, BucketName, ACLs, IndexVer, Tag, \
      Flags, VersionedEpoch, ObjCategory, Etag, Owner, OwnerDisplayName, \
      StorageClass, Appendable, ContentType, IndexHashSource, ObjSize, \
      AccountedSize, Mtime, Epoch, ObjTag, TailTag, WriteTag, FakeTag, \
      ShadowObj, HasData, IsVersioned, VersionNum, PGVer, ZoneShortID, \
      ObjVersion, ObjVersionTag, ObjAttrs, HeadSize, MaxHeadSize, \
      ObjID, TailInstance, HeadPlacementRuleName, HeadPlacementRuleStorageClass, \
      TailPlacementRuleName, TailPlacementStorageClass, \
      ManifestPartObjs, ManifestPartRules, Omap, IsMultipart, MPPartsList, \
      HeadData from '{}' \
      where BucketName = {} and ObjName = {} ORDER BY VersionNum DESC LIMIT {}";
  public:
    virtual ~ListVersionedObjectsOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      /* XXX: Include obj_id, delim */
      return fmt::format(Query,
          params.object_table,
          params.op.bucket.bucket_name,
          params.op.obj.obj_name,
          params.op.list_max_count);
    }
};

class UpdateObjectOp: virtual public DBOp {
  private:
    // Updates Omap
    static constexpr std::string_view OmapQuery =
      "UPDATE '{}' SET Omap = {}, Mtime = {} \
      where BucketName = {} and ObjName = {} and ObjInstance = {}";
    static constexpr std::string_view AttrsQuery =
      "UPDATE '{}' SET ObjAttrs = {}, Mtime = {}  \
      where BucketName = {} and ObjName = {} and ObjInstance = {}";
    static constexpr std::string_view MPQuery =
      "UPDATE '{}' SET MPPartsList = {}, Mtime = {}  \
      where BucketName = {} and ObjName = {} and ObjInstance = {}";
    static constexpr std::string_view MetaQuery =
      "UPDATE '{}' SET \
       ObjNS = {}, ACLs = {}, IndexVer = {}, Tag = {}, Flags = {}, VersionedEpoch = {}, \
       ObjCategory = {}, Etag = {}, Owner = {}, OwnerDisplayName = {}, \
       StorageClass = {}, Appendable = {}, ContentType = {}, \
       IndexHashSource = {}, ObjSize = {}, AccountedSize = {}, Mtime = {}, \
       Epoch = {}, ObjTag = {}, TailTag = {}, WriteTag = {}, FakeTag = {}, \
       ShadowObj = {}, HasData = {}, IsVersioned = {}, VersionNum = {}, PGVer = {}, \
       ZoneShortID = {}, ObjVersion = {}, ObjVersionTag = {}, ObjAttrs = {}, \
       HeadSize = {}, MaxHeadSize = {}, ObjID = {}, TailInstance = {}, \
       HeadPlacementRuleName = {}, HeadPlacementRuleStorageClass = {}, \
       TailPlacementRuleName = {}, TailPlacementStorageClass = {}, \
       ManifestPartObjs = {}, ManifestPartRules = {}, Omap = {}, \
       IsMultipart = {}, MPPartsList = {}, HeadData = {} \
       WHERE ObjName = {} and ObjInstance = {} and BucketName = {}";

  public:
    virtual ~UpdateObjectOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      if (params.op.query_str == "omap") {
        return fmt::format(OmapQuery,
            params.object_table, params.op.obj.omap,
            params.op.obj.mtime,
            params.op.bucket.bucket_name,
            params.op.obj.obj_name,
            params.op.obj.obj_instance);
      }
      if (params.op.query_str == "attrs") {
        return fmt::format(AttrsQuery,
            params.object_table, params.op.obj.obj_attrs,
            params.op.obj.mtime,
            params.op.bucket.bucket_name,
            params.op.obj.obj_name,
            params.op.obj.obj_instance);
      }
      if (params.op.query_str == "mp") {
        return fmt::format(MPQuery,
            params.object_table, params.op.obj.mp_parts,
            params.op.obj.mtime,
            params.op.bucket.bucket_name,
            params.op.obj.obj_name,
            params.op.obj.obj_instance);
      }
      if (params.op.query_str == "meta") {
        return fmt::format(MetaQuery,
          params.object_table,
          params.op.obj.obj_ns, params.op.obj.acls, params.op.obj.index_ver,
          params.op.obj.tag, params.op.obj.flags, params.op.obj.versioned_epoch,
          params.op.obj.obj_category, params.op.obj.etag, params.op.obj.owner,
          params.op.obj.owner_display_name, params.op.obj.storage_class,
          params.op.obj.appendable, params.op.obj.content_type,
          params.op.obj.index_hash_source, params.op.obj.obj_size,
          params.op.obj.accounted_size, params.op.obj.mtime,
          params.op.obj.epoch, params.op.obj.obj_tag, params.op.obj.tail_tag,
          params.op.obj.write_tag, params.op.obj.fake_tag, params.op.obj.shadow_obj,
          params.op.obj.has_data, params.op.obj.is_versioned, params.op.obj.version_num,
          params.op.obj.pg_ver, params.op.obj.zone_short_id,
          params.op.obj.obj_version, params.op.obj.obj_version_tag,
          params.op.obj.obj_attrs, params.op.obj.head_size,
          params.op.obj.max_head_size, params.op.obj.obj_id,
          params.op.obj.tail_instance,
          params.op.obj.head_placement_rule_name,
          params.op.obj.head_placement_storage_class,
          params.op.obj.tail_placement_rule_name,
          params.op.obj.tail_placement_storage_class,
          params.op.obj.manifest_part_objs,
          params.op.obj.manifest_part_rules, params.op.obj.omap,
          params.op.obj.is_multipart, params.op.obj.mp_parts,
          params.op.obj.head_data, 
          params.op.obj.obj_name, params.op.obj.obj_instance,
          params.op.bucket.bucket_name);
      }
      return "";
    }
};

class PutObjectDataOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "INSERT OR REPLACE INTO '{}' \
      (ObjName, ObjInstance, ObjNS, BucketName, ObjID, MultipartPartStr, PartNum, Offset, Size, Mtime, Data) \
      VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})";

  public:
    virtual ~PutObjectDataOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query,
          params.objectdata_table,
          params.op.obj.obj_name, params.op.obj.obj_instance,
          params.op.obj.obj_ns,
          params.op.bucket.bucket_name,
          params.op.obj.obj_id,
          params.op.obj_data.multipart_part_str,
          params.op.obj_data.part_num,
          params.op.obj_data.offset,
          params.op.obj_data.size,
          params.op.obj.mtime,
          params.op.obj_data.data);
    }
};

/* XXX: Recheck if this is really needed */
class UpdateObjectDataOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "UPDATE '{}' \
      SET Mtime = {} WHERE ObjName = {} and ObjInstance = {} and \
      BucketName = {} and ObjID = {}";

  public:
    virtual ~UpdateObjectDataOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query,
          params.objectdata_table,
          params.op.obj.mtime,
          params.op.obj.obj_name, params.op.obj.obj_instance,
          params.op.bucket.bucket_name,
          params.op.obj.obj_id);
    }
};

class GetObjectDataOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "SELECT  \
      ObjName, ObjInstance, ObjNS, BucketName, ObjID, MultipartPartStr, PartNum, Offset, Size, Mtime, Data \
      from '{}' where BucketName = {} and ObjName = {} and ObjInstance = {} and ObjID = {} ORDER BY MultipartPartStr, PartNum";

  public:
    virtual ~GetObjectDataOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query,
          params.objectdata_table,
          params.op.bucket.bucket_name,
          params.op.obj.obj_name,
          params.op.obj.obj_instance,
          params.op.obj.obj_id);
    }
};

class DeleteObjectDataOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "DELETE from '{}' where BucketName = {} and ObjName = {} and ObjInstance = {} and ObjID = {}";

  public:
    virtual ~DeleteObjectDataOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query,
          params.objectdata_table,
          params.op.bucket.bucket_name,
          params.op.obj.obj_name,
          params.op.obj.obj_instance,
          params.op.obj.obj_id);
    }
};

class DeleteStaleObjectDataOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "DELETE from '{}' WHERE (ObjName, ObjInstance, ObjID) NOT IN (SELECT s.ObjName, s.ObjInstance, s.ObjID from '{}' as s INNER JOIN '{}' USING (ObjName, BucketName, ObjInstance, ObjID)) and Mtime < {}";

  public:
    virtual ~DeleteStaleObjectDataOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query,
          params.objectdata_table,
          params.objectdata_table,
          params.object_table,
          params.op.obj.mtime);
    }
};

class InsertLCEntryOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "INSERT OR REPLACE INTO '{}' \
      (LCIndex, BucketName, StartTime, Status) \
      VALUES ({}, {}, {}, {})";

  public:
    virtual ~InsertLCEntryOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.lc_entry_table,
          params.op.lc_entry.index, params.op.lc_entry.bucket_name,
          params.op.lc_entry.start_time, params.op.lc_entry.status);
    }
};

class RemoveLCEntryOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "DELETE from '{}' where LCIndex = {} and BucketName = {}";

  public:
    virtual ~RemoveLCEntryOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.lc_entry_table,
          params.op.lc_entry.index, params.op.lc_entry.bucket_name);
    }
};

class GetLCEntryOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query = "SELECT  \
                          LCIndex, BucketName, StartTime, Status \
                          from '{}' where LCIndex = {} and BucketName = {}";
    static constexpr std::string_view NextQuery = "SELECT  \
                          LCIndex, BucketName, StartTime, Status \
                          from '{}' where LCIndex = {} and BucketName > {} ORDER BY BucketName ASC";

  public:
    virtual ~GetLCEntryOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      if (params.op.query_str == "get_next_entry") {
        return fmt::format(NextQuery, params.lc_entry_table,
            params.op.lc_entry.index, params.op.lc_entry.bucket_name);
      }
      // default 
      return fmt::format(Query, params.lc_entry_table,
          params.op.lc_entry.index, params.op.lc_entry.bucket_name);
    }
};

class ListLCEntriesOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query = "SELECT  \
                          LCIndex, BucketName, StartTime, Status \
                          FROM '{}' WHERE LCIndex = {} AND BucketName > {} ORDER BY BucketName ASC LIMIT {}";

  public:
    virtual ~ListLCEntriesOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.lc_entry_table,
          params.op.lc_entry.index, params.op.lc_entry.min_marker,
          params.op.list_max_count);
    }
};

class InsertLCHeadOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "INSERT OR REPLACE INTO '{}' \
      (LCIndex, Marker, StartDate) \
      VALUES ({}, {}, {})";

  public:
    virtual ~InsertLCHeadOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.lc_head_table,
          params.op.lc_head.index, params.op.lc_head.marker,
          params.op.lc_head.start_date);
    }
};

class RemoveLCHeadOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query =
      "DELETE from '{}' where LCIndex = {}";

  public:
    virtual ~RemoveLCHeadOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.lc_head_table,
          params.op.lc_head.index);
    }
};

class GetLCHeadOp: virtual public DBOp {
  private:
    static constexpr std::string_view Query = "SELECT  \
                          LCIndex, Marker, StartDate \
                          from '{}' where LCIndex = {}";

  public:
    virtual ~GetLCHeadOp() {}

    static std::string Schema(DBOpPrepareParams &params) {
      return fmt::format(Query, params.lc_head_table,
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
    rgw::sal::Driver* driver;
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
    user_table(db_name+"_user_table"),
    bucket_table(db_name+"_bucket_table"),
    quota_table(db_name+"_quota_table"),
    lc_head_table(db_name+"_lc_head_table"),
    lc_entry_table(db_name+"_lc_entry_table"),
    cct(_cct),
    dp(_cct, ceph_subsys_rgw, "rgw DBStore backend: ")
  {}
    /*	DB() {}*/

    DB(CephContext *_cct) : db_name("default_db"),
    user_table(db_name+"_user_table"),
    bucket_table(db_name+"_bucket_table"),
    quota_table(db_name+"_quota_table"),
    lc_head_table(db_name+"_lc_head_table"),
    lc_entry_table(db_name+"_lc_entry_table"),
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
      return db_name+"_"+bucket+"_object_table"; }
    const std::string getObjectDataTable(std::string bucket) {
      return db_name+"_"+bucket+"_objectdata_table"; }
    const std::string getObjectView(std::string bucket) {
      return db_name+"_"+bucket+"_object_view"; }
    const std::string getObjectTrigger(std::string bucket) {
      return db_name+"_"+bucket+"_object_trigger"; }

    std::map<std::string, class ObjectOp*> getObjectMap();

    struct DBOps dbops; // DB operations, make it private?

    void set_driver(rgw::sal::Driver* _driver) {
      driver = _driver;
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

    int InitializeParams(const DoutPrefixProvider *dpp, DBOpParams *params);
    int ProcessOp(const DoutPrefixProvider *dpp, std::string_view Op, DBOpParams *params);
    std::shared_ptr<class DBOp> getDBOp(const DoutPrefixProvider *dpp, std::string_view Op, const DBOpParams *params);
    int objectmapInsert(const DoutPrefixProvider *dpp, std::string bucket, class ObjectOp* ptr);
    int objectmapDelete(const DoutPrefixProvider *dpp, std::string bucket);

    virtual uint64_t get_blob_limit() { return 0; };
    virtual void *openDB(const DoutPrefixProvider *dpp) { return NULL; }
    virtual int closeDB(const DoutPrefixProvider *dpp) { return 0; }
    virtual int createTables(const DoutPrefixProvider *dpp) { return 0; }
    virtual int InitializeDBOps(const DoutPrefixProvider *dpp) { return 0; }
    virtual int InitPrepareParams(const DoutPrefixProvider *dpp,
                                  DBOpPrepareParams &p_params,
                                  DBOpParams* params) = 0;
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
        const rgw_owner& owner, const rgw_bucket& bucket,
        const std::string& zonegroup_id,
        const rgw_placement_rule& placement_rule,
        const std::map<std::string, bufferlist>& attrs,
        const std::optional<std::string>& swift_ver_location,
        const std::optional<RGWQuotaInfo>& quota,
        std::optional<ceph::real_time> creation_time,
        obj_version *pep_objv,
        RGWBucketInfo& info,
        optional_yield y);

    int next_bucket_id() { return ++max_bucket_id; };

    int remove_bucket(const DoutPrefixProvider *dpp, const RGWBucketInfo info);
    int list_buckets(const DoutPrefixProvider *dpp, const std::string& query_str,
        std::string& owner,
        const std::string& marker,
        const std::string& end_marker,
        uint64_t max,
        bool need_stats,
        RGWUserBuckets *buckets,
        bool *is_truncated);
    int update_bucket(const DoutPrefixProvider *dpp, const std::string& query_str,
        RGWBucketInfo& info, bool exclusive,
        const rgw_owner* powner, std::map<std::string, bufferlist>* pattrs,
        ceph::real_time* pmtime, RGWObjVersionTracker* pobjv);

    uint64_t get_max_head_size() { return ObjHeadSize; }
    uint64_t get_max_chunk_size() { return ObjChunkSize; }
    void gen_rand_obj_instance_name(rgw_obj_key *target_key);

    // db raw obj string is of format -
    // "<bucketname>_<objname>_<objinstance>_<multipart-part-str>_<partnum>"
    static constexpr std::string_view raw_obj_oid = "{0}_{1}_{2}_{3}_{4}";

    std::string to_oid(std::string_view bucket, std::string_view obj_name,
                       std::string_view obj_instance, std::string_view obj_id,
                       std::string_view mp_str, uint64_t partnum) {
      return fmt::format(raw_obj_oid, bucket, obj_name, obj_instance, obj_id, mp_str, partnum);
    }
    int from_oid(const std::string& oid, std::string& bucket, std::string& obj_name, std::string& obj_id,
        std::string& obj_instance,
        std::string& mp_str, uint64_t& partnum) {
      // TODO: use ceph::split() from common/split.h
      // XXX: doesn't this break if obj_name has underscores in it?
      std::vector<std::string> result;
      boost::split(result, oid, boost::is_any_of("_"));
      bucket = result[0];
      obj_name = result[1];
      obj_instance = result[2];
      obj_id = result[3];
      mp_str = result[4];
      partnum = stoi(result[5]);

      return 0;
    }

    struct raw_obj {
      DB* db;

      std::string bucket_name;
      std::string obj_name;
      std::string obj_instance;
      std::string obj_ns;
      std::string obj_id;
      std::string multipart_part_str;
      uint64_t part_num;

      std::string obj_table;
      std::string obj_data_table;

      raw_obj(DB* _db) {
        db = _db;
      }

      raw_obj(DB* _db, std::string& _bname, std::string& _obj_name, std::string& _obj_instance,
          std::string& _obj_ns, std::string& _obj_id, std::string _mp_part_str, int _part_num) {
        db = _db;
        bucket_name = _bname;
        obj_name = _obj_name;
        obj_instance = _obj_instance;
        obj_ns = _obj_ns;
        obj_id = _obj_id;
        multipart_part_str = _mp_part_str;
        part_num = _part_num;

        obj_table = bucket_name+".object.table";
        obj_data_table = bucket_name+".objectdata.table";
      }

      raw_obj(DB* _db, std::string& oid) {
        int r;

        db = _db;
        r = db->from_oid(oid, bucket_name, obj_name, obj_instance, obj_id, multipart_part_str,
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

    class GC : public Thread {
      const DoutPrefixProvider *dpp;
      DB *db;
      /* Default time interval for GC 
       * XXX: Make below options configurable
       *
       * gc_interval: The time between successive gc thread runs
       * gc_obj_min_wait: Min. time to wait before deleting any data post its creation.
       *                    
       */
      std::mutex mtx;
      std::condition_variable cv;
      bool stop_signalled = false;
      uint32_t gc_interval = 24*60*60; //sec ; default: 24*60*60
      uint32_t gc_obj_min_wait = 60*60; //60*60sec default
      std::string bucket_marker;
      std::string user_marker;

    public:
      GC(const DoutPrefixProvider *_dpp, DB* _db) :
            dpp(_dpp), db(_db) {}

      void *entry() override;

      void signal_stop() {
	std::lock_guard<std::mutex> lk_guard(mtx);
	stop_signalled = true;
	cv.notify_one();
      }

      friend class DB;
    };
    std::unique_ptr<DB::GC> gc_worker;

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
	  rgw::AccessListFilter access_list_filter;
          RGWBucketListNameFilter force_check_filter;
          bool list_versions;
	  bool allow_unordered;

          Params() :
	        enforce_ns(true),
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

      RGWObjState obj_state;
      std::string obj_id;

      bool versioning_disabled;

      bool bs_initialized;

      public:
      Object(DB *_store, const RGWBucketInfo& _bucket_info, const rgw_obj& _obj) : store(_store), bucket_info(_bucket_info),
      obj(_obj),
      versioning_disabled(false),
      bs_initialized(false) {}

      Object(DB *_store, const RGWBucketInfo& _bucket_info, const rgw_obj& _obj, const std::string& _obj_id) : store(_store), bucket_info(_bucket_info), obj(_obj), obj_id(_obj_id) {}

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
      };

      struct Delete {
        DB::Object *target;

        struct DeleteParams {
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
        int delete_obj_impl(const DoutPrefixProvider *dpp, DBOpParams& del_params);
        int create_dm(const DoutPrefixProvider *dpp, DBOpParams& del_params);
      };

      /* XXX: the parameters may be subject to change. All we need is bucket name
       * & obj name,instance - keys */
      int get_object_impl(const DoutPrefixProvider *dpp, DBOpParams& params);
      int get_obj_state(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info,
                        const rgw_obj& obj,
                        bool follow_olh, RGWObjState **state);
      int get_state(const DoutPrefixProvider *dpp, RGWObjState **pstate, bool follow_olh);
      int list_versioned_objects(const DoutPrefixProvider *dpp,
                                 std::list<rgw_bucket_dir_entry>& list_entries);

      DB *get_store() { return store; }
      rgw_obj& get_obj() { return obj; }
      RGWBucketInfo& get_bucket_info() { return bucket_info; }

      int InitializeParamsfromObject(const DoutPrefixProvider *dpp, DBOpParams* params);
      int set_attrs(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist>& setattrs,
          std::map<std::string, bufferlist>* rmattrs);
      int transition(const DoutPrefixProvider *dpp,
                     const rgw_placement_rule& rule, const real_time& mtime,
                     uint64_t olh_epoch);
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
		  std::unique_ptr<rgw::sal::Lifecycle::LCEntry>* entry);
    int get_next_entry(const std::string& oid, const std::string& marker,
		  std::unique_ptr<rgw::sal::Lifecycle::LCEntry>* entry);
    int set_entry(const std::string& oid, rgw::sal::Lifecycle::LCEntry& entry);
    int list_entries(const std::string& oid, const std::string& marker,
			   uint32_t max_entries, std::vector<std::unique_ptr<rgw::sal::Lifecycle::LCEntry>>& entries);
    int rm_entry(const std::string& oid, rgw::sal::Lifecycle::LCEntry& entry);
    int get_head(const std::string& oid, std::unique_ptr<rgw::sal::Lifecycle::LCHead>* head);
    int put_head(const std::string& oid, rgw::sal::Lifecycle::LCHead& head);
    int delete_stale_objs(const DoutPrefixProvider *dpp, const std::string& bucket,
                          uint32_t min_wait);
    int createGC(const DoutPrefixProvider *_dpp);
    int stopGC();
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
