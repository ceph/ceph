// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "sqliteDB.h"
#include "rgw_account.h"
#include "common/iso_8601.h"

using namespace std;

#define SQL_PREPARE(dpp, params, sdb, stmt, ret, Op) 	\
  do {							\
    string schema;			   		\
    schema = Schema(params);	   		\
    sqlite3_prepare_v2 (*sdb, schema.c_str(), 	\
        -1, &stmt , NULL);		\
    if (!stmt) {					\
      ldpp_dout(dpp, 0) <<"failed to prepare statement " \
      <<"for Op("<<Op<<"); Errmsg -"\
      <<sqlite3_errmsg(*sdb)<< dendl;\
      ret = -1;				\
      goto out;				\
    }						\
    ldpp_dout(dpp, 20)<<"Successfully Prepared stmt for Op("<<Op	\
    <<") schema("<<schema<<") stmt("<<stmt<<")"<< dendl;	\
    ret = 0;					\
  } while(0);

#define SQL_BIND_INDEX(dpp, stmt, index, str, sdb)	\
  do {						\
    index = sqlite3_bind_parameter_index(stmt, str);     \
    \
    if (index <=0)  {				     \
      ldpp_dout(dpp, 0) <<"failed to fetch bind parameter"\
      " index for str("<<str<<") in "   \
      <<"stmt("<<stmt<<"); Errmsg -"    \
      <<sqlite3_errmsg(*sdb)<< dendl; 	     \
      rc = -1;				     \
      goto out;				     \
    }						     \
    ldpp_dout(dpp, 20)<<"Bind parameter index for str("  \
    <<str<<") in stmt("<<stmt<<") is "  \
    <<index<< dendl;			     \
  }while(0);

#define SQL_BIND_TEXT(dpp, stmt, index, str, sdb)			\
  do {								\
    rc = sqlite3_bind_text(stmt, index, str, -1, SQLITE_TRANSIENT); 	\
    if (rc != SQLITE_OK) {					      	\
      ldpp_dout(dpp, 0)<<"sqlite bind text failed for index("     	\
      <<index<<"), str("<<str<<") in stmt("   	\
      <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
      << dendl;				\
      rc = -1;					\
      goto out;					\
    }							\
    ldpp_dout(dpp, 20)<<"Bind parameter text for index("  \
    <<index<<") in stmt("<<stmt<<") is "  \
    <<str<< dendl;			     \
  }while(0);

#define SQL_BIND_INT(dpp, stmt, index, num, sdb)			\
  do {								\
    rc = sqlite3_bind_int(stmt, index, num);		\
    \
    if (rc != SQLITE_OK) {					\
      ldpp_dout(dpp, 0)<<"sqlite bind int failed for index("     	\
      <<index<<"), num("<<num<<") in stmt("   	\
      <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
      << dendl;				\
      rc = -1;					\
      goto out;					\
    }							\
    ldpp_dout(dpp, 20)<<"Bind parameter int for index("  \
    <<index<<") in stmt("<<stmt<<") is "  \
    <<num<< dendl;			     \
  }while(0);

#define SQL_BIND_BLOB(dpp, stmt, index, blob, size, sdb)		\
  do {								\
    rc = sqlite3_bind_blob(stmt, index, blob, size, SQLITE_TRANSIENT);  \
    \
    if (rc != SQLITE_OK) {					\
      ldpp_dout(dpp, 0)<<"sqlite bind blob failed for index("     	\
      <<index<<"), blob("<<blob<<") in stmt("   	\
      <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
      << dendl;				\
      rc = -1;					\
      goto out;					\
    }							\
  }while(0);

#define SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, param, sdb)		\
  do {								\
    bufferlist b;						\
    encode(param, b);					\
    SQL_BIND_BLOB(dpp, stmt, index, b.c_str(), b.length(), sdb); \
  }while(0);

#define SQL_READ_BLOB(dpp, stmt, index, void_ptr, len)		\
  do {								\
    void_ptr = NULL;					\
    void_ptr = (void *)sqlite3_column_blob(stmt, index);	\
    len = sqlite3_column_bytes(stmt, index);		\
    \
    if (!void_ptr || len == 0) {				\
      ldpp_dout(dpp, 20)<<"Null value for blob index("  \
      <<index<<") in stmt("<<stmt<<") "<< dendl;   \
    }							\
  }while(0);

#define SQL_DECODE_BLOB_PARAM(dpp, stmt, index, param, sdb)		\
  do {								\
    bufferlist b;						\
    void *blob;						\
    int blob_len = 0;					\
    \
    SQL_READ_BLOB(dpp, stmt, index, blob, blob_len);		\
    \
    b.append(reinterpret_cast<char *>(blob), blob_len);	\
    \
    decode(param, b);					\
  }while(0);

#define SQL_EXECUTE(dpp, params, stmt, cbk, args...) \
  do{						\
    const std::lock_guard<std::mutex> lk(((DBOp*)(this))->mtx); \
    if (!stmt) {				\
      ret = Prepare(dpp, params);		\
    }					\
    \
    if (!stmt) {				\
      ldpp_dout(dpp, 0) <<"No prepared statement "<< dendl;	\
      goto out;			\
    }					\
    \
    ret = Bind(dpp, params);			\
    if (ret) {				\
      ldpp_dout(dpp, 0) <<"Bind parameters failed for stmt(" <<stmt<<") "<< dendl;		\
      goto out;			\
    }					\
    \
    ret = Step(dpp, params->op, stmt, cbk);		\
    \
    Reset(dpp, stmt);				\
    \
    if (ret) {				\
      ldpp_dout(dpp, 0) <<"Execution failed for stmt(" <<stmt<<")"<< dendl;		\
      goto out;			\
    }					\
  }while(0);

int SQLiteDB::InitPrepareParams(const DoutPrefixProvider *dpp,
                                DBOpPrepareParams &p_params,
                                DBOpParams* params)
{
  std::string bucket;

  if (!params)
    return -1;

  if (params->account_table.empty()) {
    params->account_table = getAccountTable();
  }
  if (params->role_table.empty()) {
    params->role_table = getRoleTable();
  }
  if (params->oidc_table.empty()) {
    params->oidc_table = getOIDCTable();
  }
  if (params->group_table.empty()) {
    params->group_table = getGroupTable();
  }
  if (params->group_users_table.empty()) {
    params->group_users_table = getGroupUsersTable();
  }
  if (params->topic_table.empty()) {
    params->topic_table = getTopicTable();
  }
  if (params->bucket_topic_mapping_table.empty()) {
    params->bucket_topic_mapping_table = getBucketTopicMappingTable();
  }
  if (params->user_table.empty()) {
    params->user_table = getUserTable();
  }
  if (params->bucket_table.empty()) {
    params->bucket_table = getBucketTable();
  }
  if (params->quota_table.empty()) {
    params->quota_table = getQuotaTable();
  }
  if (params->bucket_stats_table.empty()) {
    params->bucket_stats_table = getBucketStatsTable();
  }
  if (params->lc_entry_table.empty()) {
    params->lc_entry_table = getLCEntryTable();
  }
  if (params->lc_head_table.empty()) {
    params->lc_head_table = getLCHeadTable();
  }

  p_params.account_table = params->account_table;
  p_params.role_table = params->role_table;
  p_params.oidc_table = params->oidc_table;
  p_params.group_table = params->group_table;
  p_params.group_users_table = params->group_users_table;
  p_params.topic_table = params->topic_table;
  p_params.bucket_topic_mapping_table = params->bucket_topic_mapping_table;
  p_params.user_table = params->user_table;
  p_params.bucket_table = params->bucket_table;
  p_params.quota_table = params->quota_table;
  p_params.bucket_stats_table = params->bucket_stats_table;
  p_params.lc_entry_table = params->lc_entry_table;
  p_params.lc_head_table = params->lc_head_table;

  p_params.op.query_str = params->op.query_str;

  bucket = params->op.bucket.info.bucket.name;

  if (!bucket.empty()) {
    if (params->object_table.empty()) {
      params->object_table = getObjectTable(bucket);
    }
    if (params->objectdata_table.empty()) {
      params->objectdata_table = getObjectDataTable(bucket);
    }
    if (params->object_view.empty()) {
      params->object_view = getObjectView(bucket);
    }
    if (params->object_trigger.empty()) {
      params->object_trigger = getObjectTrigger(bucket);
    }
    p_params.object_table = params->object_table;
    p_params.objectdata_table = params->objectdata_table;
    p_params.object_view = params->object_view;
  }

  return 0;
}

static int list_callback(void *None, int argc, char **argv, char **aname)
{
  int i;
  for(i=0; i < argc; i++) {
    string arg = argv[i] ? argv[i] : "NULL";
    cout<<aname[i]<<" = "<<arg<<"\n";
  }
  return 0;
}

enum GetAccount {
  AccountID = 0,
  AccountTenant,
  AccountName,
  Email,
  AccountQuota,
  AccountBucketQuota,
  MaxUsers,
  MaxRoles,
  MaxGroups,
  AccountMaxBuckets,
  MaxAccessKeys,
  AccountAttrs,
};

enum GetRole {
  RoleID = 0,
  RoleName,
  RoleTenant,
  RoleAccountID,
  RolePath,
  RoleARN,
  TrustPolicy,
  PermPolicies,
  ManagedPolicies,
  RoleTags,
  RoleMaxSessionDuration,
  RoleDescription,
  RoleCreationDate,
};

enum GetOIDCProvider {
  OIDCProviderURL = 0,
  OIDCTenant,
  OIDCClientIDs,
  OIDCThumbprints,
  OIDCProviderARN,
  OIDCCreationDate,
};

enum GetGroup {
  GroupID = 0,
  GroupName,
  GroupTenant,
  GroupAccountID,
  GroupPath,
  GroupAttrsCol,
};

enum GetGroupUser {
  GUGroupID = 0,
  GUUserID,
};

enum GetTopic {
  TopicNameCol = 0,
  TopicTenantCol,
  TopicOwnerCol,
  TopicARNCol,
  TopicDestCol,
  TopicOpaqueDataCol,
  TopicPolicyTextCol,
  TopicObjVersionCol,
  TopicObjVersionTagCol,
};

enum GetBucketTopicMapping {
  BTMBucketName = 0,
};

enum GetUser {
  UserID = 0,
  Tenant,
  NS,
  DisplayName,
  UserEmail,
  AccessKeys,
  SwiftKeys,
  SubUsers,
  Suspended,
  MaxBuckets,
  OpMask,
  UserCaps,
  Admin,
  System,
  PlacementName,
  PlacementStorageClass,
  PlacementTags,
  BucketQuota,
  TempURLKeys,
  UserQuota,
  TYPE,
  MfaIDs,
  AssumedRoleARN,
  UserAccountID,
  UserPath,
  UserCreateDate,
  UserAttrs,
  UserVersion,
  UserVersionTag,
};

enum GetBucket {
  BucketName = 0,
  Bucket_Tenant, //Tenant
  Marker,
  BucketID,
  Size,
  SizeRounded,
  CreationTime,
  Count,
  Bucket_PlacementName,
  Bucket_PlacementStorageClass,
  OwnerID,
  Flags,
  Zonegroup,
  HasInstanceObj,
  Quota,
  RequesterPays,
  HasWebsite,
  WebsiteConf,
  SwiftVersioning,
  SwiftVerLocation,
  MdsearchConfig,
  NewBucketInstanceID,
  ObjectLock,
  SyncPolicyInfoGroups,
  BucketAttrs,
  BucketVersion,
  BucketVersionTag,
  Mtime,
  Bucket_User_NS
};

enum GetObject {
  ObjName,
  ObjInstance,
  ObjNS,
  ObjBucketName,
  ACLs,
  IndexVer,
  Tag,
  ObjFlags,
  VersionedEpoch,
  ObjCategory,
  Etag,
  Owner,
  OwnerDisplayName,
  StorageClass,
  Appendable,
  ContentType,
  IndexHashSource,
  ObjSize,
  AccountedSize,
  ObjMtime,
  Epoch,
  ObjTag,
  TailTag,
  WriteTag,
  FakeTag,
  ShadowObj,
  HasData,
  IsVersioned,
  VersionNum,
  PGVer,
  ZoneShortID,
  ObjVersion,
  ObjVersionTag,
  ObjAttrs,
  HeadSize,
  MaxHeadSize,
  ObjID,
  TailInstance,
  HeadPlacementRuleName,
  HeadPlacementRuleStorageClass,
  TailPlacementRuleName,
  TailPlacementStorageClass,
  ManifestPartObjs,
  ManifestPartRules,
  Omap,
  IsMultipart,
  MPPartsList,
  HeadData,
  Versions
};

enum GetObjectData {
  ObjDataName,
  ObjDataInstance,
  ObjDataNS,
  ObjDataBucketName,
  ObjDataID,
  MultipartPartStr,
  PartNum,
  Offset,
  ObjDataSize,
  ObjDataMtime,
  ObjData
};

enum GetLCEntry {
  LCEntryIndex,
  LCEntryBucketName,
  LCEntryStartTime,
  LCEntryStatus
};

enum GetLCHead {
  LCHeadIndex,
  LCHeadMarker,
  LCHeadStartDate
};

static int list_account(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.account.info.id = (const char*)sqlite3_column_text(stmt, AccountID);
  op.account.info.tenant = (const char*)sqlite3_column_text(stmt, AccountTenant);
  op.account.info.name = (const char*)sqlite3_column_text(stmt, AccountName);
  op.account.info.email = (const char*)sqlite3_column_text(stmt, Email);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, AccountQuota, op.account.info.quota, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, AccountBucketQuota, op.account.info.bucket_quota, sdb);

  op.account.info.max_users = sqlite3_column_int(stmt, MaxUsers);
  op.account.info.max_roles = sqlite3_column_int(stmt, MaxRoles);
  op.account.info.max_groups = sqlite3_column_int(stmt, MaxGroups);
  op.account.info.max_buckets = sqlite3_column_int(stmt, AccountMaxBuckets);
  op.account.info.max_access_keys = sqlite3_column_int(stmt, MaxAccessKeys);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, AccountAttrs, op.account.account_attrs, sdb);

  return 0;
}

static int list_role(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  RGWRoleInfo rinfo;

  rinfo.id = (const char*)sqlite3_column_text(stmt, RoleID);
  rinfo.name = (const char*)sqlite3_column_text(stmt, RoleName);

  const char *t = (const char*)sqlite3_column_text(stmt, RoleTenant);
  if (t) rinfo.tenant = t;

  const char *a = (const char*)sqlite3_column_text(stmt, RoleAccountID);
  if (a) rinfo.account_id = a;

  t = (const char*)sqlite3_column_text(stmt, RolePath);
  if (t) rinfo.path = t;

  t = (const char*)sqlite3_column_text(stmt, RoleARN);
  if (t) rinfo.arn = t;

  t = (const char*)sqlite3_column_text(stmt, TrustPolicy);
  if (t) rinfo.trust_policy = t;

  SQL_DECODE_BLOB_PARAM(dpp, stmt, PermPolicies, rinfo.perm_policy_map, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ManagedPolicies, rinfo.managed_policies, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, RoleTags, rinfo.tags, sdb);

  rinfo.max_session_duration = sqlite3_column_int64(stmt, RoleMaxSessionDuration);

  t = (const char*)sqlite3_column_text(stmt, RoleDescription);
  if (t) rinfo.description = t;

  t = (const char*)sqlite3_column_text(stmt, RoleCreationDate);
  if (t) rinfo.creation_date = t;

  op.role.info = rinfo;
  op.role.list_entries.push_back(rinfo);

  return 0;
}

static int list_role_count(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  RGWRoleInfo dummy;
  dummy.id = std::to_string(sqlite3_column_int(stmt, 0));
  op.role.list_entries.push_back(dummy);

  return 0;
}

static int list_oidc_provider(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  RGWOIDCProviderInfo info;

  info.provider_url = (const char*)sqlite3_column_text(stmt, OIDCProviderURL);

  const char *t = (const char*)sqlite3_column_text(stmt, OIDCTenant);
  if (t) info.tenant = t;

  SQL_DECODE_BLOB_PARAM(dpp, stmt, OIDCClientIDs, info.client_ids, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, OIDCThumbprints, info.thumbprints, sdb);

  t = (const char*)sqlite3_column_text(stmt, OIDCProviderARN);
  if (t) info.arn = t;

  t = (const char*)sqlite3_column_text(stmt, OIDCCreationDate);
  if (t) info.creation_date = t;

  op.oidc.info = info;
  op.oidc.list_entries.push_back(info);

  return 0;
}

static int list_topic(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  rgw_pubsub_topic topic;

  topic.name = (const char*)sqlite3_column_text(stmt, TopicNameCol);

  {
    void *blob_ptr = NULL;
    int blob_len = 0;
    SQL_READ_BLOB(dpp, stmt, TopicOwnerCol, blob_ptr, blob_len);
    if (blob_ptr && blob_len > 0) {
      bufferlist bl;
      bl.append(reinterpret_cast<char*>(blob_ptr), blob_len);
      auto iter = bl.cbegin();
      ceph::converted_variant::decode(topic.owner, iter);
    }
  }

  const char *t = (const char*)sqlite3_column_text(stmt, TopicARNCol);
  if (t) topic.arn = t;

  SQL_DECODE_BLOB_PARAM(dpp, stmt, TopicDestCol, topic.dest, sdb);

  t = (const char*)sqlite3_column_text(stmt, TopicOpaqueDataCol);
  if (t) topic.opaque_data = t;

  t = (const char*)sqlite3_column_text(stmt, TopicPolicyTextCol);
  if (t) topic.policy_text = t;

  op.topic.topic = topic;
  op.topic.topic_version.ver = sqlite3_column_int64(stmt, TopicObjVersionCol);
  t = (const char*)sqlite3_column_text(stmt, TopicObjVersionTagCol);
  if (t) op.topic.topic_version.tag = t;

  op.topic.list_entries.push_back(topic);

  return 0;
}

static int list_bucket_topic_mapping(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  const char *b = (const char*)sqlite3_column_text(stmt, BTMBucketName);
  if (b) op.topic.bucket_list.push_back(b);

  return 0;
}

static int list_group(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  RGWGroupInfo info;

  info.id = (const char*)sqlite3_column_text(stmt, GroupID);
  info.name = (const char*)sqlite3_column_text(stmt, GroupName);

  const char *t = (const char*)sqlite3_column_text(stmt, GroupTenant);
  if (t) info.tenant = t;

  t = (const char*)sqlite3_column_text(stmt, GroupAccountID);
  if (t) info.account_id = t;

  t = (const char*)sqlite3_column_text(stmt, GroupPath);
  if (t) info.path = t;

  SQL_DECODE_BLOB_PARAM(dpp, stmt, GroupAttrsCol, op.group.group_attrs, sdb);

  op.group.info = info;
  op.group.list_entries.push_back(info);

  return 0;
}

static int list_group_count(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  RGWGroupInfo dummy;
  dummy.id = std::to_string(sqlite3_column_int(stmt, 0));
  op.group.list_entries.push_back(dummy);

  return 0;
}

static int list_group_user(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  const char *uid = (const char*)sqlite3_column_text(stmt, 0);
  if (uid) {
    op.group.user_list.push_back(uid);
  }

  return 0;
}

static int list_user(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.user.uinfo.user_id.tenant = (const char*)sqlite3_column_text(stmt, Tenant);
  op.user.uinfo.user_id.id = (const char*)sqlite3_column_text(stmt, UserID);
  op.user.uinfo.user_id.ns = (const char*)sqlite3_column_text(stmt, NS);
  op.user.uinfo.display_name = (const char*)sqlite3_column_text(stmt, DisplayName); // user_name
  op.user.uinfo.user_email = (const char*)sqlite3_column_text(stmt, UserEmail);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, SwiftKeys, op.user.uinfo.swift_keys, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, SubUsers, op.user.uinfo.subusers, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, AccessKeys, op.user.uinfo.access_keys, sdb);

  op.user.uinfo.suspended = sqlite3_column_int(stmt, Suspended);
  op.user.uinfo.max_buckets = sqlite3_column_int(stmt, MaxBuckets);
  op.user.uinfo.op_mask = sqlite3_column_int(stmt, OpMask);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, UserCaps, op.user.uinfo.caps, sdb);

  op.user.uinfo.admin = sqlite3_column_int(stmt, Admin);
  op.user.uinfo.system = sqlite3_column_int(stmt, System);

  op.user.uinfo.default_placement.name = (const char*)sqlite3_column_text(stmt, PlacementName);

  op.user.uinfo.default_placement.storage_class = (const char*)sqlite3_column_text(stmt, PlacementStorageClass);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, PlacementTags, op.user.uinfo.placement_tags, sdb);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, BucketQuota, op.user.uinfo.quota.bucket_quota, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, TempURLKeys, op.user.uinfo.temp_url_keys, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, UserQuota, op.user.uinfo.quota.user_quota, sdb);

  op.user.uinfo.type = sqlite3_column_int(stmt, TYPE);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, MfaIDs, op.user.uinfo.mfa_ids, sdb);

  {
    const char *acct = (const char*)sqlite3_column_text(stmt, UserAccountID);
    if (acct) op.user.uinfo.account_id = acct;
  }
  {
    const char *p = (const char*)sqlite3_column_text(stmt, UserPath);
    if (p) op.user.uinfo.path = p;
  }
  {
    const char *cd = (const char*)sqlite3_column_text(stmt, UserCreateDate);
    if (cd) {
      auto tp = ceph::from_iso_8601(cd, false);
      if (tp) op.user.uinfo.create_date = *tp;
    }
  }

  SQL_DECODE_BLOB_PARAM(dpp, stmt, UserAttrs, op.user.user_attrs, sdb);
  op.user.user_version.ver = sqlite3_column_int(stmt, UserVersion);
  op.user.user_version.tag = (const char*)sqlite3_column_text(stmt, UserVersionTag);

  op.user.list_entries.push_back(op.user.uinfo);
  return 0;
}

static int list_user_count(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  RGWUserInfo dummy;
  dummy.user_id.id = std::to_string(sqlite3_column_int(stmt, 0));
  op.user.list_entries.push_back(dummy);

  return 0;
}

static int list_bucket(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.bucket.ent.bucket.name = (const char*)sqlite3_column_text(stmt, BucketName);
  op.bucket.ent.bucket.tenant = (const char*)sqlite3_column_text(stmt, Bucket_Tenant);
  op.bucket.ent.bucket.marker = (const char*)sqlite3_column_text(stmt, Marker);
  op.bucket.ent.bucket.bucket_id = (const char*)sqlite3_column_text(stmt, BucketID);
  op.bucket.ent.size = sqlite3_column_int(stmt, Size);
  op.bucket.ent.size_rounded = sqlite3_column_int(stmt, SizeRounded);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, CreationTime, op.bucket.ent.creation_time, sdb);
  op.bucket.ent.count = sqlite3_column_int(stmt, Count);
  op.bucket.ent.placement_rule.name = (const char*)sqlite3_column_text(stmt, Bucket_PlacementName);
  op.bucket.ent.placement_rule.storage_class = (const char*)sqlite3_column_text(stmt, Bucket_PlacementStorageClass);

  op.bucket.info.bucket = op.bucket.ent.bucket;
  op.bucket.info.placement_rule = op.bucket.ent.placement_rule;
  op.bucket.info.creation_time = op.bucket.ent.creation_time;

  const char* owner_id = (const char*)sqlite3_column_text(stmt, OwnerID);
  op.bucket.info.owner = parse_owner(owner_id);

  op.bucket.info.flags = sqlite3_column_int(stmt, Flags);
  op.bucket.info.zonegroup = (const char*)sqlite3_column_text(stmt, Zonegroup);
  op.bucket.info.has_instance_obj = sqlite3_column_int(stmt, HasInstanceObj);

  SQL_DECODE_BLOB_PARAM(dpp, stmt, Quota, op.bucket.info.quota, sdb);
  op.bucket.info.requester_pays = sqlite3_column_int(stmt, RequesterPays);
  op.bucket.info.has_website = sqlite3_column_int(stmt, HasWebsite);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, WebsiteConf, op.bucket.info.website_conf, sdb);
  op.bucket.info.swift_versioning = sqlite3_column_int(stmt, SwiftVersioning);
  op.bucket.info.swift_ver_location = (const char*)sqlite3_column_text(stmt, SwiftVerLocation);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, MdsearchConfig, op.bucket.info.mdsearch_config, sdb);
  op.bucket.info.new_bucket_instance_id = (const char*)sqlite3_column_text(stmt, NewBucketInstanceID);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjectLock, op.bucket.info.obj_lock, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, SyncPolicyInfoGroups, op.bucket.info.sync_policy, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, BucketAttrs, op.bucket.bucket_attrs, sdb);
  op.bucket.bucket_version.ver = sqlite3_column_int(stmt, BucketVersion);
  op.bucket.bucket_version.tag = (const char*)sqlite3_column_text(stmt, BucketVersionTag);

  /* Read bucket version into info.objv_tracker.read_ver. No need
   * to set write_ver as its not used anywhere. Still keeping its
   * value same as read_ver */
  op.bucket.info.objv_tracker.read_version = op.bucket.bucket_version;
  op.bucket.info.objv_tracker.write_version = op.bucket.bucket_version;

  SQL_DECODE_BLOB_PARAM(dpp, stmt, Mtime, op.bucket.mtime, sdb);

  op.bucket.list_entries.push_back(op.bucket.ent);

  return 0;
}

static int list_object(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  //cout<<sqlite3_column_text(stmt, 0)<<", ";
  //cout<<sqlite3_column_text(stmt, 1) << "\n";

  op.obj.state.exists = true;
  op.obj.state.obj.key.name = (const char*)sqlite3_column_text(stmt, ObjName);
  op.bucket.info.bucket.name = (const char*)sqlite3_column_text(stmt, ObjBucketName);
  op.obj.state.obj.key.instance = (const char*)sqlite3_column_text(stmt, ObjInstance);
  op.obj.state.obj.key.ns = (const char*)sqlite3_column_text(stmt, ObjNS);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ACLs, op.obj.acls, sdb);
  op.obj.index_ver = sqlite3_column_int(stmt, IndexVer);
  op.obj.tag = (const char*)sqlite3_column_text(stmt, Tag);
  op.obj.flags = sqlite3_column_int(stmt, ObjFlags); 
  op.obj.versioned_epoch = sqlite3_column_int(stmt, VersionedEpoch);
  op.obj.category = (RGWObjCategory)sqlite3_column_int(stmt, ObjCategory); 
  op.obj.etag = (const char*)sqlite3_column_text(stmt, Etag);
  op.obj.owner = (const char*)sqlite3_column_text(stmt, Owner);
  op.obj.owner_display_name = (const char*)sqlite3_column_text(stmt, OwnerDisplayName);
  op.obj.storage_class = (const char*)sqlite3_column_text(stmt, StorageClass);
  op.obj.appendable = sqlite3_column_int(stmt, Appendable); 
  op.obj.content_type = (const char*)sqlite3_column_text(stmt, ContentType);
  op.obj.state.obj.index_hash_source = (const char*)sqlite3_column_text(stmt, IndexHashSource);
  op.obj.state.size = sqlite3_column_int(stmt, ObjSize); 
  op.obj.state.accounted_size = sqlite3_column_int(stmt, AccountedSize); 
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjMtime, op.obj.state.mtime, sdb);
  op.obj.state.epoch = sqlite3_column_int(stmt, Epoch);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjTag, op.obj.state.obj_tag, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, TailTag, op.obj.state.tail_tag, sdb);
  op.obj.state.write_tag = (const char*)sqlite3_column_text(stmt, WriteTag);
  op.obj.state.fake_tag = sqlite3_column_int(stmt, FakeTag);
  op.obj.state.shadow_obj = (const char*)sqlite3_column_text(stmt, ShadowObj);
  op.obj.state.has_data = sqlite3_column_int(stmt, HasData); 
  op.obj.is_versioned = sqlite3_column_int(stmt, IsVersioned); 
  op.obj.version_num = sqlite3_column_int(stmt, VersionNum); 
  op.obj.state.pg_ver = sqlite3_column_int(stmt, PGVer); 
  op.obj.state.zone_short_id = sqlite3_column_int(stmt, ZoneShortID); 
  op.obj.state.objv_tracker.read_version.ver = sqlite3_column_int(stmt, ObjVersion); 
  op.obj.state.objv_tracker.read_version.tag = (const char*)sqlite3_column_text(stmt, ObjVersionTag);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjAttrs, op.obj.state.attrset, sdb);
  op.obj.head_size = sqlite3_column_int(stmt, HeadSize); 
  op.obj.max_head_size = sqlite3_column_int(stmt, MaxHeadSize); 
  op.obj.obj_id = (const char*)sqlite3_column_text(stmt, ObjID);
  op.obj.tail_instance = (const char*)sqlite3_column_text(stmt, TailInstance);
  op.obj.head_placement_rule.name = (const char*)sqlite3_column_text(stmt, HeadPlacementRuleName);
  op.obj.head_placement_rule.storage_class = (const char*)sqlite3_column_text(stmt, HeadPlacementRuleStorageClass);
  op.obj.tail_placement.placement_rule.name = (const char*)sqlite3_column_text(stmt, TailPlacementRuleName);
  op.obj.tail_placement.placement_rule.storage_class = (const char*)sqlite3_column_text(stmt, TailPlacementStorageClass);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ManifestPartObjs, op.obj.objs, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ManifestPartRules, op.obj.rules, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, Omap, op.obj.omap, sdb);
  op.obj.is_multipart = sqlite3_column_int(stmt, IsMultipart);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, MPPartsList, op.obj.mp_parts, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, HeadData, op.obj.head_data, sdb);
  op.obj.state.data = op.obj.head_data;

  rgw_bucket_dir_entry dent;
  dent.key.name = op.obj.state.obj.key.name;
  dent.key.instance = op.obj.state.obj.key.instance;
  dent.tag = op.obj.tag;
  dent.flags = op.obj.flags;
  dent.versioned_epoch = op.obj.versioned_epoch;
  dent.index_ver = op.obj.index_ver;
  dent.exists = true;
  dent.meta.category = op.obj.category;
  dent.meta.size = op.obj.state.size;
  dent.meta.accounted_size = op.obj.state.accounted_size;
  dent.meta.mtime = op.obj.state.mtime;
  dent.meta.etag = op.obj.etag;
  dent.meta.owner = op.obj.owner;
  dent.meta.owner_display_name = op.obj.owner_display_name;
  dent.meta.content_type = op.obj.content_type;
  dent.meta.storage_class = op.obj.storage_class;
  dent.meta.appendable = op.obj.appendable;

  op.obj.list_entries.push_back(dent);
  return 0;
}

static int get_objectdata(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.obj.state.obj.key.name = (const char*)sqlite3_column_text(stmt, ObjName);
  op.bucket.info.bucket.name = (const char*)sqlite3_column_text(stmt, ObjBucketName);
  op.obj.state.obj.key.instance = (const char*)sqlite3_column_text(stmt, ObjInstance);
  op.obj.state.obj.key.ns = (const char*)sqlite3_column_text(stmt, ObjNS);
  op.obj.obj_id = (const char*)sqlite3_column_text(stmt, ObjDataID);
  op.obj_data.part_num = sqlite3_column_int(stmt, PartNum);
  op.obj_data.offset = sqlite3_column_int(stmt, Offset);
  op.obj_data.size = sqlite3_column_int(stmt, ObjDataSize);
  op.obj_data.multipart_part_str = (const char*)sqlite3_column_text(stmt, MultipartPartStr);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjDataMtime, op.obj.state.mtime, sdb);
  SQL_DECODE_BLOB_PARAM(dpp, stmt, ObjData, op.obj_data.data, sdb);

  return 0;
}

static int list_lc_entry(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.lc_entry.index = (const char*)sqlite3_column_text(stmt, LCEntryIndex);
  op.lc_entry.entry.bucket = (const char*)sqlite3_column_text(stmt, LCEntryBucketName);
  op.lc_entry.entry.start_time = sqlite3_column_int(stmt, LCEntryStartTime);
  op.lc_entry.entry.status = sqlite3_column_int(stmt, LCEntryStatus);
 
  op.lc_entry.list_entries.push_back(op.lc_entry.entry);

  return 0;
}

static int list_lc_head(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  int64_t start_date;

  op.lc_head.index = (const char*)sqlite3_column_text(stmt, LCHeadIndex);
  op.lc_head.head.marker = (const char*)sqlite3_column_text(stmt, LCHeadMarker);
 
  SQL_DECODE_BLOB_PARAM(dpp, stmt, LCHeadStartDate, start_date, sdb);
  op.lc_head.head.start_date = start_date;

  return 0;
}

int SQLiteDB::InitializeDBOps(const DoutPrefixProvider *dpp)
{
  (void)createTables(dpp);
  dbops.InsertAccount = make_shared<SQLInsertAccount>(&this->db, this->getDBname(), cct);
  dbops.RemoveAccount = make_shared<SQLRemoveAccount>(&this->db, this->getDBname(), cct);
  dbops.GetAccount = make_shared<SQLGetAccount>(&this->db, this->getDBname(), cct);
  dbops.WriteBucketStats = make_shared<SQLWriteBucketStats>(&this->db, this->getDBname(), cct);
  dbops.ReadOwnerStats = make_shared<SQLReadOwnerStats>(&this->db, this->getDBname(), cct);
  dbops.DeleteBucketStats = make_shared<SQLDeleteBucketStats>(&this->db, this->getDBname(), cct);
  dbops.InsertRole = make_shared<SQLInsertRole>(&this->db, this->getDBname(), cct);
  dbops.RemoveRole = make_shared<SQLRemoveRole>(&this->db, this->getDBname(), cct);
  dbops.GetRole = make_shared<SQLGetRole>(&this->db, this->getDBname(), cct);
  dbops.ListRoles = make_shared<SQLListRoles>(&this->db, this->getDBname(), cct);
  dbops.InsertOIDCProvider = make_shared<SQLInsertOIDCProvider>(&this->db, this->getDBname(), cct);
  dbops.RemoveOIDCProvider = make_shared<SQLRemoveOIDCProvider>(&this->db, this->getDBname(), cct);
  dbops.GetOIDCProvider = make_shared<SQLGetOIDCProvider>(&this->db, this->getDBname(), cct);
  dbops.ListOIDCProviders = make_shared<SQLListOIDCProviders>(&this->db, this->getDBname(), cct);
  dbops.InsertTopic = make_shared<SQLInsertTopic>(&this->db, this->getDBname(), cct);
  dbops.RemoveTopic = make_shared<SQLRemoveTopic>(&this->db, this->getDBname(), cct);
  dbops.GetTopic = make_shared<SQLGetTopic>(&this->db, this->getDBname(), cct);
  dbops.ListTopics = make_shared<SQLListTopics>(&this->db, this->getDBname(), cct);
  dbops.InsertBucketTopicMapping = make_shared<SQLInsertBucketTopicMapping>(&this->db, this->getDBname(), cct);
  dbops.RemoveBucketTopicMapping = make_shared<SQLRemoveBucketTopicMapping>(&this->db, this->getDBname(), cct);
  dbops.GetBucketTopicMapping = make_shared<SQLGetBucketTopicMapping>(&this->db, this->getDBname(), cct);
  dbops.RemoveBucketFromTopicMappings = make_shared<SQLRemoveBucketFromTopicMappings>(&this->db, this->getDBname(), cct);
  dbops.InsertGroup = make_shared<SQLInsertGroup>(&this->db, this->getDBname(), cct);
  dbops.RemoveGroup = make_shared<SQLRemoveGroup>(&this->db, this->getDBname(), cct);
  dbops.GetGroup = make_shared<SQLGetGroup>(&this->db, this->getDBname(), cct);
  dbops.ListGroups = make_shared<SQLListGroups>(&this->db, this->getDBname(), cct);
  dbops.InsertGroupUser = make_shared<SQLInsertGroupUser>(&this->db, this->getDBname(), cct);
  dbops.RemoveGroupUser = make_shared<SQLRemoveGroupUser>(&this->db, this->getDBname(), cct);
  dbops.ListGroupUsers = make_shared<SQLListGroupUsers>(&this->db, this->getDBname(), cct);
  dbops.RemoveUserGroups = make_shared<SQLRemoveUserGroups>(&this->db, this->getDBname(), cct);
  dbops.ListUserGroups = make_shared<SQLListUserGroups>(&this->db, this->getDBname(), cct);
  dbops.InsertAccessKey = make_shared<SQLInsertAccessKey>(&this->db, this->getDBname(), cct);
  dbops.RemoveAccessKey = make_shared<SQLRemoveAccessKey>(&this->db, this->getDBname(), cct);
  dbops.RemoveUserAccessKeys = make_shared<SQLRemoveUserAccessKeys>(&this->db, this->getDBname(), cct);
  dbops.GetAccountUser = make_shared<SQLGetAccountUser>(&this->db, this->getDBname(), cct);
  dbops.ListAccountUsers = make_shared<SQLListAccountUsers>(&this->db, this->getDBname(), cct);
  dbops.InsertUser = make_shared<SQLInsertUser>(&this->db, this->getDBname(), cct);
  dbops.RemoveUser = make_shared<SQLRemoveUser>(&this->db, this->getDBname(), cct);
  dbops.GetUser = make_shared<SQLGetUser>(&this->db, this->getDBname(), cct);
  dbops.ListUsers = make_shared<SQLListUsers>(&this->db, this->getDBname(), cct);
  dbops.InsertBucket = make_shared<SQLInsertBucket>(&this->db, this->getDBname(), cct);
  dbops.UpdateBucket = make_shared<SQLUpdateBucket>(&this->db, this->getDBname(), cct);
  dbops.RemoveBucket = make_shared<SQLRemoveBucket>(&this->db, this->getDBname(), cct);
  dbops.GetBucket = make_shared<SQLGetBucket>(&this->db, this->getDBname(), cct);
  dbops.ListUserBuckets = make_shared<SQLListUserBuckets>(&this->db, this->getDBname(), cct);
  dbops.InsertLCEntry = make_shared<SQLInsertLCEntry>(&this->db, this->getDBname(), cct);
  dbops.RemoveLCEntry = make_shared<SQLRemoveLCEntry>(&this->db, this->getDBname(), cct);
  dbops.GetLCEntry = make_shared<SQLGetLCEntry>(&this->db, this->getDBname(), cct);
  dbops.ListLCEntries = make_shared<SQLListLCEntries>(&this->db, this->getDBname(), cct);
  dbops.InsertLCHead = make_shared<SQLInsertLCHead>(&this->db, this->getDBname(), cct);
  dbops.RemoveLCHead = make_shared<SQLRemoveLCHead>(&this->db, this->getDBname(), cct);
  dbops.GetLCHead = make_shared<SQLGetLCHead>(&this->db, this->getDBname(), cct);

  return 0;
}

void *SQLiteDB::openDB(const DoutPrefixProvider *dpp)
{
  string dbname;
  int rc = 0;

  dbname = getDBfile();
  if (dbname.empty()) {
    ldpp_dout(dpp, 0)<<"dbname is NULL" << dendl;
    goto out;
  }

  rc = sqlite3_open_v2(dbname.c_str(), (sqlite3**)&db,
      SQLITE_OPEN_READWRITE |
      SQLITE_OPEN_CREATE |
      SQLITE_OPEN_FULLMUTEX,
      NULL);

  if (rc) {
    ldpp_dout(dpp, 0) <<"Cant open "<<dbname<<"; Errmsg - "\
      <<sqlite3_errmsg((sqlite3*)db) <<  dendl;
  } else {
    ldpp_dout(dpp, 0) <<"Opened database("<<dbname<<") successfully" <<  dendl;
  }

  exec(dpp, "PRAGMA foreign_keys=ON", NULL);

out:
  return db;
}

int SQLiteDB::closeDB(const DoutPrefixProvider *dpp)
{
  if (db)
    sqlite3_close((sqlite3 *)db);

  db = NULL;

  return 0;
}

int SQLiteDB::Reset(const DoutPrefixProvider *dpp, sqlite3_stmt *stmt)
{
  int ret = -1;

  if (!stmt) {
    return -1;
  }
  sqlite3_clear_bindings(stmt);
  ret = sqlite3_reset(stmt);

  return ret;
}

int SQLiteDB::Step(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt,
    int (*cbk)(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt))
{
  int ret = -1;

  if (!stmt) {
    return -1;
  }

again:
  ret = sqlite3_step(stmt);

  if ((ret != SQLITE_DONE) && (ret != SQLITE_ROW)) {
    ldpp_dout(dpp, 0)<<"sqlite step failed for stmt("<<stmt \
      <<"); Errmsg - "<<sqlite3_errmsg((sqlite3*)db) << dendl;
    return -1;
  } else if (ret == SQLITE_ROW) {
    if (cbk) {
      (*cbk)(dpp, op, stmt);
    } else {
    }
    goto again;
  }

  ldpp_dout(dpp, 20)<<"sqlite step successfully executed for stmt(" \
    <<stmt<<")  ret = " << ret << dendl;

  return 0;
}

int SQLiteDB::exec(const DoutPrefixProvider *dpp, const char *schema,
    int (*callback)(void*,int,char**,char**))
{
  int ret = -1;
  char *errmsg = NULL;

  if (!db)
    goto out;

  ret = sqlite3_exec((sqlite3*)db, schema, callback, 0, &errmsg);
  if (ret != SQLITE_OK) {
    ldpp_dout(dpp, 0) <<"sqlite exec failed for schema("<<schema \
      <<"); Errmsg - "<<errmsg <<  dendl;
    sqlite3_free(errmsg);
    goto out;
  }
  ret = 0;
  ldpp_dout(dpp, 10) <<"sqlite exec successfully processed for schema(" \
    <<schema<<")" <<  dendl;
out:
  return ret;
}

int SQLiteDB::createTables(const DoutPrefixProvider *dpp)
{
  int ret = -1;
  int ca = 0, cr = 0, co = 0, cg = 0, cgu = 0, cu = 0, cb = 0, cq = 0;
  DBOpParams params = {};

  params.account_table = getAccountTable();
  params.role_table = getRoleTable();
  params.oidc_table = getOIDCTable();
  params.group_table = getGroupTable();
  params.group_users_table = getGroupUsersTable();
  params.user_table = getUserTable();
  params.bucket_table = getBucketTable();
  params.quota_table = getQuotaTable();

  if ((ca = createAccountTable(dpp, &params)))
    goto out;

  if ((cr = createRoleTable(dpp, &params)))
    goto out;

  if ((co = createOIDCProviderTable(dpp, &params)))
    goto out;

  if ((cg = createGroupTable(dpp, &params)))
    goto out;

  if ((cgu = createGroupUsersTable(dpp, &params)))
    goto out;

  if ((cu = createUserTable(dpp, &params)))
    goto out;

  if (createAccessKeysTable(dpp, &params))
    goto out;

  params.topic_table = getTopicTable();
  params.bucket_topic_mapping_table = getBucketTopicMappingTable();

  if (createTopicTable(dpp, &params))
    goto out;

  if (createBucketTopicMappingTable(dpp, &params))
    goto out;

  if ((cb = createBucketTable(dpp, &params)))
    goto out;

  if ((cq = createQuotaTable(dpp, &params)))
    goto out;

  params.bucket_stats_table = getBucketStatsTable();
  if (createBucketStatsTable(dpp, &params))
    goto out;

  ret = 0;
out:
  if (ret) {
    if (ca)
      DeleteAccountTable(dpp, &params);
    if (cu)
      DeleteUserTable(dpp, &params);
    if (cb)
      DeleteBucketTable(dpp, &params);
    if (cq)
      DeleteQuotaTable(dpp, &params);
    ldpp_dout(dpp, 0)<<"Creation of tables failed" << dendl;
  }

  return ret;
}

int SQLiteDB::createAccountTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Account", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateAccountTable failed" << dendl;

  ldpp_dout(dpp, 20)<<"CreateAccountTable succeeded" << dendl;

  return ret;
}

int SQLiteDB::createRoleTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Role", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateRoleTable failed" << dendl;

  ldpp_dout(dpp, 20)<<"CreateRoleTable succeeded" << dendl;

  return ret;
}

int SQLiteDB::createOIDCProviderTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("OIDCProvider", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateOIDCProviderTable failed" << dendl;

  ldpp_dout(dpp, 20)<<"CreateOIDCProviderTable succeeded" << dendl;

  return ret;
}

int SQLiteDB::createGroupTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Group", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateGroupTable failed" << dendl;

  ldpp_dout(dpp, 20)<<"CreateGroupTable succeeded" << dendl;

  return ret;
}

int SQLiteDB::createGroupUsersTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("GroupUsers", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateGroupUsersTable failed" << dendl;

  ldpp_dout(dpp, 20)<<"CreateGroupUsersTable succeeded" << dendl;

  return ret;
}

int SQLiteDB::createUserTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("User", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateUserTable failed" << dendl;

  ldpp_dout(dpp, 20)<<"CreateUserTable succeeded" << dendl;

  return ret;
}

int SQLiteDB::createAccessKeysTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("AccessKeys", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateAccessKeysTable failed" << dendl;

  ldpp_dout(dpp, 20)<<"CreateAccessKeysTable succeeded" << dendl;

  return ret;
}

int SQLiteDB::createTopicTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Topic", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateTopicTable failed" << dendl;

  ldpp_dout(dpp, 20)<<"CreateTopicTable succeeded" << dendl;

  return ret;
}

int SQLiteDB::createBucketTopicMappingTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("BucketTopicMapping", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateBucketTopicMappingTable failed" << dendl;

  ldpp_dout(dpp, 20)<<"CreateBucketTopicMappingTable succeeded" << dendl;

  return ret;
}

int SQLiteDB::createBucketTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Bucket", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateBucketTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"CreateBucketTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::createObjectTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Object", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateObjectTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"CreateObjectTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::createObjectTableTrigger(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("ObjectTrigger", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateObjectTableTrigger failed " << dendl;

  ldpp_dout(dpp, 20)<<"CreateObjectTableTrigger succeeded " << dendl;

  return ret;
}

int SQLiteDB::createObjectView(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("ObjectView", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateObjectView failed " << dendl;

  ldpp_dout(dpp, 20)<<"CreateObjectView succeeded " << dendl;

  return ret;
}

int SQLiteDB::createQuotaTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Quota", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateQuotaTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"CreateQuotaTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::createBucketStatsTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("BucketStats", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret) {
    ldpp_dout(dpp, 0)<<"CreateBucketStatsTable failed " << dendl;
    return ret;
  }

  /* create index on OwnerID for aggregate queries */
  string idx = fmt::format(
    "CREATE INDEX IF NOT EXISTS idx_bucket_stats_owner ON '{}' (OwnerID);",
    params->bucket_stats_table);
  (void)exec(dpp, idx.c_str(), NULL);

  ldpp_dout(dpp, 20)<<"CreateBucketStatsTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::createObjectDataTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("ObjectData", params);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"CreateObjectDataTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"CreateObjectDataTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::createLCTables(const DoutPrefixProvider *dpp)
{
  int ret = -1;
  string schema;
  DBOpParams params = {};

  params.lc_entry_table = getLCEntryTable();
  params.lc_head_table = getLCHeadTable();
  params.bucket_table = getBucketTable();

  schema = CreateTableSchema("LCEntry", &params);
  ret = exec(dpp, schema.c_str(), NULL);
  if (ret) {
    ldpp_dout(dpp, 0)<<"CreateLCEntryTable failed" << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20)<<"CreateLCEntryTable succeeded" << dendl;

  schema = CreateTableSchema("LCHead", &params);
  ret = exec(dpp, schema.c_str(), NULL);
  if (ret) {
    ldpp_dout(dpp, 0)<<"CreateLCHeadTable failed" << dendl;
    (void)DeleteLCEntryTable(dpp, &params);
  }
  ldpp_dout(dpp, 20)<<"CreateLCHeadTable succeeded" << dendl;

  return ret;
}

int SQLiteDB::DeleteAccountTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->account_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteAccountTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeleteAccountTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteUserTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->user_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteUserTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeleteUserTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteBucketTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->bucket_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeletebucketTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeletebucketTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteObjectTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->object_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteObjectTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeleteObjectTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteObjectDataTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->objectdata_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteObjectDataTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeleteObjectDataTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteQuotaTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->quota_table);

  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteQuotaTable failed " << dendl;

  ldpp_dout(dpp, 20)<<"DeleteQuotaTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteLCEntryTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->lc_entry_table);
  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteLCEntryTable failed " << dendl;
  ldpp_dout(dpp, 20)<<"DeleteLCEntryTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::DeleteLCHeadTable(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->lc_head_table);
  ret = exec(dpp, schema.c_str(), NULL);
  if (ret)
    ldpp_dout(dpp, 0)<<"DeleteLCHeadTable failed " << dendl;
  ldpp_dout(dpp, 20)<<"DeleteLCHeadTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::ListAllUsers(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = ListTableSchema(params->user_table);
  ret = exec(dpp, schema.c_str(), &list_callback);
  if (ret)
    ldpp_dout(dpp, 0)<<"GetUsertable failed " << dendl;

  ldpp_dout(dpp, 20)<<"GetUserTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::ListAllBuckets(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = ListTableSchema(params->bucket_table);

  ret = exec(dpp, schema.c_str(), &list_callback);
  if (ret)
    ldpp_dout(dpp, 0)<<"Listbuckettable failed " << dendl;

  ldpp_dout(dpp, 20)<<"ListbucketTable succeeded " << dendl;

  return ret;
}

int SQLiteDB::ListAllObjects(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;
  string schema;
  map<string, class ObjectOp*>::iterator iter;
  map<string, class ObjectOp*> objectmap;
  string bucket;

  objectmap = getObjectMap();

  if (objectmap.empty())
    ldpp_dout(dpp, 20)<<"objectmap empty " << dendl;

  for (iter = objectmap.begin(); iter != objectmap.end(); ++iter) {
    bucket = iter->first;
    params->object_table = getObjectTable(bucket);
    schema = ListTableSchema(params->object_table);

    ret = exec(dpp, schema.c_str(), &list_callback);
    if (ret)
      ldpp_dout(dpp, 0)<<"ListObjecttable failed " << dendl;

    ldpp_dout(dpp, 20)<<"ListObjectTable succeeded " << dendl;
  }

  return ret;
}

int SQLObjectOp::InitializeObjectOps(string db_name, const DoutPrefixProvider *dpp)
{
  PutObject = make_shared<SQLPutObject>(sdb, db_name, cct);
  DeleteObject = make_shared<SQLDeleteObject>(sdb, db_name, cct);
  GetObject = make_shared<SQLGetObject>(sdb, db_name, cct);
  UpdateObject = make_shared<SQLUpdateObject>(sdb, db_name, cct);
  ListBucketObjects = make_shared<SQLListBucketObjects>(sdb, db_name, cct);
  ListVersionedObjects = make_shared<SQLListVersionedObjects>(sdb, db_name, cct);
  PutObjectData = make_shared<SQLPutObjectData>(sdb, db_name, cct);
  UpdateObjectData = make_shared<SQLUpdateObjectData>(sdb, db_name, cct);
  GetObjectData = make_shared<SQLGetObjectData>(sdb, db_name, cct);
  DeleteObjectData = make_shared<SQLDeleteObjectData>(sdb, db_name, cct);
  DeleteStaleObjectData = make_shared<SQLDeleteStaleObjectData>(sdb, db_name, cct);

  return 0;
}

int SQLInsertAccount::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertAccount - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertAccount");
out:
  return ret;
}

int SQLInsertAccount::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.account_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.account.info.id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.tenant, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.account.info.tenant.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.account_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.account.info.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.email, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.account.info.email.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.quota, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.account.info.quota, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.bucket_quota, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.account.info.bucket_quota, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.max_users, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.account.info.max_users, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.max_roles, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.account.info.max_roles, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.max_groups, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.account.info.max_groups, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.max_buckets, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.account.info.max_buckets, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.max_access_keys, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.account.info.max_access_keys, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.account_attrs, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.account.account_attrs, sdb);

out:
  return rc;
}

int SQLInsertAccount::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveAccount::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveAccount - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveAccount");
out:
  return ret;
}

int SQLRemoveAccount::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.account_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.account.info.id.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveAccount::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetAccount::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetAccount - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  if (params->op.query_str == "name") { 
    SQL_PREPARE(dpp, p_params, sdb, name_stmt, ret, "PrepareGetAccount");
  } else if (params->op.query_str == "email") { 
    SQL_PREPARE(dpp, p_params, sdb, email_stmt, ret, "PrepareGetAccount");
  } else { // by default by account_id
    SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetAccount");
  }
out:
  return ret;
}

int SQLGetAccount::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.query_str == "name") { 
    SQL_BIND_INDEX(dpp, name_stmt, index, p_params.op.account.account_name, sdb);
    SQL_BIND_TEXT(dpp, name_stmt, index, params->op.account.info.name.c_str(), sdb);
  } else if (params->op.query_str == "email") { 
    SQL_BIND_INDEX(dpp, email_stmt, index, p_params.op.account.email, sdb);
    SQL_BIND_TEXT(dpp, email_stmt, index, params->op.account.info.email.c_str(), sdb);
  } else { // by default by account_id
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.account.account_id, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.account.info.id.c_str(), sdb);
  }

out:
  return rc;
}

int SQLGetAccount::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  if (params->op.query_str == "name") { 
    SQL_EXECUTE(dpp, params, name_stmt, list_account);
  } else if (params->op.query_str == "email") { 
    SQL_EXECUTE(dpp, params, email_stmt, list_account);
  } else { // by default by account_id
    SQL_EXECUTE(dpp, params, stmt, list_account);
  }

out:
  return ret;
}

/* --- BucketStats ops --- */

static int list_owner_stats(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  op.bucket_stats.stats.size = sqlite3_column_int64(stmt, 0);
  op.bucket_stats.stats.size_rounded = sqlite3_column_int64(stmt, 1);
  op.bucket_stats.stats.num_objects = sqlite3_column_int64(stmt, 2);

  return 0;
}

int SQLWriteBucketStats::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLWriteBucketStats - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareWriteBucketStats");
out:
  return ret;
}

int SQLWriteBucketStats::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket_stats.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket_stats.bucket_name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket_stats.owner_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket_stats.owner_id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket_stats.size, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket_stats.size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket_stats.size_rounded, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket_stats.size_rounded, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket_stats.num_objects, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket_stats.num_objects, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket_stats.last_updated, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket_stats.last_updated, sdb);

out:
  return rc;
}

int SQLWriteBucketStats::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLReadOwnerStats::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLReadOwnerStats - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareReadOwnerStats");
out:
  return ret;
}

int SQLReadOwnerStats::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket_stats.owner_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket_stats.owner_id.c_str(), sdb);

out:
  return rc;
}

int SQLReadOwnerStats::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_owner_stats);
out:
  return ret;
}

int SQLDeleteBucketStats::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLDeleteBucketStats - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareDeleteBucketStats");
out:
  return ret;
}

int SQLDeleteBucketStats::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket_stats.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket_stats.bucket_name.c_str(), sdb);

out:
  return rc;
}

int SQLDeleteBucketStats::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

/* --- Role ops --- */

int SQLInsertRole::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertRole - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertRole");
out:
  return ret;
}

int SQLInsertRole::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.role_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.tenant, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.tenant.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.account_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.account_id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.path, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.path.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.arn, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.arn.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.trust_policy, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.trust_policy.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.perm_policies, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.role.info.perm_policy_map, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.managed_policies, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.role.info.managed_policies, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.tags, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.role.info.tags, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.max_session_duration, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.role.info.max_session_duration, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.description, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.description.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.creation_date, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.creation_date.c_str(), sdb);

out:
  return rc;
}

int SQLInsertRole::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveRole::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveRole - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveRole");
out:
  return ret;
}

int SQLRemoveRole::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.role_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.id.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveRole::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetRole::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetRole - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  if (params->op.query_str == "name") {
    SQL_PREPARE(dpp, p_params, sdb, name_stmt, ret, "PrepareGetRole");
  } else if (params->op.query_str == "name_account") {
    SQL_PREPARE(dpp, p_params, sdb, name_account_stmt, ret, "PrepareGetRole");
  } else {
    SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetRole");
  }
out:
  return ret;
}

int SQLGetRole::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.query_str == "name") {
    SQL_BIND_INDEX(dpp, name_stmt, index, p_params.op.role.name, sdb);
    SQL_BIND_TEXT(dpp, name_stmt, index, params->op.role.info.name.c_str(), sdb);
    SQL_BIND_INDEX(dpp, name_stmt, index, p_params.op.role.tenant, sdb);
    SQL_BIND_TEXT(dpp, name_stmt, index, params->op.role.info.tenant.c_str(), sdb);
  } else if (params->op.query_str == "name_account") {
    SQL_BIND_INDEX(dpp, name_account_stmt, index, p_params.op.role.name, sdb);
    SQL_BIND_TEXT(dpp, name_account_stmt, index, params->op.role.info.name.c_str(), sdb);
    SQL_BIND_INDEX(dpp, name_account_stmt, index, p_params.op.role.account_id, sdb);
    SQL_BIND_TEXT(dpp, name_account_stmt, index, params->op.role.info.account_id.c_str(), sdb);
  } else {
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.role_id, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.id.c_str(), sdb);
  }

out:
  return rc;
}

int SQLGetRole::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  if (params->op.query_str == "name") {
    SQL_EXECUTE(dpp, params, name_stmt, list_role);
  } else if (params->op.query_str == "name_account") {
    SQL_EXECUTE(dpp, params, name_account_stmt, list_role);
  } else {
    SQL_EXECUTE(dpp, params, stmt, list_role);
  }

out:
  return ret;
}

int SQLListRoles::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLListRoles - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  if (params->op.query_str == "account") {
    SQL_PREPARE(dpp, p_params, sdb, account_stmt, ret, "PrepareListRoles");
  } else if (params->op.query_str == "count_account") {
    SQL_PREPARE(dpp, p_params, sdb, count_stmt, ret, "PrepareListRoles");
  } else {
    SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListRoles");
  }
out:
  return ret;
}

int SQLListRoles::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.query_str == "account") {
    SQL_BIND_INDEX(dpp, account_stmt, index, p_params.op.role.account_id, sdb);
    SQL_BIND_TEXT(dpp, account_stmt, index, params->op.role.info.account_id.c_str(), sdb);
    SQL_BIND_INDEX(dpp, account_stmt, index, p_params.op.role.path, sdb);
    SQL_BIND_TEXT(dpp, account_stmt, index, params->op.role.info.path.c_str(), sdb);
    SQL_BIND_INDEX(dpp, account_stmt, index, p_params.op.role.name, sdb);
    SQL_BIND_TEXT(dpp, account_stmt, index, params->op.role.info.name.c_str(), sdb);
    SQL_BIND_INDEX(dpp, account_stmt, index, p_params.op.list_max_count, sdb);
    SQL_BIND_INT(dpp, account_stmt, index, params->op.list_max_count, sdb);
  } else if (params->op.query_str == "count_account") {
    SQL_BIND_INDEX(dpp, count_stmt, index, p_params.op.role.account_id, sdb);
    SQL_BIND_TEXT(dpp, count_stmt, index, params->op.role.info.account_id.c_str(), sdb);
  } else {
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.tenant, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.tenant.c_str(), sdb);
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.path, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.path.c_str(), sdb);
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.role.name, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.role.info.name.c_str(), sdb);
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
    SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);
  }

out:
  return rc;
}

int SQLListRoles::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  if (params->op.query_str == "account") {
    SQL_EXECUTE(dpp, params, account_stmt, list_role);
  } else if (params->op.query_str == "count_account") {
    SQL_EXECUTE(dpp, params, count_stmt, list_role_count);
  } else {
    SQL_EXECUTE(dpp, params, stmt, list_role);
  }

out:
  return ret;
}

int SQLInsertOIDCProvider::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertOIDCProvider - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertOIDCProvider");
out:
  return ret;
}

int SQLInsertOIDCProvider::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.provider_url, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.oidc.info.provider_url.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.tenant, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.oidc.info.tenant.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.client_ids, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.oidc.info.client_ids, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.thumbprints, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.oidc.info.thumbprints, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.provider_arn, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.oidc.info.arn.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.creation_date, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.oidc.info.creation_date.c_str(), sdb);

out:
  return rc;
}

int SQLInsertOIDCProvider::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveOIDCProvider::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveOIDCProvider - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveOIDCProvider");
out:
  return ret;
}

int SQLRemoveOIDCProvider::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.provider_url, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.oidc.info.provider_url.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.tenant, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.oidc.info.tenant.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveOIDCProvider::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetOIDCProvider::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetOIDCProvider - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetOIDCProvider");
out:
  return ret;
}

int SQLGetOIDCProvider::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.provider_url, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.oidc.info.provider_url.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.tenant, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.oidc.info.tenant.c_str(), sdb);

out:
  return rc;
}

int SQLGetOIDCProvider::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_oidc_provider);
out:
  return ret;
}

int SQLListOIDCProviders::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLListOIDCProviders - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListOIDCProviders");
out:
  return ret;
}

int SQLListOIDCProviders::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.oidc.tenant, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.oidc.info.tenant.c_str(), sdb);

out:
  return rc;
}

int SQLListOIDCProviders::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_oidc_provider);
out:
  return ret;
}

/* --- Topic operations --- */

int SQLInsertTopic::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLInsertTopic - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertTopic");
out:
  return ret;
}

int SQLInsertTopic::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.topic_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.tenant, sdb);
  {
    /* extract tenant from owner variant */
    std::string tenant;
    if (auto* u = std::get_if<rgw_user>(&params->op.topic.topic.owner)) {
      tenant = u->tenant;
    } else if (auto* a = std::get_if<rgw_account_id>(&params->op.topic.topic.owner)) {
      tenant = *a;
    }
    SQL_BIND_TEXT(dpp, stmt, index, tenant.c_str(), sdb);
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.owner, sdb);
  {
    bufferlist owner_bl;
    ceph::converted_variant::encode(params->op.topic.topic.owner, owner_bl);
    SQL_BIND_BLOB(dpp, stmt, index, owner_bl.c_str(), owner_bl.length(), sdb);
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.arn, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic.arn.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.dest, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.topic.topic.dest, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.opaque_data, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic.opaque_data.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.policy_text, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic.policy_text.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.topic.topic_version.ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic_version.tag.c_str(), sdb);

out:
  return rc;
}

int SQLInsertTopic::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_topic);
out:
  return ret;
}

int SQLRemoveTopic::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLRemoveTopic - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveTopic");
out:
  return ret;
}

int SQLRemoveTopic::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.topic_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.tenant, sdb);
  {
    std::string tenant;
    if (auto* u = std::get_if<rgw_user>(&params->op.topic.topic.owner)) {
      tenant = u->tenant;
    } else if (auto* a = std::get_if<rgw_account_id>(&params->op.topic.topic.owner)) {
      tenant = *a;
    }
    SQL_BIND_TEXT(dpp, stmt, index, tenant.c_str(), sdb);
  }

out:
  return rc;
}

int SQLRemoveTopic::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_topic);
out:
  return ret;
}

int SQLGetTopic::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLGetTopic - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetTopic");
out:
  return ret;
}

int SQLGetTopic::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.topic_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.tenant, sdb);
  {
    std::string tenant;
    if (auto* u = std::get_if<rgw_user>(&params->op.topic.topic.owner)) {
      tenant = u->tenant;
    } else if (auto* a = std::get_if<rgw_account_id>(&params->op.topic.topic.owner)) {
      tenant = *a;
    }
    SQL_BIND_TEXT(dpp, stmt, index, tenant.c_str(), sdb);
  }

out:
  return rc;
}

int SQLGetTopic::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_topic);
out:
  return ret;
}

int SQLListTopics::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLListTopics - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);

  if (params->op.query_str == "owner") {
    SQL_PREPARE(dpp, p_params, sdb, owner_stmt, ret, "PrepareListTopicsByOwner");
  } else {
    SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListTopics");
  }
out:
  return ret;
}

int SQLListTopics::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.query_str == "owner") {
    SQL_BIND_INDEX(dpp, owner_stmt, index, p_params.op.topic.owner, sdb);
    {
      bufferlist owner_bl;
      ceph::converted_variant::encode(params->op.topic.topic.owner, owner_bl);
      SQL_BIND_BLOB(dpp, owner_stmt, index, owner_bl.c_str(), owner_bl.length(), sdb);
    }

    SQL_BIND_INDEX(dpp, owner_stmt, index, p_params.op.topic.topic_name, sdb);
    SQL_BIND_TEXT(dpp, owner_stmt, index, params->op.topic.topic.name.c_str(), sdb);

    SQL_BIND_INDEX(dpp, owner_stmt, index, p_params.op.list_max_count, sdb);
    SQL_BIND_INT(dpp, owner_stmt, index, params->op.list_max_count, sdb);
  } else {
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.tenant, sdb);
    {
      std::string tenant;
      if (auto* u = std::get_if<rgw_user>(&params->op.topic.topic.owner)) {
        tenant = u->tenant;
      }
      SQL_BIND_TEXT(dpp, stmt, index, tenant.c_str(), sdb);
    }

    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.topic_name, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic.name.c_str(), sdb);

    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
    SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);
  }

out:
  return rc;
}

int SQLListTopics::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  if (params->op.query_str == "owner") {
    SQL_EXECUTE(dpp, params, owner_stmt, list_topic);
  } else {
    SQL_EXECUTE(dpp, params, stmt, list_topic);
  }
out:
  return ret;
}

/* --- Bucket-Topic Mapping operations --- */

int SQLInsertBucketTopicMapping::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLInsertBucketTopicMapping - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertBucketTopicMapping");
out:
  return ret;
}

int SQLInsertBucketTopicMapping::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.topic_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.bucket_name.c_str(), sdb);

out:
  return rc;
}

int SQLInsertBucketTopicMapping::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_bucket_topic_mapping);
out:
  return ret;
}

int SQLRemoveBucketTopicMapping::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLRemoveBucketTopicMapping - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveBucketTopicMapping");
out:
  return ret;
}

int SQLRemoveBucketTopicMapping::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.topic_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.bucket_name.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveBucketTopicMapping::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_bucket_topic_mapping);
out:
  return ret;
}

int SQLGetBucketTopicMapping::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLGetBucketTopicMapping - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetBucketTopicMapping");
out:
  return ret;
}

int SQLGetBucketTopicMapping::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.topic_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.topic.name.c_str(), sdb);

out:
  return rc;
}

int SQLGetBucketTopicMapping::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_bucket_topic_mapping);
out:
  return ret;
}

int SQLRemoveBucketFromTopicMappings::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLRemoveBucketFromTopicMappings - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveBucketFromTopicMappings");
out:
  return ret;
}

int SQLRemoveBucketFromTopicMappings::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.topic.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.topic.bucket_name.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveBucketFromTopicMappings::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_bucket_topic_mapping);
out:
  return ret;
}

int SQLInsertGroup::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLInsertGroup - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertGroup");
out:
  return ret;
}

int SQLInsertGroup::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.group_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.id.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.name.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.tenant, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.tenant.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.account_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.account_id.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.path, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.path.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.group_attrs, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.group.group_attrs, sdb);
out:
  return rc;
}

int SQLInsertGroup::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveGroup::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLRemoveGroup - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveGroup");
out:
  return ret;
}

int SQLRemoveGroup::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.group_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.id.c_str(), sdb);
out:
  return rc;
}

int SQLRemoveGroup::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetGroup::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLGetGroup - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  if (params->op.query_str == "name") {
    SQL_PREPARE(dpp, p_params, sdb, name_stmt, ret, "PrepareGetGroup");
  } else {
    SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetGroup");
  }
out:
  return ret;
}

int SQLGetGroup::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (params->op.query_str == "name") {
    SQL_BIND_INDEX(dpp, name_stmt, index, p_params.op.group.name, sdb);
    SQL_BIND_TEXT(dpp, name_stmt, index, params->op.group.info.name.c_str(), sdb);
    SQL_BIND_INDEX(dpp, name_stmt, index, p_params.op.group.account_id, sdb);
    SQL_BIND_TEXT(dpp, name_stmt, index, params->op.group.info.account_id.c_str(), sdb);
  } else {
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.group_id, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.id.c_str(), sdb);
  }
out:
  return rc;
}

int SQLGetGroup::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  if (params->op.query_str == "name") {
    SQL_EXECUTE(dpp, params, name_stmt, list_group);
  } else {
    SQL_EXECUTE(dpp, params, stmt, list_group);
  }
out:
  return ret;
}

int SQLListGroups::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLListGroups - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  if (params->op.query_str == "count_account") {
    SQL_PREPARE(dpp, p_params, sdb, count_stmt, ret, "PrepareListGroups");
  } else {
    SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListGroups");
  }
out:
  return ret;
}

int SQLListGroups::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (params->op.query_str == "count_account") {
    SQL_BIND_INDEX(dpp, count_stmt, index, p_params.op.group.account_id, sdb);
    SQL_BIND_TEXT(dpp, count_stmt, index, params->op.group.info.account_id.c_str(), sdb);
  } else {
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.account_id, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.account_id.c_str(), sdb);
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.path, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.path.c_str(), sdb);
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.name, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.name.c_str(), sdb);
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
    SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);
  }
out:
  return rc;
}

int SQLListGroups::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  if (params->op.query_str == "count_account") {
    SQL_EXECUTE(dpp, params, count_stmt, list_group_count);
  } else {
    SQL_EXECUTE(dpp, params, stmt, list_group);
  }
out:
  return ret;
}

int SQLInsertGroupUser::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLInsertGroupUser - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertGroupUser");
out:
  return ret;
}

int SQLInsertGroupUser::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.group_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.id.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.user_id.c_str(), sdb);
out:
  return rc;
}

int SQLInsertGroupUser::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveGroupUser::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLRemoveGroupUser - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveGroupUser");
out:
  return ret;
}

int SQLRemoveGroupUser::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.group_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.id.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.user_id.c_str(), sdb);
out:
  return rc;
}

int SQLRemoveGroupUser::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveUserGroups::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLRemoveUserGroups - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveUserGroups");
out:
  return ret;
}

int SQLRemoveUserGroups::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.user_id.c_str(), sdb);
out:
  return rc;
}

int SQLRemoveUserGroups::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLListUserGroups::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLListUserGroups - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListUserGroups");
out:
  return ret;
}

int SQLListUserGroups::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.user_id.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.name.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);
out:
  return rc;
}

int SQLListUserGroups::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, list_group);
out:
  return ret;
}

int SQLInsertAccessKey::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLInsertAccessKey - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertAccessKey");
out:
  return ret;
}

int SQLInsertAccessKey::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.access_key_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.access_keys.begin()->second.id.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);
out:
  return rc;
}

int SQLInsertAccessKey::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveAccessKey::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLRemoveAccessKey - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveAccessKey");
out:
  return ret;
}

int SQLRemoveAccessKey::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.access_key_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.access_keys.begin()->second.id.c_str(), sdb);
out:
  return rc;
}

int SQLRemoveAccessKey::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveUserAccessKeys::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLRemoveUserAccessKeys - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveUserAccessKeys");
out:
  return ret;
}

int SQLRemoveUserAccessKeys::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);
out:
  return rc;
}

int SQLRemoveUserAccessKeys::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLListGroupUsers::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLListGroupUsers - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListGroupUsers");
out:
  return ret;
}

int SQLListGroupUsers::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.group_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.info.id.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.group.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.group.user_id.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);
out:
  return rc;
}

int SQLListGroupUsers::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, list_group_user);
out:
  return ret;
}

int SQLGetAccountUser::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLGetAccountUser - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetAccountUser");
out:
  return ret;
}

int SQLGetAccountUser::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.account_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.account_id.c_str(), sdb);
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.display_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.display_name.c_str(), sdb);
out:
  return rc;
}

int SQLGetAccountUser::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  SQL_EXECUTE(dpp, params, stmt, list_user);
out:
  return ret;
}

int SQLListAccountUsers::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (!*sdb) { ldpp_dout(dpp, 0)<<"In SQLListAccountUsers - no db" << dendl; goto out; }
  InitPrepareParams(dpp, p_params, params);
  if (params->op.query_str == "count") {
    SQL_PREPARE(dpp, p_params, sdb, count_stmt, ret, "PrepareListAccountUsers");
  } else {
    SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListAccountUsers");
  }
out:
  return ret;
}

int SQLListAccountUsers::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  if (params->op.query_str == "count") {
    SQL_BIND_INDEX(dpp, count_stmt, index, p_params.op.user.account_id, sdb);
    SQL_BIND_TEXT(dpp, count_stmt, index, params->op.user.uinfo.account_id.c_str(), sdb);
  } else {
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.account_id, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.account_id.c_str(), sdb);
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.display_name, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.display_name.c_str(), sdb);
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
    SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);
  }
out:
  return rc;
}

int SQLListAccountUsers::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  if (params->op.query_str == "count") {
    SQL_EXECUTE(dpp, params, count_stmt, list_user_count);
  } else {
    SQL_EXECUTE(dpp, params, stmt, list_user);
  }
out:
  return ret;
}

int SQLInsertUser::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertUser - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertUser");
out:
  return ret;
}

int SQLInsertUser::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.tenant, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.tenant.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.ns, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.ns.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.display_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.display_name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_email, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_email.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.access_keys, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.access_keys, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.swift_keys, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.swift_keys, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.subusers, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.subusers, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.suspended, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.suspended, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.max_buckets, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.max_buckets, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.op_mask, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.op_mask, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_caps, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.caps, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.admin, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.admin, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.system, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.system, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.placement_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.default_placement.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.placement_storage_class, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.default_placement.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.placement_tags, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.placement_tags, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.bucket_quota, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.quota.bucket_quota, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.temp_url_keys, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.temp_url_keys, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_quota, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.quota.user_quota, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.type, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.uinfo.type, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.mfa_ids, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.uinfo.mfa_ids, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.account_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.account_id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.path, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.path.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.create_date, sdb);
  {
    auto cd_str = ceph::to_iso_8601(params->op.user.uinfo.create_date);
    SQL_BIND_TEXT(dpp, stmt, index, cd_str.c_str(), sdb);
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_attrs, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.user.user_attrs, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_ver, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.user.user_version.ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_ver_tag, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.user_version.tag.c_str(), sdb);

out:
  return rc;
}

int SQLInsertUser::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveUser::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveUser - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveUser");
out:
  return ret;
}

int SQLRemoveUser::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveUser::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetUser::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetUser - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  if (params->op.query_str == "email") { 
    SQL_PREPARE(dpp, p_params, sdb, email_stmt, ret, "PrepareGetUser");
  } else if (params->op.query_str == "access_key") { 
    SQL_PREPARE(dpp, p_params, sdb, ak_stmt, ret, "PrepareGetUser");
  } else if (params->op.query_str == "user_id") { 
    SQL_PREPARE(dpp, p_params, sdb, userid_stmt, ret, "PrepareGetUser");
  } else { // by default by userid
    SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetUser");
  }
out:
  return ret;
}

int SQLGetUser::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.query_str == "email") { 
    SQL_BIND_INDEX(dpp, email_stmt, index, p_params.op.user.user_email, sdb);
    SQL_BIND_TEXT(dpp, email_stmt, index, params->op.user.uinfo.user_email.c_str(), sdb);
  } else if (params->op.query_str == "access_key") {
    if (!params->op.user.uinfo.access_keys.empty()) {
      const auto& k = params->op.user.uinfo.access_keys.begin()->second;
      SQL_BIND_INDEX(dpp, ak_stmt, index, p_params.op.user.access_key_id, sdb);
      SQL_BIND_TEXT(dpp, ak_stmt, index, k.id.c_str(), sdb);
    }
  } else if (params->op.query_str == "user_id") { 
    SQL_BIND_INDEX(dpp, userid_stmt, index, p_params.op.user.user_id, sdb);
    SQL_BIND_TEXT(dpp, userid_stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);
  } else { // by default by userid
    SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id, sdb);
    SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);
  }

out:
  return rc;
}

int SQLGetUser::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  if (params->op.query_str == "email") { 
    SQL_EXECUTE(dpp, params, email_stmt, list_user);
  } else if (params->op.query_str == "access_key") { 
    SQL_EXECUTE(dpp, params, ak_stmt, list_user);
  } else if (params->op.query_str == "user_id") { 
    SQL_EXECUTE(dpp, params, userid_stmt, list_user);
  } else { // by default by userid
    SQL_EXECUTE(dpp, params, stmt, list_user);
  }

out:
  return ret;
}

int SQLListUsers::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLListUsers - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListUsers");
out:
  return ret;
}

int SQLListUsers::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);
out:
  return rc;
}

int SQLListUsers::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_user);

out:
  return ret;
}

int SQLInsertBucket::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertBucket - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertBucket");

out:
  return ret;
}

int SQLInsertBucket::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  // user_id here is copied as OwnerID in the bucket table.
  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.user.user_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.owner.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.tenant, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.tenant.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.marker, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.marker.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.bucket_id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.size, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.ent.size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.size_rounded, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.ent.size_rounded, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.creation_time, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.creation_time, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.count, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.ent.count, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.placement_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.placement_rule.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.placement_storage_class, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.placement_rule.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.flags, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.info.flags, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.zonegroup, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.zonegroup.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.has_instance_obj, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.info.has_instance_obj, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.quota, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.quota, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.requester_pays, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.info.requester_pays, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.has_website, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.info.has_website, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.website_conf, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.website_conf, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.swift_versioning, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.info.swift_versioning, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.swift_ver_location, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.swift_ver_location.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.mdsearch_config, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.mdsearch_config, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.new_bucket_instance_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.new_bucket_instance_id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.obj_lock, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.obj_lock, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.sync_policy_info_groups, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.info.sync_policy, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_attrs, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.bucket_attrs, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_ver, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.bucket.bucket_version.ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_ver_tag, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.bucket_version.tag.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.mtime, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.bucket.mtime, sdb);

out:
  return rc;
}

int SQLInsertBucket::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  class SQLObjectOp *ObPtr = NULL;
  string bucket_name = params->op.bucket.info.bucket.name;
  struct DBOpPrepareParams p_params = PrepareParams;

  ObPtr = new SQLObjectOp(sdb, ctx());

  objectmapInsert(dpp, bucket_name, ObPtr);

  SQL_EXECUTE(dpp, params, stmt, NULL);

  /* Once Bucket is inserted created corresponding object(&data) tables
   */
  InitPrepareParams(dpp, p_params, params);

  (void)createObjectTable(dpp, params);
  (void)createObjectDataTable(dpp, params);
  (void)createObjectTableTrigger(dpp, params);
out:
  return ret;
}

int SQLUpdateBucket::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLUpdateBucket - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  if (params->op.query_str == "attrs") { 
    SQL_PREPARE(dpp, p_params, sdb, attrs_stmt, ret, "PrepareUpdateBucket");
  } else if (params->op.query_str == "owner") { 
    SQL_PREPARE(dpp, p_params, sdb, owner_stmt, ret, "PrepareUpdateBucket");
  } else if (params->op.query_str == "info") { 
    SQL_PREPARE(dpp, p_params, sdb, info_stmt, ret, "PrepareUpdateBucket");
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateBucket invalid query_str:" <<
      params->op.query_str << "" << dendl;
    goto out;
  }

out:
  return ret;
}

int SQLUpdateBucket::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  sqlite3_stmt** stmt = NULL; // Prepared statement

  /* All below fields for attrs */
  if (params->op.query_str == "attrs") { 
    stmt = &attrs_stmt;
  } else if (params->op.query_str == "owner") { 
    stmt = &owner_stmt;
  } else if (params->op.query_str == "info") { 
    stmt = &info_stmt;
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateBucket invalid query_str:" <<
      params->op.query_str << "" << dendl;
    goto out;
  }

  if (params->op.query_str == "attrs") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_attrs, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.bucket_attrs, sdb);
  } else if (params->op.query_str == "owner") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.creation_time, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.creation_time, sdb);
  } else if (params->op.query_str == "info") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.tenant, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.bucket.tenant.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.marker, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.bucket.marker.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_id, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.bucket.bucket_id.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.creation_time, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.creation_time, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.count, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.ent.count, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.placement_name, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.placement_rule.name.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.placement_storage_class, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.placement_rule.storage_class.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.flags, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.info.flags, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.zonegroup, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.zonegroup.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.has_instance_obj, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.info.has_instance_obj, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.quota, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.quota, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.requester_pays, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.info.requester_pays, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.has_website, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.info.has_website, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.website_conf, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.website_conf, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.swift_versioning, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.info.swift_versioning, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.swift_ver_location, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.swift_ver_location.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.mdsearch_config, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.mdsearch_config, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.new_bucket_instance_id, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.new_bucket_instance_id.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.obj_lock, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.obj_lock, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.sync_policy_info_groups, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.info.sync_policy, sdb);
  }

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.user.user_id, sdb);
  SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.owner.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_attrs, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.bucket_attrs, sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_ver, sdb);
  SQL_BIND_INT(dpp, *stmt, index, params->op.bucket.bucket_version.ver, sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.mtime, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.bucket.mtime, sdb);

out:
  return rc;
}

int SQLUpdateBucket::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  sqlite3_stmt** stmt = NULL; // Prepared statement

  if (params->op.query_str == "attrs") { 
    stmt = &attrs_stmt;
  } else if (params->op.query_str == "owner") { 
    stmt = &owner_stmt;
  } else if (params->op.query_str == "info") { 
    stmt = &info_stmt;
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateBucket invalid query_str:" <<
      params->op.query_str << "" << dendl;
    goto out;
  }

  SQL_EXECUTE(dpp, params, *stmt, NULL);
out:
  return ret;
}

int SQLRemoveBucket::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveBucket - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveBucket");

out:
  return ret;
}

int SQLRemoveBucket::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveBucket::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  objectmapDelete(dpp, params->op.bucket.info.bucket.name);

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetBucket::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetBucket - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetBucket");

out:
  return ret;
}

int SQLGetBucket::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

out:
  return rc;
}

int SQLGetBucket::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  class SQLObjectOp *ObPtr = NULL;

  params->op.name = "GetBucket";

  ObPtr = new SQLObjectOp(sdb, ctx());

  /* For the case when the  server restarts, need to reinsert objectmap*/
  objectmapInsert(dpp, params->op.bucket.info.bucket.name, ObPtr);
  SQL_EXECUTE(dpp, params, stmt, list_bucket);
out:
  return ret;
}

int SQLListUserBuckets::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLListUserBuckets - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  if (params->op.query_str == "all") { 
    SQL_PREPARE(dpp, p_params, sdb, all_stmt, ret, "PrepareListUserBuckets");
  }else {
    SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListUserBuckets");
  }

out:
  return ret;
}

int SQLListUserBuckets::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  sqlite3_stmt** pstmt = NULL; // Prepared statement

  if (params->op.query_str == "all") { 
    pstmt = &all_stmt;
  } else { 
    pstmt = &stmt;
  }

  if (params->op.query_str != "all") { 
    SQL_BIND_INDEX(dpp, *pstmt, index, p_params.op.user.user_id, sdb);
    SQL_BIND_TEXT(dpp, *pstmt, index, params->op.bucket.owner.c_str(), sdb);
  }

  SQL_BIND_INDEX(dpp, *pstmt, index, p_params.op.bucket.min_marker, sdb);
  SQL_BIND_TEXT(dpp, *pstmt, index, params->op.bucket.min_marker.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *pstmt, index, p_params.op.list_max_count, sdb);
  SQL_BIND_INT(dpp, *pstmt, index, params->op.list_max_count, sdb);

out:
  return rc;
}

int SQLListUserBuckets::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  if (params->op.query_str == "all") { 
    SQL_EXECUTE(dpp, params, all_stmt, list_bucket);
  } else {
    SQL_EXECUTE(dpp, params, stmt, list_bucket);
  }
out:
  return ret;
}

int SQLPutObject::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLPutObject - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PreparePutObject");

out:
  return ret;
}

int SQLPutObject::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  int VersionNum = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.obj.state.obj.key.instance.empty()) {
    params->op.obj.state.obj.key.instance = "null";
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_ns, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.ns.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.acls, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.acls, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.index_ver, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.index_ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.tag, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.tag.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.flags, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.flags, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.versioned_epoch, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.versioned_epoch, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_category, sdb);
  SQL_BIND_INT(dpp, stmt, index, (uint8_t)(params->op.obj.category), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.etag, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.etag.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.owner, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.owner.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.owner_display_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.owner_display_name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.storage_class, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.appendable, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.appendable, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.content_type, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.content_type.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.index_hash_source, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.index_hash_source.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_size, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.accounted_size, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.accounted_size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.mtime, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.mtime, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.epoch, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.epoch, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_tag, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.obj_tag, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.tail_tag, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.tail_tag, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.write_tag, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.write_tag.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.fake_tag, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.fake_tag, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.shadow_obj, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.shadow_obj.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.has_data, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.has_data, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.is_versioned, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.is_versioned, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.version_num, sdb);
  SQL_BIND_INT(dpp, stmt, index, VersionNum, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.pg_ver, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.pg_ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.zone_short_id, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.zone_short_id, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_version, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.state.objv_tracker.read_version.ver, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_version_tag, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.objv_tracker.read_version.tag.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_attrs, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.attrset, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.head_size, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.head_size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.max_head_size, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.max_head_size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.obj_id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.tail_instance, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.tail_instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.head_placement_rule_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.head_placement_rule.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.head_placement_storage_class, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.head_placement_rule.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.tail_placement_rule_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.tail_placement.placement_rule.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.tail_placement_storage_class, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.tail_placement.placement_rule.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.manifest_part_objs, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.objs, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.manifest_part_rules, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.rules, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.omap, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.omap, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.is_multipart, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.obj.is_multipart, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.mp_parts, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.mp_parts, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.head_data, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.head_data, sdb);

out:
  return rc;
}

int SQLPutObject::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLDeleteObject::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLDeleteObject - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareDeleteObject");

out:
  return ret;
}

int SQLDeleteObject::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.obj.state.obj.key.instance.empty()) {
    params->op.obj.state.obj.key.instance = "null";
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);
out:
  return rc;
}

int SQLDeleteObject::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetObject::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetObject - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetObject");

out:
  return ret;
}

int SQLGetObject::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.obj.state.obj.key.instance.empty()) {
    params->op.obj.state.obj.key.instance = "null";
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

out:
  return rc;
}

int SQLGetObject::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_object);
out:
  return ret;
}

int SQLUpdateObject::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLUpdateObject - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  if (params->op.query_str == "omap") {
    SQL_PREPARE(dpp, p_params, sdb, omap_stmt, ret, "PrepareUpdateObject");
  } else if (params->op.query_str == "attrs") {
    SQL_PREPARE(dpp, p_params, sdb, attrs_stmt, ret, "PrepareUpdateObject");
  } else if (params->op.query_str == "meta") {
    SQL_PREPARE(dpp, p_params, sdb, meta_stmt, ret, "PrepareUpdateObject");
  } else if (params->op.query_str == "mp") {
    SQL_PREPARE(dpp, p_params, sdb, mp_stmt, ret, "PrepareUpdateObject");
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateObject invalid query_str:" <<
      params->op.query_str << dendl;
    goto out;
  }

out:
  return ret;
}

int SQLUpdateObject::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  sqlite3_stmt** stmt = NULL; // Prepared statement

  /* All below fields for attrs */
  if (params->op.query_str == "omap") { 
    stmt = &omap_stmt;
  } else if (params->op.query_str == "attrs") { 
    stmt = &attrs_stmt;
  } else if (params->op.query_str == "meta") { 
    stmt = &meta_stmt;
  } else if (params->op.query_str == "mp") { 
    stmt = &mp_stmt;
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateObject invalid query_str:" <<
      params->op.query_str << dendl;
    goto out;
  }

  if (params->op.obj.state.obj.key.instance.empty()) {
    params->op.obj.state.obj.key.instance = "null";
  }

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, *stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_name, sdb);
  SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_instance, sdb);
  SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.mtime, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.mtime, sdb);

  if (params->op.query_str == "omap") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.omap, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.omap, sdb);
  }
  if (params->op.query_str == "attrs") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_attrs, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.attrset, sdb);
  }
  if (params->op.query_str == "mp") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.mp_parts, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.mp_parts, sdb);
  }
  if (params->op.query_str == "meta") { 
    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_ns, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.obj.key.ns.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.acls, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.acls, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.index_ver, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.index_ver, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.tag, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.tag.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.flags, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.flags, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.versioned_epoch, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.versioned_epoch, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_category, sdb);
    SQL_BIND_INT(dpp, *stmt, index, (uint8_t)(params->op.obj.category), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.etag, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.etag.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.owner, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.owner.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.owner_display_name, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.owner_display_name.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.storage_class, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.storage_class.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.appendable, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.appendable, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.content_type, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.content_type.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.index_hash_source, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.obj.index_hash_source.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_size, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.size, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.accounted_size, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.accounted_size, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.epoch, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.epoch, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_tag, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.obj_tag, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.tail_tag, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.tail_tag, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.write_tag, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.write_tag.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.fake_tag, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.fake_tag, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.shadow_obj, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.shadow_obj.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.has_data, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.has_data, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.is_versioned, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.is_versioned, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.version_num, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.version_num, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.pg_ver, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.pg_ver, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.zone_short_id, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.zone_short_id, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_version, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.state.objv_tracker.read_version.ver, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_version_tag, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.state.objv_tracker.read_version.tag.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_attrs, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.state.attrset, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.head_size, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.head_size, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.max_head_size, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.max_head_size, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.obj_id, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.obj_id.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.tail_instance, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.tail_instance.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.head_placement_rule_name, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.head_placement_rule.name.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.head_placement_storage_class, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.head_placement_rule.storage_class.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.tail_placement_rule_name, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.tail_placement.placement_rule.name.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.tail_placement_storage_class, sdb);
    SQL_BIND_TEXT(dpp, *stmt, index, params->op.obj.tail_placement.placement_rule.storage_class.c_str(), sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.manifest_part_objs, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.objs, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.manifest_part_rules, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.rules, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.omap, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.omap, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.is_multipart, sdb);
    SQL_BIND_INT(dpp, *stmt, index, params->op.obj.is_multipart, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.mp_parts, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.mp_parts, sdb);

    SQL_BIND_INDEX(dpp, *stmt, index, p_params.op.obj.head_data, sdb);
    SQL_ENCODE_BLOB_PARAM(dpp, *stmt, index, params->op.obj.head_data, sdb);
  }

out:
  return rc;
}

int SQLUpdateObject::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  sqlite3_stmt** stmt = NULL; // Prepared statement

  if (params->op.query_str == "omap") { 
    stmt = &omap_stmt;
  } else if (params->op.query_str == "attrs") { 
    stmt = &attrs_stmt;
  } else if (params->op.query_str == "meta") { 
    stmt = &meta_stmt;
  } else if (params->op.query_str == "mp") { 
    stmt = &mp_stmt;
  } else {
    ldpp_dout(dpp, 0)<<"In SQLUpdateObject invalid query_str:" <<
      params->op.query_str << dendl;
    goto out;
  }

  SQL_EXECUTE(dpp, params, *stmt, NULL);
out:
  return ret;
}

int SQLListBucketObjects::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLListBucketObjects - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListBucketObjects");

out:
  return ret;
}

int SQLListBucketObjects::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.obj.state.obj.key.instance.empty()) {
    params->op.obj.state.obj.key.instance = "null";
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.min_marker, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.min_marker.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.prefix, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.prefix.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);

out:
  return rc;
}

int SQLListBucketObjects::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_object);
out:
  return ret;
}

int SQLListVersionedObjects::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLListVersionedObjects - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListVersionedObjects");

out:
  return ret;
}

int SQLListVersionedObjects::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.obj.state.obj.key.instance.empty()) {
    params->op.obj.state.obj.key.instance = "null";
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);

out:
  return rc;
}

int SQLListVersionedObjects::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_object);
out:
  return ret;
}

int SQLPutObjectData::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLPutObjectData - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PreparePutObjectData");

out:
  return ret;
}

int SQLPutObjectData::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.obj.state.obj.key.instance.empty()) {
    params->op.obj.state.obj.key.instance = "null";
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name, sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance, sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_ns, sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.ns.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.obj_id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj_data.part_num, sdb);

  SQL_BIND_INT(dpp, stmt, index, params->op.obj_data.part_num, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj_data.offset, sdb);

  SQL_BIND_INT(dpp, stmt, index, params->op.obj_data.offset, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj_data.data, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj_data.data, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj_data.size, sdb);

  SQL_BIND_INT(dpp, stmt, index, params->op.obj_data.size, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj_data.multipart_part_str, sdb);

  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj_data.multipart_part_str.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.mtime, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.mtime, sdb);

out:
  return rc;
}

int SQLPutObjectData::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLUpdateObjectData::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLUpdateObjectData - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareUpdateObjectData");

out:
  return ret;
}

int SQLUpdateObjectData::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.obj.state.obj.key.instance.empty()) {
    params->op.obj.state.obj.key.instance = "null";
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.obj_id.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.mtime, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.mtime, sdb);

out:
  return rc;
}

int SQLUpdateObjectData::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetObjectData::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetObjectData - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetObjectData");

out:
  return ret;
}

int SQLGetObjectData::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.obj.state.obj.key.instance.empty()) {
    params->op.obj.state.obj.key.instance = "null";
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.obj_id.c_str(), sdb);

out:
  return rc;
}

int SQLGetObjectData::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, get_objectdata);
out:
  return ret;
}

int SQLDeleteObjectData::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLDeleteObjectData - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareDeleteObjectData");

out:
  return ret;
}

int SQLDeleteObjectData::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.obj.state.obj.key.instance.empty()) {
    params->op.obj.state.obj.key.instance = "null";
  }

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.bucket.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.name.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_instance, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.state.obj.key.instance.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.obj_id, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.obj.obj_id.c_str(), sdb);

out:
  return rc;
}

int SQLDeleteObjectData::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLDeleteStaleObjectData::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLDeleteStaleObjectData - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);
  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareDeleteStaleObjectData");

out:
  return ret;
}

int SQLDeleteStaleObjectData::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.obj.mtime, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, params->op.obj.state.mtime, sdb);

out:
  return rc;
}

int SQLDeleteStaleObjectData::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLInsertLCEntry::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertLCEntry - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertLCEntry");

out:
  return ret;
}

int SQLInsertLCEntry::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.index, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.index.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.entry.bucket.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.status, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.lc_entry.entry.status, sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.start_time, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.lc_entry.entry.start_time, sdb);

out:
  return rc;
}

int SQLInsertLCEntry::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveLCEntry::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveLCEntry - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveLCEntry");

out:
  return ret;
}

int SQLRemoveLCEntry::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.index, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.index.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.entry.bucket.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveLCEntry::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetLCEntry::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  sqlite3_stmt** pstmt = NULL; // Prepared statement
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetLCEntry - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  if (params->op.query_str == "get_next_entry") {
    pstmt = &next_stmt;
  } else {
    pstmt = &stmt;
  }
  SQL_PREPARE(dpp, p_params, sdb, *pstmt, ret, "PrepareGetLCEntry");

out:
  return ret;
}

int SQLGetLCEntry::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;
  sqlite3_stmt** pstmt = NULL; // Prepared statement

  if (params->op.query_str == "get_next_entry") {
    pstmt = &next_stmt;
  } else {
    pstmt = &stmt;
  }
  SQL_BIND_INDEX(dpp, *pstmt, index, p_params.op.lc_entry.index, sdb);
  SQL_BIND_TEXT(dpp, *pstmt, index, params->op.lc_entry.index.c_str(), sdb);

  SQL_BIND_INDEX(dpp, *pstmt, index, p_params.op.lc_entry.bucket_name, sdb);
  SQL_BIND_TEXT(dpp, *pstmt, index, params->op.lc_entry.entry.bucket.c_str(), sdb);

out:
  return rc;
}

int SQLGetLCEntry::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  sqlite3_stmt** pstmt = NULL; // Prepared statement

  if (params->op.query_str == "get_next_entry") {
    pstmt = &next_stmt;
  } else {
    pstmt = &stmt;
  }

  SQL_EXECUTE(dpp, params, *pstmt, list_lc_entry);
out:
  return ret;
}

int SQLListLCEntries::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLListLCEntries - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareListLCEntries");

out:
  return ret;
}

int SQLListLCEntries::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.index, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.index.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_entry.min_marker, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_entry.min_marker.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.list_max_count, sdb);
  SQL_BIND_INT(dpp, stmt, index, params->op.list_max_count, sdb);

out:
  return rc;
}

int SQLListLCEntries::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, list_lc_entry);
out:
  return ret;
}

int SQLInsertLCHead::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLInsertLCHead - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareInsertLCHead");

out:
  return ret;
}

int SQLInsertLCHead::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_head.index, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_head.index.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_head.marker, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_head.head.marker.c_str(), sdb);

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_head.start_date, sdb);
  SQL_ENCODE_BLOB_PARAM(dpp, stmt, index, static_cast<int64_t>(params->op.lc_head.head.start_date), sdb);

out:
  return rc;
}

int SQLInsertLCHead::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveLCHead::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLRemoveLCHead - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareRemoveLCHead");

out:
  return ret;
}

int SQLRemoveLCHead::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_head.index, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_head.index.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveLCHead::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(dpp, params, stmt, NULL);
out:
  return ret;
}

int SQLGetLCHead::Prepare(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    ldpp_dout(dpp, 0)<<"In SQLGetLCHead - no db" << dendl;
    goto out;
  }

  InitPrepareParams(dpp, p_params, params);

  SQL_PREPARE(dpp, p_params, sdb, stmt, ret, "PrepareGetLCHead");

out:
  return ret;
}

int SQLGetLCHead::Bind(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(dpp, stmt, index, p_params.op.lc_head.index, sdb);
  SQL_BIND_TEXT(dpp, stmt, index, params->op.lc_head.index.c_str(), sdb);

out:
  return rc;
}

int SQLGetLCHead::Execute(const DoutPrefixProvider *dpp, struct DBOpParams *params)
{
  int ret = -1;

  // clear the params before fetching the entry
  params->op.lc_head.head = {};
  SQL_EXECUTE(dpp, params, stmt, list_lc_head);
out:
  return ret;
}
