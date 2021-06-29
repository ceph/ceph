// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "sqliteDB.h"

#define SQL_PREPARE(params, sdb, stmt, ret, Op) 	\
  do {							\
    string schema;			   		\
    schema = Schema(params);	   		\
    sqlite3_prepare_v2 (*sdb, schema.c_str(), 	\
        -1, &stmt , NULL);		\
    if (!stmt) {					\
      cout<<"failed to prepare statement " \
      <<"for Op("<<Op<<"); Errmsg -"\
      <<sqlite3_errmsg(*sdb)<<"\n";\
      ret = -1;				\
      goto out;				\
    }						\
    dbout(L_DEBUG)<<"Successfully Prepared stmt for Op("<<Op	\
    <<") schema("<<schema<<") stmt("<<stmt<<")\n";	\
    ret = 0;					\
  } while(0);

#define SQL_BIND_INDEX(stmt, index, str, sdb)	\
  do {						\
    index = sqlite3_bind_parameter_index(stmt, str);     \
    \
    if (index <=0)  {				     \
      cout<<"failed to fetch bind parameter"\
      " index for str("<<str<<") in "   \
      <<"stmt("<<stmt<<"); Errmsg -"    \
      <<sqlite3_errmsg(*sdb)<<"\n"; 	     \
      rc = -1;				     \
      goto out;				     \
    }						     \
    dbout(L_FULLDEBUG)<<"Bind parameter index for str("  \
    <<str<<") in stmt("<<stmt<<") is "  \
    <<index<<"\n";			     \
  }while(0);

#define SQL_BIND_TEXT(stmt, index, str, sdb)			\
  do {								\
    rc = sqlite3_bind_text(stmt, index, str, -1, SQLITE_TRANSIENT); 	\
    \
    if (rc != SQLITE_OK) {					      	\
      dbout(L_ERR)<<"sqlite bind text failed for index("     	\
      <<index<<"), str("<<str<<") in stmt("   	\
      <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
      <<"\n";				\
      rc = -1;					\
      goto out;					\
    }							\
  }while(0);

#define SQL_BIND_INT(stmt, index, num, sdb)			\
  do {								\
    rc = sqlite3_bind_int(stmt, index, num);		\
    \
    if (rc != SQLITE_OK) {					\
      dbout(L_ERR)<<"sqlite bind int failed for index("     	\
      <<index<<"), num("<<num<<") in stmt("   	\
      <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
      <<"\n";				\
      rc = -1;					\
      goto out;					\
    }							\
  }while(0);

#define SQL_BIND_BLOB(stmt, index, blob, size, sdb)		\
  do {								\
    rc = sqlite3_bind_blob(stmt, index, blob, size, SQLITE_TRANSIENT);  \
    \
    if (rc != SQLITE_OK) {					\
      dbout(L_ERR)<<"sqlite bind blob failed for index("     	\
      <<index<<"), blob("<<blob<<") in stmt("   	\
      <<stmt<<"); Errmsg - "<<sqlite3_errmsg(*sdb) \
      <<"\n";				\
      rc = -1;					\
      goto out;					\
    }							\
  }while(0);

#define SQL_ENCODE_BLOB_PARAM(stmt, index, param, sdb)		\
  do {								\
    bufferlist b;						\
    encode(param, b);					\
    SQL_BIND_BLOB(stmt, index, b.c_str(), b.length(), sdb); \
  }while(0);

#define SQL_READ_BLOB(stmt, index, void_ptr, len)		\
  do {								\
    void_ptr = NULL;					\
    void_ptr = (void *)sqlite3_column_blob(stmt, index);	\
    len = sqlite3_column_bytes(stmt, index);		\
    \
    if (!void_ptr || len == 0) {				\
      dbout(L_FULLDEBUG)<<"Null value for blob index("  \
      <<index<<") in stmt("<<stmt<<") \n";   \
    }							\
  }while(0);

#define SQL_DECODE_BLOB_PARAM(stmt, index, param, sdb)		\
  do {								\
    bufferlist b;						\
    void *blob;						\
    int blob_len = 0;					\
    \
    SQL_READ_BLOB(stmt, index, blob, blob_len);		\
    \
    b.append(reinterpret_cast<char *>(blob), blob_len);	\
    \
    decode(param, b);					\
  }while(0);

#define SQL_EXECUTE(params, stmt, cbk, args...) \
  do{						\
    if (!stmt) {				\
      ret = Prepare(params);		\
    }					\
    \
    if (!stmt) {				\
      cout<<"No prepared statement \n";	\
      goto out;			\
    }					\
    \
    ret = Bind(params);			\
    if (ret) {				\
      cout<<"Bind parameters failed for stmt(" <<stmt<<") \n";		\
      goto out;			\
    }					\
    \
    ret = Step(params->op, stmt, cbk);		\
    \
    Reset(stmt);				\
    \
    if (ret) {				\
      cout<<"Execution failed for stmt(" <<stmt<<")\n";		\
      goto out;			\
    }					\
  }while(0);

static int list_callback(void *None, int argc, char **argv, char **aname)
{
  int i;
  for(i=0; i<argc; i++) {
    string arg = argv[i] ? argv[i] : "NULL";
    cout<<aname[i]<<" = "<<arg<<"\n";
  }
  return 0;
}

enum GetUser {
  UserID = 0,
  Tenant,
  NS,
  DisplayName,
  UserEmail,
  AccessKeysID,
  AccessKeysSecret,
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

static int list_user(DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  cout<<sqlite3_column_text(stmt, 0)<<"\n";
  /* Ensure the column names match with the user table defined in dbstore.h                     
     UserID TEXT ,		\ - 0
     Tenant TEXT ,		\ - 1
     NS TEXT ,		\ - 2
     DisplayName TEXT , \ - 3
     UserEmail TEXT ,	\ - 4
     AccessKeysID TEXT ,	\ - 5
     AccessKeysSecret TEXT ,	\ - 6
     AccessKeys BLOB ,	\ - 7
     SwiftKeys BLOB ,	\ - 8
     SubUsers BLOB ,		\ - 9
     Suspended INTEGER ,	\ - 10
     MaxBuckets INTEGER ,	\ - 11
     OpMask	INTEGER ,	\ - 12
     UserCaps BLOB ,		\ - 13
     Admin	INTEGER ,	\ - 14
     System INTEGER , 	\ - 15
     PlacementName TEXT , 	\ - 16
     PlacementStorageClass TEXT , 	\ - 17
     PlacementTags BLOB ,	\ - 18
     BucketQuota BLOB ,	\ - 19
     TempURLKeys BLOB ,	\ - 20
     UserQuota BLOB ,	\ - 21
     TYPE INTEGER ,		\ - 22
     MfaIDs INTEGER ,	\ - 23
     AssumedRoleARN TEXT \n);"; - 24
     */

  op.user.uinfo.user_id.tenant = (const char*)sqlite3_column_text(stmt, Tenant);
  op.user.uinfo.user_id.id = (const char*)sqlite3_column_text(stmt, UserID);
  op.user.uinfo.user_id.ns = (const char*)sqlite3_column_text(stmt, NS);
  op.user.uinfo.display_name = (const char*)sqlite3_column_text(stmt, DisplayName); // user_name
  op.user.uinfo.user_email = (const char*)sqlite3_column_text(stmt, UserEmail);

  SQL_DECODE_BLOB_PARAM(stmt, AccessKeys, op.user.uinfo.access_keys, sdb);
  SQL_DECODE_BLOB_PARAM(stmt, SwiftKeys, op.user.uinfo.swift_keys, sdb);
  SQL_DECODE_BLOB_PARAM(stmt, SubUsers, op.user.uinfo.subusers, sdb);

  op.user.uinfo.suspended = sqlite3_column_int(stmt, Suspended);
  op.user.uinfo.max_buckets = sqlite3_column_int(stmt, MaxBuckets);
  op.user.uinfo.op_mask = sqlite3_column_int(stmt, OpMask);

  SQL_DECODE_BLOB_PARAM(stmt, UserCaps, op.user.uinfo.caps, sdb);

  op.user.uinfo.admin = sqlite3_column_int(stmt, Admin);
  op.user.uinfo.system = sqlite3_column_int(stmt, System);

  op.user.uinfo.default_placement.name = (const char*)sqlite3_column_text(stmt, PlacementName);

  op.user.uinfo.default_placement.storage_class = (const char*)sqlite3_column_text(stmt, PlacementStorageClass);

  SQL_DECODE_BLOB_PARAM(stmt, PlacementTags, op.user.uinfo.placement_tags, sdb);

  SQL_DECODE_BLOB_PARAM(stmt, BucketQuota, op.user.uinfo.bucket_quota, sdb);
  SQL_DECODE_BLOB_PARAM(stmt, TempURLKeys, op.user.uinfo.temp_url_keys, sdb);
  SQL_DECODE_BLOB_PARAM(stmt, UserQuota, op.user.uinfo.user_quota, sdb);

  op.user.uinfo.type = sqlite3_column_int(stmt, TYPE);

  SQL_DECODE_BLOB_PARAM(stmt, MfaIDs, op.user.uinfo.mfa_ids, sdb);

  op.user.uinfo.assumed_role_arn = (const char*)sqlite3_column_text(stmt, AssumedRoleARN);

  SQL_DECODE_BLOB_PARAM(stmt, UserAttrs, op.user.user_attrs, sdb);
  op.user.user_version.ver = sqlite3_column_int(stmt, UserVersion);
  op.user.user_version.tag = (const char*)sqlite3_column_text(stmt, UserVersionTag);

  return 0;
}

static int list_bucket(DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  cout<<sqlite3_column_text(stmt, 0)<<", ";
  cout<<sqlite3_column_text(stmt, 1)<<"\n";

  op.bucket.ent.bucket.name = (const char*)sqlite3_column_text(stmt, BucketName);
  op.bucket.ent.bucket.tenant = (const char*)sqlite3_column_text(stmt, Bucket_Tenant);
  op.bucket.ent.bucket.marker = (const char*)sqlite3_column_text(stmt, Marker);
  op.bucket.ent.bucket.bucket_id = (const char*)sqlite3_column_text(stmt, BucketID);
  op.bucket.ent.size = sqlite3_column_int(stmt, Size);
  op.bucket.ent.size_rounded = sqlite3_column_int(stmt, SizeRounded);
  SQL_DECODE_BLOB_PARAM(stmt, CreationTime, op.bucket.ent.creation_time, sdb);
  op.bucket.ent.count = sqlite3_column_int(stmt, Count);
  op.bucket.ent.placement_rule.name = (const char*)sqlite3_column_text(stmt, Bucket_PlacementName);
  op.bucket.ent.placement_rule.storage_class = (const char*)sqlite3_column_text(stmt, Bucket_PlacementStorageClass);

  op.bucket.info.bucket = op.bucket.ent.bucket;
  op.bucket.info.placement_rule = op.bucket.ent.placement_rule;
  op.bucket.info.creation_time = op.bucket.ent.creation_time;

  op.bucket.info.owner.id = (const char*)sqlite3_column_text(stmt, OwnerID);
  op.bucket.info.owner.tenant = op.bucket.ent.bucket.tenant;

  if (op.name == "GetBucket") {
    op.bucket.info.owner.ns = (const char*)sqlite3_column_text(stmt, Bucket_User_NS);
  }

  op.bucket.info.flags = sqlite3_column_int(stmt, Flags);
  op.bucket.info.zonegroup = (const char*)sqlite3_column_text(stmt, Zonegroup);
  op.bucket.info.has_instance_obj = sqlite3_column_int(stmt, HasInstanceObj);

  SQL_DECODE_BLOB_PARAM(stmt, Quota, op.bucket.info.quota, sdb);
  op.bucket.info.requester_pays = sqlite3_column_int(stmt, RequesterPays);
  op.bucket.info.has_website = sqlite3_column_int(stmt, HasWebsite);
  SQL_DECODE_BLOB_PARAM(stmt, WebsiteConf, op.bucket.info.website_conf, sdb);
  op.bucket.info.swift_versioning = sqlite3_column_int(stmt, SwiftVersioning);
  op.bucket.info.swift_ver_location = (const char*)sqlite3_column_text(stmt, SwiftVerLocation);
  SQL_DECODE_BLOB_PARAM(stmt, MdsearchConfig, op.bucket.info.mdsearch_config, sdb);
  op.bucket.info.new_bucket_instance_id = (const char*)sqlite3_column_text(stmt, NewBucketInstanceID);
  SQL_DECODE_BLOB_PARAM(stmt, ObjectLock, op.bucket.info.obj_lock, sdb);
  SQL_DECODE_BLOB_PARAM(stmt, SyncPolicyInfoGroups, op.bucket.info.sync_policy, sdb);
  SQL_DECODE_BLOB_PARAM(stmt, BucketAttrs, op.bucket.bucket_attrs, sdb);
  op.bucket.bucket_version.ver = sqlite3_column_int(stmt, BucketVersion);
  op.bucket.bucket_version.tag = (const char*)sqlite3_column_text(stmt, BucketVersionTag);

  /* Read bucket version into info.objv_tracker.read_ver. No need
   * to set write_ver as its not used anywhere. Still keeping its
   * value same as read_ver */
  op.bucket.info.objv_tracker.read_version = op.bucket.bucket_version;
  op.bucket.info.objv_tracker.write_version = op.bucket.bucket_version;

  SQL_DECODE_BLOB_PARAM(stmt, Mtime, op.bucket.mtime, sdb);

  op.bucket.list_entries.push_back(op.bucket.ent);

  return 0;
}

static int list_object(DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  cout<<sqlite3_column_text(stmt, 0)<<", ";
  cout<<sqlite3_column_text(stmt, 1)<<"\n";

  return 0;
}

static int get_objectdata(DBOpInfo &op, sqlite3_stmt *stmt) {
  if (!stmt)
    return -1;

  int datalen = 0;
  const void *blob = NULL;

  blob = sqlite3_column_blob(stmt, 3);
  datalen = sqlite3_column_bytes(stmt, 3);

  cout<<sqlite3_column_text(stmt, 0)<<", ";
  cout<<sqlite3_column_text(stmt, 1)<<",";
  cout<<sqlite3_column_int(stmt, 2)<<",";
  char data[datalen+1] = {};
  if (blob)
    strncpy(data, (const char *)blob, datalen);

  cout<<data<<","<<datalen<<"\n";

  return 0;
}

int SQLiteDB::InitializeDBOps()
{
  (void)createTables();
  dbops.InsertUser = new SQLInsertUser(&this->db);
  dbops.RemoveUser = new SQLRemoveUser(&this->db);
  dbops.GetUser = new SQLGetUser(&this->db);
  dbops.InsertBucket = new SQLInsertBucket(&this->db);
  dbops.UpdateBucket = new SQLUpdateBucket(&this->db);
  dbops.RemoveBucket = new SQLRemoveBucket(&this->db);
  dbops.GetBucket = new SQLGetBucket(&this->db);
  dbops.ListUserBuckets = new SQLListUserBuckets(&this->db);

  return 0;
}

int SQLiteDB::FreeDBOps()
{
  delete dbops.InsertUser;
  delete dbops.RemoveUser;
  delete dbops.GetUser;
  delete dbops.InsertBucket;
  delete dbops.UpdateBucket;
  delete dbops.RemoveBucket;
  delete dbops.GetBucket;
  delete dbops.ListUserBuckets;

  return 0;
}

void *SQLiteDB::openDB()
{
  string dbname;
  int rc = 0;

  dbname	= getDBname();
  if (dbname.empty()) {
    dbout(L_ERR)<<"dbname is NULL\n";
    goto out;
  }

  rc = sqlite3_open_v2(dbname.c_str(), (sqlite3**)&db,
      SQLITE_OPEN_READWRITE |
      SQLITE_OPEN_CREATE |
      SQLITE_OPEN_FULLMUTEX,
      NULL);

  if (rc) {
    dbout(L_ERR)<<"Cant open "<<dbname<<"; Errmsg - "\
      <<sqlite3_errmsg((sqlite3*)db)<<"\n";
  } else {
    dbout(L_DEBUG)<<"Opened database("<<dbname<<") successfully\n";
  }

  exec("PRAGMA foreign_keys=ON", NULL);

out:
  return db;
}

int SQLiteDB::closeDB()
{
  if (db)
    sqlite3_close((sqlite3 *)db);

  db = NULL;

  return 0;
}

int SQLiteDB::Reset(sqlite3_stmt *stmt)
{
  int ret = -1;

  if (!stmt) {
    return -1;
  }
  sqlite3_clear_bindings(stmt);
  ret = sqlite3_reset(stmt);

  return ret;
}

int SQLiteDB::Step(DBOpInfo &op, sqlite3_stmt *stmt,
    int (*cbk)(DBOpInfo &op, sqlite3_stmt *stmt))
{
  int ret = -1;

  if (!stmt) {
    return -1;
  }

again:
  ret = sqlite3_step(stmt);

  if ((ret != SQLITE_DONE) && (ret != SQLITE_ROW)) {
    dbout(L_ERR)<<"sqlite step failed for stmt("<<stmt \
      <<"); Errmsg - "<<sqlite3_errmsg((sqlite3*)db)<<"\n";
    return -1;
  } else if (ret == SQLITE_ROW) {
    if (cbk) {
      (*cbk)(op, stmt);
    } else {
    }
    goto again;
  }

  dbout(L_FULLDEBUG)<<"sqlite step successfully executed for stmt(" \
    <<stmt<<")  ret = " << ret <<"\n";

  return 0;
}

int SQLiteDB::exec(const char *schema,
    int (*callback)(void*,int,char**,char**))
{
  int ret = -1;
  char *errmsg = NULL;

  if (!db)
    goto out;

  ret = sqlite3_exec((sqlite3*)db, schema, callback, 0, &errmsg);
  if (ret != SQLITE_OK) {
    dbout(L_ERR)<<"sqlite exec failed for schema("<<schema \
      <<"); Errmsg - "<<errmsg<<"\n";
    sqlite3_free(errmsg);
    goto out;
  }
  ret = 0;
  dbout(L_FULLDEBUG)<<"sqlite exec successfully processed for schema(" \
    <<schema<<")\n";
out:
  return ret;
}

int SQLiteDB::createTables()
{
  int ret = -1;
  int cu, cb = -1;
  DBOpParams params = {};

  params.user_table = getUserTable();
  params.bucket_table = getBucketTable();

  if ((cu = createUserTable(&params)))
    goto out;

  if ((cb = createBucketTable(&params)))
    goto out;

  if ((cb = createQuotaTable(&params)))
    goto out;

  ret = 0;
out:
  if (ret) {
    if (cu)
      DeleteUserTable(&params);
    if (cb)
      DeleteBucketTable(&params);
    dbout(L_ERR)<<"Creation of tables failed \n";
  }

  return ret;
}

int SQLiteDB::createUserTable(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("User", params);

  ret = exec(schema.c_str(), NULL);
  if (ret)
    dbout(L_ERR)<<"CreateUserTable failed \n";

  dbout(L_FULLDEBUG)<<"CreateUserTable suceeded \n";

  return ret;
}

int SQLiteDB::createBucketTable(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Bucket", params);

  ret = exec(schema.c_str(), NULL);
  if (ret)
    dbout(L_ERR)<<"CreateBucketTable failed \n";

  dbout(L_FULLDEBUG)<<"CreateBucketTable suceeded \n";

  return ret;
}

int SQLiteDB::createObjectTable(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Object", params);

  ret = exec(schema.c_str(), NULL);
  if (ret)
    dbout(L_ERR)<<"CreateObjectTable failed \n";

  dbout(L_FULLDEBUG)<<"CreateObjectTable suceeded \n";

  return ret;
}

int SQLiteDB::createQuotaTable(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("Quota", params);

  ret = exec(schema.c_str(), NULL);
  if (ret)
    dbout(L_ERR)<<"CreateQuotaTable failed \n";

  dbout(L_FULLDEBUG)<<"CreateQuotaTable suceeded \n";

  return ret;
}

int SQLiteDB::createObjectDataTable(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = CreateTableSchema("ObjectData", params);

  ret = exec(schema.c_str(), NULL);
  if (ret)
    dbout(L_ERR)<<"CreateObjectDataTable failed \n";

  dbout(L_FULLDEBUG)<<"CreateObjectDataTable suceeded \n";

  return ret;
}

int SQLiteDB::DeleteUserTable(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->user_table);

  ret = exec(schema.c_str(), NULL);
  if (ret)
    dbout(L_ERR)<<"DeleteUserTable failed \n";

  dbout(L_FULLDEBUG)<<"DeleteUserTable suceeded \n";

  return ret;
}

int SQLiteDB::DeleteBucketTable(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->bucket_table);

  ret = exec(schema.c_str(), NULL);
  if (ret)
    dbout(L_ERR)<<"DeletebucketTable failed \n";

  dbout(L_FULLDEBUG)<<"DeletebucketTable suceeded \n";

  return ret;
}

int SQLiteDB::DeleteObjectTable(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->object_table);

  ret = exec(schema.c_str(), NULL);
  if (ret)
    dbout(L_ERR)<<"DeleteObjectTable failed \n";

  dbout(L_FULLDEBUG)<<"DeleteObjectTable suceeded \n";

  return ret;
}

int SQLiteDB::DeleteObjectDataTable(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = DeleteTableSchema(params->objectdata_table);

  ret = exec(schema.c_str(), NULL);
  if (ret)
    dbout(L_ERR)<<"DeleteObjectDataTable failed \n";

  dbout(L_FULLDEBUG)<<"DeleteObjectDataTable suceeded \n";

  return ret;
}

int SQLiteDB::ListAllUsers(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = ListTableSchema(params->user_table);
  cout<<"########### Listing all Users #############\n";
  ret = exec(schema.c_str(), &list_callback);
  if (ret)
    dbout(L_ERR)<<"GetUsertable failed \n";

  dbout(L_FULLDEBUG)<<"GetUserTable suceeded \n";

  return ret;
}

int SQLiteDB::ListAllBuckets(DBOpParams *params)
{
  int ret = -1;
  string schema;

  schema = ListTableSchema(params->bucket_table);

  cout<<"########### Listing all Buckets #############\n";
  ret = exec(schema.c_str(), &list_callback);
  if (ret)
    dbout(L_ERR)<<"Listbuckettable failed \n";

  dbout(L_FULLDEBUG)<<"ListbucketTable suceeded \n";

  return ret;
}

int SQLiteDB::ListAllObjects(DBOpParams *params)
{
  int ret = -1;
  string schema;
  map<string, class ObjectOp*>::iterator iter;
  map<string, class ObjectOp*> objectmap;
  string bucket;

  cout<<"########### Listing all Objects #############\n";

  objectmap = getObjectMap();

  if (objectmap.empty())
    dbout(L_DEBUG)<<"objectmap empty \n";

  for (iter = objectmap.begin(); iter != objectmap.end(); ++iter) {
    bucket = iter->first;
    params->object_table = bucket +
      ".object.table";
    schema = ListTableSchema(params->object_table);

    ret = exec(schema.c_str(), &list_callback);
    if (ret)
      dbout(L_ERR)<<"ListObjecttable failed \n";

    dbout(L_FULLDEBUG)<<"ListObjectTable suceeded \n";
  }

  return ret;
}

int SQLObjectOp::InitializeObjectOps()
{
  InsertObject = new SQLInsertObject(sdb);
  RemoveObject = new SQLRemoveObject(sdb);
  ListObject = new SQLListObject(sdb);
  PutObjectData = new SQLPutObjectData(sdb);
  GetObjectData = new SQLGetObjectData(sdb);
  DeleteObjectData = new SQLDeleteObjectData(sdb);

  return 0;
}

int SQLObjectOp::FreeObjectOps()
{
  delete InsertObject;
  delete RemoveObject;
  delete ListObject;
  delete PutObjectData;
  delete GetObjectData;
  delete DeleteObjectData;

  return 0;
}

int SQLInsertUser::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLInsertUser - no db\n";
    goto out;
  }

  p_params.user_table = params->user_table;

  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareInsertUser");
out:
  return ret;
}

int SQLInsertUser::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.op.user.tenant.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.user_id.tenant.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.user_id.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.ns.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.user_id.ns.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.display_name.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.display_name.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.user_email.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.user_email.c_str(), sdb);

  if (!params->op.user.uinfo.access_keys.empty()) {
    string access_key;
    string key;
    map<string, RGWAccessKey>::const_iterator it =
      params->op.user.uinfo.access_keys.begin();
    const RGWAccessKey& k = it->second;
    access_key = k.id;
    key = k.key;

    SQL_BIND_INDEX(stmt, index, p_params.op.user.access_keys_id.c_str(), sdb);
    SQL_BIND_TEXT(stmt, index, access_key.c_str(), sdb);

    SQL_BIND_INDEX(stmt, index, p_params.op.user.access_keys_secret.c_str(), sdb);
    SQL_BIND_TEXT(stmt, index, key.c_str(), sdb);

    SQL_BIND_INDEX(stmt, index, p_params.op.user.access_keys.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.user.uinfo.access_keys, sdb);
  }

  SQL_BIND_INDEX(stmt, index, p_params.op.user.swift_keys.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.user.uinfo.swift_keys, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.subusers.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.user.uinfo.subusers, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.suspended.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.user.uinfo.suspended, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.max_buckets.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.user.uinfo.max_buckets, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.op_mask.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.user.uinfo.op_mask, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.user_caps.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.user.uinfo.caps, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.admin.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.user.uinfo.admin, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.system.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.user.uinfo.system, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.placement_name.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.default_placement.name.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.placement_storage_class.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.default_placement.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.placement_tags.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.user.uinfo.placement_tags, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.bucket_quota.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.user.uinfo.bucket_quota, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.temp_url_keys.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.user.uinfo.temp_url_keys, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.user_quota.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.user.uinfo.user_quota, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.type.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.user.uinfo.type, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.mfa_ids.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.user.uinfo.mfa_ids, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.assumed_role_arn.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.assumed_role_arn.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.user_attrs.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.user.user_attrs, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.user_ver.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.user.user_version.ver, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.user.user_ver_tag.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.user_version.tag.c_str(), sdb);

out:
  return rc;
}

int SQLInsertUser::Execute(struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveUser::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLRemoveUser - no db\n";
    goto out;
  }

  p_params.user_table = params->user_table;

  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareRemoveUser");
out:
  return ret;
}

int SQLRemoveUser::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.op.user.user_id.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveUser::Execute(struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(params, stmt, NULL);
out:
  return ret;
}

int SQLGetUser::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLGetUser - no db\n";
    goto out;
  }

  p_params.user_table = params->user_table;
  p_params.op.query_str = params->op.query_str;

  if (params->op.query_str == "email") { 
    SQL_PREPARE(p_params, sdb, email_stmt, ret, "PrepareGetUser");
  } else if (params->op.query_str == "access_key") { 
    SQL_PREPARE(p_params, sdb, ak_stmt, ret, "PrepareGetUser");
  } else if (params->op.query_str == "user_id") { 
    SQL_PREPARE(p_params, sdb, userid_stmt, ret, "PrepareGetUser");
  } else { // by default by userid
    SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareGetUser");
  }
out:
  return ret;
}

int SQLGetUser::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (params->op.query_str == "email") { 
    SQL_BIND_INDEX(email_stmt, index, p_params.op.user.user_email.c_str(), sdb);
    SQL_BIND_TEXT(email_stmt, index, params->op.user.uinfo.user_email.c_str(), sdb);
  } else if (params->op.query_str == "access_key") { 
    if (!params->op.user.uinfo.access_keys.empty()) {
      string access_key;
      map<string, RGWAccessKey>::const_iterator it =
        params->op.user.uinfo.access_keys.begin();
      const RGWAccessKey& k = it->second;
      access_key = k.id;

      SQL_BIND_INDEX(ak_stmt, index, p_params.op.user.access_keys_id.c_str(), sdb);
      SQL_BIND_TEXT(ak_stmt, index, access_key.c_str(), sdb);
    }
  } else if (params->op.query_str == "user_id") { 
    SQL_BIND_INDEX(userid_stmt, index, p_params.op.user.tenant.c_str(), sdb);
    SQL_BIND_TEXT(userid_stmt, index, params->op.user.uinfo.user_id.tenant.c_str(), sdb);

    SQL_BIND_INDEX(userid_stmt, index, p_params.op.user.user_id.c_str(), sdb);
    SQL_BIND_TEXT(userid_stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

    SQL_BIND_INDEX(userid_stmt, index, p_params.op.user.ns.c_str(), sdb);
    SQL_BIND_TEXT(userid_stmt, index, params->op.user.uinfo.user_id.ns.c_str(), sdb);
  } else { // by default by userid
    SQL_BIND_INDEX(stmt, index, p_params.op.user.user_id.c_str(), sdb);
    SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);
  }

out:
  return rc;
}

int SQLGetUser::Execute(struct DBOpParams *params)
{
  int ret = -1;

  if (params->op.query_str == "email") { 
    SQL_EXECUTE(params, email_stmt, list_user);
  } else if (params->op.query_str == "access_key") { 
    SQL_EXECUTE(params, ak_stmt, list_user);
  } else if (params->op.query_str == "user_id") { 
    SQL_EXECUTE(params, userid_stmt, list_user);
  } else { // by default by userid
    SQL_EXECUTE(params, stmt, list_user);
  }

out:
  return ret;
}

int SQLInsertBucket::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLInsertBucket - no db\n";
    goto out;
  }

  p_params.bucket_table = params->bucket_table;

  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareInsertBucket");

out:
  return ret;
}

int SQLInsertBucket::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.op.user.user_id.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.tenant.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.tenant.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.marker.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.marker.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_id.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.bucket_id.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.size.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.bucket.ent.size, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.size_rounded.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.bucket.ent.size_rounded, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.creation_time.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.bucket.info.creation_time, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.count.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.bucket.ent.count, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.placement_name.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.placement_rule.name.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.placement_storage_class.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.placement_rule.storage_class.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.flags.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.bucket.info.flags, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.zonegroup.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.zonegroup.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.has_instance_obj.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.bucket.info.has_instance_obj, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.quota.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.bucket.info.quota, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.requester_pays.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.bucket.info.requester_pays, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.has_website.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.bucket.info.has_website, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.website_conf.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.bucket.info.website_conf, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.swift_versioning.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.bucket.info.swift_versioning, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.swift_ver_location.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.swift_ver_location.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.mdsearch_config.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.bucket.info.mdsearch_config, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.new_bucket_instance_id.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.new_bucket_instance_id.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.obj_lock.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.bucket.info.obj_lock, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.sync_policy_info_groups.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.bucket.info.sync_policy, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_attrs.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.bucket.bucket_attrs, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_ver.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.bucket.bucket_version.ver, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_ver_tag.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.bucket_version.tag.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.mtime.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(stmt, index, params->op.bucket.mtime, sdb);

out:
  return rc;
}

int SQLInsertBucket::Execute(struct DBOpParams *params)
{
  int ret = -1;
  class SQLObjectOp *ObPtr = NULL;
  string bucket_name = params->op.bucket.info.bucket.name;

  ObPtr = new SQLObjectOp(sdb);

  objectmapInsert(bucket_name, ObPtr);

  params->object_table = bucket_name + ".object.table";

  (void)createObjectTable(params);

  SQL_EXECUTE(params, stmt, NULL);
out:
  return ret;
}

int SQLUpdateBucket::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLUpdateBucket - no db\n";
    goto out;
  }

  p_params.op.query_str = params->op.query_str;
  p_params.bucket_table = params->bucket_table;

  if (params->op.query_str == "attrs") { 
    SQL_PREPARE(p_params, sdb, attrs_stmt, ret, "PrepareUpdateBucket");
  } else if (params->op.query_str == "owner") { 
    SQL_PREPARE(p_params, sdb, owner_stmt, ret, "PrepareUpdateBucket");
  } else if (params->op.query_str == "info") { 
    SQL_PREPARE(p_params, sdb, info_stmt, ret, "PrepareUpdateBucket");
  } else {
    dbout(L_ERR)<<"In SQLUpdateBucket invalid query_str:" <<
      params->op.query_str << "\n";
    goto out;
  }

out:
  return ret;
}

int SQLUpdateBucket::Bind(struct DBOpParams *params)
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
    dbout(L_ERR)<<"In SQLUpdateBucket invalid query_str:" <<
      params->op.query_str << "\n";
    goto out;
  }

  if (params->op.query_str == "attrs") { 
    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.bucket_attrs.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(*stmt, index, params->op.bucket.bucket_attrs, sdb);
  } else if (params->op.query_str == "owner") { 
    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.creation_time.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(*stmt, index, params->op.bucket.info.creation_time, sdb);
  } else if (params->op.query_str == "info") { 
    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.tenant.c_str(), sdb);
    SQL_BIND_TEXT(*stmt, index, params->op.bucket.info.bucket.tenant.c_str(), sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.marker.c_str(), sdb);
    SQL_BIND_TEXT(*stmt, index, params->op.bucket.info.bucket.marker.c_str(), sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.bucket_id.c_str(), sdb);
    SQL_BIND_TEXT(*stmt, index, params->op.bucket.info.bucket.bucket_id.c_str(), sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.creation_time.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(*stmt, index, params->op.bucket.info.creation_time, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.count.c_str(), sdb);
    SQL_BIND_INT(*stmt, index, params->op.bucket.ent.count, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.placement_name.c_str(), sdb);
    SQL_BIND_TEXT(*stmt, index, params->op.bucket.info.placement_rule.name.c_str(), sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.placement_storage_class.c_str(), sdb);
    SQL_BIND_TEXT(*stmt, index, params->op.bucket.info.placement_rule.storage_class.c_str(), sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.flags.c_str(), sdb);
    SQL_BIND_INT(*stmt, index, params->op.bucket.info.flags, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.zonegroup.c_str(), sdb);
    SQL_BIND_TEXT(*stmt, index, params->op.bucket.info.zonegroup.c_str(), sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.has_instance_obj.c_str(), sdb);
    SQL_BIND_INT(*stmt, index, params->op.bucket.info.has_instance_obj, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.quota.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(*stmt, index, params->op.bucket.info.quota, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.requester_pays.c_str(), sdb);
    SQL_BIND_INT(*stmt, index, params->op.bucket.info.requester_pays, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.has_website.c_str(), sdb);
    SQL_BIND_INT(*stmt, index, params->op.bucket.info.has_website, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.website_conf.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(*stmt, index, params->op.bucket.info.website_conf, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.swift_versioning.c_str(), sdb);
    SQL_BIND_INT(*stmt, index, params->op.bucket.info.swift_versioning, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.swift_ver_location.c_str(), sdb);
    SQL_BIND_TEXT(*stmt, index, params->op.bucket.info.swift_ver_location.c_str(), sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.mdsearch_config.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(*stmt, index, params->op.bucket.info.mdsearch_config, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.new_bucket_instance_id.c_str(), sdb);
    SQL_BIND_TEXT(*stmt, index, params->op.bucket.info.new_bucket_instance_id.c_str(), sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.obj_lock.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(*stmt, index, params->op.bucket.info.obj_lock, sdb);

    SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.sync_policy_info_groups.c_str(), sdb);
    SQL_ENCODE_BLOB_PARAM(*stmt, index, params->op.bucket.info.sync_policy, sdb);
  }

  SQL_BIND_INDEX(*stmt, index, p_params.op.user.user_id.c_str(), sdb);
  SQL_BIND_TEXT(*stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

  SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);
  SQL_BIND_TEXT(*stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.bucket_ver.c_str(), sdb);
  SQL_BIND_INT(*stmt, index, params->op.bucket.bucket_version.ver, sdb);

  SQL_BIND_INDEX(*stmt, index, p_params.op.bucket.mtime.c_str(), sdb);
  SQL_ENCODE_BLOB_PARAM(*stmt, index, params->op.bucket.mtime, sdb);

out:
  return rc;
}

int SQLUpdateBucket::Execute(struct DBOpParams *params)
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
    dbout(L_ERR)<<"In SQLUpdateBucket invalid query_str:" <<
      params->op.query_str << "\n";
    goto out;
  }

  SQL_EXECUTE(params, *stmt, NULL);
out:
  return ret;
}

int SQLRemoveBucket::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLRemoveBucket - no db\n";
    goto out;
  }

  p_params.bucket_table = params->bucket_table;

  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareRemoveBucket");

out:
  return ret;
}

int SQLRemoveBucket::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveBucket::Execute(struct DBOpParams *params)
{
  int ret = -1;

  objectmapDelete(params->op.bucket.info.bucket.name);

  SQL_EXECUTE(params, stmt, NULL);
out:
  return ret;
}

int SQLGetBucket::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLGetBucket - no db\n";
    goto out;
  }

  p_params.bucket_table = params->bucket_table;
  p_params.user_table = params->user_table;

  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareGetBucket");

out:
  return ret;
}

int SQLGetBucket::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

out:
  return rc;
}

int SQLGetBucket::Execute(struct DBOpParams *params)
{
  int ret = -1;

  params->op.name = "GetBucket";
  SQL_EXECUTE(params, stmt, list_bucket);
out:
  return ret;
}

int SQLListUserBuckets::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLListUserBuckets - no db\n";
    goto out;
  }

  p_params.bucket_table = params->bucket_table;

  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareListUserBuckets");

out:
  return ret;
}

int SQLListUserBuckets::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.op.user.user_id.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.user.uinfo.user_id.id.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.min_marker.c_str(), sdb);
  SQL_BIND_TEXT(stmt, index, params->op.bucket.min_marker.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.list_max_count.c_str(), sdb);
  SQL_BIND_INT(stmt, index, params->op.list_max_count, sdb);

out:
  return rc;
}

int SQLListUserBuckets::Execute(struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(params, stmt, list_bucket);
out:
  return ret;
}

int SQLInsertObject::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLInsertObject - no db\n";
    goto out;
  }

  bucket_name = params->op.bucket.info.bucket.name;
  p_params.object_table = bucket_name + ".object.table";

  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareInsertObject");

out:
  return ret;
}

int SQLInsertObject::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

out:
  return rc;
}

int SQLInsertObject::Execute(struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(params, stmt, NULL);
out:
  return ret;
}

int SQLRemoveObject::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLRemoveObject - no db\n";
    goto out;
  }

  bucket_name = params->op.bucket.info.bucket.name;
  p_params.object_table = bucket_name + ".object.table";
  copy.object_table = bucket_name + ".object.table";

  (void)createObjectTable(&copy);

  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareRemoveObject");

out:
  return ret;
}

int SQLRemoveObject::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

out:
  return rc;
}

int SQLRemoveObject::Execute(struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(params, stmt, NULL);
out:
  return ret;
}

int SQLListObject::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLListObject - no db\n";
    goto out;
  }

  bucket_name = params->op.bucket.info.bucket.name;
  p_params.object_table = bucket_name + ".object.table";
  copy.object_table = bucket_name + ".object.table";

  (void)createObjectTable(&copy);


  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareListObject");

out:
  return ret;
}

int SQLListObject::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

out:
  return rc;
}

int SQLListObject::Execute(struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(params, stmt, list_object);
out:
  return ret;
}

int SQLPutObjectData::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLPutObjectData - no db\n";
    goto out;
  }

  bucket_name = params->op.bucket.info.bucket.name;
  p_params.object_table = bucket_name + ".object.table";
  p_params.objectdata_table = bucket_name + ".objectdata.table";
  copy.object_table = bucket_name + ".object.table";
  copy.objectdata_table = bucket_name + ".objectdata.table";

  (void)createObjectDataTable(&copy);

  SQL_PREPARE(p_params, sdb, stmt, ret, "PreparePutObjectData");

out:
  return ret;
}

int SQLPutObjectData::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.offset.c_str(), sdb);

  SQL_BIND_INT(stmt, 3, params->offset, sdb);

  SQL_BIND_INDEX(stmt, index, p_params.data.c_str(), sdb);

  SQL_BIND_BLOB(stmt, index, params->data.c_str(), params->data.length(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.datalen.c_str(), sdb);

  SQL_BIND_INT(stmt, index, params->data.length(), sdb);

out:
  return rc;
}

int SQLPutObjectData::Execute(struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(params, stmt, NULL);
out:
  return ret;
}

int SQLGetObjectData::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLGetObjectData - no db\n";
    goto out;
  }

  bucket_name = params->op.bucket.info.bucket.name;
  p_params.object_table = bucket_name + ".object.table";
  p_params.objectdata_table = bucket_name + ".objectdata.table";
  copy.object_table = bucket_name + ".object.table";
  copy.objectdata_table = bucket_name + ".objectdata.table";

  (void)createObjectDataTable(&copy);

  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareGetObjectData");

out:
  return ret;
}

int SQLGetObjectData::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);
out:
  return rc;
}

int SQLGetObjectData::Execute(struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(params, stmt, get_objectdata);
out:
  return ret;
}

int SQLDeleteObjectData::Prepare(struct DBOpParams *params)
{
  int ret = -1;
  struct DBOpPrepareParams p_params = PrepareParams;
  struct DBOpParams copy = *params;
  string bucket_name;

  if (!*sdb) {
    dbout(L_ERR)<<"In SQLDeleteObjectData - no db\n";
    goto out;
  }

  bucket_name = params->op.bucket.info.bucket.name;
  p_params.object_table = bucket_name + ".object.table";
  p_params.objectdata_table = bucket_name + ".objectdata.table";
  copy.object_table = bucket_name + ".object.table";
  copy.objectdata_table = bucket_name + ".objectdata.table";

  (void)createObjectDataTable(&copy);

  SQL_PREPARE(p_params, sdb, stmt, ret, "PrepareDeleteObjectData");

out:
  return ret;
}

int SQLDeleteObjectData::Bind(struct DBOpParams *params)
{
  int index = -1;
  int rc = 0;
  struct DBOpPrepareParams p_params = PrepareParams;

  SQL_BIND_INDEX(stmt, index, p_params.object.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->object.c_str(), sdb);

  SQL_BIND_INDEX(stmt, index, p_params.op.bucket.bucket_name.c_str(), sdb);

  SQL_BIND_TEXT(stmt, index, params->op.bucket.info.bucket.name.c_str(), sdb);
out:
  return rc;
}

int SQLDeleteObjectData::Execute(struct DBOpParams *params)
{
  int ret = -1;

  SQL_EXECUTE(params, stmt, NULL);
out:
  return ret;
}
