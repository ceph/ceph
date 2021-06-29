// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "dbstore.h"

using namespace std;

map<string, class ObjectOp*> DBStore::objectmap = {};

map<string, class ObjectOp*> DBStore::getObjectMap() {
  return DBStore::objectmap;
}

/* Custom Logging initialization */
ofstream fileout;
ostream *dbout;
int LogLevel = L_FULLDEBUG;
string LogFile = "dbstore.log";

static void LogInit(string logfile, int loglevel) {
  if (loglevel >= L_ERR && loglevel <= L_FULLDEBUG)
    LogLevel = loglevel;

  if (!logfile.empty()) {
    LogFile = logfile;
  }

  fileout.open(LogFile);
  dbout = &fileout;

  return;
}

static void LogDestroy() {
  if(dbout && (dbout != &cout))
    fileout.close();
  return;
}

int DBStore::Initialize(string logfile, int loglevel)
{
  int ret = -1;

  LogInit(logfile, loglevel);

  db = openDB();

  if (!db) {
    dbout(L_ERR)<<"Failed to open database \n";
    return ret;
  }

  ret = LockInit();

  if (ret) {
    dbout(L_ERR)<<"Error: mutex is NULL \n";
    closeDB();
    db = NULL;
    return ret;
  }

  ret = InitializeDBOps();

  if (ret) {
    dbout(L_ERR)<<"InitializeDBOps failed \n";
    LockDestroy();
    closeDB();
    db = NULL;
    return ret;
  }

  dbout(L_FULLDEBUG)<< "DBStore successfully initialized - name:" \
    << db_name << "\n";

  return ret;
}

int DBStore::Destroy()
{
  if (!db)
    return 0;

  closeDB();

  LockDestroy();

  FreeDBOps();

  dbout(L_FULLDEBUG)<<"DBStore successfully destroyed - name:" \
    <<db_name<<"\n";

  LogDestroy();

  return 0;
}

int DBStore::LockInit() {
  int ret;

  ret = pthread_mutex_init(&mutex, NULL);

  if (ret)
    dbout(L_ERR)<<"pthread_mutex_init failed \n";

  return ret;
}

int DBStore::LockDestroy() {
  int ret;

  ret = pthread_mutex_destroy(&mutex);

  if (ret)
    dbout(L_ERR)<<"pthread_mutex_destroy failed \n";

  return ret;
}

int DBStore::Lock() {
  int ret;

  ret = pthread_mutex_lock(&mutex);

  if (ret)
    dbout(L_ERR)<<"pthread_mutex_lock failed \n";

  return ret;
}

int DBStore::Unlock() {
  int ret;

  ret = pthread_mutex_unlock(&mutex);

  if (ret)
    dbout(L_ERR)<<"pthread_mutex_unlock failed \n";

  return ret;
}

DBOp * DBStore::getDBOp(string Op, struct DBOpParams *params)
{
  if (!Op.compare("InsertUser"))
    return dbops.InsertUser;
  if (!Op.compare("RemoveUser"))
    return dbops.RemoveUser;
  if (!Op.compare("GetUser"))
    return dbops.GetUser;
  if (!Op.compare("InsertBucket"))
    return dbops.InsertBucket;
  if (!Op.compare("UpdateBucket"))
    return dbops.UpdateBucket;
  if (!Op.compare("RemoveBucket"))
    return dbops.RemoveBucket;
  if (!Op.compare("GetBucket"))
    return dbops.GetBucket;
  if (!Op.compare("ListUserBuckets"))
    return dbops.ListUserBuckets;

  /* Object Operations */
  map<string, class ObjectOp*>::iterator iter;
  class ObjectOp* Ob;

  iter = DBStore::objectmap.find(params->op.bucket.info.bucket.name);

  if (iter == DBStore::objectmap.end()) {
    dbout(L_EVENT)<<"No objectmap found for bucket: " \
      <<params->op.bucket.info.bucket.name<<"\n";
    /* not found */
    return NULL;
  }

  Ob = iter->second;

  if (!Op.compare("InsertObject"))
    return Ob->InsertObject;
  if (!Op.compare("RemoveObject"))
    return Ob->RemoveObject;
  if (!Op.compare("ListObject"))
    return Ob->ListObject;
  if (!Op.compare("PutObjectData"))
    return Ob->PutObjectData;
  if (!Op.compare("GetObjectData"))
    return Ob->GetObjectData;
  if (!Op.compare("DeleteObjectData"))
    return Ob->DeleteObjectData;

  return NULL;
}

int DBStore::objectmapInsert(string bucket, void *ptr)
{
  map<string, class ObjectOp*>::iterator iter;
  class ObjectOp *Ob;

  iter = DBStore::objectmap.find(bucket);

  if (iter != DBStore::objectmap.end()) {
    // entry already exists
    // return success or replace it or
    // return error ?
    // return success for now
    dbout(L_DEBUG)<<"Objectmap entry already exists for bucket("\
      <<bucket<<"). Not inserted \n";
    return 0;
  }

  Ob = (class ObjectOp*) ptr;
  Ob->InitializeObjectOps();

  DBStore::objectmap.insert(pair<string, class ObjectOp*>(bucket, Ob));

  return 0;
}

int DBStore::objectmapDelete(string bucket)
{
  map<string, class ObjectOp*>::iterator iter;
  class ObjectOp *Ob;

  iter = DBStore::objectmap.find(bucket);

  if (iter == DBStore::objectmap.end()) {
    // entry doesn't exist
    // return success or return error ?
    // return success for now
    dbout(L_DEBUG)<<"Objectmap entry for bucket("<<bucket<<") "
      <<"doesnt exist to delete \n";
    return 0;
  }

  Ob = (class ObjectOp*) (iter->second);
  Ob->FreeObjectOps();

  DBStore::objectmap.erase(iter);

  return 0;
}

int DBStore::InitializeParams(string Op, DBOpParams *params)
{
  int ret = -1;

  if (!params)
    goto out;

  //reset params here
  params->user_table = user_table;
  params->bucket_table = bucket_table;

  ret = 0;
out:
  return ret;
}

int DBStore::ProcessOp(string Op, struct DBOpParams *params) {
  int ret = -1;
  class DBOp *db_op;

  Lock();
  db_op = getDBOp(Op, params);

  if (!db_op) {
    dbout(L_ERR)<<"No db_op found for Op("<<Op<<")\n";
    Unlock();
    return ret;
  }
  ret = db_op->Execute(params);

  Unlock();
  if (ret) {
    dbout(L_ERR)<<"In Process op Execute failed for fop(" \
      <<Op.c_str()<<") \n";
  } else {
    dbout(L_FULLDEBUG)<<"Successfully processed fop(" \
      <<Op.c_str()<<") \n";
  }

  return ret;
}

int DBStore::get_user(const std::string& query_str, const std::string& query_str_val,
    RGWUserInfo& uinfo, map<string, bufferlist> *pattrs,
    RGWObjVersionTracker *pobjv_tracker) {
  int ret = 0;

  if (query_str.empty()) {
    // not checking for query_str_val as the query can be to fetch
    // entries with null values
    return -1;
  }

  DBOpParams params = {};
  InitializeParams("GetUser", &params);

  params.op.query_str = query_str;

  // validate query_str with UserTable entries names
  if (query_str == "username") {
    params.op.user.uinfo.display_name = query_str_val;
  } else if (query_str == "email") {
    params.op.user.uinfo.user_email = query_str_val;
  } else if (query_str == "access_key") {
    RGWAccessKey k(query_str_val, "");
    map<string, RGWAccessKey> keys;
    keys[query_str_val] = k;
    params.op.user.uinfo.access_keys = keys;
  } else if (query_str == "user_id") {
    params.op.user.uinfo.user_id = uinfo.user_id;
  } else {
    dbout(L_ERR)<<"In GetUser Invalid query string :" <<query_str.c_str()<<") \n";
    return -1;
  }

  ret = ProcessOp("GetUser", &params);

  if (ret)
    goto out;

  uinfo = params.op.user.uinfo;

  if (pattrs) {
    *pattrs = params.op.user.user_attrs;
  }

  if (pobjv_tracker) {
    pobjv_tracker->read_version = params.op.user.user_version;
  }

out:
  return ret;
}

int DBStore::store_user(RGWUserInfo& uinfo, bool exclusive, map<string, bufferlist> *pattrs,
    RGWObjVersionTracker *pobjv, RGWUserInfo* pold_info)
{
  DBOpParams params = {};
  InitializeParams("CreateUser", &params);
  int ret = 0;

  /* Check if the user already exists and return the old info, caller will have a use for it */
  RGWUserInfo orig_info;
  RGWObjVersionTracker objv_tracker = {};
  obj_version& obj_ver = objv_tracker.read_version;

  orig_info.user_id = uinfo.user_id;
  ret = get_user(string("user_id"), "", orig_info, nullptr, &objv_tracker);

  if (!ret && obj_ver.ver) {
    /* already exists. */

    if (pold_info) {
      *pold_info = orig_info;
    }

    if (pobjv && (pobjv->read_version.ver != obj_ver.ver)) {
      /* Object version mismatch.. return ECANCELED */
      ret = -ECANCELED;
      dbout(L_ERR)<<"User Read version mismatch err:(" <<ret<<") \n";
      return ret;
    }

    if (exclusive) {
      // return
      return ret;
    }
    obj_ver.ver++;
  } else {
    obj_ver.ver = 1;
    obj_ver.tag = "UserTAG";
  }

  params.op.user.user_version = obj_ver;
  params.op.user.uinfo = uinfo;

  if (pattrs) {
    params.op.user.user_attrs = *pattrs;
  }

  ret = ProcessOp("InsertUser", &params);

  if (ret) {
    dbout(L_ERR)<<"store_user failed with err:(" <<ret<<") \n";
    goto out;
  }

  if (pobjv) {
    pobjv->read_version = obj_ver;
    pobjv->write_version = obj_ver;
  }

out:
  return ret;
}

int DBStore::remove_user(RGWUserInfo& uinfo, RGWObjVersionTracker *pobjv)
{
  DBOpParams params = {};
  InitializeParams("CreateUser", &params);
  int ret = 0;

  RGWUserInfo orig_info;
  RGWObjVersionTracker objv_tracker = {};

  orig_info.user_id = uinfo.user_id;
  ret = get_user(string("user_id"), "", orig_info, nullptr, &objv_tracker);

  if (!ret && objv_tracker.read_version.ver) {
    /* already exists. */

    if (pobjv && (pobjv->read_version.ver != objv_tracker.read_version.ver)) {
      /* Object version mismatch.. return ECANCELED */
      ret = -ECANCELED;
      dbout(L_ERR)<<"User Read version mismatch err:(" <<ret<<") \n";
      return ret;
    }
  }

  params.op.user.uinfo.user_id = uinfo.user_id;

  ret = ProcessOp("RemoveUser", &params);

  if (ret) {
    dbout(L_ERR)<<"remove_user failed with err:(" <<ret<<") \n";
    goto out;
  }

out:
  return ret;
}

int DBStore::get_bucket_info(const std::string& query_str,
    const std::string& query_str_val,
    RGWBucketInfo& info,
    rgw::sal::Attrs* pattrs, ceph::real_time* pmtime,
    obj_version* pbucket_version) {
  int ret = 0;

  if (query_str.empty()) {
    // not checking for query_str_val as the query can be to fetch
    // entries with null values
    return -1;
  }

  DBOpParams params = {};
  DBOpParams params2 = {};
  InitializeParams("GetBucket", &params);

  if (query_str == "name") {
    params.op.bucket.info.bucket.name = info.bucket.name;
  } else {
    dbout(L_ERR)<<"In GetBucket Invalid query string :" <<query_str.c_str()<<") \n";
    return -1;
  }

  ret = ProcessOp("GetBucket", &params);

  if (ret) {
    dbout(L_ERR)<<"In GetBucket failed err:(" <<ret<<") \n";
    goto out;
  }

  if (!ret && params.op.bucket.info.bucket.marker.empty()) {
    return -ENOENT;
  }
  info = params.op.bucket.info;

  if (pattrs) {
    *pattrs = params.op.bucket.bucket_attrs;
  }

  if (pmtime) {
    *pmtime = params.op.bucket.mtime;
  }
  if (pbucket_version) {
    *pbucket_version = params.op.bucket.bucket_version;
  }

out:
  return ret;
}

int DBStore::create_bucket(const RGWUserInfo& owner, rgw_bucket& bucket,
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
    const DoutPrefixProvider *dpp,
    bool exclusive)
{
  /*
   * XXX: Simple creation for now.
   *
   * Referring to RGWRados::create_bucket(), 
   * Check if bucket already exists, select_bucket_placement,
   * is explicit put/remove instance info needed? - should not be ideally
   */

  DBOpParams params = {};
  InitializeParams("CreateBucket", &params);
  int ret = 0;

  /* Check if the bucket already exists and return the old info, caller will have a use for it */
  RGWBucketInfo orig_info;
  orig_info.bucket.name = bucket.name;
  ret = get_bucket_info(string("name"), "", orig_info, nullptr, nullptr, nullptr);

  if (!ret && !orig_info.owner.id.empty() && exclusive) {
    /* already exists. Return the old info */

    info = std::move(orig_info);
    return ret;
  }

  RGWObjVersionTracker& objv_tracker = info.objv_tracker;

  objv_tracker.read_version.clear();

  if (pobjv) {
    objv_tracker.write_version = *pobjv;
  } else {
    objv_tracker.generate_new_write_ver(cct);
  }
  params.op.bucket.bucket_version = objv_tracker.write_version;
  objv_tracker.read_version = params.op.bucket.bucket_version;

  uint64_t bid = next_bucket_id();
  string s = getDBname() + "." + std::to_string(bid);
  bucket.marker = bucket.bucket_id = s;

  info.bucket = bucket;
  info.owner = owner.user_id;
  info.zonegroup = zonegroup_id;
  info.placement_rule = placement_rule;
  info.swift_ver_location = swift_ver_location;
  info.swift_versioning = (!swift_ver_location.empty());

  info.requester_pays = false;
  if (real_clock::is_zero(creation_time)) {
    info.creation_time = ceph::real_clock::now();
  } else {
    info.creation_time = creation_time;
  }
  if (pquota_info) {
    info.quota = *pquota_info;
  }

  params.op.bucket.info = info;
  params.op.bucket.bucket_attrs = attrs;
  params.op.bucket.mtime = ceph::real_time();
  params.op.user.uinfo.user_id.id = owner.user_id.id;

  ret = ProcessOp("InsertBucket", &params);

  if (ret) {
    dbout(L_ERR)<<"create_bucket failed with err:(" <<ret<<") \n";
    goto out;
  }

out:
  return ret;
}

int DBStore::remove_bucket(const RGWBucketInfo info) {
  int ret = 0;

  DBOpParams params = {};
  InitializeParams("RemoveBucket", &params);

  params.op.bucket.info.bucket.name = info.bucket.name;

  ret = ProcessOp("RemoveBucket", &params);

  if (ret) {
    dbout(L_ERR)<<"In RemoveBucket failed err:(" <<ret<<") \n";
    goto out;
  }

out:
  return ret;
}

int DBStore::list_buckets(const rgw_user& user,
    const string& marker,
    const string& end_marker,
    uint64_t max,
    bool need_stats,
    RGWUserBuckets *buckets,
    bool *is_truncated)
{
  int ret = 0;

  DBOpParams params = {};
  InitializeParams("ListUserBuckets", &params);

  params.op.user.uinfo.user_id = user;
  params.op.bucket.min_marker = marker;
  params.op.bucket.max_marker = end_marker;
  params.op.list_max_count = max;

  ret = ProcessOp("ListUserBuckets", &params);

  if (ret) {
    dbout(L_ERR)<<"In ListUserBuckets failed err:(" <<ret<<") \n";
    goto out;
  }

  /* need_stats: stats are already part of entries... In case they are maintained in
   * separate table , maybe use "Inner Join" with stats table for the query.
   */
  if (params.op.bucket.list_entries.size() == max)
    *is_truncated = true;

  for (auto& entry : params.op.bucket.list_entries) {
    if (!end_marker.empty() &&
        end_marker.compare(entry.bucket.marker) <= 0) {
      *is_truncated = false;
      break;
    }
    buckets->add(std::move(entry));
    /*  cout << "entry.bucket.marker: " << entry.bucket.marker << " min_marker: " << marker;

        if (entry.bucket.marker < marker) {
        cout << " lesser" << "\n";
        } else if (entry.bucket.marker > marker) {
        cout << " greater" << "\n";
        } else {
        cout << " equal" << "\n";
        } */
  }
out:
  return ret;
}

int DBStore::update_bucket(const std::string& query_str,
    RGWBucketInfo& info,
    bool exclusive,
    const rgw_user* powner_id,
    map<std::string, bufferlist>* pattrs,
    ceph::real_time* pmtime,
    RGWObjVersionTracker* pobjv)
{
  int ret = 0;
  DBOpParams params = {};
  obj_version bucket_version;
  RGWBucketInfo orig_info;

  /* Check if the bucket already exists and return the old info, caller will have a use for it */
  orig_info.bucket.name = info.bucket.name;
  params.op.bucket.info.bucket.name = info.bucket.name;
  ret = get_bucket_info(string("name"), "", orig_info, nullptr, nullptr,
      &bucket_version);

  if (ret) {
    dbout(L_ERR)<<"Failed to read bucket info err:(" <<ret<<") \n";
    goto out;
  }

  if (!orig_info.owner.id.empty() && exclusive) {
    /* already exists. Return the old info */

    info = std::move(orig_info);
    return ret;
  }

  /* Verify if the objv read_ver matches current bucket version */
  if (pobjv) {
    if (pobjv->read_version.ver != bucket_version.ver) {
      dbout(L_ERR)<<"Read version mismatch err:(" <<ret<<") \n";
      ret = -ECANCELED;
      goto out;
    }
  } else {
    pobjv = &info.objv_tracker;
  }

  InitializeParams("UpdateBucket", &params);

  params.op.bucket.info.bucket.name = info.bucket.name;

  if (powner_id) {
    params.op.user.uinfo.user_id.id = powner_id->id;
  } else {
    params.op.user.uinfo.user_id.id = orig_info.owner.id;
  }

  /* Update version & mtime */
  params.op.bucket.bucket_version.ver = ++(bucket_version.ver);

  if (pmtime) {
    params.op.bucket.mtime = *pmtime;;
  } else {
    params.op.bucket.mtime = ceph::real_time();
  }

  if (query_str == "attrs") {
    params.op.query_str = "attrs";
    params.op.bucket.bucket_attrs = *pattrs;
  } else if (query_str == "owner") {
    /* Update only owner i.e, chown. 
     * Update creation_time too */
    params.op.query_str = "owner";
    params.op.bucket.info.creation_time = params.op.bucket.mtime;
  } else if (query_str == "info") {
    params.op.query_str = "info";
    params.op.bucket.info = info;
  } else {
    ret = -1;
    dbout(L_ERR)<<"In UpdateBucket Invalid query_str : " << query_str <<" \n";
    goto out;
  }

  ret = ProcessOp("UpdateBucket", &params);

  if (ret) {
    dbout(L_ERR)<<"In UpdateBucket failed err:(" <<ret<<") \n";
    goto out;
  }

  if (pobjv) {
    pobjv->read_version = params.op.bucket.bucket_version;
    pobjv->write_version = params.op.bucket.bucket_version;
  }

out:
  return ret;
}
