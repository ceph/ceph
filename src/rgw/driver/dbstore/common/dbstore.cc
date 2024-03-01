// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "dbstore.h"

using namespace std;

namespace rgw { namespace store {

map<string, class ObjectOp*> DB::objectmap = {};

map<string, class ObjectOp*> DB::getObjectMap() {
  return DB::objectmap;
}

int DB::Initialize(string logfile, int loglevel)
{
  int ret = -1;
  const DoutPrefixProvider *dpp = get_def_dpp();

  if (!cct) {
    cout << "Failed to Initialize. No ceph Context \n";
    return -1;
  }

  if (loglevel > 0) {
    cct->_conf->subsys.set_log_level(ceph_subsys_rgw, loglevel);
  }
  if (!logfile.empty()) {
    cct->_log->set_log_file(logfile);
    cct->_log->reopen_log_file();
  }


  db = openDB(dpp);

  if (!db) {
    ldpp_dout(dpp, 0) <<"Failed to open database " << dendl;
    return ret;
  }

  ret = InitializeDBOps(dpp);

  if (ret) {
    ldpp_dout(dpp, 0) <<"InitializeDBOps failed " << dendl;
    closeDB(dpp);
    db = NULL;
    return ret;
  }

  ldpp_dout(dpp, 0) << "DB successfully initialized - name:" \
    << db_name << "" << dendl;

  return ret;
}

int DB::createGC(const DoutPrefixProvider *dpp) {
  int ret = 0;
  /* create gc thread */

  gc_worker = std::make_unique<DB::GC>(dpp, this);
  gc_worker->create("db_gc");

  return ret;
}

int DB::stopGC() {
  if (gc_worker) {
    gc_worker->signal_stop();
    gc_worker->join();
  }
  return 0;
}

int DB::Destroy(const DoutPrefixProvider *dpp)
{
  if (!db)
    return 0;

  stopGC();

  closeDB(dpp);


  ldpp_dout(dpp, 20)<<"DB successfully destroyed - name:" \
    <<db_name << dendl;

  return 0;
}


std::shared_ptr<class DBOp> DB::getDBOp(const DoutPrefixProvider *dpp, std::string_view Op,
                  const DBOpParams *params)
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
  if (!Op.compare("InsertLCEntry"))
    return dbops.InsertLCEntry;
  if (!Op.compare("RemoveLCEntry"))
    return dbops.RemoveLCEntry;
  if (!Op.compare("GetLCEntry"))
    return dbops.GetLCEntry;
  if (!Op.compare("ListLCEntries"))
    return dbops.ListLCEntries;
  if (!Op.compare("InsertLCHead"))
    return dbops.InsertLCHead;
  if (!Op.compare("RemoveLCHead"))
    return dbops.RemoveLCHead;
  if (!Op.compare("GetLCHead"))
    return dbops.GetLCHead;

  /* Object Operations */
  map<string, class ObjectOp*>::iterator iter;
  class ObjectOp* Ob;

  {
    const std::lock_guard<std::mutex> lk(mtx);
    iter = DB::objectmap.find(params->op.bucket.info.bucket.name);
  }

  if (iter == DB::objectmap.end()) {
    ldpp_dout(dpp, 30)<<"No objectmap found for bucket: " \
      <<params->op.bucket.info.bucket.name << dendl;
    /* not found */
    return nullptr;
  }

  Ob = iter->second;

  if (!Op.compare("PutObject"))
    return Ob->PutObject;
  if (!Op.compare("DeleteObject"))
    return Ob->DeleteObject;
  if (!Op.compare("GetObject"))
    return Ob->GetObject;
  if (!Op.compare("UpdateObject"))
    return Ob->UpdateObject;
  if (!Op.compare("ListBucketObjects"))
    return Ob->ListBucketObjects;
  if (!Op.compare("ListVersionedObjects"))
    return Ob->ListVersionedObjects;
  if (!Op.compare("PutObjectData"))
    return Ob->PutObjectData;
  if (!Op.compare("UpdateObjectData"))
    return Ob->UpdateObjectData;
  if (!Op.compare("GetObjectData"))
    return Ob->GetObjectData;
  if (!Op.compare("DeleteObjectData"))
    return Ob->DeleteObjectData;
  if (!Op.compare("DeleteStaleObjectData"))
    return Ob->DeleteStaleObjectData;

  return nullptr;
}

int DB::objectmapInsert(const DoutPrefixProvider *dpp, string bucket, class ObjectOp* ptr)
{
  map<string, class ObjectOp*>::iterator iter;
  class ObjectOp *Ob;

  const std::lock_guard<std::mutex> lk(mtx);
  iter = DB::objectmap.find(bucket);

  if (iter != DB::objectmap.end()) {
    // entry already exists
    // return success or replace it or
    // return error ?
    //
    // return success for now & delete the newly allocated ptr
    ldpp_dout(dpp, 30)<<"Objectmap entry already exists for bucket("\
      <<bucket<<"). Not inserted " << dendl;
    delete ptr;
    return 0;
  }

  Ob = (class ObjectOp*) ptr;
  Ob->InitializeObjectOps(getDBname(), dpp);

  DB::objectmap.insert(pair<string, class ObjectOp*>(bucket, Ob));

  return 0;
}

int DB::objectmapDelete(const DoutPrefixProvider *dpp, string bucket)
{
  map<string, class ObjectOp*>::iterator iter;

  const std::lock_guard<std::mutex> lk(mtx);
  iter = DB::objectmap.find(bucket);

  if (iter == DB::objectmap.end()) {
    // entry doesn't exist
    // return success or return error ?
    // return success for now
    ldpp_dout(dpp, 20)<<"Objectmap entry for bucket("<<bucket<<") "
      <<"doesnt exist to delete " << dendl;
    return 0;
  }

  DB::objectmap.erase(iter);

  return 0;
}

int DB::InitializeParams(const DoutPrefixProvider *dpp, DBOpParams *params)
{
  int ret = -1;

  if (!params)
    goto out;

  params->cct = cct;

  //reset params here
  params->user_table = user_table;
  params->bucket_table = bucket_table;
  params->quota_table = quota_table;
  params->lc_entry_table = lc_entry_table;
  params->lc_head_table = lc_head_table;

  ret = 0;
out:
  return ret;
}

int DB::ProcessOp(const DoutPrefixProvider *dpp, std::string_view Op, DBOpParams *params) {
  int ret = -1;
  shared_ptr<class DBOp> db_op;

  db_op = getDBOp(dpp, Op, params);

  if (!db_op) {
    ldpp_dout(dpp, 0)<<"No db_op found for Op("<<Op<<")" << dendl;
    return ret;
  }
  ret = db_op->Execute(dpp, params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In Process op Execute failed for fop(" << Op << ")" << dendl;
  } else {
    ldpp_dout(dpp, 20)<<"Successfully processed fop(" << Op << ")" << dendl;
  }

  return ret;
}

int DB::get_user(const DoutPrefixProvider *dpp,
    const std::string& query_str, const std::string& query_str_val,
    RGWUserInfo& uinfo, map<string, bufferlist> *pattrs,
    RGWObjVersionTracker *pobjv_tracker) {
  int ret = 0;

  if (query_str.empty() || query_str_val.empty()) {
    ldpp_dout(dpp, 0)<<"In GetUser - Invalid query(" << query_str <<"), query_str_val(" << query_str_val <<")" << dendl;
    return -1;
  }

  DBOpParams params = {};
  InitializeParams(dpp, &params);

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
    ldpp_dout(dpp, 0)<<"In GetUser Invalid query string :" <<query_str.c_str()<<") " << dendl;
    return -1;
  }

  ret = ProcessOp(dpp, "GetUser", &params);

  if (ret)
    goto out;

  /* Verify if its a valid user */
  if (params.op.user.uinfo.access_keys.empty() ||
        params.op.user.uinfo.user_id.id.empty()) {
    ldpp_dout(dpp, 0)<<"In GetUser - No user with query(" <<query_str.c_str()<<"), user_id(" << uinfo.user_id <<") found" << dendl;
    return -ENOENT;
  }

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

int DB::store_user(const DoutPrefixProvider *dpp,
    RGWUserInfo& uinfo, bool exclusive, map<string, bufferlist> *pattrs,
    RGWObjVersionTracker *pobjv, RGWUserInfo* pold_info)
{
  DBOpParams params = {};
  InitializeParams(dpp, &params);
  int ret = 0;

  /* Check if the user already exists and return the old info, caller will have a use for it */
  RGWUserInfo orig_info;
  RGWObjVersionTracker objv_tracker = {};
  obj_version& obj_ver = objv_tracker.read_version;

  orig_info.user_id = uinfo.user_id;
  ret = get_user(dpp, string("user_id"), uinfo.user_id.id, orig_info, nullptr, &objv_tracker);

  if (!ret && obj_ver.ver) {
    /* already exists. */

    if (pold_info) {
      *pold_info = orig_info;
    }

    if (pobjv && (pobjv->read_version.ver != obj_ver.ver)) {
      /* Object version mismatch.. return ECANCELED */
      ret = -ECANCELED;
      ldpp_dout(dpp, 0)<<"User Read version mismatch err:(" <<ret<<") " << dendl;
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

  ret = ProcessOp(dpp, "InsertUser", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"store_user failed with err:(" <<ret<<") " << dendl;
    goto out;
  }
  ldpp_dout(dpp, 20)<<"User creation successful - userid:(" <<uinfo.user_id<<") " << dendl;

  if (pobjv) {
    pobjv->read_version = obj_ver;
    pobjv->write_version = obj_ver;
  }

out:
  return ret;
}

int DB::remove_user(const DoutPrefixProvider *dpp,
    RGWUserInfo& uinfo, RGWObjVersionTracker *pobjv)
{
  DBOpParams params = {};
  InitializeParams(dpp, &params);
  int ret = 0;

  RGWUserInfo orig_info;
  RGWObjVersionTracker objv_tracker = {};

  orig_info.user_id = uinfo.user_id;
  ret = get_user(dpp, string("user_id"), uinfo.user_id.id, orig_info, nullptr, &objv_tracker);

  if (ret) {
    return ret;
  }

  if (!ret && objv_tracker.read_version.ver) {
    /* already exists. */

    if (pobjv && (pobjv->read_version.ver != objv_tracker.read_version.ver)) {
      /* Object version mismatch.. return ECANCELED */
      ret = -ECANCELED;
      ldpp_dout(dpp, 0)<<"User Read version mismatch err:(" <<ret<<") " << dendl;
      return ret;
    }
  }

  params.op.user.uinfo.user_id = uinfo.user_id;

  ret = ProcessOp(dpp, "RemoveUser", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"remove_user failed with err:(" <<ret<<") " << dendl;
    goto out;
  }

out:
  return ret;
}

int DB::get_bucket_info(const DoutPrefixProvider *dpp, const std::string& query_str,
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
  InitializeParams(dpp, &params);

  if (query_str == "name") {
    params.op.bucket.info.bucket.name = info.bucket.name;
  } else {
    ldpp_dout(dpp, 0)<<"In GetBucket Invalid query string :" <<query_str.c_str()<<") " << dendl;
    return -1;
  }

  ret = ProcessOp(dpp, "GetBucket", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In GetBucket failed err:(" <<ret<<") " << dendl;
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

int DB::create_bucket(const DoutPrefixProvider *dpp,
    const rgw_user& owner, const rgw_bucket& bucket,
    const std::string& zonegroup_id,
    const rgw_placement_rule& placement_rule,
    const std::map<std::string, bufferlist>& attrs,
    const std::optional<std::string>& swift_ver_location,
    const std::optional<RGWQuotaInfo>& quota,
    std::optional<ceph::real_time> creation_time,
    obj_version *pep_objv,
    RGWBucketInfo& info,
    optional_yield y)
{
  /*
   * XXX: Simple creation for now.
   *
   * Referring to RGWRados::create_bucket(), 
   * Check if bucket already exists, select_bucket_placement,
   * is explicit put/remove instance info needed? - should not be ideally
   */

  DBOpParams params = {};
  InitializeParams(dpp, &params);
  int ret = 0;

  /* Check if the bucket already exists and return the old info, caller will have a use for it */
  RGWBucketInfo orig_info;
  orig_info.bucket.name = bucket.name;
  ret = get_bucket_info(dpp, string("name"), "", orig_info, nullptr, nullptr, nullptr);

  if (!ret && !orig_info.owner.id.empty()) {
    /* already exists. Return the old info */
    info = std::move(orig_info);
    return ret;
  }

  RGWObjVersionTracker& objv_tracker = info.objv_tracker;
  objv_tracker.read_version.clear();
  objv_tracker.generate_new_write_ver(cct);

  params.op.bucket.bucket_version = objv_tracker.write_version;
  objv_tracker.read_version = params.op.bucket.bucket_version;

  info.bucket = bucket;
  if (info.bucket.marker.empty()) {
    uint64_t bid = next_bucket_id();
    string s = getDBname() + "." + std::to_string(bid);
    info.bucket.marker = info.bucket.bucket_id = s;
  }

  info.owner = owner;
  info.zonegroup = zonegroup_id;
  info.placement_rule = placement_rule;
  if (swift_ver_location) {
    info.swift_ver_location = *swift_ver_location;
  }
  info.swift_versioning = swift_ver_location.has_value();

  info.requester_pays = false;
  if (creation_time) {
    info.creation_time = *creation_time;
  } else {
    info.creation_time = ceph::real_clock::now();
  }
  if (quota) {
    info.quota = *quota;
  }

  params.op.bucket.info = info;
  params.op.bucket.bucket_attrs = attrs;
  params.op.bucket.mtime = ceph::real_time();
  params.op.user.uinfo.user_id.id = owner.id;

  ret = ProcessOp(dpp, "InsertBucket", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"create_bucket failed with err:(" <<ret<<") " << dendl;
    goto out;
  }

out:
  return ret;
}

int DB::remove_bucket(const DoutPrefixProvider *dpp, const RGWBucketInfo info) {
  int ret = 0;

  DBOpParams params = {};
  InitializeParams(dpp, &params);

  params.op.bucket.info.bucket.name = info.bucket.name;

  ret = ProcessOp(dpp, "RemoveBucket", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In RemoveBucket failed err:(" <<ret<<") " << dendl;
    goto out;
  }

out:
  return ret;
}

int DB::list_buckets(const DoutPrefixProvider *dpp, const std::string& query_str,
    rgw_user& user,
    const string& marker,
    const string& end_marker,
    uint64_t max,
    bool need_stats,
    RGWUserBuckets *buckets,
    bool *is_truncated)
{
  int ret = 0;

  DBOpParams params = {};
  InitializeParams(dpp, &params);

  params.op.user.uinfo.user_id = user;
  params.op.bucket.min_marker = marker;
  params.op.bucket.max_marker = end_marker;
  params.op.list_max_count = max;
  params.op.query_str = query_str;

  ret = ProcessOp(dpp, "ListUserBuckets", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In ListUserBuckets failed err:(" <<ret<<") " << dendl;
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
  }

  if (query_str == "all") {
    // userID/OwnerID may have changed. Update it.
    user.id = params.op.bucket.info.owner.id;
  }

out:
  return ret;
}

int DB::update_bucket(const DoutPrefixProvider *dpp, const std::string& query_str,
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
  ret = get_bucket_info(dpp, string("name"), "", orig_info, nullptr, nullptr,
      &bucket_version);

  if (ret) {
    ldpp_dout(dpp, 0)<<"Failed to read bucket info err:(" <<ret<<") " << dendl;
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
      ldpp_dout(dpp, 0)<<"Read version mismatch err:(" <<ret<<") " << dendl;
      ret = -ECANCELED;
      goto out;
    }
  } else {
    pobjv = &info.objv_tracker;
  }

  InitializeParams(dpp, &params);

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
    ldpp_dout(dpp, 0)<<"In UpdateBucket Invalid query_str : " << query_str << dendl;
    goto out;
  }

  ret = ProcessOp(dpp, "UpdateBucket", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In UpdateBucket failed err:(" <<ret<<") " << dendl;
    goto out;
  }

  if (pobjv) {
    pobjv->read_version = params.op.bucket.bucket_version;
    pobjv->write_version = params.op.bucket.bucket_version;
  }

out:
  return ret;
}

/**
 * Get ordered listing of the objects in a bucket.
 *
 * max_p: maximum number of results to return
 * bucket: bucket to list contents of
 * prefix: only return results that match this prefix
 * delim: do not include results that match this string.
 *     Any skipped results will have the matching portion of their name
 *     inserted in common_prefixes with a "true" mark.
 * marker: if filled in, begin the listing with this object.
 * end_marker: if filled in, end the listing with this object.
 * result: the objects are put in here.
 * common_prefixes: if delim is filled in, any matching prefixes are
 * placed here.
 * is_truncated: if number of objects in the bucket is bigger than
 * max, then truncated.
 */
int DB::Bucket::List::list_objects(const DoutPrefixProvider *dpp, int64_t max,
		           vector<rgw_bucket_dir_entry> *result,
		           map<string, bool> *common_prefixes, bool *is_truncated)
{
  int ret = 0;
  DB *store = target->get_store();
  int64_t count = 0;
  std::string prev_obj;

  DBOpParams db_params = {};
  store->InitializeParams(dpp, &db_params);

  db_params.op.bucket.info = target->get_bucket_info(); 
  /* XXX: Handle whole marker? key -> name, instance, ns? */
  db_params.op.obj.min_marker = params.marker.name;
  db_params.op.obj.max_marker = params.end_marker.name;
  db_params.op.obj.prefix = params.prefix + "%";
  db_params.op.list_max_count = max + 1; /* +1 for next_marker */

  ret = store->ProcessOp(dpp, "ListBucketObjects", &db_params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In ListBucketObjects failed err:(" <<ret<<") " << dendl;
    goto out;
  }

  for (auto& entry : db_params.op.obj.list_entries) {

    if (!params.list_versions) {
      if (entry.flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER) {
        prev_obj = entry.key.name;
        // skip all non-current entries and delete_marker
        continue;
      }
      if (entry.key.name == prev_obj) {
        // non current versions..skip the entry
        continue;
      }
      entry.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
    } else {
      if (entry.key.name != prev_obj) {
        // current version
        entry.flags |= rgw_bucket_dir_entry::FLAG_CURRENT;
      } else {
        entry.flags &= ~(rgw_bucket_dir_entry::FLAG_CURRENT);
        entry.flags |= rgw_bucket_dir_entry::FLAG_VER;
      }
    }

    prev_obj = entry.key.name;

    if (count >= max) {
      *is_truncated = true;
      next_marker.name = entry.key.name;
      next_marker.instance = entry.key.instance;
      break;
    }

    if (!params.delim.empty()) {
    const std::string& objname = entry.key.name;
	const int delim_pos = objname.find(params.delim, params.prefix.size());
	  if (delim_pos >= 0) {
	    /* extract key -with trailing delimiter- for CommonPrefix */
	    const std::string& prefix_key =
	      objname.substr(0, delim_pos + params.delim.length());

	    if (common_prefixes &&
		common_prefixes->find(prefix_key) == common_prefixes->end()) {
          next_marker = prefix_key;
          (*common_prefixes)[prefix_key] = true;
          count++;
        }
        continue;
      }
    }

    if (!params.end_marker.name.empty() &&
        params.end_marker.name.compare(entry.key.name) <= 0) {
      // should not include end_marker
      *is_truncated = false;
      break;
    }
    count++;
    result->push_back(std::move(entry));
  }
out:
  return ret;
}

int DB::raw_obj::InitializeParamsfromRawObj(const DoutPrefixProvider *dpp,
                                            DBOpParams* params) {
  int ret = 0;

  if (!params)
    return -1;

  params->op.bucket.info.bucket.name = bucket_name;
  params->op.obj.state.obj.key.name = obj_name;
  params->op.obj.state.obj.key.instance = obj_instance;
  params->op.obj.state.obj.key.ns = obj_ns;
  params->op.obj.obj_id = obj_id;

  if (multipart_part_str != "0.0") {
    params->op.obj.is_multipart = true;
  } else {
    params->op.obj.is_multipart = false;
  }

  params->op.obj_data.multipart_part_str = multipart_part_str;
  params->op.obj_data.part_num = part_num;

  return ret;
}

int DB::Object::InitializeParamsfromObject(const DoutPrefixProvider *dpp,
                                           DBOpParams* params) {
  int ret = 0;
  string bucket = bucket_info.bucket.name;

  if (!params)
    return -1;

  params->op.bucket.info.bucket.name = bucket;
  params->op.obj.state.obj = obj;
  params->op.obj.obj_id = obj_id;

  return ret;
}

int DB::Object::get_object_impl(const DoutPrefixProvider *dpp, DBOpParams& params) {
  int ret = 0;

  if (params.op.obj.state.obj.key.name.empty()) {
    /* Initialize */
    store->InitializeParams(dpp, &params);
    InitializeParamsfromObject(dpp, &params);
  }

  ret = store->ProcessOp(dpp, "GetObject", &params);

  /* pick one field check if object exists */
  if (!ret && !params.op.obj.state.exists) {
    ldpp_dout(dpp, 0)<<"Object(bucket:" << bucket_info.bucket.name << ", Object:"<< obj.key.name << ") doesn't exist" << dendl;
    ret = -ENOENT;
  }

  return ret;
}

int DB::Object::obj_omap_set_val_by_key(const DoutPrefixProvider *dpp,
                                        const std::string& key, bufferlist& val,
                                        bool must_exist) {
  int ret = 0;

  DBOpParams params = {};

  ret = get_object_impl(dpp, params);

  if (ret) {
    ldpp_dout(dpp, 0) <<"get_object_impl failed err:(" <<ret<<")" << dendl;
    goto out;
  }

  params.op.obj.omap[key] = val;
  params.op.query_str = "omap";
  params.op.obj.state.mtime = real_clock::now();

  ret = store->ProcessOp(dpp, "UpdateObject", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In UpdateObject failed err:(" <<ret<<") " << dendl;
    goto out;
  }

out:
  return ret;
}

int DB::Object::obj_omap_get_vals_by_keys(const DoutPrefixProvider *dpp,
                                          const std::string& oid,
                                          const std::set<std::string>& keys,
                                          std::map<std::string, bufferlist>* vals)
{
  int ret = 0;
  DBOpParams params = {};
  std::map<std::string, bufferlist> omap;

  if (!vals)
    return -1;

  ret = get_object_impl(dpp, params);

  if (ret) {
    ldpp_dout(dpp, 0) <<"get_object_impl failed err:(" <<ret<<")" << dendl;
    goto out;
  }

  omap = params.op.obj.omap;

  for (const auto& k :  keys) {
    (*vals)[k] = omap[k];
  }

out:
  return ret;
}

int DB::Object::add_mp_part(const DoutPrefixProvider *dpp,
                            RGWUploadPartInfo info) {
  int ret = 0;

  DBOpParams params = {};

  ret = get_object_impl(dpp, params);

  if (ret) {
    ldpp_dout(dpp, 0) <<"get_object_impl failed err:(" <<ret<<")" << dendl;
    goto out;
  }

  params.op.obj.mp_parts.push_back(info);
  params.op.query_str = "mp";
  params.op.obj.state.mtime = real_clock::now();

  ret = store->ProcessOp(dpp, "UpdateObject", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In UpdateObject failed err:(" <<ret<<") " << dendl;
    goto out;
  }

out:
  return ret;
}

int DB::Object::get_mp_parts_list(const DoutPrefixProvider *dpp,
                                  std::list<RGWUploadPartInfo>& info)
{
  int ret = 0;
  DBOpParams params = {};
  std::map<std::string, bufferlist> omap;

  ret = get_object_impl(dpp, params);

  if (ret) {
    ldpp_dout(dpp, 0) <<"get_object_impl failed err:(" <<ret<<")" << dendl;
    goto out;
  }

  info = params.op.obj.mp_parts;

out:
  return ret;
}

/* Taken from rgw_rados.cc */
void DB::gen_rand_obj_instance_name(rgw_obj_key *target_key)
{
#define OBJ_INSTANCE_LEN 32
  char buf[OBJ_INSTANCE_LEN + 1];

  gen_rand_alphanumeric_no_underscore(cct, buf, OBJ_INSTANCE_LEN); /* don't want it to get url escaped,
                                                                      no underscore for instance name due to the way we encode the raw keys */

  target_key->set_instance(buf);
}

int DB::Object::obj_omap_get_all(const DoutPrefixProvider *dpp,
                                 std::map<std::string, bufferlist> *m)
{
  int ret = 0;
  DBOpParams params = {};
  std::map<std::string, bufferlist> omap;

  if (!m)
    return -1;

  ret = get_object_impl(dpp, params);

  if (ret) {
    ldpp_dout(dpp, 0) <<"get_object_impl failed err:(" <<ret<<")" << dendl;
    goto out;
  }

  (*m) = params.op.obj.omap;

out:
  return ret;
}

int DB::Object::obj_omap_get_vals(const DoutPrefixProvider *dpp,
                                  const std::string& marker,
                                  uint64_t max_count,
                                  std::map<std::string, bufferlist> *m, bool* pmore)
{
  int ret = 0;
  DBOpParams params = {};
  std::map<std::string, bufferlist> omap;
  map<string, bufferlist>::iterator iter;
  uint64_t count = 0;

  if (!m)
    return -1;

  ret = get_object_impl(dpp, params);

  if (ret) {
    ldpp_dout(dpp, 0) <<"get_object_impl failed err:(" <<ret<<")" << dendl;
    goto out;
  }

  omap = params.op.obj.omap;

  for (iter = omap.begin(); iter != omap.end(); ++iter) {

    if (iter->first < marker)
      continue;

    if ((++count) > max_count) {
      *pmore = true;
      break;
    }

    (*m)[iter->first] = iter->second;
  }

out:
  return ret;
}

int DB::Object::set_attrs(const DoutPrefixProvider *dpp,
                          map<string, bufferlist>& setattrs,
                          map<string, bufferlist>* rmattrs)
{
  int ret = 0;

  DBOpParams params = {};
  rgw::sal::Attrs *attrs;
  map<string, bufferlist>::iterator iter;
  RGWObjState* state;

  store->InitializeParams(dpp, &params);
  InitializeParamsfromObject(dpp, &params);
  ret = get_state(dpp, &state, true);

  if (ret && !state->exists) {
    ldpp_dout(dpp, 0) <<"get_state failed err:(" <<ret<<")" << dendl;
    goto out;
  }

  /* For now lets keep it simple..rmattrs & setattrs ..
   * XXX: Check rgw_rados::set_attrs
   */
  params.op.obj.state = *state;
  attrs = &params.op.obj.state.attrset;
  if (rmattrs) {
    for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      (*attrs).erase(iter->first);
    }
  }
  for (iter = setattrs.begin(); iter != setattrs.end(); ++iter) {
    (*attrs)[iter->first] = iter->second;
  }

  params.op.query_str = "attrs";
  /* As per https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html, 
   * the only way for users to modify object metadata is to make a copy of the object and
   * set the metadata.
   * Hence do not update mtime for any other attr changes */

  ret = store->ProcessOp(dpp, "UpdateObject", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In UpdateObject failed err:(" <<ret<<") " << dendl;
    goto out;
  }

out:
  return ret;
}

int DB::Object::transition(const DoutPrefixProvider *dpp,
                           const rgw_placement_rule& rule,
                           const real_time& mtime,
                           uint64_t olh_epoch)
{
  int ret = 0;

  DBOpParams params = {};
  map<string, bufferlist> *attrset;

  store->InitializeParams(dpp, &params);
  InitializeParamsfromObject(dpp, &params);

  ret = store->ProcessOp(dpp, "GetObject", &params);

  if (ret) {
    ldpp_dout(dpp, 0) <<"In GetObject failed err:(" <<ret<<")" << dendl;
    goto out;
  }

  /* pick one field check if object exists */
  if (!params.op.obj.state.exists) {
    ldpp_dout(dpp, 0)<<"Object(bucket:" << bucket_info.bucket.name << ", Object:"<< obj.key.name << ") doesn't exist" << dendl;
    return -1;
  }

  params.op.query_str = "meta";
  params.op.obj.state.mtime = real_clock::now();
  params.op.obj.storage_class = rule.storage_class;
  attrset = &params.op.obj.state.attrset;
  if (!rule.storage_class.empty()) {
    bufferlist bl;
    bl.append(rule.storage_class);
    (*attrset)[RGW_ATTR_STORAGE_CLASS] = bl;
  }
  params.op.obj.versioned_epoch = olh_epoch; // XXX: not sure if needed

  /* Unlike Rados, in dbstore for now, both head and tail objects
   * refer to same storage class
   */
  params.op.obj.head_placement_rule = rule;
  params.op.obj.tail_placement.placement_rule = rule;

  ret = store->ProcessOp(dpp, "UpdateObject", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In UpdateObject failed err:(" <<ret<<") " << dendl;
    goto out;
  }

out:
  return ret;
}

int DB::raw_obj::read(const DoutPrefixProvider *dpp, int64_t ofs,
                      uint64_t len, bufferlist& bl)
{
  int ret = 0;
  DBOpParams params = {};

  db->InitializeParams(dpp, &params);
  InitializeParamsfromRawObj(dpp, &params);

  ret = db->ProcessOp(dpp, "GetObjectData", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In GetObjectData failed err:(" <<ret<<")" << dendl;
    return ret;
  }

  /* Verify if its valid obj */
  if (!params.op.obj_data.size) {
    ret = -ENOENT;
    ldpp_dout(dpp, 0)<<"In GetObjectData failed err:(" <<ret<<")" << dendl;
    return ret;
  }

  bufferlist& read_bl = params.op.obj_data.data;

  unsigned copy_len;
  copy_len = std::min((uint64_t)read_bl.length() - ofs, len);
  read_bl.begin(ofs).copy(copy_len, bl);
  return bl.length();
}

int DB::raw_obj::write(const DoutPrefixProvider *dpp, int64_t ofs, int64_t write_ofs,
                       uint64_t len, bufferlist& bl)
{
  int ret = 0;
  DBOpParams params = {};

  db->InitializeParams(dpp, &params);
  InitializeParamsfromRawObj(dpp, &params);

  /* XXX: Check for chunk_size ?? */
  params.op.obj_data.offset = ofs;
  unsigned write_len = std::min((uint64_t)bl.length() - write_ofs, len);
  bl.begin(write_ofs).copy(write_len, params.op.obj_data.data);
  params.op.obj_data.size = params.op.obj_data.data.length();
  params.op.obj.state.mtime = real_clock::now();

  ret = db->ProcessOp(dpp, "PutObjectData", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In PutObjectData failed err:(" <<ret<<")" << dendl;
    return ret;
  }

  return write_len;
}

int DB::Object::list_versioned_objects(const DoutPrefixProvider *dpp,
                                       std::list<rgw_bucket_dir_entry>& list_entries) {
  int ret = 0;
  store = get_store();
  DBOpParams db_params = {};

  store->InitializeParams(dpp, &db_params);
  InitializeParamsfromObject(dpp, &db_params);

  db_params.op.list_max_count = MAX_VERSIONED_OBJECTS;

  ret = store->ProcessOp(dpp, "ListVersionedObjects", &db_params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In ListVersionedObjects failed err:(" <<ret<<") " << dendl;
  } else {
    list_entries = db_params.op.obj.list_entries;
  }

  return ret;
}

int DB::Object::get_obj_state(const DoutPrefixProvider *dpp,
                              const RGWBucketInfo& bucket_info, const rgw_obj& obj,
                              bool follow_olh, RGWObjState** state)
{
  int ret = 0;

  DBOpParams params = {};
  RGWObjState* s;

  if (!obj.key.instance.empty()) {
    /* Versionid provided. Fetch the object */
    ret = get_object_impl(dpp, params);

    if (ret && ret != -ENOENT) {
      ldpp_dout(dpp, 0) <<"get_object_impl failed err:(" <<ret<<")" << dendl;
      goto out;
    }
  } else {
    /* Instance is empty. May or may not be versioned object.
     * List all the versions and read the most recent entry */
    ret = list_versioned_objects(dpp, params.op.obj.list_entries);

    if (params.op.obj.list_entries.size() != 0) {
       /* Ensure its not a delete marker */
      auto& ent = params.op.obj.list_entries.front();
      if (ent.flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER) {
        ret = -ENOENT;
        goto out;
      }
      store->InitializeParams(dpp, &params);
      InitializeParamsfromObject(dpp, &params);
      params.op.obj.state.obj.key = ent.key;
    
      ret = get_object_impl(dpp, params);

      if (ret) {
        ldpp_dout(dpp, 0) <<"get_object_impl of versioned object failed err:(" <<ret<<")" << dendl;
        goto out;
      }
    } else {
      ret = -ENOENT;
      return ret;
    }
  }

  s = &params.op.obj.state;
  /* XXX: For now use state->shadow_obj to store ObjectID string */
  s->shadow_obj = params.op.obj.obj_id;

  *state = &obj_state;
  **state = *s;

out:
  return ret;

}

int DB::Object::get_state(const DoutPrefixProvider *dpp, RGWObjState** pstate, bool follow_olh)
{
  return get_obj_state(dpp, bucket_info, obj, follow_olh, pstate);
}

int DB::Object::Read::get_attr(const DoutPrefixProvider *dpp, const char *name, bufferlist& dest)
{
  RGWObjState* state;
  int r = source->get_state(dpp, &state, true);
  if (r < 0)
    return r;
  if (!state->exists)
    return -ENOENT;
  if (!state->get_attr(name, dest))
    return -ENODATA;

  return 0;
}

int DB::Object::Read::prepare(const DoutPrefixProvider *dpp)
{
  DB *store = source->get_store();
  CephContext *cct = store->ctx();

  bufferlist etag;

  map<string, bufferlist>::iterator iter;

  RGWObjState* astate;

  int r = source->get_state(dpp, &astate, true);
  if (r < 0)
    return r;

  if (!astate->exists) {
    return -ENOENT;
  }

  state.obj = astate->obj;
  source->obj_id = astate->shadow_obj;

  if (params.target_obj) {
    *params.target_obj = state.obj;
  }
  if (params.attrs) {
    *params.attrs = astate->attrset;
    if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      for (iter = params.attrs->begin(); iter != params.attrs->end(); ++iter) {
        ldpp_dout(dpp, 20) << "Read xattr rgw_rados: " << iter->first << dendl;
      }
    }
  }

  if (conds.if_match || conds.if_nomatch) {
    r = get_attr(dpp, RGW_ATTR_ETAG, etag);
    if (r < 0)
      return r;

    if (conds.if_match) {
      string if_match_str = rgw_string_unquote(conds.if_match);
      ldpp_dout(dpp, 10) << "ETag: " << string(etag.c_str(), etag.length()) << " " << " If-Match: " << if_match_str << dendl;
      if (if_match_str.compare(0, etag.length(), etag.c_str(), etag.length()) != 0) {
        return -ERR_PRECONDITION_FAILED;
      }
    }

    if (conds.if_nomatch) {
      string if_nomatch_str = rgw_string_unquote(conds.if_nomatch);
      ldpp_dout(dpp, 10) << "ETag: " << string(etag.c_str(), etag.length()) << " " << " If-NoMatch: " << if_nomatch_str << dendl;
      if (if_nomatch_str.compare(0, etag.length(), etag.c_str(), etag.length()) == 0) {
        return -ERR_NOT_MODIFIED;
      }
    }
  }

  if (params.obj_size)
    *params.obj_size = astate->size;
  if (params.lastmod)
    *params.lastmod = astate->mtime;

  return 0;
}

int DB::Object::Read::range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end)
{
  if (ofs < 0) {
    ofs += obj_size;
    if (ofs < 0)
      ofs = 0;
    end = obj_size - 1;
  } else if (end < 0) {
    end = obj_size - 1;
  }

  if (obj_size > 0) {
    if (ofs >= (off_t)obj_size) {
      return -ERANGE;
    }
    if (end >= (off_t)obj_size) {
      end = obj_size - 1;
    }
  }
  return 0;
}

int DB::Object::Read::read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider *dpp)
{
  DB *store = source->get_store();

  uint64_t read_ofs = ofs;
  uint64_t len, read_len;

  bufferlist read_bl;
  uint64_t max_chunk_size = store->get_max_chunk_size();

  RGWObjState* astate;
  int r = source->get_state(dpp, &astate, true);
  if (r < 0)
    return r;

  if (!astate || !astate->exists) {
    return -ENOENT;
  }

  if (astate->size == 0) {
    end = 0;
  } else if (end >= (int64_t)astate->size) {
    end = astate->size - 1;
  }

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;


  if (len > max_chunk_size) {
    len = max_chunk_size;
  }

  int head_data_size = astate->data.length();
  bool reading_from_head = (ofs < head_data_size);

  if (reading_from_head) {
    if (!ofs && astate->data.length() >= len) {
      bl = astate->data;
      return bl.length();
    }

    if (ofs < astate->data.length()) {
      unsigned copy_len = std::min((uint64_t)head_data_size - ofs, len);
      astate->data.begin(ofs).copy(copy_len, bl);
      return bl.length();
    }
  }

  /* tail object */
  int part_num = (ofs / max_chunk_size);
  /* XXX: Handle multipart_str */
  raw_obj read_obj(store, source->get_bucket_info().bucket.name, astate->obj.key.name, 
      astate->obj.key.instance, astate->obj.key.ns, source->obj_id, "0.0", part_num);

  read_len = len;

  ldpp_dout(dpp, 20) << "dbstore->read obj-ofs=" << ofs << " read_ofs=" << read_ofs << " read_len=" << read_len << dendl;

  // read from non head object
  r = read_obj.read(dpp, read_ofs, read_len, bl);

  if (r < 0) {
    return r;
  }

  return bl.length();
}

static int _get_obj_iterate_cb(const DoutPrefixProvider *dpp,
    const DB::raw_obj& read_obj, off_t obj_ofs,
    off_t len, bool is_head_obj,
    RGWObjState* astate, void *arg)
{
  struct db_get_obj_data* d = static_cast<struct db_get_obj_data*>(arg);
  return d->store->get_obj_iterate_cb(dpp, read_obj, obj_ofs, len,
      is_head_obj, astate, arg);
}

int DB::get_obj_iterate_cb(const DoutPrefixProvider *dpp,
    const raw_obj& read_obj, off_t obj_ofs,
    off_t len, bool is_head_obj,
    RGWObjState* astate, void *arg)
{
  struct db_get_obj_data* d = static_cast<struct db_get_obj_data*>(arg);
  bufferlist bl;
  int r = 0;

  if (is_head_obj) {
    bl = astate->data;
  } else {
    // read from non head object
    raw_obj robj = read_obj;
    /* read entire data. So pass offset as '0' & len as '-1' */
    r = robj.read(dpp, 0, -1, bl);

    if (r <= 0) {
      return r;
    }
  }

  unsigned read_ofs = 0, read_len = 0;
  while (read_ofs < bl.length()) {
    unsigned chunk_len = std::min((uint64_t)bl.length() - read_ofs, (uint64_t)len);
    r = d->client_cb->handle_data(bl, read_ofs, chunk_len);
    if (r < 0)
      return r;
    read_ofs += chunk_len;
    read_len += chunk_len;
    ldpp_dout(dpp, 20) << "dbstore->get_obj_iterate_cb  obj-ofs=" << obj_ofs << " len=" << len <<  " chunk_len = " << chunk_len << " read_len = " << read_len << dendl;
  }


  d->offset += read_len;

  return read_len;
}

int DB::Object::Read::iterate(const DoutPrefixProvider *dpp, int64_t ofs, int64_t end, RGWGetDataCB *cb)
{
  DB *store = source->get_store();
  const uint64_t chunk_size = store->get_max_chunk_size();

  db_get_obj_data data(store, cb, ofs);

  int r = source->iterate_obj(dpp, source->get_bucket_info(), state.obj,
      ofs, end, chunk_size, _get_obj_iterate_cb, &data);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "iterate_obj() failed with " << r << dendl;
    return r;
  }

  return 0;
}

int DB::Object::iterate_obj(const DoutPrefixProvider *dpp,
    const RGWBucketInfo& bucket_info, const rgw_obj& obj,
    off_t ofs, off_t end, uint64_t max_chunk_size,
    iterate_obj_cb cb, void *arg)
{
  DB *store = get_store();
  uint64_t len;
  RGWObjState* astate;

  int r = get_state(dpp, &astate, true);
  if (r < 0) {
    return r;
  }

  if (!astate->exists) {
    return -ENOENT;
  }

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  /* XXX: Will it really help to store all parts info in astate like manifest in Rados? */
  int part_num = 0;
  int head_data_size = astate->data.length();

  while (ofs <= end && (uint64_t)ofs < astate->size) {
    part_num = (ofs / max_chunk_size);
    uint64_t read_len = std::min(len, max_chunk_size);

    /* XXX: Handle multipart_str */
    raw_obj read_obj(store, get_bucket_info().bucket.name, astate->obj.key.name, 
        astate->obj.key.instance, astate->obj.key.ns, obj_id, "0.0", part_num);
    bool reading_from_head = (ofs < head_data_size);

    r = cb(dpp, read_obj, ofs, read_len, reading_from_head, astate, arg);
    if (r <= 0) {
      return r;
    }
    /* r refers to chunk_len (no. of bytes) handled in cb */
    len -= r;
    ofs += r;
  }

  return 0;
}

int DB::Object::Write::prepare(const DoutPrefixProvider* dpp)
{
  DB *store = target->get_store();

  int ret = -1;

  /* XXX: handle assume_noent */

  obj_state.obj = target->obj;
 
  if (target->obj_id.empty()) {
    if (!target->obj.key.instance.empty() && (target->obj.key.instance != "null")) {
      /* versioned object. Set obj_id same as versionID/instance */
      target->obj_id = target->obj.key.instance;
    } else {
      // generate obj_id
      char buf[33];
      gen_rand_alphanumeric(store->ctx(), buf, sizeof(buf) - 1);
      target->obj_id = buf;
    }
  }

  ret = 0;
  return ret;
}

/* writes tail objects */
int DB::Object::Write::write_data(const DoutPrefixProvider* dpp,
                               bufferlist& data, uint64_t ofs) {
  DB *store = target->get_store();
  /* tail objects */
  /* XXX: Split into parts each of max_chunk_size. But later make tail
   * object chunk size limit to sqlite blob limit */
  int part_num = 0;

  uint64_t max_chunk_size = store->get_max_chunk_size();

  /* tail_obj ofs should be greater than max_head_size */
  if (mp_part_str == "0.0")  { // ensure not multipart meta object
    if (ofs < store->get_max_head_size()) {
      return -1;
    }
  }
  
  uint64_t end = data.length();
  uint64_t write_ofs = 0;
  /* as we are writing max_chunk_size at a time in sal_dbstore DBAtomicWriter::process(),
   * maybe this while loop is not needed
   */
  while (write_ofs < end) {
    part_num = (ofs / max_chunk_size);
    uint64_t len = std::min(end, max_chunk_size);

    /* XXX: Handle multipart_str */
    raw_obj write_obj(store, target->get_bucket_info().bucket.name, obj_state.obj.key.name, 
        obj_state.obj.key.instance, obj_state.obj.key.ns, target->obj_id, mp_part_str, part_num);


    ldpp_dout(dpp, 20) << "dbstore->write obj-ofs=" << ofs << " write_len=" << len << dendl;

    // write into non head object
    int r = write_obj.write(dpp, ofs, write_ofs, len, data); 
    if (r < 0) {
      return r;
    }
    /* r refers to chunk_len (no. of bytes) handled in raw_obj::write */
    len -= r;
    ofs += r;
    write_ofs += r;
  }

  return 0;
}

/* Write metadata & head object data */
int DB::Object::Write::_do_write_meta(const DoutPrefixProvider *dpp,
    uint64_t size, uint64_t accounted_size,
    map<string, bufferlist>& attrs,
    bool assume_noent, bool modify_tail)
{
  DB *store = target->get_store();

  RGWObjState* state = &obj_state;
  map<string, bufferlist> *attrset;
  DBOpParams params = {};
  int ret = 0;
  string etag;
  string content_type;
  bufferlist acl_bl;
  string storage_class;

  map<string, bufferlist>::iterator iter;

  store->InitializeParams(dpp, &params);
  target->InitializeParamsfromObject(dpp, &params);

  obj_state = params.op.obj.state;

  if (real_clock::is_zero(meta.set_mtime)) {
    meta.set_mtime = real_clock::now();
  }

  attrset = &state->attrset;
  if (target->bucket_info.obj_lock_enabled() && target->bucket_info.obj_lock.has_rule()) {
    // && meta.flags == PUT_OBJ_CREATE) {
    auto iter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
    if (iter == attrs.end()) {
      real_time lock_until_date = target->bucket_info.obj_lock.get_lock_until_date(meta.set_mtime);
      string mode = target->bucket_info.obj_lock.get_mode();
      RGWObjectRetention obj_retention(mode, lock_until_date);
      bufferlist bl;
      obj_retention.encode(bl);
      (*attrset)[RGW_ATTR_OBJECT_RETENTION] = bl;
    }
  }

  state->mtime = meta.set_mtime;

  if (meta.data) {
    /* if we want to overwrite the data, we also want to overwrite the
       xattrs, so just remove the object */
    params.op.obj.head_data = *meta.data;
  }

  if (meta.rmattrs) {
    for (iter = meta.rmattrs->begin(); iter != meta.rmattrs->end(); ++iter) {
      const string& name = iter->first;
      (*attrset).erase(name.c_str());
    }
  }

  if (meta.manifest) {
    storage_class = meta.manifest->get_tail_placement().placement_rule.storage_class;

    /* remove existing manifest attr */
    iter = attrs.find(RGW_ATTR_MANIFEST);
    if (iter != attrs.end())
      attrs.erase(iter);

    bufferlist bl;
    encode(*meta.manifest, bl);
    (*attrset)[RGW_ATTR_MANIFEST] = bl;
  }

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    (*attrset)[name.c_str()] = bl;

    if (name.compare(RGW_ATTR_ETAG) == 0) {
      etag = rgw_bl_str(bl);
      params.op.obj.etag = etag;
    } else if (name.compare(RGW_ATTR_CONTENT_TYPE) == 0) {
      content_type = rgw_bl_str(bl);
    } else if (name.compare(RGW_ATTR_ACL) == 0) {
      acl_bl = bl;
    }
  }

  if (!storage_class.empty()) {
    bufferlist bl;
    bl.append(storage_class);
    (*attrset)[RGW_ATTR_STORAGE_CLASS] = bl;
  }

  params.op.obj.state = *state ;
  params.op.obj.state.exists = true;
  params.op.obj.state.size = size;
  params.op.obj.state.accounted_size = accounted_size;
  params.op.obj.owner = target->get_bucket_info().owner.id;
  params.op.obj.category = meta.category;

  if (meta.mtime) {
    *meta.mtime = meta.set_mtime;
  }

  params.op.query_str = "meta";
  params.op.obj.obj_id = target->obj_id;

  /* Check if versioned */
  bool is_versioned = !target->obj.key.instance.empty() && (target->obj.key.instance != "null");
  params.op.obj.is_versioned = is_versioned;

  if (is_versioned && (params.op.obj.category == RGWObjCategory::Main)) {
    /* versioned object */
    params.op.obj.flags |= rgw_bucket_dir_entry::FLAG_VER;
  }
  ret = store->ProcessOp(dpp, "PutObject", &params);
  if (ret) {
    ldpp_dout(dpp, 0)<<"In PutObject failed err:(" <<ret<<")" << dendl;
    goto out;
  }


out:
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: do_write_meta returned ret=" << ret << dendl;
  }

  meta.canceled = true;

  return ret;
}

int DB::Object::Write::write_meta(const DoutPrefixProvider *dpp, uint64_t size, uint64_t accounted_size,
    map<string, bufferlist>& attrs)
{
  bool assume_noent = false;
  /* handle assume_noent */
  int r = _do_write_meta(dpp, size, accounted_size, attrs, assume_noent, meta.modify_tail);
  return r;
}

int DB::Object::Delete::delete_obj(const DoutPrefixProvider *dpp) {
  int ret = 0;
  DBOpParams del_params = {};
  bool versioning_enabled = ((params.versioning_status & BUCKET_VERSIONED) == BUCKET_VERSIONED); 
  bool versioning_suspended = ((params.versioning_status & BUCKET_VERSIONS_SUSPENDED) == BUCKET_VERSIONS_SUSPENDED); 
  bool regular_obj = true;
  std::string versionid = target->obj.key.instance;

  ret = target->get_object_impl(dpp, del_params);

  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0)<<"GetObject during delete failed err:(" <<ret<<")" << dendl;
    return ret;
  }

  regular_obj = (del_params.op.obj.category == RGWObjCategory::Main);
  if (!ret) {
    if (!versionid.empty()) {
      // version-id is provided
      ret = delete_obj_impl(dpp, del_params);
      return ret;
    } else { // version-id is empty..
      /*
       * case: bucket_versioned
       *    create_delete_marker;
       * case: bucket_suspended
       *    delete entry
       *    create delete marker with version-id null;
       * default:
       *   just delete the entry
       */
      if (versioning_suspended && regular_obj) {
        ret = delete_obj_impl(dpp, del_params);
        ret = create_dm(dpp, del_params);
      } else if (versioning_enabled && regular_obj) {
        ret = create_dm(dpp, del_params);
      } else {
        ret = delete_obj_impl(dpp, del_params);
      }
    }
  } else { // ret == -ENOENT
     /* case: VersionID given
      *     return -ENOENT
      * else: // may or may not be versioned object
      *     Listversionedobjects
      *     if (list_entries.empty()) {
      *         nothing to do..return ENOENT
      *     } else {
      *         read top entry
      *         if (top.flags | FLAG_DELETE_MARKER) {
      *            // nothing to do
      *            return -ENOENT;
      *          }
      *          if (bucket_versioned)  {
      *            // create delete marker with new version-id
      *          } else if (bucket_suspended) {
      *            // create delete marker with version-id null
      *          }
      *          bucket cannot be in unversioned state post having versions
      *     }
      */
     if (!versionid.empty()) {
       return -ENOENT;
     }
     ret = target->list_versioned_objects(dpp, del_params.op.obj.list_entries);
     if (ret) {
        ldpp_dout(dpp, 0)<<"ListVersionedObjects failed err:(" <<ret<<")" << dendl;
        return ret;
     }
    if (del_params.op.obj.list_entries.empty()) {
      return -ENOENT;
    }
    auto &ent = del_params.op.obj.list_entries.front();
    if (ent.flags & rgw_bucket_dir_entry::FLAG_DELETE_MARKER) {
      // for now do not create another delete marker..just exit
      return 0;
    }
    ret = create_dm(dpp, del_params);
  }
  return ret;
}

int DB::Object::Delete::delete_obj_impl(const DoutPrefixProvider *dpp,
                                        DBOpParams& del_params) {
  int ret = 0;
  DB *store = target->get_store();

  ret = store->ProcessOp(dpp, "DeleteObject", &del_params);
  if (ret) {
    ldpp_dout(dpp, 0) << "In DeleteObject failed err:(" <<ret<<")" << dendl;
    return ret;
  }

  /* Now that tail objects are associated with objectID, they are not deleted
   * as part of this DeleteObj operation. Such tail objects (with no head object
   * in *.object.table are cleaned up later by GC thread.
   *
   * To avoid races between writes/reads & GC delete, mtime is maintained for each
   * tail object. This mtime is updated when tail object is written and also when
   * its corresponding head object is deleted (like here in this case).
   */
  DBOpParams update_params = del_params;
  update_params.op.obj.state.mtime = real_clock::now();
  ret = store->ProcessOp(dpp, "UpdateObjectData", &update_params);

  if (ret) {
    ldpp_dout(dpp, 0) << "Updating tail objects mtime failed err:(" <<ret<<")" << dendl;
  }
  return ret;
}

/*
 * a) if no versionID specified,
 *  - create a delete marker with 
 *    - new version/instanceID (if bucket versioned)
 *    - null versionID (if versioning suspended)
 */
int DB::Object::Delete::create_dm(const DoutPrefixProvider *dpp,
                                             DBOpParams& del_params) {

  DB *store = target->get_store();
  bool versioning_suspended = ((params.versioning_status & BUCKET_VERSIONS_SUSPENDED) == BUCKET_VERSIONS_SUSPENDED); 
  int ret = -1;
  DBOpParams olh_params = {};
  std::string version_id;
  DBOpParams next_params = del_params;

  version_id = del_params.op.obj.state.obj.key.instance;

  DBOpParams dm_params = del_params;

  // create delete marker

  store->InitializeParams(dpp, &dm_params);
  target->InitializeParamsfromObject(dpp, &dm_params);
  dm_params.op.obj.category = RGWObjCategory::None;

  if (versioning_suspended) {
    dm_params.op.obj.state.obj.key.instance = "null";
  } else {
    store->gen_rand_obj_instance_name(&dm_params.op.obj.state.obj.key);
    dm_params.op.obj.obj_id = dm_params.op.obj.state.obj.key.instance;
  }

  dm_params.op.obj.flags |= (rgw_bucket_dir_entry::FLAG_DELETE_MARKER);

  ret = store->ProcessOp(dpp, "PutObject", &dm_params);

  if (ret) {
    ldpp_dout(dpp, 0) << "delete_olh: failed to create delete marker - err:(" <<ret<<")" << dendl;
    return ret;
  }
  result.delete_marker = true;
  result.version_id = dm_params.op.obj.state.obj.key.instance;
  return ret;
}

int DB::get_entry(const std::string& oid, const std::string& marker,
			      std::unique_ptr<rgw::sal::Lifecycle::LCEntry>* entry)
{
  int ret = 0;
  const DoutPrefixProvider *dpp = get_def_dpp();

  DBOpParams params = {};
  InitializeParams(dpp, &params);

  params.op.lc_entry.index = oid;
  params.op.lc_entry.entry.set_bucket(marker);

  params.op.query_str = "get_entry";
  ret = ProcessOp(dpp, "GetLCEntry", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In GetLCEntry failed err:(" <<ret<<") " << dendl;
    goto out;
  }

  if (!params.op.lc_entry.entry.get_start_time() == 0) { //ensure entry found
    rgw::sal::Lifecycle::LCEntry* e;
    e = new rgw::sal::StoreLifecycle::StoreLCEntry(params.op.lc_entry.entry);
    if (!e) {
      ret = -ENOMEM;
      goto out;
    }
    entry->reset(e);
  }

out:
  return ret;
}

int DB::get_next_entry(const std::string& oid, const std::string& marker,
			      std::unique_ptr<rgw::sal::Lifecycle::LCEntry>* entry)
{
  int ret = 0;
  const DoutPrefixProvider *dpp = get_def_dpp();

  DBOpParams params = {};
  InitializeParams(dpp, &params);

  params.op.lc_entry.index = oid;
  params.op.lc_entry.entry.set_bucket(marker);

  params.op.query_str = "get_next_entry";
  ret = ProcessOp(dpp, "GetLCEntry", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In GetLCEntry failed err:(" <<ret<<") " << dendl;
    goto out;
  }

  if (!params.op.lc_entry.entry.get_start_time() == 0) { //ensure entry found
    rgw::sal::Lifecycle::LCEntry* e;
    e = new rgw::sal::StoreLifecycle::StoreLCEntry(params.op.lc_entry.entry);
    if (!e) {
      ret = -ENOMEM;
      goto out;
    }
    entry->reset(e);
  }

out:
  return ret;
}

int DB::set_entry(const std::string& oid, rgw::sal::Lifecycle::LCEntry& entry)
{
  int ret = 0;
  const DoutPrefixProvider *dpp = get_def_dpp();

  DBOpParams params = {};
  InitializeParams(dpp, &params);

  params.op.lc_entry.index = oid;
  params.op.lc_entry.entry = entry;

  ret = ProcessOp(dpp, "InsertLCEntry", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In InsertLCEntry failed err:(" <<ret<<") " << dendl;
    goto out;
  }

out:
  return ret;
}

int DB::list_entries(const std::string& oid, const std::string& marker,
  				 uint32_t max_entries, std::vector<std::unique_ptr<rgw::sal::Lifecycle::LCEntry>>& entries)
{
  int ret = 0;
  const DoutPrefixProvider *dpp = get_def_dpp();

  entries.clear();

  DBOpParams params = {};
  InitializeParams(dpp, &params);

  params.op.lc_entry.index = oid;
  params.op.lc_entry.min_marker = marker;
  params.op.list_max_count = max_entries;

  ret = ProcessOp(dpp, "ListLCEntries", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In ListLCEntries failed err:(" <<ret<<") " << dendl;
    goto out;
  }

  for (auto& entry : params.op.lc_entry.list_entries) {
    entries.push_back(std::make_unique<rgw::sal::StoreLifecycle::StoreLCEntry>(std::move(entry)));
  }

out:
  return ret;
}

int DB::rm_entry(const std::string& oid, rgw::sal::Lifecycle::LCEntry& entry)
{
  int ret = 0;
  const DoutPrefixProvider *dpp = get_def_dpp();

  DBOpParams params = {};
  InitializeParams(dpp, &params);

  params.op.lc_entry.index = oid;
  params.op.lc_entry.entry = entry;

  ret = ProcessOp(dpp, "RemoveLCEntry", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In RemoveLCEntry failed err:(" <<ret<<") " << dendl;
    goto out;
  }

out:
  return ret;
}

int DB::get_head(const std::string& oid, std::unique_ptr<rgw::sal::Lifecycle::LCHead>* head)
{
  int ret = 0;
  const DoutPrefixProvider *dpp = get_def_dpp();

  DBOpParams params = {};
  InitializeParams(dpp, &params);

  params.op.lc_head.index = oid;

  ret = ProcessOp(dpp, "GetLCHead", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In GetLCHead failed err:(" <<ret<<") " << dendl;
    goto out;
  }

  *head = std::make_unique<rgw::sal::StoreLifecycle::StoreLCHead>(params.op.lc_head.head);

out:
  return ret;
}

int DB::put_head(const std::string& oid, rgw::sal::Lifecycle::LCHead& head)
{
  int ret = 0;
  const DoutPrefixProvider *dpp = get_def_dpp();

  DBOpParams params = {};
  InitializeParams(dpp, &params);

  params.op.lc_head.index = oid;
  params.op.lc_head.head = head;

  ret = ProcessOp(dpp, "InsertLCHead", &params);

  if (ret) {
    ldpp_dout(dpp, 0)<<"In InsertLCHead failed err:(" <<ret<<") " << dendl;
    goto out;
  }

out:
  return ret;
}

int DB::delete_stale_objs(const DoutPrefixProvider *dpp, const std::string& bucket,
                          uint32_t min_wait) {
  DBOpParams params = {};
  int ret = -1;

  params.op.bucket.info.bucket.name = bucket;
  /* Verify if bucket exists.
   * XXX: This is needed for now to create objectmap of bucket
   * in SQLGetBucket
   */
  InitializeParams(dpp, &params);
  ret = ProcessOp(dpp, "GetBucket", &params);
  if (ret) {
    ldpp_dout(dpp, 0) << "In GetBucket failed err:(" <<ret<<")" << dendl;
    return ret;
  }

  ldpp_dout(dpp, 20) << " Deleting stale_objs of bucket( " << bucket <<")" << dendl;
  /* XXX: handle reads racing with delete here. Simple approach is maybe
   * to use locks or sqlite transactions.
   */
  InitializeParams(dpp, &params);
  params.op.obj.state.mtime = (real_clock::now() - make_timespan(min_wait));
  ret = ProcessOp(dpp, "DeleteStaleObjectData", &params);
  if (ret) {
    ldpp_dout(dpp, 0) << "In DeleteStaleObjectData failed err:(" <<ret<<")" << dendl;
  }

  return ret;
}

void *DB::GC::entry() {
  do {
    std::unique_lock<std::mutex> lk(mtx);

    ldpp_dout(dpp, 2) << " DB GC started " << dendl;
    int max = 100;
    RGWUserBuckets buckets;
    bool is_truncated = false;

    do {
      std::string& marker = bucket_marker;
      rgw_user user;
      user.id = user_marker;
      buckets.clear();
      is_truncated = false;

      int r = db->list_buckets(dpp, "all", user, marker, string(),
                       max, false, &buckets, &is_truncated);
 
      if (r < 0) { //do nothing? retry later ?
        break;
      }

      for (const auto& ent : buckets.get_buckets()) {
        const std::string &bname = ent.first;

        r = db->delete_stale_objs(dpp, bname, gc_obj_min_wait);

        if (r < 0) { //do nothing? skip to next entry?
         ldpp_dout(dpp, 2) << " delete_stale_objs failed for bucket( " << bname <<")" << dendl;
        }
        bucket_marker = bname;
        user_marker = user.id;

        /* XXX: If using locks, unlock here and reacquire in the next iteration */
        cv.wait_for(lk, std::chrono::milliseconds(100));
	if (stop_signalled) {
	  goto done;
	}
      }
    } while(is_truncated);

    bucket_marker.clear();
    cv.wait_for(lk, std::chrono::milliseconds(gc_interval*10));
  } while(! stop_signalled);

done:
  return nullptr;
}

} } // namespace rgw::store

