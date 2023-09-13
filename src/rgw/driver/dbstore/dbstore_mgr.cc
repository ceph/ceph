// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "dbstore_mgr.h"
#include "common/dbstore_log.h"

#include <filesystem>

static constexpr auto dout_subsys = ceph_subsys_rgw;

using namespace std;


/* Given a tenant, find and return the DBStore handle.
 * If not found and 'create' set to true, create one
 * and return
 */
DB *DBStoreManager::getDB (string tenant, bool create)
{
  map<string, DB*>::iterator iter;
  DB *dbs = nullptr;
  pair<map<string, DB*>::iterator,bool> ret;

  if (tenant.empty())
    return default_db;

  if (DBStoreHandles.empty())
    goto not_found;

  iter = DBStoreHandles.find(tenant);

  if (iter != DBStoreHandles.end())
    return iter->second;

not_found:
  if (!create)
    return nullptr;

  dbs = createDB(tenant);

  return dbs;
}

/* Create DBStore instance */
DB *DBStoreManager::createDB(std::string tenant) {
  DB *dbs = nullptr;
  pair<map<string, DB*>::iterator,bool> ret;
  const auto& db_path = g_conf().get_val<std::string>("dbstore_db_dir");
  const auto& db_name = g_conf().get_val<std::string>("dbstore_db_name_prefix") + "-" + tenant;

  auto db_full_path = std::filesystem::path(db_path) / db_name;
  ldout(cct, 0) << "DB initialization full db_path("<<db_full_path<<")" << dendl;

  /* Create the handle */
#ifdef SQLITE_ENABLED
  dbs = new SQLiteDB(db_full_path.string(), cct);
#else
  dbs = new DB(db_full_path.string(), cct);
#endif

  /* API is DB::Initialize(string logfile, int loglevel);
   * If none provided, by default write in to dbstore.log file
   * created in current working directory with loglevel L_EVENT.
   * XXX: need to align these logs to ceph location
   */
  if (dbs->Initialize("", -1) < 0) {
    ldout(cct, 0) << "DB initialization failed for tenant("<<tenant<<")" << dendl;

    delete dbs;
    return nullptr;
  }

  /* XXX: Do we need lock to protect this map?
  */
  ret = DBStoreHandles.insert(pair<string, DB*>(tenant, dbs));

  /*
   * Its safe to check for already existing entry (just
   * incase other thread raced and created the entry)
   */
  if (ret.second == false) {
    /* Entry already created by another thread */
    delete dbs;

    dbs = ret.first->second;
  }

  return dbs;
}

void DBStoreManager::deleteDB(string tenant) {
  map<string, DB*>::iterator iter;
  DB *dbs = nullptr;

  if (tenant.empty() || DBStoreHandles.empty())
    return;

  /* XXX: Check if we need to perform this operation under a lock */
  iter = DBStoreHandles.find(tenant);

  if (iter == DBStoreHandles.end())
    return;

  dbs = iter->second;

  DBStoreHandles.erase(iter);
  dbs->Destroy(dbs->get_def_dpp());
  delete dbs;

  return;
}

void DBStoreManager::deleteDB(DB *dbs) {
  if (!dbs)
    return;

  (void)deleteDB(dbs->getDBname());
}


void DBStoreManager::destroyAllHandles(){
  map<string, DB*>::iterator iter;
  DB *dbs = nullptr;

  if (DBStoreHandles.empty())
    return;

  for (iter = DBStoreHandles.begin(); iter != DBStoreHandles.end();
      ++iter) {
    dbs = iter->second;
    dbs->Destroy(dbs->get_def_dpp());
    delete dbs;
  }

  DBStoreHandles.clear();

  return;
}


