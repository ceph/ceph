// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "dbstore_mgr.h"

/* Given a tenant, find and return the DBStore handle.
 * If not found and 'create' set to true, create one
 * and return
 */
DBStore* DBStoreManager::getDBStore (string tenant, bool create)
{
  map<string, DBStore*>::iterator iter;
  DBStore *dbs = nullptr;
  pair<map<string, DBStore*>::iterator,bool> ret;

  if (tenant.empty())
    return default_dbstore;

  if (DBStoreHandles.empty())
    goto not_found;

  iter = DBStoreHandles.find(tenant);

  if (iter != DBStoreHandles.end())
    return iter->second;

not_found:
  if (!create)
    return NULL;

  dbs = createDBStore(tenant);

  return dbs;
}

/* Create DBStore instance */
DBStore* DBStoreManager::createDBStore(string tenant) {
  DBStore *dbs = nullptr;
  pair<map<string, DBStore*>::iterator,bool> ret;

  /* Create the handle */
#ifdef SQLITE_ENABLED
  dbs = new SQLiteDB(tenant, cct);
#else
  dbs = new DBStore(tenant, cct);
#endif

  /* API is DBStore::Initialize(string logfile, int loglevel);
   * If none provided, by default write in to dbstore.log file
   * created in current working directory with loglevel L_EVENT.
   * XXX: need to align these logs to ceph location
   */
  if (dbs->Initialize("", -1) < 0) {
    cout<<"^^^^^^^^^^^^DB initialization failed for tenant("<<tenant<<")^^^^^^^^^^^^^^^^^^^^^^^^^^ \n";

    delete dbs;
    return NULL;
  }

  /* XXX: Do we need lock to protect this map?
  */
  ret = DBStoreHandles.insert(pair<string, DBStore*>(tenant, dbs));

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

void DBStoreManager::deleteDBStore(string tenant) {
  map<string, DBStore*>::iterator iter;
  DBStore *dbs = nullptr;

  if (tenant.empty() || DBStoreHandles.empty())
    return;

  /* XXX: Check if we need to perform this operation under a lock */
  iter = DBStoreHandles.find(tenant);

  if (iter == DBStoreHandles.end())
    return;

  dbs = iter->second;

  DBStoreHandles.erase(iter);
  dbs->Destroy();
  delete dbs;

  return;
}

void DBStoreManager::deleteDBStore(DBStore *dbs) {
  if (!dbs)
    return;

  (void)deleteDBStore(dbs->getDBname());
}


void DBStoreManager::destroyAllHandles(){
  map<string, DBStore*>::iterator iter;
  DBStore *dbs = nullptr;

  if (DBStoreHandles.empty())
    return;

  for (iter = DBStoreHandles.begin(); iter != DBStoreHandles.end();
      ++iter) {
    dbs = iter->second;
    dbs->Destroy();
    delete dbs;
  }

  DBStoreHandles.clear();

  return;
}


