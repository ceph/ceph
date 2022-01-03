// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>
#include "common/ceph_context.h"
#include "common/dbstore.h"
#include "sqlite/sqliteDB.h"
#include <boost/lockfree/queue.hpp>

using namespace std;
using namespace rgw::store;
using DB = rgw::store::DB;

/* XXX: Should be a dbstore config option */
const static string default_tenant = "default_ns";
#define MAX_QUEUE_DEFAULT 20 // make it configurable

using namespace std;

typedef boost::lockfree::queue<DB*, boost::lockfree::fixed_sized<true>> DBStoreQueue;

class DBStoreManager {
private:
  map<string, DB*> DBStoreHandles;
  DB *default_db = NULL;
  CephContext *cct;

  // used in the dtor for dbstore conn cleanup
  static std::atomic<uint64_t> max_conn; // XXX: make it configurable
  static std::atomic<uint64_t> total_conn; // total connections created so far

  static DBStoreQueue db_connections;
  static std::mutex db_mutex;
  static std::condition_variable db_cond;

  static void delete_conn(const DB* db) {
    delete db;
  }
public:
  DBStoreManager(CephContext *_cct): DBStoreHandles() {
    cct = _cct;
	default_db = createDB(default_tenant);

    for(uint32_t i = 0; i < DBStoreManager::max_conn; i++)  {
      DB* db = createDB(default_tenant);

      if (db) {
        DBStoreManager::db_connections.push(db);
        DBStoreManager::total_conn++;
      }
    }
  };
  DBStoreManager(string logfile, int loglevel): DBStoreHandles() {
    /* No ceph context. Create one with log args provided */
    vector<const char*> args;
    cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                      CODE_ENVIRONMENT_DAEMON, CINIT_FLAG_NO_MON_CONFIG, 1)->get();
    cct->_log->set_log_file(logfile);
    cct->_log->reopen_log_file();
    cct->_conf->subsys.set_log_level(dout_subsys, loglevel);

    for(uint32_t i = 0; i < DBStoreManager::max_conn; i++)  {
      DB* db = createDB(default_tenant);

      if (db) {
        DBStoreManager::db_connections.push(db);
        DBStoreManager::total_conn++;
      }
    }
  };
  ~DBStoreManager() {
     destroyAllHandles();
     DBStoreManager::db_connections.consume_all(delete_conn);};

  /* XXX: TBD based on testing
   * 1)  Lock to protect DBStoreHandles map.
   * 2) Refcount of each DBStore to protect from
   * being deleted while using it.
   */
  DB* getDefaultDB () { return default_db; };
  std::shared_ptr<DB> getDB() {
    DB* db;
    {
      std::unique_lock<std::mutex> guard(DBStoreManager::db_mutex);
      DBStoreManager::db_cond.wait(guard, [&](){return (DBStoreManager::total_conn > 0);});

      ceph_assert(!DBStoreManager::db_connections.empty());

      DBStoreManager::db_connections.pop(std::ref(db));
      DBStoreManager::total_conn--; 
    } 
      ldout(cct, 0) << "In getDB() tenant(" << default_tenant << "), total count is :"<< DBStoreManager::total_conn << " , newly created db:" << db << dendl;


    std::shared_ptr<DB> sh(db,
         [](DB* p){
           std::lock_guard<std::mutex> guard(DBStoreManager::db_mutex);
           DBStoreManager::db_connections.push(p);
           DBStoreManager::total_conn++;
           DBStoreManager::db_cond.notify_all();
         });
    return sh;
  }
  DB* getDB (string tenant, bool create);
  DB* createDB (string tenant);
  void deleteDB (string tenant);
  void deleteDB (DB* db);
  void destroyAllHandles();
};
