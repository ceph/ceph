// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <cerrno>
#include <cstdlib>
#include <string>
#include <cstdio>
#include <iostream>
#include <vector>

#include "common/ceph_context.h"
#include "common/dbstore.h"
#include "sqlite/sqliteDB.h"

using namespace rgw::store;
using DB = rgw::store::DB;

/* XXX: Should be a dbstore config option */
const static std::string default_tenant = "default_ns";

class DBStoreManager {
private:
  std::map<std::string, DB*> DBStoreHandles;
  DB *default_db = nullptr;
  CephContext *cct;

public:
  DBStoreManager(CephContext *_cct, std::string db_dir, std::string db_name_prefix): DBStoreHandles() {
    cct = _cct;
	default_db = createDB(default_tenant, db_dir, db_name_prefix);
  };
  DBStoreManager(CephContext *_cct, std::string logfile, int loglevel, std::string db_dir, std::string db_name_prefix): DBStoreHandles() {
    /* No ceph context. Create one with log args provided */
    cct = _cct;
    cct->_log->set_log_file(logfile);
    cct->_log->reopen_log_file();
    cct->_conf->subsys.set_log_level(ceph_subsys_rgw, loglevel);
    default_db = createDB(default_tenant, db_dir, db_name_prefix);
  };
  ~DBStoreManager() { destroyAllHandles(); };

  /* XXX: TBD based on testing
   * 1)  Lock to protect DBStoreHandles map.
   * 2) Refcount of each DBStore to protect from
   * being deleted while using it.
   */
  DB* getDB () { return default_db; };
  DB* getDB (std::string tenant, bool create, std::string db_dir, std::string db_name_prefix);
  DB* createDB (std::string tenant, std::string db_dir, std::string db_name_prefix);
  void deleteDB (std::string tenant);
  void deleteDB (DB* db);
  void destroyAllHandles();
};
