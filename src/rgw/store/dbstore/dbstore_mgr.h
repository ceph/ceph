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

using namespace std;
using namespace rgw::store;
using DB = rgw::store::DB;

/* XXX: Should be a dbstore config option */
const static string default_tenant = "default_ns";

using namespace std;

class DBStoreManager {
private:
  map<string, DB*> DBStoreHandles;
  DB *default_db = NULL;
  CephContext *cct;

public:
  DBStoreManager(CephContext *_cct): DBStoreHandles() {
    cct = _cct;
	default_db = createDB(default_tenant);
  };
  DBStoreManager(string logfile, int loglevel): DBStoreHandles() {
    /* No ceph context. Create one with log args provided */
    vector<const char*> args;
    cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                      CODE_ENVIRONMENT_DAEMON, CINIT_FLAG_NO_MON_CONFIG, 1)->get();
    cct->_log->set_log_file(logfile);
    cct->_log->reopen_log_file();
    cct->_conf->subsys.set_log_level(ceph_subsys_rgw, loglevel);
  };
  ~DBStoreManager() { destroyAllHandles(); };

  /* XXX: TBD based on testing
   * 1)  Lock to protect DBStoreHandles map.
   * 2) Refcount of each DBStore to protect from
   * being deleted while using it.
   */
  DB* getDB () { return default_db; };
  DB* getDB (string tenant, bool create);
  DB* createDB (string tenant);
  void deleteDB (string tenant);
  void deleteDB (DB* db);
  void destroyAllHandles();
};
