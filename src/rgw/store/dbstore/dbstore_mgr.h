// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>

using namespace std;

/* XXX: Should be a dbstore config option */
const static string default_tenant = "default_ns";

using namespace std;
class DBStore;

class DBStoreManager {
private:
  map<string, DBStore*> DBStoreHandles;
  DBStore *default_dbstore = NULL;

public:
  DBStoreManager(): DBStoreHandles() {
	default_dbstore = createDBStore(default_tenant);
  };
  ~DBStoreManager() { destroyAllHandles(); };

  /* XXX: TBD based on testing
   * 1)  Lock to protect DBStoreHandles map.
   * 2) Refcount of each DBStore to protect from
   * being deleted while using it.
   */
  DBStore* getDBStore () { return default_dbstore; };
  DBStore* getDBStore (string tenant, bool create);
  DBStore* createDBStore (string tenant);
  void deleteDBStore (string tenant);
  void deleteDBStore (DBStore* db);
  void destroyAllHandles();
};
