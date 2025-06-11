// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "driver/rados/rgw_bucket.h"

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include "fmt/format.h"
#include <map>
#include "rgw_sal_store.h"
#include "rgw_common.h"
#include "driver/dbstore/sqlite/sqliteDB.h"
#include "driver/dbstore/common/dbstore.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_context.h"
#include "rgw_multi.h"

#include "driver/rados/rgw_obj_manifest.h" // FIXME: subclass dependency

namespace rgw { namespace store {

class POSIXUserDB;

struct POSIXUserDBOpUserInfo : DBOpUserInfo {};

struct POSIXUserDBOpInfo : DBOpInfo {};

struct POSIXUserDBOpUserPrepareInfo : DBOpUserPrepareInfo {};

struct POSIXUserDBOpPrepareInfo : DBOpPrepareInfo {};

struct POSIXUserDBOpPrepareParams : DBOpPrepareParams {};

struct POSIXUserDBOps : DBOps {};

class POSIXUserDBOp : public DBOp {
  private:
    static constexpr std::string_view CreateUserTableQ =
      /* Corresponds to rgw::sal::User
       *
       * For now only UserID is made Primary key.
       * If multiple tenants are stored in single .db handle, should
       * make both (UserID, Tenant) as Primary Key.
       *
       * XXX:
       * - AccessKeys, SwiftKeys, Subusers (map<>) are stored as blob.
       *   To enable easy query, first accesskey is stored in separate fields
       *   AccessKeysID, AccessKeysSecret.
       *   In future, may be have separate table to store these keys and
       *   query on that table.
       * - Quota stored as blob .. should be linked to quota table.
       */
      "CREATE TABLE IF NOT EXISTS '{}' (	\
      UserID TEXT NOT NULL UNIQUE,		\
      Tenant TEXT ,		\
      NS TEXT ,		\
      DisplayName TEXT , \
      UserEmail TEXT ,	\
      AccessKeysID TEXT ,	\
      AccessKeysSecret TEXT ,	\
      AccessKeys BLOB ,	\
      SwiftKeys BLOB ,	\
      SubUsers BLOB ,		\
      Suspended INTEGER ,	\
      MaxBuckets INTEGER ,	\
      OpMask	INTEGER ,	\
      UserCaps BLOB ,		\
      Admin	INTEGER ,	\
      System INTEGER , 	\
      PlacementName TEXT , 	\
      PlacementStorageClass TEXT , 	\
      PlacementTags BLOB ,	\
      BucketQuota BLOB ,	\
      TempURLKeys BLOB ,	\
      UserQuota BLOB ,	\
      TYPE INTEGER ,		\
      MfaIDs BLOB ,	\
      AssumedRoleARN TEXT , \
      UserAttrs   BLOB,   \
      UserVersion   INTEGER,    \
      UserVersionTag TEXT,      \
      PRIMARY KEY (UserID) \n);";

  public:
    POSIXUserDBOp() : DBOp() {}
    virtual ~POSIXUserDBOp() {}
    std::mutex mtx; // to protect prepared stmt
};

class InsertPOSIXUserOp : public SQLInsertUser {};

class RemovePOSIXUserOp: public SQLRemoveUser {};

class POSIXUserDB : public SQLiteDB {
  private:
    const std::string db_name;
    rgw::sal::Driver* driver;
    const std::string user_table;

  protected:
    void *db;
    CephContext *cct;
    const DoutPrefix dp;
    // Below mutex is to protect objectmap and other shared
    // objects if any.
    std::mutex mtx;

  public:
    POSIXUserDB(std::string db_name, CephContext *_cct) : SQLiteDB(db_name, _cct),
		db_name(db_name),
		user_table(db_name+"_user_table"),
		cct(_cct),
		dp(_cct, ceph_subsys_rgw, "rgw POSIXUserDBStore backend: ")
                { DB::set_context(cct); }
    /* POSIXUserDB() {}*/

    int Initialize(std::string logfile, int loglevel);
    int Destroy(const DoutPrefixProvider *dpp);

    CephContext* ctx() { return this->cct; }

    virtual int InitPrepareParams(const DoutPrefixProvider *dpp,
                                  DBOpPrepareParams &p_params,
                                  DBOpParams* params) { return 0; } // TODO
    virtual int createLCTables(const DoutPrefixProvider *dpp) { return 0; }

    virtual int ListAllBuckets(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; } // TODO
    virtual int ListAllUsers(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; } // TODO
    virtual int ListAllObjects(const DoutPrefixProvider *dpp, DBOpParams *params) { return 0; } // TODO
};

} } // namespace rgw::store
