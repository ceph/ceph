// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

class NSFSUserDB;
class NSFSAccountDB;

struct NSFSAccountDBOpAccountInfo : DBOpAccountInfo {};

struct NSFSUserDBOpUserInfo : DBOpUserInfo {};

struct NSFSUserDBOpInfo : DBOpInfo {};

struct NSFSUserDBOpUserPrepareInfo : DBOpUserPrepareInfo {};

struct NSFSUserDBOpPrepareInfo : DBOpPrepareInfo {};

struct NSFSUserDBOpPrepareParams : DBOpPrepareParams {};

struct NSFSAccountDBOpInfo : DBOpInfo {};

struct NSFSAccountDBOpPrepareInfo : DBOpPrepareInfo {};

struct NSFSAccountDBOpPrepareParams : DBOpPrepareParams {};

struct NSFSAccountDBOps : DBOps {};

class NSFSAccountDBOp : public DBOp {
  private:
    static constexpr std::string_view CreateAccountTableQ =
      /* Corresponds to RGWAccountInfo
       *
       * AccountID is made Primary key.
       * If multiple tenants are stored in single .db handle, should
       * make both (AccountID, Tenant) as Primary Key.
       *
       * XXX:
       * - Quota stored as blob .. should be linked to quota table.
       */
      "CREATE TABLE IF NOT EXISTS '{}' (	\
      AccountID TEXT NOT NULL UNIQUE,		\
      Tenant TEXT ,		\
      AccountName TEXT , \
      Email TEXT ,	\
      Quota BLOB ,	\
      BucketQuota BLOB ,	\
      MaxUsers INTEGER ,	\
      MaxRoles INTEGER ,	\
      MaxGroups INTEGER ,	\
      MaxBuckets INTEGER ,	\
      MaxAccessKeys INTEGER ,	\
      PRIMARY KEY (AccountID) \n);";

  public:
    NSFSAccountDBOp() : DBOp() {}
    virtual ~NSFSAccountDBOp() {}
    std::mutex mtx; // to protect prepared stmt
};

class InsertNSFSAccountOp : public SQLInsertAccount {};

class RemoveNSFSAccountOp: public SQLRemoveAccount {};

class GetNSFSAccountOp: public SQLGetAccount {};

struct NSFSUserDBOps : DBOps {};

class NSFSUserDBOp : public DBOp {
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
    NSFSUserDBOp() : DBOp() {}
    virtual ~NSFSUserDBOp() {}
    std::mutex mtx; // to protect prepared stmt
};

class InsertNSFSUserOp : public SQLInsertUser {};

class RemoveNSFSUserOp: public SQLRemoveUser {};

class NSFSUserDB : public SQLiteDB {
  private:
    const std::string db_name;
    rgw::sal::Driver* driver;

  protected:
    void *db;
    CephContext *cct;
    const DoutPrefix dp;
    // Below mutex is to protect objectmap and other shared
    // objects if any.
    std::mutex mtx;

  public:
    struct DBOps dbops;

    NSFSUserDB(std::string db_name, CephContext *_cct) : SQLiteDB(db_name, _cct),
		db_name(db_name),
		cct(_cct),
		dp(_cct, ceph_subsys_rgw, "rgw NSFSUserDBStore backend: ")
                { DB::set_context(cct); }
    /* NSFSUserDB() {}*/

    int Initialize(std::string logfile, int loglevel);
    int ProcessOp(const DoutPrefixProvider *dpp, std::string_view Op, DBOpParams *params);
    int Destroy(const DoutPrefixProvider *dpp);

    CephContext* ctx() { return this->cct; }

    virtual int InitPrepareParams(const DoutPrefixProvider *dpp,
                                  DBOpPrepareParams &p_params,
                                  DBOpParams* params) override { return 0; }
    virtual int createLCTables(const DoutPrefixProvider *dpp) override { return 0; }

    virtual int ListAllBuckets(const DoutPrefixProvider *dpp, DBOpParams *params) override { return 0; }
    virtual int ListAllUsers(const DoutPrefixProvider *dpp, DBOpParams *params) override { return 0; }
    virtual int ListAllObjects(const DoutPrefixProvider *dpp, DBOpParams *params) override { return 0; }
};

class NSFSAccountDB : public SQLiteDB {
  private:
    const std::string db_name;
    const std::string account_table;
    const std::string user_table;
    const std::string bucket_table;
    const std::string quota_table;
    const std::string lc_head_table;
    const std::string lc_entry_table;

    rgw::sal::Driver* driver;

  protected:
    void *db;
    CephContext *cct;
    const DoutPrefix dp;
    // Below mutex is to protect objectmap and other shared
    // objects if any.
    std::mutex mtx;

  public:
    struct DBOps dbops;

    NSFSAccountDB(std::string db_name, CephContext *_cct) : SQLiteDB(db_name, _cct),
		db_name(db_name),
		account_table(db_name+"_account_table"),
		user_table(db_name+"_user_table"),
		cct(_cct),
		dp(_cct, ceph_subsys_rgw, "rgw NSFSAccountDBStore backend: ")
                { DB::set_context(cct); }

    int Initialize(std::string logfile, int loglevel);
    int ProcessOp(const DoutPrefixProvider *dpp, std::string_view Op, DBOpParams *params);
    int Destroy(const DoutPrefixProvider *dpp);

    CephContext* ctx() { return this->cct; }

    virtual int InitPrepareParams(const DoutPrefixProvider *dpp,
                                  DBOpPrepareParams &p_params,
                                  DBOpParams* params) override { return 0; }
    virtual int createLCTables(const DoutPrefixProvider *dpp) override { return 0; }

    virtual int ListAllBuckets(const DoutPrefixProvider *dpp, DBOpParams *params) override { return 0; }
    virtual int ListAllUsers(const DoutPrefixProvider *dpp, DBOpParams *params) override { return 0; }
    virtual int ListAllObjects(const DoutPrefixProvider *dpp, DBOpParams *params) override { return 0; }
};

} } // namespace rgw::store
