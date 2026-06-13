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

class FilesystemUserDB;
class FilesystemAccountDB;

struct FilesystemAccountDBOpAccountInfo : DBOpAccountInfo {};

struct FilesystemUserDBOpUserInfo : DBOpUserInfo {};

struct FilesystemUserDBOpInfo : DBOpInfo {};

struct FilesystemUserDBOpUserPrepareInfo : DBOpUserPrepareInfo {};

struct FilesystemUserDBOpPrepareInfo : DBOpPrepareInfo {};

struct FilesystemUserDBOpPrepareParams : DBOpPrepareParams {};

struct FilesystemAccountDBOpInfo : DBOpInfo {};

struct FilesystemAccountDBOpPrepareInfo : DBOpPrepareInfo {};

struct FilesystemAccountDBOpPrepareParams : DBOpPrepareParams {};

struct FilesystemAccountDBOps : DBOps {};

class FilesystemAccountDBOp : public DBOp {
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
    FilesystemAccountDBOp() : DBOp() {}
    virtual ~FilesystemAccountDBOp() {}
    std::mutex mtx; // to protect prepared stmt
};

class InsertFilesystemAccountOp : public SQLInsertAccount {};

class RemoveFilesystemAccountOp: public SQLRemoveAccount {};

class GetFilesystemAccountOp: public SQLGetAccount {};

struct FilesystemUserDBOps : DBOps {};

class FilesystemUserDBOp : public DBOp {
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
    FilesystemUserDBOp() : DBOp() {}
    virtual ~FilesystemUserDBOp() {}
    std::mutex mtx; // to protect prepared stmt
};

class InsertFilesystemUserOp : public SQLInsertUser {};

class RemoveFilesystemUserOp: public SQLRemoveUser {};

class FilesystemUserDB : public SQLiteDB {
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

    FilesystemUserDB(std::string db_name, CephContext *_cct) : SQLiteDB(db_name, _cct),
		db_name(db_name),
		cct(_cct),
		dp(_cct, ceph_subsys_rgw, "rgw FilesystemUserDB backend:")
                { DB::set_context(cct); }
    /* FilesystemUserDB() {}*/

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

class FilesystemAccountDB : public SQLiteDB {
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

    FilesystemAccountDB(std::string db_name, CephContext *_cct) : SQLiteDB(db_name, _cct),
		db_name(db_name),
		account_table(db_name+"_account_table"),
		user_table(db_name+"_user_table"),
		cct(_cct),
		dp(_cct, ceph_subsys_rgw, "rgw FilesystemAccountDB backend:")
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
