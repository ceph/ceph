// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef SQLITE_DB_H
#define SQLITE_DB_H

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <sqlite3.h>
#include "rgw/store/dbstore/common/dbstore.h"

using namespace std;
using namespace rgw::store;

class SQLiteDB : public DB, public DBOp{
  private:
    sqlite3_mutex *mutex = NULL;

  protected:
    CephContext *cct;

  public:	
    sqlite3_stmt *stmt = NULL;
    DBOpPrepareParams PrepareParams;

    SQLiteDB(string db_name, CephContext *_cct) : DB(db_name, _cct), cct(_cct) {
      InitPrepareParams(get_def_dpp(), PrepareParams);
    }
    SQLiteDB(sqlite3 *dbi, CephContext *_cct) : DB(_cct), cct(_cct) {
      db = (void*)dbi;
      InitPrepareParams(get_def_dpp(), PrepareParams);
    }
    ~SQLiteDB() {}

    void *openDB(const DoutPrefixProvider *dpp) override;
    int closeDB(const DoutPrefixProvider *dpp) override;
    int InitializeDBOps(const DoutPrefixProvider *dpp) override;
    int FreeDBOps(const DoutPrefixProvider *dpp) override;

    int InitPrepareParams(const DoutPrefixProvider *dpp, DBOpPrepareParams &params) override { return 0; }

    int exec(const DoutPrefixProvider *dpp, const char *schema,
        int (*callback)(void*,int,char**,char**));
    int Step(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt,
        int (*cbk)(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt));
    int Reset(const DoutPrefixProvider *dpp, sqlite3_stmt *stmt);
    /* default value matches with sqliteDB style */

    int createTables(const DoutPrefixProvider *dpp);
    int createBucketTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int createUserTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int createObjectTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int createObjectDataTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int createQuotaTable(const DoutPrefixProvider *dpp, DBOpParams *params);

    int DeleteBucketTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int DeleteUserTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int DeleteObjectTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int DeleteObjectDataTable(const DoutPrefixProvider *dpp, DBOpParams *params);

    int ListAllBuckets(const DoutPrefixProvider *dpp, DBOpParams *params) override;
    int ListAllUsers(const DoutPrefixProvider *dpp, DBOpParams *params) override;
    int ListAllObjects(const DoutPrefixProvider *dpp, DBOpParams *params) override;
};

class SQLObjectOp : public ObjectOp {
  private:
    sqlite3 **sdb = NULL;
    CephContext *cct;

  public:
    SQLObjectOp(sqlite3 **sdbi, CephContext *_cct) : sdb(sdbi), cct(_cct) {};
    ~SQLObjectOp() {}

    int InitializeObjectOps(const DoutPrefixProvider *dpp);
    int FreeObjectOps(const DoutPrefixProvider *dpp);
};

class SQLInsertUser : public SQLiteDB, public InsertUserOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLInsertUser(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    ~SQLInsertUser() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLRemoveUser : public SQLiteDB, public RemoveUserOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLRemoveUser(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    ~SQLRemoveUser() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLGetUser : public SQLiteDB, public GetUserOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement
    sqlite3_stmt *email_stmt = NULL; // Prepared statement to query by useremail
    sqlite3_stmt *ak_stmt = NULL; // Prepared statement to query by access_key_id
    sqlite3_stmt *userid_stmt = NULL; // Prepared statement to query by user_id

  public:
    SQLGetUser(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    ~SQLGetUser() {
      if (stmt)
        sqlite3_finalize(stmt);
      if (email_stmt)
        sqlite3_finalize(email_stmt);
      if (ak_stmt)
        sqlite3_finalize(ak_stmt);
      if (userid_stmt)
        sqlite3_finalize(userid_stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLInsertBucket : public SQLiteDB, public InsertBucketOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLInsertBucket(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    ~SQLInsertBucket() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLUpdateBucket : public SQLiteDB, public UpdateBucketOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *info_stmt = NULL; // Prepared statement
    sqlite3_stmt *attrs_stmt = NULL; // Prepared statement
    sqlite3_stmt *owner_stmt = NULL; // Prepared statement

  public:
    SQLUpdateBucket(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    ~SQLUpdateBucket() {
      if (info_stmt)
        sqlite3_finalize(info_stmt);
      if (attrs_stmt)
        sqlite3_finalize(attrs_stmt);
      if (owner_stmt)
        sqlite3_finalize(owner_stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLRemoveBucket : public SQLiteDB, public RemoveBucketOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLRemoveBucket(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    ~SQLRemoveBucket() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLGetBucket : public SQLiteDB, public GetBucketOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLGetBucket(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    ~SQLGetBucket() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLListUserBuckets : public SQLiteDB, public ListUserBucketsOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLListUserBuckets(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    ~SQLListUserBuckets() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLInsertObject : public SQLiteDB, public InsertObjectOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLInsertObject(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    SQLInsertObject(sqlite3 **sdbi, CephContext *cct) : SQLiteDB(*sdbi, cct), sdb(sdbi) {}

    ~SQLInsertObject() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLRemoveObject : public SQLiteDB, public RemoveObjectOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLRemoveObject(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    SQLRemoveObject(sqlite3 **sdbi, CephContext *cct) : SQLiteDB(*sdbi, cct), sdb(sdbi) {}

    ~SQLRemoveObject() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLListObject : public SQLiteDB, public ListObjectOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLListObject(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    SQLListObject(sqlite3 **sdbi, CephContext *cct) : SQLiteDB(*sdbi, cct), sdb(sdbi) {}

    ~SQLListObject() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLPutObjectData : public SQLiteDB, public PutObjectDataOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLPutObjectData(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    SQLPutObjectData(sqlite3 **sdbi, CephContext *cct) : SQLiteDB(*sdbi, cct), sdb(sdbi) {}

    ~SQLPutObjectData() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLGetObjectData : public SQLiteDB, public GetObjectDataOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLGetObjectData(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    SQLGetObjectData(sqlite3 **sdbi, CephContext *cct) : SQLiteDB(*sdbi, cct), sdb(sdbi) {}

    ~SQLGetObjectData() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLDeleteObjectData : public SQLiteDB, public DeleteObjectDataOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLDeleteObjectData(void **db, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), cct), sdb((sqlite3 **)db) {}
    SQLDeleteObjectData(sqlite3 **sdbi, CephContext *cct) : SQLiteDB(*sdbi, cct), sdb(sdbi) {}

    ~SQLDeleteObjectData() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};
#endif
