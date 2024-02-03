// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <errno.h>
#include <stdlib.h>
#include <string>
#include <sqlite3.h>
#include "rgw/driver/dbstore/common/dbstore.h"

using namespace rgw::store;

class SQLiteDB : public DB, virtual public DBOp {
  private:
    sqlite3_mutex *mutex = NULL;

  protected:
    CephContext *cct;

  public:
    sqlite3_stmt *stmt = NULL;
    DBOpPrepareParams PrepareParams;

    SQLiteDB(sqlite3 *dbi, std::string db_name, CephContext *_cct) : DB(db_name, _cct), cct(_cct) {
      db = (void*)dbi;
    }
    SQLiteDB(std::string db_name, CephContext *_cct) : DB(db_name, _cct), cct(_cct) {
    }
    ~SQLiteDB() {}

    uint64_t get_blob_limit() override { return SQLITE_LIMIT_LENGTH; }
    void *openDB(const DoutPrefixProvider *dpp) override;
    int closeDB(const DoutPrefixProvider *dpp) override;
    int InitializeDBOps(const DoutPrefixProvider *dpp) override;

    int InitPrepareParams(const DoutPrefixProvider *dpp, DBOpPrepareParams &p_params,
                          DBOpParams* params) override;

    int exec(const DoutPrefixProvider *dpp, const char *schema,
        int (*callback)(void*,int,char**,char**));
    int Step(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt,
        int (*cbk)(const DoutPrefixProvider *dpp, DBOpInfo &op, sqlite3_stmt *stmt));
    int Reset(const DoutPrefixProvider *dpp, sqlite3_stmt *stmt);
    /* default value matches with sqliteDB style */

    int createTables(const DoutPrefixProvider *dpp) override;
    int createBucketTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int createUserTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int createObjectTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int createObjectDataTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int createObjectView(const DoutPrefixProvider *dpp, DBOpParams *params);
    int createObjectTableTrigger(const DoutPrefixProvider *dpp, DBOpParams *params);
    int createQuotaTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    void populate_object_params(const DoutPrefixProvider *dpp,
                                struct DBOpPrepareParams& p_params,
                                struct DBOpParams* params, bool data);

    int createLCTables(const DoutPrefixProvider *dpp) override;

    int DeleteBucketTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int DeleteUserTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int DeleteObjectTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int DeleteObjectDataTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int DeleteQuotaTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int DeleteLCEntryTable(const DoutPrefixProvider *dpp, DBOpParams *params);
    int DeleteLCHeadTable(const DoutPrefixProvider *dpp, DBOpParams *params);

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

    int InitializeObjectOps(std::string db_name, const DoutPrefixProvider *dpp);
};

class SQLInsertUser : public SQLiteDB, public InsertUserOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLInsertUser(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
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
    SQLRemoveUser(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
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
    SQLGetUser(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
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
    SQLInsertBucket(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
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
    SQLUpdateBucket(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
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
    SQLRemoveBucket(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
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
    SQLGetBucket(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
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
    sqlite3_stmt *all_stmt = NULL; // Prepared statement

  public:
    SQLListUserBuckets(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    ~SQLListUserBuckets() {
      if (stmt)
        sqlite3_finalize(stmt);
      if (all_stmt)
        sqlite3_finalize(all_stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLPutObject : public SQLiteDB, public PutObjectOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLPutObject(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLPutObject(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

    ~SQLPutObject() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLDeleteObject : public SQLiteDB, public DeleteObjectOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLDeleteObject(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLDeleteObject(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

    ~SQLDeleteObject() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLGetObject : public SQLiteDB, public GetObjectOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLGetObject(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLGetObject(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

    ~SQLGetObject() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLUpdateObject : public SQLiteDB, public UpdateObjectOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *omap_stmt = NULL; // Prepared statement
    sqlite3_stmt *attrs_stmt = NULL; // Prepared statement
    sqlite3_stmt *meta_stmt = NULL; // Prepared statement
    sqlite3_stmt *mp_stmt = NULL; // Prepared statement

  public:
    SQLUpdateObject(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLUpdateObject(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

    ~SQLUpdateObject() {
      if (omap_stmt)
        sqlite3_finalize(omap_stmt);
      if (attrs_stmt)
        sqlite3_finalize(attrs_stmt);
      if (meta_stmt)
        sqlite3_finalize(meta_stmt);
    }

    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLListBucketObjects : public SQLiteDB, public ListBucketObjectsOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLListBucketObjects(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLListBucketObjects(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

    ~SQLListBucketObjects() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLListVersionedObjects : public SQLiteDB, public ListVersionedObjectsOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLListVersionedObjects(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLListVersionedObjects(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

    ~SQLListVersionedObjects() {
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
    SQLPutObjectData(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLPutObjectData(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

    ~SQLPutObjectData() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLUpdateObjectData : public SQLiteDB, public UpdateObjectDataOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLUpdateObjectData(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLUpdateObjectData(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

    ~SQLUpdateObjectData() {
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
    SQLGetObjectData(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLGetObjectData(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

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
    SQLDeleteObjectData(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLDeleteObjectData(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

    ~SQLDeleteObjectData() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLDeleteStaleObjectData : public SQLiteDB, public DeleteStaleObjectDataOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLDeleteStaleObjectData(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    SQLDeleteStaleObjectData(sqlite3 **sdbi, std::string db_name, CephContext *cct) : SQLiteDB(*sdbi, db_name, cct), sdb(sdbi) {}

    ~SQLDeleteStaleObjectData() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLInsertLCEntry : public SQLiteDB, public InsertLCEntryOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLInsertLCEntry(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    ~SQLInsertLCEntry() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLRemoveLCEntry : public SQLiteDB, public RemoveLCEntryOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLRemoveLCEntry(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    ~SQLRemoveLCEntry() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLGetLCEntry : public SQLiteDB, public GetLCEntryOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement
    sqlite3_stmt *next_stmt = NULL; // Prepared statement

  public:
    SQLGetLCEntry(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    ~SQLGetLCEntry() {
      if (stmt)
        sqlite3_finalize(stmt);
      if (next_stmt)
        sqlite3_finalize(next_stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLListLCEntries : public SQLiteDB, public ListLCEntriesOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLListLCEntries(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    ~SQLListLCEntries() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLInsertLCHead : public SQLiteDB, public InsertLCHeadOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLInsertLCHead(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    ~SQLInsertLCHead() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLRemoveLCHead : public SQLiteDB, public RemoveLCHeadOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLRemoveLCHead(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    ~SQLRemoveLCHead() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};

class SQLGetLCHead : public SQLiteDB, public GetLCHeadOp {
  private:
    sqlite3 **sdb = NULL;
    sqlite3_stmt *stmt = NULL; // Prepared statement

  public:
    SQLGetLCHead(void **db, std::string db_name, CephContext *cct) : SQLiteDB((sqlite3 *)(*db), db_name, cct), sdb((sqlite3 **)db) {}
    ~SQLGetLCHead() {
      if (stmt)
        sqlite3_finalize(stmt);
    }
    int Prepare(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Execute(const DoutPrefixProvider *dpp, DBOpParams *params);
    int Bind(const DoutPrefixProvider *dpp, DBOpParams *params);
};
