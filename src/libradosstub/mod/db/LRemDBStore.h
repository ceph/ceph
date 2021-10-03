// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "SQLiteCpp/SQLiteCpp.h"

#include "include/interval_set.h"

#include "LRemTransaction.h"

namespace librados {

class LRemDBOps {
  std::string name;
  int flags;

  std::unique_ptr<SQLite::Database> db;

  struct queued_statement {
    int count{0};
    std::unique_ptr<SQLite::Statement> statement;
  };

  std::map<int, queued_statement> deferred_statements;
  std::map<string, int> statement_keys;

  int statement_num{0};

  void queue_remove_key(const std::string& key);
public:
  LRemDBOps(const std::string& _name, int _flags);

  int exec(const std::string& sql);
  int exec(SQLite::Statement& stmt);
  int exec_step(SQLite::Statement& stmt);
  int create_table(const std::string& name, const std::string& defs);

  SQLite::Statement statement(const std::string& sql);
  SQLite::Statement *deferred_statement(const std::string& sql);

  void queue_statement(SQLite::Statement *statement, const std::string& key);
  void queue_statement(SQLite::Statement *statement, std::vector<std::string>& keys);
  void queue_statement_range(SQLite::Statement *statement,
                             const std::string& begin,
                             const std::string& end);

  void flush();

  struct Transaction {
    int retcode{0};
    std::unique_ptr<SQLite::Transaction> trans;

    void *p;

    void commit() {
      trans->commit();
      trans.reset();
    }

    void abort() {
      trans.reset();
    }

    Transaction(SQLite::Database& db);
    ~Transaction();

    void complete_op(int _r);
  };

  Transaction new_transaction();
  Transaction *alloc_transaction();
};

using LRemDBOpsRef = std::shared_ptr<LRemDBOps>;

class Cluster;
class LRemDBTransactionState;

namespace LRemDBStore {

  class TableBase {
  protected:
    LRemDBTransactionState *trans;

    int pool_id;

    std::string table_name;

    std::string nspace;
    std::string oid;

    void init_table_name(const std::string& table_name_prefix);

  public:
    TableBase(LRemDBTransactionState *_trans, int _pool_id,
              const std::string& table_name_prefix) : trans(_trans), pool_id(_pool_id) {
      set_instance(_trans);
      init_table_name(table_name_prefix);
    }
    virtual ~TableBase() {}

    virtual int create_table() = 0;

    void set_instance(LRemDBTransactionState *_trans);

    const string& get_table_name() const {
      return table_name;
    }
  };

  class Obj : public TableBase {
  public:
    Obj(LRemDBTransactionState *_trans, int _pool_id) : TableBase(_trans, _pool_id, "obj") {}

    struct Meta {
      uint64_t size = 0;

      ceph::real_time mtime;
      uint64_t objver = 0;

      uint64_t snap_id = -1;
      std::vector<uint64_t> snaps;
      interval_set<uint64_t> snap_overlap;

      uint64_t epoch = 0;

      void touch(uint64_t epoch);
    };

    int create_table() override;

    int read_meta(LRemDBStore::Obj::Meta *pmeta);
    int write_meta(const LRemDBStore::Obj::Meta& pmeta);
    int remove_meta();

    int read_data(uint64_t ofs, uint64_t len, bufferlist *bl);
    int write_data(uint64_t ofs, uint64_t len, const bufferlist& bl);
    int remove_data();

    int write(uint64_t ofs, uint64_t len,
              const bufferlist& bl,
              uint64_t epoch);
    int write(uint64_t ofs, uint64_t len,
              const bufferlist& bl,
              LRemDBStore::Obj::Meta& meta);

    int truncate(uint64_t ofs,
                 LRemDBStore::Obj::Meta& meta);

    int append(const bufferlist& bl,
               uint64_t epoch);

    int remove();
  };
  using ObjRef = std::shared_ptr<Obj>;

  class ObjData : public TableBase {
    static constexpr int block_size = (512 * 1024);

    int write_block(int bid, bufferlist& bl);
    int read_block(int bid, bufferlist *bl);
    int truncate_block(int bid);

  public:
    ObjData(LRemDBTransactionState *_trans, int _pool_id) : TableBase(_trans, _pool_id, "objdata") {}
    int create_table() override;

    int read(uint64_t ofs, uint64_t len, bufferlist *bl);
    int write(uint64_t ofs, uint64_t len, const bufferlist& bl);
    int remove();
    int truncate(uint64_t ofs);
  };
  using ObjDataRef = std::shared_ptr<ObjData>;

  class KVTableBase : public TableBase {
  public:
    KVTableBase(LRemDBTransactionState *_trans, int _pool_id,
                const std::string& table_name_prefix) : TableBase(_trans, _pool_id, table_name_prefix) {}
    virtual ~KVTableBase() {}

    int create_table() override;

    int get_vals(const std::string& start_after,
                 const std::string &filter_prefix,
                 uint64_t max_return,
                 std::map<std::string, bufferlist> *out_vals,
                 bool *pmore);
    int get_all_vals(std::map<std::string, bufferlist> *out_vals);
    int get_vals_by_keys(const std::set<std::string>& keys,
                         std::map<std::string, bufferlist> *out_vals);
    int get_val(const std::string& key,
                bufferlist *bl);
    int rm_keys(const std::set<std::string>& keys);
    int rm_range(const string& key_begin,
                 const string& key_end);
    int clear();
    int set(const std::map<std::string, bufferlist>& m);

    int get_header(bufferlist *bl);
    int set_header(const bufferlist& bl);
  };

  class OMap : public KVTableBase {
  public:
    OMap(LRemDBTransactionState *_trans, int _pool_id) : KVTableBase(_trans, _pool_id, "omap") {}
  };
  using OMapRef = std::shared_ptr<OMap>;

  class XAttrs : public KVTableBase {
  public:
    XAttrs(LRemDBTransactionState *_trans, int _pool_id) : KVTableBase(_trans, _pool_id, "xattrs") {}
  };
  using XAttrsRef = std::shared_ptr<XAttrs>;

  class Pool {
    LRemDBTransactionState *trans;

    int id;
    std::string name;
    std::string value;

    int init_tables();

  public:
    Pool(LRemDBTransactionState *_trans) : trans(_trans) {}
    Pool(LRemDBTransactionState *_trans, int _id, std::string _name, std::string _value) : trans(_trans),
                                                            id(_id), name(_name), value(_value) {}

    int get_id() const {
      return id;
    }

    const std::string& get_name() const {
      return name;
    }

    int create(const std::string& _name, const std::string& _val);
    int read();
    int list(std::optional<string> nspace,
             const string& marker_oid,
             std::optional<string> filter,
             int max,
             std::list<LRemCluster::ObjectLocator> *result,
             bool *more);

    ObjRef get_obj_handler();
    XAttrsRef get_xattrs_handler();
    OMapRef get_omap_handler();
  };
  using PoolRef = std::shared_ptr<Pool>;

  class Cluster {
    LRemDBTransactionState *trans;

  public:
    Cluster(CephContext *cct,
            LRemDBTransactionState *_trans);

    int init_cluster();
    int create_pool(const std::string& name, const std::string& val);
    int get_pool(const std::string& name, PoolRef *pool);
    int get_pool(int id, PoolRef *pool);
    int list_pools(std::map<std::string, PoolRef> *pools);
  };
  using ClusterRef = std::shared_ptr<Cluster>;

} // namespace LRemDBStore

struct LRemDBTransactionState : public LRemTransactionState {
  CephContext *cct;

  LRemDBOpsRef dbo;
  LRemDBStore::ClusterRef dbc;

  std::unique_ptr<LRemDBOps::Transaction> db_trans;

  struct {
    std::optional<LRemDBStore::Obj::Meta> meta;
  } cache;

  std::map<int, bufferlist> data_blocks;

  LRemDBTransactionState(CephContext *_cct);
  LRemDBTransactionState(CephContext *_cct,
                         const LRemCluster::ObjectLocator& loc);
  ~LRemDBTransactionState();

  void init(bool start_trans);

  void set_write(bool w) override;

  void commit() {
    if (db_trans) {
      db_trans->commit();
    }
  }

};

using LRemDBTransactionStateRef = std::shared_ptr<LRemDBTransactionState>;
}
