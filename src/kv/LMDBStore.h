#ifndef LM_DB_STORE_H
#define LM_DB_STORE_H

#include "include/types.h"
#include "include/buffer.h"
#include "KeyValueDB.h"
#include <set>
#include <map>
#include <queue>
#include <string>

#include <errno.h>
#include "common/errno.h"
#include "common/dout.h"
#include "include/assert.h"
#include "common/Formatter.h"

#include "common/ceph_context.h"

#include "lmdb.h"

class PerfCounters;

enum {
  l_lmdb_first = 34300,
  l_lmdb_gets,
  l_lmdb_txns,
  l_lmdb_get_latency,
  l_lmdb_submit_latency,
  l_lmdb_submit_sync_latency,
  l_lmdb_last,
};

/**
 * Uses LMDB to implement the KeyValueDB interface
 */
class LMDBStore : public KeyValueDB {
  CephContext *cct;
  PerfCounters *logger;
  string path;
  std::shared_ptr<MDB_env> env;
  MDB_dbi dbi;
  int do_open(ostream &out, bool create_if_missing);

public:

  static int _test_init(const string& dir);
  int init(string option_str="");
  /**
   * options_t: Holds options which are minimally interpreted
   * on initialization and then passed through to LMDB.
   * We transform a couple of these into actual LMDB
   * structures, but the rest are simply passed through unchanged. See
   * lmdb.h for more precise details on each.
   *
   * Set them after constructing the LMDBStore, but before calling
   * open() or create_and_open().
   */
  struct options_t {
    uint64_t map_size; /// size of memory map
    uint64_t max_readers; /// max number of threads/reader
    bool nomeminit;  // do not initialize memory
    bool noreadahead; // turn off read ahead
    bool writemap;   // use write map


    options_t() :
      map_size(0),
      max_readers(0), 
      nomeminit(false),
      noreadahead(false),
      writemap(false)
    {}
  } options;

  LMDBStore(CephContext *c, const string &path);

  ~LMDBStore();

  static bool check_omap_dir(string &omap_dir);
  /// Opens underlying db
  int open(ostream &out) {
    return do_open(out, false);
  }
  /// Creates underlying db if missing and opens it
  int create_and_open(ostream &out) override;

  void close();

  class LMDBTask {
  public:
    virtual void submit(MDB_txn *txn, MDB_dbi dbi) {}; 
  };

  class LMDBPutTask : public LMDBTask {
  public:
    LMDBPutTask(string key, string value) : key(key), value(value) {};
    string key, value;
    void submit(MDB_txn *txn, MDB_dbi dbi) override;
  };

  class LMDBDelTask : public LMDBTask {
  public:
    LMDBDelTask(string key) : key(key) {};
    string key;
    void submit(MDB_txn *txn, MDB_dbi dbi) override;
  };

  class LMDBMergeTask : public LMDBTask {
  public:
    LMDBMergeTask(string key, string value, LMDBStore *db) : 
      key(key), value(value), db(db) {};
    string key;
    string value;
    LMDBStore *db;
    void submit(MDB_txn *txn, MDB_dbi dbi) override;
  };

  class LMDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    list<bufferlist> buffers;
    list<string> keys;
    std::queue<std::unique_ptr<LMDBTask>> tasks;
    LMDBStore *db;
    MDB_dbi dbi;
    LMDBTransactionImpl(LMDBStore *_db);
    ~LMDBTransactionImpl();
    void do_tasks(MDB_txn *txn, MDB_dbi dbi);
    void set(
      const string &prefix,
      const string &key,
      const bufferlist &bl);
    void rmkey(
      const string &prefix,
      const string &key);
    void rmkeys_by_prefix(
      const string &prefix
      );
    void rm_range_keys(
      const string &prefix,
      const string &start,
      const string &end
      );
    void merge(
      const string& prefix,
      const string& key,
      const bufferlist &bl) override;
  };

  KeyValueDB::Transaction get_transaction() override {
    return std::make_shared<LMDBTransactionImpl>(this);
  }

  int submit_transaction(KeyValueDB::Transaction t);
  int submit_transaction_sync(KeyValueDB::Transaction t);
  int get(
    const string &prefix,
    const std::set<string> &key,
    std::map<string, bufferlist> *out
    );

  class LMDBWholeSpaceIteratorImpl :
    public KeyValueDB::WholeSpaceIteratorImpl {
  protected:
    LMDBStore *store;
    MDB_cursor *cursor;
    MDB_txn *txn;
    bool invalid;
  public:
    LMDBWholeSpaceIteratorImpl(LMDBStore *_store);
   
    ~LMDBWholeSpaceIteratorImpl();

    int seek_to_first();
    int seek_to_first(const string &prefix);
    int seek_to_last();
    int seek_to_last(const string &prefix);
    int upper_bound(const string &prefix, const string &after);
    int lower_bound(const string &prefix, const string &to);
    bool valid();
    int next();
    int prev();
    string key();
    pair<string,string> raw_key();
    bool raw_key_is_prefixed(const string &prefix) override;
    bufferlist value();
    int status();
  };

  /// Utility
  static string combine_strings(const string &prefix, const string &value);
  static int split_key(string &in, string *prefix, string *key);
  static bufferlist to_bufferlist(string &in);
  static string past_prefix(const string &prefix);
  int set_merge_operator(const std::string& prefix,
		                 std::shared_ptr<KeyValueDB::MergeOperator> mop) override;
  virtual uint64_t get_estimated_size(map<string,uint64_t> &extra) {
    DIR *store_dir = opendir(path.c_str());
    if (!store_dir) {
      lderr(cct) << __func__ << " something happened opening the store: "
                 << cpp_strerror(errno) << dendl;
      return 0;
    }

    uint64_t total_size = 0;
    uint64_t data_size = 0;
    uint64_t misc_size = 0;

    struct dirent *entry = NULL;
    while ((entry = readdir(store_dir)) != NULL) {
      string n(entry->d_name);

      if (n == "." || n == "..")
        continue;

      string fpath = path + '/' + n;
      struct stat s;
      int err = stat(fpath.c_str(), &s);
      if (err < 0)
	err = -errno;
      // we may race against lmdb while reading files; this should only
      // happen when those files are being updated, data is being shuffled
      // and files get removed, in which case there's not much of a problem
      // as we'll get to them next time around.
      if (err == -ENOENT) {
	continue;
      }
      if (err < 0) {
        lderr(cct) << __func__ << " error obtaining stats for " << fpath
                   << ": " << cpp_strerror(err) << dendl;
        goto err;
      }

      size_t pos = n.find_last_of('.');
      if (pos == string::npos) {
        misc_size += s.st_size;
        continue;
      }

      string ext = n.substr(0, pos);
      if (ext == "data") {
        data_size += s.st_size;
      } else {
        misc_size += s.st_size;
      }
    }

    total_size = data_size + misc_size;

    extra["data"] = data_size;
    extra["misc"] = misc_size;
    extra["total"] = total_size;

err:
    closedir(store_dir);
    return total_size;
  }


protected:
  WholeSpaceIterator _get_iterator();

  WholeSpaceIterator _get_snapshot_iterator() {
    return _get_iterator();
  }

};

#endif
