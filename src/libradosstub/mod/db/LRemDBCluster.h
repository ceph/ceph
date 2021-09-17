// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#pragma once

#include "LRemCluster.h"
#include "LRemTransaction.h"
#include "include/buffer.h"
#include "include/interval_set.h"
#include "include/int_types.h"
#include "common/ceph_mutex.h"
#include <boost/shared_ptr.hpp>
#include <list>
#include <map>
#include <set>
#include <string>

namespace librados {

namespace LRemDBStore {
  class Cluster;
}

class LRemDBCluster : public LRemCluster {
public:
  typedef struct {
    bufferlist header;
    std::map<std::string, bufferlist> data;
  } OMap;
  typedef std::map<ObjectLocator, OMap> FileOMaps;
  typedef std::map<ObjectLocator, bufferlist> FileTMaps;
  typedef std::map<std::string, bufferlist> XAttrs;
  typedef std::map<ObjectLocator, XAttrs> FileXAttrs;
  typedef std::set<ObjectHandler*> ObjectHandlers;
  typedef std::map<ObjectLocator, ObjectHandlers> FileHandlers;

  struct File {
    File();
    File(const File &rhs);

    struct timespec mtime;
    uint64_t objver;

    uint64_t snap_id;
    std::vector<uint64_t> snaps;
    interval_set<uint64_t> snap_overlap;

    uint64_t epoch = 0;

    bool exists;
    ceph::shared_mutex lock =
      ceph::make_shared_mutex("LRemDBCluster::File::lock");
  };
  typedef boost::shared_ptr<File> SharedFile;

  typedef std::list<SharedFile> FileSnapshots;
  typedef std::map<ObjectLocator, FileSnapshots> Files;

  struct FileLock {
    ceph::shared_mutex lock =
      ceph::make_shared_mutex("LRemDBCluster::FileLock::lock");
  };

  typedef std::map<ObjectLocator, FileLock> FileLocks;
  typedef std::set<uint64_t> SnapSeqs;
  struct Pool {
    Pool();

    int64_t pool_id = 0;

    SnapSeqs snap_seqs;
    uint64_t snap_id = 1;

    uint64_t epoch = 1;

    ceph::shared_mutex file_lock =
      ceph::make_shared_mutex("LRemDBCluster::Pool::file_lock");
    Files files;
    FileLocks file_locks;
    FileOMaps file_omaps;
    FileTMaps file_tmaps;
    FileXAttrs file_xattrs;
    FileHandlers file_handlers;
  };
  using PoolRef = std::shared_ptr<Pool>;

  LRemDBCluster(CephContext *cct);
  ~LRemDBCluster() override;

  LRemRadosClient *create_rados_client(CephContext *cct) override;

  int register_object_handler(LRemRadosClient *client,
                              int64_t pool_id, const ObjectLocator& locator,
                              ObjectHandler* object_handler) override;
  void unregister_object_handler(LRemRadosClient *client,
                                 int64_t pool_id, const ObjectLocator& locator,
                                 ObjectHandler* object_handler) override;

  int pool_create(LRemDBStore::Cluster& dbc, const std::string &pool_name);
  int pool_delete(LRemDBStore::Cluster& dbc, const std::string &pool_name);
  int pool_get_base_tier(int64_t pool_id, int64_t* base_tier);
  int pool_list(std::list<std::pair<int64_t, std::string> >& v);
  int64_t pool_lookup(const std::string &name);
  int pool_reverse_lookup(int64_t id, std::string *name);

  PoolRef get_pool(LRemDBStore::Cluster& dbc,
                   int64_t pool_id);
  PoolRef get_pool(LRemDBStore::Cluster& dbc,
                   const std::string &pool_name);

  void allocate_client(uint32_t *nonce, uint64_t *global_id);
  void deallocate_client(uint32_t nonce);

  bool is_blocklisted(uint32_t nonce) const;
  void blocklist(LRemRadosClient *rados_client, uint32_t nonce);

  void transaction_start(LRemTransactionStateRef& state);
  void transaction_finish(LRemTransactionStateRef& state);

  int init(LRemDBStore::Cluster& dbc);
private:

  typedef std::map<std::string, PoolRef>		Pools;
  typedef std::set<uint32_t> Blocklist;

  CephContext *cct;

  mutable ceph::mutex m_lock =
    ceph::make_mutex("LRemDBCluster::m_lock");

  Pools	m_pools;
  int64_t m_pool_id = 0;

  uint32_t m_next_nonce;
  uint64_t m_next_global_id = 1234;

  Blocklist m_blocklist;

  ceph::condition_variable m_transaction_cond;
  std::set<ObjectLocator> m_transactions;

  LRemDBCluster::PoolRef make_pool(const string& name, int id);
  PoolRef get_cached_pool(const ceph::mutex& lock,
                          int64_t pool_id);

  PoolRef get_pool(const ceph::mutex& lock,
                   LRemDBStore::Cluster& dbc,
                   int64_t pool_id);

  PoolRef get_pool(const ceph::mutex& lock,
                   LRemDBStore::Cluster& dbc,
                   const std::string &pool_name);
};

} // namespace librados
