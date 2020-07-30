// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_MEM_CLUSTER_H
#define CEPH_TEST_MEM_CLUSTER_H

#include "test/librados_test_stub/TestCluster.h"
#include "include/buffer.h"
#include "include/interval_set.h"
#include "include/int_types.h"
#include "common/ceph_mutex.h"
#include "common/RefCountedObj.h"
#include "common/RWLock.h"
#include <boost/shared_ptr.hpp>
#include <list>
#include <map>
#include <set>
#include <string>

namespace librados {

class TestMemCluster : public TestCluster {
public:
  typedef std::map<std::string, bufferlist> OMap;
  typedef std::map<ObjectLocator, OMap> FileOMaps;
  typedef std::map<ObjectLocator, bufferlist> FileTMaps;
  typedef std::map<std::string, bufferlist> XAttrs;
  typedef std::map<ObjectLocator, XAttrs> FileXAttrs;
  typedef std::set<ObjectHandler*> ObjectHandlers;
  typedef std::map<ObjectLocator, ObjectHandlers> FileHandlers;

  struct File {
    File();
    File(const File &rhs);

    bufferlist data;
    time_t mtime;
    uint64_t objver;

    uint64_t snap_id;
    std::vector<uint64_t> snaps;
    interval_set<uint64_t> snap_overlap;

    bool exists;
    ceph::shared_mutex lock =
      ceph::make_shared_mutex("TestMemCluster::File::lock");
  };
  typedef boost::shared_ptr<File> SharedFile;

  typedef std::list<SharedFile> FileSnapshots;
  typedef std::map<ObjectLocator, FileSnapshots> Files;

  typedef std::set<uint64_t> SnapSeqs;
  struct Pool : public RefCountedObject {
    Pool();

    int64_t pool_id = 0;

    SnapSeqs snap_seqs;
    uint64_t snap_id = 1;

    ceph::shared_mutex file_lock =
      ceph::make_shared_mutex("TestMemCluster::Pool::file_lock");
    Files files;
    FileOMaps file_omaps;
    FileTMaps file_tmaps;
    FileXAttrs file_xattrs;
    FileHandlers file_handlers;
  };

  TestMemCluster();
  ~TestMemCluster() override;

  TestRadosClient *create_rados_client(CephContext *cct) override;

  int register_object_handler(int64_t pool_id, const ObjectLocator& locator,
                              ObjectHandler* object_handler) override;
  void unregister_object_handler(int64_t pool_id, const ObjectLocator& locator,
                                 ObjectHandler* object_handler) override;

  int pool_create(const std::string &pool_name);
  int pool_delete(const std::string &pool_name);
  int pool_get_base_tier(int64_t pool_id, int64_t* base_tier);
  int pool_list(std::list<std::pair<int64_t, std::string> >& v);
  int64_t pool_lookup(const std::string &name);
  int pool_reverse_lookup(int64_t id, std::string *name);

  Pool *get_pool(int64_t pool_id);
  Pool *get_pool(const std::string &pool_name);

  void allocate_client(uint32_t *nonce, uint64_t *global_id);
  void deallocate_client(uint32_t nonce);

  bool is_blacklisted(uint32_t nonce) const;
  void blacklist(uint32_t nonce);

  void transaction_start(const ObjectLocator& locator);
  void transaction_finish(const ObjectLocator& locator);

private:

  typedef std::map<std::string, Pool*>		Pools;
  typedef std::set<uint32_t> Blacklist;

  mutable ceph::mutex m_lock =
    ceph::make_mutex("TestMemCluster::m_lock");

  Pools	m_pools;
  int64_t m_pool_id = 0;

  uint32_t m_next_nonce;
  uint64_t m_next_global_id = 1234;

  Blacklist m_blacklist;

  ceph::condition_variable m_transaction_cond;
  std::set<ObjectLocator> m_transactions;

  Pool *get_pool(const ceph::mutex& lock, int64_t pool_id);

};

} // namespace librados

#endif // CEPH_TEST_MEM_CLUSTER_H
