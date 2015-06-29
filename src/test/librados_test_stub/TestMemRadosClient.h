// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_MEM_RADOS_CLIENT_H
#define CEPH_TEST_MEM_RADOS_CLIENT_H

#include "test/librados_test_stub/TestRadosClient.h"
#include "include/atomic.h"
#include "include/assert.h"
#include "include/buffer.h"
#include "include/interval_set.h"
#include "common/RefCountedObj.h"
#include "common/RWLock.h"
#include <boost/shared_ptr.hpp>
#include <list>
#include <map>
#include <set>
#include <string>

namespace librados {

class AioCompletionImpl;

class TestMemRadosClient : public TestRadosClient {
public:

  typedef std::map<std::string, bufferlist> OMap;
  typedef std::map<std::string, OMap> FileOMaps;
  typedef std::map<std::string, bufferlist> FileTMaps;
  typedef std::map<std::string, bufferlist> XAttrs;
  typedef std::map<std::string, XAttrs> FileXAttrs;

  struct File {
    File();
    File(const File &rhs);

    bufferlist data;
    time_t mtime;

    uint64_t snap_id;
    std::vector<uint64_t> snaps;
    interval_set<uint64_t> snap_overlap;

    bool exists;
    RWLock lock;
  };
  typedef boost::shared_ptr<File> SharedFile;

  typedef std::list<SharedFile> FileSnapshots;
  typedef std::map<std::string, FileSnapshots> Files;

  typedef std::set<uint64_t> SnapSeqs;
  struct Pool : public RefCountedObject {
    Pool();

    int64_t pool_id;

    SnapSeqs snap_seqs;
    uint64_t snap_id;

    RWLock file_lock;
    Files files;
    FileOMaps file_omaps;
    FileTMaps file_tmaps;
    FileXAttrs file_xattrs;
  };

  TestMemRadosClient(CephContext *cct);

  virtual TestIoCtxImpl *create_ioctx(int64_t pool_id,
                                      const std::string &pool_name);

  virtual void object_list(int64_t pool_id, 
			   std::list<librados::TestRadosClient::Object> *list);

  virtual int pool_create(const std::string &pool_name);
  virtual int pool_delete(const std::string &pool_name);
  virtual int pool_get_base_tier(int64_t pool_id, int64_t* base_tier);
  virtual int pool_list(std::list<std::pair<int64_t, std::string> >& v);
  virtual int64_t pool_lookup(const std::string &name);
  virtual int pool_reverse_lookup(int64_t id, std::string *name);

  virtual int watch_flush();

  virtual int blacklist_add(const std::string& client_address,
			    uint32_t expire_seconds);
protected:
  ~TestMemRadosClient();

private:

  typedef std::map<std::string, Pool*>		Pools;

  Pools	m_pools;
  int64_t m_pool_id;

};

} // namespace librados

#endif // CEPH_TEST_MEM_RADOS_CLIENT_H
