// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_MEM_IO_CTX_IMPL_H
#define CEPH_TEST_MEM_IO_CTX_IMPL_H

#include "test/librados_test_stub/TestIoCtxImpl.h"
#include "test/librados_test_stub/TestMemCluster.h"

namespace librados {

class TestMemRadosClient;

class TestMemIoCtxImpl : public TestIoCtxImpl {
public:
  TestMemIoCtxImpl();
  TestMemIoCtxImpl(TestMemRadosClient *client, int64_t m_pool_id,
                   const std::string& pool_name,
                   TestMemCluster::Pool *pool);
  ~TestMemIoCtxImpl() override;

  TestIoCtxImpl *clone() override;

  int aio_remove(const std::string& oid, AioCompletionImpl *c, int flags = 0) override;

  int append(const std::string& oid, const bufferlist &bl,
             const SnapContext &snapc) override;

  int assert_exists(const std::string &oid) override;

  int create(const std::string& oid, bool exclusive) override;
  int list_snaps(const std::string& o, snap_set_t *out_snaps) override;
  int omap_get_vals(const std::string& oid,
                    const std::string& start_after,
                    const std::string &filter_prefix,
                    uint64_t max_return,
                    std::map<std::string, bufferlist> *out_vals) override;
  int omap_get_vals2(const std::string& oid,
                    const std::string& start_after,
                    const std::string &filter_prefix,
                    uint64_t max_return,
                    std::map<std::string, bufferlist> *out_vals,
                    bool *pmore) override;
  int omap_rm_keys(const std::string& oid,
                   const std::set<std::string>& keys) override;
  int omap_set(const std::string& oid, const std::map<std::string,
               bufferlist> &map) override;
  int read(const std::string& oid, size_t len, uint64_t off,
           bufferlist *bl) override;
  int remove(const std::string& oid, const SnapContext &snapc) override;
  int selfmanaged_snap_create(uint64_t *snapid) override;
  int selfmanaged_snap_remove(uint64_t snapid) override;
  int selfmanaged_snap_rollback(const std::string& oid,
                                uint64_t snapid) override;
  int set_alloc_hint(const std::string& oid, uint64_t expected_object_size,
                     uint64_t expected_write_size,
                     const SnapContext &snapc) override;
  int sparse_read(const std::string& oid, uint64_t off, uint64_t len,
                  std::map<uint64_t,uint64_t> *m, bufferlist *data_bl) override;
  int stat(const std::string& oid, uint64_t *psize, time_t *pmtime) override;
  int truncate(const std::string& oid, uint64_t size,
               const SnapContext &snapc) override;
  int write(const std::string& oid, bufferlist& bl, size_t len,
            uint64_t off, const SnapContext &snapc) override;
  int write_full(const std::string& oid, bufferlist& bl,
                 const SnapContext &snapc) override;
  int writesame(const std::string& oid, bufferlist& bl, size_t len,
                uint64_t off, const SnapContext &snapc) override;
  int cmpext(const std::string& oid, uint64_t off, bufferlist& cmp_bl) override;
  int xattr_get(const std::string& oid,
                std::map<std::string, bufferlist>* attrset) override;
  int xattr_set(const std::string& oid, const std::string &name,
                bufferlist& bl) override;
  int zero(const std::string& oid, uint64_t off, uint64_t len,
           const SnapContext &snapc) override;

protected:
  TestMemCluster::Pool *get_pool() {
    return m_pool;
  }

private:
  TestMemIoCtxImpl(const TestMemIoCtxImpl&);

  TestMemRadosClient *m_client = nullptr;
  TestMemCluster::Pool *m_pool = nullptr;

  void append_clone(bufferlist& src, bufferlist* dest);
  size_t clip_io(size_t off, size_t len, size_t bl_len);
  void ensure_minimum_length(size_t len, bufferlist *bl);

  TestMemCluster::SharedFile get_file(const std::string &oid, bool write,
                                      const SnapContext &snapc);

};

} // namespace librados

#endif // CEPH_TEST_MEM_IO_CTX_IMPL_H
