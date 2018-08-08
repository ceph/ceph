// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef OBJECT_CACHE_STORE_H
#define OBJECT_CACHE_STORE_H

#include "common/debug.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/cache/SharedPersistentObjectCacherFile.h"
#include "SimplePolicy.hpp"


using librados::Rados;
using librados::IoCtx;

namespace rbd {
namespace cache {

typedef shared_ptr<librados::Rados> RadosRef;
typedef shared_ptr<librados::IoCtx> IoCtxRef;

class ObjectCacheStore 
{
  public:
    ObjectCacheStore(CephContext *cct, ContextWQ* work_queue);
    ~ObjectCacheStore();

    int init(bool reset);

    int shutdown();

    int lookup_object(std::string pool_name, std::string object_name);

    int init_cache(std::string vol_name, uint64_t vol_size);

    int lock_cache(std::string vol_name);

  private:
    void evict_thread_body();
    int evict_objects();

    int do_promote(std::string pool_name, std::string object_name);

    int promote_object(librados::IoCtx*, std::string object_name,
                       librados::bufferlist* read_buf,
                       uint64_t length);

    CephContext *m_cct;
    ContextWQ* m_work_queue;
    RadosRef m_rados;


    std::map<std::string, librados::IoCtx*> m_ioctxs;

    librbd::cache::SyncFile *m_cache_file;

    Policy* m_policy;
    std::thread* evict_thd;
    bool m_evict_go = false;
};

} // namespace rbd
} // namespace cache
#endif
