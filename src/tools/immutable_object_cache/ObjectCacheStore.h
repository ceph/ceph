// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_OBJECT_CACHE_STORE_H
#define CEPH_CACHE_OBJECT_CACHE_STORE_H

#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "include/rados/librados.hpp"

#include "SimplePolicy.h"


using librados::Rados;
using librados::IoCtx;
class Context;

namespace ceph {
namespace immutable_obj_cache {

typedef shared_ptr<librados::Rados> RadosRef;
typedef shared_ptr<librados::IoCtx> IoCtxRef;

class ObjectCacheStore {
 public:
  ObjectCacheStore(CephContext *cct);
  ~ObjectCacheStore();
  int init(bool reset);
  int shutdown();
  int init_cache();
  int lookup_object(std::string pool_nspace,
                    uint64_t pool_id, uint64_t snap_id,
                    std::string object_name,
                    std::string& target_cache_file_path);

 private:
  std::string get_cache_file_name(std::string pool_nspace, uint64_t pool_id,
                                  uint64_t snap_id, std::string oid);
  std::string get_cache_file_path(std::string cache_file_name,
                                  bool mkdir = false);
  int evict_objects();
  int do_promote(std::string pool_nspace, uint64_t pool_id,
                 uint64_t snap_id, std::string object_name);
  int promote_object(librados::IoCtx*, std::string object_name,
                     librados::bufferlist* read_buf,
                     Context* on_finish);
  int handle_promote_callback(int, bufferlist*, std::string);
  int do_evict(std::string cache_file);

  CephContext *m_cct;
  RadosRef m_rados;
  std::map<uint64_t, librados::IoCtx> m_ioctx_map;
  Mutex m_ioctx_map_lock;
  Policy* m_policy;
  std::string m_cache_root_dir;
};

}  // namespace immutable_obj_cache
}  // ceph
#endif  // CEPH_CACHE_OBJECT_CACHE_STORE_H
