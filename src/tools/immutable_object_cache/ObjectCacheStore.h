// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_OBJECT_CACHE_STORE_H
#define CEPH_CACHE_OBJECT_CACHE_STORE_H

#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "common/Throttle.h"
#include "common/Cond.h"
#include "include/rados/librados.hpp"

#include "SimplePolicy.h"


using librados::Rados;
using librados::IoCtx;
class Context;

namespace ceph {
namespace immutable_obj_cache {

typedef std::shared_ptr<librados::Rados> RadosRef;
typedef std::shared_ptr<librados::IoCtx> IoCtxRef;

class ObjectCacheStore {
 public:
  ObjectCacheStore(CephContext *cct);
  ~ObjectCacheStore();
  int init(bool reset);
  int shutdown();
  int init_cache();
  int lookup_object(std::string pool_nspace,
                    uint64_t pool_id, uint64_t snap_id,
                    uint64_t object_size,
                    std::string object_name,
                    bool return_dne_path,
                    std::string& target_cache_file_path);
 private:
  enum ThrottleTypeCode {
    THROTTLE_CODE_BYTE,
    THROTTLE_CODE_OBJECT
  };

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

  bool take_token_from_throttle(uint64_t object_size, uint64_t object_num);
  void handle_throttle_ready(uint64_t tokens, uint64_t type);
  void apply_qos_tick_and_limit(const uint64_t flag,
                                std::chrono::milliseconds min_tick,
                                uint64_t limit, uint64_t burst,
                                std::chrono::seconds burst_seconds);

  CephContext *m_cct;
  RadosRef m_rados;
  std::map<uint64_t, librados::IoCtx> m_ioctx_map;
  ceph::mutex m_ioctx_map_lock =
    ceph::make_mutex("ceph::cache::ObjectCacheStore::m_ioctx_map_lock");
  Policy* m_policy;
  std::string m_cache_root_dir;
  // throttle mechanism
  uint64_t m_qos_enabled_flag{0};
  std::map<uint64_t, TokenBucketThrottle*> m_throttles;
  bool m_io_throttled{false};
  ceph::mutex m_throttle_lock =
    ceph::make_mutex("ceph::cache::ObjectCacheStore::m_throttle_lock");;
  uint64_t m_iops_tokens{0};
  uint64_t m_bps_tokens{0};
};

}  // namespace immutable_obj_cache
}  // ceph
#endif  // CEPH_CACHE_OBJECT_CACHE_STORE_H
