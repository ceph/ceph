// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_POLICY_H
#define CEPH_CACHE_POLICY_H

#include <list>
#include <string>

namespace ceph {
namespace immutable_obj_cache {

typedef enum {
  OBJ_CACHE_NONE = 0,
  OBJ_CACHE_PROMOTED,
  OBJ_CACHE_SKIP,
} cache_status_t;

class Policy {
 public:
  Policy() {}
  virtual ~Policy() {}
  virtual cache_status_t lookup_object(std::string) = 0;
  virtual int evict_entry(std::string) = 0;
  virtual void update_status(std::string, cache_status_t,
                             uint64_t size = 0) = 0;
  virtual cache_status_t get_status(std::string) = 0;
  virtual void get_evict_list(std::list<std::string>* obj_list) = 0;
};

}  // namespace immutable_obj_cache
}  // namespace ceph
#endif
