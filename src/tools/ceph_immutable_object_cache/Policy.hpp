// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_POLICY_HPP
#define CEPH_CACHE_POLICY_HPP

#include <list>
#include <string>

namespace ceph {
namespace immutable_obj_cache {

enum CACHESTATUS {
  OBJ_CACHE_NONE = 0,
  OBJ_CACHE_PROMOTING,
  OBJ_CACHE_PROMOTED,
};


class Policy {
public:
  Policy(){}
  virtual ~Policy(){};
  virtual CACHESTATUS lookup_object(std::string) = 0;
  virtual int evict_object(std::string&) = 0;
  virtual void update_status(std::string, CACHESTATUS) = 0;
  virtual CACHESTATUS get_status(std::string) = 0;
  virtual void get_evict_list(std::list<std::string>* obj_list) = 0;
};

} // namespace immutable_obj_cache
} // namespace ceph
#endif
