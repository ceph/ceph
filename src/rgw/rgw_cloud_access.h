#ifndef CEPH_RGW_CLOUD_ACCESS_H
#define CEPH_RGW_CLOUD_ACCESS_H

#include <string>

#include "include/buffer.h"

struct RGWCloudInfo {
  std::string domain_name;
  std::string bucket_host;
  std::string bucket_region;
  std::string dest_bucket;
  std::string bucket_prefix;
  std::string public_key;
  std::string private_key;
  std::string access_type;
};

class RGWCloudAccess {
protected:
  CephContext *cct;
  RGWCloudInfo& cloud_info;
public:
  RGWCloudAccess(RGWCloudInfo& _cloud_info) :cct(nullptr), cloud_info(_cloud_info)
  { }
  virtual ~RGWCloudSyncAccess() { }
  
  void set_ceph_context(CephContext* _cct) { cct = _cct; }
  
  virtual int put_obj(std::string& bucket, std::string& key, bufferlist *data, off_t len) = 0;
  virtual int remove_obj(std::string& bucket, std::string& key) = 0;
  
};

#endif
