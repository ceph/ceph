#ifndef CEPH_RGW_CLOUD_ACCESS_H
#define CEPH_RGW_CLOUD_ACCESS_H

#include <string>
#include <map>

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
  std::string upload_id;
  std::map<uint64_t, std::string> etags;              //hash value of  every part data
  uint64_t block_size = 4*1024*1024;
  uint64_t part_number = 0;

protected:
  CephContext *cct;
  RGWCloudInfo& cloud_info;
public:
  RGWCloudAccess(RGWCloudInfo& _cloud_info) :cct(nullptr), cloud_info(_cloud_info)
  { }
  virtual ~RGWCloudAccess() { }
  
  void set_ceph_context(CephContext* _cct) { cct = _cct; }

  virtual int init_multipart(const std::string& bucket, const std::string& key) = 0;
  virtual int upload_multipart(const std::string& bucket, const std::string& key, bufferlist& buf, uint64_t size) = 0;
  virtual int finish_multipart(const std::string& bucket, const std::string& key) = 0;  
  virtual int abort_multipart(const std::string& bucket, const std::string& key) = 0;
  
  virtual int put_obj(const std::string& bucket, const std::string& key, bufferlist *data, off_t len) = 0;
  virtual int remove_obj(const std::string& bucket, const std::string& key) = 0;

  uint64_t get_block_size(){ return block_size; }  
};

#endif
