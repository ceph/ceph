#ifndef CEPH_RGW_CUSTOM_ACCESS_H
#define CEPH_RGW_CUSTOM_ACCESS_H

#include <string>

#include "include/buffer.h"

using namespace std;

struct RGWCustomInfo {
  std::string domain_name;
  std::string bucket_host;
  std::string bucket_region;
  std::string dest_bucket;
  std::string bucket_prefix;
  std::string public_key;
  std::string private_key;
  std::string access_type;
};

class RGWCustomAccess {
protected:
  CephContext *cct;
  RGWCustomInfo& custom_info;
public:
  RGWCustomAccess(RGWCustomInfo& _custom_info) :cct(nullptr), custom_info(_custom_info)
  { }
  virtual ~RGWCustomAccess() { }
  
  void set_ceph_context(CephContext* _cct) { cct = _cct; }
  
  virtual int put_obj(std::string& bucket, string& key, bufferlist *data, off_t len) = 0;
  virtual int remove_obj(std::string& bucket, string& key) = 0;
  
};

class RGWUfileAccess : public RGWCustomAccess {
private:
  static const int UFILE_BUCKET_NOT_EXIST = -30010;
private:
  static void create_ufile_canonical_header(const std::string& method, const string& bucket, const string& key, const string& content_type, string& dest_str);
  static int get_ufile_header_digest(const std::string& auth_hdr, const string& key, string& dest);
  static int get_ufile_retcode(std::string& http_response, int* retcode);
  
public:
  RGWUfileAccess(RGWCustomInfo& _custom_info) :RGWCustomAccess(_custom_info) 
  { }
  int put_obj(std::string& bucket, string& key, bufferlist *data, off_t len) ;
  int remove_obj(std::string& bucket, string& key);
};

#endif
