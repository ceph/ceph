#ifndef CEPH_RGW_CUSTOM_ACCESS_H
#define CEPH_RGW_CUSTOM_ACCESS_H

#include <string>

#include "include/buffer.h"

using namespace std;

struct RGWCustomInfo {
  string domain_name;
  string bucket_host;
  string bucket_region;
  string dest_bucket;
  string bucket_prefix;
  string public_key;
  string private_key;
  string access_type;
};

class RGWCustomAccess {
protected:
  CephContext *cct;
  RGWCustomInfo& custom_info;
public:
  RGWCustomAccess(RGWCustomInfo& _custom_info) :custom_info(_custom_info)
  { }
  virtual ~RGWCustomAccess() { }
  
  void set_ceph_context(CephContext* _cct) { cct = _cct; }
  
  virtual int put_obj(string& bucket, string& key, bufferlist *data, off_t len) = 0;
  virtual int remove_obj(string& bucket, string& key) = 0;
  
};

class RGWUfileAccess : public RGWCustomAccess {
private:
  static const int UFILE_BUCKET_NOT_EXIST = -30010;
private:
  static void create_ufile_canonical_header(const string& method, const string& bucket, const string& key, const string& content_type, string& dest_str);
  static int get_ufile_header_digest(const string& auth_hdr, const string& key, string& dest);
  static int get_ufile_retcode(string& http_response, int* retcode);
  
public:
  RGWUfileAccess(RGWCustomInfo& _custom_info) :RGWCustomAccess(_custom_info) 
  { }
  int put_obj(string& bucket, string& key, bufferlist *data, off_t len) ;
  int remove_obj(string& bucket, string& key);
};

#endif
