#ifndef CEPH_RGW_CLOUD_UFILE_H
#define CEPH_RGW_CLOUD_UFILE_H

#include <string>

#include "include/buffer.h"
#include "rgw_cloud_access.h"

class RGWCloudUfile : public RGWCloudAccess {
private:
  static const int UFILE_BUCKET_NOT_EXIST = -30010;

private:
  static void create_ufile_canonical_header(const std::string& method, const std::string& bucket, 
                                            const std::string& key, const std::string& content_type, 
                                            std::string& dest_str);
  static int get_ufile_header_digest(const std::string& auth_hdr, const std::string& key, std::string& dest);
  static int get_ufile_retcode(std::string& http_response, int* retcode);
  
public:
  RGWCloudUfile(RGWCloudInfo& _cloud_info) :RGWCloudAccess(_cloud_info)
  { }

  int init_multipart(const std::string& bucket, const std::string& key) override;
  int upload_multipart(const std::string& bucket, const std::string& key, bufferlist& buf, uint64_t size) override;
  int finish_multipart(const std::string& bucket, const std::string& key) override;
  int abort_multipart(const std::string& bucket, const std::string& key) override;

  int put_obj(const std::string& bucket, const std::string& key, bufferlist *data, off_t len) override;
  int remove_obj(const std::string& bucket, const std::string& key) override;
};

#endif
