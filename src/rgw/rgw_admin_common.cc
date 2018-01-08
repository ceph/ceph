//
// Created by cache-nez on 08.01.18.
//

#include "rgw_admin_common.h"

#include <common/safe_io.h>

#include "auth/Crypto.h"
#include "compressor/Compressor.h"

int read_input(const string& infile, bufferlist& bl)
{
  const int READ_CHUNK = 8196;
  int fd = 0;
  if (!infile.empty()) {
    fd = open(infile.c_str(), O_RDONLY);
    if (fd < 0) {
      int err = -errno;
      cerr << "error reading input file " << infile << std::endl;
      return err;
    }
  }

  int r;
  int err;

  do {
    char buf[READ_CHUNK];

    r = safe_read(fd, buf, READ_CHUNK);
    if (r < 0) {
      err = -errno;
      cerr << "error while reading input" << std::endl;
      goto out;
    }
    bl.append(buf, r);
  } while (r > 0);
  err = 0;

  out:
  if (!infile.empty()) {
    close(fd);
  }
  return err;
}

int init_bucket(RGWRados *store, const string& tenant_name, const string& bucket_name, const string& bucket_id,
                RGWBucketInfo& bucket_info, rgw_bucket& bucket, map<string, bufferlist> *pattrs)
{
  if (!bucket_name.empty()) {
    RGWObjectCtx obj_ctx(store);
    int r;
    if (bucket_id.empty()) {
      r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, nullptr, pattrs);
    } else {
      string bucket_instance_id = bucket_name + ":" + bucket_id;
      r = store->get_bucket_instance_info(obj_ctx, bucket_instance_id, bucket_info, nullptr, pattrs);
    }
    if (r < 0) {
      cerr << "could not get bucket info for bucket=" << bucket_name << std::endl;
      return r;
    }
    bucket = bucket_info.bucket;
  }
  return 0;
}
