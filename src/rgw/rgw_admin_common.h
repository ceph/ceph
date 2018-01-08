#ifndef CEPH_RGW_ADMIN_COMMON_H
#define CEPH_RGW_ADMIN_COMMON_H

#include "cls/rgw/cls_rgw_types.h"

#include "common/ceph_json.h"
#include <common/errno.h>
#include <common/safe_io.h>

#include "rgw_common.h"
#include "rgw_rados.h"

int init_bucket(RGWRados *store, const string& tenant_name, const string& bucket_name, const string& bucket_id,
                RGWBucketInfo& bucket_info, rgw_bucket& bucket, map<string, bufferlist> *pattrs = nullptr);

int read_input(const string& infile, bufferlist& bl);

template <class T>
static int read_decode_json(const string& infile, T& t)
{
  bufferlist bl;
  int ret = read_input(infile, bl);
  if (ret < 0) {
    cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    cout << "failed to parse JSON" << std::endl;
    return -EINVAL;
  }

  try {
    decode_json_obj(t, &p);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  return 0;
}

int parse_date_str(const string& date_str, utime_t& ut);

int check_min_obj_stripe_size(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, uint64_t min_stripe_size, bool *need_rewrite);

int read_current_period_id(RGWRados* store, const std::string& realm_id,
                           const std::string& realm_name,
                           std::string* period_id);

int check_reshard_bucket_params(RGWRados *store,
                                const string& bucket_name,
                                const string& tenant,
                                const string& bucket_id,
                                bool num_shards_specified,
                                int num_shards,
                                int yes_i_really_mean_it,
                                rgw_bucket& bucket,
                                RGWBucketInfo& bucket_info,
                                map<string, bufferlist>& attrs);

#endif //CEPH_RGW_ADMIN_COMMON_H
