//
// Created by cache-nez on 08.01.18.
//

#ifndef CEPH_RGW_ADMIN_BUCKET_H
#define CEPH_RGW_ADMIN_BUCKET_H

#include <iostream>

#include "common/ceph_json.h"
#include "common/errno.h"

#include "rgw_admin_common.h"

template <class T, class K>
int read_decode_json(const string& infile, T& t, K *k)
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
    t.decode_json(&p, k);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  return 0;
}

bool bucket_object_check_filter(const string& name);

int do_check_object_locator(RGWRados *store, const string& tenant_name, const string& bucket_name,
                            bool fix, bool remove_bad, Formatter *f);

int set_bucket_sync_enabled(RGWRados *store, int opt_cmd, const string& tenant_name, const string& bucket_name);

int init_bucket_for_sync(RGWRados *store, const string& tenant, const string& bucket_name,
                         const string& bucket_id, rgw_bucket& bucket);


#endif //CEPH_RGW_ADMIN_BUCKET_H
