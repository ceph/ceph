#ifndef CEPH_RGW_ADMIN_BUCKET_H
#define CEPH_RGW_ADMIN_BUCKET_H

#include <iostream>

#include "common/ceph_json.h"
#include "common/errno.h"

#include "rgw_admin_argument_parsing.h"
#include "rgw_admin_common.h"
#include "rgw_bucket.h"
#include "rgw_reshard.h"

#include <cerrno>
#include <sstream>

#include <boost/optional.hpp>

#include "auth/Crypto.h"
#include "compressor/Compressor.h"

#include "common/config.h"
#include "common/ceph_argparse.h"

template <class T, class K>
static int read_decode_json(const string& infile, T& t, K *k)
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

int set_bucket_sync_enabled(RGWRados *store, int opt_cmd, const string& tenant_name, const string& bucket_name);

int handle_opt_bucket_limit_check(const rgw_user& user_id, bool warnings_only, RGWBucketAdminOpState& bucket_op,
                                  RGWFormatterFlusher& flusher, RGWRados *store);

int handle_opt_buckets_list(const string& bucket_name, const string& tenant, const string& bucket_id,
                            const string& marker, int max_entries, rgw_bucket& bucket,
                            RGWBucketAdminOpState& bucket_op, RGWFormatterFlusher& flusher,
                            RGWRados *store, Formatter *formatter);

int handle_opt_bucket_stats(RGWBucketAdminOpState& bucket_op, RGWFormatterFlusher& flusher,
                            RGWRados *store);

int handle_opt_bucket_link(const string& bucket_id, RGWBucketAdminOpState& bucket_op, RGWRados *store);

int handle_opt_bucket_unlink(RGWBucketAdminOpState& bucket_op, RGWRados *store);

int handle_opt_bucket_rewrite(const string& bucket_name, const string& tenant, const string& bucket_id,
                              const string& start_date, const string& end_date,
                              int min_rewrite_size, int max_rewrite_size, uint64_t min_rewrite_stripe_size,
                              rgw_bucket& bucket, RGWRados *store, Formatter *formatter);

int handle_opt_bucket_reshard(const string& bucket_name, const string& tenant, const string& bucket_id,
                              bool num_shards_specified, int num_shards, bool yes_i_really_mean_it, int max_entries,
                              bool verbose, RGWRados *store, Formatter *formatter);

int handle_opt_bucket_check(bool check_head_obj_locator, const string& bucket_name, const string& tenant, bool fix,
                            bool remove_bad, RGWBucketAdminOpState& bucket_op, RGWFormatterFlusher& flusher,
                            RGWRados *store, Formatter *formatter);

int handle_opt_bucket_rm(bool inconsistent_index, bool bypass_gc, bool yes_i_really_mean_it,
                         RGWBucketAdminOpState& bucket_op, RGWRados *store);

int handle_opt_bucket_sync_init(const string& source_zone, const string& bucket_name, const string& bucket_id,
                                const string& tenant, RGWBucketAdminOpState& bucket_op, RGWRados *store);

int handle_opt_bucket_sync_status(const string& source_zone, const string& bucket_name, const string& bucket_id,
                                  const string& tenant, RGWBucketAdminOpState& bucket_op, RGWRados *store, Formatter *formatter);

int handle_opt_bucket_sync_run(const string& source_zone, const string& bucket_name, const string& bucket_id,
                               const string& tenant, RGWBucketAdminOpState& bucket_op, RGWRados *store);


#endif //CEPH_RGW_ADMIN_BUCKET_H
