#ifndef CEPH_RGW_ADMIN_BUCKET_H
#define CEPH_RGW_ADMIN_BUCKET_H

#include "rgw_basic_types.h"
#include "rgw_bucket.h"
#include "rgw_formats.h"
#include "rgw_rados.h"
#include "rgw_admin_common.h"

// This header and the corresponding source file contain handling of the following commads / groups of commands:
// Bucket, bucket sync, bi, bilog, reshard, object

template <class T, class K>
int read_decode_json(const std::string& infile, T& t, K *k)
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

int set_bucket_sync_enabled(RGWRados* store, int opt_cmd, const std::string& tenant_name,
                            const std::string& bucket_name);

int bucket_sync_toggle(RgwAdminCommand opt_cmd, const std::string& bucket_name,
                       const std::string& tenant, const std::string& realm_id,
                       const std::string& realm_name, const std::string& object,
                       rgw_bucket& bucket, CephContext* context, RGWRados* store);

int handle_opt_bi_get(const std::string& object, const std::string& bucket_id,
                      const std::string& bucket_name, const std::string& tenant,
                      BIIndexType bi_index_type, const std::string& object_version,
                      rgw_bucket& bucket, RGWRados* store, Formatter* formatter);

int handle_opt_bi_list(const std::string& bucket_id, const std::string& bucket_name,
                       const std::string& tenant, int max_entries, const std::string& object,
                       std::string& marker, rgw_bucket& bucket, RGWRados* store,
                       Formatter* formatter);

int handle_opt_bi_purge(const std::string& bucket_id, const std::string& bucket_name,
                        const std::string& tenant, bool yes_i_really_mean_it, rgw_bucket& bucket,
                        RGWRados* store);

int handle_opt_bi_put(const std::string& bucket_id, const std::string& bucket_name,
                      const std::string& tenant, const std::string& infile,
                      const std::string& object_version, rgw_bucket& bucket, RGWRados* store);

int handle_opt_bilog_autotrim(RGWRados* store);

int handle_opt_bilog_list(const std::string& bucket_id, const std::string& bucket_name,
                          const std::string& tenant, int max_entries, int shard_id,
                          std::string& marker, rgw_bucket& bucket, RGWRados* store,
                          Formatter* formatter);

int handle_opt_bilog_status(const std::string& bucket_id, const std::string& bucket_name,
                            const std::string& tenant, int shard_id, rgw_bucket& bucket,
                            RGWRados* store, Formatter* formatter);

int handle_opt_bilog_trim(const std::string& bucket_id, const std::string& bucket_name,
                          const std::string& tenant, int shard_id, std::string& start_marker,
                          std::string& end_marker, rgw_bucket& bucket, RGWRados* store);

int handle_opt_bucket_check(bool check_head_obj_locator, const std::string& bucket_name,
                            const std::string& tenant, bool fix, bool remove_bad,
                            RGWBucketAdminOpState& bucket_op, RGWFormatterFlusher& flusher,
                            RGWRados* store, Formatter* formatter);

int handle_opt_bucket_limit_check(const rgw_user& user_id, bool warnings_only,
                                  RGWBucketAdminOpState& bucket_op, RGWFormatterFlusher& flusher,
                                  RGWRados* store);

int handle_opt_bucket_link(const std::string& bucket_id, RGWBucketAdminOpState& bucket_op,
                           RGWRados* store);

int handle_opt_bucket_reshard(const std::string& bucket_name, const std::string& tenant,
                              const std::string& bucket_id, bool num_shards_specified,
                              int num_shards, bool yes_i_really_mean_it, int max_entries,
                              bool verbose, RGWRados* store, Formatter* formatter);

int handle_opt_bucket_rewrite(const std::string& bucket_name, const std::string& tenant,
                              const std::string& bucket_id, const std::string& start_date,
                              const std::string& end_date, int min_rewrite_size,
                              int max_rewrite_size, uint64_t min_rewrite_stripe_size,
                              rgw_bucket& bucket, RGWRados* store, Formatter* formatter);

int handle_opt_bucket_rm(bool inconsistent_index, bool bypass_gc, bool yes_i_really_mean_it,
                         RGWBucketAdminOpState& bucket_op, RGWRados* store);

int handle_opt_bucket_stats(RGWBucketAdminOpState& bucket_op, RGWFormatterFlusher& flusher,
                            RGWRados* store);

int handle_opt_bucket_sync_init(const std::string& source_zone, const std::string& bucket_name,
                                const std::string& bucket_id, const std::string& tenant,
                                RGWBucketAdminOpState& bucket_op, RGWRados* store);

int handle_opt_bucket_sync_run(const std::string& source_zone, const std::string& bucket_name,
                               const std::string& bucket_id, const std::string& tenant,
                               RGWBucketAdminOpState& bucket_op, RGWRados* store);

int handle_opt_bucket_sync_status(const std::string& source_zone, const std::string& bucket_name,
                                  const std::string& bucket_id, const std::string& tenant,
                                  RGWBucketAdminOpState& bucket_op, RGWRados* store,
                                  Formatter* formatter);

int handle_opt_bucket_unlink(RGWBucketAdminOpState& bucket_op, RGWRados* store);

int handle_opt_buckets_list(const std::string& bucket_name, const std::string& tenant,
                            const std::string& bucket_id, const std::string& marker,
                            int max_entries, rgw_bucket& bucket, RGWBucketAdminOpState& bucket_op,
                            RGWFormatterFlusher& flusher, RGWRados* store, Formatter* formatter);

int handle_opt_object_expire(RGWRados* store);

int handle_opt_object_rewrite(const std::string& bucket_id, const std::string& bucket_name,
                              const std::string& tenant, const std::string& object,
                              const std::string& object_version, uint64_t min_rewrite_stripe_size,
                              rgw_bucket& bucket, RGWRados* store);

int handle_opt_object_rm(const std::string& bucket_id, const std::string& bucket_name,
                         const std::string& tenant, const std::string& object,
                         const std::string& object_version, rgw_bucket& bucket, RGWRados* store);

int handle_opt_object_stat(const std::string& bucket_id, const std::string& bucket_name,
                           const std::string& tenant, const std::string& object,
                           const std::string& object_version, rgw_bucket& bucket, RGWRados* store,
                           Formatter* formatter);

int handle_opt_object_unlink(const std::string& bucket_id, const std::string& bucket_name,
                             const std::string& tenant, const std::string& object,
                             const std::string& object_version, rgw_bucket& bucket,
                             RGWRados* store);

int handle_opt_policy(const std::string& format, RGWBucketAdminOpState& bucket_op,
                      RGWFormatterFlusher& flusher, RGWRados* store);

int handle_opt_reshard_add(const std::string& bucket_id, const std::string& bucket_name,
                           const std::string& tenant, bool num_shards_specified, int num_shards,
                           bool yes_i_really_mean_it, RGWRados* store);

int handle_opt_reshard_cancel(const std::string& bucket_name, RGWRados* store);

int handle_opt_reshard_list(int max_entries, RGWRados* store, Formatter* formatter);

int handle_opt_reshard_process(RGWRados* store);

int handle_opt_reshard_status(const std::string& bucket_id, const std::string& bucket_name,
                              const std::string& tenant, RGWRados* store, Formatter* formatter);

class RgwAdminBiCommandsHandler : public RgwAdminCommandGroupHandler {
public:
  explicit RgwAdminBiCommandsHandler(std::vector<const char*>& args, RGWRados *store,
                                     Formatter *formatter)
      : RgwAdminCommandGroupHandler(args, store, formatter)
  {
    if (parse_command_and_parameters(args) > 0) {
      ceph_abort();
    }
    std::cout << "Parsed command: " << m_command << std::endl;
  }

  ~RgwAdminBiCommandsHandler() override = default;

  int execute_command() override {
    switch (m_command) {
      case OPT_BI_GET : return handle_opt_bi_get();
      case OPT_BI_LIST : return handle_opt_bi_list();
      case OPT_BI_PURGE : return handle_opt_bi_purge();
      case OPT_BI_PUT : return handle_opt_bi_put();
      default: return EINVAL;
    }
  }

  RgwAdminCommandGroup get_type() const override { return BI; }

private:
  int parse_command_and_parameters(std::vector<const char*>& args) override;

  int handle_opt_bi_get() {
    return ::handle_opt_bi_get(object, bucket_id, bucket_name, tenant, bi_index_type,
                               object_version, bucket, m_store, m_formatter);
  }

  int handle_opt_bi_list() {
    return ::handle_opt_bi_list(bucket_id, bucket_name, tenant, max_entries, object, marker,
                                bucket, m_store, m_formatter);
  }

  int handle_opt_bi_purge() {
    return ::handle_opt_bi_purge(bucket_id, bucket_name, tenant, yes_i_really_mean_it, bucket,
                                 m_store);
  }

  int handle_opt_bi_put() {
    return ::handle_opt_bi_put(bucket_id, bucket_name, tenant, infile, object_version, bucket,
                               m_store);
  }

  const std::vector<std::string> COMMAND_PREFIX = {"bi"};
  const std::unordered_map<std::string, RgwAdminCommand> STRING_TO_COMMAND = {
      {"get", OPT_BI_GET},
      {"list", OPT_BI_LIST},
      {"purge", OPT_BI_PURGE},
      {"put", OPT_BI_PUT},
  };

  BIIndexType bi_index_type = PlainIdx;
  rgw_bucket bucket;
  std::string bucket_id;
  std::string bucket_name;
  std::string infile;
  std::string marker;
  int max_entries = -1;
  std::string object;
  std::string object_version;
  std::string tenant;
  bool yes_i_really_mean_it = false;
};

#endif //CEPH_RGW_ADMIN_BUCKET_H
