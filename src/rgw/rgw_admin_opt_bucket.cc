#include "rgw_admin_opt_bucket.h"

#include <boost/program_options.hpp>

#include "common/ceph_argparse.h"
#include "common/ceph_json.h"
#include "common/errno.h"

#include "rgw_reshard.h"
#include "rgw_orphan.h"
#include "rgw_data_sync.h"
#include "rgw_admin_argument_parsing.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

static bool bucket_object_check_filter(const std::string& name)
{
  rgw_obj_key k;
  std::string ns; /* empty namespace */
  return rgw_obj_key::oid_to_key_in_ns(name, &k, ns);
}

static int check_obj_locator_underscore(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, rgw_obj_key& key,
                                        bool fix, bool remove_bad, Formatter *f) {
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "head");
  f->dump_string("name", key.name);
  f->dump_string("instance", key.instance);
  f->close_section();

  std::string oid;
  std::string locator;

  get_obj_bucket_and_oid_loc(obj, oid, locator);

  f->dump_string("oid", oid);
  f->dump_string("locator", locator);


  RGWObjectCtx obj_ctx(store);

  RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  int ret = read_op.prepare();
  bool needs_fixing = (ret == -ENOENT);

  f->dump_bool("needs_fixing", needs_fixing);

  std::string status = (needs_fixing ? "needs_fixing" : "ok");

  if ((needs_fixing || remove_bad) && fix) {
    ret = store->fix_head_obj_locator(bucket_info, needs_fixing, remove_bad, key);
    if (ret < 0) {
      cerr << "ERROR: fix_head_object_locator() returned ret=" << ret << std::endl;
      goto done;
    }
    status = "fixed";
  }

  done:
  f->dump_string("status", status);

  f->close_section();

  return 0;
}

static int check_obj_tail_locator_underscore(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, rgw_obj_key& key, bool fix, Formatter *f) {
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "tail");
  f->dump_string("name", key.name);
  f->dump_string("instance", key.instance);
  f->close_section();

  bool needs_fixing;
  std::string status;

  int ret = store->fix_tail_obj_locator(bucket_info, key, fix, &needs_fixing);
  if (ret < 0) {
    cerr << "ERROR: fix_tail_object_locator_underscore() returned ret=" << ret << std::endl;
    status = "failed";
  } else {
    status = (needs_fixing && !fix ? "needs_fixing" : "ok");
  }

  f->dump_bool("needs_fixing", needs_fixing);
  f->dump_string("status", status);

  f->close_section();

  return 0;
}

static int do_check_object_locator(RGWRados *store, const std::string& tenant_name, const std::string& bucket_name,
                                   bool fix, bool remove_bad, Formatter *f)
{
  if (remove_bad && !fix) {
    cerr << "ERROR: can't have remove_bad specified without fix" << std::endl;
    return -EINVAL;
  }

  RGWBucketInfo bucket_info;
  rgw_bucket bucket;
  std::string bucket_id;

  f->open_object_section("bucket");
  f->dump_string("bucket", bucket_name);
  int ret = init_bucket(store, tenant_name, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  bool truncated;
  int count = 0;

  int max_entries = 1000;

  std::string prefix;
  std::string delim;
  vector<rgw_bucket_dir_entry> result;
  map<std::string, bool> common_prefixes;
  std::string ns;

  RGWRados::Bucket target(store, bucket_info);
  RGWRados::Bucket::List list_op(&target);

  std::string marker;

  list_op.params.prefix = prefix;
  list_op.params.delim = delim;
  list_op.params.marker = rgw_obj_key(marker);
  list_op.params.ns = ns;
  list_op.params.enforce_ns = true;
  list_op.params.list_versions = true;

  f->open_array_section("check_objects");
  do {
    ret = list_op.list_objects(max_entries - count, &result, &common_prefixes, &truncated);
    if (ret < 0) {
      cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    count += result.size();

    for (auto &iter : result) {
      rgw_obj_key key = iter.key;
      rgw_obj obj(bucket, key);

      if (key.name[0] == '_') {
        ret = check_obj_locator_underscore(store, bucket_info, obj, key, fix, remove_bad, f);

        if (ret >= 0) {
          ret = check_obj_tail_locator_underscore(store, bucket_info, obj, key, fix, f);
          if (ret < 0) {
            cerr << "ERROR: check_obj_tail_locator_underscore(): " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
        }
      }
    }
    f->flush(cout);
  } while (truncated && count < max_entries);
  f->close_section();
  f->close_section();

  f->flush(cout);

  return 0;
}

int set_bucket_sync_enabled(RGWRados *store, int opt_cmd, const std::string& tenant_name, const std::string& bucket_name)
{
  RGWBucketInfo bucket_info;
  map<std::string, bufferlist> attrs;
  RGWObjectCtx obj_ctx(store);

  int r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, nullptr, &attrs);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  if (opt_cmd == OPT_BUCKET_SYNC_ENABLE) {
    bucket_info.flags &= ~BUCKET_DATASYNC_DISABLED;
  } else if (opt_cmd == OPT_BUCKET_SYNC_DISABLE) {
    bucket_info.flags |= BUCKET_DATASYNC_DISABLED;
  }

  r = store->put_bucket_instance_info(bucket_info, false, real_time(), &attrs);
  if (r < 0) {
    cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  int shards_num = bucket_info.num_shards? bucket_info.num_shards : 1;
  int shard_id = bucket_info.num_shards? 0 : -1;

  if (opt_cmd == OPT_BUCKET_SYNC_DISABLE) {
    r = store->stop_bi_log_entries(bucket_info, -1);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing stop bilog" << dendl;
      return r;
    }
  } else {
    r = store->resync_bi_log_entries(bucket_info, -1);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing resync bilog" << dendl;
      return r;
    }
  }

  for (int i = 0; i < shards_num; ++i, ++shard_id) {
    r = store->data_log->add_entry(bucket_info.bucket, shard_id);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
      return r;
    }
  }

  return 0;
}

static int init_bucket_for_sync(RGWRados *store, const std::string& tenant, const std::string& bucket_name,
                                const std::string& bucket_id, rgw_bucket& bucket)
{
  RGWBucketInfo bucket_info;

  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  return 0;
}

int handle_opt_bucket_limit_check(const rgw_user& user_id, bool warnings_only, RGWBucketAdminOpState& bucket_op,
                                  RGWFormatterFlusher& flusher, RGWRados *store) {
  void *handle;
  std::list<std::string> user_ids;
  std::string metadata_key = "user";
  int max = 1000;

  bool truncated;
  int ret;

  if (! user_id.empty()) {
    user_ids.push_back(user_id.id);
    ret =
        RGWBucketAdminOp::limit_check(store, bucket_op, user_ids, flusher,
                                      warnings_only);
  } else {
    /* list users in groups of max-keys, then perform user-bucket
     * limit-check on each group */
    ret = store->meta_mgr->list_keys_init(metadata_key, &handle);
    if (ret < 0) {
      cerr << "ERROR: buckets limit check can't get user metadata_key: "
           << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    do {
      ret = store->meta_mgr->list_keys_next(handle, max, user_ids,
                                            &truncated);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: buckets limit check lists_keys_next(): "
             << cpp_strerror(-ret) << std::endl;
        break;
      } else {
        /* ok, do the limit checks for this group */
        ret =
            RGWBucketAdminOp::limit_check(store, bucket_op, user_ids, flusher,
                                          warnings_only);
        if (ret < 0)
          break;
      }
      user_ids.clear();
    } while (truncated);
    store->meta_mgr->list_keys_complete(handle);
  }
  return -ret;
}

int handle_opt_buckets_list(const std::string& bucket_name, const std::string& tenant, const std::string& bucket_id,
                            const std::string& marker, int max_entries, rgw_bucket& bucket,
                            RGWBucketAdminOpState& bucket_op, RGWFormatterFlusher& flusher,
                            RGWRados *store, Formatter *formatter) {
  if (bucket_name.empty()) {
    RGWBucketAdminOp::info(store, bucket_op, flusher);
  } else {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_array_section("entries");
    bool truncated;
    int count = 0;
    if (max_entries < 0)
      max_entries = 1000;

    std::string prefix;
    std::string delim;
    vector<rgw_bucket_dir_entry> result;
    map<std::string, bool> common_prefixes;
    std::string ns;

    RGWRados::Bucket target(store, bucket_info);
    RGWRados::Bucket::List list_op(&target);

    list_op.params.prefix = prefix;
    list_op.params.delim = delim;
    list_op.params.marker = rgw_obj_key(marker);
    list_op.params.ns = ns;
    list_op.params.enforce_ns = false;
    list_op.params.list_versions = true;

    do {
      ret = list_op.list_objects(max_entries - count, &result, &common_prefixes, &truncated);
      if (ret < 0) {
        cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      count += result.size();

      for (auto &entry : result) {
        encode_json("entry", entry, formatter);
      }
      formatter->flush(cout);
    } while (truncated && count < max_entries);

    formatter->close_section();
    formatter->flush(cout);
  } /* have bucket_name */
  return 0;
}

int handle_opt_bucket_stats(RGWBucketAdminOpState& bucket_op, RGWFormatterFlusher& flusher,
                            RGWRados *store) {
  bucket_op.set_fetch_stats(true);

  int r = RGWBucketAdminOp::info(store, bucket_op, flusher);
  if (r < 0) {
    cerr << "failure: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int handle_opt_bucket_link(const std::string& bucket_id, RGWBucketAdminOpState& bucket_op, RGWRados *store) {
  std::string err;
  bucket_op.set_bucket_id(bucket_id);
  int r = RGWBucketAdminOp::link(store, bucket_op, &err);
  if (r < 0) {
    cerr << "failure: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

int handle_opt_bucket_unlink(RGWBucketAdminOpState& bucket_op, RGWRados *store) {
  int r = RGWBucketAdminOp::unlink(store, bucket_op);
  if (r < 0) {
    cerr << "failure: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int handle_opt_bucket_rewrite(const std::string& bucket_name, const std::string& tenant, const std::string& bucket_id,
                              const std::string& start_date, const std::string& end_date,
                              int min_rewrite_size, int max_rewrite_size, uint64_t min_rewrite_stripe_size,
                              rgw_bucket& bucket, RGWRados *store, Formatter *formatter){
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }

  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  uint64_t start_epoch = 0;
  uint64_t end_epoch = 0;

  if (!end_date.empty()) {
    ret = utime_t::parse_date(end_date, &end_epoch, nullptr);
    if (ret < 0) {
      cerr << "ERROR: failed to parse end date" << std::endl;
      return EINVAL;
    }
  }
  if (!start_date.empty()) {
    ret = utime_t::parse_date(start_date, &start_epoch, nullptr);
    if (ret < 0) {
      cerr << "ERROR: failed to parse start date" << std::endl;
      return EINVAL;
    }
  }

  bool is_truncated = true;

  rgw_obj_index_key marker;
  std::string prefix;

  formatter->open_object_section("result");
  formatter->dump_string("bucket", bucket_name);
  formatter->open_array_section("objects");
  while (is_truncated) {
    map<std::string, rgw_bucket_dir_entry> result;
    int r = store->cls_bucket_list(bucket_info, RGW_NO_SHARD, marker, prefix, 1000, true,
                                   result, &is_truncated, &marker,
                                   bucket_object_check_filter);

    if (r < 0 && r != -ENOENT) {
      cerr << "ERROR: failed operation r=" << r << std::endl;
    }

    if (r == -ENOENT)
      break;

    map<std::string, rgw_bucket_dir_entry>::iterator iter;
    for (iter = result.begin(); iter != result.end(); ++iter) {
      rgw_obj_key key = iter->second.key;
      rgw_bucket_dir_entry& entry = iter->second;

      formatter->open_object_section("object");
      formatter->dump_string("name", key.name);
      formatter->dump_string("instance", key.instance);
      formatter->dump_int("size", entry.meta.size);
      utime_t ut(entry.meta.mtime);
      ut.gmtime(formatter->dump_stream("mtime"));

      if ((entry.meta.size < min_rewrite_size) ||
          (entry.meta.size > max_rewrite_size) ||
          (start_epoch > 0 && start_epoch > (uint64_t)ut.sec()) ||
          (end_epoch > 0 && end_epoch < (uint64_t)ut.sec())) {
        formatter->dump_string("status", "Skipped");
      } else {
        rgw_obj obj(bucket, key);

        bool need_rewrite = true;
        if (min_rewrite_stripe_size > 0) {
          r = check_min_obj_stripe_size(store, bucket_info, obj, min_rewrite_stripe_size, &need_rewrite);
          if (r < 0) {
            ldout(store->ctx(), 0) << "WARNING: check_min_obj_stripe_size failed, r=" << r << dendl;
          }
        }
        if (!need_rewrite) {
          formatter->dump_string("status", "Skipped");
        } else {
          r = store->rewrite_obj(bucket_info, obj);
          if (r == 0) {
            formatter->dump_string("status", "Success");
          } else {
            formatter->dump_string("status", cpp_strerror(-r));
          }
        }
      }
      formatter->dump_int("flags", entry.flags);

      formatter->close_section();
      formatter->flush(cout);
    }
  }
  formatter->close_section();
  formatter->close_section();
  formatter->flush(cout);

  return 0;
}

int handle_opt_bucket_reshard(const std::string& bucket_name, const std::string& tenant, const std::string& bucket_id,
                              bool num_shards_specified, int num_shards, bool yes_i_really_mean_it, int max_entries,
                              bool verbose, RGWRados *store, Formatter *formatter) {
  const int DEFAULT_RESHARD_MAX_ENTRIES = 1000;
  rgw_bucket bucket;
  RGWBucketInfo bucket_info;
  map<std::string, bufferlist> attrs;

  int ret = check_reshard_bucket_params(store,
                                        bucket_name,
                                        tenant,
                                        bucket_id,
                                        num_shards_specified,
                                        num_shards,
                                        yes_i_really_mean_it,
                                        bucket,
                                        bucket_info,
                                        attrs);
  if (ret < 0) {
    return ret;
  }

  RGWBucketReshard br(store, bucket_info, attrs);

  if (max_entries < 1) {
    max_entries = DEFAULT_RESHARD_MAX_ENTRIES;
  }

  return br.execute(num_shards, max_entries,
                    verbose, &cout, formatter);
}

int handle_opt_bucket_check(bool check_head_obj_locator, const std::string& bucket_name, const std::string& tenant, bool fix,
                            bool remove_bad, RGWBucketAdminOpState& bucket_op, RGWFormatterFlusher& flusher,
                            RGWRados *store, Formatter *formatter) {
  if (check_head_obj_locator) {
    if (bucket_name.empty()) {
      cerr << "ERROR: need to specify bucket name" << std::endl;
      return EINVAL;
    }
    do_check_object_locator(store, tenant, bucket_name, fix, remove_bad, formatter);
  } else {
    RGWBucketAdminOp::check_index(store, bucket_op, flusher);
  }
  return 0;
}

int handle_opt_bucket_rm(bool inconsistent_index, bool bypass_gc, bool yes_i_really_mean_it,
                         RGWBucketAdminOpState& bucket_op, RGWRados *store) {
  if (!inconsistent_index) {
    RGWBucketAdminOp::remove_bucket(store, bucket_op, bypass_gc, true);
  } else {
    if (!yes_i_really_mean_it) {
      cerr << "using --inconsistent_index can corrupt the bucket index " << std::endl
           << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return 1;
    }
    RGWBucketAdminOp::remove_bucket(store, bucket_op, bypass_gc, false);
  }
  return 0;
}

int bucket_sync_toggle(RgwAdminCommand opt_cmd, const std::string& bucket_name, const std::string& tenant,
                       const std::string& realm_id, const std::string& realm_name, const std::string& object,
                       rgw_bucket& bucket, CephContext *context, RGWRados *store)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }

  RGWPeriod period;
  int ret = period.init(context, store, realm_id, realm_name, true);
  if (ret < 0) {
    cerr << "failed to init period " << ": " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  if (!store->is_meta_master()) {
    cerr << "failed to update bucket sync: only allowed on meta master zone "  << std::endl;
    cerr << period.get_master_zone() << " | " << period.get_realm() << std::endl;
    return EINVAL;
  }

  rgw_obj obj(bucket, object);
  ret = set_bucket_sync_enabled(store, opt_cmd, tenant, bucket_name);
  if (ret < 0)
    return -ret;
  return 0;
}

int handle_opt_bucket_sync_init(const std::string& source_zone, const std::string& bucket_name, const std::string& bucket_id,
                                const std::string& tenant, RGWBucketAdminOpState& bucket_op, RGWRados *store) {
  if (source_zone.empty()) {
    cerr << "ERROR: source zone not specified" << std::endl;
    return EINVAL;
  }
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }
  rgw_bucket bucket;
  int ret = init_bucket_for_sync(store, tenant, bucket_name, bucket_id, bucket);
  if (ret < 0) {
    return -ret;
  }
  RGWBucketSyncStatusManager sync(store, source_zone, bucket);

  ret = sync.init();
  if (ret < 0) {
    cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
    return -ret;
  }
  ret = sync.init_sync_status();
  if (ret < 0) {
    cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_bucket_sync_status(const std::string& source_zone, const std::string& bucket_name, const std::string& bucket_id,
                                  const std::string& tenant, RGWBucketAdminOpState& bucket_op, RGWRados *store, Formatter *formatter) {
  if (source_zone.empty()) {
    cerr << "ERROR: source zone not specified" << std::endl;
    return EINVAL;
  }
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }
  rgw_bucket bucket;
  int ret = init_bucket_for_sync(store, tenant, bucket_name, bucket_id, bucket);
  if (ret < 0) {
    return -ret;
  }
  RGWBucketSyncStatusManager sync(store, source_zone, bucket);

  ret = sync.init();
  if (ret < 0) {
    cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
    return -ret;
  }
  ret = sync.read_sync_status();
  if (ret < 0) {
    cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
    return -ret;
  }

  map<int, rgw_bucket_shard_sync_info>& sync_status = sync.get_sync_status();

  encode_json("sync_status", sync_status, formatter);
  formatter->flush(cout);

  return 0;
}

int handle_opt_bucket_sync_run(const std::string& source_zone, const std::string& bucket_name, const std::string& bucket_id,
                               const std::string& tenant, RGWBucketAdminOpState& bucket_op, RGWRados *store) {
  if (source_zone.empty()) {
    cerr << "ERROR: source zone not specified" << std::endl;
    return EINVAL;
  }
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }
  rgw_bucket bucket;
  int ret = init_bucket_for_sync(store, tenant, bucket_name, bucket_id, bucket);
  if (ret < 0) {
    return -ret;
  }
  RGWBucketSyncStatusManager sync(store, source_zone, bucket);

  ret = sync.init();
  if (ret < 0) {
    cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
    return -ret;
  }

  ret = sync.run();
  if (ret < 0) {
    cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_policy(const std::string& format, RGWBucketAdminOpState& bucket_op, RGWFormatterFlusher& flusher,
                      RGWRados *store)
{
  if (format == "xml") {
    int ret = RGWBucketAdminOp::dump_s3_policy(store, bucket_op, cout);
    if (ret < 0) {
      cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  } else {
    int ret = RGWBucketAdminOp::get_policy(store, bucket_op, flusher);
    if (ret < 0) {
      cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }
  return 0;
}

int handle_opt_bi_get(const std::string& object, const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant,
                      BIIndexType bi_index_type, const std::string& object_version, rgw_bucket& bucket, RGWRados *store, Formatter *formatter)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket name not specified" << std::endl;
    return EINVAL;
  }
  if (object.empty()) {
    cerr << "ERROR: object not specified" << std::endl;
    return EINVAL;
  }
  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  rgw_obj obj(bucket, object);
  if (!object_version.empty()) {
    obj.key.set_instance(object_version);
  }

  rgw_cls_bi_entry entry;

  ret = store->bi_get(bucket, obj, bi_index_type, &entry);
  if (ret < 0) {
    cerr << "ERROR: bi_get(): " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  encode_json("entry", entry, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_bi_put(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant, const std::string& infile,
                      const std::string& object_version, rgw_bucket& bucket, RGWRados *store)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket name not specified" << std::endl;
    return EINVAL;
  }
  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  rgw_cls_bi_entry entry;
  cls_rgw_obj_key key;
  ret = read_decode_json(infile, entry, &key);
  if (ret < 0) {
    return 1;
  }

  rgw_obj obj(bucket, key);

  ret = store->bi_put(bucket, obj, entry);
  if (ret < 0) {
    cerr << "ERROR: bi_put(): " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_bi_list(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant, int max_entries,
                       const std::string& object, std::string& marker, rgw_bucket& bucket, RGWRados *store, Formatter *formatter)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket name not specified" << std::endl;
    return EINVAL;
  }
  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  list<rgw_cls_bi_entry> entries;
  bool is_truncated;
  if (max_entries < 0) {
    max_entries = 1000;
  }

  int max_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

  formatter->open_array_section("entries");

  for (int i = 0; i < max_shards; i++) {
    RGWRados::BucketShard bs(store);
    int shard_id = (bucket_info.num_shards > 0  ? i : -1);
    int ret = bs.init(bucket, shard_id);
    marker.clear();

    if (ret < 0) {
      cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << shard_id << "): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    do {
      entries.clear();
      ret = store->bi_list(bs, object, marker, max_entries, &entries, &is_truncated);
      if (ret < 0) {
        cerr << "ERROR: bi_list(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      list<rgw_cls_bi_entry>::iterator iter;
      for (iter = entries.begin(); iter != entries.end(); ++iter) {
        rgw_cls_bi_entry& entry = *iter;
        encode_json("entry", entry, formatter);
        marker = entry.idx;
      }
      formatter->flush(cout);
    } while (is_truncated);
    formatter->flush(cout);
  }
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_bi_purge(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant, bool yes_i_really_mean_it,
                        rgw_bucket& bucket, RGWRados *store)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket name not specified" << std::endl;
    return EINVAL;
  }
  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  RGWBucketInfo cur_bucket_info;
  rgw_bucket cur_bucket;
  ret = init_bucket(store, tenant, bucket_name, std::string(), cur_bucket_info, cur_bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init current bucket info for bucket_name=" << bucket_name << ": " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  if (cur_bucket_info.bucket.bucket_id == bucket_info.bucket.bucket_id && !yes_i_really_mean_it) {
    cerr << "specified bucket instance points to a current bucket instance" << std::endl;
    cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
    return EINVAL;
  }

  int max_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

  for (int i = 0; i < max_shards; i++) {
    RGWRados::BucketShard bs(store);
    int shard_id = (bucket_info.num_shards > 0  ? i : -1);
    int ret = bs.init(bucket, shard_id);
    if (ret < 0) {
      cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << shard_id << "): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    ret = store->bi_remove(bs);
    if (ret < 0) {
      cerr << "ERROR: failed to remove bucket index object: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }
  return 0;
}

int handle_opt_bilog_list(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant, int max_entries,
                          int shard_id, std::string& marker, rgw_bucket& bucket, RGWRados *store, Formatter *formatter)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }
  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  formatter->open_array_section("entries");
  bool truncated;
  int count = 0;
  if (max_entries < 0)
    max_entries = 1000;

  do {
    list<rgw_bi_log_entry> entries;
    ret = store->list_bi_log_entries(bucket_info, shard_id, marker, max_entries - count, entries, &truncated);
    if (ret < 0) {
      cerr << "ERROR: list_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    count += entries.size();

    for (auto &entry : entries) {
      encode_json("entry", entry, formatter);

      marker = entry.id;
    }
    formatter->flush(cout);
  } while (truncated && count < max_entries);

  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_bilog_trim(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant, int shard_id,
                          std::string& start_marker, std::string& end_marker, rgw_bucket& bucket, RGWRados *store)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }
  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = store->trim_bi_log_entries(bucket_info, shard_id, start_marker, end_marker);
  if (ret < 0) {
    cerr << "ERROR: trim_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_bilog_status(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant, int shard_id,
                            rgw_bucket& bucket, RGWRados *store, Formatter *formatter)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }
  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  map<int, std::string> markers;
  ret = store->get_bi_log_status(bucket_info, shard_id, markers);
  if (ret < 0) {
    cerr << "ERROR: get_bi_log_status(): " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  formatter->open_object_section("entries");
  encode_json("markers", markers, formatter);
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_bilog_autotrim(RGWRados *store)
{
  RGWCoroutinesManager crs(store->ctx(), store->get_cr_registry());
  RGWHTTPManager http(store->ctx(), crs.get_completion_mgr());
  int ret = http.set_threaded();
  if (ret < 0) {
    cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
    return -ret;
  }

  rgw::BucketTrimConfig config;
  configure_bucket_trim(store->ctx(), config);

  rgw::BucketTrimManager trim(store, config);
  ret = trim.init();
  if (ret < 0) {
    cerr << "trim manager init failed with " << cpp_strerror(ret) << std::endl;
    return -ret;
  }
  ret = crs.run(trim.create_admin_bucket_trim_cr(&http));
  if (ret < 0) {
    cerr << "automated bilog trim failed with " << cpp_strerror(ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_reshard_add(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant,
                           bool num_shards_specified, int num_shards, bool yes_i_really_mean_it,
                           RGWRados *store)
{
  rgw_bucket bucket;
  RGWBucketInfo bucket_info;
  map<std::string, bufferlist> attrs;

  int ret = check_reshard_bucket_params(store,
                                        bucket_name,
                                        tenant,
                                        bucket_id,
                                        num_shards_specified,
                                        num_shards,
                                        yes_i_really_mean_it,
                                        bucket,
                                        bucket_info,
                                        attrs);
  if (ret < 0) {
    return ret;
  }

  int num_source_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

  RGWReshard reshard(store);
  cls_rgw_reshard_entry entry;
  entry.time = real_clock::now();
  entry.tenant = tenant;
  entry.bucket_name = bucket_name;
  entry.bucket_id = bucket_info.bucket.bucket_id;
  entry.old_num_shards = num_source_shards;
  entry.new_num_shards = num_shards;

  return reshard.add(entry);
}

int handle_opt_reshard_list(int max_entries, RGWRados *store, Formatter *formatter)
{
  list<cls_rgw_reshard_entry> entries;
  int ret;
  int count = 0;
  if (max_entries < 0) {
    max_entries = 1000;
  }

  int num_logshards = store->ctx()->_conf->rgw_reshard_num_logs;

  RGWReshard reshard(store);

  formatter->open_array_section("reshard");
  for (int i = 0; i < num_logshards; i++) {
    bool is_truncated = true;
    std::string marker;
    do {
      entries.clear();
      ret = reshard.list(i, marker, max_entries, entries, &is_truncated);
      if (ret < 0) {
        cerr << "Error listing resharding buckets: " << cpp_strerror(-ret) << std::endl;
        return ret;
      }
      for (auto &entry : entries) {
        encode_json("entry", entry, formatter);
        entry.get_key(&marker);
      }
      count += entries.size();
      formatter->flush(cout);
    } while (is_truncated && count < max_entries);

    if (count >= max_entries) {
      break;
    }
  }

  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_reshard_status(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant,
                              RGWRados *store, Formatter *formatter)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }

  rgw_bucket bucket;
  RGWBucketInfo bucket_info;
  map<std::string, bufferlist> attrs;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket, &attrs);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  RGWBucketReshard br(store, bucket_info, attrs);
  list<cls_rgw_bucket_instance_entry> status;
  int r = br.get_status(&status);
  if (r < 0) {
    cerr << "ERROR: could not get resharding status for bucket " << bucket_name << std::endl;
    return -r;
  }

  encode_json("status", status, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_reshard_process(RGWRados *store)
{
  RGWReshard reshard(store, true, &cout);

  int ret = reshard.process_all_logshards();
  if (ret < 0) {
    cerr << "ERROR: failed to process reshard logs, error=" << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_reshard_cancel(const std::string& bucket_name, RGWRados *store)
{
  RGWReshard reshard(store);

  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }
  cls_rgw_reshard_entry entry;
  //entry.tenant = tenant;
  entry.bucket_name = bucket_name;
  //entry.bucket_id = bucket_id;
  int ret = reshard.get(entry);
  if (ret < 0) {
    cerr << "Error in getting bucket " << bucket_name << ": " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  /* TBD stop running resharding */

  ret = reshard.remove(entry);
  if (ret < 0) {
    cerr << "Error removing bucket " << bucket_name << " for resharding queue: " << cpp_strerror(-ret) <<
         std::endl;
    return ret;
  }
  return 0;
}

template <class T>
static bool decode_dump(const char *field_name, bufferlist& bl, Formatter *f)
{
  T t;

  bufferlist::iterator iter = bl.begin();

  try {
    decode(t, iter);
  } catch (buffer::error& err) {
    return false;
  }

  encode_json(field_name, t, f);

  return true;
}

static bool dump_string(const char *field_name, bufferlist& bl, Formatter *f)
{
  std::string val;
  if (bl.length() > 0) {
    val.assign(bl.c_str());
  }
  f->dump_string(field_name, val);

  return true;
}

int handle_opt_object_rm(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant, const std::string& object,
                         const std::string& object_version, rgw_bucket& bucket, RGWRados *store)
{
  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  rgw_obj_key key(object, object_version);
  ret = rgw_remove_object(store, bucket_info, bucket, key);

  if (ret < 0) {
    cerr << "ERROR: object remove returned: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_object_rewrite(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant,
                              const std::string& object, const std::string& object_version, uint64_t min_rewrite_stripe_size,
                              rgw_bucket& bucket, RGWRados *store)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return EINVAL;
  }
  if (object.empty()) {
    cerr << "ERROR: object not specified" << std::endl;
    return EINVAL;
  }

  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  rgw_obj obj(bucket, object);
  obj.key.set_instance(object_version);
  bool need_rewrite = true;
  if (min_rewrite_stripe_size > 0) {
    ret = check_min_obj_stripe_size(store, bucket_info, obj, min_rewrite_stripe_size, &need_rewrite);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "WARNING: check_min_obj_stripe_size failed, r=" << ret << dendl;
    }
  }
  if (need_rewrite) {
    ret = store->rewrite_obj(bucket_info, obj);
    if (ret < 0) {
      cerr << "ERROR: object rewrite returned: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  } else {
    ldout(store->ctx(), 20) << "skipped object" << dendl;
  }
  return 0;
}

int handle_opt_object_expire(RGWRados *store)
{
  int ret = store->process_expire_objects();
  if (ret < 0) {
    cerr << "ERROR: process_expire_objects() processing returned error: " << cpp_strerror(-ret) << std::endl;
    return 1;
  }
  return 0;
}

int handle_opt_object_unlink(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant,
                             const std::string& object, const std::string& object_version, rgw_bucket& bucket, RGWRados *store)
{
  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  list<rgw_obj_index_key> oid_list;
  rgw_obj_key key(object, object_version);
  rgw_obj_index_key index_key;
  key.get_index_key(&index_key);
  oid_list.push_back(index_key);
  ret = store->remove_objs_from_index(bucket_info, oid_list);
  if (ret < 0) {
    cerr << "ERROR: remove_obj_from_index() returned error: " << cpp_strerror(-ret) << std::endl;
    return 1;
  }
  return 0;
}

int handle_opt_object_stat(const std::string& bucket_id, const std::string& bucket_name, const std::string& tenant,
                           const std::string& object, const std::string& object_version, rgw_bucket& bucket,
                           RGWRados *store, Formatter *formatter)
{
  RGWBucketInfo bucket_info;
  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  rgw_obj obj(bucket, object);
  obj.key.set_instance(object_version);

  uint64_t obj_size;
  map<std::string, bufferlist> attrs;
  RGWObjectCtx obj_ctx(store);
  RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrs;
  read_op.params.obj_size = &obj_size;

  ret = read_op.prepare();
  if (ret < 0) {
    cerr << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << std::endl;
    return 1;
  }
  formatter->open_object_section("object_metadata");
  formatter->dump_string("name", object);
  formatter->dump_unsigned("size", obj_size);

  map<std::string, bufferlist>::iterator iter;
  map<std::string, bufferlist> other_attrs;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    bufferlist& bl = iter->second;
    bool handled = false;
    if (iter->first == RGW_ATTR_MANIFEST) {
      handled = decode_dump<RGWObjManifest>("manifest", bl, formatter);
    } else if (iter->first == RGW_ATTR_ACL) {
      handled = decode_dump<RGWAccessControlPolicy>("policy", bl, formatter);
    } else if (iter->first == RGW_ATTR_ID_TAG) {
      handled = dump_string("tag", bl, formatter);
    } else if (iter->first == RGW_ATTR_ETAG) {
      handled = dump_string("etag", bl, formatter);
    } else if (iter->first == RGW_ATTR_COMPRESSION) {
      handled = decode_dump<RGWCompressionInfo>("compression", bl, formatter);
    }

    if (!handled)
      other_attrs[iter->first] = bl;
  }

  formatter->open_object_section("attrs");
  for (iter = other_attrs.begin(); iter != other_attrs.end(); ++iter) {
    dump_string(iter->first.c_str(), iter->second, formatter);
  }
  formatter->close_section();
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int RgwAdminBiCommandsHandler::parse_command_and_parameters(std::vector<const char*>& args) {
  const char BUCKET_ID[] = "bucket-id";
  const char BUCKET_NAME[] = "bucket";
  const char INDEX_TYPE[] = "index-type";
  const char INFILE[] = "infile";
  const char MARKER[] = "marker";
  const char MAX_ENTRIES[] = "max-entries";
  const char OBJECT[] = "object";
  const char OBJECT_VERSION[] = "object-version";
  const char TENANT[] = "tenant";
  const char MEAN_IT[] = "yes-i-really-mean-it";
  std::string bi_index_type_str;
  boost::program_options::options_description desc{"Bi options"};
  desc.add_options()
      (BUCKET_ID, boost::program_options::value(&bucket_id), "Bucket id")
      (BUCKET_NAME, boost::program_options::value(&bucket_name), "Specify the bucket name")
      (INFILE, boost::program_options::value(&infile), "A file to read in when setting data")
      (INDEX_TYPE, boost::program_options::value(&bi_index_type_str), "")
      (MAX_ENTRIES, boost::program_options::value(&max_entries), "The maximum number of entries to display")
      (MARKER, boost::program_options::value(&marker), "")
      (OBJECT, boost::program_options::value(&object), "Object name")
      (OBJECT_VERSION, boost::program_options::value(&object_version), "")
      (TENANT, boost::program_options::value(&tenant), "Tenant name")
      (MEAN_IT, "Confirmation of purging certain information");
  boost::program_options::variables_map var_map;

  int ret = parse_command(args, desc, var_map);
  if (ret > 0) {
    return ret;
  }
  if (var_map.count(INDEX_TYPE)) {
    bi_index_type = get_bi_index_type(bi_index_type_str);
    if (bi_index_type == InvalidIdx) {
      return EINVAL;
    }
  }
  if (var_map.count(MEAN_IT)) {
    yes_i_really_mean_it = true;
  }
  return 0;
}
