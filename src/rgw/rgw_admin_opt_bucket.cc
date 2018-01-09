#include "rgw_admin_opt_bucket.h"
#include "rgw_bucket.h"

#include "cls/rgw/cls_rgw_client.h"


bool bucket_object_check_filter(const string& name)
{
  rgw_obj_key k;
  string ns; /* empty namespace */
  return rgw_obj_key::oid_to_key_in_ns(name, &k, ns);
}

static int check_obj_locator_underscore(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, rgw_obj_key& key, bool fix, bool remove_bad, Formatter *f) {
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "head");
  f->dump_string("name", key.name);
  f->dump_string("instance", key.instance);
  f->close_section();

  string oid;
  string locator;

  get_obj_bucket_and_oid_loc(obj, oid, locator);

  f->dump_string("oid", oid);
  f->dump_string("locator", locator);


  RGWObjectCtx obj_ctx(store);

  RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  int ret = read_op.prepare();
  bool needs_fixing = (ret == -ENOENT);

  f->dump_bool("needs_fixing", needs_fixing);

  string status = (needs_fixing ? "needs_fixing" : "ok");

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
  string status;

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

int do_check_object_locator(RGWRados *store, const string& tenant_name, const string& bucket_name,
                            bool fix, bool remove_bad, Formatter *f)
{
  if (remove_bad && !fix) {
    cerr << "ERROR: can't have remove_bad specified without fix" << std::endl;
    return -EINVAL;
  }

  RGWBucketInfo bucket_info;
  rgw_bucket bucket;
  string bucket_id;

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

  string prefix;
  string delim;
  vector<rgw_bucket_dir_entry> result;
  map<string, bool> common_prefixes;
  string ns;

  RGWRados::Bucket target(store, bucket_info);
  RGWRados::Bucket::List list_op(&target);

  string marker;

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

    for (auto iter = result.begin(); iter != result.end(); ++iter) {
      rgw_obj_key key = iter->key;
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

int set_bucket_sync_enabled(RGWRados *store, int opt_cmd, const string& tenant_name, const string& bucket_name)
{
  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
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

int init_bucket_for_sync(RGWRados *store, const string& tenant, const string& bucket_name,
                         const string& bucket_id, rgw_bucket& bucket)
{
  RGWBucketInfo bucket_info;

  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  return 0;
}
