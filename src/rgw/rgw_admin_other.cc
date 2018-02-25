#include "rgw_admin_other.h"

#include "rgw_data_sync.h"
#include "rgw_admin_argument_parsing.h"

#define dout_context g_ceph_context

int handle_opt_pool_add(const std::string& pool_name, rgw_pool& pool, RGWRados *store)
{
  if (pool_name.empty()) {
    cerr << "need to specify pool to add!" << std::endl;
    usage();
    ceph_abort();
  }

  int ret = store->add_bucket_placement(pool);
  if (ret < 0)
    cerr << "failed to add bucket placement: " << cpp_strerror(-ret) << std::endl;
  return 0;
}

int handle_opt_pool_rm(const std::string& pool_name, rgw_pool& pool, RGWRados *store)
{
  if (pool_name.empty()) {
    cerr << "need to specify pool to remove!" << std::endl;
    usage();
    ceph_abort();
  }

  int ret = store->remove_bucket_placement(pool);
  if (ret < 0)
    cerr << "failed to remove bucket placement: " << cpp_strerror(-ret) << std::endl;
  return 0;
}

int handle_opt_pools_list(RGWRados *store, Formatter *formatter)
{
  set<rgw_pool> pools;
  int ret = store->list_placement_set(pools);
  if (ret < 0) {
    cerr << "could not list placement set: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  formatter->reset();
  formatter->open_array_section("pools");
  for (const auto &pool : pools) {
    formatter->open_object_section("pool");
    formatter->dump_string("name", pool.to_str());
    formatter->close_section();
  }
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
  return 0;
}

int handle_opt_log_list(const std::string& date, RGWRados *store, Formatter *formatter)
{
  // filter by date?
  if (!date.empty() && date.size() != 10) {
    cerr << "bad date format for '" << date << "', expect YYYY-MM-DD" << std::endl;
    return EINVAL;
  }

  formatter->reset();
  formatter->open_array_section("logs");
  RGWAccessHandle access_handle;
  int ret = store->log_list_init(date, &access_handle);
  if (ret == -ENOENT) {
    // no logs.
  } else {
    if (ret < 0) {
      cerr << "log list: error " << ret << std::endl;
      return -ret;
    }
    while (true) {
      std::string name;
      ret = store->log_list_next(access_handle, &name);
      if (ret == -ENOENT)
        break;
      if (ret < 0) {
        cerr << "log list: error " << ret << std::endl;
        return -ret;
      }
      formatter->dump_string("object", name);
    }
  }
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
  return 0;
}

int handle_opt_log_show(const std::string& object, const std::string& date,
                        const std::string& bucket_id, const std::string& bucket_name, bool show_log_entries,
                        bool skip_zero_entries,  bool show_log_sum, RGWRados *store, Formatter *formatter) {
  if (object.empty() && (date.empty() || bucket_name.empty() || bucket_id.empty())) {
    cerr << "specify an object or a date, bucket and bucket-id" << std::endl;
    usage();
    ceph_abort();
  }

  std::string oid;
  if (!object.empty()) {
    oid = object;
  } else {
    oid = date;
    oid += "-";
    oid += bucket_id;
    oid += "-";
    oid += bucket_name;
  }

  RGWAccessHandle h;

  int r = store->log_show_init(oid, &h);
  if (r < 0) {
    cerr << "error opening log " << oid << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  formatter->reset();
  formatter->open_object_section("log");

  struct rgw_log_entry entry;

  // peek at first entry to get bucket metadata
  r = store->log_show_next(h, &entry);
  if (r < 0) {
    cerr << "error reading log " << oid << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  formatter->dump_string("bucket_id", entry.bucket_id);
  formatter->dump_string("bucket_owner", entry.bucket_owner.to_str());
  formatter->dump_string("bucket", entry.bucket);

  uint64_t agg_time = 0;
  uint64_t agg_bytes_sent = 0;
  uint64_t agg_bytes_received = 0;
  uint64_t total_entries = 0;

  if (show_log_entries)
    formatter->open_array_section("log_entries");

  do {
    uint64_t total_time =  entry.total_time.to_msec();

    agg_time += total_time;
    agg_bytes_sent += entry.bytes_sent;
    agg_bytes_received += entry.bytes_received;
    total_entries++;

    if (skip_zero_entries && entry.bytes_sent == 0 &&
        entry.bytes_received == 0) {
      goto next;
    }

    if (show_log_entries) {
      rgw_format_ops_log_entry(entry, formatter);
      formatter->flush(cout);
    }
    next:
    r = store->log_show_next(h, &entry);
  } while (r > 0);

  if (r < 0) {
    cerr << "error reading log " << oid << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  if (show_log_entries)
    formatter->close_section();

  if (show_log_sum) {
    formatter->open_object_section("log_sum");
    formatter->dump_int("bytes_sent", agg_bytes_sent);
    formatter->dump_int("bytes_received", agg_bytes_received);
    formatter->dump_int("total_time", agg_time);
    formatter->dump_int("total_entries", total_entries);
    formatter->close_section();
  }
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
  return 0;
}

int handle_opt_log_rm(const std::string& object, const std::string& date,
                      const std::string& bucket_id, const std::string& bucket_name, RGWRados *store) {
  if (object.empty() && (date.empty() || bucket_name.empty() || bucket_id.empty())) {
    cerr << "specify an object or a date, bucket and bucket-id" << std::endl;
    usage();
    ceph_abort();
  }

  std::string oid;
  if (!object.empty()) {
    oid = object;
  } else {
    oid = date;
    oid += "-";
    oid += bucket_id;
    oid += "-";
    oid += bucket_name;
  }

  int r = store->log_remove(oid);
  if (r < 0) {
    cerr << "error removing log " << oid << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int handle_opt_usage_show(rgw_user& user_id, const std::string& start_date, const std::string& end_date,
                          bool show_log_entries, bool show_log_sum, RGWFormatterFlusher& flusher,
                          std::map<std::string, bool> *categories, RGWRados *store)
{
  uint64_t start_epoch = 0;
  auto end_epoch = (uint64_t)-1;

  int ret;

  if (!start_date.empty()) {
    ret = utime_t::parse_date(start_date, &start_epoch, nullptr);
    if (ret < 0) {
      cerr << "ERROR: failed to parse start date" << std::endl;
      return 1;
    }
  }
  if (!end_date.empty()) {
    ret = utime_t::parse_date(end_date, &end_epoch, nullptr);
    if (ret < 0) {
      cerr << "ERROR: failed to parse end date" << std::endl;
      return 1;
    }
  }


  ret = RGWUsage::show(store, user_id, start_epoch, end_epoch,
                       show_log_entries, show_log_sum, categories,
                       flusher);
  if (ret < 0) {
    cerr << "ERROR: failed to show usage" << std::endl;
    return 1;
  }
  return 0;
}

int handle_opt_usage_trim(rgw_user& user_id, const std::string& start_date, const std::string& end_date,
                          bool yes_i_really_mean_it, RGWRados *store)
{

  if (user_id.empty() && !yes_i_really_mean_it) {
    cerr << "usage trim without user specified will remove *all* users data" << std::endl;
    cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
    return 1;
  }
  int ret;
  uint64_t start_epoch = 0;
  auto end_epoch = (uint64_t)-1;


  if (!start_date.empty()) {
    ret = utime_t::parse_date(start_date, &start_epoch, nullptr);
    if (ret < 0) {
      cerr << "ERROR: failed to parse start date" << std::endl;
      return 1;
    }
  }

  if (!end_date.empty()) {
    ret = utime_t::parse_date(end_date, &end_epoch, nullptr);
    if (ret < 0) {
      cerr << "ERROR: failed to parse end date" << std::endl;
      return 1;
    }
  }

  ret = RGWUsage::trim(store, user_id, start_epoch, end_epoch);
  if (ret < 0) {
    cerr << "ERROR: read_usage() returned ret=" << ret << std::endl;
    return 1;
  }
  return 0;
}

int handle_opt_usage_clear(bool yes_i_really_mean_it, RGWRados *store)
{
  if (!yes_i_really_mean_it) {
    cerr << "usage clear would remove *all* users usage data for all time" << std::endl;
    cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
    return 1;
  }

  return -RGWUsage::clear(store);
}


int handle_opt_olh_get(const std::string& tenant, const std::string& bucket_id, const std::string& bucket_name,
                       const std::string& object, rgw_bucket& bucket, RGWRados *store, Formatter *formatter)
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
  RGWOLHInfo olh;
  rgw_obj obj(bucket, object);
  ret = store->get_olh(bucket_info, obj, &olh);
  if (ret < 0) {
    cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  encode_json("olh", olh, formatter);
  formatter->flush(cout);
  return 0;
}

int handle_opt_olh_readlog(const std::string& tenant, const std::string& bucket_id, const std::string& bucket_name,
                           const std::string& object, rgw_bucket& bucket, RGWRados *store, Formatter *formatter)
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
  map<uint64_t, vector<rgw_bucket_olh_log_entry> > log;
  bool is_truncated;

  RGWObjectCtx rctx(store);
  rgw_obj obj(bucket, object);

  RGWObjState *state;

  ret = store->get_obj_state(&rctx, bucket_info, obj, &state, false); /* don't follow olh */
  if (ret < 0) {
    return -ret;
  }

  ret = store->bucket_index_read_olh_log(bucket_info, *state, obj, 0, &log, &is_truncated);
  if (ret < 0) {
    cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  formatter->open_object_section("result");
  encode_json("is_truncated", is_truncated, formatter);
  encode_json("log", log, formatter);
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_gc_list(bool include_all, std::string& marker, RGWRados *store, Formatter *formatter)
{
  int index = 0;
  bool truncated;
  formatter->open_array_section("entries");

  do {
    list<cls_rgw_gc_obj_info> result;
    int ret = store->list_gc_objs(&index, marker, 1000, !include_all, result, &truncated);
    if (ret < 0) {
      cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }


    list<cls_rgw_gc_obj_info>::iterator iter;
    for (iter = result.begin(); iter != result.end(); ++iter) {
      cls_rgw_gc_obj_info& info = *iter;
      formatter->open_object_section("chain_info");
      formatter->dump_string("tag", info.tag);
      formatter->dump_stream("time") << info.time;
      formatter->open_array_section("objs");
      list<cls_rgw_obj>::iterator liter;
      cls_rgw_obj_chain& chain = info.chain;
      for (liter = chain.objs.begin(); liter != chain.objs.end(); ++liter) {
        cls_rgw_obj& obj = *liter;
        encode_json("obj", obj, formatter);
      }
      formatter->close_section(); // objs
      formatter->close_section(); // obj_chain
      formatter->flush(cout);
    }
  } while (truncated);
  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_gc_process(bool include_all, RGWRados *store)
{
  int ret = store->process_gc(!include_all);
  if (ret < 0) {
    cerr << "ERROR: gc processing returned error: " << cpp_strerror(-ret) << std::endl;
    return 1;
  }
  return 0;
}

int handle_opt_lc_list(int max_entries, RGWRados *store, Formatter *formatter)
{
  formatter->open_array_section("lifecycle_list");
  map<std::string, int> bucket_lc_map;
  std::string marker;
  const int MAX_LC_LIST_ENTRIES = 100;
  if (max_entries < 0) {
    max_entries = MAX_LC_LIST_ENTRIES;
  }
  do {
    int ret = store->list_lc_progress(marker, max_entries, &bucket_lc_map);
    if (ret < 0) {
      cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
    map<std::string, int>::iterator iter;
    for (iter = bucket_lc_map.begin(); iter != bucket_lc_map.end(); ++iter) {
      formatter->open_object_section("bucket_lc_info");
      formatter->dump_string("bucket", iter->first);
      std::string lc_status = LC_STATUS[iter->second];
      formatter->dump_string("status", lc_status);
      formatter->close_section(); // objs
      formatter->flush(cout);
      marker = iter->first;
    }
  } while (!bucket_lc_map.empty());

  formatter->close_section(); //lifecycle list
  formatter->flush(cout);
  return 0;
}

int handle_opt_lc_process(RGWRados *store)
{
  int ret = store->process_lc();
  if (ret < 0) {
    cerr << "ERROR: lc processing returned error: " << cpp_strerror(-ret) << std::endl;
    return 1;
  }
  return 0;
}

int handle_opt_metadata_get(std::string& metadata_key, RGWRados *store, Formatter *formatter)
{
  int ret = store->meta_mgr->get(metadata_key, formatter);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  formatter->flush(cout);
  return 0;
}

int handle_opt_metadata_put(std::string& metadata_key, std::string& infile, RGWRados *store, Formatter *formatter)
{
  bufferlist bl;
  int ret = read_input(infile, bl);
  if (ret < 0) {
    cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  ret = store->meta_mgr->put(metadata_key, bl, RGWMetadataHandler::APPLY_ALWAYS);
  if (ret < 0) {
    cerr << "ERROR: can't put key: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_metadata_rm(std::string& metadata_key, RGWRados *store, Formatter *formatter)
{
  int ret = store->meta_mgr->remove(metadata_key);
  if (ret < 0) {
    cerr << "ERROR: can't remove key: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_metadata_list(const std::string& metadata_key, const std::string& marker, bool max_entries_specified,
                             int max_entries, RGWRados *store, Formatter *formatter)
{
  void *handle;
  int max = 1000;
  int ret = store->meta_mgr->list_keys_init(metadata_key, marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  bool truncated;
  uint64_t count = 0;

  if (max_entries_specified) {
    formatter->open_object_section("result");
  }
  formatter->open_array_section("keys");

  uint64_t left;
  do {
    list<std::string> keys;
    left = (max_entries_specified ? max_entries - count : max);
    ret = store->meta_mgr->list_keys_next(handle, left, keys, &truncated);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    } if (ret != -ENOENT) {
      for (auto &key : keys) {
        formatter->dump_string("key", key);
        ++count;
      }
      formatter->flush(cout);
    }
  } while (truncated && left > 0);

  formatter->close_section();

  if (max_entries_specified) {
    encode_json("truncated", truncated, formatter);
    encode_json("count", count, formatter);
    if (truncated) {
      encode_json("marker", store->meta_mgr->get_marker(handle), formatter);
    }
    formatter->close_section();
  }
  formatter->flush(cout);

  store->meta_mgr->list_keys_complete(handle);
  return 0;
}

int handle_opt_user_list(const std::string& marker, bool max_entries_specified, int max_entries,
                         RGWRados *store, Formatter *formatter) {
  return handle_opt_metadata_list("user", marker, max_entries_specified, max_entries, store, formatter);
}

int handle_opt_mdlog_list(const std::string& start_date, const std::string& end_date, bool specified_shard_id,
                          int shard_id, const std::string& realm_id, const std::string& realm_name,
                          std::string& marker, std::string& period_id, RGWRados *store, Formatter *formatter)
{
  utime_t start_time, end_time;

  int ret = parse_date_str(start_date, start_time);
  if (ret < 0)
    return -ret;

  ret = parse_date_str(end_date, end_time);
  if (ret < 0)
    return -ret;

  int i = (specified_shard_id ? shard_id : 0);

  if (period_id.empty()) {
    ret= read_current_period_id(store, realm_id, realm_name, &period_id);
    if (ret < 0) {
      return -ret;
    }
    std::cerr << "No --period given, using current period="
              << period_id << std::endl;
  }
  RGWMetadataLog *meta_log = store->meta_mgr->get_log(period_id);

  formatter->open_array_section("entries");
  for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
    void *handle;
    list<cls_log_entry> entries;


    meta_log->init_list_entries(i, start_time.to_real_time(), end_time.to_real_time(), marker, &handle);
    bool truncated;
    do {
      ret = meta_log->list_entries(handle, 1000, entries, nullptr, &truncated);
      if (ret < 0) {
        cerr << "ERROR: meta_log->list_entries(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      for (auto &entry : entries) {
        store->meta_mgr->dump_log_entry(entry, formatter);
      }
      formatter->flush(cout);
    } while (truncated);

    meta_log->complete_list_entries(handle);

    if (specified_shard_id)
      break;
  }

  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_mdlog_status(bool specified_shard_id, int shard_id, const std::string& realm_id,
                            const std::string& realm_name, std::string& marker, std::string& period_id,
                            RGWRados *store, Formatter *formatter)
{
  int i = (specified_shard_id ? shard_id : 0);

  if (period_id.empty()) {
    int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
    if (ret < 0) {
      return -ret;
    }
    std::cerr << "No --period given, using current period="
              << period_id << std::endl;
  }
  RGWMetadataLog *meta_log = store->meta_mgr->get_log(period_id);

  formatter->open_array_section("entries");

  for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
    RGWMetadataLogInfo info;
    meta_log->get_info(i, &info);

    ::encode_json("info", info, formatter);

    if (specified_shard_id)
      break;
  }

  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_mdlog_autotrim(RGWRados *store)
{
  // need a full history for purging old mdlog periods
  store->meta_mgr->init_oldest_log_period();

  RGWCoroutinesManager crs(store->ctx(), store->get_cr_registry());
  RGWHTTPManager http(store->ctx(), crs.get_completion_mgr());
  int ret = http.set_threaded();
  if (ret < 0) {
    cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
    return -ret;
  }

  int64_t num_shards = g_conf->rgw_md_log_max_shards;
  ret = crs.run(create_admin_meta_log_trim_cr(store, &http, num_shards));
  if (ret < 0) {
    cerr << "automated mdlog trim failed with " << cpp_strerror(ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_mdlog_trim(const std::string& start_date, const std::string& end_date, bool specified_shard_id,
                          int shard_id, const std::string& start_marker, const std::string& end_marker,
                          std::string& period_id, RGWRados *store)
{
  utime_t start_time, end_time;

  if (!specified_shard_id) {
    cerr << "ERROR: shard-id must be specified for trim operation" << std::endl;
    return EINVAL;
  }

  int ret = parse_date_str(start_date, start_time);
  if (ret < 0)
    return -ret;

  ret = parse_date_str(end_date, end_time);
  if (ret < 0)
    return -ret;

  if (period_id.empty()) {
    std::cerr << "missing --period argument" << std::endl;
    return EINVAL;
  }
  RGWMetadataLog *meta_log = store->meta_mgr->get_log(period_id);

  ret = meta_log->trim(shard_id, start_time.to_real_time(), end_time.to_real_time(), start_marker, end_marker);
  if (ret < 0) {
    cerr << "ERROR: meta_log->trim(): " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_sync_error_list(int max_entries, const std::string& start_date, const std::string& end_date,
                               bool specified_shard_id, int shard_id, std::string& marker,
                               RGWRados *store, Formatter *formatter)
{
  if (max_entries < 0) {
    max_entries = 1000;
  }

  bool truncated;
  utime_t start_time, end_time;

  int ret = parse_date_str(start_date, start_time);
  if (ret < 0)
    return -ret;

  ret = parse_date_str(end_date, end_time);
  if (ret < 0)
    return -ret;

  if (shard_id < 0) {
    shard_id = 0;
  }

  formatter->open_array_section("entries");

  for (; shard_id < ERROR_LOGGER_SHARDS; ++shard_id) {
    formatter->open_object_section("shard");
    encode_json("shard_id", shard_id, formatter);
    formatter->open_array_section("entries");

    int count = 0;
    std::string oid = RGWSyncErrorLogger::get_shard_oid(RGW_SYNC_ERROR_LOG_SHARD_PREFIX, shard_id);

    do {
      list<cls_log_entry> entries;
      ret = store->time_log_list(oid, start_time.to_real_time(), end_time.to_real_time(),
                                 max_entries - count, entries, marker, &marker, &truncated);
      if (ret == -ENOENT) {
        break;
      }
      if (ret < 0) {
        cerr << "ERROR: store->time_log_list(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      count += entries.size();

      for (auto& cls_entry : entries) {
        rgw_sync_error_info log_entry;

        auto iter = cls_entry.data.begin();
        try {
          ::decode(log_entry, iter);
        } catch (buffer::error& err) {
          cerr << "ERROR: failed to decode log entry" << std::endl;
          continue;
        }
        formatter->open_object_section("entry");
        encode_json("id", cls_entry.id, formatter);
        encode_json("section", cls_entry.section, formatter);
        encode_json("name", cls_entry.name, formatter);
        encode_json("timestamp", cls_entry.timestamp, formatter);
        encode_json("info", log_entry, formatter);
        formatter->close_section();
        formatter->flush(cout);
      }
    } while (truncated && count < max_entries);

    formatter->close_section();
    formatter->close_section();

    if (specified_shard_id) {
      break;
    }
  }

  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_sync_error_trim(const std::string& start_date, const std::string& end_date,
                               bool specified_shard_id, int shard_id, const std::string& start_marker,
                               const std::string& end_marker,
                               RGWRados *store)
{
  utime_t start_time, end_time;
  int ret = parse_date_str(start_date, start_time);
  if (ret < 0)
    return -ret;

  ret = parse_date_str(end_date, end_time);
  if (ret < 0)
    return -ret;

  if (shard_id < 0) {
    shard_id = 0;
  }

  for (; shard_id < ERROR_LOGGER_SHARDS; ++shard_id) {
    std::string oid = RGWSyncErrorLogger::get_shard_oid(RGW_SYNC_ERROR_LOG_SHARD_PREFIX, shard_id);
    ret = store->time_log_trim(oid, start_time.to_real_time(), end_time.to_real_time(), start_marker, end_marker);
    if (ret < 0 && ret != -ENODATA) {
      cerr << "ERROR: sync error trim: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    if (specified_shard_id) {
      break;
    }
  }
  return 0;
}

int handle_opt_datalog_status(bool specified_shard_id, int shard_id, RGWRados *store, Formatter *formatter)
{
  RGWDataChangesLog *log = store->data_log;
  int i = (specified_shard_id ? shard_id : 0);

  formatter->open_array_section("entries");
  for (; i < g_ceph_context->_conf->rgw_data_log_num_shards; i++) {
    list<cls_log_entry> entries;

    RGWDataChangesLogInfo info;
    log->get_info(i, &info);

    encode_json("info", info, formatter);

    if (specified_shard_id)
      break;
  }

  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_datalog_list(int max_entries, const std::string& start_date, const std::string& end_date,
                            bool extra_info, RGWRados *store, Formatter *formatter)
{
  formatter->open_array_section("entries");
  bool truncated;
  int count = 0;
  if (max_entries < 0)
    max_entries = 1000;

  utime_t start_time, end_time;

  int ret = parse_date_str(start_date, start_time);
  if (ret < 0)
    return -ret;

  ret = parse_date_str(end_date, end_time);
  if (ret < 0)
    return -ret;

  RGWDataChangesLog *log = store->data_log;
  RGWDataChangesLog::LogMarker marker;

  do {
    list<rgw_data_change_log_entry> entries;
    ret = log->list_entries(start_time.to_real_time(), end_time.to_real_time(), max_entries - count, entries, marker, &truncated);
    if (ret < 0) {
      cerr << "ERROR: list_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    count += entries.size();

    for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
      rgw_data_change_log_entry& entry = *iter;
      if (!extra_info) {
        encode_json("entry", entry.entry, formatter);
      } else {
        encode_json("entry", entry, formatter);
      }
    }
    formatter->flush(cout);
  } while (truncated && count < max_entries);

  formatter->close_section();
  formatter->flush(cout);
  return 0;
}

int handle_opt_datalog_trim(const std::string& start_date, const std::string& end_date, const std::string& start_marker,
                            const std::string& end_marker, RGWRados *store)
{
  utime_t start_time, end_time;

  int ret = parse_date_str(start_date, start_time);
  if (ret < 0)
    return -ret;

  ret = parse_date_str(end_date, end_time);
  if (ret < 0)
    return -ret;

  RGWDataChangesLog *log = store->data_log;
  ret = log->trim_entries(start_time.to_real_time(), end_time.to_real_time(), start_marker, end_marker);
  if (ret < 0) {
    cerr << "ERROR: trim_entries(): " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_opstate_set(const std::string& client_id, const std::string& op_id, const std::string& object,
                           const std::string& state_str, RGWRados *store)
{
  RGWOpState oc(store);

  RGWOpState::OpState state;
  if (object.empty() || client_id.empty() || op_id.empty()) {
    cerr << "ERROR: need to specify client_id, op_id, and object" << std::endl;
    return EINVAL;
  }
  if (state_str.empty()) {
    cerr << "ERROR: state was not specified" << std::endl;
    return EINVAL;
  }
  int ret = oc.state_from_str(state_str, &state);
  if (ret < 0) {
    cerr << "ERROR: invalid state: " << state_str << std::endl;
    return -ret;
  }

  ret = oc.set_state(client_id, op_id, object, state);
  if (ret < 0) {
    cerr << "ERROR: failed to set state: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_opstate_renew(const std::string& client_id, const std::string& op_id, const std::string& object,
                             const std::string& state_str, RGWRados *store)
{
  RGWOpState oc(store);

  RGWOpState::OpState state;
  if (object.empty() || client_id.empty() || op_id.empty()) {
    cerr << "ERROR: need to specify client_id, op_id, and object" << std::endl;
    return EINVAL;
  }
  if (state_str.empty()) {
    cerr << "ERROR: state was not specified" << std::endl;
    return EINVAL;
  }
  int ret = oc.state_from_str(state_str, &state);
  if (ret < 0) {
    cerr << "ERROR: invalid state: " << state_str << std::endl;
    return -ret;
  }

  ret = oc.renew_state(client_id, op_id, object, state);
  if (ret < 0) {
    cerr << "ERROR: failed to renew state: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_opstate_list(const std::string& client_id, const std::string& op_id, const std::string& object,
                            RGWRados *store, Formatter *formatter)
{
  RGWOpState oc(store);

  int max = 1000;

  void *handle;
  oc.init_list_entries(client_id, op_id, object, &handle);
  list<cls_statelog_entry> entries;
  bool done;
  formatter->open_array_section("entries");
  do {
    int ret = oc.list_entries(handle, max, entries, &done);
    if (ret < 0) {
      cerr << "oc.list_entries returned " << cpp_strerror(-ret) << std::endl;
      oc.finish_list_entries(handle);
      return -ret;
    }

    for (auto &entrie : entries) {
      oc.dump_entry(entrie, formatter);
    }

    formatter->flush(cout);
  } while (!done);
  formatter->close_section();
  formatter->flush(cout);
  oc.finish_list_entries(handle);
  return 0;
}

int handle_opt_opstate_rm(const std::string& client_id, const std::string& op_id, const std::string& object,
                          RGWRados *store)
{
  RGWOpState oc(store);

  if (object.empty() || client_id.empty() || op_id.empty()) {
    cerr << "ERROR: need to specify client_id, op_id, and object" << std::endl;
    return EINVAL;
  }
  int ret = oc.remove_entry(client_id, op_id, object);
  if (ret < 0) {
    cerr << "ERROR: failed to set state: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  return 0;
}

int handle_opt_replicalog_get(const std::string& replica_log_type_str, ReplicaLogType replica_log_type,
                              bool specified_shard_id, int shard_id, const std::string& bucket_id,
                              const std::string& bucket_name, const std::string& tenant, rgw_pool& pool,
                              rgw_bucket& bucket, RGWRados *store, Formatter *formatter)
{
  if (replica_log_type_str.empty()) {
    cerr << "ERROR: need to specify --replica-log-type=<metadata | data | bucket>" << std::endl;
    return EINVAL;
  }
  RGWReplicaBounds bounds;
  if (replica_log_type == ReplicaLog_Metadata) {
    if (!specified_shard_id) {
      cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
      return EINVAL;
    }

    RGWReplicaObjectLogger logger(store, pool, META_REPLICA_LOG_OBJ_PREFIX);
    int ret = logger.get_bounds(shard_id, bounds);
    if (ret < 0)
      return -ret;
  } else if (replica_log_type == ReplicaLog_Data) {
    if (!specified_shard_id) {
      cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
      return EINVAL;
    }
    RGWReplicaObjectLogger logger(store, pool, DATA_REPLICA_LOG_OBJ_PREFIX);
    int ret = logger.get_bounds(shard_id, bounds);
    if (ret < 0)
      return -ret;
  } else if (replica_log_type == ReplicaLog_Bucket) {
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

    RGWReplicaBucketLogger logger(store);
    ret = logger.get_bounds(bucket, shard_id, bounds);
    if (ret < 0)
      return -ret;
  } else { // shouldn't get here
    ceph_abort();
  }
  encode_json("bounds", bounds, formatter);
  formatter->flush(cout);
  cout << std::endl;
  return 0;
}

int handle_opt_replicalog_delete(const std::string& replica_log_type_str, ReplicaLogType replica_log_type,
                                 bool specified_shard_id, int shard_id, bool specified_daemon_id,
                                 const std::string& daemon_id, const std::string& bucket_id,
                                 const std::string& bucket_name, const std::string& tenant, rgw_pool& pool,
                                 rgw_bucket& bucket, RGWRados *store)
{
  if (replica_log_type_str.empty()) {
    cerr << "ERROR: need to specify --replica-log-type=<metadata | data | bucket>" << std::endl;
    return EINVAL;
  }
  if (replica_log_type == ReplicaLog_Metadata) {
    if (!specified_shard_id) {
      cerr << "ERROR: shard-id must be specified for delete operation" << std::endl;
      return EINVAL;
    }
    if (!specified_daemon_id) {
      cerr << "ERROR: daemon-id must be specified for delete operation" << std::endl;
      return EINVAL;
    }
    RGWReplicaObjectLogger logger(store, pool, META_REPLICA_LOG_OBJ_PREFIX);
    int ret = logger.delete_bound(shard_id, daemon_id, false);
    if (ret < 0)
      return -ret;
  } else if (replica_log_type == ReplicaLog_Data) {
    if (!specified_shard_id) {
      cerr << "ERROR: shard-id must be specified for delete operation" << std::endl;
      return EINVAL;
    }
    if (!specified_daemon_id) {
      cerr << "ERROR: daemon-id must be specified for delete operation" << std::endl;
      return EINVAL;
    }
    RGWReplicaObjectLogger logger(store, pool, DATA_REPLICA_LOG_OBJ_PREFIX);
    int ret = logger.delete_bound(shard_id, daemon_id, false);
    if (ret < 0)
      return -ret;
  } else if (replica_log_type == ReplicaLog_Bucket) {
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

    RGWReplicaBucketLogger logger(store);
    ret = logger.delete_bound(bucket, shard_id, daemon_id, false);
    if (ret < 0)
      return -ret;
  }
  return 0;
}

int handle_opt_replicalog_update(const std::string& replica_log_type_str, ReplicaLogType replica_log_type,
                                 const std::string& marker, const std::string& date, const std::string& infile,
                                 bool specified_shard_id, int shard_id, bool specified_daemon_id,
                                 const std::string& daemon_id, const std::string& bucket_id,
                                 const std::string& bucket_name, const std::string& tenant, rgw_pool& pool,
                                 rgw_bucket& bucket, RGWRados *store)
{
  if (replica_log_type_str.empty()) {
    cerr << "ERROR: need to specify --replica-log-type=<metadata | data | bucket>" << std::endl;
    return EINVAL;
  }
  if (marker.empty()) {
    cerr << "ERROR: marker was not specified" <<std::endl;
    return EINVAL;
  }
  utime_t time = ceph_clock_now();
  if (!date.empty()) {
    int ret = parse_date_str(date, time);
    if (ret < 0) {
      cerr << "ERROR: failed to parse start date" << std::endl;
      return EINVAL;
    }
  }
  list<RGWReplicaItemMarker> entries;
  int ret = read_decode_json(infile, entries);
  if (ret < 0) {
    cerr << "ERROR: failed to decode entries" << std::endl;
    return EINVAL;
  }
  RGWReplicaBounds bounds;
  if (replica_log_type == ReplicaLog_Metadata) {
    if (!specified_shard_id) {
      cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
      return EINVAL;
    }

    RGWReplicaObjectLogger logger(store, pool, META_REPLICA_LOG_OBJ_PREFIX);
    ret = logger.update_bound(shard_id, daemon_id, marker, time, &entries);
    if (ret < 0) {
      cerr << "ERROR: failed to update bounds: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  } else if (replica_log_type == ReplicaLog_Data) {
    if (!specified_shard_id) {
      cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
      return EINVAL;
    }
    RGWReplicaObjectLogger logger(store, pool, DATA_REPLICA_LOG_OBJ_PREFIX);
    ret = logger.update_bound(shard_id, daemon_id, marker, time, &entries);
    if (ret < 0) {
      cerr << "ERROR: failed to update bounds: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  } else if (replica_log_type == ReplicaLog_Bucket) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWReplicaBucketLogger logger(store);
    ret = logger.update_bound(bucket, shard_id, daemon_id, marker, time, &entries);
    if (ret < 0) {
      cerr << "ERROR: failed to update bounds: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }
  return 0;
}

static void flush_ss(std::stringstream& ss, list<std::string>& l)
{
  if (!ss.str().empty()) {
    l.push_back(ss.str());
  }
  ss.str("");
}

static std::stringstream& push_ss(std::stringstream& ss, list<std::string>& l, int tab = 0)
{
  flush_ss(ss, l);
  if (tab > 0) {
    ss << setw(tab) << "" << setw(1);
  }
  return ss;
}

static void get_md_sync_status(list<std::string>& status, RGWRados *store)
{
  RGWMetaSyncStatusManager sync(store, store->get_async_rados());

  int ret = sync.init();
  if (ret < 0) {
    status.push_back(std::string("failed to retrieve sync info: sync.init() failed: ") + cpp_strerror(-ret));
    return;
  }

  rgw_meta_sync_status sync_status;
  ret = sync.read_sync_status(&sync_status);
  if (ret < 0) {
    status.push_back(std::string("failed to read sync status: ") + cpp_strerror(-ret));
    return;
  }

  std::string status_str;
  switch (sync_status.sync_info.state) {
    case rgw_meta_sync_info::StateInit:
      status_str = "init";
      break;
    case rgw_meta_sync_info::StateBuildingFullSyncMaps:
      status_str = "preparing for full sync";
      break;
    case rgw_meta_sync_info::StateSync:
      status_str = "syncing";
      break;
    default:
      status_str = "unknown";
  }

  status.push_back(status_str);

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  int num_full = 0;
  int num_inc = 0;
  int total_shards = 0;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    total_shards++;
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
      num_full++;
      full_complete += marker_iter.second.pos;
    } else {
      full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync) {
      num_inc++;
    }
  }

  std::stringstream ss;
  push_ss(ss, status) << "full sync: " << num_full << "/" << total_shards << " shards";

  if (num_full > 0) {
    push_ss(ss, status) << "full sync: " << full_total - full_complete << " entries to sync";
  }

  push_ss(ss, status) << "incremental sync: " << num_inc << "/" << total_shards << " shards";

  rgw_mdlog_info log_info;
  ret = sync.read_log_info(&log_info);
  if (ret < 0) {
    status.push_back(std::string("failed to fetch local sync status: ") + cpp_strerror(-ret));
    return;
  }

  map<int, RGWMetadataLogInfo> master_shards_info;
  std::string master_period = store->get_current_period_id();

  ret = sync.read_master_log_shards_info(master_period, &master_shards_info);
  if (ret < 0) {
    status.push_back(std::string("failed to fetch master sync status: ") + cpp_strerror(-ret));
    return;
  }

  map<int, std::string> shards_behind;
  if (sync_status.sync_info.period != master_period) {
    status.emplace_back("master is on a different period: master_period=" +
                        master_period + " local_period=" + sync_status.sync_info.period);
  } else {
    for (auto local_iter : sync_status.sync_markers) {
      int shard_id = local_iter.first;
      auto iter = master_shards_info.find(shard_id);

      if (iter == master_shards_info.end()) {
        /* huh? */
        derr << "ERROR: could not find remote sync shard status for shard_id=" << shard_id << dendl;
        continue;
      }
      auto master_marker = iter->second.marker;
      if (local_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync &&
          master_marker > local_iter.second.marker) {
        shards_behind[shard_id] = local_iter.second.marker;
      }
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  if (total_behind == 0) {
    push_ss(ss, status) << "metadata is caught up with master";
  } else {
    push_ss(ss, status) << "metadata is behind on " << total_behind << " shards";

    map<int, rgw_mdlog_shard_data> master_pos;
    ret = sync.read_master_log_shards_next(sync_status.sync_info.period, shards_behind, &master_pos);
    if (ret < 0) {
      derr << "ERROR: failed to fetch master next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      ceph::real_time oldest;
      for (auto iter : master_pos) {
        rgw_mdlog_shard_data& shard_data = iter.second;

        if (!shard_data.entries.empty()) {
          rgw_mdlog_entry& entry = shard_data.entries.front();
          if (ceph::real_clock::is_zero(oldest)) {
            oldest = entry.timestamp;
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest) {
            oldest = entry.timestamp;
          }
        }
      }

      if (!ceph::real_clock::is_zero(oldest)) {
        push_ss(ss, status) << "oldest incremental change not applied: " << oldest;
      }
    }
  }

  flush_ss(ss, status);
}

static void get_data_sync_status(const std::string& source_zone, list<std::string>& status, int tab, RGWRados *store)
{
  std::stringstream ss;

  auto ziter = store->zone_by_id.find(source_zone);
  if (ziter == store->zone_by_id.end()) {
    push_ss(ss, status, tab) << std::string("zone not found");
    flush_ss(ss, status);
    return;
  }
  RGWZone& sz = ziter->second;

  if (!store->zone_syncs_from(store->get_zone(), sz)) {
    push_ss(ss, status, tab) << std::string("not syncing from zone");
    flush_ss(ss, status);
    return;
  }
  RGWDataSyncStatusManager sync(store, store->get_async_rados(), source_zone);

  int ret = sync.init();
  if (ret < 0) {
    push_ss(ss, status, tab) << std::string("failed to retrieve sync info: ") + cpp_strerror(-ret);
    flush_ss(ss, status);
    return;
  }

  rgw_data_sync_status sync_status;
  ret = sync.read_sync_status(&sync_status);
  if (ret < 0 && ret != -ENOENT) {
    push_ss(ss, status, tab) << std::string("failed read sync status: ") + cpp_strerror(-ret);
    return;
  }

  std::string status_str;
  switch (sync_status.sync_info.state) {
    case rgw_data_sync_info::StateInit:
      status_str = "init";
      break;
    case rgw_data_sync_info::StateBuildingFullSyncMaps:
      status_str = "preparing for full sync";
      break;
    case rgw_data_sync_info::StateSync:
      status_str = "syncing";
      break;
    default:
      status_str = "unknown";
  }

  push_ss(ss, status, tab) << status_str;

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  int num_full = 0;
  int num_inc = 0;
  int total_shards = 0;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    total_shards++;
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::FullSync) {
      num_full++;
      full_complete += marker_iter.second.pos;
    } else {
      full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync) {
      num_inc++;
    }
  }

  push_ss(ss, status, tab) << "full sync: " << num_full << "/" << total_shards << " shards";

  if (num_full > 0) {
    push_ss(ss, status, tab) << "full sync: " << full_total - full_complete << " buckets to sync";
  }

  push_ss(ss, status, tab) << "incremental sync: " << num_inc << "/" << total_shards << " shards";

  rgw_datalog_info log_info;
  ret = sync.read_log_info(&log_info);
  if (ret < 0) {
    push_ss(ss, status, tab) << std::string("failed to fetch local sync status: ") + cpp_strerror(-ret);
    return;
  }


  map<int, RGWDataChangesLogInfo> source_shards_info;

  ret = sync.read_source_log_shards_info(&source_shards_info);
  if (ret < 0) {
    push_ss(ss, status, tab) << std::string("failed to fetch source sync status: ") + cpp_strerror(-ret);
    return;
  }

  map<int, std::string> shards_behind;

  for (auto local_iter : sync_status.sync_markers) {
    int shard_id = local_iter.first;
    auto iter = source_shards_info.find(shard_id);

    if (iter == source_shards_info.end()) {
      /* huh? */
      derr << "ERROR: could not find remote sync shard status for shard_id=" << shard_id << dendl;
      continue;
    }
    auto master_marker = iter->second.marker;
    if (local_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync &&
        master_marker > local_iter.second.marker) {
      shards_behind[shard_id] = local_iter.second.marker;
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  if (total_behind == 0) {
    push_ss(ss, status, tab) << "data is caught up with source";
  } else {
    push_ss(ss, status, tab) << "data is behind on " << total_behind << " shards";

    map<int, rgw_datalog_shard_data> master_pos;
    ret = sync.read_source_log_shards_next(shards_behind, &master_pos);
    if (ret < 0) {
      derr << "ERROR: failed to fetch next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      ceph::real_time oldest;
      for (auto iter : master_pos) {
        rgw_datalog_shard_data& shard_data = iter.second;

        if (!shard_data.entries.empty()) {
          rgw_datalog_entry& entry = shard_data.entries.front();
          if (ceph::real_clock::is_zero(oldest)) {
            oldest = entry.timestamp;
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest) {
            oldest = entry.timestamp;
          }
        }
      }

      if (!ceph::real_clock::is_zero(oldest)) {
        push_ss(ss, status, tab) << "oldest incremental change not applied: " << oldest;
      }
    }
  }

  flush_ss(ss, status);
}

static void tab_dump(const std::string& header, int width, const list<std::string>& entries)
{
  std::string s = header;

  for (auto e : entries) {
    cout << std::setw(width) << s << std::setw(1) << " " << e << std::endl;
    s.clear();
  }
}


void handle_opt_sync_status(RGWRados *store)
{
  RGWRealm& realm = store->realm;
  RGWZoneGroup& zonegroup = store->get_zonegroup();
  RGWZone& zone = store->get_zone();

  int width = 15;

  cout << std::setw(width) << "realm" << std::setw(1) << " " << realm.get_id() << " (" << realm.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zonegroup" << std::setw(1) << " " << zonegroup.get_id() << " (" << zonegroup.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zone" << std::setw(1) << " " << zone.id << " (" << zone.name << ")" << std::endl;

  list<std::string> md_status;

  if (store->is_meta_master()) {
    md_status.emplace_back("no sync (zone is master)");
  } else {
    get_md_sync_status(md_status, store);
  }

  tab_dump("metadata sync", width, md_status);

  list<std::string> data_status;

  for (auto iter : store->zone_conn_map) {
    const std::string& source_id = iter.first;
    std::string source_str = "source: ";
    std::string s = source_str + source_id;
    auto siter = store->zone_by_id.find(source_id);
    if (siter != store->zone_by_id.end()) {
      s += std::string(" (") + siter->second.name + ")";
    }
    data_status.push_back(s);
    get_data_sync_status(source_id, data_status, source_str.size(), store);
  }

  tab_dump("data sync", width, data_status);
}