// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Clock.h"
#include "common/Timer.h"
#include "common/utf8.h"
#include "common/OutputDataSocket.h"
#include "common/Formatter.h"

#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_rados.h"
#include "rgw_client_io.h"
#include "rgw_rest.h"
#include "cls/lock/cls_lock_client.h"

#define dout_subsys ceph_subsys_rgw

static void set_param_str(struct req_state *s, const char *name, string& str)
{
  const char *p = s->info.env->get(name);
  if (p)
    str = p;
}

string render_log_object_name(const string& format,
			      struct tm *dt, string& bucket_id,
			      const string& bucket_name)
{
  string o;
  for (unsigned i=0; i<format.size(); i++) {
    if (format[i] == '%' && i+1 < format.size()) {
      i++;
      char buf[32];
      switch (format[i]) {
      case '%':
	strcpy(buf, "%");
	break;
      case 'Y':
	sprintf(buf, "%.4d", dt->tm_year + 1900);
	break;
      case 'y':
	sprintf(buf, "%.2d", dt->tm_year % 100);
	break;
      case 'm':
	sprintf(buf, "%.2d", dt->tm_mon + 1);
	break;
      case 'd':
	sprintf(buf, "%.2d", dt->tm_mday);
	break;
      case 'H':
	sprintf(buf, "%.2d", dt->tm_hour);
	break;
      case 'I':
	sprintf(buf, "%.2d", (dt->tm_hour % 12) + 1);
	break;
      case 'k':
	sprintf(buf, "%d", dt->tm_hour);
	break;
      case 'l':
	sprintf(buf, "%d", (dt->tm_hour % 12) + 1);
	break;
      case 'M':
	sprintf(buf, "%.2d", dt->tm_min);
	break;
      case 'S':
        {
          if (dt->tm_sec < 30) {
            sprintf(buf, "%.2d", 0);
          } else {
            sprintf(buf, "%.2d", 30);
          }
        }
        break;

      case 'i':
	o += bucket_id;
	continue;
      case 'n':
	o += bucket_name;
	continue;
      case 'u':
        {
#define OPSLOG_UNIQUE_STRING_LEN    16
          static bool unique_str_specified = false;
          static std::string unique_str;
          if (!unique_str_specified) {
            unique_str_specified = true;
            char unique_string_buf[OPSLOG_UNIQUE_STRING_LEN + 1];
            gen_rand_alphanumeric_plain(g_ceph_context, unique_string_buf,
                                        sizeof(unique_string_buf));
            unique_str = std::string(unique_string_buf);
          }
          o += unique_str;
        }
	continue;
      default:
	// unknown code
	sprintf(buf, "%%%c", format[i]);
	break;
      }
      o += buf;
      continue;
    }
    o += format[i];
  }
  return o;
}

/* usage logger */
class UsageLogger {
  CephContext *cct;
  RGWRados *store;
  map<rgw_user_bucket, RGWUsageBatch> usage_map;
  Mutex lock;
  int32_t num_entries;
  Mutex timer_lock;
  SafeTimer timer;
  utime_t round_timestamp;

  class C_UsageLogTimeout : public Context {
    UsageLogger *logger;
  public:
    explicit C_UsageLogTimeout(UsageLogger *_l) : logger(_l) {}
    void finish(int r) override {
      logger->flush();
      logger->set_timer();
    }
  };

  void set_timer() {
    timer.add_event_after(cct->_conf->rgw_usage_log_tick_interval, new C_UsageLogTimeout(this));
  }
public:

  UsageLogger(CephContext *_cct, RGWRados *_store) : cct(_cct), store(_store), lock("UsageLogger"), num_entries(0), timer_lock("UsageLogger::timer_lock"), timer(cct, timer_lock) {
    timer.init();
    Mutex::Locker l(timer_lock);
    set_timer();
    utime_t ts = ceph_clock_now();
    recalc_round_timestamp(ts);
  }

  ~UsageLogger() {
    Mutex::Locker l(timer_lock);
    flush();
    timer.cancel_all_events();
    timer.shutdown();
  }

  void recalc_round_timestamp(utime_t& ts) {
    round_timestamp = ts.round_to_hour();
  }

  void insert_user(utime_t& timestamp, const rgw_user& user, rgw_usage_log_entry& entry) {
    lock.Lock();
    if (timestamp.sec() > round_timestamp + 3600)
      recalc_round_timestamp(timestamp);
    entry.epoch = round_timestamp.sec();
    bool account;
    string u = user.to_str();
    rgw_user_bucket ub(u, entry.bucket);
    real_time rt = round_timestamp.to_real_time();
    usage_map[ub].insert(rt, entry, &account);
    if (account)
      num_entries++;
    bool need_flush = (num_entries > cct->_conf->rgw_usage_log_flush_threshold);
    lock.Unlock();
    if (need_flush) {
      Mutex::Locker l(timer_lock);
      flush();
    }
  }

  void insert(utime_t& timestamp, rgw_usage_log_entry& entry) {
    if (entry.payer.empty()) {
      insert_user(timestamp, entry.owner, entry);
    } else {
      insert_user(timestamp, entry.payer, entry);
    }
  }

  void flush() {
    map<rgw_user_bucket, RGWUsageBatch> old_map;
    lock.Lock();
    old_map.swap(usage_map);
    num_entries = 0;
    lock.Unlock();

    store->log_usage(old_map);
  }
};

static UsageLogger *usage_logger = NULL;

void rgw_log_usage_init(CephContext *cct, RGWRados *store)
{
  usage_logger = new UsageLogger(cct, store);
}

void rgw_log_usage_finalize()
{
  delete usage_logger;
  usage_logger = NULL;
}

static void log_usage(struct req_state *s, const string& op_name)
{
  if (s->system_request) /* don't log system user operations */
    return;

  if (!usage_logger)
    return;

  rgw_user user;
  rgw_user payer;
  string bucket_name;

  bucket_name = s->bucket_name;

  if (!bucket_name.empty()) {
    user = s->bucket_owner.get_id();
    if (s->bucket_info.requester_pays) {
      payer = s->user->user_id;
    }
  } else {
      user = s->user->user_id;
  }

  bool error = s->err.is_err();
  if (error && s->err.http_ret == 404) {
    bucket_name = "-"; /* bucket not found, use the invalid '-' as bucket name */
  }

  string u = user.to_str();
  string p = payer.to_str();
  rgw_usage_log_entry entry(u, p, bucket_name);

  uint64_t bytes_sent = ACCOUNTING_IO(s)->get_bytes_sent();
  uint64_t bytes_received = ACCOUNTING_IO(s)->get_bytes_received();

  rgw_usage_data data(bytes_sent, bytes_received);

  data.ops = 1;
  if (!s->is_err())
    data.successful_ops = 1;

  ldout(s->cct, 30) << "log_usage: bucket_name=" << bucket_name
	<< " tenant=" << s->bucket_tenant
	<< ", bytes_sent=" << bytes_sent << ", bytes_received="
	<< bytes_received << ", success=" << data.successful_ops << dendl;

  entry.add(op_name, data);

  utime_t ts = ceph_clock_now();

  usage_logger->insert(ts, entry);
}

void rgw_format_ops_log_entry(struct rgw_log_entry& entry, Formatter *formatter)
{
  formatter->open_object_section("log_entry");
  formatter->dump_string("bucket", entry.bucket);
  entry.time.gmtime(formatter->dump_stream("time"));      // UTC
  entry.time.localtime(formatter->dump_stream("time_local"));
  formatter->dump_string("remote_addr", entry.remote_addr);
  string obj_owner = entry.object_owner.to_str();
  if (obj_owner.length())
    formatter->dump_string("object_owner", obj_owner);
  formatter->dump_string("user", entry.user);
  formatter->dump_string("operation", entry.op);
  formatter->dump_string("uri", entry.uri);
  formatter->dump_string("http_status", entry.http_status);
  formatter->dump_string("error_code", entry.error_code);
  formatter->dump_int("bytes_sent", entry.bytes_sent);
  formatter->dump_int("bytes_received", entry.bytes_received);
  formatter->dump_int("object_size", entry.obj_size);
  uint64_t total_time =  entry.total_time.to_msec();

  formatter->dump_int("total_time", total_time);
  formatter->dump_string("user_agent",  entry.user_agent);
  formatter->dump_string("referrer",  entry.referrer);
  formatter->dump_string("prot_flags", rgw_prot_flags[entry.prot_flags]);
  formatter->dump_string("resource", rgw_resources[entry.resource]);
  formatter->dump_string("http_method", rgw_http_methods[entry.http_method]);
  if (entry.x_headers.size() > 0) {
    formatter->open_array_section("http_x_headers");
    for (const auto& iter: entry.x_headers) {
      formatter->open_object_section(iter.first.c_str());
      formatter->dump_string(iter.first.c_str(), iter.second);
      formatter->close_section();
    }
    formatter->close_section();
  }
  formatter->close_section();
}

void OpsLogSocket::formatter_to_bl(bufferlist& bl)
{
  stringstream ss;
  formatter->flush(ss);
  const string& s = ss.str();

  bl.append(s);
}

void OpsLogSocket::init_connection(bufferlist& bl)
{
  bl.append("[");
}

OpsLogSocket::OpsLogSocket(CephContext *cct, uint64_t _backlog) : OutputDataSocket(cct, _backlog), lock("OpsLogSocket")
{
  formatter = new JSONFormatter;
  delim.append(",\n");
}

OpsLogSocket::~OpsLogSocket()
{
  delete formatter;
}

void OpsLogSocket::log(struct rgw_log_entry& entry)
{
  bufferlist bl;

  lock.Lock();
  rgw_format_ops_log_entry(entry, formatter);
  formatter_to_bl(bl);
  lock.Unlock();

  append_output(bl);
}

int rgw_log_op(RGWRados *store, RGWREST* const rest, struct req_state *s,
	       const string& op_name, OpsLogSocket *olog)
{
  struct rgw_log_entry entry;
  string bucket_id;

  if (s->enable_usage_log)
    log_usage(s, op_name);

  RGWBucketLoggingStatus status(s->cct);
  if (s->bucket_attrs.empty()) {
    RGWObjectCtx obj_ctx(store);
    int ret = store->get_bucket_info(obj_ctx, s->bucket_tenant, s->bucket_name,
                                     s->bucket_info, NULL, &s->bucket_attrs);
    if (ret != 0 && !s->enable_ops_log)
      return 0;
  }

  map<string, bufferlist>::iterator siter = s->bucket_attrs.find(RGW_ATTR_BL);
  if (siter != s->bucket_attrs.end()) {
    bufferlist::iterator iter(&siter->second);
    try {
      status.decode(iter);
    } catch (const buffer::error& e) {
      ldout(s->cct, 20) << __func__ <<  " decode bucket logging status failed" << dendl;
      if (!s->enable_ops_log)
        return 0;
    }
  }

  // overwrite ops log config
  // enable ops log when bucket logging is enabled
  if (!s->enable_ops_log && !status.is_enabled()) {
      return 0;
  }

  if (s->bucket_name.empty()) {
    ldout(s->cct, 5) << "nothing to log for operation" << dendl;
    return -EINVAL;
  }
  if (s->err.ret == -ERR_NO_SUCH_BUCKET) {
    if (!s->cct->_conf->rgw_log_nonexistent_bucket) {
      ldout(s->cct, 5) << "bucket " << s->bucket << " doesn't exist, not logging" << dendl;
      return 0;
    }
    bucket_id = "";
  } else {
    bucket_id = s->bucket.bucket_id;
  }
  rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name, entry.bucket);

  if (check_utf8(entry.bucket.c_str(), entry.bucket.size()) != 0) {
    ldout(s->cct, 5) << "not logging op on bucket with non-utf8 name" << dendl;
    return 0;
  }

  if (!s->object.empty()) {
    entry.obj = s->object;
  } else {
    entry.obj = rgw_obj_key("-");
  }

  entry.obj_size = s->obj_size;

  if (s->cct->_conf->rgw_remote_addr_param.length())
    set_param_str(s, s->cct->_conf->rgw_remote_addr_param.c_str(),
		  entry.remote_addr);
  else
    set_param_str(s, "REMOTE_ADDR", entry.remote_addr);
  set_param_str(s, "HTTP_USER_AGENT", entry.user_agent);
  // legacy apps are still using misspelling referer, such as curl -e option
  if (s->info.env->exists("HTTP_REFERRER"))
    set_param_str(s, "HTTP_REFERRER", entry.referrer);
  else
    set_param_str(s, "HTTP_REFERER", entry.referrer);
  set_param_str(s, "REQUEST_URI", entry.uri);
  set_param_str(s, "REQUEST_METHOD", entry.op);

  entry.http_method = s->op; // track parsed http method

  /* custom header logging */
  if (rest) {
    if (rest->log_x_headers()) {
      for (const auto& iter : s->info.env->get_map()) {
	if (rest->log_x_header(iter.first)) {
	  entry.x_headers.insert(
	    rgw_log_entry::headers_map::value_type(iter.first, iter.second));
	}
      }
    }
  }

  entry.user = s->user->user_id.to_str();
  if (s->object_acl)
    entry.object_owner = s->object_acl->get_owner().get_id();
  entry.bucket_owner = s->bucket_owner.get_id();

  uint64_t bytes_sent = ACCOUNTING_IO(s)->get_bytes_sent();
  uint64_t bytes_received = ACCOUNTING_IO(s)->get_bytes_received();

  entry.time = s->time;
  entry.total_time = ceph_clock_now() - s->time;
  entry.bytes_sent = bytes_sent;
  entry.bytes_received = bytes_received;
  if (s->err.http_ret) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", s->err.http_ret);
    entry.http_status = buf;
  } else
    entry.http_status = "200"; // default

  entry.error_code = s->err.err_code;
  entry.bucket_id = bucket_id;
  entry.request_id = s->trans_id;
  entry.prot_flags = s->prot_flags;
  entry.resource = s->resource;

  bufferlist bl;
  ::encode(entry, bl);

  struct tm bdt;
  time_t t = entry.time.sec();
  if (s->cct->_conf->rgw_log_object_name_utc)
    gmtime_r(&t, &bdt);
  else
    localtime_r(&t, &bdt);

  int bl_ret = 0;

  if (status.is_enabled()) {
    std::string rgw_log_obj_name = "%Y-%m-%d-%H-%M-%S-%i-%n-%u";
    std::string bl_oid = render_log_object_name(rgw_log_obj_name, &bdt,
                                                s->bucket.bucket_id, entry.bucket);

    ldout(s->cct, 15) << "rgw_log_op get bl ops log obj bl_oid: " << bl_oid << dendl;

    rgw_raw_obj obj(store->get_zone_params().bl_pool, bl_oid);

    rados::cls::lock::Lock l(bl_oid);
    // the default rgw_usage_log_tick_interval is 30 secs, so for keep
    // consistent with usage log, we set the default value of
    // rgw_bl_ops_log_lock_duration to 30 secs.
    utime_t time(s->cct->_conf->rgw_bl_ops_log_lock_duration, 0);
    l.set_duration(time);
    librados::IoCtx *ctx = store->get_bl_pool_ctx();
    bl_ret = l.lock_exclusive(ctx, bl_oid);
    if (bl_ret < 0 && bl_ret != -EEXIST) {
      ldout(s->cct, 0) << "rgw_log_op failed to acquire lock " << bl_oid
                         << " bl_ret: " << bl_ret << dendl;
      goto bl_done;
    }

    ldout(s->cct, 10) << "rgw_log_op acquire lock name = " << bl_oid << dendl;

    bl_ret = store->append_async(obj, bl.length(), bl);
    if (bl_ret == -ENOENT) {
      bl_ret = store->create_pool(store->get_zone_params().bl_pool);
      if (bl_ret < 0)
        goto bl_done;
      // retry
      bl_ret = store->append_async(obj, bl.length(), bl);
    }

bl_done:
    if (bl_ret < 0) {
      ldout(s->cct, 0) << "ERROR: failed to bl ops log entry, bl_ret = "
                       << bl_ret << dendl;
      l.unlock(ctx, bl_oid);
    }
  }

  int ret = 0;

  if (s->enable_ops_log && s->cct->_conf->rgw_ops_log_rados) {
    std::string oid = render_log_object_name(s->cct->_conf->rgw_log_object_name, &bdt,
                                             s->bucket.bucket_id, entry.bucket);

    ldout(s->cct, 15) << "rgw_log_op get ops log obj oid: " << oid << dendl;

    rgw_raw_obj obj(store->get_zone_params().log_pool, oid);

    ret = store->append_async(obj, bl.length(), bl);
    if (ret == -ENOENT) {
      ret = store->create_pool(store->get_zone_params().log_pool);
      if (ret < 0)
        goto done;
      // retry
      ret = store->append_async(obj, bl.length(), bl);
    }
  }

  if (olog) {
    olog->log(entry);
  }
done:
  if (ret < 0)
    ldout(s->cct, 0) << "ERROR: failed to log entry, ret = "
                     << ret << dendl;

  return (ret || bl_ret);
}

