// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/Clock.h"
#include "common/Timer.h"
#include "common/utf8.h"
#include "common/OutputDataSocket.h"
#include "common/Formatter.h"

#include "rgw_bucket.h"
#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_client_io.h"
#include "rgw_rest.h"
#include "rgw_zone.h"
#include "rgw_rados.h"

#include "services/svc_zone.h"

#include <chrono>
#include <math.h>

#define dout_subsys ceph_subsys_rgw

using namespace std;

static void set_param_str(struct req_state *s, const char *name, string& str)
{
  const char *p = s->info.env->get(name);
  if (p)
    str = p;
}

string render_log_object_name(const string& format,
			      struct tm *dt, const string& bucket_id,
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

      case 'i':
	o += bucket_id;
	continue;
      case 'n':
	o += bucket_name;
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
class UsageLogger : public DoutPrefixProvider {
  CephContext *cct;
  rgw::sal::Store* store;
  map<rgw_user_bucket, RGWUsageBatch> usage_map;
  ceph::mutex lock = ceph::make_mutex("UsageLogger");
  int32_t num_entries;
  ceph::mutex timer_lock = ceph::make_mutex("UsageLogger::timer_lock");
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

  UsageLogger(CephContext *_cct, rgw::sal::Store* _store) : cct(_cct), store(_store), num_entries(0), timer(cct, timer_lock) {
    timer.init();
    std::lock_guard l{timer_lock};
    set_timer();
    utime_t ts = ceph_clock_now();
    recalc_round_timestamp(ts);
  }

  ~UsageLogger() {
    std::lock_guard l{timer_lock};
    flush();
    timer.cancel_all_events();
    timer.shutdown();
  }

  void recalc_round_timestamp(utime_t& ts) {
    round_timestamp = ts.round_to_hour();
  }

  void insert_user(utime_t& timestamp, const rgw_user& user, rgw_usage_log_entry& entry) {
    lock.lock();
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
    lock.unlock();
    if (need_flush) {
      std::lock_guard l{timer_lock};
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
    lock.lock();
    old_map.swap(usage_map);
    num_entries = 0;
    lock.unlock();

    store->log_usage(this, old_map);
  }

  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "rgw UsageLogger: "; }
};

static UsageLogger *usage_logger = NULL;

void rgw_log_usage_init(CephContext *cct, rgw::sal::Store* store)
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
  bucket_name = s->bucket_name;
    user = s->bucket_owner.get_id();
    if (!rgw::sal::Bucket::empty(s->bucket.get()) &&
	s->bucket->get_info().requester_pays) {
      payer = s->user->get_id();
    }
  } else {
      user = s->user->get_id();
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

  ldpp_dout(s, 30) << "log_usage: bucket_name=" << bucket_name
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
  {
    auto t = utime_t{entry.time};
    t.gmtime(formatter->dump_stream("time"));      // UTC
    t.localtime(formatter->dump_stream("time_local"));
  }
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
  {
    using namespace std::chrono;
    uint64_t total_time = duration_cast<milliseconds>(entry.total_time).count();
    formatter->dump_int("total_time", total_time);
  }
  formatter->dump_string("user_agent",  entry.user_agent);
  formatter->dump_string("referrer",  entry.referrer);
  if (entry.x_headers.size() > 0) {
    formatter->open_array_section("http_x_headers");
    for (const auto& iter: entry.x_headers) {
      formatter->open_object_section(iter.first.c_str());
      formatter->dump_string(iter.first.c_str(), iter.second);
      formatter->close_section();
    }
    formatter->close_section();
  }
  formatter->dump_string("trans_id", entry.trans_id);
  switch(entry.identity_type) {
    case TYPE_RGW:
      formatter->dump_string("authentication_type","Local");
      break;
    case TYPE_LDAP:
      formatter->dump_string("authentication_type","LDAP");
      break;
    case TYPE_KEYSTONE:
      formatter->dump_string("authentication_type","Keystone");
      break;
    case TYPE_WEB:
      formatter->dump_string("authentication_type","OIDC Provider");
      break;
    case TYPE_ROLE:
      formatter->dump_string("authentication_type","STS");
      break;
    default:
      break;
  }
  if (entry.token_claims.size() > 0) {
    if (entry.token_claims[0] == "sts") {
      formatter->open_object_section("sts_info");
      for (const auto& iter: entry.token_claims) {
        auto pos = iter.find(":");
        if (pos != string::npos) {
          formatter->dump_string(iter.substr(0, pos), iter.substr(pos + 1));
        }
      }
      formatter->close_section();
    }
  }

  formatter->close_section();
}

OpsLogManifold::~OpsLogManifold()
{
    for (const auto &sink : sinks) {
        delete sink;
    }
}

void OpsLogManifold::add_sink(OpsLogSink* sink)
{
    sinks.push_back(sink);
}

int OpsLogManifold::log(struct req_state* s, struct rgw_log_entry& entry)
{
  int ret = 0;
  for (const auto &sink : sinks) {
    if (sink->log(s, entry) < 0) {
      ret = -1;
    }
  }
  return ret;
}

OpsLogFile::OpsLogFile(CephContext* cct, std::string& path, uint64_t max_data_size) :
  cct(cct), file(path, std::ofstream::app), data_size(0), max_data_size(max_data_size)
{
}

void OpsLogFile::flush()
{
  std::scoped_lock flush_lock(flush_mutex);
  {
    std::scoped_lock log_lock(log_mutex);
    assert(flush_buffer.empty());
    flush_buffer.swap(log_buffer);
    data_size = 0;
  }
  for (auto bl : flush_buffer) {
    int try_num = 0;
    while (true) {
      bl.write_stream(file);
      if (!file) {
        ldpp_dout(this, 0) << "ERROR: failed to log RGW ops log file entry" << dendl;
        file.clear();
        if (stopped) {
          break;
        }
        int sleep_time_secs = std::min((int) pow(2, try_num), 60);
        std::this_thread::sleep_for(std::chrono::seconds(sleep_time_secs));
        try_num++;
      } else {
        break;
      }
    }
  }
  flush_buffer.clear();
  file << std::endl;
}

void* OpsLogFile::entry() {
  std::unique_lock lock(log_mutex);
  while (!stopped) {
    if (!log_buffer.empty()) {
      lock.unlock();
      flush();
      lock.lock();
      continue;
    }
    cond_flush.wait(lock);
  }
  flush();
  return NULL;
}

void OpsLogFile::start() {
  stopped = false;
  create("ops_log_file");
}

void OpsLogFile::stop() {
  {
    cond_flush.notify_one();
    stopped = true;
  }
  join();
}

OpsLogFile::~OpsLogFile()
{
  if (!stopped) {
    stop();
  }
  file.close();
}

int OpsLogFile::log_json(struct req_state* s, bufferlist& bl)
{
  std::unique_lock lock(log_mutex);
  if (data_size + bl.length() >= max_data_size) {
    ldout(s->cct, 0) << "ERROR: RGW ops log file buffer too full, dropping log for txn: " << s->trans_id << dendl;
    return -1;
  }
  log_buffer.push_back(bl);
  data_size += bl.length();
  cond_flush.notify_all();
  return 0;
}

JsonOpsLogSink::JsonOpsLogSink() {
  formatter = new JSONFormatter;
}

JsonOpsLogSink::~JsonOpsLogSink() {
  delete formatter;
}

void JsonOpsLogSink::formatter_to_bl(bufferlist& bl)
{
  stringstream ss;
  formatter->flush(ss);
  const string& s = ss.str();
  bl.append(s);
}

int JsonOpsLogSink::log(struct req_state* s, struct rgw_log_entry& entry)
{
  bufferlist bl;

  lock.lock();
  rgw_format_ops_log_entry(entry, formatter);
  formatter_to_bl(bl);
  lock.unlock();

  return log_json(s, bl);
}

void OpsLogSocket::init_connection(bufferlist& bl)
{
  bl.append("[");
}

OpsLogSocket::OpsLogSocket(CephContext *cct, uint64_t _backlog) : OutputDataSocket(cct, _backlog)
{
  delim.append(",\n");
}

int OpsLogSocket::log_json(struct req_state* s, bufferlist& bl)
{
  append_output(bl);
  return 0;
}

OpsLogRados::OpsLogRados(rgw::sal::Store* const& store): store(store)
{
}

int OpsLogRados::log(struct req_state* s, struct rgw_log_entry& entry)
{
  if (!s->cct->_conf->rgw_ops_log_rados) {
    return 0;
  }
  bufferlist bl;
  encode(entry, bl);

  struct tm bdt;
  time_t t = req_state::Clock::to_time_t(entry.time);
  if (s->cct->_conf->rgw_log_object_name_utc)
    gmtime_r(&t, &bdt);
  else
    localtime_r(&t, &bdt);
  string oid = render_log_object_name(s->cct->_conf->rgw_log_object_name, &bdt,
                                      entry.bucket_id, entry.bucket);
  if (store->log_op(s, oid, bl) < 0) {
    ldpp_dout(s, 0) << "ERROR: failed to log RADOS RGW ops log entry for txn: " << s->trans_id << dendl;
    return -1;
  }
  return 0;
}

int rgw_log_op(RGWREST* const rest, struct req_state *s, const string& op_name, OpsLogSink *olog)
{
  struct rgw_log_entry entry;
  string bucket_id;

  if (s->enable_usage_log)
    log_usage(s, op_name);

  if (!s->enable_ops_log)
    return 0;

  if (s->bucket_name.empty()) {
    /* this case is needed for, e.g., list_buckets */
  } else {
    if (s->err.ret == -ERR_NO_SUCH_BUCKET ||
	rgw::sal::Bucket::empty(s->bucket.get())) {
      if (!s->cct->_conf->rgw_log_nonexistent_bucket) {
	ldout(s->cct, 5) << "bucket " << s->bucket_name << " doesn't exist, not logging" << dendl;
	return 0;
      }
      bucket_id = "";
    } else {
      bucket_id = s->bucket->get_bucket_id();
    }
    entry.bucket = rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name);

    if (check_utf8(entry.bucket.c_str(), entry.bucket.size()) != 0) {
      ldpp_dout(s, 5) << "not logging op on bucket with non-utf8 name" << dendl;
      return 0;
    }

    if (!rgw::sal::Object::empty(s->object.get())) {
      entry.obj = s->object->get_key();
    } else {
      entry.obj = rgw_obj_key("-");
    }

    entry.obj_size = s->obj_size;
  } /* !bucket empty */

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

  std::string uri;
  if (s->info.env->exists("REQUEST_METHOD")) {
    uri.append(s->info.env->get("REQUEST_METHOD"));
    uri.append(" ");
  }

  if (s->info.env->exists("REQUEST_URI")) {
    uri.append(s->info.env->get("REQUEST_URI"));
  }

  if (s->info.env->exists("QUERY_STRING")) {
    const char* qs = s->info.env->get("QUERY_STRING");
    if(qs && (*qs != '\0')) {
      uri.append("?");
      uri.append(qs);
    }
  }

  if (s->info.env->exists("HTTP_VERSION")) {
    uri.append(" ");
    uri.append("HTTP/");
    uri.append(s->info.env->get("HTTP_VERSION"));
  }

  entry.uri = std::move(uri);

  entry.op = op_name;

  if (s->auth.identity) {
    entry.identity_type = s->auth.identity->get_identity_type();
  } else {
    entry.identity_type = TYPE_NONE;
  }

  if (! s->token_claims.empty()) {
    entry.token_claims = std::move(s->token_claims);
  }

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

  entry.user = s->user->get_id().to_str();
  if (s->object_acl)
    entry.object_owner = s->object_acl->get_owner().get_id();
  entry.bucket_owner = s->bucket_owner.get_id();

  uint64_t bytes_sent = ACCOUNTING_IO(s)->get_bytes_sent();
  uint64_t bytes_received = ACCOUNTING_IO(s)->get_bytes_received();

  entry.time = s->time;
  entry.total_time = s->time_elapsed();
  entry.bytes_sent = bytes_sent;
  entry.bytes_received = bytes_received;
  if (s->err.http_ret) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", s->err.http_ret);
    entry.http_status = buf;
  } else {
    entry.http_status = "200"; // default
  }
  entry.error_code = s->err.err_code;
  entry.bucket_id = bucket_id;
  entry.trans_id = s->trans_id;
  if (olog) {
    return olog->log(s, entry);
  }
  return 0;
}

void rgw_log_entry::generate_test_instances(list<rgw_log_entry*>& o)
{
  rgw_log_entry *e = new rgw_log_entry;
  e->object_owner = "object_owner";
  e->bucket_owner = "bucket_owner";
  e->bucket = "bucket";
  e->remote_addr = "1.2.3.4";
  e->user = "user";
  e->obj = rgw_obj_key("obj");
  e->uri = "http://uri/bucket/obj";
  e->http_status = "200";
  e->error_code = "error_code";
  e->bytes_sent = 1024;
  e->bytes_received = 512;
  e->obj_size = 2048;
  e->user_agent = "user_agent";
  e->referrer = "referrer";
  e->bucket_id = "10";
  e->trans_id = "trans_id";
  e->identity_type = TYPE_RGW;
  o.push_back(e);
  o.push_back(new rgw_log_entry);
}

void rgw_log_entry::dump(Formatter *f) const
{
  f->dump_string("object_owner", object_owner.to_str());
  f->dump_string("bucket_owner", bucket_owner.to_str());
  f->dump_string("bucket", bucket);
  f->dump_stream("time") << time;
  f->dump_string("remote_addr", remote_addr);
  f->dump_string("user", user);
  f->dump_stream("obj") << obj;
  f->dump_string("op", op);
  f->dump_string("uri", uri);
  f->dump_string("http_status", http_status);
  f->dump_string("error_code", error_code);
  f->dump_unsigned("bytes_sent", bytes_sent);
  f->dump_unsigned("bytes_received", bytes_received);
  f->dump_unsigned("obj_size", obj_size);
  f->dump_stream("total_time") << total_time;
  f->dump_string("user_agent", user_agent);
  f->dump_string("referrer", referrer);
  f->dump_string("bucket_id", bucket_id);
  f->dump_string("trans_id", trans_id);
  f->dump_unsigned("identity_type", identity_type);
}

