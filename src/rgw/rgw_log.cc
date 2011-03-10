#include "common/Clock.h"

#include "rgw_log.h"
#include "rgw_acl.h"
#include "rgw_access.h"

static void set_param_str(struct req_state *s, const char *name, string& str)
{
  const char *p = FCGX_GetParam(name, s->fcgx->envp);
  if (p)
    str = p;
}

int rgw_log_op(struct req_state *s)
{
  struct rgw_log_entry entry;

  if (!s->should_log)
    return 0;

  if (!s->bucket) {
    RGW_LOG(0) << "nothing to log for operation" << std::endl;
    return -EINVAL;
  }
  entry.bucket = s->bucket;

  if (s->object)
    entry.obj = s->object;
  else
    entry.obj = "-";

  set_param_str(s, "REMOTE_ADDR", entry.remote_addr);
  set_param_str(s, "HTTP_USER_AGENT", entry.user_agent);
  set_param_str(s, "HTTP_REFERRER", entry.referrer);
  set_param_str(s, "REQUEST_URI", entry.uri);
  set_param_str(s, "REQUEST_METHOD", entry.op);

  entry.user = s->user.user_id;
  if (s->acl)
    entry.owner = s->acl->get_owner().get_id();

  entry.time = s->time;
  entry.total_time = g_clock.now() - s->time;
  entry.bytes_sent = s->bytes_sent;
  if (s->status)
    entry.http_status = s->status;
  else
    entry.http_status = "200"; // default

  if (s->err_exist)
    entry.error_code = s->err.code;
  else
    entry.error_code = "-";

  bufferlist bl;
  ::encode(entry, bl);

  string log_bucket = RGW_LOG_BUCKET_NAME;

  struct tm bdt;
  time_t t = entry.time.sec();
  localtime_r(&t, &bdt);
  
  char buf[entry.bucket.size() + 16];
  sprintf(buf, "%.4d-%.2d-%.2d-%s", (bdt.tm_year+1900), (bdt.tm_mon+1), bdt.tm_mday, entry.bucket.c_str());
  string oid = buf;

  int ret = rgwstore->append_async(log_bucket, oid, bl.length(), bl);

  if (ret == -ENOENT) {
    string id;
    map<std::string, bufferlist> attrs;
    ret = rgwstore->create_bucket(id, log_bucket, attrs);
    if (ret < 0)
      goto done;
    ret = rgwstore->append_async(log_bucket, entry.bucket, bl.length(), bl);
  }
done:
  if (ret < 0)
    RGW_LOG(0) << "failed to log entry" << std::endl;

  return ret;
}
