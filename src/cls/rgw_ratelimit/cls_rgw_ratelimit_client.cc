#include "cls/rgw_ratelimit/cls_rgw_ratelimit_client.h"

#include <errno.h>

#include "include/encoding.h"

namespace cls::rgw::ratelimit {

int consume(librados::IoCtx* ioctx,
            const std::string& oid,
            const std::string& key,
            OpType op_type,
            const RGWRateLimitInfo& info,
            ceph::timespan ts,
            int64_t interval,
            int64_t* delay)
{
  cls_rgw_ratelimit_consume_op cop;
  cop.key = key;
  cop.op_type = static_cast<uint8_t>(op_type);
  cop.info = info;
  cop.ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(ts).count();
  cop.interval = interval;

  bufferlist in;
  encode(cop, in);
  bufferlist out;
  int ret = ioctx->exec(oid, method::consume, in, out);
  if (ret < 0) {
    return ret;
  }

  cls_rgw_ratelimit_consume_reply reply;
  try {
    auto iter = out.cbegin();
    decode(reply, iter);
  } catch (const buffer::error& err) {
    return -EBADMSG;
  }
  if (delay) {
    *delay = reply.delay;
  }
  return 0;
}

int giveback(librados::IoCtx* ioctx,
             const std::string& oid,
             const std::string& key,
             OpType op_type)
{
  cls_rgw_ratelimit_giveback_op gop;
  gop.key = key;
  gop.op_type = static_cast<uint8_t>(op_type);

  bufferlist in;
  encode(gop, in);
  bufferlist out;
  return ioctx->exec(oid, method::giveback, in, out);
}

int decrease_bytes(librados::IoCtx* ioctx,
                   const std::string& oid,
                   const std::string& key,
                   bool is_read,
                   int64_t amount,
                   const RGWRateLimitInfo& info)
{
  cls_rgw_ratelimit_decrease_bytes_op dop;
  dop.key = key;
  dop.is_read = is_read;
  dop.amount = amount;
  dop.info = info;

  bufferlist in;
  encode(dop, in);
  bufferlist out;
  return ioctx->exec(oid, method::decrease_bytes, in, out);
}

} // namespace cls::rgw::ratelimit
