#include "cls/rgw_ratelimit/cls_rgw_ratelimit_client.h"

#include <errno.h>

#include "include/encoding.h"
#include "include/rados/librados.hpp"

namespace cls::rgw::ratelimit {

template <typename Method>
static int exec_write(librados::IoCtx* ioctx,
                      const std::string& oid,
                      const Method& method,
                      bufferlist& in,
                      bufferlist* out = nullptr)
{
  int rval = 0;
  librados::ObjectWriteOperation op;
  if (out) {
    op.exec(method, in, out, &rval);
  } else {
    bufferlist discard;
    op.exec(method, in, &discard, &rval);
  }

  const int ret = ioctx->operate(oid, &op, librados::OPERATION_RETURNVEC);
  if (ret < 0) {
    return ret;
  }
  return rval;
}

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
  int ret = exec_write(ioctx, oid, method::consume, in, &out);
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
  return exec_write(ioctx, oid, method::giveback, in);
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
  return exec_write(ioctx, oid, method::decrease_bytes, in);
}

} // namespace cls::rgw::ratelimit
