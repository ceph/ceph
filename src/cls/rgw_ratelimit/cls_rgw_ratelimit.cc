#include "objclass/objclass.h"

#include <errno.h>

#include "cls/rgw_ratelimit/cls_rgw_ratelimit_ops.h"
#include "rgw_ratelimit_core.h"

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

CLS_VER(1, 0)
CLS_NAME(rgw_ratelimit)

static int load_state(cls_method_context_t hctx,
                        const std::string& key,
                        RGWRateLimitCounterState& state)
{
  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, key, &bl);
  if (ret == -ENOENT || ret == -ENODATA) {
    return 0;
  }
  if (ret < 0) {
    return ret;
  }
  auto iter = bl.cbegin();
  try {
    state.decode(iter);
  } catch (const buffer::error& err) {
    return -EBADMSG;
  }
  return 0;
}

static int store_state(cls_method_context_t hctx,
                         const std::string& key,
                         const RGWRateLimitCounterState& state)
{
  bufferlist bl;
  state.encode(bl);
  return cls_cxx_map_set_val(hctx, key, &bl);
}

static int handle_consume(cls_method_context_t hctx, bufferlist* in, bufferlist* out)
{
  cls_rgw_ratelimit_consume_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (const buffer::error& err) {
    return -EINVAL;
  }

  RGWRateLimitCounterState state;
  int ret = load_state(hctx, op.key, state);
  if (ret < 0) {
    return ret;
  }

  const ceph::timespan ts{std::chrono::nanoseconds(op.ts_ns)};
  const int64_t delay = rgw::ratelimit::consume(
      state,
      static_cast<OpType>(op.op_type),
      &op.info,
      ts,
      op.interval);

  ret = store_state(hctx, op.key, state);
  if (ret < 0) {
    return ret;
  }

  cls_rgw_ratelimit_consume_reply reply;
  reply.delay = delay;
  encode(reply, *out);
  return 0;
}

static int handle_giveback(cls_method_context_t hctx, bufferlist* in, bufferlist* out)
{
  cls_rgw_ratelimit_giveback_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (const buffer::error& err) {
    return -EINVAL;
  }

  RGWRateLimitCounterState state;
  int ret = load_state(hctx, op.key, state);
  if (ret < 0) {
    return ret;
  }

  rgw::ratelimit::giveback(state, static_cast<OpType>(op.op_type));
  return store_state(hctx, op.key, state);
}

static int handle_decrease_bytes(cls_method_context_t hctx, bufferlist* in, bufferlist* out)
{
  cls_rgw_ratelimit_decrease_bytes_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (const buffer::error& err) {
    return -EINVAL;
  }

  RGWRateLimitCounterState state;
  int ret = load_state(hctx, op.key, state);
  if (ret < 0) {
    return ret;
  }

  rgw::ratelimit::decrease_bytes(state, op.is_read, op.amount, &op.info);
  return store_state(hctx, op.key, state);
}

CLS_INIT(rgw_ratelimit)
{
  CLS_LOG(20, "loading cls_rgw_ratelimit");

  cls_handle_t h_class;
  cls_method_handle_t h_consume;
  cls_method_handle_t h_giveback;
  cls_method_handle_t h_decrease_bytes;

  cls_register("rgw_ratelimit", &h_class);

  cls_register_cxx_method(h_class, cls::rgw::ratelimit::method::consume,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          handle_consume, &h_consume);
  cls_register_cxx_method(h_class, cls::rgw::ratelimit::method::giveback,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          handle_giveback, &h_giveback);
  cls_register_cxx_method(h_class, cls::rgw::ratelimit::method::decrease_bytes,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          handle_decrease_bytes, &h_decrease_bytes);
}
