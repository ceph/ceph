// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include <errno.h>

#include "objclass/objclass.h"
#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/queue/cls_queue_src.h"

CLS_VER(1,0)
CLS_NAME(queue)

static int cls_init_queue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();
  cls_queue_init_op op;
  op.has_urgent_data = false;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_init_op(): failed to decode entry\n");
    return -EINVAL;
  }

  return init_queue(hctx, op);
}

static int cls_get_queue_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_queue_get_size_ret op_ret;
  auto ret = get_queue_size(hctx, op_ret);
  if (ret < 0) {
    return ret;
  }

  encode(op_ret, *out);
  return 0;
}

static int cls_enqueue(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto iter = in->cbegin();
  cls_queue_enqueue_op op;
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_enqueue: failed to decode input data \n");
    return -EINVAL;
  }

  cls_queue_head head;
  auto ret = get_queue_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  ret = enqueue(hctx, op, head);
  if (ret < 0) {
    return ret;
  }

  //Write back head
  return write_queue_head(hctx, head);
}

static int cls_queue_list_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();
  cls_queue_list_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_list_entries(): failed to decode input data\n");
    return -EINVAL;
  }

  cls_queue_head head;
  auto ret = get_queue_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_queue_list_ret op_ret;
  ret = queue_list_entries(hctx, op, op_ret, head);
  if (ret < 0) {
    return ret;
  }

  encode(op_ret, *out);
  return 0;
}

static int cls_queue_remove_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();
  cls_queue_remove_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_queue_remove_entries: failed to decode input data\n");
    return -EINVAL;
  }

  cls_queue_head head;
  auto ret = get_queue_head(hctx, head);
  if (ret < 0) {
    return ret;
  }
  ret = queue_remove_entries(hctx, op, head);
  if (ret < 0) {
    return ret;
  }
  return write_queue_head(hctx, head);
}

CLS_INIT(queue)
{
  CLS_LOG(1, "Loaded queue class!");

  cls_handle_t h_class;
  cls_method_handle_t h_init_queue;
  cls_method_handle_t h_get_queue_size;
  cls_method_handle_t h_enqueue;
  cls_method_handle_t h_queue_list_entries;
  cls_method_handle_t h_queue_remove_entries;
 
  cls_register(QUEUE_CLASS, &h_class);

  /* queue*/
  cls_register_cxx_method(h_class, INIT_QUEUE, CLS_METHOD_WR, cls_init_queue, &h_init_queue);
  cls_register_cxx_method(h_class, GET_QUEUE_SIZE, CLS_METHOD_RD, cls_get_queue_size, &h_get_queue_size);
  cls_register_cxx_method(h_class, ENQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_enqueue, &h_enqueue);
  cls_register_cxx_method(h_class, QUEUE_LIST_ENTRIES, CLS_METHOD_RD, cls_queue_list_entries, &h_queue_list_entries);
  cls_register_cxx_method(h_class, QUEUE_REMOVE_ENTRIES, CLS_METHOD_RD | CLS_METHOD_WR, cls_queue_remove_entries, &h_queue_remove_entries);

  return;
}

