#ifndef CEPH_CLS_QUEUE_SRC_H
#define CEPH_CLS_QUEUE_SRC_H

#include "objclass/objclass.h"
#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_queue_ops.h"

int queue_write_head(cls_method_context_t hctx, cls_queue_head& head);
int queue_read_head(cls_method_context_t hctx, cls_queue_head& head);
int queue_init(cls_method_context_t hctx, const cls_queue_init_op& op);
int queue_get_capacity(cls_method_context_t hctx, cls_queue_get_capacity_ret& op_ret);
int queue_enqueue(cls_method_context_t hctx, cls_queue_enqueue_op& op, cls_queue_head& head);
int queue_list_entries(cls_method_context_t hctx, const cls_queue_list_op& op, cls_queue_list_ret& op_ret, cls_queue_head& head);
int queue_remove_entries(cls_method_context_t hctx, const cls_queue_remove_op& op, cls_queue_head& head);

#endif /* CEPH_CLS_QUEUE_SRC_H */
