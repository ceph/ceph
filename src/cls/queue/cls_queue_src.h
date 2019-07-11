#ifndef CEPH_CLS_QUEUE_SRC_H
#define CEPH_CLS_QUEUE_SRC_H

int write_queue_head(cls_method_context_t hctx, cls_queue_head& head);
int get_queue_head(cls_method_context_t hctx, cls_queue_head& head);
int init_queue(cls_method_context_t hctx, const cls_queue_init_op& op);
int get_queue_size(cls_method_context_t hctx, cls_queue_get_size_ret& op_ret);
int enqueue(cls_method_context_t hctx, cls_queue_enqueue_op& op, cls_queue_head& head);
int queue_list_entries(cls_method_context_t hctx, const cls_queue_list_op& op, cls_queue_list_ret& op_ret, cls_queue_head& head);
int queue_remove_entries(cls_method_context_t hctx, const cls_queue_remove_op& op, cls_queue_head& head);

#endif /* CEPH_CLS_QUEUE_SRC_H */