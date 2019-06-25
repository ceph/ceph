#ifndef CEPH_CLS_QUEUE_H
#define CEPH_CLS_QUEUE_H

int cls_create_queue(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_init_queue(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_get_queue_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_enqueue(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_dequeue(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_queue_list_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_queue_remove_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_queue_get_last_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_queue_update_last_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_queue_read_urgent_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_queue_write_urgent_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out);
int cls_queue_can_urgent_data_fit(cls_method_context_t hctx, bufferlist *in, bufferlist *out);

#endif /* CEPH_CLS_QUEUE_H */