#ifndef CEPH_CLS_QUEUE_CONSTS_H
#define CEPH_CLS_QUEUE_CONSTS_H

#define QUEUE_CLASS "queue"
#define RGW_QUEUE_CLASS "rgw_queue"

#define INIT_QUEUE "init_queue"
#define GET_QUEUE_SIZE "get_queue_size"
#define ENQUEUE "enqueue"
#define QUEUE_LIST_ENTRIES "queue_list_entries"
#define QUEUE_REMOVE_ENTRIES "queue_remove_entries"
#define QUEUE_READ_URGENT_DATA "queue_read_urgent_data"
#define QUEUE_WRITE_URGENT_DATA "queue_write_urgent_data"
#define QUEUE_CAN_URGENT_DATA_FIT "queue_can_urgent_data_fit"

#define GC_INIT_QUEUE "gc_init_queue"
#define GC_ENQUEUE "gc_enqueue"
#define GC_DEQUEUE "gc_dequeue"
#define GC_QUEUE_LIST_ENTRIES "gc_queue_list_entries"
#define GC_QUEUE_REMOVE_ENTRIES "gc_queue_remove_entries"
#define GC_QUEUE_UPDATE_ENTRY "gc_queue_update_entry"

#endif