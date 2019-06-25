// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include <errno.h>

#include "objclass/objclass.h"
#include "cls/queue/cls_queue_types.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/queue/cls_queue.h"

CLS_VER(1,0)
CLS_NAME(queue)

CLS_INIT(queue)
{
  CLS_LOG(1, "Loaded queue class!");

  cls_handle_t h_class;
  cls_method_handle_t h_create_queue;
  cls_method_handle_t h_init_queue;
  cls_method_handle_t h_get_queue_size;
  cls_method_handle_t h_enqueue;
  cls_method_handle_t h_dequeue;
  cls_method_handle_t h_queue_list_entries;
  cls_method_handle_t h_queue_remove_entries;
  cls_method_handle_t h_queue_update_last_entry;
  cls_method_handle_t h_queue_get_last_entry;
  cls_method_handle_t h_queue_read_urgent_data;
  cls_method_handle_t h_queue_write_urgent_data;
  cls_method_handle_t h_queue_can_urgent_data_fit;
 
  cls_register(QUEUE_CLASS, &h_class);

  /* queue*/
  cls_register_cxx_method(h_class, CREATE_QUEUE, CLS_METHOD_WR, cls_create_queue, &h_create_queue);
  cls_register_cxx_method(h_class, INIT_QUEUE, CLS_METHOD_WR, cls_init_queue, &h_init_queue);
  cls_register_cxx_method(h_class, GET_QUEUE_SIZE, CLS_METHOD_RD, cls_get_queue_size, &h_get_queue_size);
  cls_register_cxx_method(h_class, ENQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_enqueue, &h_enqueue);
  cls_register_cxx_method(h_class, DEQUEUE, CLS_METHOD_RD | CLS_METHOD_WR, cls_dequeue, &h_dequeue);
  cls_register_cxx_method(h_class, QUEUE_LIST_ENTRIES, CLS_METHOD_RD | CLS_METHOD_WR, cls_queue_list_entries, &h_queue_list_entries);
  cls_register_cxx_method(h_class, QUEUE_REMOVE_ENTRIES, CLS_METHOD_RD | CLS_METHOD_WR, cls_queue_remove_entries, &h_queue_remove_entries);
  cls_register_cxx_method(h_class, QUEUE_UPDATE_LAST_ENTRY, CLS_METHOD_RD | CLS_METHOD_WR, cls_queue_update_last_entry, &h_queue_update_last_entry);
  cls_register_cxx_method(h_class, QUEUE_GET_LAST_ENTRY, CLS_METHOD_RD | CLS_METHOD_WR, cls_queue_get_last_entry, &h_queue_get_last_entry);
  cls_register_cxx_method(h_class, QUEUE_READ_URGENT_DATA, CLS_METHOD_RD, cls_queue_read_urgent_data, &h_queue_read_urgent_data);
  cls_register_cxx_method(h_class, QUEUE_WRITE_URGENT_DATA, CLS_METHOD_RD | CLS_METHOD_WR, cls_queue_write_urgent_data, &h_queue_write_urgent_data);
  cls_register_cxx_method(h_class, QUEUE_CAN_URGENT_DATA_FIT, CLS_METHOD_RD | CLS_METHOD_WR, cls_queue_can_urgent_data_fit, &h_queue_can_urgent_data_fit);

  return;
}

