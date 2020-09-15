// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <vector>
#include "include/rados/librados.hpp"
#include "cls/queue/cls_queue_types.h"
#include "cls/2pc_queue/cls_2pc_queue_types.h"

// initialize the queue with maximum size (bytes)
// note that the actual size of the queue will be larger, as 24K bytes will be allocated in the head object
// and more may be allocated as xattrs of the object (depending with the number of concurrent reservations)
void cls_2pc_queue_init(librados::ObjectWriteOperation& op, const std::string& queue_name, uint64_t size);

// these overloads which call io_ctx.operate() or io_ctx.exec() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()/exec()
#ifndef CLS_CLIENT_HIDE_IOCTX
// return capacity (bytes)
int cls_2pc_queue_get_capacity(librados::IoCtx& io_ctx, const string& queue_name, uint64_t& size);

// make a reservation on the queue (in bytes) and number of expected entries (to calculate overhead)
// return a reservation id if reservations is possible, 0 otherwise
int cls_2pc_queue_reserve(librados::IoCtx& io_ctx, const std::string& queue_name, 
        uint64_t res_size, uint32_t entries, cls_2pc_reservation::id_t& res_id);

// incremental listing of all entries in the queue
int cls_2pc_queue_list_entries(librados::IoCtx& io_ctx, const std::string& queue_name, const std::string& marker, uint32_t max,
        std::vector<cls_queue_entry>& entries, bool *truncated, std::string& next_marker);

// list all pending reservations in the queue
int cls_2pc_queue_list_reservations(librados::IoCtx& io_ctx, const std::string& queue_name, cls_2pc_reservations& reservations);
#endif

// optionally async method for getting capacity (bytes) 
// after answer is received, call cls_2pc_queue_get_capacity_result() to parse the results
void cls_2pc_queue_get_capacity(librados::ObjectReadOperation& op,  bufferlist* obl, int* prval);

int cls_2pc_queue_get_capacity_result(const bufferlist& bl, uint64_t& size);

// optionally async method for making a reservation on the queue (in bytes) and number of expected entries (to calculate overhead)
// notes: 
// (1) make sure that librados::OPERATION_RETURNVEC is passed to the executing function
// (2) multiple operations cannot be executed in a batch (operations both read and write)
// after answer is received, call cls_2pc_queue_reserve_result() to parse the results
void cls_2pc_queue_reserve(librados::ObjectWriteOperation& op, uint64_t res_size, 
    uint32_t entries, bufferlist* obl, int* prval);

int cls_2pc_queue_reserve_result(const bufferlist& bl, cls_2pc_reservation::id_t& res_id);

// commit data using a reservation done beforehand
// res_id must be allocated using cls_2pc_queue_reserve, and could be either committed or aborted once
// the size of bl_data_vec must be equal or smaller to the size reserved for the res_id
// note that the number of entries in bl_data_vec does not have to match the number of entries reserved
// only size (including the overhead of the entries) is checked
void cls_2pc_queue_commit(librados::ObjectWriteOperation& op, std::vector<bufferlist> bl_data_vec, 
        cls_2pc_reservation::id_t res_id);

// abort a reservation
// res_id must be allocated using cls_2pc_queue_reserve
void cls_2pc_queue_abort(librados::ObjectWriteOperation& op, 
        cls_2pc_reservation::id_t res_id);

// optionally async incremental listing of all entries in the queue
// after answer is received, call cls_2pc_queue_list_entries_result() to parse the results
void cls_2pc_queue_list_entries(librados::ObjectReadOperation& op, const std::string& marker, uint32_t max, bufferlist* obl, int* prval);

int cls_2pc_queue_list_entries_result(const bufferlist& bl, std::vector<cls_queue_entry>& entries,
                            bool *truncated, std::string& next_marker);

// optionally async listing of all pending reservations in the queue
// after answer is received, call cls_2pc_queue_list_reservations_result() to parse the results
void cls_2pc_queue_list_reservations(librados::ObjectReadOperation& op, bufferlist* obl, int* prval);

int cls_2pc_queue_list_reservations_result(const librados::bufferlist& bl, cls_2pc_reservations& reservations);

// expire stale reservations (older than the given time)
void cls_2pc_queue_expire_reservations(librados::ObjectWriteOperation& op, 
        ceph::coarse_real_time stale_time);

// remove all entries up to the given marker
void cls_2pc_queue_remove_entries(librados::ObjectWriteOperation& op, const std::string& end_marker);

