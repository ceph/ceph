#ifndef CEPH_CLS_STATELOG_CLIENT_H
#define CEPH_CLS_STATELOG_CLIENT_H

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls_statelog_types.h"

/*
 * log objclass
 */

void cls_statelog_add_prepare_entry(cls_statelog_entry& entry, const string& client_id, const string& op_id,
                 const string& object, const utime_t& timestamp, uint32_t state, bufferlist& bl);

void cls_statelog_add(librados::ObjectWriteOperation& op, list<cls_statelog_entry>& entry);
void cls_statelog_add(librados::ObjectWriteOperation& op, cls_statelog_entry& entry);
void cls_statelog_add(librados::ObjectWriteOperation& op, const string& client_id, const string& op_id,
                 const string& object, const utime_t& timestamp, uint32_t state, bufferlist& bl);

void cls_statelog_list(librados::ObjectReadOperation& op,
                       const string& client_id, const string& op_id, const string& object, /* op_id may be empty, also one of client_id, object*/
                       const string& in_marker, int max_entries, list<cls_statelog_entry>& entries,
                       string *out_marker, bool *truncated);

void cls_statelog_remove_by_client(librados::ObjectWriteOperation& op, const string& client_id, const string& op_id);
void cls_statelog_remove_by_object(librados::ObjectWriteOperation& op, const string& object, const string& op_id);

void cls_statelog_check_state(librados::ObjectOperation& op, const string& client_id, const string& op_id, const string& object, uint32_t state);
#endif
