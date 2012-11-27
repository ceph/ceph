#ifndef CEPH_CLS_RGW_CLIENT_H
#define CEPH_CLS_RGW_CLIENT_H

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls_rgw_types.h"

/* bucket index */
void cls_rgw_bucket_init(librados::ObjectWriteOperation& o);

void cls_rgw_bucket_set_tag_timeout(librados::ObjectWriteOperation& o, uint64_t tag_timeout);

void cls_rgw_bucket_prepare_op(librados::ObjectWriteOperation& o, uint8_t op, string& tag,
                               string& name, string& locator);

void cls_rgw_bucket_complete_op(librados::ObjectWriteOperation& o, uint8_t op, string& tag,
                                uint64_t epoch, string& name, rgw_bucket_dir_entry_meta& dir_meta,
				list<string> *remove_objs);

int cls_rgw_list_op(librados::IoCtx& io_ctx, string& oid, string& start_obj,
                    string& filter_prefix, uint32_t num_entries,
                    rgw_bucket_dir *dir, bool *is_truncated);

int cls_rgw_bucket_check_index_op(librados::IoCtx& io_ctx, string& oid,
				  rgw_bucket_dir_header *existing_header,
				  rgw_bucket_dir_header *calculated_header);
int cls_rgw_bucket_rebuild_index_op(librados::IoCtx& io_ctx, string& oid);
  
int cls_rgw_get_dir_header(librados::IoCtx& io_ctx, string& oid, rgw_bucket_dir_header *header);

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent, bufferlist& updates);

void cls_rgw_suggest_changes(librados::ObjectWriteOperation& o, bufferlist& updates);

/* usage logging */
int cls_rgw_usage_log_read(librados::IoCtx& io_ctx, string& oid, string& user,
                           uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                           string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage,
                           bool *is_truncated);

void cls_rgw_usage_log_trim(librados::ObjectWriteOperation& op, string& user,
                           uint64_t start_epoch, uint64_t end_epoch);

void cls_rgw_usage_log_add(librados::ObjectWriteOperation& op, rgw_usage_log_info& info);

/* garbage collection */
void cls_rgw_gc_set_entry(librados::ObjectWriteOperation& op, uint32_t expiration_secs, cls_rgw_gc_obj_info& info);
void cls_rgw_gc_defer_entry(librados::ObjectWriteOperation& op, uint32_t expiration_secs, const string& tag);

int cls_rgw_gc_list(librados::IoCtx& io_ctx, string& oid, string& marker, uint32_t max,
                    list<cls_rgw_gc_obj_info>& entries, bool *truncated);

void cls_rgw_gc_remove(librados::ObjectWriteOperation& op, const list<string>& tags);

#endif
