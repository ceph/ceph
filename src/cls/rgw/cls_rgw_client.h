#ifndef CEPH_CLS_RGW_CLIENT_H
#define CEPH_CLS_RGW_CLIENT_H

#include "include/types.h"

/* bucket index */
void cls_rgw_bucket_prepare_op(librados::ObjectWriteOperation& o, uint8_t op, string& tag,
                               string& name, string& locator);

void cls_rgw_bucket_complete_op(librados::ObjectWriteOperation& o, uint8_t op, string& tag,
                                uint64_t epoch, string& name, rgw_bucket_dir_entry_meta& dir_meta);

int cls_rgw_list_op(librados::IoCtx& io_ctx, string& oid, string& start_obj,
                    string& filter_prefix, uint32_t num_entries,
                    rgw_bucket_dir *dir, bool *is_truncated);

int cls_rgw_get_dir_header(librados::IoCtx& io_ctx, string& oid, rgw_bucket_dir_header *header);

/* usage logging */
int cls_rgw_usage_log_read(librados::IoCtx& io_ctx, string& oid, string& user,
                           uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                           string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage,
                           bool *is_truncated);

void cls_rgw_usage_log_trim(librados::ObjectWriteOperation& op, string& user,
                           uint64_t start_epoch, uint64_t end_epoch);

void cls_rgw_usage_log_add(librados::ObjectWriteOperation& op, rgw_usage_log_info& info);

#endif
