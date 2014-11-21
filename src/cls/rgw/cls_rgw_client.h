#ifndef CEPH_CLS_RGW_CLIENT_H
#define CEPH_CLS_RGW_CLIENT_H

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls_rgw_types.h"
#include "common/RefCountedObj.h"

class RGWGetDirHeader_CB : public RefCountedObject {
public:
  virtual ~RGWGetDirHeader_CB() {}
  virtual void handle_response(int r, rgw_bucket_dir_header& header) = 0;
};

/* bucket index */
void cls_rgw_bucket_init(librados::ObjectWriteOperation& o);

void cls_rgw_bucket_set_tag_timeout(librados::ObjectWriteOperation& o, uint64_t tag_timeout);

void cls_rgw_bucket_prepare_op(librados::ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                               const cls_rgw_obj_key& key, const string& locator, bool log_op);

void cls_rgw_bucket_complete_op(librados::ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                                rgw_bucket_entry_ver& ver,
                                const cls_rgw_obj_key& key,
                                rgw_bucket_dir_entry_meta& dir_meta,
				list<cls_rgw_obj_key> *remove_objs, bool log_op);

int cls_rgw_list_op(librados::IoCtx& io_ctx, const string& oid,
                    const cls_rgw_obj_key& start_obj,
                    const string& filter_prefix, uint32_t num_entries, bool list_versions,
                    rgw_bucket_dir *dir, bool *is_truncated);

void cls_rgw_remove_obj(librados::ObjectWriteOperation& o, list<string>& keep_attr_prefixes);

int cls_rgw_bi_get(librados::IoCtx& io_ctx, const string oid,
                   BIIndexType index_type, cls_rgw_obj_key& key,
                   rgw_cls_bi_entry *entry);
int cls_rgw_bi_put(librados::IoCtx& io_ctx, const string oid, rgw_cls_bi_entry& entry);
int cls_rgw_bi_list(librados::IoCtx& io_ctx, const string oid,
                   const string& name, const string& marker, uint32_t max,
                   list<rgw_cls_bi_entry> *entries, bool *is_truncated);


int cls_rgw_bucket_link_olh(librados::IoCtx& io_ctx, const string& oid, const cls_rgw_obj_key& key, bufferlist& olh_tag,
                            bool delete_marker, const string& op_tag, struct rgw_bucket_dir_entry_meta *meta,
                            uint64_t olh_epoch);
int cls_rgw_bucket_unlink_instance(librados::IoCtx& io_ctx, const string& oid, const cls_rgw_obj_key& key, const string& op_tag,
                                   uint64_t olh_epoch);
int cls_rgw_get_olh_log(librados::IoCtx& io_ctx, string& oid, librados::ObjectReadOperation& op, const cls_rgw_obj_key& olh, uint64_t ver_marker,
                        const string& olh_tag,
                        map<uint64_t, vector<struct rgw_bucket_olh_log_entry> > *log, bool *is_truncated);
void cls_rgw_trim_olh_log(librados::ObjectWriteOperation& op, string& oid, const cls_rgw_obj_key& olh, uint64_t ver, const string& olh_tag);

int cls_rgw_bucket_check_index_op(librados::IoCtx& io_ctx, string& oid,
				  rgw_bucket_dir_header *existing_header,
				  rgw_bucket_dir_header *calculated_header);
int cls_rgw_bucket_rebuild_index_op(librados::IoCtx& io_ctx, string& oid);
  
int cls_rgw_get_dir_header(librados::IoCtx& io_ctx, string& oid, rgw_bucket_dir_header *header);
int cls_rgw_get_dir_header_async(librados::IoCtx& io_ctx, string& oid, RGWGetDirHeader_CB *ctx);

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent, bufferlist& updates);

void cls_rgw_suggest_changes(librados::ObjectWriteOperation& o, bufferlist& updates);

/* bucket index log */

int cls_rgw_bi_log_list(librados::IoCtx& io_ctx, string& oid, string& marker, uint32_t max,
                    list<rgw_bi_log_entry>& entries, bool *truncated);
int cls_rgw_bi_log_trim(librados::IoCtx& io_ctx, string& oid, string& start_marker, string& end_marker);

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

int cls_rgw_gc_list(librados::IoCtx& io_ctx, string& oid, string& marker, uint32_t max, bool expired_only,
                    list<cls_rgw_gc_obj_info>& entries, bool *truncated);

void cls_rgw_gc_remove(librados::ObjectWriteOperation& op, const list<string>& tags);

#endif
