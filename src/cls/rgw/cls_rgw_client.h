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

/**
 * Init bucket index objects.
 *
 * io_ctx      - IO context for rados.
 * bucket_objs - a lit of bucket index objects.
 * max_io      - the maximum number of AIO (for throttling).
 *
 * Reutrn 0 on success, a failure code otherwise.
 */
int cls_rgw_bucket_index_init_op(librados::IoCtx &io_ctx,
        const vector<string>& bucket_objs, uint32_t max_io);

int cls_rgw_bucket_set_tag_timeout(librados::IoCtx& io_ctx, const vector<string>& bucket_objs, uint64_t tag_timeout,
        uint32_t max_io);

void cls_rgw_bucket_prepare_op(librados::ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                               string& name, string& locator, bool log_op);

void cls_rgw_bucket_complete_op(librados::ObjectWriteOperation& o, RGWModifyOp op, string& tag,
                                rgw_bucket_entry_ver& ver, string& name, rgw_bucket_dir_entry_meta& dir_meta,
				list<string> *remove_objs, bool log_op);

/**
 * List the bucket with the starting object and filter prefix.
 * NOTE: this method do listing requests for each bucket index shards identified by
 *       the keys of the *list_results* map, which means the map should be popludated
 *       by the caller to fill with each bucket index object id.
 *
 * io_ctx        - IO context for rados.
 * start_obj     - marker for the listing.
 * filter_prefix - filer prefix.
 * num_entries   - number of entries to request for each object (note the total 
 *                 amount of entries returned depends on the number of shardings).
 * list_results  - the list results keyed by bucket index object id.
 * max_aio       - the maximum number of AIO (for throttling).
 *
 * Return 0 on success, a failure code otherwise.
*/
int cls_rgw_list_op(librados::IoCtx& io_ctx, const string & start_obj,
                    const string& filter_prefix, uint32_t num_entries,
                    map<string, struct rgw_cls_list_ret>& list_results,
                    uint32_t max_aio);

/**
 * Check the bucket index.
 *
 * io_ctx  - IO context for rados.
 * results - the results keyed by the bubcket index object id (shard).
 * max_io  - the maximum number of AIO (for throttling).
 *
 * Return 0 on success, a failure code otherwise.
 */
int cls_rgw_bucket_check_index_op(librados::IoCtx& io_ctx,
        map<string, struct rgw_cls_check_index_ret>& results, uint32_t max_io);

/**
 * Rebuild the bucket index.
 *
 * io_ctx      - IO context for rados.
 * bucket_objs - a list of bucket objects (shard) to operate.
 * max_io      - the maximum number of AIO (for throttling).
 *
 * Return 0 on success, a failure code otherwise.
 */
int cls_rgw_bucket_rebuild_index_op(librados::IoCtx& io_ctx, const vector<string>& bucket_objs,
        uint32_t max_io);
  
/**
 * Remove objects from bucket index entry.
 *
 * io_ctx   - IO context for rados.
 * updates  - a list of updates to perform. Key is the bucket index object id (shard),
 *            value is a list of entries to remove.
 * max_io   - the maximum number of AIO (for throttling).
 *
 * Return 0 on success, a failure code otherwise.
 */ 
int cls_rgw_bucket_remove_objs_op(librados::IoCtx& io_ctx,
        const map<string, vector<rgw_bucket_dir_entry> >& updates, uint32_t max_io);

/**
 * Get dir headers with a bucket.
 *
 * io_ctx       - IO context for rados.
 * list_results - list results for headers, keyed by the object index object id (shard).
 * max_io       - maximum number of AIO (for throttling).
 *
 * Return 0 on success, a failure code otherwise.
 */
int cls_rgw_get_dir_header(librados::IoCtx& io_ctx,
        map<string, rgw_cls_list_ret>& headers, uint32_t max_io);

int cls_rgw_get_dir_header_async(librados::IoCtx& io_ctx, string& oid, RGWGetDirHeader_CB *ctx);

void cls_rgw_encode_suggestion(char op, rgw_bucket_dir_entry& dirent, bufferlist& updates);

void cls_rgw_suggest_changes(librados::ObjectWriteOperation& o, bufferlist& updates);

/* bucket index log */

int cls_rgw_bi_log_list(librados::IoCtx& io_ctx, map<string, struct cls_rgw_bi_log_list_ret>& list_results,
        string& marker, uint32_t max, uint32_t max_io);
int cls_rgw_bi_log_trim(librados::IoCtx& io_ctx, vector<string>& bucket_objs, string& start_marker, string& end_marker, uint32_t max_io);

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

/**
 * This helper method iterates the given AIO completion list and wait for completion
 * of all of them.
 *
 * completions - a list of AIO completions.
 *
 * Return 0 on success, otherwise a failure code.
 */
int cls_rgw_util_wait_for_complete(const vector<librados::AioCompletion*>& completions);

/**
 * This helper method iterates the given AIO completion list and wait for the completion
 * and callbacks of all of them.
 *
 * completions - a list of AIO completions.
 *
 * Return 0 on success, a failure code otherwise.
 */
int cls_rgw_util_wait_for_complete_and_cb(const vector<librados::AioCompletion*>& completions);

/**
 * Parse markers (key as object id, value as marker for the object) from the given marker string.
 *
 * marker   - the original marker string, with schema: {oid}#{marker},{oid}#{marker}...
 * markers  - the output markers map with oid as key and marker as value.
 *
 * Return 0 on succes, a failure code otherwise.
 */
int cls_rgw_util_parse_markers(const string& marker, map<string, string>& markers);

#endif
