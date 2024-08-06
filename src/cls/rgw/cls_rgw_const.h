// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#define RGW_CLASS "rgw"

/* Special error code returned by cls bucket list operation if it was
 * unable to skip past enough not visible entries to return any
 * entries in the call. */
constexpr int RGWBIAdvanceAndRetryError = -EFBIG;

/* bucket index */
#define RGW_BUCKET_INIT_INDEX "bucket_init_index"


#define RGW_BUCKET_SET_TAG_TIMEOUT "bucket_set_tag_timeout"
#define RGW_BUCKET_LIST "bucket_list"
#define RGW_BUCKET_CHECK_INDEX "bucket_check_index"
#define RGW_BUCKET_REBUILD_INDEX "bucket_rebuild_index"
#define RGW_BUCKET_UPDATE_STATS "bucket_update_stats"
#define RGW_BUCKET_PREPARE_OP "bucket_prepare_op"
#define RGW_BUCKET_COMPLETE_OP "bucket_complete_op"
#define RGW_BUCKET_LINK_OLH "bucket_link_olh"
#define RGW_BUCKET_UNLINK_INSTANCE "bucket_unlink_instance"
#define RGW_BUCKET_READ_OLH_LOG "bucket_read_olh_log"
#define RGW_BUCKET_TRIM_OLH_LOG "bucket_trim_olh_log"
#define RGW_BUCKET_CLEAR_OLH "bucket_clear_olh"

#define RGW_OBJ_REMOVE "obj_remove"
#define RGW_OBJ_STORE_PG_VER "obj_store_pg_ver"
#define RGW_OBJ_CHECK_ATTRS_PREFIX "obj_check_attrs_prefix"
#define RGW_OBJ_CHECK_MTIME "obj_check_mtime"

#define RGW_BI_GET "bi_get"
#define RGW_BI_PUT "bi_put"
#define RGW_BI_LIST "bi_list"

#define RGW_BI_LOG_LIST "bi_log_list"
#define RGW_BI_LOG_TRIM "bi_log_trim"
#define RGW_DIR_SUGGEST_CHANGES "dir_suggest_changes"

#define RGW_BI_LOG_RESYNC "bi_log_resync"
#define RGW_BI_LOG_STOP "bi_log_stop"

/* usage logging */
#define RGW_USER_USAGE_LOG_ADD "user_usage_log_add"
#define RGW_USER_USAGE_LOG_READ "user_usage_log_read"
#define RGW_USER_USAGE_LOG_TRIM "user_usage_log_trim"
#define RGW_USAGE_LOG_CLEAR "usage_log_clear"

/* garbage collection */
#define RGW_GC_SET_ENTRY "gc_set_entry"
#define RGW_GC_DEFER_ENTRY "gc_defer_entry"
#define RGW_GC_LIST "gc_list"
#define RGW_GC_REMOVE "gc_remove"

/* lifecycle bucket list */
#define RGW_LC_GET_ENTRY "lc_get_entry"
#define RGW_LC_SET_ENTRY "lc_set_entry"
#define RGW_LC_RM_ENTRY "lc_rm_entry"
#define RGW_LC_GET_NEXT_ENTRY "lc_get_next_entry"
#define RGW_LC_PUT_HEAD "lc_put_head"
#define RGW_LC_GET_HEAD "lc_get_head"
#define RGW_LC_LIST_ENTRIES "lc_list_entries"

/* multipart */
#define RGW_MP_UPLOAD_PART_INFO_UPDATE "mp_upload_part_info_update"

/* resharding */
#define RGW_RESHARD_ADD "reshard_add"
#define RGW_RESHARD_LIST "reshard_list"
#define RGW_RESHARD_GET "reshard_get"
#define RGW_RESHARD_REMOVE "reshard_remove"

/* resharding attribute  */
#define RGW_SET_BUCKET_RESHARDING "set_bucket_resharding"
#define RGW_CLEAR_BUCKET_RESHARDING "clear_bucket_resharding"
#define RGW_GUARD_BUCKET_RESHARDING "guard_bucket_resharding"
#define RGW_GET_BUCKET_RESHARDING "get_bucket_resharding"
