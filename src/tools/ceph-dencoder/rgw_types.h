#ifdef WITH_RADOSGW

#include "rgw/rgw_rados.h"
TYPE(RGWOLHInfo)
TYPE(RGWObjManifestPart)
TYPE(RGWObjManifest)
TYPE(objexp_hint_entry)

#include "rgw/rgw_zone.h"
TYPE(RGWZoneParams)
TYPE(RGWZone)
TYPE(RGWZoneGroup)
TYPE(RGWRealm)
TYPE(RGWPeriod)
TYPE(RGWPeriodLatestEpochInfo)

#include "rgw/rgw_acl.h"
TYPE(ACLPermission)
TYPE(ACLGranteeType)
TYPE(ACLGrant)
TYPE(RGWAccessControlList)
TYPE(ACLOwner)
TYPE(RGWAccessControlPolicy)

#include "rgw/rgw_cache.h"
TYPE(ObjectMetaInfo)
TYPE(ObjectCacheInfo)
TYPE(RGWCacheNotifyInfo)

#include "rgw/rgw_lc.h"
TYPE(RGWLifecycleConfiguration)

#include "cls/rgw/cls_rgw_types.h"
TYPE(rgw_bucket_pending_info)
TYPE(rgw_bucket_dir_entry_meta)
TYPE(rgw_bucket_entry_ver)
TYPE(rgw_bucket_dir_entry)
TYPE(rgw_bucket_category_stats)
TYPE(rgw_bucket_dir_header)
TYPE(rgw_bucket_dir)
TYPE(rgw_bucket_entry_ver)
TYPE(cls_rgw_obj_key)
TYPE(rgw_bucket_olh_log_entry)
TYPE(rgw_usage_log_entry)

#include "cls/rgw/cls_rgw_ops.h"
TYPE(rgw_cls_obj_prepare_op)
TYPE(rgw_cls_obj_complete_op)
TYPE(rgw_cls_list_op)
TYPE(rgw_cls_list_ret)
TYPE(cls_rgw_gc_defer_entry_op)
TYPE(cls_rgw_gc_list_op)
TYPE(cls_rgw_gc_list_ret)
TYPE(cls_rgw_gc_obj_info)
TYPE(cls_rgw_gc_remove_op)
TYPE(cls_rgw_gc_set_entry_op)
TYPE(cls_rgw_obj)
TYPE(cls_rgw_obj_chain)
TYPE(rgw_cls_tag_timeout_op)
TYPE(cls_rgw_bi_log_list_op)
TYPE(cls_rgw_bi_log_trim_op)
TYPE(cls_rgw_bi_log_list_ret)
TYPE(rgw_cls_link_olh_op)
TYPE(rgw_cls_unlink_instance_op)
TYPE(rgw_cls_read_olh_log_op)
TYPE(rgw_cls_read_olh_log_ret)
TYPE(rgw_cls_trim_olh_log_op)
TYPE(rgw_cls_bucket_clear_olh_op)
TYPE(rgw_cls_check_index_ret)
TYPE(cls_rgw_reshard_add_op)
TYPE(cls_rgw_reshard_list_op)
TYPE(cls_rgw_reshard_list_ret)
TYPE(cls_rgw_reshard_get_op)
TYPE(cls_rgw_reshard_get_ret)
TYPE(cls_rgw_reshard_remove_op)
TYPE(cls_rgw_set_bucket_resharding_op)
TYPE(cls_rgw_clear_bucket_resharding_op)
TYPE(cls_rgw_lc_obj_head)

#include "cls/rgw/cls_rgw_client.h"
TYPE(rgw_bi_log_entry)
TYPE(cls_rgw_reshard_entry)
TYPE(cls_rgw_bucket_instance_entry)

#include "cls/user/cls_user_types.h"
TYPE(cls_user_bucket)
TYPE(cls_user_bucket_entry)
TYPE(cls_user_stats)
TYPE(cls_user_header)

#include "cls/user/cls_user_ops.h"
TYPE(cls_user_set_buckets_op)
TYPE(cls_user_remove_bucket_op)
TYPE(cls_user_list_buckets_op)
TYPE(cls_user_list_buckets_ret)
TYPE(cls_user_get_header_op)
TYPE(cls_user_get_header_ret)
TYPE(cls_user_complete_stats_sync_op)

#include "cls/journal/cls_journal_types.h"
TYPE(cls::journal::ObjectPosition)
TYPE(cls::journal::ObjectSetPosition)
TYPE(cls::journal::Client)
TYPE(cls::journal::Tag)

#include "rgw/rgw_common.h"
TYPE(RGWAccessKey)
TYPE(RGWSubUser)
TYPE(RGWUserInfo)
TYPE(rgw_bucket)
TYPE(RGWBucketInfo)
TYPE(RGWBucketEnt)
TYPE(rgw_obj)

#include "rgw/rgw_log.h"
TYPE(rgw_log_entry)

#include "rgw/rgw_meta_sync_status.h"
TYPE(rgw_meta_sync_info)
TYPE(rgw_meta_sync_marker)
TYPE(rgw_meta_sync_status)

#include "rgw/rgw_multi.h"
TYPE(RGWUploadPartInfo)

#include "rgw/rgw_data_sync.h"
TYPE(rgw_data_sync_info)
TYPE(rgw_data_sync_marker)
TYPE(rgw_data_sync_status)

#include "rgw/rgw_bucket_encryption.h"
TYPE(RGWBucketEncryptionConfig)

#endif
