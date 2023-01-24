#ifdef WITH_RADOSGW

#include "rgw_rados.h"
TYPE(RGWOLHInfo)
TYPE(RGWObjManifestPart)
TYPE(RGWObjManifest)
TYPE(objexp_hint_entry)
TYPE(RGWOLHPendingInfo)

#include "rgw_zone.h"
TYPE(RGWZoneParams)
TYPE(RGWZone)
TYPE(RGWZoneGroup)
TYPE(RGWRealm)
TYPE(RGWPeriod)
TYPE(RGWPeriodLatestEpochInfo)
TYPE(RGWNameToId)

#include "rgw_acl.h"
TYPE(ACLPermission)
TYPE(ACLGranteeType)
TYPE(ACLGrant)
TYPE(RGWAccessControlList)
TYPE(ACLOwner)
TYPE(RGWAccessControlPolicy)

#include "rgw_cache.h"
TYPE(ObjectMetaInfo)
TYPE(ObjectCacheInfo)
TYPE(RGWCacheNotifyInfo)

#include "rgw_lc.h"
TYPE(RGWLifecycleConfiguration)

#include "cls/log/cls_log_types.h"
TYPE(cls::log::entry)

#include "cls/log/cls_log_ops.h"
TYPE(cls::log::ops::add_op)

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
TYPE(rgw_cls_bi_entry)
TYPE(rgw_bucket_olh_entry)
TYPE(rgw_usage_data)
TYPE(rgw_s3select_usage_data)
TYPE(rgw_usage_log_info)
TYPE(rgw_user_bucket)
TYPE(cls_rgw_lc_entry)
TYPE(rgw_zone_set)

#include "cls/rgw/cls_rgw_ops.h"
TYPE(cls_rgw_lc_get_entry_ret)
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
TYPE(rgw_cls_bi_get_op)
TYPE(rgw_cls_bi_get_ret)
TYPE(rgw_cls_bi_list_op)
TYPE(rgw_cls_bi_list_ret)
TYPE(rgw_cls_bi_put_op)
TYPE(rgw_cls_obj_check_attrs_prefix)
TYPE(rgw_cls_obj_remove_op)
TYPE(rgw_cls_obj_store_pg_ver_op)
TYPE(rgw_cls_usage_log_add_op)
TYPE(rgw_cls_usage_log_read_op)
TYPE(rgw_cls_usage_log_read_ret)
TYPE(rgw_cls_usage_log_trim_op)
TYPE(cls_rgw_guard_bucket_resharding_op)
TYPE(cls_rgw_lc_set_entry_op)

#include "cls/rgw_gc/cls_rgw_gc_ops.h"
TYPE(cls_rgw_gc_queue_init_op)
#include "cls/rgw_gc/cls_rgw_gc_types.h"
TYPE(cls_rgw_gc_urgent_data)




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
using namespace cls::journal;
TYPE(ObjectSetPosition)
TYPE(ObjectPosition)
TYPE(Tag)
TYPE(Client)

#include "cls/version/cls_version_types.h"
TYPE(obj_version)

#include "rgw_common.h"
TYPE(RGWAccessKey)
TYPE(RGWSubUser)
TYPE(RGWUserInfo)
TYPE(rgw_bucket)
TYPE(RGWBucketInfo)
TYPE(RGWBucketEnt)
TYPE(rgw_obj)
TYPE(RGWBucketEntryPoint)
TYPE(multipart_upload_info)


#include "rgw_log.h"
TYPE(rgw_log_entry)

#include "rgw_datalog.h"
TYPE(rgw_data_change)

#include "rgw_mdlog.h"
TYPE(RGWMetadataLogData)

#include "rgw_meta_sync_status.h"
TYPE(rgw_meta_sync_info)
TYPE(rgw_meta_sync_marker)
TYPE(rgw_meta_sync_status)

#include "rgw_multi.h"
TYPE(RGWUploadPartInfo)

#include "rgw_data_sync.h"
TYPE(rgw_data_sync_info)
TYPE(rgw_data_sync_marker)
TYPE(rgw_data_sync_status)

#include "rgw_bucket_encryption.h"
TYPE(RGWBucketEncryptionConfig)

#include "rgw_compression_types.h"
TYPE(RGWCompressionInfo)

#include "rgw_cors.h"
TYPE(RGWCORSRule)

#include "rgw_object_lock.h"
TYPE(RGWObjectLock)
TYPE(ObjectLockRule)


#include "rgw_obj_types.h"
TYPE(rgw_obj_index_key)
TYPE(rgw_raw_obj)

#include "rgw_obj_manifest.h"
TYPE(RGWObjManifestRule)
TYPE(RGWObjTier)

#include "rgw_tag.h"
TYPE(RGWObjTags)

#include "rgw_placement_types.h"
TYPE(rgw_placement_rule)

#include "rgw_pool_types.h"
TYPE(rgw_pool)

#include "rgw_quota_types.h"
TYPE(RGWQuotaInfo)


#include "rgw_sync_policy.h"
TYPE(rgw_sync_policy_info)

#include "rgw_basic_types.h"
TYPE(rgw_zone_id)
TYPE(RGWZoneStorageClass)
TYPE(RGWZoneStorageClasses)
TYPE(RGWZonePlacementInfo)
TYPE(RGWZoneGroupPlacementTarget)
TYPE(RGWUserCaps)


#include "rgw_user.h"
TYPE(RGWUID)

#include "rgw_user_types.h"
TYPE(rgw_user)

#endif
