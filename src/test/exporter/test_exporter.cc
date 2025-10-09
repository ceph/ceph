#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/config_proxy.h"
#include "common/admin_socket.h"
#include "common/admin_socket_client.h"
#include <gmock/gmock.h>
#include "gtest/gtest.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "exporter/util.h"
#include "exporter/DaemonMetricCollector.h"
#include <filesystem>

#include <regex>
#include <string>
#include <vector>
#include <utility>

typedef std::map<std::string, std::string> labels_t;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::Pointee;
using ::testing::Matcher;
using ::testing::_;
using ::testing::SetArgReferee;
using ::testing::Invoke;
using ::testing::WithArgs;
using ::testing::AtLeast;

// 17.2.6's memento mori:
// This data was gathered from the python implementation of the promethize method
// where we transform the path of a counter to a valid prometheus name.
static std::vector<std::pair<std::string, std::string>> promethize_data = {
  {"bluefs.alloc_slow_fallback", "ceph_bluefs_alloc_slow_fallback"},
  {"bluefs.alloc_slow_size_fallback", "ceph_bluefs_alloc_slow_size_fallback"},
  {"bluefs.alloc_unit_db", "ceph_bluefs_alloc_unit_db"},
  {"bluefs.alloc_unit_main", "ceph_bluefs_alloc_unit_main"},
  {"bluefs.alloc_unit_wal", "ceph_bluefs_alloc_unit_wal"},
  {"bluefs.bytes_written_slow", "ceph_bluefs_bytes_written_slow"},
  {"bluefs.bytes_written_sst", "ceph_bluefs_bytes_written_sst"},
  {"bluefs.bytes_written_wal", "ceph_bluefs_bytes_written_wal"},
  {"bluefs.compact_lat_count", "ceph_bluefs_compact_lat_count"},
  {"bluefs.compact_lat_sum", "ceph_bluefs_compact_lat_sum"},
  {"bluefs.compact_lock_lat_count", "ceph_bluefs_compact_lock_lat_count"},
  {"bluefs.compact_lock_lat_sum", "ceph_bluefs_compact_lock_lat_sum"},
  {"bluefs.db_total_bytes", "ceph_bluefs_db_total_bytes"},
  {"bluefs.db_used_bytes", "ceph_bluefs_db_used_bytes"},
  {"bluefs.log_bytes", "ceph_bluefs_log_bytes"},
  {"bluefs.logged_bytes", "ceph_bluefs_logged_bytes"},
  {"bluefs.max_bytes_db", "ceph_bluefs_max_bytes_db"},
  {"bluefs.max_bytes_slow", "ceph_bluefs_max_bytes_slow"},
  {"bluefs.max_bytes_wal", "ceph_bluefs_max_bytes_wal"},
  {"bluefs.num_files", "ceph_bluefs_num_files"},
  {"bluefs.read_bytes", "ceph_bluefs_read_bytes"},
  {"bluefs.read_count", "ceph_bluefs_read_count"},
  {"bluefs.read_disk_bytes", "ceph_bluefs_read_disk_bytes"},
  {"bluefs.read_disk_bytes_db", "ceph_bluefs_read_disk_bytes_db"},
  {"bluefs.read_disk_bytes_slow", "ceph_bluefs_read_disk_bytes_slow"},
  {"bluefs.read_disk_bytes_wal", "ceph_bluefs_read_disk_bytes_wal"},
  {"bluefs.read_disk_count", "ceph_bluefs_read_disk_count"},
  {"bluefs.read_prefetch_bytes", "ceph_bluefs_read_prefetch_bytes"},
  {"bluefs.read_prefetch_count", "ceph_bluefs_read_prefetch_count"},
  {"bluefs.read_random_buffer_bytes", "ceph_bluefs_read_random_buffer_bytes"},
  {"bluefs.read_random_buffer_count", "ceph_bluefs_read_random_buffer_count"},
  {"bluefs.read_random_bytes", "ceph_bluefs_read_random_bytes"},
  {"bluefs.read_random_count", "ceph_bluefs_read_random_count"},
  {"bluefs.read_random_disk_bytes", "ceph_bluefs_read_random_disk_bytes"},
  {"bluefs.read_random_disk_bytes_db", "ceph_bluefs_read_random_disk_bytes_db"},
  {"bluefs.read_random_disk_bytes_slow", "ceph_bluefs_read_random_disk_bytes_slow"},
  {"bluefs.read_random_disk_bytes_wal", "ceph_bluefs_read_random_disk_bytes_wal"},
  {"bluefs.read_random_disk_count", "ceph_bluefs_read_random_disk_count"},
  {"bluefs.slow_total_bytes", "ceph_bluefs_slow_total_bytes"},
  {"bluefs.slow_used_bytes", "ceph_bluefs_slow_used_bytes"},
  {"bluefs.wal_total_bytes", "ceph_bluefs_wal_total_bytes"},
  {"bluefs.wal_used_bytes", "ceph_bluefs_wal_used_bytes"},
  {"bluestore-pricache.cache_bytes", "ceph_bluestore_pricache_cache_bytes"},
  {"bluestore-pricache.heap_bytes", "ceph_bluestore_pricache_heap_bytes"},
  {"bluestore-pricache.mapped_bytes", "ceph_bluestore_pricache_mapped_bytes"},
  {"bluestore-pricache.target_bytes", "ceph_bluestore_pricache_target_bytes"},
  {"bluestore-pricache.unmapped_bytes", "ceph_bluestore_pricache_unmapped_bytes"},
  {"bluestore-pricache:data.committed_bytes", "ceph_bluestore_pricache:data_committed_bytes"},
  {"bluestore-pricache:data.pri0_bytes", "ceph_bluestore_pricache:data_pri0_bytes"},
  {"bluestore-pricache:data.pri10_bytes", "ceph_bluestore_pricache:data_pri10_bytes"},
  {"bluestore-pricache:data.pri11_bytes", "ceph_bluestore_pricache:data_pri11_bytes"},
  {"bluestore-pricache:data.pri1_bytes", "ceph_bluestore_pricache:data_pri1_bytes"},
  {"bluestore-pricache:data.pri2_bytes", "ceph_bluestore_pricache:data_pri2_bytes"},
  {"bluestore-pricache:data.pri3_bytes", "ceph_bluestore_pricache:data_pri3_bytes"},
  {"bluestore-pricache:data.pri4_bytes", "ceph_bluestore_pricache:data_pri4_bytes"},
  {"bluestore-pricache:data.pri5_bytes", "ceph_bluestore_pricache:data_pri5_bytes"},
  {"bluestore-pricache:data.pri6_bytes", "ceph_bluestore_pricache:data_pri6_bytes"},
  {"bluestore-pricache:data.pri7_bytes", "ceph_bluestore_pricache:data_pri7_bytes"},
  {"bluestore-pricache:data.pri8_bytes", "ceph_bluestore_pricache:data_pri8_bytes"},
  {"bluestore-pricache:data.pri9_bytes", "ceph_bluestore_pricache:data_pri9_bytes"},
  {"bluestore-pricache:data.reserved_bytes", "ceph_bluestore_pricache:data_reserved_bytes"},
  {"bluestore-pricache:kv.committed_bytes", "ceph_bluestore_pricache:kv_committed_bytes"},
  {"bluestore-pricache:kv.pri0_bytes", "ceph_bluestore_pricache:kv_pri0_bytes"},
  {"bluestore-pricache:kv.pri10_bytes", "ceph_bluestore_pricache:kv_pri10_bytes"},
  {"bluestore-pricache:kv.pri11_bytes", "ceph_bluestore_pricache:kv_pri11_bytes"},
  {"bluestore-pricache:kv.pri1_bytes", "ceph_bluestore_pricache:kv_pri1_bytes"},
  {"bluestore-pricache:kv.pri2_bytes", "ceph_bluestore_pricache:kv_pri2_bytes"},
  {"bluestore-pricache:kv.pri3_bytes", "ceph_bluestore_pricache:kv_pri3_bytes"},
  {"bluestore-pricache:kv.pri4_bytes", "ceph_bluestore_pricache:kv_pri4_bytes"},
  {"bluestore-pricache:kv.pri5_bytes", "ceph_bluestore_pricache:kv_pri5_bytes"},
  {"bluestore-pricache:kv.pri6_bytes", "ceph_bluestore_pricache:kv_pri6_bytes"},
  {"bluestore-pricache:kv.pri7_bytes", "ceph_bluestore_pricache:kv_pri7_bytes"},
  {"bluestore-pricache:kv.pri8_bytes", "ceph_bluestore_pricache:kv_pri8_bytes"},
  {"bluestore-pricache:kv.pri9_bytes", "ceph_bluestore_pricache:kv_pri9_bytes"},
  {"bluestore-pricache:kv.reserved_bytes", "ceph_bluestore_pricache:kv_reserved_bytes"},
  {"bluestore-pricache:kv_onode.committed_bytes", "ceph_bluestore_pricache:kv_onode_committed_bytes"},
  {"bluestore-pricache:kv_onode.pri0_bytes", "ceph_bluestore_pricache:kv_onode_pri0_bytes"},
  {"bluestore-pricache:kv_onode.pri10_bytes", "ceph_bluestore_pricache:kv_onode_pri10_bytes"},
  {"bluestore-pricache:kv_onode.pri11_bytes", "ceph_bluestore_pricache:kv_onode_pri11_bytes"},
  {"bluestore-pricache:kv_onode.pri1_bytes", "ceph_bluestore_pricache:kv_onode_pri1_bytes"},
  {"bluestore-pricache:kv_onode.pri2_bytes", "ceph_bluestore_pricache:kv_onode_pri2_bytes"},
  {"bluestore-pricache:kv_onode.pri3_bytes", "ceph_bluestore_pricache:kv_onode_pri3_bytes"},
  {"bluestore-pricache:kv_onode.pri4_bytes", "ceph_bluestore_pricache:kv_onode_pri4_bytes"},
  {"bluestore-pricache:kv_onode.pri5_bytes", "ceph_bluestore_pricache:kv_onode_pri5_bytes"},
  {"bluestore-pricache:kv_onode.pri6_bytes", "ceph_bluestore_pricache:kv_onode_pri6_bytes"},
  {"bluestore-pricache:kv_onode.pri7_bytes", "ceph_bluestore_pricache:kv_onode_pri7_bytes"},
  {"bluestore-pricache:kv_onode.pri8_bytes", "ceph_bluestore_pricache:kv_onode_pri8_bytes"},
  {"bluestore-pricache:kv_onode.pri9_bytes", "ceph_bluestore_pricache:kv_onode_pri9_bytes"},
  {"bluestore-pricache:kv_onode.reserved_bytes", "ceph_bluestore_pricache:kv_onode_reserved_bytes"},
  {"bluestore-pricache:meta.committed_bytes", "ceph_bluestore_pricache:meta_committed_bytes"},
  {"bluestore-pricache:meta.pri0_bytes", "ceph_bluestore_pricache:meta_pri0_bytes"},
  {"bluestore-pricache:meta.pri10_bytes", "ceph_bluestore_pricache:meta_pri10_bytes"},
  {"bluestore-pricache:meta.pri11_bytes", "ceph_bluestore_pricache:meta_pri11_bytes"},
  {"bluestore-pricache:meta.pri1_bytes", "ceph_bluestore_pricache:meta_pri1_bytes"},
  {"bluestore-pricache:meta.pri2_bytes", "ceph_bluestore_pricache:meta_pri2_bytes"},
  {"bluestore-pricache:meta.pri3_bytes", "ceph_bluestore_pricache:meta_pri3_bytes"},
  {"bluestore-pricache:meta.pri4_bytes", "ceph_bluestore_pricache:meta_pri4_bytes"},
  {"bluestore-pricache:meta.pri5_bytes", "ceph_bluestore_pricache:meta_pri5_bytes"},
  {"bluestore-pricache:meta.pri6_bytes", "ceph_bluestore_pricache:meta_pri6_bytes"},
  {"bluestore-pricache:meta.pri7_bytes", "ceph_bluestore_pricache:meta_pri7_bytes"},
  {"bluestore-pricache:meta.pri8_bytes", "ceph_bluestore_pricache:meta_pri8_bytes"},
  {"bluestore-pricache:meta.pri9_bytes", "ceph_bluestore_pricache:meta_pri9_bytes"},
  {"bluestore-pricache:meta.reserved_bytes", "ceph_bluestore_pricache:meta_reserved_bytes"},
  {"bluestore.alloc_unit", "ceph_bluestore_alloc_unit"},
  {"bluestore.allocated", "ceph_bluestore_allocated"},
  {"bluestore.clist_lat_count", "ceph_bluestore_clist_lat_count"},
  {"bluestore.clist_lat_sum", "ceph_bluestore_clist_lat_sum"},
  {"bluestore.compress_lat_count", "ceph_bluestore_compress_lat_count"},
  {"bluestore.compress_lat_sum", "ceph_bluestore_compress_lat_sum"},
  {"bluestore.compressed", "ceph_bluestore_compressed"},
  {"bluestore.compressed_allocated", "ceph_bluestore_compressed_allocated"},
  {"bluestore.compressed_original", "ceph_bluestore_compressed_original"},
  {"bluestore.csum_lat_count", "ceph_bluestore_csum_lat_count"},
  {"bluestore.csum_lat_sum", "ceph_bluestore_csum_lat_sum"},
  {"bluestore.decompress_lat_count", "ceph_bluestore_decompress_lat_count"},
  {"bluestore.decompress_lat_sum", "ceph_bluestore_decompress_lat_sum"},
  {"bluestore.kv_commit_lat_count", "ceph_bluestore_kv_commit_lat_count"},
  {"bluestore.kv_commit_lat_sum", "ceph_bluestore_kv_commit_lat_sum"},
  {"bluestore.kv_final_lat_count", "ceph_bluestore_kv_final_lat_count"},
  {"bluestore.kv_final_lat_sum", "ceph_bluestore_kv_final_lat_sum"},
  {"bluestore.kv_flush_lat_count", "ceph_bluestore_kv_flush_lat_count"},
  {"bluestore.kv_flush_lat_sum", "ceph_bluestore_kv_flush_lat_sum"},
  {"bluestore.kv_sync_lat_count", "ceph_bluestore_kv_sync_lat_count"},
  {"bluestore.kv_sync_lat_sum", "ceph_bluestore_kv_sync_lat_sum"},
  {"bluestore.omap_get_keys_lat_count", "ceph_bluestore_omap_get_keys_lat_count"},
  {"bluestore.omap_get_keys_lat_sum", "ceph_bluestore_omap_get_keys_lat_sum"},
  {"bluestore.omap_get_values_lat_count", "ceph_bluestore_omap_get_values_lat_count"},
  {"bluestore.omap_get_values_lat_sum", "ceph_bluestore_omap_get_values_lat_sum"},
  {"bluestore.omap_lower_bound_lat_count", "ceph_bluestore_omap_lower_bound_lat_count"},
  {"bluestore.omap_lower_bound_lat_sum", "ceph_bluestore_omap_lower_bound_lat_sum"},
  {"bluestore.omap_next_lat_count", "ceph_bluestore_omap_next_lat_count"},
  {"bluestore.omap_next_lat_sum", "ceph_bluestore_omap_next_lat_sum"},
  {"bluestore.omap_seek_to_first_lat_count", "ceph_bluestore_omap_seek_to_first_lat_count"},
  {"bluestore.omap_seek_to_first_lat_sum", "ceph_bluestore_omap_seek_to_first_lat_sum"},
  {"bluestore.omap_upper_bound_lat_count", "ceph_bluestore_omap_upper_bound_lat_count"},
  {"bluestore.omap_upper_bound_lat_sum", "ceph_bluestore_omap_upper_bound_lat_sum"},
  {"bluestore.onode_hits", "ceph_bluestore_onode_hits"},
  {"bluestore.onode_misses", "ceph_bluestore_onode_misses"},
  {"bluestore.read_lat_count", "ceph_bluestore_read_lat_count"},
  {"bluestore.read_lat_sum", "ceph_bluestore_read_lat_sum"},
  {"bluestore.read_onode_meta_lat_count", "ceph_bluestore_read_onode_meta_lat_count"},
  {"bluestore.read_onode_meta_lat_sum", "ceph_bluestore_read_onode_meta_lat_sum"},
  {"bluestore.read_wait_aio_lat_count", "ceph_bluestore_read_wait_aio_lat_count"},
  {"bluestore.read_wait_aio_lat_sum", "ceph_bluestore_read_wait_aio_lat_sum"},
  {"bluestore.reads_with_retries", "ceph_bluestore_reads_with_retries"},
  {"bluestore.remove_lat_count", "ceph_bluestore_remove_lat_count"},
  {"bluestore.remove_lat_sum", "ceph_bluestore_remove_lat_sum"},
  {"bluestore.state_aio_wait_lat_count", "ceph_bluestore_state_aio_wait_lat_count"},
  {"bluestore.state_aio_wait_lat_sum", "ceph_bluestore_state_aio_wait_lat_sum"},
  {"bluestore.state_deferred_aio_wait_lat_count", "ceph_bluestore_state_deferred_aio_wait_lat_count"},
  {"bluestore.state_deferred_aio_wait_lat_sum", "ceph_bluestore_state_deferred_aio_wait_lat_sum"},
  {"bluestore.state_deferred_cleanup_lat_count", "ceph_bluestore_state_deferred_cleanup_lat_count"},
  {"bluestore.state_deferred_cleanup_lat_sum", "ceph_bluestore_state_deferred_cleanup_lat_sum"},
  {"bluestore.state_deferred_queued_lat_count", "ceph_bluestore_state_deferred_queued_lat_count"},
  {"bluestore.state_deferred_queued_lat_sum", "ceph_bluestore_state_deferred_queued_lat_sum"},
  {"bluestore.state_done_lat_count", "ceph_bluestore_state_done_lat_count"},
  {"bluestore.state_done_lat_sum", "ceph_bluestore_state_done_lat_sum"},
  {"bluestore.state_finishing_lat_count", "ceph_bluestore_state_finishing_lat_count"},
  {"bluestore.state_finishing_lat_sum", "ceph_bluestore_state_finishing_lat_sum"},
  {"bluestore.state_io_done_lat_count", "ceph_bluestore_state_io_done_lat_count"},
  {"bluestore.state_io_done_lat_sum", "ceph_bluestore_state_io_done_lat_sum"},
  {"bluestore.state_kv_commiting_lat_count", "ceph_bluestore_state_kv_commiting_lat_count"},
  {"bluestore.state_kv_commiting_lat_sum", "ceph_bluestore_state_kv_commiting_lat_sum"},
  {"bluestore.state_kv_done_lat_count", "ceph_bluestore_state_kv_done_lat_count"},
  {"bluestore.state_kv_done_lat_sum", "ceph_bluestore_state_kv_done_lat_sum"},
  {"bluestore.state_kv_queued_lat_count", "ceph_bluestore_state_kv_queued_lat_count"},
  {"bluestore.state_kv_queued_lat_sum", "ceph_bluestore_state_kv_queued_lat_sum"},
  {"bluestore.state_prepare_lat_count", "ceph_bluestore_state_prepare_lat_count"},
  {"bluestore.state_prepare_lat_sum", "ceph_bluestore_state_prepare_lat_sum"},
  {"bluestore.stored", "ceph_bluestore_stored"},
  {"bluestore.truncate_lat_count", "ceph_bluestore_truncate_lat_count"},
  {"bluestore.truncate_lat_sum", "ceph_bluestore_truncate_lat_sum"},
  {"bluestore.txc_commit_lat_count", "ceph_bluestore_txc_commit_lat_count"},
  {"bluestore.txc_commit_lat_sum", "ceph_bluestore_txc_commit_lat_sum"},
  {"bluestore.txc_submit_lat_count", "ceph_bluestore_txc_submit_lat_count"},
  {"bluestore.txc_submit_lat_sum", "ceph_bluestore_txc_submit_lat_sum"},
  {"bluestore.txc_throttle_lat_count", "ceph_bluestore_txc_throttle_lat_count"},
  {"bluestore.txc_throttle_lat_sum", "ceph_bluestore_txc_throttle_lat_sum"},
  {"cluster_by_class_total_bytes", "ceph_cluster_by_class_total_bytes"},
  {"cluster_by_class_total_used_bytes", "ceph_cluster_by_class_total_used_bytes"},
  {"cluster_by_class_total_used_raw_bytes", "ceph_cluster_by_class_total_used_raw_bytes"},
  {"cluster_osd_blocklist_count", "ceph_cluster_osd_blocklist_count"},
  {"cluster_total_bytes", "ceph_cluster_total_bytes"},
  {"cluster_total_used_bytes", "ceph_cluster_total_used_bytes"},
  {"cluster_total_used_raw_bytes", "ceph_cluster_total_used_raw_bytes"},
  {"daemon_health_metrics", "ceph_daemon_health_metrics"},
  {"disk_occupation", "ceph_disk_occupation"},
  {"disk_occupation_human", "ceph_disk_occupation_human"},
  {"fs_metadata", "ceph_fs_metadata"},
  {"health_detail", "ceph_health_detail"},
  {"health_status", "ceph_health_status"},
  {"healthcheck_slow_ops", "ceph_healthcheck_slow_ops"},
  {"mds.caps", "ceph_mds_caps"},
  {"mds.ceph_cap_op_flush_ack", "ceph_mds_ceph_cap_op_flush_ack"},
  {"mds.ceph_cap_op_flushsnap_ack", "ceph_mds_ceph_cap_op_flushsnap_ack"},
  {"mds.ceph_cap_op_grant", "ceph_mds_ceph_cap_op_grant"},
  {"mds.ceph_cap_op_revoke", "ceph_mds_ceph_cap_op_revoke"},
  {"mds.ceph_cap_op_trunc", "ceph_mds_ceph_cap_op_trunc"},
  {"mds.dir_commit", "ceph_mds_dir_commit"},
  {"mds.dir_fetch_complete", "ceph_mds_dir_fetch_complete"},
  {"mds.dir_fetch_keys", "ceph_mds_dir_fetch_keys"},
  {"mds.dir_merge", "ceph_mds_dir_merge"},
  {"mds.dir_split", "ceph_mds_dir_split"},
  {"mds.exported_inodes", "ceph_mds_exported_inodes"},
  {"mds.forward", "ceph_mds_forward"},
  {"mds.handle_client_cap_release", "ceph_mds_handle_client_cap_release"},
  {"mds.handle_client_caps", "ceph_mds_handle_client_caps"},
  {"mds.handle_client_caps_dirty", "ceph_mds_handle_client_caps_dirty"},
  {"mds.handle_inode_file_caps", "ceph_mds_handle_inode_file_caps"},
  {"mds.imported_inodes", "ceph_mds_imported_inodes"},
  {"mds.inodes", "ceph_mds_inodes"},
  {"mds.inodes_expired", "ceph_mds_inodes_expired"},
  {"mds.inodes_pinned", "ceph_mds_inodes_pinned"},
  {"mds.inodes_with_caps", "ceph_mds_inodes_with_caps"},
  {"mds.load_cent", "ceph_mds_load_cent"},
  {"mds.openino_dir_fetch", "ceph_mds_openino_dir_fetch"},
  {"mds.process_request_cap_release", "ceph_mds_process_request_cap_release"},
  {"mds.reply_latency_count", "ceph_mds_reply_latency_count"},
  {"mds.reply_latency_sum", "ceph_mds_reply_latency_sum"},
  {"mds.request", "ceph_mds_request"},
  {"mds.root_rbytes", "ceph_mds_root_rbytes"},
  {"mds.root_rfiles", "ceph_mds_root_rfiles"},
  {"mds.root_rsnaps", "ceph_mds_root_rsnaps"},
  {"mds.slow_reply", "ceph_mds_slow_reply"},
  {"mds.subtrees", "ceph_mds_subtrees"},
  {"mds_cache.ireq_enqueue_scrub", "ceph_mds_cache_ireq_enqueue_scrub"},
  {"mds_cache.ireq_exportdir", "ceph_mds_cache_ireq_exportdir"},
  {"mds_cache.ireq_flush", "ceph_mds_cache_ireq_flush"},
  {"mds_cache.ireq_fragmentdir", "ceph_mds_cache_ireq_fragmentdir"},
  {"mds_cache.ireq_fragstats", "ceph_mds_cache_ireq_fragstats"},
  {"mds_cache.ireq_inodestats", "ceph_mds_cache_ireq_inodestats"},
  {"mds_cache.num_recovering_enqueued", "ceph_mds_cache_num_recovering_enqueued"},
  {"mds_cache.num_recovering_prioritized", "ceph_mds_cache_num_recovering_prioritized"},
  {"mds_cache.num_recovering_processing", "ceph_mds_cache_num_recovering_processing"},
  {"mds_cache.num_strays", "ceph_mds_cache_num_strays"},
  {"mds_cache.num_strays_delayed", "ceph_mds_cache_num_strays_delayed"},
  {"mds_cache.num_strays_enqueuing", "ceph_mds_cache_num_strays_enqueuing"},
  {"mds_cache.recovery_completed", "ceph_mds_cache_recovery_completed"},
  {"mds_cache.recovery_started", "ceph_mds_cache_recovery_started"},
  {"mds_cache.strays_created", "ceph_mds_cache_strays_created"},
  {"mds_cache.strays_enqueued", "ceph_mds_cache_strays_enqueued"},
  {"mds_cache.strays_migrated", "ceph_mds_cache_strays_migrated"},
  {"mds_cache.strays_reintegrated", "ceph_mds_cache_strays_reintegrated"},
  {"mds_log.ev", "ceph_mds_log_ev"},
  {"mds_log.evadd", "ceph_mds_log_evadd"},
  {"mds_log.evex", "ceph_mds_log_evex"},
  {"mds_log.evexd", "ceph_mds_log_evexd"},
  {"mds_log.evexg", "ceph_mds_log_evexg"},
  {"mds_log.evtrm", "ceph_mds_log_evtrm"},
  {"mds_log.jlat_count", "ceph_mds_log_jlat_count"},
  {"mds_log.jlat_sum", "ceph_mds_log_jlat_sum"},
  {"mds_log.replayed", "ceph_mds_log_replayed"},
  {"mds_log.seg", "ceph_mds_log_seg"},
  {"mds_log.segadd", "ceph_mds_log_segadd"},
  {"mds_log.segex", "ceph_mds_log_segex"},
  {"mds_log.segexd", "ceph_mds_log_segexd"},
  {"mds_log.segexg", "ceph_mds_log_segexg"},
  {"mds_log.segtrm", "ceph_mds_log_segtrm"},
  {"mds_mem.cap", "ceph_mds_mem_cap"},
  {"mds_mem.cap+", "ceph_mds_mem_cap_plus"},
  {"mds_mem.cap-", "ceph_mds_mem_cap_minus"},
  {"mds_mem.dir", "ceph_mds_mem_dir"},
  {"mds_mem.dir+", "ceph_mds_mem_dir_plus"},
  {"mds_mem.dir-", "ceph_mds_mem_dir_minus"},
  {"mds_mem.dn", "ceph_mds_mem_dn"},
  {"mds_mem.dn+", "ceph_mds_mem_dn_plus"},
  {"mds_mem.dn-", "ceph_mds_mem_dn_minus"},
  {"mds_mem.heap", "ceph_mds_mem_heap"},
  {"mds_mem.ino", "ceph_mds_mem_ino"},
  {"mds_mem.ino+", "ceph_mds_mem_ino_plus"},
  {"mds_mem.ino-", "ceph_mds_mem_ino_minus"},
  {"mds_metadata", "ceph_mds_metadata"},
  {"mds_server.cap_acquisition_throttle", "ceph_mds_server_cap_acquisition_throttle"},
  {"mds_server.cap_revoke_eviction", "ceph_mds_server_cap_revoke_eviction"},
  {"mds_server.handle_client_request", "ceph_mds_server_handle_client_request"},
  {"mds_server.handle_client_session", "ceph_mds_server_handle_client_session"},
  {"mds_server.handle_peer_request", "ceph_mds_server_handle_peer_request"},
  {"mds_server.req_create_latency_count", "ceph_mds_server_req_create_latency_count"},
  {"mds_server.req_create_latency_sum", "ceph_mds_server_req_create_latency_sum"},
  {"mds_server.req_getattr_latency_count", "ceph_mds_server_req_getattr_latency_count"},
  {"mds_server.req_getattr_latency_sum", "ceph_mds_server_req_getattr_latency_sum"},
  {"mds_server.req_getfilelock_latency_count", "ceph_mds_server_req_getfilelock_latency_count"},
  {"mds_server.req_getfilelock_latency_sum", "ceph_mds_server_req_getfilelock_latency_sum"},
  {"mds_server.req_getvxattr_latency_count", "ceph_mds_server_req_getvxattr_latency_count"},
  {"mds_server.req_getvxattr_latency_sum", "ceph_mds_server_req_getvxattr_latency_sum"},
  {"mds_server.req_link_latency_count", "ceph_mds_server_req_link_latency_count"},
  {"mds_server.req_link_latency_sum", "ceph_mds_server_req_link_latency_sum"},
  {"mds_server.req_lookup_latency_count", "ceph_mds_server_req_lookup_latency_count"},
  {"mds_server.req_lookup_latency_sum", "ceph_mds_server_req_lookup_latency_sum"},
  {"mds_server.req_lookuphash_latency_count", "ceph_mds_server_req_lookuphash_latency_count"},
  {"mds_server.req_lookuphash_latency_sum", "ceph_mds_server_req_lookuphash_latency_sum"},
  {"mds_server.req_lookupino_latency_count", "ceph_mds_server_req_lookupino_latency_count"},
  {"mds_server.req_lookupino_latency_sum", "ceph_mds_server_req_lookupino_latency_sum"},
  {"mds_server.req_lookupname_latency_count", "ceph_mds_server_req_lookupname_latency_count"},
  {"mds_server.req_lookupname_latency_sum", "ceph_mds_server_req_lookupname_latency_sum"},
  {"mds_server.req_lookupparent_latency_count", "ceph_mds_server_req_lookupparent_latency_count"},
  {"mds_server.req_lookupparent_latency_sum", "ceph_mds_server_req_lookupparent_latency_sum"},
  {"mds_server.req_lookupsnap_latency_count", "ceph_mds_server_req_lookupsnap_latency_count"},
  {"mds_server.req_lookupsnap_latency_sum", "ceph_mds_server_req_lookupsnap_latency_sum"},
  {"mds_server.req_lssnap_latency_count", "ceph_mds_server_req_lssnap_latency_count"},
  {"mds_server.req_lssnap_latency_sum", "ceph_mds_server_req_lssnap_latency_sum"},
  {"mds_server.req_mkdir_latency_count", "ceph_mds_server_req_mkdir_latency_count"},
  {"mds_server.req_mkdir_latency_sum", "ceph_mds_server_req_mkdir_latency_sum"},
  {"mds_server.req_mknod_latency_count", "ceph_mds_server_req_mknod_latency_count"},
  {"mds_server.req_mknod_latency_sum", "ceph_mds_server_req_mknod_latency_sum"},
  {"mds_server.req_mksnap_latency_count", "ceph_mds_server_req_mksnap_latency_count"},
  {"mds_server.req_mksnap_latency_sum", "ceph_mds_server_req_mksnap_latency_sum"},
  {"mds_server.req_open_latency_count", "ceph_mds_server_req_open_latency_count"},
  {"mds_server.req_open_latency_sum", "ceph_mds_server_req_open_latency_sum"},
  {"mds_server.req_readdir_latency_count", "ceph_mds_server_req_readdir_latency_count"},
  {"mds_server.req_readdir_latency_sum", "ceph_mds_server_req_readdir_latency_sum"},
  {"mds_server.req_rename_latency_count", "ceph_mds_server_req_rename_latency_count"},
  {"mds_server.req_rename_latency_sum", "ceph_mds_server_req_rename_latency_sum"},
  {"mds_server.req_renamesnap_latency_count", "ceph_mds_server_req_renamesnap_latency_count"},
  {"mds_server.req_renamesnap_latency_sum", "ceph_mds_server_req_renamesnap_latency_sum"},
  {"mds_server.req_rmdir_latency_count", "ceph_mds_server_req_rmdir_latency_count"},
  {"mds_server.req_rmdir_latency_sum", "ceph_mds_server_req_rmdir_latency_sum"},
  {"mds_server.req_rmsnap_latency_count", "ceph_mds_server_req_rmsnap_latency_count"},
  {"mds_server.req_rmsnap_latency_sum", "ceph_mds_server_req_rmsnap_latency_sum"},
  {"mds_server.req_rmxattr_latency_count", "ceph_mds_server_req_rmxattr_latency_count"},
  {"mds_server.req_rmxattr_latency_sum", "ceph_mds_server_req_rmxattr_latency_sum"},
  {"mds_server.req_setattr_latency_count", "ceph_mds_server_req_setattr_latency_count"},
  {"mds_server.req_setattr_latency_sum", "ceph_mds_server_req_setattr_latency_sum"},
  {"mds_server.req_setdirlayout_latency_count", "ceph_mds_server_req_setdirlayout_latency_count"},
  {"mds_server.req_setdirlayout_latency_sum", "ceph_mds_server_req_setdirlayout_latency_sum"},
  {"mds_server.req_setfilelock_latency_count", "ceph_mds_server_req_setfilelock_latency_count"},
  {"mds_server.req_setfilelock_latency_sum", "ceph_mds_server_req_setfilelock_latency_sum"},
  {"mds_server.req_setlayout_latency_count", "ceph_mds_server_req_setlayout_latency_count"},
  {"mds_server.req_setlayout_latency_sum", "ceph_mds_server_req_setlayout_latency_sum"},
  {"mds_server.req_setxattr_latency_count", "ceph_mds_server_req_setxattr_latency_count"},
  {"mds_server.req_setxattr_latency_sum", "ceph_mds_server_req_setxattr_latency_sum"},
  {"mds_server.req_symlink_latency_count", "ceph_mds_server_req_symlink_latency_count"},
  {"mds_server.req_symlink_latency_sum", "ceph_mds_server_req_symlink_latency_sum"},
  {"mds_server.req_unlink_latency_count", "ceph_mds_server_req_unlink_latency_count"},
  {"mds_server.req_unlink_latency_sum", "ceph_mds_server_req_unlink_latency_sum"},
  {"mds_sessions.average_load", "ceph_mds_sessions_average_load"},
  {"mds_sessions.avg_session_uptime", "ceph_mds_sessions_avg_session_uptime"},
  {"mds_sessions.session_add", "ceph_mds_sessions_session_add"},
  {"mds_sessions.session_count", "ceph_mds_sessions_session_count"},
  {"mds_sessions.session_remove", "ceph_mds_sessions_session_remove"},
  {"mds_sessions.sessions_open", "ceph_mds_sessions_sessions_open"},
  {"mds_sessions.sessions_stale", "ceph_mds_sessions_sessions_stale"},
  {"mds_sessions.total_load", "ceph_mds_sessions_total_load"},
  {"mgr_metadata", "ceph_mgr_metadata"},
  {"mgr_module_can_run", "ceph_mgr_module_can_run"},
  {"mgr_module_status", "ceph_mgr_module_status"},
  {"mgr_status", "ceph_mgr_status"},
  {"mon.election_call", "ceph_mon_election_call"},
  {"mon.election_lose", "ceph_mon_election_lose"},
  {"mon.election_win", "ceph_mon_election_win"},
  {"mon.num_elections", "ceph_mon_num_elections"},
  {"mon.num_sessions", "ceph_mon_num_sessions"},
  {"mon.session_add", "ceph_mon_session_add"},
  {"mon.session_rm", "ceph_mon_session_rm"},
  {"mon.session_trim", "ceph_mon_session_trim"},
  {"mon_metadata", "ceph_mon_metadata"},
  {"mon_quorum_status", "ceph_mon_quorum_status"},
  {"num_objects_degraded", "ceph_num_objects_degraded"},
  {"num_objects_misplaced", "ceph_num_objects_misplaced"},
  {"num_objects_unfound", "ceph_num_objects_unfound"},
  {"objecter-0x5591781656c0.op_active", "ceph_objecter_0x5591781656c0_op_active"},
  {"objecter-0x5591781656c0.op_r", "ceph_objecter_0x5591781656c0_op_r"},
  {"objecter-0x5591781656c0.op_rmw", "ceph_objecter_0x5591781656c0_op_rmw"},
  {"objecter-0x5591781656c0.op_w", "ceph_objecter_0x5591781656c0_op_w"},
  {"objecter-0x559178165930.op_active", "ceph_objecter_0x559178165930_op_active"},
  {"objecter-0x559178165930.op_r", "ceph_objecter_0x559178165930_op_r"},
  {"objecter-0x559178165930.op_rmw", "ceph_objecter_0x559178165930_op_rmw"},
  {"objecter-0x559178165930.op_w", "ceph_objecter_0x559178165930_op_w"},
  {"objecter.op_active", "ceph_objecter_op_active"},
  {"objecter.op_r", "ceph_objecter_op_r"},
  {"objecter.op_rmw", "ceph_objecter_op_rmw"},
  {"objecter.op_w", "ceph_objecter_op_w"},
  {"osd.numpg", "ceph_osd_numpg"},
  {"osd.numpg_removing", "ceph_osd_numpg_removing"},
  {"osd.op", "ceph_osd_op"},
  {"osd.op_in_bytes", "ceph_osd_op_in_bytes"},
  {"osd.op_latency_count", "ceph_osd_op_latency_count"},
  {"osd.op_latency_sum", "ceph_osd_op_latency_sum"},
  {"osd.op_out_bytes", "ceph_osd_op_out_bytes"},
  {"osd.op_prepare_latency_count", "ceph_osd_op_prepare_latency_count"},
  {"osd.op_prepare_latency_sum", "ceph_osd_op_prepare_latency_sum"},
  {"osd.op_process_latency_count", "ceph_osd_op_process_latency_count"},
  {"osd.op_process_latency_sum", "ceph_osd_op_process_latency_sum"},
  {"osd.op_r", "ceph_osd_op_r"},
  {"osd.op_r_latency_count", "ceph_osd_op_r_latency_count"},
  {"osd.op_r_latency_sum", "ceph_osd_op_r_latency_sum"},
  {"osd.op_r_out_bytes", "ceph_osd_op_r_out_bytes"},
  {"osd.op_r_prepare_latency_count", "ceph_osd_op_r_prepare_latency_count"},
  {"osd.op_r_prepare_latency_sum", "ceph_osd_op_r_prepare_latency_sum"},
  {"osd.op_r_process_latency_count", "ceph_osd_op_r_process_latency_count"},
  {"osd.op_r_process_latency_sum", "ceph_osd_op_r_process_latency_sum"},
  {"osd.op_rw", "ceph_osd_op_rw"},
  {"osd.op_rw_in_bytes", "ceph_osd_op_rw_in_bytes"},
  {"osd.op_rw_latency_count", "ceph_osd_op_rw_latency_count"},
  {"osd.op_rw_latency_sum", "ceph_osd_op_rw_latency_sum"},
  {"osd.op_rw_out_bytes", "ceph_osd_op_rw_out_bytes"},
  {"osd.op_rw_prepare_latency_count", "ceph_osd_op_rw_prepare_latency_count"},
  {"osd.op_rw_prepare_latency_sum", "ceph_osd_op_rw_prepare_latency_sum"},
  {"osd.op_rw_process_latency_count", "ceph_osd_op_rw_process_latency_count"},
  {"osd.op_rw_process_latency_sum", "ceph_osd_op_rw_process_latency_sum"},
  {"osd.op_w", "ceph_osd_op_w"},
  {"osd.op_w_in_bytes", "ceph_osd_op_w_in_bytes"},
  {"osd.op_w_latency_count", "ceph_osd_op_w_latency_count"},
  {"osd.op_w_latency_sum", "ceph_osd_op_w_latency_sum"},
  {"osd.op_w_prepare_latency_count", "ceph_osd_op_w_prepare_latency_count"},
  {"osd.op_w_prepare_latency_sum", "ceph_osd_op_w_prepare_latency_sum"},
  {"osd.op_w_process_latency_count", "ceph_osd_op_w_process_latency_count"},
  {"osd.op_w_process_latency_sum", "ceph_osd_op_w_process_latency_sum"},
  {"osd.op_wip", "ceph_osd_op_wip"},
  {"osd.recovery_bytes", "ceph_osd_recovery_bytes"},
  {"osd.recovery_ops", "ceph_osd_recovery_ops"},
  {"osd.stat_bytes", "ceph_osd_stat_bytes"},
  {"osd.stat_bytes_used", "ceph_osd_stat_bytes_used"},
  {"osd_apply_latency_ms", "ceph_osd_apply_latency_ms"},
  {"osd_commit_latency_ms", "ceph_osd_commit_latency_ms"},
  {"osd_flag_nobackfill", "ceph_osd_flag_nobackfill"},
  {"osd_flag_nodeep-scrub", "ceph_osd_flag_nodeep_scrub"},
  {"osd_flag_nodown", "ceph_osd_flag_nodown"},
  {"osd_flag_noin", "ceph_osd_flag_noin"},
  {"osd_flag_noout", "ceph_osd_flag_noout"},
  {"osd_flag_norebalance", "ceph_osd_flag_norebalance"},
  {"osd_flag_norecover", "ceph_osd_flag_norecover"},
  {"osd_flag_noscrub", "ceph_osd_flag_noscrub"},
  {"osd_flag_noup", "ceph_osd_flag_noup"},
  {"osd_in", "ceph_osd_in"},
  {"osd_metadata", "ceph_osd_metadata"},
  {"osd_up", "ceph_osd_up"},
  {"osd_weight", "ceph_osd_weight"},
  {"paxos.accept_timeout", "ceph_paxos_accept_timeout"},
  {"paxos.begin", "ceph_paxos_begin"},
  {"paxos.begin_bytes_count", "ceph_paxos_begin_bytes_count"},
  {"paxos.begin_bytes_sum", "ceph_paxos_begin_bytes_sum"},
  {"paxos.begin_keys_count", "ceph_paxos_begin_keys_count"},
  {"paxos.begin_keys_sum", "ceph_paxos_begin_keys_sum"},
  {"paxos.begin_latency_count", "ceph_paxos_begin_latency_count"},
  {"paxos.begin_latency_sum", "ceph_paxos_begin_latency_sum"},
  {"paxos.collect", "ceph_paxos_collect"},
  {"paxos.collect_bytes_count", "ceph_paxos_collect_bytes_count"},
  {"paxos.collect_bytes_sum", "ceph_paxos_collect_bytes_sum"},
  {"paxos.collect_keys_count", "ceph_paxos_collect_keys_count"},
  {"paxos.collect_keys_sum", "ceph_paxos_collect_keys_sum"},
  {"paxos.collect_latency_count", "ceph_paxos_collect_latency_count"},
  {"paxos.collect_latency_sum", "ceph_paxos_collect_latency_sum"},
  {"paxos.collect_timeout", "ceph_paxos_collect_timeout"},
  {"paxos.collect_uncommitted", "ceph_paxos_collect_uncommitted"},
  {"paxos.commit", "ceph_paxos_commit"},
  {"paxos.commit_bytes_count", "ceph_paxos_commit_bytes_count"},
  {"paxos.commit_bytes_sum", "ceph_paxos_commit_bytes_sum"},
  {"paxos.commit_keys_count", "ceph_paxos_commit_keys_count"},
  {"paxos.commit_keys_sum", "ceph_paxos_commit_keys_sum"},
  {"paxos.commit_latency_count", "ceph_paxos_commit_latency_count"},
  {"paxos.commit_latency_sum", "ceph_paxos_commit_latency_sum"},
  {"paxos.lease_ack_timeout", "ceph_paxos_lease_ack_timeout"},
  {"paxos.lease_timeout", "ceph_paxos_lease_timeout"},
  {"paxos.new_pn", "ceph_paxos_new_pn"},
  {"paxos.new_pn_latency_count", "ceph_paxos_new_pn_latency_count"},
  {"paxos.new_pn_latency_sum", "ceph_paxos_new_pn_latency_sum"},
  {"paxos.refresh", "ceph_paxos_refresh"},
  {"paxos.refresh_latency_count", "ceph_paxos_refresh_latency_count"},
  {"paxos.refresh_latency_sum", "ceph_paxos_refresh_latency_sum"},
  {"paxos.restart", "ceph_paxos_restart"},
  {"paxos.share_state", "ceph_paxos_share_state"},
  {"paxos.share_state_bytes_count", "ceph_paxos_share_state_bytes_count"},
  {"paxos.share_state_bytes_sum", "ceph_paxos_share_state_bytes_sum"},
  {"paxos.share_state_keys_count", "ceph_paxos_share_state_keys_count"},
  {"paxos.share_state_keys_sum", "ceph_paxos_share_state_keys_sum"},
  {"paxos.start_leader", "ceph_paxos_start_leader"},
  {"paxos.start_peon", "ceph_paxos_start_peon"},
  {"paxos.store_state", "ceph_paxos_store_state"},
  {"paxos.store_state_bytes_count", "ceph_paxos_store_state_bytes_count"},
  {"paxos.store_state_bytes_sum", "ceph_paxos_store_state_bytes_sum"},
  {"paxos.store_state_keys_count", "ceph_paxos_store_state_keys_count"},
  {"paxos.store_state_keys_sum", "ceph_paxos_store_state_keys_sum"},
  {"paxos.store_state_latency_count", "ceph_paxos_store_state_latency_count"},
  {"paxos.store_state_latency_sum", "ceph_paxos_store_state_latency_sum"},
  {"pg_activating", "ceph_pg_activating"},
  {"pg_active", "ceph_pg_active"},
  {"pg_backfill_toofull", "ceph_pg_backfill_toofull"},
  {"pg_backfill_unfound", "ceph_pg_backfill_unfound"},
  {"pg_backfill_wait", "ceph_pg_backfill_wait"},
  {"pg_backfilling", "ceph_pg_backfilling"},
  {"pg_clean", "ceph_pg_clean"},
  {"pg_creating", "ceph_pg_creating"},
  {"pg_deep", "ceph_pg_deep"},
  {"pg_degraded", "ceph_pg_degraded"},
  {"pg_down", "ceph_pg_down"},
  {"pg_failed_repair", "ceph_pg_failed_repair"},
  {"pg_forced_backfill", "ceph_pg_forced_backfill"},
  {"pg_forced_recovery", "ceph_pg_forced_recovery"},
  {"pg_incomplete", "ceph_pg_incomplete"},
  {"pg_inconsistent", "ceph_pg_inconsistent"},
  {"pg_laggy", "ceph_pg_laggy"},
  {"pg_peered", "ceph_pg_peered"},
  {"pg_peering", "ceph_pg_peering"},
  {"pg_premerge", "ceph_pg_premerge"},
  {"pg_recovering", "ceph_pg_recovering"},
  {"pg_recovery_toofull", "ceph_pg_recovery_toofull"},
  {"pg_recovery_unfound", "ceph_pg_recovery_unfound"},
  {"pg_recovery_wait", "ceph_pg_recovery_wait"},
  {"pg_remapped", "ceph_pg_remapped"},
  {"pg_repair", "ceph_pg_repair"},
  {"pg_scrubbing", "ceph_pg_scrubbing"},
  {"pg_snaptrim", "ceph_pg_snaptrim"},
  {"pg_snaptrim_error", "ceph_pg_snaptrim_error"},
  {"pg_snaptrim_wait", "ceph_pg_snaptrim_wait"},
  {"pg_stale", "ceph_pg_stale"},
  {"pg_total", "ceph_pg_total"},
  {"pg_undersized", "ceph_pg_undersized"},
  {"pg_unknown", "ceph_pg_unknown"},
  {"pg_wait", "ceph_pg_wait"},
  {"pool_avail_raw", "ceph_pool_avail_raw"},
  {"pool_bytes_used", "ceph_pool_bytes_used"},
  {"pool_compress_bytes_used", "ceph_pool_compress_bytes_used"},
  {"pool_compress_under_bytes", "ceph_pool_compress_under_bytes"},
  {"pool_dirty", "ceph_pool_dirty"},
  {"pool_max_avail", "ceph_pool_max_avail"},
  {"pool_metadata", "ceph_pool_metadata"},
  {"pool_num_bytes_recovered", "ceph_pool_num_bytes_recovered"},
  {"pool_num_objects_recovered", "ceph_pool_num_objects_recovered"},
  {"pool_objects", "ceph_pool_objects"},
  {"pool_objects_repaired", "ceph_pool_objects_repaired"},
  {"pool_percent_used", "ceph_pool_percent_used"},
  {"pool_quota_bytes", "ceph_pool_quota_bytes"},
  {"pool_quota_objects", "ceph_pool_quota_objects"},
  {"pool_rd", "ceph_pool_rd"},
  {"pool_rd_bytes", "ceph_pool_rd_bytes"},
  {"pool_recovering_bytes_per_sec", "ceph_pool_recovering_bytes_per_sec"},
  {"pool_recovering_keys_per_sec", "ceph_pool_recovering_keys_per_sec"},
  {"pool_recovering_objects_per_sec", "ceph_pool_recovering_objects_per_sec"},
  {"pool_stored", "ceph_pool_stored"},
  {"pool_stored_raw", "ceph_pool_stored_raw"},
  {"pool_wr", "ceph_pool_wr"},
  {"pool_wr_bytes", "ceph_pool_wr_bytes"},
  {"prioritycache.cache_bytes", "ceph_prioritycache_cache_bytes"},
  {"prioritycache.heap_bytes", "ceph_prioritycache_heap_bytes"},
  {"prioritycache.mapped_bytes", "ceph_prioritycache_mapped_bytes"},
  {"prioritycache.target_bytes", "ceph_prioritycache_target_bytes"},
  {"prioritycache.unmapped_bytes", "ceph_prioritycache_unmapped_bytes"},
  {"prioritycache:full.committed_bytes", "ceph_prioritycache:full_committed_bytes"},
  {"prioritycache:full.pri0_bytes", "ceph_prioritycache:full_pri0_bytes"},
  {"prioritycache:full.pri10_bytes", "ceph_prioritycache:full_pri10_bytes"},
  {"prioritycache:full.pri11_bytes", "ceph_prioritycache:full_pri11_bytes"},
  {"prioritycache:full.pri1_bytes", "ceph_prioritycache:full_pri1_bytes"},
  {"prioritycache:full.pri2_bytes", "ceph_prioritycache:full_pri2_bytes"},
  {"prioritycache:full.pri3_bytes", "ceph_prioritycache:full_pri3_bytes"},
  {"prioritycache:full.pri4_bytes", "ceph_prioritycache:full_pri4_bytes"},
  {"prioritycache:full.pri5_bytes", "ceph_prioritycache:full_pri5_bytes"},
  {"prioritycache:full.pri6_bytes", "ceph_prioritycache:full_pri6_bytes"},
  {"prioritycache:full.pri7_bytes", "ceph_prioritycache:full_pri7_bytes"},
  {"prioritycache:full.pri8_bytes", "ceph_prioritycache:full_pri8_bytes"},
  {"prioritycache:full.pri9_bytes", "ceph_prioritycache:full_pri9_bytes"},
  {"prioritycache:full.reserved_bytes", "ceph_prioritycache:full_reserved_bytes"},
  {"prioritycache:inc.committed_bytes", "ceph_prioritycache:inc_committed_bytes"},
  {"prioritycache:inc.pri0_bytes", "ceph_prioritycache:inc_pri0_bytes"},
  {"prioritycache:inc.pri10_bytes", "ceph_prioritycache:inc_pri10_bytes"},
  {"prioritycache:inc.pri11_bytes", "ceph_prioritycache:inc_pri11_bytes"},
  {"prioritycache:inc.pri1_bytes", "ceph_prioritycache:inc_pri1_bytes"},
  {"prioritycache:inc.pri2_bytes", "ceph_prioritycache:inc_pri2_bytes"},
  {"prioritycache:inc.pri3_bytes", "ceph_prioritycache:inc_pri3_bytes"},
  {"prioritycache:inc.pri4_bytes", "ceph_prioritycache:inc_pri4_bytes"},
  {"prioritycache:inc.pri5_bytes", "ceph_prioritycache:inc_pri5_bytes"},
  {"prioritycache:inc.pri6_bytes", "ceph_prioritycache:inc_pri6_bytes"},
  {"prioritycache:inc.pri7_bytes", "ceph_prioritycache:inc_pri7_bytes"},
  {"prioritycache:inc.pri8_bytes", "ceph_prioritycache:inc_pri8_bytes"},
  {"prioritycache:inc.pri9_bytes", "ceph_prioritycache:inc_pri9_bytes"},
  {"prioritycache:inc.reserved_bytes", "ceph_prioritycache:inc_reserved_bytes"},
  {"prioritycache:kv.committed_bytes", "ceph_prioritycache:kv_committed_bytes"},
  {"prioritycache:kv.pri0_bytes", "ceph_prioritycache:kv_pri0_bytes"},
  {"prioritycache:kv.pri10_bytes", "ceph_prioritycache:kv_pri10_bytes"},
  {"prioritycache:kv.pri11_bytes", "ceph_prioritycache:kv_pri11_bytes"},
  {"prioritycache:kv.pri1_bytes", "ceph_prioritycache:kv_pri1_bytes"},
  {"prioritycache:kv.pri2_bytes", "ceph_prioritycache:kv_pri2_bytes"},
  {"prioritycache:kv.pri3_bytes", "ceph_prioritycache:kv_pri3_bytes"},
  {"prioritycache:kv.pri4_bytes", "ceph_prioritycache:kv_pri4_bytes"},
  {"prioritycache:kv.pri5_bytes", "ceph_prioritycache:kv_pri5_bytes"},
  {"prioritycache:kv.pri6_bytes", "ceph_prioritycache:kv_pri6_bytes"},
  {"prioritycache:kv.pri7_bytes", "ceph_prioritycache:kv_pri7_bytes"},
  {"prioritycache:kv.pri8_bytes", "ceph_prioritycache:kv_pri8_bytes"},
  {"prioritycache:kv.pri9_bytes", "ceph_prioritycache:kv_pri9_bytes"},
  {"prioritycache:kv.reserved_bytes", "ceph_prioritycache:kv_reserved_bytes"},
  {"prometheus_collect_duration_seconds_count", "ceph_prometheus_collect_duration_seconds_count"},
  {"prometheus_collect_duration_seconds_sum", "ceph_prometheus_collect_duration_seconds_sum"},
  {"purge_queue.pq_executed", "ceph_purge_queue_pq_executed"},
  {"purge_queue.pq_executing", "ceph_purge_queue_pq_executing"},
  {"purge_queue.pq_executing_high_water", "ceph_purge_queue_pq_executing_high_water"},
  {"purge_queue.pq_executing_ops", "ceph_purge_queue_pq_executing_ops"},
  {"purge_queue.pq_executing_ops_high_water", "ceph_purge_queue_pq_executing_ops_high_water"},
  {"purge_queue.pq_item_in_journal", "ceph_purge_queue_pq_item_in_journal"},
  {"rbd_mirror_metadata", "ceph_rbd_mirror_metadata"},
  {"rgw.cache_hit", "ceph_rgw_cache_hit"},
  {"rgw.cache_miss", "ceph_rgw_cache_miss"},
  {"rgw.failed_req", "ceph_rgw_failed_req"},
  {"rgw.gc_retire_object", "ceph_rgw_gc_retire_object"},
  {"rgw.get", "ceph_rgw_get"},
  {"rgw.get_b", "ceph_rgw_get_b"},
  {"rgw.get_initial_lat_count", "ceph_rgw_get_initial_lat_count"},
  {"rgw.get_initial_lat_sum", "ceph_rgw_get_initial_lat_sum"},
  {"rgw.keystone_token_cache_hit", "ceph_rgw_keystone_token_cache_hit"},
  {"rgw.keystone_token_cache_miss", "ceph_rgw_keystone_token_cache_miss"},
  {"rgw.lc_abort_mpu", "ceph_rgw_lc_abort_mpu"},
  {"rgw.lc_expire_current", "ceph_rgw_lc_expire_current"},
  {"rgw.lc_expire_dm", "ceph_rgw_lc_expire_dm"},
  {"rgw.lc_expire_noncurrent", "ceph_rgw_lc_expire_noncurrent"},
  {"rgw.lc_transition_current", "ceph_rgw_lc_transition_current"},
  {"rgw.lc_transition_noncurrent", "ceph_rgw_lc_transition_noncurrent"},
  {"rgw.lua_current_vms", "ceph_rgw_lua_current_vms"},
  {"rgw.lua_script_fail", "ceph_rgw_lua_script_fail"},
  {"rgw.lua_script_ok", "ceph_rgw_lua_script_ok"},
  {"rgw.pubsub_event_lost", "ceph_rgw_pubsub_event_lost"},
  {"rgw.pubsub_event_triggered", "ceph_rgw_pubsub_event_triggered"},
  {"rgw.pubsub_events", "ceph_rgw_pubsub_events"},
  {"rgw.pubsub_missing_conf", "ceph_rgw_pubsub_missing_conf"},
  {"rgw.pubsub_push_failed", "ceph_rgw_pubsub_push_failed"},
  {"rgw.pubsub_push_ok", "ceph_rgw_pubsub_push_ok"},
  {"rgw.pubsub_push_pending", "ceph_rgw_pubsub_push_pending"},
  {"rgw.pubsub_store_fail", "ceph_rgw_pubsub_store_fail"},
  {"rgw.pubsub_store_ok", "ceph_rgw_pubsub_store_ok"},
  {"rgw.put", "ceph_rgw_put"},
  {"rgw.put_b", "ceph_rgw_put_b"},
  {"rgw.put_initial_lat_count", "ceph_rgw_put_initial_lat_count"},
  {"rgw.put_initial_lat_sum", "ceph_rgw_put_initial_lat_sum"},
  {"rgw.qactive", "ceph_rgw_qactive"},
  {"rgw.qlen", "ceph_rgw_qlen"},
  {"rgw.req", "ceph_rgw_req"},
  {"rgw_metadata", "ceph_rgw_metadata"},
  {"rocksdb.compact", "ceph_rocksdb_compact"},
  {"rocksdb.compact_queue_len", "ceph_rocksdb_compact_queue_len"},
  {"rocksdb.compact_queue_merge", "ceph_rocksdb_compact_queue_merge"},
  {"rocksdb.compact_range", "ceph_rocksdb_compact_range"},
  {"rocksdb.get_latency_count", "ceph_rocksdb_get_latency_count"},
  {"rocksdb.get_latency_sum", "ceph_rocksdb_get_latency_sum"},
  {"rocksdb.rocksdb_write_delay_time_count", "ceph_rocksdb_rocksdb_write_delay_time_count"},
  {"rocksdb.rocksdb_write_delay_time_sum", "ceph_rocksdb_rocksdb_write_delay_time_sum"},
  {"rocksdb.rocksdb_write_memtable_time_count", "ceph_rocksdb_rocksdb_write_memtable_time_count"},
  {"rocksdb.rocksdb_write_memtable_time_sum", "ceph_rocksdb_rocksdb_write_memtable_time_sum"},
  {"rocksdb.rocksdb_write_pre_and_post_time_count", "ceph_rocksdb_rocksdb_write_pre_and_post_time_count"},
  {"rocksdb.rocksdb_write_pre_and_post_time_sum", "ceph_rocksdb_rocksdb_write_pre_and_post_time_sum"},
  {"rocksdb.rocksdb_write_wal_time_count", "ceph_rocksdb_rocksdb_write_wal_time_count"},
  {"rocksdb.rocksdb_write_wal_time_sum", "ceph_rocksdb_rocksdb_write_wal_time_sum"},
  {"rocksdb.submit_latency_count", "ceph_rocksdb_submit_latency_count"},
  {"rocksdb.submit_latency_sum", "ceph_rocksdb_submit_latency_sum"},
  {"rocksdb.submit_sync_latency_count", "ceph_rocksdb_submit_sync_latency_count"},
  {"rocksdb.submit_sync_latency_sum", "ceph_rocksdb_submit_sync_latency_sum"}
};


class AdminSocketTest
{
public:
  explicit AdminSocketTest(AdminSocket *asokc)
    : m_asokc(asokc)
  {
  }
  bool init(const std::string &uri) {
    return m_asokc->init(uri);
  }
  std::string bind_and_listen(const std::string &sock_path, int *fd) {
    return m_asokc->bind_and_listen(sock_path, fd);
  }
  bool shutdown() {
    m_asokc->shutdown();
    return true;
  }
  AdminSocket *m_asokc;
};

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_MON_CONFIG);

  g_conf().set_val("exporter_sort_metrics", "true");
  g_conf().set_val("exporter_prio_limit", "5");
  common_init_finish(g_ceph_context);

  int r = RUN_ALL_TESTS();
  return r;
}

TEST(Exporter, promethize) {
  for (auto &test_case : promethize_data) {
    std::string path = test_case.first;
    promethize(path);
    ASSERT_EQ(path, test_case.second);
  }
}

TEST(Exporter, check_labels_and_metric_name) {
  static std::vector<std::pair<std::string, std::string>> counters_data;
  counters_data.emplace_back("ceph-osd.0", "ceph_osd_numpg");
  counters_data.emplace_back("ceph-client.rgw.foo.ceph-node-00.hrgsea.2.94739968030880", "ceph_rgw_get");

  static std::vector<labels_t> labels_vec;
  labels_vec.emplace_back(labels_t{{"ceph_daemon", "\"osd.0\""}});
  labels_vec.emplace_back(labels_t{{"instance_id", "\"hrgsea\""}});
  auto counter_data_itr = counters_data.begin();
  auto labels_vec_itr = labels_vec.begin();
  for (; counter_data_itr != counters_data.end() && labels_vec_itr != labels_vec.end();
         ++counter_data_itr, ++labels_vec_itr) {
        std::string daemon_name = counter_data_itr->first;
        std::string counter_name = counter_data_itr->second;
        DaemonMetricCollector &collector = collector_instance();
        labels_t result = collector.get_extra_labels(daemon_name);
        ASSERT_EQ(result, *labels_vec_itr);
  }
  // test for fail case with daemon_name.size() < 4
  std::string short_daemon_name = "ceph-client.rgw.foo";
  std::string counter_name = "ceph_rgw_get";
  DaemonMetricCollector &collector = collector_instance();
  labels_t fail_result = collector.get_extra_labels(short_daemon_name);
  // This is a special case, the daemon name is not of the required size for fetching instance_id.
  // So no labels should be added.
  ASSERT_TRUE(fail_result.empty());
}

enum LabelType {
  UNLABELED,
  LABELED_RGW,
  LABELED_RBD_MIRROR,
};

void setup_test_data(std::map<std::string, AdminSocketClient> clients, std::string daemon, LabelType label, DaemonMetricCollector &collector) {
  std::string asok_path = "/tmp/" + daemon + ".asok";
  AdminSocketClient client(asok_path);
  clients.insert({daemon, std::move(client)});
  collector.clients = clients;
  std::string expectedCounterDump = "";
  std::string expectedCounterSchema = "";

  if (label == UNLABELED) {
    expectedCounterDump = R"(
    {
      "mon": [
          {
              "labels": {},
              "counters": {
                  "num_sessions": 1,
                  "session_add": 1,
                  "session_rm": 577,
                  "session_trim": 9,
                  "num_elections": 2
              }
          }
      ]
    })";
    expectedCounterSchema = R"(
    {
      "mon": [
          {
              "labels": {},
              "counters": {
                  "num_sessions": {
                      "type": 2,
                      "metric_type": "gauge",
                      "value_type": "integer",
                      "description": "Open sessions",
                      "nick": "sess",
                      "priority": 5,
                      "units": "none"
                  },
                  "session_add": {
                      "type": 10,
                      "metric_type": "counter",
                      "value_type": "integer",
                      "description": "Created sessions",
                      "nick": "sadd",
                      "priority": 8,
                      "units": "none"
                  },
                  "session_rm": {
                      "type": 10,
                      "metric_type": "counter",
                      "value_type": "integer",
                      "description": "Removed sessions",
                      "nick": "srm",
                      "priority": 8,
                      "units": "none"
                  },
                  "session_trim": {
                      "type": 10,
                      "metric_type": "counter",
                      "value_type": "integer",
                      "description": "Trimmed sessions",
                      "nick": "strm",
                      "priority": 5,
                      "units": "none"
                  },
                  "num_elections": {
                      "type": 10,
                      "metric_type": "counter",
                      "value_type": "integer",
                      "description": "Elections participated in",
                      "nick": "ecnt",
                      "priority": 5,
                      "units": "none"
                  }
              }
          }
      ]})";
  }
  else if(label == LABELED_RGW) {
    expectedCounterDump = R"(
    {
      "rgw_op": [
        {
            "labels": {
                "Bucket": "bucket1"
            },
            "counters": {
                "put_obj_ops": 2,
                "put_obj_bytes": 5327,
                "put_obj_lat": {
                    "avgcount": 2,
                    "sum": 2.818064835,
                    "avgtime": 1.409032417
                },
                "get_obj_ops": 5,
                "get_obj_bytes": 5325,
                "get_obj_lat": {
                    "avgcount": 2,
                    "sum": 0.003000069,
                    "avgtime": 0.001500034
                },
                "list_buckets_ops": 1,
                "list_buckets_lat": {
                    "avgcount": 1,
                    "sum": 0.002300000,
                    "avgtime": 0.002300000
                }
            }
        },
        {
            "labels": {
                "User": "dashboard"
            },
            "counters": {
                "put_obj_ops": 0,
                "put_obj_bytes": 0,
                "put_obj_lat": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                },
                "get_obj_ops": 0,
                "get_obj_bytes": 0,
                "get_obj_lat": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                },
                "del_obj_ops": 0,
                "del_obj_bytes": 0,
                "del_obj_lat": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                },
                "del_bucket_ops": 0,
                "del_bucket_lat": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                },
                "copy_obj_ops": 0,
                "copy_obj_bytes": 0,
                "copy_obj_lat": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                },
                "list_obj_ops": 0,
                "list_obj_lat": {
                    "avgcount": 0,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                },
                "list_buckets_ops": 1,
                "list_buckets_lat": {
                    "avgcount": 1,
                    "sum": 0.000000000,
                    "avgtime": 0.000000000
                }
            }
        }
    ]})";
    expectedCounterSchema = R"(
      {
        "rgw_op": [
          {
            "labels": {
                "Bucket": "bucket1"
            },
            "counters": {
                "put_obj_ops": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Puts",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "put_obj_bytes": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Size of puts",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "put_obj_lat": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "Put latency",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "get_obj_ops": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Gets",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "get_obj_bytes": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Size of gets",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "get_obj_lat": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "Get latency",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "list_buckets_ops": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "List buckets",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "list_buckets_lat": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "List buckets latency",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                }
            }
        },
        {
            "labels": {
                "User": "dashboard"
            },
            "counters": {
                "put_obj_ops": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Puts",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "put_obj_bytes": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Size of puts",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "put_obj_lat": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "Put latency",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "get_obj_ops": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Gets",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "get_obj_bytes": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Size of gets",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "get_obj_lat": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "Get latency",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "del_obj_ops": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Delete objects",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "del_obj_bytes": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Size of delete objects",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "del_obj_lat": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "Delete object latency",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "del_bucket_ops": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Delete Buckets",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "del_bucket_lat": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "Delete bucket latency",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "copy_obj_ops": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Copy objects",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "copy_obj_bytes": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Size of copy objects",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "copy_obj_lat": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "Copy object latency",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "list_obj_ops": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "List objects",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "list_obj_lat": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "List objects latency",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "list_buckets_ops": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "List buckets",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "list_buckets_lat": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "List buckets latency",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                }
            }
        }
        ]
      }

    )";

  }

  else if(label == LABELED_RBD_MIRROR) {
    expectedCounterSchema = R"(
      {
        "rbd_mirror_snapshot_image": [
          {
            "labels": {
                "image": "image1",
                "namespace": "",
                "pool": "data"
            },
            "counters": {
                "snapshots": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Number of snapshots synced",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "sync_time": {
                    "type": 5,
                    "metric_type": "gauge",
                    "value_type": "real-integer-pair",
                    "description": "Average sync time",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "sync_bytes": {
                    "type": 10,
                    "metric_type": "counter",
                    "value_type": "integer",
                    "description": "Total bytes synced",
                    "nick": "",
                    "priority": 5,
                    "units": "bytes"
                },
                "remote_timestamp": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "Timestamp of the remote snapshot",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "local_timestamp": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "Timestamp of the local snapshot",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "last_sync_time": {
                    "type": 1,
                    "metric_type": "gauge",
                    "value_type": "real",
                    "description": "Time taken to sync the last snapshot",
                    "nick": "",
                    "priority": 5,
                    "units": "none"
                },
                "last_sync_bytes": {
                    "type": 2,
                    "metric_type": "gauge",
                    "value_type": "integer",
                    "description": "Bytes synced for the last snapshot",
                    "nick": "",
                    "priority": 5,
                    "units": "bytes"
                }
            }
          }
        ]
      })";

      expectedCounterDump = R"(
      {
        "rbd_mirror_snapshot_image": [
        {
            "labels": {
                "image": "image1",
                "namespace": "",
                "pool": "data"
            },
            "counters": {
                "snapshots": 1,
                "sync_time": {
                    "avgcount": 1,
                    "sum": 0.194675703,
                    "avgtime": 0.194675703
                },
                "sync_bytes": 52428800,
                "remote_timestamp": 1702884232.559626929,
                "local_timestamp": 1702884232.559626929,
                "last_sync_time": 0.194675703,
                "last_sync_bytes": 52428800
            }
        }
      ]
    })";
  }
  collector.dump_asok_metrics(true, 5, false, expectedCounterDump, expectedCounterSchema, false);
}

TEST(Exporter, dump_asok_metrics) {
  std::map<std::string, AdminSocketClient> clients;
  DaemonMetricCollector &collector = collector_instance();
  collector.metrics = "";

  // Test for unlabeled metrics
  std::string daemon = "mon.a";
  setup_test_data(clients, daemon, UNLABELED, collector);

  std::string expectedMetrics = R"(
# HELP ceph_mon_num_elections Elections participated in
# TYPE ceph_mon_num_elections counter
ceph_mon_num_elections{ceph_daemon="mon.a"} 2
# HELP ceph_mon_num_sessions Open sessions
# TYPE ceph_mon_num_sessions gauge
ceph_mon_num_sessions{ceph_daemon="mon.a"} 1
# HELP ceph_mon_session_add Created sessions
# TYPE ceph_mon_session_add counter
ceph_mon_session_add{ceph_daemon="mon.a"} 1
# HELP ceph_mon_session_rm Removed sessions
# TYPE ceph_mon_session_rm counter
ceph_mon_session_rm{ceph_daemon="mon.a"} 577
# HELP ceph_mon_session_trim Trimmed sessions
# TYPE ceph_mon_session_trim counter
ceph_mon_session_trim{ceph_daemon="mon.a"} 9
)";

  std::string actualMetrics = collector.metrics;
  std::cout << "Actual MON Metrics: " << actualMetrics << std::endl;
  ASSERT_TRUE(actualMetrics.find(expectedMetrics) != std::string::npos);
  //ASSERT_TRUE(collector.metrics.find(expectedMetrics) != std::string::npos);

  // Test for labeled metrics - RGW
  daemon = "ceph-client.rgw.foo.ceph-node-00.aayrrj.2.93993527376064";
  setup_test_data(clients, daemon, LABELED_RGW, collector);
  expectedMetrics = R"(
# HELP ceph_rgw_op_copy_obj_bytes Size of copy objects
# TYPE ceph_rgw_op_copy_obj_bytes counter
ceph_rgw_op_copy_obj_bytes{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_copy_obj_lat_count Copy object latency Count
# TYPE ceph_rgw_op_copy_obj_lat_count counter
ceph_rgw_op_copy_obj_lat_count{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_copy_obj_lat_sum Copy object latency Total
# TYPE ceph_rgw_op_copy_obj_lat_sum gauge
ceph_rgw_op_copy_obj_lat_sum{User="dashboard",instance_id="aayrrj"} 0.000000
# HELP ceph_rgw_op_copy_obj_ops Copy objects
# TYPE ceph_rgw_op_copy_obj_ops counter
ceph_rgw_op_copy_obj_ops{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_del_bucket_lat_count Delete bucket latency Count
# TYPE ceph_rgw_op_del_bucket_lat_count counter
ceph_rgw_op_del_bucket_lat_count{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_del_bucket_lat_sum Delete bucket latency Total
# TYPE ceph_rgw_op_del_bucket_lat_sum gauge
ceph_rgw_op_del_bucket_lat_sum{User="dashboard",instance_id="aayrrj"} 0.000000
# HELP ceph_rgw_op_del_bucket_ops Delete Buckets
# TYPE ceph_rgw_op_del_bucket_ops counter
ceph_rgw_op_del_bucket_ops{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_del_obj_bytes Size of delete objects
# TYPE ceph_rgw_op_del_obj_bytes counter
ceph_rgw_op_del_obj_bytes{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_del_obj_lat_count Delete object latency Count
# TYPE ceph_rgw_op_del_obj_lat_count counter
ceph_rgw_op_del_obj_lat_count{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_del_obj_lat_sum Delete object latency Total
# TYPE ceph_rgw_op_del_obj_lat_sum gauge
ceph_rgw_op_del_obj_lat_sum{User="dashboard",instance_id="aayrrj"} 0.000000
# HELP ceph_rgw_op_del_obj_ops Delete objects
# TYPE ceph_rgw_op_del_obj_ops counter
ceph_rgw_op_del_obj_ops{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_get_obj_bytes Size of gets
# TYPE ceph_rgw_op_get_obj_bytes counter
ceph_rgw_op_get_obj_bytes{Bucket="bucket1",instance_id="aayrrj"} 5325
ceph_rgw_op_get_obj_bytes{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_get_obj_lat_count Get latency Count
# TYPE ceph_rgw_op_get_obj_lat_count counter
ceph_rgw_op_get_obj_lat_count{Bucket="bucket1",instance_id="aayrrj"} 2
ceph_rgw_op_get_obj_lat_count{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_get_obj_lat_sum Get latency Total
# TYPE ceph_rgw_op_get_obj_lat_sum gauge
ceph_rgw_op_get_obj_lat_sum{Bucket="bucket1",instance_id="aayrrj"} 0.003000
ceph_rgw_op_get_obj_lat_sum{User="dashboard",instance_id="aayrrj"} 0.000000
# HELP ceph_rgw_op_get_obj_ops Gets
# TYPE ceph_rgw_op_get_obj_ops counter
ceph_rgw_op_get_obj_ops{Bucket="bucket1",instance_id="aayrrj"} 5
ceph_rgw_op_get_obj_ops{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_list_buckets_lat_count List buckets latency Count
# TYPE ceph_rgw_op_list_buckets_lat_count counter
ceph_rgw_op_list_buckets_lat_count{Bucket="bucket1",instance_id="aayrrj"} 1
ceph_rgw_op_list_buckets_lat_count{User="dashboard",instance_id="aayrrj"} 1
# HELP ceph_rgw_op_list_buckets_lat_sum List buckets latency Total
# TYPE ceph_rgw_op_list_buckets_lat_sum gauge
ceph_rgw_op_list_buckets_lat_sum{Bucket="bucket1",instance_id="aayrrj"} 0.002300
ceph_rgw_op_list_buckets_lat_sum{User="dashboard",instance_id="aayrrj"} 0.000000
# HELP ceph_rgw_op_list_buckets_ops List buckets
# TYPE ceph_rgw_op_list_buckets_ops counter
ceph_rgw_op_list_buckets_ops{Bucket="bucket1",instance_id="aayrrj"} 1
ceph_rgw_op_list_buckets_ops{User="dashboard",instance_id="aayrrj"} 1
# HELP ceph_rgw_op_list_obj_lat_count List objects latency Count
# TYPE ceph_rgw_op_list_obj_lat_count counter
ceph_rgw_op_list_obj_lat_count{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_list_obj_lat_sum List objects latency Total
# TYPE ceph_rgw_op_list_obj_lat_sum gauge
ceph_rgw_op_list_obj_lat_sum{User="dashboard",instance_id="aayrrj"} 0.000000
# HELP ceph_rgw_op_list_obj_ops List objects
# TYPE ceph_rgw_op_list_obj_ops counter
ceph_rgw_op_list_obj_ops{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_put_obj_bytes Size of puts
# TYPE ceph_rgw_op_put_obj_bytes counter
ceph_rgw_op_put_obj_bytes{Bucket="bucket1",instance_id="aayrrj"} 5327
ceph_rgw_op_put_obj_bytes{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_put_obj_lat_count Put latency Count
# TYPE ceph_rgw_op_put_obj_lat_count counter
ceph_rgw_op_put_obj_lat_count{Bucket="bucket1",instance_id="aayrrj"} 2
ceph_rgw_op_put_obj_lat_count{User="dashboard",instance_id="aayrrj"} 0
# HELP ceph_rgw_op_put_obj_lat_sum Put latency Total
# TYPE ceph_rgw_op_put_obj_lat_sum gauge
ceph_rgw_op_put_obj_lat_sum{Bucket="bucket1",instance_id="aayrrj"} 2.818065
ceph_rgw_op_put_obj_lat_sum{User="dashboard",instance_id="aayrrj"} 0.000000
# HELP ceph_rgw_op_put_obj_ops Puts
# TYPE ceph_rgw_op_put_obj_ops counter
ceph_rgw_op_put_obj_ops{Bucket="bucket1",instance_id="aayrrj"} 2
ceph_rgw_op_put_obj_ops{User="dashboard",instance_id="aayrrj"} 0
)";

  ASSERT_TRUE(collector.metrics.find(expectedMetrics) != std::string::npos);

  // Test for labeled metrics - RBD_MIRROR
  daemon = "rbd-mirror.a";
  setup_test_data(clients, daemon, LABELED_RBD_MIRROR, collector);

  expectedMetrics = R"(
# HELP ceph_rbd_mirror_snapshot_image_last_sync_bytes Bytes synced for the last snapshot
# TYPE ceph_rbd_mirror_snapshot_image_last_sync_bytes gauge
ceph_rbd_mirror_snapshot_image_last_sync_bytes{ceph_daemon="rbd-mirror.a",image="image1",namespace="",pool="data"} 52428800
# HELP ceph_rbd_mirror_snapshot_image_last_sync_time Time taken to sync the last snapshot
# TYPE ceph_rbd_mirror_snapshot_image_last_sync_time gauge
ceph_rbd_mirror_snapshot_image_last_sync_time{ceph_daemon="rbd-mirror.a",image="image1",namespace="",pool="data"} 0.194676
# HELP ceph_rbd_mirror_snapshot_image_local_timestamp Timestamp of the local snapshot
# TYPE ceph_rbd_mirror_snapshot_image_local_timestamp gauge
ceph_rbd_mirror_snapshot_image_local_timestamp{ceph_daemon="rbd-mirror.a",image="image1",namespace="",pool="data"} 1702884232.559627
# HELP ceph_rbd_mirror_snapshot_image_remote_timestamp Timestamp of the remote snapshot
# TYPE ceph_rbd_mirror_snapshot_image_remote_timestamp gauge
ceph_rbd_mirror_snapshot_image_remote_timestamp{ceph_daemon="rbd-mirror.a",image="image1",namespace="",pool="data"} 1702884232.559627
# HELP ceph_rbd_mirror_snapshot_image_snapshots Number of snapshots synced
# TYPE ceph_rbd_mirror_snapshot_image_snapshots counter
ceph_rbd_mirror_snapshot_image_snapshots{ceph_daemon="rbd-mirror.a",image="image1",namespace="",pool="data"} 1
# HELP ceph_rbd_mirror_snapshot_image_sync_bytes Total bytes synced
# TYPE ceph_rbd_mirror_snapshot_image_sync_bytes counter
ceph_rbd_mirror_snapshot_image_sync_bytes{ceph_daemon="rbd-mirror.a",image="image1",namespace="",pool="data"} 52428800
# HELP ceph_rbd_mirror_snapshot_image_sync_time_count Average sync time Count
# TYPE ceph_rbd_mirror_snapshot_image_sync_time_count counter
ceph_rbd_mirror_snapshot_image_sync_time_count{ceph_daemon="rbd-mirror.a",image="image1",namespace="",pool="data"} 1
# HELP ceph_rbd_mirror_snapshot_image_sync_time_sum Average sync time Total
# TYPE ceph_rbd_mirror_snapshot_image_sync_time_sum gauge
ceph_rbd_mirror_snapshot_image_sync_time_sum{ceph_daemon="rbd-mirror.a",image="image1",namespace="",pool="data"} 0.194676
)";
  ASSERT_TRUE(collector.metrics.find(expectedMetrics) != std::string::npos);
}

TEST(Exporter, add_fixed_name_metrics) {
    std::vector<std::string> metrics = {
      "ceph_data_sync_from_zone2-zg1-realm1_fetch_bytes",
      "ceph_data_sync_from_zone2-zg1-realm1_fetch_bytes_sum",
      "ceph_data_sync_from_zone2-zg1-realm1_poll_latency_sum",
    };
    std::vector<std::string> expected_metrics = {
      "ceph_data_sync_from_zone_fetch_bytes",
      "ceph_data_sync_from_zone_fetch_bytes_sum",
      "ceph_data_sync_from_zone_poll_latency_sum",
    };
    std::string metric_name;
    std::pair<labels_t, std::string> new_metric;
    labels_t expected_labels;
    std::string expected_metric_name;
    for (std::size_t index = 0; index < metrics.size(); ++index) {
        std::string &metric_name = metrics[index];
        DaemonMetricCollector &collector = collector_instance();
        auto new_metric = collector.add_fixed_name_metrics(metric_name);
        expected_labels = {{"source_zone", "\"zone2-zg1-realm1\""}};
        std::string expected_metric_name = expected_metrics[index];
        EXPECT_EQ(new_metric.first, expected_labels);
        ASSERT_EQ(new_metric.second, expected_metric_name);
    }

    metric_name = "ceph_data_sync_from_zone2_fetch_bytes_count";
    DaemonMetricCollector &collector = collector_instance();
    new_metric = collector.add_fixed_name_metrics(metric_name);
    expected_labels = {{"source_zone", "\"zone2\""}};
    expected_metric_name = "ceph_data_sync_from_zone_fetch_bytes_count";
    EXPECT_EQ(new_metric.first, expected_labels);
    ASSERT_TRUE(new_metric.second == expected_metric_name);
}

TEST(Exporter, UpdateSockets) {
    const std::string mock_dir = "/tmp/fake_sock_dir";

    // Create the mock directory
    std::filesystem::create_directories(mock_dir);

    // Create a mix of vstart and real cluster mock .asok files
    std::ofstream(mock_dir + "/ceph-osd.0.asok").close();
    std::ofstream(mock_dir + "/ceph-mds.a.asok").close();
    std::ofstream(mock_dir + "/ceph-mgr.chatest-node-00.ijzynn.asok").close();
    std::ofstream(mock_dir + "/ceph-client.rgw.rgwfoo.chatest-node-00.yqaoen.2.94354846193952.asok").close();
    std::ofstream(mock_dir + "/ceph-client.ceph-exporter.chatest-node-00.asok").close();
    std::ofstream(mock_dir + "/ceph-mon.chatest-node-00.asok").close();

    g_conf().set_val("exporter_sock_dir", mock_dir);

    DaemonMetricCollector collector;

    // Run the function that interacts with the mock directory
    collector.update_sockets();

    // Verify the expected results
    ASSERT_EQ(collector.clients.size(), 4);
    ASSERT_TRUE(collector.clients.find("ceph-osd.0") != collector.clients.end());
    ASSERT_TRUE(collector.clients.find("ceph-mds.a") != collector.clients.end());
    ASSERT_TRUE(collector.clients.find("ceph-mon.chatest-node-00") != collector.clients.end());
    ASSERT_TRUE(collector.clients.find("ceph-client.rgw.rgwfoo.chatest-node-00.yqaoen.2.94354846193952") != collector.clients.end());


    // Remove the mock directory and files
    std::filesystem::remove_all(mock_dir);
}


TEST(Exporter, HealthMetrics) {
    std::map<std::string, AdminSocketClient> clients;
    DaemonMetricCollector &collector = collector_instance();
    std::string daemon = "test_daemon";
    std::string expectedCounterDump = "";
    std::string expectedCounterSchema = "";
    std::string metricName = "ceph_daemon_socket_up";

    // Fake admin socket
    std::string asok_path = "/tmp/" + daemon + ".asok";
    std::unique_ptr<AdminSocket> asokc = std::make_unique<AdminSocket>(g_ceph_context);
    AdminSocketClient client(asok_path);

    // Add the daemon clients to the collector
    clients.insert({daemon, std::move(client)});
    collector.clients = clients;

    auto verifyMetricValue = [&](const std::string &metricValue, bool shouldInitializeSocket) {
        collector.metrics = "";

        if (shouldInitializeSocket) {
            AdminSocketTest asoct(asokc.get());
            ASSERT_TRUE(asoct.init(asok_path));
        }

        collector.dump_asok_metrics(true, 5, true, expectedCounterDump, expectedCounterSchema, false);

        if (shouldInitializeSocket) {
            AdminSocketTest asoct(asokc.get());
            ASSERT_TRUE(asoct.shutdown());
        }

        std::string retrievedMetrics = collector.metrics;
        std::string pattern = metricName + R"(\{[^}]*ceph_daemon=\")" + daemon + R"(\"[^}]*\}\s+)" + metricValue + R"(\b)";
        std::regex regexPattern(pattern);
        ASSERT_TRUE(std::regex_search(retrievedMetrics, regexPattern));
    };

    // Test an admin socket not answering: metric value should be "0"
    verifyMetricValue("0", false);

    // Test an admin socket answering: metric value should be "1"
    verifyMetricValue("1", true);
}
