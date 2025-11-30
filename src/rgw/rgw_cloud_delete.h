// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <map>
#include <optional>
#include <atomic>
#include "rgw_common.h"
#include "common/Thread.h"
#include "common/Formatter.h"
#include "common/ceph_mutex.h"

#define RGW_CLOUD_DELETE_MAX_SHARDS 7877

namespace rgw::sal {
class Driver;
}

namespace rgw::cloud_delete {

// Persisted entry stored in the cloud-delete FIFO queue for async remote deletion
struct CloudDeleteEntry {
  rgw_bucket src_bucket;
  rgw_obj_key src_key;
  std::string src_version_id;
  std::string target_bucket_name;
  rgw_placement_rule placement_rule;  // tier lookup key (name + storage_class)
  bool target_by_bucket{false};       // from manifest, for cloud object naming
  uint32_t retry_count{0};
  ceph::real_time enqueue_time;
  ceph::real_time next_retry_time;  // Don't process before this time (exponential backoff)
  uint64_t pass_id{0};  // boundary tag: set by the processor to detect re-enqueued entries
  std::string src_etag;  // source object etag at enqueue time

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(src_bucket, bl);
    encode(src_key, bl);
    encode(src_version_id, bl);
    encode(target_bucket_name, bl);
    encode(placement_rule, bl);
    encode(target_by_bucket, bl);
    encode(retry_count, bl);
    encode(enqueue_time, bl);
    encode(next_retry_time, bl);
    encode(pass_id, bl);
    encode(src_etag, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(src_bucket, bl);
    decode(src_key, bl);
    decode(src_version_id, bl);
    decode(target_bucket_name, bl);
    decode(placement_rule, bl);
    decode(target_by_bucket, bl);
    decode(retry_count, bl);
    decode(enqueue_time, bl);
    decode(next_retry_time, bl);
    decode(pass_id, bl);
    decode(src_etag, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
};
WRITE_CLASS_ENCODER(CloudDeleteEntry)

class CloudDelete : public DoutPrefixProvider {
  CephContext *cct{nullptr};
  rgw::sal::Driver* driver{nullptr};
  std::unique_ptr<rgw::sal::CloudDelete> sal_cloud_delete;
  int max_objs{0};
  std::vector<std::string> obj_names;
  std::atomic<bool> down_flag{false};
  uint64_t next_pass_id{1};  // monotonic counter for boundary tagging

  class Worker : public Thread {
    CloudDelete *parent;
    ceph::mutex lock = ceph::make_mutex("CloudDelete::Worker");
    ceph::condition_variable cond;
  public:
    explicit Worker(CloudDelete *p) : parent(p) {}
    void *entry() override;
    void wake();
    std::string thr_name() { return std::string{"cloud_del_thrd"}; }
  };

  std::unique_ptr<Worker> worker;

public:
  ~CloudDelete();

  CloudDelete() = default;

  int initialize(CephContext *_cct, rgw::sal::Driver* _driver);
  int enqueue(const DoutPrefixProvider* dpp, optional_yield y,
              const CloudDeleteEntry& entry);
  void start_processor();
  void stop_processor();

  // Accessors for administrative listing of the shard FIFOs.
  int num_shards() const { return max_objs; }
  const std::string& shard_name(int index) const { return obj_names.at(index); }
  int list_entries(const DoutPrefixProvider* dpp, optional_yield y,
                   int index, const std::string& marker,
                   std::string* out_marker, uint32_t max_entries,
                   std::vector<CloudDeleteEntry>& entries,
                   bool* truncated);

  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const override;
  std::ostream& gen_prefix(std::ostream& out) const override;

  int process(optional_yield y, ceph::real_time* earliest_retry = nullptr);
};

// Lifetime: callers pass &check_objv.value() to del_op params, so this
// struct must outlive the delete operation that references it.
struct CloudDeleteContext {
  std::optional<CloudDeleteEntry> entry;  // pre-built (version_id filled post-delete)
  std::optional<obj_version> check_objv;  // version to check during delete
  /**
   * When versioning is suspended, a delete without version-id creates a null
   * delete marker that replaces the existing null version. The cloud copy of
   * that destroyed null version must be cleaned up even though the RADOS
   * result reports delete_marker=true.
   */
  bool enqueue_on_delete_marker{false};
};

std::unique_ptr<CloudDelete> make_cloud_delete();

CloudDeleteContext prepare_cloud_delete_context(
    const DoutPrefixProvider* dpp,
    rgw::sal::Driver* driver,
    rgw::sal::Object* obj,
    bool is_versioned_delete_marker_creation,
    const rgw_bucket& bucket,
    const rgw_owner& bucket_owner,
    optional_yield y,
    const rgw::sal::Attrs* preloaded_attrs = nullptr);

void maybe_set_check_objv(CloudDeleteContext& cloud_ctx,
                          obj_version** check_objv);

int maybe_enqueue_after_delete(const DoutPrefixProvider* dpp,
                               rgw::sal::Driver* driver,
                               CloudDeleteContext& cloud_ctx,
                               const std::string& version_id,
                               bool delete_succeeded,
                               bool delete_marker,
                               optional_yield y);

} // namespace rgw::cloud_delete
