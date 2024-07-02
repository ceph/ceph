// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_QCOW_FORMAT_H
#define CEPH_LIBRBD_MIGRATION_QCOW_FORMAT_H

#include "include/int_types.h"
#include "librbd/Types.h"
#include "librbd/migration/FormatInterface.h"
#include "librbd/migration/QCOW.h"
#include "acconfig.h"
#include "json_spirit/json_spirit.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <deque>
#include <vector>
#include <memory>

struct Context;

namespace librbd {

struct AsioEngine;
struct ImageCtx;

namespace migration {

template <typename> struct SourceSpecBuilder;
struct StreamInterface;

namespace qcow_format {

struct LookupTable {
  LookupTable() {}
  LookupTable(uint32_t size) : size(size) {}

  bufferlist bl;
  uint64_t* cluster_offsets = nullptr;
  uint32_t size = 0;
  bool decoded = false;

  void init();
  void decode();
};

} // namespace qcow_format

template <typename ImageCtxT>
class QCOWFormat : public FormatInterface<ImageCtxT> {
public:
  static QCOWFormat* create(
      const json_spirit::mObject& json_object,
      const SourceSpecBuilder<ImageCtxT>* source_spec_builder) {
    return new QCOWFormat(json_object, source_spec_builder);
  }

  QCOWFormat(const json_spirit::mObject& json_object,
             const SourceSpecBuilder<ImageCtxT>* source_spec_builder);
  QCOWFormat(const QCOWFormat&) = delete;
  QCOWFormat& operator=(const QCOWFormat&) = delete;

  void open(librados::IoCtx& dst_io_ctx, ImageCtxT* dst_image_ctx,
            ImageCtxT** src_image_ctx, Context* on_finish) override;
  void close(Context* on_finish) override;

  void get_snapshots(SnapInfos* snap_infos, Context* on_finish) override;
  void get_image_size(uint64_t snap_id, uint64_t* size,
                      Context* on_finish) override;

  bool read(io::AioCompletion* aio_comp, uint64_t snap_id,
            io::Extents&& image_extents, io::ReadResult&& read_result,
            int op_flags, int read_flags,
            const ZTracer::Trace &parent_trace) override;

  void list_snaps(io::Extents&& image_extents, io::SnapIds&& snap_ids,
                  int list_snaps_flags, io::SnapshotDelta* snapshot_delta,
                  const ZTracer::Trace &parent_trace,
                  Context* on_finish) override;

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   *  OPEN
   *    |
   *    v
   *  PROBE
   *    |
   *    |\---> READ V1 HEADER ----------\
   *    |                               |
   *    \----> READ V2 HEADER           |
   *              |                     |
   *              |     /----------\    |
   *              |     |          |    |
   *              v     v          |    |
   *           READ SNAPSHOT       |    |
   *              |                |    |
   *              v                |    |
   *           READ SNAPSHOT EXTRA |    |
   *              |                |    |
   *              v                |    |
   *           READ SNAPSHOT L1 TABLE   |
   *              |                     |
   *              \--------------------\|
   *                                    |
   *                                    v
   *                              READ L1 TABLE
   *                                    |
   *                                    v
   *                              READ BACKING FILE
   *                                    |
   *    /-------------------------------/
   *    |
   *    v
   * <opened>
   *
   * @endverbatim
   */

  struct Cluster;
  struct ClusterCache;
  struct L2TableCache;
  struct ReadRequest;
  struct ListSnapsRequest;

  struct Snapshot {
    std::string id;
    std::string name;

    utime_t timestamp;
    uint64_t size = 0;

    uint64_t l1_table_offset = 0;
    qcow_format::LookupTable l1_table;

    uint32_t extra_data_size = 0;
  };

  ImageCtxT* m_image_ctx;
  json_spirit::mObject m_json_object;
  const SourceSpecBuilder<ImageCtxT>* m_source_spec_builder;

  std::shared_ptr<StreamInterface> m_stream;

  bufferlist m_bl;

  uint64_t m_size = 0;

  uint64_t m_backing_file_offset = 0;
  uint32_t m_backing_file_size = 0;

  uint32_t m_cluster_bits = 0;
  uint32_t m_cluster_size = 0;
  uint64_t m_cluster_offset_mask = 0;
  uint64_t m_cluster_mask = 0;

  uint32_t m_l1_shift = 0;
  uint64_t m_l1_table_offset = 0;
  qcow_format::LookupTable m_l1_table;

  uint32_t m_l2_bits = 0;
  uint32_t m_l2_size = 0;

  uint32_t m_snapshot_count = 0;
  uint64_t m_snapshots_offset = 0;
  std::map<uint64_t, Snapshot> m_snapshots;

  std::unique_ptr<L2TableCache> m_l2_table_cache;
  std::unique_ptr<ClusterCache> m_cluster_cache;

  void handle_open(int r, Context* on_finish);

  void probe(Context* on_finish);
  void handle_probe(int r, Context* on_finish);

#ifdef WITH_RBD_MIGRATION_FORMAT_QCOW_V1
  void read_v1_header(Context* on_finish);
  void handle_read_v1_header(int r, Context* on_finish);
#endif // WITH_RBD_MIGRATION_FORMAT_QCOW_V1

  void read_v2_header(Context* on_finish);
  void handle_read_v2_header(int r, Context* on_finish);

  void read_snapshot(Context* on_finish);
  void handle_read_snapshot(int r, Context* on_finish);

  void read_snapshot_extra(Context* on_finish);
  void handle_read_snapshot_extra(int r, Context* on_finish);

  void read_snapshot_l1_table(Context* on_finish);
  void handle_read_snapshot_l1_table(int r, Context* on_finish);

  void read_l1_table(Context* on_finish);
  void handle_read_l1_table(int r, Context* on_finish);

  void read_backing_file(Context* on_finish);

  void handle_list_snaps(int r, io::Extents&& image_extents,
                         io::SnapIds&& snap_ids,
                         io::SnapshotDelta* snapshot_delta, Context* on_finish);
};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::QCOWFormat<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_QCOW_FORMAT_H
