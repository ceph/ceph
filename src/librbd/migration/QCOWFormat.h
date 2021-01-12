// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_QCOW_FORMAT_H
#define CEPH_LIBRBD_MIGRATION_QCOW_FORMAT_H

#include "include/int_types.h"
#include "librbd/Types.h"
#include "librbd/migration/FormatInterface.h"
#include "librbd/migration/QCOW.h"
#include "json_spirit/json_spirit.h"
#include <boost/asio/io_context_strand.hpp>
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

template <typename ImageCtxT>
class QCOWFormat : public FormatInterface {
public:
  static QCOWFormat* create(
      ImageCtxT* image_ctx, const json_spirit::mObject& json_object,
      const SourceSpecBuilder<ImageCtxT>* source_spec_builder) {
    return new QCOWFormat(image_ctx, json_object, source_spec_builder);
  }

  QCOWFormat(ImageCtxT* image_ctx, const json_spirit::mObject& json_object,
             const SourceSpecBuilder<ImageCtxT>* source_spec_builder);
  QCOWFormat(const QCOWFormat&) = delete;
  QCOWFormat& operator=(const QCOWFormat&) = delete;

  void open(Context* on_finish) override;
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
   *    |\---> READ V1 HEADER ----\
   *    |                         |
   *    \---> READ V2 HEADER ----\|
   *                              |
   *                              v
   *                        READ L1 TABLE
   *                              |
   *                              v
   *                        READ BACKING FILE
   *                              |
   *    /-------------------------/
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

  ImageCtxT* m_image_ctx;
  json_spirit::mObject m_json_object;
  const SourceSpecBuilder<ImageCtxT>* m_source_spec_builder;

  boost::asio::io_context::strand m_strand;
  std::shared_ptr<StreamInterface> m_stream;

  bufferlist m_bl;

  uint64_t m_size = 0;

  uint64_t m_backing_file_offset = 0;
  uint32_t m_backing_file_size = 0;

  uint32_t m_cluster_bits = 0;
  uint32_t m_cluster_size = 0;
  uint64_t m_cluster_offset_mask = 0;
  uint64_t m_cluster_mask = 0;

  uint32_t m_l1_size = 0;
  uint64_t m_l1_table_offset = 0;
  uint64_t* m_l1_table = nullptr;
  bufferlist m_l1_table_bl;

  std::unique_ptr<L2TableCache> m_l2_table_cache;
  std::unique_ptr<ClusterCache> m_cluster_cache;

  void handle_open(int r, Context* on_finish);

  void probe(Context* on_finish);
  void handle_probe(int r, Context* on_finish);

  void read_v1_header(Context* on_finish);
  void handle_read_v1_header(int r, Context* on_finish);

  void read_v2_header(Context* on_finish);
  void handle_read_v2_header(int r, Context* on_finish);

  void read_l1_table(Context* on_finish);
  void handle_read_l1_table(int r, Context* on_finish);

  void read_backing_file(Context* on_finish);
};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::QCOWFormat<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_QCOW_FORMAT_H
