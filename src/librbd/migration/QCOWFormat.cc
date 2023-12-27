// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/QCOWFormat.h"
#include "common/Clock.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/intarith.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ReadResult.h"
#include "librbd/migration/SnapshotInterface.h"
#include "librbd/migration/SourceSpecBuilder.h"
#include "librbd/migration/StreamInterface.h"
#include "librbd/migration/Utils.h"
#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/endian/conversion.hpp>
#include <deque>
#include <tuple>
#include <unordered_map>
#include <vector>

#define dout_subsys ceph_subsys_rbd

namespace librbd {
namespace migration {

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::QCOWFormat: " \
                           << __func__ << ": "

using boost::endian::big_to_native;

namespace qcow_format {

struct ClusterExtent {
  uint64_t cluster_offset;
  uint64_t cluster_length;
  uint64_t intra_cluster_offset;
  uint64_t image_offset;
  uint64_t buffer_offset;

  ClusterExtent(uint64_t cluster_offset, uint64_t cluster_length,
                uint64_t intra_cluster_offset, uint64_t image_offset,
                uint64_t buffer_offset)
    : cluster_offset(cluster_offset), cluster_length(cluster_length),
      intra_cluster_offset(intra_cluster_offset), image_offset(image_offset),
      buffer_offset(buffer_offset) {
  }
};

typedef std::vector<ClusterExtent> ClusterExtents;

void LookupTable::init() {
  if (cluster_offsets == nullptr) {
    cluster_offsets = reinterpret_cast<uint64_t*>(bl.c_str());
  }
}

void LookupTable::decode() {
  init();

  // L2 tables are selectively byte-swapped on demand if only requesting a
  // single cluster offset
  if (decoded) {
    return;
  }

  // translate the lookup table (big-endian -> CPU endianness)
  for (auto idx = 0UL; idx < size; ++idx) {
    cluster_offsets[idx] = big_to_native(cluster_offsets[idx]);
  }

  decoded = true;
}

void populate_cluster_extents(CephContext* cct, uint64_t cluster_size,
                              const io::Extents& image_extents,
                              ClusterExtents* cluster_extents) {
  uint64_t buffer_offset = 0;
  for (auto [image_offset, image_length] : image_extents) {
    while (image_length > 0) {
      auto intra_cluster_offset = image_offset & (cluster_size - 1);
      auto intra_cluster_length = cluster_size - intra_cluster_offset;
      auto cluster_length = std::min(image_length, intra_cluster_length);

      ldout(cct, 20) << "image_offset=" << image_offset << ", "
                     << "image_length=" << image_length << ", "
                     << "cluster_length=" << cluster_length << dendl;


      cluster_extents->emplace_back(0, cluster_length, intra_cluster_offset,
                                   image_offset, buffer_offset);

      image_offset += cluster_length;
      image_length -= cluster_length;
      buffer_offset += cluster_length;
    }
  }
}

} // namespace qcow_format

using namespace qcow_format;

template <typename I>
struct QCOWFormat<I>::Cluster {
  const uint64_t cluster_offset;
  bufferlist cluster_data_bl;

  Cluster(uint64_t cluster_offset) : cluster_offset(cluster_offset) {
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::QCOWFormat::ClusterCache: " \
                           << this << " " << __func__ << ": "

template <typename I>
class QCOWFormat<I>::ClusterCache {
public:
  ClusterCache(QCOWFormat* qcow_format)
    : qcow_format(qcow_format),
      m_strand(boost::asio::make_strand(
        *qcow_format->m_image_ctx->asio_engine)) {
  }

  void get_cluster(uint64_t cluster_offset, uint64_t cluster_length,
                   uint64_t intra_cluster_offset, bufferlist* bl,
                   Context* on_finish) {
    auto cct = qcow_format->m_image_ctx->cct;
    ldout(cct, 20) << "cluster_offset=" << cluster_offset << dendl;

    // cache state machine runs in a single strand thread
    boost::asio::dispatch(
      m_strand,
      [this, cluster_offset, cluster_length, intra_cluster_offset, bl,
       on_finish]() {
        execute_get_cluster(cluster_offset, cluster_length,
                            intra_cluster_offset, bl, on_finish);
      });
  }

private:
  typedef std::tuple<uint64_t, uint64_t, bufferlist*, Context*> Completion;
  typedef std::list<Completion> Completions;

  QCOWFormat* qcow_format;
  boost::asio::strand<boost::asio::io_context::executor_type> m_strand;

  std::shared_ptr<Cluster> cluster;
  std::unordered_map<uint64_t, Completions> cluster_completions;

  void execute_get_cluster(uint64_t cluster_offset, uint64_t cluster_length,
                           uint64_t intra_cluster_offset, bufferlist* bl,
                           Context* on_finish) {
    auto cct = qcow_format->m_image_ctx->cct;
    ldout(cct, 20) << "cluster_offset=" << cluster_offset << dendl;

    if (cluster && cluster->cluster_offset == cluster_offset) {
      // most-recent cluster matches
      bl->substr_of(cluster->cluster_data_bl, intra_cluster_offset,
                    cluster_length);
      boost::asio::post(*qcow_format->m_image_ctx->asio_engine,
                        [on_finish]() { on_finish->complete(0); });
      return;
    }

    // record callback for cluster
    bool new_request = (cluster_completions.count(cluster_offset) == 0);
    cluster_completions[cluster_offset].emplace_back(
      intra_cluster_offset, cluster_length, bl, on_finish);
    if (new_request) {
      // start the new read request
      read_cluster(std::make_shared<Cluster>(cluster_offset));
    }
  }

  void read_cluster(std::shared_ptr<Cluster> cluster) {
    auto cct = qcow_format->m_image_ctx->cct;

    uint64_t stream_offset = cluster->cluster_offset;
    uint64_t stream_length = qcow_format->m_cluster_size;
    if ((cluster->cluster_offset & QCOW_OFLAG_COMPRESSED) != 0) {
      // compressed clusters encode the compressed length in the lower bits
      stream_offset = cluster->cluster_offset &
                      qcow_format->m_cluster_offset_mask;
      stream_length = (cluster->cluster_offset >>
                        (63 - qcow_format->m_cluster_bits)) &
                      (qcow_format->m_cluster_size - 1);
    }

    ldout(cct, 20) << "cluster_offset=" << cluster->cluster_offset << ", "
                   << "stream_offset=" << stream_offset << ", "
                   << "stream_length=" << stream_length << dendl;

    // read the cluster into the cache entry
    auto ctx = new LambdaContext([this, cluster](int r) {
      boost::asio::post(m_strand, [this, cluster, r]() {
        handle_read_cluster(r, cluster); }); });
    qcow_format->m_stream->read({{stream_offset, stream_length}},
                                &cluster->cluster_data_bl, ctx);
  }

  void handle_read_cluster(int r, std::shared_ptr<Cluster> cluster) {
    auto cct = qcow_format->m_image_ctx->cct;
    ldout(cct, 20) << "r=" << r << ", "
                   << "cluster_offset=" << cluster->cluster_offset << dendl;

    auto completions = std::move(cluster_completions[cluster->cluster_offset]);
    cluster_completions.erase(cluster->cluster_offset);

    if (r < 0) {
      lderr(cct) << "failed to read cluster offset " << cluster->cluster_offset
                 << ": " << cpp_strerror(r) << dendl;
    } else {
      if ((cluster->cluster_offset & QCOW_OFLAG_COMPRESSED) != 0) {
        bufferlist compressed_bl{std::move(cluster->cluster_data_bl)};
        cluster->cluster_data_bl.clear();

        // TODO
        lderr(cct) << "support for compressed clusters is not available"
                   << dendl;
        r = -EINVAL;
      } else {
        // cache the MRU cluster in case of sequential IO
        this->cluster = cluster;
      }
    }

    // complete the IO back to caller
    boost::asio::post(*qcow_format->m_image_ctx->asio_engine,
                      [r, cluster, completions=std::move(completions)]() {
      for (auto completion : completions) {
        if (r >= 0) {
          std::get<2>(completion)->substr_of(
            cluster->cluster_data_bl,
            std::get<0>(completion),
            std::get<1>(completion));
        }
        std::get<3>(completion)->complete(r);
      }
    });
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::QCOWFormat::L2TableCache: " \
                           << this << " " << __func__ << ": "

template <typename I>
class QCOWFormat<I>::L2TableCache {
public:
  L2TableCache(QCOWFormat* qcow_format)
    : qcow_format(qcow_format),
      m_strand(boost::asio::make_strand(
        *qcow_format->m_image_ctx->asio_engine)),
      l2_cache_entries(QCOW_L2_CACHE_SIZE) {
  }

  void get_l2_table(const LookupTable* l1_table, uint64_t l2_table_offset,
                    std::shared_ptr<const LookupTable>* l2_table,
                    Context* on_finish) {
    auto cct = qcow_format->m_image_ctx->cct;
    ldout(cct, 20) << "l2_table_offset=" << l2_table_offset << dendl;

    // cache state machine runs in a single strand thread
    Request request{l1_table, l2_table_offset, l2_table, on_finish};
    boost::asio::dispatch(
      m_strand, [this, request=std::move(request)]() {
        requests.push_back(std::move(request));
      });
    dispatch_request();
  }

  void get_cluster_offset(const LookupTable* l1_table,
                          uint64_t image_offset, uint64_t* cluster_offset,
                          Context* on_finish) {
    auto cct = qcow_format->m_image_ctx->cct;
    uint32_t l1_table_index = image_offset >> qcow_format->m_l1_shift;
    uint64_t l2_table_offset = l1_table->cluster_offsets[std::min<uint32_t>(
                                 l1_table_index, l1_table->size - 1)] &
                               qcow_format->m_cluster_mask;
    uint32_t l2_table_index = (image_offset >> qcow_format->m_cluster_bits) &
                              (qcow_format->m_l2_size - 1);
    ldout(cct, 20) << "image_offset=" << image_offset << ", "
                   << "l1_table_index=" << l1_table_index << ", "
                   << "l2_table_offset=" << l2_table_offset << ", "
                   << "l2_table_index=" << l2_table_index << dendl;

    if (l1_table_index >= l1_table->size) {
      lderr(cct) << "L1 index " << l1_table_index << " out-of-bounds" << dendl;
      on_finish->complete(-ERANGE);
      return;
    } else if (l2_table_offset == 0) {
      // L2 table has not been allocated for specified offset
      ldout(cct, 20) << "image_offset=" << image_offset << ", "
                     << "cluster_offset=DNE" << dendl;
      *cluster_offset = 0;
      on_finish->complete(-ENOENT);
      return;
    }

    // cache state machine runs in a single strand thread
    Request request{l1_table, l2_table_offset, l2_table_index, cluster_offset,
                    on_finish};
    boost::asio::dispatch(
      m_strand, [this, request=std::move(request)]() {
        requests.push_back(std::move(request));
      });
    dispatch_request();
  }

private:
  QCOWFormat* qcow_format;

  boost::asio::strand<boost::asio::io_context::executor_type> m_strand;

  struct Request {
    const LookupTable* l1_table;

    uint64_t l2_table_offset;

    // get_cluster_offset request
    uint32_t l2_table_index;
    uint64_t* cluster_offset = nullptr;

    // get_l2_table request
    std::shared_ptr<const LookupTable>* l2_table;

    Context* on_finish;

    Request(const LookupTable* l1_table, uint64_t l2_table_offset,
            uint32_t l2_table_index, uint64_t* cluster_offset,
            Context* on_finish)
      : l1_table(l1_table), l2_table_offset(l2_table_offset),
        l2_table_index(l2_table_index), cluster_offset(cluster_offset),
        on_finish(on_finish) {
    }
    Request(const LookupTable* l1_table, uint64_t l2_table_offset,
            std::shared_ptr<const LookupTable>* l2_table, Context* on_finish)
      : l1_table(l1_table), l2_table_offset(l2_table_offset),
        l2_table(l2_table), on_finish(on_finish) {
    }
  };

  typedef std::deque<Request> Requests;

  struct L2Cache {
    uint64_t l2_offset = 0;
    std::shared_ptr<LookupTable> l2_table;

    utime_t timestamp;
    uint32_t count = 0;
    bool in_flight = false;

    int ret_val = 0;
  };
  std::vector<L2Cache> l2_cache_entries;

  Requests requests;

  void dispatch_request() {
    boost::asio::dispatch(m_strand, [this]() { execute_request(); });
  }

  void execute_request() {
    auto cct = qcow_format->m_image_ctx->cct;
    if (requests.empty()) {
      return;
    }

    auto request = requests.front();
    ldout(cct, 20) << "l2_table_offset=" << request.l2_table_offset << dendl;

    std::shared_ptr<LookupTable> l2_table;
    int r = l2_table_lookup(request.l2_table_offset, &l2_table);
    if (r < 0) {
      lderr(cct) << "failed to load L2 table: l2_table_offset="
                 << request.l2_table_offset << ": "
                 << cpp_strerror(r) << dendl;
    } else if (l2_table == nullptr) {
      // table not in cache -- will restart once its loaded
      return;
    } else if (request.cluster_offset != nullptr) {
      auto cluster_offset = l2_table->cluster_offsets[request.l2_table_index];
      if (!l2_table->decoded) {
        // table hasn't been byte-swapped
        cluster_offset = big_to_native(cluster_offset);
      }

      *request.cluster_offset = cluster_offset & qcow_format->m_cluster_mask;
      if (*request.cluster_offset == QCOW_OFLAG_ZERO) {
        ldout(cct, 20) << "l2_table_offset=" << request.l2_table_offset << ", "
                       << "l2_table_index=" << request.l2_table_index << ", "
                       << "cluster_offset=zeroed" << dendl;
      } else {
        ldout(cct, 20) << "l2_table_offset=" << request.l2_table_offset << ", "
                       << "l2_table_index=" << request.l2_table_index << ", "
                       << "cluster_offset=" << *request.cluster_offset
                       << dendl;
      }
    } else if (request.l2_table != nullptr) {
      // ensure it's in the correct byte-order
      l2_table->decode();
      *request.l2_table = l2_table;
    } else {
      ceph_assert(false);
    }

    // complete the L2 cache request
    boost::asio::post(*qcow_format->m_image_ctx->asio_engine,
                      [r, ctx=request.on_finish]() { ctx->complete(r); });
    requests.pop_front();

    // process next request (if any)
    dispatch_request();
  }

  int l2_table_lookup(uint64_t l2_offset,
                      std::shared_ptr<LookupTable>* l2_table) {
    auto cct = qcow_format->m_image_ctx->cct;

    l2_table->reset();

    // find a match in the existing cache
    for (auto idx = 0U; idx < l2_cache_entries.size(); ++idx) {
      auto& l2_cache = l2_cache_entries[idx];
      if (l2_cache.l2_offset == l2_offset) {
        if (l2_cache.in_flight) {
          ldout(cct, 20) << "l2_offset=" << l2_offset << ", "
                         << "index=" << idx << " (in-flight)" << dendl;
          return 0;
        }

        if (l2_cache.ret_val < 0) {
          ldout(cct, 20) << "l2_offset=" << l2_offset << ", "
                         << "index=" << idx << " (error): "
                         << cpp_strerror(l2_cache.ret_val) << dendl;
          int r = l2_cache.ret_val;
          l2_cache = L2Cache{};

          return r;
        }

        ++l2_cache.count;
        if (l2_cache.count == std::numeric_limits<uint32_t>::max()) {
          for (auto& entry : l2_cache_entries) {
            entry.count >>= 1;
          }
        }

        ldout(cct, 20) << "l2_offset=" << l2_offset << ", " << "index=" << idx
                       << dendl;
        *l2_table = l2_cache.l2_table;
        return 0;
      }
    }

    // find the least used entry
    int32_t min_idx = -1;
    uint32_t min_count = std::numeric_limits<uint32_t>::max();
    utime_t min_timestamp;
    for (uint32_t idx = 0U; idx < l2_cache_entries.size(); ++idx) {
      auto& l2_cache = l2_cache_entries[idx];
      if (l2_cache.in_flight) {
        continue;
      }

      if (l2_cache.count > 0) {
        --l2_cache.count;
      }

      if (l2_cache.count <= min_count) {
        if (min_idx == -1 || l2_cache.timestamp < min_timestamp) {
          min_timestamp = l2_cache.timestamp;
          min_count = l2_cache.count;
          min_idx = idx;
        }
      }
    }

    if (min_idx == -1) {
      // no space in the cache due to in-flight requests
      ldout(cct, 20) << "l2_offset=" << l2_offset << ", "
                     << "index=DNE (cache busy)" << dendl;
      return 0;
    }

    ldout(cct, 20) << "l2_offset=" << l2_offset << ", "
                   << "index=" << min_idx << " (loading)" << dendl;
    auto& l2_cache = l2_cache_entries[min_idx];
    l2_cache.l2_table = std::make_shared<LookupTable>(qcow_format->m_l2_size);
    l2_cache.l2_offset = l2_offset;
    l2_cache.timestamp = ceph_clock_now();
    l2_cache.count = 1;
    l2_cache.in_flight = true;

    // read the L2 table into the L2 cache entry
    auto ctx = new LambdaContext([this, index=min_idx, l2_offset](int r) {
      boost::asio::post(m_strand, [this, index, l2_offset, r]() {
        handle_l2_table_lookup(r, index, l2_offset); }); });
    qcow_format->m_stream->read(
      {{l2_offset, qcow_format->m_l2_size * sizeof(uint64_t)}},
      &l2_cache.l2_table->bl, ctx);
    return 0;
  }

  void handle_l2_table_lookup(int r, uint32_t index, uint64_t l2_offset) {
    auto cct = qcow_format->m_image_ctx->cct;
    ldout(cct, 20) << "r=" << r << ", "
                   << "l2_offset=" << l2_offset << ", "
                   << "index=" << index << dendl;

    auto& l2_cache = l2_cache_entries[index];
    ceph_assert(l2_cache.in_flight);
    l2_cache.in_flight = false;

    if (r < 0) {
      lderr(cct) << "failed to load L2 table: "
                 << "l2_offset=" << l2_cache.l2_offset << ": "
                 << cpp_strerror(r) << dendl;
      l2_cache.ret_val = r;
    } else {
      // keep the L2 table in big-endian byte-order until the full table
      // is requested
      l2_cache.l2_table->init();
    }

    // restart the state machine
    dispatch_request();
  }

};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::QCOWFormat::ReadRequest: " \
                           << this << " " << __func__ << ": "

template <typename I>
class QCOWFormat<I>::ReadRequest {
public:
  ReadRequest(QCOWFormat* qcow_format, io::AioCompletion* aio_comp,
              const LookupTable* l1_table, io::Extents&& image_extents)
    : qcow_format(qcow_format), aio_comp(aio_comp), l1_table(l1_table),
      image_extents(std::move(image_extents)) {
  }

  void send() {
    get_cluster_offsets();
  }

private:
  QCOWFormat* qcow_format;
  io::AioCompletion* aio_comp;

  const LookupTable* l1_table;
  io::Extents image_extents;

  size_t image_extents_idx = 0;
  uint32_t image_extent_offset = 0;

  ClusterExtents cluster_extents;

  void get_cluster_offsets() {
    auto cct = qcow_format->m_image_ctx->cct;
    populate_cluster_extents(cct, qcow_format->m_cluster_size, image_extents,
                             &cluster_extents);

    ldout(cct, 20) << dendl;
    auto ctx = new LambdaContext([this](int r) {
      handle_get_cluster_offsets(r); });
    auto gather_ctx = new C_Gather(cct, ctx);

    for (auto& cluster_extent : cluster_extents) {
      auto sub_ctx = new LambdaContext(
        [this, &cluster_extent, on_finish=gather_ctx->new_sub()](int r) {
          handle_get_cluster_offset(r, cluster_extent, on_finish); });
      qcow_format->m_l2_table_cache->get_cluster_offset(
        l1_table, cluster_extent.image_offset,
        &cluster_extent.cluster_offset, sub_ctx);
    }

    gather_ctx->activate();
  }

  void handle_get_cluster_offset(int r, const ClusterExtent& cluster_extent,
                                 Context* on_finish) {
    auto cct = qcow_format->m_image_ctx->cct;
    ldout(cct, 20) << "r=" << r << ", "
                   << "image_offset=" << cluster_extent.image_offset << ", "
                   << "cluster_offset=" << cluster_extent.cluster_offset
                   << dendl;

    if (r == -ENOENT) {
      ldout(cct, 20) << "image offset DNE in QCOW image" << dendl;
      r = 0;
    } else if (r < 0) {
      lderr(cct) << "failed to map image offset " << cluster_extent.image_offset
                 << ": " << cpp_strerror(r) << dendl;
    }

    on_finish->complete(r);
  }

  void handle_get_cluster_offsets(int r) {
    auto cct = qcow_format->m_image_ctx->cct;
    ldout(cct, 20) << "r=" << r << dendl;

    if (r < 0) {
      lderr(cct) << "failed to retrieve cluster extents: " << cpp_strerror(r)
                 << dendl;
      aio_comp->fail(r);
      delete this;
      return;
    }

    read_clusters();
  }

  void read_clusters() {
    auto cct = qcow_format->m_image_ctx->cct;
    ldout(cct, 20) << dendl;

    aio_comp->set_request_count(cluster_extents.size());
    for (auto& cluster_extent : cluster_extents) {
      auto read_ctx = new io::ReadResult::C_ImageReadRequest(
        aio_comp, cluster_extent.buffer_offset,
        {{cluster_extent.image_offset, cluster_extent.cluster_length}});
      read_ctx->ignore_enoent = true;

      auto log_ctx = new LambdaContext(
        [this, cct=qcow_format->m_image_ctx->cct,
         image_offset=cluster_extent.image_offset,
         image_length=cluster_extent.cluster_length, ctx=read_ctx](int r) {
          handle_read_cluster(cct, r, image_offset, image_length, ctx);
        });

      if (cluster_extent.cluster_offset == 0) {
        // QCOW header is at offset 0, implies cluster DNE
        log_ctx->complete(-ENOENT);
      } else if (cluster_extent.cluster_offset == QCOW_OFLAG_ZERO) {
        // explicitly zeroed section
        read_ctx->bl.append_zero(cluster_extent.cluster_length);
        log_ctx->complete(0);
      } else {
        // request the (sub)cluster from the cluster cache
        qcow_format->m_cluster_cache->get_cluster(
          cluster_extent.cluster_offset, cluster_extent.cluster_length,
          cluster_extent.intra_cluster_offset, &read_ctx->bl, log_ctx);
      }
    }

    delete this;
  }

  void handle_read_cluster(CephContext* cct, int r, uint64_t image_offset,
                           uint64_t image_length, Context* on_finish) const {
    // NOTE: treat as static function, expect object has been deleted

    ldout(cct, 20) << "r=" << r << ", "
                   << "image_offset=" << image_offset << ", "
                   << "image_length=" << image_length << dendl;

    if (r != -ENOENT && r < 0) {
      lderr(cct) << "failed to read image extent " << image_offset << "~"
                 << image_length << ": " << cpp_strerror(r) << dendl;
    }

    on_finish->complete(r);
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::QCOWFormat::" \
                           << "ListSnapsRequest: " << this << " " \
                           << __func__ << ": "

template <typename I>
class QCOWFormat<I>::ListSnapsRequest {
public:
  ListSnapsRequest(
      QCOWFormat* qcow_format, uint32_t l1_table_index,
      ClusterExtents&& cluster_extents,
      const std::map<uint64_t, const LookupTable*>& snap_id_to_l1_table,
      io::SnapshotDelta* snapshot_delta, Context* on_finish)
    : qcow_format(qcow_format),
      m_strand(
              boost::asio::make_strand(*qcow_format->m_image_ctx->asio_engine)),
      l1_table_index(l1_table_index),
      cluster_extents(std::move(cluster_extents)),
      snap_id_to_l1_table(snap_id_to_l1_table), snapshot_delta(snapshot_delta),
      on_finish(on_finish) {
  }

  void send() {
    get_l2_table();
  }

private:
  QCOWFormat* qcow_format;
  boost::asio::strand<boost::asio::io_context::executor_type> m_strand;
  uint32_t l1_table_index;
  ClusterExtents cluster_extents;
  std::map<uint64_t, const LookupTable*> snap_id_to_l1_table;
  io::SnapshotDelta* snapshot_delta;
  Context* on_finish;

  std::shared_ptr<const LookupTable> previous_l2_table;
  std::shared_ptr<const LookupTable> l2_table;

  void get_l2_table() {
    auto cct = qcow_format->m_image_ctx->cct;
    if (snap_id_to_l1_table.empty()) {
      finish(0);
      return;
    }

    auto it = snap_id_to_l1_table.begin();
    auto [snap_id, l1_table] = *it;
    snap_id_to_l1_table.erase(it);

    previous_l2_table = l2_table;
    l2_table.reset();

    auto ctx = new LambdaContext([this, snap_id = snap_id](int r) {
      boost::asio::post(m_strand, [this, snap_id, r]() {
        handle_get_l2_table(r, snap_id);
        });
    });

    if (l1_table_index >= l1_table->size ||
        l1_table->cluster_offsets[l1_table_index] == 0) {
      ldout(cct, 20) << "l1_table_index=" << l1_table_index << ", "
                     << "snap_id=" << snap_id << ": DNE" << dendl;
      ctx->complete(-ENOENT);
      return;
    }

    uint64_t l2_table_offset = l1_table->cluster_offsets[l1_table_index] &
                               qcow_format->m_cluster_mask;

    ldout(cct, 20) << "l1_table_index=" << l1_table_index << ", "
                   << "snap_id=" << snap_id << ", "
                   << "l2_table_offset=" << l2_table_offset << dendl;
    qcow_format->m_l2_table_cache->get_l2_table(l1_table, l2_table_offset,
                                                &l2_table, ctx);
  }

  void handle_get_l2_table(int r, uint64_t snap_id) {
    ceph_assert(m_strand.running_in_this_thread());

    auto cct = qcow_format->m_image_ctx->cct;
    ldout(cct, 20) << "r=" << r << ", "
                   << "snap_id=" << snap_id << dendl;

    if (r == -ENOENT) {
      l2_table.reset();
    } else if (r < 0) {
      lderr(cct) << "failed to retrieve L2 table for snapshot " << snap_id
                 << ": " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    // compare the cluster offsets at each requested L2 offset between
    // the previous snapshot's L2 table and the current L2 table.
    auto& sparse_extents = (*snapshot_delta)[{snap_id, snap_id}];
    for (auto& cluster_extent : cluster_extents) {
      uint32_t l2_table_index =
        (cluster_extent.image_offset >> qcow_format->m_cluster_bits) &
        (qcow_format->m_l2_size - 1);

      std::optional<uint64_t> cluster_offset;
      if (l2_table && l2_table_index < l2_table->size) {
        cluster_offset = l2_table->cluster_offsets[l2_table_index] &
                         qcow_format->m_cluster_offset_mask;
      }

      std::optional<uint64_t> prev_cluster_offset;
      if (previous_l2_table && l2_table_index < previous_l2_table->size) {
        prev_cluster_offset =
          previous_l2_table->cluster_offsets[l2_table_index] &
          qcow_format->m_cluster_offset_mask;
      }

      ldout(cct, 20) << "l1_table_index=" << l1_table_index << ", "
                     << "snap_id=" << snap_id << ", "
                     << "image_offset=" << cluster_extent.image_offset << ", "
                     << "l2_table_index=" << l2_table_index << ", "
                     << "cluster_offset=" << cluster_offset << ", "
                     << "prev_cluster_offset=" << prev_cluster_offset << dendl;

      auto state = io::SPARSE_EXTENT_STATE_DATA;
      if (cluster_offset == prev_cluster_offset) {
        continue;
      } else if ((prev_cluster_offset && !cluster_offset) ||
                 *cluster_offset == QCOW_OFLAG_ZERO) {
        // explicitly zeroed or deallocated
        state = io::SPARSE_EXTENT_STATE_ZEROED;
      }

      sparse_extents.insert(
        cluster_extent.image_offset, cluster_extent.cluster_length,
        {state, cluster_extent.cluster_length});
    }

    ldout(cct, 20) << "l1_table_index=" << l1_table_index << ", "
                   << "snap_id=" << snap_id << ", "
                   << "sparse_extents=" << sparse_extents << dendl;

    // continue processing the L2 table at this index for all snapshots
    boost::asio::post(*qcow_format->m_image_ctx->asio_engine,
                      [this]() { get_l2_table(); });
  }


  void finish(int r) {
    auto cct = qcow_format->m_image_ctx->cct;
    ldout(cct, 20) << "r=" << r << dendl;

    on_finish->complete(r);
    delete this;
  }
};

#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::QCOWFormat: " << this \
                           << " " << __func__ << ": "

template <typename I>
QCOWFormat<I>::QCOWFormat(
    const json_spirit::mObject& json_object,
    const SourceSpecBuilder<I>* source_spec_builder)
  : m_json_object(json_object),
    m_source_spec_builder(source_spec_builder) {
}

template <typename I>
void QCOWFormat<I>::open(librados::IoCtx& dst_io_ctx, I* dst_image_ctx,
                         I** src_image_ctx, Context* on_finish) {
  ldout(reinterpret_cast<CephContext *>(dst_io_ctx.cct()), 10) << dendl;

  // create source image context
  *src_image_ctx = I::create("", "", CEPH_NOSNAP, dst_io_ctx, true);
  m_image_ctx = *src_image_ctx;
  m_image_ctx->child = dst_image_ctx;
  auto cct = m_image_ctx->cct;

  // use default layout values (placeholders)
  m_image_ctx->order = 22;
  m_image_ctx->layout = file_layout_t();
  m_image_ctx->layout.stripe_count = 1;
  m_image_ctx->layout.stripe_unit = 1ULL << m_image_ctx->order;
  m_image_ctx->layout.object_size = 1ULL << m_image_ctx->order;
  m_image_ctx->layout.pool_id = -1;

  int r = m_source_spec_builder->build_stream(*src_image_ctx, m_json_object,
                                              &m_stream);
  if (r < 0) {
    lderr(cct) << "failed to build migration stream handler" << cpp_strerror(r)
               << dendl;
    on_finish->complete(r);
    return;
  }

  auto ctx = new LambdaContext([this, on_finish](int r) {
    handle_open(r, on_finish); });
  m_stream->open(ctx);
}

template <typename I>
void QCOWFormat<I>::handle_open(int r, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to open QCOW image: " << cpp_strerror(r)
               << dendl;
    on_finish->complete(r);
    return;
  }

  probe(on_finish);
}

template <typename I>
void QCOWFormat<I>::probe(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto ctx = new LambdaContext([this, on_finish](int r) {
    handle_probe(r, on_finish); });
  m_bl.clear();
  m_stream->read({{0, 8}}, &m_bl, ctx);
}

template <typename I>
void QCOWFormat<I>::handle_probe(int r, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to probe QCOW image: " << cpp_strerror(r)
               << dendl;
    on_finish->complete(r);
    return;
  }

  auto header_probe = *reinterpret_cast<QCowHeaderProbe*>(
    m_bl.c_str());
  header_probe.magic = big_to_native(header_probe.magic);
  header_probe.version = big_to_native(header_probe.version);

  if (header_probe.magic != QCOW_MAGIC) {
    lderr(cct) << "invalid QCOW header magic" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  m_bl.clear();
  if (header_probe.version == 1) {
#ifdef WITH_RBD_MIGRATION_FORMAT_QCOW_V1
    read_v1_header(on_finish);
#else // WITH_RBD_MIGRATION_FORMAT_QCOW_V1
    lderr(cct) << "QCOW is not supported" << dendl;
    on_finish->complete(-ENOTSUP);
#endif // WITH_RBD_MIGRATION_FORMAT_QCOW_V1
    return;
  } else if (header_probe.version >= 2 && header_probe.version <= 3) {
    read_v2_header(on_finish);
    return;
  } else {
    lderr(cct) << "invalid QCOW header version " << header_probe.version
               << dendl;
    on_finish->complete(-EINVAL);
    return;
  }
}

#ifdef WITH_RBD_MIGRATION_FORMAT_QCOW_V1

template <typename I>
void QCOWFormat<I>::read_v1_header(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto ctx = new LambdaContext([this, on_finish](int r) {
    handle_read_v1_header(r, on_finish); });
  m_bl.clear();
  m_stream->read({{0, sizeof(QCowHeaderV1)}}, &m_bl, ctx);
}

template <typename I>
void QCOWFormat<I>::handle_read_v1_header(int r, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to read QCOW header: " << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  auto header = *reinterpret_cast<QCowHeaderV1*>(m_bl.c_str());

  // byte-swap important fields
  header.magic = big_to_native(header.magic);
  header.version = big_to_native(header.version);
  header.backing_file_offset = big_to_native(header.backing_file_offset);
  header.backing_file_size = big_to_native(header.backing_file_size);
  header.size = big_to_native(header.size);
  header.crypt_method = big_to_native(header.crypt_method);
  header.l1_table_offset = big_to_native(header.l1_table_offset);

  if (header.magic != QCOW_MAGIC || header.version != 1) {
    // honestly shouldn't happen since we've already validated it
    lderr(cct) << "header is not QCOW" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  if (header.cluster_bits < QCOW_MIN_CLUSTER_BITS ||
      header.cluster_bits > QCOW_MAX_CLUSTER_BITS) {
    lderr(cct) << "invalid cluster bits: " << header.cluster_bits << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  if (header.l2_bits < (QCOW_MIN_CLUSTER_BITS - 3) ||
      header.l2_bits > (QCOW_MAX_CLUSTER_BITS - 3)) {
    lderr(cct) << "invalid L2 bits: " << header.l2_bits << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  if (header.crypt_method != QCOW_CRYPT_NONE) {
    lderr(cct) << "invalid or unsupported encryption method" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  m_size = header.size;
  if (p2roundup(m_size, static_cast<uint64_t>(512)) != m_size) {
    lderr(cct) << "image size is not a multiple of block size" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  m_backing_file_offset = header.backing_file_offset;
  m_backing_file_size = header.backing_file_size;

  m_cluster_bits = header.cluster_bits;
  m_cluster_size = 1UL << header.cluster_bits;
  m_cluster_offset_mask = (1ULL << (63 - header.cluster_bits)) - 1;
  m_cluster_mask = ~QCOW_OFLAG_COMPRESSED;

  m_l2_bits = header.l2_bits;
  m_l2_size = (1UL << m_l2_bits);

  m_l1_shift = m_cluster_bits + m_l2_bits;
  m_l1_table.size = (m_size + (1LL << m_l1_shift) - 1) >> m_l1_shift;
  m_l1_table_offset = header.l1_table_offset;
  if (m_size > (std::numeric_limits<uint64_t>::max() - (1ULL << m_l1_shift)) ||
      m_l1_table.size >
        (std::numeric_limits<int32_t>::max() / sizeof(uint64_t))) {
    lderr(cct) << "image size too big: " << m_size << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  ldout(cct, 15) << "size=" << m_size << ", "
                 << "cluster_bits=" << m_cluster_bits << ", "
                 << "l2_bits=" << m_l2_bits << dendl;

  // allocate memory for L1 table and L2 + cluster caches
  m_l2_table_cache = std::make_unique<L2TableCache>(this);
  m_cluster_cache = std::make_unique<ClusterCache>(this);

  read_l1_table(on_finish);
}

#endif // WITH_RBD_MIGRATION_FORMAT_QCOW_V1

template <typename I>
void QCOWFormat<I>::read_v2_header(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto ctx = new LambdaContext([this, on_finish](int r) {
    handle_read_v2_header(r, on_finish); });
  m_bl.clear();
  m_stream->read({{0, sizeof(QCowHeader)}}, &m_bl, ctx);
}

template <typename I>
void QCOWFormat<I>::handle_read_v2_header(int r, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to read QCOW2 header: " << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  auto header = *reinterpret_cast<QCowHeader*>(m_bl.c_str());

  // byte-swap important fields
  header.magic = big_to_native(header.magic);
  header.version = big_to_native(header.version);
  header.backing_file_offset = big_to_native(header.backing_file_offset);
  header.backing_file_size = big_to_native(header.backing_file_size);
  header.cluster_bits = big_to_native(header.cluster_bits);
  header.size = big_to_native(header.size);
  header.crypt_method = big_to_native(header.crypt_method);
  header.l1_size = big_to_native(header.l1_size);
  header.l1_table_offset = big_to_native(header.l1_table_offset);
  header.nb_snapshots = big_to_native(header.nb_snapshots);
  header.snapshots_offset = big_to_native(header.snapshots_offset);

  if (header.version == 2) {
    // valid only for version >= 3
    header.incompatible_features = 0;
    header.compatible_features = 0;
    header.autoclear_features = 0;
    header.header_length = 72;
    header.compression_type = 0;
  } else {
    header.incompatible_features = big_to_native(header.incompatible_features);
    header.compatible_features = big_to_native(header.compatible_features);
    header.autoclear_features = big_to_native(header.autoclear_features);
    header.header_length = big_to_native(header.header_length);
  }

  if (header.magic != QCOW_MAGIC || header.version < 2 || header.version > 3) {
    // honestly shouldn't happen since we've already validated it
    lderr(cct) << "header is not QCOW2" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  if (header.cluster_bits < QCOW_MIN_CLUSTER_BITS ||
      header.cluster_bits > QCOW_MAX_CLUSTER_BITS) {
    lderr(cct) << "invalid cluster bits: " << header.cluster_bits << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  if (header.crypt_method != QCOW_CRYPT_NONE) {
    lderr(cct) << "invalid or unsupported encryption method" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  m_size = header.size;
  if (p2roundup(m_size, static_cast<uint64_t>(512)) != m_size) {
    lderr(cct) << "image size is not a multiple of block size" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  if (header.header_length <= offsetof(QCowHeader, compression_type)) {
    header.compression_type = 0;
  }

  if ((header.compression_type != 0) ||
      ((header.incompatible_features & QCOW2_INCOMPAT_COMPRESSION) != 0)) {
    lderr(cct) << "invalid or unsupported compression type" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  if ((header.incompatible_features & QCOW2_INCOMPAT_DATA_FILE) != 0) {
    lderr(cct) << "external data file feature not supported" << dendl;
    on_finish->complete(-ENOTSUP);
  }

  if ((header.incompatible_features & QCOW2_INCOMPAT_EXTL2) != 0) {
    lderr(cct) << "extended L2 table feature not supported" << dendl;
    on_finish->complete(-ENOTSUP);
    return;
  }

  header.incompatible_features &= ~QCOW2_INCOMPAT_MASK;
  if (header.incompatible_features != 0) {
    lderr(cct) << "unknown incompatible feature enabled" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  m_backing_file_offset = header.backing_file_offset;
  m_backing_file_size = header.backing_file_size;

  m_cluster_bits = header.cluster_bits;
  m_cluster_size = 1UL << header.cluster_bits;
  m_cluster_offset_mask = (1ULL << (63 - header.cluster_bits)) - 1;
  m_cluster_mask = ~(QCOW_OFLAG_COMPRESSED | QCOW_OFLAG_COPIED);

  // L2 table is fixed a (1) cluster block to hold 8-byte (3 bit) offsets
  m_l2_bits = m_cluster_bits - 3;
  m_l2_size = (1UL << m_l2_bits);

  m_l1_shift = m_cluster_bits + m_l2_bits;
  m_l1_table.size = (m_size + (1LL << m_l1_shift) - 1) >> m_l1_shift;
  m_l1_table_offset = header.l1_table_offset;
  if (m_size > (std::numeric_limits<uint64_t>::max() - (1ULL << m_l1_shift)) ||
      m_l1_table.size >
        (std::numeric_limits<int32_t>::max() / sizeof(uint64_t))) {
    lderr(cct) << "image size too big: " << m_size << dendl;
    on_finish->complete(-EINVAL);
    return;
  } else if (m_l1_table.size > header.l1_size) {
    lderr(cct) << "invalid L1 table size in header (" << header.l1_size
               << " < " << m_l1_table.size << ")" << dendl;
    on_finish->complete(-EINVAL);
    return;
  }

  m_snapshot_count = header.nb_snapshots;
  m_snapshots_offset = header.snapshots_offset;

  ldout(cct, 15) << "size=" << m_size << ", "
                 << "cluster_bits=" << m_cluster_bits << ", "
                 << "l1_table_offset=" << m_l1_table_offset << ", "
                 << "snapshot_count=" << m_snapshot_count << ", "
                 << "snapshots_offset=" << m_snapshots_offset << dendl;

  // allocate memory for L1 table and L2 + cluster caches
  m_l2_table_cache = std::make_unique<L2TableCache>(this);
  m_cluster_cache = std::make_unique<ClusterCache>(this);

  read_snapshot(on_finish);
}

template <typename I>
void QCOWFormat<I>::read_snapshot(Context* on_finish) {
  if (m_snapshots_offset == 0 || m_snapshots.size() == m_snapshot_count) {
    read_l1_table(on_finish);
    return;
  }

  // header is always aligned on 8 byte boundary
  m_snapshots_offset = p2roundup(m_snapshots_offset, static_cast<uint64_t>(8));

  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "snap_id=" << (m_snapshots.size() + 1) << ", "
                 << "offset=" << m_snapshots_offset << dendl;

  auto ctx = new LambdaContext([this, on_finish](int r) {
    handle_read_snapshot(r, on_finish); });
  m_bl.clear();
  m_stream->read({{m_snapshots_offset, sizeof(QCowSnapshotHeader)}}, &m_bl,
                 ctx);
}

template <typename I>
void QCOWFormat<I>::handle_read_snapshot(int r, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << ", "
                 << "index=" << m_snapshots.size() << dendl;

  if (r < 0) {
    lderr(cct) << "failed to read QCOW2 snapshot header: " << cpp_strerror(r)
               << dendl;
    on_finish->complete(r);
    return;
  }

  m_snapshots_offset += m_bl.length();
  auto header = *reinterpret_cast<QCowSnapshotHeader*>(m_bl.c_str());

  auto& snapshot = m_snapshots[m_snapshots.size() + 1];
  snapshot.id.resize(big_to_native(header.id_str_size));
  snapshot.name.resize(big_to_native(header.name_size));
  snapshot.l1_table_offset = big_to_native(header.l1_table_offset);
  snapshot.l1_table.size = big_to_native(header.l1_size);
  snapshot.timestamp.sec_ref() = big_to_native(header.date_sec);
  snapshot.timestamp.nsec_ref() = big_to_native(header.date_nsec);
  snapshot.extra_data_size = big_to_native(header.extra_data_size);

  ldout(cct, 10) << "snap_id=" << m_snapshots.size() << ", "
                 << "id_str_len=" << snapshot.id.size() << ", "
                 << "name_str_len=" << snapshot.name.size() << ", "
                 << "l1_table_offset=" << snapshot.l1_table_offset << ", "
                 << "l1_size=" << snapshot.l1_table.size << ", "
                 << "extra_data_size=" << snapshot.extra_data_size << dendl;

  read_snapshot_extra(on_finish);
}

template <typename I>
void QCOWFormat<I>::read_snapshot_extra(Context* on_finish) {
  ceph_assert(!m_snapshots.empty());
  auto& snapshot = m_snapshots.rbegin()->second;

  uint32_t length = snapshot.extra_data_size +
                    snapshot.id.size() +
                    snapshot.name.size();
  if (length == 0) {
    uuid_d uuid_gen;
    uuid_gen.generate_random();
    snapshot.name = uuid_gen.to_string();

    read_snapshot(on_finish);
    return;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "snap_id=" << m_snapshots.size() << ", "
                 << "offset=" << m_snapshots_offset << ", "
                 << "length=" << length << dendl;

  auto offset = m_snapshots_offset;
  m_snapshots_offset += length;

  auto ctx = new LambdaContext([this, on_finish](int r) {
    handle_read_snapshot_extra(r, on_finish); });
  m_bl.clear();
  m_stream->read({{offset, length}}, &m_bl, ctx);
}

template <typename I>
void QCOWFormat<I>::handle_read_snapshot_extra(int r, Context* on_finish) {
  ceph_assert(!m_snapshots.empty());
  auto& snapshot = m_snapshots.rbegin()->second;

  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << ", "
                 << "snap_id=" << m_snapshots.size() << dendl;

  if (r < 0) {
    lderr(cct) << "failed to read QCOW2 snapshot header extra: "
               << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  if (snapshot.extra_data_size >=
        offsetof(QCowSnapshotExtraData, disk_size) + sizeof(uint64_t))  {
    auto extra = reinterpret_cast<const QCowSnapshotExtraData*>(m_bl.c_str());
    snapshot.size = big_to_native(extra->disk_size);
  } else {
    snapshot.size = m_size;
  }

  auto data = reinterpret_cast<const char*>(m_bl.c_str());
  data += snapshot.extra_data_size;

  if (!snapshot.id.empty()) {
    snapshot.id = std::string(data, snapshot.id.size());
    data += snapshot.id.size();
  }

  if (!snapshot.name.empty()) {
    snapshot.name = std::string(data, snapshot.name.size());
    data += snapshot.name.size();
  } else {
    uuid_d uuid_gen;
    uuid_gen.generate_random();
    snapshot.name = uuid_gen.to_string();
  }

  ldout(cct, 10) << "snap_id=" << m_snapshots.size() << ", "
                 << "name=" << snapshot.name << ", "
                 << "size=" << snapshot.size << dendl;
  read_snapshot_l1_table(on_finish);
}

template <typename I>
void QCOWFormat<I>::read_snapshot_l1_table(Context* on_finish) {
  ceph_assert(!m_snapshots.empty());
  auto& snapshot = m_snapshots.rbegin()->second;

  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "snap_id=" << m_snapshots.size() << ", "
                 << "l1_table_offset=" << snapshot.l1_table_offset
                 << dendl;

  auto ctx = new LambdaContext([this, on_finish](int r) {
    handle_read_snapshot_l1_table(r, on_finish); });
  m_stream->read({{snapshot.l1_table_offset,
                   snapshot.l1_table.size * sizeof(uint64_t)}},
                 &snapshot.l1_table.bl, ctx);
}

template <typename I>
void QCOWFormat<I>::handle_read_snapshot_l1_table(int r, Context* on_finish) {
  ceph_assert(!m_snapshots.empty());
  auto& snapshot = m_snapshots.rbegin()->second;

  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << ", "
                 << "snap_id=" << m_snapshots.size() << dendl;

  if (r < 0) {
    lderr(cct) << "failed to read snapshot L1 table: " << cpp_strerror(r)
               << dendl;
    on_finish->complete(r);
    return;
  }

  snapshot.l1_table.decode();
  read_snapshot(on_finish);
}

template <typename I>
void QCOWFormat<I>::read_l1_table(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  auto ctx = new LambdaContext([this, on_finish](int r) {
    handle_read_l1_table(r, on_finish); });
  m_stream->read({{m_l1_table_offset,
                   m_l1_table.size * sizeof(uint64_t)}},
                 &m_l1_table.bl, ctx);
}

template <typename I>
void QCOWFormat<I>::handle_read_l1_table(int r, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to read L1 table: " << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  m_l1_table.decode();
  read_backing_file(on_finish);
}

template <typename I>
void QCOWFormat<I>::read_backing_file(Context* on_finish) {
  if (m_backing_file_offset == 0 || m_backing_file_size == 0) {
    // all data is within the specified file
    on_finish->complete(0);
    return;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  // TODO add support for backing files
  on_finish->complete(-ENOTSUP);
}

template <typename I>
void QCOWFormat<I>::close(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  m_stream->close(on_finish);
}

template <typename I>
void QCOWFormat<I>::get_snapshots(SnapInfos* snap_infos, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  snap_infos->clear();
  for (auto& [snap_id, snapshot] : m_snapshots) {
    SnapInfo snap_info(snapshot.name, cls::rbd::UserSnapshotNamespace{},
                       snapshot.size, {}, 0, 0, snapshot.timestamp);
    snap_infos->emplace(snap_id, snap_info);
  }

  on_finish->complete(0);
}

template <typename I>
void QCOWFormat<I>::get_image_size(uint64_t snap_id, uint64_t* size,
                                  Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "snap_id=" << snap_id << dendl;

  if (snap_id == CEPH_NOSNAP) {
    *size = m_size;
  } else {
    auto snapshot_it = m_snapshots.find(snap_id);
    if (snapshot_it == m_snapshots.end()) {
      on_finish->complete(-ENOENT);
      return;
    }

    auto& snapshot = snapshot_it->second;
    *size = snapshot.size;
  }

  on_finish->complete(0);
}

template <typename I>
bool QCOWFormat<I>::read(
    io::AioCompletion* aio_comp, uint64_t snap_id, io::Extents&& image_extents,
    io::ReadResult&& read_result, int op_flags, int read_flags,
    const ZTracer::Trace &parent_trace) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "snap_id=" << snap_id << ", "
                 << "image_extents=" << image_extents << dendl;

  const LookupTable* l1_table = nullptr;
  if (snap_id == CEPH_NOSNAP) {
    l1_table = &m_l1_table;
  } else {
    auto snapshot_it = m_snapshots.find(snap_id);
    if (snapshot_it == m_snapshots.end()) {
      aio_comp->fail(-ENOENT);
      return true;
    }

    auto& snapshot = snapshot_it->second;
    l1_table = &snapshot.l1_table;
  }

  aio_comp->read_result = std::move(read_result);
  aio_comp->read_result.set_image_extents(image_extents);

  auto read_request = new ReadRequest(this, aio_comp, l1_table,
                                      std::move(image_extents));
  read_request->send();

  return true;
}

template <typename I>
void QCOWFormat<I>::list_snaps(io::Extents&& image_extents,
                              io::SnapIds&& snap_ids, int list_snaps_flags,
                              io::SnapshotDelta* snapshot_delta,
                              const ZTracer::Trace &parent_trace,
                              Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  ClusterExtents cluster_extents;
  populate_cluster_extents(cct, m_cluster_size, image_extents,
                           &cluster_extents);

  // map L1 table indexes to cluster extents
  std::map<uint64_t, ClusterExtents> l1_cluster_extents;
  for (auto& cluster_extent : cluster_extents) {
    uint32_t l1_table_index = cluster_extent.image_offset >> m_l1_shift;
    auto& l1_cluster_extent = l1_cluster_extents[l1_table_index];
    l1_cluster_extent.reserve(cluster_extents.size());
    l1_cluster_extent.push_back(cluster_extent);
  }

  std::map<uint64_t, const LookupTable*> snap_id_to_l1_table;
  for (auto& [snap_id, snapshot] : m_snapshots) {
    snap_id_to_l1_table[snap_id] = &snapshot.l1_table;
  }
  snap_id_to_l1_table[CEPH_NOSNAP] = &m_l1_table;

  on_finish = new LambdaContext([this, image_extents,
                                 snap_ids=std::move(snap_ids),
                                 snapshot_delta, on_finish](int r) mutable {
    handle_list_snaps(r, std::move(image_extents), std::move(snap_ids),
                      snapshot_delta, on_finish);
  });

  auto gather_ctx = new C_Gather(cct, on_finish);

  for (auto& [l1_table_index, cluster_extents] : l1_cluster_extents) {
    auto list_snaps_request = new ListSnapsRequest(
      this, l1_table_index, std::move(cluster_extents), snap_id_to_l1_table,
      snapshot_delta, gather_ctx->new_sub());
    list_snaps_request->send();
  }

  gather_ctx->activate();
}

template <typename I>
void QCOWFormat<I>::handle_list_snaps(int r, io::Extents&& image_extents,
                                      io::SnapIds&& snap_ids,
                                      io::SnapshotDelta* snapshot_delta,
                                      Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << ", "
                 << "snapshot_delta=" << *snapshot_delta << dendl;

  std::optional<uint64_t> previous_size = std::nullopt;
  for (auto& [snap_id, snapshot] : m_snapshots) {
    auto sparse_extents = &(*snapshot_delta)[{snap_id, snap_id}];
    util::zero_shrunk_snapshot(cct, image_extents, snap_id, snapshot.size,
                               &previous_size, sparse_extents);
  }

  auto sparse_extents = &(*snapshot_delta)[{CEPH_NOSNAP, CEPH_NOSNAP}];
  util::zero_shrunk_snapshot(cct, image_extents, CEPH_NOSNAP, m_size,
                             &previous_size, sparse_extents);

  util::merge_snapshot_delta(snap_ids, snapshot_delta);
  on_finish->complete(r);
}

} // namespace migration
} // namespace librbd

template class librbd::migration::QCOWFormat<librbd::ImageCtx>;
