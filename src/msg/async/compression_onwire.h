// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_COMPRESSION_ONWIRE_H
#define CEPH_COMPRESSION_ONWIRE_H

#include <cstdint>
#include <optional>

#include "compressor/Compressor.h"
#include "include/buffer.h"

class CompConnectionMeta;

namespace ceph::compression::onwire {
  using Compressor = TOPNSPC::Compressor;
  using CompressorRef = TOPNSPC::CompressorRef;

  class Handler {
  public:
    Handler(CephContext* const cct, CompressorRef compressor)
      : m_cct(cct), m_compressor(compressor) {}

  protected:
    CephContext* const m_cct;
    CompressorRef m_compressor;
  };

  class RxHandler final : private Handler {
  public:
    RxHandler(CephContext* const cct, CompressorRef compressor)
      : Handler(cct, compressor) {}
    ~RxHandler() {};

    /**
     * Decompresses a bufferlist 
     *
     * @param input compressed bufferlist
     * @param out decompressed bufferlist
     *
     * @returns true on success, false on failure
     */
    std::optional<ceph::bufferlist> decompress(const ceph::bufferlist &input);
  };

  class TxHandler final : private Handler {
  public:
    TxHandler(CephContext* const cct, CompressorRef compressor, int mode, std::uint64_t min_size)
      : Handler(cct, compressor),
	m_min_size(min_size),
	m_mode(static_cast<Compressor::CompressionMode>(mode))
    {}
    ~TxHandler() {}

    void reset_handler(int num_segments, uint64_t size) {
      m_init_onwire_size = size;
      m_compress_potential = size;
      m_onwire_size = 0;
    }

    void done();

    /**
     * Compresses a bufferlist 
     *
     * @param input bufferlist to compress
     * @param out compressed bufferlist
     *
     * @returns true on success, false on failure
     */
    std::optional<ceph::bufferlist> compress(const ceph::bufferlist &input);

    double get_ratio() const {
      return get_initial_size() / (double) get_final_size();
    }

    uint64_t get_initial_size() const {
      return m_init_onwire_size;
    } 

    uint64_t get_final_size() const {
      return m_onwire_size;
    }

  private:
    uint64_t m_min_size; 
    Compressor::CompressionMode m_mode;

    uint64_t m_init_onwire_size;
    uint64_t m_onwire_size;
    uint64_t m_compress_potential;
  };

  struct rxtx_t {
    std::unique_ptr<RxHandler> rx;
    std::unique_ptr<TxHandler> tx;

    static rxtx_t create_handler_pair(
      CephContext* ctx,
      const CompConnectionMeta& comp_meta,
      std::uint64_t compress_min_size);
  };
}

#endif // CEPH_COMPRESSION_ONWIRE_H
