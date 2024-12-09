// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "compression_onwire.h"
#include "compression_meta.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_ms

namespace ceph::compression::onwire {

rxtx_t rxtx_t::create_handler_pair(
    CephContext* ctx,
    const CompConnectionMeta& comp_meta,
    std::uint64_t compress_min_size)
{
  if (comp_meta.is_compress()) {
     CompressorRef compressor = Compressor::create(ctx, comp_meta.get_method());
    if (compressor) {
      return {std::make_unique<RxHandler>(ctx, compressor),
	      std::make_unique<TxHandler>(ctx, compressor,
					  comp_meta.get_mode(),
					  compress_min_size)};
    }
  }
  return {};
}

std::optional<ceph::bufferlist> TxHandler::compress(const ceph::bufferlist &input)
{
  if (m_init_onwire_size < m_min_size) {
    ldout(m_cct, 20) << __func__ 
		     << " discovered frame that is smaller than threshold, aborting compression"
		     << dendl;
    return {};
  }

  m_compress_potential -= input.length();

  ceph::bufferlist out;
  if (input.length() == 0) {
    ldout(m_cct, 20) << __func__ 
		     << " discovered an empty segment, skipping compression without aborting"
		     << dendl;
    out.clear();
    return out;
  }

  std::optional<int32_t> compressor_message;
  if (m_compressor->compress(input, out, compressor_message)) {
    return {};
  } else {
    ldout(m_cct, 20) << __func__ << " uncompressed.length()=" << input.length()
                     << " compressed.length()=" << out.length() << dendl;
    m_onwire_size += out.length();
    return out;
  }
}

std::optional<ceph::bufferlist> RxHandler::decompress(const ceph::bufferlist &input)
{
  ceph::bufferlist out;
  if (input.length() == 0) {
    ldout(m_cct, 20) << __func__
		     << " discovered an empty segment, skipping decompression without aborting"
		     << dendl;
    out.clear();
    return out;
  }

  std::optional<int32_t> compressor_message;
  if (m_compressor->decompress(input, out, compressor_message)) {
    return {};
  } else {
    ldout(m_cct, 20) << __func__ << " compressed.length()=" << input.length()
                     << " uncompressed.length()=" << out.length() << dendl;
    return out;
  }
}

void TxHandler::done()
{
  ldout(m_cct, 25) << __func__ << " compression ratio=" << get_ratio() << dendl;
}

std::string_view RxHandler::compressor_name() const {
  return m_compressor->get_type_name();
}

std::string_view TxHandler::compressor_name() const {
  return m_compressor->get_type_name();
}

} // namespace ceph::compression::onwire
