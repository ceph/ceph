// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "compressor_registry.h"
#include "common/dout.h"

using namespace std::literals;

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "CompressorRegistry(" << this << ") "

CompressorRegistry::CompressorRegistry(CephContext *cct)
  : cct(cct)
{
  cct->_conf.add_observer(this);
}

CompressorRegistry::~CompressorRegistry()
{
  cct->_conf.remove_observer(this);
}

std::vector<std::string> CompressorRegistry::get_tracked_keys() const noexcept
{
  return {
    "ms_osd_compress_mode"s,
    "ms_osd_compression_algorithm"s,
    "ms_osd_compress_min_size"s,
    "ms_compress_secure"s
  };
}

void CompressorRegistry::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string>& changed)
{
  std::scoped_lock l(lock);
  _refresh_config();
}

std::vector<uint32_t> CompressorRegistry::_parse_method_list(const std::string& s)
{
  std::vector<uint32_t> methods;

  for_each_substr(s, ";,= \t", [&] (auto method) {
    ldout(cct,20) << "adding algorithm method: " << method << dendl;

    auto alg_type = Compressor::get_comp_alg_type(method);
    if (alg_type) {
      methods.push_back(*alg_type);
    } else {
      ldout(cct,5) << "WARNING: unknown algorithm method " << method << dendl;
    }
  });

  if (methods.empty()) {
    methods.push_back(Compressor::COMP_ALG_NONE);
  }
  ldout(cct,20) << __func__ << " " << s << " -> " << methods << dendl;

  return methods;
}

void CompressorRegistry::_refresh_config()
{
  auto c_mode = Compressor::get_comp_mode_type(cct->_conf.get_val<std::string>("ms_osd_compress_mode"));

  if (c_mode) {
    ms_osd_compress_mode = *c_mode;
  } else {
    ldout(cct,1) << __func__ << " failed to identify ms_osd_compress_mode " 
      << ms_osd_compress_mode << dendl;

    ms_osd_compress_mode = Compressor::COMP_NONE;
  }

  ms_osd_compression_methods = _parse_method_list(cct->_conf.get_val<std::string>("ms_osd_compression_algorithm"));
  ms_osd_compress_min_size = cct->_conf.get_val<std::uint64_t>("ms_osd_compress_min_size");

  ms_compress_secure = cct->_conf.get_val<bool>("ms_compress_secure");

  ldout(cct,10) << __func__ << " ms_osd_compression_mode " << ms_osd_compress_mode
    << " ms_osd_compression_methods " << ms_osd_compression_methods
    << " ms_osd_compress_above_min_size " << ms_osd_compress_min_size
    << " ms_compress_secure " << ms_compress_secure
    << dendl;
}

Compressor::CompressionAlgorithm
CompressorRegistry::pick_method(uint32_t peer_type,
                                const std::vector<uint32_t>& preferred_methods)
{
  std::vector<uint32_t> allowed_methods = get_methods(peer_type);
  auto preferred = std::find_first_of(preferred_methods.begin(),
                                      preferred_methods.end(),
                                      allowed_methods.begin(),
                                      allowed_methods.end());
  if (preferred == preferred_methods.end()) {
    ldout(cct,1) << "failed to pick compression method from client's "
                 << preferred_methods
                 << " and our " << allowed_methods << dendl;
    return Compressor::COMP_ALG_NONE;
  } else {
    return static_cast<Compressor::CompressionAlgorithm>(*preferred);
  }
}

Compressor::CompressionMode
CompressorRegistry::get_mode(uint32_t peer_type, bool is_secure)
{
  std::scoped_lock l(lock);
  ldout(cct, 20) << __func__ << " peer_type " << peer_type 
    << " is_secure " << is_secure << dendl;

  if (is_secure && !ms_compress_secure) {
    return Compressor::COMP_NONE;
  }

  switch (peer_type) {
  case CEPH_ENTITY_TYPE_OSD:
    return static_cast<Compressor::CompressionMode>(ms_osd_compress_mode);
  default:
    return Compressor::COMP_NONE;
  }
}
