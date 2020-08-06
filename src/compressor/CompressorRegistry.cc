#include "CompressorRegistry.h"
#include "common/dout.h"

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

const char** CompressorRegistry::get_tracked_conf_keys() const
{
  static const char *keys[] = {
    "ms_osd_compress_mode",
    "ms_osd_compression_algorithm",
    "ms_osd_compress_min_size",
    "ms_compress_secure",
    NULL
  };
  return keys;
}

void CompressorRegistry::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string>& changed)
{
  _refresh_config();
}

std::vector<uint32_t> CompressorRegistry::_parse_method_list(const string& s) {
  std::vector<uint32_t> methods;

  std::list<std::string> supported_list;
  for_each_substr(s, ";,= \t", [&] (auto token) {
    std::string alg_type_str(token.data(), token.size());

    ldout(cct,20) << "adding algorithm method: " << alg_type_str << dendl;

    auto alg_type = Compressor::get_comp_alg_type(alg_type_str.data());
    if (alg_type) {
      methods.push_back(*alg_type);
    } else {
      ldout(cct,5) << "WARNING: unknown algorithm method " << alg_type_str << dendl;
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
  auto c_mode = Compressor::get_comp_mode_type(cct->_conf.get_val<string>("ms_osd_compress_mode"));

  if (c_mode) {
    ms_osd_compress_mode = *c_mode;
  } else {
    ldout(cct,1) << __func__ << " failed to identify ms_osd_compress_mode " 
      << ms_osd_compress_mode << dendl;

    ms_osd_compress_mode = Compressor::COMP_NONE;
  }

  ms_osd_compression_methods = _parse_method_list(cct->_conf.get_val<string>("ms_osd_compression_algorithm"));
  ms_osd_compress_min_size = cct->_conf.get_val<std::uint64_t>("ms_osd_compress_min_size");

  ms_compress_secure = cct->_conf.get_val<bool>("ms_compress_secure");

  ldout(cct,10) << __func__ << " ms_osd_compression_mode " << ms_osd_compress_mode
    << " ms_osd_compression_methods " << ms_osd_compression_methods
    << " ms_osd_compress_above_min_size " << ms_osd_compress_min_size
    << " ms_compress_secure " << ms_compress_secure
    << dendl;
}

uint32_t CompressorRegistry::pick_method(uint32_t peer_type, uint32_t comp_mode, const std::vector<uint32_t>& preferred_methods) {
  if (comp_mode == Compressor::COMP_NONE) {
    return Compressor::COMP_ALG_NONE;
  }

  std::vector<uint32_t> allowed_methods = get_methods(peer_type);
  
  for (auto mode : preferred_methods) {
    if (std::find(allowed_methods.begin(), allowed_methods.end(), mode)
	!= allowed_methods.end()) {
      return mode;
    }
  }

  ldout(cct,1) << "failed to pick compression method from client's " << preferred_methods
	       << " and our " << allowed_methods << dendl;
  return Compressor::COMP_ALG_NONE;
}

const uint32_t CompressorRegistry::get_mode(uint32_t peer_type, bool is_secure) {
  ldout(cct, 20) << __func__ << " peer_type " << peer_type 
    << " is_secure " << is_secure << dendl;

  if (is_secure && !ms_compress_secure) {
    return Compressor::COMP_NONE;
  }

  switch (peer_type) {
    case CEPH_ENTITY_TYPE_OSD:
      return ms_osd_compress_mode;
    default:
      return Compressor::COMP_NONE;
  }
}
