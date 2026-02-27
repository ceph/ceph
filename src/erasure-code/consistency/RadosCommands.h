#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/program_options.hpp>
#include "common/ceph_json.h"
#include "librados/librados_asio.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "erasure-code/ErasureCodePlugin.h"

#define dout_context g_ceph_context

namespace ceph {
namespace consistency {
class RadosCommands {
  private:
    librados::Rados& rados;
    std::unique_ptr<JSONFormatter> formatter;

  public:
    RadosCommands(librados::Rados& rados);
    int get_primary_osd(const std::string& pool_name,
                        const std::string& oid);
    std::string get_pool_ec_profile_name(const std::string& pool_name);
    bool get_pool_allow_ec_optimizations(const std::string& pool_name);
    ceph::ErasureCodeProfile get_ec_profile_for_pool(const std::string& pool_name);
    int get_ec_chunk_size_for_pool(const std::string& pool_name);
    void inject_parity_read_on_primary_osd(const std::string& pool_name,
                                           const std::string& oid);
    void inject_clear_parity_read_on_primary_osd(const std::string& pool_name,
                                                 const std::string& oid);
};
}
}