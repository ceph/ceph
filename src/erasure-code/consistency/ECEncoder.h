#pragma once

#include "include/buffer.h"
#include "erasure-code/ErasureCode.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "osd/ECUtil.h"
#include "osd/ECUtilL.h"

using stripe_info_l_t = ECLegacy::ECUtilL::stripe_info_t;
using stripe_info_o_t = ECUtil::stripe_info_t;

namespace ceph {
namespace consistency {
template <typename SInfo>
class ECEncoder {
  private:
    ceph::ErasureCodeProfile profile;
    ceph::ErasureCodeInterfaceRef ec_impl;
    std::unique_ptr<SInfo> stripe_info;
    int chunk_size;
    int ec_init_plugin(ceph::ErasureCodeInterfaceRef &ec_impl);
    std::unique_ptr<SInfo> ec_init_sinfo(ceph::ErasureCodeInterfaceRef &ec_impl);
    std::optional<ceph::bufferlist> do_encode(ceph::bufferlist inbl,
                                              SInfo &sinfo);
  public:
    ECEncoder(ceph::ErasureCodeProfile profile, int chunk_size);
    std::optional<ceph::bufferlist> do_encode(ceph::bufferlist inbl);
    int get_k(void);
    int get_m(void);
};
}
}