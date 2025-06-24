#pragma once

#include "ECEncoder.h"
#include "include/buffer.h"
#include "erasure-code/ErasureCode.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "osd/ECUtil.h"
#include "osd/ECUtilL.h"

using stripe_info_l_t = ECLegacy::ECUtilL::stripe_info_t;

namespace ceph {
namespace consistency {
class ECEncoderSwitch {
  private:
    ceph::consistency::ECEncoder<ECUtil::stripe_info_t> encoder_optimized;
    ceph::consistency::ECEncoder<stripe_info_l_t> encoder_legacy;
    bool optimizations_enabled;

  public:
    ECEncoderSwitch(ceph::ErasureCodeProfile profile,
                    int chunk_size,
                    bool optimizations_enabled);
    std::optional<ceph::bufferlist> do_encode(ceph::bufferlist inbl);
    int get_k(void);
    int get_m(void);
};
}
}

