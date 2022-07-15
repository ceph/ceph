#pragma once

#include <string>
#include "include/common_fwd.h"

class req_state;
class RGWREST;
class OpsLogSink;
namespace rgw::sal {
  class RGWRadosStore;
}

namespace rgw::lua::request {

// execute a lua script in the Request context
int execute(
    rgw::sal::RGWRadosStore* store,
    RGWREST* rest,
    OpsLogSink* olog,
    req_state *s, 
    RGWOp* op,
    const std::string& script);

}

