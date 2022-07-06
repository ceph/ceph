#pragma once

#include <string>
#include "include/common_fwd.h"
#include "rgw_sal_fwd.h"

struct req_state;
class RGWREST;
class OpsLogSink;
namespace rgw::lua {
  class Background;
}

namespace rgw::lua::request {

// execute a lua script in the Request context
int execute(
    rgw::sal::Store* store,
    RGWREST* rest,
    OpsLogSink* olog,
    req_state *s, 
    const char* op_name,
    const std::string& script,
    rgw::lua::Background* background = nullptr);

}

