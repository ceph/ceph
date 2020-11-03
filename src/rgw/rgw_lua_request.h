#pragma once

#include <string>
#include "include/common_fwd.h"

class req_state;
class RGWREST;
class OpsLogSocket;
namespace rgw::sal {
  class RGWRadosStore;
}

namespace rgw::lua::request {

// execute a lua script in the Request context
int execute(
    rgw::sal::RGWRadosStore* store,
    RGWREST* rest,
    OpsLogSocket* olog,
    req_state *s, 
    const char* op_name,
    const std::string& script);

}

