#pragma once

#include <string>
#include "include/common_fwd.h"
#include "rgw_sal_fwd.h"

struct lua_State;
class req_state;
class RGWREST;
class OpsLogSink;

namespace rgw::lua::request {

// create the request metatable
void create_top_metatable(lua_State* L, req_state* s, const char* op_name);

// execute a lua script in the Request context
int execute(
    rgw::sal::Driver* driver,
    RGWREST* rest,
    OpsLogSink* olog,
    req_state *s, 
    RGWOp* op,
    const std::string& script);
} // namespace rgw::lua::request

