#pragma once

#include <string>
#include "include/common_fwd.h"
#include "rgw_sal_fwd.h"

struct lua_State;
class req_state;
class RGWREST;
class OpsLogSink;
class RGWOp;

namespace rgw::lua::request {

// create the request metatable
void create_top_metatable(lua_State* L, req_state* s, const char* op_name);

// execute a lua script in the Request context
int execute(
    RGWREST* rest,
    OpsLogSink* olog,
    req_state *s, 
    RGWOp* op,
    const std::string& script);

int execute(
    RGWREST* rest,
    OpsLogSink* olog,
    req_state *s, 
    RGWOp* op,
    const std::string& script,
    int& script_return_code );
} // namespace rgw::lua::request

