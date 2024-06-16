#include <sstream>
#include <stdexcept>
#include <lua.hpp>
#include "common/dout.h"
#include "services/svc_zone.h"
#include "rgw_lua_utils.h"
#include "rgw_lua.h"
#include "rgw_common.h"
#include "rgw_log.h"
#include "rgw_op.h"
#include "rgw_process_env.h"
#include "rgw_zone.h"
#include "rgw_acl.h"
#include "rgw_sal_rados.h"
#include "rgw_lua_background.h"
#include "rgw_perf_counters.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua::request {

// closure that perform ops log action
// e.g.
//    Request.Log()
//
constexpr const char* RequestLogAction{"Log"};

int RequestLog(lua_State* L) 
{
  const auto rest = reinterpret_cast<RGWREST*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
  const auto olog = reinterpret_cast<OpsLogSink*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
  const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(THIRD_UPVAL)));
  const auto op(reinterpret_cast<RGWOp*>(lua_touserdata(L, lua_upvalueindex(FOURTH_UPVAL))));
  if (s) {
    const auto rc = rgw_log_op(rest, s, op, olog);
    lua_pushinteger(L, rc);
  } else {
    ldpp_dout(s, 1) << "Lua ERROR: missing request state, cannot use ops log"  << dendl;
    lua_pushinteger(L, -EINVAL);
  }

  return ONE_RETURNVAL;
}

int SetAttribute(lua_State* L)  {
  auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(1)));

  if (!s->trace || !s->trace->IsRecording()) {
    return 0;
  }

  auto key = luaL_checkstring(L, 1);
  int value_type = lua_type(L, 2);

  switch (value_type) {
    case LUA_TSTRING:
      s->trace->SetAttribute(key, lua_tostring(L, 2));
      break;

    case LUA_TNUMBER:
      if (lua_isinteger(L, 2)) {
        s->trace->SetAttribute(key, static_cast<int64_t>(lua_tointeger(L, 2)));
      } else {
        s->trace->SetAttribute(key, static_cast<double>(lua_tonumber(L, 2)));
      }
      break;

    default:
      luaL_error(L, "unsupported value type for SetAttribute");
  }
  return 0;
}

int AddEvent(lua_State* L)  {
  auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(1)));

  if (!s->trace || !s->trace->IsRecording()) {
    return 0;
  }

  int args = lua_gettop(L);
  if (args == 1) {
    auto log = luaL_checkstring(L, 1);
    s->trace->AddEvent(log);
  } else if(args == 2) {
    auto event_name = luaL_checkstring(L, 1);
    std::unordered_map<const char*, jspan_attribute> event_values;
    lua_pushnil(L);
    while (lua_next(L, 2) != 0) {
      if (lua_type(L, -2) != LUA_TSTRING) {
        // skip pair if key is not a string
        lua_pop(L, 1);
        continue;
      }

      auto key = luaL_checkstring(L, -2);
      int value_type = lua_type(L, -1);
      switch (value_type) {
        case LUA_TSTRING:
          event_values.emplace(key, lua_tostring(L, -1));
          break;

        case LUA_TNUMBER:
          if (lua_isinteger(L, -1)) {
            event_values.emplace(key, static_cast<int64_t>(lua_tointeger(L, -1)));
          } else {
            event_values.emplace(key, static_cast<double>(lua_tonumber(L, -1)));
          }
          break;
      }
      lua_pop(L, 1);
    }
    lua_pop(L, 1);
    s->trace->AddEvent(event_name, event_values);
  }
  return 0;
}

struct ResponseMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto err = reinterpret_cast<const rgw_err*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "HTTPStatusCode") == 0) {
      lua_pushinteger(L, err->http_ret);
    } else if (strcasecmp(index, "RGWCode") == 0) {
      lua_pushinteger(L, err->ret);
    } else if (strcasecmp(index, "HTTPStatus") == 0) {
      pushstring(L, err->err_code);
    } else if (strcasecmp(index, "Message") == 0) {
      pushstring(L, err->message);
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
  
  static int NewIndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    auto err = reinterpret_cast<rgw_err*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "HTTPStatusCode") == 0) {
      err->http_ret = luaL_checkinteger(L, 3);
    } else if (strcasecmp(index, "RGWCode") == 0) {
      err->ret = luaL_checkinteger(L, 3);
    } else if (strcasecmp(index, "HTTPStatus") == 0) {
      err->err_code.assign(luaL_checkstring(L, 3));
    } else if (strcasecmp(index, "Message") == 0) {
      err->message.assign(luaL_checkstring(L, 3));
    } else {
      return error_unknown_field(L, index, name);
    }
    return NO_RETURNVAL;
  }
};

struct QuotaMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto info = reinterpret_cast<RGWQuotaInfo*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "MaxSize") == 0) {
      lua_pushinteger(L, info->max_size);
    } else if (strcasecmp(index, "MaxObjects") == 0) {
      lua_pushinteger(L, info->max_objects);
    } else if (strcasecmp(index, "Enabled") == 0) {
      lua_pushboolean(L, info->enabled);
    } else if (strcasecmp(index, "Rounded") == 0) {
      lua_pushboolean(L, !info->check_on_raw);
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

struct PlacementRuleMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto rule = reinterpret_cast<rgw_placement_rule*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Name") == 0) {
      pushstring(L, rule->name);
    } else if (strcasecmp(index, "StorageClass") == 0) {
      pushstring(L, rule->storage_class);
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

struct UserMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto user = reinterpret_cast<const rgw_user*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Tenant") == 0) {
      pushstring(L, user->tenant);
    } else if (strcasecmp(index, "Id") == 0) {
      pushstring(L, user->id);
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

struct TraceMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Enable") == 0) {
      lua_pushboolean(L, s->trace_enabled);
    } else if(strcasecmp(index, "SetAttribute") == 0) {
        lua_pushlightuserdata(L, s);
        lua_pushcclosure(L, SetAttribute, ONE_UPVAL);
    } else if(strcasecmp(index, "AddEvent") == 0) {
        lua_pushlightuserdata(L, s);
        lua_pushcclosure(L, AddEvent, ONE_UPVAL);
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }

  static int NewIndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Enable") == 0) {
      s->trace_enabled = lua_toboolean(L, 3);
    } else {
      return error_unknown_field(L, index, name);
    }
    return NO_RETURNVAL;
  }
};

struct OwnerMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto owner = reinterpret_cast<ACLOwner*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "DisplayName") == 0) {
      pushstring(L, owner->display_name);
    } else if (strcasecmp(index, "User") == 0) {
      pushstring(L, to_string(owner->id));
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

struct BucketMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto bucket = s->bucket.get();

    const char* index = luaL_checkstring(L, 2);

    if (rgw::sal::Bucket::empty(bucket)) {
      if (strcasecmp(index, "Name") == 0) {
        pushstring(L, s->init_state.url_bucket);
      } else {
        lua_pushnil(L);
      }
    } else if (strcasecmp(index, "Tenant") == 0) {
      pushstring(L, bucket->get_tenant());
    } else if (strcasecmp(index, "Name") == 0) {
      pushstring(L, bucket->get_name());
    } else if (strcasecmp(index, "Marker") == 0) {
      pushstring(L, bucket->get_marker());
    } else if (strcasecmp(index, "Id") == 0) {
      pushstring(L, bucket->get_bucket_id());
    } else if (strcasecmp(index, "ZoneGroupId") == 0) {
      pushstring(L, bucket->get_info().zonegroup);
    } else if (strcasecmp(index, "CreationTime") == 0) {
      pushtime(L, bucket->get_creation_time());
    } else if (strcasecmp(index, "MTime") == 0) {
      pushtime(L, bucket->get_modification_time());
    } else if (strcasecmp(index, "Quota") == 0) {
      create_metatable<QuotaMetaTable>(L, name, index, false, &(bucket->get_info().quota));
    } else if (strcasecmp(index, "PlacementRule") == 0) {
      create_metatable<PlacementRuleMetaTable>(L, name, index, false, &(bucket->get_info().placement_rule));
    } else if (strcasecmp(index, "User") == 0) {
      const rgw_owner& owner = bucket->get_owner();
      if (const rgw_user* u = std::get_if<rgw_user>(&owner); u) {
        create_metatable<UserMetaTable>(L, name, index, false, const_cast<rgw_user*>(u));
      } else {
        lua_pushnil(L);
      }
    } else if (strcasecmp(index, "Account") == 0) {
      const rgw_owner& owner = bucket->get_owner();
      if (const rgw_account_id* a = std::get_if<rgw_account_id>(&owner); a) {
        pushstring(L, *a);
      } else {
        lua_pushnil(L);
      }
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
  
  static int NewIndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto bucket = s->bucket.get();

    const char* index = luaL_checkstring(L, 2);

    if (rgw::sal::Bucket::empty(bucket)) {
      if (strcasecmp(index, "Name") == 0) {
        s->init_state.url_bucket = luaL_checkstring(L, 3);
        return NO_RETURNVAL;
      }
    }
    return error_unknown_field(L, index, name);
  }
};

struct ObjectMetaTable : public EmptyMetaTable {
  using Type = rgw::sal::Object;

  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto obj = reinterpret_cast<const Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Name") == 0) {
      pushstring(L, obj->get_name());
    } else if (strcasecmp(index, "Instance") == 0) {
      pushstring(L, obj->get_instance());
    } else if (strcasecmp(index, "Id") == 0) {
      pushstring(L, obj->get_oid());
    } else if (strcasecmp(index, "Size") == 0) {
      lua_pushinteger(L, obj->get_size());
    } else if (strcasecmp(index, "MTime") == 0) {
      pushtime(L, obj->get_mtime());
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

struct GrantMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto grant = reinterpret_cast<ACLGrant*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Type") == 0) {
      lua_pushinteger(L, grant->get_type().get_type());
    } else if (strcasecmp(index, "User") == 0) {
      if (const auto user = grant->get_user(); user) {
        pushstring(L, to_string(user->id));
      } else {
        lua_pushnil(L);
      }
    } else if (strcasecmp(index, "Permission") == 0) {
      lua_pushinteger(L, grant->get_permission().get_permissions());
    } else if (strcasecmp(index, "GroupType") == 0) {
      if (const auto group = grant->get_group(); group) {
        lua_pushinteger(L, group->type);
      } else {
        lua_pushnil(L);
      }
    } else if (strcasecmp(index, "Referer") == 0) {
      if (const auto referer = grant->get_referer(); referer) {
        pushstring(L, referer->url_spec);
      } else {
        lua_pushnil(L);
      }
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

struct GrantsMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Grants";}
  static std::string Name() {return TableName() + "Meta";}

  using Type = ACLGrantMap;

  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto map = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    const auto it = map->find(std::string(index));
    if (it == map->end()) {
      lua_pushnil(L);
    } else {
      create_metatable<GrantMetaTable>(L, name, index, false, &(it->second));
    }
    return ONE_RETURNVAL;
  }
  
  static int PairsClosure(lua_State* L) {
    return Pairs<Type, next<Type, GrantMetaTable>>(L);
  }
  
  static int LenClosure(lua_State* L) {
    const auto map = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));

    lua_pushinteger(L, map->size());

    return ONE_RETURNVAL;
  }
};

struct ACLMetaTable : public EmptyMetaTable {
  using Type = RGWAccessControlPolicy;

  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto acl = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const auto index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Owner") == 0) {
      create_metatable<OwnerMetaTable>(L, name, index, false, 
          &(acl->get_owner()));
    } else if (strcasecmp(index, "Grants") == 0) {
      create_metatable<GrantsMetaTable>(L, name, index, false, &(acl->get_acl().get_grant_map()));
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

struct StatementsMetaTable : public EmptyMetaTable {
  using Type = std::vector<rgw::IAM::Statement>;

  static std::string statement_to_string(const rgw::IAM::Statement& statement) {
    std::stringstream ss;
    ss << statement;
    return ss.str();
  }

  static int IndexClosure(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    const auto statements = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    ceph_assert(statements);

    const auto index = luaL_checkinteger(L, 2);

    if (index >= (int)statements->size() || index < 0) {
      lua_pushnil(L);
    } else {
      // TODO: policy language could be interpreted to lua and executed as such
      pushstring(L, statement_to_string((*statements)[index]));
    }
    return ONE_RETURNVAL;
  }
  
  static int PairsClosure(lua_State* L) {
    return Pairs<Type, stateless_iter>(L);
  }
  
  static int stateless_iter(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    auto statements = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    size_t next_it;
    if (lua_isnil(L, -1)) {
      next_it = 0;
    } else {
      const auto it = luaL_checkinteger(L, -1);
      next_it = it+1;
    }

    if (next_it >= statements->size()) {
      // index of the last element was provided
      lua_pushnil(L);
      lua_pushnil(L);
      // return nil, nil
    } else {
      lua_pushinteger(L, next_it);
      pushstring(L, statement_to_string((*statements)[next_it]));
      // return key, value
    }

    return TWO_RETURNVALS;
  }

  static int LenClosure(lua_State* L) {
    const auto statements = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));

    lua_pushinteger(L, statements->size());

    return ONE_RETURNVAL;
  }
};

struct PolicyMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto policy = reinterpret_cast<rgw::IAM::Policy*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const auto index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Text") == 0) {
      pushstring(L, policy->text);
    } else if (strcasecmp(index, "Id") == 0) {
      // TODO create pushstring for std::unique_ptr
      if (!policy->id) {
        lua_pushnil(L);
      } else {
        pushstring(L, policy->id.get());
      }
    } else if (strcasecmp(index, "Statements") == 0) {
      create_metatable<StatementsMetaTable>(L, name, index, false, &(policy->statements));
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

struct PoliciesMetaTable : public EmptyMetaTable {
  using Type = std::vector<rgw::IAM::Policy>;

  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto policies = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const auto index = luaL_checkinteger(L, 2);

    if (index >= (int)policies->size() || index < 0) {
      lua_pushnil(L);
    } else {
      create_metatable<PolicyMetaTable>(L, name, std::to_string(index), 
          false, &((*policies)[index]));
    }
    return ONE_RETURNVAL;
  }
  
  static int PairsClosure(lua_State* L) {
    return Pairs<Type, stateless_iter>(L);
  }
  
  static int stateless_iter(lua_State* L) {
    const auto name = table_name_upvalue(L);
    auto policies = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    size_t next_it;
    if (lua_isnil(L, -1)) {
      next_it = 0;
    } else {
      ceph_assert(lua_isinteger(L, -1));
      const auto it = luaL_checkinteger(L, -1);
      next_it = it+1;
    }

    if (next_it >= policies->size()) {
      // index of the last element was provided
      lua_pushnil(L);
      lua_pushnil(L);
      // return nil, nil
    } else {
      lua_pushinteger(L, next_it);
      create_metatable<PolicyMetaTable>(L, name, std::to_string(next_it), 
          false, &((*policies)[next_it]));
      // return key, value
    }

    return TWO_RETURNVALS;
  }

  static int LenClosure(lua_State* L) {
    const auto policies = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));

    lua_pushinteger(L, policies->size());

    return ONE_RETURNVAL;
  }
};

struct HTTPMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto info = reinterpret_cast<req_info*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const auto index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Parameters") == 0) {
      create_metatable<StringMapMetaTable<>>(L, name, index, false, &(info->args.get_params()));
    } else if (strcasecmp(index, "Resources") == 0) {
      // TODO: add non-const api to get resources
      create_metatable<StringMapMetaTable<>>(L, name, index, false,
          const_cast<std::map<std::string, std::string>*>(&(info->args.get_sub_resources())));
    } else if (strcasecmp(index, "Metadata") == 0) {
      create_metatable<StringMapMetaTable<meta_map_t, StringMapWriteableNewIndex<meta_map_t>>>(L, name, index, 
          false, &(info->x_meta_map));
    } else if (strcasecmp(index, "Host") == 0) {
      pushstring(L, info->host);
    } else if (strcasecmp(index, "Method") == 0) {
      pushstring(L, info->method);
    } else if (strcasecmp(index, "URI") == 0) {
      pushstring(L, info->request_uri);
    } else if (strcasecmp(index, "QueryString") == 0) {
      pushstring(L, info->request_params);
    } else if (strcasecmp(index, "Domain") == 0) {
      pushstring(L, info->domain);
    } else if (strcasecmp(index, "StorageClass") == 0) {
      pushstring(L, info->storage_class);
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }

  static int NewIndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    auto info = reinterpret_cast<req_info*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const auto index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "StorageClass") == 0) {
      info->storage_class = luaL_checkstring(L, 3);
   } else {
      return error_unknown_field(L, index, name);
   }
    return NO_RETURNVAL;
  }
};

struct CopyFromMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const auto index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Tenant") == 0) {
      pushstring(L, s->src_tenant_name);
    } else if (strcasecmp(index, "Bucket") == 0) {
      pushstring(L, s->src_bucket_name);
    } else if (strcasecmp(index, "Object") == 0) {
      create_metatable<ObjectMetaTable>(L, name, index, false, s->src_object);
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

struct ZoneGroupMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const auto index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Name") == 0) {
      pushstring(L, s->zonegroup_name);
    } else if (strcasecmp(index, "Endpoint") == 0) {
      pushstring(L, s->zonegroup_endpoint);
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

struct RequestMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto op_name = reinterpret_cast<const char*>(lua_touserdata(L, lua_upvalueindex(THIRD_UPVAL)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "RGWOp") == 0) {
      pushstring(L, op_name);
    } else if (strcasecmp(index, "DecodedURI") == 0) {
      pushstring(L, s->decoded_uri);
    } else if (strcasecmp(index, "ContentLength") == 0) {
      lua_pushinteger(L, s->content_length);
    } else if (strcasecmp(index, "GenericAttributes") == 0) {
      create_metatable<StringMapMetaTable<>>(L, name, index, false, &(s->generic_attrs));
    } else if (strcasecmp(index, "Response") == 0) {
      create_metatable<ResponseMetaTable>(L, name, index, false, &(s->err));
    } else if (strcasecmp(index, "SwiftAccountName") == 0) {
      if (s->dialect == "swift") {
        pushstring(L, s->account_name);
      } else {
        lua_pushnil(L);
      }
    } else if (strcasecmp(index, "Bucket") == 0) {
      create_metatable<BucketMetaTable>(L, name, index, false, s);
    } else if (strcasecmp(index, "Object") == 0) {
      create_metatable<ObjectMetaTable>(L, name, index, false, s->object);
    } else if (strcasecmp(index, "CopyFrom") == 0) {
      if (s->op_type == RGW_OP_COPY_OBJ) {
        create_metatable<CopyFromMetaTable>(L, name, index, false, s);
      } else {
        lua_pushnil(L);
      }
    } else if (strcasecmp(index, "ObjectOwner") == 0) {
      create_metatable<OwnerMetaTable>(L, name, index, false, &(s->owner));
    } else if (strcasecmp(index, "ZoneGroup") == 0) {
      create_metatable<ZoneGroupMetaTable>(L, name, index, false, s);
    } else if (strcasecmp(index, "UserACL") == 0) {
      create_metatable<ACLMetaTable>(L, name, index, false, &s->user_acl);
    } else if (strcasecmp(index, "BucketACL") == 0) {
      create_metatable<ACLMetaTable>(L, name, index, false, &s->bucket_acl);
    } else if (strcasecmp(index, "ObjectACL") == 0) {
      create_metatable<ACLMetaTable>(L, name, index, false, &s->object_acl);
    } else if (strcasecmp(index, "Environment") == 0) {
        create_metatable<StringMapMetaTable<rgw::IAM::Environment>>(L, name, index, false, &(s->env));
    } else if (strcasecmp(index, "Policy") == 0) {
      // TODO: create a wrapper to std::optional
      if (!s->iam_policy) {
        lua_pushnil(L);
      } else {
        create_metatable<PolicyMetaTable>(L, name, index, false, s->iam_policy.get_ptr());
      }
    } else if (strcasecmp(index, "UserPolicies") == 0) {
        create_metatable<PoliciesMetaTable>(L, name, index, false, &(s->iam_identity_policies));
    } else if (strcasecmp(index, "RGWId") == 0) {
      pushstring(L, s->host_id);
    } else if (strcasecmp(index, "HTTP") == 0) {
        create_metatable<HTTPMetaTable>(L, name, index, false, &(s->info));
    } else if (strcasecmp(index, "Time") == 0) {
      pushtime(L, s->time);
    } else if (strcasecmp(index, "Dialect") == 0) {
      pushstring(L, s->dialect);
    } else if (strcasecmp(index, "Id") == 0) {
      pushstring(L, s->req_id);
    } else if (strcasecmp(index, "TransactionId") == 0) {
      pushstring(L, s->trans_id);
    } else if (strcasecmp(index, "Tags") == 0) {
      create_metatable<StringMapMetaTable<RGWObjTags::tag_map_t>>(L, name, index, false, &(s->tagset.get_tags()));
    } else if (strcasecmp(index, "User") == 0) {
      if (!s->user) {
        lua_pushnil(L);
      } else {
        create_metatable<UserMetaTable>(L, name, index, false,
            const_cast<rgw_user*>(&(s->user->get_id())));
      }
    } else if (strcasecmp(index, "Trace") == 0) {
        create_metatable<TraceMetaTable>(L, name, index, false, s);
    } else {
      return error_unknown_field(L, index, name);
    }
    return ONE_RETURNVAL;
  }
};

void create_top_metatable(lua_State* L, req_state* s, const char* op_name) {
  static const char* request_table_name = "Request";
  create_metatable<RequestMetaTable>(L, "", request_table_name, true, s, const_cast<char*>(op_name));
  const auto type = lua_getglobal(L, request_table_name);
  ceph_assert(type == LUA_TTABLE);
}

int execute(
    rgw::sal::Driver* driver,
    RGWREST* rest,
    OpsLogSink* olog,
    req_state* s, 
    RGWOp* op,
    const std::string& script)
{
  lua_state_guard lguard(s->cct->_conf->rgw_lua_max_memory_per_state, s);
  auto L = lguard.get();
  if (!L) {
    ldpp_dout(s, 1) << "Failed to create state for Lua request context" << dendl;
    return -ENOMEM;
  }
  const char* op_name = op ? op->name() : "Unknown";

  int rc = 0;
  try {
    open_standard_libs(L);
    set_package_path(L, s->penv.lua.manager->luarocks_path());

    create_debug_action(L, s->cct);  
  
    create_top_metatable(L, s, const_cast<char*>(op_name));  

    // add the ops log action
    pushstring(L, RequestLogAction);
    lua_pushlightuserdata(L, rest);
    lua_pushlightuserdata(L, olog);
    lua_pushlightuserdata(L, s);
    lua_pushlightuserdata(L, op);
    lua_pushcclosure(L, RequestLog, FOUR_UPVALS);
    lua_rawset(L, -3);
  
    if (s->penv.lua.background) {
      s->penv.lua.background->create_background_metatable(L);
    }

    // execute the lua script
    if (luaL_dostring(L, script.c_str()) != LUA_OK) {
      const std::string err(lua_tostring(L, -1));
      ldpp_dout(s, 1) << "Lua ERROR: " << err << dendl;
      rc = -1;
    }
  } catch (const std::runtime_error& e) {
    ldpp_dout(s, 1) << "Lua ERROR: " << e.what() << dendl;
    rc = -1;
  }
  if (perfcounter) {
    perfcounter->inc((rc == -1 ? l_rgw_lua_script_fail : l_rgw_lua_script_ok), 1);
  }

  return rc;
}

} // namespace rgw::lua::request

