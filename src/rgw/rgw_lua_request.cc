#include <array>
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

  if (!s) {
    ldpp_dout(s, 1) << "Lua ERROR: missing request state, cannot use ops log"  << dendl;
    lua_pushinteger(L, -EINVAL);
    return ONE_RETURNVAL;
  }

  const auto rc = rgw_log_op(rest, s, op, olog);
  lua_pushinteger(L, rc);

  return ONE_RETURNVAL;
}

int SetAttribute(lua_State* L)  {
  auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(1)));

  if (!s->trace || !s->trace->IsRecording()) {
    return 0;
  }

  const auto key = luaL_checkstring(L, 1);
  const auto value_type = lua_type(L, 2);

  switch (value_type) {
    case LUA_TSTRING:
      s->trace->SetAttribute(key, lua_tostring(L, 2));
      break;

    case LUA_TNUMBER:
      if (lua_isinteger(L, 2)) {
        s->trace->SetAttribute(key, static_cast<int64_t>(lua_tointeger(L, 2)));
        break;
      }

      s->trace->SetAttribute(key, static_cast<double>(lua_tonumber(L, 2)));
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

  const auto args = lua_gettop(L);
  if (1 == args) {
    const auto log = luaL_checkstring(L, 1);
    s->trace->AddEvent(log);
    return 0;
  }

  if (2 != args) {
    return 0;
  }

  const auto event_name = luaL_checkstring(L, 1);
  std::unordered_map<const char*, jspan_attribute> event_values;
  push_nil(L);
  while (0 != lua_next(L, 2)) {
    if (LUA_TSTRING != lua_type(L, -2)) {
      // skip pair if key is not a string
      lua_pop(L, 1);
      continue;
    }

    const auto key = luaL_checkstring(L, -2);
    const auto value_type = lua_type(L, -1);
    switch (value_type) {
      case LUA_TSTRING:
        event_values.emplace(key, lua_tostring(L, -1));
        break;

      case LUA_TNUMBER:
        if (lua_isinteger(L, -1)) {
          event_values.emplace(key, static_cast<int64_t>(lua_tointeger(L, -1)));
          break;
        }

        event_values.emplace(key, static_cast<double>(lua_tonumber(L, -1)));
        break;
    }

    lua_pop(L, 1);
  }

  lua_pop(L, 1);
  s->trace->AddEvent(event_name, event_values);
  return 0;
}

struct ResponseMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto err = reinterpret_cast<const rgw_err*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<const rgw_err> {"HTTPStatusCode", lua_push_member<const rgw_err, &rgw_err::http_ret>},
      lua_field<const rgw_err> {"RGWCode", lua_push_member<const rgw_err, &rgw_err::ret>},
      lua_field<const rgw_err> {"HTTPStatus", lua_push_member<const rgw_err, &rgw_err::err_code>},
      lua_field<const rgw_err> {"Message", lua_push_member<const rgw_err, &rgw_err::message>},
    };

    return lua_dispatch_fields(L, *err, fields);
  }
  
  static int NewIndexClosure(lua_State* L) {
    auto err = reinterpret_cast<rgw_err*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<rgw_err> {"HTTPStatusCode", [](lua_State * const L, std::string_view,
                                               std::string_view, rgw_err& err) {
        err.http_ret = luaL_checkinteger(L, 3);
        return NO_RETURNVAL;
      }},
      lua_field<rgw_err> {"RGWCode", [](lua_State * const L, std::string_view,
                                        std::string_view, rgw_err& err) {
        err.ret = luaL_checkinteger(L, 3);
        return NO_RETURNVAL;
      }},
      lua_field<rgw_err> {"HTTPStatus", [](lua_State * const L, std::string_view,
                                           std::string_view, rgw_err& err) {
        err.err_code.assign(luaL_checkstring(L, 3));
        return NO_RETURNVAL;
      }},
      lua_field<rgw_err> {"Message", [](lua_State * const L, std::string_view,
                                        std::string_view, rgw_err& err) {
        err.message.assign(luaL_checkstring(L, 3));
        return NO_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, *err, fields);
  }
};

struct QuotaMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto info = reinterpret_cast<RGWQuotaInfo*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<RGWQuotaInfo> {"MaxSize", lua_push_member<RGWQuotaInfo, &RGWQuotaInfo::max_size>},
      lua_field<RGWQuotaInfo> {"MaxObjects", lua_push_member<RGWQuotaInfo, &RGWQuotaInfo::max_objects>},
      lua_field<RGWQuotaInfo> {"Enabled", lua_push_member<RGWQuotaInfo, &RGWQuotaInfo::enabled>},
      lua_field<RGWQuotaInfo> {"Rounded", [](lua_State * const L, std::string_view,
                                             std::string_view, RGWQuotaInfo& info) {
        lua_pushboolean(L, !info.check_on_raw);
        return ONE_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, *info, fields);
  }
};

struct PlacementRuleMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto rule = reinterpret_cast<rgw_placement_rule*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<rgw_placement_rule> {"Name", lua_push_member<rgw_placement_rule, &rgw_placement_rule::name>},
      lua_field<rgw_placement_rule> {"StorageClass", lua_push_member<rgw_placement_rule, &rgw_placement_rule::storage_class>},
    };

    return lua_dispatch_fields(L, *rule, fields);
  }
};

struct UserMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto user = reinterpret_cast<const rgw_user*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<const rgw_user> {"Tenant", lua_push_member<const rgw_user, &rgw_user::tenant>},
      lua_field<const rgw_user> {"Id", lua_push_member<const rgw_user, &rgw_user::id>},
    };

    return lua_dispatch_fields(L, *user, fields);
  }
};

struct TraceMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<req_state> {"Enable", lua_push_member<req_state, &req_state::trace_enabled>},
      lua_field<req_state> {"SetAttribute", [](lua_State * const L, std::string_view,
                                               std::string_view, req_state& s) {
        lua_pushlightuserdata(L, &s);
        lua_pushcclosure(L, SetAttribute, ONE_UPVAL);
        return ONE_RETURNVAL;
      }},
      lua_field<req_state> {"AddEvent", [](lua_State * const L, std::string_view,
                                           std::string_view, req_state& s) {
        lua_pushlightuserdata(L, &s);
        lua_pushcclosure(L, AddEvent, ONE_UPVAL);
        return ONE_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, *s, fields);
  }

  static int NewIndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<req_state> {"Enable", [](lua_State * const L, std::string_view,
                                         std::string_view, req_state& s) {
        s.trace_enabled = lua_toboolean(L, 3);
        return NO_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, *s, fields);
  }
};

struct OwnerMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto owner = reinterpret_cast<ACLOwner*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<ACLOwner> {"DisplayName", lua_push_member<ACLOwner, &ACLOwner::display_name>},
      lua_field<ACLOwner> {"User", [](lua_State * const L, std::string_view,
                                      std::string_view, ACLOwner& owner) {
        pushstring(L, to_string(owner.id));
        return ONE_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, *owner, fields);
  }
};

struct BucketTagsTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto key = lua_checkstring_view(L, 2);

    try {
      RGWObjTags tags;
      auto bl_it = bl->cbegin();
      tags.decode(bl_it);

      const auto& tag_map = tags.get_tags();
      const auto tag_it = find_string_map_entry(tag_map, key);

      if (tag_it != tag_map.end()) {
        pushstring(L, tag_it->second);
        return ONE_RETURNVAL;
      }
    } catch (const buffer::error& err) {
      return return_nil(L);
    }

    return return_nil(L);
  }

  static int LenClosure(lua_State* L) {
    const auto bl = reinterpret_cast<bufferlist*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));

    try {
      RGWObjTags tags;
      auto bl_it = bl->cbegin();
      tags.decode(bl_it);
      lua_pushinteger(L, tags.get_tags().size());
      return ONE_RETURNVAL;
    } catch (const buffer::error& err) {
      lua_pushinteger(L, 0);
      return ONE_RETURNVAL;
    }
  }
};

struct BucketMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto bucket = s->bucket.get();

    const auto index = lua_checkstring_view(L, 2);

    if (rgw::sal::Bucket::empty(bucket)) {
      if (!boost::iequals(index, "Name")) {
        return return_nil(L);
      }

      pushstring(L, s->init_state.url_bucket);
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "Tenant")) {
      pushstring(L, bucket->get_tenant());
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "Name")) {
      pushstring(L, bucket->get_name());
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "Marker")) {
      pushstring(L, bucket->get_marker());
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "Id")) {
      pushstring(L, bucket->get_bucket_id());
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "ZoneGroupId")) {
      pushstring(L, bucket->get_info().zonegroup);
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "CreationTime")) {
      pushtime(L, bucket->get_creation_time());
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "MTime")) {
      pushtime(L, bucket->get_modification_time());
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "Tags")) {
      const auto it = find_string_map_entry(s->bucket_attrs, RGW_ATTR_TAGS);

      if (it == s->bucket_attrs.end()) {
        return return_nil(L);
      }

      create_metatable<BucketTagsTable>(L, name, index, false, &(it->second));
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "Quota")) {
      create_metatable<QuotaMetaTable>(L, name, index, false, &(bucket->get_info().quota));
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "PlacementRule")) {
      create_metatable<PlacementRuleMetaTable>(L, name, index, false, &(bucket->get_info().placement_rule));
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "User")) {
      const rgw_owner& owner = bucket->get_owner();
      const auto u = std::get_if<rgw_user>(&owner);

      if (!u) {
        return return_nil(L);
      }

      create_metatable<UserMetaTable>(L, name, index, false, const_cast<rgw_user*>(u));
      return ONE_RETURNVAL;
    }

    if (boost::iequals(index, "Account")) {
      const rgw_owner& owner = bucket->get_owner();
      const auto a = std::get_if<rgw_account_id>(&owner);

      if (!a) {
        return return_nil(L);
      }

      pushstring(L, *a);
      return ONE_RETURNVAL;
    }

    return error_unknown_field(L, std::string {index}, name);
  }
  
  static int NewIndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto bucket = s->bucket.get();

    const auto index = lua_checkstring_view(L, 2);

    if (!rgw::sal::Bucket::empty(bucket) || !boost::iequals(index, "Name")) {
      return error_unknown_field(L, std::string {index}, name);
    }

    s->init_state.url_bucket = luaL_checkstring(L, 3);
    return NO_RETURNVAL;
  }
};

struct ObjectMetaTable : public EmptyMetaTable {
  using Type = rgw::sal::Object;

  static int IndexClosure(lua_State* L) {
    const auto obj = reinterpret_cast<const Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<const Type> {"Name", lua_push_getter<const Type, &Type::get_name>},
      lua_field<const Type> {"Instance", lua_push_getter<const Type, &Type::get_instance>},
      lua_field<const Type> {"Id", lua_push_getter<const Type, &Type::get_oid>},
      lua_field<const Type> {"Size", lua_push_getter<const Type, &Type::get_size>},
      lua_field<const Type> {"MTime", [](lua_State * const L, std::string_view,
                                         std::string_view, const Type& obj) {
        pushtime(L, obj.get_mtime());
        return ONE_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, *obj, fields);
  }
};

struct GrantMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto grant = reinterpret_cast<ACLGrant*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<ACLGrant> {"Type", [](lua_State * const L, std::string_view,
                                      std::string_view, ACLGrant& grant) {
        lua_pushinteger(L, grant.get_type().get_type());
        return ONE_RETURNVAL;
      }},
      lua_field<ACLGrant> {"User", [](lua_State * const L, std::string_view,
                                      std::string_view, ACLGrant& grant) {
        if (const auto user = grant.get_user(); user) {
          pushstring(L, to_string(user->id));
          return ONE_RETURNVAL;
        }

        return return_nil(L);
      }},
      lua_field<ACLGrant> {"Permission", [](lua_State * const L, std::string_view,
                                            std::string_view, ACLGrant& grant) {
        lua_pushinteger(L, grant.get_permission().get_permissions());
        return ONE_RETURNVAL;
      }},
      lua_field<ACLGrant> {"GroupType", [](lua_State * const L, std::string_view,
                                           std::string_view, ACLGrant& grant) {
        if (const auto group = grant.get_group(); group) {
          lua_pushinteger(L, group->type);
          return ONE_RETURNVAL;
        }

        return return_nil(L);
      }},
      lua_field<ACLGrant> {"Referer", [](lua_State * const L, std::string_view,
                                         std::string_view, ACLGrant& grant) {
        if (const auto referer = grant.get_referer(); referer) {
          pushstring(L, referer->url_spec);
          return ONE_RETURNVAL;
        }

        return return_nil(L);
      }},
    };

    return lua_dispatch_fields(L, *grant, fields);
  }
};

struct GrantsMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Grants";}
  static std::string Name() {return TableName() + "Meta";}

  using Type = ACLGrantMap;

  static int IndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto map = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    const auto index = lua_checkstring_view(L, 2);

    const auto it = find_string_map_entry(*map, index);

    if (it == map->end()) {
      return return_nil(L);
    }

    create_metatable<GrantMetaTable>(L, name, index, false, &(it->second));
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
    const auto acl = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<Type> {"Owner", [](lua_State * const L, const std::string_view name,
                                   const std::string_view index, Type& acl) {
        create_metatable<OwnerMetaTable>(L, name, index, false, &(acl.get_owner()));
        return ONE_RETURNVAL;
      }},
      lua_field<Type> {"Grants", [](lua_State * const L, const std::string_view name,
                                    const std::string_view index, Type& acl) {
        create_metatable<GrantsMetaTable>(L, name, index, false, &(acl.get_acl().get_grant_map()));
        return ONE_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, *acl, fields);
  }
};

static std::string statement_to_string(const rgw::IAM::Statement& statement)
{
  std::stringstream ss;
  ss << statement;
  return ss.str();
}

static void push_statement(lua_State * const L,
                           const std::string_view,
                           const std::size_t,
                           rgw::IAM::Statement& statement)
{
  // TODO: policy language could be interpreted to lua and executed as such
  pushstring(L, statement_to_string(statement));
}

struct StatementsMetaTable :
  IndexedContainerMetaTable<std::vector<rgw::IAM::Statement>, void, push_statement> {
};

struct PolicyMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto policy = reinterpret_cast<rgw::IAM::Policy*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<rgw::IAM::Policy> {"Text", lua_push_member<rgw::IAM::Policy, &rgw::IAM::Policy::text>},
      lua_field<rgw::IAM::Policy> {"Id", [](lua_State * const L, std::string_view,
                                            std::string_view, rgw::IAM::Policy& policy) {
        // TODO create pushstring for std::unique_ptr
        if (!policy.id) {
          return return_nil(L);
        }

        pushstring(L, policy.id.get());
        return ONE_RETURNVAL;
      }},
      lua_field<rgw::IAM::Policy> {"Statements", [](lua_State * const L, const std::string_view name,
                                                    const std::string_view index,
                                                    rgw::IAM::Policy& policy) {
        create_metatable<StatementsMetaTable>(
          L, name, index, false, &(policy.statements));
        return ONE_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, *policy, fields);
  }
};

struct PoliciesMetaTable :
  IndexedContainerMetaTable<std::vector<rgw::IAM::Policy>, PolicyMetaTable> {
};

struct HTTPMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto info = reinterpret_cast<req_info*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<req_info> {"Parameters", [](lua_State * const L, const std::string_view name,
                                            const std::string_view index, req_info& info) {
        create_metatable<StringMapMetaTable<RGWHTTPArgs::name_value_map>>(
          L, name, index, false, &(info.args.get_params()));
        return ONE_RETURNVAL;
      }},
      lua_field<req_info> {"Resources", [](lua_State * const L, const std::string_view name,
                                           const std::string_view index, req_info& info) {
        // TODO: add non-const api to get resources
        create_metatable<StringMapMetaTable<RGWHTTPArgs::name_value_map>>(
          L, name, index, false,
          const_cast<RGWHTTPArgs::name_value_map *>(&(info.args.get_sub_resources())));
        return ONE_RETURNVAL;
      }},
      lua_field<req_info> {"Metadata", [](lua_State * const L, const std::string_view name,
                                          const std::string_view index, req_info& info) {
        create_metatable<StringMapMetaTable<meta_map_t, StringMapWriteableNewIndex<meta_map_t>>>(
          L, name, index, false, &(info.x_meta_map));
        return ONE_RETURNVAL;
      }},
      lua_field<req_info> {"Host", lua_push_member<req_info, &req_info::host>},
      lua_field<req_info> {"Method", lua_push_member<req_info, &req_info::method>},
      lua_field<req_info> {"URI", lua_push_member<req_info, &req_info::request_uri>},
      lua_field<req_info> {"QueryString", lua_push_member<req_info, &req_info::request_params>},
      lua_field<req_info> {"Domain", lua_push_member<req_info, &req_info::domain>},
      lua_field<req_info> {"StorageClass", lua_push_member<req_info, &req_info::storage_class>},
    };

    return lua_dispatch_fields(L, *info, fields);
  }

  static int NewIndexClosure(lua_State* L) {
    auto info = reinterpret_cast<req_info*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<req_info> {"StorageClass", [](lua_State * const L, std::string_view,
                                             std::string_view, req_info& info) {
        info.storage_class = luaL_checkstring(L, 3);
        return NO_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, *info, fields);
  }
};

struct CopyFromMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<req_state> {"Tenant", lua_push_member<req_state, &req_state::src_tenant_name>},
      lua_field<req_state> {"Bucket", lua_push_member<req_state, &req_state::src_bucket_name>},
      lua_field<req_state> {"Object", [](lua_State * const L, const std::string_view name,
                                         const std::string_view index, req_state& s) {
        create_metatable<ObjectMetaTable>(L, name, index, false, s.src_object);
        return ONE_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, *s, fields);
  }
};

struct ZoneGroupMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_field<req_state> {"Name", lua_push_member<req_state, &req_state::zonegroup_name>},
      lua_field<req_state> {"Endpoint", lua_push_member<req_state, &req_state::zonegroup_endpoint>},
    };

    return lua_dispatch_fields(L, *s, fields);
  }
};

struct request_lua_context {
  req_state& s;
  const char *op_name;
};

struct RequestMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto op_name = reinterpret_cast<const char*>(lua_touserdata(L, lua_upvalueindex(THIRD_UPVAL)));
    request_lua_context ctx {*s, op_name};

    static constexpr std::array fields {
      lua_field<request_lua_context> {"RGWOp", [](lua_State * const L, std::string_view,
                                                  std::string_view, request_lua_context& ctx) {
        pushstring(L, ctx.op_name);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"DecodedURI", [](lua_State * const L, std::string_view,
                                                       std::string_view, request_lua_context& ctx) {
        pushstring(L, ctx.s.decoded_uri);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"ContentLength", [](lua_State * const L, std::string_view,
                                                          std::string_view, request_lua_context& ctx) {
        lua_pushinteger(L, ctx.s.content_length);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"GenericAttributes", [](lua_State * const L, const std::string_view name,
                                                              const std::string_view index, request_lua_context& ctx) {
        create_metatable<StringMapMetaTable<>>(L, name, index, false, &(ctx.s.generic_attrs));
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"Response", [](lua_State * const L, const std::string_view name,
                                                     const std::string_view index, request_lua_context& ctx) {
        create_metatable<ResponseMetaTable>(L, name, index, false, &(ctx.s.err));
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"SwiftAccountName", [](lua_State * const L, std::string_view,
                                                             std::string_view, request_lua_context& ctx) {
        if ("swift" == ctx.s.dialect) {
          pushstring(L, ctx.s.account_name);
          return ONE_RETURNVAL;
        }

        return return_nil(L);
      }},
      lua_field<request_lua_context> {"Bucket", [](lua_State * const L, const std::string_view name,
                                                   const std::string_view index, request_lua_context& ctx) {
        create_metatable<BucketMetaTable>(L, name, index, false, &ctx.s);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"Object", [](lua_State * const L, const std::string_view name,
                                                   const std::string_view index, request_lua_context& ctx) {
        create_metatable<ObjectMetaTable>(L, name, index, false, ctx.s.object);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"CopyFrom", [](lua_State * const L, const std::string_view name,
                                                     const std::string_view index, request_lua_context& ctx) {
        if (RGW_OP_COPY_OBJ == ctx.s.op_type) {
          create_metatable<CopyFromMetaTable>(L, name, index, false, &ctx.s);
          return ONE_RETURNVAL;
        }

        return return_nil(L);
      }},
      lua_field<request_lua_context> {"ObjectOwner", [](lua_State * const L, const std::string_view name,
                                                        const std::string_view index, request_lua_context& ctx) {
        create_metatable<OwnerMetaTable>(L, name, index, false, &(ctx.s.owner));
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"ZoneGroup", [](lua_State * const L, const std::string_view name,
                                                      const std::string_view index, request_lua_context& ctx) {
        create_metatable<ZoneGroupMetaTable>(L, name, index, false, &ctx.s);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"UserACL", [](lua_State * const L, const std::string_view name,
                                                    const std::string_view index, request_lua_context& ctx) {
        create_metatable<ACLMetaTable>(L, name, index, false, &ctx.s.user_acl);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"BucketACL", [](lua_State * const L, const std::string_view name,
                                                      const std::string_view index, request_lua_context& ctx) {
        create_metatable<ACLMetaTable>(L, name, index, false, &ctx.s.bucket_acl);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"ObjectACL", [](lua_State * const L, const std::string_view name,
                                                      const std::string_view index, request_lua_context& ctx) {
        create_metatable<ACLMetaTable>(L, name, index, false, &ctx.s.object_acl);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"Environment", [](lua_State * const L, const std::string_view name,
                                                        const std::string_view index, request_lua_context& ctx) {
        create_metatable<StringMapMetaTable<rgw::IAM::Environment>>(
          L, name, index, false, &(ctx.s.env));
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"Policy", [](lua_State * const L, const std::string_view name,
                                                   const std::string_view index, request_lua_context& ctx) {
        // TODO: create a wrapper to std::optional
        if (!ctx.s.iam_policy) {
          return return_nil(L);
        }

        create_metatable<PolicyMetaTable>(L, name, index, false, ctx.s.iam_policy.get_ptr());
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"UserPolicies", [](lua_State * const L, const std::string_view name,
                                                         const std::string_view index, request_lua_context& ctx) {
        create_metatable<PoliciesMetaTable>(L, name, index, false, &(ctx.s.iam_identity_policies));
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"RGWId", [](lua_State * const L, std::string_view,
                                                  std::string_view, request_lua_context& ctx) {
        pushstring(L, ctx.s.host_id);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"HTTP", [](lua_State * const L, const std::string_view name,
                                                 const std::string_view index, request_lua_context& ctx) {
        create_metatable<HTTPMetaTable>(L, name, index, false, &(ctx.s.info));
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"Time", [](lua_State * const L, std::string_view,
                                                 std::string_view, request_lua_context& ctx) {
        pushtime(L, ctx.s.time);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"Dialect", [](lua_State * const L, std::string_view,
                                                    std::string_view, request_lua_context& ctx) {
        pushstring(L, ctx.s.dialect);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"Id", [](lua_State * const L, std::string_view,
                                               std::string_view, request_lua_context& ctx) {
        pushstring(L, ctx.s.req_id);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"TransactionId", [](lua_State * const L, std::string_view,
                                                          std::string_view, request_lua_context& ctx) {
        pushstring(L, ctx.s.trans_id);
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"Tags", [](lua_State * const L, const std::string_view name,
                                                 const std::string_view index, request_lua_context& ctx) {
        create_metatable<StringMapMetaTable<RGWObjTags::tag_map_t>>(
          L, name, index, false, &(ctx.s.tagset.get_tags()));
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"User", [](lua_State * const L, const std::string_view name,
                                                 const std::string_view index, request_lua_context& ctx) {
        if (!ctx.s.user) {
          return return_nil(L);
        }

        create_metatable<UserMetaTable>(
          L, name, index, false, const_cast<rgw_user*>(&(ctx.s.user->get_id())));
        return ONE_RETURNVAL;
      }},
      lua_field<request_lua_context> {"Trace", [](lua_State * const L, const std::string_view name,
                                                  const std::string_view index, request_lua_context& ctx) {
        create_metatable<TraceMetaTable>(L, name, index, false, &ctx.s);
        return ONE_RETURNVAL;
      }},
    };

    return lua_dispatch_fields(L, ctx, fields);
  }
};

void create_top_metatable(lua_State* L, req_state* s, const char* op_name) {
  static const char* request_table_name = "Request";
  create_metatable<RequestMetaTable>(L, "", request_table_name, true, s, const_cast<char*>(op_name));
  const auto type = lua_getglobal(L, request_table_name);
  ceph_assert(type == LUA_TTABLE);
}

int execute(
    RGWREST* rest,
    OpsLogSink* olog,
    req_state* s, 
    RGWOp* op,
    const rgw::lua::LuaCodeType& code,
    int& script_return_code)
{
  lua_state_guard lguard(s->cct->_conf->rgw_lua_max_memory_per_state,
                         s->cct->_conf->rgw_lua_max_runtime_per_state, s);
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

    //Make special error code available to lua scripts
    lua_pushinteger(L, -EPERM);
    lua_setglobal(L, "RGW_ABORT_REQUEST");
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
    rc = rgw::lua::lua_execute(L, s, code);

    if (0 == rc) {
      if (lua_isinteger(L, -1)) {
        script_return_code = static_cast<int>(lua_tointeger(L, -1));
        ldpp_dout(s, 20) << "Lua script executed successfully and returned code: " << script_return_code << dendl;
      }

      if (!lua_isinteger(L, -1)) {
        ldpp_dout(s, 20) << "Lua script executed, but did not return an integer. Ignoring return code." << dendl;
      }
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

int execute(
    RGWREST* rest,
    OpsLogSink* olog,
    req_state* s, 
    RGWOp* op,
    const rgw::lua::LuaCodeType& code)
{
  int dummy_script_return_code = 0;
  return execute(rest, olog, s, op, code, dummy_script_return_code);
}

} // namespace rgw::lua::request
