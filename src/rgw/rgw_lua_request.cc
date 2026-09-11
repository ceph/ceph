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

  if (const auto args = lua_gettop(L); args == 1) {
    const auto log = luaL_checkstring(L, 1);
    s->trace->AddEvent(log);
  } else if (args == 2) {
    const auto event_name = luaL_checkstring(L, 1);
    std::unordered_map<const char*, jspan_attribute> event_values;
    lua_pushnil(L);
    while (lua_next(L, 2) != 0) {
      if (lua_type(L, -2) != LUA_TSTRING) {
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
    const auto err = reinterpret_cast<const rgw_err*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_const_member<&rgw_err::http_ret>("HTTPStatusCode"),
      lua_const_member<&rgw_err::ret>("RGWCode"),
      lua_const_member<&rgw_err::err_code>("HTTPStatus"),
      lua_const_member<&rgw_err::message>("Message"),
    };

    return lua_dispatch_fields(L, *err, fields);
  }
  
  static int NewIndexClosure(lua_State* L) {
    auto err = reinterpret_cast<rgw_err*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_callback("HTTPStatusCode", [](lua_field_context ctx, rgw_err& err) {
        err.http_ret = luaL_checkinteger(ctx.L, 3);
        return NO_RETURNVAL;
      }),
      lua_callback("RGWCode", [](lua_field_context ctx, rgw_err& err) {
        err.ret = luaL_checkinteger(ctx.L, 3);
        return NO_RETURNVAL;
      }),
      lua_callback("HTTPStatus", [](lua_field_context ctx, rgw_err& err) {
        err.err_code.assign(luaL_checkstring(ctx.L, 3));
        return NO_RETURNVAL;
      }),
      lua_callback("Message", [](lua_field_context ctx, rgw_err& err) {
        err.message.assign(luaL_checkstring(ctx.L, 3));
        return NO_RETURNVAL;
      }),
    };

    return lua_dispatch_fields(L, *err, fields);
  }
};

struct QuotaMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto info = reinterpret_cast<RGWQuotaInfo*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_member<&RGWQuotaInfo::max_size>("MaxSize"),
      lua_member<&RGWQuotaInfo::max_objects>("MaxObjects"),
      lua_member<&RGWQuotaInfo::enabled>("Enabled"),
      lua_callback("Rounded", [](lua_field_context ctx, RGWQuotaInfo& info) {
        lua_pushboolean(ctx.L, !info.check_on_raw);
        return ONE_RETURNVAL;
      }),
    };

    return lua_dispatch_fields(L, *info, fields);
  }
};

struct PlacementRuleMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto rule = reinterpret_cast<rgw_placement_rule*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_member<&rgw_placement_rule::name>("Name"),
      lua_member<&rgw_placement_rule::storage_class>("StorageClass"),
    };

    return lua_dispatch_fields(L, *rule, fields);
  }
};

struct UserMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto user = reinterpret_cast<const rgw_user*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_const_member<&rgw_user::tenant>("Tenant"),
      lua_const_member<&rgw_user::id>("Id"),
    };

    return lua_dispatch_fields(L, *user, fields);
  }
};

struct TraceMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_member<&req_state::trace_enabled>("Enable"),
      lua_callback("SetAttribute", [](lua_field_context ctx, req_state& s) {
        lua_pushlightuserdata(ctx.L, &s);
        lua_pushcclosure(ctx.L, SetAttribute, ONE_UPVAL);
        return ONE_RETURNVAL;
      }),
      lua_callback("AddEvent", [](lua_field_context ctx, req_state& s) {
        lua_pushlightuserdata(ctx.L, &s);
        lua_pushcclosure(ctx.L, AddEvent, ONE_UPVAL);
        return ONE_RETURNVAL;
      }),
    };

    return lua_dispatch_fields(L, *s, fields);
  }

  static int NewIndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_callback("Enable", [](lua_field_context ctx, req_state& s) {
        s.trace_enabled = lua_toboolean(ctx.L, 3);
        return NO_RETURNVAL;
      }),
    };

    return lua_dispatch_fields(L, *s, fields);
  }
};

struct OwnerMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto owner = reinterpret_cast<ACLOwner*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_member<&ACLOwner::display_name>("DisplayName"),
      lua_callback("User", [](lua_field_context ctx, ACLOwner& owner) {
        pushstring(ctx.L, to_string(owner.id));
        return ONE_RETURNVAL;
      }),
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
      if (const auto tag_it = find_string_map_entry(tag_map, key); tag_it != tag_map.end()) {
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
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto bucket = s->bucket.get();

    const auto index = lua_checkstring_view(L, 2);

    if (rgw::sal::Bucket::empty(bucket)) {
      if (boost::iequals(index, "Name")) {
        pushstring(L, s->init_state.url_bucket);
        return ONE_RETURNVAL;
      }

      return return_nil(L);
    }

    static constexpr std::array fields {
      lua_callback("Tenant", [](lua_field_context ctx, req_state& s) {
        pushstring(ctx.L, s.bucket->get_tenant());
        return ONE_RETURNVAL;
      }),
      lua_callback("Name", [](lua_field_context ctx, req_state& s) {
        pushstring(ctx.L, s.bucket->get_name());
        return ONE_RETURNVAL;
      }),
      lua_callback("Marker", [](lua_field_context ctx, req_state& s) {
        pushstring(ctx.L, s.bucket->get_marker());
        return ONE_RETURNVAL;
      }),
      lua_callback("Id", [](lua_field_context ctx, req_state& s) {
        pushstring(ctx.L, s.bucket->get_bucket_id());
        return ONE_RETURNVAL;
      }),
      lua_callback("ZoneGroupId", [](lua_field_context ctx, req_state& s) {
        pushstring(ctx.L, s.bucket->get_info().zonegroup);
        return ONE_RETURNVAL;
      }),
      lua_callback("CreationTime", [](lua_field_context ctx, req_state& s) {
        pushtime(ctx.L, s.bucket->get_creation_time());
        return ONE_RETURNVAL;
      }),
      lua_callback("MTime", [](lua_field_context ctx, req_state& s) {
        pushtime(ctx.L, s.bucket->get_modification_time());
        return ONE_RETURNVAL;
      }),
      lua_callback("Tags", [](lua_field_context ctx, req_state& s) {
        if (const auto it = find_string_map_entry(s.bucket_attrs, RGW_ATTR_TAGS); it != s.bucket_attrs.end()) {
          create_metatable<BucketTagsTable>(
            ctx.L, ctx.table_name, ctx.field_name, false, &(it->second));
          return ONE_RETURNVAL;
        }

        return return_nil(ctx.L);
      }),
      lua_callback("Quota", [](lua_field_context ctx, req_state& s) {
        create_metatable<QuotaMetaTable>(
          ctx.L, ctx.table_name, ctx.field_name, false, &(s.bucket->get_info().quota));
        return ONE_RETURNVAL;
      }),
      lua_callback("PlacementRule", [](lua_field_context ctx, req_state& s) {
        create_metatable<PlacementRuleMetaTable>(ctx.L, ctx.table_name, ctx.field_name, false,
                                                 &(s.bucket->get_info().placement_rule));
        return ONE_RETURNVAL;
      }),
      lua_callback("User", [](lua_field_context ctx, req_state& s) {
        if (const auto u = std::get_if<rgw_user>(&s.bucket->get_owner()); u) {
          create_metatable<UserMetaTable>(
            ctx.L, ctx.table_name, ctx.field_name, false, const_cast<rgw_user*>(u));
          return ONE_RETURNVAL;
        }

        return return_nil(ctx.L);
      }),
      lua_callback("Account", [](lua_field_context ctx, req_state& s) {
        if (const auto a = std::get_if<rgw_account_id>(&s.bucket->get_owner()); a) {
          pushstring(ctx.L, *a);
          return ONE_RETURNVAL;
        }

        return return_nil(ctx.L);
      }),
    };

    return lua_dispatch_fields(L, *s, fields);
  }
  
  static int NewIndexClosure(lua_State* L) {
    const auto name = table_name_upvalue(L);
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    const auto bucket = s->bucket.get();

    const auto index = lua_checkstring_view(L, 2);

    if (rgw::sal::Bucket::empty(bucket)) {
      if (boost::iequals(index, "Name")) {
        s->init_state.url_bucket = luaL_checkstring(L, 3);
        return NO_RETURNVAL;
      }
    }

    return error_unknown_field(L, std::string {index}, name);
  }
};

struct ObjectMetaTable : public EmptyMetaTable {
  using Type = rgw::sal::Object;

  static int IndexClosure(lua_State* L) {
    const auto obj = reinterpret_cast<const Type*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_getter<&Type::get_name>("Name"),
      lua_getter<&Type::get_instance>("Instance"),
      lua_getter<&Type::get_oid>("Id"),
      lua_getter<&Type::get_size>("Size"),
      lua_callback("MTime", [](lua_field_context ctx, const Type& obj) {
        pushtime(ctx.L, obj.get_mtime());
        return ONE_RETURNVAL;
      }),
    };

    return lua_dispatch_fields(L, *obj, fields);
  }
};

struct GrantMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto grant = reinterpret_cast<ACLGrant*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_callback("Type", [](lua_field_context ctx, ACLGrant& grant) {
        lua_pushinteger(ctx.L, grant.get_type().get_type());
        return ONE_RETURNVAL;
      }),
      lua_callback("User", [](lua_field_context ctx, ACLGrant& grant) {
        if (const auto user = grant.get_user(); user) {
          pushstring(ctx.L, to_string(user->id));
          return ONE_RETURNVAL;
        }

        return return_nil(ctx.L);
      }),
      lua_callback("Permission", [](lua_field_context ctx, ACLGrant& grant) {
        lua_pushinteger(ctx.L, grant.get_permission().get_permissions());
        return ONE_RETURNVAL;
      }),
      lua_callback("GroupType", [](lua_field_context ctx, ACLGrant& grant) {
        if (const auto group = grant.get_group(); group) {
          lua_pushinteger(ctx.L, group->type);
          return ONE_RETURNVAL;
        }

        return return_nil(ctx.L);
      }),
      lua_callback("Referer", [](lua_field_context ctx, ACLGrant& grant) {
        if (const auto referer = grant.get_referer(); referer) {
          pushstring(ctx.L, referer->url_spec);
          return ONE_RETURNVAL;
        }

        return return_nil(ctx.L);
      }),
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
      lua_callback("Owner", [](lua_field_context ctx, Type& acl) {
        create_metatable<OwnerMetaTable>(
          ctx.L, ctx.table_name, ctx.field_name, false, &(acl.get_owner()));
        return ONE_RETURNVAL;
      }),
      lua_callback("Grants", [](lua_field_context ctx, Type& acl) {
        create_metatable<GrantsMetaTable>(
          ctx.L, ctx.table_name, ctx.field_name, false, &(acl.get_acl().get_grant_map()));
        return ONE_RETURNVAL;
      }),
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
      lua_member<&rgw::IAM::Policy::text>("Text"),
      lua_callback("Id", [](lua_field_context ctx, rgw::IAM::Policy& policy) {
        // TODO create pushstring for std::unique_ptr
        if (!policy.id) {
          return return_nil(ctx.L);
        }

        pushstring(ctx.L, policy.id.get());
        return ONE_RETURNVAL;
      }),
      lua_callback("Statements", [](lua_field_context ctx, rgw::IAM::Policy& policy) {
        create_metatable<StatementsMetaTable>(
          ctx.L, ctx.table_name, ctx.field_name, false, &(policy.statements));
        return ONE_RETURNVAL;
      }),
    };

    return lua_dispatch_fields(L, *policy, fields);
  }
};

struct PoliciesMetaTable :
  IndexedContainerMetaTable<std::vector<rgw::IAM::Policy>, PolicyMetaTable> {
};

struct HTTPMetaTable : public EmptyMetaTable {
  using http_args_map = std::map<std::string, std::string>;

  static int IndexClosure(lua_State* L) {
    const auto info = reinterpret_cast<req_info*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_callback("Parameters", [](lua_field_context ctx, req_info& info) {
        create_metatable<StringMapMetaTable<http_args_map>>(
          ctx.L, ctx.table_name, ctx.field_name, false, &(info.args.get_params()));
        return ONE_RETURNVAL;
      }),
      lua_callback("Resources", [](lua_field_context ctx, req_info& info) {
        // TODO: add non-const api to get resources
        create_metatable<StringMapMetaTable<http_args_map>>(
          ctx.L, ctx.table_name, ctx.field_name, false,
          const_cast<http_args_map *>(&(info.args.get_sub_resources())));
        return ONE_RETURNVAL;
      }),
      lua_callback("Metadata", [](lua_field_context ctx, req_info& info) {
        create_metatable<StringMapMetaTable<meta_map_t, StringMapWriteableNewIndex<meta_map_t>>>(
          ctx.L, ctx.table_name, ctx.field_name, false, &(info.x_meta_map));
        return ONE_RETURNVAL;
      }),
      lua_member<&req_info::host>("Host"),
      lua_member<&req_info::method>("Method"),
      lua_member<&req_info::request_uri>("URI"),
      lua_member<&req_info::request_params>("QueryString"),
      lua_member<&req_info::domain>("Domain"),
      lua_member<&req_info::storage_class>("StorageClass"),
    };

    return lua_dispatch_fields(L, *info, fields);
  }

  static int NewIndexClosure(lua_State* L) {
    auto info = reinterpret_cast<req_info*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_callback("StorageClass", [](lua_field_context ctx, req_info& info) {
        info.storage_class = luaL_checkstring(ctx.L, 3);
        return NO_RETURNVAL;
      }),
    };

    return lua_dispatch_fields(L, *info, fields);
  }
};

struct CopyFromMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_member<&req_state::src_tenant_name>("Tenant"),
      lua_member<&req_state::src_bucket_name>("Bucket"),
      lua_callback("Object", [](lua_field_context ctx, req_state& s) {
        create_metatable<ObjectMetaTable>(ctx.L, ctx.table_name, ctx.field_name, false, s.src_object);
        return ONE_RETURNVAL;
      }),
    };

    return lua_dispatch_fields(L, *s, fields);
  }
};

struct ZoneGroupMetaTable : public EmptyMetaTable {
  static int IndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));

    static constexpr std::array fields {
      lua_member<&req_state::zonegroup_name>("Name"),
      lua_member<&req_state::zonegroup_endpoint>("Endpoint"),
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
      lua_callback("RGWOp", [](lua_field_context field, request_lua_context& ctx) {
        pushstring(field.L, ctx.op_name);
        return ONE_RETURNVAL;
      }),
      lua_callback("DecodedURI", [](lua_field_context field, request_lua_context& ctx) {
        pushstring(field.L, ctx.s.decoded_uri);
        return ONE_RETURNVAL;
      }),
      lua_callback("ContentLength", [](lua_field_context field, request_lua_context& ctx) {
        lua_pushinteger(field.L, ctx.s.content_length);
        return ONE_RETURNVAL;
      }),
      lua_callback("GenericAttributes", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<StringMapMetaTable<>>(
          field.L, field.table_name, field.field_name, false, &(ctx.s.generic_attrs));
        return ONE_RETURNVAL;
      }),
      lua_callback("Response", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<ResponseMetaTable>(field.L, field.table_name, field.field_name, false, &(ctx.s.err));
        return ONE_RETURNVAL;
      }),
      lua_callback("SwiftAccountName", [](lua_field_context field, request_lua_context& ctx) {
        if ("swift" == ctx.s.dialect) {
          pushstring(field.L, ctx.s.account_name);
          return ONE_RETURNVAL;
        }

        return return_nil(field.L);
      }),
      lua_callback("Bucket", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<BucketMetaTable>(field.L, field.table_name, field.field_name, false, &ctx.s);
        return ONE_RETURNVAL;
      }),
      lua_callback("Object", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<ObjectMetaTable>(field.L, field.table_name, field.field_name, false, ctx.s.object);
        return ONE_RETURNVAL;
      }),
      lua_callback("CopyFrom", [](lua_field_context field, request_lua_context& ctx) {
        if (RGW_OP_COPY_OBJ == ctx.s.op_type) {
          create_metatable<CopyFromMetaTable>(field.L, field.table_name, field.field_name, false, &ctx.s);
          return ONE_RETURNVAL;
        }

        return return_nil(field.L);
      }),
      lua_callback("ObjectOwner", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<OwnerMetaTable>(field.L, field.table_name, field.field_name, false, &(ctx.s.owner));
        return ONE_RETURNVAL;
      }),
      lua_callback("ZoneGroup", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<ZoneGroupMetaTable>(field.L, field.table_name, field.field_name, false, &ctx.s);
        return ONE_RETURNVAL;
      }),
      lua_callback("UserACL", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<ACLMetaTable>(field.L, field.table_name, field.field_name, false, &ctx.s.user_acl);
        return ONE_RETURNVAL;
      }),
      lua_callback("BucketACL", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<ACLMetaTable>(field.L, field.table_name, field.field_name, false, &ctx.s.bucket_acl);
        return ONE_RETURNVAL;
      }),
      lua_callback("ObjectACL", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<ACLMetaTable>(field.L, field.table_name, field.field_name, false, &ctx.s.object_acl);
        return ONE_RETURNVAL;
      }),
      lua_callback("Environment", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<StringMapMetaTable<rgw::IAM::Environment>>(
          field.L, field.table_name, field.field_name, false, &(ctx.s.env));
        return ONE_RETURNVAL;
      }),
      lua_callback("Policy", [](lua_field_context field, request_lua_context& ctx) {
        // TODO: create a wrapper to std::optional
        if (!ctx.s.iam_policy) {
          return return_nil(field.L);
        }

        create_metatable<PolicyMetaTable>(
          field.L, field.table_name, field.field_name, false, ctx.s.iam_policy.get_ptr());
        return ONE_RETURNVAL;
      }),
      lua_callback("UserPolicies", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<PoliciesMetaTable>(
          field.L, field.table_name, field.field_name, false, &(ctx.s.iam_identity_policies));
        return ONE_RETURNVAL;
      }),
      lua_callback("RGWId", [](lua_field_context field, request_lua_context& ctx) {
        pushstring(field.L, ctx.s.host_id);
        return ONE_RETURNVAL;
      }),
      lua_callback("HTTP", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<HTTPMetaTable>(field.L, field.table_name, field.field_name, false, &(ctx.s.info));
        return ONE_RETURNVAL;
      }),
      lua_callback("Time", [](lua_field_context field, request_lua_context& ctx) {
        pushtime(field.L, ctx.s.time);
        return ONE_RETURNVAL;
      }),
      lua_callback("Dialect", [](lua_field_context field, request_lua_context& ctx) {
        pushstring(field.L, ctx.s.dialect);
        return ONE_RETURNVAL;
      }),
      lua_callback("Id", [](lua_field_context field, request_lua_context& ctx) {
        pushstring(field.L, ctx.s.req_id);
        return ONE_RETURNVAL;
      }),
      lua_callback("TransactionId", [](lua_field_context field, request_lua_context& ctx) {
        pushstring(field.L, ctx.s.trans_id);
        return ONE_RETURNVAL;
      }),
      lua_callback("Tags", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<StringMapMetaTable<RGWObjTags::tag_map_t>>(
          field.L, field.table_name, field.field_name, false, &(ctx.s.tagset.get_tags()));
        return ONE_RETURNVAL;
      }),
      lua_callback("User", [](lua_field_context field, request_lua_context& ctx) {
        if (!ctx.s.user) {
          return return_nil(field.L);
        }

        create_metatable<UserMetaTable>(
          field.L, field.table_name, field.field_name, false, const_cast<rgw_user*>(&(ctx.s.user->get_id())));
        return ONE_RETURNVAL;
      }),
      lua_callback("Trace", [](lua_field_context field, request_lua_context& ctx) {
        create_metatable<TraceMetaTable>(field.L, field.table_name, field.field_name, false, &ctx.s);
        return ONE_RETURNVAL;
      }),
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

    if (!rc) {
      if (lua_isinteger(L, -1)) {
        script_return_code = static_cast<int>(lua_tointeger(L, -1));
        ldpp_dout(s, 20) << "Lua script executed successfully and returned code: " << script_return_code << dendl;
      } else {
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
