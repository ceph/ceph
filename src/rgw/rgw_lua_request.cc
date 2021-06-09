#include <sstream>
#include <stdexcept>
#include <lua.hpp>
#include "common/dout.h"
#include "services/svc_zone.h"
#include "rgw_lua_utils.h"
#include "rgw_lua.h"
#include "rgw_common.h"
#include "rgw_log.h"
#include "rgw_process.h"
#include "rgw_zone.h"
#include "rgw_acl.h"
#include "rgw_sal_rados.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua::request {

// closure that perform ops log action
// e.g.
//    Request.Log()
//
constexpr const char* RequestLogAction{"Log"};

int RequestLog(lua_State* L) 
{
  const auto store = reinterpret_cast<rgw::sal::RadosStore*>(lua_touserdata(L, lua_upvalueindex(1)));
  const auto rest = reinterpret_cast<RGWREST*>(lua_touserdata(L, lua_upvalueindex(2)));
  const auto olog = reinterpret_cast<OpsLogSocket*>(lua_touserdata(L, lua_upvalueindex(3)));
  const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(4)));
  const std::string op_name(reinterpret_cast<const char*>(lua_touserdata(L, lua_upvalueindex(5))));
  if (store && s) {
    const auto rc = rgw_log_op(store, rest, s, op_name, olog);
    lua_pushinteger(L, rc);
  } else {
    ldpp_dout(s, 1) << "Lua ERROR: missing rados store, cannot use ops log"  << dendl;
    lua_pushinteger(L, -EINVAL);
  }

  return ONE_RETURNVAL;
}

struct ResponseMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Response";} 
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto err = reinterpret_cast<const rgw_err*>(lua_touserdata(L, lua_upvalueindex(1)));

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
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
  
  static int NewIndexClosure(lua_State* L) {
    auto err = reinterpret_cast<rgw_err*>(lua_touserdata(L, lua_upvalueindex(1)));

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
      return error_unknown_field(L, index, TableName());
    }
    return NO_RETURNVAL;
  }
};

struct QuotaMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Quota";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto info = reinterpret_cast<RGWQuotaInfo*>(lua_touserdata(L, lua_upvalueindex(1)));

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
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct PlacementRuleMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "PlacementRule";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto rule = reinterpret_cast<rgw_placement_rule*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Name") == 0) {
      pushstring(L, rule->name);
    } else if (strcasecmp(index, "StorageClass") == 0) {
      pushstring(L, rule->storage_class);
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct UserMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "User";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto user = reinterpret_cast<const rgw_user*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Tenant") == 0) {
      pushstring(L, user->tenant);
    } else if (strcasecmp(index, "Id") == 0) {
      pushstring(L, user->id);
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct OwnerMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Owner";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto owner = reinterpret_cast<ACLOwner*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "DisplayName") == 0) {
      pushstring(L, owner->get_display_name());
    } else if (strcasecmp(index, "User") == 0) {
      create_metatable<UserMetaTable>(L, false, &(owner->get_id()));
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct BucketMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Bucket";}
  static std::string Name() {return TableName() + "Meta";}

  using Type = rgw::sal::Bucket;

  static int IndexClosure(lua_State* L) {
    const auto bucket = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Tenant") == 0) {
      pushstring(L, bucket->get_tenant());
    } else if (strcasecmp(index, "Name") == 0) {
      pushstring(L, bucket->get_name());
    } else if (strcasecmp(index, "Marker") == 0) {
      pushstring(L, bucket->get_marker());
    } else if (strcasecmp(index, "Id") == 0) {
      pushstring(L, bucket->get_bucket_id());
    } else if (strcasecmp(index, "Count") == 0) {
      lua_pushinteger(L, bucket->get_count());
    } else if (strcasecmp(index, "Size") == 0) {
      lua_pushinteger(L, bucket->get_size());
    } else if (strcasecmp(index, "ZoneGroupId") == 0) {
      pushstring(L, bucket->get_info().zonegroup);
    } else if (strcasecmp(index, "CreationTime") == 0) {
      pushtime(L, bucket->get_creation_time());
    } else if (strcasecmp(index, "MTime") == 0) {
      pushtime(L, bucket->get_modification_time());
    } else if (strcasecmp(index, "Quota") == 0) {
      create_metatable<QuotaMetaTable>(L, false, &(bucket->get_info().quota));
    } else if (strcasecmp(index, "PlacementRule") == 0) {
      create_metatable<PlacementRuleMetaTable>(L, false, &(bucket->get_info().placement_rule));
    } else if (strcasecmp(index, "User") == 0) {
      create_metatable<UserMetaTable>(L, false, &(bucket->get_info().owner));
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct ObjectMetaTable : public EmptyMetaTable {
  static const std::string TableName() {return "Object";}
  static std::string Name() {return TableName() + "Meta";}
  
  using Type = rgw::sal::Object;

  static int IndexClosure(lua_State* L) {
    const auto obj = reinterpret_cast<const Type*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Name") == 0) {
      pushstring(L, obj->get_name());
    } else if (strcasecmp(index, "Instance") == 0) {
      pushstring(L, obj->get_instance());
    } else if (strcasecmp(index, "Id") == 0) {
      pushstring(L, obj->get_oid());
    } else if (strcasecmp(index, "Size") == 0) {
      lua_pushinteger(L, obj->get_obj_size());
    } else if (strcasecmp(index, "MTime") == 0) {
      pushtime(L, obj->get_mtime());
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

typedef int MetaTableClosure(lua_State* L);

template<typename MapType>
int StringMapWriteableNewIndex(lua_State* L) {
  const auto map = reinterpret_cast<MapType*>(lua_touserdata(L, lua_upvalueindex(1)));

  const char* index = luaL_checkstring(L, 2);
  const char* value = luaL_checkstring(L, 3);
  map->insert_or_assign(index, value);
  return NO_RETURNVAL;
}

template<typename MapType=std::map<std::string, std::string>,
  MetaTableClosure NewIndex=EmptyMetaTable::NewIndexClosure>
struct StringMapMetaTable : public EmptyMetaTable {

  static std::string TableName() {return "StringMap";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto map = reinterpret_cast<MapType*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    const auto it = map->find(std::string(index));
    if (it == map->end()) {
      lua_pushnil(L);
    } else {
      pushstring(L, it->second);
    }
    return ONE_RETURNVAL;
  }

  static int NewIndexClosure(lua_State* L) {
    return NewIndex(L);
  }

  static int PairsClosure(lua_State* L) {
    auto map = reinterpret_cast<MapType*>(lua_touserdata(L, lua_upvalueindex(1)));
    ceph_assert(map);
    lua_pushlightuserdata(L, map);
    lua_pushcclosure(L, stateless_iter, ONE_UPVAL); // push the stateless iterator function
    lua_pushnil(L);                                 // indicate this is the first call
    // return stateless_iter, nil

    return TWO_RETURNVALS;
  }
  
  static int stateless_iter(lua_State* L) {
    // based on: http://lua-users.org/wiki/GeneralizedPairsAndIpairs
    auto map = reinterpret_cast<MapType*>(lua_touserdata(L, lua_upvalueindex(1)));
    typename MapType::const_iterator next_it;
    if (lua_isnil(L, -1)) {
      next_it = map->begin();
    } else {
      const char* index = luaL_checkstring(L, 2);
      const auto it = map->find(std::string(index));
      ceph_assert(it != map->end());
      next_it = std::next(it);
    }

    if (next_it == map->end()) {
      // index of the last element was provided
      lua_pushnil(L);
      lua_pushnil(L);
      // return nil, nil
    } else {
      pushstring(L, next_it->first);
      pushstring(L, next_it->second);
      // return key, value
    }

    return TWO_RETURNVALS;
  }

  static int LenClosure(lua_State* L) {
    const auto map = reinterpret_cast<MapType*>(lua_touserdata(L, lua_upvalueindex(1)));

    lua_pushinteger(L, map->size());

    return ONE_RETURNVAL;
  }
};

struct GrantMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Grant";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto grant = reinterpret_cast<ACLGrant*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Type") == 0) {
      lua_pushinteger(L, grant->get_type().get_type());
    } else if (strcasecmp(index, "User") == 0) {
      const auto id_ptr = grant->get_id();
      if (id_ptr) {
        create_metatable<UserMetaTable>(L, false, const_cast<rgw_user*>(id_ptr));
      } else {
        lua_pushnil(L);
      }
    } else if (strcasecmp(index, "Permission") == 0) {
      lua_pushinteger(L, grant->get_permission().get_permissions());
    } else if (strcasecmp(index, "GroupType") == 0) {
      lua_pushinteger(L, grant->get_group());
    } else if (strcasecmp(index, "Referer") == 0) {
      pushstring(L, grant->get_referer());
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct GrantsMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Grants";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto map = reinterpret_cast<ACLGrantMap*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    const auto it = map->find(std::string(index));
    if (it == map->end()) {
      lua_pushnil(L);
    } else {
      create_metatable<GrantMetaTable>(L, false, &(it->second));
    }
    return ONE_RETURNVAL;
  }
  
  static int PairsClosure(lua_State* L) {
    auto map = reinterpret_cast<ACLGrantMap*>(lua_touserdata(L, lua_upvalueindex(1)));
    ceph_assert(map);
    lua_pushlightuserdata(L, map);
    lua_pushcclosure(L, stateless_iter, ONE_UPVAL); // push the stateless iterator function
    lua_pushnil(L);                                 // indicate this is the first call
    // return stateless_iter, nil

    return TWO_RETURNVALS;
  }
  
  static int stateless_iter(lua_State* L) {
    // based on: http://lua-users.org/wiki/GeneralizedPairsAndIpairs
    auto map = reinterpret_cast<ACLGrantMap*>(lua_touserdata(L, lua_upvalueindex(1)));
    ACLGrantMap::iterator next_it;
    if (lua_isnil(L, -1)) {
      next_it = map->begin();
    } else {
      const char* index = luaL_checkstring(L, 2);
      const auto it = map->find(std::string(index));
      ceph_assert(it != map->end());
      next_it = std::next(it);
    }

    if (next_it == map->end()) {
      // index of the last element was provided
      lua_pushnil(L);
      lua_pushnil(L);
      return TWO_RETURNVALS;
      // return nil, nil
    }

    while (next_it->first.empty()) {
      // this is a multimap and the next element does not have a unique key
      ++next_it;
      if (next_it == map->end()) {
        // index of the last element was provided
        lua_pushnil(L);
        lua_pushnil(L);
        return TWO_RETURNVALS;
        // return nil, nil
      }
    }

    pushstring(L, next_it->first);
    create_metatable<GrantMetaTable>(L, false, &(next_it->second));
    // return key, value
    
    return TWO_RETURNVALS;
  }

  static int LenClosure(lua_State* L) {
    const auto map = reinterpret_cast<ACLGrantMap*>(lua_touserdata(L, lua_upvalueindex(1)));

    lua_pushinteger(L, map->size());

    return ONE_RETURNVAL;
  }
};

struct ACLMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "ACL";}
  static std::string Name() {return TableName() + "Meta";}

  using Type = RGWAccessControlPolicy;

  static int IndexClosure(lua_State* L) {
    const auto acl = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Owner") == 0) {
      create_metatable<OwnerMetaTable>(L, false, &(acl->get_owner()));
    } else if (strcasecmp(index, "Grants") == 0) {
      create_metatable<GrantsMetaTable>(L, false, &(acl->get_acl().get_grant_map()));
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct StatementsMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Statements";}
  static std::string Name() {return TableName() + "Meta";}

  using Type = std::vector<rgw::IAM::Statement>;

  static std::string statement_to_string(const rgw::IAM::Statement& statement) {
    std::stringstream ss;
    ss << statement;
    return ss.str();
  }

  static int IndexClosure(lua_State* L) {
    const auto statements = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));

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
    auto statements = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));
    ceph_assert(statements);
    lua_pushlightuserdata(L, statements);
    lua_pushcclosure(L, stateless_iter, ONE_UPVAL); // push the stateless iterator function
    lua_pushnil(L);                                 // indicate this is the first call
    // return stateless_iter, nil

    return TWO_RETURNVALS;
  }
  
  static int stateless_iter(lua_State* L) {
    auto statements = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));
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
    const auto statements = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));

    lua_pushinteger(L, statements->size());

    return ONE_RETURNVAL;
  }
};

struct PolicyMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Policy";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto policy = reinterpret_cast<rgw::IAM::Policy*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

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
      create_metatable<StatementsMetaTable>(L, &(policy->statements));
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct PoliciesMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Policies";}
  static std::string Name() {return TableName() + "Meta";}
  
  using Type = std::vector<rgw::IAM::Policy>;

  static int IndexClosure(lua_State* L) {
    const auto policies = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));

    const auto index = luaL_checkinteger(L, 2);

    if (index >= (int)policies->size() || index < 0) {
      lua_pushnil(L);
    } else {
      create_metatable<PolicyMetaTable>(L, false, &((*policies)[index]));
    }
    return ONE_RETURNVAL;
  }
  
  static int PairsClosure(lua_State* L) {
    auto policies = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));
    ceph_assert(policies);
    lua_pushlightuserdata(L, policies);
    lua_pushcclosure(L, stateless_iter, ONE_UPVAL); // push the stateless iterator function
    lua_pushnil(L);                                 // indicate this is the first call
    // return stateless_iter, nil

    return TWO_RETURNVALS;
  }
  
  static int stateless_iter(lua_State* L) {
    auto policies = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));
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
      create_metatable<PolicyMetaTable>(L, false, &((*policies)[next_it]));
      // return key, value
    }

    return TWO_RETURNVALS;
  }

  static int LenClosure(lua_State* L) {
    const auto policies = reinterpret_cast<Type*>(lua_touserdata(L, lua_upvalueindex(1)));

    lua_pushinteger(L, policies->size());

    return ONE_RETURNVAL;
  }
};

struct HTTPMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "HTTP";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto info = reinterpret_cast<req_info*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Parameters") == 0) {
      create_metatable<StringMapMetaTable<>>(L, false, &(info->args.get_params()));
    } else if (strcasecmp(index, "Resources") == 0) {
      // TODO: add non-const api to get resources
      create_metatable<StringMapMetaTable<>>(L, false, 
          const_cast<std::map<std::string, std::string>*>(&(info->args.get_sub_resources())));
    } else if (strcasecmp(index, "Metadata") == 0) {
      create_metatable<StringMapMetaTable<meta_map_t, StringMapWriteableNewIndex<meta_map_t>>>(L, false, &(info->x_meta_map));
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
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct CopyFromMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "CopyFrom";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Tenant") == 0) {
      pushstring(L, s->src_tenant_name);
    } else if (strcasecmp(index, "Bucket") == 0) {
      pushstring(L, s->src_bucket_name);
    } else if (strcasecmp(index, "Object") == 0) {
      create_metatable<ObjectMetaTable>(L, false, s->src_object);
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct ZoneGroupMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "ZoneGroup";}
  static std::string Name() {return TableName() + "Meta";}

  static int IndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(1)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "Name") == 0) {
      pushstring(L, s->zonegroup_name);
    } else if (strcasecmp(index, "Endpoint") == 0) {
      pushstring(L, s->zonegroup_endpoint);
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

struct RequestMetaTable : public EmptyMetaTable {
  static std::string TableName() {return "Request";}
  static std::string Name() {return TableName() + "Meta";}

  // __index closure that expect req_state to be captured
  static int IndexClosure(lua_State* L) {
    const auto s = reinterpret_cast<req_state*>(lua_touserdata(L, lua_upvalueindex(1)));
    const auto op_name = reinterpret_cast<const char*>(lua_touserdata(L, lua_upvalueindex(2)));

    const char* index = luaL_checkstring(L, 2);

    if (strcasecmp(index, "RGWOp") == 0) {
      pushstring(L, op_name);
    } else if (strcasecmp(index, "DecodedURI") == 0) {
      pushstring(L, s->decoded_uri);
    } else if (strcasecmp(index, "ContentLength") == 0) {
      lua_pushinteger(L, s->content_length);
    } else if (strcasecmp(index, "GenericAttributes") == 0) {
      create_metatable<StringMapMetaTable<>>(L, false, &(s->generic_attrs));
    } else if (strcasecmp(index, "Response") == 0) {
      create_metatable<ResponseMetaTable>(L, false, &(s->err));
    } else if (strcasecmp(index, "SwiftAccountName") == 0) {
      if (s->dialect == "swift") {
        pushstring(L, s->account_name);
      } else {
        lua_pushnil(L);
      }
    } else if (strcasecmp(index, "Bucket") == 0) {
      create_metatable<BucketMetaTable>(L, false, s->bucket);
    } else if (strcasecmp(index, "Object") == 0) {
      create_metatable<ObjectMetaTable>(L, false, s->object);
    } else if (strcasecmp(index, "CopyFrom") == 0) {
      if (s->op_type == RGW_OP_COPY_OBJ) {
        create_metatable<CopyFromMetaTable>(L, s);
      } else {
        lua_pushnil(L);
      }
    } else if (strcasecmp(index, "ObjectOwner") == 0) {
      create_metatable<OwnerMetaTable>(L, false, &(s->owner));
    } else if (strcasecmp(index, "ZoneGroup") == 0) {
      create_metatable<ZoneGroupMetaTable>(L, false, s);
    } else if (strcasecmp(index, "UserACL") == 0) {
      create_metatable<ACLMetaTable>(L, false, s->user_acl);
    } else if (strcasecmp(index, "BucketACL") == 0) {
      create_metatable<ACLMetaTable>(L, false, s->bucket_acl);
    } else if (strcasecmp(index, "ObjectACL") == 0) {
      create_metatable<ACLMetaTable>(L, false, s->object_acl);
    } else if (strcasecmp(index, "Environment") == 0) {
        create_metatable<StringMapMetaTable<rgw::IAM::Environment>>(L, false, &(s->env));
    } else if (strcasecmp(index, "Policy") == 0) {
      // TODO: create a wrapper to std::optional
      if (!s->iam_policy) {
        lua_pushnil(L);
      } else {
        create_metatable<PolicyMetaTable>(L, false, s->iam_policy.get_ptr());
      }
    } else if (strcasecmp(index, "UserPolicies") == 0) {
        create_metatable<PoliciesMetaTable>(L, false, &(s->iam_user_policies));
    } else if (strcasecmp(index, "RGWId") == 0) {
      pushstring(L, s->host_id);
    } else if (strcasecmp(index, "HTTP") == 0) {
        create_metatable<HTTPMetaTable>(L, false, &(s->info));
    } else if (strcasecmp(index, "Time") == 0) {
      pushtime(L, s->time);
    } else if (strcasecmp(index, "Dialect") == 0) {
      pushstring(L, s->dialect);
    } else if (strcasecmp(index, "Id") == 0) {
      pushstring(L, s->req_id);
    } else if (strcasecmp(index, "TransactionId") == 0) {
      pushstring(L, s->trans_id);
    } else if (strcasecmp(index, "Tags") == 0) {
      create_metatable<StringMapMetaTable<RGWObjTags::tag_map_t>>(L, false, &(s->tagset.get_tags()));
    } else {
      return error_unknown_field(L, index, TableName());
    }
    return ONE_RETURNVAL;
  }
};

int execute(
    rgw::sal::Store* store,
    RGWREST* rest,
    OpsLogSocket* olog,
    req_state* s, 
    const char* op_name,
    const std::string& script)

{
  auto L = luaL_newstate();
  lua_state_guard lguard(L);

  open_standard_libs(L);
  set_package_path(L, store ?
      store->get_luarocks_path() : 
      "");

  create_debug_action(L, s->cct);  

  create_metatable<RequestMetaTable>(L, true, s, const_cast<char*>(op_name));
  
  // add the ops log action
  lua_getglobal(L, RequestMetaTable::TableName().c_str());
  ceph_assert(lua_istable(L, -1));
  pushstring(L, RequestLogAction);
  lua_pushlightuserdata(L, store);
  lua_pushlightuserdata(L, rest);
  lua_pushlightuserdata(L, olog);
  lua_pushlightuserdata(L, s);
  lua_pushlightuserdata(L, const_cast<char*>(op_name));
  lua_pushcclosure(L, RequestLog, FIVE_UPVALS);
  lua_rawset(L, -3);

  try {
    // execute the lua script
    if (luaL_dostring(L, script.c_str()) != LUA_OK) {
      const std::string err(lua_tostring(L, -1));
      ldpp_dout(s, 1) << "Lua ERROR: " << err << dendl;
      return -1;
    }
  } catch (const std::runtime_error& e) {
    ldpp_dout(s, 1) << "Lua ERROR: " << e.what() << dendl;
    return -1;
  }

  return 0;
}

}
