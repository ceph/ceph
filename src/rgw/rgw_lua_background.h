#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include <string>
#include "rgw_lua_utils.h"

namespace rgw::lua {

//Interval between each execution of the script is set to 5 seconds
constexpr const int INIT_EXECUTE_INTERVAL = 5;

//Writeable meta table named RGW with mutex protection
using BackgroundMap = std::unordered_map<std::string, std::string>;
struct RGWTable : StringMapMetaTable<BackgroundMap,
  StringMapWriteableNewIndex<BackgroundMap>> {
    static std::string TableName() {return "RGW";}
    static std::string Name() {return TableName() + "Meta";}
    static int IndexClosure(lua_State* L) {
      auto& mtx = *reinterpret_cast<std::mutex*>(lua_touserdata(L, lua_upvalueindex(2)));
      std::lock_guard l(mtx);
      return StringMapMetaTable::IndexClosure(L);
    }
    static int LenClosure(lua_State* L) {
      auto& mtx = *reinterpret_cast<std::mutex*>(lua_touserdata(L, lua_upvalueindex(2)));
      std::lock_guard l(mtx);
      return StringMapMetaTable::LenClosure(L);
    }
    static int NewIndexClosure(lua_State* L) {
      auto& mtx = *reinterpret_cast<std::mutex*>(lua_touserdata(L, lua_upvalueindex(2)));
      std::lock_guard l(mtx);
      return StringMapMetaTable::NewIndexClosure(L);
    }
};

class Background {

private:
  BackgroundMap rgw_map;
  std::string rgw_script;
  bool stopped = false;
  int execute_interval = INIT_EXECUTE_INTERVAL;
  const DoutPrefixProvider* const dpp;
  rgw::sal::Store* const store;
  CephContext* const cct;
  std::string luarocks_path;
  std::thread runner;
  std::mutex m_mutex;

  void run();

public:
  Background(const DoutPrefixProvider* dpp,
      rgw::sal::Store* store,
      CephContext* cct,
      const std::string& luarocks_path) :
    dpp(dpp),
    store(store),
    cct(cct),
    luarocks_path(luarocks_path),
    runner(std::thread(&Background::run, this)) {
      const auto rc = ceph_pthread_setname(runner.native_handle(),
                                           "lua_background");
      ceph_assert(rc == 0);
    }

    ~Background() = default;
    void stop();
    void shutdown();
    void create_background_metatable(lua_State* L);
};

} //namepsace lua

