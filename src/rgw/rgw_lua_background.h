#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include <string>
#include "rgw_lua_utils.h"
#include "rgw_realm_reloader.h"

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

class Background : public RGWRealmReloader::Pauser {

private:
  BackgroundMap rgw_map;
  bool stopped = false;
  bool started = false;
  bool paused = false;
  int execute_interval;
  const DoutPrefix dp;
  rgw::sal::Store* store;
  CephContext* const cct;
  const std::string luarocks_path;
  std::thread runner;
  mutable std::mutex table_mutex;
  std::mutex cond_mutex;
  std::mutex pause_mutex;
  std::condition_variable cond;
  static const std::string empty_table_value;

  void run();

protected:
  std::string rgw_script;
  virtual int read_script();

public:
  Background(rgw::sal::Store* store,
      CephContext* cct,
      const std::string& luarocks_path,
      int execute_interval = INIT_EXECUTE_INTERVAL);

    virtual ~Background() = default;
    void start();
    void shutdown();
    void create_background_metatable(lua_State* L);
    const std::string& get_table_value(const std::string& key) const;
    void put_table_value(const std::string& key, const std::string& value);
    
    void pause() override;
    void resume(rgw::sal::Store* _store) override;
};

} //namepsace lua

