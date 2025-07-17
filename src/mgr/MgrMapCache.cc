#include "mgr/MgrMapCache.h"
#include "common/config_proxy.h"
#include "common/debug.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "api cache " << __func__ << " "

template <class Key, class Value>
MgrMapCache<Key, Value>::MgrMapCache(uint16_t size)
    : CacheImp(size, g_conf().get_val<bool>("mgr_map_cache_enabled")) {
  dout(20) << __func__ << ": creating cache with size " << size << dendl;
  LFUCache::set_enabled(g_conf().get_val<bool>("mgr_map_cache_enabled"));
  g_conf().add_observer(this);
}

template <class Key, class Value>
MgrMapCache<Key, Value>::~MgrMapCache() {
  this->clear();
}
template <class Key, class Value>
Value MgrMapCache<Key, Value>::get(const Key &key, bool count_hit) {
  Value value = CacheImp::get(key, count_hit);
  return value;
}

template <class Key, class Value>
void MgrMapCache<Key, Value>::erase(Key key) {
  dout(25) << __func__ << ": erasing key: " << key << dendl;
  CacheImp::erase(std::move(key));
}

template <class Key, class Value>
void MgrMapCache<Key, Value>::clear() {
  dout(10) << __func__ << ": clearing cache" << dendl;
  CacheImp::clear();
}

template <class Key, class Value>
void MgrMapCache<Key, Value>::insert(Key key, Value value) {
  dout(10) << __func__ << ": inserting key: " << key << dendl;
  CacheImp::insert(std::move(key), std::move(value));
}

template <class Key, class Value>
std::vector<std::string> MgrMapCache<Key, Value>::get_tracked_keys() const noexcept {
  dout(10) << __func__ << ": returning tracked keys" << dendl;
  return {
    "mgr_map_cache_enabled",
  };
}

template <class Key, class Value>
void MgrMapCache<Key, Value>::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed) {
  if (changed.count("mgr_map_cache_enabled")) {
    dout(10) << __func__ << ": mgr_map_cache_enabled changed" << dendl;
    CacheImp::set_enabled(conf.get_val<bool>("mgr_map_cache_enabled"));
  }
}

template <class Key>
MgrMapCache<Key, PyObject*>::MgrMapCache(uint16_t size)
    : CacheImp(size, g_conf().get_val<bool>("mgr_map_cache_enabled")) {
    dout(20) << __func__ << ": creating cache with size " << size << dendl;
    LFUCache::set_enabled(g_conf().get_val<bool>("mgr_map_cache_enabled"));
    g_conf().add_observer(this);
}

template <class Key>
MgrMapCache<Key, PyObject*>::~MgrMapCache() {
  CacheImp::clear();
}

template <class Key>
PyObject* MgrMapCache<Key, PyObject*>::get(Key key) {
  if (!this->is_cacheable(key) || !this->is_enabled()) {
    return nullptr;
  }
  
  PyObject* cached_value = CacheImp::get(key, true);
  PyGILState_STATE gstate = PyGILState_Ensure();
  Py_INCREF(cached_value);
  PyGILState_Release(gstate);
  dout(25) << ": cache hit for key: " << key << " py count: "
           << Py_REFCNT(cached_value) << dendl;
  
  return cached_value;
}

template <class Key>
void MgrMapCache<Key, PyObject*>::erase(Key key) {
  if (!this->is_cacheable(key) || !this->is_enabled()) {
    return;
  }
  try {
    PyObject* cached_value = CacheImp::get(key, false);
    dout(10) << ": cache hit for key: " << key << " py count: "
             << Py_REFCNT(cached_value) << dendl;
    CacheImp::erase(std::move(key));
    PyGILState_STATE gstate = PyGILState_Ensure();
    Py_DECREF(cached_value);
    PyGILState_Release(gstate);
    dout(20) << ": erased key: " << key << " py count: "
             << Py_REFCNT(cached_value) << dendl;
  } catch (const std::out_of_range&) {
    dout(20) << ": key not found in cache: " << key << dendl;
  }
}

template <class Key>
void MgrMapCache<Key, PyObject*>::clear() {
  PyGILState_STATE gstate = PyGILState_Ensure();
  for (auto const& [key, entry] : this->cache_data) {
    Py_DECREF(entry.val);
  }
  PyGILState_Release(gstate);
  CacheImp::clear();
}

template <class Key>
void MgrMapCache<Key, PyObject*>::insert(Key key, PyObject* value) {
  if (!this->is_cacheable(key) || !this->is_enabled()) return;

  dout(10) << __func__ << ": inserting key: " << key << dendl;
  CacheImp::insert(std::move(key), value);
  PyGILState_STATE gstate = PyGILState_Ensure();
  Py_INCREF(value);
  PyGILState_Release(gstate);
}

template <class Key>
void MgrMapCache<Key, PyObject*>::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed) {
  dout(10) << __func__ << ": handling config change" << dendl;
  if (changed.count("mgr_map_cache_enabled")) {
    dout(10) << __func__ << ": mgr_map_cache_enabled changed" << dendl;
    CacheImp::set_enabled(g_conf().get_val<bool>("mgr_map_cache_enabled"));
  }
}

template <class Key>
std::vector<std::string> MgrMapCache<Key, PyObject*>::get_tracked_keys() const noexcept {
  dout(10) << __func__ << ": returning tracked keys" << dendl;
  return {
    "mgr_map_cache_enabled",
  };
}

template class MgrMapCache<std::string, PyObject*>;