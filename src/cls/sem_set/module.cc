// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <concepts>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include "include/rados/objclass.h"

#include "common/config_proxy.h"

#include "cls/sem_set/ops.h"

#include "objclass/objclass.h"


namespace buffer = ::ceph::buffer;
namespace ss = ::cls::sem_set;

using namespace std::literals;

namespace {
/// So we don't clash with other OMAP keys.
inline constexpr auto PREFIX = "CLS_SEM_SET_"sv;

/// Returns a value encoded in a bufferlist
template<std::default_initializable T>
inline auto from_bl(const buffer::list& bl)
{
  using ceph::decode;
  T t;
  auto bi = bl.begin();
  decode(t, bi);
  return t;
}

int increment(cls_method_context_t hctx, buffer::list *in, buffer::list *out)
{
  CLS_LOG(10, "%s", __PRETTY_FUNCTION__);

  ss::increment op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const std::exception& e) {
    CLS_ERR("ERROR: %s:%d failed to decode request: %s", __PRETTY_FUNCTION__,
            __LINE__, e.what());
    return -EINVAL;
  }

  const auto& max_omap = cls_get_config(hctx).get_val<std::uint64_t>(
    "osd_max_omap_entries_per_request"sv);
  if (op.keys.size() > max_omap) {
    CLS_ERR("ERROR: %s:%d too many keys: %zu", __PRETTY_FUNCTION__,
	    __LINE__, op.keys.size());
    return -E2BIG;
  }

  for (const auto& key_ : op.keys) try {
      buffer::list valbl;
      auto key = std::string(PREFIX) + key_;
      auto r = cls_cxx_map_get_val(hctx, key, &valbl);
      std::uint64_t sem = 0;
      if (r >= 0) {
	sem = from_bl<std::uint64_t>(valbl);
	valbl.clear();
      } else if (r == -ENOENT) {
	sem = 0;
      } else {
        CLS_ERR("ERROR: %s:%d failed to read semaphore: r=%d",
                __PRETTY_FUNCTION__, __LINE__, r);
        return r;
      }
      ceph::encode(sem + 1, valbl);
      r = cls_cxx_map_set_val(hctx, key, &valbl);
      if (r < 0) {
        CLS_ERR("ERROR: %s:%d failed to update semaphore: r=%d",
                __PRETTY_FUNCTION__, __LINE__, r);
        return r;
      }
    } catch (const std::exception& e) {
      CLS_ERR("CAN'T HAPPEN: %s:%d failed to decode semaphore: %s",
              __PRETTY_FUNCTION__, __LINE__, e.what());
      return -EIO;
    }

  return 0;
}

int decrement(cls_method_context_t hctx, buffer::list *in, buffer::list *out)
{
  CLS_LOG(10, "%s", __PRETTY_FUNCTION__);

  ss::decrement op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const std::exception& e) {
    CLS_ERR("ERROR: %s:%d failed to decode request: %s", __PRETTY_FUNCTION__,
            __LINE__, e.what());
    return -EINVAL;
  }

  const auto& max_omap = cls_get_config(hctx).get_val<std::uint64_t>(
    "osd_max_omap_entries_per_request"sv);
  if (op.keys.size() > max_omap) {
    CLS_ERR("ERROR: %s:%d too many keys: %zu", __PRETTY_FUNCTION__,
	    __LINE__, op.keys.size());
    return -E2BIG;
  }

  for (const auto& key_ : op.keys) try {
      buffer::list valbl;
      auto key = std::string(PREFIX) + key_;
      auto r = cls_cxx_map_get_val(hctx, key, &valbl);
      if (r < 0) {
        CLS_ERR("ERROR: %s:%d failed to read semaphore: r=%d",
                __PRETTY_FUNCTION__, __LINE__, r);
        return r;
      }
      auto sem = from_bl<std::uint64_t>(valbl);
      if (sem > 1) {
	--sem;
        valbl.clear();
        ceph::encode(sem, valbl);
        r = cls_cxx_map_set_val(hctx, key, &valbl);
        if (r < 0) {
          CLS_ERR("ERROR: %s:%d failed to update semaphore: r=%d",
                  __PRETTY_FUNCTION__, __LINE__, r);
          return r;
        }
      } else {
        r = cls_cxx_map_remove_key(hctx, key);
        if (r < 0) {
          CLS_ERR("ERROR: %s:%d failed to remove key %s: r=%d",
                  __PRETTY_FUNCTION__, __LINE__, key.c_str(), r);
          return r;
        }
      }
    } catch (const std::exception& e) {
      CLS_ERR("CORRUPTION: %s:%d failed to decode semaphore: %s",
              __PRETTY_FUNCTION__, __LINE__, e.what());
      return -EIO;
    }

  return 0;
}

int list(cls_method_context_t hctx, buffer::list *in, buffer::list *out)
{
  CLS_LOG(10, "%s", __PRETTY_FUNCTION__);

  ss::list_op op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const std::exception& e) {
    CLS_ERR("ERROR: %s:%d failed to decode request: %s", __PRETTY_FUNCTION__,
            __LINE__, e.what());
    return -EINVAL;
  }

  auto count = op.count;
  const auto& max_omap = cls_get_config(hctx).get_val<std::uint64_t>(
    "osd_max_omap_entries_per_request"sv);
  if (count > max_omap) {
    // We clamp rather than error, here, since a lister can just
    // continue listing with the supplied cursor.
    count = max_omap;
  }
  ss::list_ret res;
  res.cursor = op.cursor;

  while (count > 0) {
    std::map<std::string, ceph::buffer::list> vals;
    bool more = false;
    auto r = cls_cxx_map_get_vals(hctx,
				  res.cursor ? *res.cursor : std::string{},
				  std::string(PREFIX), count,
				  &vals, &more);
    if (r < 0) {
      CLS_ERR("ERROR: %s:%d failed to read semaphore: r=%d",
              __PRETTY_FUNCTION__, __LINE__, r);
      return r;
    }

    count = vals.size() <= count ? count - vals.size() : 0;

    if (!vals.empty()) {
      res.cursor = (--vals.end())->first;
    }
    for (auto&& [key_, valbl] : vals) {
      res.kvs.emplace(std::piecewise_construct,
                      std::forward_as_tuple(std::move(key_), PREFIX.size()),
                      std::forward_as_tuple(from_bl<std::uint64_t>(valbl)));
    }
    if (!more) {
      res.cursor.reset();
      break;
    }
  }

  encode(res, *out);
  return 0;
}
} // namespace (anonymous)

CLS_INIT(sem_set)
{
  cls_handle_t h_class;
  cls_method_handle_t h_increment;
  cls_method_handle_t h_decrement;
  cls_method_handle_t h_list;

  cls_register(ss::CLASS, &h_class);
  cls_register_cxx_method(h_class, ss::INCREMENT,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          &increment, &h_increment);

  cls_register_cxx_method(h_class, ss::INCREMENT,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          &decrement, &h_increment);

  cls_register_cxx_method(h_class, ss::DECREMENT,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          &decrement, &h_decrement);

  cls_register_cxx_method(h_class, ss::LIST,
                          CLS_METHOD_RD,
                          &list, &h_list);

  return;
}
