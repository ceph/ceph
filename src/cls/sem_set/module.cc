// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <concepts>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include <boost/system/system_error.hpp>

#include "include/rados/objclass.h"

#include "cls/sem_set/ops.h"

#include "objclass/objclass.h"


namespace buffer = ::ceph::buffer;
namespace sys = ::boost::system;
namespace ss = ::cls::sem_set;

using namespace std::literals;

namespace {
/// So we don't clash with other OMAP keys.
inline constexpr auto PREFIX = "CLS_SEM_SET_"sv;

struct sem_val {
  uint64_t value = 0;
  ceph::real_time last_decrement = ceph::real_time::min();

  sem_val() = default;

  sem_val(uint64_t value) : value(value) {}

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(value, bl);
    encode(last_decrement, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(value, bl);
    decode(last_decrement, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(sem_val);


int increment(cls_method_context_t hctx, buffer::list *in, buffer::list *out)
{
  CLS_LOG(10, "%s", __PRETTY_FUNCTION__);

  ss::increment op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const std::exception& e) {
    CLS_ERR("ERROR: %s: failed to decode request: %s", __PRETTY_FUNCTION__,
            e.what());
    return -EINVAL;
  }

  if (op.keys.size() > ::cls::sem_set::max_keys) {
    CLS_ERR("ERROR: %s: too many keys: %zu", __PRETTY_FUNCTION__,
	    op.keys.size());
    return -E2BIG;
  }

  for (const auto& key_ : op.keys) try {
      buffer::list valbl;
      auto key = std::string(PREFIX) + key_;
      auto r = cls_cxx_map_get_val(hctx, key, &valbl);
      sem_val val;
      if (r >= 0) {
	auto bi = valbl.cbegin();
	decode(val, bi);
	valbl.clear();
      } else if (r == -ENOENT) {
	val.value = 0;
      } else {
        CLS_ERR("ERROR: %s: failed to read semaphore: r=%d",
                __PRETTY_FUNCTION__, r);
        return r;
      }
      val.value += 1;
      encode(val, valbl);
      r = cls_cxx_map_set_val(hctx, key, &valbl);
      if (r < 0) {
        CLS_ERR("ERROR: %s: failed to update semaphore: r=%d",
                __PRETTY_FUNCTION__, r);
        return r;
      }
    } catch (const std::exception& e) {
      CLS_ERR("CAN'T HAPPEN: %s: failed to decode semaphore: %s",
              __PRETTY_FUNCTION__, e.what());
      return -EIO;
    }

  return 0;
}

int reset(cls_method_context_t hctx, buffer::list *in, buffer::list *out)
{
  CLS_LOG(10, "%s", __PRETTY_FUNCTION__);

  ss::reset op;
  try {
    auto iter = in->cbegin();
    decode(op, iter);
  } catch (const std::exception& e) {
    CLS_ERR("ERROR: %s: failed to decode request: %s", __PRETTY_FUNCTION__,
            e.what());
    return -EINVAL;
  }

  if (op.keys.size() > ::cls::sem_set::max_keys) {
    CLS_ERR("ERROR: %s: too many keys: %zu", __PRETTY_FUNCTION__,
	    op.keys.size());
    return -E2BIG;
  }

  for (const auto& [key_, v] : op.keys) try {
      buffer::list valbl;
      auto key = std::string(PREFIX) + key_;
      sem_val val{v};
      encode(val, valbl);
      auto r = cls_cxx_map_set_val(hctx, key, &valbl);
      if (r < 0) {
        CLS_ERR("ERROR: %s: failed to reset semaphore: r=%d",
                __PRETTY_FUNCTION__, r);
        return r;
      }
    } catch (const std::exception& e) {
      CLS_ERR("CAN'T HAPPEN: %s: failed to decode semaphore: %s",
              __PRETTY_FUNCTION__, e.what());
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
    CLS_ERR("ERROR: %s: failed to decode request: %s", __PRETTY_FUNCTION__,
            e.what());
    return -EINVAL;
  }

  if (op.keys.size() > ::cls::sem_set::max_keys) {
    CLS_ERR("ERROR: %s: too many keys: %zu", __PRETTY_FUNCTION__,
	    op.keys.size());
    return -E2BIG;
  }

  auto now = ceph::real_clock::now();
  for (const auto& key_ : op.keys) try {
      buffer::list valbl;
      auto key = std::string(PREFIX) + key_;
      auto r = cls_cxx_map_get_val(hctx, key, &valbl);
      if (r < 0) {
        CLS_ERR("ERROR: %s: failed to read semaphore: r=%d",
                __PRETTY_FUNCTION__, r);
        return r;
      }
      sem_val val;
      auto bi = valbl.cbegin();
      decode(val, bi);
      // Don't decrement if we're within the grace period (or there's
      // screwy time stuff)
      if ((now < val.last_decrement) ||
	  (now - val.last_decrement < op.grace)) {
	continue;
      }
      if (val.value > 1) {
	--(val.value);
	val.last_decrement = now;
        valbl.clear();
        encode(val, valbl);
        r = cls_cxx_map_set_val(hctx, key, &valbl);
        if (r < 0) {
          CLS_ERR("ERROR: %s: failed to update semaphore: r=%d",
                  __PRETTY_FUNCTION__, r);
          return r;
        }
      } else {
        r = cls_cxx_map_remove_key(hctx, key);
        if (r < 0) {
          CLS_ERR("ERROR: %s: failed to remove key %s: r=%d",
                  __PRETTY_FUNCTION__, key.c_str(), r);
          return r;
        }
      }
    } catch (const std::exception& e) {
      CLS_ERR("CORRUPTION: %s: failed to decode semaphore: %s",
              __PRETTY_FUNCTION__, e.what());
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
    CLS_ERR("ERROR: %s: failed to decode request: %s", __PRETTY_FUNCTION__,
            e.what());
    return -EINVAL;
  }

  auto count = op.count;
  ss::list_ret res;
  res.cursor = op.cursor;

  while (count > 0) {
    auto cursor = std::string{PREFIX} + res.cursor + '\001';
    std::map<std::string, ceph::buffer::list> vals;
    bool more = false;
    auto r = cls_cxx_map_get_vals(hctx, cursor,
				  std::string(PREFIX), count,
				  &vals, &more);
    if (r < 0) {
      CLS_ERR("ERROR: %s: failed to read semaphore: r=%d",
              __PRETTY_FUNCTION__, r);
      return r;
    }

    count = vals.size() <= count ? count - vals.size() : 0;

    if (!vals.empty()) {
      res.cursor = std::string{(--vals.end())->first, PREFIX.size()};
    }
    try {
      for (auto&& [key_, valbl] : vals) {
	sem_val val;
	auto bi = valbl.cbegin();
	decode(val, bi);
	res.kvs.emplace(std::piecewise_construct,
			std::forward_as_tuple(std::move(key_), PREFIX.size()),
			std::forward_as_tuple(val.value));
      }
    } catch (const sys::system_error& e) {
      CLS_ERR("CAN'T HAPPEN: %s: failed to decode seamaphore: %s",
	      __PRETTY_FUNCTION__, e.what());
      return ceph::from_error_code(e.code());;
    }
    if (!more) {
      res.cursor.clear();
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

  cls_register_cxx_method(h_class, ss::DECREMENT,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          &decrement, &h_decrement);

  cls_register_cxx_method(h_class, ss::RESET,
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          &reset, &h_decrement);

  cls_register_cxx_method(h_class, ss::LIST,
                          CLS_METHOD_RD,
                          &list, &h_list);

  return;
}
