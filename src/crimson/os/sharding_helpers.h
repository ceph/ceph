// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <seastar/core/sharded.hh>

#include "crimson/common/smp_helpers.h"

namespace crimson::os {

struct shard_desc_t {
  size_t local_index; // local in the domain of same reactor
  size_t global_shards_num; // number of storage shards across all reactors

  auto global_index() const {
    auto gidx = seastar::this_shard_id() + seastar::smp::count * local_index;
    ceph_assert(gidx < global_shards_num);
    return gidx;
  }
};

template <class ServiceT>
class simplysharded : private seastar::sharded<ServiceT> {
  using base_t = seastar::sharded<ServiceT>;
public:

  template <typename... Args>
  seastar::future<> start(size_t num_shards, Args&&... args) noexcept;

  template <typename Func, typename... Args>
  seastar::future<> invoke_on_locals(Func func, Args... args) noexcept {
    return func(this->local(), args...);
  }

  using base_t::stop;
  using base_t::local;
  using base_t::map_reduce0;
  using base_t::map;
  using base_t::invoke_on_all;
};

template <typename ServiceT>
template <typename... Args>
seastar::future<>
simplysharded<ServiceT>::start(const size_t num_shards, Args&&... args) noexcept
{
  // never ever change num of shards after mkfs
  ceph_assert(num_shards == seastar::smp::count);
  return base_t::start(
    shard_desc_t{0, num_shards},
    std::forward<Args>(args)...);
}


template <class ServiceT>
class multisharded : private seastar::sharded<ServiceT> {
  using base_t = seastar::sharded<ServiceT>;
  std::vector<seastar::shared_ptr<ServiceT>> _instances;
public:
  template <typename... Args>
  seastar::future<> start(size_t num_shards, Args&&... args) noexcept;
  seastar::future<> stop() noexcept;

  ServiceT& local(const size_t local_store_index=0) noexcept;
  const ServiceT& local(const size_t local_store_index=0) const noexcept;

  template <typename Func, typename... Args>
  seastar::future<> invoke_on_all(Func func, Args... args) noexcept;

  template <typename Func, typename... Args>
  seastar::future<> invoke_on_locals(Func func, Args... args) noexcept;

  template <typename Mapper, typename Initial, typename Reduce>
  seastar::future<Initial>
  map_reduce0(Mapper map, Initial initial, Reduce reduce) const;

  template <typename Mapper> auto map(Mapper mapper);
};

template <typename ServiceT>
template <typename... Args>
seastar::future<>
multisharded<ServiceT>::start(const std::size_t num_shards, Args&&... args) noexcept
{
  _instances.resize(num_shards);
  return seastar::parallel_for_each(
    std::views::iota(size_t(0), num_shards),
    [this, num_shards, ...args = std::forward<Args>(args)](auto c) mutable {
      return seastar::smp::submit_to(c % seastar::smp::count, [this, c, num_shards, args...] mutable {
        this->_instances[c] = seastar::make_shared<ServiceT>(
          shard_desc_t{c / seastar::smp::count, num_shards},
          args...);
        return seastar::now();
      });
    });
}


template <typename ServiceT>
seastar::future<>
multisharded<ServiceT>::stop() noexcept
{
  return seastar::parallel_for_each(
    std::views::iota(size_t(0), _instances.size()),
    [this](auto c) mutable {
      return seastar::smp::submit_to(c % seastar::smp::count, [this, c] mutable {
       	// in constrast to seastar::sharded not trying to call ServiceT::stop()
	this->_instances[c] = nullptr;
        return seastar::now();
      });
    });
}

#if 0
// using C++23's "deducing this" (aka Explicit Object Parameter) to avoid
// the const / non-const boilerplate
template <typename ServiceT>
auto&& multisharded<ServiceT>::local(this auto&& self, const size_t local_store_index) noexcept
{
  // de facto it's a twodimensional array: _instances[core_id, local_store_index]
  const size_t global_idx =
    (local_store_index * seastar::smp::count) + seastar::this_shard_id();
  ceph_assert(global_idx < self._instances.size());
  ceph_assert(self._instances[global_idx] != nullptr);
  return std::forward_like<decltype(self)>(*self._instances[global_idx]);
}
#endif

template <typename ServiceT>
ServiceT& multisharded<ServiceT>::local(const size_t local_store_index) noexcept
{
  // de facto it's a twodimensional array: _instances[core_id, local_store_index]
  const size_t global_idx =
    (local_store_index * seastar::smp::count) + seastar::this_shard_id();
  ceph_assert(global_idx < _instances.size());
  ceph_assert(_instances[global_idx] != nullptr);
  return *_instances[global_idx];
}
template <typename ServiceT>
const ServiceT& multisharded<ServiceT>::local(const size_t local_store_index) const noexcept
{
  // de facto it's a twodimensional array: _instances[core_id, local_store_index]
  const size_t global_idx =
    (local_store_index * seastar::smp::count) + seastar::this_shard_id();
  ceph_assert(global_idx < _instances.size());
  ceph_assert(_instances[global_idx] != nullptr);
  return *_instances[global_idx];
}

template <typename ServiceT>
template <typename Func, typename... Args>
seastar::future<>
multisharded<ServiceT>::invoke_on_all(Func func, Args... args) noexcept
{
  return seastar::parallel_for_each(
    std::views::iota(size_t(0), _instances.size()),
    [this, func, args...](size_t c) mutable {
      return seastar::smp::submit_to(c % seastar::smp::count, 
        [this, c, func, args...] mutable {
          return func(*(this->_instances[c]), args...);
        }
      );
    });
}

template <typename ServiceT>
template <typename Func, typename... Args>
seastar::future<>
multisharded<ServiceT>::invoke_on_locals(Func func, Args... args) noexcept
{
  return seastar::parallel_for_each(
    std::views::iota(size_t(seastar::this_shard_id()), _instances.size())
      || std::views::stride(seastar::smp::count),
    [this, func, args...](size_t global_idx) mutable {
      ceph_assert(global_idx % seastar::smp::count == seastar::this_shard_id());
      return func(*(this->_instances[global_idx]), args...);
    });
}

template <typename ServiceT>
template <typename Mapper, typename Initial, typename Reduce>
seastar::future<Initial>
multisharded<ServiceT>::map_reduce0(Mapper map, Initial initial, Reduce reduce) const
{
  auto wrapped_map = [this, map] (unsigned c) {
    return seastar::smp::submit_to(c % seastar::smp::count, [this, map, c] {
      return std::invoke(map, *this->_instances[c]);
    });
  };
  return ::seastar::map_reduce(
    std::views::iota(size_t(0), _instances.size()),
    std::move(wrapped_map),
    std::move(initial),
    std::move(reduce));
}

template <typename ServiceT>
template <typename Mapper>
auto multisharded<ServiceT>::map(Mapper mapper)
{
  using fut_t = seastar::futurize_t<std::invoke_result_t<Mapper, ServiceT&>>;
  using vec_t = std::vector<typename fut_t::value_type>;
  return seastar::do_with(vec_t{}, std::move(mapper),
    [this] (auto& vec, auto& mapper) mutable {
      vec.resize(_instances.size());
      return seastar::parallel_for_each(
        std::views::iota(0u, _instances.size()),
        [this, &vec, &mapper] (unsigned c) {
          return seastar::smp::submit_to(c % seastar::smp::count, [this, &mapper, c] {
            return mapper(*this->_instances[c]);
          }).then([&vec, c] (auto&& res) {
            // seastar does it that way and it makes sense -- no CPU cache's false sharing
            vec[c] = std::move(res);
          });
      }).then([&vec] {
        return seastar::make_ready_future<vec_t>(std::move(vec));
      });
    });
}

} // namespace crimson::os
