// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_ASIO_UNIQUE_FUNCTION_HPP
#define CEPH_LIBRBD_ASIO_UNIQUE_FUNCTION_HPP

#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace librbd {
namespace asio {

namespace detail {

template <typename T>
class is_void_invocable {
  template <typename U>
  static auto test(int) -> decltype(std::declval<U&>()(), std::true_type());
  template <typename>
  static std::false_type test(...);
public:
  static const bool value = decltype(test<T>(0))::value;
};

} // namespace detail

/**
 * Move-only type-erased invocable with small-buffer optimization.
 *
 * Replaces std::function on the ContextWQ hot path so move-only Asio
 * executor_function values can be posted without shared_ptr / atomics.
 */
template <std::size_t Capacity = 64>
class UniqueFunction {
public:
  UniqueFunction() noexcept = default;

  UniqueFunction(std::nullptr_t) noexcept {}

  template <typename F,
            typename Decayed = typename std::decay<F>::type,
            typename = typename std::enable_if<
              !std::is_same<Decayed, UniqueFunction>::value &&
              detail::is_void_invocable<Decayed>::value>::type>
  UniqueFunction(F&& f) {
    emplace(std::forward<F>(f));
  }

  UniqueFunction(UniqueFunction&& other) noexcept {
    move_from(std::move(other));
  }

  UniqueFunction& operator=(UniqueFunction&& other) noexcept {
    if (this != &other) {
      // Drop any owned callable first. If other is empty, move_from is a
      // no-op and this remains empty (function pointers cleared by reset).
      reset();
      move_from(std::move(other));
    }
    return *this;
  }

  UniqueFunction& operator=(std::nullptr_t) noexcept {
    reset();
    return *this;
  }

  UniqueFunction(const UniqueFunction&) = delete;
  UniqueFunction& operator=(const UniqueFunction&) = delete;

  ~UniqueFunction() {
    reset();
  }

  explicit operator bool() const noexcept {
    return invoke_ != nullptr;
  }

  void operator()() {
    invoke_(object());
  }

  void reset() noexcept {
    if (destroy_) {
      destroy_(object());
    }
    // Always leave a pristine empty state (including after reset of an
    // already-empty / moved-from instance).
    destroy_ = nullptr;
    invoke_ = nullptr;
    move_ = nullptr;
  }

private:
  using invoke_fn = void (*)(void*);
  using destroy_fn = void (*)(void*);
  using move_fn = void (*)(void* dst, void* src);

  template <typename Decayed>
  void emplace(Decayed&& f) {
    using T = typename std::decay<Decayed>::type;
    emplace_dispatch(
        std::forward<Decayed>(f),
        typename std::integral_constant<
            bool,
            (sizeof(T) <= Capacity &&
             alignof(T) <= alignof(std::max_align_t) &&
             std::is_move_constructible<T>::value)>::type());
  }

  // Prefer SBO whenever T fits. Asio binders are often not marked noexcept
  // moveable; UniqueFunction::move stays noexcept (throwing T move → terminate).
  template <typename Decayed>
  void emplace_dispatch(Decayed&& f, std::true_type /* use_sbo */) {
    using T = typename std::decay<Decayed>::type;
    ::new (object()) T(std::forward<Decayed>(f));
    invoke_ = [](void* p) { (*static_cast<T*>(p))(); };
    destroy_ = [](void* p) { static_cast<T*>(p)->~T(); };
    move_ = [](void* dst, void* src) {
      ::new (dst) T(std::move(*static_cast<T*>(src)));
      static_cast<T*>(src)->~T();
    };
  }

  template <typename Decayed>
  void emplace_dispatch(Decayed&& f, std::false_type /* use_heap */) {
    using T = typename std::decay<Decayed>::type;
    // Owning T* via placement new (strict-aliasing safe). unique_ptr covers
    // the window between allocating T and starting the T* lifetime in storage_.
    static_assert(sizeof(T*) <= Capacity, "Capacity too small for heap ptr");
    static_assert(alignof(T*) <= alignof(std::max_align_t),
                  "pointer alignment exceeds storage");
    std::unique_ptr<T> held(new T(std::forward<Decayed>(f)));
    ::new (object()) T*(held.get());
    held.release();
    invoke_ = [](void* p) { (**static_cast<T**>(p))(); };
    destroy_ = [](void* p) {
      T** pp = static_cast<T**>(p);
      delete *pp;
      using Ptr = T*;
      pp->~Ptr();
    };
    move_ = [](void* dst, void* src) {
      T** sp = static_cast<T**>(src);
      ::new (dst) T*(*sp);
      *sp = nullptr;
      using Ptr = T*;
      sp->~Ptr();
    };
  }

  void move_from(UniqueFunction&& other) noexcept {
    if (!other.invoke_) {
      return;
    }
    invoke_ = other.invoke_;
    destroy_ = other.destroy_;
    move_ = other.move_;
    move_(object(), other.object());
    other.invoke_ = nullptr;
    other.destroy_ = nullptr;
    other.move_ = nullptr;
  }

  void* object() noexcept {
    return storage_;
  }

  alignas(std::max_align_t) unsigned char storage_[Capacity];
  invoke_fn invoke_ = nullptr;
  destroy_fn destroy_ = nullptr;
  move_fn move_ = nullptr;
};

} // namespace asio
} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_UNIQUE_FUNCTION_HPP
