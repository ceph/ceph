#pragma once
#include <memory>
#include <type_traits>

template <class T>
struct auto_shared_ptr : public std::shared_ptr<T>
{
  using Base = std::shared_ptr<T>;

  auto_shared_ptr() { }
  auto_shared_ptr(decltype(nullptr)) { }

  template <class Y>
    requires std::convertible_to<Y*, T*>
  auto_shared_ptr(const std::shared_ptr<Y>& src)
      : Base(src)
  {
  }

  template <class Y>
    requires std::convertible_to<Y*, T*>
  auto_shared_ptr(std::shared_ptr<Y>&& src)
      : Base(src)
  {
  }

  // an implicit constructor from a conforming pointer that will add a shared reference to the target
  template <class Y>
    requires std::convertible_to<T*, const std::enable_shared_from_this<Y>*>
  auto_shared_ptr(std::enable_shared_from_this<Y>* src)
      : Base(src ? std::static_pointer_cast<T, Y>(src->shared_from_this()) : Base())
  {
  }

  operator T*() const { return this->get(); }
  operator Base&() { return *this; }
  operator const Base&() const { return *this; }

  // allow assignment from a conforming pointer that will add a shared reference to the target.
  template <class Y>
    requires std::convertible_to<T*, const std::enable_shared_from_this<Y>*>
  auto_shared_ptr<T>& operator=(std::enable_shared_from_this<Y>* rhs)
  {
    if (rhs) {
      *this = std::static_pointer_cast<T, Y>(rhs->shared_from_this());
    } else {
      this->reset();
    }
    return *this;
  }

  template <class Y>
    requires std::convertible_to<Y*, T*>
  auto_shared_ptr<T>& operator=(const std::shared_ptr<Y>& rhs)
  {
    (void)Base::operator=(rhs);
    return *this;
  }

  template <class Y>
    requires std::convertible_to<Y*, T*>
  auto_shared_ptr<T>& operator=(std::shared_ptr<Y>&& rhs)
  {
    (void)Base::operator=(rhs);
    return *this;
  }
};
