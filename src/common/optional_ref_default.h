
#pragma once

namespace ceph {

/*
 * Why?
 *
 * Can be used to pass optional param by reference, and param can be std::nullopt, in which
 * case we construct a default value and use that as a reference. This is useful for passing
 * in multiple arguments as an optional structure (with default values), and there is no need to check if
 * passed structure is actually initialized.
 */

template <class T>
class optional_ref_default
{
  std::optional<T> default_ref_val;
  std::optional<std::reference_wrapper<T>> ref;

public:
  optional_ref_default(std::nullopt_t opt) : default_ref_val(T()),
                                             ref(*default_ref_val) {}

  optional_ref_default(T& t) : ref(t) {}

  T *operator->() {
    return &(ref->get());
  }

  T& operator*() {
    return ref->get();
  }
};

}
