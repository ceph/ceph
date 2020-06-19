#pragma once

#include "include/encoding.h"


template <class T>
class dirty_val {
  T val;
  bool dirty;
public:
  dirty_val() : dirty(false) {}
  dirty_val(const T& _val, bool _dirty = false) : val(_val),
                                                  dirty(_dirty) {}

  T& operator=(const T& _val) {
    return set(_val, true);
  }

  bool operator==(const dirty_val<T>& dv) const {
    return val == dv.val;
  }

  template <class TT>
  bool operator==(const TT& v) const {
    return val == v;
  }

  template <class TT>
  bool operator!=(const TT& v) const {
    return !(val == v);
  }

  bool is_dirty() const {
    return dirty;
  }

  operator bool() const {
    return is_dirty();
  }

  const T& operator*() const {
    return val;
  }

  const T *operator->() const {
    return &val;
  }

  T& modify() {
    dirty = true;
    return val;
  }

  const T& clean() {
    dirty = false;
    return val;
  }

  T& raw() { /* raw access to val without updating dirty state */
    return val;
  }

  T& set(const T& _val, bool _dirty = true) {
    val = _val;
    dirty = _dirty;
    return val;
  }

  void encode_val(bufferlist& bl) const {
    encode(val, bl);
  }

  void decode_val(bufferlist::const_iterator& bl) {
    decode(val, bl);
  }

};


template<class T>
inline void encode(const dirty_val<T>& dv, bufferlist& bl)
{
  dv.encode_val(bl);
}

template<class T>
inline void decode(dirty_val<T>& dv, bufferlist::const_iterator& bl)
{
  dv.decode_val(bl);
}

