// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include "include/buffer_fwd.h"
#include "msg/Message.h"

namespace ceph {
  class Formatter;
}

struct Dencoder {
  virtual ~Dencoder() {}
  virtual std::string decode(bufferlist bl, uint64_t seek) = 0;
  virtual void encode(bufferlist& out, uint64_t features) = 0;
  virtual void dump(ceph::Formatter *f) = 0;
  virtual void copy() {
    std::cerr << "copy operator= not supported" << std::endl;
  }
  virtual void copy_ctor() {
    std::cerr << "copy ctor not supported" << std::endl;
  }
  virtual void generate() = 0;
  virtual int num_generated() = 0;
  virtual std::string select_generated(unsigned n) = 0;
  virtual bool is_deterministic() = 0;
  virtual void set_stray_okay() = 0;
  unsigned get_struct_v(bufferlist bl, uint64_t seek) const {
    auto p = bl.cbegin(seek);
    uint8_t struct_v = 0;
    ceph::decode(struct_v, p);
    return struct_v;
  }
  //virtual void print(ostream& out) = 0;
};

template<class T>
class DencoderBase : public Dencoder {
  using deleter_type = std::function<void(T*)>;
  using ptr_type = std::unique_ptr<T, deleter_type>;
protected:
  enum class do_delete {
    no,
    yes,
  };
  static ptr_type make_ptr(T* ptr, do_delete del) {
    auto deleter = (del == do_delete::yes ?
		    deleter_type{std::default_delete<T>{}} :
		    deleter_type{[](T*) {}});
    return ptr_type{ptr, deleter};
  }
  ptr_type m_object = make_ptr(nullptr, do_delete::yes);
  std::list<T> m_list;
  bool stray_okay;
  bool nondeterministic;
public:
  DencoderBase(bool stray_okay, bool nondeterministic)
    : stray_okay(stray_okay),
      nondeterministic(nondeterministic) {}
  ~DencoderBase() override {}

  std::string decode(bufferlist bl, uint64_t seek) override {
    auto p = bl.cbegin();
    p.seek(seek);
    m_object = make_ptr(new T, do_delete::yes);
    try {
      using ceph::decode;
      decode(*m_object, p);
    }
    catch (buffer::error& e) {
      return e.what();
    }
    if (!stray_okay && !p.end()) {
      std::ostringstream ss;
      ss << "stray data at end of buffer, offset " << p.get_off();
      return ss.str();
    }
    return {};
  }

  void encode(bufferlist& out, uint64_t features) override = 0;

  void dump(ceph::Formatter *f) override {
    assert(m_object);
    m_object->dump(f);
  }
  void generate() override {
    m_list = T::generate_test_instances();
  }
  int num_generated() override {
    return m_list.size();
  }
  std::string select_generated(unsigned i) override {
    // allow 0- or 1-based (by wrapping)
    if (i == 0)
      i = m_list.size();
    if ((i == 0) || (i > m_list.size()))
      return "invalid id for generated object";
    m_object = make_ptr(std::addressof(*std::next(m_list.begin(), i-1)), do_delete::no);
    return {};
  }

  bool is_deterministic() override {
    return !nondeterministic;
  }

  void set_stray_okay() {
    stray_okay = true;
  }
};

template<class T>
class DencoderImplNoFeatureNoCopy : public DencoderBase<T> {
public:
  DencoderImplNoFeatureNoCopy(bool stray_ok, bool nondeterministic)
    : DencoderBase<T>(stray_ok, nondeterministic) {}
  void encode(bufferlist& out, uint64_t features) override {
    out.clear();
    assert(this->m_object);
    using ceph::encode;
    encode(*this->m_object, out);
  }
};

template<class T>
class DencoderImplNoFeature : public DencoderImplNoFeatureNoCopy<T> {
  using do_delete = typename DencoderBase<T>::do_delete;

public:
  DencoderImplNoFeature(bool stray_ok, bool nondeterministic)
    : DencoderImplNoFeatureNoCopy<T>(stray_ok, nondeterministic) {}
  void copy() override {
    assert(this->m_object);
    T *n = new T;
    *n = *this->m_object;
    this->m_object = this->make_ptr(n, do_delete::yes);
  }
  void copy_ctor() override {
    assert(this->m_object);
    T *n = new T(*this->m_object);
    this->m_object = this->make_ptr(n, do_delete::yes);
  }
};

template<class T>
class DencoderImplFeaturefulNoCopy : public DencoderBase<T> {
public:
  DencoderImplFeaturefulNoCopy(bool stray_ok, bool nondeterministic)
    : DencoderBase<T>(stray_ok, nondeterministic) {}
  void encode(bufferlist& out, uint64_t features) override {
    assert(this->m_object);
    out.clear();
    using ceph::encode;
    encode(*(this->m_object), out, features);
  }
};

template<class T>
class DencoderImplFeatureful : public DencoderImplFeaturefulNoCopy<T> {
  using do_delete = typename DencoderBase<T>::do_delete;

public:
  DencoderImplFeatureful(bool stray_ok, bool nondeterministic)
    : DencoderImplFeaturefulNoCopy<T>(stray_ok, nondeterministic) {}
  void copy() override {
    assert(this->m_object);
    T* n = new T;
    *n = *this->m_object;
    this->m_object = this->make_ptr(n, do_delete::yes);
  }
  void copy_ctor() override {
    assert(this->m_object);
    T *n = new T(*this->m_object);
    this->m_object = this->make_ptr(n, do_delete::yes);
  }
};

template<class T>
class MessageDencoderImpl : public Dencoder {
  ref_t<T> m_object;
  std::list<ref_t<T>> m_list;

public:
  MessageDencoderImpl() : m_object{make_message<T>()} {}
  ~MessageDencoderImpl() override {}

  std::string decode(bufferlist bl, uint64_t seek) override {
    auto p = bl.cbegin();
    p.seek(seek);
    try {
      ref_t<Message> n(decode_message(g_ceph_context, 0, p), false);
      if (!n)
	throw std::runtime_error("failed to decode");
      if (n->get_type() != m_object->get_type()) {
	std::stringstream ss;
	ss << "decoded type " << n->get_type() << " instead of expected " << m_object->get_type();
	throw std::runtime_error(ss.str());
      }
      m_object = ref_cast<T>(n);
    }
    catch (buffer::error& e) {
      return e.what();
    }
    if (!p.end()) {
      std::ostringstream ss;
      ss << "stray data at end of buffer, offset " << p.get_off();
      return ss.str();
    }
    return {};
  }

  void encode(bufferlist& out, uint64_t features) override {
    out.clear();
    encode_message(m_object.get(), features, out);
  }

  void dump(ceph::Formatter *f) override {
    m_object->dump(f);
  }
  void generate() override {
    //m_list = T::generate_test_instances();
  }
  int num_generated() override {
    return m_list.size();
  }
  std::string select_generated(unsigned i) override {
    // allow 0- or 1-based (by wrapping)
    if (i == 0)
      i = m_list.size();
    if ((i == 0) || (i > m_list.size()))
      return "invalid id for generated object";
    m_object = *(std::next(m_list.begin(), i-1));
    return {};
  }
  bool is_deterministic() override {
    return true;
  }
  void set_stray_okay() override {
  }

  //void print(ostream& out) {
  //out << m_object << std::endl;
  //}
};

class DencoderRegistry
{
  using dencoders_t = std::map<std::string_view, Dencoder*>;

public:
  dencoders_t& get() {
    return dencoders;
  }
  void register_dencoder(std::string_view name, Dencoder* denc) {
    dencoders.emplace(name, denc);
  }
private:
  dencoders_t dencoders;
};
